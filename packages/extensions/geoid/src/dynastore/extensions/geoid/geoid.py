import os
import html
import logging
import itertools
from contextlib import asynccontextmanager
from typing import List, Any, Dict, Optional, Callable

from fastapi import FastAPI, APIRouter
from fastapi.responses import HTMLResponse

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.models.protocols.web import WebOverrideProtocol, WebPageProtocol, StaticFilesProtocol

# Use the module-level decorators so Web discovers pages and static files automatically
from dynastore.modules.web.decorators import expose_static, expose_web_page

from .lookup_router import router as lookup_router

import user_agents  # Enforces installation-driven discovery

logger = logging.getLogger(__name__)


_GEOID_HOME_COPY = {
    "en": {
        "hero_lead": "OGC-native geospatial catalog",
        "hero_accent": "at planetary scale",
        "hero_body": "A multi-tenant catalog services platform. Manage trillions of features across isolated tenants and expose them through every major OGC API standard — without bespoke integrations.",
        "cta_docs": "Read the docs",
        "cta_explore": "Explore data",
        "pillars_heading": "Three Pillars Architecture",
        "pillars_sub": "Strict separation of concerns prevents the \"big ball of mud\".",
        "pillar_modules_kicker": "Pillar I",
        "pillar_modules_title": "Modules",
        "pillar_modules_body": "Backend-agnostic libraries. Single source of truth per data domain. One module owns one table, no HTTP.",
        "pillar_extensions_kicker": "Pillar II",
        "pillar_extensions_title": "Extensions",
        "pillar_extensions_body": "Stateless HTTP adapters. Translate requests into module calls. Plug in new OGC standards without touching the core.",
        "pillar_tasks_kicker": "Pillar III",
        "pillar_tasks_title": "Tasks",
        "pillar_tasks_body": "Isolated background workers exposed through OGC API – Processes. Ingestion, indexing and analysis run in their own containers.",
        "matrix_heading": "Standards Coverage",
        "matrix_sub": "Each badge fetches its OGC API conformance endpoint live.",
        "cap_heading": "Capabilities",
        "cap_tenancy_title": "Multi-tenancy",
        "cap_tenancy_body": "Each catalog maps to a distinct PostgreSQL schema. Zero data leakage. Rename without data movement.",
        "cap_ogc_title": "OGC native",
        "cap_ogc_body": "Features (Parts 1-4), STAC 1.0.0, Coverages, Tiles, Maps, Processes, Records, DGGS, Connected Systems, Moving Features, Styles.",
        "cap_scale_title": "Trillion-row scale",
        "cap_scale_body": "Lazy JIT partitioning via DB triggers. Base62-encoded physical schemas. No upfront capacity planning.",
        "cap_authz_title": "ABAC / RBAC",
        "cap_authz_body": "Fine-grained, declarative authorization. Identity matchers + content-hash gating for write paths.",
        "cap_assets_title": "Asset registry",
        "cap_assets_body": "Stable URIs for every asset. Content-hash deduplication. Bridges STAC items to physical files in object storage.",
        "cap_olap_title": "OLAP Dimensions",
        "cap_olap_body": "Paginated datacube dimensions (research). Bridges OGC API to OLAP query patterns.",
        "qs_heading": "Get started",
        "qs_step1": "1. Discover catalogs",
        "qs_step2": "2. Open a notebook",
        "qs_step2_link": "Browse the JupyterLite notebooks →",
        "qs_step3": "3. Read the docs",
        "qs_step3_link": "Architecture, API reference, contributing →",
    },
    "es": {
        "hero_lead": "Catálogo geoespacial nativo OGC",
        "hero_accent": "a escala planetaria",
        "hero_body": "Una plataforma de servicios de catálogo multitenant. Gestione billones de entidades en tenants aislados y expóngalas a través de todos los estándares OGC API principales — sin integraciones a medida.",
        "cta_docs": "Leer la documentación",
        "cta_explore": "Explorar datos",
        "pillars_heading": "Arquitectura de los Tres Pilares",
        "pillars_sub": "La separación estricta de responsabilidades evita el \"big ball of mud\".",
        "pillar_modules_kicker": "Pilar I",
        "pillar_modules_title": "Módulos",
        "pillar_modules_body": "Bibliotecas independientes del backend. Fuente única de verdad por dominio. Un módulo es dueño de una tabla, sin HTTP.",
        "pillar_extensions_kicker": "Pilar II",
        "pillar_extensions_title": "Extensiones",
        "pillar_extensions_body": "Adaptadores HTTP sin estado. Traducen solicitudes en llamadas a módulos. Conecte nuevos estándares OGC sin tocar el núcleo.",
        "pillar_tasks_kicker": "Pilar III",
        "pillar_tasks_title": "Tareas",
        "pillar_tasks_body": "Workers en segundo plano aislados expuestos a través de OGC API – Processes. La ingesta, la indexación y el análisis corren en sus propios contenedores.",
        "matrix_heading": "Cobertura de estándares",
        "matrix_sub": "Cada insignia consulta su endpoint OGC API conformance en vivo.",
        "cap_heading": "Capacidades",
        "cap_tenancy_title": "Multitenancy",
        "cap_tenancy_body": "Cada catálogo se asigna a un esquema PostgreSQL distinto. Cero filtración de datos. Renombrado sin movimiento de datos.",
        "cap_ogc_title": "OGC nativo",
        "cap_ogc_body": "Features (Partes 1-4), STAC 1.0.0, Coverages, Tiles, Maps, Processes, Records, DGGS, Connected Systems, Moving Features, Styles.",
        "cap_scale_title": "Escala de billones de filas",
        "cap_scale_body": "Particionado JIT mediante triggers. Esquemas físicos en base62. Sin planificación de capacidad anticipada.",
        "cap_authz_title": "ABAC / RBAC",
        "cap_authz_body": "Autorización declarativa de grano fino. Identity matchers + content-hash gating para las rutas de escritura.",
        "cap_assets_title": "Registro de assets",
        "cap_assets_body": "URIs estables para cada asset. Deduplicación por content-hash. Une los items STAC con los archivos físicos en object storage.",
        "cap_olap_title": "Dimensiones OLAP",
        "cap_olap_body": "Dimensiones de datacube paginadas (research). Conecta OGC API con patrones de consulta OLAP.",
        "qs_heading": "Empezar",
        "qs_step1": "1. Descubrir catálogos",
        "qs_step2": "2. Abrir un notebook",
        "qs_step2_link": "Explorar los notebooks JupyterLite →",
        "qs_step3": "3. Leer la documentación",
        "qs_step3_link": "Arquitectura, referencia de API, contribuir →",
    },
    "fr": {
        "hero_lead": "Catalogue géospatial natif OGC",
        "hero_accent": "à l'échelle planétaire",
        "hero_body": "Une plateforme de services de catalogue multilocataire. Gérez des milliers de milliards d'entités dans des locataires isolés et exposez-les via toutes les normes OGC API majeures — sans intégrations sur mesure.",
        "cta_docs": "Lire la documentation",
        "cta_explore": "Explorer les données",
        "pillars_heading": "Architecture à trois piliers",
        "pillars_sub": "La séparation stricte des responsabilités évite le \"big ball of mud\".",
        "pillar_modules_kicker": "Pilier I",
        "pillar_modules_title": "Modules",
        "pillar_modules_body": "Bibliothèques indépendantes du backend. Source unique de vérité par domaine. Un module possède une table, sans HTTP.",
        "pillar_extensions_kicker": "Pilier II",
        "pillar_extensions_title": "Extensions",
        "pillar_extensions_body": "Adaptateurs HTTP sans état. Traduisent les requêtes en appels de modules. Branchez de nouvelles normes OGC sans toucher au cœur.",
        "pillar_tasks_kicker": "Pilier III",
        "pillar_tasks_title": "Tâches",
        "pillar_tasks_body": "Workers d'arrière-plan isolés exposés via OGC API – Processes. L'ingestion, l'indexation et l'analyse s'exécutent dans leurs propres conteneurs.",
        "matrix_heading": "Couverture des normes",
        "matrix_sub": "Chaque badge interroge son endpoint OGC API conformance en direct.",
        "cap_heading": "Capacités",
        "cap_tenancy_title": "Multilocation",
        "cap_tenancy_body": "Chaque catalogue est lié à un schéma PostgreSQL distinct. Zéro fuite de données. Renommage sans déplacement.",
        "cap_ogc_title": "OGC natif",
        "cap_ogc_body": "Features (Parties 1-4), STAC 1.0.0, Coverages, Tiles, Maps, Processes, Records, DGGS, Connected Systems, Moving Features, Styles.",
        "cap_scale_title": "Échelle massive",
        "cap_scale_body": "Partitionnement JIT via triggers. Schémas physiques en base62. Aucune planification de capacité préalable.",
        "cap_authz_title": "ABAC / RBAC",
        "cap_authz_body": "Autorisation déclarative à grain fin. Identity matchers + content-hash gating pour les chemins d'écriture.",
        "cap_assets_title": "Registre d'assets",
        "cap_assets_body": "URIs stables pour chaque asset. Déduplication par content-hash. Lie les items STAC aux fichiers physiques en object storage.",
        "cap_olap_title": "Dimensions OLAP",
        "cap_olap_body": "Dimensions de datacube paginées (research). Pont entre OGC API et patterns de requêtes OLAP.",
        "qs_heading": "Démarrer",
        "qs_step1": "1. Découvrir les catalogues",
        "qs_step2": "2. Ouvrir un notebook",
        "qs_step2_link": "Parcourir les notebooks JupyterLite →",
        "qs_step3": "3. Lire la documentation",
        "qs_step3_link": "Architecture, référence API, contribuer →",
    },
}


class Geoid(ExtensionProtocol, WebOverrideProtocol, WebPageProtocol, StaticFilesProtocol):
    """
    GeoID extension — a content provider that plugs into the Web extension's
    routing infrastructure.

    Responsibilities:
    - Override the platform home section with GeoID branding (is_embed page).
    - Provide the /web/pages/geoid service page.
    - Serve geoid static assets under the "geoid" prefix.
    - Declare style overrides for landing-page rebranding.

    Everything else (middleware, root redirects, /web/config/pages, /web/docs-*,
    /web/health, /{prefix}/{filename} static dispatch) is handled by the Web
    extension, which discovers this class through the StaticFilesProtocol,
    WebPageProtocol and WebOverrideProtocol at configure_app time.
    """

    priority: int = -10  # Load before Web so policies are ready

    def get_web_pages(self):
        from dynastore.extensions.tools.web_collect import collect_web_pages
        return collect_web_pages(self)

    def __init__(self, app: FastAPI):
        self.app = app
        self.router: APIRouter = lookup_router
        self.static_dir = os.path.join(os.path.dirname(__file__), "static")

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        from dynastore.modules.iam.conditions import register_condition_handler
        from .conditions import CatalogLookupAudienceHandler, CollectionWriteAudienceHandler
        from .policies import register_geoid_policies

        register_condition_handler(CatalogLookupAudienceHandler())
        register_condition_handler(CollectionWriteAudienceHandler())
        register_geoid_policies()
        yield

    # ------------------------------------------------------------------ #
    #  StaticFilesProtocol                                                 #
    # ------------------------------------------------------------------ #

    def get_static_prefix(self) -> str:
        """Prefix under which geoid static files are served: /web/geoid/<filename>."""
        return "geoid"

    async def is_file_provided(self, path: str) -> bool:
        """Return True if *path* (relative to the geoid static dir) exists on disk."""
        if not self.static_dir:
            return False
        full_path = os.path.join(self.static_dir, path.lstrip("/"))
        return os.path.isfile(full_path)

    async def list_static_files(
        self, query: Optional[str] = None, limit: int = 100, offset: int = 0
    ) -> List[str]:
        """Return absolute paths of geoid static files, optionally filtered and paginated."""
        if not self.static_dir or not os.path.isdir(self.static_dir):
            return []

        files: List[str] = []
        for root, _, filenames in os.walk(self.static_dir):
            for filename in filenames:
                full_path = os.path.join(root, filename)
                rel_path = os.path.relpath(full_path, self.static_dir)
                if query and query.lower() not in rel_path.lower():
                    continue
                files.append(full_path)

        files.sort()
        return list(itertools.islice(files, offset, offset + limit))

    # Static files are registered via the StaticFilesProtocol methods above
    # (get_static_prefix / list_static_files / is_file_provided), discovered by
    # WebModule's global protocol scan.  No @expose_static decorator needed here.

    # ------------------------------------------------------------------ #
    #  WebPageProtocol                                                     #
    # ------------------------------------------------------------------ #

    def get_web_page_config(self) -> Dict[str, Any]:
        """
        Metadata for the Geoid service page, used by WebModule for navigation
        and role-based visibility.  Served at /web/pages/geoid via render_page().
        """
        return {
            "id": "geoid",
            "title": {
                "en": "Geoid Service",
                "es": "Servicio de Geoide",
                "fr": "Service géoïde",
            },
            "icon": "fa-earth-americas",
            "description": {
                "en": "Geoid height calculation service.",
                "es": "Cálculo de altura del geoide.",
                "fr": "Calcul de la hauteur du géoïde.",
            },
            "priority": 0,
            "is_embed": False,
        }

    async def render_page(self, request: Any, language: str = "en") -> Any:
        """
        Called by Web's /web/pages/geoid handler.  Delegates to geoid_page()
        so all rendering logic lives in one place.
        """
        return self.geoid_page(language=language)

    def geoid_page(self, language: str = "en") -> HTMLResponse:
        """
        Serve the language-specific Geoid SPA entry point.

        Lookup order:
          1. static/index_{lang}.html  (e.g. index_es.html)
          2. static/index.html         (language-neutral fallback)
          3. 404 response if neither exists
        """
        lang_code = language.lower().split("-")[0]
        if lang_code not in ["en", "es", "fr"]:
            lang_code = "en"

        index_path = os.path.join(self.static_dir, f"index_{lang_code}.html")
        if not os.path.exists(index_path):
            index_path = os.path.join(self.static_dir, "index.html")

        if os.path.exists(index_path):
            with open(index_path, "r", encoding="utf-8") as f:
                return HTMLResponse(content=f.read())
        return HTMLResponse("Geoid service page not found", status_code=404)

    # ------------------------------------------------------------------ #
    #  WebOverrideProtocol                                                 #
    # ------------------------------------------------------------------ #

    def get_landing_page_override(self) -> Optional[Dict[str, Any]]:
        """
        Rebrand the platform landing page with GeoID / UN-FAO identity.
        Consumed by WebModule.get_landing_page_override() aggregators.
        """
        return {
            "title": {
                "en": "GeoID - UN-FAO",
                "es": "GeoID - UN-FAO",
                "fr": "GeoID - UN-FAO",
            },
            "description": {
                "en": "Global Geoid Height Service",
                "es": "Servicio Global de Altura del Geoide",
                "fr": "Service mondial de hauteur du géoïde",
            },
            "icon": "fa-globe",
        }

    def get_style_overrides(self) -> List[str]:
        """CSS paths injected into the platform shell for GeoID branding."""
        return ["/web/geoid/static/custom_style.css"]

    def get_component_overrides(self) -> Dict[str, Callable]:
        """Reserved for future UI component substitutions; currently empty."""
        return {}

    # ------------------------------------------------------------------ #
    #  Home embed — injected into the Web platform's home page            #
    # ------------------------------------------------------------------ #

    @expose_web_page(
        page_id="geoid_home",
        title="GeoID Home",
        icon="fa-home",
        priority=-500,
        section="home",
        is_embed=True,
    )
    async def geoid_home_page(self, language: str = "en") -> str:
        """Platform-home embed: Three Pillars, live OGC matrix, capabilities, get-started.

        Returned HTML is server-rendered. Operator-supplied copy strings are
        passed through ``html.escape`` before interpolation. The injected
        ``<script>`` blocks build the DOM via ``createElement`` only — no
        ``innerHTML`` — so the page is XSS-safe by construction.
        """
        lang = (language or "en").lower().split("-")[0]
        if lang not in ("en", "es", "fr"):
            lang = "en"

        copy = _GEOID_HOME_COPY[lang]
        e = lambda k: html.escape(copy[k])

        return f"""
        <script>
            document.querySelectorAll('.ds-default-home').forEach(function(el){{ el.style.display = 'none'; }});
        </script>
        <script src="/web/geoid/conformance-matrix.js" defer></script>
        <div class="mt-12 space-y-12">

          <!-- Hero -->
          <section class="text-center py-8">
            <h2 class="text-3xl md:text-5xl font-bold text-white tracking-tight mb-4">
              {e('hero_lead')}
              <span class="block text-transparent bg-clip-text bg-gradient-to-r from-blue-400 to-emerald-400">
                {e('hero_accent')}
              </span>
            </h2>
            <p class="text-slate-400 max-w-3xl mx-auto text-lg">{e('hero_body')}</p>
            <div class="flex gap-3 justify-center mt-8 flex-wrap">
              <button onclick="switchTab('docs')"
                class="px-6 py-3 rounded-xl bg-blue-600 hover:bg-blue-500 text-white font-semibold transition-all shadow-lg shadow-blue-500/20">
                {e('cta_docs')}
              </button>
              <button onclick="switchTab('stac_browser')"
                class="px-6 py-3 rounded-xl bg-slate-800 hover:bg-slate-700 text-white font-semibold transition-all border border-white/10">
                {e('cta_explore')}
              </button>
            </div>
          </section>

          <!-- Three Pillars -->
          <section>
            <h3 class="text-2xl font-bold text-white mb-2">{e('pillars_heading')}</h3>
            <p class="text-slate-400 mb-6">{e('pillars_sub')}</p>
            <div class="grid md:grid-cols-3 gap-4">
              <div class="glass-panel p-6 rounded-xl border border-white/5">
                <div class="text-xs uppercase tracking-widest font-bold text-blue-400 mb-2">{e('pillar_modules_kicker')}</div>
                <h4 class="text-xl font-semibold text-white mb-2">{e('pillar_modules_title')}</h4>
                <p class="text-slate-400 text-sm">{e('pillar_modules_body')}</p>
              </div>
              <div class="glass-panel p-6 rounded-xl border border-white/5">
                <div class="text-xs uppercase tracking-widest font-bold text-emerald-400 mb-2">{e('pillar_extensions_kicker')}</div>
                <h4 class="text-xl font-semibold text-white mb-2">{e('pillar_extensions_title')}</h4>
                <p class="text-slate-400 text-sm">{e('pillar_extensions_body')}</p>
              </div>
              <div class="glass-panel p-6 rounded-xl border border-white/5">
                <div class="text-xs uppercase tracking-widest font-bold text-purple-400 mb-2">{e('pillar_tasks_kicker')}</div>
                <h4 class="text-xl font-semibold text-white mb-2">{e('pillar_tasks_title')}</h4>
                <p class="text-slate-400 text-sm">{e('pillar_tasks_body')}</p>
              </div>
            </div>
          </section>

          <!-- Live OGC matrix -->
          <section>
            <h3 class="text-2xl font-bold text-white mb-2">{e('matrix_heading')}</h3>
            <p class="text-slate-400 mb-6">{e('matrix_sub')}</p>
            <div id="ogc-matrix" data-snapshot="/web/geoid/conformance-snapshot.json"
                 class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3"></div>
            <p id="ogc-matrix-footer" class="mt-4 text-sm text-slate-500 text-center"></p>
          </section>

          <!-- Capabilities -->
          <section>
            <h3 class="text-2xl font-bold text-white mb-2">{e('cap_heading')}</h3>
            <div class="grid md:grid-cols-2 lg:grid-cols-3 gap-4">
              <div class="glass-panel p-5 rounded-xl border border-white/5">
                <h4 class="font-semibold text-white mb-1">{e('cap_tenancy_title')}</h4>
                <p class="text-slate-400 text-sm">{e('cap_tenancy_body')}</p>
              </div>
              <div class="glass-panel p-5 rounded-xl border border-white/5">
                <h4 class="font-semibold text-white mb-1">{e('cap_ogc_title')}</h4>
                <p class="text-slate-400 text-sm">{e('cap_ogc_body')}</p>
              </div>
              <div class="glass-panel p-5 rounded-xl border border-white/5">
                <h4 class="font-semibold text-white mb-1">{e('cap_scale_title')}</h4>
                <p class="text-slate-400 text-sm">{e('cap_scale_body')}</p>
              </div>
              <div class="glass-panel p-5 rounded-xl border border-white/5">
                <h4 class="font-semibold text-white mb-1">{e('cap_authz_title')}</h4>
                <p class="text-slate-400 text-sm">{e('cap_authz_body')}</p>
              </div>
              <div class="glass-panel p-5 rounded-xl border border-white/5">
                <h4 class="font-semibold text-white mb-1">{e('cap_assets_title')}</h4>
                <p class="text-slate-400 text-sm">{e('cap_assets_body')}</p>
              </div>
              <div class="glass-panel p-5 rounded-xl border border-white/5">
                <h4 class="font-semibold text-white mb-1">{e('cap_olap_title')}</h4>
                <p class="text-slate-400 text-sm">{e('cap_olap_body')}</p>
              </div>
            </div>
          </section>

          <!-- Get started -->
          <section>
            <h3 class="text-2xl font-bold text-white mb-2">{e('qs_heading')}</h3>
            <ol class="space-y-4 list-none pl-0">
              <li class="glass-panel p-4 rounded-xl border border-white/5">
                <div class="text-sm font-semibold text-slate-300 mb-2">{e('qs_step1')}</div>
                <pre class="bg-slate-950 rounded-lg p-3 overflow-auto text-xs text-emerald-300 font-mono">curl https://example.org/stac/catalogs</pre>
              </li>
              <li class="glass-panel p-4 rounded-xl border border-white/5">
                <div class="text-sm font-semibold text-slate-300 mb-2">{e('qs_step2')}</div>
                <a href="/notebooks" class="text-blue-400 hover:text-blue-300 text-sm">{e('qs_step2_link')}</a>
              </li>
              <li class="glass-panel p-4 rounded-xl border border-white/5">
                <div class="text-sm font-semibold text-slate-300 mb-2">{e('qs_step3')}</div>
                <button type="button" onclick="switchTab('docs')" class="text-blue-400 hover:text-blue-300 text-sm bg-transparent border-0 p-0 cursor-pointer text-left">{e('qs_step3_link')}</button>
              </li>
            </ol>
          </section>
        </div>
        """
