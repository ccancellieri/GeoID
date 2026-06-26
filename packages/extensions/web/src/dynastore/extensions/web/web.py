#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

import asyncio
import os
import hashlib
import itertools
import logging
import re
from contextlib import asynccontextmanager
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Callable, ClassVar, Dict, List, Optional, Tuple, cast

from fastapi import APIRouter, FastAPI, Response, HTTPException, Request, Query, Depends

from fastapi.middleware.gzip import GZipMiddleware
from dynastore.extensions.web.cors_middleware import DynamicCORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.extensions.web.decorators import expose_static, expose_web_page
from dynastore.models.auth import Condition
from dynastore.models.protocols.authorization import IamRolesConfig
from dynastore.models.protocols.policies import Policy, Role, Principal
from dynastore.tools.discovery import get_protocol, get_protocols, register_plugin
from dynastore.extensions.tools.language_utils import get_language

# Register public access policy for web extension
logger = logging.getLogger(__name__)

# Module-level scheme — ``scheme_name`` must match the key declared by
# IAM's ``build_iam_openapi_schema`` so Swagger UI ties dashboard routes
# to the same security scheme entry as the rest of the platform.

def _web_policies(sysadmin_role_name: Optional[str] = None) -> List[Policy]:
    """Pure declaration of the web extension's authorization policies.

    Returned to IAM via ``Web.get_policies``; never registers anything.
    The per-catalog dashboard policy carries ``sysadmin_role`` through
    its ``catalog_membership_required`` condition so seed-time bindings
    and runtime bypass checks share a single source of truth.
    """
    sysadmin_role_name = sysadmin_role_name or IamRolesConfig().sysadmin_role_name

    # Per-extension static prefixes registered via @expose_static are served
    # at /web/{prefix}/. Anonymous browser pages load their JS/CSS from these
    # paths, so each prefix must appear in the anonymous ALLOW list.
    # Patterns are anchored to /web/{prefix}/ — an unanchored /web/stac
    # pattern would also match /web/stac-authoring/… accidentally.
    _literal_extension_prefixes: List[str] = [
        "/web/stac/.*",
        "/web/records/.*",
        "/web/features/.*",
        "/web/assets/.*",
        "/web/edr/.*",
        "/web/movingfeatures/.*",
        "/web/tiles/.*",
        "/web/auth/.*",
        "/web/coverages/.*",
    ]

    # The complete set of /web/... resource patterns that are already
    # authoritative in the policy (both the extension-prefix list above
    # and the hardcoded entries in web_public_access.resources).  The
    # dynamic block must not emit a wildcard pattern for any prefix that
    # is already referenced here in any form — even a narrow entry like
    # /web/dashboard/?$ must prevent the dynamic block from widening it
    # to /web/dashboard/.*, which would open the gated data endpoints.
    _all_literal_web_resources: List[str] = _literal_extension_prefixes + [
        "/web/?$",
        "/web/pages/.*",
        "/web/extension-static/.*",
        "/web/static/.*",
        "/web/website/.*",
        "/web/docs-content/.*",
        "/web/docs-manifest$",
        "/web/config/.*",
        "/web/dashboard/?$",
        "/web/lite/.*",
    ]

    # Best-effort dynamic derivation: if WebModule is already registered,
    # collect every prefix whose metadata marks it public=True and append
    # any that are not already covered by the literal list. Extensions
    # added in the future are thereby covered without a manual edit here.
    # The whole block is wrapped in try/except so a missing WebModule
    # (startup ordering, test isolation) never prevents the literal floor
    # from being returned.
    #
    # Prefix-aware skip: if ANY literal resource already references a
    # prefix (anchored, bare, or regex-prefixed forms), the literal list
    # is authoritative for that prefix and the dynamic block must not emit
    # a pattern for it.  This prevents the dynamic block from widening a
    # deliberately narrow literal entry — e.g. /web/dashboard/?$ allows
    # only the shell page while the gated data endpoints stay protected.
    def _literal_covers_prefix(pfx: str) -> bool:
        bare = f"/web/{pfx}/"
        anchored = f"^/web/{pfx}/"
        bare_no_slash = f"/web/{pfx}"
        for r in _all_literal_web_resources:
            if (
                r.startswith(bare)
                or r.startswith(anchored)
                or r == bare_no_slash
                or r.startswith(bare_no_slash + "?")
                or r.startswith(bare_no_slash + "$")
            ):
                return True
        return False

    _dynamic_extension_prefixes: List[str] = []
    try:
        _web_mod = get_protocol(WebModuleProtocol)
        if _web_mod is not None and hasattr(_web_mod, "get_static_prefix_meta"):
            for _pfx, _meta in _web_mod.get_static_prefix_meta().items():
                if not _meta.get("public", True):
                    continue
                if _literal_covers_prefix(_pfx):
                    continue
                _pattern = f"/web/{re.escape(_pfx)}/.*"
                _dynamic_extension_prefixes.append(_pattern)
    except Exception:
        logger.debug(
            "_web_policies: could not derive dynamic extension prefixes from "
            "WebModule; literal list remains the guaranteed floor.",
            exc_info=True,
        )

    # Deduplicate preserving order (literals first, then novel dynamic ones).
    _seen_pfx: set = set()
    _all_extension_prefixes: List[str] = []
    for _p in _literal_extension_prefixes + _dynamic_extension_prefixes:
        if _p not in _seen_pfx:
            _seen_pfx.add(_p)
            _all_extension_prefixes.append(_p)

    return [
        # Anonymous-allowed web paths. Resource patterns are anchored where
        # ambiguity matters because PolicyService uses ``re.match`` (matches
        # at start of string, no implicit end anchor) — an unanchored
        # ``/web`` pattern would also match
        # ``/web/dashboard/catalogs/X/stats`` and accidentally allow
        # anonymous access to gated dashboard endpoints.
        Policy(
            id="web_public_access",
            description="Allows anonymous access to web UI, pages, and static assets. "
                        "Resources are enumerated explicitly + anchored so dashboard "
                        "data endpoints stay gated by web_dashboard_*_access policies.",
            actions=["GET", "OPTIONS"],
            resources=[
                "/$",
                "/health$",
                "/docs.*",
                "/openapi.json",
                "/favicon.ico.*",
                "/web/?$",
                "/web/pages/.*",
                "/web/extension-static/.*",
                "/web/static/.*",
                "/web/website/.*",
                "/web/docs-content/.*",
                "/web/docs-manifest$",
                "/web/config/.*",
                "/web/dashboard/?$",
                "/web/lite/.*",
                "/.well-known/.*",
                "/processes.*",
                "/configs/registry$",
                "/configs/registry/.*",
                f"/configs/plugins/{WebConfig.class_key()}$",
                *_all_extension_prefixes,
            ],
            effect="ALLOW",
        ),
        Policy(
            id="web_sysadmin_access",
            description="Grants sysadmin write access to web admin management endpoints.",
            actions=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
            resources=[
                "/web/admin",
                "/web/admin/",
                "/web/admin/.*",
                "/web/pages/exposure",
                "/web/pages/governance",
                "/web/pages/stac-authoring",
                "/web/pages/ingest",
                r"^/web/admin/pages/[^/]+/providers$",
            ],
            effect="ALLOW",
        ),
        Policy(
            id="web_admin_access",
            description="Allows admins and authenticated users to load the catalog-scoped admin pages.",
            actions=["GET", "OPTIONS"],
            resources=[
                "/web/pages/governance",
                "/web/pages/stac-authoring",
                "/web/pages/ingest",
            ],
            effect="ALLOW",
        ),
        Policy(
            id="web_dashboard_platform_access",
            description="Sysadmin-only access to platform-tier dashboard endpoints (stats, tasks).",
            actions=["GET", "OPTIONS"],
            resources=[r"^/web/dashboard/(stats|tasks)/?$"],
            effect="ALLOW",
        ),
        Policy(
            id="web_dashboard_ogc_compliance_access",
            description="Read-only OGC conformance check accessible to authenticated users.",
            actions=["GET", "OPTIONS"],
            resources=[r"^/web/dashboard/ogc-compliance/?$"],
            effect="ALLOW",
        ),
        Policy(
            id="web_dashboard_per_catalog_access",
            description="Per-catalog dashboard access; requires catalog membership.",
            actions=["GET", "OPTIONS"],
            resources=[r"^/web/dashboard/catalogs/[^/]+(/.*)?$"],
            effect="ALLOW",
            conditions=[
                Condition(
                    type="catalog_membership_required",
                    config={"sysadmin_role": sysadmin_role_name},
                )
            ],
        ),
    ]


def _web_role_bindings(
    sysadmin_role_name: Optional[str] = None,
    admin_role_name: Optional[str] = None,
    anonymous_role_name: Optional[str] = None,
) -> List[Role]:
    """Pure declaration of the web extension's role-to-policy bindings."""
    cfg = IamRolesConfig()
    sysadmin_role_name = sysadmin_role_name or cfg.sysadmin_role_name
    admin_role_name = admin_role_name or cfg.admin_role_name
    anonymous_role_name = anonymous_role_name or cfg.anonymous_role_name
    # Bind ``web_public_access`` (which whitelists root ``/health``) to BOTH
    # the configured ``anonymous_role_name`` AND the literal default
    # ``"unauthenticated"``. The literal binding is a defence layer: if an
    # operator renames ``IamRolesConfig.anonymous_role_name`` away from
    # ``"unauthenticated"``, the Cloud Run / load-balancer startup probe
    # (which still hits ``/health`` anonymously and gets mapped to the
    # platform's default anonymous role) keeps passing instead of 403'ing
    # the revision into a boot loop. The INSERT_ROLE ON CONFLICT union in
    # ``iam_queries`` dedups when the two names coincide.
    anon_bindings = [Role(name=anonymous_role_name, policies=["web_public_access"])]
    if anonymous_role_name != "unauthenticated":
        anon_bindings.append(Role(name="unauthenticated", policies=["web_public_access"]))
    return [
        *anon_bindings,
        Role(name=sysadmin_role_name, policies=["web_sysadmin_access"]),
        Role(name=admin_role_name, policies=["web_admin_access"]),
        Role(name=sysadmin_role_name, policies=["web_dashboard_platform_access"]),
        Role(name=admin_role_name, policies=["web_dashboard_ogc_compliance_access"]),
        Role(name=admin_role_name, policies=["web_dashboard_per_catalog_access"]),
    ]


def _register_anonymous_principal() -> None:
    """Register the platform's anonymous Principal so discovery returns it.

    Distinct from policy registration: this is identity scaffolding, not
    authz wiring. Kept as an explicit side-effecting call that runs once
    at extension init.
    """
    anonymous_role = IamRolesConfig().anonymous_role_name
    register_plugin(
        Principal(
            provider="system",
            subject_id=anonymous_role,
            display_name="Anonymous User",
            roles=[anonymous_role],
            is_active=True,
        )
    )

def _find_project_root(start_path: str, markers: List[str]) -> Optional[str]:
    """Walks up from start_path to find a directory containing one of the marker files.
    Uses pathlib for robust parent traversal.
    """
    try:
        # Resolve path and ensure we start from a directory
        start = Path(start_path).resolve()
        if start.is_file():
            start = start.parent

        # Iterate up through parents (including current directory)
        for current_path in [start, *start.parents]:
            # Check for marker files in current path
            for marker in markers:
                marker_path = current_path / marker
                if marker_path.exists():
                    # If the marker is a project/config file, accept immediately
                    if marker.lower() in ("pyproject.toml", "setup.py"):
                        logger.info(
                            f"Found project root via marker '{marker}' at: {current_path}"
                        )
                        return str(current_path)

                    # For generic markers (main.py), verify app structure
                    if _has_app_structure(current_path):
                        logger.info(
                            f"Found project root via marker '{marker}' at: {current_path}"
                        )
                        return str(current_path)

        return None
    except Exception as e:
        logger.debug(f"Error resolving project root: {e}")
        return None


def _has_app_structure(root_dir: Path) -> bool:
    """Helper to verify if a directory looks like a Dynastore app."""
    comps = ["modules", "extensions", "tasks"]

    # 1. Check direct children
    for comp in comps:
        if (root_dir / comp).is_dir():
            return True

    # 2. Check src directory
    src_dir = root_dir / "src"
    if src_dir.is_dir():
        for child in src_dir.iterdir():
            if child.is_dir():
                for comp in comps:
                    if (child / comp).is_dir():
                        return True

    # 3. Check immediate subdirectories (permissive check)
    try:
        for child in root_dir.iterdir():
            if child.is_dir():
                for comp in comps:
                    if (child / comp).is_dir():
                        return True
    except Exception:
        pass

    return False


WEB_CONFORMANCE_URIS = [
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas30",
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas31",
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/html",
]

from pydantic import Field
from dynastore.models.protocols.web import WebModuleProtocol, StaticFilesProtocol
from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig

from starlette.types import ASGIApp, Receive, Scope, Send
from starlette.routing import Match, Router
from starlette._utils import get_route_path
from dynastore.models.driver_context import DriverContext


class RelativeSlashRedirectMiddleware:
    """Replace Starlette's redirect_slashes with proxy-safe relative redirects.

    Starlette's built-in redirect_slashes emits an absolute ``Location`` URL
    built from ``scope["path"]`` which does **not** include ``root_path``.
    Behind a prefix-stripping reverse proxy this sends the browser to the
    wrong path (e.g. ``/maps/`` instead of ``/geospatial/v2/api/maps/``).

    This middleware performs the same trailing-slash probe but returns a
    **relative** redirect (just the last path segment + ``/``), which the
    browser resolves correctly regardless of any proxy prefix.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    def _get_router(self) -> "Router | None":
        """Walk the middleware stack to find the underlying Router."""
        app = self.app
        while app is not None:
            if isinstance(app, Router):
                return app
            app = getattr(app, "app", None)
        return None

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            route_path = get_route_path(scope)
            if route_path != "/" and not route_path.endswith("/"):
                # Skip redirect for static file paths (they use {prefix}/{filename:path} pattern)
                # Static files are served at /web/{prefix}/{filename} and must not be redirected
                path_parts = route_path.strip("/").split("/")
                if len(path_parts) >= 2:
                    # Check if this looks like a static file path (prefix/filename)
                    # by checking if there's a registered static provider for the prefix
                    from dynastore.modules import get_protocol
                    from dynastore.models.protocols.web import WebModuleProtocol
                    web_module = get_protocol(WebModuleProtocol)
                    if web_module and hasattr(web_module, 'static_providers'):
                        potential_prefix = path_parts[1] if path_parts[0] == "web" else path_parts[0]
                        if potential_prefix in web_module.static_providers:
                            # This is a static file path - skip redirect
                            await self.app(scope, receive, send)
                            return
                
                router = self._get_router()
                if router is not None:
                    # Skip redirect if the original path already matches a route
                    original_matched = any(
                        route.matches(scope)[0] == Match.FULL
                        for route in router.routes
                    )
                    if not original_matched:
                        probe_scope = dict(scope)
                        probe_scope["path"] = scope["path"] + "/"
                        for route in router.routes:
                            match, _ = route.matches(probe_scope)
                            if match != Match.NONE:
                                segment = route_path.rsplit("/", 1)[-1]
                                response = RedirectResponse(url=f"{segment}/")
                                await response(scope, receive, send)
                                return

        await self.app(scope, receive, send)


class WebConfig(PluginConfig):
    """Configuration for the Web Platform interface.

    Web is ``always_on = True`` on the extension class — its routes are
    mounted unconditionally and the per-scope ``enabled`` toggle from
    ``ExposableConfigMixin`` has no effect.  See the dead-config audit in
    ``find_dead_exposable_configs()`` for the framework invariant.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "modules", "web")

    brand_name: Mutable[str] = "Agro-Informatics Platform"
    brand_subtitle: Mutable[str] = "Catalog Services"
    token_key: Mutable[str] = "ds_token"
    default_language: Mutable[str] = "en"
    enterprise_tier: Mutable[bool] = True
    root_path: Mutable[str] = Field(default_factory=lambda: os.getenv("API_ROOT_PATH", "").rstrip("/"))



class _WebColdBootContributor:
    """Cold-boot contributor that applies web public-access presets at priority=50.

    IAM-aware: skips seeding when PermissionProtocol is not registered in this
    process (non-IAM service tiers); those grants are owned and seeded by the
    IAM-running service.
    """

    name: str = "web"
    priority: int = 50

    async def run(self, engine: Any) -> None:
        from dynastore.tools.discovery import get_protocol as _gp
        from dynastore.models.protocols.policies import PermissionProtocol

        if _gp(PermissionProtocol) is None:
            logger.info(
                "WebService: IAM module not installed in this process — skipping "
                "IAM public-access seeding (web_enable, ogc_conformance_public); "
                "these anonymous grants are seeded by the IAM-running service."
            )
            return

        # Login-page reachability: re-assert web_enable on every cold-boot.
        try:
            from dynastore.modules.storage.presets.lifecycle import bootstrap_preset_if_absent
            await bootstrap_preset_if_absent(engine, preset_name="web_enable", force=True)
        except Exception as exc:  # noqa: BLE001 — login seeding must not abort boot
            logger.error(
                "WebService: cold-boot re-assert of 'web_enable' public-access "
                "policy failed; anonymous users may get 403 on /web (login UI) "
                "until it is applied manually: %s",
                exc,
                exc_info=True,
            )

        # OGC conformance reachability: conditionally apply ogc_conformance_public.
        try:
            from dynastore.models.protocols import ConfigsProtocol
            from dynastore.modules.storage.presets.lifecycle import bootstrap_preset_if_absent as _bpa
            from dynastore.extensions.tools.conformance_config import ConformanceExposureConfig
            import dynastore.extensions.tools.conformance_preset  # noqa: F401 — side-effect registration

            _conf_cfg: ConformanceExposureConfig = ConformanceExposureConfig()
            try:
                _configs_svc = _gp(ConfigsProtocol)
                if _configs_svc is not None:
                    _conf_cfg = await _configs_svc.get_config(ConformanceExposureConfig)
            except Exception:  # noqa: BLE001 — fall back to default (public=True)
                pass

            if _conf_cfg.public:
                await _bpa(engine, preset_name="ogc_conformance_public", force=True)
                logger.info(
                    "WebService: 'ogc_conformance_public' preset applied — "
                    "anonymous GET on /{svc}/conformance and /{svc}/ is now allowed."
                )
            else:
                logger.info(
                    "WebService: ConformanceExposureConfig.public=False — "
                    "skipping 'ogc_conformance_public' preset; conformance endpoints "
                    "remain gated by deny-by-default IAM."
                )
        except Exception as exc:  # noqa: BLE001 — conformance seeding must not abort boot
            logger.error(
                "WebService: cold-boot apply of 'ogc_conformance_public' preset "
                "failed; anonymous users will not be able to read conformance "
                "endpoints until it is applied manually: %s",
                exc,
                exc_info=True,
            )


class Web(ExtensionProtocol, OGCServiceMixin):
    always_on = True
    """
    Core web platform extension.

    Pages (home, docs, dashboard) and static prefixes (static, website,
    dashboard, extension-static) are declared via the @expose_web_page /
    @expose_static decorators and surfaced through the
    WebPageContributor / StaticAssetProvider capability protocols
    implemented on this class.  WebModule iterates those protocols during
    its own lifespan — no reflective class-walking across extensions.

    Legacy WebPageProtocol / StaticFilesProtocol implementations on other
    extensions are still honoured by WebModule; the deduplication guard in
    WebModule.register_web_page prevents double-rendering when both paths
    provide the same handler.
    """

    conformance_uris = WEB_CONFORMANCE_URIS
    priority: int = 100  # High number = low priority (registers last)
    router: APIRouter = APIRouter(prefix="/web", tags=["Dynastore Web Service"])

    def generate_etag(self, content_parts: List[bytes]) -> str:
        """Generates a strong ETag for a list of content parts."""
        hasher = hashlib.md5()
        for part in content_parts:
            hasher.update(part)
        return f'"{hasher.hexdigest()}"'

    def get_cache_headers(self, max_age: Optional[int] = None) -> Dict[str, str]:
        """Provides default cache control headers."""
        eff_max_age = max_age if max_age is not None else self.DEFAULT_CACHE_MAX_AGE
        return {
            "Cache-Control": f"public, max-age={eff_max_age}, stale-while-revalidate=60",
            "Vary": "Accept-Encoding",
        }

    def configure_app(self, app: FastAPI):
        """Configures global settings like middleware and CORS."""
        app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")
        app.add_middleware(GZipMiddleware, minimum_size=1000)
        app.add_middleware(DynamicCORSMiddleware)

        # Disable Starlette's built-in redirect_slashes: it generates absolute
        # Location URLs that omit root_path, breaking redirects behind prefix
        # proxies (e.g. /maps → http://host/maps/ instead of relative "maps/").
        # The RelativeSlashRedirectMiddleware below replaces it with
        # proxy-safe relative redirects.
        app.router.redirect_slashes = False
        app.add_middleware(RelativeSlashRedirectMiddleware)

        # Discovery of page/static providers across extensions now happens
        # in WebModule.lifespan via the WebPageContributor / StaticAssetProvider
        # capability protocols — no reflective class-walking here.

        # Add root redirect if no other root endpoint exists.
        # This assumes the 'web' extension is loaded LAST (configured via SCOPE)
        # to correctly detect if other extensions (like 'features') have already claimed the root path.
        # has_root_endpoint = any(
        #     r.path == "/" for r in app.routes if isinstance(r, APIRoute)
        # )
        # if not has_root_endpoint:
        # This route is added directly to the app, not the router, to be at the root.
        @app.get("", include_in_schema=False)
        @app.get("/", include_in_schema=False)
        async def root_redirect(request: Request):
            # Absolute path including root_path: a relative "web/" resolves
            # against the parent when the request URL has no trailing slash
            # (e.g. /geospatial/v2/api/auth → /geospatial/v2/api/web/).
            root_path = request.scope.get("root_path", "").rstrip("/")
            return RedirectResponse(url=f"{root_path}/web/")

        # Explicitly handle /web redirect to ensure consistent behavior
        # regardless of Router prefix mounting order or strict slashes config.
        @app.get("/web", include_in_schema=False)
        async def web_redirect(request: Request):
            root_path = request.scope.get("root_path", "").rstrip("/")
            return RedirectResponse(url=f"{root_path}/web/")

        #     # Prepend the new route to avoid being shadowed by path converters.
        #     # This is a common pattern when dynamically adding routes to a FastAPI app instance.
        #     app.router.routes.insert(0, app.router.routes.pop())
        #     logger.info("WebService: No root ('/') endpoint found. Added redirect to '/web'.")

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        _register_anonymous_principal()

        # Public, unfiltered catalog-list provider (priority 100) backing the
        # /web/catalogs picker endpoint. An auth-aware provider (e.g. IAM)
        # registers at a lower priority and outranks this one when mounted.
        from dynastore.extensions.web.catalog_source import (
            DefaultCatalogListProvider,
        )
        register_plugin(DefaultCatalogListProvider())

        # Register push-based CORS config handler
        from dynastore.modules.iam.security_config import SecurityPluginConfig
        from dynastore.extensions.web.cors_middleware import (
            on_security_config_changed,
            _cors_instance,
        )
        SecurityPluginConfig.register_apply_handler(
            cast(Any, on_security_config_changed)
        )
        if _cors_instance is not None:
            await _cors_instance.initialize_from_db()
        logger.info("WebService: CORS push handler registered.")

        # Register the web cold-boot contributor so run_cold_boot (called from
        # main.py) applies the web_enable and ogc_conformance_public presets at
        # priority=50 — after IAM (100) but before auth (40) and file presets (10).
        # The contributor is IAM-aware: it skips seeding when PermissionProtocol
        # is absent (non-IAM services; see WebColdBootContributor.run).
        from dynastore.modules.presets.cold_boot import register_cold_boot_contributor
        try:
            register_cold_boot_contributor(_WebColdBootContributor())
        except ValueError:
            logger.debug("WebColdBootContributor already registered; skipping duplicate.")

        yield

    # NotebookContributorProtocol — opt-in surface picked up by
    # NotebooksModule via discovery. Returns [] when NotebookContribution
    # can't be imported so the extension stays loadable in SCOPEs that
    # don't include the notebooks module.
    def get_notebooks(self):
        try:
            from .notebooks import build_contributions
        except Exception:
            return []
        return build_contributions()

    def __init__(self, app: Optional[FastAPI] = None):
        self.app = app

        # Use __file__ with helper to find root
        self.project_root = _find_project_root(
            __file__, ["setup.py", "pyproject.toml", "main.py"]
        )

        if not self.project_root:
            logger.warning(
                "WebService: Could not find project root. Docs scanning will be limited to local web directory."
            )

        self.static_dir = self._find_static_dir()

        # Discover application directories under the detected project root
        self.app_dirs: List[str] = []
        if self.project_root:
            try:
                self.app_dirs = self._discover_app_dirs(self.project_root)
                logger.info(f"WebService: discovered app dirs: {self.app_dirs}")
            except Exception as e:
                logger.debug(f"Error discovering app dirs: {e}")

        # Scan and build a registry of all documentation
        self.docs_registry: Dict[str, Dict[str, Any]] = self._scan_for_documentation()

        self.DEFAULT_CACHE_MAX_AGE = int(os.getenv("DEFAULT_CACHE_MAX_AGE", 3600))

        # Registry for pluggable web pages: DEPRECATED - now managed by WebModule
        # self.web_pages: Dict[str, Dict[str, Any]] = {}

        # Self-register via discovery if possible
        self.web_module = get_protocol(WebModuleProtocol)

        self._register_routes()

    def _find_static_dir(self) -> Optional[str]:
        # MODERN APPROACH: Use pathlib to find the directory of this file
        current_path = Path(__file__).resolve().parent

        possible_dirs = [current_path / "static", current_path.parent / "static"]

        for d in possible_dirs:
            if d.is_dir():
                logger.info(f"WebService: Serving static files from {d}")
                return str(d)

        logger.warning(
            f"WebService: Could not find 'static' directory. Checked: {[str(p) for p in possible_dirs]}"
        )
        return None

    def _discover_app_dirs(self, project_root: str) -> List[str]:
        """Return a list of candidate application directories."""
        candidates: List[str] = []
        comps = ("modules", "extensions", "tasks")
        root = Path(project_root)

        # 1) Direct child directories under <project_root>/src
        src_dir = root / "src"
        if src_dir.is_dir():
            for child in src_dir.iterdir():
                if child.is_dir():
                    for comp in comps:
                        if (child / comp).is_dir():
                            candidates.append(str(child.resolve()))
                            break

        # 2) Direct child directories under project_root
        for child in root.iterdir():
            if child.is_dir():
                for comp in comps:
                    if (child / comp).is_dir():
                        candidates.append(str(child.resolve()))
                        break

        # 3) project_root itself
        for comp in comps:
            if (root / comp).is_dir():
                candidates.append(str(root.resolve()))
                break

        # Deduplicate while preserving order
        seen = set()
        result: List[str] = []
        for p in candidates:
            if p not in seen:
                seen.add(p)
                result.append(p)
        return result

    def _scan_for_documentation(self) -> Dict[str, Dict[str, Any]]:
        """
        Scans for documentation recursively and returns a flat registry map.
        Key: Unique ID
        Value: { id, title, path, category }
        """
        registry = {}

        def process_doc_file(
            fpath: Path,
            category: str,
            component_name: Optional[str],
            root_reference: Path,
        ):
            """Helper to process a single markdown file"""
            try:
                # 1. Generate a clean, unique, and readable ID
                id_parts = []
                if category != "root":
                    id_parts.append(category)

                if component_name:
                    id_parts.append(component_name)

                is_readme = fpath.stem.lower() == "readme"
                if not is_readme:
                    stem = fpath.stem.lower()
                    # If stem is 'iam_upgrade' and component is 'iam', we just want 'upgrade'
                    if component_name and stem.startswith(component_name):
                        clean_stem = stem[len(component_name) :].lstrip("_-")
                        # Only use if it's not empty
                        if clean_stem:
                            id_parts.append(clean_stem)
                        # else, it was something like 'iam.md', which we treat as a readme
                    else:
                        id_parts.append(stem)

                if not id_parts:
                    id_parts.append(fpath.stem.lower())

                # Use ':' as a separator for readability, it's a valid path parameter character.
                doc_id = ":".join(id_parts)

                # Final collision check for safety (e.g. readme.md and README.md in same folder)
                final_id = doc_id
                counter = 1
                while final_id in registry:
                    final_id = f"{doc_id}_{counter}"
                    counter += 1
                doc_id = final_id

                # 2. Extract Title (First H1)
                # Smart Fallback:
                # If README.md in 'iam', title is likely 'Iam' (parent dir), not 'Readme'
                if is_readme:
                    title = fpath.parent.name.replace("_", " ").title()
                else:
                    title = fpath.stem.replace("_", " ").title()

                # Attempt to read the actual title from file content
                try:
                    with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                        for _ in range(50):  # Read first 50 lines max
                            line = f.readline()
                            if not line:
                                break
                            stripped = line.strip()
                            if stripped.startswith("#"):
                                raw_title = stripped.lstrip("#").strip()
                                if raw_title:
                                    # Clean Markdown links [Text](url) -> Text
                                    clean_title = re.sub(
                                        r"\[([^\]]+)\]\([^)]+\)", r"\1", raw_title
                                    )
                                    # Clean Markdown formatting * _ `
                                    title = re.sub(r"[*_`]", "", clean_title).strip()
                                    break
                except Exception as e:
                    logger.debug(f"Could not read title from {fpath}: {e}")

                # Contextualize title for sub-components
                if component_name and category != "root":
                    comp_title = component_name.replace("_", " ").title()
                    # Check if title already starts with component name (case insensitive)
                    # And avoid duplicating if title is exactly the component name
                    if (
                        title.lower() != comp_title.lower()
                        and not title.lower().startswith(comp_title.lower())
                    ):
                        title = f"{comp_title}: {title}"

                # 3. Add to Registry
                registry[doc_id] = {
                    "id": doc_id,
                    "title": title,
                    "path": str(fpath.resolve()),
                    "category": category,
                }
                logger.debug(f"Registered doc: {doc_id} -> {title}")

            except Exception as e:
                logger.error(f"Error processing doc file {fpath}: {e}")

        # --- A. Scan Project Root ---
        if self.project_root:
            root_path = Path(self.project_root)
            logger.info(f"Scanning root docs in: {root_path}")
            for item in root_path.glob("*.md"):
                process_doc_file(item, "root", None, root_path)

            # Also scan 'docs' folder in root if it exists
            docs_folder = root_path / "docs"
            if docs_folder.is_dir():
                _SKIP_STEMS = {"files_to_remove"}  # internal/admin docs to hide
                for item in sorted(docs_folder.iterdir()):
                    if item.is_file() and item.suffix.lower() in (".md", ".markdown"):
                        if item.stem.lower() not in _SKIP_STEMS:
                            process_doc_file(item, "platform", None, docs_folder)
                    elif item.is_dir():
                        # Sub-directory (architecture/, components/, …) → own category
                        subcat = item.name.lower()
                        for md in sorted(item.rglob("*.md")):
                            process_doc_file(md, subcat, None, item)

            # --- B. Scan App Directories (Modules, Extensions, Tasks) ---
            app_dirs = getattr(self, "app_dirs", []) or []
            categories = ["modules", "extensions", "tasks"]

            for app_dir_str in app_dirs:
                app_path = Path(app_dir_str)

                # Scan 'docs' folder in app_path if it exists
                app_docs = app_path / "docs"
                if app_docs.is_dir():
                    for item in app_docs.rglob("*.md"):
                        process_doc_file(item, "root", None, app_docs)

                for cat in categories:
                    cat_dir = app_path / cat
                    if not cat_dir.is_dir():
                        continue

                    # Iterate over components (e.g., modules/auth, modules/users)
                    for component_dir in cat_dir.iterdir():
                        if not component_dir.is_dir():
                            continue

                        # Recursively find ALL .md files in this component
                        # but EXPLICITLY SKIP node_modules

                        # Note: We use os.walk instead of rglob to efficiently skip directories
                        for root, dirs, files in os.walk(component_dir):
                            # In-place filtering of dirs to skip node_modules traversal
                            dirs[:] = [
                                d
                                for d in dirs
                                if d != "node_modules" and not d.startswith(".")
                            ]

                            for filename in files:
                                if filename.lower().endswith((".md", ".markdown")):
                                    md_file = Path(root) / filename
                                    process_doc_file(
                                        md_file, cat, component_dir.name, component_dir
                                    )

                    # If cat is "extensions", also check "extensions" in the module itself
                    if cat == "extensions":
                        # Some extensions might be inside modules
                        # Re-scan "dynastore/modules" to see if any module has an "extensions" folder
                        pass  # This logic is a bit circular if not careful. Sticking to standard structure.

        logger.info(f"Documentation scan complete. Found {len(registry)} documents.")
        return registry

    # Pages / static prefixes are declared via @expose_web_page / @expose_static
    # decorators and surfaced through the WebPageContributor / StaticAssetProvider
    # capability protocols implemented below.

    def get_web_pages(self):
        # Drop admin-section pages when no PermissionProtocol provider is
        # registered: their required_roles can never match in deployments
        # without IAM, so the entries would be dead in the API response.
        # Anonymous pages (home/docs/dashboard) stay unconditional.
        from dynastore.extensions.tools.web_collect import collect_web_pages
        from dynastore.models.protocols.policies import PermissionProtocol
        pages = collect_web_pages(self)
        if get_protocol(PermissionProtocol) is None:
            pages = [p for p in pages if p.section != "admin"]
        return pages

    def get_static_assets(self):
        from dynastore.extensions.tools.web_collect import collect_static_assets
        return collect_static_assets(self)

    @expose_web_page(page_id="home", title="Home", icon="fa-home", priority=-100)
    def home_page(self, language: str = "en"):
        """Server-rendered home fragment with localized hero + live OGC coverage panel.

        The hero block is wrapped in ``ds-default-home`` so brand-overriding
        embeds (e.g. ``geoid_home_page``) can hide it via the existing JS
        ``document.querySelectorAll('.ds-default-home').forEach(...)`` pattern.

        The OGC coverage panel reads ``get_conformance_summary()`` directly so
        anonymous visitors see the platform's standards posture without an
        extra API call (the dashboard endpoint at /web/dashboard/ogc-compliance
        is admin/user-gated). Always-fresh: re-rendered each request.
        """
        from dynastore.extensions.tools.conformance import get_conformance_summary

        lang = (language or "en").lower().split("-")[0]
        if lang not in ("en", "es", "fr"):
            lang = "en"

        # Localized hero copy. Same en/es/fr coverage as the GeoID embed
        # (``geoid_home_page``) and the GeoID standalone page templates.
        BADGE = {
            "en": "Agro-Informatics Platform",
            "es": "Plataforma Agro-Informática",
            "fr": "Plateforme Agro-Informatique",
        }
        TITLE = {
            "en": "Agro-Informatics Hub",
            "es": "Centro Agro-Informático",
            "fr": "Plateforme Agro-Informatique",
        }
        SUBTITLE = {
            "en": "Digital Agricultural Infrastructure",
            "es": "Infraestructura Agrícola Digital",
            "fr": "Infrastructure Agricole Numérique",
        }
        DESCRIPTION = {
            "en": "High-performance geospatial data catalog and processing platform for agricultural intelligence.",
            "es": "Plataforma de catálogo y procesamiento de datos geoespaciales de alto rendimiento para inteligencia agrícola.",
            "fr": "Plateforme haute performance de catalogage et de traitement de données géospatiales pour l'intelligence agricole.",
        }
        DOCS_LABEL = {"en": "Documentation", "es": "Documentación", "fr": "Documentation"}
        EXPLORE_LABEL = {"en": "Explore Data", "es": "Explorar Datos", "fr": "Explorer les données"}
        STAC_TITLE = {"en": "STAC Browser", "es": "Navegador STAC", "fr": "Navigateur STAC"}
        STAC_DESC = {
            "en": "Explore satellite imagery and geospatial assets using the STAC standard.",
            "es": "Explora imágenes satelitales y activos geoespaciales con el estándar STAC.",
            "fr": "Parcourez l'imagerie satellite et les actifs géospatiaux avec le standard STAC.",
        }
        MAP_TITLE = {"en": "Map Viewer", "es": "Visor de Mapas", "fr": "Visualiseur de cartes"}
        MAP_DESC = {
            "en": "Visualize tiled datasets and explore geospatial layers in real-time.",
            "es": "Visualiza conjuntos de datos en mosaico y explora capas geoespaciales en tiempo real.",
            "fr": "Visualisez des jeux de données tuilés et explorez des couches géospatiales en temps réel.",
        }
        STATS_TITLE = {"en": "Platform Stats", "es": "Estadísticas de la Plataforma", "fr": "Statistiques de la plateforme"}
        STATS_DESC = {
            "en": "Monitor system health, usage statistics, and background task progress.",
            "es": "Supervisa el estado del sistema, el uso y el progreso de las tareas en segundo plano.",
            "fr": "Surveillez l'état du système, les statistiques d'usage et la progression des tâches.",
        }
        STANDARDS_TITLE = {
            "en": "Standards & Conformance",
            "es": "Estándares y Conformidad",
            "fr": "Standards et Conformité",
        }
        STANDARDS_LEAD = {
            "en": "Live count of OGC API conformance classes the platform advertises, grouped by standard.",
            "es": "Recuento en vivo de clases de conformidad OGC API que la plataforma declara, agrupadas por estándar.",
            "fr": "Décompte en direct des classes de conformité OGC API déclarées par la plateforme, groupées par standard.",
        }
        ROADMAP_LABEL = {
            "en": "Roadmap — not yet implemented",
            "es": "Hoja de ruta — aún no implementado",
            "fr": "Feuille de route — non encore implémenté",
        }
        TOTAL_LABEL = {
            "en": "conformance classes across",
            "es": "clases de conformidad en",
            "fr": "classes de conformité réparties sur",
        }
        FAMILIES_LABEL = {"en": "standard families", "es": "familias de estándares", "fr": "familles de standards"}

        # OGC coverage — server-rendered from the live conformance registry.
        # Implemented cards and roadmap pills link out to each standard's
        # canonical OGC documentation page (URLs from OGC_COMPATIBILITY_STUDY).
        try:
            summary = get_conformance_summary()
            standards_html_parts: List[str] = []
            for s in summary.standards:
                card_inner = f"""
                    <div class="flex items-center justify-between mb-1">
                        <h4 class="text-sm font-semibold text-white group-hover:text-emerald-300 transition-colors">{s.name}</h4>
                        <span class="px-2 py-0.5 rounded-full text-[10px] font-bold bg-emerald-500/10 text-emerald-300 border border-emerald-500/20">
                            {s.implemented}
                        </span>
                    </div>
                    <div class="text-[10px] text-slate-500 uppercase tracking-wider">{s.status}</div>
                """
                if s.doc_url:
                    standards_html_parts.append(
                        f'<a href="{s.doc_url}" target="_blank" rel="noopener" '
                        f'class="block glass-panel p-4 rounded-xl border border-white/5 '
                        f'hover:border-emerald-500/40 transition-colors group">{card_inner}</a>'
                    )
                else:
                    standards_html_parts.append(
                        f'<div class="glass-panel p-4 rounded-xl border border-white/5 group">{card_inner}</div>'
                    )
            standards_grid = "".join(standards_html_parts) or (
                f'<div class="text-slate-500 text-sm col-span-full">No conformance classes registered.</div>'
            )

            not_impl_pill_parts: List[str] = []
            for entry in summary.roadmap:
                base_cls = "px-2 py-1 rounded-full text-[11px] border border-slate-700/60 bg-slate-800/40"
                if entry.doc_url:
                    not_impl_pill_parts.append(
                        f'<a href="{entry.doc_url}" target="_blank" rel="noopener" '
                        f'class="{base_cls} text-slate-400 hover:text-emerald-300 hover:border-emerald-500/40 transition-colors">'
                        f'{entry.name}</a>'
                    )
                else:
                    not_impl_pill_parts.append(
                        f'<span class="{base_cls} text-slate-500">{entry.name}</span>'
                    )
            not_impl_pills = "".join(not_impl_pill_parts) or (
                '<span class="text-slate-500 text-sm">All advertised standards have at least one conformance class registered.</span>'
            )

            ogc_total = summary.total_conformance_classes
            ogc_families = len(summary.standards)
        except Exception:
            # Degrade gracefully if the conformance registry is unavailable —
            # the page must still render for anonymous visitors.
            standards_grid = ""
            not_impl_pills = ""
            ogc_total = 0
            ogc_families = 0

        ogc_panel = ""
        if standards_grid:
            ogc_panel = f"""
            <section class="mt-16 ds-default-home">
                <div class="flex items-end justify-between mb-6 flex-wrap gap-3">
                    <div>
                        <h2 class="text-2xl font-bold text-white mb-1">{STANDARDS_TITLE[lang]}</h2>
                        <p class="text-slate-400 text-sm">{STANDARDS_LEAD[lang]}</p>
                    </div>
                    <div class="text-right text-xs text-slate-500">
                        <span class="text-emerald-300 font-bold">{ogc_total}</span> {TOTAL_LABEL[lang]}
                        <span class="text-emerald-300 font-bold">{ogc_families}</span> {FAMILIES_LABEL[lang]}
                    </div>
                </div>
                <div class="grid sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-3">
                    {standards_grid}
                </div>
                <div class="mt-6 pt-4 border-t border-white/5">
                    <h4 class="text-xs font-bold text-slate-500 uppercase tracking-wider mb-3">{ROADMAP_LABEL[lang]}</h4>
                    <div class="flex flex-wrap gap-2">
                        {not_impl_pills}
                    </div>
                </div>
            </section>
            """

        return f"""
        <div id="section-home" class="fade-in max-w-7xl mx-auto">
            <header class="py-16 text-center ds-default-home">
                <div class="inline-flex items-center gap-2 px-3 py-1 rounded-full border border-emerald-500/30 bg-emerald-500/10 text-emerald-300 text-xs font-medium mb-6">
                    <span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span>
                    {BADGE[lang]}
                </div>
                <h1 class="text-4xl md:text-6xl font-bold text-white mb-6 tracking-tight">
                    <span id="home-title">{TITLE[lang]}</span><br>
                    <span class="text-transparent bg-clip-text bg-gradient-to-r from-emerald-400 to-blue-400" id="home-subtitle">{SUBTITLE[lang]}</span>
                </h1>
                <p class="text-slate-400 max-w-2xl mx-auto mb-10" id="home-description">
                    {DESCRIPTION[lang]}
                </p>
                <div class="flex items-center justify-center gap-6" id="home-actions">
                    <button onclick="switchTab('docs')" class="px-6 py-3 rounded-lg bg-white/5 hover:bg-white/10 border border-white/10 text-white font-medium transition-all">
                        {DOCS_LABEL[lang]}
                    </button>
                    <button onclick="switchTab('stac_browser')" class="px-6 py-3 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white font-medium shadow-lg shadow-emerald-500/20 transition-all">
                        {EXPLORE_LABEL[lang]}
                    </button>
                </div>
            </header>

            <div id="home-content-append" class="space-y-12">
                <!-- Extensions can append content here or replace parts via JS -->
            </div>

            <div class="grid md:grid-cols-2 lg:grid-cols-3 gap-6 mt-12 ds-default-home" id="home-featured-grid">
                 <div class="glass-panel p-6 rounded-2xl border border-white/5 hover:border-emerald-500/30 transition-colors group cursor-pointer" onclick="switchTab('stac_browser')">
                    <div class="w-12 h-12 rounded-xl bg-emerald-500/10 flex items-center justify-center text-emerald-400 mb-4 group-hover:scale-110 transition-transform">
                        <i class="fa-solid fa-layer-group text-xl"></i>
                    </div>
                    <h3 class="text-xl font-semibold text-white mb-2">{STAC_TITLE[lang]}</h3>
                    <p class="text-slate-400 text-sm">{STAC_DESC[lang]}</p>
                 </div>
                 <div class="glass-panel p-6 rounded-2xl border border-white/5 hover:border-blue-500/30 transition-colors group cursor-pointer" onclick="switchTab('map_viewer')">
                    <div class="w-12 h-12 rounded-xl bg-blue-500/10 flex items-center justify-center text-blue-400 mb-4 group-hover:scale-110 transition-transform">
                        <i class="fa-solid fa-map text-xl"></i>
                    </div>
                    <h3 class="text-xl font-semibold text-white mb-2">{MAP_TITLE[lang]}</h3>
                    <p class="text-slate-400 text-sm">{MAP_DESC[lang]}</p>
                 </div>
                 <div class="glass-panel p-6 rounded-2xl border border-white/5 hover:border-purple-500/30 transition-colors group cursor-pointer" onclick="switchTab('dashboard')">
                    <div class="w-12 h-12 rounded-xl bg-purple-500/10 flex items-center justify-center text-purple-400 mb-4 group-hover:scale-110 transition-transform">
                        <i class="fa-solid fa-gauge-high text-xl"></i>
                    </div>
                    <h3 class="text-xl font-semibold text-white mb-2">{STATS_TITLE[lang]}</h3>
                    <p class="text-slate-400 text-sm">{STATS_DESC[lang]}</p>
                 </div>
            </div>

            {ogc_panel}
        </div>
        """

    @staticmethod
    def _asset_version_token(asset_paths: Optional[List[str]] = None) -> str:
        """Cache-busting token for the static assets a shell page references.

        Returns the newest mtime (epoch seconds) across ``asset_paths`` so the
        token changes on every real asset edit -- including same-version
        redeploys, which a package-version token would miss. Falls back to the
        package version when no readable asset path is given.
        """
        from dynastore._version import VERSION
        mtimes: List[float] = []
        for path in asset_paths or []:
            try:
                mtimes.append(os.path.getmtime(path))
            except OSError:
                continue
        return str(int(max(mtimes))) if mtimes else VERSION

    async def _serve_html_template(
        self, html_path: str, asset_paths: Optional[List[str]] = None
    ) -> HTMLResponse:
        """Serve an app-shell HTML entry-point with the canonical cache policy.

        Substitutes ``{{VERSION}}`` (package version) and ``{{ASSET_V}}`` (a
        cache-busting token derived from ``asset_paths``), then stamps the
        no-cache policy that ``_cache_headers_for`` assigns to HTML entry-points
        so the shell always revalidates while its long-cached JS/CSS bust on
        change. Single serving path for every web shell page.
        """
        from dynastore._version import VERSION
        content = await asyncio.to_thread(Path(html_path).read_text, encoding="utf-8")
        content = content.replace("{{VERSION}}", VERSION)
        content = content.replace("{{ASSET_V}}", self._asset_version_token(asset_paths))
        return HTMLResponse(content, headers=self._cache_headers_for("static", "index.html"))

    @expose_web_page(
        page_id="exposure",
        title="Service Exposure",
        icon="fa-toggle-on",
        description="Toggle extensions per platform / catalog scope.",
        audience_policy_id="web_sysadmin_access",
        section="admin",
        priority=20,
    )
    async def exposure_page(self, request: Request):
        """Serve the service exposure control panel HTML page."""
        static_dir = os.path.join(os.path.dirname(__file__), "static", "admin")
        html_path = os.path.join(static_dir, "exposure.html")
        if not os.path.exists(html_path):
            raise HTTPException(status_code=404, detail="Service exposure panel template not found.")
        return await self._serve_html_template(html_path)

    @expose_web_page(
        page_id="governance",
        title="Governance",
        icon="fa-scale-balanced",
        description="Roles, policies and principal bindings at platform and catalog scope.",
        audience_policy_id="web_admin_access",
        section="admin",
        priority=15,
    )
    async def governance_page(self, request: Request):
        """Roles, policies, principals. Client-side gates catalog scope via
        ``/web/catalogs``; server-side ``/admin/*`` endpoints enforce the
        actual authorization on every mutation."""
        static_dir = os.path.join(os.path.dirname(__file__), "static", "admin")
        html_path = os.path.join(static_dir, "governance.html")
        if not os.path.exists(html_path):
            raise HTTPException(status_code=404, detail="Governance page template not found.")
        return await self._serve_html_template(html_path)

    @expose_web_page(
        page_id="access-bindings",
        title="Access & Bindings",
        icon="fa-link",
        description=(
            "Manage role and policy bindings at collection scope: pick a "
            "principal and a collection to view/edit their access, or reverse "
            "the question and see every principal bound on a collection."
        ),
        audience_policy_id="web_admin_access",
        section="admin",
        priority=14,
    )
    async def access_bindings_page(self, request: Request):
        """Collection-scoped binding editor (#1342). The page is a thin client
        over the existing /admin endpoints — principals/roles/policies CRUD,
        the collection-binding CRUD added in #1360 (POST/GET/DELETE
        /admin/catalogs/{cat}/collections/{coll}/grants), and the reverse
        view via list_grants_for_resource. Server-side /admin/* endpoints
        enforce authorization on every mutation; the page itself is gated by
        ``web_admin_access`` like the rest of the hub."""
        static_dir = os.path.join(os.path.dirname(__file__), "static", "admin")
        html_path = os.path.join(static_dir, "access-bindings.html")
        if not os.path.exists(html_path):
            raise HTTPException(
                status_code=404, detail="Access & Bindings page template not found."
            )
        return await self._serve_html_template(html_path)

    @expose_web_page(
        page_id="stac-authoring",
        title="STAC Authoring",
        icon="fa-book-atlas",
        description="Charter catalogs (sysadmin) and commission collections (catalog admin).",
        audience_policy_id="web_admin_access",
        section="admin",
        priority=12,
    )
    async def stac_authoring_page(self, request: Request):
        """STAC catalog + collection authoring forms."""
        static_dir = os.path.join(os.path.dirname(__file__), "static", "admin")
        html_path = os.path.join(static_dir, "stac-authoring.html")
        if not os.path.exists(html_path):
            raise HTTPException(status_code=404, detail="STAC authoring template not found.")
        return await self._serve_html_template(html_path)

    @expose_web_page(
        page_id="ingest",
        title="Ingest Features",
        icon="fa-upload",
        description="Drop GeoJSON into a target collection; server reports per-feature results.",
        audience_policy_id="web_admin_access",
        section="admin",
        priority=8,
    )
    async def ingest_page(self, request: Request):
        """Drag-and-drop GeoJSON ingest. The server does all authorization
        for the underlying ``POST /catalogs/{cid}/collections/{colid}/items``
        call; this page just shepherds the request and renders the report."""
        static_dir = os.path.join(os.path.dirname(__file__), "static", "admin")
        html_path = os.path.join(static_dir, "ingest.html")
        if not os.path.exists(html_path):
            raise HTTPException(status_code=404, detail="Ingest page template not found.")
        return await self._serve_html_template(html_path)

    @expose_web_page(page_id="docs", title="Documentation", icon="fa-book", priority=-100)
    def docs_page(self, language: str = "en"):
        return """
        <div class="grid lg:grid-cols-[300px_1fr] gap-8">
            <aside class="glass-panel p-4 rounded-xl h-[calc(100vh-200px)] overflow-y-auto" id="docs-sidebar-content"></aside>
            <article class="glass-panel p-8 rounded-xl prose prose-invert max-w-none" id="docs-content">
                <p class="text-slate-500">Select a document from the sidebar to begin.</p>
            </article>
        </div>
        """


    @expose_web_page(page_id="dashboard", title="Dashboard", icon="fa-gauge-high", priority=-100)
    def dashboard_page(self, language: str = "en"):
        return """
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6" id="dashboard-stats">
            <!-- Stats will be loaded here by custom.js -->
            <div class="glass-panel p-6 rounded-xl border border-white/5">
                <p class="text-slate-500 text-[10px] uppercase font-bold tracking-wider mb-1">Total Requests</p>
                <div class="text-2xl font-bold text-white mb-2" id="stat-total-requests">0</div>
                <div class="text-emerald-400 text-[10px]"><i class="fa-solid fa-arrow-up mr-1"></i> Live</div>
            </div>
            <div class="glass-panel p-6 rounded-xl border border-white/5">
                <p class="text-slate-500 text-[10px] uppercase font-bold tracking-wider mb-1">Avg Latency</p>
                <div class="text-2xl font-bold text-white mb-2" id="stat-avg-latency">0ms</div>
                <div class="text-blue-400 text-[10px]"><i class="fa-solid fa-bolt mr-1"></i> Real-time</div>
            </div>
            <div class="glass-panel p-6 rounded-xl border border-white/5">
                <p class="text-slate-500 text-[10px] uppercase font-bold tracking-wider mb-1">Success Rate</p>
                <div class="text-2xl font-bold text-white mb-2" id="stat-success-rate">0%</div>
                <div class="text-blue-400 text-[10px]"><i class="fa-solid fa-check-circle mr-1"></i> Verified</div>
            </div>
            <div class="glass-panel p-6 rounded-xl border border-white/5">
                <p class="text-slate-500 text-[10px] uppercase font-bold tracking-wider mb-1">Active Tasks</p>
                <div class="text-2xl font-bold text-white mb-2" id="stat-active-tasks">0</div>
                <div class="text-purple-400 text-[10px]"><i class="fa-solid fa-tasks mr-1"></i> Background</div>
            </div>
        </div>

        <div class="grid lg:grid-cols-2 gap-8 mt-8">
            <div class="glass-panel p-6 rounded-2xl border border-white/5">
                <div class="flex items-center justify-between mb-6">
                    <h3 class="text-lg font-bold text-white flex items-center gap-2">
                        <i class="fa-solid fa-terminal text-blue-400"></i> System Activity
                    </h3>
                    <select id="log-filter-level" onchange="fetchDashboardLogs()" class="bg-white/5 border border-white/10 text-slate-300 text-xs rounded-lg px-2 py-1 outline-none">
                        <option value="INFO">INFO</option>
                        <option value="WARNING">WARNING</option>
                        <option value="ERROR">ERROR</option>
                        <option value="DEBUG">DEBUG</option>
                    </select>
                </div>
                <div id="dashboard-logs" class="h-80 overflow-y-auto space-y-1 font-mono text-[11px]">
                    <div class="text-slate-500 py-4 text-center">Loading logs...</div>
                </div>
            </div>

            <div class="glass-panel p-6 rounded-2xl border border-white/5">
                 <h3 class="text-lg font-bold text-white mb-6 flex items-center gap-2">
                    <i class="fa-solid fa-list-check text-purple-400"></i> Background Tasks
                </h3>
                <div id="dashboard-tasks" class="h-80 overflow-y-auto space-y-3">
                    <div class="text-slate-500 py-4 text-center">No active tasks</div>
                </div>
            </div>
        </div>
        """

    def register_web_page(self, config: Dict[str, Any], provider: Callable[[], Any]):
        if self.web_module:
            self.web_module.register_web_page(config, provider)
        else:
            logger.error("WebService: Cannot register web page, WebModule not available")

    def register_static_provider(
        self,
        prefix: str,
        provider: Callable[[], List[str]],
        owner: str = "",
        description: str = "",
        public: bool = True,
    ) -> None:
        if self.web_module:
            self.web_module.register_static_provider(
                prefix, provider, owner=owner, description=description, public=public
            )
        else:
            logger.error("WebService: Cannot register static provider, WebModule not available")

    @expose_static(
        "static",
        owner="web",
        description="Core platform CSS, JS, images, and fonts.",
    )
    def _provide_default_static(self) -> List[str]:
        if not self.static_dir:
            return []
        files = []
        for root, _, filenames in os.walk(self.static_dir):
            for filename in filenames:
                files.append(os.path.join(root, filename))
        return files

    @expose_static(
        "website",
        owner="web",
        description="Web shell entry-point (index.html) and its assets.",
    )
    def _provide_website_static(self) -> List[str]:
        website_dir = os.path.join(os.path.dirname(__file__), "static", "website")
        files = []
        for root, _, filenames in os.walk(website_dir):
            for filename in filenames:
                files.append(os.path.join(root, filename))
        return files

    @expose_static(
        "dashboard",
        owner="web",
        description="Per-catalog dashboard HTML shell and its assets.",
        public=False,
    )
    def _provide_dashboard_static(self) -> List[str]:
        dashboard_dir = os.path.join(os.path.dirname(__file__), "static", "dashboard")
        files = []
        for root, _, filenames in os.walk(dashboard_dir):
            for filename in filenames:
                files.append(os.path.join(root, filename))
        return files

    @expose_static(
        "extension-static",
        owner="web",
        description="Alias for the 'static' prefix; kept for backward compatibility.",
    )
    def _provide_extension_static(self) -> List[str]:
        return self._provide_default_static()

    async def serve_file(
        self, file_path: str, extra_headers: Optional[Dict[str, str]] = None
    ) -> Response:
        if not os.path.isfile(file_path):
            raise HTTPException(status_code=404, detail="File not found")

        mime_types = {
            ".css": "text/css",
            ".js": "application/javascript",
            ".json": "application/json",
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".svg": "image/svg+xml",
            ".gif": "image/gif",
            ".ico": "image/x-icon",
            ".html": "text/html",
            ".md": "text/markdown",
            ".wasm": "application/wasm",
            ".whl": "application/octet-stream",
        }
        _, ext = os.path.splitext(file_path)
        media_type = mime_types.get(ext.lower(), "application/octet-stream")

        try:
            content = await asyncio.to_thread(Path(file_path).read_bytes)
            return Response(
                content=content, media_type=media_type, headers=extra_headers or None
            )
        except Exception as e:
            logger.error(f"Error serving file {file_path}: {e}")
            raise HTTPException(status_code=500, detail="Internal Server Error") from e

    @staticmethod
    def _cache_headers_for(prefix: str, rel_path: str) -> Dict[str, str]:
        # Content-hashed bundles + versioned wheels: cache forever
        IMMUTABLE = "public, max-age=31536000, immutable"
        MEDIA_30D = "public, max-age=2592000"
        VENDOR_1D = "public, max-age=86400"
        NO_CACHE = "no-cache, must-revalidate"

        if prefix == "lite":
            # App entry points and behaviour-critical scripts must revalidate
            sensitive = {
                "lab/index.html", "index.html", "jupyter-lite.json",
                "service-worker.js", "bridge.js",
            }
            if rel_path in sensitive:
                return {"Cache-Control": NO_CACHE}
            if (rel_path.startswith("build/")
                    or rel_path.startswith("pypi/")
                    or rel_path.startswith("extensions/")):
                return {"Cache-Control": IMMUTABLE}
            ext = os.path.splitext(rel_path)[1].lower()
            if ext in {".woff2", ".woff", ".ttf", ".png", ".ico", ".svg", ".jpg", ".jpeg"}:
                return {"Cache-Control": MEDIA_30D}
            if ext in {".html", ".json"}:
                return {"Cache-Control": NO_CACHE}
            return {"Cache-Control": VENDOR_1D}

        if prefix == "static":
            ext = os.path.splitext(rel_path)[1].lower()
            if ext in {".html"}:
                return {"Cache-Control": NO_CACHE}
            return {"Cache-Control": VENDOR_1D}

        return {"Cache-Control": NO_CACHE}

    def _register_routes(self):

        from starlette.responses import RedirectResponse

        @self.router.get("", include_in_schema=False)
        async def redirect_web_root(request: Request):
            # Force trailing slash for relative assets to work.
            # Use the last path segment so the redirect resolves correctly
            # behind any prefix proxy (avoids root_path duplication).
            path = request.scope.get("path", "")
            if not path.endswith("/"):
                segment = path.rsplit("/", 1)[-1]
                return RedirectResponse(url=f"{segment}/")
            # If already has slash, serve index (should be handled by @router.get("/"))
            return await read_extension_root()

        @self.router.get("/", include_in_schema=False)
        async def read_extension_root():
            static_root = os.path.join(os.path.dirname(__file__), "static")
            website_index = os.path.join(static_root, "website", "index.html")
            if os.path.exists(website_index):
                return await self._serve_html_template(
                    website_index,
                    asset_paths=[
                        os.path.join(static_root, "custom.js"),
                        os.path.join(static_root, "common", "tailwind.css"),
                        os.path.join(static_root, "vendor", "vendor.css"),
                    ],
                )
            return HTMLResponse("Not Found", status_code=404)

        _static_root = os.path.join(os.path.dirname(__file__), "static")
        _dashboard_index = os.path.join(_static_root, "dashboard", "index.html")
        # Assets every dashboard/processes shell references; used to derive the
        # {{ASSET_V}} cache-busting token (see _serve_html_template).
        _dashboard_assets = [
            os.path.join(_static_root, "common", "admin.css"),
            os.path.join(_static_root, "dashboard", "styles.css"),
        ]

        @self.router.get("/dashboard/")
        async def read_dashboard_root():
            """Catalog-picker root. Anonymous-allowed: lists catalogs the
            caller can see (filtered downstream by IAM) and lets them pick
            a per-catalog dashboard."""
            if os.path.exists(_dashboard_index):
                return await self._serve_html_template(
                    _dashboard_index, asset_paths=_dashboard_assets
                )
            return HTMLResponse("Dashboard Not Found", status_code=404)

        @self.router.get("/dashboard/catalogs/{catalog_id}/")
        async def read_dashboard_per_catalog(catalog_id: str):
            """Per-catalog dashboard HTML shell. Same template as the root —
            JS reads ``catalog_id`` from ``window.location.pathname`` and
            uses relative URLs (``stats``, ``logs``, ``events``) which
            resolve against this base path."""
            if os.path.exists(_dashboard_index):
                return await self._serve_html_template(
                    _dashboard_index, asset_paths=_dashboard_assets
                )
            return HTMLResponse("Dashboard Not Found", status_code=404)

        @self.router.get("/dashboard/catalogs/{catalog_id}/processes/")
        async def read_processes_page(catalog_id: str):
            """Per-catalog processes HTML shell. Same template as before;
            authz gated upstream by web_dashboard_per_catalog_access policy."""
            processes_index = os.path.join(_static_root, "dashboard", "processes.html")
            if os.path.exists(processes_index):
                return await self._serve_html_template(
                    processes_index, asset_paths=_dashboard_assets
                )
            return HTMLResponse("Processes Page Not Found", status_code=404)

        @self.router.get("/config/pages", response_class=JSONResponse)
        async def get_web_pages_config(
            request: Request,
            language: str = Query("en"),
        ):
            """Return the navigation page list visible to the caller.

            Auth-blind by design: the route fetches the raw page list and
            delegates filtering to ``PageVisibilityFilter`` (registered
            by IAM). When IAM is unloaded, only pages without
            ``required_roles`` (or explicitly tagged anonymous) are
            returned — so the route can render a usable nav even in
            slim deployments.
            """
            if not self.web_module:
                return []

            from dynastore.models.protocols.page_visibility_filter import (
                PageVisibilityFilter,
            )
            anonymous = IamRolesConfig().anonymous_role_name

            pages = await self.web_module.get_web_pages_config(language)

            # Extension-exposure gate: pages owned by an extension the operator
            # disabled at platform scope (Service Exposure / `enabled=false`
            # plugin config) disappear from the navigation. Pages without an
            # owner (core/web-module pages) are unaffected.
            matrix = getattr(request.app.state, "exposure_matrix", None)
            if matrix is not None:
                snap = await matrix.get()
                pages = [
                    p for p in pages
                    if p.get("owner") is None
                    or snap.platform.get(p["owner"], True)
                ]

            page_filter = get_protocol(PageVisibilityFilter)
            if page_filter is not None:
                results = list(await page_filter.filter_visible(pages, request))
            else:
                # No IAM provider — show only the anonymous-visible subset.
                results = [
                    p for p in pages
                    if not p.get("required_roles") or anonymous in p.get("required_roles", [])
                ]

            results.sort(key=lambda x: x.get("priority", 0))
            return results


        @self.router.get("/config/static-prefixes", response_class=JSONResponse)
        async def list_static_prefixes():
            """List every registered static-file prefix with its owner and description.

            Anonymous-accessible (covered by the ``web_public_access`` policy via
            the ``/web/config/.*`` resource pattern).  Returns a JSON array where
            each entry has ``prefix``, ``owner``, and ``description`` fields.

            Page authors can call this endpoint instead of hardcoding URL paths such
            as ``/web/geoid/...`` — the prefix registry tells them which namespaces
            are available and which extension owns them.
            """
            if not self.web_module:
                return []
            if hasattr(self.web_module, "list_static_prefix_info"):
                return self.web_module.list_static_prefix_info()
            # Fallback for older WebModule without the method
            return [{"prefix": p, "owner": "", "description": ""} for p in sorted(self.web_module.static_providers)]

        @self.router.get("/search", response_class=JSONResponse)
        async def global_search(
            q: str = Query(..., min_length=2, description="Search query"),
            types: str = Query("catalog,collection,item,doc,task", description="Comma-separated result types"),
            limit: int = Query(5, ge=1, le=50, description="Max results per type"),
        ):
            """Federated search across catalogs, collections, items, docs, and tasks.

            Anonymous-accessible endpoint that searches multiple data sources
            and returns grouped results. Each group is limited to ``limit`` items.

            Returns:
                {
                    "results": {
                        "catalogs": [...],
                        "collections": [...],
                        "items": [...],
                        "docs": [...],
                        "tasks": [...]
                    },
                    "total": 42,
                    "query_time_ms": 23
                }
            """
            import time
            start_time = time.time()
            
            results = {
                "results": {
                    "catalogs": [],
                    "collections": [],
                    "items": [],
                    "docs": [],
                    "tasks": [],
                },
                "total": 0,
                "query_time_ms": 0,
            }
            
            if not self.web_module:
                return results
            
            query = q.lower().strip()
            type_filters = [t.strip() for t in types.split(",")]
            
            try:
                # Search catalogs
                if "catalog" in type_filters:
                    from dynastore.models.protocols.catalogs import CatalogsProtocol
                    catalog_proto = get_protocol(CatalogsProtocol)
                    if catalog_proto and hasattr(catalog_proto, "list"):
                        catalogs = await catalog_proto.list()
                        matches = [
                            {
                                "id": c.get("id"),
                                "title": c.get("title", c.get("id")),
                                "description": c.get("description", ""),
                                "url": f"/?catalog={c.get('id')}",
                            }
                            for c in catalogs
                            if query in c.get("id", "").lower()
                            or query in c.get("title", "").lower()
                            or query in c.get("description", "").lower()
                        ][:limit]
                        results["results"]["catalogs"] = matches
                        results["total"] += len(matches)
                
                # Search collections (simplified - just search across known catalogs)
                if "collection" in type_filters:
                    # TODO: Implement collection search when we have a better approach
                    # For now, this is a placeholder
                    pass
                
                # Search docs
                if "doc" in type_filters:
                    matches = []
                    for doc_id, doc_item in self.docs_registry.items():
                        title = doc_item.get("title", "")
                        if query in title.lower() or query in doc_id.lower():
                            matches.append({
                                "id": doc_id,
                                "title": title,
                                "category": doc_item.get("category", "root"),
                                "url": f"/web/docs#{doc_id}",
                            })
                            if len(matches) >= limit:
                                break
                    results["results"]["docs"] = matches
                    results["total"] += len(matches)
                
                # Search tasks (if available)
                if "task" in type_filters:
                    # TODO: Implement task search when task protocol is available
                    pass
                
            except Exception as e:
                logger.exception(f"Search error: {e}")
            
            results["query_time_ms"] = int((time.time() - start_time) * 1000)
            return results

        @self.router.get("/admin/pages/{page_id}/providers", response_class=JSONResponse)
        async def get_page_providers(page_id: str):
            """Sysadmin endpoint: list every handler registered for a page id.

            Gated by the ``web_sysadmin_access`` policy.  Returns a JSON array
            where each entry describes one handler:

            - ``priority`` — lower values render first
            - ``is_embed`` — ``true`` for fragment contributors, ``false`` for
              the primary page handler
            - ``handler`` — fully-qualified Python callable name (module.qualname)

            Useful when debugging why an embed did not appear: compare
            ``is_embed`` + ``priority`` values for all registered providers.
            """
            if not self.web_module:
                raise HTTPException(status_code=500, detail="Web module not available")
            if hasattr(self.web_module, "list_page_providers"):
                providers = self.web_module.list_page_providers(page_id)
            else:
                entry = self.web_module.web_pages.get(page_id)
                if not entry:
                    providers = []
                else:
                    providers = [
                        {
                            "priority": p,
                            "is_embed": e,
                            "handler": getattr(h, "__qualname__", repr(h)),
                        }
                        for p, h, e in entry.get("providers", [])
                    ]
            if not providers and page_id not in (self.web_module.web_pages or {}):
                raise HTTPException(status_code=404, detail=f"Page '{page_id}' not registered")
            return providers

        @self.router.get("/pages/{page_id}", response_class=HTMLResponse)
        async def get_web_page_content(
            page_id: str, request: Request, language: str = Query("en")
        ):
            if not self.web_module:
                raise HTTPException(status_code=500, detail="Web module not available")

            # Extension-exposure gate: mirror the 503 the owning extension's
            # own router returns when its `enabled` plugin config is false.
            owner_ext = None
            if hasattr(self.web_module, "get_page_owner_extension"):
                owner_ext = self.web_module.get_page_owner_extension(page_id)
            if owner_ext is not None:
                matrix = getattr(request.app.state, "exposure_matrix", None)
                if matrix is not None:
                    snap = await matrix.get()
                    if not snap.platform.get(owner_ext, True):
                        raise HTTPException(
                            status_code=503,
                            detail=(
                                f"Service '{owner_ext}' is disabled on this "
                                "platform. Contact the administrator."
                            ),
                        )

            content = await self.web_module.get_web_page_content(page_id, request, language)
            if content is None:
                raise HTTPException(status_code=404, detail="Page not found")
            
            return HTMLResponse(content=content)

        # Demo-data provisioning is no longer a bespoke /admin/demo/* route.
        # Apply the standard `demo_data` preset instead:
        #   POST   /configs/presets/demo_data   (provision)
        #   DELETE /configs/presets/demo_data   (clean up)

        @self.router.get("/docs-manifest", response_class=JSONResponse)
        async def get_docs_manifest():
            """
            Returns the manifest of all found documentation, grouped by category.
            Includes ID, Title, and Full Path.
            """
            # Initialize buckets with all expected categories (must match frontend categoryMapping)
            buckets = {
                "platform": [],
                "architecture": [],
                "components": [],
                "modules": [],
                "extensions": [],
                "tasks": [],
                "root": [],
            }

            # Iterate through the registry and bucket items by category
            for doc_item in self.docs_registry.values():
                cat = doc_item.get("category", "root")
                if cat not in buckets:
                    buckets[cat] = []

                buckets[cat].append(
                    {
                        "id": doc_item["id"],
                        "title": doc_item["title"],
                        "path": doc_item["path"],
                    }
                )

            # Create the final manifest using raw category IDs as keys
            manifest = {}
            for cat, items in buckets.items():
                if not items:
                    continue

                # Sort each category alphabetically by title
                items.sort(key=lambda x: x["title"])

                # Use raw category ID as key for the frontend
                manifest[cat] = items

            return manifest

        @self.router.get("/docs-content/{doc_id:path}", response_class=HTMLResponse)
        async def get_doc_content(doc_id: str):
            doc_item = self.docs_registry.get(doc_id)

            if not doc_item or not os.path.exists(doc_item["path"]):
                raise HTTPException(status_code=404, detail="Documentation not found")

            try:
                import markdown
            except ImportError:
                logger.error(
                    "Markdown library not installed. Documentation rendering unavailable."
                )
                return HTMLResponse(
                    "<h1>Documentation renderer (markdown) not installed</h1>",
                    status_code=500,
                )

            try:
                md_content = await asyncio.to_thread(
                    Path(doc_item["path"]).read_text, encoding="utf-8"
                )

                # Convert to HTML
                html_content = markdown.markdown(
                    md_content,
                    extensions=["fenced_code", "tables", "def_list", "nl2br"],
                )
                return HTMLResponse(content=html_content)
            except Exception as e:
                logger.error(f"Error reading doc {doc_id}: {e}")
                raise HTTPException(status_code=500, detail="Error reading document") from e

        @self.router.get("/dashboard/catalogs", response_class=JSONResponse)
        async def get_dashboard_catalogs(
            q: Optional[str] = Query(None, description="Search query"),
            limit: int = Query(100, ge=1, le=1000),
            offset: int = Query(0, ge=0),
        ):
            """List catalogs visible to the caller.

            Visibility is transparent: the storage layer narrows the listing
            to what the request's caller may see (``RequestVisibility`` →
            ``ListingVisibilityProtocol``), so this route is a plain protocol
            call. With no authorization layer mounted the full list returns.
            """
            from dynastore.models.protocols import CatalogsProtocol

            catalogs_provider: Optional[CatalogsProtocol] = get_protocol(CatalogsProtocol)
            if not catalogs_provider:
                return []

            cats = await catalogs_provider.list_catalogs(limit=limit, offset=offset, q=q)
            return [c.model_dump() for c in cats]

        @self.router.get("/catalogs", response_class=JSONResponse)
        async def get_catalog_options(
            request: Request,
            language: str = Depends(get_language),
        ):
            """Catalog options for UI pickers — pluggable, server-resolved.

            Returns ``{"catalogs": [{"id", "title"}]}`` from the
            highest-priority *available* ``CatalogListProvider``. Caller
            visibility is enforced transparently beneath the provider's
            ``CatalogsProtocol`` call (``RequestVisibility`` →
            ``ListingVisibilityProtocol``), so the default full-list
            provider already returns only what the caller may see. New
            providers become selectable by registering a higher priority;
            no front-end change is needed.
            """
            from dynastore.models.protocols.catalog_source import (
                CatalogListProvider,
            )

            providers = get_protocols(CatalogListProvider)
            if not providers:
                return {"catalogs": []}
            winner = providers[0]  # priority-sorted, is_available()-gated
            options = await winner.list_catalog_options(request, language)
            return {
                "catalogs": [{"id": o.id, "title": o.title} for o in options]
            }

        @self.router.get(
            "/dashboard/catalogs/{catalog_id}/collections", response_class=JSONResponse
        )
        async def get_dashboard_collections(
            catalog_id: str,
            q: Optional[str] = Query(None, description="Search query"),
            limit: int = Query(100, ge=1, le=1000),
            offset: int = Query(0, ge=0),
        ):
            from dynastore.models.protocols import CollectionsProtocol
            from dynastore.tools.discovery import get_protocol

            collections_provider: Optional[CollectionsProtocol] = get_protocol(
                CollectionsProtocol
            )
            if collections_provider:
                try:
                    cols = await collections_provider.list_collections(
                        catalog_id=catalog_id, limit=limit, offset=offset, q=q
                    )
                    return [c.model_dump() for c in cols]
                except (ValueError, KeyError):
                    return []
            return []

        # ------------------------------------------------------------------ #
        # Dashboard data endpoints — TWO TIERS, both gated declaratively by  #
        # PermissionProtocol policies declared by web_enable preset and    #
        # seeded by IAM via PolicyContributorPreset:                        #
        #                                                                     #
        #   Platform tier  : /web/dashboard/{stats|logs|events|tasks}        #
        #     Sysadmin-only via web_dashboard_platform_access policy.         #
        #     /web/dashboard/ogc-compliance is admin+user via                 #
        #     web_dashboard_ogc_compliance_access (broader audience).         #
        #                                                                     #
        #   Per-catalog    : /web/dashboard/catalogs/{catalog_id}/...         #
        #     Anyone with catalog membership for the URL's catalog_id, via    #
        #     web_dashboard_per_catalog_access policy + the                   #
        #     catalog_membership_required ConditionHandler.                   #
        #                                                                     #
        # The system tier lives ABOVE the catalog hierarchy (storage          #
        # parallel: platform tables live in `{module_name}.{table_name}`     #
        # schemas, distinct from per-catalog schemas). The URL must reflect   #
        # that — `/web/dashboard/catalogs/_system_/...` is intentionally NOT  #
        # a route. Handler bodies carry zero authz code.                      #
        # ------------------------------------------------------------------ #

        async def _stats_summary(
            catalog_id: str,
            collection_id: Optional[str],
            principal_id: Optional[str],
            start_date: Optional[datetime],
            end_date: Optional[datetime],
            path_pattern: Optional[str] = None,
            methods: Optional[List[str]] = None,
        ) -> Dict[str, Any]:
            """Shared stats fan-out used by per-catalog and per-collection routes."""
            from dynastore.modules import get_protocol
            from dynastore.models.protocols import CatalogsProtocol
            from dynastore.models.protocols.stats import StatsProtocol

            catalogs = get_protocol(CatalogsProtocol)
            db_resource = getattr(catalogs, "engine", None) if catalogs else None
            schema = "catalog"
            if catalog_id and catalog_id != "_system_" and catalogs is not None:
                try:
                    schema = (
                        await catalogs.resolve_physical_schema(
                            catalog_id, ctx=DriverContext(db_resource=db_resource)
                        )
                        or "catalog"
                    )
                except ValueError:
                    pass

            stats_service = get_protocol(StatsProtocol)
            summary = None
            if stats_service:
                summary = await stats_service.get_summary(
                    schema=schema,
                    catalog_id=catalog_id if catalog_id != "_system_" else None,
                    collection_id=collection_id,
                    principal_id=principal_id,
                    start_date=start_date,
                    end_date=end_date,
                    path_pattern=path_pattern,
                    methods=methods,
                )
            return (
                summary.model_dump()
                if summary
                else {"total_requests": 0, "average_latency_ms": 0}
            )

        # ----- Platform-tier dashboard endpoints (sysadmin-only via Policy) -- #

        @self.router.get("/dashboard/stats", response_class=JSONResponse)
        async def get_dashboard_platform_stats(
            principal_id: Optional[str] = Query(None, description="Filter by Principal ID."),
            start_date: Optional[datetime] = Query(None, description="Start date for stats aggregation."),
            end_date: Optional[datetime] = Query(None, description="End date for stats aggregation."),
            path_pattern: Optional[str] = Query(None, description="Regex on request path (e.g. '^/tiles/')."),
            methods: Optional[List[str]] = Query(None, description="Restrict to one or more HTTP methods."),
        ):
            return await _stats_summary("_system_", None, principal_id, start_date, end_date, path_pattern, methods)

        # /dashboard/logs and /dashboard/events deleted: callers now hit
        # the canonical /logs/ and /events/ extension surfaces directly
        # (one canonical REST surface per module — no dashboard proxies).

        @self.router.get("/dashboard/tasks", response_class=JSONResponse)
        async def get_dashboard_platform_tasks(
            language: str = Depends(get_language),
        ):
            from dynastore.models.protocols.tasks import TasksProtocol
            from dynastore.models.protocols import DatabaseProtocol
            from dynastore.modules.db_config.query_executor import managed_transaction
            from dynastore.modules.tasks.tasks_module import get_task_schema
            from dynastore.extensions.tools.response_i18n import localize_response_dict

            tasks_svc = get_protocol(TasksProtocol)
            if tasks_svc is None:
                return []
            db = get_protocol(DatabaseProtocol)
            engine = db.get_any_engine() if db else None
            if engine is None:
                return []
            try:
                schema = get_task_schema()
                async with managed_transaction(engine) as conn:
                    tasks = await tasks_svc.list_tasks(conn, schema)
                result = []
                for t in tasks:
                    d = t.model_dump() if hasattr(t, "model_dump") else t
                    if isinstance(d, dict):
                        # Task.title is excluded from model_dump (it holds the raw
                        # {lang: text} map); surface the stored title from the
                        # inputs JSONB so it can be resolved to the request language.
                        d["title"] = (d.get("inputs") or {}).get("title")
                        localize_response_dict(d, language, text_fields=("title",), link_keys=())
                    result.append(d)
                return result
            except Exception:
                return []

        @self.router.get(
            "/dashboard/ogc-compliance",
            response_class=JSONResponse,
            tags=["Dynastore Web Service"],
        )
        async def get_dashboard_platform_ogc_compliance():
            from dynastore.extensions.tools.conformance import get_conformance_summary
            return get_conformance_summary().model_dump()

        # ----- Per-catalog dashboard endpoints (membership-gated via Policy) - #

        @self.router.get("/dashboard/catalogs/{catalog_id}/stats", response_class=JSONResponse)
        async def get_dashboard_stats(
            catalog_id: str,
            principal_id: Optional[str] = Query(None, description="Filter by Principal ID."),
            start_date: Optional[datetime] = Query(None, description="Start date for stats aggregation."),
            end_date: Optional[datetime] = Query(None, description="End date for stats aggregation."),
            path_pattern: Optional[str] = Query(None, description="Regex on request path (e.g. '^/tiles/')."),
            methods: Optional[List[str]] = Query(None, description="Restrict to one or more HTTP methods."),
        ):
            return await _stats_summary(catalog_id, None, principal_id, start_date, end_date, path_pattern, methods)

        @self.router.get(
            "/dashboard/catalogs/{catalog_id}/collections/{collection_id}/stats",
            response_class=JSONResponse,
        )
        async def get_dashboard_stats_per_collection(
            catalog_id: str,
            collection_id: str,
            principal_id: Optional[str] = Query(None, description="Filter by Principal ID."),
            start_date: Optional[datetime] = Query(None, description="Start date for stats aggregation."),
            end_date: Optional[datetime] = Query(None, description="End date for stats aggregation."),
            path_pattern: Optional[str] = Query(None, description="Regex on request path (e.g. '^/tiles/')."),
            methods: Optional[List[str]] = Query(None, description="Restrict to one or more HTTP methods."),
        ):
            return await _stats_summary(catalog_id, collection_id, principal_id, start_date, end_date, path_pattern, methods)

        @self.router.get("/dashboard/catalogs/{catalog_id}/tasks", response_class=JSONResponse)
        async def get_dashboard_tasks(
            catalog_id: str,
            language: str = Depends(get_language),
        ):
            from dynastore.models.protocols.tasks import TasksProtocol
            from dynastore.models.protocols import DatabaseProtocol
            from dynastore.modules.db_config.query_executor import managed_transaction
            from dynastore.extensions.tools.response_i18n import localize_response_dict

            tasks_svc = get_protocol(TasksProtocol)
            if tasks_svc is None:
                return []
            db = get_protocol(DatabaseProtocol)
            engine = db.get_any_engine() if db else None
            if engine is None:
                return []
            try:
                async with managed_transaction(engine) as conn:
                    tasks = await tasks_svc.list_tasks_for_catalog(conn, catalog_id)
                result = []
                for t in tasks:
                    d = t.model_dump() if hasattr(t, "model_dump") else t
                    if isinstance(d, dict):
                        # Task.title is excluded from model_dump (it holds the raw
                        # {lang: text} map); surface the stored title from the
                        # inputs JSONB so it can be resolved to the request language.
                        d["title"] = (d.get("inputs") or {}).get("title")
                        localize_response_dict(d, language, text_fields=("title",), link_keys=())
                    result.append(d)
                return result
            except Exception:
                return []

        # /dashboard/catalogs/{cat}[/collections/{col}]/{logs,events} deleted:
        # callers fetch canonical /logs/catalogs/{cat}... and /events/catalogs/{cat}...
        # directly. Per-catalog access is enforced at the canonical endpoints
        # (catalog_membership_required condition).

        @self.router.get(
            "/dashboard/catalogs/{catalog_id}/ogc-compliance",
            response_class=JSONResponse,
            tags=["Dynastore Web Service"],
        )
        async def get_ogc_compliance(catalog_id: str):
            """Return the OGC API conformance summary. The summary is
            deployment-wide today, but the route is per-catalog so callers
            traverse the same path convention as every other dashboard data
            endpoint; authz is enforced upstream by the
            web_dashboard_per_catalog_access policy."""
            from dynastore.extensions.tools.conformance import get_conformance_summary
            summary = get_conformance_summary()
            return summary.model_dump()

        @self.router.get("/lite/api/contents/{contents_path:path}", include_in_schema=False)
        async def serve_jupyterlite_contents(contents_path: str):
            """Serve JupyterLite contents API from the notebooks database.

            The JupyterLite service worker (scope /web/lite/) intercepts
            requests to /api/contents/ and falls back to the network when
            files are absent from its cache.  This route answers those
            fallback requests so the lab can open DB-stored notebooks.
            """
            from dynastore.modules.notebooks import notebooks_module as nb_svc
            from dynastore.modules.db_config.exceptions import ResourceNotFoundError

            now_iso = datetime.now(timezone.utc).isoformat()

            # Strip trailing slashes and the all.json suffix for normalisation
            path = contents_path.rstrip("/")
            is_all_json = path.endswith("/all.json") or path == "all.json"
            if is_all_json:
                path = path[: -len("all.json")].rstrip("/")

            # Root directory listing → just the "platform" directory
            if path == "":
                try:
                    items, _total = await nb_svc.list_platform_notebooks(limit=200)
                except Exception:
                    items, _total = [], 0
                nb_entries = []
                for nb in items:
                    nb_d = nb if isinstance(nb, dict) else nb.model_dump()
                    nid = nb_d.get("notebook_id", "")
                    upd = nb_d.get("updated_at")
                    ts = upd.isoformat() if upd is not None else now_iso
                    nb_entries.append({
                        "name": f"{nid}.ipynb", "path": f"platform/{nid}.ipynb",
                        "type": "notebook", "format": "json", "writable": False,
                        "created": ts, "last_modified": ts,
                        "content": None, "mimetype": None, "size": None,
                    })
                entries = []
                if _total:
                    entries.append({
                        "name": "platform", "path": "platform",
                        "type": "directory", "format": "json",
                        "writable": False, "created": now_iso,
                        "last_modified": now_iso, "content": nb_entries,
                        "mimetype": None, "size": None,
                    })
                return JSONResponse(entries, headers={"Cache-Control": "no-cache"})

            # Platform directory listing
            if path == "platform":
                try:
                    items, _ = await nb_svc.list_platform_notebooks(limit=200)
                except Exception:
                    items = []
                content = []
                for nb in items:
                    nb_d = nb if isinstance(nb, dict) else nb.model_dump()
                    nid = nb_d.get("notebook_id", "")
                    upd = nb_d.get("updated_at")
                    ts = upd.isoformat() if upd is not None else now_iso
                    content.append({
                        "name": f"{nid}.ipynb", "path": f"platform/{nid}.ipynb",
                        "type": "notebook", "format": "json", "writable": False,
                        "created": ts, "last_modified": ts,
                        "content": None, "mimetype": None, "size": None,
                    })
                if is_all_json:
                    return JSONResponse(content, headers={"Cache-Control": "no-cache"})
                return JSONResponse({
                    "name": "platform", "path": "platform",
                    "type": "directory", "format": "json",
                    "writable": False, "created": now_iso, "last_modified": now_iso,
                    "content": content, "mimetype": None, "size": None,
                }, headers={"Cache-Control": "no-cache"})

            # Individual notebook file: platform/{notebook_id}.ipynb
            parts = path.split("/")
            if len(parts) == 2 and parts[0] == "platform" and parts[1].endswith(".ipynb"):
                notebook_id = parts[1][:-6]
                try:
                    nb = await nb_svc.get_platform_notebook(notebook_id)
                    nb_d = nb if isinstance(nb, dict) else nb.model_dump()
                    nb_content = nb_d.get("content") or {}
                    upd = nb_d.get("updated_at")
                    ts = upd.isoformat() if upd is not None else now_iso
                    return JSONResponse({
                        "name": f"{notebook_id}.ipynb",
                        "path": f"platform/{notebook_id}.ipynb",
                        "type": "notebook", "format": "json",
                        "writable": False, "created": ts, "last_modified": ts,
                        "content": nb_content, "mimetype": None, "size": None,
                    }, headers={"Cache-Control": "no-cache"})
                except (ResourceNotFoundError, Exception) as exc:
                    raise HTTPException(status_code=404, detail="Notebook not found") from exc

            raise HTTPException(status_code=404, detail="Not found")

        @self.router.get("/lite/api/workspaces/{ws_path:path}", include_in_schema=False)
        async def serve_jupyterlite_workspaces(ws_path: str):
            # JupyterLab boots by GET /api/workspaces/all.json (and individual
            # workspace IDs). We don't persist workspaces server-side — the
            # lab keeps them in IndexedDB. Return empty payloads so the lab
            # stops 404-spamming the access log.
            if ws_path.rstrip("/").endswith("all.json"):
                return JSONResponse({"workspaces": {"ids": [], "values": []}},
                                    headers={"Cache-Control": "no-cache"})
            return JSONResponse({"metadata": {"id": ws_path}, "data": {}},
                                headers={"Cache-Control": "no-cache"})

        @self.router.get("/{prefix}/{filename:path}", include_in_schema=False)
        async def serve_static_content(prefix: str, filename: str):
            # Resolve the provider callable for this prefix
            provider_callable = None

            if self.web_module and prefix in self.web_module.static_providers:
                provider_callable = self.web_module.static_providers[prefix]
            elif prefix == "static":
                # Fallback: serve directly from the static directory if not registered
                provider_callable = self._provide_default_static
            
            if provider_callable is None:
                raise HTTPException(
                    status_code=404, detail=f"No provider found for prefix '{prefix}'"
                )

            try:
                if isinstance(provider_callable, StaticFilesProtocol):
                    allowed_files = await provider_callable.list_static_files()
                else:
                    # Legacy: provider is a callable returning absolute paths
                    allowed_files = provider_callable()
            except Exception as e:
                logger.error(f"Static provider for '{prefix}' raised an error: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Provider error: {e}") from e

            if not allowed_files:
                # If the provider returned an empty list, the file doesn't exist under this prefix
                raise HTTPException(status_code=404, detail="File not found")
            
            # Build a relative-path -> absolute-path map using the common root of all files
            try:
                dirs = [os.path.dirname(f) for f in allowed_files]
                common_root = os.path.commonpath(dirs) if len(dirs) > 1 else dirs[0]
            except ValueError as e:
                logger.error(f"commonpath failed for prefix '{prefix}': {e}")
                raise HTTPException(status_code=500, detail="Static file layout error") from e

            allowed_map = {
                os.path.relpath(f, common_root).replace(os.sep, "/"): f 
                for f in allowed_files
            }
            
            lookup_key = filename.replace(os.sep, "/")
            target_file = allowed_map.get(lookup_key)

            if not target_file:
                logger.warning(
                    f"File '{filename}' not found in allowlist for prefix '{prefix}'. "
                    f"Available: {list(itertools.islice(allowed_map.keys(), 10))}"
                )
                raise HTTPException(status_code=404, detail="File not found")

            cache_headers = self._cache_headers_for(prefix, lookup_key)
            return await self.serve_file(target_file, extra_headers=cache_headers)
