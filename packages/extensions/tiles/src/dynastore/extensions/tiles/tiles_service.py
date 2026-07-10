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

# dynastore/extensions/tiles/tiles_service.py

import logging
import json
import asyncio
import hashlib
import re
import time
from typing import Any, Awaitable, ClassVar, FrozenSet, Literal, Optional, Dict, List, Tuple
from contextlib import asynccontextmanager
from fastapi import (
    FastAPI,
    APIRouter,
    Depends,
    HTTPException,
    Response,
    Query,
    Request,
    Path,
    BackgroundTasks,
)
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.extensions import protocols
from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.extensions.tools.ogc_common_models import LandingPage
from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols.configs import ConfigsProtocol
from dynastore.models.protocols.crs import CRSProtocol
from dynastore.models.protocols.web import WebModuleProtocol, StaticFilesProtocol
from dynastore.extensions.web.decorators import expose_static
from dynastore.extensions.tools.db import get_async_engine
from dynastore.modules.db_config.query_executor import (
    _read_live_fg_acquire_timeout,
    acquire_engine_connection_bounded,
    DQLQuery,
    ResultHandler,
)
from dynastore.modules.db_config.exceptions import (
    DatabaseConnectionError,
    PoolSaturationError,
    QueryExecutionError,
)
from dynastore.tools.render_admission import RenderAdmissionGate, RenderAdmissionRejected
from dynastore.extensions.tools.query import parse_hints_param, validate_filter_lang
from dynastore.extensions.tools.resolvers import (
    resolve_internal_catalog_id_or_404,
    resolve_internal_collection_id_or_404,
)
import dynastore.modules.tiles.tiles_module as tms_manager
from dynastore.tools.geospatial import SimplificationAlgorithm
from dynastore.extensions.web.decorators import expose_web_page
import os

from dynastore.modules.tiles.tiles_module import TileStorageProtocol, TileArchiveStorageProtocol
from dynastore.modules.tiles.tiles_config import (
    TilesConfig,
    TilesCachingConfig,
    cache_on_demand_enabled,
)
from dynastore.modules.tiles.tiles_models import (
    TileMatrixSetList,
    TileMatrixSet,
    TileMatrixSetRef,
    Link,
    TileMatrixSetCreate,
    TileSetItem,
    TileSetList,
)
from dynastore.modules.tiles.tms_definitions import BUILTIN_TILE_MATRIX_SETS
from .tile_cache_writer import TileCacheWriter

logger = logging.getLogger(__name__)

_FILTER_LANG_QUERY = Query(
    "cql2-text",
    alias="filter-lang",
    description="CQL2 filter encoding. Supported: 'cql2-text' and 'cql2-json'.",
)
_FILTER_CRS_QUERY = Query(
    None,
    alias="filter-crs",
    description=(
        "URI of the CRS used by geometry literals in the CQL2 filter. "
        "Defaults to CRS84 / EPSG:4326 semantics."
    ),
)

# Upper bound on how long TilesService.lifespan waits for the tile-cache
# writer to drain its queue on shutdown, kept comfortably below the Cloud Run
# SIGTERM grace period so shutdown never stalls on a slow bucket write.
_TILE_CACHE_WRITER_DRAIN_SECONDS = 8.0

# PostgreSQL pgcode for "query_canceled" — raised when a statement exceeds
# ``statement_timeout``. Used by ``get_vector_tile`` (#2813) to distinguish a
# bounded-timeout cancellation from a genuine query failure (500). A
# cancellation means the tile's content is simply unknown — not "no data" —
# so it is never reported as 204; it falls back to a stale cached tile or a
# 503 instead (#2965).
_QUERY_CANCELED_PGCODE = "57014"

# A second cancellation shape (#3181): asyncpg's own statement-cancel
# handling can lose a race against another operation on the same wire
# (SQLAlchemy's rollback-on-error, or ``managed_transaction``'s cancel
# drain in ``query_executor.py``) and raise ``InterfaceError`` instead of
# the clean pgcode-57014 ``QueryCanceledError``. Detected by class name +
# message rather than an ``isinstance`` check — this extension has no
# other reason to depend on asyncpg directly.
_INTERFACE_ERROR_CLASS_NAME = "InterfaceError"
_ANOTHER_OPERATION_IN_PROGRESS_FRAGMENT = "another operation is in progress"


def _is_timeout_cancel_race(
    exc: BaseException, elapsed_s: float, timeout_s: float
) -> bool:
    """True if ``exc`` represents the live-tile statement-timeout cancelling
    the render, in either of its two observed shapes (#3181).

    Walks ``exc``'s cause chain — ``.original_exception`` (this codebase's
    ``DatabaseError`` idiom), ``.orig`` (SQLAlchemy's ``DBAPIError`` idiom),
    then ``__cause__``/``__context__`` (plain Python exception chaining) —
    mirroring the walk in ``query_executor._is_transient_asyncpg_error``,
    looking for either:

    - pgcode ``57014`` (``query_canceled``): PostgreSQL cleanly reported the
      cancellation. Matches regardless of ``elapsed_s``.
    - An ``InterfaceError`` whose message says another operation was
      already in progress on the wire: asyncpg's own cancel-vs-concurrent-
      operation race, which carries no pgcode. Only counted as *this*
      request's timeout once ``elapsed_s`` has actually reached
      ``timeout_s`` — an ``InterfaceError`` well before the deadline is an
      unrelated wire fault and must still surface as a 500.

    The walk is bounded to a handful of hops so a malformed cause chain (or
    a test double whose attribute access never returns ``None``) cannot
    loop forever.
    """
    seen: set[int] = set()
    candidate: Optional[BaseException] = exc
    for _ in range(6):
        if candidate is None or id(candidate) in seen:
            break
        seen.add(id(candidate))
        if getattr(candidate, "pgcode", None) == _QUERY_CANCELED_PGCODE:
            return True
        if (
            type(candidate).__name__ == _INTERFACE_ERROR_CLASS_NAME
            and _ANOTHER_OPERATION_IN_PROGRESS_FRAGMENT in str(candidate)
        ):
            return elapsed_s >= timeout_s
        candidate = (
            getattr(candidate, "original_exception", None)
            or getattr(candidate, "orig", None)
            or getattr(candidate, "__cause__", None)
            or getattr(candidate, "__context__", None)
        )
    return False

# Raster render imports — guarded so the tiles extension can load in
# environments without rio-tiler (graceful degradation: map-tile routes
# return 422 when rio-tiler is absent rather than failing import).
_RENDER_COG_TILE = None
_RENDER_COG_TERRAIN_RGB = None
_RENDER_COG_HILLSHADE = None
_PARSE_SLD_COLORMAP = None
_EXTRACT_SLD_BODY = None
_FETCH_SLD_BODY = None
_STYLE_URL_FROM_ITEM = None
_STYLE_URL_CACHE_ID = None
_BUILD_RENDER_CACHE_KEY = None
_BUILD_RENDER_PARAMS_HASH = None
_RenderCachingConfig = None
try:
    from dynastore.modules.renders.engine import (  # noqa: E402
        render_cog_tile as _rct,
        render_cog_terrain_rgb as _rctr,
        render_cog_hillshade as _rch,
    )
    from dynastore.modules.renders.colormap import (  # noqa: E402
        parse_sld_colormap as _psc,
        extract_sld_body as _esb,
    )
    from dynastore.modules.renders.style_url import (  # noqa: E402
        fetch_sld_body as _fsb,
        style_url_from_item as _sufi,
        style_url_cache_id as _suci,
    )
    from dynastore.modules.renders.config import (  # noqa: E402
        build_render_cache_key as _brck,
        build_render_params_hash as _brph,
        RenderCachingConfig as _RCC,
    )
    _RENDER_COG_TILE = _rct
    _RENDER_COG_TERRAIN_RGB = _rctr
    _RENDER_COG_HILLSHADE = _rch
    _PARSE_SLD_COLORMAP = _psc
    _EXTRACT_SLD_BODY = _esb
    _FETCH_SLD_BODY = _fsb
    _STYLE_URL_FROM_ITEM = _sufi
    _STYLE_URL_CACHE_ID = _suci
    _BUILD_RENDER_CACHE_KEY = _brck
    _BUILD_RENDER_PARAMS_HASH = _brph
    _RenderCachingConfig = _RCC
except ImportError:
    pass

# Allowed characters in style_id path segment (reject path-traversal attempts).
_STYLE_ID_RE = re.compile(r'^[A-Za-z0-9._-]+$')

_FORMAT_MEDIA_TYPE: dict[str, str] = {
    "png": "image/png",
    "webp": "image/webp",
}

OGC_API_TILES_URIS = [
    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tileset",
    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tilesets-list",
    "http://www.opengis.net/spec/tms/2.0/conf/tilematrixset",
    "http://www.opengis.net/spec/tms/2.0/conf/json-tilematrixset",
    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/mvt",
    # Map-tile (dataType=map) conformance classes — this extension owns /map/tiles/...
    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/geodata-tilesets",
    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/collections-selection",
    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/png",
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/tilesets",
]


class TilesService(protocols.ExtensionProtocol, StaticFilesProtocol, OGCServiceMixin):
    priority: int = 100
    """
    Provides OGC API - Tiles functionality.

    Supports both vector (MVT) tiles backed by PostGIS and raster map tiles
    (dataType=map) rendered from COG assets via rio-tiler with SLD colormaps,
    terrain-RGB, and hillshade. Map-tile routes live under the collection
    resource: /catalogs/{cat}/collections/{coll}/map/tiles/{tms}/{z}/{x}/{y}.
    """

    conformance_uris: ClassVar[List[str]] = OGC_API_TILES_URIS
    prefix = "/tiles"
    protocol_title = "DynaStore OGC API - Tiles"
    protocol_description = (
        "Vector tile generation (MVT) backed by PostGIS and raster map tiles "
        "(dataType=map) rendered from COG assets via rio-tiler"
    )
    landing_response_model = LandingPage
    router: APIRouter

    # Bounded background writer draining interactive tile-cache writes
    # (see tile_cache_writer.py). Stashed here at startup so request
    # handlers can submit without touching the raw BackgroundTasks queue.
    # Defaults to None so tests that construct TilesService without running
    # lifespan (object.__new__ + manual attribute wiring) degrade to a no-op
    # write instead of raising.
    _tile_cache_writer: Optional[TileCacheWriter] = None

    # Per-worker render admission gate (geoid#3155) — caps concurrent MVT
    # vector-tile and raster/vector map-tile renders against this worker's
    # memory budget so a burst of heavy renders can no longer pin unbounded
    # heap together (see tools/render_admission.py). A class-level default
    # (construction is pure env/derived, no I/O) so tests that build a
    # TilesService via ``object.__new__`` — bypassing ``__init__``, the same
    # pattern already used for ``_tile_cache_writer`` above — still exercise
    # real admission control instead of silently skipping it; ``__init__``
    # replaces it with a fresh per-instance gate.
    _render_gate: RenderAdmissionGate = RenderAdmissionGate()

    def get_web_pages(self):
        from dynastore.extensions.tools.web_collect import collect_web_pages
        return collect_web_pages(self)

    def get_static_assets(self):
        from dynastore.extensions.tools.web_collect import collect_static_assets
        return collect_static_assets(self)

    def get_notebooks(self):
        try:
            from .notebooks import build_contributions
        except Exception:
            return []
        return build_contributions()

    def configure_app(self, app: FastAPI):
        """Early configuration for the Tiles extension."""
        pass

    async def _resolve_crs_srid(
        self, conn: Any, catalog_id: str, crs_uri: Optional[str]
    ) -> Optional[int]:
        """Resolve a CRS URI to an SRID for CQL geometry literals."""
        if not crs_uri:
            return None
        if "CRS84" in crs_uri.upper():
            return 4326
        match = re.search(r"[/|:](\d+)$", crs_uri)
        if match:
            return int(match.group(1))

        crs_svc = get_protocol(CRSProtocol)
        if crs_svc:
            crs_def = await crs_svc.get_crs_by_uri(conn, catalog_id, crs_uri)
            if crs_def and hasattr(crs_def, "srid"):
                return crs_def.srid
        return None

    def __init__(self, app: Optional[FastAPI] = None):
        super().__init__()
        self.app = app
        self.router = APIRouter(tags=["OGC API - Tiles"], prefix="/tiles")
        self._render_gate = RenderAdmissionGate()
        self._register_routes()
        logger.info("Tiles Service: Initializing.")

    def contribute(self, ref):
        """AssetContributor: emit a vector-tiles XYZ template link for items."""
        from dynastore.models.protocols.asset_contrib import AssetLink
        if ref.item_id is None:
            return
        href = (
            f"{ref.base_url}{self.router.prefix}/{ref.catalog_id}"
            f"/tiles/{{z}}/{{x}}/{{y}}.mvt?collections={ref.collection_id}"
        )
        yield AssetLink(
            key="vector_tiles",
            href=href,
            title="Vector Tiles (MVT)",
            media_type="application/vnd.mapbox-vector-tile",
            roles=("tiles",),
        )

    def _register_routes(self):
        self.register_ogc_standard_routes()
        col = "/catalogs/{catalog_id}/collections/{collection_id}"
        route_table: list[tuple[str, str, list[str], dict[str, Any]]] = [
            # Tile Matrix Sets (server-level, untouched)
            (
                "/tileMatrixSets", "get_tile_matrix_sets", ["GET"],
                {
                    "response_model": TileMatrixSetList,
                    "summary": "Retrieve available Tile Matrix Sets",
                },
            ),
            (
                "/tileMatrixSets/{tileMatrixSetId}", "get_tile_matrix_set", ["GET"],
                {
                    "response_model": TileMatrixSet,
                    "summary": "Retrieve a Tile Matrix Set definition",
                },
            ),
            # Tile Content — deprecated flat/catalog-dataset paths
            (
                "/catalogs/{dataset}/tiles/{z}/{x}/{y}.mvt", "get_vector_tile_catalog_default", ["GET"],
                {
                    "deprecated": True,
                    "summary": (
                        "Catalog-centric MVT endpoint (deprecated). "
                        "Use /tiles/catalogs/{catalog_id}/collections/{collection_id}/tiles/{z}/{x}/{y}.mvt instead."
                    ),
                },
            ),
            (
                "/catalogs/{dataset}/tiles/{tileMatrixSetId}/{z}/{x}/{y}.{format}", "get_vector_tile_catalog", ["GET"],
                {
                    "deprecated": True,
                    "summary": (
                        "Catalog-centric MVT with TMS (deprecated). "
                        "Use /tiles/catalogs/{catalog_id}/collections/{collection_id}/tiles/{tms}/{z}/{x}/{y}.{format} instead."
                    ),
                },
            ),
            (
                "/{dataset}/tiles/{z}/{x}/{y}.mvt", "get_vector_tile_default", ["GET"],
                {
                    "deprecated": True,
                    "summary": (
                        "Legacy MVT endpoint (deprecated). "
                        "Use /tiles/catalogs/{catalog_id}/collections/{collection_id}/tiles/{z}/{x}/{y}.mvt instead."
                    ),
                },
            ),
            (
                "/{dataset}/tiles/{tileMatrixSetId}/{z}/{x}/{y}.{format}", "get_vector_tile", ["GET"],
                {
                    "deprecated": True,
                    "summary": (
                        "Get filtered MVT (deprecated). "
                        "Use /tiles/catalogs/{catalog_id}/collections/{collection_id}/tiles/{tms}/{z}/{x}/{y}.{format} instead."
                    ),
                },
            ),
            # Cache Management — deprecated flat path
            (
                "/{dataset}/tiles/cache", "invalidate_tile_cache", ["DELETE"],
                {
                    "status_code": 200,
                    "deprecated": True,
                    "summary": (
                        "Invalidate tile cache (deprecated). "
                        "Use DELETE /tiles/catalogs/{catalog_id}/collections/{collection_id}/tiles/cache instead."
                    ),
                },
            ),
            # Tile Matrix Sets (per-collection deprecated path)
            (
                "/{dataset}/tileMatrixSets", "create_tile_matrix_set", ["POST"],
                {
                    "deprecated": True,
                    "response_model": TileMatrixSet,
                    "status_code": 201,
                    "summary": (
                        "Create a custom Tile Matrix Set (deprecated). "
                        "Use POST /tiles/catalogs/{catalog_id}/collections/{collection_id}/tileMatrixSets instead."
                    ),
                },
            ),
            # --- Aligned endpoints: /tiles/catalogs/{catalog_id}/collections/{collection_id}/... ---
            # Aligned vector tile endpoints
            (
                f"{col}/tiles/{{z}}/{{x}}/{{y}}.mvt", "get_vector_tile_aligned_default", ["GET"],
                {
                    "summary": "Get vector tile (MVT) for a collection (OGC aligned path, default WebMercatorQuad TMS)",
                    "name": "get_vector_tile_aligned_default",
                },
            ),
            (
                f"{col}/tiles/{{tileMatrixSetId}}/{{z}}/{{x}}/{{y}}.{{format}}", "get_vector_tile_aligned", ["GET"],
                {
                    "summary": "Get vector tile for a collection with explicit TMS (OGC aligned path)",
                    "name": "get_vector_tile_aligned",
                },
            ),
            # Aligned tileset list
            (
                f"{col}/tiles", "get_collection_tilesets", ["GET"],
                {
                    "response_model": TileSetList,
                    "summary": "List available tilesets for a collection (OGC API Tiles §7.1)",
                    "name": "get_collection_tilesets",
                },
            ),
            # Tileset-metadata: full TileSet document for a specific TMS (vector tiles)
            (
                f"{col}/tiles/{{tileMatrixSetId}}", "get_collection_tileset", ["GET"],
                {
                    "response_model": TileSetItem,
                    "summary": (
                        "Tileset metadata for vector tiles of a collection with a given TMS "
                        "(OGC API Tiles §7.2, dataType='vector')"
                    ),
                    "name": "get_collection_tileset",
                },
            ),
            # Tileset-metadata: full TileSet document for map tiles (dataType=map)
            (
                f"{col}/map/tiles/{{tms_id}}", "get_collection_map_tileset", ["GET"],
                {
                    "response_model": TileSetItem,
                    "summary": (
                        "Tileset metadata for raster map tiles of a collection with a given TMS "
                        "(OGC API Maps §7.2, dataType='map')"
                    ),
                    "name": "get_collection_map_tileset",
                },
            ),
            # Aligned cache invalidation
            (
                f"{col}/tiles/cache", "invalidate_collection_tile_cache", ["DELETE"],
                {
                    "status_code": 200,
                    "summary": "Invalidate tile cache for a specific collection (OGC aligned path)",
                    "name": "invalidate_collection_tile_cache",
                },
            ),
            # Map tiles (dataType=map) — default style resolved via binding
            (
                f"{col}/map/tiles/{{tms_id}}/{{z}}/{{x}}/{{y}}.{{format}}", "get_map_tile", ["GET"],
                {
                    "summary": (
                        "Render a styled raster map tile (dataType=map) from a COG asset using "
                        "the collection's default style. catalog_id and collection_id are public "
                        "(external) IDs."
                    ),
                    "name": "get_map_tile",
                },
            ),
            # Map tiles with explicit style
            (
                f"{col}/styles/{{style_id}}/map/tiles/{{tms_id}}/{{z}}/{{x}}/{{y}}.{{format}}", "get_map_tile_styled", ["GET"],
                {
                    "summary": (
                        "Render a styled raster map tile (dataType=map) with an explicit style. "
                        "Use style_id='terrain-rgb' for Terrain-RGB encoding. "
                        "Add ?relief=hillshade for hillshade rendering. "
                        "catalog_id and collection_id are public (external) IDs."
                    ),
                    "name": "get_map_tile_styled",
                },
            ),
        ]
        for path, handler_name, methods, kwargs in route_table:
            self.router.add_api_route(path, getattr(self, handler_name), methods=methods, **kwargs)

    @expose_static("tiles")
    def provide_static_files(self) -> list[str]:
        """Exposes the internal static directory for the tiles viewer."""
        static_dir = os.path.join(os.path.dirname(__file__), "static")
        files = []
        for root, _, filenames in os.walk(static_dir):
            for filename in filenames:
                files.append(os.path.join(root, filename))
        return files

    def get_static_prefix(self) -> str:
        """Returns the static prefix for tiles."""
        return "tiles"

    async def is_file_provided(self, path: str) -> bool:
        """Checks if a static file is provided."""
        static_dir = os.path.realpath(os.path.join(os.path.dirname(__file__), "static"))
        full_path = os.path.realpath(os.path.join(static_dir, path.lstrip("/")))
        if not full_path.startswith(static_dir + os.sep) and full_path != static_dir:
            return False
        return os.path.isfile(full_path)

    async def list_static_files(self, query: Optional[str] = None, limit: int = 100, offset: int = 0) -> List[str]:
        """Lists static files for tiles with pagination and search."""
        static_dir = os.path.join(os.path.dirname(__file__), "static")
        files = []
        for root, _, filenames in os.walk(static_dir):
            for filename in filenames:
                full_path = os.path.join(root, filename)
                rel_path = os.path.relpath(full_path, static_dir)
                if not query or query.lower() in rel_path.lower():
                    files.append(full_path)
        return sorted(files)[offset : offset + limit]

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        """Manages the Tiles Service configuration."""
        from dynastore.tools.discovery import register_plugin, unregister_plugin

        # The STAC links contributor imports the stac extension's shared
        # tileability gate (dynastore.extensions.stac._map_tiles_gate).
        # Scopes that ship tiles without the stac extension (scope_maps)
        # cannot import it — they serve no STAC items either, so skip the
        # contributor instead of failing the whole tiles lifespan (#3177).
        contributor = None
        try:
            from .stac_contributor import TilesStacContributor
        except ImportError:
            logger.info(
                "Tiles Service: stac extension not installed — skipping the "
                "STAC map-tile links contributor."
            )
        else:
            contributor = TilesStacContributor()
            register_plugin(contributor)
        logger.info("Tiles Service startup.")

        # Register the tiles cold-boot contributor so run_cold_boot (called
        # from main.py) self-heals 'tiles_enable' at priority=35 whenever an
        # IAM policy writer is present in this process AND the deployment
        # already opted into tiles_enable (e.g. via the platform_demo
        # composite). Mirrors extensions/auth's _AuthColdBootContributor: a
        # service with no IAM writer (e.g. a maps-only tier) skips cleanly
        # instead of crashing on ctx.policy.update_policy — see
        # modules/presets/enable_cold_boot.py for the shared mechanism.
        from dynastore.modules.presets.cold_boot import register_cold_boot_contributor
        from dynastore.modules.presets.enable_cold_boot import make_enable_cold_boot_contributor
        try:
            register_cold_boot_contributor(
                make_enable_cold_boot_contributor(
                    name="tiles", priority=35, preset_name="tiles_enable",
                )
            )
        except ValueError:
            logger.debug("TilesColdBootContributor already registered; skipping duplicate.")

        from dynastore.modules.tiles.tiles_config import _load_caching_config
        caching_cfg = await _load_caching_config()
        self._tile_cache_writer = TileCacheWriter(
            buffer_max_bytes=caching_cfg.cache_writer_buffer_max_bytes,
            workers=caching_cfg.cache_writer_workers,
        )
        self._tile_cache_writer.start()

        try:
            yield
        finally:
            await self._tile_cache_writer.stop(
                drain_timeout=_TILE_CACHE_WRITER_DRAIN_SECONDS
            )
            if contributor is not None:
                unregister_plugin(contributor)
            logger.info("Tiles Service shutdown.")

    @expose_web_page(
        page_id="map_viewer",
        title="Map Viewer",
        icon="fa-map",
        priority=10,
        description="Visualize tiled datasets.",
    )
    async def provide_map_viewer(self, request: Request):
        file_path = os.path.join(os.path.dirname(__file__), "static", "map.html")
        if not os.path.exists(file_path):
            return Response(content="Template map.html not found", status_code=404)
        with open(file_path, "r", encoding="utf-8") as f:
            return Response(content=f.read(), media_type="text/html")

    @expose_web_page(
        page_id="terrain_viewer",
        title="Terrain Viewer",
        icon="fa-mountain",
        description="3D terrain with hillshade and colormap overlay from a DEM COG.",
        priority=90,
    )
    async def provide_terrain_viewer(self, request: Request) -> Response:
        from dynastore._version import VERSION
        file_path = os.path.join(os.path.dirname(__file__), "static", "terrain_viewer.html")
        if not os.path.exists(file_path):
            return Response(content="Template terrain_viewer.html not found", status_code=404)
        with open(file_path, "r", encoding="utf-8") as fh:
            return Response(
                content=fh.read().replace("{{VERSION}}", VERSION),
                media_type="text/html",
            )

    # OGC API Common (landing/conformance) delegated to OGCServiceMixin via
    # register_ogc_standard_routes; see _register_routes.

    # --- Tile Matrix Sets Endpoints ---

    async def create_tile_matrix_set(self, dataset: str, tms_data: TileMatrixSetCreate):
        """Creates a new custom TileMatrixSet scoped to a specific dataset (catalog).

        ``dataset`` is the public external catalog id. It is resolved to the
        immutable internal id before writing so the row survives a catalog rename.
        """
        catalogs_svc = await self._get_catalogs_service()
        internal_catalog_id = await resolve_internal_catalog_id_or_404(catalogs_svc, dataset)
        stored_tms = await tms_manager.create_custom_tms(
            catalog_id=internal_catalog_id, tms_data=tms_data
        )
        return stored_tms.definition

    async def get_tile_matrix_sets(
        self,
        request: Request,
        dataset: Optional[str] = Query(
            None, description="Filter TMS list by dataset (catalog)."
        ),
    ):
        """List all supported Tile Matrix Sets."""
        tms_refs = []
        for tms_id, tms_def in BUILTIN_TILE_MATRIX_SETS.items():
            tms_refs.append(
                TileMatrixSetRef(
                    id=tms_id,
                    title=tms_def.title,
                    links=[
                        Link(
                            href=str(
                                request.url_for(
                                    "get_tile_matrix_set", tileMatrixSetId=tms_id
                                )
                            ),
                            rel="self",
                            type="application/json",
                            title=tms_def.title,
                            hreflang="en",
                        )
                    ],
                )
            )

        # Append custom TMS from DB if a dataset is specified. Resolve the
        # external dataset id to the internal id so the DB lookup uses the
        # partition key that survives catalog renames.
        if dataset:
            catalogs_svc = await self._get_catalogs_service()
            internal_dataset_id = await resolve_internal_catalog_id_or_404(catalogs_svc, dataset)
            custom_tms_list = await tms_manager.list_custom_tms(catalog_id=internal_dataset_id)
            for tms in custom_tms_list:
                # Avoid duplicating if a custom TMS overrides a built-in one
                if not any(ref.id == tms.id for ref in tms_refs):
                    tms_refs.append(
                        TileMatrixSetRef(
                            id=tms.id,
                            title=tms.title,
                            links=[
                                Link(
                                    href=str(
                                        request.url_for(
                                            "get_tile_matrix_set",
                                            tileMatrixSetId=tms.id,
                                        ).include_query_params(dataset=dataset)
                                    ),
                                    rel="self",
                                    type="application/json",
                                    title=tms.title,
                                    hreflang="en",
                                )
                            ],
                        )
                    )
        return TileMatrixSetList(tileMatrixSets=tms_refs)

    async def get_tile_matrix_set(
        self,
        tileMatrixSetId: str = Path(
            ..., description="The Identifier of the Tile Matrix Set"
        ),
        dataset: Optional[str] = Query(
            None, description="Dataset (catalog) for custom TMS lookup."
        ),
    ):
        """Return the full definition of a specific Tile Matrix Set.

        When ``dataset`` is provided, the external catalog id is resolved to
        the immutable internal id before querying for a custom TMS, ensuring
        the lookup works after a catalog rename.
        """
        tms = None
        if dataset:
            catalogs_svc = await self._get_catalogs_service()
            internal_dataset_id = await resolve_internal_catalog_id_or_404(catalogs_svc, dataset)
            tms = await tms_manager.get_custom_tms(
                catalog_id=internal_dataset_id, tms_id=tileMatrixSetId
            )
        if not tms:
            tms_model = BUILTIN_TILE_MATRIX_SETS.get(tileMatrixSetId)
            if not tms_model:
                raise HTTPException(
                    status_code=404,
                    detail=f"TileMatrixSet '{tileMatrixSetId}' not found.",
                )
            return tms_model
        return tms

    # --- Tile Content Endpoints ---

    async def get_vector_tile_catalog_default(
        self,
        dataset: str,
        z: int,
        x: int,
        y: int,
        request: Request,
        background_tasks: BackgroundTasks,
        collections: str = Query(..., description="Comma-separated collection IDs."),
        datetime: Optional[str] = Query(None),
        filter: Optional[str] = Query(None, description="CQL2 Filter expression."),
        filter_lang: Optional[str] = _FILTER_LANG_QUERY,
        filter_crs: Optional[str] = _FILTER_CRS_QUERY,
        subset: Optional[str] = Query(None),
        simplification: Optional[float] = Query(None),
        simplification_by_zoom: Optional[str] = Query(None),
        simplification_algorithm: SimplificationAlgorithm = Query(
            SimplificationAlgorithm.TOPOLOGY_PRESERVING
        ),
        disable_cache: bool = Query(
            False, description="Disable cache for this request."
        ),
        refresh_cache: bool = Query(
            False, description="Refresh cache by invalidating before fetching."
        ),
        serve: Optional[Literal["proxy", "redirect"]] = Query(
            None,
            description=(
                "Per-request cache-hit delivery override: 'proxy' streams tile "
                "bytes (no redirect, for clients like QGIS that don't follow "
                "redirects); 'redirect' issues a 307 to a signed bucket URL. "
                "Omit for the platform default."
            ),
        ),
        # Accepted for uniform protocol consistency; MVT is generated by PostGIS
        # and does not pass through the hints routing layer.
        request_hints: FrozenSet = Depends(parse_hints_param),
    ):
        """Catalog-centric endpoint defaulting to WebMercatorQuad."""
        catalogs_svc = await self._get_catalogs_service()
        internal_dataset = await resolve_internal_catalog_id_or_404(catalogs_svc, dataset)
        return await self.get_vector_tile(
            request=request,
            dataset=internal_dataset,
            tileMatrixSetId="WebMercatorQuad",
            z=z,
            x=x,
            y=y,
            format="mvt",
            background_tasks=background_tasks,
            collections=collections,
            datetime=datetime,
            filter=filter,
            filter_lang=filter_lang,
            filter_crs=filter_crs,
            subset=subset,
            simplification=simplification,
            simplification_by_zoom=simplification_by_zoom,
            simplification_algorithm=simplification_algorithm,
            disable_cache=disable_cache,
            refresh_cache=refresh_cache,
            serve=serve,
        )

    async def get_vector_tile_catalog(
        self,
        request: Request,
        dataset: str,
        tileMatrixSetId: str,
        z: int,
        x: int,
        y: int,
        format: str,
        background_tasks: BackgroundTasks,
        collections: str = Query(
            ..., description="Comma-separated list of collection IDs to include."
        ),
        datetime: Optional[str] = Query(None, description="Temporal filter."),
        filter: Optional[str] = Query(None, description="CQL2 Filter expression."),
        filter_lang: Optional[str] = _FILTER_LANG_QUERY,
        filter_crs: Optional[str] = _FILTER_CRS_QUERY,
        subset: Optional[str] = Query(None),
        simplification: Optional[float] = Query(None),
        simplification_by_zoom: Optional[str] = Query(None),
        simplification_algorithm: SimplificationAlgorithm = Query(
            SimplificationAlgorithm.TOPOLOGY_PRESERVING
        ),
        disable_cache: bool = Query(
            False, description="Disable cache for this request."
        ),
        refresh_cache: bool = Query(
            False, description="Refresh cache by invalidating before fetching."
        ),
        serve: Optional[Literal["proxy", "redirect"]] = Query(
            None,
            description=(
                "Per-request cache-hit delivery override: 'proxy' streams tile "
                "bytes (no redirect, for clients like QGIS that don't follow "
                "redirects); 'redirect' issues a 307 to a signed bucket URL. "
                "Omit for the platform default."
            ),
        ),
        # Accepted for uniform protocol consistency; MVT is generated by PostGIS
        # and does not pass through the hints routing layer.
        request_hints: FrozenSet = Depends(parse_hints_param),
    ):
        """Catalog-centric endpoint with full TMS support."""
        catalogs_svc = await self._get_catalogs_service()
        internal_dataset = await resolve_internal_catalog_id_or_404(catalogs_svc, dataset)
        return await self.get_vector_tile(
            request=request,
            dataset=internal_dataset,
            tileMatrixSetId=tileMatrixSetId,
            z=z,
            x=x,
            y=y,
            format=format,
            background_tasks=background_tasks,
            collections=collections,
            datetime=datetime,
            filter=filter,
            filter_lang=filter_lang,
            filter_crs=filter_crs,
            subset=subset,
            simplification=simplification,
            simplification_by_zoom=simplification_by_zoom,
            simplification_algorithm=simplification_algorithm,
            disable_cache=disable_cache,
            refresh_cache=refresh_cache,
            serve=serve,
        )

    async def get_vector_tile_default(
        self,
        dataset: str,
        z: int,
        x: int,
        y: int,
        request: Request,
        background_tasks: BackgroundTasks,
        collections: str = Query(..., description="Comma-separated collection IDs."),
        datetime: Optional[str] = Query(None),
        filter: Optional[str] = Query(None, description="CQL2 Filter expression."),
        filter_lang: Optional[str] = _FILTER_LANG_QUERY,
        filter_crs: Optional[str] = _FILTER_CRS_QUERY,
        subset: Optional[str] = Query(None),
        simplification: Optional[float] = Query(None),
        simplification_by_zoom: Optional[str] = Query(None),
        simplification_algorithm: SimplificationAlgorithm = Query(
            SimplificationAlgorithm.TOPOLOGY_PRESERVING
        ),
        disable_cache: bool = Query(
            False, description="Disable cache for this request."
        ),
        refresh_cache: bool = Query(
            False, description="Refresh cache by invalidating before fetching."
        ),
        serve: Optional[Literal["proxy", "redirect"]] = Query(
            None,
            description=(
                "Per-request cache-hit delivery override: 'proxy' streams tile "
                "bytes (no redirect, for clients like QGIS that don't follow "
                "redirects); 'redirect' issues a 307 to a signed bucket URL. "
                "Omit for the platform default."
            ),
        ),
        # Accepted for uniform protocol consistency; MVT is generated by PostGIS
        # and does not pass through the hints routing layer.
        request_hints: FrozenSet = Depends(parse_hints_param),
    ):
        """Defaults to WebMercatorQuad."""
        catalogs_svc = await self._get_catalogs_service()
        internal_dataset = await resolve_internal_catalog_id_or_404(catalogs_svc, dataset)
        return await self.get_vector_tile(
            request=request,
            dataset=internal_dataset,
            tileMatrixSetId="WebMercatorQuad",
            z=z,
            x=x,
            y=y,
            format="mvt",
            background_tasks=background_tasks,
            collections=collections,
            datetime=datetime,
            filter=filter,
            filter_lang=filter_lang,
            filter_crs=filter_crs,
            subset=subset,
            simplification=simplification,
            simplification_by_zoom=simplification_by_zoom,
            simplification_algorithm=simplification_algorithm,
            disable_cache=disable_cache,
            refresh_cache=refresh_cache,
            serve=serve,
        )

    async def get_vector_tile(
        self,
        request: Request,
        dataset: str,
        tileMatrixSetId: str,
        z: int,
        x: int,
        y: int,
        format: str,
        background_tasks: BackgroundTasks,
        collections: str = Query(
            ..., description="Comma-separated list of collection IDs to include."
        ),
        datetime: Optional[str] = Query(None, description="Temporal filter."),
        filter: Optional[str] = Query(None, description="CQL2 Filter expression."),
        filter_lang: Optional[str] = _FILTER_LANG_QUERY,
        filter_crs: Optional[str] = _FILTER_CRS_QUERY,
        subset: Optional[str] = Query(None),
        simplification: Optional[float] = Query(None),
        simplification_by_zoom: Optional[str] = Query(None),
        simplification_algorithm: SimplificationAlgorithm = Query(
            SimplificationAlgorithm.TOPOLOGY_PRESERVING
        ),
        disable_cache: bool = Query(
            False, description="Disable cache for this request."
        ),
        refresh_cache: bool = Query(
            False, description="Refresh cache by invalidating before fetching."
        ),
        serve: Optional[Literal["proxy", "redirect"]] = Query(
            None,
            description=(
                "Per-request override of how a cache HIT is delivered. "
                "'proxy' streams the tile bytes through the API (no redirect) — "
                "use for clients that do not follow redirects to signed bucket "
                "URLs, e.g. QGIS. 'redirect' issues a 307 to a signed GCS URL "
                "(offloads egress to the bucket). Omit to use the platform "
                "default (tiles_caching_config.cache_serve_mode)."
            ),
        ),
        # Accepted for uniform protocol consistency; MVT is generated by PostGIS
        # and does not pass through the hints routing layer.
        request_hints: FrozenSet = Depends(parse_hints_param),
    ):
        """
        Generates an MVT tile dynamically reprojected to the requested TileMatrixSet.
        Checks for pre-seeded tiles first.
        """
        start_time = time.perf_counter()
        _conn: Optional[AsyncConnection] = None
        # True while THIS coroutine owns the render-admission slot acquired
        # below. Flipped to False the moment ownership moves to render_task
        # (its own wrapper releases the slot when the render itself finishes,
        # even if this request coroutine is later cancelled by a client
        # disconnect and the render keeps running shielded) — mirrors how
        # `_conn` is set to None on the same handoff a few lines down.
        _render_admitted = False

        try:
            # 1. Validation & Configuration
            if format not in ["mvt", "pbf"]:
                raise HTTPException(
                    status_code=400, detail=f"Format '{format}' not supported."
                )

            config_manager = get_protocol(ConfigsProtocol)
            if not config_manager:
                raise HTTPException(
                    status_code=500, detail="Configuration service unavailable."
                )
            requested_cols_list = [c.strip() for c in collections.split(",")]

            # Enforce collection-visibility before any catalog/tile resolution.
            # Each collection in the comma-separated list is checked
            # independently; a hidden collection is indistinguishable from a
            # missing one (404), matching CatalogService.get_collection.
            for _coll_id in requested_cols_list:
                await self._require_collection_visible(dataset, _coll_id)

            tiles_config = await self._resolve_request_config(
                config_manager, dataset
            )

            query_params = getattr(request, "query_params", {}) or {}

            def _query_value(name: str) -> Optional[str]:
                getter = getattr(query_params, "get", None)
                if getter is None:
                    return None
                value = getter(name)
                return value if isinstance(value, str) else None

            if not isinstance(filter_lang, str) or not filter_lang:
                filter_lang = "cql2-text"
            if not isinstance(filter_crs, str):
                filter_crs = None
            raw_filter_lang = _query_value("filter-lang") or _query_value("filter_lang")
            if raw_filter_lang and filter_lang == "cql2-text":
                filter_lang = raw_filter_lang
            filter_lang = validate_filter_lang(filter_lang)

            raw_filter_crs = _query_value("filter-crs") or _query_value("filter_crs")
            if raw_filter_crs and filter_crs is None:
                filter_crs = raw_filter_crs

            # Render wall-clock budget + client-disconnect check (#2898).
            # Checked only at the render-phase loop boundaries below (never
            # per-feature) — the cache lookup/redirect path above never calls
            # this, so it stays unaffected. ``render_deadline`` is measured
            # from ``start_time`` (function entry, same baseline as every
            # ``duration_ms`` log in this handler) so the budget bounds the
            # whole request, not just the render phase in isolation.
            render_deadline = start_time + tiles_config.render_budget_seconds

            async def _should_abort() -> Optional[str]:
                if time.perf_counter() >= render_deadline:
                    return "budget"
                if await request.is_disconnected():
                    return "disconnected"
                return None

            # 2. Storage & Cache Resolution
            cache_enabled = await self._is_cache_enabled(
                config_manager, dataset, requested_cols_list, tiles_config
            )

            # 3. Cache Key & Pre-seed Check
            params_hash = self._generate_params_hash(
                collections,
                datetime,
                filter,
                filter_lang,
                filter_crs,
                subset,
                simplification,
                simplification_by_zoom,
                simplification_algorithm,
            )

            # Handle cache control flags
            effective_cache_enabled = cache_enabled and not disable_cache

            if effective_cache_enabled:
                cache_id = (
                    collections
                    if len(requested_cols_list) > 1
                    else requested_cols_list[0]
                )
                effective_cache_id = (
                    f"{cache_id}@{params_hash}" if params_hash else cache_id
                )

                provider = get_protocol(TileStorageProtocol)
                if provider:
                    # If refresh_cache is True, invalidate the tile first
                    if refresh_cache:
                        try:
                            await provider.delete_tile(  # type: ignore[attr-defined]
                                dataset,
                                effective_cache_id,
                                tileMatrixSetId,
                                z,
                                x,
                                y,
                                format,
                            )
                            logger.info(
                                f"Tile cache REFRESHED: {dataset}/{collections}/{z}/{x}/{y}"
                            )
                        except Exception as e:
                            logger.warning(f"Failed to refresh tile cache: {e}")
                    else:
                        # Attempt redirect or proxy
                        caching_cfg = await config_manager.get_config(
                            TilesCachingConfig
                        )
                        serve_mode = serve or (
                            caching_cfg.cache_serve_mode
                            if isinstance(caching_cfg, TilesCachingConfig)
                            else "redirect"
                        )
                        res = await self._try_cached_tile(
                            provider,
                            dataset,
                            effective_cache_id,
                            tileMatrixSetId,
                            z,
                            x,
                            y,
                            format,
                            start_time,
                            serve_mode=serve_mode,
                        )
                        if res:
                            return res

            logger.info(
                "tile_cache event=miss catalog=%s collection=%s z=%s x=%s y=%s "
                "cache_enabled=%s",
                dataset, collections, z, x, y, effective_cache_enabled,
            )

            # 4. TMS & Coordinate Validation (HTTP-specific z/x/y bounds check;
            # stays here — tiles_engine.build_render_context re-resolves the
            # same TMS internally for the SRID/source-selection it owns).
            # Runs before any DB connection is acquired — it's a cheap,
            # cached lookup, not something worth holding pool capacity for.
            await self._validate_tms_and_matrix(dataset, tileMatrixSetId, z, x, y)

            # 5. Resolve the render context: collection metadata, TMS, target
            # SRID, and TileSource — consolidated with the preseed task's
            # identical resolution in tiles_engine.build_render_context.
            #
            # Resolved BEFORE the main render connection is acquired (#3014):
            # the collection-metadata lookup this triggers
            # (tiles_module.get_tile_resolution_params) always opens its own
            # separate pooled connection regardless of what's passed as
            # ``engine`` here, so running it while a render connection was
            # already checked out meant every in-flight request could need
            # two connections from the pool at once. Under a burst wide
            # enough to approach the pool size, every request ends up
            # holding its first connection while blocked on the second,
            # deadlocking the pool. Passing the bare engine (not a checked-
            # out connection) for the optional custom-CRS SRID resolution
            # below is the same pattern already used by the preseed and
            # export tasks, which never hold a render connection at all.
            from dynastore.modules.tiles import tiles_engine
            from dynastore.modules.tiles.tiles_source import TileSourceNotSupported

            try:
                ctx = await tiles_engine.build_render_context(
                    dataset, requested_cols_list, tileMatrixSetId,
                    engine=get_async_engine(request), should_abort=_should_abort,
                )
            except TileSourceNotSupported as exc:
                logger.error("get_vector_tile: %s", exc)
                raise HTTPException(status_code=500, detail=str(exc)) from exc
            except tiles_engine.RenderAborted as exc:
                return self._handle_render_aborted(
                    exc.reason, dataset, collections, z, x, y, start_time,
                )

            if ctx is None:
                logger.warning(
                    "No valid collections found for %s/%s "
                    "(requested=%d) — see tile metadata warnings above for "
                    "the underlying driver/routing failure",
                    dataset, collections, len(requested_cols_list),
                )
                return self._finalize_response(request, b"")

            # L2 cache miss — try PMTiles archive fallback before hitting PostGIS
            if effective_cache_enabled and not disable_cache:
                archive_storage = get_protocol(TileArchiveStorageProtocol)
                if archive_storage:
                    single_col = (
                        requested_cols_list[0] if len(requested_cols_list) == 1 else None
                    )
                    if single_col and await archive_storage.archive_exists(
                        dataset, single_col, tileMatrixSetId
                    ):
                        tile_bytes = await archive_storage.get_tile_from_archive(
                            dataset, single_col, tileMatrixSetId, z, x, y
                        )
                        if tile_bytes is not None:
                            duration_ms = (time.perf_counter() - start_time) * 1000
                            logger.info(
                                "tile_cache event=hit source=pmtiles_archive catalog=%s "
                                "collection=%s z=%s x=%s y=%s duration_ms=%.2f bytes=%d",
                                dataset, single_col, z, x, y, duration_ms, len(tile_bytes),
                            )
                            from fastapi.responses import Response as FResponse
                            return FResponse(
                                content=tile_bytes,
                                media_type="application/vnd.mapbox-vector-tile",
                                headers={
                                    "X-Tile-Cache": "hit",
                                    "X-Tile-Source": "pmtiles_archive",
                                },
                            )

            # Render-budget/disconnect boundary (#2898) ahead of the actual
            # PostGIS statement — the last point where aborting still saves
            # real work (the render itself is a single query, so there is no
            # further loop boundary inside it to check).
            _abort_reason = await _should_abort()
            if _abort_reason:
                return self._handle_render_aborted(
                    _abort_reason, dataset, collections, z, x, y, start_time,
                )

            # Render admission gate (#3155): cap concurrent renders on this
            # worker against its memory budget, queueing briefly then
            # shedding rather than letting an unbounded burst race the
            # worker's RSS budget — the failure mode that OOM-killed maps
            # under a QGIS multi-tile viewport refresh. Resolved BEFORE the
            # DB connection below so a render queued on the gate never holds
            # a pool connection idle while it waits.
            try:
                await self._render_gate.acquire()
            except RenderAdmissionRejected as exc:
                logger.warning(
                    "get_vector_tile: render admission rejected (%s) "
                    "catalog=%s z=%s x=%s y=%s — attempting stale tile fallback",
                    exc.reason, dataset, z, x, y,
                )
                stale = await self._try_stale_tile_fallback(
                    effective_cache_enabled, dataset, requested_cols_list,
                    collections, params_hash, tileMatrixSetId, z, x, y,
                    format, start_time,
                )
                if stale:
                    return stale
                raise HTTPException(
                    status_code=503,
                    detail="Render capacity exhausted on this worker, try again shortly.",
                    headers={"Retry-After": str(exc.retry_after_seconds)},
                ) from exc
            _render_admitted = True

            # Acquire DB connection with pool-saturation guard.
            # On timeout: try serving a stale cached tile before failing fast.
            # acquire_engine_connection_bounded gives this a live-configurable
            # deadline shorter than the engine's own pool_timeout, with the
            # same pool hygiene (poisoned-slot eviction, rollback-on-checkout)
            # every other managed_transaction consumer gets (#2933).
            #
            # Acquired last, once metadata resolution above is done (#3014) —
            # this is the only connection a render ever needs to hold now, so
            # a burst of concurrent requests can never deadlock the pool by
            # each holding one connection while blocked on a second.
            fg_timeout_s = await _read_live_fg_acquire_timeout()
            try:
                _conn = await acquire_engine_connection_bounded(
                    get_async_engine(request), fg_timeout_s
                )
            except PoolSaturationError as exc:
                # The render never started — release the admission slot here
                # rather than leaving it to the top-level `finally` (the same
                # ownership discipline as `_conn` above: whoever fails to
                # hand a resource off to the render task must release it).
                self._render_gate.release()
                _render_admitted = False
                logger.warning(
                    "get_vector_tile: DB pool saturated (timeout=%.1fs) "
                    "catalog=%s z=%s x=%s y=%s — attempting stale tile fallback",
                    fg_timeout_s, dataset, z, x, y,
                )
                stale = await self._try_stale_tile_fallback(
                    effective_cache_enabled, dataset, requested_cols_list,
                    collections, params_hash, tileMatrixSetId, z, x, y,
                    format, start_time,
                )
                if stale:
                    return stale
                raise HTTPException(
                    status_code=503,
                    detail="Database pool saturated, try again shortly.",
                    headers={"Retry-After": str(exc.retry_after)},
                ) from None
            conn = _conn
            filter_crs_srid = await self._resolve_crs_srid(conn, dataset, filter_crs)
            if filter_crs and filter_crs_srid is None:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unsupported filter-crs '{filter_crs}'.",
                )

            # Retrieve MVT content via the unified render engine — L1 cache
            # enabled (the live request-serving path); the preseed task
            # renders each tile exactly once and passes use_l1_cache=False.
            #
            # Bound the render query with a per-request SET LOCAL
            # statement_timeout (#2813) — mirrors the preseed task's
            # per-zoom-transaction timeout. On PostGIS canceling the
            # statement (pgcode 57014), fall back to a stale cached tile or
            # a 503 (below) instead of surfacing a 500 or a false 204 (#2965);
            # any other query failure still propagates to the generic error
            # handler.
            statement_timeout_ms = int(tiles_config.live_tile_timeout_seconds * 1000)
            render_task: Optional["asyncio.Task[Optional[bytes]]"] = None
            try:
                await DQLQuery(
                    f"SET LOCAL statement_timeout = {statement_timeout_ms}",
                    result_handler=ResultHandler.NONE,
                ).execute(conn)
                # Render as a standalone task, awaited behind `shield` (#2898):
                # a client disconnect cancels this coroutine, but `shield`
                # leaves `render_task` running rather than cancelling it too,
                # so a heavy render already under way isn't discarded. The
                # `SET LOCAL statement_timeout` just above still bounds how
                # long it can run either way.
                render_task = asyncio.ensure_future(
                    self._render_and_release_gate(
                        tiles_engine.render_tile(
                            conn,
                            ctx,
                            str(z),
                            x,
                            y,
                            format=format,
                            use_l1_cache=True,
                            datetime_str=datetime,
                            cql_filter=filter,
                            filter_lang=filter_lang,
                            filter_crs_srid=filter_crs_srid,
                            subset_params=subset,  # type: ignore[arg-type]
                            simplification=simplification,
                            simplification_algorithm=simplification_algorithm,
                        )
                    )
                )
                # Ownership of the admission slot moves to render_task's own
                # wrapper from here — it releases when the render itself
                # finishes, not when this coroutine does (see the
                # CancelledError branch below, which shields render_task past
                # a client disconnect).
                _render_admitted = False
                mvt_content = await asyncio.shield(render_task)
            except (QueryExecutionError, DatabaseConnectionError) as exc:
                pgcode = getattr(exc.original_exception, "pgcode", None)
                elapsed_s = time.perf_counter() - start_time
                cancel_race = _is_timeout_cancel_race(
                    exc, elapsed_s, tiles_config.live_tile_timeout_seconds,
                )
                if pgcode != _QUERY_CANCELED_PGCODE and not cancel_race:
                    raise
                # A cancelled statement means this render never learned
                # whether the tile has data or not — reporting 204 here would
                # claim "confirmed empty" (/req/core/tc-error part B) for a
                # tile that may well be full. Fall back to a stale cached
                # tile (200, honest even if outdated), else tell the client
                # to retry (503) rather than render a false hole (#2965).
                #
                # ``cancel_race`` covers the second cancellation shape
                # (#3181): asyncpg's own cancel handling losing a race
                # against another wire operation raises ``InterfaceError``
                # (no pgcode) instead of the clean pgcode-57014 cancel, and
                # the DB layer's transient-asyncpg classifier (#235/#239)
                # surfaces that as ``DatabaseConnectionError`` rather than
                # ``QueryExecutionError`` — same unknown-content situation
                # as the pgcode case, so it gets the same fallback here
                # instead of falling through to a raw 500.
                trigger = "pgcode" if pgcode == _QUERY_CANCELED_PGCODE else "cancel-race"
                logger.warning(
                    "get_vector_tile: statement timeout (trigger=%s, pgcode=%s, "
                    "live_tile_timeout_seconds=%s) catalog=%s collection=%s "
                    "z=%s x=%s y=%s — attempting stale tile fallback",
                    trigger, pgcode, tiles_config.live_tile_timeout_seconds,
                    dataset, collections, z, x, y,
                )
                stale = await self._try_stale_tile_fallback(
                    effective_cache_enabled, dataset, requested_cols_list,
                    collections, params_hash, tileMatrixSetId, z, x, y,
                    format, start_time,
                )
                if stale:
                    return stale
                raise HTTPException(
                    status_code=503,
                    detail="Tile render timed out, try again shortly.",
                    headers={"Retry-After": "5"},
                ) from None
            except ValueError as exc:
                if str(exc).startswith("Invalid CQL filter"):
                    raise HTTPException(status_code=400, detail=str(exc)) from exc
                raise
            except asyncio.CancelledError:
                if render_task is not None and not render_task.done():
                    # The client already gave up, so there is no response
                    # left to send — only the cache write-back and `conn`
                    # (still in use by the render) are left to finish.
                    # Ownership of `conn` moves to the done-callback below;
                    # the handler's own `finally` must not close it out from
                    # under the still-running render.
                    cache_id = (
                        collections
                        if len(requested_cols_list) > 1
                        else requested_cols_list[0]
                    )
                    effective_cache_id = (
                        f"{cache_id}@{params_hash}" if params_hash else cache_id
                    )
                    provider = (
                        get_protocol(TileStorageProtocol)
                        if cache_enabled and not disable_cache
                        else None
                    )
                    render_task.add_done_callback(
                        self._make_shielded_render_callback(
                            conn, provider, dataset, effective_cache_id,
                            tileMatrixSetId, z, x, y, format,
                        )
                    )
                    _conn = None
                raise

            # 9. Background Caching
            # `mvt_content is not None` (not truthy) so a confirmed-empty
            # render (`b""` — zero features, distinct from the `None` a
            # failed/aborted render leaves above) is persisted too; otherwise
            # every empty tile re-renders from PostGIS on every request.
            effective_cache_enabled = cache_enabled and not disable_cache
            if mvt_content is not None and effective_cache_enabled:
                cache_id = (
                    collections
                    if len(requested_cols_list) > 1
                    else requested_cols_list[0]
                )
                effective_cache_id = (
                    f"{cache_id}@{params_hash}" if params_hash else cache_id
                )
                provider = get_protocol(TileStorageProtocol)
                if provider and self._tile_cache_writer is not None:
                    self._tile_cache_writer.submit_nowait(
                        provider,
                        dataset,
                        effective_cache_id,
                        tileMatrixSetId,
                        z,
                        x,
                        y,
                        mvt_content,
                        format,
                    )

            if not mvt_content:
                duration_ms = (time.perf_counter() - start_time) * 1000
                logger.info(
                    "tile_cache event=miss source=postgis catalog=%s collection=%s "
                    "z=%s x=%s y=%s duration_ms=%.2f bytes=0",
                    dataset, collections, z, x, y, duration_ms,
                )
                return Response(
                    status_code=204,
                    headers={"X-Tile-Cache": "miss", "X-Tile-Source": "postgis"},
                )

            duration_ms = (time.perf_counter() - start_time) * 1000
            logger.info(
                "tile_cache event=miss source=postgis catalog=%s collection=%s "
                "z=%s x=%s y=%s duration_ms=%.2f bytes=%d",
                dataset, collections, z, x, y, duration_ms, len(mvt_content),
            )

            response = self._finalize_response(request, mvt_content)
            response.headers["X-Tile-Cache"] = "miss"
            response.headers["X-Tile-Source"] = "postgis"
            return response

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"CRITICAL ERROR in get_vector_tile: {e}", exc_info=True)
            raise HTTPException(
                status_code=500, detail=f"Internal Server Error: {str(e)}"
            ) from e
        finally:
            if _conn is not None:
                await _conn.close()
            if _render_admitted:
                self._render_gate.release()

    # --- Helper Private Methods ---
    #
    # MVT generation itself (with its L1 in-process cache) moved to
    # dynastore.modules.tiles.tiles_engine.render_tile — shared with the
    # preseed task rather than duplicated here.

    async def _render_and_release_gate(
        self, render_coro: Awaitable[Optional[bytes]]
    ) -> Optional[bytes]:
        """Await ``render_coro``, releasing the render-admission slot exactly
        when the render itself finishes — success, failure, or eventual
        cancellation — regardless of the awaiting request coroutine's own
        lifetime (#3155). Pairs with the ``self._render_gate.acquire()`` in
        ``get_vector_tile``, called right before the wrapped task is created;
        see the ownership-handoff comment there.
        """
        try:
            return await render_coro
        finally:
            self._render_gate.release()

    @staticmethod
    def _handle_render_aborted(
        reason: str,
        dataset: str,
        collections: str,
        z: int,
        x: int,
        y: int,
        start_time: float,
    ) -> Response:
        """Handle a render aborted via ``ShouldAbort`` (#2898).

        ``reason="budget"``: the render exceeded ``TilesConfig.render_budget_seconds``
        — logged once at WARNING and mirrors the pool-saturation fail-fast
        (503 + ``Retry-After``) so a stacking client sees the same backoff
        signal. ``reason="disconnected"``: the client already gave up (LB
        timeout/retry) — logged at INFO since it's an expected outcome, not a
        failure, and the render stops quietly.
        """
        elapsed_s = time.perf_counter() - start_time
        if reason == "budget":
            logger.warning(
                "get_vector_tile: render budget exceeded elapsed=%.1fs "
                "catalog=%s collections=%s z=%s x=%s y=%s — aborting render",
                elapsed_s, dataset, collections, z, x, y,
            )
            raise HTTPException(
                status_code=503,
                detail="Tile render exceeded its time budget, try again shortly.",
                headers={"Retry-After": "5"},
            )
        logger.info(
            "get_vector_tile: client disconnected elapsed=%.1fs "
            "catalog=%s collections=%s z=%s x=%s y=%s — aborting render",
            elapsed_s, dataset, collections, z, x, y,
        )
        return Response(status_code=499)

    @staticmethod
    def _make_shielded_render_callback(
        conn: AsyncConnection,
        provider: Optional[TileStorageProtocol],
        dataset: str,
        effective_cache_id: str,
        tms_id: str,
        z: int,
        x: int,
        y: int,
        format: str,
    ):
        """Build the ``add_done_callback`` for a render shielded past a
        client disconnect (#2898).

        ``add_done_callback`` requires a plain (non-async) callable, so this
        returns one that schedules the actual persistence coroutine —
        ``_persist_shielded_render`` — as a detached task.
        """

        def _on_done(task: "asyncio.Task[Optional[bytes]]") -> None:
            asyncio.ensure_future(
                TilesService._persist_shielded_render(
                    task, conn, provider, dataset, effective_cache_id,
                    tms_id, z, x, y, format,
                )
            )

        return _on_done

    @staticmethod
    async def _persist_shielded_render(
        render_task: "asyncio.Task[Optional[bytes]]",
        conn: AsyncConnection,
        provider: Optional[TileStorageProtocol],
        dataset: str,
        effective_cache_id: str,
        tms_id: str,
        z: int,
        x: int,
        y: int,
        format: str,
    ) -> None:
        """Persist a render that outlived the (already-cancelled) request
        that started it, then close the connection handed off to it.

        The client is long gone by the time this runs, so there is nothing
        left to serve — only the cache write-back (skipped on render failure
        or when caching is disabled) and closing ``conn``.
        """
        try:
            if render_task.cancelled():
                return
            exc = render_task.exception()
            if exc is not None:
                logger.warning(
                    "get_vector_tile: shielded render failed after disconnect "
                    "catalog=%s collection=%s z=%s x=%s y=%s: %s",
                    dataset, effective_cache_id, z, x, y, exc,
                )
                return
            result = render_task.result()
            if result is not None and provider is not None:
                try:
                    await provider.save_tile(
                        dataset, effective_cache_id, tms_id, z, x, y, result, format,
                    )
                except Exception as exc:
                    logger.warning(
                        "get_vector_tile: post-disconnect save_tile failed "
                        "catalog=%s collection=%s z=%s x=%s y=%s: %s",
                        dataset, effective_cache_id, z, x, y, exc,
                    )
        finally:
            await conn.close()

    @staticmethod
    async def _resolve_request_config(
        config_manager, dataset: str
    ) -> TilesConfig:
        config = await config_manager.get_config(TilesConfig, dataset)
        if isinstance(config, TilesConfig) and not config.enabled:
            raise HTTPException(
                status_code=404, detail="Tiles are disabled for this catalog."
            )
        return config

    @staticmethod
    async def _is_cache_enabled(
        config_manager,
        dataset: str,
        collections: List[str],
        catalog_config: TilesConfig,
    ) -> bool:
        # config_manager is unused here: catalog_config is already loaded and
        # cache_on_demand_enabled resolves its own protocol for the (at most
        # one) collection config fetch below.
        collection_id = collections[0] if len(collections) == 1 else None
        return await cache_on_demand_enabled(
            dataset, collection_id, catalog_config=catalog_config
        )

    @staticmethod
    def _generate_params_hash(*args) -> Optional[str]:
        # Canonical tiles have no extra params
        if not any(args[1:]):
            return None
        params_str = "|".join(str(a) for a in args)
        return hashlib.sha256(params_str.encode()).hexdigest()[:16]

    async def _try_stale_tile_fallback(
        self,
        effective_cache_enabled: bool,
        dataset: str,
        requested_cols_list: List[str],
        collections: str,
        params_hash: Optional[str],
        tileMatrixSetId: str,
        z: int,
        x: int,
        y: int,
        format: str,
        start_time: float,
    ) -> Optional[Response]:
        """Look up a cached/stale tile for a request that could not complete
        a fresh render — the shared ladder for DB-pool saturation (#2845) and
        a render cancelled by ``statement_timeout`` (pgcode 57014, #2965).

        Returns the cached response (redirect, proxied bytes, or a
        confirmed-empty 204 from a *previous, completed* render) if one
        exists, or ``None`` when there is nothing cached — the caller must
        then fail with an honest 503 + Retry-After rather than fabricate a
        204 for a render that never actually confirmed the tile was empty.
        """
        if not effective_cache_enabled:
            return None
        provider = get_protocol(TileStorageProtocol)
        if not provider:
            return None
        cache_id = (
            collections if len(requested_cols_list) > 1 else requested_cols_list[0]
        )
        effective_cache_id = f"{cache_id}@{params_hash}" if params_hash else cache_id
        return await self._try_cached_tile(
            provider, dataset, effective_cache_id, tileMatrixSetId,
            z, x, y, format, start_time, serve_mode="proxy",
        )

    @staticmethod
    async def _try_cached_tile(
        provider,
        dataset,
        cache_id,
        tms_id,
        z,
        x,
        y,
        format,
        start_time,
        serve_mode: Literal["proxy", "redirect"] = "redirect",
    ):
        """Return a cached-tile response or None (cache miss → caller renders).

        ``serve_mode="redirect"`` (default): resolve a short-lived signed URL
        and issue a 307 so the client pulls bytes directly from GCS; falls back
        to proxy if signing raises (logged as WARNING).

        ``serve_mode="proxy"``: stream bytes through this process — no signing
        call, lower concurrency ceiling, use when signing credentials are absent
        or a CDN handles redirects upstream.
        """
        # --- Redirect mode: try signed URL, fall back to proxy on failure ---
        if serve_mode == "redirect":
            url: Optional[str] = None
            try:
                url = await provider.get_tile_url(
                    dataset, cache_id, tms_id, z, x, y, format
                )
            except Exception as exc:
                logger.warning(
                    "tile_cache: signed URL raised %s (serve_mode=redirect), "
                    "falling back to proxy — catalog=%s collection=%s: %s",
                    type(exc).__name__, dataset, cache_id, exc,
                )
            if url:
                duration_ms = (time.perf_counter() - start_time) * 1000
                logger.info(
                    "tile_cache event=hit source=bucket_redirect catalog=%s collection=%s "
                    "z=%s x=%s y=%s duration_ms=%.2f",
                    dataset, cache_id, z, x, y, duration_ms,
                )
                return RedirectResponse(
                    url=url,
                    status_code=307,
                    headers={
                        "X-Tile-Cache": "hit",
                        "X-Tile-Source": "bucket_redirect",
                    },
                )
            else:
                logger.debug(
                    "tile_cache: get_tile_url returned None (serve_mode=redirect) "
                    "catalog=%s collection=%s z=%s x=%s y=%s "
                    "— tile absent in cache or signing unavailable; trying proxy",
                    dataset, cache_id, z, x, y,
                )

        # --- Proxy path: serve_mode=="proxy" OR redirect fell through ---
        try:
            tile = await provider.get_tile(dataset, cache_id, tms_id, z, x, y, format)
        except Exception as exc:
            logger.warning("tile_cache: proxy lookup failed: %s", exc)
            return None
        if tile is not None:
            duration_ms = (time.perf_counter() - start_time) * 1000
            if serve_mode == "redirect":
                # Proxy returned bytes while redirect mode is active.
                # This means the tile IS in the bucket but get_tile_url returned
                # None — most likely because blob.exists() on the metadata API
                # returned False despite the object being readable via the
                # download API, or signing failed silently.
                # Action: confirm the SA has storage.objects.get on the bucket
                # for both metadata and download, and that signBlob is granted.
                logger.warning(
                    "tile_cache: serve_mode=redirect but proxy returned bytes "
                    "catalog=%s collection=%s z=%s x=%s y=%s "
                    "— redirect is misconfigured or SA lacks blob.exists() permission; "
                    "check roles/storage.objectViewer + roles/iam.serviceAccountTokenCreator "
                    "on the SA for the cache bucket",
                    dataset, cache_id, z, x, y,
                )
            logger.info(
                "tile_cache event=hit source=bucket_proxy catalog=%s collection=%s "
                "z=%s x=%s y=%s duration_ms=%.2f bytes=%d",
                dataset, cache_id, z, x, y, duration_ms, len(tile),
            )
            if not tile:
                # Confirmed-empty tile cached from a prior render — serve 204
                # without falling through to a fresh PostGIS render.
                return Response(
                    status_code=204,
                    headers={"X-Tile-Cache": "hit", "X-Tile-Source": "bucket_proxy"},
                )
            return Response(
                content=tile,
                media_type="application/vnd.mapbox-vector-tile",
                headers={
                    "X-Tile-Cache": "hit",
                    "X-Tile-Source": "bucket_proxy",
                },
            )
        return None

    @staticmethod
    async def _validate_tms_and_matrix(dataset, tms_id, z, x, y):
        # Built-ins are global and need no catalog lookup. Checking them first
        # also avoids false 404s when a route has already resolved a public
        # catalog id to its immutable id but the custom-TMS registry expects
        # public dataset ids.
        tms_def = BUILTIN_TILE_MATRIX_SETS.get(tms_id)
        if not tms_def:
            try:
                tms_def = await tms_manager.get_custom_tms(
                    catalog_id=dataset, tms_id=tms_id
                )
            except Exception as exc:
                logger.debug(
                    "tiles: custom TMS lookup failed for %s/%s: %s",
                    dataset, tms_id, exc,
                )
                tms_def = None
        if not tms_def:
            raise HTTPException(
                status_code=404, detail=f"TMS '{tms_id}' not supported."
            )

        matrix = next((m for m in tms_def.tileMatrices if m.id == str(z)), None)
        if not matrix:
            raise HTTPException(
                status_code=400, detail=f"Zoom {z} not defined in TMS {tms_id}."
            )

        if not (0 <= x < matrix.matrixWidth and 0 <= y < matrix.matrixHeight):
            raise HTTPException(
                status_code=400, detail=f"Tile out of bounds for TMS {tms_id} @ {z}."
            )
        return tms_def

    @staticmethod
    def _parse_simplification_rules(
        rules_str: Optional[str],
    ) -> Optional[Dict[int, float]]:
        if not rules_str:
            return None
        try:
            return {int(k): float(v) for k, v in json.loads(rules_str).items()}
        except Exception as e:
            raise HTTPException(
                status_code=400, detail=f"Invalid simplification_by_zoom format: {e}"
            ) from e

    @staticmethod
    def _finalize_response(request: Request, content: bytes) -> Response:
        web_service = get_protocol(WebModuleProtocol)
        if web_service:
            etag = web_service.generate_etag([content])
            if etag == request.headers.get("if-none-match"):
                return Response(status_code=304)
            response = Response(
                content=content, media_type="application/vnd.mapbox-vector-tile"
            )
            response.headers.update(web_service.get_cache_headers())
            response.headers["ETag"] = etag
            return response
        return Response(
            content=content, media_type="application/vnd.mapbox-vector-tile"
        )

    # ------------------------------------------------------------------
    # Map-tile helpers (raster COG → styled PNG/WebP via rio-tiler)
    # ------------------------------------------------------------------

    @staticmethod
    def _require_raster_engine() -> None:
        """Raise 422 when rio-tiler is not installed in this environment."""
        if _RENDER_COG_TILE is None:
            raise HTTPException(
                status_code=422,
                detail=(
                    "Raster map-tile rendering is unavailable in this deployment. "
                    "Install the tiles extension with raster extras "
                    "(rio-tiler, rasterio, lxml)."
                ),
            )

    @staticmethod
    def _validate_style_id(style_id: str) -> None:
        """Raise 400 when style_id contains characters unsafe for cache keys."""
        if not _STYLE_ID_RE.match(style_id):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Invalid style_id {style_id!r}: "
                    "only [A-Za-z0-9._-] are allowed."
                ),
            )

    @staticmethod
    def _parse_multiband_params(
        bands_str: Optional[str],
        expression: Optional[str],
        rescale_str: Optional[str],
    ) -> Tuple[Optional[List[int]], Optional[str], Optional[List[Tuple[float, float]]]]:
        """Parse and validate optional multiband query params."""
        bands: Optional[List[int]] = None
        rescale: Optional[List[Tuple[float, float]]] = None

        if bands_str:
            try:
                bands = [int(b.strip()) for b in bands_str.split(",") if b.strip()]
            except ValueError as exc:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid 'bands' parameter {bands_str!r}: expected comma-separated integers.",
                ) from exc
            if not bands:
                raise HTTPException(
                    status_code=400,
                    detail="'bands' parameter must contain at least one band index.",
                )

        if rescale_str:
            try:
                rescale = []
                for pair in rescale_str.split(";"):
                    pair = pair.strip()
                    if not pair:
                        continue
                    lo_s, hi_s = pair.split(",", 1)
                    rescale.append((float(lo_s.strip()), float(hi_s.strip())))
            except (ValueError, TypeError) as exc:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Invalid 'rescale' parameter {rescale_str!r}: "
                        "expected semicolon-separated 'min,max' pairs."
                    ),
                ) from exc

        return bands, expression, rescale

    @staticmethod
    def _style_url_from_item(item: dict, style_id: Optional[str]) -> Optional[str]:
        if _STYLE_URL_FROM_ITEM is None:
            return None
        try:
            return _STYLE_URL_FROM_ITEM(item, style_id)
        except Exception as exc:
            logger.debug("map_tile: style URL extraction failed: %s", exc)
            return None

    @staticmethod
    async def _colormap_from_style_url(
        style_url: Optional[str],
        *,
        catalog_id: str,
        collection_id: str,
        style_id: str,
        required: bool,
    ):
        if not style_url:
            return None
        if _FETCH_SLD_BODY is None or _PARSE_SLD_COLORMAP is None:
            if required:
                raise HTTPException(
                    status_code=422,
                    detail="Style URL rendering is unavailable: SLD parser is not installed.",
                )
            return None
        try:
            sld_body = await _FETCH_SLD_BODY(style_url)
            return _PARSE_SLD_COLORMAP(sld_body) or None
        except ValueError as exc:
            if required:
                raise HTTPException(
                    status_code=422,
                    detail=f"SLD colormap parse failed: {exc}",
                ) from exc
            logger.warning(
                "map_tile: style_url SLD parse failed for %s/%s style=%s url=%s: %s",
                catalog_id, collection_id, style_id, style_url, exc,
            )
            return None
        except Exception as exc:
            if required:
                raise HTTPException(
                    status_code=502,
                    detail=f"Style URL could not be fetched or parsed: {exc}",
                ) from exc
            logger.warning(
                "map_tile: style_url fetch failed for %s/%s style=%s url=%s: %s",
                catalog_id, collection_id, style_id, style_url, exc,
            )
            return None

    @staticmethod
    async def _get_style_record(
        request: Request,
        internal_catalog_id: str,
        internal_collection_id: str,
        style_id: str,
        *,
        external_catalog_id: str,
        external_collection_id: str,
    ):
        """Fetch a style row by internal ids on a real connection.

        ``StylesService.get_style`` is a FastAPI route handler: it expects
        EXTERNAL ids (it re-resolves them through the external-only
        ``resolve_catalog_id``) and a request-scoped ``conn`` dependency, so
        calling it programmatically with internal ids raises
        ``ValueError("Catalog '<internal id>' not found.")`` — surfacing the
        internal id on the wire as a 404. Mirror the maps extension's
        ``_get_style_to_render`` instead: query the styles db directly with
        the ids this service already resolved. Returns ``None`` when the
        style does not exist.
        """
        from dynastore.modules.styles import db as styles_db

        engine = get_async_engine(request)
        async with engine.connect() as conn:
            return await styles_db.get_style_by_id_and_collection(
                conn,
                internal_catalog_id,
                internal_collection_id,
                style_id,
                external_catalog_id=external_catalog_id,
                external_collection_id=external_collection_id,
            )

    @staticmethod
    async def _load_render_caching_config():  # type: ignore[return]
        """Fetch live RenderCachingConfig; fall back to defaults if unavailable."""
        if _RenderCachingConfig is None:
            return None
        from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
        mgr = get_protocol(PlatformConfigsProtocol)
        if mgr is None:
            return _RenderCachingConfig()
        try:
            cfg = await mgr.get_config(_RenderCachingConfig)
        except Exception as exc:
            logger.debug("RenderCachingConfig: get_config failed (%s); using defaults", exc)
            return _RenderCachingConfig()
        return cfg if isinstance(cfg, _RenderCachingConfig) else _RenderCachingConfig()

    @staticmethod
    async def _try_render_cache(
        provider: TileStorageProtocol,
        catalog_id: str,
        cache_key: str,
        tms_id: str,
        z: int,
        x: int,
        y: int,
        fmt: str,
        start: float,
        cfg,
        serve_mode: Literal["proxy", "redirect"] = "redirect",
    ) -> Optional[Response]:
        """Return a 307 redirect or proxy response on a cache hit, else None.

        ``serve_mode="proxy"`` skips signed-URL resolution and streams the
        cached bytes through this process — for clients that do not follow
        redirects to signed bucket URLs (e.g. QGIS).
        """
        try:
            if serve_mode != "proxy":
                url = await provider.get_tile_url(
                    catalog_id, cache_key, tms_id, z, x, y, fmt
                )
                if url:
                    duration_ms = (time.perf_counter() - start) * 1000
                    logger.info(
                        "map_tile: cache=hit source=bucket_redirect catalog=%s "
                        "cache_key=%s z=%s x=%s y=%s duration_ms=%.2f",
                        catalog_id, cache_key, z, x, y, duration_ms,
                    )
                    return RedirectResponse(
                        url=url,
                        status_code=307,
                        headers={
                            "X-Render-Cache": "hit",
                            "X-Render-Source": "bucket_redirect",
                            "Cache-Control": f"public, max-age={cfg.ttl_seconds}",
                        },
                    )

            tile = await provider.get_tile(
                catalog_id, cache_key, tms_id, z, x, y, fmt
            )
            if tile is not None:
                duration_ms = (time.perf_counter() - start) * 1000
                logger.info(
                    "map_tile: cache=hit source=bucket_proxy catalog=%s "
                    "cache_key=%s z=%s x=%s y=%s duration_ms=%.2f bytes=%d",
                    catalog_id, cache_key, z, x, y, duration_ms, len(tile),
                )
                if not tile:
                    return Response(
                        status_code=204,
                        headers={
                            "X-Render-Cache": "hit",
                            "X-Render-Source": "bucket_proxy",
                            "Cache-Control": f"public, max-age={cfg.ttl_seconds}",
                        },
                    )
                media_type = _FORMAT_MEDIA_TYPE.get(fmt, "application/octet-stream")
                return Response(
                    content=tile,
                    media_type=media_type,
                    headers={
                        "X-Render-Cache": "hit",
                        "X-Render-Source": "bucket_proxy",
                        "Cache-Control": f"public, max-age={cfg.ttl_seconds}",
                    },
                )
        except Exception as exc:
            logger.warning("map_tile: cache lookup failed: %s", exc)
        return None

    @staticmethod
    async def _render_raster_tile(
        tile_cache_writer: Optional[TileCacheWriter],
        renderer,
        renderer_args: tuple,
        renderer_kwargs: dict,
        *,
        catalog_id: str,
        collection_id: str,
        cache_key: str,
        tms_id: str,
        z: int,
        x: int,
        y: int,
        fmt_lower: str,
        render_source: str,
        log_tag: str,
        error_prefix: str,
        check_invalid_expression: bool,
        provider: Optional[TileStorageProtocol],
        cfg,
        render_gate: RenderAdmissionGate,
    ) -> Response:
        """Run a rio-tiler renderer off-thread and build its HTTP response.

        Shared by ``get_map_tile``'s default-style raster branch and all
        three ``get_map_tile_styled`` branches (styled, terrain-rgb,
        hillshade): each calls a different renderer with different args, but
        the exception translation (``InvalidExpression`` -> 422,
        ``TileOutsideBounds`` -> 204, anything else -> 500), the background
        cache write-back, and the response construction are identical.
        ``check_invalid_expression`` is False for terrain-rgb, whose renderer
        never raises that exception type.

        ``render_gate`` bounds concurrent renders on this worker (#3155) —
        acquired around the actual off-thread render call, released before
        this returns either way.
        """
        from dynastore.modules.concurrency import run_in_thread

        try:
            async with render_gate.admit():
                tile_bytes = await run_in_thread(renderer, *renderer_args, **renderer_kwargs)
        except RenderAdmissionRejected as exc:
            logger.warning(
                "map_tile: %s render admission rejected (%s) for %s/%s "
                "z=%s x=%s y=%s",
                log_tag, exc.reason, catalog_id, collection_id, z, x, y,
            )
            raise HTTPException(
                status_code=503,
                detail="Render capacity exhausted on this worker, try again shortly.",
                headers={"Retry-After": str(exc.retry_after_seconds)},
            ) from exc
        except Exception as exc:
            exc_type = type(exc).__name__
            if check_invalid_expression and exc_type == "InvalidExpression":
                raise HTTPException(
                    status_code=422,
                    detail=f"Invalid band expression: {exc}",
                ) from exc
            if exc_type == "TileOutsideBounds":
                return Response(status_code=204)
            logger.error(
                "map_tile: %s failed for %s/%s z=%s x=%s y=%s: %s",
                log_tag, catalog_id, collection_id, z, x, y, exc,
                exc_info=True,
            )
            raise HTTPException(
                status_code=500,
                detail=f"{error_prefix} failed: {exc}",
            ) from exc

        if provider and cfg and cfg.cache_enabled and tile_bytes and tile_cache_writer is not None:
            tile_cache_writer.submit_nowait(
                provider,
                catalog_id,
                cache_key,
                tms_id,
                z,
                x,
                y,
                tile_bytes,
                fmt_lower,
            )

        media_type = _FORMAT_MEDIA_TYPE[fmt_lower]
        return Response(
            content=tile_bytes,
            media_type=media_type,
            headers={
                "X-Render-Cache": "miss",
                "X-Render-Source": render_source,
                "Cache-Control": f"public, max-age={cfg.ttl_seconds if cfg else 3600}",
            },
        )

    async def _resolve_catalog_and_collection(
        self,
        catalog_id: str,
        collection_id: str,
    ) -> Tuple[str, str]:
        """Resolve external → internal catalog and collection IDs.

        Raises HTTPException(404) when either ID is not found. Delegates to
        the shared resolvers, which already split ValueError (not found)
        from AttributeError (test stub) — ValueError must not be swallowed.
        """
        catalogs_svc = await self._get_catalogs_service()
        internal_catalog_id = await resolve_internal_catalog_id_or_404(
            catalogs_svc, catalog_id
        )
        internal_collection_id = await resolve_internal_collection_id_or_404(
            catalogs_svc, internal_catalog_id, collection_id
        )
        return internal_catalog_id, internal_collection_id

    async def _get_raster_source_item(
        self,
        catalog_id: str,
        collection_id: str,
    ) -> Optional[dict]:
        """Return an item-like raster source from items or collection assets."""
        item = await self._get_first_item(catalog_id, collection_id)
        if item:
            return item

        try:
            catalogs_svc = await self._get_catalogs_service()
            collection = await catalogs_svc.get_collection(catalog_id, collection_id)
        except Exception:
            return None
        if not collection:
            return None
        data = (
            collection.model_dump(by_alias=True, exclude_none=True)
            if hasattr(collection, "model_dump")
            else dict(collection)
        )
        assets = data.get("assets") or data.get("item_assets") or {}
        links = data.get("links") or []
        if not assets and not links:
            return None
        return {"assets": assets, "links": links, "properties": data.get("properties") or {}}

    # ------------------------------------------------------------------
    # Map-tile route handlers
    # ------------------------------------------------------------------

    async def get_map_tile(
        self,
        request: Request,
        background_tasks: BackgroundTasks,
        catalog_id: str = Path(..., description="Public catalog ID (external_id)."),
        collection_id: str = Path(..., description="Public collection ID (external_id)."),
        tms_id: str = Path(..., description="Tile Matrix Set ID."),
        z: int = Path(..., ge=0, le=30, description="Zoom level."),
        x: int = Path(..., ge=0, description="Tile column."),
        y: int = Path(..., ge=0, description="Tile row."),
        format: str = Path(..., description="Output image format: 'png' or 'webp'."),
        bands: Optional[str] = Query(
            None,
            description="Comma-separated 1-based band indices for multiband rendering.",
        ),
        expression: Optional[str] = Query(
            None,
            description="Band-math expression evaluated by rio-tiler.",
        ),
        rescale: Optional[str] = Query(
            None,
            description="Per-band rescale ranges as semicolon-separated 'min,max' pairs.",
        ),
        style_url: Optional[str] = Query(
            None,
            alias="style-url",
            description="External SLD URL to apply to raster tile rendering.",
        ),
        style_url_compat: Optional[str] = Query(
            None,
            alias="style_url",
            include_in_schema=False,
            description="Compatibility spelling of 'style-url'.",
        ),
        serve: Optional[Literal["proxy", "redirect"]] = Query(
            None,
            description=(
                "Per-request cache-hit delivery override: 'proxy' streams the "
                "rendered tile bytes (no redirect, for clients like QGIS that "
                "don't follow redirects); 'redirect' (default) issues a 307 to a "
                "signed bucket URL."
            ),
        ),
    ) -> Response:
        """Render a map tile using the collection's default style.

        RASTER collections render from a COG asset via rio-tiler (flow below).
        VECTOR (and RECORDS) collections dispatch to ``_get_vector_map_tile``,
        which renders default-style PNG from PostGIS via the maps extension's
        ``MapsPngTileSource`` (registered into core's ``TileSourceProtocol``).

        Raster flow:
        1. Validate format; ensure rio-tiler is available.
        2. Resolve external catalog/collection IDs to internal IDs.
        3. Check bucket cache.
        4. Resolve the default style via binding, fetch SLD, parse colormap.
        5. Resolve the first COG asset href.
        6. Validate TMS/matrix (upgrades renders' WebMercatorQuad-only frozenset).
        7. Render via run_in_thread(render_cog_tile); write to cache in background.
        """
        from dynastore.modules.catalog.catalog_config import CollectionKind

        # isinstance guard: direct (non-FastAPI) callers leave the Query
        # sentinel in place of the compat param — adopt only a real string.
        if style_url is None and isinstance(style_url_compat, str):
            style_url = style_url_compat
        fmt_lower = format.lower()

        start = time.perf_counter()

        internal_catalog_id, internal_collection_id = await self._resolve_catalog_and_collection(
            catalog_id, collection_id
        )
        await self._require_collection_visible(internal_catalog_id, internal_collection_id)

        # Validate TMS before cache check (avoids a spurious cache lookup on bad TMS).
        # Built-ins are global; custom TMS lookup follows the public dataset id.
        await self._validate_tms_and_matrix(catalog_id, tms_id, z, x, y)

        kind = await self._collection_kind(internal_catalog_id, internal_collection_id)
        if kind != CollectionKind.RASTER:
            return await self._get_vector_map_tile(
                request,
                background_tasks,
                internal_catalog_id,
                internal_collection_id,
                tms_id,
                z,
                x,
                y,
                fmt_lower,
                start,
            )

        self._require_raster_engine()

        if fmt_lower not in _FORMAT_MEDIA_TYPE:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported format '{format}'. Use 'png' or 'webp'.",
            )
        output_format: Literal["PNG", "WEBP"] = "PNG" if fmt_lower == "png" else "WEBP"

        bands_parsed, expression_parsed, rescale_parsed = self._parse_multiband_params(
            bands, expression, rescale
        )

        # Resolve default style via binding — needs the first item's properties.
        item = await self._get_raster_source_item(internal_catalog_id, internal_collection_id)
        if not item:
            raise HTTPException(
                status_code=404,
                detail=f"Collection '{collection_id}' has no raster source item or asset.",
            )

        style_id = "default"
        try:
            from dynastore.modules.styles.binding_resolver import resolve_binding_style_id
            item_props = item.get("properties") or item
            resolved = await resolve_binding_style_id(
                internal_catalog_id, internal_collection_id, item_props
            )
            if resolved:
                style_id = resolved
        except Exception as exc:
            logger.debug(
                "map_tile: binding resolver failed for %s/%s: %s — using 'default'",
                internal_catalog_id, internal_collection_id, exc,
            )

        effective_style_url = style_url or self._style_url_from_item(item, style_id)
        cache_style_id = style_id
        if effective_style_url and _STYLE_URL_CACHE_ID is not None:
            cache_style_id = f"{style_id}-url-{_STYLE_URL_CACHE_ID(effective_style_url)}"

        cfg = await self._load_render_caching_config()
        params_hash = _BUILD_RENDER_PARAMS_HASH(  # type: ignore[misc]
            bands=bands_parsed,
            expression=expression_parsed,
            rescale=rescale_parsed,
        ) if _BUILD_RENDER_PARAMS_HASH else None

        cache_key = _BUILD_RENDER_CACHE_KEY(  # type: ignore[misc]
            cfg.key_prefix if cfg else "",  # cfg is non-None when _BUILD_RENDER_CACHE_KEY is set
            internal_collection_id,
            cache_style_id,
            tms_id,
            z,
            x,
            y,
            fmt_lower,
            params_hash=params_hash,
        ) if _BUILD_RENDER_CACHE_KEY else f"map/{internal_collection_id}/{style_id}/{tms_id}/{z}/{x}/{y}.{fmt_lower}"

        provider = get_protocol(TileStorageProtocol)
        if provider and cfg and cfg.cache_enabled:
            res = await self._try_render_cache(
                provider, internal_catalog_id, cache_key, tms_id, z, x, y, fmt_lower, start, cfg,
                serve_mode=serve or "redirect",
            )
            if res is not None:
                return res

        # Resolve style colormap
        colormap = None
        if effective_style_url:
            colormap = await self._colormap_from_style_url(
                effective_style_url,
                catalog_id=internal_catalog_id,
                collection_id=internal_collection_id,
                style_id=style_id,
                required=False,
            )
        from dynastore.models.protocols import StylesProtocol as _StylesProtocol
        styles_svc = get_protocol(_StylesProtocol)
        if colormap is None and styles_svc and style_id != "default":
            style_obj = await self._get_style_record(
                request,
                internal_catalog_id,
                internal_collection_id,
                style_id,
                external_catalog_id=catalog_id,
                external_collection_id=collection_id,
            )
            if style_obj and _EXTRACT_SLD_BODY:
                sld_body = _EXTRACT_SLD_BODY(style_obj)
                if sld_body and _PARSE_SLD_COLORMAP:
                    try:
                        colormap = _PARSE_SLD_COLORMAP(sld_body) or None
                    except ValueError as exc:
                        raise HTTPException(
                            status_code=422,
                            detail=f"SLD colormap parse failed: {exc}",
                        ) from exc

        from dynastore.extensions.ogc_base import ogc_asset_href
        cog_href = ogc_asset_href(
            item,
            error_detail=(
                f"No COG asset href found for collection '{collection_id}'. "
                "Ensure at least one item carries a 'data' or 'coverage' asset."
            ),
        )

        logger.info(
            "map_tile: cache=miss catalog=%s collection=%s style=%s tms=%s z=%s x=%s y=%s fmt=%s",
            internal_catalog_id, internal_collection_id, style_id, tms_id, z, x, y, fmt_lower,
        )
        assert _RENDER_COG_TILE is not None  # guaranteed by _require_raster_engine()
        return await self._render_raster_tile(
            self._tile_cache_writer,
            _RENDER_COG_TILE,
            (cog_href, z, x, y),
            dict(
                colormap=colormap,
                output_format=output_format,
                bands=bands_parsed,
                expression=expression_parsed,
                rescale=rescale_parsed,
            ),
            catalog_id=internal_catalog_id,
            collection_id=internal_collection_id,
            cache_key=cache_key,
            tms_id=tms_id,
            z=z,
            x=x,
            y=y,
            fmt_lower=fmt_lower,
            render_source="rio-tiler",
            log_tag="rio-tiler",
            error_prefix="Raster render",
            check_invalid_expression=True,
            provider=provider,
            cfg=cfg,
            render_gate=self._render_gate,
        )

    async def _get_vector_map_tile(
        self,
        request: Request,
        background_tasks: BackgroundTasks,
        internal_catalog_id: str,
        internal_collection_id: str,
        tms_id: str,
        z: int,
        x: int,
        y: int,
        fmt_lower: str,
        start: float,
    ) -> Response:
        """Default-style vector map tile (PNG), dispatched from ``get_map_tile``.

        Rendered by ``MapsPngTileSource`` (packages/extensions/maps), which
        registers into core's ``TileSourceProtocol`` registry for
        ``format="png"`` — this extension never imports the maps extension;
        the maps extension imports core and registers itself (DI).

        Cached under the plain ``internal_collection_id`` (no
        ``build_render_cache_key``, no style/params suffix) — the SAME
        cache-id the vector MVT lane uses — so the existing feature-write
        invalidation (which iterates ``SERVED_TILE_FORMATS`` per z/x/y) drops
        it automatically. Only the default style is supported here; a named
        style still 404s via ``get_map_tile_styled``.
        """
        if fmt_lower != "png":
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Format '{fmt_lower}' is not available for vector map tiles; "
                    "only 'png' (default style) is supported."
                ),
            )

        cache_id = internal_collection_id
        provider = get_protocol(TileStorageProtocol)
        cache_enabled = await cache_on_demand_enabled(internal_catalog_id, internal_collection_id)

        if provider and cache_enabled:
            cached = await provider.get_tile(
                internal_catalog_id, cache_id, tms_id, z, x, y, "png"
            )
            if cached is not None:
                duration_ms = (time.perf_counter() - start) * 1000
                logger.info(
                    "map_tile: cache=hit source=tile_storage catalog=%s cache_id=%s "
                    "z=%s x=%s y=%s duration_ms=%.2f bytes=%d",
                    internal_catalog_id, cache_id, z, x, y, duration_ms, len(cached),
                )
                if not cached:
                    return Response(
                        status_code=204,
                        headers={"X-Render-Cache": "hit", "X-Render-Source": "tile_storage"},
                    )
                return Response(
                    content=cached,
                    media_type="image/png",
                    headers={"X-Render-Cache": "hit", "X-Render-Source": "tile_storage"},
                )

        from dynastore.modules.tiles import tiles_engine
        from dynastore.modules.tiles.tiles_source import TileSourceNotSupported

        # Resolved BEFORE the render connection is acquired below (#3014,
        # same reorder applied to get_vector_tile in #3022): the collection-
        # metadata lookup this triggers (tiles_module.get_tile_resolution_params)
        # always opens its own separate pooled connection regardless of what's
        # passed as ``engine`` here, so running it while a render connection
        # was already checked out meant this handler could need two pool
        # connections at once. Passing the bare engine for the optional
        # custom-CRS SRID resolution matches the pattern already used by
        # get_vector_tile and the preseed/export tasks.
        engine = get_async_engine(request)
        try:
            ctx = await tiles_engine.build_render_context(
                internal_catalog_id,
                [internal_collection_id],
                tms_id,
                engine=engine,
                format="png",
            )
        except TileSourceNotSupported as exc:
            logger.error("map_tile: %s", exc)
            raise HTTPException(status_code=500, detail=str(exc)) from exc

        if ctx is None:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Collection '{internal_collection_id}' could not be resolved "
                    "for map-tile rendering."
                ),
            )

        # Render admission gate (#3155), resolved BEFORE the render
        # connection so a render queued on the gate never holds a pool
        # connection idle while it waits — same ordering as get_vector_tile.
        try:
            async with self._render_gate.admit():
                # Acquired last, once metadata resolution above is done —
                # this is the only connection this handler ever needs to
                # hold now.
                try:
                    conn = await engine.connect()
                except Exception as exc:
                    logger.error(
                        "map_tile: failed to acquire DB connection for vector "
                        "PNG render catalog=%s collection=%s: %s",
                        internal_catalog_id, internal_collection_id, exc,
                    )
                    raise HTTPException(status_code=503, detail="Database unavailable.") from exc

                try:
                    tile_bytes = await tiles_engine.render_tile(
                        conn, ctx, str(z), x, y, format="png", use_l1_cache=True,
                    )
                finally:
                    await conn.close()
        except RenderAdmissionRejected as exc:
            logger.warning(
                "map_tile: render admission rejected (%s) catalog=%s "
                "collection=%s z=%s x=%s y=%s",
                exc.reason, internal_catalog_id, internal_collection_id, z, x, y,
            )
            raise HTTPException(
                status_code=503,
                detail="Render capacity exhausted on this worker, try again shortly.",
                headers={"Retry-After": str(exc.retry_after_seconds)},
            ) from exc

        # Persist on `is not None` (not truthy) so a confirmed-empty render
        # (`b""` — zero features) is cached too; otherwise every empty tile
        # re-renders from PostGIS on every request (#2898).
        if tile_bytes is not None and provider and cache_enabled and self._tile_cache_writer is not None:
            self._tile_cache_writer.submit_nowait(
                provider,
                internal_catalog_id, cache_id, tms_id, z, x, y, tile_bytes, "png",
            )

        if not tile_bytes:
            duration_ms = (time.perf_counter() - start) * 1000
            logger.info(
                "map_tile: cache=miss source=vector_png catalog=%s collection=%s "
                "z=%s x=%s y=%s duration_ms=%.2f bytes=0 (no features)",
                internal_catalog_id, internal_collection_id, z, x, y, duration_ms,
            )
            return Response(status_code=204)

        duration_ms = (time.perf_counter() - start) * 1000
        logger.info(
            "map_tile: cache=miss source=vector_png catalog=%s collection=%s "
            "z=%s x=%s y=%s duration_ms=%.2f bytes=%d",
            internal_catalog_id, internal_collection_id, z, x, y, duration_ms, len(tile_bytes),
        )
        return Response(
            content=tile_bytes,
            media_type="image/png",
            headers={"X-Render-Cache": "miss", "X-Render-Source": "vector_png"},
        )

    async def get_map_tile_styled(
        self,
        request: Request,
        background_tasks: BackgroundTasks,
        catalog_id: str = Path(..., description="Public catalog ID (external_id)."),
        collection_id: str = Path(..., description="Public collection ID (external_id)."),
        style_id: str = Path(
            ...,
            description=(
                "Style ID. Use 'terrain-rgb' for Terrain-RGB elevation encoding. "
                "Any other ID resolves via StylesProtocol for SLD colormap."
            ),
        ),
        tms_id: str = Path(..., description="Tile Matrix Set ID."),
        z: int = Path(..., ge=0, le=30, description="Zoom level."),
        x: int = Path(..., ge=0, description="Tile column."),
        y: int = Path(..., ge=0, description="Tile row."),
        format: str = Path(..., description="Output image format: 'png' or 'webp'."),
        bands: Optional[str] = Query(
            None,
            description="Comma-separated 1-based band indices for multiband rendering.",
        ),
        expression: Optional[str] = Query(
            None,
            description="Band-math expression evaluated by rio-tiler.",
        ),
        rescale: Optional[str] = Query(
            None,
            description="Per-band rescale ranges as semicolon-separated 'min,max' pairs.",
        ),
        band: int = Query(default=1, ge=1, description="Elevation band index (1-based); used for terrain-rgb and hillshade."),
        relief: Optional[str] = Query(
            None,
            description="Relief mode. Use 'hillshade' to render shaded-relief + colormap.",
        ),
        style_url: Optional[str] = Query(
            None,
            alias="style-url",
            description="External SLD URL to apply to raster tile rendering.",
        ),
        style_url_compat: Optional[str] = Query(
            None,
            alias="style_url",
            include_in_schema=False,
            description="Compatibility spelling of 'style-url'.",
        ),
        azimuth: float = Query(default=315.0, ge=0.0, lt=360.0, description="Hillshade sun azimuth in degrees (0=North, clockwise)."),
        altitude: float = Query(default=45.0, ge=0.0, le=90.0, description="Hillshade sun altitude above horizon in degrees."),
        serve: Optional[Literal["proxy", "redirect"]] = Query(
            None,
            description=(
                "Per-request cache-hit delivery override: 'proxy' streams the "
                "rendered tile bytes (no redirect, for clients like QGIS that "
                "don't follow redirects); 'redirect' (default) issues a 307 to a "
                "signed bucket URL."
            ),
        ),
    ) -> Response:
        """Render a styled COG map tile with explicit style, terrain-RGB, or hillshade.

        - ``style_id='terrain-rgb'``: Mapbox Terrain-RGB elevation encoding.
        - ``?relief=hillshade``: shaded-relief + hypsometric colormap.
        - All other style IDs: fetch SLD from StylesProtocol, parse colormap.

        Flow:
        1. Validate style_id characters (reject path-traversal).
        2. Validate format; ensure rio-tiler is available.
        3. Resolve external IDs → internal IDs.
        4. Check bucket cache.
        5. Branch on terrain-rgb / hillshade / styled.
        6. Render via run_in_thread; write to cache in background.
        """
        # Security: reject style_id values that could contaminate the cache key.
        self._validate_style_id(style_id)
        self._require_raster_engine()

        # isinstance guard: direct (non-FastAPI) callers leave the Query
        # sentinel in place of the compat param — adopt only a real string.
        if style_url is None and isinstance(style_url_compat, str):
            style_url = style_url_compat
        fmt_lower = format.lower()
        if fmt_lower not in _FORMAT_MEDIA_TYPE:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported format '{format}'. Use 'png' or 'webp'.",
            )

        is_terrain_rgb = style_id == "terrain-rgb"
        is_hillshade = (relief or "").lower() == "hillshade"

        # terrain-rgb always PNG; hillshade always PNG
        if is_terrain_rgb or is_hillshade:
            fmt_lower = "png"
        output_format: Literal["PNG", "WEBP"] = "PNG" if fmt_lower == "png" else "WEBP"

        bands_parsed: Optional[List[int]] = None
        expression_parsed: Optional[str] = None
        rescale_parsed: Optional[List[Tuple[float, float]]] = None
        if not is_terrain_rgb and not is_hillshade:
            bands_parsed, expression_parsed, rescale_parsed = self._parse_multiband_params(
                bands, expression, rescale
            )

        start = time.perf_counter()

        internal_catalog_id, internal_collection_id = await self._resolve_catalog_and_collection(
            catalog_id, collection_id
        )
        await self._require_collection_visible(internal_catalog_id, internal_collection_id)

        # Validate TMS before cache check. Built-in TMS definitions are global;
        # custom TMS lookup is keyed like the public route, so use catalog_id.
        await self._validate_tms_and_matrix(catalog_id, tms_id, z, x, y)

        cfg = await self._load_render_caching_config()

        # Resolve first COG asset href before cache-key construction so an
        # attached source SLD URL can participate in the cache key.
        item = await self._get_raster_source_item(internal_catalog_id, internal_collection_id)
        if not item:
            raise HTTPException(
                status_code=404,
                detail=f"Collection '{collection_id}' has no raster source item or asset.",
            )
        effective_style_url = style_url or self._style_url_from_item(item, style_id)

        # Build cache key
        if is_terrain_rgb:
            cache_style_segment = "terrain-rgb"
        elif is_hillshade:
            az_int = int(round(azimuth))
            alt_int = int(round(altitude))
            cache_style_segment = f"hillshade-{style_id}-az{az_int}-alt{alt_int}"
            if effective_style_url and _STYLE_URL_CACHE_ID is not None:
                cache_style_segment += f"-url-{_STYLE_URL_CACHE_ID(effective_style_url)}"
        else:
            params_hash = _BUILD_RENDER_PARAMS_HASH(  # type: ignore[misc]
                bands=bands_parsed,
                expression=expression_parsed,
                rescale=rescale_parsed,
            ) if _BUILD_RENDER_PARAMS_HASH else None
            style_segment = style_id
            if effective_style_url and _STYLE_URL_CACHE_ID is not None:
                style_segment = f"{style_id}-url-{_STYLE_URL_CACHE_ID(effective_style_url)}"
            cache_style_segment = f"{style_segment}@{params_hash}" if params_hash else style_segment

        cache_key = _BUILD_RENDER_CACHE_KEY(  # type: ignore[misc]
            cfg.key_prefix if cfg else "",  # cfg is non-None when _BUILD_RENDER_CACHE_KEY is set
            internal_collection_id,
            cache_style_segment,
            tms_id,
            z,
            x,
            y,
            fmt_lower,
        ) if _BUILD_RENDER_CACHE_KEY else (
            f"map/{internal_collection_id}/{cache_style_segment}/{tms_id}/{z}/{x}/{y}.{fmt_lower}"
        )

        provider = get_protocol(TileStorageProtocol)
        if provider and cfg and cfg.cache_enabled:
            res = await self._try_render_cache(
                provider, internal_catalog_id, cache_key, tms_id, z, x, y, fmt_lower, start, cfg,
                serve_mode=serve or "redirect",
            )
            if res is not None:
                return res

        from dynastore.extensions.ogc_base import ogc_asset_href
        cog_href = ogc_asset_href(
            item,
            error_detail=f"No COG asset href found for collection '{collection_id}'.",
        )

        # ------------------------------------------------------------------
        # Terrain-RGB branch
        # ------------------------------------------------------------------
        if is_terrain_rgb:
            logger.info(
                "map_tile: terrain-rgb cache=miss catalog=%s collection=%s tms=%s z=%s x=%s y=%s",
                internal_catalog_id, internal_collection_id, tms_id, z, x, y,
            )
            assert _RENDER_COG_TERRAIN_RGB is not None  # guaranteed by _require_raster_engine()
            return await self._render_raster_tile(
                self._tile_cache_writer,
                _RENDER_COG_TERRAIN_RGB,
                (cog_href, z, x, y),
                dict(band=band),
                catalog_id=internal_catalog_id,
                collection_id=internal_collection_id,
                cache_key=cache_key,
                tms_id=tms_id,
                z=z,
                x=x,
                y=y,
                fmt_lower=fmt_lower,
                render_source="rio-tiler-terrain-rgb",
                log_tag="terrain-rgb",
                error_prefix="Terrain-RGB render",
                check_invalid_expression=False,
                provider=provider,
                cfg=cfg,
                render_gate=self._render_gate,
            )

        # ------------------------------------------------------------------
        # Resolve SLD colormap (shared by styled and hillshade paths)
        # ------------------------------------------------------------------
        colormap = await self._colormap_from_style_url(
            effective_style_url,
            catalog_id=internal_catalog_id,
            collection_id=internal_collection_id,
            style_id=style_id,
            required=bool(effective_style_url and not is_hillshade),
        )
        from dynastore.models.protocols import StylesProtocol as _StylesProtocol
        styles_svc = get_protocol(_StylesProtocol)
        if colormap is None and styles_svc:
            style_obj = await self._get_style_record(
                request,
                internal_catalog_id,
                internal_collection_id,
                style_id,
                external_catalog_id=catalog_id,
                external_collection_id=collection_id,
            )
            if not style_obj and not is_hillshade:
                raise HTTPException(
                    status_code=404,
                    detail=f"Style '{style_id}' not found for collection '{collection_id}'.",
                )
            if style_obj and _EXTRACT_SLD_BODY:
                sld_body = _EXTRACT_SLD_BODY(style_obj)
                if sld_body and _PARSE_SLD_COLORMAP:
                    try:
                        colormap = _PARSE_SLD_COLORMAP(sld_body) or None
                    except ValueError as exc:
                        if not is_hillshade:
                            raise HTTPException(
                                status_code=422,
                                detail=f"SLD colormap parse failed: {exc}",
                            ) from exc
                        logger.warning(
                            "map_tile: hillshade SLD parse failed for style=%s: %s — "
                            "falling back to greyscale",
                            style_id, exc,
                        )
        elif colormap is None and not is_hillshade:
            raise HTTPException(
                status_code=500, detail="Styles service not available."
            )

        # ------------------------------------------------------------------
        # Hillshade branch
        # ------------------------------------------------------------------
        if is_hillshade:
            logger.info(
                "map_tile: hillshade cache=miss catalog=%s collection=%s style=%s "
                "azimuth=%.1f altitude=%.1f tms=%s z=%s x=%s y=%s",
                internal_catalog_id, internal_collection_id, style_id,
                azimuth, altitude, tms_id, z, x, y,
            )
            assert _RENDER_COG_HILLSHADE is not None  # guaranteed by _require_raster_engine()
            return await self._render_raster_tile(
                self._tile_cache_writer,
                _RENDER_COG_HILLSHADE,
                (cog_href, z, x, y),
                dict(band=band, azimuth=azimuth, altitude=altitude, colormap=colormap),
                catalog_id=internal_catalog_id,
                collection_id=internal_collection_id,
                cache_key=cache_key,
                tms_id=tms_id,
                z=z,
                x=x,
                y=y,
                fmt_lower=fmt_lower,
                render_source="rio-tiler-hillshade",
                log_tag="hillshade",
                error_prefix="Hillshade render",
                check_invalid_expression=True,
                provider=provider,
                cfg=cfg,
                render_gate=self._render_gate,
            )

        # ------------------------------------------------------------------
        # Styled raster tile branch
        # ------------------------------------------------------------------
        logger.info(
            "map_tile: cache=miss catalog=%s collection=%s style=%s tms=%s z=%s x=%s y=%s fmt=%s",
            internal_catalog_id, internal_collection_id, style_id, tms_id, z, x, y, fmt_lower,
        )
        assert _RENDER_COG_TILE is not None  # guaranteed by _require_raster_engine()
        return await self._render_raster_tile(
            self._tile_cache_writer,
            _RENDER_COG_TILE,
            (cog_href, z, x, y),
            dict(
                colormap=colormap,
                output_format=output_format,
                bands=bands_parsed,
                expression=expression_parsed,
                rescale=rescale_parsed,
            ),
            catalog_id=internal_catalog_id,
            collection_id=internal_collection_id,
            cache_key=cache_key,
            tms_id=tms_id,
            z=z,
            x=x,
            y=y,
            fmt_lower=fmt_lower,
            render_source="rio-tiler",
            log_tag="rio-tiler",
            error_prefix="Raster render",
            check_invalid_expression=True,
            provider=provider,
            cfg=cfg,
            render_gate=self._render_gate,
        )

    async def _invalidate_tile_cache_impl(self, catalog_id: str, collection_id: Optional[str]) -> dict:
        """Shared cache invalidation logic for deprecated and aligned endpoints."""
        delete_tasks = []
        invalidated_targets = []

        if collection_id:
            invalidated_targets = [f"{catalog_id}:{collection_id}"]
            logger.info("Invalidating tile cache for collection: %s", invalidated_targets)
            delete_tasks.append(
                tms_manager.invalidate_collection_tiles(
                    catalog_id=catalog_id, collection_id=collection_id
                )
            )
        else:
            invalidated_targets = [f"{catalog_id} (full catalog)"]
            logger.info("Invalidating tile cache for entire catalog: %s", catalog_id)
            delete_tasks.append(
                tms_manager.invalidate_catalog_tiles(catalog_id=catalog_id)
            )

        await asyncio.gather(*delete_tasks, return_exceptions=True)
        return {
            "message": f"Successfully triggered tile cache invalidation for catalog '{catalog_id}'.",
            "invalidated_targets": invalidated_targets,
        }

    async def invalidate_tile_cache(
        self,
        dataset: str,
        collections: Optional[str] = Query(
            None,
            description="Comma-separated list of collection IDs to invalidate. If not provided, the cache for the entire catalog is cleared.",
        ),
    ):
        """
        Deletes cached tiles from the underlying storage for a given catalog or a subset of its collections.
        This is useful for forcing a refresh of tiles after data updates.
        """
        try:
            delete_tasks = []
            invalidated_targets = []

            if collections:
                collection_list = collections.split(",")
                invalidated_targets = [
                    f"{dataset}:{coll_id}" for coll_id in collection_list
                ]
                logger.info(
                    f"Invalidating tile cache for specific collections: {invalidated_targets}"
                )
                for coll_id in collection_list:
                    delete_tasks.append(
                        tms_manager.invalidate_collection_tiles(
                            catalog_id=dataset, collection_id=coll_id
                        )
                    )
            else:
                invalidated_targets = [f"{dataset} (full catalog)"]
                logger.info(f"Invalidating tile cache for entire catalog: {dataset}")
                delete_tasks.append(
                    tms_manager.invalidate_catalog_tiles(catalog_id=dataset)
                )

            await asyncio.gather(*delete_tasks, return_exceptions=True)

            return {
                "message": f"Successfully triggered tile cache invalidation for catalog '{dataset}'.",
                "invalidated_targets": invalidated_targets,
            }
        except Exception as e:
            logger.error(
                f"Failed to invalidate tile cache for '{dataset}': {e}", exc_info=True
            )
            raise HTTPException(
                status_code=500,
                detail=f"An error occurred during cache invalidation: {e}",
            ) from e

    # ------------------------------------------------------------------
    # Aligned endpoints: /tiles/catalogs/{catalog_id}/collections/{collection_id}/...
    # ------------------------------------------------------------------

    async def get_vector_tile_aligned_default(
        self,
        catalog_id: str,
        collection_id: str,
        z: int,
        x: int,
        y: int,
        request: Request,
        background_tasks: BackgroundTasks,
        datetime: Optional[str] = Query(None),
        filter: Optional[str] = Query(None, description="CQL2 Filter expression."),
        filter_lang: Optional[str] = _FILTER_LANG_QUERY,
        filter_crs: Optional[str] = _FILTER_CRS_QUERY,
        subset: Optional[str] = Query(None),
        simplification: Optional[float] = Query(None),
        simplification_by_zoom: Optional[str] = Query(None),
        simplification_algorithm: SimplificationAlgorithm = Query(
            SimplificationAlgorithm.TOPOLOGY_PRESERVING
        ),
        disable_cache: bool = Query(False, description="Disable cache for this request."),
        refresh_cache: bool = Query(False, description="Refresh cache by invalidating before fetching."),
        serve: Optional[Literal["proxy", "redirect"]] = Query(
            None,
            description=(
                "Per-request cache-hit delivery override: 'proxy' streams tile "
                "bytes (no redirect, for clients like QGIS that don't follow "
                "redirects); 'redirect' issues a 307 to a signed bucket URL. "
                "Omit for the platform default."
            ),
        ),
        request_hints: FrozenSet = Depends(parse_hints_param),
    ):
        """Get a vector tile (MVT) using the default WebMercatorQuad TMS.

        catalog_id and collection_id are external (public) IDs. They are
        validated here (404 on unknown ids) but dispatched unresolved: tile
        cache namespaces are keyed by external ids (preseed and the flat
        routes write ``<external_id>@<content_hash>/…``), so delegating
        internal ids would fork a second cache namespace per collection and
        miss every preseeded tile.
        """
        await self._resolve_catalog_and_collection(catalog_id, collection_id)
        return await self.get_vector_tile(
            request=request,
            dataset=catalog_id,
            tileMatrixSetId="WebMercatorQuad",
            z=z,
            x=x,
            y=y,
            format="mvt",
            background_tasks=background_tasks,
            collections=collection_id,
            datetime=datetime,
            filter=filter,
            filter_lang=filter_lang,
            filter_crs=filter_crs,
            subset=subset,
            simplification=simplification,
            simplification_by_zoom=simplification_by_zoom,
            simplification_algorithm=simplification_algorithm,
            disable_cache=disable_cache,
            refresh_cache=refresh_cache,
            serve=serve,
        )

    async def get_vector_tile_aligned(
        self,
        catalog_id: str,
        collection_id: str,
        tileMatrixSetId: str,
        z: int,
        x: int,
        y: int,
        format: str,
        request: Request,
        background_tasks: BackgroundTasks,
        datetime: Optional[str] = Query(None),
        filter: Optional[str] = Query(None, description="CQL2 Filter expression."),
        filter_lang: Optional[str] = _FILTER_LANG_QUERY,
        filter_crs: Optional[str] = _FILTER_CRS_QUERY,
        subset: Optional[str] = Query(None),
        simplification: Optional[float] = Query(None),
        simplification_by_zoom: Optional[str] = Query(None),
        simplification_algorithm: SimplificationAlgorithm = Query(
            SimplificationAlgorithm.TOPOLOGY_PRESERVING
        ),
        disable_cache: bool = Query(False, description="Disable cache for this request."),
        refresh_cache: bool = Query(False, description="Refresh cache by invalidating before fetching."),
        serve: Optional[Literal["proxy", "redirect"]] = Query(
            None,
            description=(
                "Per-request cache-hit delivery override: 'proxy' streams tile "
                "bytes (no redirect, for clients like QGIS that don't follow "
                "redirects); 'redirect' issues a 307 to a signed bucket URL. "
                "Omit for the platform default."
            ),
        ),
        request_hints: FrozenSet = Depends(parse_hints_param),
    ):
        """Get a vector tile for a collection with an explicit TMS (OGC aligned path).

        catalog_id and collection_id are external (public) IDs. They are
        validated here (404 on unknown ids) but dispatched unresolved — same
        rationale as get_vector_tile_aligned_default: external-id cache
        namespaces must stay unified with preseed and the flat routes.
        """
        await self._resolve_catalog_and_collection(catalog_id, collection_id)
        return await self.get_vector_tile(
            request=request,
            dataset=catalog_id,
            tileMatrixSetId=tileMatrixSetId,
            z=z,
            x=x,
            y=y,
            format=format,
            background_tasks=background_tasks,
            collections=collection_id,
            datetime=datetime,
            filter=filter,
            filter_lang=filter_lang,
            filter_crs=filter_crs,
            subset=subset,
            simplification=simplification,
            simplification_by_zoom=simplification_by_zoom,
            simplification_algorithm=simplification_algorithm,
            disable_cache=disable_cache,
            refresh_cache=refresh_cache,
            serve=serve,
        )

    def _build_tileset_item(
        self,
        tms_id: str,
        title: Optional[str],
        data_type: Literal["vector", "map", "coverage"],
        self_href: str,
        tms_scheme_href: str,
    ) -> TileSetItem:
        """Construct a TileSetItem with the required self and tiling-scheme links."""
        return TileSetItem(
            id=tms_id,
            dataType=data_type,
            title=title,
            links=[
                Link(
                    href=self_href,
                    rel="self",
                    type="application/json",
                    title=title,
                    hreflang="en",
                ),
                Link(
                    href=tms_scheme_href,
                    rel="http://www.opengis.net/def/rel/ogc/1.0/tiling-scheme",
                    type="application/json",
                    title=title,
                    hreflang="en",
                ),
            ],
        )

    async def get_collection_tilesets(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
    ) -> TileSetList:
        """List available tilesets for a collection (OGC API Tiles §7.1).

        Returns an OGC API Tiles tilesets list where each entry carries
        ``dataType`` (``'vector'`` for MVT tilesets, ``'map'`` for raster
        map-tile tilesets) and the required links:

        - ``rel='self'`` → tileset-metadata resource for this TMS.
        - ``rel='http://www.opengis.net/def/rel/ogc/1.0/tiling-scheme'`` →
          the TileMatrixSet definition document.

        Resolves external IDs and enforces collection visibility.
        """
        internal_catalog_id, internal_collection_id = await self._resolve_catalog_and_collection(
            catalog_id, collection_id
        )
        await self._require_collection_visible(internal_catalog_id, internal_collection_id)

        # Collect all applicable TMS ids (built-in + per-catalog custom)
        all_tms: List[Tuple[str, Optional[str]]] = [
            (tms_id, tms_def.title)
            for tms_id, tms_def in BUILTIN_TILE_MATRIX_SETS.items()
        ]
        seen_ids = {tms_id for tms_id, _ in all_tms}
        custom_tms_list = await tms_manager.list_custom_tms(catalog_id=internal_catalog_id)
        for tms in custom_tms_list:
            if tms.id not in seen_ids:
                all_tms.append((tms.id, tms.title))
                seen_ids.add(tms.id)

        tilesets: List[TileSetItem] = []
        for tms_id, title in all_tms:
            tms_scheme_href = str(
                request.url_for("get_tile_matrix_set", tileMatrixSetId=tms_id)
            )
            # Vector tileset (MVT) — dataType='vector'
            tilesets.append(
                self._build_tileset_item(
                    tms_id=tms_id,
                    title=title,
                    data_type="vector",
                    self_href=str(
                        request.url_for(
                            "get_collection_tileset",
                            catalog_id=catalog_id,
                            collection_id=collection_id,
                            tileMatrixSetId=tms_id,
                        )
                    ),
                    tms_scheme_href=tms_scheme_href,
                )
            )
            # Map tileset (raster/COG render) — dataType='map'
            tilesets.append(
                self._build_tileset_item(
                    tms_id=tms_id,
                    title=title,
                    data_type="map",
                    self_href=str(
                        request.url_for(
                            "get_collection_map_tileset",
                            catalog_id=catalog_id,
                            collection_id=collection_id,
                            tms_id=tms_id,
                        )
                    ),
                    tms_scheme_href=tms_scheme_href,
                )
            )

        return TileSetList(tilesets=tilesets)

    async def get_collection_tileset(
        self,
        catalog_id: str,
        collection_id: str,
        tileMatrixSetId: str,
        request: Request,
    ) -> TileSetItem:
        """Tileset metadata for vector tiles of a collection (OGC API Tiles §7.2).

        Returns a TileSet document with ``dataType='vector'`` for the requested
        TileMatrixSet.  Resolves external IDs and enforces collection visibility.
        """
        internal_catalog_id, internal_collection_id = await self._resolve_catalog_and_collection(
            catalog_id, collection_id
        )
        await self._require_collection_visible(internal_catalog_id, internal_collection_id)

        # Resolve title from built-in TMS or custom TMS
        tms_def = BUILTIN_TILE_MATRIX_SETS.get(tileMatrixSetId)
        title: Optional[str] = tms_def.title if tms_def else None
        if title is None:
            custom = await tms_manager.get_custom_tms(
                catalog_id=internal_catalog_id, tms_id=tileMatrixSetId
            )
            if custom is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"TileMatrixSet '{tileMatrixSetId}' not found.",
                )
            title = custom.title

        return self._build_tileset_item(
            tms_id=tileMatrixSetId,
            title=title,
            data_type="vector",
            self_href=str(
                request.url_for(
                    "get_collection_tileset",
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    tileMatrixSetId=tileMatrixSetId,
                )
            ),
            tms_scheme_href=str(
                request.url_for("get_tile_matrix_set", tileMatrixSetId=tileMatrixSetId)
            ),
        )

    async def get_collection_map_tileset(
        self,
        catalog_id: str,
        collection_id: str,
        tms_id: str,
        request: Request,
    ) -> TileSetItem:
        """Tileset metadata for raster map tiles of a collection (OGC API Maps §7.2).

        Returns a TileSet document with ``dataType='map'`` for the requested
        TileMatrixSet.  Resolves external IDs and enforces collection visibility.
        """
        internal_catalog_id, internal_collection_id = await self._resolve_catalog_and_collection(
            catalog_id, collection_id
        )
        await self._require_collection_visible(internal_catalog_id, internal_collection_id)

        tms_def = BUILTIN_TILE_MATRIX_SETS.get(tms_id)
        title: Optional[str] = tms_def.title if tms_def else None
        if title is None:
            custom = await tms_manager.get_custom_tms(
                catalog_id=internal_catalog_id, tms_id=tms_id
            )
            if custom is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"TileMatrixSet '{tms_id}' not found.",
                )
            title = custom.title

        return self._build_tileset_item(
            tms_id=tms_id,
            title=title,
            data_type="map",
            self_href=str(
                request.url_for(
                    "get_collection_map_tileset",
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    tms_id=tms_id,
                )
            ),
            tms_scheme_href=str(
                request.url_for("get_tile_matrix_set", tileMatrixSetId=tms_id)
            ),
        )

    async def invalidate_collection_tile_cache(
        self,
        catalog_id: str,
        collection_id: str,
    ):
        """Invalidate the tile cache for a specific collection (OGC aligned path).

        Validates the external ids (404 on unknown), then invalidates the
        external-id cache namespace — the one preseed and the tile routes
        write to. The internal-id namespace is swept as well: renders
        dispatched through this route used to key the cache by internal ids,
        and those entries linger in the buckets until invalidated here.
        """
        try:
            internal_catalog_id, internal_collection_id = await self._resolve_catalog_and_collection(
                catalog_id, collection_id
            )
            result = await self._invalidate_tile_cache_impl(catalog_id, collection_id)
            if (internal_catalog_id, internal_collection_id) != (catalog_id, collection_id):
                await self._invalidate_tile_cache_impl(internal_catalog_id, internal_collection_id)
            return result
        except HTTPException:
            raise
        except Exception as e:
            logger.error(
                "Failed to invalidate tile cache for '%s/%s': %s", catalog_id, collection_id, e,
                exc_info=True,
            )
            raise HTTPException(
                status_code=500,
                detail=f"An error occurred during cache invalidation: {e}",
            ) from e
