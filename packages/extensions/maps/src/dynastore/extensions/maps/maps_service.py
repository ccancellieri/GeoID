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

# dynastore/extensions/maps/maps_service.py

import logging
import asyncio
from typing import Any, List, Optional
from concurrent.futures import ProcessPoolExecutor
from fastapi import Depends, FastAPI, APIRouter, HTTPException, Response, Query, Request
from sqlalchemy.ext.asyncio import AsyncConnection
from contextlib import asynccontextmanager

from dynastore.modules.concurrency import run_in_thread

from dynastore.extensions.tools.db import get_async_engine
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.models.driver_context import DriverContext
from dynastore.extensions.maps.format_convert import (
    FORMAT_MEDIA_TYPES as _FORMAT_MEDIA_TYPES,
    SUPPORTED_MAP_FORMATS as _SUPPORTED_MAP_FORMATS,
    convert_png_to_format as _convert_png_to_format,
)
from dynastore.extensions.maps.renderer import render_map_image
from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.extensions.tools.resolvers import (
    resolve_internal_catalog_id_or_404,
    resolve_internal_collection_id_or_404,
)
from dynastore.modules.db_config import shared_queries
from dynastore.tools.geospatial import BboxDimensionality, parse_bbox_string
from dynastore.tools.ogc_common import parse_subset_parameter
from dynastore.modules.storage.drivers.pg_sidecars import (
    GeometriesSidecarConfig,
    driver_sidecars,
)
from . import maps_db
from .maps_tile_cache import try_mvt_cache_layers, MVT_TILE_SRID
from dynastore.models.localization import LocalizedText
from .maps_models import MapsLandingPage, DatasetMaps, MapContent, Link
from .maps_config import MapsConfig
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.extensions.web.decorators import expose_web_page
from dynastore.extensions.tools.language_utils import get_language
import os


logger = logging.getLogger(__name__)

# Slice 2: raster render imports — guarded so the maps extension can still
# load in environments without rio-tiler (graceful degradation: raster
# branch returns 422 when rio-tiler is absent rather than failing import).
_RENDER_COG_MAP = None
_RENDER_COG_TILE = None
_PARSE_SLD_COLORMAP = None
_FETCH_SLD_BODY = None
_STYLE_URL_FROM_ITEM = None
_BUILD_RENDER_CACHE_KEY = None
_RenderCachingConfig = None
try:
    from dynastore.modules.renders.engine import render_cog_map as _rcm, render_cog_tile as _rct  # noqa: E402
    from dynastore.modules.renders.colormap import parse_sld_colormap as _psc  # noqa: E402
    from dynastore.modules.renders.style_url import fetch_sld_body as _fsb, style_url_from_item as _sufi  # noqa: E402
    from dynastore.modules.renders.config import build_render_cache_key as _brck, RenderCachingConfig as _RCC  # noqa: E402
    _RENDER_COG_MAP = _rcm
    _RENDER_COG_TILE = _rct
    _PARSE_SLD_COLORMAP = _psc
    _FETCH_SLD_BODY = _fsb
    _STYLE_URL_FROM_ITEM = _sufi
    _BUILD_RENDER_CACHE_KEY = _brck
    _RenderCachingConfig = _RCC
except ImportError:
    pass

OGC_API_MAPS_URIS = [
    # OGC API - Maps Part 1: Core — req/conf classes
    # /conf/core implements the /map operation (Req 7/8) for any collection.
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/core",
    # /conf/dataset-map — /map at dataset (landing) level (Req 10).
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/dataset-map",
    # /conf/collection-map — /collections/{cid}/map at collection level (Req 11).
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/collection-map",
    # /conf/styled-map — /styles/{styleId}/map override (Req 12).
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/styled-map",
    # /conf/png, /conf/jpeg, /conf/tiff — advertised image content types.
    # Note: the Maps encoding slug is `tiff` (not `geotiff`; that slug belongs
    # to OGC API - Coverages). Both PNG and TIFF outputs are served via the
    # format_convert pipeline (PNG passthrough; TIFF via rasterio).
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/png",
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/jpeg",
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/tiff",
    # Map-tile generation (OGC API - Maps /conf/tilesets, Req 24) is served by the
    # Tiles extension as map-tiles (dataType=map) under
    # /tiles/catalogs/{cat}/collections/{coll}/map/tiles/... — not claimed here.
    # /conf/scaling — width/height on /map resample output to the requested
    # pixel dimensions via rio-tiler COGReader.part(width=, height=) (Req 15).
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/scaling",
    # /conf/background — bgcolor and transparent params accepted on /map (Req 16).
    # Note: NOT /conf/display-resolution (mm-per-pixel class, not implemented).
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/background",
    # /conf/spatial-subsetting — bbox/bbox-crs accepted on /map (Req 17/18).
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/spatial-subsetting",
]


# --- Output format conversion ---------------------------------------------
#
# The rendering pipeline produces PNG bytes. For JPEG and GeoTIFF we convert
# after the renderer returns via ``format_convert.convert_png_to_format``.
# The helper lives in its own module so it is importable without the GDAL
# renderer (useful for unit testing in dev venvs without osgeo).

# --- Helpers ---

def _resolve_target_srid(layer_config: Any) -> int:
    """Resolve the render SRID for a collection's config.

    ``target_srid`` used to live on a ``geometry_storage`` field that was
    removed when the PG driver's sidecar tables became a derived
    (Computed) ``sidecars`` list (GeoID #2744). We look the geometries
    sidecar up in that list via ``driver_sidecars()``, which degrades to
    ``[]`` for non-PG driver configs.

    The sidecars list is legitimately empty in two cases that are NOT
    errors: a FEATURES/VECTOR collection that hasn't been materialised
    yet (``ensure_storage()`` populates it lazily), and a RECORDS
    collection, which never gets a geometries sidecar (#2655). Both fall
    back to the geometries sidecar's own default SRID so rendering can
    proceed with the standard CRS.
    """
    return next(
        (
            sc.target_srid
            for sc in driver_sidecars(layer_config)
            if isinstance(sc, GeometriesSidecarConfig)
        ),
        GeometriesSidecarConfig.model_fields["target_srid"].default,
    )


async def _get_style_to_render(conn: AsyncConnection, dataset: str, collection_id: Optional[str], style_name: Optional[str]) -> Optional[Any]:
    """
    Fetches a style record and finds the first compatible stylesheet (SLD or MapboxGL).
    Returns the stylesheet content object, or None if no style is requested.
    Raises HTTPException if the style is not found.
    """
    if not style_name or not collection_id:
        return None

    from dynastore.models.protocols import StylesProtocol
    styles_ext = get_protocol(StylesProtocol)
    if not styles_ext:
        return None # Styles extension is not enabled

    from dynastore.modules.styles import db as styles_db
    from dynastore.modules.styles.models import StyleFormatEnum

    style_record = await styles_db.get_style_by_id_and_collection(conn, dataset, collection_id, style_name)
    if not style_record:
        raise HTTPException(status_code=404, detail=f"Style with name '{style_name}' not found for collection '{collection_id}'.")

    # Find a compatible stylesheet from the list.
    # The renderer supports SLD and MapboxGL, so we look for those.
    for ss in style_record.stylesheets:
        if ss.content.format in [StyleFormatEnum.SLD_1_1, StyleFormatEnum.MAPBOX_GL]:
            return ss # Return the first compatible StyleSheet object
    
    return None # No compatible stylesheet format found in the style record

async def _validate_collections_helper(conn, dataset, requested_collections):
    """Shared helper to check logical and physical existence of collections.

    ``collection_id`` is not guaranteed to be the physical table name — the
    PostgreSQL driver config carries a separately-resolved ``physical_table``
    (see ``CollectionService.resolve_physical_table``; every other physical
    read/write path in the codebase resolves it explicitly instead of
    assuming ``physical_table == collection_id``). Querying
    ``information_schema.tables`` with the raw collection id verbatim, as
    this helper used to, false-negatives on any collection whose physical
    table name diverges from its id and 404s a render that the direct-by-id
    read paths (items, tiles) serve correctly.
    """
    catalogs_svc = get_protocol(CatalogsProtocol)
    if not catalogs_svc:
        return []

    # Physical schema != catalog_id: the physical table lives in the resolved
    # physical schema, not one named after the logical catalog_id. Qualify the
    # table-existence probe with it (mirroring the tiles path,
    # tiles_module._get_schema); fall back to ``dataset`` when unresolved. No
    # db_resource is passed so the alru_cache is not bypassed (AGENTS.md).
    physical_schema = await catalogs_svc.resolve_physical_schema(dataset) or dataset

    collection_metadata_coroutines = [
        catalogs_svc.get_collection(catalog_id=dataset, collection_id=coll_id)
        for coll_id in requested_collections
    ]
    collection_metadata_results = await asyncio.gather(*collection_metadata_coroutines)

    # Sequential — every check runs `.execute(conn, ...)` on the SAME asyncpg
    # Connection.  Concurrent SELECTs on a single wire deadlock asyncpg's
    # single-stream protocol (regression observed in PRs #28, #32, #43).
    # Per-table latency is ~1ms; serializing N checks is fine.
    physical_table_results = []
    for i, coll_id in enumerate(requested_collections):
        if collection_metadata_results[i]:
            physical_table = await catalogs_svc.resolve_physical_table(
                dataset, coll_id, db_resource=conn
            )
            physical_table_results.append(
                await shared_queries.table_exists_query.execute(
                    conn, schema=physical_schema, table=physical_table or coll_id
                )
            )
        else:
            physical_table_results.append(False)

    valid_collections = []
    for i, coll_id in enumerate(requested_collections):
        if collection_metadata_results[i] and physical_table_results[i]:
            valid_collections.append(coll_id)
    return valid_collections


def _feature_to_dict(feature: Any) -> dict:
    if hasattr(feature, "model_dump"):
        return feature.model_dump(by_alias=True, exclude_none=True)
    return dict(feature)


def _first_asset_href(source: dict) -> Optional[str]:
    assets = source.get("assets") or {}
    for key in ("data", "coverage"):
        asset = assets.get(key) or {}
        if asset.get("href"):
            return asset["href"]
    for asset in assets.values():
        if isinstance(asset, dict) and asset.get("href"):
            return asset["href"]
    return None


async def _first_routed_item(
    catalog_id: str,
    collection_id: str,
) -> Optional[dict]:
    try:
        from dynastore.models.query_builder import QueryRequest  # type: ignore[import]
        from dynastore.modules.storage.hints import Hint
        from dynastore.modules.storage.router import get_driver
        from dynastore.modules.storage.routing_config import Operation

        query = QueryRequest(limit=1)
        driver = await get_driver(
            Operation.READ,
            catalog_id,
            collection_id,
            hints=frozenset({Hint.GEOMETRY_EXACT}),
        )
        async for first in driver.read_entities(
            catalog_id, collection_id, request=query, limit=1
        ):
            return _feature_to_dict(first)
    except Exception:
        logger.debug(
            "maps/raster: routed item lookup failed for %s/%s",
            catalog_id,
            collection_id,
            exc_info=True,
        )
    return None


async def _collection_asset_source(
    catalog_id: str,
    collection_id: str,
) -> Optional[dict]:
    catalogs_svc = get_protocol(CatalogsProtocol)
    if not catalogs_svc:
        return None
    try:
        collection = await catalogs_svc.get_collection(catalog_id, collection_id)
    except Exception:
        return None
    if not collection:
        return None
    data = _feature_to_dict(collection)
    assets = data.get("assets") or data.get("item_assets") or {}
    links = data.get("links") or []
    if not assets and not links:
        return None
    return {"assets": assets, "links": links, "properties": data.get("properties") or {}}


async def _resolve_raster_cog_href(
    catalog_id: str,
    collection_id: str,
) -> Optional[str]:
    """Return the first COG asset href from a raster collection, or None.

    Searches for a ``data`` or ``coverage`` asset key first, then falls back
    to the first asset carrying an ``href``.  Returns ``None`` when the
    collection has no items or no usable href.
    """
    item = await _first_routed_item(catalog_id, collection_id)
    if item:
        href = _first_asset_href(item)
        if href:
            return href

    catalogs_svc = get_protocol(CatalogsProtocol)
    if catalogs_svc:
        try:
            from dynastore.models.query_builder import QueryRequest  # type: ignore[import]
            features = await catalogs_svc.search_items(
                catalog_id, collection_id, QueryRequest(limit=1)
            )
        except Exception:
            features = []
        if features:
            href = _first_asset_href(_feature_to_dict(features[0]))
            if href:
                return href

    collection_source = await _collection_asset_source(catalog_id, collection_id)
    if collection_source:
        return _first_asset_href(collection_source)
    return None


async def _resolve_raster_style_url(
    catalog_id: str,
    collection_id: str,
    style_name: Optional[str],
) -> Optional[str]:
    """Resolve a source-linked SLD/style URL from the first raster item."""
    if _STYLE_URL_FROM_ITEM is None:
        return None
    item = await _first_routed_item(catalog_id, collection_id)
    if item:
        resolved = _STYLE_URL_FROM_ITEM(item, style_name)
        if resolved:
            return resolved

    catalogs_svc = get_protocol(CatalogsProtocol)
    if catalogs_svc:
        try:
            from dynastore.models.query_builder import QueryRequest  # type: ignore[import]
            features = await catalogs_svc.search_items(
                catalog_id, collection_id, QueryRequest(limit=1)
            )
        except Exception:
            features = []
        if features:
            resolved = _STYLE_URL_FROM_ITEM(_feature_to_dict(features[0]), style_name)
            if resolved:
                return resolved

    collection_source = await _collection_asset_source(catalog_id, collection_id)
    if collection_source:
        return _STYLE_URL_FROM_ITEM(collection_source, style_name)
    return None


async def _resolve_raster_colormap(
    catalog_id: str,
    collection_id: str,
    style_name: Optional[str],
    style_url: Optional[str],
    conn: Any,
) -> Optional[Any]:
    """Parse an SLD colormap for a raster collection.

    Returns the ``RioColormap`` dict (``{int: (R,G,B,A)}``) when an SLD
    stylesheet is found and parseable, or ``None`` when no style was
    requested or no SLD stylesheet is available. A parse failure is logged
    and treated as no colormap (raw pixel values rendered).
    """
    if _PARSE_SLD_COLORMAP is None:
        return None
    if style_url and _FETCH_SLD_BODY is not None:
        try:
            sld_body = await _FETCH_SLD_BODY(style_url)
            return _PARSE_SLD_COLORMAP(sld_body) or None
        except Exception as exc:
            logger.warning(
                "maps/raster: style_url SLD colormap parse failed for %s/%s "
                "style=%s url=%s: %s",
                catalog_id, collection_id, style_name, style_url, exc,
            )
            return None
    if not style_name:
        return None
    sheet = await _get_style_to_render(conn, catalog_id, collection_id, style_name)
    if sheet is None:
        return None
    from dynastore.modules.styles.models import SLDContent, StyleFormatEnum  # type: ignore[import]
    content = getattr(sheet, "content", None)
    if content is None:
        return None
    if isinstance(content, SLDContent):
        sld_body = content.sld_body
    elif isinstance(content, dict) and content.get("format") == StyleFormatEnum.SLD_1_1:
        sld_body = content.get("sld_body")
    else:
        return None
    if not sld_body:
        return None
    try:
        cmap = _PARSE_SLD_COLORMAP(sld_body)  # type: ignore[misc]
        return cmap or None
    except Exception as exc:
        logger.warning(
            "maps/raster: SLD colormap parse failed for %s/%s style=%s: %s",
            catalog_id, collection_id, style_name, exc,
        )
        return None


async def _resolve_internal_collection_id(
    catalog_id: str,
    collection_id: str,
) -> str:
    """Resolve external collection id to internal immutable id.

    Returns the provided ``collection_id`` unchanged when the catalogs
    service does not support ``resolve_collection_id`` (e.g. test stubs).
    """
    catalogs_svc = get_protocol(CatalogsProtocol)
    if not catalogs_svc:
        return collection_id
    try:
        internal_id = await catalogs_svc.collections.resolve_collection_id(
            catalog_id, collection_id, allow_missing=False
        )
        return internal_id if internal_id else collection_id
    except Exception:
        return collection_id


async def _is_raster_collection(catalog_id: str, collection_id: str) -> bool:
    """Return True when the collection is of kind RASTER."""
    from dynastore.models.protocols import ConfigsProtocol as _ConfigsProtocol  # type: ignore[import]
    from dynastore.modules.catalog.catalog_config import CollectionInfo, CollectionKind  # type: ignore[import]
    configs_svc = get_protocol(_ConfigsProtocol)
    if not configs_svc:
        return False
    try:
        info = await configs_svc.get_config(CollectionInfo, catalog_id, collection_id)
        return isinstance(info, CollectionInfo) and info.kind == CollectionKind.RASTER
    except Exception:
        return False


async def _render_raster_map(
    *,
    catalog_id: str,
    collection_id: str,
    bbox: List[float],
    width: int,
    height: int,
    style_name: Optional[str],
    style_url: Optional[str],
    fmt: str,
    request: Any,
) -> Response:
    """Raster branch for ``GET /{dataset}/map``.

    Resolves external→internal IDs, enforces collection visibility, fetches
    the COG href and SLD colormap (with a brief DB window for the style only),
    then renders via ``render_cog_map`` in a thread.  The DB connection is
    released before the CPU-bound render to satisfy GeoID #703.

    Args:
        catalog_id: External (public) catalog ID.
        collection_id: External (public) collection ID.
        bbox: ``[min_lon, min_lat, max_lon, max_lat]`` in EPSG:4326.
        width: Output pixel width.
        height: Output pixel height.
        style_name: Optional style identifier for SLD colormap lookup.
        fmt: Output format string (``"png"``, ``"jpeg"``, or ``"geotiff"``).
        request: FastAPI ``Request`` (used to get the async DB engine).
    """
    if _RENDER_COG_MAP is None:
        raise HTTPException(
            status_code=422,
            detail="Raster rendering is not available: rio-tiler is not installed.",
        )

    # Resolve external → internal IDs at the request boundary so cache keys
    # and visibility checks use the immutable internal id.
    internal_catalog_id = catalog_id
    catalogs_svc = get_protocol(CatalogsProtocol)
    if catalogs_svc:
        try:
            internal_catalog_id = await catalogs_svc.resolve_catalog_id(
                catalog_id, allow_missing=False
            ) or catalog_id
        except Exception:
            pass

    internal_collection_id = await _resolve_internal_collection_id(
        internal_catalog_id, collection_id
    )

    # Visibility guard mirrors tiles/coverages/EDR pattern.
    from dynastore.models.protocols.visibility import resolve_collection_listing_ids  # type: ignore[import]
    visible_ids = await resolve_collection_listing_ids(internal_catalog_id)
    if visible_ids is not None and internal_collection_id not in visible_ids:
        raise HTTPException(status_code=404, detail="Collection not found.")

    # Fetch the COG href from the first item (no DB connection held).
    cog_href = await _resolve_raster_cog_href(internal_catalog_id, internal_collection_id)
    if not cog_href:
        raise HTTPException(
            status_code=404,
            detail=f"No COG asset found for collection '{collection_id}'.",
        )

    # Resolve colormap from SLD style if requested (opens and closes a DB
    # connection for the style lookup, then releases before the render).
    colormap = None
    effective_style_url = style_url
    if not effective_style_url:
        effective_style_url = await _resolve_raster_style_url(
            internal_catalog_id, internal_collection_id, style_name
        )
    if style_name or effective_style_url:
        engine = get_async_engine(request)
        async with managed_transaction(engine) as conn:
            colormap = await _resolve_raster_colormap(
                internal_catalog_id, internal_collection_id, style_name,
                effective_style_url, conn
            )

    # Render via rio-tiler in a thread (no DB connection held during render).
    try:
        image_bytes: bytes = await run_in_thread(
            _RENDER_COG_MAP,
            cog_href,
            bbox=bbox,
            width=width,
            height=height,
            colormap=colormap,
            output_format="PNG",
        )
    except Exception as exc:
        logger.error(
            "maps/raster: render_cog_map failed for %s/%s: %s",
            internal_catalog_id, internal_collection_id, exc,
            exc_info=True,
        )
        raise HTTPException(
            status_code=500, detail=f"Raster map render failed: {exc}"
        ) from exc

    # Convert to requested output format.
    try:
        out_bytes = _convert_png_to_format(image_bytes, fmt, bbox=bbox, crs="EPSG:4326")
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("maps/raster: format conversion failed: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=500, detail="Format conversion failed."
        ) from exc

    return Response(content=out_bytes, media_type=_FORMAT_MEDIA_TYPES[fmt])


class MapsService(ExtensionProtocol, OGCServiceMixin):
    priority: int = 100
    """Provides OGC API - Maps (WMS-like) functionality with filtering and Tiling."""
    conformance_uris = OGC_API_MAPS_URIS
    prefix = "/maps"
    protocol_title = "DynaStore OGC API - Maps"
    protocol_description = "Map rendering (WMS-like) with filtering and tiling"
    router:APIRouter = APIRouter(tags=["OGC API - Maps (WMS)"], prefix="/maps")
    process_pool: Optional[ProcessPoolExecutor] = None

    # OGCServiceMixin standard-route wiring: landing_response_model matches
    # what the "/" route declared before migration. static_dir lets the
    # inherited _serve_page_template() locate this extension's own static/
    # directory; static_prefix is intentionally left unset (Maps has no
    # @expose_static-decorated static-files provider today, so leaving it
    # unset keeps get_static_assets() returning the same empty list as
    # before this migration).
    landing_response_model = MapsLandingPage
    static_dir = os.path.join(os.path.dirname(__file__), "static")

    def __init__(self, app: Optional[FastAPI] = None):
        self.app = app
        # Maps' landing page lists dataset links only (no self/conformance/
        # service-doc links) — a genuinely different shape from the shared
        # default, so ogc_landing_page_handler is overridden below rather
        # than reused as-is. landing_name preserves the pre-existing route
        # name: get_catalog_maps/get_collection_maps resolve it via
        # request.url_for("get_maps_landing_page").
        self.register_ogc_standard_routes(landing_name="get_maps_landing_page")

    def configure_app(self, app: FastAPI):
        """Early configuration for the Maps extension."""
        # Web pages / static assets are discovered by WebModule via the
        # WebPageContributor / StaticAssetProvider capability protocols.
        return None

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        # Policies declared via PolicyContributor; IAM forwards centrally.
        import multiprocessing

        # Use an explicit "spawn" context: the Python 3.14 Linux default
        # ("forkserver") binds an AF_UNIX socket under TMPDIR, which fails
        # with OSError(95) when TMPDIR points at a filesystem without
        # Unix-socket support (e.g. the GCS FUSE volume the deploy mounts
        # for large temp files). "spawn" uses pipes only, and pool workers
        # persist, so the higher per-worker start cost is paid once.
        mp_context = multiprocessing.get_context("spawn")
        MapsService.process_pool = ProcessPoolExecutor(mp_context=mp_context)
        logger.info(
            "Maps Service startup: process pool started (executor=%s, "
            "mp_start_method=%s, max_workers=%s)",
            type(MapsService.process_pool).__name__,
            mp_context.get_start_method(),
            getattr(MapsService.process_pool, "_max_workers", "unknown"),
        )
        app.state.maps_config = MapsConfig()

        # Register the default-style vector PNG map-tile source into core's
        # TileSourceProtocol registry (format-gated on "png"), so
        # tiles_engine.build_render_context(..., format="png") picks this
        # source instead of PostgisTileSource. Guarded: the maps extension
        # must still start when the tiles module isn't installed in this
        # deployment — in that case the vector /map/tiles/....png route falls
        # back to its existing 404.
        png_tile_source = None
        try:
            from dynastore.tools.discovery import register_plugin
            from .maps_png_tilesource import MapsPngTileSource

            png_tile_source = MapsPngTileSource()
            register_plugin(png_tile_source)
            logger.info("Maps Service: registered MapsPngTileSource (format=png).")
        except Exception as exc:
            logger.debug(
                "Maps Service: MapsPngTileSource registration skipped (tiles "
                "module unavailable?): %s", exc,
            )

        yield
        logger.info("Maps Service shutdown: closing process pool.")
        if MapsService.process_pool:
            MapsService.process_pool.shutdown(wait=True)
        if png_tile_source is not None:
            from dynastore.tools.discovery import unregister_plugin
            unregister_plugin(png_tile_source)

    def contribute(self, ref):
        """AssetContributor: emit a map-preview link when the resource has a bbox."""
        from dynastore.models.protocols.asset_contrib import AssetLink
        if ref.bbox is None or ref.item_id is None:
            return
        bbox_str = ",".join(str(c) for c in ref.bbox)
        style_q = f"&style={ref.style}" if ref.style else ""
        href = (
            f"{ref.base_url}{self.router.prefix}/{ref.catalog_id}/map"
            f"?collections={ref.collection_id}&bbox={bbox_str}"
            f"&crs=EPSG:4326&width=512&height=512{style_q}"
        )
        yield AssetLink(
            key="map_preview",
            href=href,
            title="Rendered Map Preview",
            media_type="image/png",
            roles=("thumbnail", "visual"),
        )

    async def ogc_landing_page_handler(self, request: Request) -> MapsLandingPage:  # type: ignore[override]
        """Maps landing page: dataset links only (no self/conformance/
        service-doc links) — overrides OGCServiceMixin's generic default
        since OGC API - Maps' landing shape here is per-catalog map access,
        not a plain OGC Common landing page.
        """
        catalogs_svc = get_protocol(CatalogsProtocol)
        catalogs = await catalogs_svc.list_catalogs(limit=1000) if catalogs_svc else []
        base = str(request.url).rstrip("/")
        links = [Link(href=str(request.url), rel="self", type="application/json", title=LocalizedText(en="this document"))]
        for cat in catalogs:
            # cat.id is the immutable internal id; resolve_catalog_id() (used by
            # get_catalog_maps and siblings) is external-only, so advertising the
            # internal id here 404s every link. Mirror the public_id pattern used
            # for collection links below.
            public_id = cat.external_id or cat.id
            links.append(Link(
                href=f"{base}/catalogs/{public_id}",
                rel="dataset", type="application/json", title=LocalizedText(en=f"Maps for dataset '{public_id}'")
            ))
        return MapsLandingPage(links=links)

    @expose_web_page(
        page_id="map_viewer",
        title="Map Viewer",
        icon="fa-map",
        description="Visualize geospatial data on an interactive map.",
    )
    async def provide_map_viewer(self, request: Request):
        return await self._serve_page_template("map_viewer.html")

    # --- Map Endpoint (shared implementation) ---

    @staticmethod
    async def _get_map_impl(
        *,
        dataset: str,
        request: Any,
        collections: str,
        bbox: str,
        bbox_crs: Optional[str],
        crs: str,
        width: int,
        height: int,
        style: Optional[str],
        style_url: Optional[str],
        bgcolor: Optional[str],
        transparent: bool,
        datetime: Optional[str],
        subset: Optional[str],
        f: str,
    ) -> Response:
        """Shared implementation for the /map endpoint.

        Called by both the deprecated ``/{dataset}/map`` route and the new aligned
        ``/catalogs/{catalog_id}/collections/{collection_id}/map`` route.
        The caller is responsible for resolving ``dataset`` to the internal catalog
        ID and for enforcing collection visibility before calling this helper.
        """
        import re

        fmt = f.lower()
        if fmt not in _SUPPORTED_MAP_FORMATS:
            raise HTTPException(
                status_code=415, detail=f"Unsupported map format: {f!r}",
            )

        catalogs_svc = get_protocol(CatalogsProtocol)
        if not catalogs_svc or not await catalogs_svc.get_catalog_model(dataset):
            raise HTTPException(status_code=404, detail=f"Dataset '{dataset}' not found.")

        requested_collections = [c.strip() for c in collections.split(',')]

        # Handle BBOX CRS (Req 18)
        bbox_srid = 4326
        if bbox_crs:
            try:
                match = re.search(r'(\d+)$', bbox_crs)
                if match:
                    bbox_srid = int(match.group(1))
            except Exception:
                pass

        try:
            parsed_bbox = parse_bbox_string(
                bbox,
                dimensionality=BboxDimensionality.STRICT_2D,
                allow_none=False,
                validate_geometry=False,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail="Invalid BBOX format.") from e
        assert parsed_bbox is not None  # allow_none=False guarantees this
        bbox_list = list(parsed_bbox)

        # Raster branch: when the first requested collection is RASTER-kind,
        # skip the vector DB/render pipeline entirely and use the COG engine.
        first_collection = requested_collections[0] if requested_collections else ""
        if first_collection and await _is_raster_collection(dataset, first_collection):
            return await _render_raster_map(
                catalog_id=dataset,
                collection_id=first_collection,
                bbox=bbox_list,
                width=width,
                height=height,
                style_name=style,
                style_url=style_url,
                fmt=fmt,
                request=request,
            )

        # The DB-dependent steps (collection validation, feature fetch, style
        # resolution) run under a single connection that is released before the
        # CPU-bound render below, so a pooled slot is never held across
        # run_in_executor (GeoID #703).
        db_engine = get_async_engine(request)
        # Connection 1: validation + config + style only. Released before both
        # the (possibly slow) MVT cache reads and the CPU-bound render, so a
        # pooled slot is never held across external I/O or the executor (#703).
        async with managed_transaction(db_engine) as conn:
            ctx = DriverContext(db_resource=conn)

            valid_collections = await _validate_collections_helper(conn, dataset, requested_collections)
            if not valid_collections:
                raise HTTPException(status_code=404, detail="One or more collections not found.")

            if not catalogs_svc:
                raise HTTPException(status_code=500, detail="Catalogs service not available.")
            layer_config = await catalogs_svc.get_collection_config(
                dataset, valid_collections[0], ctx=ctx
            )
            if layer_config is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Collection '{valid_collections[0]}' has no storage config.",
                )

            style_to_render = await _get_style_to_render(
                conn, dataset, valid_collections[0] if valid_collections else None, style
            )

        # Best-effort fast path: a default-style, unfiltered, single-collection
        # vector map can be assembled from the cached MVT pyramid, skipping the
        # PostGIS geometry fetch + simplification. Returns None (→ source render)
        # unless the tiles module is loaded, caching is enabled, and every
        # covering tile is present. OGC semantics are identical either way.
        layers_data = None
        render_source_srid = _resolve_target_srid(layer_config)
        if (
            style_to_render is None
            and datetime is None
            and subset is None
            and len(valid_collections) == 1
        ):
            mvt_layers = await try_mvt_cache_layers(
                catalog_id=dataset,
                collection_id=valid_collections[0],
                bbox=bbox_list,
                bbox_srid=bbox_srid,
                width=width,
                height=height,
            )
            if mvt_layers is not None:
                layers_data = mvt_layers
                render_source_srid = MVT_TILE_SRID

        if layers_data is None:
            # Physical schema != catalog_id: the physical table lives in the
            # resolved physical schema, not one named after the logical
            # catalog_id. Resolve it via the protocol (mirroring the tiles
            # path, tiles_module._get_schema) and fall back to ``dataset``
            # when unresolved. ``dataset`` is already the INTERNAL catalog id
            # (resolved by the caller — see docstring). No ``ctx``/db_resource
            # is passed so the alru_cache is not bypassed (AGENTS.md caching).
            physical_schema = (
                await catalogs_svc.resolve_physical_schema(dataset) or dataset
            )
            # Connection 2: source geometry fetch (short-lived, released before render).
            async with managed_transaction(db_engine) as conn:
                try:
                    layers_data = await maps_db.get_features_for_rendering(
                        conn=conn,
                        schema=dataset,
                        physical_schema=physical_schema,
                        collections=valid_collections,
                        bbox=bbox_list,
                        crs=crs,
                        width=width,
                        height=height,
                        bbox_srid=bbox_srid,
                        datetime_str=datetime,
                        subset_params=parse_subset_parameter(subset),
                    )
                except ValueError as e:
                    logger.error(f"Data Error: {e}")
                    raise HTTPException(status_code=400, detail=str(e)) from e

        # Render (CPU-bound, no DB connection held)
        try:
            loop = asyncio.get_running_loop()
            image_bytes = await loop.run_in_executor(
                MapsService.process_pool,
                render_map_image,
                width, height, bbox_list, crs,
                render_source_srid,
                layers_data, style_to_render,
                transparent, bgcolor, bbox_srid,
            )
        except Exception as e:
            # Log the exception type and full traceback, not just str(e) — a
            # bare str() hides whether the failure originates in the pool
            # machinery (worker spawn, arg pickling), GDAL, or the renderer
            # itself. The user-facing message stays generic.
            logger.exception(
                "Render Error: %r (bbox=%s, collections=%s, crs=%s)",
                e, bbox_list, valid_collections, crs,
            )
            raise HTTPException(status_code=500, detail="Failed to render map.") from e

        try:
            out_bytes = _convert_png_to_format(
                image_bytes, fmt, bbox=bbox_list, crs=crs,
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Format conversion error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Format conversion failed.") from e

        return Response(content=out_bytes, media_type=_FORMAT_MEDIA_TYPES[fmt])

    # --- Aligned endpoints: /maps/catalogs/{catalog_id}[/collections/{collection_id}]/... ---

    @router.get(
        "/catalogs/{catalog_id}",
        response_model=DatasetMaps,
        summary="Catalog map metadata (OGC API - Maps aligned path)",
    )
    async def get_catalog_maps(  # type: ignore[reportGeneralTypeIssues]
        catalog_id: str,
        request: Request,
        language: str = Depends(get_language),
    ):
        """Return map metadata for every visible collection in a catalog under the aligned path."""
        catalogs_svc = get_protocol(CatalogsProtocol)
        if not catalogs_svc:
            raise HTTPException(status_code=500, detail="Catalogs service not available.")

        internal_catalog_id = await resolve_internal_catalog_id_or_404(
            catalogs_svc, catalog_id
        )

        from dynastore.models.protocols.visibility import resolve_collection_listing_ids
        visible_ids = await resolve_collection_listing_ids(internal_catalog_id)

        collections = await catalogs_svc.list_collections(internal_catalog_id, limit=1000)
        base = str(request.url).rstrip("/")
        maps = []
        for coll in collections:
            # visible_ids is the internal-id allowlist; coll.id is the internal id
            # (the public external_id is only swapped in at serialization time).
            if visible_ids is not None and coll.id not in visible_ids:
                continue
            public_id = coll.external_id or coll.id
            title = coll.title.resolve(language) if coll.title is not None else None
            maps.append(
                MapContent(
                    title=title,
                    links=[
                        Link(
                            href=f"{base}/collections/{public_id}",
                            rel="item",
                            type="application/json",
                            title=LocalizedText(en=f"Maps for collection '{public_id}'"),
                        )
                    ],
                )
            )
        links = [
            Link(href=str(request.url), rel="self"),
            Link(href=str(request.url_for("get_maps_landing_page")), rel="up"),
        ]
        return DatasetMaps(
            title=f"Maps for catalog '{catalog_id}'",
            maps=maps,
            links=links,
        )

    @router.get(
        "/catalogs/{catalog_id}/map",
        summary="Render a dataset-level map for a catalog (OGC API - Maps aligned path)",
    )
    async def get_catalog_map(  # type: ignore[reportGeneralTypeIssues]
        catalog_id: str,
        request: Request,
        collections: str = Query(..., description="Comma-separated list of collections to render."),
        bbox: str = Query("-180,-90,180,90", description="Bounding box in CRS coordinates."),
        bbox_crs: str = Query(None, description="CRS of the BBOX (defaults to OGC:CRS84)."),
        crs: str = Query("EPSG:3857", description="Coordinate Reference System."),
        width: int = Query(768, description="Width of the output image."),
        height: int = Query(768, description="Height of the output image."),
        style: Optional[str] = Query(None, description="Name of the style to apply."),
        style_url: Optional[str] = Query(
            None,
            alias="style-url",
            description="External SLD URL to apply to raster map rendering.",
        ),
        bgcolor: Optional[str] = Query(None, description="Background color of the map."),
        transparent: bool = Query(True, description="Whether the map background should be transparent."),
        datetime: Optional[str] = Query(None, description="Temporal filter (timestamp or interval)."),
        subset: Optional[str] = Query(None, description="Custom dimension filter."),
        f: str = Query("png", description="Output format: png | jpeg | geotiff."),
    ):
        """Render a composited dataset-level map for selected collections under the aligned path.

        Resolves the external catalog_id and each external collection id to internal
        immutable IDs, enforces visibility per collection, then delegates to
        ``_get_map_impl`` (OGC API - Maps /conf/dataset-map, Req 10).
        """
        catalogs_svc = get_protocol(CatalogsProtocol)
        if not catalogs_svc:
            raise HTTPException(status_code=500, detail="Catalogs service not available.")

        internal_catalog_id = await resolve_internal_catalog_id_or_404(
            catalogs_svc, catalog_id
        )

        from dynastore.models.protocols.visibility import resolve_collection_listing_ids
        visible_ids = await resolve_collection_listing_ids(internal_catalog_id)

        requested = [c.strip() for c in collections.split(",") if c.strip()]
        if not requested:
            raise HTTPException(status_code=400, detail="At least one collection is required.")
        internal_collections: List[str] = []
        for ext_id in requested:
            internal_id = await resolve_internal_collection_id_or_404(
                catalogs_svc, internal_catalog_id, ext_id
            )
            if visible_ids is not None and internal_id not in visible_ids:
                raise HTTPException(status_code=404, detail="Collection not found.")
            internal_collections.append(internal_id)

        return await MapsService._get_map_impl(
            dataset=internal_catalog_id,
            request=request,
            collections=",".join(internal_collections),
            bbox=bbox,
            bbox_crs=bbox_crs,
            crs=crs,
            width=width,
            height=height,
            style=style,
            style_url=style_url,
            bgcolor=bgcolor,
            transparent=transparent,
            datetime=datetime,
            subset=subset,
            f=f,
        )

    @router.get(
        "/catalogs/{catalog_id}/collections/{collection_id}",
        response_model=DatasetMaps,
        summary="Collection map metadata (OGC API - Maps aligned path)",
    )
    async def get_collection_maps(  # type: ignore[reportGeneralTypeIssues]
        catalog_id: str,
        collection_id: str,
        request: Request,
        language: str = Depends(get_language),
    ):
        """Return map metadata for a single collection under the aligned platform path."""
        catalogs_svc = get_protocol(CatalogsProtocol)
        if not catalogs_svc:
            raise HTTPException(status_code=500, detail="Catalogs service not available.")

        internal_catalog_id = await resolve_internal_catalog_id_or_404(
            catalogs_svc, catalog_id
        )

        internal_collection_id = await resolve_internal_collection_id_or_404(
            catalogs_svc, internal_catalog_id, collection_id
        )

        from dynastore.models.protocols.visibility import resolve_collection_listing_ids
        visible_ids = await resolve_collection_listing_ids(internal_catalog_id)
        if visible_ids is not None and internal_collection_id not in visible_ids:
            raise HTTPException(status_code=404, detail="Collection not found.")

        coll = await catalogs_svc.get_collection(
            catalog_id=internal_catalog_id, collection_id=internal_collection_id
        )
        if not coll:
            raise HTTPException(status_code=404, detail=f"Collection '{collection_id}' not found.")

        title = coll.title.resolve(language) if coll.title is not None else None
        base = str(request.url)
        map_links = [
            Link(href=f"{base}/map?bbox=-180,-90,180,90&crs=EPSG:4326", rel="http://www.opengis.net/def/rel/ogc/1.0/map", type="image/png"),
        ]
        links = [
            Link(href=base, rel="self"),
            Link(href=str(request.url_for("get_maps_landing_page")), rel="up"),
        ]
        return DatasetMaps(
            title=f"Maps for collection '{collection_id}' in '{catalog_id}'",
            maps=[MapContent(title=title, links=map_links)],
            links=links,
        )

    @router.get(
        "/catalogs/{catalog_id}/collections/{collection_id}/map",
        summary="Render map for a collection (OGC API - Maps aligned path, default style)",
    )
    async def get_collection_map(  # type: ignore[reportGeneralTypeIssues]
        catalog_id: str,
        collection_id: str,
        request: Request,
        bbox: str = Query("-180,-90,180,90", description="Bounding box in CRS coordinates."),
        bbox_crs: str = Query(None, description="CRS of the BBOX (defaults to OGC:CRS84)."),
        crs: str = Query("EPSG:3857", description="Coordinate Reference System."),
        width: int = Query(768, description="Width of the output image."),
        height: int = Query(768, description="Height of the output image."),
        bgcolor: Optional[str] = Query(None, description="Background color of the map."),
        transparent: bool = Query(True, description="Whether the map background should be transparent."),
        datetime: Optional[str] = Query(None, description="Temporal filter (timestamp or interval)."),
        subset: Optional[str] = Query(None, description="Custom dimension filter."),
        f: str = Query("png", description="Output format: png | jpeg | geotiff."),
        style_url: Optional[str] = Query(
            None,
            alias="style-url",
            description="External SLD URL to apply to raster map rendering.",
        ),
    ):
        """Render the default-style map for a specific collection under the aligned path.

        Resolves catalog_id and collection_id from external (public) IDs to
        internal immutable IDs, enforces visibility, then delegates to
        ``_get_map_impl``.
        """
        catalogs_svc = get_protocol(CatalogsProtocol)
        if not catalogs_svc:
            raise HTTPException(status_code=500, detail="Catalogs service not available.")

        internal_catalog_id = await resolve_internal_catalog_id_or_404(
            catalogs_svc, catalog_id
        )

        internal_collection_id = await resolve_internal_collection_id_or_404(
            catalogs_svc, internal_catalog_id, collection_id
        )

        from dynastore.models.protocols.visibility import resolve_collection_listing_ids
        visible_ids = await resolve_collection_listing_ids(internal_catalog_id)
        if visible_ids is not None and internal_collection_id not in visible_ids:
            raise HTTPException(status_code=404, detail="Collection not found.")

        return await MapsService._get_map_impl(
            dataset=internal_catalog_id,
            request=request,
            collections=internal_collection_id,
            bbox=bbox,
            bbox_crs=bbox_crs,
            crs=crs,
            width=width,
            height=height,
            style=None,
            style_url=style_url,
            bgcolor=bgcolor,
            transparent=transparent,
            datetime=datetime,
            subset=subset,
            f=f,
        )

    @router.get(
        "/catalogs/{catalog_id}/collections/{collection_id}/styles/{style_id}/map",
        summary="Render styled map for a collection (OGC API - Maps aligned path, explicit style)",
    )
    async def get_collection_map_styled(  # type: ignore[reportGeneralTypeIssues]
        catalog_id: str,
        collection_id: str,
        style_id: str,
        request: Request,
        bbox: str = Query("-180,-90,180,90", description="Bounding box in CRS coordinates."),
        bbox_crs: str = Query(None, description="CRS of the BBOX (defaults to OGC:CRS84)."),
        crs: str = Query("EPSG:3857", description="Coordinate Reference System."),
        width: int = Query(768, description="Width of the output image."),
        height: int = Query(768, description="Height of the output image."),
        bgcolor: Optional[str] = Query(None, description="Background color of the map."),
        transparent: bool = Query(True, description="Whether the map background should be transparent."),
        datetime: Optional[str] = Query(None, description="Temporal filter (timestamp or interval)."),
        subset: Optional[str] = Query(None, description="Custom dimension filter."),
        f: str = Query("png", description="Output format: png | jpeg | geotiff."),
        style_url: Optional[str] = Query(
            None,
            alias="style-url",
            description="External SLD URL to apply to raster map rendering.",
        ),
    ):
        """Render a map for a specific collection with an explicit style under the aligned path.

        Resolves catalog_id and collection_id from external (public) IDs to
        internal immutable IDs, enforces visibility, then delegates to
        ``_get_map_impl``.
        """
        catalogs_svc = get_protocol(CatalogsProtocol)
        if not catalogs_svc:
            raise HTTPException(status_code=500, detail="Catalogs service not available.")

        internal_catalog_id = await resolve_internal_catalog_id_or_404(
            catalogs_svc, catalog_id
        )

        internal_collection_id = await resolve_internal_collection_id_or_404(
            catalogs_svc, internal_catalog_id, collection_id
        )

        from dynastore.models.protocols.visibility import resolve_collection_listing_ids
        visible_ids = await resolve_collection_listing_ids(internal_catalog_id)
        if visible_ids is not None and internal_collection_id not in visible_ids:
            raise HTTPException(status_code=404, detail="Collection not found.")

        return await MapsService._get_map_impl(
            dataset=internal_catalog_id,
            request=request,
            collections=internal_collection_id,
            bbox=bbox,
            bbox_crs=bbox_crs,
            crs=crs,
            width=width,
            height=height,
            style=style_id,
            style_url=style_url,
            bgcolor=bgcolor,
            transparent=transparent,
            datetime=datetime,
            subset=subset,
            f=f,
        )
