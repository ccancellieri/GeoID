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
from typing import Any, FrozenSet, List, Optional
from concurrent.futures import ProcessPoolExecutor
from fastapi import Depends, FastAPI, APIRouter, HTTPException, Response, Query, Request, Path
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
import dynastore.modules.tiles.tiles_module as tms_manager
from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.db_config import shared_queries
from dynastore.tools.ogc_common import parse_subset_parameter
from . import maps_db
from dynastore.models.localization import LocalizedText
from .maps_models import MapsLandingPage, DatasetMaps, MapContent, Link
from .maps_config import MapsConfig
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.extensions.tools.ogc_common_models import Conformance
from dynastore.extensions.web.decorators import expose_web_page
from dynastore.extensions.tools.query import parse_hints_param
from dynastore.extensions.tools.language_utils import get_language
import os

# Imports for Tiling Support
from dynastore.modules.tiles.tiles_models import TileMatrixSetList, TileMatrixSet, TileMatrixSetRef, Link as TileLink
from dynastore.modules.tiles.tms_definitions import BUILTIN_TILE_MATRIX_SETS

logger = logging.getLogger(__name__)

# Slice 2: raster render imports — guarded so the maps extension can still
# load in environments without rio-tiler (graceful degradation: raster
# branch returns 422 when rio-tiler is absent rather than failing import).
_RENDER_COG_MAP = None
_RENDER_COG_TILE = None
_PARSE_SLD_COLORMAP = None
_BUILD_RENDER_CACHE_KEY = None
_RenderCachingConfig = None
try:
    from dynastore.modules.renders.engine import render_cog_map as _rcm, render_cog_tile as _rct  # noqa: E402
    from dynastore.modules.renders.colormap import parse_sld_colormap as _psc  # noqa: E402
    from dynastore.modules.renders.config import build_render_cache_key as _brck, RenderCachingConfig as _RCC  # noqa: E402
    _RENDER_COG_MAP = _rcm
    _RENDER_COG_TILE = _rct
    _PARSE_SLD_COLORMAP = _psc
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
    # /conf/styled-map — /styles/{styleId}/map override (Req 12).
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/styled-map",
    # /conf/png, /conf/jpeg, /conf/geotiff — advertised image content types.
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/png",
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/jpeg",
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/geotiff",
    # /conf/tilesets-map — exposes /map/tiles tileset list and /map/tiles/{tms}
    # definition resources (Req 22/23); map-tile generation (Req 24).
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/tilesets-map",
    # /conf/scaling — width/height query params accepted on /map (Req 15).
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/scaling",
    # /conf/display — bgcolor/transparent accepted on /map (Req 16).
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/display",
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
    """Shared helper to check logical and physical existence of collections."""
    catalogs_svc = get_protocol(CatalogsProtocol)
    if not catalogs_svc:
        return []

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
            physical_table_results.append(
                await shared_queries.table_exists_query.execute(
                    conn, schema=dataset, table=coll_id
                )
            )
        else:
            physical_table_results.append(False)

    valid_collections = []
    for i, coll_id in enumerate(requested_collections):
        if collection_metadata_results[i] and physical_table_results[i]:
            valid_collections.append(coll_id)
    return valid_collections

async def _resolve_raster_cog_href(
    catalog_id: str,
    collection_id: str,
) -> Optional[str]:
    """Return the first COG asset href from a raster collection, or None.

    Searches for a ``data`` or ``coverage`` asset key first, then falls back
    to the first asset carrying an ``href``.  Returns ``None`` when the
    collection has no items or no usable href.
    """
    catalogs_svc = get_protocol(CatalogsProtocol)
    if not catalogs_svc:
        return None
    try:
        from dynastore.models.query_builder import QueryRequest  # type: ignore[import]
        features = await catalogs_svc.search_items(
            catalog_id, collection_id, QueryRequest(limit=1)
        )
    except Exception:
        return None
    if not features:
        return None
    first = features[0]
    item: dict = (
        first.model_dump(by_alias=True, exclude_none=True)
        if hasattr(first, "model_dump")
        else dict(first)
    )
    assets = item.get("assets") or {}
    for key in ("data", "coverage"):
        if key in assets and assets[key].get("href"):
            return assets[key]["href"]
    for a in assets.values():
        if a.get("href"):
            return a["href"]
    return None


async def _resolve_raster_colormap(
    catalog_id: str,
    collection_id: str,
    style_name: Optional[str],
    conn: Any,
) -> Optional[Any]:
    """Parse an SLD colormap for a raster collection.

    Returns the ``RioColormap`` dict (``{int: (R,G,B,A)}``) when an SLD
    stylesheet is found and parseable, or ``None`` when no style was
    requested or no SLD stylesheet is available. A parse failure is logged
    and treated as no colormap (raw pixel values rendered).
    """
    if not style_name or _PARSE_SLD_COLORMAP is None:
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
    if style_name:
        engine = get_async_engine(request)
        async with managed_transaction(engine) as conn:
            colormap = await _resolve_raster_colormap(
                internal_catalog_id, internal_collection_id, style_name, conn
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


async def _render_raster_map_tile(
    *,
    catalog_id: str,
    collection_id: str,
    tms_id: str,
    z: int,
    x: int,
    y: int,
    tile_width: int,
    tile_height: int,
    tms_def: Any,
    style_name: Optional[str],
    request: Any,
) -> Response:
    """Raster branch for ``GET /{dataset}/map/tiles/{tms}/{z}/{x}/{y}``.

    For WebMercatorQuad the native ``render_cog_tile`` path is used (rio-tiler
    reads the correct overview directly).  For other TMS the bbox is computed
    from the tile matrix geometry and rendered via ``render_cog_map``.

    DB connection is opened only for the style lookup and released before the
    CPU-bound render (GeoID #703).
    """
    if _RENDER_COG_TILE is None or _RENDER_COG_MAP is None:
        raise HTTPException(
            status_code=422,
            detail="Raster rendering is not available: rio-tiler is not installed.",
        )

    # External → internal ID resolution at the request boundary.
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

    # Visibility guard.
    from dynastore.models.protocols.visibility import resolve_collection_listing_ids  # type: ignore[import]
    visible_ids = await resolve_collection_listing_ids(internal_catalog_id)
    if visible_ids is not None and internal_collection_id not in visible_ids:
        raise HTTPException(status_code=404, detail="Collection not found.")

    # COG href from the first item.
    cog_href = await _resolve_raster_cog_href(internal_catalog_id, internal_collection_id)
    if not cog_href:
        raise HTTPException(
            status_code=404,
            detail=f"No COG asset found for collection '{collection_id}'.",
        )

    # Colormap from SLD style (DB connection released before render).
    colormap = None
    if style_name:
        engine = get_async_engine(request)
        async with managed_transaction(engine) as conn:
            colormap = await _resolve_raster_colormap(
                internal_catalog_id, internal_collection_id, style_name, conn
            )

    # Render in a thread.  WebMercatorQuad uses the native tile reader;
    # other TMS fall back to the bbox reader so the overview selection is
    # still correct (rio-tiler picks the best overview for the given area).
    _WMQ = "WebMercatorQuad"
    try:
        if tms_id == _WMQ:
            tile_bytes: bytes = await run_in_thread(
                _RENDER_COG_TILE,
                cog_href,
                z,
                x,
                y,
                colormap=colormap,
                output_format="PNG",
            )
        else:
            # Compute geographic bbox from tile matrix geometry (TopLeft origin).
            matrix_def = next(
                (m for m in tms_def.tileMatrices if m.id == str(z)), None
            )
            if matrix_def is None:
                raise HTTPException(
                    status_code=400,
                    detail=f"Zoom level '{z}' not found in TMS '{tms_id}'.",
                )
            px = matrix_def.tileWidth * matrix_def.cellSize
            py = matrix_def.tileHeight * matrix_def.cellSize
            min_x = matrix_def.pointOfOrigin[0] + x * px
            max_y = matrix_def.pointOfOrigin[1] - y * py
            bbox_tile = [min_x, max_y - py, min_x + px, max_y]
            tile_bytes = await run_in_thread(
                _RENDER_COG_MAP,
                cog_href,
                bbox=bbox_tile,
                width=tile_width,
                height=tile_height,
                colormap=colormap,
                output_format="PNG",
            )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(
            "maps/raster: tile render failed for %s/%s tms=%s z=%s x=%s y=%s: %s",
            internal_catalog_id, internal_collection_id, tms_id, z, x, y, exc,
            exc_info=True,
        )
        raise HTTPException(
            status_code=500, detail=f"Raster tile render failed: {exc}"
        ) from exc

    return Response(content=tile_bytes, media_type="image/png")


def _return_empty_tile(width, height):
    # Create a transparent 1x1 pixel or full size empty PNG
    # Minimal 1x1 transparent PNG signature
    empty_png = b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82'
    # To generate a full-size empty tile, a library like Pillow would be better,
    # but for now, we return a minimal valid PNG to avoid client errors.
    # from PIL import Image
    # img = Image.new('RGBA', (width, height), (255, 255, 255, 0))
    # buffer = io.BytesIO()
    # img.save(buffer, format="PNG")
    # return Response(content=buffer.getvalue(), media_type="image/png")
    return Response(content=empty_png, media_type="image/png")


class MapsService(ExtensionProtocol, OGCServiceMixin):
    priority: int = 100
    """Provides OGC API - Maps (WMS-like) functionality with filtering and Tiling."""
    conformance_uris = OGC_API_MAPS_URIS
    prefix = "/maps"
    protocol_title = "DynaStore OGC API - Maps"
    protocol_description = "Map rendering (WMS-like) with filtering and tiling"
    router:APIRouter = APIRouter(tags=["OGC API - Maps (WMS)"], prefix="/maps")
    process_pool: Optional[ProcessPoolExecutor] = None

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
        """Early configuration for the Maps extension."""
        # Web pages / static assets are discovered by WebModule via the
        # WebPageContributor / StaticAssetProvider capability protocols.
        return None

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        # Policies declared via PolicyContributor; IAM forwards centrally.
        logger.info("Maps Service startup: process pool starting...")
        MapsService.process_pool = ProcessPoolExecutor()
        app.state.maps_config = MapsConfig()
        yield
        logger.info("Maps Service shutdown: closing process pool.")
        if MapsService.process_pool:
            MapsService.process_pool.shutdown(wait=True)

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

    @router.get("/conformance", response_model=Conformance)
    async def get_maps_conformance() -> Conformance:  # type: ignore[reportGeneralTypeIssues]
        return Conformance(conformsTo=OGC_API_MAPS_URIS)

    @router.get("/", response_model=MapsLandingPage)
    async def get_maps_landing_page(request: Request):  # type: ignore[reportGeneralTypeIssues]
        catalogs_svc = get_protocol(CatalogsProtocol)
        catalogs = await catalogs_svc.list_catalogs(limit=1000) if catalogs_svc else []
        links = [Link(href=str(request.url), rel="self", type="application/json", title=LocalizedText(en="this document"))]
        for cat in catalogs:
            links.append(Link(
                href=str(request.url_for('get_dataset_maps', dataset=cat.id)),
                rel="dataset", type="application/json", title=LocalizedText(en=f"Maps for dataset '{cat.id}'")
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

    async def _serve_page_template(self, filename: str):
        from dynastore._version import VERSION
        file_path = os.path.join(os.path.dirname(__file__), "static", filename)
        if not os.path.exists(file_path):
             return Response(content=f"Template {filename} not found", status_code=404)
        with open(file_path, "r", encoding="utf-8") as f:
             return Response(content=f.read().replace("{{VERSION}}", VERSION), media_type="text/html")

    @router.get("/{dataset}", response_model=DatasetMaps)
    async def get_dataset_maps(  # type: ignore[reportGeneralTypeIssues]
        dataset: str,
        request: Request,
        request_hints: FrozenSet = Depends(parse_hints_param),
        language: str = Depends(get_language),
    ):
        # Accepted for uniform cross-protocol routing-hints support; this route
        # returns dataset-maps metadata (links) and performs no vector-geometry read.
        catalogs_svc = get_protocol(CatalogsProtocol)
        if not catalogs_svc or not await catalogs_svc.get_catalog_model(dataset):
            raise HTTPException(status_code=404, detail=f"Dataset '{dataset}' not found.")

        collections = await catalogs_svc.list_collections(dataset, limit=1000)
        maps = []
        for coll in collections:
            map_links = [
                Link(href=f"{request.url}/map?collections={coll.id}&bbox=-180,-90,180,90&crs=EPSG:4326", rel="item", type="image/png"),
                Link(href=f"{request.url}/map/tiles", rel="http://www.opengis.net/def/rel/ogc/1.0/tilesets-map", type="application/json", title=LocalizedText(en="Map Tilesets"))
            ]
            # The collection title is stored as a multi-language LocalizedText,
            # but OGC MapContent.title is a single string. Resolve to the
            # requested language (lang query param / Accept-Language, default
            # 'en'); lang='*' returns the full multi-language object.
            title = coll.title.resolve(language) if coll.title is not None else None
            maps.append(MapContent(title=title, links=map_links))
        
        links = [Link(href=str(request.url), rel="self"), Link(href=str(request.url_for('get_maps_landing_page')), rel="up")]
        return DatasetMaps(title=f"Maps for '{dataset}'", maps=maps, links=links)

    # --- Tiling Endpoints (Requirements Class "Map Tilesets") ---

    @router.get("/{dataset}/map/tiles", response_model=TileMatrixSetList, summary="Retrieve available Map Tile Matrix Sets")
    async def get_map_tilesets(dataset: str, request: Request):  # type: ignore[reportGeneralTypeIssues]
        """List all supported Tile Matrix Sets for rendering raster map tiles."""
        catalogs_svc = get_protocol(CatalogsProtocol)
        if not catalogs_svc or not await catalogs_svc.get_catalog_model(dataset):
            raise HTTPException(status_code=404, detail=f"Dataset '{dataset}' not found.")

        tms_refs = []
        # 1. Built-in TMS
        for tms_id, tms_def in BUILTIN_TILE_MATRIX_SETS.items():
            tms_refs.append(TileMatrixSetRef(
                id=tms_id,
                title=tms_def.title,
                links=[
                    TileLink(
                        href=str(request.url_for("get_map_tileset", dataset=dataset, tileMatrixSetId=tms_id)), 
                        rel="self", 
                        type="application/json",
                        title=tms_def.title)
                ]
            ))
        
        # 2. Custom TMS from DB
        custom_tms_list = await tms_manager.list_custom_tms(catalog_id=dataset)
        for tms in custom_tms_list:
            if not any(ref.id == tms.id for ref in tms_refs):
                tms_refs.append(TileMatrixSetRef(
                    id=tms.id,
                    title=tms.title,
                    links=[TileLink(
                        href=str(request.url_for("get_map_tileset", dataset=dataset, tileMatrixSetId=tms.id)), 
                        rel="self", 
                        type="application/json",
                        title=tms.title)]
                ))
        return TileMatrixSetList(tileMatrixSets=tms_refs)

    @router.get("/{dataset}/map/tiles/{tileMatrixSetId}", response_model=TileMatrixSet, summary="Retrieve a Map Tile Matrix Set definition")
    async def get_map_tileset(dataset: str, tileMatrixSetId: str = Path(..., description="The Identifier of the Tile Matrix Set")):  # type: ignore[reportGeneralTypeIssues]
        """Return the full definition of a specific Tile Matrix Set."""
        tms = await tms_manager.get_custom_tms(catalog_id=dataset, tms_id=tileMatrixSetId)
        if not tms:
            tms = BUILTIN_TILE_MATRIX_SETS.get(tileMatrixSetId)
            if not tms:
                raise HTTPException(status_code=404, detail=f"TileMatrixSet '{tileMatrixSetId}' not found.")
        return tms

    @router.get("/{dataset}/map/tiles/{tileMatrixSetId}/{z}/{x}/{y}", summary="Get Rendered Map Tile")
    async def get_map_tile(
        request: Request, dataset: str, tileMatrixSetId: str, z: str, x: int, y: int,  # type: ignore[reportGeneralTypeIssues]
        collections: str = Query(..., description="Comma-separated list of collection IDs."),
        datetime: Optional[str] = Query(None, description="Temporal filter."),
        subset: Optional[str] = Query(None, description="Custom dimension filter."),
        bgcolor: Optional[str] = Query(None),
        transparent: bool = Query(True),
        style: Optional[str] = Query(None)
    ):
        """
        Generates a raster map tile (PNG) for the specific Z/X/Y.
        """
        # 1. Fetch TMS Definition
        tms_def = await tms_manager.get_custom_tms(catalog_id=dataset, tms_id=tileMatrixSetId)
        if not tms_def:
            tms_def = BUILTIN_TILE_MATRIX_SETS.get(tileMatrixSetId)
            if not tms_def:
                raise HTTPException(status_code=404, detail=f"TileMatrixSet {tileMatrixSetId} not supported.")

        # 2. Validate Matrix (Zoom Level)
        matrix_def = next((m for m in tms_def.tileMatrices if m.id == str(z)), None)
        if not matrix_def:
            raise HTTPException(status_code=400, detail=f"Zoom level '{z}' not found in TMS '{tileMatrixSetId}'.")

        # 3. Validate Coordinates
        if not (0 <= x < matrix_def.matrixWidth and 0 <= y < matrix_def.matrixHeight):
            raise HTTPException(status_code=400, detail="Tile coordinates out of bounds.")

        # 3a. Raster branch — when the first collection is RASTER-kind, use the
        # COG tile engine instead of the vector pipeline. DB connection is not
        # opened here; the COG path resolves items and styles without one.
        requested_collections_raw = [c.strip() for c in collections.split(',')]
        first_collection = requested_collections_raw[0] if requested_collections_raw else ""
        if first_collection and await _is_raster_collection(dataset, first_collection):
            return await _render_raster_map_tile(
                catalog_id=dataset,
                collection_id=first_collection,
                tms_id=tileMatrixSetId,
                z=int(z),
                x=x,
                y=y,
                tile_width=matrix_def.tileWidth,
                tile_height=matrix_def.tileHeight,
                tms_def=tms_def,
                style_name=style,
                request=request,
            )

        # Steps 4-8 only need the DB. Acquire a connection for that window and
        # release it before the CPU-bound render (step 9) so a pooled slot is
        # never held across run_in_executor (GeoID #703).
        engine = get_async_engine(request)
        async with managed_transaction(engine) as conn:
            ctx = DriverContext(db_resource=conn)

            # 4. Resolve CRS and SRID
            try:
                target_srid = await tms_manager.resolve_srid(conn=conn, crs_str=tms_def.crs, catalog_id=dataset)
            except Exception as e:
                logger.error(f"CRS Error: {e}")
                raise HTTPException(status_code=500, detail=f"Could not process CRS '{tms_def.crs}' in TMS '{tileMatrixSetId}'.") from e

            # 5. Calculate Bounding Box for the Tile
            # OGC Tiles usually assume TopLeft origin for the matrix
            pixel_span_x = matrix_def.tileWidth * matrix_def.cellSize
            pixel_span_y = matrix_def.tileHeight * matrix_def.cellSize

            tile_min_x = matrix_def.pointOfOrigin[0] + (x * pixel_span_x)
            tile_max_y = matrix_def.pointOfOrigin[1] - (y * pixel_span_y)
            tile_max_x = tile_min_x + pixel_span_x
            tile_min_y = tile_max_y - pixel_span_y

            bbox_list = [tile_min_x, tile_min_y, tile_max_x, tile_max_y]

            # 6. Validate Collections
            requested_collections = [c.strip() for c in collections.split(',')]
            # Reuse validation logic (check metadata + table existence)
            valid_collections = await _validate_collections_helper(conn, dataset, requested_collections)
            if not valid_collections:
                 # Return transparent empty tile if no valid data source
                 return _return_empty_tile(matrix_def.tileWidth, matrix_def.tileHeight)

            subset_params = parse_subset_parameter(subset)

            # 7. Fetch Features (Optimized for Render)
            # Note: We pass the Tile Width/Height and the TMS SRID (target_srid) as the BBOX SRID
            catalogs_svc = get_protocol(CatalogsProtocol)
            if not catalogs_svc:
                raise HTTPException(status_code=500, detail="Catalogs service not available.")
            try:
                layer_config, layers_data = await asyncio.gather(
                    catalogs_svc.get_collection_config(dataset, valid_collections[0], ctx=ctx),
                    maps_db.get_features_for_rendering(
                        conn=conn,
                        schema=dataset,
                        collections=valid_collections,
                        bbox=bbox_list,
                        crs=tms_def.crs,
                        width=matrix_def.tileWidth,
                        height=matrix_def.tileHeight,
                        bbox_srid=target_srid, # Vital: The computed BBOX is in the TMS CRS
                        datetime_str=datetime,
                        subset_params=subset_params
                    )
                )
            except ValueError as e:
                logger.error(f"Render Fetch Error: {e}")
                raise HTTPException(status_code=400, detail=str(e)) from e
            if layer_config is None or layer_config.geometry_storage is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Collection '{valid_collections[0]}' has no geometry storage config.",
                )

            # 8. Resolve style
            style_to_render = await _get_style_to_render(
                conn, dataset, valid_collections[0] if valid_collections else None, style
            )

        # 9. Render Image (CPU-bound, no DB connection held)
        try:
            loop = asyncio.get_running_loop()
            image_bytes = await loop.run_in_executor(
                MapsService.process_pool,
                render_map_image,
                matrix_def.tileWidth, matrix_def.tileHeight, 
                bbox_list, tms_def.crs, 
                layer_config.geometry_storage.target_srid, # Source SRID
                layers_data, style_to_render,
                transparent, bgcolor
            )
            return Response(content=image_bytes, media_type="image/png")
        except Exception as e:
            logger.error(f"Tile Render Failed: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Rendering failed.") from e

    # --- Existing Map Endpoint ---

    @router.get("/{dataset}/map")
    async def get_map(
        dataset: str,  # type: ignore[reportGeneralTypeIssues]
        request: Request,
        collections: str = Query(..., description="Comma-separated list of collections to render."),
        bbox: str = Query(..., description="Bounding box in CRS coordinates."),
        bbox_crs: str = Query(None, description="CRS of the BBOX (defaults to OGC:CRS84)."),
        crs: str = Query("EPSG:3857", description="Coordinate Reference System."),
        width: int = Query(768, description="Width of the output image."),
        height: int = Query(768, description="Height of the output image."),
        style: Optional[str] = Query(None, description="Name of the style to apply."),
        bgcolor: Optional[str] = Query(None, description="Background color of the map."),
        transparent: bool = Query(True, description="Whether the map background should be transparent."),
        datetime: Optional[str] = Query(None, description="Temporal filter (timestamp or interval)."),
        subset: Optional[str] = Query(None, description="Custom dimension filter."),
        f: str = Query("png", description="Output format: png | jpeg | geotiff."),
    ):
        fmt = f.lower()
        if fmt not in _SUPPORTED_MAP_FORMATS:
            raise HTTPException(
                status_code=415, detail=f"Unsupported map format: {f!r}",
            )
        # ... (Existing validation logic) ...
        catalogs_svc = get_protocol(CatalogsProtocol)
        if not catalogs_svc or not await catalogs_svc.get_catalog_model(dataset):
            raise HTTPException(status_code=404, detail=f"Dataset '{dataset}' not found.")

        requested_collections = [c.strip() for c in collections.split(',')]

        # Handle BBOX CRS (Req 18)
        # If bbox_crs is provided, extract SRID. If NOT provided, standard says default is CRS84 (4326).
        bbox_srid = 4326
        if bbox_crs:
            try:
                # Simple parsing for [EPSG:XXXX] or URIs
                import re
                match = re.search(r'(\d+)$', bbox_crs)
                if match:
                    bbox_srid = int(match.group(1))
            except Exception:
                pass # Fallback or error handling

        try:
            bbox_list = [float(coord) for coord in bbox.split(',')]
        except ValueError as e:
            raise HTTPException(status_code=400, detail="Invalid BBOX format.") from e

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
                fmt=fmt,
                request=request,
            )

        # The DB-dependent steps (collection validation, feature fetch, style
        # resolution) run under a single connection that is released before the
        # CPU-bound render below, so a pooled slot is never held across
        # run_in_executor (GeoID #703).
        engine = get_async_engine(request)
        async with managed_transaction(engine) as conn:
            ctx = DriverContext(db_resource=conn)

            valid_collections = await _validate_collections_helper(conn, dataset, requested_collections)
            if not valid_collections:
                raise HTTPException(status_code=404, detail="One or more collections not found.")

            # Fetch Data with Updated DB Signature
            if not catalogs_svc:
                raise HTTPException(status_code=500, detail="Catalogs service not available.")
            try:
                layer_config, layers_data = await asyncio.gather(
                    catalogs_svc.get_collection_config(dataset, valid_collections[0], ctx=ctx),
                    maps_db.get_features_for_rendering(
                        conn=conn,
                        schema=dataset,
                        collections=valid_collections,
                        bbox=bbox_list,
                        crs=crs,
                        width=width,
                        height=height,
                        bbox_srid=bbox_srid,
                        datetime_str=datetime,
                        subset_params=parse_subset_parameter(subset)
                    )
                )
            except ValueError as e:
                logger.error(f"Data Error: {e}")
                raise HTTPException(status_code=400, detail=str(e)) from e
            if layer_config is None or layer_config.geometry_storage is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Collection '{valid_collections[0]}' has no geometry storage config.",
                )

            # Fetch style to render
            style_to_render = await _get_style_to_render(
                conn, dataset, valid_collections[0] if valid_collections else None, style
            )

        # Render (CPU-bound, no DB connection held)
        try:
            loop = asyncio.get_running_loop()
            image_bytes = await loop.run_in_executor(
                MapsService.process_pool,
                render_map_image,
                width, height, bbox_list, crs,
                layer_config.geometry_storage.target_srid,
                layers_data, style_to_render,
                transparent, bgcolor
            )
        except Exception as e:
            logger.error(f"Render Error: {e}")
            raise HTTPException(status_code=500, detail="Failed to render map.") from e

        # Convert PNG to requested format (png passthrough; jpeg/geotiff post-process).
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