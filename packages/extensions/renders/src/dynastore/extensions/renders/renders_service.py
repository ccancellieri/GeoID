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

"""OGC Renders extension — styled raster tile service for COG assets.

Scope: Slice 1 — single-band COG → SLD colormap → PNG/WebP tile, with
bucket-cache reuse (same per-catalog bucket as the vector tile cache) and
STAC ``render:renders`` enrichment via the ``RendersStacContributor``.

The tile endpoint accepts optional multiband / band-math params:

- ``bands`` — comma-separated 1-based band indices for RGB composites
  (e.g. ``3,2,1`` for true-colour).  Overrides the single-band ``band`` param.
- ``expression`` — band-math expression evaluated by rio-tiler / numexpr
  (e.g. ``(B1-B2)/(B1+B2)`` for NDVI).  Overrides ``bands`` when supplied.
- ``rescale`` — per-band rescale ranges as ``min,max`` pairs separated by
  semicolons (e.g. ``0,3000;0,3000;0,3000`` for three bands).

Each distinct combination of ``bands`` / ``expression`` / ``rescale`` maps to a
separate cache blob via a ``params_hash`` suffix on the style segment of the key.

Elevation endpoints:

- Terrain-RGB endpoint: encodes a single-band elevation COG to the Mapbox
  Terrain-RGB PNG scheme so MapLibre can consume it as a ``raster-dem`` source.
- Hillshade endpoint: produces shaded-relief + hypsometric-colormap RGBA tiles
  driven by the same SLD style-resolution path as the coloured raster route.
- ``terrain_viewer.html``: in-browser MapLibre viewer with terrain, hillshade,
  colormap overlay, and a vertical-exaggeration / on-off toggle.

Route patterns::

    GET /renders/catalogs/{catalog_id}/collections/{collection_id}
        /styles/{style_id}/tiles/{tms_id}/{z}/{x}/{y}.{format}
        ?bands=3,2,1&rescale=0,3000;0,3000;0,3000

    GET /renders/catalogs/{catalog_id}/collections/{collection_id}
        /terrain-rgb/{tms_id}/{z}/{x}/{y}.png

    GET /renders/catalogs/{catalog_id}/collections/{collection_id}
        /styles/{style_id}/hillshade/{tms_id}/{z}/{x}/{y}.png

``catalog_id`` and ``collection_id`` are EXTERNAL (public) IDs in the path.
They are resolved to INTERNAL IDs at the request boundary, before any cache
key is built or any DB/storage call is made.
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager
from typing import List, Literal, Optional, Tuple

import rio_tiler as _rio_tiler_scope_gate  # noqa: F401  # SCOPE gate: requires rio-tiler
_ = _rio_tiler_scope_gate  # silence pyright "unused"

from dynastore.extensions.web.decorators import expose_web_page  # noqa: E402

from fastapi import (  # noqa: E402
    APIRouter,
    BackgroundTasks,
    FastAPI,
    HTTPException,
    Path,
    Query,
    Request,
)
from fastapi.responses import RedirectResponse, Response  # noqa: E402

from dynastore.extensions import protocols  # noqa: E402
from dynastore.extensions.ogc_base import OGCServiceMixin  # noqa: E402
from dynastore.models.protocols import StylesProtocol  # noqa: E402
from dynastore.modules.concurrency import run_in_thread  # noqa: E402
from dynastore.modules.renders.colormap import parse_sld_colormap  # noqa: E402
from dynastore.modules.renders.config import (  # noqa: E402
    RenderCachingConfig,
    build_render_cache_key,
    build_render_params_hash,
)
from dynastore.modules.renders.engine import (  # noqa: E402
    render_cog_hillshade,
    render_cog_terrain_rgb,
    render_cog_tile,
)
from dynastore.modules.tiles.tiles_module import TileStorageProtocol  # noqa: E402
from dynastore.tools.discovery import get_protocol  # noqa: E402

logger = logging.getLogger(__name__)

_FORMAT_MEDIA_TYPE: dict[str, str] = {
    "png": "image/png",
    "webp": "image/webp",
}

# Only WebMercatorQuad is supported in Slice 1.
_SUPPORTED_TMS = frozenset({"WebMercatorQuad"})


def _parse_multiband_params(
    bands_str: Optional[str],
    expression: Optional[str],
    rescale_str: Optional[str],
) -> Tuple[
    Optional[List[int]],
    Optional[str],
    Optional[List[Tuple[float, float]]],
]:
    """Parse and validate the optional multiband query params.

    Args:
        bands_str: Comma-separated band indices string (e.g. ``"3,2,1"``).
        expression: Band-math expression (passed through unchanged).
        rescale_str: Semicolon-separated ``"min,max"`` pairs
            (e.g. ``"0,3000;0,3000;0,3000"``).

    Returns:
        Tuple of ``(bands, expression, rescale)`` with types suitable for
        ``render_cog_tile`` / ``render_cog_map``.  Any unparseable component
        raises ``HTTPException(400)``.
    """
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


async def _load_render_caching_config() -> RenderCachingConfig:
    """Fetch live ``RenderCachingConfig``; fall back to defaults if unavailable.

    Mirrors the ``_load_caching_config`` pattern from ``gcp/tiles_storage.py``.
    """
    from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
    from dynastore.tools.discovery import get_protocol as _get_protocol

    mgr = _get_protocol(PlatformConfigsProtocol)
    if mgr is None:
        return RenderCachingConfig()
    try:
        cfg = await mgr.get_config(RenderCachingConfig)
    except Exception as exc:
        logger.debug("RenderCachingConfig: get_config failed (%s); using defaults", exc)
        return RenderCachingConfig()
    return cfg if isinstance(cfg, RenderCachingConfig) else RenderCachingConfig()


class RendersService(protocols.ExtensionProtocol, OGCServiceMixin):
    """COG raster tile service with SLD colormap and multiband rendering.

    Route prefix: ``/renders``.  Reuses the per-catalog GCS bucket via the
    existing ``TileStorageProtocol`` so no new storage infrastructure is
    needed. Registers ``RendersStacContributor`` at lifespan so STAC reads
    in the same process advertise ``render:renders`` entries for COG items.

    Supports single-band and multiband / band-math rendering through the same
    route (the optional ``bands``, ``expression``, and ``rescale`` query params
    select the render recipe), plus terrain-RGB and hillshade routes and an
    in-browser terrain viewer page.
    """

    priority: int = 100
    conformance_uris: List[str] = []
    prefix = "/renders"
    protocol_title = "DynaStore Raster Tile Renders"
    protocol_description = "Styled PNG/WebP tiles rendered from COG assets via rio-tiler"
    router: APIRouter

    def __init__(self, app: Optional[FastAPI] = None):
        super().__init__()
        self.app = app
        self.router = APIRouter(tags=["Renders"], prefix="/renders")
        self._register_routes()
        logger.info("RendersService: initialised.")

    def configure_app(self, app: FastAPI) -> None:
        pass

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        from dynastore.tools.discovery import register_plugin, unregister_plugin
        from .stac_contributor import RendersStacContributor

        contributor = RendersStacContributor()
        register_plugin(contributor)
        try:
            yield
        finally:
            unregister_plugin(contributor)

    def register_policies(self) -> None:
        # Policies are declared in presets/__init__.py via register_ogc_preset
        # and applied when the operator runs the IAM preset. No inline policy
        # declarations here.
        pass

    # ------------------------------------------------------------------
    # Web page + static assets
    # ------------------------------------------------------------------

    def get_web_pages(self):
        from dynastore.extensions.tools.web_collect import collect_web_pages
        return collect_web_pages(self)

    def get_static_assets(self):
        from dynastore.extensions.tools.web_collect import collect_static_assets
        return collect_static_assets(self)

    def _serve_page_template(self, filename: str) -> Response:
        from dynastore._version import VERSION
        file_path = os.path.join(os.path.dirname(__file__), "static", filename)
        if not os.path.exists(file_path):
            return Response(content=f"Template {filename} not found", status_code=404)
        with open(file_path, "r", encoding="utf-8") as fh:
            return Response(
                content=fh.read().replace("{{VERSION}}", VERSION),
                media_type="text/html",
            )

    @expose_web_page(
        page_id="terrain_viewer",
        title="Terrain Viewer",
        icon="fa-mountain",
        description="3D terrain with hillshade and colormap overlay from a DEM COG.",
        priority=90,
    )
    async def provide_terrain_viewer(self, request: Request) -> Response:
        return self._serve_page_template("terrain_viewer.html")

    # ------------------------------------------------------------------
    # Route registration
    # ------------------------------------------------------------------

    def _register_routes(self) -> None:
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}"
            "/styles/{style_id}/tiles/{tms_id}/{z}/{x}/{y}.{format}",
            self.get_render_tile,
            methods=["GET"],
            summary=(
                "Render a styled raster tile from a COG asset. "
                "catalog_id and collection_id are public (external) IDs."
            ),
            name="get_render_tile",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}"
            "/terrain-rgb/{tms_id}/{z}/{x}/{y}.png",
            self.get_terrain_rgb_tile,
            methods=["GET"],
            summary=(
                "Encode a single-band elevation COG tile to Terrain-RGB PNG "
                "(Mapbox raster-dem scheme). catalog_id and collection_id are "
                "public (external) IDs."
            ),
            name="get_terrain_rgb_tile",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}"
            "/styles/{style_id}/hillshade/{tms_id}/{z}/{x}/{y}.png",
            self.get_hillshade_tile,
            methods=["GET"],
            summary=(
                "Render a shaded-relief (hillshade) + hypsometric-colormap RGBA tile "
                "from a single-band elevation COG. catalog_id and collection_id are "
                "public (external) IDs."
            ),
            name="get_hillshade_tile",
        )

    # ------------------------------------------------------------------
    # Route handler
    # ------------------------------------------------------------------

    async def get_render_tile(
        self,
        request: Request,
        background_tasks: BackgroundTasks,
        catalog_id: str = Path(..., description="Public catalog ID (external_id)."),
        collection_id: str = Path(..., description="Public collection ID (external_id)."),
        style_id: str = Path(..., description="Style ID registered for this collection."),
        tms_id: str = Path(..., description="Tile Matrix Set ID. Only WebMercatorQuad is supported."),
        z: int = Path(..., ge=0, le=30, description="Zoom level."),
        x: int = Path(..., ge=0, description="Tile column."),
        y: int = Path(..., ge=0, description="Tile row."),
        format: str = Path(..., description="Output image format: 'png' or 'webp'."),
        bands: Optional[str] = Query(
            None,
            description=(
                "Comma-separated 1-based band indices for multiband / RGB composite "
                "rendering (e.g. '3,2,1' for true-colour). "
                "Overrides the single-band default. "
                "Ignored when 'expression' is also supplied."
            ),
        ),
        expression: Optional[str] = Query(
            None,
            description=(
                "Band-math expression evaluated by rio-tiler "
                "(e.g. '(B1-B2)/(B1+B2)' for NDVI). "
                "Takes precedence over 'bands'."
            ),
        ),
        rescale: Optional[str] = Query(
            None,
            description=(
                "Per-band rescale ranges as semicolon-separated 'min,max' pairs, "
                "one per output band (e.g. '0,3000;0,3000;0,3000'). "
                "Applied before rendering to normalise pixel values."
            ),
        ),
    ) -> Response:
        """Render a COG asset tile with optional SLD colormap and multiband params.

        Flow:
        1. Validate format and TMS; parse optional multiband params.
        2. Resolve external catalog/collection IDs to internal immutable IDs.
        3. Check bucket cache (signed-URL redirect on hit).
        4. Fetch the style's SLD body and parse its ColorMap.
        5. Resolve the first COG asset href from the collection's items.
        6. Render via rio-tiler (run_in_thread to avoid blocking the event loop).
        7. Write rendered bytes to bucket cache in background.
        """
        start = time.perf_counter()

        # 1. Validate format and TMS; parse optional multiband params.
        fmt_lower = format.lower()
        if fmt_lower not in _FORMAT_MEDIA_TYPE:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported format '{format}'. Use 'png' or 'webp'.",
            )
        output_format: Literal["PNG", "WEBP"] = "PNG" if fmt_lower == "png" else "WEBP"

        bands_parsed, expression_parsed, rescale_parsed = _parse_multiband_params(
            bands, expression, rescale
        )

        if tms_id not in _SUPPORTED_TMS:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"TMS '{tms_id}' not supported. "
                    f"Supported: {sorted(_SUPPORTED_TMS)}"
                ),
            )

        # 2. Resolve external → internal IDs (request boundary)
        catalogs_svc = await self._get_catalogs_service()
        try:
            internal_catalog_id = await catalogs_svc.resolve_catalog_id(
                catalog_id, allow_missing=False
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        if not internal_catalog_id:
            raise HTTPException(
                status_code=404, detail=f"Catalog '{catalog_id}' not found."
            )

        try:
            internal_collection_id = await catalogs_svc.collections.resolve_collection_id(
                internal_catalog_id, collection_id, allow_missing=False
            )
        except (ValueError, AttributeError):
            # Fall back: some implementations may not have resolve_collection_id
            # (e.g. test stubs). Use the provided id as-is — downstream get_collection
            # will 404 if it is truly missing.
            internal_collection_id = collection_id

        if not internal_collection_id:
            raise HTTPException(
                status_code=404,
                detail=f"Collection '{collection_id}' not found.",
            )

        # Visibility guard (mirrors tiles service pattern)
        await self._require_collection_visible(internal_catalog_id, internal_collection_id)

        # 3. Load cache config and check bucket cache
        cfg = await _load_render_caching_config()
        params_hash = build_render_params_hash(
            bands=bands_parsed,
            expression=expression_parsed,
            rescale=rescale_parsed,
        )
        cache_key = build_render_cache_key(
            cfg.key_prefix,
            internal_collection_id,
            style_id,
            tms_id,
            z,
            x,
            y,
            fmt_lower,
            params_hash=params_hash,
        )

        provider = get_protocol(TileStorageProtocol)
        if provider and cfg.cache_enabled:
            res = await self._try_render_cache(
                provider, internal_catalog_id, cache_key, tms_id, z, x, y, fmt_lower, start
            )
            if res is not None:
                return res

        # 4. Resolve style → SLD body → colormap
        styles_svc = get_protocol(StylesProtocol)
        if not styles_svc:
            raise HTTPException(
                status_code=500, detail="Styles service not available."
            )

        style_obj = await styles_svc.get_style(
            internal_catalog_id, internal_collection_id, style_id
        )
        if not style_obj:
            raise HTTPException(
                status_code=404,
                detail=f"Style '{style_id}' not found for collection '{collection_id}'.",
            )

        sld_body = self._extract_sld_body(style_obj)
        colormap = None
        if sld_body:
            try:
                colormap = parse_sld_colormap(sld_body) or None
            except ValueError as exc:
                raise HTTPException(
                    status_code=422,
                    detail=f"SLD colormap parse failed: {exc}",
                ) from exc
        else:
            # Multiband / expression renders do not require an SLD stylesheet;
            # single-band renders without a colormap produce greyscale output.
            if not bands_parsed and not expression_parsed:
                logger.warning(
                    "renders: style=%s has no SLD stylesheet — rendering greyscale "
                    "(supply 'bands' or 'expression' for multiband output)",
                    style_id,
                )

        # 5. Resolve COG href from the collection's first item
        item = await self._get_first_item(internal_catalog_id, internal_collection_id)
        if not item:
            raise HTTPException(
                status_code=404,
                detail=f"Collection '{collection_id}' has no items.",
            )

        from dynastore.extensions.ogc_base import ogc_asset_href
        try:
            cog_href = ogc_asset_href(
                item,
                error_detail=(
                    f"No COG asset href found for collection '{collection_id}'. "
                    "Ensure at least one item carries a 'data' or 'coverage' asset."
                ),
            )
        except HTTPException:
            raise

        # 6. Render via rio-tiler in a thread (avoids blocking the event loop)
        logger.info(
            "renders: cache=miss catalog=%s collection=%s style=%s z=%s x=%s y=%s fmt=%s"
            " bands=%s expression=%s",
            internal_catalog_id, internal_collection_id, style_id, z, x, y, fmt_lower,
            bands_parsed, expression_parsed,
        )
        try:
            tile_bytes = await run_in_thread(
                render_cog_tile,
                cog_href,
                z,
                x,
                y,
                colormap=colormap or None,
                output_format=output_format,
                bands=bands_parsed,
                expression=expression_parsed,
                rescale=rescale_parsed,
            )
        except Exception as exc:
            logger.error(
                "renders: rio-tiler failed for %s/%s z=%s x=%s y=%s: %s",
                internal_catalog_id, internal_collection_id, z, x, y, exc,
                exc_info=True,
            )
            raise HTTPException(
                status_code=500,
                detail=f"Raster render failed: {exc}",
            ) from exc

        # 7. Background write to bucket cache
        if provider and cfg.cache_enabled and tile_bytes:
            background_tasks.add_task(
                provider.save_tile,
                internal_catalog_id,
                cache_key,
                tms_id,
                z,
                x,
                y,
                tile_bytes,
                fmt_lower,
            )

        duration_ms = (time.perf_counter() - start) * 1000
        logger.info(
            "renders: rendered catalog=%s collection=%s style=%s z=%s x=%s y=%s "
            "fmt=%s duration_ms=%.2f bytes=%d",
            internal_catalog_id, internal_collection_id, style_id,
            z, x, y, fmt_lower, duration_ms, len(tile_bytes) if tile_bytes else 0,
        )

        media_type = _FORMAT_MEDIA_TYPE[fmt_lower]
        return Response(
            content=tile_bytes,
            media_type=media_type,
            headers={
                "X-Render-Cache": "miss",
                "X-Render-Source": "rio-tiler",
                "Cache-Control": f"public, max-age={cfg.ttl_seconds}",
            },
        )

    # ------------------------------------------------------------------
    # Terrain-RGB tile handler
    # ------------------------------------------------------------------

    async def get_terrain_rgb_tile(
        self,
        request: Request,
        background_tasks: BackgroundTasks,
        catalog_id: str = Path(..., description="Public catalog ID (external_id)."),
        collection_id: str = Path(..., description="Public collection ID (external_id)."),
        tms_id: str = Path(..., description="Tile Matrix Set ID. Only WebMercatorQuad is supported."),
        z: int = Path(..., ge=0, le=30, description="Zoom level."),
        x: int = Path(..., ge=0, description="Tile column."),
        y: int = Path(..., ge=0, description="Tile row."),
        band: int = Query(default=1, ge=1, description="Elevation band index (1-based)."),
    ) -> Response:
        """Encode a single-band elevation COG to a Terrain-RGB PNG tile.

        The Mapbox Terrain-RGB encoding allows MapLibre to consume this tile
        as a ``raster-dem`` source for 3-D terrain and vertical exaggeration.
        Tiles are cached in the per-catalog bucket under the ``terrain-rgb``
        virtual style segment.

        Flow:
        1. Validate TMS.
        2. Resolve external catalog/collection IDs to internal immutable IDs.
        3. Check bucket cache (307 redirect on hit).
        4. Resolve the first COG asset href.
        5. Render via ``render_cog_terrain_rgb`` (run_in_thread).
        6. Write rendered bytes to bucket cache in background.
        """
        start = time.perf_counter()

        if tms_id not in _SUPPORTED_TMS:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"TMS '{tms_id}' not supported. "
                    f"Supported: {sorted(_SUPPORTED_TMS)}"
                ),
            )

        # Resolve external → internal IDs
        catalogs_svc = await self._get_catalogs_service()
        try:
            internal_catalog_id = await catalogs_svc.resolve_catalog_id(
                catalog_id, allow_missing=False
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        if not internal_catalog_id:
            raise HTTPException(status_code=404, detail=f"Catalog '{catalog_id}' not found.")

        try:
            internal_collection_id = await catalogs_svc.collections.resolve_collection_id(
                internal_catalog_id, collection_id, allow_missing=False
            )
        except (ValueError, AttributeError):
            internal_collection_id = collection_id

        if not internal_collection_id:
            raise HTTPException(
                status_code=404, detail=f"Collection '{collection_id}' not found."
            )

        await self._require_collection_visible(internal_catalog_id, internal_collection_id)

        # Cache check — virtual style_id "terrain-rgb" ensures no collision with named styles
        cfg = await _load_render_caching_config()
        cache_key = build_render_cache_key(
            cfg.key_prefix,
            internal_collection_id,
            "terrain-rgb",
            tms_id,
            z,
            x,
            y,
            "png",
        )

        provider = get_protocol(TileStorageProtocol)
        if provider and cfg.cache_enabled:
            res = await self._try_render_cache(
                provider, internal_catalog_id, cache_key, tms_id, z, x, y, "png", start
            )
            if res is not None:
                return res

        # Resolve COG href
        item = await self._get_first_item(internal_catalog_id, internal_collection_id)
        if not item:
            raise HTTPException(
                status_code=404, detail=f"Collection '{collection_id}' has no items."
            )

        from dynastore.extensions.ogc_base import ogc_asset_href
        cog_href = ogc_asset_href(
            item,
            error_detail=(
                f"No COG asset href found for collection '{collection_id}'."
            ),
        )

        logger.info(
            "renders: terrain-rgb cache=miss catalog=%s collection=%s z=%s x=%s y=%s",
            internal_catalog_id, internal_collection_id, z, x, y,
        )
        try:
            tile_bytes = await run_in_thread(
                render_cog_terrain_rgb,
                cog_href,
                z,
                x,
                y,
                band=band,
            )
        except Exception as exc:
            logger.error(
                "renders: terrain-rgb failed for %s/%s z=%s x=%s y=%s: %s",
                internal_catalog_id, internal_collection_id, z, x, y, exc,
                exc_info=True,
            )
            raise HTTPException(
                status_code=500, detail=f"Terrain-RGB render failed: {exc}"
            ) from exc

        if provider and cfg.cache_enabled and tile_bytes:
            background_tasks.add_task(
                provider.save_tile,
                internal_catalog_id,
                cache_key,
                tms_id,
                z,
                x,
                y,
                tile_bytes,
                "png",
            )

        duration_ms = (time.perf_counter() - start) * 1000
        logger.info(
            "renders: terrain-rgb catalog=%s collection=%s z=%s x=%s y=%s "
            "duration_ms=%.2f bytes=%d",
            internal_catalog_id, internal_collection_id, z, x, y,
            duration_ms, len(tile_bytes) if tile_bytes else 0,
        )
        return Response(
            content=tile_bytes,
            media_type="image/png",
            headers={
                "X-Render-Cache": "miss",
                "X-Render-Source": "rio-tiler-terrain-rgb",
                "Cache-Control": f"public, max-age={cfg.ttl_seconds}",
            },
        )

    # ------------------------------------------------------------------
    # Hillshade tile handler
    # ------------------------------------------------------------------

    async def get_hillshade_tile(
        self,
        request: Request,
        background_tasks: BackgroundTasks,
        catalog_id: str = Path(..., description="Public catalog ID (external_id)."),
        collection_id: str = Path(..., description="Public collection ID (external_id)."),
        style_id: str = Path(..., description="Style ID registered for this collection."),
        tms_id: str = Path(..., description="Tile Matrix Set ID. Only WebMercatorQuad is supported."),
        z: int = Path(..., ge=0, le=30, description="Zoom level."),
        x: int = Path(..., ge=0, description="Tile column."),
        y: int = Path(..., ge=0, description="Tile row."),
        band: int = Query(default=1, ge=1, description="Elevation band index (1-based)."),
        azimuth: float = Query(default=315.0, ge=0.0, lt=360.0, description="Sun azimuth in degrees (0=North, clockwise)."),
        altitude: float = Query(default=45.0, ge=0.0, le=90.0, description="Sun altitude above horizon in degrees."),
    ) -> Response:
        """Render a shaded-relief (hillshade) RGBA tile from an elevation COG.

        Applies the SLD colormap from the named style as a hypsometric tinting
        layer modulated by hillshade intensity. When the style has no ColorMap,
        a greyscale hillshade is returned.

        Flow:
        1. Validate TMS.
        2. Resolve external catalog/collection IDs to internal immutable IDs.
        3. Check bucket cache.
        4. Fetch the style's SLD body and parse its ColorMap (optional).
        5. Resolve the first COG asset href.
        6. Render via ``render_cog_hillshade`` (run_in_thread).
        7. Write rendered bytes to bucket cache in background.
        """
        start = time.perf_counter()

        if tms_id not in _SUPPORTED_TMS:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"TMS '{tms_id}' not supported. "
                    f"Supported: {sorted(_SUPPORTED_TMS)}"
                ),
            )

        # Resolve external → internal IDs
        catalogs_svc = await self._get_catalogs_service()
        try:
            internal_catalog_id = await catalogs_svc.resolve_catalog_id(
                catalog_id, allow_missing=False
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        if not internal_catalog_id:
            raise HTTPException(status_code=404, detail=f"Catalog '{catalog_id}' not found.")

        try:
            internal_collection_id = await catalogs_svc.collections.resolve_collection_id(
                internal_catalog_id, collection_id, allow_missing=False
            )
        except (ValueError, AttributeError):
            internal_collection_id = collection_id

        if not internal_collection_id:
            raise HTTPException(
                status_code=404, detail=f"Collection '{collection_id}' not found."
            )

        await self._require_collection_visible(internal_catalog_id, internal_collection_id)

        # Cache key includes azimuth+altitude so different lighting is cached separately
        az_int = int(round(azimuth))
        alt_int = int(round(altitude))
        hillshade_style_segment = f"hillshade-{style_id}-az{az_int}-alt{alt_int}"
        cfg = await _load_render_caching_config()
        cache_key = build_render_cache_key(
            cfg.key_prefix,
            internal_collection_id,
            hillshade_style_segment,
            tms_id,
            z,
            x,
            y,
            "png",
        )

        provider = get_protocol(TileStorageProtocol)
        if provider and cfg.cache_enabled:
            res = await self._try_render_cache(
                provider, internal_catalog_id, cache_key, tms_id, z, x, y, "png", start
            )
            if res is not None:
                return res

        # Resolve style → SLD body → colormap (optional; greyscale fallback if absent)
        colormap = None
        styles_svc = get_protocol(StylesProtocol)
        if styles_svc:
            style_obj = await styles_svc.get_style(
                internal_catalog_id, internal_collection_id, style_id
            )
            if style_obj:
                sld_body = self._extract_sld_body(style_obj)
                if sld_body:
                    try:
                        colormap = parse_sld_colormap(sld_body) or None
                    except ValueError as exc:
                        logger.warning(
                            "renders: hillshade SLD parse failed for style=%s: %s — "
                            "falling back to greyscale",
                            style_id, exc,
                        )

        # Resolve COG href
        item = await self._get_first_item(internal_catalog_id, internal_collection_id)
        if not item:
            raise HTTPException(
                status_code=404, detail=f"Collection '{collection_id}' has no items."
            )

        from dynastore.extensions.ogc_base import ogc_asset_href
        cog_href = ogc_asset_href(
            item,
            error_detail=(
                f"No COG asset href found for collection '{collection_id}'."
            ),
        )

        logger.info(
            "renders: hillshade cache=miss catalog=%s collection=%s style=%s "
            "azimuth=%.1f altitude=%.1f z=%s x=%s y=%s",
            internal_catalog_id, internal_collection_id, style_id,
            azimuth, altitude, z, x, y,
        )
        try:
            tile_bytes = await run_in_thread(
                render_cog_hillshade,
                cog_href,
                z,
                x,
                y,
                band=band,
                azimuth=azimuth,
                altitude=altitude,
                colormap=colormap,
            )
        except Exception as exc:
            logger.error(
                "renders: hillshade failed for %s/%s z=%s x=%s y=%s: %s",
                internal_catalog_id, internal_collection_id, z, x, y, exc,
                exc_info=True,
            )
            raise HTTPException(
                status_code=500, detail=f"Hillshade render failed: {exc}"
            ) from exc

        if provider and cfg.cache_enabled and tile_bytes:
            background_tasks.add_task(
                provider.save_tile,
                internal_catalog_id,
                cache_key,
                tms_id,
                z,
                x,
                y,
                tile_bytes,
                "png",
            )

        duration_ms = (time.perf_counter() - start) * 1000
        logger.info(
            "renders: hillshade catalog=%s collection=%s style=%s z=%s x=%s y=%s "
            "duration_ms=%.2f bytes=%d",
            internal_catalog_id, internal_collection_id, style_id,
            z, x, y, duration_ms, len(tile_bytes) if tile_bytes else 0,
        )
        return Response(
            content=tile_bytes,
            media_type="image/png",
            headers={
                "X-Render-Cache": "miss",
                "X-Render-Source": "rio-tiler-hillshade",
                "Cache-Control": f"public, max-age={cfg.ttl_seconds}",
            },
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

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
    ) -> Optional[Response]:
        """Return a 307 redirect or proxy response on a cache hit, else None."""
        try:
            url = await provider.get_tile_url(
                catalog_id, cache_key, tms_id, z, x, y, fmt
            )
            if url:
                duration_ms = (time.perf_counter() - start) * 1000
                logger.info(
                    "renders: cache=hit source=bucket_redirect catalog=%s "
                    "cache_key=%s z=%s x=%s y=%s duration_ms=%.2f",
                    catalog_id, cache_key, z, x, y, duration_ms,
                )
                return RedirectResponse(
                    url=url,
                    status_code=307,
                    headers={
                        "X-Render-Cache": "hit",
                        "X-Render-Source": "bucket_redirect",
                    },
                )

            tile = await provider.get_tile(
                catalog_id, cache_key, tms_id, z, x, y, fmt
            )
            if tile:
                duration_ms = (time.perf_counter() - start) * 1000
                logger.info(
                    "renders: cache=hit source=bucket_proxy catalog=%s "
                    "cache_key=%s z=%s x=%s y=%s duration_ms=%.2f bytes=%d",
                    catalog_id, cache_key, z, x, y, duration_ms, len(tile),
                )
                media_type = _FORMAT_MEDIA_TYPE.get(fmt, "application/octet-stream")
                return Response(
                    content=tile,
                    media_type=media_type,
                    headers={
                        "X-Render-Cache": "hit",
                        "X-Render-Source": "bucket_proxy",
                    },
                )
        except Exception as exc:
            logger.warning("renders: cache lookup failed: %s", exc)
        return None

    @staticmethod
    def _extract_sld_body(style_obj: object) -> Optional[str]:
        """Extract the SLD body string from a ``Style`` model.

        Looks for the first ``SLDContent`` stylesheet in the style object's
        ``stylesheets`` list (the shape returned by ``StylesProtocol.get_style``).
        Returns ``None`` when no SLD stylesheet is present.
        """
        from dynastore.modules.styles.models import SLDContent, StyleFormatEnum

        stylesheets = getattr(style_obj, "stylesheets", None) or []
        for sheet in stylesheets:
            content = getattr(sheet, "content", None)
            if content is None:
                continue
            if isinstance(content, SLDContent):
                return content.sld_body
            # Also handle the case where content is a dict (from JSON deserialisation)
            if isinstance(content, dict) and content.get("format") == StyleFormatEnum.SLD_1_1:
                return content.get("sld_body")
        return None
