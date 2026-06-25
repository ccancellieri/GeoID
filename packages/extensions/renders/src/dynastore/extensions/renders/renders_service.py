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

"""OGC Renders extension — styled raster tile service for single-band COG assets.

Scope: Slice 1 — single-band COG → SLD colormap → PNG/WebP tile, with
bucket-cache reuse (same per-catalog bucket as the vector tile cache) and
STAC ``render:renders`` enrichment via the ``RendersStacContributor``.

Route pattern::

    GET /renders/catalogs/{catalog_id}/collections/{collection_id}
        /styles/{style_id}/tiles/{tms_id}/{z}/{x}/{y}.{format}

``catalog_id`` and ``collection_id`` are EXTERNAL (public) IDs in the path.
They are resolved to INTERNAL IDs at the request boundary, before any cache
key is built or any DB/storage call is made.

Deferred (later slices):
- OGC Maps ``/map`` endpoint (Slice 2)
- CQL2 style-binding selectors (Slice 3)
- Terrain-RGB / hillshade (Slice 4)
- Multiband RGB composite (Slice 5)
"""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from typing import List, Literal, Optional

import rio_tiler as _rio_tiler_scope_gate  # noqa: F401  # SCOPE gate: requires rio-tiler
_ = _rio_tiler_scope_gate  # silence pyright "unused"

from fastapi import (  # noqa: E402
    APIRouter,
    BackgroundTasks,
    FastAPI,
    HTTPException,
    Path,
    Request,
)
from fastapi.responses import RedirectResponse, Response  # noqa: E402

from dynastore.extensions import protocols  # noqa: E402
from dynastore.extensions.ogc_base import OGCServiceMixin  # noqa: E402
from dynastore.models.protocols import StylesProtocol  # noqa: E402
from dynastore.modules.concurrency import run_in_thread  # noqa: E402
from dynastore.modules.renders.colormap import parse_sld_colormap  # noqa: E402
from dynastore.modules.renders.config import RenderCachingConfig, build_render_cache_key  # noqa: E402
from dynastore.modules.renders.engine import render_cog_tile  # noqa: E402
from dynastore.modules.tiles.tiles_module import TileStorageProtocol  # noqa: E402
from dynastore.tools.discovery import get_protocol  # noqa: E402

logger = logging.getLogger(__name__)

_FORMAT_MEDIA_TYPE: dict[str, str] = {
    "png": "image/png",
    "webp": "image/webp",
}

# Only WebMercatorQuad is supported in Slice 1.
_SUPPORTED_TMS = frozenset({"WebMercatorQuad"})


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
    """Single-band COG → styled raster tile service.

    Route prefix: ``/renders``.  Reuses the per-catalog GCS bucket via the
    existing ``TileStorageProtocol`` so no new storage infrastructure is
    needed. Registers ``RendersStacContributor`` at lifespan so STAC reads
    in the same process advertise ``render:renders`` entries for COG items.
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
    ) -> Response:
        """Render a single-band COG asset tile with SLD colormap styling.

        Flow:
        1. Validate format and TMS.
        2. Resolve external catalog/collection IDs to internal immutable IDs.
        3. Check bucket cache (signed-URL redirect on hit).
        4. Fetch the style's SLD body and parse its ColorMap.
        5. Resolve the first COG asset href from the collection's items.
        6. Render via rio-tiler (run_in_thread to avoid blocking the event loop).
        7. Write rendered bytes to bucket cache in background.
        """
        start = time.perf_counter()

        # 1. Validate format and TMS
        fmt_lower = format.lower()
        if fmt_lower not in _FORMAT_MEDIA_TYPE:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported format '{format}'. Use 'png' or 'webp'.",
            )
        output_format: Literal["PNG", "WEBP"] = "PNG" if fmt_lower == "png" else "WEBP"

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
        cache_key = build_render_cache_key(
            cfg.key_prefix,
            internal_collection_id,
            style_id,
            tms_id,
            z,
            x,
            y,
            fmt_lower,
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
        if not sld_body:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Style '{style_id}' has no SLD stylesheet. "
                    "Single-band raster rendering requires an SLD ColorMap."
                ),
            )

        try:
            colormap = parse_sld_colormap(sld_body)
        except ValueError as exc:
            raise HTTPException(
                status_code=422,
                detail=f"SLD colormap parse failed: {exc}",
            ) from exc

        if not colormap:
            logger.warning(
                "renders: empty colormap for style=%s collection=%s/%s — rendering without colormap",
                style_id, internal_catalog_id, internal_collection_id,
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
            "renders: cache=miss catalog=%s collection=%s style=%s z=%s x=%s y=%s fmt=%s",
            internal_catalog_id, internal_collection_id, style_id, z, x, y, fmt_lower,
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
