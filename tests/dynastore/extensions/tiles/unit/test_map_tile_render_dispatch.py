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

"""Characterization tests for the shared raster render-and-respond path used
by ``get_map_tile`` and ``get_map_tile_styled`` (epic #2830 D3).

These pin down the behavior of the render-dispatch step that was duplicated
across the two handlers before extraction into
``TilesService._render_raster_tile``:

- successful render -> 200, correct media type, X-Render-Cache/X-Render-Source
  headers, and the exact ``provider.save_tile`` call when caching is enabled.
- ``InvalidExpression`` -> 422 for renderers that support band expressions
  (default-style raster, hillshade, explicit style), but NOT for terrain-rgb
  (its renderer never raises that type, and the original code never checked
  for it there).
- ``TileOutsideBounds`` -> 204 for every raster branch.
- any other renderer exception -> 500 with a branch-specific ``detail``
  message and log line.

All rio-tiler calls are mocked — no C-extensions required.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fastapi import HTTPException

from dynastore.extensions.tiles.tiles_service import TilesService
import dynastore.extensions.tiles.tiles_service as _ts_mod
from dynastore.modules.catalog.catalog_config import CollectionKind


# ---------------------------------------------------------------------------
# Helpers (mirrors tests/dynastore/extensions/tiles/unit/test_map_tile_handlers.py)
# ---------------------------------------------------------------------------


def _make_service() -> TilesService:
    svc = object.__new__(TilesService)
    svc._ogc_catalogs_protocol = None  # type: ignore[attr-defined]
    svc._ogc_configs_protocol = None  # type: ignore[attr-defined]
    svc._ogc_storage_protocol = None  # type: ignore[attr-defined]
    svc._tile_cache_writer = MagicMock()  # type: ignore[attr-defined]
    return svc


def _make_request() -> MagicMock:
    req = MagicMock()
    req.headers = {}
    return req


def _make_bg_tasks() -> MagicMock:
    bg = MagicMock()
    bg.add_task = MagicMock()
    return bg


def _mock_catalogs_svc(
    catalog_result: str = "internal-cat",
    collection_result: str = "internal-coll",
) -> MagicMock:
    svc = MagicMock()
    svc.resolve_catalog_id = AsyncMock(return_value=catalog_result)
    svc.collections.resolve_collection_id = AsyncMock(return_value=collection_result)
    return svc


def _make_render_config(cache_enabled: bool = False, ttl: int = 3600) -> MagicMock:
    cfg = MagicMock()
    cfg.cache_enabled = cache_enabled
    cfg.key_prefix = "renders/collections"
    cfg.ttl_seconds = ttl
    return cfg


def _first_item_stub() -> dict:
    return {
        "assets": {"data": {"href": "https://s3/cog.tif", "roles": ["data"]}},
        "properties": {},
    }


def _wire_common_mocks(
    svc: TilesService,
    *,
    kind: CollectionKind | None = None,
) -> None:
    svc._get_catalogs_service = AsyncMock(return_value=_mock_catalogs_svc())
    svc._require_collection_visible = AsyncMock()
    svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock())
    svc._get_first_item = AsyncMock(return_value=_first_item_stub())
    if kind is not None:
        svc._collection_kind = AsyncMock(return_value=kind)


_STYLED_DEFAULTS: dict[str, Any] = dict(
    relief=None, band=1, azimuth=315.0, altitude=45.0,
    bands=None, expression=None, rescale=None, style_url=None,
)


class _FakeInvalidExpression(Exception):
    pass


_FakeInvalidExpression.__name__ = "InvalidExpression"


class _FakeTileOutsideBounds(Exception):
    pass


_FakeTileOutsideBounds.__name__ = "TileOutsideBounds"


async def _run_sync(fn, *a, **kw):
    """Stand-in for run_in_thread: calls the renderer inline."""
    return fn(*a, **kw)


def _patch_common(cfg, provider=None):
    """Context manager stack shared by every dispatch test below."""
    import dynastore.extensions.ogc_base as _ogc_base_real

    return (
        patch(
            "dynastore.extensions.tiles.tiles_service.TilesService._load_render_caching_config",
            new_callable=AsyncMock,
            return_value=cfg,
        ),
        patch("dynastore.extensions.tiles.tiles_service.get_protocol", return_value=provider),
        patch("dynastore.modules.concurrency.run_in_thread", side_effect=_run_sync),
        patch.object(_ogc_base_real, "ogc_asset_href", return_value="https://s3/cog.tif"),
        patch(
            "dynastore.modules.styles.binding_resolver.resolve_binding_style_id",
            new_callable=AsyncMock,
            return_value=None,
        ),
    )


# ---------------------------------------------------------------------------
# get_map_tile — default-style raster branch (never directly exercised by
# the pre-existing test_map_tile_handlers.py suite, which only covers
# get_map_tile_styled).
# ---------------------------------------------------------------------------


class TestGetMapTileRasterHappyPath:
    @pytest.mark.asyncio
    async def test_success_returns_200_with_headers_and_no_cache_writeback(self):
        original = _ts_mod._RENDER_COG_TILE
        try:
            fake_tile = MagicMock(return_value=b"TILE-BYTES")
            _ts_mod._RENDER_COG_TILE = fake_tile

            svc = _make_service()
            _wire_common_mocks(svc, kind=CollectionKind.RASTER)
            bg = _make_bg_tasks()
            cfg = _make_render_config(cache_enabled=False)

            p1, p2, p3, p4, p5 = _patch_common(cfg, provider=None)
            with p1, p2, p3, p4, p5:
                response = await svc.get_map_tile(
                    request=_make_request(),
                    background_tasks=bg,
                    catalog_id="cat",
                    collection_id="coll",
                    tms_id="WebMercatorQuad",
                    z=5,
                    x=0,
                    y=0,
                    format="png",
                    bands=None,
                    expression=None,
                    rescale=None,
                    style_url=None,
                )

            assert response.status_code == 200
            assert response.body == b"TILE-BYTES"
            assert response.media_type == "image/png"
            assert response.headers["X-Render-Cache"] == "miss"
            assert response.headers["X-Render-Source"] == "rio-tiler"
            svc._tile_cache_writer.submit_nowait.assert_not_called()
        finally:
            _ts_mod._RENDER_COG_TILE = original

    @pytest.mark.asyncio
    async def test_success_with_cache_enabled_writes_back_via_background_task(self):
        original = _ts_mod._RENDER_COG_TILE
        try:
            fake_tile = MagicMock(return_value=b"TILE-BYTES")
            _ts_mod._RENDER_COG_TILE = fake_tile

            svc = _make_service()
            _wire_common_mocks(svc, kind=CollectionKind.RASTER)
            bg = _make_bg_tasks()
            cfg = _make_render_config(cache_enabled=True, ttl=1234)
            provider = MagicMock()

            p1, p2, p3, p4, p5 = _patch_common(cfg, provider=provider)
            with p1, p2, p3, p4, p5:
                response = await svc.get_map_tile(
                    request=_make_request(),
                    background_tasks=bg,
                    catalog_id="cat",
                    collection_id="coll",
                    tms_id="WebMercatorQuad",
                    z=5,
                    x=1,
                    y=2,
                    format="png",
                    bands=None,
                    expression=None,
                    rescale=None,
                    style_url=None,
                )

            assert response.status_code == 200
            assert "1234" in response.headers["Cache-Control"]
            svc._tile_cache_writer.submit_nowait.assert_called_once()
            call_args = svc._tile_cache_writer.submit_nowait.call_args.args
            # (provider, catalog_id, cache_key, tms_id, z, x, y, tile_bytes, fmt)
            assert call_args[0] is provider
            assert call_args[1] == "internal-cat"
            assert call_args[3] == "WebMercatorQuad"
            assert call_args[4:7] == (5, 1, 2)
            assert call_args[7] == b"TILE-BYTES"
            assert call_args[8] == "png"
        finally:
            _ts_mod._RENDER_COG_TILE = original

    @pytest.mark.asyncio
    async def test_invalid_expression_raises_422(self):
        original = _ts_mod._RENDER_COG_TILE
        try:
            def _raise(*a, **kw):
                raise _FakeInvalidExpression("bad expr")

            _ts_mod._RENDER_COG_TILE = _raise

            svc = _make_service()
            _wire_common_mocks(svc, kind=CollectionKind.RASTER)
            cfg = _make_render_config(cache_enabled=False)

            p1, p2, p3, p4, p5 = _patch_common(cfg, provider=None)
            with p1, p2, p3, p4, p5:
                with pytest.raises(HTTPException) as exc_info:
                    await svc.get_map_tile(
                        request=_make_request(),
                        background_tasks=_make_bg_tasks(),
                        catalog_id="cat",
                        collection_id="coll",
                        tms_id="WebMercatorQuad",
                        z=5,
                        x=0,
                        y=0,
                        format="png",
                        bands=None,
                        expression="(B1-B2)/(B1+B2)",
                        rescale=None,
                        style_url=None,
                    )
            assert exc_info.value.status_code == 422
            assert "Invalid band expression" in exc_info.value.detail
        finally:
            _ts_mod._RENDER_COG_TILE = original

    @pytest.mark.asyncio
    async def test_tile_outside_bounds_returns_204(self):
        original = _ts_mod._RENDER_COG_TILE
        try:
            def _raise(*a, **kw):
                raise _FakeTileOutsideBounds("oob")

            _ts_mod._RENDER_COG_TILE = _raise

            svc = _make_service()
            _wire_common_mocks(svc, kind=CollectionKind.RASTER)
            cfg = _make_render_config(cache_enabled=False)

            p1, p2, p3, p4, p5 = _patch_common(cfg, provider=None)
            with p1, p2, p3, p4, p5:
                response = await svc.get_map_tile(
                    request=_make_request(),
                    background_tasks=_make_bg_tasks(),
                    catalog_id="cat",
                    collection_id="coll",
                    tms_id="WebMercatorQuad",
                    z=5,
                    x=0,
                    y=0,
                    format="png",
                    bands=None,
                    expression=None,
                    rescale=None,
                    style_url=None,
                )
            assert response.status_code == 204
        finally:
            _ts_mod._RENDER_COG_TILE = original

    @pytest.mark.asyncio
    async def test_generic_exception_raises_500_with_raster_render_detail(self):
        original = _ts_mod._RENDER_COG_TILE
        try:
            def _raise(*a, **kw):
                raise RuntimeError("disk on fire")

            _ts_mod._RENDER_COG_TILE = _raise

            svc = _make_service()
            _wire_common_mocks(svc, kind=CollectionKind.RASTER)
            cfg = _make_render_config(cache_enabled=False)

            p1, p2, p3, p4, p5 = _patch_common(cfg, provider=None)
            with p1, p2, p3, p4, p5:
                with pytest.raises(HTTPException) as exc_info:
                    await svc.get_map_tile(
                        request=_make_request(),
                        background_tasks=_make_bg_tasks(),
                        catalog_id="cat",
                        collection_id="coll",
                        tms_id="WebMercatorQuad",
                        z=5,
                        x=0,
                        y=0,
                        format="png",
                        bands=None,
                        expression=None,
                        rescale=None,
                        style_url=None,
                    )
            assert exc_info.value.status_code == 500
            assert exc_info.value.detail == "Raster render failed: disk on fire"
        finally:
            _ts_mod._RENDER_COG_TILE = original


# ---------------------------------------------------------------------------
# get_map_tile_styled — hillshade branch (not covered by the pre-existing
# test suite at all).
# ---------------------------------------------------------------------------


class TestHillshadeDispatch:
    @pytest.mark.asyncio
    async def test_success_returns_200_png_with_hillshade_source(self):
        original = _ts_mod._RENDER_COG_HILLSHADE
        try:
            fake_hillshade = MagicMock(return_value=b"HILLSHADE-BYTES")
            _ts_mod._RENDER_COG_HILLSHADE = fake_hillshade

            svc = _make_service()
            _wire_common_mocks(svc)
            bg = _make_bg_tasks()
            cfg = _make_render_config(cache_enabled=False)

            p1, p2, p3, p4, p5 = _patch_common(cfg, provider=None)
            with p1, p2, p3, p4, p5:
                response = await svc.get_map_tile_styled(
                    request=_make_request(),
                    background_tasks=bg,
                    catalog_id="cat",
                    collection_id="coll",
                    style_id="ndvi",
                    tms_id="WebMercatorQuad",
                    z=5,
                    x=0,
                    y=0,
                    format="png",
                    relief="hillshade",
                    band=1,
                    azimuth=315.0,
                    altitude=45.0,
                    bands=None,
                    expression=None,
                    rescale=None,
                    style_url=None,
                )

            assert response.status_code == 200
            assert response.body == b"HILLSHADE-BYTES"
            assert response.media_type == "image/png"
            assert response.headers["X-Render-Source"] == "rio-tiler-hillshade"
        finally:
            _ts_mod._RENDER_COG_HILLSHADE = original

    @pytest.mark.asyncio
    async def test_cache_writeback_uses_png_save_format(self):
        original = _ts_mod._RENDER_COG_HILLSHADE
        try:
            fake_hillshade = MagicMock(return_value=b"HILLSHADE-BYTES")
            _ts_mod._RENDER_COG_HILLSHADE = fake_hillshade

            svc = _make_service()
            _wire_common_mocks(svc)
            bg = _make_bg_tasks()
            cfg = _make_render_config(cache_enabled=True)
            provider = MagicMock()
            svc._get_style_record = AsyncMock(return_value=None)  # hillshade tolerates a missing style

            p1, p2, p3, p4, p5 = _patch_common(cfg, provider=provider)
            with p1, p2, p3, p4, p5:
                await svc.get_map_tile_styled(
                    request=_make_request(),
                    background_tasks=bg,
                    catalog_id="cat",
                    collection_id="coll",
                    style_id="ndvi",
                    tms_id="WebMercatorQuad",
                    z=5,
                    x=0,
                    y=0,
                    format="webp",  # forced to png internally for hillshade
                    relief="hillshade",
                    band=1,
                    azimuth=315.0,
                    altitude=45.0,
                   bands=None,
                   expression=None,
                   rescale=None,
                    style_url=None,
                )

            svc._tile_cache_writer.submit_nowait.assert_called_once()
            call_args = svc._tile_cache_writer.submit_nowait.call_args.args
            assert call_args[7] == b"HILLSHADE-BYTES"
            assert call_args[8] == "png"
        finally:
            _ts_mod._RENDER_COG_HILLSHADE = original

    @pytest.mark.asyncio
    async def test_invalid_expression_raises_422(self):
        original = _ts_mod._RENDER_COG_HILLSHADE
        try:
            def _raise(*a, **kw):
                raise _FakeInvalidExpression("bad expr")

            _ts_mod._RENDER_COG_HILLSHADE = _raise

            svc = _make_service()
            _wire_common_mocks(svc)
            cfg = _make_render_config(cache_enabled=False)

            p1, p2, p3, p4, p5 = _patch_common(cfg, provider=None)
            with p1, p2, p3, p4, p5:
                with pytest.raises(HTTPException) as exc_info:
                    await svc.get_map_tile_styled(
                        request=_make_request(),
                        background_tasks=_make_bg_tasks(),
                        catalog_id="cat",
                        collection_id="coll",
                        style_id="ndvi",
                        tms_id="WebMercatorQuad",
                        z=5,
                        x=0,
                        y=0,
                        format="png",
                        relief="hillshade",
                        band=1,
                        azimuth=315.0,
                        altitude=45.0,
                       bands=None,
                       expression=None,
                       rescale=None,
                        style_url=None,
                    )
            assert exc_info.value.status_code == 422
        finally:
            _ts_mod._RENDER_COG_HILLSHADE = original

    @pytest.mark.asyncio
    async def test_tile_outside_bounds_returns_204(self):
        original = _ts_mod._RENDER_COG_HILLSHADE
        try:
            def _raise(*a, **kw):
                raise _FakeTileOutsideBounds("oob")

            _ts_mod._RENDER_COG_HILLSHADE = _raise

            svc = _make_service()
            _wire_common_mocks(svc)
            cfg = _make_render_config(cache_enabled=False)

            p1, p2, p3, p4, p5 = _patch_common(cfg, provider=None)
            with p1, p2, p3, p4, p5:
                response = await svc.get_map_tile_styled(
                    request=_make_request(),
                    background_tasks=_make_bg_tasks(),
                    catalog_id="cat",
                    collection_id="coll",
                    style_id="ndvi",
                    tms_id="WebMercatorQuad",
                    z=5,
                    x=0,
                    y=0,
                    format="png",
                    relief="hillshade",
                    band=1,
                    azimuth=315.0,
                    altitude=45.0,
                    bands=None,
                    expression=None,
                    rescale=None,
                    style_url=None,
                )
            assert response.status_code == 204
        finally:
            _ts_mod._RENDER_COG_HILLSHADE = original

    @pytest.mark.asyncio
    async def test_generic_exception_raises_500_with_hillshade_detail(self):
        original = _ts_mod._RENDER_COG_HILLSHADE
        try:
            def _raise(*a, **kw):
                raise RuntimeError("boom")

            _ts_mod._RENDER_COG_HILLSHADE = _raise

            svc = _make_service()
            _wire_common_mocks(svc)
            cfg = _make_render_config(cache_enabled=False)

            p1, p2, p3, p4, p5 = _patch_common(cfg, provider=None)
            with p1, p2, p3, p4, p5:
                with pytest.raises(HTTPException) as exc_info:
                    await svc.get_map_tile_styled(
                        request=_make_request(),
                        background_tasks=_make_bg_tasks(),
                        catalog_id="cat",
                        collection_id="coll",
                        style_id="ndvi",
                        tms_id="WebMercatorQuad",
                        z=5,
                        x=0,
                        y=0,
                        format="png",
                        relief="hillshade",
                        band=1,
                        azimuth=315.0,
                        altitude=45.0,
                        bands=None,
                        expression=None,
                        rescale=None,
                        style_url=None,
                    )
            assert exc_info.value.status_code == 500
            assert exc_info.value.detail == "Hillshade render failed: boom"
        finally:
            _ts_mod._RENDER_COG_HILLSHADE = original


# ---------------------------------------------------------------------------
# get_map_tile_styled — terrain-rgb: InvalidExpression is NOT special-cased
# (pin down this asymmetry so the refactor cannot accidentally add it).
# ---------------------------------------------------------------------------


class TestTerrainRgbDoesNotCheckInvalidExpression:
    @pytest.mark.asyncio
    async def test_invalid_expression_type_still_surfaces_as_500(self):
        original = _ts_mod._RENDER_COG_TERRAIN_RGB
        try:
            def _raise(*a, **kw):
                raise _FakeInvalidExpression("irrelevant for terrain-rgb")

            _ts_mod._RENDER_COG_TERRAIN_RGB = _raise

            svc = _make_service()
            _wire_common_mocks(svc)
            cfg = _make_render_config(cache_enabled=False)

            p1, p2, p3, p4, p5 = _patch_common(cfg, provider=None)
            with p1, p2, p3, p4, p5:
                with pytest.raises(HTTPException) as exc_info:
                    await svc.get_map_tile_styled(
                        request=_make_request(),
                        background_tasks=_make_bg_tasks(),
                        catalog_id="cat",
                        collection_id="coll",
                        style_id="terrain-rgb",
                        tms_id="WebMercatorQuad",
                        z=5,
                        x=0,
                        y=0,
                        format="png",
                        **_STYLED_DEFAULTS,
                    )
            # Terrain-RGB's renderer never raises InvalidExpression in
            # practice, and the handler never special-cased it — any
            # exception besides TileOutsideBounds falls through to 500.
            assert exc_info.value.status_code == 500
            assert exc_info.value.detail == (
                "Terrain-RGB render failed: irrelevant for terrain-rgb"
            )
        finally:
            _ts_mod._RENDER_COG_TERRAIN_RGB = original

    @pytest.mark.asyncio
    async def test_tile_outside_bounds_returns_204(self):
        original = _ts_mod._RENDER_COG_TERRAIN_RGB
        try:
            def _raise(*a, **kw):
                raise _FakeTileOutsideBounds("oob")

            _ts_mod._RENDER_COG_TERRAIN_RGB = _raise

            svc = _make_service()
            _wire_common_mocks(svc)
            cfg = _make_render_config(cache_enabled=False)

            p1, p2, p3, p4, p5 = _patch_common(cfg, provider=None)
            with p1, p2, p3, p4, p5:
                response = await svc.get_map_tile_styled(
                    request=_make_request(),
                    background_tasks=_make_bg_tasks(),
                    catalog_id="cat",
                    collection_id="coll",
                    style_id="terrain-rgb",
                    tms_id="WebMercatorQuad",
                    z=5,
                    x=0,
                    y=0,
                    format="png",
                    **_STYLED_DEFAULTS,
                )
            assert response.status_code == 204
        finally:
            _ts_mod._RENDER_COG_TERRAIN_RGB = original

    @pytest.mark.asyncio
    async def test_cache_writeback_uses_png_save_format(self):
        original = _ts_mod._RENDER_COG_TERRAIN_RGB
        try:
            fake_terrain = MagicMock(return_value=b"TERRAIN-BYTES")
            _ts_mod._RENDER_COG_TERRAIN_RGB = fake_terrain

            svc = _make_service()
            _wire_common_mocks(svc)
            bg = _make_bg_tasks()
            cfg = _make_render_config(cache_enabled=True)
            provider = MagicMock()

            p1, p2, p3, p4, p5 = _patch_common(cfg, provider=provider)
            with p1, p2, p3, p4, p5:
                response = await svc.get_map_tile_styled(
                    request=_make_request(),
                    background_tasks=bg,
                    catalog_id="cat",
                    collection_id="coll",
                    style_id="terrain-rgb",
                    tms_id="WebMercatorQuad",
                    z=5,
                    x=0,
                    y=0,
                    format="webp",  # forced to png internally for terrain-rgb
                    **_STYLED_DEFAULTS,
                )

            assert response.status_code == 200
            assert response.media_type == "image/png"
            svc._tile_cache_writer.submit_nowait.assert_called_once()
            call_args = svc._tile_cache_writer.submit_nowait.call_args.args
            assert call_args[7] == b"TERRAIN-BYTES"
            assert call_args[8] == "png"
        finally:
            _ts_mod._RENDER_COG_TERRAIN_RGB = original


# ---------------------------------------------------------------------------
# get_map_tile_styled — explicit styled-raster branch (500 detail + headers
# not previously asserted; only 204/307/404 paths were covered).
# ---------------------------------------------------------------------------


class TestStyledRasterDispatch:
    @pytest.mark.asyncio
    async def test_source_sld_link_styles_tile_without_internal_style(self):
        original_rct = _ts_mod._RENDER_COG_TILE
        original_fetch = _ts_mod._FETCH_SLD_BODY
        original_parse = _ts_mod._PARSE_SLD_COLORMAP
        try:
            captured: dict[str, Any] = {}

            def _render(*a, **kw):
                captured.update(kw)
                return b"STYLED-BYTES"

            _ts_mod._RENDER_COG_TILE = _render
            _ts_mod._FETCH_SLD_BODY = AsyncMock(return_value="<StyledLayerDescriptor/>")
            _ts_mod._PARSE_SLD_COLORMAP = lambda _sld: {1: (255, 0, 0, 255)}

            svc = _make_service()
            _wire_common_mocks(svc)
            svc._get_first_item = AsyncMock(return_value={
                "assets": {"data": {"href": "https://s3/cog.tif", "roles": ["data"]}},
                "links": [{"rel": "sld", "href": "https://styles.example.test/ndvi.sld"}],
                "properties": {},
            })
            cfg = _make_render_config(cache_enabled=False)

            p1, p2, p3, p4, p5 = _patch_common(cfg, provider=None)
            with p1, p2, p3, p4, p5:
                response = await svc.get_map_tile_styled(
                    request=_make_request(),
                    background_tasks=_make_bg_tasks(),
                    catalog_id="cat",
                    collection_id="coll",
                    style_id="ndvi",
                    tms_id="WebMercatorQuad",
                    z=5,
                    x=0,
                    y=0,
                    format="png",
                    **_STYLED_DEFAULTS,
                )

            assert response.status_code == 200
            assert response.body == b"STYLED-BYTES"
            _ts_mod._FETCH_SLD_BODY.assert_awaited_once_with(  # type: ignore[attr-defined]
                "https://styles.example.test/ndvi.sld"
            )
            assert captured["colormap"] == {1: (255, 0, 0, 255)}
        finally:
            _ts_mod._RENDER_COG_TILE = original_rct
            _ts_mod._FETCH_SLD_BODY = original_fetch
            _ts_mod._PARSE_SLD_COLORMAP = original_parse

    @pytest.mark.asyncio
    async def test_generic_exception_raises_500_with_raster_render_detail(self):
        original = _ts_mod._RENDER_COG_TILE
        try:
            def _raise(*a, **kw):
                raise RuntimeError("styled boom")

            _ts_mod._RENDER_COG_TILE = _raise

            svc = _make_service()
            _wire_common_mocks(svc)
            cfg = _make_render_config(cache_enabled=False)

            styles_svc = MagicMock()
            svc._get_style_record = AsyncMock(return_value=MagicMock())
            _ts_mod._EXTRACT_SLD_BODY = lambda obj: None

            p1, p2, p3, p4, p5 = _patch_common(cfg, provider=styles_svc)
            with p1, p2, p3, p4, p5:
                with pytest.raises(HTTPException) as exc_info:
                    await svc.get_map_tile_styled(
                        request=_make_request(),
                        background_tasks=_make_bg_tasks(),
                        catalog_id="cat",
                        collection_id="coll",
                        style_id="ndvi",
                        tms_id="WebMercatorQuad",
                        z=5,
                        x=0,
                        y=0,
                        format="png",
                        **_STYLED_DEFAULTS,
                    )
            assert exc_info.value.status_code == 500
            assert exc_info.value.detail == "Raster render failed: styled boom"
        finally:
            _ts_mod._RENDER_COG_TILE = original

    @pytest.mark.asyncio
    async def test_success_returns_200_with_correct_media_type_for_webp(self):
        original = _ts_mod._RENDER_COG_TILE
        try:
            fake_tile = MagicMock(return_value=b"WEBP-BYTES")
            _ts_mod._RENDER_COG_TILE = fake_tile

            svc = _make_service()
            _wire_common_mocks(svc)
            cfg = _make_render_config(cache_enabled=False)

            styles_svc = MagicMock()
            svc._get_style_record = AsyncMock(return_value=MagicMock())
            _ts_mod._EXTRACT_SLD_BODY = lambda obj: None

            p1, p2, p3, p4, p5 = _patch_common(cfg, provider=styles_svc)
            with p1, p2, p3, p4, p5:
                response = await svc.get_map_tile_styled(
                    request=_make_request(),
                    background_tasks=_make_bg_tasks(),
                    catalog_id="cat",
                    collection_id="coll",
                    style_id="ndvi",
                    tms_id="WebMercatorQuad",
                    z=5,
                    x=0,
                    y=0,
                    format="webp",
                    **_STYLED_DEFAULTS,
                )
            assert response.status_code == 200
            assert response.body == b"WEBP-BYTES"
            assert response.media_type == "image/webp"
            assert response.headers["X-Render-Source"] == "rio-tiler"
        finally:
            _ts_mod._RENDER_COG_TILE = original
