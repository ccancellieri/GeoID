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

"""Unit tests for the map-tile handlers added to TilesService.

Covers:
- _validate_style_id: rejects / and non-safe chars → 400.
- _require_raster_engine: 422 when rio-tiler absent.
- _parse_multiband_params: valid/invalid bands and rescale.
- _resolve_catalog_and_collection: ValueError → 404, AttributeError fallback.
- get_map_tile_styled: bad format → 400, style_id with / → 400,
  terrain-rgb dispatch, TileOutsideBounds → 204, cache hit → redirect,
  missing style → 404.
- OGC_API_TILES_URIS contains the new map-tile conformance classes.
- _STYLE_ID_RE matches safe chars and rejects unsafe ones.

All rio-tiler / rasterio / lxml calls are mocked — no C-extensions required.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Import the module under test
# (All real dynastore modules are available via PYTHONPATH; only stub C exts.)
# ---------------------------------------------------------------------------

from fastapi import HTTPException  # noqa: E402
from dynastore.extensions.tiles.tiles_service import (  # noqa: E402
    TilesService,
    OGC_API_TILES_URIS,
    _STYLE_ID_RE,
)
import dynastore.extensions.tiles.tiles_service as _ts_mod  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_service() -> TilesService:
    """Build a TilesService instance bypassing __init__ (no FastAPI app needed)."""
    svc = object.__new__(TilesService)
    # Initialise the OGCServiceMixin cached-protocol slots
    svc._ogc_catalogs_protocol = None  # type: ignore[attr-defined]
    svc._ogc_configs_protocol = None  # type: ignore[attr-defined]
    svc._ogc_storage_protocol = None  # type: ignore[attr-defined]
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
    catalog_raises: Exception | None = None,
    collection_raises: Exception | None = None,
) -> MagicMock:
    svc = MagicMock()
    if catalog_raises:
        svc.resolve_catalog_id = AsyncMock(side_effect=catalog_raises)
    else:
        svc.resolve_catalog_id = AsyncMock(return_value=catalog_result)
    if collection_raises:
        svc.collections.resolve_collection_id = AsyncMock(side_effect=collection_raises)
    else:
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


# ---------------------------------------------------------------------------
# _get_raster_source_item
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_raster_source_item_prefers_first_item():
    svc = _make_service()
    svc._get_first_item = AsyncMock(return_value=_first_item_stub())

    result = await svc._get_raster_source_item("cat", "coll")

    assert result == _first_item_stub()


@pytest.mark.asyncio
async def test_get_raster_source_item_falls_back_to_collection_assets():
    svc = _make_service()
    svc._get_first_item = AsyncMock(return_value=None)

    class _Collection:
        def model_dump(self, **kwargs):
            return {
                "assets": {"data": {"href": "gs://bucket/from-collection.tif"}},
                "links": [{"rel": "sld", "href": "https://styles/sld"}],
            }

    catalogs = AsyncMock()
    catalogs.get_collection = AsyncMock(return_value=_Collection())
    svc._get_catalogs_service = AsyncMock(return_value=catalogs)

    result = await svc._get_raster_source_item("cat", "coll")

    assert result == {
        "assets": {"data": {"href": "gs://bucket/from-collection.tif"}},
        "links": [{"rel": "sld", "href": "https://styles/sld"}],
        "properties": {},
    }


# ---------------------------------------------------------------------------
# _validate_style_id
# ---------------------------------------------------------------------------


class TestValidateStyleId:
    def test_accepts_alphanumeric(self):
        _make_service()._validate_style_id("myStyle123")

    def test_accepts_dots_dashes_underscores(self):
        _make_service()._validate_style_id("my.style-v1_0")

    def test_rejects_slash(self):
        with pytest.raises(HTTPException) as exc_info:
            _make_service()._validate_style_id("style/traversal")
        assert exc_info.value.status_code == 400

    def test_rejects_percent(self):
        with pytest.raises(HTTPException) as exc_info:
            _make_service()._validate_style_id("style%20name")
        assert exc_info.value.status_code == 400

    def test_rejects_space(self):
        with pytest.raises(HTTPException) as exc_info:
            _make_service()._validate_style_id("style name")
        assert exc_info.value.status_code == 400

    def test_terrain_rgb_accepted(self):
        _make_service()._validate_style_id("terrain-rgb")


# ---------------------------------------------------------------------------
# _require_raster_engine
# ---------------------------------------------------------------------------


class TestRequireRasterEngine:
    def test_raises_422_when_engine_absent(self):
        original = _ts_mod._RENDER_COG_TILE
        try:
            _ts_mod._RENDER_COG_TILE = None
            with pytest.raises(HTTPException) as exc_info:
                _make_service()._require_raster_engine()
            assert exc_info.value.status_code == 422
        finally:
            _ts_mod._RENDER_COG_TILE = original

    def test_no_raise_when_engine_present(self):
        original = _ts_mod._RENDER_COG_TILE
        try:
            _ts_mod._RENDER_COG_TILE = lambda *a, **kw: b""
            _make_service()._require_raster_engine()  # must not raise
        finally:
            _ts_mod._RENDER_COG_TILE = original


# ---------------------------------------------------------------------------
# _parse_multiband_params
# ---------------------------------------------------------------------------


class TestParseMultibandParams:
    def test_parses_valid_bands(self):
        bands, expr, rescale = _make_service()._parse_multiband_params("3,2,1", None, None)
        assert bands == [3, 2, 1]
        assert expr is None
        assert rescale is None

    def test_invalid_bands_raises_400(self):
        with pytest.raises(HTTPException) as exc_info:
            _make_service()._parse_multiband_params("a,b,c", None, None)
        assert exc_info.value.status_code == 400

    def test_parses_valid_rescale(self):
        _, _, rescale = _make_service()._parse_multiband_params(None, None, "0,3000;0,3000;0,3000")
        assert rescale == [(0.0, 3000.0), (0.0, 3000.0), (0.0, 3000.0)]

    def test_invalid_rescale_raises_400(self):
        with pytest.raises(HTTPException) as exc_info:
            _make_service()._parse_multiband_params(None, None, "bad-format")
        assert exc_info.value.status_code == 400

    def test_expression_passed_through(self):
        _, expr, _ = _make_service()._parse_multiband_params(None, "(B1-B2)/(B1+B2)", None)
        assert expr == "(B1-B2)/(B1+B2)"

    def test_empty_bands_raises_400(self):
        with pytest.raises(HTTPException) as exc_info:
            _make_service()._parse_multiband_params(",,,", None, None)
        assert exc_info.value.status_code == 400


# ---------------------------------------------------------------------------
# _resolve_catalog_and_collection
# ---------------------------------------------------------------------------


class TestResolveCatalogAndCollection:
    @pytest.mark.asyncio
    async def test_happy_path(self):
        svc = _make_service()
        svc._get_catalogs_service = AsyncMock(return_value=_mock_catalogs_svc("int-cat", "int-coll"))
        cat, coll = await svc._resolve_catalog_and_collection("ext-cat", "ext-coll")
        assert cat == "int-cat"
        assert coll == "int-coll"

    @pytest.mark.asyncio
    async def test_catalog_valueerror_raises_404(self):
        svc = _make_service()
        svc._get_catalogs_service = AsyncMock(
            return_value=_mock_catalogs_svc(catalog_raises=ValueError("not found"))
        )
        with pytest.raises(HTTPException) as exc_info:
            await svc._resolve_catalog_and_collection("bad-cat", "coll")
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_collection_valueerror_raises_404(self):
        svc = _make_service()
        svc._get_catalogs_service = AsyncMock(
            return_value=_mock_catalogs_svc(
                catalog_result="int-cat",
                collection_raises=ValueError("collection not found"),
            )
        )
        with pytest.raises(HTTPException) as exc_info:
            await svc._resolve_catalog_and_collection("cat", "bad-coll")
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_collection_attributeerror_falls_back(self):
        """AttributeError → fall back to the input collection ID (test-stub path)."""
        svc = _make_service()
        svc._get_catalogs_service = AsyncMock(
            return_value=_mock_catalogs_svc(
                catalog_result="int-cat",
                collection_raises=AttributeError("no such attr"),
            )
        )
        cat, coll = await svc._resolve_catalog_and_collection("cat", "ext-coll")
        assert cat == "int-cat"
        assert coll == "ext-coll"


# ---------------------------------------------------------------------------
# OGC_API_TILES_URIS — conformance class assertions
# ---------------------------------------------------------------------------


class TestConformanceUris:
    def test_contains_mvt_class(self):
        assert "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/mvt" in OGC_API_TILES_URIS

    def test_contains_geodata_tilesets(self):
        assert (
            "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/geodata-tilesets"
            in OGC_API_TILES_URIS
        )

    def test_contains_collections_selection(self):
        assert (
            "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/collections-selection"
            in OGC_API_TILES_URIS
        )

    def test_contains_png_class(self):
        assert "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/png" in OGC_API_TILES_URIS


# ---------------------------------------------------------------------------
# _STYLE_ID_RE pattern
# ---------------------------------------------------------------------------


class TestStyleIdRe:
    def test_safe_chars_match(self):
        assert _STYLE_ID_RE.match("ndvi")
        assert _STYLE_ID_RE.match("terrain-rgb")
        assert _STYLE_ID_RE.match("my.style_v1")
        assert _STYLE_ID_RE.match("A1B2C3")

    def test_slash_no_match(self):
        assert _STYLE_ID_RE.match("foo/bar") is None

    def test_space_no_match(self):
        assert _STYLE_ID_RE.match("foo bar") is None

    def test_percent_no_match(self):
        assert _STYLE_ID_RE.match("foo%20bar") is None


# ---------------------------------------------------------------------------
# get_map_tile_styled — bad format → 400
# ---------------------------------------------------------------------------


_STYLED_DEFAULTS = dict(
    relief=None, band=1, azimuth=315.0, altitude=45.0,
    bands=None, expression=None, rescale=None, style_url=None,
)


class TestGetMapTileStyledBadFormat:
    @pytest.mark.asyncio
    async def test_bad_format_raises_400(self):
        original = _ts_mod._RENDER_COG_TILE
        try:
            _ts_mod._RENDER_COG_TILE = lambda *a, **kw: b"PNG"
            svc = _make_service()
            svc._get_catalogs_service = AsyncMock(return_value=_mock_catalogs_svc())
            svc._require_collection_visible = AsyncMock()
            svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock())

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
                    format="gif",
                    **_STYLED_DEFAULTS,
                )
            assert exc_info.value.status_code == 400
        finally:
            _ts_mod._RENDER_COG_TILE = original


# ---------------------------------------------------------------------------
# get_map_tile_styled — style_id with / → 400
# ---------------------------------------------------------------------------


class TestGetMapTileStyledBadStyleId:
    @pytest.mark.asyncio
    async def test_slash_in_style_id_raises_400(self):
        original = _ts_mod._RENDER_COG_TILE
        try:
            _ts_mod._RENDER_COG_TILE = lambda *a, **kw: b"PNG"
            svc = _make_service()

            with pytest.raises(HTTPException) as exc_info:
                await svc.get_map_tile_styled(
                    request=_make_request(),
                    background_tasks=_make_bg_tasks(),
                    catalog_id="cat",
                    collection_id="coll",
                    style_id="../traversal",
                    tms_id="WebMercatorQuad",
                    z=5,
                    x=0,
                    y=0,
                    format="png",
                    **_STYLED_DEFAULTS,
                )
            assert exc_info.value.status_code == 400
        finally:
            _ts_mod._RENDER_COG_TILE = original


# ---------------------------------------------------------------------------
# get_map_tile_styled — terrain-rgb dispatch
# ---------------------------------------------------------------------------


class TestTerrainRgbDispatch:
    @pytest.mark.asyncio
    async def test_terrain_rgb_returns_png_response(self):
        """Terrain-RGB route returns a 200 PNG response when rio-tiler is available."""
        terrain_calls: list = []

        original_rct = _ts_mod._RENDER_COG_TILE
        original_rctr = _ts_mod._RENDER_COG_TERRAIN_RGB
        try:
            fake_tile = MagicMock(return_value=b"TILE")
            fake_terrain = MagicMock(return_value=b"TERRAIN")
            _ts_mod._RENDER_COG_TILE = fake_tile
            _ts_mod._RENDER_COG_TERRAIN_RGB = fake_terrain

            async def _fake_run(fn, *a, **kw):
                terrain_calls.append(fn)
                return b"TERRAIN"

            svc = _make_service()
            svc._get_catalogs_service = AsyncMock(return_value=_mock_catalogs_svc())
            svc._require_collection_visible = AsyncMock()
            svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock())
            svc._get_first_item = AsyncMock(return_value=_first_item_stub())

            import dynastore.extensions.ogc_base as _ogc_base_real

            with patch(
                "dynastore.extensions.tiles.tiles_service.TilesService._load_render_caching_config",
                new_callable=AsyncMock,
                return_value=_make_render_config(cache_enabled=False),
            ):
                with patch("dynastore.modules.concurrency.run_in_thread", side_effect=_fake_run):
                    with patch.object(
                        _ogc_base_real, "ogc_asset_href", return_value="https://s3/dem.tif"
                    ):
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

            assert response.status_code == 200
            assert response.media_type == "image/png"
            # Verify terrain renderer (not tile renderer) was dispatched
            assert len(terrain_calls) == 1
            assert terrain_calls[0] is fake_terrain
        finally:
            _ts_mod._RENDER_COG_TILE = original_rct
            _ts_mod._RENDER_COG_TERRAIN_RGB = original_rctr


# ---------------------------------------------------------------------------
# get_map_tile_styled — TileOutsideBounds → 204
# ---------------------------------------------------------------------------


class TestTileOutsideBounds:
    @pytest.mark.asyncio
    async def test_tile_outside_bounds_returns_204(self):
        class _FakeTileOutsideBounds(Exception):
            pass
        _FakeTileOutsideBounds.__name__ = "TileOutsideBounds"

        async def _fake_run(fn, *a, **kw):
            raise _FakeTileOutsideBounds("out of bounds")

        original_rct = _ts_mod._RENDER_COG_TILE
        try:
            _ts_mod._RENDER_COG_TILE = lambda *a, **kw: b""

            svc = _make_service()
            svc._get_catalogs_service = AsyncMock(return_value=_mock_catalogs_svc())
            svc._require_collection_visible = AsyncMock()
            svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock())
            svc._get_first_item = AsyncMock(return_value=_first_item_stub())

            styles_svc = MagicMock()
            svc._get_style_record = AsyncMock(return_value=MagicMock())

            original_esb = _ts_mod._EXTRACT_SLD_BODY
            original_psc = _ts_mod._PARSE_SLD_COLORMAP
            _ts_mod._EXTRACT_SLD_BODY = lambda obj: "<sld/>"
            _ts_mod._PARSE_SLD_COLORMAP = lambda s: {0: (0, 0, 0, 255)}

            try:
                with patch("dynastore.extensions.tiles.tiles_service.get_protocol", return_value=styles_svc):
                    with patch("dynastore.modules.concurrency.run_in_thread", side_effect=_fake_run):
                        with patch(
                            "dynastore.extensions.tiles.tiles_service.TilesService._load_render_caching_config",
                            new_callable=AsyncMock,
                            return_value=_make_render_config(cache_enabled=False),
                        ):
                            import dynastore.extensions.ogc_base as _ogc_base_real
                            with patch.object(
                                _ogc_base_real, "ogc_asset_href", return_value="https://s3/cog.tif"
                            ):
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

                assert response.status_code == 204
            finally:
                _ts_mod._EXTRACT_SLD_BODY = original_esb
                _ts_mod._PARSE_SLD_COLORMAP = original_psc
        finally:
            _ts_mod._RENDER_COG_TILE = original_rct


# ---------------------------------------------------------------------------
# get_map_tile_styled — cache hit → 307 redirect with Cache-Control
# ---------------------------------------------------------------------------


class TestCacheHitRedirect:
    @pytest.mark.asyncio
    async def test_cache_hit_returns_307_with_cache_control(self):
        original_rct = _ts_mod._RENDER_COG_TILE
        try:
            _ts_mod._RENDER_COG_TILE = lambda *a, **kw: b"PNG"

            svc = _make_service()
            svc._get_catalogs_service = AsyncMock(return_value=_mock_catalogs_svc())
            svc._require_collection_visible = AsyncMock()
            svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock())
            svc._get_first_item = AsyncMock(return_value=_first_item_stub())

            provider = MagicMock()
            provider.get_tile_url = AsyncMock(return_value="https://storage.example.com/tile.png")
            cfg = _make_render_config(cache_enabled=True, ttl=86400)

            with patch("dynastore.extensions.tiles.tiles_service.get_protocol", return_value=provider):
                with patch(
                    "dynastore.extensions.tiles.tiles_service.TilesService._load_render_caching_config",
                    new_callable=AsyncMock,
                    return_value=cfg,
                ):
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

            assert response.status_code == 307
            assert "Cache-Control" in response.headers
            assert "86400" in response.headers["Cache-Control"]
        finally:
            _ts_mod._RENDER_COG_TILE = original_rct


# ---------------------------------------------------------------------------
# get_map_tile_styled — missing style → 404
# ---------------------------------------------------------------------------


class TestMissingStyle404:
    @pytest.mark.asyncio
    async def test_missing_style_raises_404(self):
        original_rct = _ts_mod._RENDER_COG_TILE
        try:
            _ts_mod._RENDER_COG_TILE = lambda *a, **kw: b"PNG"

            svc = _make_service()
            svc._get_catalogs_service = AsyncMock(return_value=_mock_catalogs_svc())
            svc._require_collection_visible = AsyncMock()
            svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock())
            svc._get_first_item = AsyncMock(return_value=_first_item_stub())

            styles_svc = MagicMock()
            svc._get_style_record = AsyncMock(return_value=None)  # style not found

            with patch("dynastore.extensions.tiles.tiles_service.get_protocol", return_value=styles_svc):
                with patch(
                    "dynastore.extensions.tiles.tiles_service.TilesService._load_render_caching_config",
                    new_callable=AsyncMock,
                    return_value=_make_render_config(cache_enabled=False),
                ):
                    with pytest.raises(HTTPException) as exc_info:
                        await svc.get_map_tile_styled(
                            request=_make_request(),
                            background_tasks=_make_bg_tasks(),
                            catalog_id="cat",
                            collection_id="coll",
                            style_id="missing-style",
                            tms_id="WebMercatorQuad",
                            z=5,
                            x=0,
                            y=0,
                            format="png",
                            **_STYLED_DEFAULTS,
                        )

            assert exc_info.value.status_code == 404
        finally:
            _ts_mod._RENDER_COG_TILE = original_rct


# ---------------------------------------------------------------------------
# bands/expression/rescale param hashing produces distinct cache segments
# ---------------------------------------------------------------------------


class TestParamsHashDistinctness:
    def test_bands_hashes_are_distinct(self):
        from dynastore.modules.renders.config import build_render_params_hash
        h1 = build_render_params_hash(bands=[1])
        h2 = build_render_params_hash(bands=[3, 2, 1])
        assert h1 != h2

    def test_no_params_returns_none(self):
        from dynastore.modules.renders.config import build_render_params_hash
        assert build_render_params_hash() is None

    def test_expression_distinct_from_none(self):
        from dynastore.modules.renders.config import build_render_params_hash
        h = build_render_params_hash(expression="(B1-B2)/(B1+B2)")
        assert h is not None
