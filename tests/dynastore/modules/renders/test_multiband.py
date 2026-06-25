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

"""Unit tests for Slice 5 multiband / band-math rendering additions.

Pure: no DB, no HTTP, no rio-tiler required.  Tests cover:

- ``_resolve_indexes`` engine helper (priority logic)
- ``build_render_params_hash`` cache-key isolation
- ``build_render_cache_key`` with params_hash
- ``_parse_multiband_params`` service parser (happy + error paths)
- ``render_cog_tile`` and ``render_cog_map`` call signatures (via mocking)
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from dynastore.modules.renders.config import (
    build_render_cache_key,
    build_render_params_hash,
)
from dynastore.modules.renders.engine import _resolve_indexes


# ---------------------------------------------------------------------------
# _resolve_indexes — priority logic
# ---------------------------------------------------------------------------


class TestResolveIndexes:
    def test_single_band_default(self):
        indexes, expr = _resolve_indexes(band=1, bands=None, expression=None)
        assert indexes == (1,)
        assert expr is None

    def test_bands_overrides_band(self):
        indexes, expr = _resolve_indexes(band=1, bands=(3, 2, 1), expression=None)
        assert indexes == (3, 2, 1)
        assert expr is None

    def test_expression_overrides_bands_and_band(self):
        indexes, expr = _resolve_indexes(
            band=1, bands=(3, 2, 1), expression="(B1-B2)/(B1+B2)"
        )
        assert indexes is None
        assert expr == "(B1-B2)/(B1+B2)"

    def test_expression_without_bands(self):
        indexes, expr = _resolve_indexes(band=2, bands=None, expression="B1/B2")
        assert indexes is None
        assert expr == "B1/B2"

    def test_empty_bands_falls_back_to_band(self):
        # Empty tuple is falsy — falls through to single-band path.
        indexes, expr = _resolve_indexes(band=3, bands=(), expression=None)
        assert indexes == (3,)
        assert expr is None

    def test_band_4_single(self):
        indexes, expr = _resolve_indexes(band=4, bands=None, expression=None)
        assert indexes == (4,)
        assert expr is None


# ---------------------------------------------------------------------------
# build_render_params_hash
# ---------------------------------------------------------------------------


class TestBuildRenderParamsHash:
    def test_no_params_returns_none(self):
        assert build_render_params_hash() is None
        assert build_render_params_hash(bands=None, expression=None, rescale=None) is None

    def test_bands_only_returns_hash(self):
        h = build_render_params_hash(bands=(3, 2, 1))
        assert h is not None
        assert len(h) == 16

    def test_expression_only_returns_hash(self):
        h = build_render_params_hash(expression="(B1-B2)/(B1+B2)")
        assert h is not None
        assert len(h) == 16

    def test_rescale_only_returns_hash(self):
        h = build_render_params_hash(rescale=[(0, 3000), (0, 3000), (0, 3000)])
        assert h is not None
        assert len(h) == 16

    def test_distinct_bands_produce_distinct_hashes(self):
        h1 = build_render_params_hash(bands=(3, 2, 1))
        h2 = build_render_params_hash(bands=(1, 2, 3))
        assert h1 != h2

    def test_distinct_expressions_produce_distinct_hashes(self):
        h1 = build_render_params_hash(expression="(B1-B2)/(B1+B2)")
        h2 = build_render_params_hash(expression="B1/B2")
        assert h1 != h2

    def test_distinct_rescale_produce_distinct_hashes(self):
        h1 = build_render_params_hash(rescale=[(0, 1000)])
        h2 = build_render_params_hash(rescale=[(0, 3000)])
        assert h1 != h2

    def test_same_params_same_hash(self):
        h1 = build_render_params_hash(bands=(3, 2, 1), rescale=[(0, 3000)] * 3)
        h2 = build_render_params_hash(bands=(3, 2, 1), rescale=[(0, 3000)] * 3)
        assert h1 == h2

    def test_hash_is_hex(self):
        h = build_render_params_hash(bands=(1, 2))
        assert h is not None
        int(h, 16)  # should not raise


# ---------------------------------------------------------------------------
# build_render_cache_key with params_hash
# ---------------------------------------------------------------------------


class TestBuildRenderCacheKeyWithParamsHash:
    def test_no_params_hash_unchanged_key_shape(self):
        key = build_render_cache_key(
            "renders/collections", "c_int", "sld_ndvi", "WebMercatorQuad", 5, 16, 10, "png"
        )
        assert key == "renders/collections/c_int/sld_ndvi/WebMercatorQuad/5/16/10.png"

    def test_params_hash_appended_to_style_segment(self):
        key = build_render_cache_key(
            "renders/collections", "c_int", "sld_ndvi", "WebMercatorQuad", 5, 16, 10, "png",
            params_hash="abcdef0123456789",
        )
        assert "sld_ndvi@abcdef0123456789" in key
        assert key.endswith("WebMercatorQuad/5/16/10.png")

    def test_single_band_and_multiband_have_different_keys(self):
        # Single-band: no params_hash
        key_single = build_render_cache_key(
            "rend/c", "col", "style", "WebMercatorQuad", 5, 0, 0, "png",
        )
        # Multiband: with params_hash
        ph = build_render_params_hash(bands=(3, 2, 1))
        key_multi = build_render_cache_key(
            "rend/c", "col", "style", "WebMercatorQuad", 5, 0, 0, "png",
            params_hash=ph,
        )
        assert key_single != key_multi

    def test_distinct_band_combos_cache_separately(self):
        ph1 = build_render_params_hash(bands=(3, 2, 1))
        ph2 = build_render_params_hash(bands=(4, 3, 2))
        key1 = build_render_cache_key("rend/c", "col", "s", "WMQ", 5, 0, 0, "png", params_hash=ph1)
        key2 = build_render_cache_key("rend/c", "col", "s", "WMQ", 5, 0, 0, "png", params_hash=ph2)
        assert key1 != key2

    def test_distinct_expressions_cache_separately(self):
        ph1 = build_render_params_hash(expression="(B1-B2)/(B1+B2)")
        ph2 = build_render_params_hash(expression="B1+B2")
        key1 = build_render_cache_key("rend/c", "col", "s", "WMQ", 5, 0, 0, "png", params_hash=ph1)
        key2 = build_render_cache_key("rend/c", "col", "s", "WMQ", 5, 0, 0, "png", params_hash=ph2)
        assert key1 != key2

    def test_distinct_rescale_cache_separately(self):
        ph1 = build_render_params_hash(rescale=[(0, 1000)])
        ph2 = build_render_params_hash(rescale=[(0, 3000)])
        key1 = build_render_cache_key("rend/c", "col", "s", "WMQ", 5, 0, 0, "png", params_hash=ph1)
        key2 = build_render_cache_key("rend/c", "col", "s", "WMQ", 5, 0, 0, "png", params_hash=ph2)
        assert key1 != key2


# ---------------------------------------------------------------------------
# _parse_multiband_params — service layer parser
# ---------------------------------------------------------------------------


class TestParseMultibandParams:
    """Tests for renders_service._parse_multiband_params.

    We import it directly; the function only depends on fastapi.HTTPException
    which is available in the test env.
    """

    def _parse(self, bands=None, expression=None, rescale=None):
        from dynastore.extensions.renders.renders_service import _parse_multiband_params
        return _parse_multiband_params(bands, expression, rescale)

    def test_all_none_returns_nones(self):
        b, e, r = self._parse()
        assert b is None
        assert e is None
        assert r is None

    def test_bands_comma_separated(self):
        b, e, r = self._parse(bands="3,2,1")
        assert b == [3, 2, 1]
        assert e is None
        assert r is None

    def test_bands_single(self):
        b, _, _ = self._parse(bands="4")
        assert b == [4]

    def test_expression_passthrough(self):
        _, e, _ = self._parse(expression="(B1-B2)/(B1+B2)")
        assert e == "(B1-B2)/(B1+B2)"

    def test_rescale_three_bands(self):
        _, _, r = self._parse(rescale="0,3000;0,3000;0,3000")
        assert r == [(0.0, 3000.0), (0.0, 3000.0), (0.0, 3000.0)]

    def test_rescale_float_values(self):
        _, _, r = self._parse(rescale="-1.5,1.5")
        assert r == [(-1.5, 1.5)]

    def test_invalid_bands_raises_400(self):
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            self._parse(bands="3,a,1")
        assert exc_info.value.status_code == 400

    def test_empty_bands_raises_400(self):
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            self._parse(bands=",,,")
        assert exc_info.value.status_code == 400

    def test_invalid_rescale_raises_400(self):
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            self._parse(rescale="0-3000")
        assert exc_info.value.status_code == 400

    def test_rescale_with_trailing_semicolon(self):
        # Trailing semicolons are skipped gracefully.
        _, _, r = self._parse(rescale="0,1000;")
        assert r == [(0.0, 1000.0)]

    def test_all_params_together(self):
        b, e, r = self._parse(bands="3,2,1", expression="B1+B2", rescale="0,255;0,255;0,255")
        # All three populated; expression priority is enforced in engine, not parser.
        assert b == [3, 2, 1]
        assert e == "B1+B2"
        assert r == [(0.0, 255.0)] * 3


# ---------------------------------------------------------------------------
# render_cog_tile — call signature (COGReader mocked, no GDAL required)
# ---------------------------------------------------------------------------


class TestRenderCogTileCallSignature:
    """Verify that render_cog_tile passes the right args to COGReader."""

    def _make_mock_img(self):
        img = MagicMock()
        img.render.return_value = b"\x89PNG"
        img.rescale.return_value = None
        return img

    def test_single_band_default(self):
        from dynastore.modules.renders.engine import render_cog_tile

        mock_img = self._make_mock_img()
        mock_cog = MagicMock()
        mock_cog.__enter__ = MagicMock(return_value=mock_cog)
        mock_cog.__exit__ = MagicMock(return_value=False)
        mock_cog.tile.return_value = mock_img

        # COGReader is imported lazily inside the function; patch at the source.
        with patch("rio_tiler.io.COGReader", return_value=mock_cog):
            render_cog_tile("s3://bucket/file.tif", 5, 16, 10)

        mock_cog.tile.assert_called_once()
        call_kwargs = mock_cog.tile.call_args
        assert call_kwargs.kwargs.get("indexes") == (1,)
        assert call_kwargs.kwargs.get("expression") is None
        mock_img.rescale.assert_not_called()

    def test_multiband_rgb_composite(self):
        from dynastore.modules.renders.engine import render_cog_tile

        mock_img = self._make_mock_img()
        mock_cog = MagicMock()
        mock_cog.__enter__ = MagicMock(return_value=mock_cog)
        mock_cog.__exit__ = MagicMock(return_value=False)
        mock_cog.tile.return_value = mock_img

        with patch("rio_tiler.io.COGReader", return_value=mock_cog):
            render_cog_tile("s3://bucket/file.tif", 5, 16, 10, bands=(3, 2, 1))

        call_kwargs = mock_cog.tile.call_args
        assert call_kwargs.kwargs.get("indexes") == (3, 2, 1)
        assert call_kwargs.kwargs.get("expression") is None
        mock_img.rescale.assert_not_called()

    def test_expression_overrides_bands(self):
        from dynastore.modules.renders.engine import render_cog_tile

        mock_img = self._make_mock_img()
        mock_cog = MagicMock()
        mock_cog.__enter__ = MagicMock(return_value=mock_cog)
        mock_cog.__exit__ = MagicMock(return_value=False)
        mock_cog.tile.return_value = mock_img

        with patch("rio_tiler.io.COGReader", return_value=mock_cog):
            render_cog_tile(
                "s3://bucket/file.tif", 5, 16, 10,
                bands=(3, 2, 1),
                expression="(B1-B2)/(B1+B2)",
            )

        call_kwargs = mock_cog.tile.call_args
        assert call_kwargs.kwargs.get("indexes") is None
        assert call_kwargs.kwargs.get("expression") == "(B1-B2)/(B1+B2)"

    def test_rescale_called_when_provided(self):
        from dynastore.modules.renders.engine import render_cog_tile

        mock_img = self._make_mock_img()
        mock_cog = MagicMock()
        mock_cog.__enter__ = MagicMock(return_value=mock_cog)
        mock_cog.__exit__ = MagicMock(return_value=False)
        mock_cog.tile.return_value = mock_img

        rescale = [(0, 3000), (0, 3000), (0, 3000)]
        with patch("rio_tiler.io.COGReader", return_value=mock_cog):
            render_cog_tile(
                "s3://bucket/file.tif", 5, 16, 10,
                bands=(3, 2, 1),
                rescale=rescale,
            )

        mock_img.rescale.assert_called_once_with(in_range=rescale)

    def test_rescale_not_called_when_absent(self):
        from dynastore.modules.renders.engine import render_cog_tile

        mock_img = self._make_mock_img()
        mock_cog = MagicMock()
        mock_cog.__enter__ = MagicMock(return_value=mock_cog)
        mock_cog.__exit__ = MagicMock(return_value=False)
        mock_cog.tile.return_value = mock_img

        with patch("rio_tiler.io.COGReader", return_value=mock_cog):
            render_cog_tile("s3://bucket/file.tif", 5, 16, 10)

        mock_img.rescale.assert_not_called()

    def test_missing_rio_tiler_raises_import_error(self):
        from dynastore.modules.renders.engine import render_cog_tile

        with patch.dict("sys.modules", {"rio_tiler": None, "rio_tiler.io": None}):
            with pytest.raises(ImportError, match="rio-tiler"):
                render_cog_tile("s3://bucket/file.tif", 5, 16, 10)


# ---------------------------------------------------------------------------
# render_cog_map — call signature (COGReader mocked, no GDAL required)
# ---------------------------------------------------------------------------


class TestRenderCogMapCallSignature:
    """Verify that render_cog_map passes the right args to COGReader."""

    def _make_mock_img(self):
        img = MagicMock()
        img.render.return_value = b"\x89PNG"
        img.rescale.return_value = None
        return img

    def test_single_band_default(self):
        from dynastore.modules.renders.engine import render_cog_map

        mock_img = self._make_mock_img()
        mock_cog = MagicMock()
        mock_cog.__enter__ = MagicMock(return_value=mock_cog)
        mock_cog.__exit__ = MagicMock(return_value=False)
        mock_cog.part.return_value = mock_img

        # COGReader is imported lazily inside the function; patch at the source.
        with patch("rio_tiler.io.COGReader", return_value=mock_cog):
            render_cog_map(
                "s3://bucket/file.tif",
                bbox=[-180.0, -90.0, 180.0, 90.0],
                width=256,
                height=256,
            )

        call_kwargs = mock_cog.part.call_args
        assert call_kwargs.kwargs.get("indexes") == (1,)
        assert call_kwargs.kwargs.get("expression") is None
        mock_img.rescale.assert_not_called()

    def test_multiband_rgb_composite(self):
        from dynastore.modules.renders.engine import render_cog_map

        mock_img = self._make_mock_img()
        mock_cog = MagicMock()
        mock_cog.__enter__ = MagicMock(return_value=mock_cog)
        mock_cog.__exit__ = MagicMock(return_value=False)
        mock_cog.part.return_value = mock_img

        with patch("rio_tiler.io.COGReader", return_value=mock_cog):
            render_cog_map(
                "s3://bucket/file.tif",
                bbox=[0.0, 0.0, 1.0, 1.0],
                width=512,
                height=512,
                bands=(3, 2, 1),
            )

        call_kwargs = mock_cog.part.call_args
        assert call_kwargs.kwargs.get("indexes") == (3, 2, 1)
        assert call_kwargs.kwargs.get("expression") is None
        mock_img.rescale.assert_not_called()

    def test_expression_overrides_bands(self):
        from dynastore.modules.renders.engine import render_cog_map

        mock_img = self._make_mock_img()
        mock_cog = MagicMock()
        mock_cog.__enter__ = MagicMock(return_value=mock_cog)
        mock_cog.__exit__ = MagicMock(return_value=False)
        mock_cog.part.return_value = mock_img

        with patch("rio_tiler.io.COGReader", return_value=mock_cog):
            render_cog_map(
                "s3://bucket/file.tif",
                bbox=[0.0, 0.0, 1.0, 1.0],
                width=256,
                height=256,
                bands=(3, 2, 1),
                expression="(B1-B2)/(B1+B2)",
            )

        call_kwargs = mock_cog.part.call_args
        assert call_kwargs.kwargs.get("indexes") is None
        assert call_kwargs.kwargs.get("expression") == "(B1-B2)/(B1+B2)"

    def test_rescale_called_when_provided(self):
        from dynastore.modules.renders.engine import render_cog_map

        mock_img = self._make_mock_img()
        mock_cog = MagicMock()
        mock_cog.__enter__ = MagicMock(return_value=mock_cog)
        mock_cog.__exit__ = MagicMock(return_value=False)
        mock_cog.part.return_value = mock_img

        rescale = [(0, 3000), (0, 3000), (0, 3000)]
        with patch("rio_tiler.io.COGReader", return_value=mock_cog):
            render_cog_map(
                "s3://bucket/file.tif",
                bbox=[0.0, 0.0, 1.0, 1.0],
                width=256,
                height=256,
                bands=(3, 2, 1),
                rescale=rescale,
            )

        mock_img.rescale.assert_called_once_with(in_range=rescale)

    def test_bbox_unpacked_correctly(self):
        from dynastore.modules.renders.engine import render_cog_map

        mock_img = self._make_mock_img()
        mock_cog = MagicMock()
        mock_cog.__enter__ = MagicMock(return_value=mock_cog)
        mock_cog.__exit__ = MagicMock(return_value=False)
        mock_cog.part.return_value = mock_img

        bbox = [10.0, 20.0, 30.0, 40.0]
        with patch("rio_tiler.io.COGReader", return_value=mock_cog):
            render_cog_map(
                "s3://bucket/file.tif",
                bbox=bbox,
                width=256,
                height=256,
            )

        call_kwargs = mock_cog.part.call_args
        assert call_kwargs.kwargs.get("bbox") == (10.0, 20.0, 30.0, 40.0)
        assert call_kwargs.kwargs.get("width") == 256
        assert call_kwargs.kwargs.get("height") == 256

    def test_missing_rio_tiler_raises_import_error(self):
        from dynastore.modules.renders.engine import render_cog_map

        with patch.dict("sys.modules", {"rio_tiler": None, "rio_tiler.io": None}):
            with pytest.raises(ImportError, match="rio-tiler"):
                render_cog_map(
                    "s3://bucket/file.tif",
                    bbox=[0.0, 0.0, 1.0, 1.0],
                    width=256,
                    height=256,
                )
