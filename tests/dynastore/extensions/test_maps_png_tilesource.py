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

"""Unit tests for ``MapsPngTileSource`` — the default-style vector PNG
map-tile ``TileSourceProtocol`` registered by the maps extension (Phase 2 of
the /map acceleration redesign).

Gated behind ``pytest.importorskip("osgeo")``: importing the module pulls in
``renderer.py``, which imports GDAL unconditionally at module scope, just like
the existing MVT-cache-fast-path tests in ``test_maps_tile_cache.py``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

osgeo = pytest.importorskip("osgeo")

from dynastore.extensions.maps.maps_png_tilesource import (  # noqa: E402
    MapsPngTileSource,
    _tile_bbox,
)
from dynastore.tools.geospatial import SimplificationAlgorithm  # noqa: E402


def _pg_driver() -> MagicMock:
    driver = MagicMock()
    driver._get_effective_driver_config = AsyncMock()
    return driver


def _non_pg_driver() -> MagicMock:
    return MagicMock(spec=[])  # no _get_effective_driver_config attribute


def test_supports_only_gates_on_png_format():
    source = MapsPngTileSource()
    driver = _pg_driver()

    assert source.supports(driver, "png") is True
    assert source.supports(driver, "mvt") is False
    assert source.supports(driver, "pbf") is False
    # Protocol default is "mvt" — an un-passed format must not match.
    assert source.supports(driver) is False


def test_supports_rejects_driver_without_pg_duck_type():
    source = MapsPngTileSource()
    assert source.supports(_non_pg_driver(), "png") is False


class _FakeMatrix:
    def __init__(self, id_, pointOfOrigin, cellSize, tileWidth, tileHeight):
        self.id = id_
        self.pointOfOrigin = pointOfOrigin
        self.cellSize = cellSize
        self.tileWidth = tileWidth
        self.tileHeight = tileHeight


class _FakeTMS:
    def __init__(self, id_, tileMatrices):
        self.id = id_
        self.tileMatrices = tileMatrices


def test_tile_bbox_computes_topleft_origin_no_yflip():
    # A single z=0 matrix spanning the full WebMercatorQuad world extent.
    full = 20037508.342789244
    matrix = _FakeMatrix(
        id_="0", pointOfOrigin=[-full, full], cellSize=(2 * full) / 256,
        tileWidth=256, tileHeight=256,
    )
    tms = _FakeTMS("WebMercatorQuad", [matrix])

    bbox = _tile_bbox(tms, "0", 0, 0)

    assert bbox is not None
    minx, miny, maxx, maxy = bbox
    assert minx == pytest.approx(-full)
    assert maxy == pytest.approx(full)  # row 0 is north — no y-flip.
    assert maxx == pytest.approx(full)
    assert miny == pytest.approx(-full)


def test_tile_bbox_returns_none_for_unknown_matrix_id():
    tms = _FakeTMS("WebMercatorQuad", [])
    assert _tile_bbox(tms, "5", 1, 1) is None


@pytest.mark.asyncio
async def test_render_tile_returns_none_without_resolved_collections():
    source = MapsPngTileSource()
    result = await source.render_tile(
        MagicMock(),
        resolved_collections=[],
        tms_def=_FakeTMS("WebMercatorQuad", []),
        target_srid=3857,
        z="0",
        x=0,
        y=0,
    )
    assert result is None


@pytest.mark.asyncio
async def test_render_tile_falls_back_to_postgis_when_mvt_cache_absent(monkeypatch):
    """No cached MVT tile ⇒ falls back to get_features_for_rendering, then renders."""
    import dynastore.extensions.maps.maps_png_tilesource as mod

    full = 20037508.342789244
    matrix = _FakeMatrix(
        id_="0", pointOfOrigin=[-full, full], cellSize=(2 * full) / 256,
        tileWidth=256, tileHeight=256,
    )
    tms = _FakeTMS("WebMercatorQuad", [matrix])

    monkeypatch.setattr(mod, "_TILES_IMPORTS_OK", False)
    features = [{"layer": "coll1", "geom": b"\x00", "geoid": "1", "attributes": {}}]
    fetch_mock = AsyncMock(return_value=features)
    monkeypatch.setattr(mod.maps_db, "get_features_for_rendering", fetch_mock)
    render_mock = MagicMock(return_value=b"png-bytes")
    monkeypatch.setattr(mod, "render_map_image", render_mock)

    source = MapsPngTileSource()
    result = await source.render_tile(
        MagicMock(),
        resolved_collections=[
            {"catalog_id": "cat1", "collection_id": "coll1", "source_srid": 4326}
        ],
        tms_def=tms,
        target_srid=3857,
        z="0",
        x=0,
        y=0,
        format="png",
    )

    assert result == b"png-bytes"
    fetch_mock.assert_awaited_once()
    call_kwargs = fetch_mock.await_args.kwargs
    assert call_kwargs["schema"] == "cat1"
    assert call_kwargs["collections"] == ["coll1"]
    render_mock.assert_called_once()
    # Native storage SRID (4326), not the tile's render SRID (3857).
    assert render_mock.call_args.args[4] == 4326


@pytest.mark.asyncio
async def test_render_tile_accepts_every_engine_forwarded_kwarg(monkeypatch):
    """tiles_engine.render_tile forwards the FULL TileSourceProtocol kwarg set
    (including filter_lang / filter_crs_srid) to whichever source it resolved;
    a source signature that lags the protocol turns every render into a
    TypeError 500. Call with the engine's exact keyword set so drift fails here
    instead of in production."""
    import dynastore.extensions.maps.maps_png_tilesource as mod

    full = 20037508.342789244
    matrix = _FakeMatrix(
        id_="0", pointOfOrigin=[-full, full], cellSize=(2 * full) / 256,
        tileWidth=256, tileHeight=256,
    )
    tms = _FakeTMS("WebMercatorQuad", [matrix])

    monkeypatch.setattr(mod, "_TILES_IMPORTS_OK", False)
    features = [{"layer": "coll1", "geom": b"\x00", "geoid": "1", "attributes": {}}]
    monkeypatch.setattr(
        mod.maps_db, "get_features_for_rendering", AsyncMock(return_value=features)
    )
    monkeypatch.setattr(mod, "render_map_image", MagicMock(return_value=b"png-bytes"))

    source = MapsPngTileSource()
    result = await source.render_tile(
        MagicMock(),
        resolved_collections=[
            {"catalog_id": "cat1", "collection_id": "coll1", "source_srid": 4326}
        ],
        tms_def=tms,
        target_srid=3857,
        z="0",
        x=0,
        y=0,
        format="png",
        datetime_str=None,
        cql_filter=None,
        filter_lang="cql2-text",
        filter_crs_srid=None,
        subset_params=None,
        simplification=None,
        simplification_algorithm=SimplificationAlgorithm.TOPOLOGY_PRESERVING,
    )

    assert result == b"png-bytes"


@pytest.mark.asyncio
async def test_render_tile_returns_empty_bytes_when_no_features_found(monkeypatch):
    """A query that runs and confirms zero features is a cacheable empty
    tile (`b""`), distinct from an attempt-failure (`None`) — #2898."""
    import dynastore.extensions.maps.maps_png_tilesource as mod

    full = 20037508.342789244
    matrix = _FakeMatrix(
        id_="0", pointOfOrigin=[-full, full], cellSize=(2 * full) / 256,
        tileWidth=256, tileHeight=256,
    )
    tms = _FakeTMS("WebMercatorQuad", [matrix])

    monkeypatch.setattr(mod, "_TILES_IMPORTS_OK", False)
    monkeypatch.setattr(
        mod.maps_db, "get_features_for_rendering", AsyncMock(return_value=[])
    )

    source = MapsPngTileSource()
    result = await source.render_tile(
        MagicMock(),
        resolved_collections=[{"catalog_id": "cat1", "collection_id": "coll1"}],
        tms_def=tms,
        target_srid=3857,
        z="0",
        x=0,
        y=0,
    )
    assert result == b""
