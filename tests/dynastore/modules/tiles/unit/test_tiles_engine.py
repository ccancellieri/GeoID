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

"""Tests for `tiles_engine.build_render_context` / `render_tile` — the
consolidated metadata/TMS/SRID/TileSource resolution shared by the live
tile-serving path (`tiles_service`) and the preseed task."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.tiles import tiles_engine
from dynastore.modules.tiles.tiles_source import TileSourceNotSupported


class _FakeSource:
    def __init__(self, supports_result: bool = True):
        self._supports_result = supports_result
        self.render_calls = []

    def supports(self, driver):
        return self._supports_result

    async def render_tile(self, conn, **kwargs):
        self.render_calls.append(kwargs)
        return b"mvt-bytes"


@pytest.fixture
def patched_tiles_module(monkeypatch):
    import dynastore.modules.tiles.tiles_module as tm

    meta = {"catalog_id": "cat1", "collection_id": "coll1", "phys_table": "t"}
    monkeypatch.setattr(tm, "get_tile_resolution_params", AsyncMock(return_value=meta))
    monkeypatch.setattr(tm, "get_custom_tms", AsyncMock(return_value=None))
    monkeypatch.setattr(tm, "resolve_srid", AsyncMock(return_value=3857))
    return tm


@pytest.fixture
def patched_driver(monkeypatch):
    import dynastore.modules.storage.router as router

    driver = MagicMock()
    driver._get_effective_driver_config = AsyncMock()
    monkeypatch.setattr(router, "get_driver", AsyncMock(return_value=driver))
    return driver


@pytest.fixture
def patched_source(monkeypatch):
    import dynastore.modules as dmodules

    source = _FakeSource(supports_result=True)
    monkeypatch.setattr(dmodules, "get_protocols", lambda proto: [source])
    return source


@pytest.mark.asyncio
async def test_build_render_context_resolves_builtin_tms(
    patched_tiles_module, patched_driver, patched_source,
):
    ctx = await tiles_engine.build_render_context("cat1", ["coll1"], "WebMercatorQuad")

    assert ctx is not None
    assert ctx.catalog_id == "cat1"
    assert ctx.resolved_collections == [{"catalog_id": "cat1", "collection_id": "coll1", "phys_table": "t"}]
    assert ctx.target_srid == 3857
    assert ctx.source is patched_source
    assert ctx.driver is patched_driver


@pytest.mark.asyncio
async def test_build_render_context_returns_none_when_no_collections_resolve(
    patched_driver, patched_source, monkeypatch,
):
    import dynastore.modules.tiles.tiles_module as tm

    monkeypatch.setattr(tm, "get_tile_resolution_params", AsyncMock(return_value=None))
    ctx = await tiles_engine.build_render_context("cat1", ["missing-coll"], "WebMercatorQuad")
    assert ctx is None


@pytest.mark.asyncio
async def test_build_render_context_raises_when_no_source_supports_driver(
    patched_tiles_module, patched_driver, monkeypatch,
):
    import dynastore.modules as dmodules

    unsupporting_source = _FakeSource(supports_result=False)
    monkeypatch.setattr(dmodules, "get_protocols", lambda proto: [unsupporting_source])

    with pytest.raises(TileSourceNotSupported):
        await tiles_engine.build_render_context("cat1", ["coll1"], "WebMercatorQuad")


@pytest.mark.asyncio
async def test_build_render_context_srid_failure_falls_back_to_3857(
    patched_driver, patched_source, monkeypatch,
):
    import dynastore.modules.tiles.tiles_module as tm

    meta = {"catalog_id": "cat1", "collection_id": "coll1"}
    monkeypatch.setattr(tm, "get_tile_resolution_params", AsyncMock(return_value=meta))
    monkeypatch.setattr(tm, "get_custom_tms", AsyncMock(return_value=None))
    monkeypatch.setattr(tm, "resolve_srid", AsyncMock(side_effect=RuntimeError("boom")))

    ctx = await tiles_engine.build_render_context("cat1", ["coll1"], "WebMercatorQuad")
    assert ctx is not None
    assert ctx.target_srid == 3857


@pytest.mark.asyncio
async def test_render_tile_without_l1_cache_dispatches_to_source(
    patched_tiles_module, patched_driver, patched_source,
):
    ctx = await tiles_engine.build_render_context("cat1", ["coll1"], "WebMercatorQuad")
    result = await tiles_engine.render_tile(
        MagicMock(), ctx, "5", 1, 1, format="mvt", use_l1_cache=False,
    )
    assert result == b"mvt-bytes"
    assert len(patched_source.render_calls) == 1


@pytest.mark.asyncio
async def test_render_tile_with_l1_cache_reuses_cached_result(
    patched_tiles_module, patched_driver, patched_source,
):
    ctx = await tiles_engine.build_render_context("cat1", ["coll1"], "WebMercatorQuad")
    conn = MagicMock()
    r1 = await tiles_engine.render_tile(conn, ctx, "5", 2, 2, format="mvt", use_l1_cache=True)
    r2 = await tiles_engine.render_tile(conn, ctx, "5", 2, 2, format="mvt", use_l1_cache=True)
    assert r1 == b"mvt-bytes" == r2
    # Second call served from the L1 cache — source.render_tile called once.
    assert len(patched_source.render_calls) == 1
