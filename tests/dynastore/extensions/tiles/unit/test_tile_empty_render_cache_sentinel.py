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

"""Unit tests for the empty-tile cache sentinel discipline (#2898).

Before this fix, ``get_vector_tile``'s write gate (``if mvt_content and
...``) and ``_try_cached_tile``'s read gate (``if tile:``) both treated a
confirmed-empty render (``b""`` — zero features) the same as a failed
render (``None``): neither ever got cached, so an empty tile re-rendered
the full PostGIS query on every request. The fix distinguishes ``b""``
(cacheable) from ``None`` (not cacheable, render never completed).

All PostGIS / DB calls are mocked; no real I/O.
"""
from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.extensions.tiles.tiles_service import TilesService
from dynastore.modules.tiles.tiles_config import TilesConfig


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
    req.url_for = MagicMock(side_effect=lambda name, **kw: f"http://testserver/{name}")
    req.app = MagicMock()
    req.app.state = MagicMock()
    req.app.state.engine = None
    req.is_disconnected = AsyncMock(return_value=False)
    return req


def _make_bg_tasks() -> MagicMock:
    from fastapi import BackgroundTasks

    bg = MagicMock(spec=BackgroundTasks)
    bg.add_task = MagicMock()
    return bg


def _minimal_tile_kwargs(**overrides):
    defaults = dict(
        dataset="cat-1",
        tileMatrixSetId="WebMercatorQuad",
        z=5,
        x=1,
        y=1,
        format="mvt",
        collections="coll-1",
        datetime=None,
        filter=None,
        filter_lang="cql2-text",
        subset=None,
        simplification=None,
        simplification_by_zoom=None,
        simplification_algorithm=MagicMock(),
        disable_cache=True,  # skip cache lookups so we reach the render step
        refresh_cache=False,
        request_hints=frozenset(),
    )
    defaults.update(overrides)
    return defaults


def _instant_connect_engine() -> MagicMock:
    mock_conn = AsyncMock()
    mock_conn.close = AsyncMock()

    async def _instant_connect():
        return mock_conn

    mock_engine = MagicMock()
    mock_engine.connect = MagicMock(side_effect=lambda: _instant_connect())
    return mock_engine


def _do_everything_protocol_mock() -> MagicMock:
    """A single mock standing in for every ``get_protocol(...)`` lookup made
    along the way to the render call (``ConfigsProtocol``,
    ``TileStorageProtocol``, ``TileArchiveStorageProtocol``) — shaped so each
    of those call sites gets a clean miss/no-op and control reaches the
    render step."""
    mock = MagicMock()
    mock.get_config = AsyncMock(return_value=MagicMock())  # ConfigsProtocol
    mock.get_tile_url = AsyncMock(return_value=None)  # TileStorageProtocol: no redirect
    mock.get_tile = AsyncMock(return_value=None)  # TileStorageProtocol: cache miss
    mock.save_tile = AsyncMock()  # TileStorageProtocol: write-back target
    mock.archive_exists = AsyncMock(return_value=False)  # TileArchiveStorageProtocol
    return mock


async def _run_get_vector_tile(*, render_result, cache_enabled: bool, bg_tasks):
    svc = _make_service()
    svc._require_collection_visible = AsyncMock()
    svc._resolve_request_config = AsyncMock(return_value=TilesConfig())
    svc._is_cache_enabled = AsyncMock(return_value=cache_enabled)
    svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock(crs="EPSG:3857"))

    async def _fast_timeout() -> float:
        return 5.0

    fake_ctx = MagicMock(target_srid=3857)
    protocol_mock = _do_everything_protocol_mock()

    import dynastore.extensions.tiles.tiles_service as tiles_service_mod

    with patch(
        "dynastore.extensions.tiles.tiles_service._read_live_fg_acquire_timeout",
        _fast_timeout,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_async_engine",
        return_value=_instant_connect_engine(),
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_protocol",
        return_value=protocol_mock,
    ), patch.object(
        tiles_service_mod.DQLQuery, "execute", AsyncMock(return_value=None),
    ), patch(
        "dynastore.modules.tiles.tiles_engine.build_render_context",
        AsyncMock(return_value=fake_ctx),
    ), patch(
        "dynastore.modules.tiles.tiles_engine.render_tile",
        AsyncMock(return_value=render_result),
    ):
        result = await svc.get_vector_tile(
            request=_make_request(),
            background_tasks=bg_tasks,
            **_minimal_tile_kwargs(disable_cache=False),
        )
    return result, protocol_mock, svc


@pytest.mark.asyncio
async def test_confirmed_empty_render_is_persisted_and_served_as_204():
    """A render returning ``b""`` (query ran, zero features) must schedule
    ``save_tile`` and still respond 204 — same wire behavior as before, but
    now cached."""
    bg_tasks = _make_bg_tasks()
    result, _, svc = await _run_get_vector_tile(
        render_result=b"", cache_enabled=True, bg_tasks=bg_tasks,
    )

    assert result.status_code == 204
    svc._tile_cache_writer.submit_nowait.assert_called_once()
    call_args = svc._tile_cache_writer.submit_nowait.call_args
    # submit_nowait(provider, dataset, cache_id, tms_id, z, x, y, data, format)
    assert call_args.args[7] == b""


@pytest.mark.asyncio
async def test_failed_render_none_is_not_persisted():
    """A render returning ``None`` (attempt failed / aborted) must never be
    handed to ``save_tile`` — only a confirmed-empty or non-empty render is
    cacheable."""
    bg_tasks = _make_bg_tasks()
    result, _, svc = await _run_get_vector_tile(
        render_result=None, cache_enabled=True, bg_tasks=bg_tasks,
    )

    assert result.status_code == 204
    svc._tile_cache_writer.submit_nowait.assert_not_called()


@pytest.mark.asyncio
async def test_read_gate_serves_cached_empty_tile_as_204_without_rerender():
    """``_try_cached_tile`` must treat a stored ``b""`` as a genuine hit (204,
    ``X-Tile-Cache: hit``) rather than falling through to a fresh render."""
    provider = MagicMock()
    provider.get_tile_url = AsyncMock(return_value=None)
    provider.get_tile = AsyncMock(return_value=b"")

    resp = await TilesService._try_cached_tile(
        provider, "cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt",
        start_time=time.perf_counter(),
        serve_mode="proxy",
    )

    assert resp is not None
    assert resp.status_code == 204
    assert resp.headers["X-Tile-Cache"] == "hit"
    assert resp.headers["X-Tile-Source"] == "bucket_proxy"


@pytest.mark.asyncio
async def test_read_gate_still_misses_on_genuine_absence():
    """A genuine cache miss (``None``) is unaffected by the sentinel fix."""
    provider = MagicMock()
    provider.get_tile_url = AsyncMock(return_value=None)
    provider.get_tile = AsyncMock(return_value=None)

    resp = await TilesService._try_cached_tile(
        provider, "cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt",
        start_time=time.perf_counter(),
        serve_mode="proxy",
    )

    assert resp is None


@pytest.mark.asyncio
async def test_disable_cache_bypasses_empty_tile_write():
    """``disable_cache=True`` must skip the write gate entirely, even for a
    confirmed-empty render — the request explicitly opted out of caching."""
    bg_tasks = _make_bg_tasks()
    svc = _make_service()
    svc._require_collection_visible = AsyncMock()
    svc._resolve_request_config = AsyncMock(return_value=TilesConfig())
    svc._is_cache_enabled = AsyncMock(return_value=True)
    svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock(crs="EPSG:3857"))

    async def _fast_timeout() -> float:
        return 5.0

    fake_ctx = MagicMock(target_srid=3857)
    protocol_mock = _do_everything_protocol_mock()

    with patch(
        "dynastore.extensions.tiles.tiles_service._read_live_fg_acquire_timeout",
        _fast_timeout,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_async_engine",
        return_value=_instant_connect_engine(),
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_protocol",
        return_value=protocol_mock,
    ), patch(
        "dynastore.modules.tiles.tiles_engine.build_render_context",
        AsyncMock(return_value=fake_ctx),
    ), patch(
        "dynastore.modules.tiles.tiles_engine.render_tile",
        AsyncMock(return_value=b""),
    ):
        result = await svc.get_vector_tile(
            request=_make_request(),
            background_tasks=bg_tasks,
            **_minimal_tile_kwargs(disable_cache=True),
        )

    assert result.status_code == 204
    protocol_mock.get_tile.assert_not_called()
    protocol_mock.get_tile_url.assert_not_called()
    bg_tasks.add_task.assert_not_called()
