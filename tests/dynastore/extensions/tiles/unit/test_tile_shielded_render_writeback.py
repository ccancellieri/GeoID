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

"""Unit tests for the shielded-render cache write-back on client disconnect
(#2898).

Before this fix, a client disconnect mid-render cancelled ``get_vector_tile``
outright: the in-flight PostGIS render was abandoned and its result (even a
fully-rendered tile) was never persisted, so a heavy tile stuck in a
disconnect/retry storm never healed. Now the render runs as a standalone
task awaited behind ``asyncio.shield`` — a disconnect cancels the request
coroutine but leaves the render running, and its result is persisted via the
task's ``done_callback`` once it completes.

Uses real asyncio task cancellation (no real DB/network I/O — the render and
DB calls are mocked); the render is a short real ``asyncio.sleep`` so the
cancellation genuinely races it.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.tiles.tiles_config import TilesConfig


def _make_service():
    from dynastore.extensions.tiles.tiles_service import TilesService

    svc = object.__new__(TilesService)
    svc._ogc_catalogs_protocol = None  # type: ignore[attr-defined]
    svc._ogc_configs_protocol = None  # type: ignore[attr-defined]
    svc._ogc_storage_protocol = None  # type: ignore[attr-defined]
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
        disable_cache=False,
        refresh_cache=False,
        request_hints=frozenset(),
    )
    defaults.update(overrides)
    return defaults


def _instant_connect_engine(mock_conn) -> MagicMock:
    async def _instant_connect():
        return mock_conn

    mock_engine = MagicMock()
    mock_engine.connect = MagicMock(side_effect=lambda: _instant_connect())
    return mock_engine


def _do_everything_protocol_mock() -> MagicMock:
    mock = MagicMock()
    mock.get_config = AsyncMock(return_value=MagicMock())
    mock.get_tile_url = AsyncMock(return_value=None)
    mock.get_tile = AsyncMock(return_value=None)
    mock.save_tile = AsyncMock()
    mock.archive_exists = AsyncMock(return_value=False)
    return mock


@pytest.mark.asyncio
async def test_cancelled_request_still_persists_shielded_render_result():
    """A client disconnect (task cancellation) mid-render must not discard
    the render: once the shielded render task completes, its bytes are
    persisted via ``save_tile`` even though the request itself already
    raised ``CancelledError``."""
    svc = _make_service()
    svc._require_collection_visible = AsyncMock()
    svc._resolve_request_config = AsyncMock(return_value=TilesConfig())
    svc._is_cache_enabled = AsyncMock(return_value=True)
    svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock(crs="EPSG:3857"))

    async def _fast_timeout() -> float:
        return 5.0

    fake_ctx = MagicMock(target_srid=3857)
    protocol_mock = _do_everything_protocol_mock()

    mock_conn = AsyncMock()
    mock_conn.close = AsyncMock()

    render_started = asyncio.Event()

    async def _slow_render(*args, **kwargs):
        render_started.set()
        await asyncio.sleep(0.1)
        return b"slow-render-bytes"

    import dynastore.extensions.tiles.tiles_service as tiles_service_mod

    with patch(
        "dynastore.extensions.tiles.tiles_service._read_live_fg_acquire_timeout",
        _fast_timeout,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_async_engine",
        return_value=_instant_connect_engine(mock_conn),
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_protocol",
        return_value=protocol_mock,
    ), patch.object(
        tiles_service_mod.DQLQuery, "execute", AsyncMock(return_value=None),
    ), patch(
        "dynastore.modules.tiles.tiles_engine.build_render_context",
        AsyncMock(return_value=fake_ctx),
    ), patch(
        "dynastore.modules.tiles.tiles_engine.render_tile", _slow_render,
    ):
        task = asyncio.ensure_future(
            svc.get_vector_tile(
                request=_make_request(),
                background_tasks=_make_bg_tasks(),
                **_minimal_tile_kwargs(),
            )
        )

        # Let the request coroutine progress into the render before
        # cancelling it, so the cancellation genuinely races the shield.
        await asyncio.wait_for(render_started.wait(), timeout=2.0)
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        # The render request was cancelled, but the shielded render_task
        # must keep running to completion (~0.1s sleep) and its
        # done-callback must persist the result once it settles.
        await asyncio.sleep(0.3)

    protocol_mock.save_tile.assert_awaited_once()
    save_args = protocol_mock.save_tile.await_args
    assert save_args.args[0] == "cat-1"
    assert save_args.args[-2] == b"slow-render-bytes"
    mock_conn.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_uncancelled_disconnect_free_request_is_unaffected():
    """Sanity check: without any cancellation, the shielded render still
    completes normally and the connection is still closed exactly once by
    the handler's own ``finally`` (not the done-callback path)."""
    svc = _make_service()
    svc._require_collection_visible = AsyncMock()
    svc._resolve_request_config = AsyncMock(return_value=TilesConfig())
    svc._is_cache_enabled = AsyncMock(return_value=True)
    svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock(crs="EPSG:3857"))
    svc._finalize_response = MagicMock(return_value=MagicMock(status_code=200, headers={}))

    async def _fast_timeout() -> float:
        return 5.0

    fake_ctx = MagicMock(target_srid=3857)
    protocol_mock = _do_everything_protocol_mock()

    mock_conn = AsyncMock()
    mock_conn.close = AsyncMock()

    import dynastore.extensions.tiles.tiles_service as tiles_service_mod

    with patch(
        "dynastore.extensions.tiles.tiles_service._read_live_fg_acquire_timeout",
        _fast_timeout,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_async_engine",
        return_value=_instant_connect_engine(mock_conn),
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
        AsyncMock(return_value=b"fast-render"),
    ):
        result = await svc.get_vector_tile(
            request=_make_request(),
            background_tasks=_make_bg_tasks(),
            **_minimal_tile_kwargs(),
        )

    assert result is not None
    mock_conn.close.assert_awaited_once()
