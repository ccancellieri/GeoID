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

"""Unit tests for the render wall-clock budget and client-disconnect abort
(#2898).

Before this fix, ``get_vector_tile`` never observed an LB timeout or an
abandoned client: the render kept running (and holding a DB connection) long
after the client was gone, and every retry stacked another render on top,
eventually exhausting the pool. Now the render phase is bounded by
``TilesConfig.render_budget_seconds`` and checked against
``Request.is_disconnected()`` at loop boundaries (never per-feature).

All PostGIS / DB calls are mocked; no real I/O.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import BackgroundTasks, HTTPException

from dynastore.extensions.tiles.tiles_service import TilesService
from dynastore.modules.tiles.tiles_config import TilesConfig


def _make_service() -> TilesService:
    svc = object.__new__(TilesService)
    svc._ogc_catalogs_protocol = None  # type: ignore[attr-defined]
    svc._ogc_configs_protocol = None  # type: ignore[attr-defined]
    svc._ogc_storage_protocol = None  # type: ignore[attr-defined]
    return svc


def _make_request(*, disconnected: bool = False) -> MagicMock:
    req = MagicMock()
    req.headers = {}
    req.url_for = MagicMock(side_effect=lambda name, **kw: f"http://testserver/{name}")
    req.app = MagicMock()
    req.app.state = MagicMock()
    req.app.state.engine = None
    req.is_disconnected = AsyncMock(return_value=disconnected)
    return req


def _make_bg_tasks() -> BackgroundTasks:
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


def _wire_common(svc, config: TilesConfig):
    svc._require_collection_visible = AsyncMock()
    svc._resolve_request_config = AsyncMock(return_value=config)
    svc._is_cache_enabled = AsyncMock(return_value=False)
    svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock(crs="EPSG:3857"))


def _instant_connect_engine() -> MagicMock:
    mock_conn = AsyncMock()
    mock_conn.close = AsyncMock()

    async def _instant_connect():
        return mock_conn

    mock_engine = MagicMock()
    mock_engine.connect = MagicMock(side_effect=lambda: _instant_connect())
    return mock_engine


# ---------------------------------------------------------------------------
# Budget exceeded -> 503 + Retry-After, render never reached
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_vector_tile_returns_503_when_render_budget_exceeded():
    """A render_budget_seconds of 1s, with wall-clock simulated past the
    deadline, must abort with 503 + Retry-After before the actual PostGIS
    render call — no real sleep needed, ``time.perf_counter`` is patched to
    report elapsed time deterministically."""
    svc = _make_service()
    _wire_common(svc, TilesConfig(render_budget_seconds=1))

    async def _fast_acquire_timeout() -> float:
        return 5.0

    fake_ctx = MagicMock(target_srid=3857)

    call_count = {"n": 0}

    def _fake_perf_counter():
        call_count["n"] += 1
        # First call captures start_time (0.0); every call after simulates
        # the 1s budget having elapsed (1000.0 >= 0.0 + 1).
        return 0.0 if call_count["n"] == 1 else 1000.0

    with patch(
        "dynastore.extensions.tiles.tiles_service._read_live_fg_acquire_timeout",
        _fast_acquire_timeout,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_async_engine",
        return_value=_instant_connect_engine(),
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_protocol",
        return_value=MagicMock(get_config=AsyncMock(return_value=MagicMock())),
    ), patch(
        "dynastore.modules.tiles.tiles_engine.build_render_context",
        AsyncMock(return_value=fake_ctx),
    ), patch(
        "dynastore.modules.tiles.tiles_engine.render_tile",
        AsyncMock(return_value=b"fake-mvt"),
    ) as fake_render_tile, patch(
        "time.perf_counter", side_effect=_fake_perf_counter,
    ):
        with pytest.raises(HTTPException) as exc_info:
            await svc.get_vector_tile(
                request=_make_request(),
                background_tasks=_make_bg_tasks(),
                **_minimal_tile_kwargs(),
            )

    assert exc_info.value.status_code == 503
    assert exc_info.value.headers is not None
    assert exc_info.value.headers.get("Retry-After") == "5"
    fake_render_tile.assert_not_awaited()


# ---------------------------------------------------------------------------
# Client disconnected -> quiet abort, render never reached
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_vector_tile_aborts_quietly_when_client_disconnected():
    """A request whose is_disconnected() reports True must stop before the
    PostGIS render call without raising, and never call render_tile again."""
    svc = _make_service()
    _wire_common(svc, TilesConfig())  # healthy default budget

    async def _fast_acquire_timeout() -> float:
        return 5.0

    fake_ctx = MagicMock(target_srid=3857)

    with patch(
        "dynastore.extensions.tiles.tiles_service._read_live_fg_acquire_timeout",
        _fast_acquire_timeout,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_async_engine",
        return_value=_instant_connect_engine(),
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_protocol",
        return_value=MagicMock(get_config=AsyncMock(return_value=MagicMock())),
    ), patch(
        "dynastore.modules.tiles.tiles_engine.build_render_context",
        AsyncMock(return_value=fake_ctx),
    ), patch(
        "dynastore.modules.tiles.tiles_engine.render_tile",
        AsyncMock(return_value=b"fake-mvt"),
    ) as fake_render_tile:
        result = await svc.get_vector_tile(
            request=_make_request(disconnected=True),
            background_tasks=_make_bg_tasks(),
            **_minimal_tile_kwargs(),
        )

    assert result.status_code == 499
    fake_render_tile.assert_not_awaited()


# ---------------------------------------------------------------------------
# Healthy path unaffected
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_vector_tile_healthy_render_still_returns_200():
    """A fast render with a connected client is unaffected by the budget or
    disconnect checks — same happy path as before #2898."""
    svc = _make_service()
    _wire_common(svc, TilesConfig())
    svc._finalize_response = MagicMock(return_value=MagicMock(status_code=200))

    async def _fast_acquire_timeout() -> float:
        return 5.0

    fake_ctx = MagicMock(target_srid=3857)

    with patch(
        "dynastore.extensions.tiles.tiles_service._read_live_fg_acquire_timeout",
        _fast_acquire_timeout,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_async_engine",
        return_value=_instant_connect_engine(),
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_protocol",
        return_value=MagicMock(get_config=AsyncMock(return_value=MagicMock())),
    ), patch(
        "dynastore.modules.tiles.tiles_engine.build_render_context",
        AsyncMock(return_value=fake_ctx),
    ) as fake_build_ctx, patch(
        "dynastore.modules.tiles.tiles_engine.render_tile",
        AsyncMock(return_value=b"fake-mvt"),
    ) as fake_render_tile:
        result = await svc.get_vector_tile(
            request=_make_request(disconnected=False),
            background_tasks=_make_bg_tasks(),
            **_minimal_tile_kwargs(),
        )

    assert result is not None
    fake_build_ctx.assert_awaited_once()
    fake_render_tile.assert_awaited_once()
