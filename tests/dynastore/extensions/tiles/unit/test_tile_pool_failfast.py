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

"""Unit tests for the tile-handler DB pool fail-fast path (Part B).

Covers:
- When the DB pool is saturated (engine.connect() times out), get_vector_tile
  returns HTTP 503 with Retry-After: 5.
- When pool is saturated but a cached tile is available, the handler serves
  the stale cached tile instead of returning 503.
- Happy-path: pool not saturated, connection acquired, tile generated normally.

All PostGIS / DB calls are mocked; no real I/O.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import BackgroundTasks, HTTPException

from dynastore.extensions.tiles.tiles_service import TilesService
from dynastore.modules.tiles.tiles_config import TilesConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_service() -> TilesService:
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
        disable_cache=True,  # default: skip cache so we reach the conn-acquire step
        refresh_cache=False,
        request_hints=frozenset(),
    )
    defaults.update(overrides)
    return defaults


# ---------------------------------------------------------------------------
# 503 on pool saturation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_vector_tile_returns_503_on_pool_timeout():
    """When engine.connect() times out, the handler raises HTTP 503."""
    svc = _make_service()
    svc._require_collection_visible = AsyncMock()
    # Metadata/TMS resolution now runs before the connection is acquired
    # (#3014) — mock it out so this test still exercises the pool-timeout
    # path in isolation, not the real (DB-backed) TMS lookup. A real
    # TilesConfig (not a bare MagicMock) is required too, since the render
    # budget check now runs unconditionally ahead of the acquire attempt.
    svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock(crs="EPSG:3857"))
    svc._resolve_request_config = AsyncMock(return_value=TilesConfig())

    # Patch timeout to 0.01 s so test runs instantly
    async def _slow_timeout() -> float:
        return 0.01

    async def _never_connects():
        await asyncio.sleep(10)  # longer than the timeout

    mock_engine = MagicMock()
    mock_engine.connect = MagicMock(side_effect=lambda: _never_connects())

    config_mock = MagicMock()
    config_mock.get_config = AsyncMock(return_value=MagicMock())

    fake_ctx = MagicMock(target_srid=3857)

    with patch(
        "dynastore.extensions.tiles.tiles_service._read_live_fg_acquire_timeout",
        _slow_timeout,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_async_engine",
        return_value=mock_engine,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_protocol",
        return_value=config_mock,
    ), patch(
        "dynastore.modules.tiles.tiles_engine.build_render_context",
        AsyncMock(return_value=fake_ctx),
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


# ---------------------------------------------------------------------------
# Stale tile served on pool saturation when cache is available
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_vector_tile_serves_stale_on_pool_timeout_with_cache():
    """When pool times out and cache has a tile, the stale tile is served."""
    from fastapi.responses import Response as FResponse

    svc = _make_service()
    svc._require_collection_visible = AsyncMock()
    svc._resolve_request_config = AsyncMock(return_value=TilesConfig())
    svc._is_cache_enabled = AsyncMock(return_value=True)
    # Metadata/TMS resolution now runs before the connection is acquired
    # (#3014) — mock it out so this test still exercises the pool-timeout
    # path in isolation, not the real (DB-backed) TMS lookup.
    svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock(crs="EPSG:3857"))

    stale_response = FResponse(
        content=b"stale-mvt",
        media_type="application/vnd.mapbox-vector-tile",
        headers={"X-Tile-Cache": "hit"},
    )
    svc._try_cached_tile = AsyncMock(return_value=stale_response)

    async def _slow_timeout() -> float:
        return 0.01

    async def _never_connects():
        await asyncio.sleep(10)

    mock_engine = MagicMock()
    mock_engine.connect = MagicMock(side_effect=lambda: _never_connects())

    config_mock = MagicMock()
    config_mock.get_config = AsyncMock(return_value=MagicMock())
    # get_protocol is patched to return this mock for every protocol
    # requested, including TileArchiveStorageProtocol — the PMTiles archive
    # fallback (now resolved ahead of the connection acquire) needs an
    # awaitable here so it reports "no archive" and continues instead of
    # crashing on a bare MagicMock.
    config_mock.archive_exists = AsyncMock(return_value=False)

    fake_ctx = MagicMock(target_srid=3857)

    # Simulate cache miss from the primary check (to reach the connect step)
    # but stale available on the fallback path
    with patch(
        "dynastore.extensions.tiles.tiles_service._read_live_fg_acquire_timeout",
        _slow_timeout,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_async_engine",
        return_value=mock_engine,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_protocol",
        return_value=config_mock,
    ), patch(
        "dynastore.modules.tiles.tiles_engine.build_render_context",
        AsyncMock(return_value=fake_ctx),
    ):
        # Enable cache but not the primary try (disable_cache=False triggers cache
        # check; _try_cached_tile returns None first, then stale on second call)
        svc._try_cached_tile = AsyncMock(side_effect=[None, stale_response])

        result = await svc.get_vector_tile(
            request=_make_request(),
            background_tasks=_make_bg_tasks(),
            **_minimal_tile_kwargs(disable_cache=False),
        )

    assert result is stale_response


# ---------------------------------------------------------------------------
# Happy path: pool not saturated
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_vector_tile_acquires_conn_on_happy_path():
    """When engine.connect() succeeds quickly, the handler proceeds normally."""
    svc = _make_service()
    svc._require_collection_visible = AsyncMock()
    svc._resolve_request_config = AsyncMock(return_value=TilesConfig())
    svc._is_cache_enabled = AsyncMock(return_value=False)
    svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock(crs="EPSG:3857"))
    svc._finalize_response = MagicMock(return_value=MagicMock(status_code=200))

    async def _fast_timeout() -> float:
        return 5.0

    mock_conn = AsyncMock()
    mock_conn.close = AsyncMock()

    async def _instant_connect():
        return mock_conn

    mock_engine = MagicMock()
    mock_engine.connect = MagicMock(side_effect=lambda: _instant_connect())

    config_mock = MagicMock()
    config_mock.get_config = AsyncMock(return_value=MagicMock())

    # get_vector_tile lazily does `from dynastore.modules.tiles import
    # tiles_engine` inside the handler, then calls attributes on that (real,
    # already-imported-elsewhere) module object — patching the module's own
    # attributes (rather than sys.modules) is what actually intercepts the
    # call regardless of import-caching order across the test session.
    fake_ctx = MagicMock(target_srid=3857)

    with patch(
        "dynastore.extensions.tiles.tiles_service._read_live_fg_acquire_timeout",
        _fast_timeout,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_async_engine",
        return_value=mock_engine,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_protocol",
        return_value=config_mock,
    ), patch(
        "dynastore.modules.tiles.tiles_engine.build_render_context",
        AsyncMock(return_value=fake_ctx),
    ) as fake_build_ctx, patch(
        "dynastore.modules.tiles.tiles_engine.render_tile",
        AsyncMock(return_value=b"fake-mvt"),
    ) as fake_render_tile:
        result = await svc.get_vector_tile(
            request=_make_request(),
            background_tasks=_make_bg_tasks(),
            **_minimal_tile_kwargs(),
        )

    assert result is not None
    fake_build_ctx.assert_awaited_once()
    fake_render_tile.assert_awaited_once()
    # Connection must have been closed in the finally block
    mock_conn.close.assert_awaited_once()
