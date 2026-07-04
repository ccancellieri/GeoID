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

"""Unit tests for the live-tile statement-timeout handling (#2813, #2965).

A PostGIS ``ST_AsMVT`` query that exceeds the DB statement_timeout raises a
``QueryExecutionError`` carrying pgcode 57014 (``query_canceled``). Such a
cancellation means the tile's content is unknown, not confirmed empty, so
``get_vector_tile`` must never report HTTP 204 for it
(``/req/core/tc-error`` part B reserves 204 for a render that actually
confirmed no data in the area). Instead it mirrors the DB pool-saturation
ladder (#2845): serve a stale cached tile with 200 if one exists, else fail
honestly with 503 + Retry-After so the client retries instead of painting a
false hole.

A render that legitimately completes with zero features (no exception, empty
bytes) is unaffected and still returns 204 — covered here as a regression
guard.

All PostGIS / DB calls are mocked; no real I/O.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from dynastore.extensions.tiles.tiles_service import TilesService
from dynastore.modules.db_config.exceptions import QueryExecutionError
from dynastore.modules.tiles.tiles_config import TilesConfig


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


def _make_bg_tasks():
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
        disable_cache=True,  # default: skip cache lookups so we reach the render step
        refresh_cache=False,
        request_hints=frozenset(),
    )
    defaults.update(overrides)
    return defaults


def _query_canceled_error() -> QueryExecutionError:
    """Build a ``QueryExecutionError`` shaped like a real statement-timeout
    cancellation: ``original_exception.pgcode == "57014"`` — mirrors what
    ``BaseExecutor._handle_db_exception`` raises when asyncpg cancels a
    statement past ``SET LOCAL statement_timeout``."""
    orig = MagicMock()
    orig.pgcode = "57014"
    return QueryExecutionError("Database query failed.", original_exception=orig)


def _connectable_engine(mock_conn):
    async def _instant_connect():
        return mock_conn

    mock_engine = MagicMock()
    mock_engine.connect = MagicMock(side_effect=lambda: _instant_connect())
    return mock_engine


@pytest.mark.asyncio
async def test_get_vector_tile_returns_503_on_statement_timeout_without_cache():
    """A canceled statement (pgcode 57014) with nothing cached must yield an
    honest HTTP 503 + Retry-After — never a 204, since the render never
    confirmed the tile was empty."""
    svc = _make_service()
    svc._require_collection_visible = AsyncMock()
    svc._resolve_request_config = AsyncMock(return_value=TilesConfig(live_tile_timeout_seconds=7))
    svc._is_cache_enabled = AsyncMock(return_value=False)
    svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock(crs="EPSG:3857"))

    async def _fast_timeout() -> float:
        return 5.0

    mock_conn = AsyncMock()
    mock_conn.close = AsyncMock()
    mock_engine = _connectable_engine(mock_conn)

    config_mock = MagicMock()
    config_mock.get_config = AsyncMock(return_value=MagicMock())

    fake_ctx = MagicMock(target_srid=3857)

    import dynastore.extensions.tiles.tiles_service as tiles_service_mod

    with patch(
        "dynastore.extensions.tiles.tiles_service._read_live_fg_acquire_timeout",
        _fast_timeout,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_async_engine",
        return_value=mock_engine,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_protocol",
        return_value=config_mock,
    ), patch.object(
        tiles_service_mod.DQLQuery, "execute", AsyncMock(return_value=None),
    ), patch(
        "dynastore.modules.tiles.tiles_engine.build_render_context",
        AsyncMock(return_value=fake_ctx),
    ), patch(
        "dynastore.modules.tiles.tiles_engine.render_tile",
        AsyncMock(side_effect=_query_canceled_error()),
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
    mock_conn.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_vector_tile_serves_stale_on_statement_timeout_with_cache():
    """A canceled statement with a stale cached tile available must serve
    that tile with 200, not fabricate a 204."""
    from fastapi.responses import Response as FResponse

    svc = _make_service()
    svc._require_collection_visible = AsyncMock()
    svc._resolve_request_config = AsyncMock(return_value=TilesConfig(live_tile_timeout_seconds=7))
    svc._is_cache_enabled = AsyncMock(return_value=True)
    svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock(crs="EPSG:3857"))

    stale_response = FResponse(
        content=b"stale-mvt",
        media_type="application/vnd.mapbox-vector-tile",
        headers={"X-Tile-Cache": "hit", "X-Tile-Source": "bucket_proxy"},
    )
    # First call is the pre-render cache-check (miss, so we proceed to
    # render); second call is the post-timeout stale-tile fallback (hit).
    svc._try_cached_tile = AsyncMock(side_effect=[None, stale_response])

    async def _fast_timeout() -> float:
        return 5.0

    mock_conn = AsyncMock()
    mock_conn.close = AsyncMock()
    mock_engine = _connectable_engine(mock_conn)

    config_mock = MagicMock()
    config_mock.get_config = AsyncMock(return_value=MagicMock())
    # get_protocol is patched below to hand back config_mock for *every*
    # protocol lookup, including TileArchiveStorageProtocol's L2 pmtiles
    # fallback reached en route to the render call — make that lookup a
    # clean miss so it does not interfere with the timeout path under test.
    config_mock.archive_exists = AsyncMock(return_value=False)

    fake_ctx = MagicMock(target_srid=3857)

    import dynastore.extensions.tiles.tiles_service as tiles_service_mod

    with patch(
        "dynastore.extensions.tiles.tiles_service._read_live_fg_acquire_timeout",
        _fast_timeout,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_async_engine",
        return_value=mock_engine,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_protocol",
        return_value=config_mock,
    ), patch.object(
        tiles_service_mod.DQLQuery, "execute", AsyncMock(return_value=None),
    ), patch(
        "dynastore.modules.tiles.tiles_engine.build_render_context",
        AsyncMock(return_value=fake_ctx),
    ), patch(
        "dynastore.modules.tiles.tiles_engine.render_tile",
        AsyncMock(side_effect=_query_canceled_error()),
    ):
        bg_tasks = _make_bg_tasks()
        result = await svc.get_vector_tile(
            request=_make_request(),
            background_tasks=bg_tasks,
            **_minimal_tile_kwargs(disable_cache=False),
        )

    assert result is stale_response
    assert result.status_code == 200
    # The timeout must never trigger a cache write-back (#2916 guard).
    bg_tasks.add_task.assert_not_called()


@pytest.mark.asyncio
async def test_get_vector_tile_returns_empty_tile_on_genuine_no_data():
    """A render that completes normally with zero features (no exception)
    still returns 204 — regression guard so the timeout fix doesn't
    accidentally break the genuinely-empty case (/req/core/tc-error part B)."""
    svc = _make_service()
    svc._require_collection_visible = AsyncMock()
    svc._resolve_request_config = AsyncMock(return_value=TilesConfig(live_tile_timeout_seconds=7))
    svc._is_cache_enabled = AsyncMock(return_value=False)
    svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock(crs="EPSG:3857"))

    async def _fast_timeout() -> float:
        return 5.0

    mock_conn = AsyncMock()
    mock_conn.close = AsyncMock()
    mock_engine = _connectable_engine(mock_conn)

    config_mock = MagicMock()
    config_mock.get_config = AsyncMock(return_value=MagicMock())

    fake_ctx = MagicMock(target_srid=3857)

    import dynastore.extensions.tiles.tiles_service as tiles_service_mod

    with patch(
        "dynastore.extensions.tiles.tiles_service._read_live_fg_acquire_timeout",
        _fast_timeout,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_async_engine",
        return_value=mock_engine,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_protocol",
        return_value=config_mock,
    ), patch.object(
        tiles_service_mod.DQLQuery, "execute", AsyncMock(return_value=None),
    ), patch(
        "dynastore.modules.tiles.tiles_engine.build_render_context",
        AsyncMock(return_value=fake_ctx),
    ), patch(
        "dynastore.modules.tiles.tiles_engine.render_tile",
        AsyncMock(return_value=b""),
    ):
        result = await svc.get_vector_tile(
            request=_make_request(),
            background_tasks=_make_bg_tasks(),
            **_minimal_tile_kwargs(),
        )

    assert result.status_code == 204
    assert result.headers.get("X-Tile-Cache") == "miss"
    assert result.headers.get("X-Tile-Source") == "postgis"
    mock_conn.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_vector_tile_reraises_non_timeout_query_errors():
    """A QueryExecutionError NOT carrying pgcode 57014 must still surface as
    HTTP 500 — only the statement-timeout cancellation gets the stale-tile /
    503 treatment."""
    svc = _make_service()
    svc._require_collection_visible = AsyncMock()
    svc._resolve_request_config = AsyncMock(return_value=TilesConfig())
    svc._is_cache_enabled = AsyncMock(return_value=False)
    svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock(crs="EPSG:3857"))

    async def _fast_timeout() -> float:
        return 5.0

    mock_conn = AsyncMock()
    mock_conn.close = AsyncMock()
    mock_engine = _connectable_engine(mock_conn)

    config_mock = MagicMock()
    config_mock.get_config = AsyncMock(return_value=MagicMock())

    fake_ctx = MagicMock(target_srid=3857)

    other_error = QueryExecutionError(
        "Database query failed.", original_exception=MagicMock(pgcode="42P01")
    )

    import dynastore.extensions.tiles.tiles_service as tiles_service_mod

    with patch(
        "dynastore.extensions.tiles.tiles_service._read_live_fg_acquire_timeout",
        _fast_timeout,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_async_engine",
        return_value=mock_engine,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_protocol",
        return_value=config_mock,
    ), patch.object(
        tiles_service_mod.DQLQuery, "execute", AsyncMock(return_value=None),
    ), patch(
        "dynastore.modules.tiles.tiles_engine.build_render_context",
        AsyncMock(return_value=fake_ctx),
    ), patch(
        "dynastore.modules.tiles.tiles_engine.render_tile",
        AsyncMock(side_effect=other_error),
    ):
        with pytest.raises(HTTPException) as exc_info:
            await svc.get_vector_tile(
                request=_make_request(),
                background_tasks=_make_bg_tasks(),
                **_minimal_tile_kwargs(),
            )

    assert exc_info.value.status_code == 500
