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

"""Unit tests for #3249 Finding B — the vector-PNG map-tile lane
(``TilesService._get_vector_map_tile``) must treat a cancelled /
statement-timeout PostGIS query the same way ``get_vector_tile`` already
does (#2965, #3181, #3200, #3251): serve a stale cached tile with 200 if one
exists, else fail honestly with 503 + Retry-After — never let the cancel
race's raw ``InterfaceError``-wrapping ``QueryExecutionError`` /
``DatabaseConnectionError`` fall through as an unhandled 500.

All DB / render calls are mocked; no real I/O.
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


def _cancel_race_interface_error() -> Exception:
    """Shaped like asyncpg's cancel-vs-concurrent-operation race (#3181):
    no pgcode, just an ``InterfaceError`` — keyed on class name, not an
    asyncpg import."""
    return type("InterfaceError", (Exception,), {})(
        "cannot perform operation: another operation is in progress"
    )


def _cancel_race_error() -> QueryExecutionError:
    return QueryExecutionError(
        "Database query failed.", original_exception=_cancel_race_interface_error()
    )


def _statement_timeout_error() -> QueryExecutionError:
    orig = MagicMock()
    orig.pgcode = "57014"
    return QueryExecutionError("Database query failed.", original_exception=orig)


def _connectable_engine(mock_conn):
    async def _instant_connect():
        return mock_conn

    mock_engine = MagicMock()
    mock_engine.connect = MagicMock(side_effect=lambda: _instant_connect())
    return mock_engine


def _patch_common(*, engine, render_side_effect, tiles_config=None):
    fake_ctx = MagicMock()
    config_mock = MagicMock()
    config_mock.get_config = AsyncMock(
        return_value=tiles_config if tiles_config is not None else TilesConfig()
    )
    return (
        patch(
            "dynastore.extensions.tiles.tiles_service.get_async_engine",
            return_value=engine,
        ),
        patch(
            "dynastore.extensions.tiles.tiles_service.get_protocol",
            return_value=config_mock,
        ),
        patch(
            "dynastore.modules.tiles.tiles_engine.build_render_context",
            AsyncMock(return_value=fake_ctx),
        ),
        patch(
            "dynastore.modules.tiles.tiles_engine.render_tile",
            AsyncMock(side_effect=render_side_effect),
        ),
    )


@pytest.mark.asyncio
async def test_cancel_race_without_stale_cache_returns_503_not_500():
    """A cancel-race InterfaceError with nothing cached must yield an honest
    503 + Retry-After — never an unhandled 500."""
    svc = _make_service()
    svc._require_collection_visible = AsyncMock()
    svc._validate_tms_and_matrix = AsyncMock()
    svc._collection_kind = AsyncMock()

    mock_conn = AsyncMock()
    mock_conn.close = AsyncMock()
    mock_engine = _connectable_engine(mock_conn)

    p1, p2, p3, p4 = _patch_common(
        engine=mock_engine, render_side_effect=_cancel_race_error(),
    )
    with p1, p2, p3, p4:
        with pytest.raises(HTTPException) as exc_info:
            await svc._get_vector_map_tile(
                _make_request(), _make_bg_tasks(),
                "cat-1", "coll-1", "WebMercatorQuad", 5, 1, 1, "png", 0.0,
            )

    assert exc_info.value.status_code == 503
    assert exc_info.value.headers is not None
    assert exc_info.value.headers.get("Retry-After") == "5"
    mock_conn.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_statement_timeout_pgcode_without_stale_cache_returns_503():
    """A clean pgcode-57014 cancellation gets the identical treatment,
    regardless of elapsed time (mirrors get_vector_tile)."""
    svc = _make_service()
    svc._require_collection_visible = AsyncMock()
    svc._validate_tms_and_matrix = AsyncMock()
    svc._collection_kind = AsyncMock()

    mock_conn = AsyncMock()
    mock_conn.close = AsyncMock()
    mock_engine = _connectable_engine(mock_conn)

    p1, p2, p3, p4 = _patch_common(
        engine=mock_engine, render_side_effect=_statement_timeout_error(),
    )
    with p1, p2, p3, p4:
        with pytest.raises(HTTPException) as exc_info:
            await svc._get_vector_map_tile(
                _make_request(), _make_bg_tasks(),
                "cat-1", "coll-1", "WebMercatorQuad", 5, 1, 1, "png", 0.0,
            )

    assert exc_info.value.status_code == 503


@pytest.mark.asyncio
async def test_cancel_race_with_stale_cache_serves_it_with_200():
    """A cancel-race InterfaceError with a previously-cached PNG tile
    available must serve that tile with 200, not 500 or a fabricated 204."""
    svc = _make_service()
    svc._require_collection_visible = AsyncMock()
    svc._validate_tms_and_matrix = AsyncMock()
    svc._collection_kind = AsyncMock()

    mock_conn = AsyncMock()
    mock_conn.close = AsyncMock()
    mock_engine = _connectable_engine(mock_conn)

    provider = MagicMock()
    # First call: the pre-render cache-check (miss, so the render is
    # attempted); second call: the post-cancel-race stale-tile fallback (hit).
    provider.get_tile = AsyncMock(side_effect=[None, b"stale-png-bytes"])

    fake_ctx = MagicMock()
    config_mock = MagicMock()
    config_mock.get_config = AsyncMock(return_value=TilesConfig())

    from dynastore.modules.tiles.tiles_module import TileStorageProtocol

    def _get_protocol(proto, *a, **kw):
        return provider if proto is TileStorageProtocol else config_mock

    with patch(
        "dynastore.extensions.tiles.tiles_service.get_async_engine",
        return_value=mock_engine,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_protocol",
        side_effect=_get_protocol,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.cache_on_demand_enabled",
        AsyncMock(return_value=True),
    ), patch(
        "dynastore.modules.tiles.tiles_engine.build_render_context",
        AsyncMock(return_value=fake_ctx),
    ), patch(
        "dynastore.modules.tiles.tiles_engine.render_tile",
        AsyncMock(side_effect=_cancel_race_error()),
    ):
        result = await svc._get_vector_map_tile(
            _make_request(), _make_bg_tasks(),
            "cat-1", "coll-1", "WebMercatorQuad", 5, 1, 1, "png", 0.0,
        )

    assert result.status_code == 200
    assert result.body == b"stale-png-bytes"
    assert result.headers.get("X-Render-Cache") == "hit"
    # The cancel-race fallback must never trigger a fresh cache write-back.
    svc._tile_cache_writer.submit_nowait.assert_not_called()


@pytest.mark.asyncio
async def test_non_cancel_race_query_error_still_propagates():
    """A QueryExecutionError NOT shaped like a cancellation (unrelated
    pgcode) must still surface unchanged — only the cancel-race gets the
    stale-tile/503 treatment."""
    svc = _make_service()
    svc._require_collection_visible = AsyncMock()
    svc._validate_tms_and_matrix = AsyncMock()
    svc._collection_kind = AsyncMock()

    mock_conn = AsyncMock()
    mock_conn.close = AsyncMock()
    mock_engine = _connectable_engine(mock_conn)

    other_error = QueryExecutionError(
        "Database query failed.", original_exception=MagicMock(pgcode="42P01")
    )

    p1, p2, p3, p4 = _patch_common(engine=mock_engine, render_side_effect=other_error)
    with p1, p2, p3, p4:
        with pytest.raises(QueryExecutionError):
            await svc._get_vector_map_tile(
                _make_request(), _make_bg_tasks(),
                "cat-1", "coll-1", "WebMercatorQuad", 5, 1, 1, "png", 0.0,
            )

    mock_conn.close.assert_awaited_once()
