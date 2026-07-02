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

"""Unit tests for the live-tile statement-timeout fix (#2813).

Before this fix, a PostGIS ``ST_AsMVT`` query that exceeded the DB
statement_timeout fell into the generic ``except Exception`` in
``get_vector_tile`` and surfaced as an opaque HTTP 500. Now the render path
sets a bounded ``SET LOCAL statement_timeout`` (driven by
``TilesConfig.live_tile_timeout_seconds``) and a canceled statement
(pgcode 57014) is treated as an empty tile — the same 204 response already
returned when ``render_tile`` legitimately produces no bytes.

All PostGIS / DB calls are mocked; no real I/O.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

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
        disable_cache=True,  # skip cache lookups so we reach the render step
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


@pytest.mark.asyncio
async def test_get_vector_tile_returns_empty_tile_on_statement_timeout():
    """A canceled statement (pgcode 57014) during render_tile must yield the
    existing 204 empty-tile response, not a 500."""
    svc = _make_service()
    svc._require_collection_visible = AsyncMock()
    svc._resolve_request_config = AsyncMock(return_value=TilesConfig(live_tile_timeout_seconds=7))
    svc._is_cache_enabled = AsyncMock(return_value=False)
    svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock(crs="EPSG:3857"))

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
    HTTP 500 — only the statement-timeout cancellation gets the graceful
    empty-tile treatment."""
    from fastapi import HTTPException

    svc = _make_service()
    svc._require_collection_visible = AsyncMock()
    svc._resolve_request_config = AsyncMock(return_value=TilesConfig())
    svc._is_cache_enabled = AsyncMock(return_value=False)
    svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock(crs="EPSG:3857"))

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
