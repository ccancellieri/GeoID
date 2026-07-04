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

"""Unit tests for the #3014 pool self-deadlock fix on the default-style
vector PNG map-tile handler (``_get_vector_map_tile``).

``get_vector_tile``'s own connection-acquire-ordering coverage lives in
``test_tile_metadata_before_render_connection.py`` (#3022) — these tests
cover ``_get_vector_map_tile``, which has the identical acquire-then-resolve
shape but wasn't touched by that fix, plus the one case the #3022 test file
doesn't pin: a metadata-resolution failure on ``get_vector_tile`` must never
touch the pool at all.

All PostGIS / DB calls are mocked; no real I/O.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.extensions.tiles.tiles_service import TilesService
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
        disable_cache=True,
        refresh_cache=False,
        request_hints=frozenset(),
    )
    defaults.update(overrides)
    return defaults


@pytest.mark.asyncio
async def test_get_vector_tile_no_ctx_never_acquires_main_connection():
    """When metadata resolution finds nothing (ctx is None), the handler must
    return before ever touching the pool for the main connection — a request
    for a collection that can't resolve shouldn't consume a pool slot."""
    svc = _make_service()
    svc._require_collection_visible = AsyncMock()
    svc._resolve_request_config = AsyncMock(return_value=TilesConfig())
    svc._is_cache_enabled = AsyncMock(return_value=False)
    svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock(crs="EPSG:3857"))
    svc._finalize_response = MagicMock(return_value=MagicMock(status_code=204))

    async def _fast_timeout() -> float:
        return 5.0

    acquire_mock = AsyncMock()
    config_mock = MagicMock()
    config_mock.get_config = AsyncMock(return_value=MagicMock())

    with patch(
        "dynastore.extensions.tiles.tiles_service._read_live_fg_acquire_timeout",
        _fast_timeout,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_async_engine",
        return_value=MagicMock(),
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_protocol",
        return_value=config_mock,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.acquire_engine_connection_bounded",
        acquire_mock,
    ), patch(
        "dynastore.modules.tiles.tiles_engine.build_render_context",
        AsyncMock(return_value=None),
    ):
        result = await svc.get_vector_tile(
            request=_make_request(),
            background_tasks=_make_bg_tasks(),
            **_minimal_tile_kwargs(),
        )

    assert result is not None
    acquire_mock.assert_not_called()


@pytest.mark.asyncio
async def test_vector_map_tile_resolves_metadata_before_connect():
    """The default-style vector PNG handler (``_get_vector_map_tile``) has
    the same acquire-then-resolve shape #3022 fixed on ``get_vector_tile``:
    ``build_render_context`` must resolve before ``engine.connect()`` is
    called for the render connection."""
    events: list[str] = []

    svc = _make_service()

    async def _fake_build_render_context(*args, **kwargs):
        events.append("build_render_context")
        return MagicMock()

    mock_conn = AsyncMock()
    mock_conn.close = AsyncMock()

    async def _fake_connect_impl():
        events.append("engine.connect")
        return mock_conn

    mock_engine = MagicMock()
    mock_engine.connect = MagicMock(side_effect=lambda: _fake_connect_impl())

    with patch(
        "dynastore.extensions.tiles.tiles_service.get_protocol",
        return_value=None,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.cache_on_demand_enabled",
        AsyncMock(return_value=False),
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_async_engine",
        return_value=mock_engine,
    ), patch(
        "dynastore.modules.tiles.tiles_engine.build_render_context",
        _fake_build_render_context,
    ), patch(
        "dynastore.modules.tiles.tiles_engine.render_tile",
        AsyncMock(return_value=b"fake-png"),
    ):
        result = await svc._get_vector_map_tile(
            _make_request(),
            _make_bg_tasks(),
            "cat-1",
            "coll-1",
            "WebMercatorQuad",
            5,
            1,
            1,
            "png",
            0.0,
        )

    assert result.status_code == 200
    assert events == ["build_render_context", "engine.connect"]
    mock_conn.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_vector_map_tile_ctx_none_never_connects():
    """A collection that can't be resolved must 404 before the render
    connection is ever acquired."""
    from fastapi import HTTPException

    svc = _make_service()

    mock_engine = MagicMock()
    mock_engine.connect = AsyncMock()

    with patch(
        "dynastore.extensions.tiles.tiles_service.get_protocol",
        return_value=None,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.cache_on_demand_enabled",
        AsyncMock(return_value=False),
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_async_engine",
        return_value=mock_engine,
    ), patch(
        "dynastore.modules.tiles.tiles_engine.build_render_context",
        AsyncMock(return_value=None),
    ):
        with pytest.raises(HTTPException) as exc_info:
            await svc._get_vector_map_tile(
                _make_request(),
                _make_bg_tasks(),
                "cat-1",
                "coll-1",
                "WebMercatorQuad",
                5,
                1,
                1,
                "png",
                0.0,
            )

    assert exc_info.value.status_code == 404
    mock_engine.connect.assert_not_called()
