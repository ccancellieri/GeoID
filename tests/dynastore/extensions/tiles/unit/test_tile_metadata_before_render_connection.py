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

"""Unit tests guarding the connection-acquire ordering fixed for #3014.

Before this fix, ``get_vector_tile`` acquired its main render connection
first and only then resolved collection metadata (``build_render_context`` ->
``tiles_module.get_tile_resolution_params``), which always opens its own
separate pooled connection. A request therefore briefly needed two
connections from the pool at once; a burst wide enough to approach the pool
size deadlocked every in-flight request on the second (nested) acquire.

These tests assert the structural property that actually breaks the
deadlock: metadata resolution completes before the render connection is
requested, and the render connection is acquired exactly once per request.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import BackgroundTasks

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
        disable_cache=True,
        refresh_cache=False,
        request_hints=frozenset(),
    )
    defaults.update(overrides)
    return defaults


@pytest.mark.asyncio
async def test_metadata_resolved_before_render_connection_is_acquired():
    """``build_render_context`` (collection-metadata resolution) must
    complete before the main render connection is acquired — the ordering
    that keeps a single request from ever needing two pool connections at
    once (#3014)."""
    svc = _make_service()
    svc._require_collection_visible = AsyncMock()
    svc._resolve_request_config = AsyncMock(return_value=TilesConfig())
    svc._is_cache_enabled = AsyncMock(return_value=False)
    svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock(crs="EPSG:3857"))
    svc._finalize_response = MagicMock(return_value=MagicMock(status_code=200))

    call_order: list[str] = []

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

    async def _fake_build_render_context(*args, **kwargs):
        call_order.append("build_render_context")
        return fake_ctx

    async def _fake_acquire_conn(*args, **kwargs):
        call_order.append("acquire_engine_connection_bounded")
        return mock_conn

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
        "dynastore.extensions.tiles.tiles_service.acquire_engine_connection_bounded",
        _fake_acquire_conn,
    ), patch(
        "dynastore.modules.tiles.tiles_engine.build_render_context",
        _fake_build_render_context,
    ), patch(
        "dynastore.modules.tiles.tiles_engine.render_tile",
        AsyncMock(return_value=b"fake-mvt"),
    ):
        result = await svc.get_vector_tile(
            request=_make_request(),
            background_tasks=_make_bg_tasks(),
            **_minimal_tile_kwargs(),
        )

    assert result is not None
    # The connection is acquired exactly once per request, and only after
    # metadata resolution has already finished — never held concurrently
    # with a second, nested acquire.
    assert call_order == ["build_render_context", "acquire_engine_connection_bounded"]
    mock_conn.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_render_connection_acquired_only_once_per_request():
    """A single ``get_vector_tile`` call must acquire the render connection
    exactly once — guards against a future regression re-introducing a
    second (nested) acquire while the first is still held."""
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
        "dynastore.extensions.tiles.tiles_service.acquire_engine_connection_bounded",
        AsyncMock(return_value=mock_conn),
    ) as fake_acquire, patch(
        "dynastore.modules.tiles.tiles_engine.build_render_context",
        AsyncMock(return_value=fake_ctx),
    ), patch(
        "dynastore.modules.tiles.tiles_engine.render_tile",
        AsyncMock(return_value=b"fake-mvt"),
    ):
        result = await svc.get_vector_tile(
            request=_make_request(),
            background_tasks=_make_bg_tasks(),
            **_minimal_tile_kwargs(),
        )

    assert result is not None
    fake_acquire.assert_awaited_once()
    mock_conn.close.assert_awaited_once()
