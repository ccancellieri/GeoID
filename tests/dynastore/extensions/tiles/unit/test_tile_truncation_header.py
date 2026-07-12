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

"""Unit tests for the `X-Tile-Features-Truncated` response header (#3296).

`get_vector_tile` passes an `on_truncation` callback down through
`tiles_engine.render_tile`; when the per-tile feature cap actually discarded
rows, the callback fires and the handler surfaces the kept count as a
response header on that fresh render. `tiles_engine.render_tile` itself is
mocked here (its wiring of `on_truncation` down to `tiles_db` is covered by
`tests/dynastore/modules/tiles/unit/test_tiles_engine.py` and
`test_tiles_db_unit.py`) — these tests pin only the handler's own read-back
of the callback into a header.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import BackgroundTasks, Response

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


async def _render_tile_reporting_truncation(*args, **kwargs):
    on_truncation = kwargs.get("on_truncation")
    if on_truncation is not None:
        on_truncation(20000, 20000)
    return b"fake-mvt-bytes"


async def _render_tile_without_truncation(*args, **kwargs):
    return b"fake-mvt-bytes"


@pytest.mark.asyncio
async def test_get_vector_tile_sets_truncation_header_on_fresh_capped_render():
    """A fresh render whose ``on_truncation`` callback fires surfaces the
    kept count on ``X-Tile-Features-Truncated``."""
    svc = _make_service()
    _wire_common(svc, TilesConfig())

    async def _fast_acquire_timeout() -> float:
        return 5.0

    fake_ctx = MagicMock(target_srid=3857)
    svc._finalize_response = MagicMock(
        return_value=Response(
            content=b"placeholder", media_type="application/vnd.mapbox-vector-tile"
        )
    )

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
        AsyncMock(side_effect=_render_tile_reporting_truncation),
    ) as fake_render_tile:
        result = await svc.get_vector_tile(
            request=_make_request(),
            background_tasks=_make_bg_tasks(),
            **_minimal_tile_kwargs(),
        )

    fake_render_tile.assert_awaited_once()
    assert fake_render_tile.await_args.kwargs.get("on_truncation") is not None
    assert result.headers.get("X-Tile-Features-Truncated") == "20000"


@pytest.mark.asyncio
async def test_get_vector_tile_omits_truncation_header_when_not_capped():
    """An untruncated render never calls ``on_truncation`` — no header."""
    svc = _make_service()
    _wire_common(svc, TilesConfig())

    async def _fast_acquire_timeout() -> float:
        return 5.0

    fake_ctx = MagicMock(target_srid=3857)
    svc._finalize_response = MagicMock(
        return_value=Response(
            content=b"placeholder", media_type="application/vnd.mapbox-vector-tile"
        )
    )

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
        AsyncMock(side_effect=_render_tile_without_truncation),
    ):
        result = await svc.get_vector_tile(
            request=_make_request(),
            background_tasks=_make_bg_tasks(),
            **_minimal_tile_kwargs(),
        )

    assert "X-Tile-Features-Truncated" not in result.headers
