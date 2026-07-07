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

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from dynastore.extensions.tiles.tiles_service import TilesService
from dynastore.modules.tiles.tiles_config import TilesConfig


def _make_service() -> TilesService:
    svc = object.__new__(TilesService)
    svc._ogc_catalogs_protocol = None  # type: ignore[attr-defined]
    svc._ogc_configs_protocol = None  # type: ignore[attr-defined]
    svc._ogc_storage_protocol = None  # type: ignore[attr-defined]
    return svc


def _make_request(query_params: dict[str, str] | None = None) -> MagicMock:
    req = MagicMock()
    req.headers = {}
    req.query_params = query_params or {}
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


def _connectable_engine(mock_conn: AsyncMock) -> MagicMock:
    async def _instant_connect():
        return mock_conn

    mock_engine = MagicMock()
    mock_engine.connect = MagicMock(side_effect=lambda: _instant_connect())
    return mock_engine


@pytest.mark.asyncio
async def test_get_vector_tile_passes_cql_language_and_filter_crs_to_render():
    svc = _make_service()
    svc._require_collection_visible = AsyncMock()
    svc._resolve_request_config = AsyncMock(return_value=TilesConfig())
    svc._is_cache_enabled = AsyncMock(return_value=False)
    svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock(crs="EPSG:3857"))

    async def _fast_timeout() -> float:
        return 5.0

    mock_conn = AsyncMock()
    mock_conn.close = AsyncMock()
    config_mock = MagicMock()
    config_mock.get_config = AsyncMock(return_value=MagicMock())
    fake_ctx = MagicMock(target_srid=3857)

    import dynastore.extensions.tiles.tiles_service as tiles_service_mod

    render_tile = AsyncMock(return_value=b"tile-bytes")

    with patch(
        "dynastore.extensions.tiles.tiles_service._read_live_fg_acquire_timeout",
        _fast_timeout,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_async_engine",
        return_value=_connectable_engine(mock_conn),
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
        render_tile,
    ):
        await svc.get_vector_tile(
            request=_make_request(),
            background_tasks=_make_bg_tasks(),
            dataset="cat-1",
            tileMatrixSetId="WebMercatorQuad",
            z=5,
            x=1,
            y=1,
            format="mvt",
            collections="coll-1",
            datetime=None,
            filter='{"op":"=","args":[{"property":"CODE"},"IT"]}',
            filter_lang="cql2-json",
            filter_crs="http://www.opengis.net/def/crs/EPSG/0/3857",
            subset=None,
            simplification=None,
            simplification_by_zoom=None,
            simplification_algorithm=MagicMock(),
            disable_cache=True,
            refresh_cache=False,
            request_hints=frozenset(),
        )

    render_tile.assert_awaited_once()
    kwargs = render_tile.await_args.kwargs
    assert kwargs["filter_lang"] == "cql2-json"
    assert kwargs["filter_crs_srid"] == 3857


@pytest.mark.asyncio
async def test_get_vector_tile_returns_400_for_invalid_cql_filter():
    svc = _make_service()
    svc._require_collection_visible = AsyncMock()
    svc._resolve_request_config = AsyncMock(return_value=TilesConfig())
    svc._is_cache_enabled = AsyncMock(return_value=False)
    svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock(crs="EPSG:3857"))

    async def _fast_timeout() -> float:
        return 5.0

    mock_conn = AsyncMock()
    mock_conn.close = AsyncMock()
    config_mock = MagicMock()
    config_mock.get_config = AsyncMock(return_value=MagicMock())
    fake_ctx = MagicMock(target_srid=3857)

    import dynastore.extensions.tiles.tiles_service as tiles_service_mod

    with patch(
        "dynastore.extensions.tiles.tiles_service._read_live_fg_acquire_timeout",
        _fast_timeout,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_async_engine",
        return_value=_connectable_engine(mock_conn),
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
        AsyncMock(side_effect=ValueError("Invalid CQL filter: Unknown field 'BAD'")),
    ):
        with pytest.raises(HTTPException) as exc_info:
            await svc.get_vector_tile(
                request=_make_request(),
                background_tasks=_make_bg_tasks(),
                dataset="cat-1",
                tileMatrixSetId="WebMercatorQuad",
                z=5,
                x=1,
                y=1,
                format="mvt",
                collections="coll-1",
                datetime=None,
                filter="BAD = 1",
                filter_lang="cql2-text",
                filter_crs=None,
                subset=None,
                simplification=None,
                simplification_by_zoom=None,
                simplification_algorithm=MagicMock(),
                disable_cache=True,
                refresh_cache=False,
                request_hints=frozenset(),
            )

    assert exc_info.value.status_code == 400
    assert "Invalid CQL filter" in str(exc_info.value.detail)


@pytest.mark.asyncio
async def test_get_vector_tile_accepts_legacy_filter_lang_query_spelling():
    svc = _make_service()
    svc._require_collection_visible = AsyncMock()
    svc._resolve_request_config = AsyncMock(return_value=TilesConfig())
    svc._is_cache_enabled = AsyncMock(return_value=False)
    svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock(crs="EPSG:3857"))

    async def _fast_timeout() -> float:
        return 5.0

    mock_conn = AsyncMock()
    mock_conn.close = AsyncMock()
    config_mock = MagicMock()
    config_mock.get_config = AsyncMock(return_value=MagicMock())
    fake_ctx = MagicMock(target_srid=3857)

    import dynastore.extensions.tiles.tiles_service as tiles_service_mod

    render_tile = AsyncMock(return_value=b"tile-bytes")

    with patch(
        "dynastore.extensions.tiles.tiles_service._read_live_fg_acquire_timeout",
        _fast_timeout,
    ), patch(
        "dynastore.extensions.tiles.tiles_service.get_async_engine",
        return_value=_connectable_engine(mock_conn),
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
        render_tile,
    ):
        await svc.get_vector_tile(
            request=_make_request(
                {
                    "filter_lang": "cql2-json",
                    "filter_crs": "http://www.opengis.net/def/crs/EPSG/0/3857",
                }
            ),
            background_tasks=_make_bg_tasks(),
            dataset="cat-1",
            tileMatrixSetId="WebMercatorQuad",
            z=5,
            x=1,
            y=1,
            format="mvt",
            collections="coll-1",
            datetime=None,
            filter='{"op":"=","args":[{"property":"CODE"},"IT"]}',
            filter_lang="cql2-text",
            filter_crs=None,
            subset=None,
            simplification=None,
            simplification_by_zoom=None,
            simplification_algorithm=MagicMock(),
            disable_cache=True,
            refresh_cache=False,
            request_hints=frozenset(),
        )

    kwargs = render_tile.await_args.kwargs
    assert kwargs["filter_lang"] == "cql2-json"
    assert kwargs["filter_crs_srid"] == 3857
