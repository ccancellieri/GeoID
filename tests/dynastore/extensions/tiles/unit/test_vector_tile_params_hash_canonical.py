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

"""Regression test for #3249 — the vector MVT cache-key hash must canonicalize
default-valued render params so a fully-default request probes/writes under
the SAME plain ``{collection_id}`` cache id the tile-preseed task uses,
instead of a stray ``{collection_id}@{hash}`` that a preseeded pyramid entry
can never match.

Before the fix, ``filter_lang`` (default ``"cql2-text"``) and
``simplification_algorithm`` (default ``TOPOLOGY_PRESERVING``) were both
always-truthy, so ``TilesService._generate_params_hash``'s
``any(args[1:])`` guard was ``True`` on every request — even a fully
canonical one — and every live tile therefore cached under a hash suffix
that ``MapsPngTileSource``'s plain-id MVT-cache probe (and the preseed task's
plain-id writes) could never match.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.extensions.tiles.tiles_service import TilesService
from dynastore.modules.tiles.tiles_config import TilesConfig
from dynastore.tools.geospatial import SimplificationAlgorithm


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
        simplification_algorithm=SimplificationAlgorithm.TOPOLOGY_PRESERVING,
        disable_cache=False,
        refresh_cache=False,
        request_hints=frozenset(),
    )
    defaults.update(overrides)
    return defaults


def _connectable_engine(mock_conn):
    async def _instant_connect():
        return mock_conn

    mock_engine = MagicMock()
    mock_engine.connect = MagicMock(side_effect=lambda: _instant_connect())
    return mock_engine


@pytest.mark.asyncio
async def test_get_vector_tile_default_params_probe_canonical_plain_cache_id():
    """A fully-default-params request (no filter/datetime/subset, and the
    *default* filter_lang/simplification_algorithm) must probe the cache
    under the plain collection id — the same canonical id the preseed task
    writes preseeded MVT pyramid entries under — not a params-hash-suffixed
    id that a preseeded entry can never match."""
    svc = _make_service()
    svc._require_collection_visible = AsyncMock()
    svc._resolve_request_config = AsyncMock(return_value=TilesConfig())
    svc._is_cache_enabled = AsyncMock(return_value=True)
    svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock(crs="EPSG:3857"))
    svc._try_cached_tile = AsyncMock(return_value=None)

    mock_conn = AsyncMock()
    mock_conn.close = AsyncMock()
    mock_engine = _connectable_engine(mock_conn)

    config_mock = MagicMock()
    config_mock.get_config = AsyncMock(return_value=MagicMock())
    # L2 pmtiles-archive fallback check reached en route to the render call —
    # a clean miss so it does not interfere with the assertion under test.
    config_mock.archive_exists = AsyncMock(return_value=False)

    fake_ctx = MagicMock(target_srid=3857)

    import dynastore.extensions.tiles.tiles_service as tiles_service_mod

    with patch(
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
        AsyncMock(return_value=b"mvt-bytes"),
    ):
        await svc.get_vector_tile(
            request=_make_request(),
            background_tasks=_make_bg_tasks(),
            **_minimal_tile_kwargs(),
        )

    svc._try_cached_tile.assert_awaited_once()
    effective_cache_id = svc._try_cached_tile.await_args.args[2]
    assert effective_cache_id == "coll-1"


@pytest.mark.asyncio
async def test_get_vector_tile_custom_simplification_algorithm_still_hashes():
    """A request that actually customizes simplification_algorithm away from
    its default must still get its own distinct (hashed) cache id — the
    canonicalization must not collapse genuinely different requests onto the
    canonical entry."""
    svc = _make_service()
    svc._require_collection_visible = AsyncMock()
    svc._resolve_request_config = AsyncMock(return_value=TilesConfig())
    svc._is_cache_enabled = AsyncMock(return_value=True)
    svc._validate_tms_and_matrix = AsyncMock(return_value=MagicMock(crs="EPSG:3857"))
    svc._try_cached_tile = AsyncMock(return_value=None)

    mock_conn = AsyncMock()
    mock_conn.close = AsyncMock()
    mock_engine = _connectable_engine(mock_conn)

    config_mock = MagicMock()
    config_mock.get_config = AsyncMock(return_value=MagicMock())
    # L2 pmtiles-archive fallback check reached en route to the render call —
    # a clean miss so it does not interfere with the assertion under test.
    config_mock.archive_exists = AsyncMock(return_value=False)

    fake_ctx = MagicMock(target_srid=3857)

    import dynastore.extensions.tiles.tiles_service as tiles_service_mod

    with patch(
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
        AsyncMock(return_value=b"mvt-bytes"),
    ):
        await svc.get_vector_tile(
            request=_make_request(),
            background_tasks=_make_bg_tasks(),
            **_minimal_tile_kwargs(
                simplification_algorithm=SimplificationAlgorithm.VISVALINGAM_WHYATT,
            ),
        )

    svc._try_cached_tile.assert_awaited_once()
    effective_cache_id = svc._try_cached_tile.await_args.args[2]
    assert effective_cache_id.startswith("coll-1@")
