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

"""Unit tests for the aligned vector tile routes added to TilesService.

Covers:
- get_vector_tile_aligned_default: resolves IDs and delegates to get_vector_tile.
- get_vector_tile_aligned: resolves IDs and delegates to get_vector_tile.
- get_collection_tilesets: lists tilesets; hidden collection → 404.
- invalidate_collection_tile_cache: resolves IDs and calls invalidation impl.
- Deprecated flat routes still reachable (delegated via existing handlers).
- _resolve_catalog_and_collection: ValueError → 404 (reused from existing test).

All rio-tiler / DB calls are mocked; no real I/O.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException, BackgroundTasks

from dynastore.extensions.tiles.tiles_service import TilesService


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
    return req


def _make_bg_tasks() -> BackgroundTasks:
    bg = MagicMock(spec=BackgroundTasks)
    bg.add_task = MagicMock()
    return bg


def _mock_catalogs_svc(
    catalog_result: str = "internal-cat",
    collection_result: str = "internal-coll",
    catalog_raises: Exception | None = None,
    collection_raises: Exception | None = None,
) -> MagicMock:
    svc = MagicMock()
    if catalog_raises:
        svc.resolve_catalog_id = AsyncMock(side_effect=catalog_raises)
    else:
        svc.resolve_catalog_id = AsyncMock(return_value=catalog_result)
    if collection_raises:
        svc.collections.resolve_collection_id = AsyncMock(side_effect=collection_raises)
    else:
        svc.collections.resolve_collection_id = AsyncMock(return_value=collection_result)
    return svc


def _conn_stub() -> MagicMock:
    return MagicMock(name="conn")


# ---------------------------------------------------------------------------
# get_collection_tilesets
# ---------------------------------------------------------------------------


class TestGetCollectionTilesets:
    @pytest.mark.asyncio
    async def test_happy_path_returns_tms_list(self):
        svc = _make_service()
        svc._resolve_catalog_and_collection = AsyncMock(
            return_value=("internal-cat", "internal-coll")
        )
        svc._require_collection_visible = AsyncMock()

        with patch(
            "dynastore.extensions.tiles.tiles_service.tms_manager.list_custom_tms",
            AsyncMock(return_value=[]),
        ):
            result = await svc.get_collection_tilesets(
                catalog_id="ext-cat",
                collection_id="ext-coll",
                request=_make_request(),
            )

        assert hasattr(result, "tileMatrixSets")
        # At minimum the built-in TMS should be present
        assert len(result.tileMatrixSets) >= 1

    @pytest.mark.asyncio
    async def test_hidden_collection_raises_404(self):
        svc = _make_service()
        svc._resolve_catalog_and_collection = AsyncMock(
            return_value=("internal-cat", "internal-coll")
        )
        svc._require_collection_visible = AsyncMock(
            side_effect=HTTPException(status_code=404, detail="Collection not found.")
        )

        with pytest.raises(HTTPException) as exc_info:
            with patch(
                "dynastore.extensions.tiles.tiles_service.tms_manager.list_custom_tms",
                AsyncMock(return_value=[]),
            ):
                await svc.get_collection_tilesets(
                    catalog_id="ext-cat",
                    collection_id="hidden-coll",
                    request=_make_request(),
                )

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_catalog_not_found_raises_404(self):
        svc = _make_service()
        svc._resolve_catalog_and_collection = AsyncMock(
            side_effect=HTTPException(status_code=404, detail="Catalog 'bad-cat' not found.")
        )

        with pytest.raises(HTTPException) as exc_info:
            await svc.get_collection_tilesets(
                catalog_id="bad-cat",
                collection_id="coll",
                request=_make_request(),
            )

        assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# invalidate_collection_tile_cache
# ---------------------------------------------------------------------------


class TestInvalidateCollectionTileCache:
    @pytest.mark.asyncio
    async def test_happy_path_calls_impl(self):
        svc = _make_service()
        svc._resolve_catalog_and_collection = AsyncMock(
            return_value=("internal-cat", "internal-coll")
        )

        impl_calls: list[tuple] = []

        async def _fake_impl(catalog_id, collection_id):
            impl_calls.append((catalog_id, collection_id))
            return {"message": "ok", "invalidated_targets": [f"{catalog_id}:{collection_id}"]}

        svc._invalidate_tile_cache_impl = _fake_impl

        result = await svc.invalidate_collection_tile_cache(
            catalog_id="ext-cat",
            collection_id="ext-coll",
        )

        assert impl_calls == [("internal-cat", "internal-coll")]
        assert "ok" in result["message"]

    @pytest.mark.asyncio
    async def test_unknown_catalog_raises_404(self):
        svc = _make_service()
        svc._resolve_catalog_and_collection = AsyncMock(
            side_effect=HTTPException(status_code=404, detail="Catalog not found.")
        )

        with pytest.raises(HTTPException) as exc_info:
            await svc.invalidate_collection_tile_cache(
                catalog_id="bad-cat",
                collection_id="coll",
            )

        assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# get_vector_tile_aligned_default / get_vector_tile_aligned
# — just verify ID resolution + delegation; tile-gen logic is tested elsewhere
# ---------------------------------------------------------------------------


class TestGetVectorTileAlignedDefault:
    @pytest.mark.asyncio
    async def test_delegates_to_get_vector_tile_with_internal_ids(self):
        svc = _make_service()
        svc._resolve_catalog_and_collection = AsyncMock(
            return_value=("int-cat", "int-coll")
        )

        delegated: list[dict] = []

        async def _fake_get_vector_tile(**kwargs):
            delegated.append(kwargs)
            return MagicMock(status_code=200)

        svc.get_vector_tile = _fake_get_vector_tile

        await svc.get_vector_tile_aligned_default(
            catalog_id="ext-cat",
            collection_id="ext-coll",
            z=5,
            x=1,
            y=1,
            request=_make_request(),
            background_tasks=_make_bg_tasks(),
            conn=_conn_stub(),
            datetime=None,
            filter=None,
            filter_lang="cql2-text",
            subset=None,
            simplification=None,
            simplification_by_zoom=None,
            simplification_algorithm=MagicMock(),
            disable_cache=False,
            refresh_cache=False,
            request_hints=frozenset(),
        )

        assert len(delegated) == 1
        assert delegated[0]["dataset"] == "int-cat"
        assert delegated[0]["collections"] == "int-coll"
        assert delegated[0]["tileMatrixSetId"] == "WebMercatorQuad"

    @pytest.mark.asyncio
    async def test_resolution_failure_raises_404(self):
        svc = _make_service()
        svc._resolve_catalog_and_collection = AsyncMock(
            side_effect=HTTPException(status_code=404, detail="Catalog not found.")
        )

        with pytest.raises(HTTPException) as exc_info:
            await svc.get_vector_tile_aligned_default(
                catalog_id="bad-cat",
                collection_id="coll",
                z=0,
                x=0,
                y=0,
                request=_make_request(),
                background_tasks=_make_bg_tasks(),
                conn=_conn_stub(),
                datetime=None,
                filter=None,
                filter_lang="cql2-text",
                subset=None,
                simplification=None,
                simplification_by_zoom=None,
                simplification_algorithm=MagicMock(),
                disable_cache=False,
                refresh_cache=False,
                request_hints=frozenset(),
            )

        assert exc_info.value.status_code == 404


class TestGetVectorTileAligned:
    @pytest.mark.asyncio
    async def test_delegates_with_explicit_tms(self):
        svc = _make_service()
        svc._resolve_catalog_and_collection = AsyncMock(
            return_value=("int-cat", "int-coll")
        )

        delegated: list[dict] = []

        async def _fake_get_vector_tile(**kwargs):
            delegated.append(kwargs)
            return MagicMock(status_code=200)

        svc.get_vector_tile = _fake_get_vector_tile

        await svc.get_vector_tile_aligned(
            catalog_id="ext-cat",
            collection_id="ext-coll",
            tileMatrixSetId="WorldCRS84Quad",
            z=2,
            x=0,
            y=0,
            format="mvt",
            request=_make_request(),
            background_tasks=_make_bg_tasks(),
            conn=_conn_stub(),
            datetime=None,
            filter=None,
            filter_lang="cql2-text",
            subset=None,
            simplification=None,
            simplification_by_zoom=None,
            simplification_algorithm=MagicMock(),
            disable_cache=False,
            refresh_cache=False,
            request_hints=frozenset(),
        )

        assert len(delegated) == 1
        assert delegated[0]["dataset"] == "int-cat"
        assert delegated[0]["collections"] == "int-coll"
        assert delegated[0]["tileMatrixSetId"] == "WorldCRS84Quad"


# ---------------------------------------------------------------------------
# _invalidate_tile_cache_impl (shared helper)
# ---------------------------------------------------------------------------


class TestInvalidateTileCacheImpl:
    @pytest.mark.asyncio
    async def test_calls_invalidate_collection_tiles(self):
        svc = _make_service()

        import dynastore.modules.tiles.tiles_module as tms_manager_real

        calls: list[tuple] = []

        async def _fake_invalidate_coll(catalog_id, collection_id):
            calls.append(("coll", catalog_id, collection_id))

        with patch.object(tms_manager_real, "invalidate_collection_tiles", _fake_invalidate_coll):
            result = await svc._invalidate_tile_cache_impl("int-cat", "int-coll")

        assert ("coll", "int-cat", "int-coll") in calls
        assert "int-cat" in result["message"]

    @pytest.mark.asyncio
    async def test_calls_invalidate_catalog_tiles_when_no_collection(self):
        svc = _make_service()

        import dynastore.modules.tiles.tiles_module as tms_manager_real

        calls: list[str] = []

        async def _fake_invalidate_cat(catalog_id):
            calls.append(catalog_id)

        with patch.object(tms_manager_real, "invalidate_catalog_tiles", _fake_invalidate_cat):
            result = await svc._invalidate_tile_cache_impl("int-cat", None)

        assert "int-cat" in calls
        assert "int-cat" in result["message"]
