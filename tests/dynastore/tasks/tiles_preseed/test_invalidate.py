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

"""``TilePreseedTask`` ``operation=invalidate`` light delete path (#1298).

DB-free: drives ``_invalidate_tiles`` directly with a fake config manager and a
fake ``TileStorageProtocol`` provider that records deletes. The fake's method
signatures match the REAL protocol, so a green run is genuine verification.
"""
from __future__ import annotations

from typing import List, Sequence, Tuple

import pytest

import dynastore.tasks.tiles_preseed.task as task_mod
from dynastore.tasks.tiles_preseed.task import TilePreseedTask
from dynastore.tasks.tiles_preseed.models import TilePreseedRequest
from dynastore.modules.processes.models import ExecuteRequest
from dynastore.modules.tasks.models import TaskPayload
from dynastore.modules.tiles.tiles_config import TilesConfig, TilesPreseedConfig
from dynastore.modules.tiles.tile_cache_sync import (
    SERVED_TILE_FORMATS,
    affected_tiles_for_batch,
)


class _FakeStore:
    """Fake TileStorageProtocol recording coordinate-variant deletes.

    Signature of ``delete_tile_variants`` matches the real protocol exactly.
    """

    def __init__(self) -> None:
        self.variant_calls: List[Tuple] = []
        self.per_format: List[Tuple] = []

    async def delete_tile_variants(
        self, catalog_id, collection_id, tms_id, z, x, y, formats: Sequence[str],
    ) -> bool:
        self.variant_calls.append(
            (catalog_id, collection_id, tms_id, z, x, y, tuple(formats))
        )
        return True


class _LegacyStore:
    """Fake provider WITHOUT the variant primitive — exercises the per-format fallback."""

    def __init__(self) -> None:
        self.deleted: List[Tuple] = []

    async def delete_tile(self, catalog_id, collection_id, tms_id, z, x, y, format) -> bool:
        self.deleted.append((catalog_id, collection_id, tms_id, z, x, y, format))
        return True


class _FakeConfigManager:
    def __init__(self, preseed: TilesPreseedConfig, runtime: TilesConfig) -> None:
        self._preseed = preseed
        self._runtime = runtime

    async def get_config(self, model, catalog_id, collection_id=None):
        if model is TilesPreseedConfig:
            return self._preseed
        if model is TilesConfig:
            return self._runtime
        return None


def _make_task() -> TilePreseedTask:
    # Bypass __init__ (which calls get_engine()) — we drive _invalidate_tiles
    # directly with explicit engine/schema args.
    return TilePreseedTask.__new__(TilePreseedTask)


def _make_payload() -> TaskPayload:
    # task_id=None skips the DB update_task calls; we test the delete fan-out.
    from uuid import uuid4

    return TaskPayload(
        task_id=uuid4(),
        caller_id="test",
        inputs=ExecuteRequest(inputs={}),
    )


@pytest.mark.asyncio
async def test_invalidate_deletes_covered_tiles(monkeypatch):
    store = _FakeStore()
    monkeypatch.setattr(task_mod, "get_protocol", lambda _proto: store)

    runtime = TilesConfig(min_zoom=0, max_zoom=2, supported_tms_ids=["WebMercatorQuad"])
    preseed = TilesPreseedConfig(preseed_enabled=True, target_tms_ids=["WebMercatorQuad"])
    cfgmgr = _FakeConfigManager(preseed, runtime)

    bbox = [12.0, 41.0, 13.0, 42.0]
    request = TilePreseedRequest(
        catalog_id="cat", collection_id="col",
        operation="invalidate", update_bbox=[tuple(bbox)],
    )

    # task_id=None → no DB update_task; just exercise the delete fan-out.
    payload = _make_payload()
    payload.task_id = None  # type: ignore[assignment]

    task = _make_task()
    result = await task._invalidate_tiles(
        engine=object(), request=request, payload=payload,
        config_manager=cfgmgr, catalog_id="cat", schema="s_cat",
    )
    assert result is None

    # Expected covered tiles from the same pure math the production path uses.
    expected = affected_tiles_for_batch(
        [tuple(bbox)], ["WebMercatorQuad"], 0, 2,
    )
    assert len(store.variant_calls) == len(expected)
    # Every delete used the full served-format set for the (cat, col) coordinate.
    deleted_coords = set()
    for (cat, col, tms_id, z, x, y, formats) in store.variant_calls:
        assert (cat, col) == ("cat", "col")
        assert set(formats) == set(SERVED_TILE_FORMATS)
        deleted_coords.add((tms_id, z, x, y))
    assert deleted_coords == expected


@pytest.mark.asyncio
async def test_invalidate_legacy_fallback_deletes_each_format(monkeypatch):
    store = _LegacyStore()
    monkeypatch.setattr(task_mod, "get_protocol", lambda _proto: store)

    runtime = TilesConfig(min_zoom=5, max_zoom=5, supported_tms_ids=["WebMercatorQuad"])
    preseed = TilesPreseedConfig(preseed_enabled=True, target_tms_ids=["WebMercatorQuad"])
    cfgmgr = _FakeConfigManager(preseed, runtime)

    request = TilePreseedRequest(
        catalog_id="cat", collection_id="col",
        operation="invalidate", update_bbox=[(12.0, 41.0, 12.01, 41.01)],
    )
    payload = _make_payload()
    payload.task_id = None  # type: ignore[assignment]

    task = _make_task()
    await task._invalidate_tiles(
        engine=object(), request=request, payload=payload,
        config_manager=cfgmgr, catalog_id="cat", schema="s_cat",
    )
    # Per-format delete_tile for each served format on each covered coordinate.
    assert len(store.deleted) >= len(SERVED_TILE_FORMATS)
    deleted_formats = {d[6] for d in store.deleted}
    assert deleted_formats == set(SERVED_TILE_FORMATS)


@pytest.mark.asyncio
async def test_invalidate_degrades_when_no_store(monkeypatch):
    monkeypatch.setattr(task_mod, "get_protocol", lambda _proto: None)

    runtime = TilesConfig(min_zoom=0, max_zoom=2, supported_tms_ids=["WebMercatorQuad"])
    preseed = TilesPreseedConfig(preseed_enabled=True, target_tms_ids=["WebMercatorQuad"])
    cfgmgr = _FakeConfigManager(preseed, runtime)

    request = TilePreseedRequest(
        catalog_id="cat", collection_id="col",
        operation="invalidate", update_bbox=[(12.0, 41.0, 13.0, 42.0)],
    )
    payload = _make_payload()
    payload.task_id = None  # type: ignore[assignment]

    task = _make_task()
    # Missing provider → log + complete, no crash.
    result = await task._invalidate_tiles(
        engine=object(), request=request, payload=payload,
        config_manager=cfgmgr, catalog_id="cat", schema="s_cat",
    )
    assert result is None


@pytest.mark.asyncio
async def test_invalidate_uses_request_update_bbox(monkeypatch):
    """update_bbox from the request drives coverage (not the config bbox)."""
    store = _FakeStore()
    monkeypatch.setattr(task_mod, "get_protocol", lambda _proto: store)

    # Config bbox is a different, larger area; the request's small bbox must win.
    runtime = TilesConfig(
        min_zoom=4, max_zoom=4, supported_tms_ids=["WebMercatorQuad"],
        bbox=[(-180.0, -85.0, 180.0, 85.0)],
    )
    preseed = TilesPreseedConfig(preseed_enabled=True, target_tms_ids=["WebMercatorQuad"])
    cfgmgr = _FakeConfigManager(preseed, runtime)

    small = (12.0, 41.0, 12.01, 41.01)
    request = TilePreseedRequest(
        catalog_id="cat", collection_id="col",
        operation="invalidate", update_bbox=[small],
    )
    payload = _make_payload()
    payload.task_id = None  # type: ignore[assignment]

    task = _make_task()
    await task._invalidate_tiles(
        engine=object(), request=request, payload=payload,
        config_manager=cfgmgr, catalog_id="cat", schema="s_cat",
    )
    expected = affected_tiles_for_batch([small], ["WebMercatorQuad"], 4, 4)
    assert len(store.variant_calls) == len(expected)
    # A small bbox at z4 covers far fewer tiles than the world bbox would.
    world = affected_tiles_for_batch(
        [(-180.0, -85.0, 180.0, 85.0)], ["WebMercatorQuad"], 4, 4,
    )
    assert len(expected) < len(world)
