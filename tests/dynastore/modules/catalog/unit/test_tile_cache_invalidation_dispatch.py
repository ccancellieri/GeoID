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

"""``ItemService._dispatch_tile_cache_invalidation`` (#1292 / #1298).

Verifies the write-path hook:
* enqueues a ``tiles_preseed`` invalidate task via
  ``enqueue_tile_invalidation_task``, passing the resolved DB engine + tenant
  physical schema (needed to INSERT the task row),
* is a no-op for an empty batch,
* NEVER raises out — a cache failure must not break the write.
"""
from __future__ import annotations

from typing import Any, List

import pytest

import dynastore.modules.tiles.tile_cache_sync as tcs
from dynastore.modules.catalog.item_service import ItemService


@pytest.mark.asyncio
async def test_dispatch_enqueues_invalidate_task_with_engine_and_schema(monkeypatch):
    calls: List[Any] = []

    async def _fake_enqueue(
        catalog_id, collection_id, features, *, engine, schema, prior_bboxes=None,
    ):
        calls.append((catalog_id, collection_id, list(features), engine, schema))
        return len(features)

    async def _fake_resolve_schema(self, catalog_id, db_resource=None):
        return "s_cat"

    monkeypatch.setattr(tcs, "enqueue_tile_invalidation_task", _fake_enqueue)
    monkeypatch.setattr(
        ItemService, "_resolve_physical_schema", _fake_resolve_schema, raising=False
    )

    engine = object()
    svc = ItemService(engine=engine)  # type: ignore[arg-type]
    feats = [{"id": "a", "bbox": [0, 0, 1, 1]}]
    await svc._dispatch_tile_cache_invalidation("cat", "col", feats)  # type: ignore[arg-type]

    assert len(calls) == 1
    cat, col, passed, used_engine, schema = calls[0]
    assert (cat, col) == ("cat", "col")
    assert passed == feats
    assert used_engine is engine
    assert schema == "s_cat"


@pytest.mark.asyncio
async def test_dispatch_uses_explicit_db_resource(monkeypatch):
    """A db_resource kwarg is preferred over self.engine for the enqueue."""
    seen: List[Any] = []

    async def _fake_enqueue(
        catalog_id, collection_id, features, *, engine, schema, prior_bboxes=None,
    ):
        seen.append(engine)
        return 0

    async def _fake_resolve_schema(self, catalog_id, db_resource=None):
        return "s_cat"

    monkeypatch.setattr(tcs, "enqueue_tile_invalidation_task", _fake_enqueue)
    monkeypatch.setattr(
        ItemService, "_resolve_physical_schema", _fake_resolve_schema, raising=False
    )

    explicit = object()
    svc = ItemService(engine=object())  # type: ignore[arg-type]
    await svc._dispatch_tile_cache_invalidation(
        "cat", "col", [{"id": "a", "bbox": [0, 0, 1, 1]}],  # type: ignore[arg-type]
        db_resource=explicit,  # type: ignore[arg-type]
    )
    assert seen == [explicit]


@pytest.mark.asyncio
async def test_dispatch_noop_without_engine(monkeypatch):
    """No engine in scope → skip the enqueue (cannot create the task row)."""
    called = False

    async def _fake_enqueue(*a, **k):
        nonlocal called
        called = True
        return 0

    monkeypatch.setattr(tcs, "enqueue_tile_invalidation_task", _fake_enqueue)
    svc = ItemService(engine=None)  # type: ignore[arg-type]
    await svc._dispatch_tile_cache_invalidation(
        "cat", "col", [{"id": "a", "bbox": [0, 0, 1, 1]}],  # type: ignore[arg-type]
    )
    assert called is False


@pytest.mark.asyncio
async def test_dispatch_noop_on_empty(monkeypatch):
    called = False

    async def _fake_enqueue(*a, **k):
        nonlocal called
        called = True
        return 0

    monkeypatch.setattr(tcs, "enqueue_tile_invalidation_task", _fake_enqueue)
    svc = ItemService(engine=object())  # type: ignore[arg-type]
    await svc._dispatch_tile_cache_invalidation("cat", "col", [])  # type: ignore[arg-type]
    assert called is False


@pytest.mark.asyncio
async def test_dispatch_passes_prior_bboxes_with_empty_results(monkeypatch):
    """Phase 2 (#1297): a delete dispatches with empty ``results`` and a
    ``prior_bboxes`` extent — the enqueue still fires and forwards the prior
    bbox so the old footprint's tiles are dropped."""
    seen: List[Any] = []

    async def _fake_enqueue(
        catalog_id, collection_id, features, *, engine, schema, prior_bboxes=None,
    ):
        seen.append((list(features), prior_bboxes))
        return 1

    async def _fake_resolve_schema(self, catalog_id, db_resource=None):
        return "s_cat"

    monkeypatch.setattr(tcs, "enqueue_tile_invalidation_task", _fake_enqueue)
    monkeypatch.setattr(
        ItemService, "_resolve_physical_schema", _fake_resolve_schema, raising=False
    )

    svc = ItemService(engine=object())  # type: ignore[arg-type]
    prior = [(12.0, 41.0, 13.0, 42.0)]
    await svc._dispatch_tile_cache_invalidation(
        "cat", "col", [], prior_bboxes=prior,  # type: ignore[arg-type]
    )
    assert seen == [([], prior)]


@pytest.mark.asyncio
async def test_dispatch_never_raises(monkeypatch):
    async def _boom_enqueue(*a, **k):
        raise RuntimeError("boom")

    async def _fake_resolve_schema(self, catalog_id, db_resource=None):
        return "s_cat"

    monkeypatch.setattr(tcs, "enqueue_tile_invalidation_task", _boom_enqueue)
    monkeypatch.setattr(
        ItemService, "_resolve_physical_schema", _fake_resolve_schema, raising=False
    )

    svc = ItemService(engine=object())  # type: ignore[arg-type]
    # Must not raise — invalidation failure cannot break the write.
    await svc._dispatch_tile_cache_invalidation(
        "cat", "col", [{"id": "a", "bbox": [0, 0, 1, 1]}],  # type: ignore[arg-type]
    )
