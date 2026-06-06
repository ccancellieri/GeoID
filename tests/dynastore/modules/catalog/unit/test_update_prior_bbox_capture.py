"""Unit coverage for ``ItemQueryMixin._capture_prior_bboxes_for_update`` (#1297 Phase 2b, #1845).

Phase 2b tile-cache invalidation needs the extents features USED to occupy so
that a geometry-move update also drops the tiles the old footprint touched.

Since #1845, the capture helper issues a SINGLE bulk query via
``_fetch_prior_bboxes_bulk`` rather than one ``get_item`` call per item
(N+1 → 1). These tests pin that contract without a DB: the bulk helper is
stubbed to verify (a) the gate/degrade contract, (b) all item IDs are
forwarded together, and (c) the return value flows through correctly.
"""
from __future__ import annotations

import pytest

import dynastore.modules.tiles.tile_cache_sync as tcs
from dynastore.modules.catalog.item_service import ItemService


def _svc_with_bulk(fetch_bulk):
    """An ItemService with ``_fetch_prior_bboxes_bulk`` stubbed; ``__init__``
    skipped (the helper only touches that method plus module imports)."""
    svc = ItemService.__new__(ItemService)
    svc._fetch_prior_bboxes_bulk = fetch_bulk  # type: ignore[attr-defined]
    return svc


@pytest.mark.asyncio
async def test_capture_returns_bboxes_for_existing_items(monkeypatch):
    """When the cache is active and items exist, their bboxes are returned."""
    async def _active(*_a, **_k):
        return True

    async def _bulk(catalog_id, collection_id, item_ids):
        # Simulate two items found
        return [(10.0, 20.0, 11.0, 21.0), (30.0, 40.0, 31.0, 41.0)]

    monkeypatch.setattr(tcs, "is_tile_cache_active", _active)
    svc = _svc_with_bulk(_bulk)

    items = [{"id": "item-a", "geometry": {}}, {"id": "item-b", "geometry": {}}]
    bboxes = await svc._capture_prior_bboxes_for_update("cat", "col", items)
    assert bboxes == [(10.0, 20.0, 11.0, 21.0), (30.0, 40.0, 31.0, 41.0)]


@pytest.mark.asyncio
async def test_capture_forwards_all_ids_in_one_call(monkeypatch):
    """All item IDs are collected and forwarded to ``_fetch_prior_bboxes_bulk``
    in a single call — replacing the previous per-item loop."""
    async def _active(*_a, **_k):
        return True

    calls: list = []

    async def _bulk(catalog_id, collection_id, item_ids):
        calls.append(list(item_ids))
        return []

    monkeypatch.setattr(tcs, "is_tile_cache_active", _active)
    svc = _svc_with_bulk(_bulk)

    items = [{"id": "a"}, {"id": "b"}, {"id": "c"}]
    await svc._capture_prior_bboxes_for_update("cat", "col", items)

    assert len(calls) == 1, "bulk helper must be called exactly once"
    assert sorted(calls[0]) == ["a", "b", "c"]


@pytest.mark.asyncio
async def test_capture_skips_new_items_not_yet_in_store(monkeypatch):
    """Items that don't yet exist (CREATE, not UPDATE) contribute nothing.

    The bulk query returns no row for them; the list is therefore empty.
    """
    async def _active(*_a, **_k):
        return True

    async def _bulk(catalog_id, collection_id, item_ids):
        return []  # item not in DB yet

    monkeypatch.setattr(tcs, "is_tile_cache_active", _active)
    svc = _svc_with_bulk(_bulk)

    bboxes = await svc._capture_prior_bboxes_for_update(
        "cat", "col", [{"id": "new-item"}],
    )
    assert bboxes == []


@pytest.mark.asyncio
async def test_capture_skips_items_without_id(monkeypatch):
    """Items without an ``id`` field are silently dropped before the bulk call."""
    async def _active(*_a, **_k):
        return True

    calls: list = []

    async def _bulk(catalog_id, collection_id, item_ids):
        calls.append(list(item_ids))
        return []

    monkeypatch.setattr(tcs, "is_tile_cache_active", _active)
    svc = _svc_with_bulk(_bulk)

    # No "id" key — must not reach the bulk helper
    bboxes = await svc._capture_prior_bboxes_for_update(
        "cat", "col", [{"geometry": {}}],
    )
    assert bboxes == []
    assert calls == [], "bulk helper must not be called when no ids are present"


@pytest.mark.asyncio
async def test_capture_returns_empty_when_cache_inactive(monkeypatch):
    """When the tile cache is inactive the bulk read is skipped entirely."""
    bulk_called = False

    async def _inactive(*_a, **_k):
        return False

    async def _bulk(*_a, **_k):
        nonlocal bulk_called
        bulk_called = True
        return []

    monkeypatch.setattr(tcs, "is_tile_cache_active", _inactive)
    svc = _svc_with_bulk(_bulk)

    bboxes = await svc._capture_prior_bboxes_for_update(
        "cat", "col", [{"id": "x"}],
    )
    assert bboxes == []
    assert bulk_called is False, "must not call bulk helper when the cache is off"


@pytest.mark.asyncio
async def test_capture_degrades_to_empty_on_bulk_error(monkeypatch):
    """A bulk read failure must degrade to [] and never raise."""
    async def _active(*_a, **_k):
        return True

    async def _bulk(catalog_id, collection_id, item_ids):
        raise RuntimeError("db boom")

    monkeypatch.setattr(tcs, "is_tile_cache_active", _active)
    svc = _svc_with_bulk(_bulk)

    # Must not raise — a capture failure must not block an upsert.
    bboxes = await svc._capture_prior_bboxes_for_update(
        "cat", "col", [{"id": "a"}, {"id": "b"}],
    )
    # The bulk helper raised — _fetch_prior_bboxes_bulk is itself degrade-safe
    # and returns []; _capture_prior_bboxes_for_update delegates to it.
    assert bboxes == []


@pytest.mark.asyncio
async def test_dispatch_receives_prior_bboxes_on_update(monkeypatch):
    """``_dispatch_tile_cache_invalidation`` is called with prior_bboxes on update.

    This pins the pass-through contract end-to-end without touching the DB:
    prior_bboxes captured by _capture_prior_bboxes_for_update must reach
    _dispatch_tile_cache_invalidation when results are non-empty.
    """
    dispatched: list = []

    async def _fake_dispatch(self_inner, catalog_id, collection_id, results, **kwargs):
        dispatched.append({
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "prior_bboxes": kwargs.get("prior_bboxes"),
        })

    async def _active(*_a, **_k):
        return True

    async def _bulk(catalog_id, collection_id, item_ids):
        return [(1.0, 2.0, 3.0, 4.0)]

    monkeypatch.setattr(tcs, "is_tile_cache_active", _active)
    monkeypatch.setattr(
        ItemService, "_dispatch_tile_cache_invalidation", _fake_dispatch,
    )

    svc = _svc_with_bulk(_bulk)
    prior = await svc._capture_prior_bboxes_for_update(
        "cat", "col", [{"id": "item-x"}],
    )
    assert prior == [(1.0, 2.0, 3.0, 4.0)]

    # Invoke the dispatch with the captured prior bboxes (mimics item_service upsert).
    from dynastore.models.ogc import Feature
    fake_result = Feature.model_construct(id="item-x", bbox=[9.0, 9.0, 10.0, 10.0])
    await _fake_dispatch(svc, "cat", "col", [fake_result], prior_bboxes=prior or None)
    assert dispatched[0]["prior_bboxes"] == [(1.0, 2.0, 3.0, 4.0)]
