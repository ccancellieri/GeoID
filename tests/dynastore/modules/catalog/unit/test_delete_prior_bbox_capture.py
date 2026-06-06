"""Unit coverage for ``ItemQueryMixin._capture_prior_bbox_for_delete`` (#1297, #1845).

Phase 2 tile-cache invalidation needs the extent a feature USED to occupy so a
delete can drop the tiles it used to occupy. Since #1845, the capture helper
delegates to ``_fetch_prior_bboxes_bulk`` (a single bulk query) rather than
calling ``get_item`` directly. It is gated on ``is_tile_cache_active`` (non-tile
deployments pay nothing) and degrade-safe (never raises — a capture failure
must not block a delete). These tests pin that contract without a DB.
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
async def test_capture_returns_bbox_when_active(monkeypatch):
    async def _active(*_a, **_k):
        return True

    async def _bulk(catalog_id, collection_id, item_ids):
        # Simulate the item found with a bbox
        return [(12.0, 41.0, 13.0, 42.0)]

    monkeypatch.setattr(tcs, "is_tile_cache_active", _active)
    svc = _svc_with_bulk(_bulk)

    bb = await svc._capture_prior_bbox_for_delete("cat", "col", "x", None)
    assert bb == (12.0, 41.0, 13.0, 42.0)


@pytest.mark.asyncio
async def test_capture_skips_read_when_inactive(monkeypatch):
    bulk_called = False

    async def _inactive(*_a, **_k):
        return False

    async def _bulk(*_a, **_k):
        nonlocal bulk_called
        bulk_called = True
        return [(0.0, 0.0, 1.0, 1.0)]

    monkeypatch.setattr(tcs, "is_tile_cache_active", _inactive)
    svc = _svc_with_bulk(_bulk)

    bb = await svc._capture_prior_bbox_for_delete("cat", "col", "x", None)
    assert bb is None
    assert bulk_called is False, "must not call bulk helper when the cache is off"


@pytest.mark.asyncio
async def test_capture_none_when_item_missing(monkeypatch):
    async def _active(*_a, **_k):
        return True

    async def _bulk(catalog_id, collection_id, item_ids):
        return []  # item not found

    monkeypatch.setattr(tcs, "is_tile_cache_active", _active)
    svc = _svc_with_bulk(_bulk)

    bb = await svc._capture_prior_bbox_for_delete("cat", "col", "x", None)
    assert bb is None


@pytest.mark.asyncio
async def test_capture_degrades_to_none_on_error(monkeypatch):
    async def _active(*_a, **_k):
        return True

    async def _bulk(*_a, **_k):
        raise RuntimeError("read boom")

    monkeypatch.setattr(tcs, "is_tile_cache_active", _active)
    svc = _svc_with_bulk(_bulk)

    # Must not raise — capturing the prior bbox cannot break a delete.
    bb = await svc._capture_prior_bbox_for_delete("cat", "col", "x", None)
    assert bb is None
