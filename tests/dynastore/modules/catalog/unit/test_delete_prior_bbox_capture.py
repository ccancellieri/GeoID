"""Unit coverage for ``ItemQueryMixin._capture_prior_bbox_for_delete`` (#1297).

Phase 2 tile-cache invalidation needs the extent a feature USED to occupy so a
delete can drop the tiles it used to occupy. The capture helper reads that
extent off the materialized bbox envelope via the normal read path BEFORE the
soft-delete. It is gated on ``is_tile_cache_active`` (non-tile deployments pay
nothing) and degrade-safe (never raises — a capture failure must not block a
delete). These tests pin that contract without a DB.
"""
from __future__ import annotations

import pytest

import dynastore.modules.tiles.tile_cache_sync as tcs
from dynastore.modules.catalog.item_service import ItemService


def _svc_with_get_item(get_item):
    """An ItemService with ``get_item`` stubbed; ``__init__`` skipped (the
    helper only touches ``get_item`` plus module imports)."""
    svc = ItemService.__new__(ItemService)
    svc.get_item = get_item  # type: ignore[attr-defined]
    return svc


@pytest.mark.asyncio
async def test_capture_returns_bbox_when_active(monkeypatch):
    async def _active(*_a, **_k):
        return True

    async def _get_item(catalog_id, collection_id, item_id, ctx=None):
        return {"id": item_id, "bbox": [12.0, 41.0, 13.0, 42.0]}

    monkeypatch.setattr(tcs, "is_tile_cache_active", _active)
    svc = _svc_with_get_item(_get_item)

    bb = await svc._capture_prior_bbox_for_delete("cat", "col", "x", None)
    assert bb == (12.0, 41.0, 13.0, 42.0)


@pytest.mark.asyncio
async def test_capture_skips_read_when_inactive(monkeypatch):
    read_called = False

    async def _inactive(*_a, **_k):
        return False

    async def _get_item(*_a, **_k):
        nonlocal read_called
        read_called = True
        return {"bbox": [0, 0, 1, 1]}

    monkeypatch.setattr(tcs, "is_tile_cache_active", _inactive)
    svc = _svc_with_get_item(_get_item)

    bb = await svc._capture_prior_bbox_for_delete("cat", "col", "x", None)
    assert bb is None
    assert read_called is False, "must not read the item when the cache is off"


@pytest.mark.asyncio
async def test_capture_none_when_item_missing(monkeypatch):
    async def _active(*_a, **_k):
        return True

    async def _get_item(*_a, **_k):
        return None

    monkeypatch.setattr(tcs, "is_tile_cache_active", _active)
    svc = _svc_with_get_item(_get_item)

    bb = await svc._capture_prior_bbox_for_delete("cat", "col", "x", None)
    assert bb is None


@pytest.mark.asyncio
async def test_capture_degrades_to_none_on_error(monkeypatch):
    async def _active(*_a, **_k):
        return True

    async def _get_item(*_a, **_k):
        raise RuntimeError("read boom")

    monkeypatch.setattr(tcs, "is_tile_cache_active", _active)
    svc = _svc_with_get_item(_get_item)

    # Must not raise — capturing the prior bbox cannot break a delete.
    bb = await svc._capture_prior_bbox_for_delete("cat", "col", "x", None)
    assert bb is None
