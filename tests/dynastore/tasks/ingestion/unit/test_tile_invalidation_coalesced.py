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

"""Regression coverage: a bulk ingestion must enqueue ONE coalesced
``tiles_invalidate`` for the whole ingested extent, not one per write batch.

The write path (``ItemService._dispatch_tile_cache_invalidation``) fires once per
``upsert`` call; ingestion calls ``upsert`` once per batch, so a large ingestion
would spawn hundreds of redundant invalidation tasks. The ingestion task signals
``defer_tile_invalidation`` in its upsert ``processing_context`` to suppress the
per-batch enqueue, then enqueues a single invalidation for the recomputed extent.
"""

from __future__ import annotations

import inspect
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.catalog.item_service import ItemService


# ---------------------------------------------------------------------------
# Runtime: the deferred write path must NOT enqueue per batch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatch_skips_enqueue_when_deferred(monkeypatch):
    svc = ItemService(engine=MagicMock())
    enqueue = AsyncMock()
    monkeypatch.setattr(
        "dynastore.modules.tiles.tile_cache_sync.enqueue_tile_invalidation_task",
        enqueue,
    )
    await svc._dispatch_tile_cache_invalidation(
        "cat",
        "col",
        [{"id": "1"}],
        db_resource=MagicMock(),
        processing_context={"defer_tile_invalidation": True},
    )
    enqueue.assert_not_awaited()


@pytest.mark.asyncio
async def test_dispatch_noop_when_nothing_to_invalidate(monkeypatch):
    """Sanity: empty results + no prior bboxes is still a no-op (unchanged)."""
    svc = ItemService(engine=MagicMock())
    enqueue = AsyncMock()
    monkeypatch.setattr(
        "dynastore.modules.tiles.tile_cache_sync.enqueue_tile_invalidation_task",
        enqueue,
    )
    await svc._dispatch_tile_cache_invalidation(
        "cat", "col", [], db_resource=MagicMock(), processing_context={}
    )
    enqueue.assert_not_awaited()


# ---------------------------------------------------------------------------
# Source-shape guards
# ---------------------------------------------------------------------------


def test_dispatch_source_honours_defer_flag():
    src = inspect.getsource(ItemService._dispatch_tile_cache_invalidation)
    assert "defer_tile_invalidation" in src, (
        "_dispatch_tile_cache_invalidation no longer honours "
        "defer_tile_invalidation — bulk ingestion will resume enqueueing one "
        "tiles_invalidate task per batch."
    )


def test_ingestion_defers_and_coalesces_invalidation():
    from dynastore.tasks.ingestion.main_ingestion import run_ingestion_task

    src = inspect.getsource(run_ingestion_task)
    assert '"defer_tile_invalidation": True' in src, (
        "run_ingestion_task must set defer_tile_invalidation in the upsert "
        "processing_context so the per-batch write path skips invalidation."
    )
    assert "enqueue_tile_invalidation_task" in src, (
        "run_ingestion_task must enqueue ONE coalesced tiles_invalidate after "
        "the write loop for the whole ingested extent."
    )
    assert "ingested_extent" in src and "prior_bboxes=ingested_extent" in src, (
        "the coalesced invalidation must cover the recomputed ingested extent."
    )


def test_recalculate_extents_returns_bbox():
    from dynastore.modules.catalog.tools import recalculate_and_update_extents

    src = inspect.getsource(recalculate_and_update_extents)
    assert "return bbox" in src, (
        "recalculate_and_update_extents must return the computed extent bbox so "
        "ingestion can reuse it for the coalesced tile invalidation."
    )
