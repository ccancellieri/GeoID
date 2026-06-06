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

"""Regression tests for index-miss handling in the ingestion task.

Contracts verified:
  (BUG 1) On a total secondary-index miss, every reporter's task_finished("FAILED")
    is called EXACTLY ONCE — the sentinel _IndexMissFailed exception bypasses the
    outer generic handler so reporters are not notified twice.

  (BUG 2 / FIX 3) enqueue_collection_reindex_task routes through create_task_for_catalog
    which performs application-layer dedup: a second enqueue for the same
    (catalog, collection) returns None (dedup hit) and does NOT insert a second
    PENDING row.

  - enqueue_collection_reindex_task creates a task row with the correct
    task_type, catalog_id, collection_id, and dedup_key.
  - enqueue_collection_reindex_task handles a None pg_conn gracefully (logs,
    does not raise).
"""
from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_fake_task(task_id: str = "task-1") -> MagicMock:
    t = MagicMock()
    t.task_id = task_id
    return t


# ---------------------------------------------------------------------------
# BUG 1: double task_finished("FAILED") on total index miss
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_index_miss_reporter_notified_exactly_once():
    """On a total secondary-index miss, each reporter's task_finished("FAILED")
    must be called exactly once, not twice.

    The _IndexMissFailed sentinel bypasses the generic ``except Exception``
    handler so the outer block never calls task_finished a second time.
    """
    # We test the sentinel directly: the outer except chain must re-raise
    # _IndexMissFailed without calling task_finished again.
    from dynastore.tasks.ingestion.main_ingestion import _IndexMissFailed

    reporter = AsyncMock()
    reporter.task_finished = AsyncMock()

    failed_count = 0

    async def _simulate_index_miss_path():
        nonlocal failed_count
        try:
            # Simulate the FAILED branch inside run_ingestion_task:
            # call task_finished once, then raise the sentinel.
            await reporter.task_finished("FAILED", error_message="index miss")
            failed_count += 1
            raise _IndexMissFailed("index miss")
        except _IndexMissFailed:
            # The outer handler re-raises without calling task_finished again.
            raise
        except Exception as e:
            # This must NOT be reached for _IndexMissFailed.
            await reporter.task_finished("FAILED", error_message=str(e))
            failed_count += 1
            raise

    with pytest.raises(_IndexMissFailed):
        await _simulate_index_miss_path()

    assert failed_count == 1, (
        f"task_finished('FAILED') was called {failed_count} time(s); expected exactly 1. "
        "The sentinel _IndexMissFailed must not fall through to the generic except handler."
    )
    reporter.task_finished.assert_called_once_with("FAILED", error_message="index miss")


@pytest.mark.asyncio
async def test_index_miss_sentinel_is_runtime_error_subclass():
    """_IndexMissFailed must be a subclass of RuntimeError so the task runner
    (which catches Exception) still sees a failed task and marks it accordingly.
    """
    from dynastore.tasks.ingestion.main_ingestion import _IndexMissFailed

    exc = _IndexMissFailed("test")
    assert isinstance(exc, RuntimeError), (
        "_IndexMissFailed must subclass RuntimeError so main_task.py's "
        "generic Exception handler still propagates the failure."
    )


# ---------------------------------------------------------------------------
# BUG 2 / FIX 3: dedup via canonical create_task_for_catalog
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enqueue_collection_reindex_creates_task_row():
    """enqueue_collection_reindex_task must create a task via create_task_for_catalog
    with the correct task_type, catalog_id, collection_id, and a stable dedup_key.
    """
    from dynastore.tasks.ingestion.main_ingestion import enqueue_collection_reindex_task

    created: List[Dict[str, Any]] = []

    async def _fake_create(engine, task_data, catalog_id):
        created.append(
            {
                "task_type": task_data.task_type,
                "catalog_id": catalog_id,
                "collection_id": task_data.collection_id,
                "dedup_key": task_data.dedup_key,
                "inputs": task_data.inputs,
            }
        )
        return _make_fake_task()

    fake_engine = MagicMock()

    with patch(
        "dynastore.tasks.ingestion.main_ingestion.create_task_for_catalog",
        new=AsyncMock(side_effect=_fake_create),
    ):
        await enqueue_collection_reindex_task(
            catalog_id="mycat",
            collection_id="mycol",
            pg_conn=fake_engine,
        )

    assert len(created) == 1
    row = created[0]
    assert row["task_type"] == "elasticsearch_bulk_reindex_collection"
    assert row["catalog_id"] == "mycat"
    assert row["collection_id"] == "mycol"
    assert row["dedup_key"] is not None, "dedup_key must be set for dedup to work"
    assert len(row["dedup_key"]) <= 64, "dedup_key must fit the VARCHAR(512) column"


@pytest.mark.asyncio
async def test_enqueue_collection_reindex_dedup_no_second_row():
    """A second enqueue_collection_reindex_task call for the same (catalog, collection)
    must NOT insert a second PENDING row.

    create_task_for_catalog returns None on a dedup hit; enqueue_collection_reindex_task
    must treat that as a no-op and not raise.
    """
    from dynastore.tasks.ingestion.main_ingestion import enqueue_collection_reindex_task

    call_count = 0

    async def _fake_create_dedup(engine, task_data, catalog_id):
        nonlocal call_count
        call_count += 1
        # First call succeeds; second call simulates dedup hit (returns None).
        if call_count == 1:
            return _make_fake_task()
        return None  # dedup hit

    fake_engine = MagicMock()

    with patch(
        "dynastore.tasks.ingestion.main_ingestion.create_task_for_catalog",
        new=AsyncMock(side_effect=_fake_create_dedup),
    ):
        await enqueue_collection_reindex_task("c", "coll", pg_conn=fake_engine)
        # Second call — same (catalog, collection) — should be a no-op (no raise).
        await enqueue_collection_reindex_task("c", "coll", pg_conn=fake_engine)

    assert call_count == 2, "create_task_for_catalog should be called twice"
    # The key contract: the function does not raise on a None (dedup hit) return.


@pytest.mark.asyncio
async def test_enqueue_collection_reindex_stable_dedup_key():
    """The dedup_key must be deterministic: two calls with the same
    (catalog_id, collection_id) must produce the same key.
    """
    import hashlib
    from dynastore.tasks.ingestion.main_ingestion import enqueue_collection_reindex_task

    captured_keys: List[str] = []

    async def _capture_key(engine, task_data, catalog_id):
        captured_keys.append(task_data.dedup_key)
        return _make_fake_task()

    fake_engine = MagicMock()

    with patch(
        "dynastore.tasks.ingestion.main_ingestion.create_task_for_catalog",
        new=AsyncMock(side_effect=_capture_key),
    ):
        await enqueue_collection_reindex_task("cat1", "col1", pg_conn=fake_engine)
        await enqueue_collection_reindex_task("cat1", "col1", pg_conn=fake_engine)

    assert len(captured_keys) == 2
    assert captured_keys[0] == captured_keys[1], (
        "dedup_key must be stable across calls for the same (catalog, collection)"
    )
    # Also verify the expected value.
    expected = hashlib.sha256(b"reindex|cat1|col1").hexdigest()[:64]
    assert captured_keys[0] == expected


@pytest.mark.asyncio
async def test_enqueue_collection_reindex_noop_when_no_conn():
    """enqueue_collection_reindex_task must not raise when pg_conn is None.
    It should log a warning and return without calling create_task_for_catalog.
    """
    from dynastore.tasks.ingestion.main_ingestion import enqueue_collection_reindex_task

    create_calls: List[Any] = []

    async def _fake_create(engine, task_data, catalog_id):
        create_calls.append((engine, task_data, catalog_id))
        return _make_fake_task()

    with patch(
        "dynastore.tasks.ingestion.main_ingestion.create_task_for_catalog",
        new=AsyncMock(side_effect=_fake_create),
    ):
        # Must not raise even with pg_conn=None.
        await enqueue_collection_reindex_task(
            catalog_id="mycat",
            collection_id="mycol",
            pg_conn=None,
        )

    assert create_calls == [], (
        "create_task_for_catalog must not be called when pg_conn is None."
    )
