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

"""Regression tests — FIX 2: dispatch result propagation + fail-loud on total miss.

Contracts verified:
  - _dispatch_index_upsert returns Dict[str, BulkResult] (not None).
  - _do_dispatch returns the fan_out_bulk result dict.
  - A silent no-op BulkResult (total>0, succeeded=0, failed=0) from fan_out_bulk
    causes _dispatch_index_upsert to route through on_failure policy:
    the caller receives a result dict containing the no-op entry.
  - main_ingestion marks the task FAILED when all secondary indexers report
    total>0 with succeeded==0 (total miss).
  - main_ingestion keeps COMPLETED but includes index warning in outputs when
    there is a partial miss (succeeded > 0 but < total).
"""
from __future__ import annotations

from typing import Dict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.models.protocols.indexer import BulkResult, IndexOp


# ---------------------------------------------------------------------------
# _do_dispatch / _dispatch_index_upsert return-value contract
# ---------------------------------------------------------------------------


def _make_features(n: int = 3) -> list:
    features = []
    for i in range(n):
        f = MagicMock()
        f.id = f"geoid-{i}"
        f.model_dump.return_value = {
            "id": f"geoid-{i}",
            "type": "Feature",
            "geometry": None,
            "properties": {},
        }
        features.append(f)
    return features


@pytest.mark.asyncio
async def test_do_dispatch_returns_bulk_result_dict():
    """_do_dispatch must return the dict from fan_out_bulk, not None."""
    from dynastore.modules.catalog.item_service import ItemService

    expected = {"items_elasticsearch_driver": BulkResult(total=2, succeeded=2)}

    fake_dispatcher = MagicMock()
    fake_dispatcher.fan_out_bulk = AsyncMock(return_value=expected)

    ops = [
        IndexOp(op_type="upsert", entity_type="item", entity_id="g1"),
        IndexOp(op_type="upsert", entity_type="item", entity_id="g2"),
    ]

    svc = ItemService()
    result = await svc._do_dispatch(
        fake_dispatcher, "cat1", "col1", ops, pg_conn=None,
    )

    assert result is not None, "_do_dispatch must return the bulk result dict"
    assert result == expected


@pytest.mark.asyncio
async def test_dispatch_index_upsert_returns_dict():
    """_dispatch_index_upsert must return Dict[str, BulkResult], not None."""
    from dynastore.modules.catalog.item_service import ItemService

    expected = {"items_elasticsearch_driver": BulkResult(total=1, succeeded=1)}
    features = _make_features(1)

    svc = ItemService()

    # Patch _do_dispatch directly so we do not need to thread through the
    # local get_index_dispatcher import or managed_transaction.
    with patch.object(
        svc,
        "_do_dispatch",
        new=AsyncMock(return_value=expected),
    ):
        with patch.object(
            svc,
            "_resolve_index_stamp_context",
            new=AsyncMock(return_value=MagicMock()),
        ):
            result = await svc._dispatch_index_upsert(
                "cat1", "col1", features, db_resource=None,
            )

    assert result is not None, "_dispatch_index_upsert must return a dict"
    assert isinstance(result, dict)


@pytest.mark.asyncio
async def test_dispatch_index_upsert_threads_write_id_to_ops():
    """A PG-primary write batch id must ride every IndexOp so the generic
    async writer can enqueue one grouped ledger row instead of per-item payloads.
    """
    from dynastore.modules.catalog.item_service import ItemService

    expected = {"items_elasticsearch_driver": BulkResult(total=2, succeeded=2)}
    features = _make_features(2)
    captured_ops = []

    async def _capture_dispatch(
        dispatcher, catalog_id, collection_id, ops, *, pg_conn, tx_factory=None
    ):
        captured_ops.extend(ops)
        return expected

    svc = ItemService()
    with patch.object(svc, "_do_dispatch", new=_capture_dispatch):
        with patch.object(
            svc,
            "_resolve_index_stamp_context",
            new=AsyncMock(return_value=MagicMock()),
        ):
            result = await svc._dispatch_index_upsert(
                "cat1",
                "col1",
                features,
                db_resource=None,
                processing_context={"write_id": "w-batch-1"},
            )

    assert result == expected
    assert captured_ops
    assert {getattr(op, "write_id", None) for op in captured_ops} == {"w-batch-1"}


@pytest.mark.asyncio
async def test_do_dispatch_noop_bulk_result_not_masked():
    """A silent no-op from fan_out_bulk (total>0, succeeded=0, failed=0) must be
    returned to the caller, not silently discarded.
    """
    from dynastore.modules.catalog.item_service import ItemService

    noop = {"items_elasticsearch_driver": BulkResult(total=2, succeeded=0, failed=0)}

    fake_dispatcher = MagicMock()
    fake_dispatcher.fan_out_bulk = AsyncMock(return_value=noop)

    ops = [
        IndexOp(op_type="upsert", entity_type="item", entity_id="g1"),
        IndexOp(op_type="upsert", entity_type="item", entity_id="g2"),
    ]

    svc = ItemService()
    result = await svc._do_dispatch(
        fake_dispatcher, "cat1", "col1", ops, pg_conn=None,
    )

    assert result is not None
    dr = result.get("items_elasticsearch_driver")
    assert dr is not None, "No-op result must be present in returned dict"
    assert dr.total == 2
    assert dr.succeeded == 0
    assert dr.failed == 0, "A no-op (succeeded=0, failed=0) must propagate, not be silently masked"


# ---------------------------------------------------------------------------
# Ingestion task marks FAILED on total miss
# ---------------------------------------------------------------------------


def _make_ingestion_summary_tester():
    """Returns a helper that captures reporter.task_finished calls."""
    finished_calls: list = []

    class _MockReporter:
        async def task_started(self, *a, **kw):
            pass

        async def update_progress(self, *a, **kw):
            pass

        async def process_batch_outcome(self, *a, **kw):
            pass

        async def task_finished(self, status: str, **kwargs):
            finished_calls.append({"status": status, **kwargs})

    return _MockReporter(), finished_calls


@pytest.mark.asyncio
async def test_ingestion_marks_failed_on_total_index_miss():
    """When features are written to PG but all secondary indexers return
    succeeded==0 (total miss), the ingestion task must call
    reporter.task_finished('FAILED') — not 'COMPLETED'.

    This test exercises the ingestion-level health check introduced by FIX 2.
    """
    from dynastore.tasks.ingestion.main_ingestion import _check_index_health

    # All indexers failed (total miss): total=5, succeeded=0, failed=0 (no-op)
    index_results: Dict[str, BulkResult] = {
        "items_elasticsearch_driver": BulkResult(total=5, succeeded=0, failed=0),
    }

    status, msg = _check_index_health(
        rows_written=5, index_results=index_results,
    )

    assert status == "FAILED", (
        "A total secondary-index miss (succeeded=0 across all indexers) must "
        "produce status='FAILED', not 'COMPLETED'."
    )
    assert msg is not None, "A failure status must include a human-readable message"


@pytest.mark.asyncio
async def test_ingestion_keeps_completed_on_partial_miss():
    """When some items were indexed successfully (succeeded > 0 but < total),
    the ingestion keeps COMPLETED but must carry a warning in the returned message.
    """
    from dynastore.tasks.ingestion.main_ingestion import _check_index_health

    index_results: Dict[str, BulkResult] = {
        "items_elasticsearch_driver": BulkResult(total=10, succeeded=7, failed=3),
    }

    status, msg = _check_index_health(
        rows_written=10, index_results=index_results,
    )

    assert status == "COMPLETED", (
        "A partial miss (some items indexed) should still be COMPLETED."
    )
    assert msg is not None, "A partial miss must carry a warning message"
    assert "7" in msg or "indexed" in msg.lower() or "partial" in msg.lower(), (
        "Partial-miss message must mention the indexed count or indicate partial success."
    )


@pytest.mark.asyncio
async def test_ingestion_completed_on_full_success():
    """When all items were indexed, COMPLETED with no warning."""
    from dynastore.tasks.ingestion.main_ingestion import _check_index_health

    index_results: Dict[str, BulkResult] = {
        "items_elasticsearch_driver": BulkResult(total=5, succeeded=5, failed=0),
    }

    status, msg = _check_index_health(
        rows_written=5, index_results=index_results,
    )

    assert status == "COMPLETED"
    # No failure message needed on full success.
    assert msg is None or "partial" not in (msg or "").lower()


@pytest.mark.asyncio
async def test_ingestion_completed_when_no_secondary_indexers():
    """When fan_out_bulk returns {} (no secondary indexers configured),
    ingestion keeps COMPLETED — this is a valid routing config, not a failure.
    """
    from dynastore.tasks.ingestion.main_ingestion import _check_index_health

    status, msg = _check_index_health(rows_written=5, index_results={})

    assert status == "COMPLETED", (
        "No secondary indexers configured is not a failure — routing may "
        "intentionally omit ES. Must stay COMPLETED."
    )
