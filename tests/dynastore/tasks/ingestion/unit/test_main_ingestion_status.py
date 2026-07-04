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

"""Regression coverage for GeoID #2891: ingestion reports COMPLETED when every
row in the run was rejected by the upsert.

Pre-fix, ``rows_ingested`` counted rows PROCESSED (batch size) regardless of
whether the upsert accepted them, and ``_check_index_health`` treated
``rows_written == 0`` as the trivial "nothing to index" COMPLETED case — so a
run where every row failed validation reported success with an empty
FeatureCollection persisted, or worse, none at all.

``run_ingestion_task`` is driven end-to-end here with every external
dependency (catalog protocol, reader, reporters, write-driver ensure_storage,
extent recalculation) stubbed, so the accumulator/gate logic under test runs
unmodified.
"""
from __future__ import annotations

import contextlib
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.models.driver_context import DriverContext
from dynastore.modules.catalog.asset_service import AssetStatus
from dynastore.tasks.ingestion.ingestion_models import (
    ColumnMappingConfig,
    IngestionAsset,
    TaskIngestionRequest,
)


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeReporter:
    """Records every ReportingInterface call for assertion."""

    def __init__(self) -> None:
        self.progress_calls: List[Any] = []
        self.batch_outcomes: List[Any] = []
        self.finished_calls: List[Any] = []

    async def task_started(self, *args, **kwargs) -> None:
        pass

    async def update_progress(self, *args, **kwargs) -> None:
        self.progress_calls.append(args)

    async def process_batch_outcome(self, outcomes) -> None:
        self.batch_outcomes.append(outcomes)

    async def task_finished(self, status, **kwargs) -> None:
        self.finished_calls.append((status, kwargs))


def _make_reader_class(records: List[Dict[str, Any]]):
    """``resolve_reader(...)`` returns a reader CLASS; ``run_ingestion_task``
    instantiates it and calls ``.open(...)`` as a context manager yielding
    an iterable of raw records."""

    class _FakeReader:
        reader_id = "fake_reader"

        def feature_count(self, path, content_type=None):
            return len(records)

        def open(self, path, **kwargs):
            return contextlib.nullcontext(iter(records))

    return _FakeReader


def _make_fake_catalog_module(upsert_side_effect):
    catalog_module = MagicMock()
    catalog_module.resolve_physical_schema = AsyncMock(return_value="tasks")
    catalog_module.ensure_collection_exists = AsyncMock()
    catalog_module.get_collection_config = AsyncMock(return_value=None)
    catalog_module.configs = MagicMock()
    catalog_module.configs.get_config = AsyncMock(return_value=None)
    catalog_module.get_catalog = AsyncMock(return_value=MagicMock())
    catalog_module.get_collection = AsyncMock(return_value=MagicMock())
    catalog_module.upsert = AsyncMock(side_effect=upsert_side_effect)

    fake_asset = MagicMock()
    fake_asset.status = AssetStatus.ACTIVE
    fake_asset.uri = "file:///fake/source.geojson"
    fake_asset.href = None
    fake_asset.asset_id = "asset-1"
    fake_asset.metadata = {}
    fake_asset.kind = "virtual"

    asset_manager = MagicMock()
    asset_manager.get_asset = AsyncMock(return_value=fake_asset)
    asset_manager.create_asset = AsyncMock(return_value=fake_asset)
    asset_manager.add_asset_reference = AsyncMock()
    catalog_module.assets = asset_manager

    return catalog_module, fake_asset


def _upsert_batches(results: List[Any], rejections: List[List[dict]]):
    """Build an ``upsert`` side_effect cycling through per-batch
    (result, rejections) pairs and stamping them onto ``ctx.extensions`` the
    way the real upsert does."""
    call_state = {"i": 0}

    async def _fake_upsert(catalog_id, collection_id, batch, *, ctx: DriverContext, processing_context=None):
        i = call_state["i"]
        call_state["i"] += 1
        result = results[i] if i < len(results) else []
        rej = rejections[i] if i < len(rejections) else []
        ctx.extensions["_rejections"] = rej
        ctx.extensions["_index_results"] = {}
        ctx.extensions["_generated_stats"] = None
        return result

    return _fake_upsert


@contextlib.contextmanager
def _run_ingestion_harness(records, upsert_side_effect):
    """Patch every external seam ``run_ingestion_task`` touches and yield the
    reporter list it will use, so callers can drive ``run_ingestion_task``
    and assert on ``reporter.finished_calls`` / ``reporter.batch_outcomes``.
    """
    catalog_module, _asset = _make_fake_catalog_module(upsert_side_effect)
    reporters = [_FakeReporter()]

    with (
        patch("dynastore.tools.discovery.get_protocol", return_value=catalog_module),
        patch(
            "dynastore.tasks.ingestion.main_ingestion.initialize_reporters",
            return_value=reporters,
        ),
        patch(
            "dynastore.tasks.ingestion.readers.resolve_reader",
            return_value=_make_reader_class(records),
        ),
        patch(
            "dynastore.modules.storage.router.get_write_drivers",
            new=AsyncMock(return_value=[]),
        ),
        patch(
            "dynastore.tasks.ingestion.main_ingestion._maybe_apply_ingest_backpressure",
            new=AsyncMock(),
        ),
        patch(
            "dynastore.tasks.ingestion.main_ingestion.recalculate_and_update_extents",
            new=AsyncMock(return_value=None),
        ),
        patch(
            "dynastore.tasks.ingestion.main_ingestion.enqueue_collection_reindex_task",
            new=AsyncMock(),
        ) as reindex_mock,
        patch(
            "dynastore.tasks.ingestion.temp_reaper.reap_orphan_task_dirs",
            new=AsyncMock(),
        ),
    ):
        yield reporters, reindex_mock


def _make_task_request(*, batch_size: int = 50) -> TaskIngestionRequest:
    return TaskIngestionRequest(
        asset=IngestionAsset(asset_id="asset-1"),
        column_mapping=ColumnMappingConfig(),
        database_batch_size=batch_size,
    )


def _feature(i: int) -> Dict[str, Any]:
    return {
        "id": f"f{i}",
        "geometry": {"type": "Point", "coordinates": [float(i), float(i)]},
        "properties": {"name": f"row-{i}"},
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fully_rejected_run_reports_failed():
    """0 persisted, N rejected -> FAILED with count + first message; no
    restore task enqueued (rejections are not a secondary-index miss)."""
    from dynastore.tasks.ingestion.main_ingestion import (
        _RejectionFailed,
        run_ingestion_task,
    )

    records = [_feature(1), _feature(2)]
    rejections = [[
        {"reason": "validation_error", "message": "bad row 1", "record": {"id": "f1"}},
        {"reason": "validation_error", "message": "bad row 2", "record": {"id": "f2"}},
    ]]
    upsert_fx = _upsert_batches(results=[[]], rejections=rejections)

    with _run_ingestion_harness(records, upsert_fx) as (reporters, reindex_mock):
        with pytest.raises(_RejectionFailed):
            await run_ingestion_task(
                None, "task-1", "cat1", "col1", _make_task_request(),
            )

    reporter = reporters[0]
    assert len(reporter.finished_calls) == 1
    status, kwargs = reporter.finished_calls[0]
    assert status == "FAILED"
    assert "2" in kwargs["error_message"]
    assert "bad row 1" in kwargs["error_message"]
    reindex_mock.assert_not_called()


@pytest.mark.asyncio
async def test_partial_rejection_reports_completed_with_counts():
    """k persisted + m>0 rejected -> COMPLETED with rejection_summary counts."""
    from dynastore.tasks.ingestion.main_ingestion import run_ingestion_task

    records = [_feature(1), _feature(2), _feature(3)]
    upsert_results = [[{"id": "f1"}, {"id": "f2"}]]
    rejections = [[
        {"reason": "validation_error", "message": "bad row 3", "record": {"id": "f3"}},
    ]]
    upsert_fx = _upsert_batches(results=upsert_results, rejections=rejections)

    with _run_ingestion_harness(records, upsert_fx) as (reporters, reindex_mock):
        await run_ingestion_task(
            None, "task-2", "cat1", "col1", _make_task_request(),
        )

    reporter = reporters[0]
    assert len(reporter.finished_calls) == 1
    status, kwargs = reporter.finished_calls[0]
    assert status == "COMPLETED"
    summary = kwargs.get("summary") or {}
    assert summary["rejection_summary"] == {
        "persisted": 2,
        "rejected": 1,
        "first_message": "bad row 3",
    }
    reindex_mock.assert_not_called()


@pytest.mark.asyncio
async def test_clean_run_unchanged():
    """No rejections -> COMPLETED, no rejection_summary in the outputs."""
    from dynastore.tasks.ingestion.main_ingestion import run_ingestion_task

    records = [_feature(1), _feature(2)]
    upsert_results = [[{"id": "f1"}, {"id": "f2"}]]
    upsert_fx = _upsert_batches(results=upsert_results, rejections=[[]])

    with _run_ingestion_harness(records, upsert_fx) as (reporters, reindex_mock):
        await run_ingestion_task(
            None, "task-3", "cat1", "col1", _make_task_request(),
        )

    reporter = reporters[0]
    status, kwargs = reporter.finished_calls[0]
    assert status == "COMPLETED"
    summary = kwargs.get("summary")
    assert not summary or "rejection_summary" not in summary
    reindex_mock.assert_not_called()


@pytest.mark.asyncio
async def test_empty_file_completed():
    """0 rows read -> the rejection gate is false (0 persisted, 0 rejected)
    -> unchanged COMPLETED behaviour."""
    from dynastore.tasks.ingestion.main_ingestion import run_ingestion_task

    upsert_fx = _upsert_batches(results=[], rejections=[])

    with _run_ingestion_harness([], upsert_fx) as (reporters, reindex_mock):
        await run_ingestion_task(
            None, "task-4", "cat1", "col1", _make_task_request(),
        )

    reporter = reporters[0]
    status, kwargs = reporter.finished_calls[0]
    assert status == "COMPLETED"
    reindex_mock.assert_not_called()


@pytest.mark.asyncio
async def test_secondary_indexing_flag_on_pending_backlog():
    """#2897: flag on + a non-empty tasks.storage backlog for this collection
    -> summary carries a "pending" secondary_indexing block with the count,
    and run_ingestion_task returns that same block to its caller."""
    from dynastore.tasks.ingestion.main_ingestion import run_ingestion_task

    records = [_feature(1), _feature(2)]
    upsert_results = [[{"id": "f1"}, {"id": "f2"}]]
    upsert_fx = _upsert_batches(results=upsert_results, rejections=[[]])

    with (
        _run_ingestion_harness(records, upsert_fx) as (reporters, _reindex_mock),
        patch(
            "dynastore.tasks.ingestion.main_ingestion._resolve_items_secondary_via_storage_plane",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "dynastore.tasks.ingestion.main_ingestion._count_pending_secondary_index_ops",
            new=AsyncMock(return_value=7),
        ),
    ):
        result = await run_ingestion_task(
            None, "task-6", "cat1", "col1", _make_task_request(),
        )

    reporter = reporters[0]
    status, kwargs = reporter.finished_calls[0]
    assert status == "COMPLETED"
    summary = kwargs.get("summary") or {}
    assert summary["secondary_indexing"] == {
        "state": "pending",
        "queued": 7,
        "message": "primary write complete; 7 entries pending asynchronous indexing",
    }
    assert result == summary["secondary_indexing"]


@pytest.mark.asyncio
async def test_secondary_indexing_flag_on_zero_backlog_converged():
    """#2897: flag on but the backlog for this collection is already 0
    -> summary carries a "converged" secondary_indexing block."""
    from dynastore.tasks.ingestion.main_ingestion import run_ingestion_task

    records = [_feature(1)]
    upsert_results = [[{"id": "f1"}]]
    upsert_fx = _upsert_batches(results=upsert_results, rejections=[[]])

    with (
        _run_ingestion_harness(records, upsert_fx) as (reporters, _reindex_mock),
        patch(
            "dynastore.tasks.ingestion.main_ingestion._resolve_items_secondary_via_storage_plane",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "dynastore.tasks.ingestion.main_ingestion._count_pending_secondary_index_ops",
            new=AsyncMock(return_value=0),
        ),
    ):
        result = await run_ingestion_task(
            None, "task-7", "cat1", "col1", _make_task_request(),
        )

    reporter = reporters[0]
    status, kwargs = reporter.finished_calls[0]
    assert status == "COMPLETED"
    summary = kwargs.get("summary") or {}
    assert summary["secondary_indexing"] == {"state": "converged", "queued": 0}
    assert result == {"state": "converged", "queued": 0}


@pytest.mark.asyncio
async def test_secondary_indexing_flag_off_omits_field():
    """#2897: flag off -> no secondary_indexing field at all, and the count
    helper is never even called (existing payloads must stay unchanged)."""
    from dynastore.tasks.ingestion.main_ingestion import run_ingestion_task

    records = [_feature(1)]
    upsert_results = [[{"id": "f1"}]]
    upsert_fx = _upsert_batches(results=upsert_results, rejections=[[]])

    with (
        _run_ingestion_harness(records, upsert_fx) as (reporters, _reindex_mock),
        patch(
            "dynastore.tasks.ingestion.main_ingestion._resolve_items_secondary_via_storage_plane",
            new=AsyncMock(return_value=False),
        ),
        patch(
            "dynastore.tasks.ingestion.main_ingestion._count_pending_secondary_index_ops",
            new=AsyncMock(side_effect=AssertionError("must not be called when the flag is off")),
        ),
    ):
        result = await run_ingestion_task(
            None, "task-8", "cat1", "col1", _make_task_request(),
        )

    reporter = reporters[0]
    status, kwargs = reporter.finished_calls[0]
    assert status == "COMPLETED"
    summary = kwargs.get("summary")
    assert not summary or "secondary_indexing" not in summary
    assert result is None


@pytest.mark.asyncio
async def test_secondary_indexing_count_failure_omits_field():
    """#2897: flag on but the COUNT itself raises -> best-effort, the field
    is omitted entirely and the (already-successful) task still COMPLETEs."""
    from dynastore.tasks.ingestion.main_ingestion import run_ingestion_task

    records = [_feature(1)]
    upsert_results = [[{"id": "f1"}]]
    upsert_fx = _upsert_batches(results=upsert_results, rejections=[[]])

    with (
        _run_ingestion_harness(records, upsert_fx) as (reporters, _reindex_mock),
        patch(
            "dynastore.tasks.ingestion.main_ingestion._resolve_items_secondary_via_storage_plane",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "dynastore.tasks.ingestion.main_ingestion._count_pending_secondary_index_ops",
            new=AsyncMock(side_effect=RuntimeError("pool exhausted")),
        ),
    ):
        result = await run_ingestion_task(
            None, "task-9", "cat1", "col1", _make_task_request(),
        )

    reporter = reporters[0]
    status, kwargs = reporter.finished_calls[0]
    assert status == "COMPLETED"
    summary = kwargs.get("summary")
    assert not summary or "secondary_indexing" not in summary
    assert result is None


@pytest.mark.asyncio
async def test_check_index_health_receives_persisted_count_not_processed_count():
    """A batch with rejections must pass rows_persisted (not rows_ingested,
    which also counts rejected rows) to _check_index_health."""
    from dynastore.tasks.ingestion.main_ingestion import run_ingestion_task

    records = [_feature(1), _feature(2)]
    upsert_results = [[{"id": "f1"}]]
    rejections = [[
        {"reason": "validation_error", "message": "bad row 2", "record": {"id": "f2"}},
    ]]
    upsert_fx = _upsert_batches(results=upsert_results, rejections=rejections)

    with (
        _run_ingestion_harness(records, upsert_fx) as (reporters, _reindex_mock),
        patch(
            "dynastore.tasks.ingestion.main_ingestion._check_index_health",
            wraps=__import__(
                "dynastore.tasks.ingestion.main_ingestion", fromlist=["_check_index_health"]
            )._check_index_health,
        ) as health_spy,
    ):
        await run_ingestion_task(
            None, "task-5", "cat1", "col1", _make_task_request(),
        )

    assert health_spy.call_args.kwargs["rows_written"] == 1, (
        "rows_ingested (2, includes the rejected row) must not be passed to "
        "_check_index_health — only rows_persisted (1) reflects what was "
        "actually written to the source store."
    )
