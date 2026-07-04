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

"""Regression coverage for GeoID #2820: checkpointed/resumable bulk ingestion.

A run killed partway through (Cloud Run task-timeout) restarted at offset 0
on every retry — burning the full compute budget again and re-writing rows
that were already durably persisted. These tests pin:

1. A committed-row cursor is persisted (``_persist_ingestion_cursor``) after
   every successful batch commit, at the correct offset.
2. A resumed run (``task_request.offset`` seeded from that persisted cursor
   by the caller — see ``IngestionTask.run`` / ``tasks_module.
   update_task_ingestion_offset``) reports progress relative to the resume
   point, not from 0%.
3. Replaying the same batch twice (simulating a crash between a successful
   upsert and the cursor write) converges via the existing #2709
   deterministic-identity precedence rather than duplicating rows.

``run_ingestion_task`` is driven end-to-end with every external dependency
(catalog protocol, reader, reporters, write-driver ensure_storage, extent
recalculation) stubbed, mirroring the harness in
``test_main_ingestion_status.py``.
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
# Fakes (mirrors test_main_ingestion_status.py's harness)
# ---------------------------------------------------------------------------


class _FakeReporter:
    def __init__(self) -> None:
        self.progress_calls: List[Any] = []
        self.finished_calls: List[Any] = []

    async def task_started(self, *args, **kwargs) -> None:
        pass

    async def update_progress(self, *args, **kwargs) -> None:
        self.progress_calls.append(args)

    async def process_batch_outcome(self, outcomes) -> None:
        pass

    async def task_finished(self, status, **kwargs) -> None:
        self.finished_calls.append((status, kwargs))


def _make_reader_class(records: List[Dict[str, Any]]):
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


def _echo_upsert():
    """Accept every batch and echo it back (all rows persisted, none rejected)."""
    batches_seen: List[list] = []

    async def _fake_upsert(catalog_id, collection_id, batch, *, ctx: DriverContext, processing_context=None):
        batches_seen.append([dict(f) for f in batch])
        ctx.extensions["_rejections"] = []
        ctx.extensions["_index_results"] = {}
        ctx.extensions["_generated_stats"] = None
        return list(batch)

    return _fake_upsert, batches_seen


@contextlib.contextmanager
def _run_ingestion_harness(records, upsert_side_effect):
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
        ),
        patch(
            "dynastore.tasks.ingestion.temp_reaper.reap_orphan_task_dirs",
            new=AsyncMock(),
        ),
    ):
        yield reporters


def _make_task_request(*, offset: int = 0, batch_size: int = 50) -> TaskIngestionRequest:
    return TaskIngestionRequest(
        asset=IngestionAsset(asset_id="asset-1"),
        column_mapping=ColumnMappingConfig(),
        database_batch_size=batch_size,
        offset=offset,
    )


def _feature(i: int) -> Dict[str, Any]:
    return {
        "id": f"f{i}",
        "geometry": {"type": "Point", "coordinates": [float(i), float(i)]},
        "properties": {"name": f"row-{i}"},
    }


# ---------------------------------------------------------------------------
# 1. Cursor persisted after every successful batch commit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cursor_persisted_after_each_batch_commit():
    """5 rows, batch size 2 -> 3 flushes (2, 2, 1); the resume cursor must
    advance to 2, then 4, then 5 -- the exact row count durably committed so
    far, not just the current batch's size."""
    from dynastore.tasks.ingestion.main_ingestion import run_ingestion_task

    records = [_feature(i) for i in range(5)]
    upsert_fx, _batches = _echo_upsert()

    with (
        _run_ingestion_harness(records, upsert_fx),
        patch(
            "dynastore.tasks.ingestion.main_ingestion._persist_ingestion_cursor",
            new=AsyncMock(),
        ) as cursor_mock,
    ):
        await run_ingestion_task(
            None, "task-cursor-1", "cat1", "col1",
            _make_task_request(batch_size=2),
        )

    persisted_offsets = [call.args[2] for call in cursor_mock.await_args_list]
    assert persisted_offsets == [2, 4, 5], (
        "expected the resume cursor to be stamped once per batch flush at "
        "the cumulative row count committed so far"
    )
    for call in cursor_mock.await_args_list:
        assert call.args[1] == "task-cursor-1"


@pytest.mark.asyncio
async def test_cursor_write_failure_does_not_fail_ingestion():
    """Cursor persistence is best-effort: a raising cursor-write must not
    abort an otherwise-successful ingestion (degrades to pre-#2820 restart-
    from-original-offset behaviour on the NEXT retry, not a failed run now)."""
    from dynastore.tasks.ingestion.main_ingestion import run_ingestion_task

    records = [_feature(0), _feature(1)]
    upsert_fx, _batches = _echo_upsert()

    with (
        _run_ingestion_harness(records, upsert_fx) as reporters,
        patch(
            "dynastore.modules.tasks.tasks_module.update_task_ingestion_offset",
            new=AsyncMock(side_effect=RuntimeError("db unavailable")),
        ),
    ):
        await run_ingestion_task(
            None, "task-cursor-2", "cat1", "col1", _make_task_request(),
        )

    status, _kwargs = reporters[0].finished_calls[0]
    assert status == "COMPLETED"


# ---------------------------------------------------------------------------
# 2. Resumed run reports progress from the seeded offset, not from 0
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resumed_run_reports_progress_from_offset_not_zero():
    """task_request.offset=3 on a 5-row source (the caller seeds this from
    the persisted cursor of a prior killed attempt -- see
    tasks_module.update_task_ingestion_offset) must report progress
    starting at 3/5, and finish at 5/5 -- never regressing to 0."""
    from dynastore.tasks.ingestion.main_ingestion import run_ingestion_task

    records = [_feature(i) for i in range(5)]
    upsert_fx, batches_seen = _echo_upsert()

    with (
        _run_ingestion_harness(records, upsert_fx) as reporters,
        patch(
            "dynastore.tasks.ingestion.main_ingestion._persist_ingestion_cursor",
            new=AsyncMock(),
        ) as cursor_mock,
    ):
        await run_ingestion_task(
            None, "task-resume-1", "cat1", "col1",
            _make_task_request(offset=3),
        )

    # Only the 2 rows AFTER the resume point are actually processed/upserted.
    assert sum(len(b) for b in batches_seen) == 2
    assert [f["id"] for f in batches_seen[0]] == ["f3", "f4"]

    progress = reporters[0].progress_calls
    assert progress[0] == (3, 5), (
        "the initial progress report must seed from task_request.offset, "
        "not hardcode 0 -- otherwise a resumed run visibly regresses to 0% "
        "before climbing back up"
    )
    assert progress[-1] == (5, 5)

    # And the persisted cursor after finishing must be the full row count.
    assert cursor_mock.await_args_list[-1].args[2] == 5


# ---------------------------------------------------------------------------
# 3. Replaying the same batch (crash between commit and cursor write)
#    converges instead of duplicating -- relies on the existing #2709
#    deterministic-identity precedence.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_replayed_batch_converges_on_same_identity():
    """Simulates the crash window this feature targets: the upsert for a
    batch succeeds but the process is killed before ``_persist_ingestion_
    cursor`` lands, so a retry replays the SAME source rows from the stale
    (pre-crash) offset. The #2709 identity precedence must resolve the SAME
    feature id both times, so the idempotent upsert converges rather than
    inserting a duplicate row."""
    from dynastore.tasks.ingestion.main_ingestion import run_ingestion_task

    records = [_feature(0), _feature(1), _feature(2)]

    upsert_fx_1, batches_1 = _echo_upsert()
    with (
        _run_ingestion_harness(records, upsert_fx_1),
        patch(
            "dynastore.tasks.ingestion.main_ingestion._persist_ingestion_cursor",
            new=AsyncMock(),
        ),
    ):
        await run_ingestion_task(
            None, "task-replay-1", "cat1", "col1", _make_task_request(),
        )

    # Retry: cursor write never landed, so the replay starts at offset 0
    # again over the identical source rows.
    upsert_fx_2, batches_2 = _echo_upsert()
    with (
        _run_ingestion_harness(records, upsert_fx_2),
        patch(
            "dynastore.tasks.ingestion.main_ingestion._persist_ingestion_cursor",
            new=AsyncMock(),
        ),
    ):
        await run_ingestion_task(
            None, "task-replay-1", "cat1", "col1", _make_task_request(),
        )

    ids_first_attempt = [f["id"] for f in batches_1[0]]
    ids_replay = [f["id"] for f in batches_2[0]]
    assert ids_first_attempt == ids_replay == ["f0", "f1", "f2"], (
        "a replayed batch must resolve the exact same feature ids as the "
        "first attempt so the upsert converges (idempotent) instead of "
        "duplicating rows"
    )
