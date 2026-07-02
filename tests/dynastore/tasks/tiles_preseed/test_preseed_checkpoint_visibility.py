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

"""Tests for the per-zoom-transaction checkpoint-visibility fix (#2813).

The old preseed loop wrapped ALL zoom x bbox x tile iterations in ONE
transaction, holding a pooled connection for the whole run — so
`tasks_module.update_task`'s own connection checkout (a second pool
checkout) starved on a small pool and progress checkpoints never landed.
`TilePreseedTask._preseed_mvt` now opens one transaction PER ZOOM, freeing
the pool between zooms. These tests verify that structural property (one
`managed_transaction` entry per zoom, not one for the whole range) with a
fake transaction/engine — no real DB.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("morecantile")

from dynastore.tasks.tiles_preseed.task import TilePreseedTask
from dynastore.modules.tasks.models import TaskPayload, TaskUpdate
from dynastore.modules.processes.models import ExecuteRequest


def _make_task() -> TilePreseedTask:
    task = TilePreseedTask.__new__(TilePreseedTask)
    task.app_state = None
    task.engine = MagicMock()
    return task


def _fake_mc_tms(tiles_per_zoom):
    tms = MagicMock()

    def _tiles(*bbox, zooms):
        z = zooms[0]
        return [MagicMock(x=i, y=i) for i in range(tiles_per_zoom.get(z, 0))]

    tms.tiles = MagicMock(side_effect=_tiles)
    return tms


@pytest.mark.asyncio
async def test_one_transaction_opened_per_zoom_not_for_whole_range(monkeypatch):
    """The core checkpoint-visibility assertion: managed_transaction is
    entered once per zoom level, not once for the entire [min_zoom, max_zoom]
    range."""
    import dynastore.tasks.tiles_preseed.task as task_mod

    transaction_entries = []

    @asynccontextmanager
    async def fake_managed_transaction(engine):
        transaction_entries.append(1)
        yield MagicMock()

    monkeypatch.setattr(task_mod, "managed_transaction", fake_managed_transaction)
    monkeypatch.setattr(task_mod.DQLQuery, "execute", AsyncMock(return_value=None))
    monkeypatch.setattr(task_mod.tiles_engine, "render_tile", AsyncMock(return_value=None))
    monkeypatch.setattr(task_mod.tasks_module, "update_task", AsyncMock())

    task = _make_task()
    ctx = MagicMock()
    runtime_config = MagicMock(min_zoom=0, max_zoom=3, simplification_algorithm=None)
    preseed_config = MagicMock(simplification_by_zoom_override=None)
    storage = MagicMock()
    storage.save_tile = AsyncMock()

    payload = TaskPayload(task_id="00000000-0000-0000-0000-000000000001", inputs=ExecuteRequest(inputs={}), caller_id="test")

    results = {"generated": 0, "skipped": 0, "errors": 0}
    total = await task._preseed_mvt(
        engine=task.engine, payload=payload, ctx=ctx, storage=storage,
        catalog_id="cat1", col_id="coll1", tms_id="WebMercatorQuad",
        mc_tms=_fake_mc_tms({0: 1, 1: 1, 2: 1, 3: 1}),
        effective_bboxes=[(-10.0, -10.0, 10.0, 10.0)],
        effective_max_zoom=3,
        runtime_config=runtime_config, preseed_config=preseed_config,
        formats=["mvt"], statement_timeout_ms=30000,
        schema="test_schema", results=results, total_processed_so_far=0,
    )

    # zoom range [0,3] inclusive -> 4 zoom levels -> 4 transactions, not 1.
    assert len(transaction_entries) == 4
    assert total == 4


@pytest.mark.asyncio
async def test_checkpoint_carries_progress_percentage(monkeypatch):
    """Every 100-tile checkpoint (and the final one) writes a progress value,
    not just outputs — the visibility fix is meaningless if progress stays
    at its initial value throughout the run."""
    import dynastore.tasks.tiles_preseed.task as task_mod

    @asynccontextmanager
    async def fake_managed_transaction(engine):
        yield MagicMock()

    monkeypatch.setattr(task_mod, "managed_transaction", fake_managed_transaction)
    monkeypatch.setattr(task_mod.DQLQuery, "execute", AsyncMock(return_value=None))
    monkeypatch.setattr(task_mod.tiles_engine, "render_tile", AsyncMock(return_value=None))

    update_calls = []

    async def fake_update_task(engine, task_id, update: TaskUpdate, schema):
        update_calls.append(update)

    monkeypatch.setattr(task_mod.tasks_module, "update_task", fake_update_task)

    task = _make_task()
    ctx = MagicMock()
    runtime_config = MagicMock(min_zoom=0, max_zoom=1, simplification_algorithm=None)
    preseed_config = MagicMock(simplification_by_zoom_override=None)
    storage = MagicMock()
    storage.save_tile = AsyncMock()

    payload = TaskPayload(task_id="00000000-0000-0000-0000-000000000001", inputs=ExecuteRequest(inputs={}), caller_id="test")

    results = {"generated": 0, "skipped": 0, "errors": 0}
    await task._preseed_mvt(
        engine=task.engine, payload=payload, ctx=ctx, storage=storage,
        catalog_id="cat1", col_id="coll1", tms_id="WebMercatorQuad",
        mc_tms=_fake_mc_tms({0: 1, 1: 1}),
        effective_bboxes=[(-10.0, -10.0, 10.0, 10.0)],
        effective_max_zoom=1,
        runtime_config=runtime_config, preseed_config=preseed_config,
        formats=["mvt"], statement_timeout_ms=30000,
        schema="test_schema", results=results, total_processed_so_far=0,
    )

    assert update_calls, "expected at least the final checkpoint update"
    assert any(u.progress is not None for u in update_calls)


@pytest.mark.asyncio
async def test_zoom_level_error_does_not_abort_subsequent_zooms(monkeypatch):
    """A poisoned per-zoom transaction (e.g. a statement-timeout) is
    contained to that zoom — subsequent zooms still get their own fresh
    transaction and are processed."""
    import dynastore.tasks.tiles_preseed.task as task_mod

    entries = []

    @asynccontextmanager
    async def fake_managed_transaction(engine):
        entries.append(1)
        if len(entries) == 1:
            raise RuntimeError("simulated statement timeout")
        yield MagicMock()

    monkeypatch.setattr(task_mod, "managed_transaction", fake_managed_transaction)
    monkeypatch.setattr(task_mod.DQLQuery, "execute", AsyncMock(return_value=None))
    monkeypatch.setattr(task_mod.tiles_engine, "render_tile", AsyncMock(return_value=None))
    monkeypatch.setattr(task_mod.tasks_module, "update_task", AsyncMock())

    task = _make_task()
    ctx = MagicMock()
    runtime_config = MagicMock(min_zoom=0, max_zoom=1, simplification_algorithm=None)
    preseed_config = MagicMock(simplification_by_zoom_override=None)
    storage = MagicMock()
    storage.save_tile = AsyncMock()
    payload = TaskPayload(task_id="00000000-0000-0000-0000-000000000001", inputs=ExecuteRequest(inputs={}), caller_id="test")

    results = {"generated": 0, "skipped": 0, "errors": 0}
    await task._preseed_mvt(
        engine=task.engine, payload=payload, ctx=ctx, storage=storage,
        catalog_id="cat1", col_id="coll1", tms_id="WebMercatorQuad",
        mc_tms=_fake_mc_tms({0: 1, 1: 1}),
        effective_bboxes=[(-10.0, -10.0, 10.0, 10.0)],
        effective_max_zoom=1,
        runtime_config=runtime_config, preseed_config=preseed_config,
        formats=["mvt"], statement_timeout_ms=30000,
        schema="test_schema", results=results, total_processed_so_far=0,
    )

    # Both zoom levels attempted (transaction opened twice) even though the
    # first raised; the error is counted, not fatal to the whole run.
    assert len(entries) == 2
    assert results["errors"] == 1
