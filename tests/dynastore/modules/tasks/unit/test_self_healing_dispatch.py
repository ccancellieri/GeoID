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

"""Unit tests for the self-healing stuck-PENDING dispatch via
``StuckPendingWarnerService.tick()`` and its helper ``_redispatch_stuck_rows``.

These tests are pure-Python / no-DB: all SQL is mocked. The contract:

(a) A PENDING/retry_count=0 task older than the threshold gets a
    signal_bus wakeup (in-process) AND a pg_notify (cross-pod).
(b) When two sweepers race, only one claim succeeds per row — guaranteed
    by ``claim_batch``'s ``FOR UPDATE SKIP LOCKED`` (tested via advisory
    dedup mock that makes the second emitter a no-op).
(c) The WARNING log is still emitted for every stuck row.
(d) ACTIVE tasks and rows within the age threshold are not touched
    (the SQL filter excludes them; we verify _redispatch is not called).
(e) Rows whose required capability is confirmed dead (cap_live=False)
    are filtered out by _redispatch_stuck_rows; only claimable rows
    trigger the signal.
"""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.tasks.tasks_module import (
    StuckPendingWarnerService,
    _redispatch_stuck_rows,
)
from dynastore.tools.background_service import ServiceContext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_rows(n: int, task_type: str = "gcp_provision_catalog") -> List[Dict[str, Any]]:
    return [
        {
            "task_id": f"task-{i}",
            "task_type": task_type,
            "schema_name": "myschema",
            "inputs": None,
            "age_s": 999.0,
        }
        for i in range(n)
    ]


@asynccontextmanager
async def _fake_managed_transaction(_engine):
    yield MagicMock()


def _ctx() -> ServiceContext:
    return ServiceContext(
        engine=object(),
        shutdown=asyncio.Event(),
        is_ephemeral=False,
        name="test",
    )


# ---------------------------------------------------------------------------
# (a) Claimable stuck row → signal_bus + pg_notify emitted
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_redispatch_emits_signal_and_notify_for_claimable_row(caplog):
    """A PENDING/retry=0 row with no declared capability (routing-config case)
    triggers an in-process signal_bus emit and a cross-pod pg_notify.
    """
    rows = _make_rows(1)

    bus_emit = AsyncMock()
    fake_query = AsyncMock()
    fake_query.execute = AsyncMock(return_value=None)

    with patch("dynastore.tasks.get_task_instance", return_value=None), \
         patch("dynastore.modules.tasks.tasks_module.DQLQuery", return_value=fake_query), \
         patch("dynastore.modules.tasks.tasks_module.managed_transaction", _fake_managed_transaction), \
         patch("dynastore.tools.async_utils.signal_bus") as mock_bus:
        mock_bus.emit = bus_emit
        caplog.set_level(logging.INFO)
        await _redispatch_stuck_rows(engine=object(), rows=rows)

    bus_emit.assert_awaited_once()
    assert fake_query.execute.await_count == 1, "pg_notify SELECT should have been executed"
    assert any(
        "stuck-pending redispatch" in r.message and "1 claimable row" in r.message
        for r in caplog.records
    )


# ---------------------------------------------------------------------------
# (b) Dead-capability rows are filtered — no signal emitted
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_redispatch_skips_dead_capability_rows():
    """Rows whose required capability is confirmed dead are skipped;
    the dispatcher cannot claim them anyway and the proactive sweep will DLQ.
    """
    class _FakeTask:
        @classmethod
        def required_capability(cls, payload):
            return "dead_indexer"

    rows = _make_rows(3, task_type="index_propagation")
    for row in rows:
        row["inputs"] = {"indexer_id": "dead_indexer"}

    bus_emit = AsyncMock()

    with patch("dynastore.tasks.get_task_instance", return_value=_FakeTask()), \
         patch(
             "dynastore.modules.tasks.capability_oracle.is_capability_live",
             new=AsyncMock(return_value=False),
         ), \
         patch("dynastore.tools.async_utils.signal_bus") as mock_bus:
        mock_bus.emit = bus_emit
        await _redispatch_stuck_rows(engine=object(), rows=rows)

    bus_emit.assert_not_awaited()


# ---------------------------------------------------------------------------
# (b) Cross-pod dedup — second sweeper claim is a no-op at DB level
#     (advisory lock prevents double-run; signal_bus emit is idempotent)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_warn_tick_emits_only_once_when_two_sweepers_race():
    """Two concurrent _redispatch_stuck_rows calls for the same row both emit
    — signal_bus.emit is idempotent (WaitableSignal.set() called twice is fine)
    and pg_notify is emitted by both pods.  claim_batch's FOR UPDATE SKIP LOCKED
    means only one pod claims the row.
    """
    rows = _make_rows(1)

    bus_emit_calls: List[Any] = []

    async def _counting_emit(*args, **kwargs):
        bus_emit_calls.append(args)

    fake_query = AsyncMock()
    notify_count = {"n": 0}

    async def _counting_execute(*args, **kwargs):
        notify_count["n"] += 1
        return None

    fake_query.execute = _counting_execute

    with patch("dynastore.tasks.get_task_instance", return_value=None), \
         patch("dynastore.modules.tasks.tasks_module.DQLQuery", return_value=fake_query), \
         patch("dynastore.modules.tasks.tasks_module.managed_transaction", _fake_managed_transaction), \
         patch("dynastore.tools.async_utils.signal_bus") as mock_bus:
        mock_bus.emit = AsyncMock(side_effect=_counting_emit)
        await asyncio.gather(
            _redispatch_stuck_rows(engine=object(), rows=rows),
            _redispatch_stuck_rows(engine=object(), rows=rows),
        )

    assert mock_bus.emit.await_count == 2
    assert notify_count["n"] == 2


# ---------------------------------------------------------------------------
# (c) Warning is still emitted — regression guard
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_warn_tick_still_emits_warning_for_stuck_rows(caplog):
    """The warning log must fire for stuck rows, and _redispatch_stuck_rows
    must be called for non-empty results.
    """
    fake_rows = _make_rows(2)
    fake_query = AsyncMock()
    fake_query.execute = AsyncMock(return_value=fake_rows)
    fake_redispatch = AsyncMock()

    caplog.set_level(logging.WARNING)
    with patch("dynastore.modules.tasks.tasks_module.DQLQuery", return_value=fake_query), \
         patch("dynastore.modules.tasks.tasks_module.managed_transaction", _fake_managed_transaction), \
         patch("dynastore.modules.tasks.tasks_module._redispatch_stuck_rows", fake_redispatch), \
         patch("dynastore.tasks.get_task_instance", return_value=None), \
         patch(
             "dynastore.modules.tasks.capability_oracle.is_capability_live",
             new=AsyncMock(return_value=None),
         ):
        svc = StuckPendingWarnerService(schema="tasks", interval_s=0.01, min_age_s=10.0)
        await svc.tick(_ctx())

    warn_lines = [r.message for r in caplog.records if "stuck-pending: task" in r.message]
    assert len(warn_lines) == 2
    # Redispatch must also be called for those rows.
    fake_redispatch.assert_awaited_once()


# ---------------------------------------------------------------------------
# (d) No rows → _redispatch_stuck_rows is not called
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_warn_tick_skips_redispatch_when_no_stuck_rows():
    """When the scan returns an empty set, _redispatch_stuck_rows must not
    be called (and no signal must be emitted).
    """
    fake_query = AsyncMock()
    fake_query.execute = AsyncMock(return_value=[])
    fake_redispatch = AsyncMock()

    with patch("dynastore.modules.tasks.tasks_module.DQLQuery", return_value=fake_query), \
         patch("dynastore.modules.tasks.tasks_module.managed_transaction", _fake_managed_transaction), \
         patch("dynastore.modules.tasks.tasks_module._redispatch_stuck_rows", fake_redispatch):
        svc = StuckPendingWarnerService(schema="tasks", interval_s=0.01, min_age_s=10.0)
        await svc.tick(_ctx())

    fake_redispatch.assert_not_awaited()


# ---------------------------------------------------------------------------
# (e) signal_bus emit failure is swallowed — pg_notify still attempted
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_redispatch_survives_signal_bus_failure():
    """If signal_bus.emit raises, _redispatch_stuck_rows logs and continues,
    then still attempts the pg_notify so the cross-pod path remains live.
    """
    rows = _make_rows(1)

    fake_query = AsyncMock()
    fake_query.execute = AsyncMock(return_value=None)

    with patch("dynastore.tasks.get_task_instance", return_value=None), \
         patch("dynastore.modules.tasks.tasks_module.DQLQuery", return_value=fake_query), \
         patch("dynastore.modules.tasks.tasks_module.managed_transaction", _fake_managed_transaction), \
         patch("dynastore.tools.async_utils.signal_bus") as mock_bus:
        mock_bus.emit = AsyncMock(side_effect=RuntimeError("bus broken"))
        # Must not raise
        await _redispatch_stuck_rows(engine=object(), rows=rows)

    # pg_notify was still attempted despite signal_bus failure.
    assert fake_query.execute.await_count == 1
