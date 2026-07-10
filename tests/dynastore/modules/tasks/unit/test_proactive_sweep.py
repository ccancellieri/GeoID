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

"""Unit tests for ``ProactiveSweepService.tick()`` (#524 PR B).

Covers:
- Distinct-pending query failure does not crash the tick.
- ``sweep_dead_capability_rows`` failure for one capability does not prevent
  subsequent capabilities from being swept.
- Nonzero sweep result emits the structured INFO line.
- Shutdown during inner iteration short-circuits cleanly.

The dispatcher's ``sweep_dead_capability_rows`` and the SQL DISTINCT query
are mocked — both have their own coverage upstream (reactive_reaper tests +
the new PG integration test, respectively).
"""
from __future__ import annotations

import asyncio
import logging
from unittest.mock import ANY, AsyncMock, patch

import pytest

from dynastore.modules.tasks.tasks_module import (
    ProactiveSweepService,
    _distinct_pending_capability_ids,
)
from dynastore.tools.background_service import ServiceContext


def _ctx(*, shutdown_set: bool = False) -> ServiceContext:
    ev = asyncio.Event()
    if shutdown_set:
        ev.set()
    return ServiceContext(
        engine=object(),
        shutdown=ev,
        is_ephemeral=False,
        name="test",
    )


@pytest.fixture(autouse=True)
def _capability_gated_task_type():
    """No production ``TaskProtocol`` currently declares
    ``required_capability`` (``index_propagation`` was the last one,
    retired). Register a synthetic mapping so ``ProactiveSweepService.
    tick()``'s per-task-type sweep loop — driven by
    ``TASK_TYPE_CAPABILITY_INPUTS_KEY`` — stays exercised end-to-end; a
    real capability-gated task lands its own entry here (#522).
    """
    from dynastore.modules.tasks.capability_oracle import (
        TASK_TYPE_CAPABILITY_INPUTS_KEY,
    )
    with patch.dict(
        TASK_TYPE_CAPABILITY_INPUTS_KEY, {"storage_drain": "indexer_id"}, clear=True,
    ):
        yield


@pytest.mark.asyncio
async def test_sweep_logs_dlq_count_on_nonzero_result(caplog):
    """When sweep_dead_capability_rows returns N>0 the tick emits a
    structured INFO line so a log-based metric can pick it up."""
    caplog.set_level(logging.INFO, logger="dynastore.modules.tasks.tasks_module")

    sweep = AsyncMock(return_value=4)
    distinct = AsyncMock(return_value=["dead_cap_1"])

    with patch(
        "dynastore.modules.tasks.tasks_module._distinct_pending_capability_ids",
        new=distinct,
    ), patch(
        "dynastore.modules.tasks.dispatcher.sweep_dead_capability_rows",
        new=sweep,
    ), patch(
        "dynastore.modules.tasks.tasks_module._run_mandatory_backstop_pass",
        new=AsyncMock(),
    ), patch(
        "dynastore.modules.tasks.tasks_module.sweep_wedged_provisioning_catalogs",
        new=AsyncMock(return_value=0),
    ):
        svc = ProactiveSweepService(schema="tasks", interval_s=0.01)
        await svc.tick(_ctx())

    sweep.assert_awaited_with(ANY, "dead_cap_1", task_type="storage_drain")
    info_lines = [
        r.message for r in caplog.records if r.levelno == logging.INFO
    ]
    assert any(
        "proactive_sweep: DLQ'd 4 row(s)" in m and "dead_cap_1" in m
        for m in info_lines
    ), f"missing proactive_sweep info line in: {info_lines}"


@pytest.mark.asyncio
async def test_sweep_continues_when_distinct_query_fails(caplog):
    """A failing DISTINCT query for one task_type must be logged and the
    tick must keep going (does not crash the entire sweeper)."""
    caplog.set_level(logging.WARNING, logger="dynastore.modules.tasks.tasks_module")

    distinct = AsyncMock(side_effect=RuntimeError("boom"))

    with patch(
        "dynastore.modules.tasks.tasks_module._distinct_pending_capability_ids",
        new=distinct,
    ), patch(
        "dynastore.modules.tasks.tasks_module._run_mandatory_backstop_pass",
        new=AsyncMock(),
    ), patch(
        "dynastore.modules.tasks.tasks_module.sweep_wedged_provisioning_catalogs",
        new=AsyncMock(return_value=0),
    ):
        svc = ProactiveSweepService(schema="tasks", interval_s=0.01)
        await svc.tick(_ctx())

    warn_lines = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    assert any(
        "proactive_sweep: distinct query failed" in m and "boom" in m
        for m in warn_lines
    ), f"missing distinct-failure warning in: {warn_lines}"


@pytest.mark.asyncio
async def test_sweep_continues_when_one_capability_sweep_fails(caplog):
    """A sweep_dead_capability_rows raising for one (cap, task_type) pair
    must not abort the pass — subsequent cap_ids still get a chance."""
    caplog.set_level(logging.WARNING, logger="dynastore.modules.tasks.tasks_module")

    distinct = AsyncMock(return_value=["bad_cap", "good_cap"])

    sweep_results = {"bad_cap": RuntimeError("ouch"), "good_cap": 2}

    async def sweep_side_effect(_engine, cap_id, *, task_type):
        result = sweep_results[cap_id]
        if isinstance(result, Exception):
            raise result
        return result

    sweep = AsyncMock(side_effect=sweep_side_effect)

    with patch(
        "dynastore.modules.tasks.tasks_module._distinct_pending_capability_ids",
        new=distinct,
    ), patch(
        "dynastore.modules.tasks.dispatcher.sweep_dead_capability_rows",
        new=sweep,
    ), patch(
        "dynastore.modules.tasks.tasks_module._run_mandatory_backstop_pass",
        new=AsyncMock(),
    ), patch(
        "dynastore.modules.tasks.tasks_module.sweep_wedged_provisioning_catalogs",
        new=AsyncMock(return_value=0),
    ):
        svc = ProactiveSweepService(schema="tasks", interval_s=0.01)
        await svc.tick(_ctx())

    # Both cap_ids must have been tried — bad_cap fails, good_cap succeeds.
    swept_caps = [call.args[1] for call in sweep.await_args_list]
    assert "bad_cap" in swept_caps and "good_cap" in swept_caps, (
        f"both caps should have been attempted; got {swept_caps}"
    )
    warn_lines = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    assert any(
        "proactive_sweep: sweep failed" in m and "bad_cap" in m
        for m in warn_lines
    ), f"missing sweep-failure warning in: {warn_lines}"


@pytest.mark.asyncio
async def test_sweep_tick_short_circuits_on_shutdown_during_inner_iteration():
    """tick() checks ctx.shutdown during the inner cap_id loop and returns early."""
    ctx = _ctx()

    sweep_calls: list = []

    async def _sweep_and_set(_engine, cap_id, *, task_type):
        sweep_calls.append(cap_id)
        # Signal shutdown after the first cap — the second should not run.
        ctx.shutdown.set()
        return 0

    distinct = AsyncMock(return_value=["cap_one", "cap_two"])

    with patch(
        "dynastore.modules.tasks.tasks_module._distinct_pending_capability_ids",
        new=distinct,
    ), patch(
        "dynastore.modules.tasks.dispatcher.sweep_dead_capability_rows",
        new=AsyncMock(side_effect=_sweep_and_set),
    ), patch(
        "dynastore.modules.tasks.tasks_module._run_mandatory_backstop_pass",
        new=AsyncMock(),
    ), patch(
        "dynastore.modules.tasks.tasks_module.sweep_wedged_provisioning_catalogs",
        new=AsyncMock(return_value=0),
    ):
        svc = ProactiveSweepService(schema="tasks", interval_s=0.01)
        await svc.tick(ctx)

    # Only the first cap should have been swept before shutdown.
    assert sweep_calls == ["cap_one"]


@pytest.mark.asyncio
async def test_distinct_query_filters_pending_retry_zero_with_min_age():
    """The SQL DISTINCT query must filter on status=PENDING + retry_count=0
    + non-null inputs key + age threshold. Verified by inspecting the SQL
    text passed to DQLQuery."""
    captured_sqls = []

    class _FakeQuery:
        def __init__(self, sql, *args, **kwargs):
            captured_sqls.append(sql)
            self._sql = sql

        async def execute(self, conn, **kwargs):
            return [{"cap_id": "x"}, {"cap_id": "y"}, {"cap_id": None}]

    class _FakeTxn:
        async def __aenter__(self):
            return object()
        async def __aexit__(self, *_):
            return False

    with patch(
        "dynastore.modules.tasks.tasks_module.DQLQuery", _FakeQuery,
    ), patch(
        "dynastore.modules.tasks.tasks_module.background_managed_transaction",
        return_value=_FakeTxn(),
    ):
        out = await _distinct_pending_capability_ids(
            engine=object(),
            schema="tasks",
            task_type="storage_drain",
            inputs_key="indexer_id",
            min_age_s=300.0,
            sample_limit=50,
        )

    assert out == ["x", "y"], f"None must be filtered; got {out}"
    assert len(captured_sqls) == 1
    sql = captured_sqls[0]
    assert "status = 'PENDING'" in sql
    assert "retry_count = 0" in sql
    assert "inputs->>'indexer_id'" in sql
    assert "make_interval(secs => :min_age_s)" in sql
