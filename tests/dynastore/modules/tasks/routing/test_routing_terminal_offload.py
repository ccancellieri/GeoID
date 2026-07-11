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

"""Terminal-Action wiring for the two offloaded execution paths.

Covers:

1. BackgroundRunner timeout — ``asyncio.wait_for`` ceiling wraps
   ``task_instance.run``; ``TimeoutError`` dead-letters the row and fires
   ``on_timeout`` (NOT ``on_failure``).

2. BackgroundRunner fast-path — no timeout fires; the success path calls
   ``complete_task`` and ``apply_terminal_action(outcome='success')``.

3. GcpLivenessReconciler exactly-once terminal Actions — for each terminal
   verdict (TERMINAL_SUCCEEDED / TERMINAL_FAILED / DEAD), when the
   owner-guarded write returns True the correct Action is fired; when it
   returns False (race lost) the Action is NOT fired.

All DB-free: DB helpers, routing resolver, and ``apply_terminal_action`` are
mocked. Run with ``--noconftest`` to skip the live-database conftest.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers shared by both test groups
# ---------------------------------------------------------------------------


def _sentinel_action(name: str):
    """Return a dummy Action object identifiable by its 'process' attribute."""
    from dynastore.modules.tasks.routing.model import Action, ActionVerb

    return Action(action=ActionVerb.ROUTE, process=name)


def _make_routing_terminal(*, timeout_seconds=None, on_success=None,
                           on_failure=None, on_timeout=None):
    """Return a RoutingTerminal-like SimpleNamespace."""
    from dynastore.modules.tasks.routing.model import Action, ActionVerb

    return SimpleNamespace(
        timeout_seconds=timeout_seconds,
        on_success=on_success or Action(action=ActionVerb.REPORT),
        on_failure=on_failure or Action(action=ActionVerb.DEAD_LETTER),
        on_timeout=on_timeout or Action(action=ActionVerb.DEAD_LETTER),
    )


# ---------------------------------------------------------------------------
# Group 1 — BackgroundRunner timeout and fast-path
# ---------------------------------------------------------------------------


def _make_runner_context():
    """Minimal RunnerContext-compatible namespace for BackgroundRunner tests."""
    return SimpleNamespace(
        engine=MagicMock(),
        task_type="test_task",
        inputs={"key": "val"},
        caller_id="user@example.com",
        collection_id="col",
        db_schema="tasks",
        extra_context={},
        # asset is expected by raw_payload construction
        asset=None,
        execution_overrides=None,
    )


def _patch_background_runner_deps(
    monkeypatch, *, terminal, task_run_coro,
    complete_returns: bool = True, fail_returns: bool = True,
    dead_letter_returns: bool = True,
):
    """Patch all external helpers called inside ``_execute_background_claimed``.

    Returns a namespace with the mocked callables.
    """
    from dynastore.modules.tasks import tasks_module, execution as exec_mod

    # Routing terminal (resolve_routing_terminal)
    monkeypatch.setattr(exec_mod, "_default_task_timeout", AsyncMock(return_value=None))

    resolve_mock = AsyncMock(return_value=terminal)
    monkeypatch.setattr(
        "dynastore.modules.tasks.execution.resolve_routing_terminal",
        resolve_mock,
    )

    # Terminal DB helpers
    complete = AsyncMock(return_value=complete_returns)
    fail = AsyncMock(return_value=fail_returns)
    dead_letter = AsyncMock(return_value=dead_letter_returns)
    monkeypatch.setattr(tasks_module, "complete_task", complete)
    monkeypatch.setattr(tasks_module, "fail_task", fail)
    monkeypatch.setattr(tasks_module, "dead_letter_task", dead_letter)

    # apply_terminal_action — captured for assertion
    apply_ta = AsyncMock()
    monkeypatch.setattr(
        "dynastore.modules.tasks.execution.apply_terminal_action",
        apply_ta,
    )

    # hydrate_task_payload — return a simple object with .inputs
    monkeypatch.setattr(
        "dynastore.tasks.hydrate_task_payload",
        lambda inst, raw: SimpleNamespace(inputs=raw.get("inputs", {})),
    )

    # task_instance with configurable run() coroutine
    task_instance = MagicMock()
    task_instance.run = MagicMock(side_effect=task_run_coro)

    return SimpleNamespace(
        complete=complete,
        fail=fail,
        dead_letter=dead_letter,
        apply_ta=apply_ta,
        resolve=resolve_mock,
        task_instance=task_instance,
    )


async def _run_background_claimed_direct(runner, context, task_instance, task_id_str):
    """Drive the inner ``_execute_background_claimed`` coroutine directly.

    ``_run_claimed`` submits it to the BackgroundExecutor; we extract and
    ``await`` it ourselves so the test stays simple and DB-free.
    """
    submitted = []

    class _FakeExecutor:
        def submit(self, coro, *, task_name):
            submitted.append(coro)
            t = MagicMock()
            t.add_done_callback = MagicMock()
            return t

    with patch(
        "dynastore.modules.tasks.runners.get_background_executor",
        return_value=_FakeExecutor(),
    ):
        await runner._run_claimed(
            context=context,
            tasks_mgr=MagicMock(),
            task_instance=task_instance,
            claimed_task_id_str=task_id_str,
            claimed_timestamp=datetime.now(timezone.utc),
        )

    assert submitted, "BackgroundExecutor.submit was not called"
    await submitted[0]  # run the background coroutine


@pytest.mark.asyncio
async def test_background_timeout_dead_letters_and_fires_on_timeout(monkeypatch):
    """run() that sleeps past timeout_seconds → dead_letter_task + on_timeout."""
    on_timeout_action = _sentinel_action("compensation")
    terminal = _make_routing_terminal(
        timeout_seconds=0.01,
        on_timeout=on_timeout_action,
    )

    async def _slow_run(_payload):
        await asyncio.sleep(10)  # exceeds 0.01s ceiling
        return {}

    context = _make_runner_context()
    mocks = _patch_background_runner_deps(
        monkeypatch, terminal=terminal, task_run_coro=_slow_run,
    )

    from dynastore.modules.tasks.runners import BackgroundRunner

    runner = BackgroundRunner()
    task_id = str(uuid.uuid4())
    await _run_background_claimed_direct(runner, context, mocks.task_instance, task_id)

    # Dead-letter must be called exactly once.
    mocks.dead_letter.assert_awaited_once()

    # apply_terminal_action must be called with outcome="timeout" and the sentinel.
    mocks.apply_ta.assert_awaited_once()
    _, kwargs = mocks.apply_ta.await_args
    assert kwargs["outcome"] == "timeout"
    assert kwargs["action"] is on_timeout_action

    # complete_task and fail_task must NOT be called on the timeout path.
    mocks.complete.assert_not_awaited()
    mocks.fail.assert_not_awaited()


@pytest.mark.asyncio
async def test_background_fast_task_no_timeout_calls_on_success(monkeypatch):
    """run() that completes quickly → complete_task + apply_terminal_action(success)."""
    on_success_action = _sentinel_action("next_step")
    terminal = _make_routing_terminal(
        timeout_seconds=5.0,  # generous ceiling
        on_success=on_success_action,
    )

    async def _fast_run(_payload):
        return {"result": "ok"}

    context = _make_runner_context()
    mocks = _patch_background_runner_deps(
        monkeypatch, terminal=terminal, task_run_coro=_fast_run,
    )

    from dynastore.modules.tasks.runners import BackgroundRunner

    runner = BackgroundRunner()
    task_id = str(uuid.uuid4())
    await _run_background_claimed_direct(runner, context, mocks.task_instance, task_id)

    mocks.complete.assert_awaited_once()
    mocks.apply_ta.assert_awaited_once()
    _, kwargs = mocks.apply_ta.await_args
    assert kwargs["outcome"] == "success"
    assert kwargs["action"] is on_success_action

    # Timeout path must not fire.
    mocks.dead_letter.assert_not_awaited()


@pytest.mark.asyncio
async def test_background_success_race_lost_no_action(monkeypatch):
    """#3264: ``complete_task`` losing the prior_owner_id race (row already
    reclaimed and completed by a fresh attempt) must NOT fire
    ``apply_terminal_action`` — the winning attempt already fired its own
    follow-on off the fresh row."""
    on_success_action = _sentinel_action("next_step")
    terminal = _make_routing_terminal(
        timeout_seconds=5.0,
        on_success=on_success_action,
    )

    async def _fast_run(_payload):
        return {"result": "ok"}

    context = _make_runner_context()
    mocks = _patch_background_runner_deps(
        monkeypatch, terminal=terminal, task_run_coro=_fast_run,
        complete_returns=False,
    )

    from dynastore.modules.tasks.runners import BackgroundRunner

    runner = BackgroundRunner()
    task_id = str(uuid.uuid4())
    await _run_background_claimed_direct(runner, context, mocks.task_instance, task_id)

    mocks.complete.assert_awaited_once()
    mocks.apply_ta.assert_not_awaited()


# ---------------------------------------------------------------------------
# Group 2 — GcpLivenessReconciler exactly-once terminal Actions
# ---------------------------------------------------------------------------


def _make_reconciler():
    from dynastore.modules.gcp.liveness_reconciler import GcpLivenessReconciler

    return GcpLivenessReconciler(
        engine=MagicMock(),
        interval_seconds=0.01,
        extend_visibility_seconds=300,
        unknown_grace_seconds=180,
    )


def _row_for_reconciler(task_type="ingest_job"):
    return {
        "task_id": uuid.uuid4(),
        "schema_name": "tasks",
        "task_type": task_type,
        "owner_id": "gcp_cloud_run_abc",
        "runner_ref": "projects/p/locations/us/jobs/j/executions/e1",
        "started_at": datetime.now(timezone.utc) - timedelta(seconds=30),
        "outputs": {"rows": 42},
        "retry_count": 0,
        "max_retries": 3,
        "locked_until": datetime.now(timezone.utc) - timedelta(seconds=5),
        "inputs": {"source": "gs://bucket/file.csv"},
        "caller_id": "sysadmin",
        "collection_id": "col_a",
        "scope": "CATALOG",
    }


def _patch_reconciler_deps(monkeypatch, *, verdict,
                           write_returns: bool = True):
    """Patch probe, terminal DB helpers, routing, and apply_terminal_action."""
    from dynastore.modules.tasks import tasks_module
    from dynastore.modules.gcp import liveness_reconciler as rec_mod

    class _Probe:
        def owns(self, owner_id):
            return True

        async def probe_liveness(self, task):
            return verdict

    monkeypatch.setattr(rec_mod, "resolve_probe", lambda owner_id: _Probe())

    fail = AsyncMock(return_value=write_returns)
    complete = AsyncMock(return_value=write_returns)
    monkeypatch.setattr(tasks_module, "fail_task", fail)
    monkeypatch.setattr(tasks_module, "complete_task", complete)

    # on_success / on_failure / on_timeout sentinels
    on_success = _sentinel_action("follow_on_success")
    on_failure = _sentinel_action("follow_on_failure")
    on_timeout = _sentinel_action("follow_on_timeout")

    terminal = _make_routing_terminal(
        on_success=on_success,
        on_failure=on_failure,
        on_timeout=on_timeout,
    )

    resolve_mock = AsyncMock(return_value=terminal)
    apply_ta = AsyncMock()

    monkeypatch.setattr(
        "dynastore.modules.tasks.execution.resolve_routing_terminal",
        resolve_mock,
    )
    monkeypatch.setattr(
        "dynastore.modules.tasks.execution.apply_terminal_action",
        apply_ta,
    )

    return SimpleNamespace(
        fail=fail,
        complete=complete,
        resolve=resolve_mock,
        apply_ta=apply_ta,
        on_success=on_success,
        on_failure=on_failure,
        on_timeout=on_timeout,
    )


@pytest.mark.asyncio
async def test_reconciler_terminal_succeeded_fires_on_success(monkeypatch):
    """TERMINAL_SUCCEEDED + write=True → apply_terminal_action(outcome='success')."""
    from dynastore.modules.tasks.liveness import LivenessVerdict

    mocks = _patch_reconciler_deps(
        monkeypatch, verdict=LivenessVerdict.TERMINAL_SUCCEEDED, write_returns=True,
    )
    rec = _make_reconciler()
    row = _row_for_reconciler()

    await rec._reconcile_row(row)

    mocks.apply_ta.assert_awaited_once()
    _, kwargs = mocks.apply_ta.await_args
    assert kwargs["outcome"] == "success"
    assert kwargs["action"] is mocks.on_success


@pytest.mark.asyncio
async def test_reconciler_terminal_succeeded_race_lost_no_action(monkeypatch):
    """TERMINAL_SUCCEEDED + write=False (race lost) → apply_terminal_action NOT called."""
    from dynastore.modules.tasks.liveness import LivenessVerdict

    mocks = _patch_reconciler_deps(
        monkeypatch, verdict=LivenessVerdict.TERMINAL_SUCCEEDED, write_returns=False,
    )
    rec = _make_reconciler()

    await rec._reconcile_row(_row_for_reconciler())

    mocks.apply_ta.assert_not_awaited()


@pytest.mark.asyncio
async def test_reconciler_terminal_failed_fires_on_failure(monkeypatch):
    """TERMINAL_FAILED + write=True → apply_terminal_action(outcome='failure')."""
    from dynastore.modules.tasks.liveness import LivenessVerdict

    mocks = _patch_reconciler_deps(
        monkeypatch, verdict=LivenessVerdict.TERMINAL_FAILED, write_returns=True,
    )
    rec = _make_reconciler()

    await rec._reconcile_row(_row_for_reconciler())

    mocks.apply_ta.assert_awaited_once()
    _, kwargs = mocks.apply_ta.await_args
    assert kwargs["outcome"] == "failure"
    assert kwargs["action"] is mocks.on_failure


@pytest.mark.asyncio
async def test_reconciler_terminal_failed_race_lost_no_action(monkeypatch):
    """TERMINAL_FAILED + write=False → apply_terminal_action NOT called."""
    from dynastore.modules.tasks.liveness import LivenessVerdict

    mocks = _patch_reconciler_deps(
        monkeypatch, verdict=LivenessVerdict.TERMINAL_FAILED, write_returns=False,
    )
    rec = _make_reconciler()

    await rec._reconcile_row(_row_for_reconciler())

    mocks.apply_ta.assert_not_awaited()


@pytest.mark.asyncio
async def test_reconciler_dead_fires_on_timeout(monkeypatch):
    """DEAD + write=True → apply_terminal_action(outcome='timeout')."""
    from dynastore.modules.tasks.liveness import LivenessVerdict

    mocks = _patch_reconciler_deps(
        monkeypatch, verdict=LivenessVerdict.DEAD, write_returns=True,
    )
    rec = _make_reconciler()

    await rec._reconcile_row(_row_for_reconciler())

    mocks.apply_ta.assert_awaited_once()
    _, kwargs = mocks.apply_ta.await_args
    assert kwargs["outcome"] == "timeout"
    assert kwargs["action"] is mocks.on_timeout


@pytest.mark.asyncio
async def test_reconciler_dead_race_lost_no_action(monkeypatch):
    """DEAD + write=False (race lost) → apply_terminal_action NOT called."""
    from dynastore.modules.tasks.liveness import LivenessVerdict

    mocks = _patch_reconciler_deps(
        monkeypatch, verdict=LivenessVerdict.DEAD, write_returns=False,
    )
    rec = _make_reconciler()

    await rec._reconcile_row(_row_for_reconciler())

    mocks.apply_ta.assert_not_awaited()


@pytest.mark.asyncio
async def test_reconciler_action_fields_passed_correctly(monkeypatch):
    """The reconciler passes the correct task-context fields to apply_terminal_action."""
    from dynastore.modules.tasks.liveness import LivenessVerdict

    mocks = _patch_reconciler_deps(
        monkeypatch, verdict=LivenessVerdict.TERMINAL_SUCCEEDED, write_returns=True,
    )
    rec = _make_reconciler()
    row = _row_for_reconciler(task_type="gdal_warp")

    await rec._reconcile_row(row)

    mocks.apply_ta.assert_awaited_once()
    _, kwargs = mocks.apply_ta.await_args
    assert kwargs["task_type"] == "gdal_warp"
    assert kwargs["inputs"] == row["inputs"]
    assert kwargs["caller_id"] == row["caller_id"]
    assert kwargs["collection_id"] == row["collection_id"]
    assert kwargs["schema"] == row["schema_name"]
    assert kwargs["scope"] == row["scope"]


@pytest.mark.asyncio
async def test_reconciler_apply_ta_failure_does_not_propagate(monkeypatch):
    """A raised exception inside apply_terminal_action must NOT escape the reconciler."""
    from dynastore.modules.tasks.liveness import LivenessVerdict

    mocks = _patch_reconciler_deps(
        monkeypatch, verdict=LivenessVerdict.TERMINAL_SUCCEEDED, write_returns=True,
    )
    mocks.apply_ta.side_effect = RuntimeError("follow-on PG down")

    rec = _make_reconciler()
    # Must not raise — fail-soft.
    outcome = await rec._reconcile_row(_row_for_reconciler())

    # ReconcileOutcome is still returned (acted).
    assert outcome is not None
    assert outcome.race_lost is False
