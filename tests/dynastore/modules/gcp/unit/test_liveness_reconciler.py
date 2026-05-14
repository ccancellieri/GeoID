"""Unit tests for ``GcpLivenessReconciler`` — the per-verdict reconciler loop.

#735: this loop replaces the fixed spawn-lease guess. Every ~20s (faster than
the 60s pg_cron reaper, so it gets first look) it scans lapsed-lease
``gcp_cloud_run_*`` task rows, probes the owning runner, and acts on the
verdict — extending the lease of a live execution, failing a dead one, or
reconciling a row whose execution already terminated. The pg_cron reaper stays
unchanged as the ultimate backstop.
"""

from __future__ import annotations

import asyncio
import inspect
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


@pytest.fixture(autouse=True)
def disable_managed_eventing():
    """Neutralize the DB-bound autouse fixture from gcp/conftest.py."""
    return None


def _reconciler_mod():
    from dynastore.modules.gcp import liveness_reconciler
    return liveness_reconciler


def _make_reconciler(**kwargs):
    from dynastore.modules.gcp.liveness_reconciler import GcpLivenessReconciler

    defaults = dict(
        interval_seconds=0.01,
        extend_visibility_seconds=300,
        unknown_grace_seconds=180,
    )
    defaults.update(kwargs)
    return GcpLivenessReconciler(engine=SimpleNamespace(), **defaults)


def _row(*, owner_id="gcp_cloud_run_abc", runner_ref="projects/p/.../executions/e",
         started_at=None, outputs=None):
    return {
        "task_id": uuid.uuid4(),
        "schema_name": "tasks",
        "task_type": "ingest",
        "owner_id": owner_id,
        "runner_ref": runner_ref,
        "started_at": started_at or datetime.now(timezone.utc),
        "outputs": outputs,
        "retry_count": 0,
        "max_retries": 3,
        "locked_until": datetime.now(timezone.utc) - timedelta(seconds=5),
    }


class _Probe:
    runner_type = "gcp_cloud_run"

    def __init__(self, verdict):
        self._verdict = verdict

    def owns(self, owner_id):
        return True

    async def probe_liveness(self, task):
        return self._verdict


def _patch_actions(monkeypatch):
    """Replace the terminal/heartbeat helpers with AsyncMocks."""
    from dynastore.modules.tasks import tasks_module

    hb = AsyncMock()
    fail = AsyncMock()
    complete = AsyncMock()
    monkeypatch.setattr(tasks_module, "heartbeat_tasks", hb)
    monkeypatch.setattr(tasks_module, "fail_task", fail)
    monkeypatch.setattr(tasks_module, "complete_task", complete)
    return SimpleNamespace(heartbeat=hb, fail=fail, complete=complete)


def _patch_probe(monkeypatch, verdict):
    probe = _Probe(verdict)
    monkeypatch.setattr(_reconciler_mod(), "resolve_probe", lambda owner_id: probe)
    return probe


# --- per-verdict actions ---------------------------------------------------

@pytest.mark.asyncio
async def test_alive_extends_lease(monkeypatch):
    from dynastore.modules.tasks.liveness import LivenessVerdict

    actions = _patch_actions(monkeypatch)
    _patch_probe(monkeypatch, LivenessVerdict.ALIVE)
    rec = _make_reconciler()
    row = _row()

    await rec._reconcile_row(row)

    actions.heartbeat.assert_awaited_once()
    assert actions.heartbeat.await_args.args[1] == [row["task_id"]]
    actions.fail.assert_not_awaited()
    actions.complete.assert_not_awaited()


@pytest.mark.asyncio
async def test_dead_fails_task_with_retry(monkeypatch):
    from dynastore.modules.tasks.liveness import LivenessVerdict

    actions = _patch_actions(monkeypatch)
    _patch_probe(monkeypatch, LivenessVerdict.DEAD)
    rec = _make_reconciler()
    row = _row()

    await rec._reconcile_row(row)

    actions.fail.assert_awaited_once()
    assert actions.fail.await_args.kwargs.get("retry") is True
    actions.heartbeat.assert_not_awaited()
    actions.complete.assert_not_awaited()


@pytest.mark.asyncio
async def test_terminal_failed_fails_task_with_retry(monkeypatch):
    from dynastore.modules.tasks.liveness import LivenessVerdict

    actions = _patch_actions(monkeypatch)
    _patch_probe(monkeypatch, LivenessVerdict.TERMINAL_FAILED)
    rec = _make_reconciler()

    await rec._reconcile_row(_row())

    actions.fail.assert_awaited_once()
    assert actions.fail.await_args.kwargs.get("retry") is True


@pytest.mark.asyncio
async def test_terminal_succeeded_completes_task_with_outputs(monkeypatch):
    from dynastore.modules.tasks.liveness import LivenessVerdict

    actions = _patch_actions(monkeypatch)
    _patch_probe(monkeypatch, LivenessVerdict.TERMINAL_SUCCEEDED)
    rec = _make_reconciler()
    row = _row(outputs={"result": "ok"})

    await rec._reconcile_row(row)

    actions.complete.assert_awaited_once()
    assert actions.complete.await_args.kwargs.get("outputs") == {"result": "ok"}
    actions.fail.assert_not_awaited()


@pytest.mark.asyncio
async def test_unknown_young_no_ref_extends_once(monkeypatch):
    """A row with no runner_ref yet but still inside the spawn→capture grace
    window gets one short lease extension — covering the capture gap."""
    from dynastore.modules.tasks.liveness import LivenessVerdict

    actions = _patch_actions(monkeypatch)
    _patch_probe(monkeypatch, LivenessVerdict.UNKNOWN)
    rec = _make_reconciler(unknown_grace_seconds=180)
    row = _row(runner_ref=None, started_at=datetime.now(timezone.utc) - timedelta(seconds=10))

    await rec._reconcile_row(row)

    actions.heartbeat.assert_awaited_once()


@pytest.mark.asyncio
async def test_unknown_old_is_noop(monkeypatch):
    """Past the grace window with no runner_ref — leave it for the pg_cron reaper."""
    from dynastore.modules.tasks.liveness import LivenessVerdict

    actions = _patch_actions(monkeypatch)
    _patch_probe(monkeypatch, LivenessVerdict.UNKNOWN)
    rec = _make_reconciler(unknown_grace_seconds=60)
    row = _row(runner_ref=None, started_at=datetime.now(timezone.utc) - timedelta(seconds=600))

    await rec._reconcile_row(row)

    actions.heartbeat.assert_not_awaited()
    actions.fail.assert_not_awaited()
    actions.complete.assert_not_awaited()


@pytest.mark.asyncio
async def test_unmapped_owner_is_noop(monkeypatch):
    """No probe owns the row (in-process / ephemeral runner) — reconciler
    no-ops, the pg_cron reaper handles it exactly as today."""
    actions = _patch_actions(monkeypatch)
    monkeypatch.setattr(_reconciler_mod(), "resolve_probe", lambda owner_id: None)
    rec = _make_reconciler()

    await rec._reconcile_row(_row(owner_id="dispatcher-pod-1"))

    actions.heartbeat.assert_not_awaited()
    actions.fail.assert_not_awaited()
    actions.complete.assert_not_awaited()


# --- loop resilience -------------------------------------------------------

@pytest.mark.asyncio
async def test_reconcile_once_one_bad_row_does_not_stop_the_rest(monkeypatch):
    from dynastore.modules.tasks import tasks_module
    from dynastore.modules.tasks.liveness import LivenessVerdict

    actions = _patch_actions(monkeypatch)
    rows = [_row(), _row(), _row()]
    monkeypatch.setattr(
        tasks_module, "select_lapsed_gcp_tasks", AsyncMock(return_value=rows)
    )

    # Second row's probe raises; first and third must still be reconciled.
    calls = {"n": 0}

    class _FlakyProbe:
        runner_type = "gcp_cloud_run"

        def owns(self, owner_id):
            return True

        async def probe_liveness(self, task):
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("probe blew up")
            return LivenessVerdict.ALIVE

    monkeypatch.setattr(_reconciler_mod(), "resolve_probe", lambda owner_id: _FlakyProbe())
    rec = _make_reconciler()

    await rec._reconcile_once()

    # Two good rows still extended despite the middle one raising.
    assert actions.heartbeat.await_count == 2


# --- lifecycle -------------------------------------------------------------

@pytest.mark.asyncio
async def test_start_stop_lifecycle(monkeypatch):
    rec = _make_reconciler(interval_seconds=0.01)
    ran = {"n": 0}

    async def _fake_once():
        ran["n"] += 1

    monkeypatch.setattr(rec, "_reconcile_once", _fake_once)

    rec.start()
    await asyncio.sleep(0.05)
    await rec.stop()

    assert ran["n"] >= 1
    # After stop the background task is finished and cleared.
    assert rec._task is None


@pytest.mark.asyncio
async def test_stop_is_safe_when_never_started():
    rec = _make_reconciler()
    await rec.stop()  # must not raise


def test_lifespan_gates_reconciler_on_job_runner_host():
    """The reconciler is started in ``GCPModule.lifespan`` only on a process
    that hosts the job-runner — Cloud Run Job containers and opted-out
    services must not run it (they would compete needlessly)."""
    from dynastore.modules.gcp import gcp_module

    src = inspect.getsource(gcp_module.GCPModule.lifespan)
    assert "GcpLivenessReconciler" in src
    assert "_should_register_gcp_job_runner" in src
