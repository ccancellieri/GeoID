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

"""Unit tests for GcpJobRunner confirmed-dismiss (signal_stop / force_stop).

Covers:
  - GcpJobRunner.signal_stop / force_stop: cancel called with right name;
    no-runner_ref → False; error → False (never raises).
  - Reconciler DISMISSED-unconfirmed branch:
      * already stopped → stamps dismiss_confirmed_at, returns True.
      * still ALIVE within deadline → signal_stop called, stays NULL, returns False.
      * still ALIVE past deadline → force_stop called + stamp + metric.
  - StopSignalProtocol structural check (GcpJobRunner satisfies it).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest  # noqa: F401 — collected by pytest, not imported directly


@pytest.fixture(autouse=True)
def disable_managed_eventing():
    """Neutralize the DB-bound autouse fixture from gcp/conftest.py."""
    return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _runner():
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner
    return GcpJobRunner()


def _make_task(runner_ref=None, task_id=None):
    """Minimal Task-like namespace sufficient for stop-signal methods."""
    return SimpleNamespace(
        task_id=task_id or uuid.uuid4(),
        runner_ref=runner_ref,
        owner_id="gcp_cloud_run_abc",
        schema_name="tasks",
        task_type="ingest",
        status="DISMISSED",
        dismiss_confirmed_at=None,
        timestamp=datetime.now(timezone.utc) - timedelta(minutes=5),
        started_at=datetime.now(timezone.utc) - timedelta(minutes=5),
        last_heartbeat_at=datetime.now(timezone.utc) - timedelta(minutes=2),
        retry_count=0,
        max_retries=3,
        locked_until=None,
        caller_id="user@example.com",
        inputs={},
        outputs=None,
        collection_id=None,
        scope="CATALOG",
        error_message=None,
        progress=0,
        type="task",
        execution_mode="ASYNCHRONOUS",
        dedup_key=None,
        finished_at=None,
        runner_type="gcp_cloud_run",
        links=[],
        title=None,
        jobID=task_id or uuid.uuid4(),
    )


def _make_row(
    *,
    owner_id="gcp_cloud_run_abc",
    runner_ref: str | None = "projects/p/locations/r/jobs/j/executions/e",
    last_heartbeat_at=None,
    timestamp=None,
    task_id=None,
):
    now = datetime.now(timezone.utc)
    return {
        "task_id": task_id or uuid.uuid4(),
        "schema_name": "tasks",
        "task_type": "ingest",
        "owner_id": owner_id,
        "runner_ref": runner_ref,
        "timestamp": timestamp or (now - timedelta(minutes=5)),
        "started_at": now - timedelta(minutes=5),
        "last_heartbeat_at": last_heartbeat_at or (now - timedelta(minutes=2)),
        "retry_count": 0,
        "max_retries": 3,
    }


def _make_reconciler():
    from dynastore.modules.gcp.liveness_reconciler import GcpLivenessReconciler
    return GcpLivenessReconciler(
        engine=SimpleNamespace(),
        interval_seconds=0.01,
        extend_visibility_seconds=300,
        unknown_grace_seconds=180,
    )


def _reconciler_mod():
    from dynastore.modules.gcp import liveness_reconciler
    return liveness_reconciler


# ---------------------------------------------------------------------------
# StopSignalProtocol structural check
# ---------------------------------------------------------------------------

def test_gcp_job_runner_satisfies_stop_signal_protocol():
    """GcpJobRunner must structurally satisfy StopSignalProtocol (runtime_checkable)
    so get_protocols(StopSignalProtocol) can discover it."""
    from dynastore.modules.tasks.liveness import StopSignalProtocol
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner
    assert isinstance(GcpJobRunner(), StopSignalProtocol)


def test_gcp_job_runner_still_satisfies_liveness_probe_protocol():
    """Adding StopSignalProtocol MUST NOT drop GcpJobRunner from LivenessProbeProtocol
    — both protocols are independently satisfied."""
    from dynastore.modules.tasks.liveness import LivenessProbeProtocol
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner
    assert isinstance(GcpJobRunner(), LivenessProbeProtocol)


# ---------------------------------------------------------------------------
# GcpJobRunner.signal_stop
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_signal_stop_no_runner_ref_returns_false():
    """No runner_ref → cannot cancel → returns False without calling GCP."""
    runner = _runner()
    task = _make_task(runner_ref=None)
    result = await runner.signal_stop(task)
    assert result is False


@pytest.mark.asyncio
async def test_signal_stop_calls_cancel_execution_with_correct_name():
    """Happy path: cancel_execution is called with name=runner_ref."""
    runner = _runner()
    ref = "projects/p/locations/r/jobs/j/executions/e"
    task = _make_task(runner_ref=ref)

    mock_client = MagicMock()
    mock_client.cancel_execution = AsyncMock(return_value=MagicMock())

    with patch.object(
        runner.__class__, "_get_executions_client_safe", return_value=mock_client
    ):
        result = await runner.signal_stop(task)

    assert result is True
    mock_client.cancel_execution.assert_awaited_once_with(name=ref)


@pytest.mark.asyncio
async def test_signal_stop_api_error_returns_false_not_raise():
    """Any GCP API error must return False — signal_stop MUST NOT raise."""
    runner = _runner()
    ref = "projects/p/locations/r/jobs/j/executions/e"
    task = _make_task(runner_ref=ref)

    mock_client = MagicMock()
    mock_client.cancel_execution = AsyncMock(side_effect=RuntimeError("quota exceeded"))

    with patch.object(
        runner.__class__, "_get_executions_client_safe", return_value=mock_client
    ):
        result = await runner.signal_stop(task)

    assert result is False  # error swallowed, not raised


@pytest.mark.asyncio
async def test_signal_stop_no_client_returns_false():
    """When the executions client is unavailable, return False gracefully."""
    runner = _runner()
    task = _make_task(runner_ref="projects/p/.../executions/e")

    with patch.object(
        runner.__class__, "_get_executions_client_safe", return_value=None
    ):
        result = await runner.signal_stop(task)

    assert result is False


# ---------------------------------------------------------------------------
# GcpJobRunner.force_stop
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_force_stop_no_runner_ref_returns_false():
    """No runner_ref → cannot delete → returns False."""
    runner = _runner()
    task = _make_task(runner_ref=None)
    result = await runner.force_stop(task)
    assert result is False


@pytest.mark.asyncio
async def test_force_stop_calls_delete_execution():
    """Happy path: delete_execution is called with name=runner_ref."""
    runner = _runner()
    ref = "projects/p/locations/r/jobs/j/executions/e"
    task = _make_task(runner_ref=ref)

    mock_client = MagicMock()
    mock_client.delete_execution = AsyncMock(return_value=MagicMock())

    with patch.object(
        runner.__class__, "_get_executions_client_safe", return_value=mock_client
    ):
        result = await runner.force_stop(task)

    assert result is True
    mock_client.delete_execution.assert_awaited_once_with(name=ref)


@pytest.mark.asyncio
async def test_force_stop_api_error_returns_false_not_raise():
    """Any GCP API error must return False — force_stop MUST NOT raise."""
    runner = _runner()
    ref = "projects/p/locations/r/jobs/j/executions/e"
    task = _make_task(runner_ref=ref)

    mock_client = MagicMock()
    mock_client.delete_execution = AsyncMock(side_effect=PermissionError("IAM denied"))

    with patch.object(
        runner.__class__, "_get_executions_client_safe", return_value=mock_client
    ):
        result = await runner.force_stop(task)

    assert result is False


# ---------------------------------------------------------------------------
# Reconciler: DISMISSED-unconfirmed branch (_reconcile_dismissed_row)
# ---------------------------------------------------------------------------

def _patch_dismiss_actions(monkeypatch):
    """Patch stamp_dismiss_confirmed and select_dismissed_unconfirmed_gcp_tasks."""
    from dynastore.modules.tasks import tasks_module

    stamp = AsyncMock(return_value=True)
    select_dismissed = AsyncMock(return_value=[])
    monkeypatch.setattr(tasks_module, "stamp_dismiss_confirmed", stamp)
    monkeypatch.setattr(
        tasks_module, "select_dismissed_unconfirmed_gcp_tasks", select_dismissed
    )
    return SimpleNamespace(stamp=stamp, select_dismissed=select_dismissed)


class _FakeProbe:
    """A probe that returns a configurable verdict — no GCP I/O."""
    runner_type = "gcp_cloud_run"

    def __init__(self, verdict):
        self._verdict = verdict

    def owns(self, owner_id):
        return True

    async def probe_liveness(self, task):
        return self._verdict


class _FakeStopRunner:
    """A stop-signal runner whose signal/force methods are AsyncMocks."""
    runner_type = "gcp_cloud_run"

    def __init__(self):
        self.signal_stop = AsyncMock(return_value=True)
        self.force_stop = AsyncMock(return_value=True)

    def owns(self, owner_id):
        return True


@pytest.mark.asyncio
async def test_dismissed_already_stopped_stamps_confirmed(monkeypatch):
    """When the probe says DEAD, dismiss_confirmed_at is stamped → returns True."""
    from dynastore.modules.tasks.liveness import LivenessVerdict

    actions = _patch_dismiss_actions(monkeypatch)
    fake_runner = _FakeStopRunner()
    monkeypatch.setattr(_reconciler_mod(), "resolve_probe", lambda oid: _FakeProbe(LivenessVerdict.DEAD))
    monkeypatch.setattr(_reconciler_mod(), "resolve_stop_signal", lambda oid: fake_runner)

    rec = _make_reconciler()
    row = _make_row()
    result = await rec._reconcile_dismissed_row(row)

    assert result is True
    actions.stamp.assert_awaited_once_with(rec._engine, row["task_id"])
    # signal_stop / force_stop must NOT be called when already stopped.
    fake_runner.signal_stop.assert_not_awaited()
    fake_runner.force_stop.assert_not_awaited()


@pytest.mark.asyncio
async def test_dismissed_terminal_succeeded_stamps_confirmed(monkeypatch):
    """TERMINAL_SUCCEEDED is also a stopped state — confirm immediately."""
    from dynastore.modules.tasks.liveness import LivenessVerdict

    actions = _patch_dismiss_actions(monkeypatch)
    monkeypatch.setattr(_reconciler_mod(), "resolve_probe", lambda oid: _FakeProbe(LivenessVerdict.TERMINAL_SUCCEEDED))
    monkeypatch.setattr(_reconciler_mod(), "resolve_stop_signal", lambda oid: _FakeStopRunner())

    rec = _make_reconciler()
    result = await rec._reconcile_dismissed_row(_make_row())

    assert result is True
    actions.stamp.assert_awaited_once()


@pytest.mark.asyncio
async def test_dismissed_alive_within_deadline_calls_signal_stop(monkeypatch):
    """Execution ALIVE and within deadline → signal_stop called, stays unconfirmed."""
    from dynastore.modules.tasks.liveness import LivenessVerdict

    actions = _patch_dismiss_actions(monkeypatch)
    fake_runner = _FakeStopRunner()
    monkeypatch.setattr(_reconciler_mod(), "resolve_probe", lambda oid: _FakeProbe(LivenessVerdict.ALIVE))
    monkeypatch.setattr(_reconciler_mod(), "resolve_stop_signal", lambda oid: fake_runner)

    rec = _make_reconciler()
    # last_heartbeat_at is recent — well within the 600s deadline.
    now = datetime.now(timezone.utc)
    row = _make_row(last_heartbeat_at=now - timedelta(seconds=30))

    result = await rec._reconcile_dismissed_row(row)

    assert result is False  # not yet confirmed
    fake_runner.signal_stop.assert_awaited_once()
    fake_runner.force_stop.assert_not_awaited()
    actions.stamp.assert_not_awaited()


@pytest.mark.asyncio
async def test_dismissed_alive_past_deadline_calls_force_stop_and_stamps(monkeypatch):
    """Execution ALIVE past _DISMISS_FORCE_DELETE_AFTER → force_stop + stamp → True."""
    from dynastore.modules.tasks.liveness import LivenessVerdict
    from dynastore.modules.gcp.liveness_reconciler import _DISMISS_FORCE_DELETE_AFTER

    actions = _patch_dismiss_actions(monkeypatch)
    fake_runner = _FakeStopRunner()
    monkeypatch.setattr(_reconciler_mod(), "resolve_probe", lambda oid: _FakeProbe(LivenessVerdict.ALIVE))
    monkeypatch.setattr(_reconciler_mod(), "resolve_stop_signal", lambda oid: fake_runner)

    rec = _make_reconciler()
    # last_heartbeat_at is old enough to be past the deadline.
    now = datetime.now(timezone.utc)
    old_ts = now - _DISMISS_FORCE_DELETE_AFTER - timedelta(seconds=60)
    row = _make_row(last_heartbeat_at=old_ts)

    result = await rec._reconcile_dismissed_row(row)

    assert result is True
    fake_runner.force_stop.assert_awaited_once()
    fake_runner.signal_stop.assert_not_awaited()
    actions.stamp.assert_awaited_once_with(rec._engine, row["task_id"])


@pytest.mark.asyncio
async def test_dismissed_unknown_no_ref_stamps_confirmed(monkeypatch):
    """UNKNOWN verdict with no runner_ref → treat as stopped → stamp confirmed."""
    from dynastore.modules.tasks.liveness import LivenessVerdict

    actions = _patch_dismiss_actions(monkeypatch)
    fake_runner = _FakeStopRunner()
    monkeypatch.setattr(_reconciler_mod(), "resolve_probe", lambda oid: _FakeProbe(LivenessVerdict.UNKNOWN))
    monkeypatch.setattr(_reconciler_mod(), "resolve_stop_signal", lambda oid: fake_runner)

    rec = _make_reconciler()
    row = _make_row(runner_ref=None)  # no handle → nothing to probe
    result = await rec._reconcile_dismissed_row(row)

    assert result is True
    actions.stamp.assert_awaited_once()
    fake_runner.signal_stop.assert_not_awaited()
    fake_runner.force_stop.assert_not_awaited()


@pytest.mark.asyncio
async def test_dismiss_pass_emits_metric_log_when_rows_present(monkeypatch, caplog):
    """When dismissed rows are scanned, a structured dismiss-pass log line is emitted."""
    from dynastore.modules.tasks import tasks_module
    from dynastore.modules.tasks.liveness import LivenessVerdict

    _patch_dismiss_actions(monkeypatch)
    # select_lapsed_gcp_tasks returns nothing (we only care about dismissed pass).
    monkeypatch.setattr(tasks_module, "select_lapsed_gcp_tasks", AsyncMock(return_value=[]))
    # One dismissed row that resolves as stopped.
    row = _make_row()
    monkeypatch.setattr(
        tasks_module, "select_dismissed_unconfirmed_gcp_tasks", AsyncMock(return_value=[row])
    )
    monkeypatch.setattr(_reconciler_mod(), "resolve_probe", lambda oid: _FakeProbe(LivenessVerdict.DEAD))
    monkeypatch.setattr(_reconciler_mod(), "resolve_stop_signal", lambda oid: _FakeStopRunner())

    rec = _make_reconciler()
    with caplog.at_level("INFO", logger="dynastore.modules.gcp.liveness_reconciler"):
        await rec._reconcile_once()

    dismiss_lines = [
        r for r in caplog.records
        if "liveness_dismiss_pass" in r.getMessage()
    ]
    assert dismiss_lines, "dismiss pass must emit a structured log line when rows are scanned"
    msg = dismiss_lines[-1].getMessage()
    assert "scanned=1" in msg
    assert "dismiss_unconfirmed_total=1" in msg
    assert "service=" in msg


@pytest.mark.asyncio
async def test_dismiss_pass_no_log_when_no_dismissed_rows(monkeypatch, caplog):
    """When no dismissed rows are found, no dismiss-pass log line is emitted."""
    from dynastore.modules.tasks import tasks_module

    _patch_dismiss_actions(monkeypatch)
    monkeypatch.setattr(tasks_module, "select_lapsed_gcp_tasks", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        tasks_module, "select_dismissed_unconfirmed_gcp_tasks", AsyncMock(return_value=[])
    )

    rec = _make_reconciler()
    with caplog.at_level("INFO", logger="dynastore.modules.gcp.liveness_reconciler"):
        await rec._reconcile_once()

    dismiss_lines = [
        r for r in caplog.records
        if "liveness_dismiss_pass" in r.getMessage()
    ]
    assert not dismiss_lines, "no dismiss-pass line should appear when there are no dismissed rows"


@pytest.mark.asyncio
async def test_dismiss_row_error_does_not_stop_rest(monkeypatch):
    """One bad dismissed row must not prevent the rest from being processed."""
    from dynastore.modules.tasks import tasks_module
    from dynastore.modules.tasks.liveness import LivenessVerdict

    actions = _patch_dismiss_actions(monkeypatch)
    monkeypatch.setattr(tasks_module, "select_lapsed_gcp_tasks", AsyncMock(return_value=[]))

    rows = [_make_row(), _make_row(), _make_row()]
    monkeypatch.setattr(
        tasks_module, "select_dismissed_unconfirmed_gcp_tasks", AsyncMock(return_value=rows)
    )

    call_count = {"n": 0}

    class _FlakyProbe:
        runner_type = "gcp_cloud_run"

        def owns(self, owner_id):
            return True

        async def probe_liveness(self, task):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise RuntimeError("probe exploded")
            return LivenessVerdict.DEAD

    monkeypatch.setattr(_reconciler_mod(), "resolve_probe", lambda oid: _FlakyProbe())
    monkeypatch.setattr(_reconciler_mod(), "resolve_stop_signal", lambda oid: _FakeStopRunner())

    rec = _make_reconciler()
    await rec._reconcile_once()

    # Two good rows were processed (stamp called twice).
    assert actions.stamp.await_count == 2
