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

"""Unit tests for ``reconcile_task_liveness`` — the on-demand, read-path
liveness-reconcile trigger.

A Cloud Run Job container's SIGTERM-kill on ``taskTimeout`` leaves a task row
``ACTIVE``/``RUNNING`` with a lapsing lease and no terminal write. The periodic
``GcpLivenessReconciler`` fixes this on a timer, but a client poll never
triggers a probe between passes. ``reconcile_task_liveness`` closes that gap
by probing the row's owning runner right before a single-job GET response is
serialized — best-effort and budget-capped so it can never turn a poll into a
slow or failed request.

``decide_verdict_action`` is the shared verdict→action mapping also used by
``GcpLivenessReconciler._reconcile_row`` — tested here as the single source of
truth for which write each verdict authorizes.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest


def _reconciliation():
    from dynastore.modules.tasks import reconciliation
    return reconciliation


def _task(*, status, locked_until=None, owner_id="gcp_cloud_run_abc", outputs=None,
          catalog_id="s_cat"):
    from dynastore.models.tasks import Task
    return Task(
        task_type="ingest",
        status=status,
        catalog_id=catalog_id,
        owner_id=owner_id,
        locked_until=locked_until,
        outputs=outputs,
    )


class _Probe:
    """A fake probe returning a fixed verdict, or raising / hanging on demand."""

    runner_type = "gcp_cloud_run"

    def __init__(self, verdict=None, *, raises=None, hang_seconds=None):
        self._verdict = verdict
        self._raises = raises
        self._hang_seconds = hang_seconds

    def owns(self, owner_id):
        return True

    async def probe_liveness(self, task):
        if self._hang_seconds is not None:
            await asyncio.sleep(self._hang_seconds)
        if self._raises is not None:
            raise self._raises
        return self._verdict


def _patch_probe(monkeypatch, probe):
    monkeypatch.setattr(_reconciliation(), "resolve_probe", lambda owner_id: probe)


def _patch_writers(monkeypatch, **overrides):
    """Replace the owner-guarded writers + get_task with AsyncMocks.

    Defaults all writers to ``True`` (acted) and ``get_task`` to a sentinel
    "refreshed" task, mirroring _patch_actions in test_liveness_reconciler.py.
    """
    from dynastore.modules.tasks import tasks_module

    defaults = dict(
        heartbeat_task_if_active=AsyncMock(return_value=True),
        fail_task=AsyncMock(return_value=True),
        complete_task=AsyncMock(return_value=True),
        get_task=AsyncMock(return_value=_task(status="COMPLETED")),
    )
    defaults.update(overrides)
    for name, mock in defaults.items():
        monkeypatch.setattr(tasks_module, name, mock)
    return defaults


# --- decide_verdict_action: the shared pure mapping -------------------------


def test_decide_verdict_action_mapping():
    from dynastore.modules.tasks.liveness import LivenessVerdict

    reconciliation = _reconciliation()
    decide = reconciliation.decide_verdict_action
    VerdictAction = reconciliation.VerdictAction

    assert decide(LivenessVerdict.ALIVE) == VerdictAction.EXTEND_LEASE
    assert decide(LivenessVerdict.DEAD) == VerdictAction.FAIL_RETRY
    assert decide(LivenessVerdict.TERMINAL_FAILED) == VerdictAction.FAIL_RETRY
    assert decide(LivenessVerdict.TERMINAL_SUCCEEDED) == VerdictAction.COMPLETE
    assert decide(LivenessVerdict.UNKNOWN) == VerdictAction.NOOP


# --- guard: no-op paths ------------------------------------------------------


@pytest.mark.asyncio
async def test_returns_unchanged_when_status_terminal(monkeypatch):
    reconciliation = _reconciliation()
    probe_resolved = {"called": False}
    monkeypatch.setattr(
        reconciliation, "resolve_probe",
        lambda owner_id: probe_resolved.__setitem__("called", True) or None,
    )
    task = _task(
        status="COMPLETED",
        locked_until=datetime.now(timezone.utc) - timedelta(seconds=30),
    )

    result = await reconciliation.reconcile_task_liveness(object(), task, schema="s_cat")

    assert result is task
    assert probe_resolved["called"] is False, "a terminal status must never reach the probe"


@pytest.mark.asyncio
async def test_returns_unchanged_when_lease_not_lapsed(monkeypatch):
    reconciliation = _reconciliation()
    probe_resolved = {"called": False}
    monkeypatch.setattr(
        reconciliation, "resolve_probe",
        lambda owner_id: probe_resolved.__setitem__("called", True) or None,
    )
    task = _task(
        status="ACTIVE",
        locked_until=datetime.now(timezone.utc) + timedelta(seconds=30),
    )

    result = await reconciliation.reconcile_task_liveness(object(), task, schema="s_cat")

    assert result is task
    assert probe_resolved["called"] is False, "a fresh lease must never reach the probe"


@pytest.mark.asyncio
async def test_returns_unchanged_when_lease_is_null(monkeypatch):
    """A never-locked row (locked_until=None) is not lapsed by definition."""
    reconciliation = _reconciliation()
    monkeypatch.setattr(reconciliation, "resolve_probe", lambda owner_id: (_ for _ in ()).throw(
        AssertionError("resolve_probe must not be called")
    ))
    task = _task(status="ACTIVE", locked_until=None)

    result = await reconciliation.reconcile_task_liveness(object(), task, schema="s_cat")

    assert result is task


@pytest.mark.asyncio
async def test_returns_unchanged_when_no_probe_resolved(monkeypatch):
    reconciliation = _reconciliation()
    _patch_probe(monkeypatch, None)
    writers = _patch_writers(monkeypatch)
    task = _task(
        status="ACTIVE",
        locked_until=datetime.now(timezone.utc) - timedelta(seconds=30),
    )

    result = await reconciliation.reconcile_task_liveness(object(), task, schema="s_cat")

    assert result is task
    writers["heartbeat_task_if_active"].assert_not_awaited()
    writers["fail_task"].assert_not_awaited()
    writers["complete_task"].assert_not_awaited()


@pytest.mark.asyncio
async def test_returns_unchanged_on_probe_timeout(monkeypatch):
    reconciliation = _reconciliation()
    _patch_probe(monkeypatch, _Probe(hang_seconds=5))
    writers = _patch_writers(monkeypatch)
    task = _task(
        status="ACTIVE",
        locked_until=datetime.now(timezone.utc) - timedelta(seconds=30),
    )

    result = await reconciliation.reconcile_task_liveness(
        object(), task, schema="s_cat", budget_seconds=0.05,
    )

    assert result is task
    writers["heartbeat_task_if_active"].assert_not_awaited()
    writers["fail_task"].assert_not_awaited()
    writers["complete_task"].assert_not_awaited()


@pytest.mark.asyncio
async def test_returns_unchanged_when_probe_raises(monkeypatch):
    reconciliation = _reconciliation()
    _patch_probe(monkeypatch, _Probe(raises=RuntimeError("Executions API unreachable")))
    writers = _patch_writers(monkeypatch)
    task = _task(
        status="ACTIVE",
        locked_until=datetime.now(timezone.utc) - timedelta(seconds=30),
    )

    result = await reconciliation.reconcile_task_liveness(object(), task, schema="s_cat")

    assert result is task
    writers["heartbeat_task_if_active"].assert_not_awaited()
    writers["fail_task"].assert_not_awaited()
    writers["complete_task"].assert_not_awaited()


@pytest.mark.asyncio
async def test_unknown_verdict_leaves_row_unchanged(monkeypatch):
    from dynastore.modules.tasks.liveness import LivenessVerdict

    reconciliation = _reconciliation()
    _patch_probe(monkeypatch, _Probe(LivenessVerdict.UNKNOWN))
    writers = _patch_writers(monkeypatch)
    task = _task(
        status="ACTIVE",
        locked_until=datetime.now(timezone.utc) - timedelta(seconds=30),
    )

    result = await reconciliation.reconcile_task_liveness(object(), task, schema="s_cat")

    assert result is task
    writers["heartbeat_task_if_active"].assert_not_awaited()
    writers["fail_task"].assert_not_awaited()
    writers["complete_task"].assert_not_awaited()
    writers["get_task"].assert_not_awaited()


# --- per-verdict actions -----------------------------------------------------


@pytest.mark.asyncio
async def test_alive_extends_lease_and_returns_refreshed_task(monkeypatch):
    from dynastore.modules.tasks.liveness import LivenessVerdict

    reconciliation = _reconciliation()
    _patch_probe(monkeypatch, _Probe(LivenessVerdict.ALIVE))
    refreshed = _task(status="ACTIVE", owner_id="gcp_cloud_run_abc")
    writers = _patch_writers(monkeypatch, get_task=AsyncMock(return_value=refreshed))
    task = _task(
        status="ACTIVE",
        locked_until=datetime.now(timezone.utc) - timedelta(seconds=30),
    )

    result = await reconciliation.reconcile_task_liveness(object(), task, schema="s_cat")

    writers["heartbeat_task_if_active"].assert_awaited_once()
    assert writers["heartbeat_task_if_active"].await_args.args[1] == task.task_id
    writers["fail_task"].assert_not_awaited()
    writers["complete_task"].assert_not_awaited()
    writers["get_task"].assert_awaited_once()
    assert writers["get_task"].await_args.args[1:] == (task.task_id, "s_cat")
    assert result is refreshed


@pytest.mark.asyncio
async def test_dead_fails_task_with_retry_and_owner_guard(monkeypatch):
    from dynastore.modules.tasks.liveness import LivenessVerdict

    reconciliation = _reconciliation()
    _patch_probe(monkeypatch, _Probe(LivenessVerdict.DEAD))
    writers = _patch_writers(monkeypatch)
    task = _task(
        status="ACTIVE",
        owner_id="gcp_cloud_run_xyz",
        locked_until=datetime.now(timezone.utc) - timedelta(seconds=30),
    )

    result = await reconciliation.reconcile_task_liveness(object(), task, schema="s_cat")

    writers["fail_task"].assert_awaited_once()
    kwargs = writers["fail_task"].await_args.kwargs
    assert kwargs.get("retry") is True
    assert kwargs.get("owner_id") == "gcp_cloud_run_xyz"
    error_message = writers["fail_task"].await_args.args[3]
    assert "Reconciled" in error_message
    assert "dead" in error_message
    writers["heartbeat_task_if_active"].assert_not_awaited()
    writers["complete_task"].assert_not_awaited()
    assert result is writers["get_task"].return_value


@pytest.mark.asyncio
async def test_terminal_failed_fails_task_with_retry(monkeypatch):
    from dynastore.modules.tasks.liveness import LivenessVerdict

    reconciliation = _reconciliation()
    _patch_probe(monkeypatch, _Probe(LivenessVerdict.TERMINAL_FAILED))
    writers = _patch_writers(monkeypatch)
    task = _task(
        status="RUNNING",
        locked_until=datetime.now(timezone.utc) - timedelta(seconds=30),
    )

    await reconciliation.reconcile_task_liveness(object(), task, schema="s_cat")

    writers["fail_task"].assert_awaited_once()
    assert writers["fail_task"].await_args.kwargs.get("retry") is True


@pytest.mark.asyncio
async def test_terminal_succeeded_completes_with_outputs_and_owner_guard(monkeypatch):
    from dynastore.modules.tasks.liveness import LivenessVerdict

    reconciliation = _reconciliation()
    _patch_probe(monkeypatch, _Probe(LivenessVerdict.TERMINAL_SUCCEEDED))
    writers = _patch_writers(monkeypatch)
    task = _task(
        status="ACTIVE",
        owner_id="gcp_cloud_run_xyz",
        outputs={"result": "ok"},
        locked_until=datetime.now(timezone.utc) - timedelta(seconds=30),
    )

    result = await reconciliation.reconcile_task_liveness(object(), task, schema="s_cat")

    writers["complete_task"].assert_awaited_once()
    kwargs = writers["complete_task"].await_args.kwargs
    assert kwargs.get("outputs") == {"result": "ok"}
    assert kwargs.get("owner_id") == "gcp_cloud_run_xyz"
    writers["fail_task"].assert_not_awaited()
    writers["heartbeat_task_if_active"].assert_not_awaited()
    assert result is writers["get_task"].return_value


# --- lost-race handling -------------------------------------------------------


@pytest.mark.asyncio
async def test_lost_race_on_heartbeat_returns_task_unchanged_without_refetch(monkeypatch):
    """A 0-row heartbeat means the periodic reconciler / reaper won the race
    between this probe and the write — the row moved out from under us. The
    on-demand helper must return the ORIGINAL task, not re-fetch."""
    from dynastore.modules.tasks.liveness import LivenessVerdict

    reconciliation = _reconciliation()
    _patch_probe(monkeypatch, _Probe(LivenessVerdict.ALIVE))
    writers = _patch_writers(
        monkeypatch, heartbeat_task_if_active=AsyncMock(return_value=False)
    )
    task = _task(
        status="ACTIVE",
        locked_until=datetime.now(timezone.utc) - timedelta(seconds=30),
    )

    result = await reconciliation.reconcile_task_liveness(object(), task, schema="s_cat")

    assert result is task
    writers["get_task"].assert_not_awaited()


@pytest.mark.asyncio
async def test_lost_race_on_fail_task_returns_task_unchanged(monkeypatch):
    from dynastore.modules.tasks.liveness import LivenessVerdict

    reconciliation = _reconciliation()
    _patch_probe(monkeypatch, _Probe(LivenessVerdict.DEAD))
    writers = _patch_writers(monkeypatch, fail_task=AsyncMock(return_value=False))
    task = _task(
        status="ACTIVE",
        locked_until=datetime.now(timezone.utc) - timedelta(seconds=30),
    )

    result = await reconciliation.reconcile_task_liveness(object(), task, schema="s_cat")

    assert result is task
    writers["get_task"].assert_not_awaited()


# ---------------------------------------------------------------------------
# reconcile_secondary_indexing (#2897) — read-side convergence flip
# ---------------------------------------------------------------------------


def _pending_task(*, queued=3, catalog_id="s_cat", collection_id="col1"):
    return _task(
        status="COMPLETED",
        catalog_id=catalog_id,
        outputs={
            "message": "https://example.org/items",
            "secondary_indexing": {
                "state": "pending",
                "queued": queued,
                "message": f"primary write complete; {queued} entries pending asynchronous indexing",
            },
        },
    ).model_copy(update={"collection_id": collection_id})


def _patch_count(monkeypatch, *, return_value=None, side_effect=None):
    from dynastore.modules.tasks import async_writer_backlog

    mock = AsyncMock(return_value=return_value, side_effect=side_effect)
    monkeypatch.setattr(async_writer_backlog, "count_pending_item_ops", mock)
    return mock


def _patch_persist_outputs(monkeypatch, **kwargs):
    from dynastore.modules.tasks import tasks_module

    mock = AsyncMock(**kwargs)
    monkeypatch.setattr(tasks_module, "persist_outputs", mock)
    return mock


def _patch_managed_transaction(monkeypatch, *, raises=None):
    """Stand in for the real ``managed_transaction`` so tests never open an
    actual pool/transaction. ``raises`` simulates a bounded-acquire failure
    (e.g. ``PoolSaturationError``) before any connection is yielded; the
    success path yields a fake connection (specced as ``AsyncConnection`` so
    the function's own ``isinstance`` narrowing check passes) whose
    ``.execute`` (the ``SET LOCAL statement_timeout`` calls) is a no-op.
    """
    from contextlib import asynccontextmanager

    from sqlalchemy.ext.asyncio import AsyncConnection

    from dynastore.modules.db_config import query_executor

    fake_conn = AsyncMock(spec=AsyncConnection)

    @asynccontextmanager
    async def _fake(db_resource, *, acquire_timeout=None):
        if raises is not None:
            raise raises
        yield fake_conn

    monkeypatch.setattr(query_executor, "managed_transaction", _fake)
    return fake_conn


@pytest.mark.asyncio
async def test_secondary_indexing_noop_when_not_completed(monkeypatch):
    reconciliation = _reconciliation()
    count_mock = _patch_count(monkeypatch, return_value=0)
    task = _pending_task()
    task = task.model_copy(update={"status": "ACTIVE"})

    result = await reconciliation.reconcile_secondary_indexing(object(), task)

    assert result is task
    count_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_secondary_indexing_noop_when_no_outputs(monkeypatch):
    reconciliation = _reconciliation()
    count_mock = _patch_count(monkeypatch, return_value=0)
    task = _task(status="COMPLETED", outputs=None)

    result = await reconciliation.reconcile_secondary_indexing(object(), task)

    assert result is task
    count_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_secondary_indexing_noop_when_state_not_pending(monkeypatch):
    reconciliation = _reconciliation()
    count_mock = _patch_count(monkeypatch, return_value=0)
    task = _task(
        status="COMPLETED",
        outputs={"secondary_indexing": {"state": "converged", "queued": 0}},
    )

    result = await reconciliation.reconcile_secondary_indexing(object(), task)

    assert result is task
    count_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_secondary_indexing_zero_backlog_flips_to_converged_and_persists(monkeypatch):
    reconciliation = _reconciliation()
    _patch_managed_transaction(monkeypatch)
    count_mock = _patch_count(monkeypatch, return_value=0)
    persist_mock = _patch_persist_outputs(monkeypatch)
    task = _pending_task(queued=3)

    result = await reconciliation.reconcile_secondary_indexing(object(), task)

    assert result is not task
    assert result.outputs["secondary_indexing"] == {"state": "converged", "queued": 0}
    assert result.outputs["message"] == "https://example.org/items", (
        "other outputs keys must survive the merge"
    )
    count_mock.assert_awaited_once()
    persist_mock.assert_awaited_once()
    persisted_outputs = persist_mock.await_args.args[2]
    assert persisted_outputs["secondary_indexing"] == {"state": "converged", "queued": 0}


@pytest.mark.asyncio
async def test_secondary_indexing_nonzero_backlog_refreshes_count_without_persist(monkeypatch):
    reconciliation = _reconciliation()
    _patch_managed_transaction(monkeypatch)
    count_mock = _patch_count(monkeypatch, return_value=5)
    persist_mock = _patch_persist_outputs(monkeypatch)
    task = _pending_task(queued=3)

    result = await reconciliation.reconcile_secondary_indexing(object(), task)

    assert result.outputs["secondary_indexing"]["state"] == "pending"
    assert result.outputs["secondary_indexing"]["queued"] == 5
    count_mock.assert_awaited_once()
    persist_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_secondary_indexing_count_failure_serves_unchanged(monkeypatch):
    """A generic count failure (e.g. a 57014 statement-timeout cancel raised
    by the server-side ``SET LOCAL statement_timeout`` bound) must serve the
    task unchanged rather than raise or persist anything. The connection
    itself is never wrapped in ``asyncio.wait_for`` -- see
    ``test_secondary_indexing_pool_saturation_serves_unchanged`` for the
    acquire-side bound instead."""
    reconciliation = _reconciliation()
    _patch_managed_transaction(monkeypatch)
    count_mock = _patch_count(
        monkeypatch,
        side_effect=RuntimeError(
            "canceling statement due to statement timeout (57014)"
        ),
    )
    persist_mock = _patch_persist_outputs(monkeypatch)
    task = _pending_task(queued=3)

    result = await reconciliation.reconcile_secondary_indexing(object(), task)

    assert result is task
    count_mock.assert_awaited_once()
    persist_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_secondary_indexing_pool_saturation_serves_unchanged(monkeypatch):
    """A saturated pool must fail fast at ACQUIRE time (``PoolSaturationError``
    from ``managed_transaction(engine, acquire_timeout=...)``) and never reach
    the query -- the whole point of bounding the acquire instead of wrapping
    the in-flight query in ``asyncio.wait_for`` (which would leak the
    checked-out connection on cancellation)."""
    from dynastore.modules.db_config.exceptions import PoolSaturationError

    reconciliation = _reconciliation()
    _patch_managed_transaction(
        monkeypatch, raises=PoolSaturationError("pool saturated"),
    )
    count_mock = _patch_count(monkeypatch, return_value=0)
    persist_mock = _patch_persist_outputs(monkeypatch)
    task = _pending_task(queued=3)

    result = await reconciliation.reconcile_secondary_indexing(
        object(), task, budget_seconds=0.05,
    )

    assert result is task
    count_mock.assert_not_awaited()
    persist_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_secondary_indexing_persist_failure_still_serves_converged(monkeypatch):
    """The flip is served for THIS response even when persisting it fails."""
    reconciliation = _reconciliation()
    _patch_managed_transaction(monkeypatch)
    _patch_count(monkeypatch, return_value=0)
    persist_mock = _patch_persist_outputs(monkeypatch, side_effect=RuntimeError("db down"))
    task = _pending_task(queued=3)

    result = await reconciliation.reconcile_secondary_indexing(object(), task)

    assert result.outputs["secondary_indexing"] == {"state": "converged", "queued": 0}
    persist_mock.assert_awaited_once()
