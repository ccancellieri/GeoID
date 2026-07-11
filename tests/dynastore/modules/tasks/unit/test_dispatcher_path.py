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

"""Unit tests for the dispatcher-path fix in BackgroundRunner.

Before the M3 task-orchestration hardening, ``BackgroundRunner.run()``
always called ``tasks_mgr.create_task(..., initial_status='RUNNING')`` —
even when the dispatcher had already claimed a row via
``claim_batch``.  This created a duplicate row and let the dispatcher
mark the ORIGINAL claimed row COMPLETED prematurely (in ~124ms) while
the real work was still pending in the BackgroundExecutor.  The
duplicate stayed RUNNING forever with NULL heartbeat, invisible to the
(ACTIVE-only) janitor.

This suite asserts the post-fix contract:

- Dispatcher-path invocation (``extra_context['task_id']`` + ``task_timestamp``
  present) does NOT create a second row; returns ``DEFERRED_COMPLETION``.
- Direct-path invocation (empty ``extra_context``) still creates a row
  and returns ``StatusInfo`` — unchanged OGC Part 1 behaviour.
- The background coroutine updates the SAME claimed row
  (``update_task(claimed_task_id, ...)``) on both success and exception.
- The ``DEFERRED_COMPLETION`` sentinel is a singleton.
- The ``tasks.reap_stuck_tasks`` DDL + one-shot orphan-cleanup UPDATE
  contain the expected guard clauses (advisory lock on SKIP LOCKED,
  heartbeat-expiry predicate, retry_count handling, NOTIFY on reap).
"""

from __future__ import annotations

import uuid as _uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.tasks.models import (
    DEFERRED_COMPLETION,
    RunnerContext,
    _DeferredCompletionSentinel,
)
from sqlalchemy.engine import Engine as _SAEngine


def _fake_engine() -> MagicMock:
    """Build a MagicMock that passes pydantic's ``is_instance[Engine]``
    check on ``RunnerContext.engine``.  Tests don't touch the DB — they
    just need to construct a valid RunnerContext."""
    return MagicMock(spec=_SAEngine)


# ---------------------------------------------------------------------------
# DEFERRED_COMPLETION sentinel
# ---------------------------------------------------------------------------


def test_deferred_completion_is_singleton():
    """``DEFERRED_COMPLETION`` must be a singleton so dispatcher can test
    by ``is`` identity (cheap and unambiguous)."""
    assert DEFERRED_COMPLETION is _DeferredCompletionSentinel()
    assert _DeferredCompletionSentinel() is _DeferredCompletionSentinel()


def test_deferred_completion_is_truthy():
    """Must be truthy so the dispatcher's legacy
    ``if result is not None`` / ``if result:`` checks continue to treat
    the runner as having handled the task."""
    assert bool(DEFERRED_COMPLETION) is True
    assert DEFERRED_COMPLETION is not None


# ---------------------------------------------------------------------------
# BackgroundRunner — direct-invocation path (backward compat)
# ---------------------------------------------------------------------------


def _make_context(extra_context: Optional[Dict[str, Any]] = None) -> RunnerContext:
    """Build a minimal RunnerContext for BackgroundRunner tests."""
    return RunnerContext(
        engine=_fake_engine(),  # DbResource spec'd so pydantic is_instance passes
        task_type="gcp_provision_catalog",
        caller_id="system",
        inputs={"catalog_id": "test_cat"},
        db_schema="tasks",
        extra_context=extra_context or {},
    )


@pytest.mark.asyncio
async def test_background_runner_direct_path_creates_row():
    """Direct path (no ``task_id`` in extra_context) — OGC Part 1 / HTTP —
    MUST still call ``create_task`` and return a ``StatusInfo``."""
    from dynastore.modules.tasks.runners import BackgroundRunner
    from dynastore.modules.processes.models import StatusInfo

    runner = BackgroundRunner()
    ctx = _make_context()

    fake_job = MagicMock()
    fake_job.task_id = _uuid.uuid4()

    fake_tasks_mgr = MagicMock()
    fake_tasks_mgr.create_task = AsyncMock(return_value=fake_job)
    fake_tasks_mgr.update_task = AsyncMock()

    fake_task_instance = MagicMock()
    fake_task_instance.run = AsyncMock(return_value={"ok": True})
    fake_task_instance.__class__.__name__ = "FakeTask"

    with (
        patch("dynastore.tools.protocol_helpers.resolve", return_value=fake_tasks_mgr),
        patch(
            "dynastore.modules.tasks.runners.get_task_instance",
            return_value=fake_task_instance,
        ),
        patch(
            "dynastore.modules.tasks.runners.get_background_executor",
        ) as fake_exec,
    ):
        fake_exec.return_value.submit = MagicMock(return_value=MagicMock())
        result = await runner.run(ctx)

    assert isinstance(result, StatusInfo)
    assert result.status == "accepted"
    assert fake_tasks_mgr.create_task.await_count == 1, (
        "Direct path must create a new task row."
    )
    # initial_status is the important contract — unchanged from pre-fix.
    kwargs = fake_tasks_mgr.create_task.await_args.kwargs
    assert kwargs.get("initial_status") == "RUNNING"


# ---------------------------------------------------------------------------
# BackgroundRunner — dispatcher-claimed path (the fix)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_background_runner_dispatcher_path_does_not_create_duplicate():
    """Dispatcher-path invocation: ``extra_context`` contains ``task_id``
    + ``task_timestamp`` from the already-claimed row.  Runner MUST NOT
    call ``create_task`` (no duplicate row) and MUST return the
    ``DEFERRED_COMPLETION`` sentinel so the dispatcher skips
    ``complete_task``."""
    from dynastore.modules.tasks.runners import BackgroundRunner

    claimed_id = _uuid.uuid4()
    claimed_ts = datetime(2026, 4, 22, 12, 0, 0, tzinfo=timezone.utc)

    fake_heartbeat = MagicMock()
    fake_heartbeat.register = AsyncMock()
    fake_heartbeat.unregister = AsyncMock()

    ctx = _make_context({
        "task_id": str(claimed_id),
        "task_timestamp": claimed_ts,
        "heartbeat": fake_heartbeat,
    })

    runner = BackgroundRunner()

    fake_tasks_mgr = MagicMock()
    fake_tasks_mgr.create_task = AsyncMock()
    fake_tasks_mgr.update_task = AsyncMock()

    fake_task_instance = MagicMock()
    fake_task_instance.run = AsyncMock(return_value={"bucket": "created"})
    fake_task_instance.__class__.__name__ = "FakeTask"

    with (
        patch("dynastore.tools.protocol_helpers.resolve", return_value=fake_tasks_mgr),
        patch(
            "dynastore.modules.tasks.runners.get_task_instance",
            return_value=fake_task_instance,
        ),
        patch(
            "dynastore.modules.tasks.runners.get_background_executor",
        ) as fake_exec,
    ):
        fake_exec.return_value.submit = MagicMock(return_value=MagicMock())
        result = await runner.run(ctx)

    assert result is DEFERRED_COMPLETION, (
        "Dispatcher-path invocation must return DEFERRED_COMPLETION so the "
        "dispatcher skips its own complete_task call."
    )
    assert fake_tasks_mgr.create_task.await_count == 0, (
        "Dispatcher-path MUST NOT call create_task — the row was already "
        "claimed by claim_batch; creating another row caused the pre-fix "
        "RUNNING-with-NULL-heartbeat duplicate bug."
    )


@pytest.mark.asyncio
async def test_background_runner_claimed_success_completes_same_row():
    """On success the background coroutine must call
    :func:`complete_task` on the CLAIMED task_id (not a fresh one)."""
    from dynastore.modules.tasks.runners import BackgroundRunner

    claimed_id = _uuid.uuid4()
    claimed_ts = datetime(2026, 4, 22, 12, 0, 0, tzinfo=timezone.utc)

    ctx = _make_context({
        "task_id": str(claimed_id),
        "task_timestamp": claimed_ts,
    })

    runner = BackgroundRunner()

    fake_tasks_mgr = MagicMock()
    fake_tasks_mgr.create_task = AsyncMock()

    fake_task_instance = MagicMock()
    fake_task_instance.run = AsyncMock(return_value={"bucket": "gs://x"})
    fake_task_instance.__class__.__name__ = "FakeTask"

    # Capture the submitted coroutine so we can await it directly
    captured: Dict[str, Any] = {}

    def _capture_submit(coro, task_name=None, **_):
        captured["coro"] = coro
        return MagicMock()

    fake_complete = AsyncMock()
    fake_fail = AsyncMock()

    with (
        patch("dynastore.tools.protocol_helpers.resolve", return_value=fake_tasks_mgr),
        patch(
            "dynastore.modules.tasks.runners.get_task_instance",
            return_value=fake_task_instance,
        ),
        patch(
            "dynastore.modules.tasks.runners.get_background_executor",
        ) as fake_exec,
        patch("dynastore.modules.tasks.tasks_module.complete_task", fake_complete),
        patch("dynastore.modules.tasks.tasks_module.fail_task", fake_fail),
    ):
        fake_exec.return_value.submit = _capture_submit
        await runner.run(ctx)
        await captured["coro"]

    fake_complete.assert_awaited_once()
    call = fake_complete.await_args
    # Positional args: (engine, task_id, timestamp); outputs kwarg
    assert call.args[1] == claimed_id, (
        "complete_task MUST be called with the CLAIMED task_id, not a new one."
    )
    assert call.kwargs.get("outputs") == {"bucket": "gs://x"}
    fake_fail.assert_not_called()


@pytest.mark.asyncio
async def test_background_runner_claimed_exception_fails_same_row():
    """On generic exception the background coroutine must call
    :func:`fail_task` with ``retry=True`` on the CLAIMED task_id."""
    from dynastore.modules.tasks.runners import BackgroundRunner

    claimed_id = _uuid.uuid4()
    claimed_ts = datetime(2026, 4, 22, 12, 0, 0, tzinfo=timezone.utc)

    ctx = _make_context({
        "task_id": str(claimed_id),
        "task_timestamp": claimed_ts,
    })

    runner = BackgroundRunner()

    fake_tasks_mgr = MagicMock()
    fake_tasks_mgr.create_task = AsyncMock()

    fake_task_instance = MagicMock()
    fake_task_instance.run = AsyncMock(side_effect=RuntimeError("GCS bucket create failed"))
    fake_task_instance.__class__.__name__ = "FakeTask"

    captured: Dict[str, Any] = {}

    def _capture_submit(coro, task_name=None, **_):
        captured["coro"] = coro
        return MagicMock()

    fake_complete = AsyncMock()
    fake_fail = AsyncMock()

    with (
        patch("dynastore.tools.protocol_helpers.resolve", return_value=fake_tasks_mgr),
        patch(
            "dynastore.modules.tasks.runners.get_task_instance",
            return_value=fake_task_instance,
        ),
        patch(
            "dynastore.modules.tasks.runners.get_background_executor",
        ) as fake_exec,
        patch("dynastore.modules.tasks.tasks_module.complete_task", fake_complete),
        patch("dynastore.modules.tasks.tasks_module.fail_task", fake_fail),
    ):
        fake_exec.return_value.submit = _capture_submit
        await runner.run(ctx)
        await captured["coro"]

    fake_fail.assert_awaited_once()
    call = fake_fail.await_args
    # Positional args: (engine, task_id, timestamp, error_message)
    assert call.args[1] == claimed_id
    assert "GCS bucket create failed" in call.args[3]
    # Generic exception → retry=True (delegates retry policy to fail_task)
    assert call.kwargs.get("retry") is True
    fake_complete.assert_not_called()


@pytest.mark.asyncio
async def test_background_runner_claimed_permanent_failure_no_retry():
    """``PermanentTaskFailure`` must call ``fail_task`` with
    ``retry=False`` — no backoff, go straight to FAILED."""
    from dynastore.modules.tasks.runners import BackgroundRunner
    from dynastore.modules.tasks.models import PermanentTaskFailure

    claimed_id = _uuid.uuid4()
    claimed_ts = datetime(2026, 4, 22, 12, 0, 0, tzinfo=timezone.utc)

    ctx = _make_context({
        "task_id": str(claimed_id),
        "task_timestamp": claimed_ts,
    })

    runner = BackgroundRunner()

    fake_tasks_mgr = MagicMock()
    fake_tasks_mgr.create_task = AsyncMock()

    fake_task_instance = MagicMock()
    fake_task_instance.run = AsyncMock(
        side_effect=PermanentTaskFailure("GCP unavailable"),
    )
    fake_task_instance.__class__.__name__ = "FakeTask"

    captured: Dict[str, Any] = {}

    def _capture_submit(coro, task_name=None, **_):
        captured["coro"] = coro
        return MagicMock()

    fake_fail = AsyncMock()

    with (
        patch("dynastore.tools.protocol_helpers.resolve", return_value=fake_tasks_mgr),
        patch(
            "dynastore.modules.tasks.runners.get_task_instance",
            return_value=fake_task_instance,
        ),
        patch(
            "dynastore.modules.tasks.runners.get_background_executor",
        ) as fake_exec,
        patch("dynastore.modules.tasks.tasks_module.fail_task", fake_fail),
    ):
        fake_exec.return_value.submit = _capture_submit
        await runner.run(ctx)
        await captured["coro"]

    fake_fail.assert_awaited_once()
    call = fake_fail.await_args
    assert call.args[1] == claimed_id
    assert call.kwargs.get("retry") is False, (
        "PermanentTaskFailure must NOT retry — goes straight to FAILED."
    )


@pytest.mark.asyncio
async def test_background_runner_claimed_re_registers_heartbeat():
    """The background coroutine must re-register on the heartbeat
    handle passed via ``extra_context`` so ``locked_until`` keeps
    getting extended while the task runs (post-dispatcher-handoff)."""
    from dynastore.modules.tasks.runners import BackgroundRunner

    claimed_id = _uuid.uuid4()
    claimed_ts = datetime(2026, 4, 22, 12, 0, 0, tzinfo=timezone.utc)

    fake_heartbeat = MagicMock()
    fake_heartbeat.register = AsyncMock()
    fake_heartbeat.unregister = AsyncMock()

    ctx = _make_context({
        "task_id": str(claimed_id),
        "task_timestamp": claimed_ts,
        "heartbeat": fake_heartbeat,
    })

    runner = BackgroundRunner()

    fake_tasks_mgr = MagicMock()
    fake_tasks_mgr.update_task = AsyncMock()

    fake_task_instance = MagicMock()
    fake_task_instance.run = AsyncMock(return_value={})
    fake_task_instance.__class__.__name__ = "FakeTask"

    captured: Dict[str, Any] = {}

    def _capture_submit(coro, task_name=None, **_):
        captured["coro"] = coro
        return MagicMock()

    with (
        patch("dynastore.tools.protocol_helpers.resolve", return_value=fake_tasks_mgr),
        patch(
            "dynastore.modules.tasks.runners.get_task_instance",
            return_value=fake_task_instance,
        ),
        patch(
            "dynastore.modules.tasks.runners.get_background_executor",
        ) as fake_exec,
    ):
        fake_exec.return_value.submit = _capture_submit
        await runner.run(ctx)
        await captured["coro"]

    fake_heartbeat.register.assert_awaited_once_with(str(claimed_id), claimed_ts)
    fake_heartbeat.unregister.assert_awaited_once_with(str(claimed_id))


# ---------------------------------------------------------------------------
# BackgroundRunner — claimed-path terminal writes guard on prior_owner_id
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_background_runner_claimed_success_passes_prior_owner_guard():
    """The claimed-path completion write must guard on the owner_id the
    dispatcher stamped at claim time (``extra_context['prior_owner_id']``) —
    otherwise a stale background coroutine could clobber a row the pg_cron
    reaper already reclaimed and handed to a fresh attempt."""
    from dynastore.modules.tasks.runners import BackgroundRunner

    claimed_id = _uuid.uuid4()
    claimed_ts = datetime(2026, 4, 22, 12, 0, 0, tzinfo=timezone.utc)

    ctx = _make_context({
        "task_id": str(claimed_id),
        "task_timestamp": claimed_ts,
        "prior_owner_id": "dispatcher-worker-1",
    })

    runner = BackgroundRunner()

    fake_tasks_mgr = MagicMock()
    fake_tasks_mgr.create_task = AsyncMock()

    fake_task_instance = MagicMock()
    fake_task_instance.run = AsyncMock(return_value={"bucket": "gs://x"})
    fake_task_instance.__class__.__name__ = "FakeTask"

    captured: Dict[str, Any] = {}

    def _capture_submit(coro, task_name=None, **_):
        captured["coro"] = coro
        return MagicMock()

    fake_complete = AsyncMock(return_value=True)
    fake_fail = AsyncMock()

    with (
        patch("dynastore.tools.protocol_helpers.resolve", return_value=fake_tasks_mgr),
        patch(
            "dynastore.modules.tasks.runners.get_task_instance",
            return_value=fake_task_instance,
        ),
        patch(
            "dynastore.modules.tasks.runners.get_background_executor",
        ) as fake_exec,
        patch("dynastore.modules.tasks.tasks_module.complete_task", fake_complete),
        patch("dynastore.modules.tasks.tasks_module.fail_task", fake_fail),
    ):
        fake_exec.return_value.submit = _capture_submit
        await runner.run(ctx)
        await captured["coro"]

    fake_complete.assert_awaited_once()
    call = fake_complete.await_args
    assert call.kwargs.get("owner_id") == "dispatcher-worker-1"
    fake_fail.assert_not_called()


@pytest.mark.asyncio
async def test_background_runner_claimed_lost_race_logs_warning(caplog):
    """When the guarded terminal write matches zero rows (the reaper
    reclaimed the row before this coroutine finished), the coroutine must
    log a clear warning instead of silently dropping the outcome."""
    import logging

    from dynastore.modules.tasks.runners import BackgroundRunner

    claimed_id = _uuid.uuid4()
    claimed_ts = datetime(2026, 4, 22, 12, 0, 0, tzinfo=timezone.utc)

    ctx = _make_context({
        "task_id": str(claimed_id),
        "task_timestamp": claimed_ts,
        "prior_owner_id": "dispatcher-worker-1",
    })

    runner = BackgroundRunner()

    fake_tasks_mgr = MagicMock()
    fake_tasks_mgr.create_task = AsyncMock()

    fake_task_instance = MagicMock()
    fake_task_instance.run = AsyncMock(return_value={"ok": True})
    fake_task_instance.__class__.__name__ = "FakeTask"

    captured: Dict[str, Any] = {}

    def _capture_submit(coro, task_name=None, **_):
        captured["coro"] = coro
        return MagicMock()

    # The row was reclaimed — the guarded UPDATE matches nothing.
    fake_complete = AsyncMock(return_value=False)

    caplog.set_level(logging.WARNING)
    with (
        patch("dynastore.tools.protocol_helpers.resolve", return_value=fake_tasks_mgr),
        patch(
            "dynastore.modules.tasks.runners.get_task_instance",
            return_value=fake_task_instance,
        ),
        patch(
            "dynastore.modules.tasks.runners.get_background_executor",
        ) as fake_exec,
        patch("dynastore.modules.tasks.tasks_module.complete_task", fake_complete),
    ):
        fake_exec.return_value.submit = _capture_submit
        await runner.run(ctx)
        await captured["coro"]

    assert any(
        "lost terminal-write race" in r.message
        and r.name == "dynastore.modules.tasks.runners"
        for r in caplog.records
    ), (
        f"expected a lost-race warning; got: "
        f"{[(r.levelname, r.name, r.getMessage()) for r in caplog.records]}"
    )


def test_background_runner_claimed_terminal_writes_pass_prior_owner_guard():
    """Source-level guard: every complete_task / fail_task / dead_letter_task
    call in ``BackgroundRunner._run_claimed`` must pass
    ``owner_id=context.extra_context.get("prior_owner_id")`` — the value the
    dispatcher's ``claim_batch`` stamped on the row it delegated. A mock-based
    check is brittle here (deeply nested closures, lazy imports); source
    inspection catches a future regression that drops the guard from any of
    the five terminal-write sites.
    """
    import inspect

    from dynastore.modules.tasks.runners import BackgroundRunner

    source = inspect.getsource(BackgroundRunner._run_claimed)
    lines = source.splitlines()
    call_sites = [
        i for i, ln in enumerate(lines)
        if any(
            f"await {fn}(" in ln
            for fn in ("_complete_task", "_fail_task", "_dead_letter_task")
        )
    ]
    assert len(call_sites) == 5, (
        f"expected 5 terminal-write call sites in _run_claimed, found "
        f"{len(call_sites)}: {[lines[i] for i in call_sites]}"
    )
    for idx in call_sites:
        window = "\n".join(lines[idx:idx + 6])
        assert 'owner_id=context.extra_context.get("prior_owner_id")' in window, (
            f"_run_claimed:{idx + 1}: terminal write missing the "
            f"prior_owner_id race guard. Window:\n{window}"
        )


# ---------------------------------------------------------------------------
# Reaper DDL — structural guards
# ---------------------------------------------------------------------------


def test_reaper_ddl_has_safe_skip_locked_scan():
    """``reap_stuck_tasks`` must use ``FOR UPDATE SKIP LOCKED`` so a
    concurrent heartbeat update doesn't block the reap pass."""
    from dynastore.modules.tasks.tasks_module import GLOBAL_TASKS_REAPER_DDL

    assert "FOR UPDATE SKIP LOCKED" in GLOBAL_TASKS_REAPER_DDL


def test_reaper_ddl_scans_active_with_expired_lock():
    """The reap predicate must match only ACTIVE rows with an expired
    ``locked_until`` — NOT live heartbeats.  Otherwise any long-running
    task would be killed mid-execution."""
    from dynastore.modules.tasks.tasks_module import GLOBAL_TASKS_REAPER_DDL

    assert "status = 'ACTIVE'" in GLOBAL_TASKS_REAPER_DDL
    assert "locked_until < NOW()" in GLOBAL_TASKS_REAPER_DDL


def test_reaper_ddl_handles_retry_and_dead_letter():
    """Rows at or above ``max_retries`` must go to DEAD_LETTER; others
    reset to PENDING with retry_count+1."""
    from dynastore.modules.tasks.tasks_module import GLOBAL_TASKS_REAPER_DDL

    assert "DEAD_LETTER" in GLOBAL_TASKS_REAPER_DDL
    assert "retry_count       = s.retry_count + 1" in GLOBAL_TASKS_REAPER_DDL or \
           "retry_count = s.retry_count + 1" in GLOBAL_TASKS_REAPER_DDL
    assert "'PENDING'" in GLOBAL_TASKS_REAPER_DDL


def test_reaper_ddl_notifies_dispatchers_on_reap():
    """When a row is reset to PENDING, the reaper must ``pg_notify`` so
    live dispatchers wake up immediately instead of waiting for their
    next signal_timeout."""
    from dynastore.modules.tasks.tasks_module import GLOBAL_TASKS_REAPER_DDL

    assert "pg_notify('new_task_queued'" in GLOBAL_TASKS_REAPER_DDL


# ---------------------------------------------------------------------------
# claim_for_dispatch — clears started_at on every (re-)dispatch (#2893)
# ---------------------------------------------------------------------------


def test_claim_for_dispatch_clears_started_at():
    """``claim_for_dispatch`` is the REMOTE offload handoff (GcpJobRunner's
    ``run``) — the row is still cold-starting, so its SET clause must reset
    ``started_at`` to NULL alongside taking ownership/renewing the lease.
    A retried dispatch of the same task must re-null it too, so the
    eventual ``claim_for_execution`` COALESCE stamps THIS attempt's real
    container-start time rather than a stale one from a prior attempt.
    """
    import inspect
    from dynastore.modules.tasks.tasks_module import claim_for_dispatch

    src = inspect.getsource(claim_for_dispatch)
    assert "started_at = NULL" in src
    # Ownership/lease/liveness fields must still be set alongside it.
    assert "owner_id = :owner_id" in src
    assert "locked_until = :locked_until" in src
    assert "last_heartbeat_at = NOW()" in src


