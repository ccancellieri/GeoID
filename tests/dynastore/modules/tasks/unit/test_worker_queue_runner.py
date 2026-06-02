"""Unit tests for :class:`WorkerQueueRunner`.

``WorkerQueueRunner`` is the local/on-prem analogue of ``GcpJobRunner``: an
ASYNCHRONOUS runner that hands async work to an *external executor* (the worker
service) by enqueuing a ``PENDING`` row onto the global task queue, instead of
launching a Cloud Run Job. It is the fallback that makes asset-scoped OGC
``gdal`` process execution work on the dev stack (where nothing else can
``can_handle`` gdal) rather than returning 501.

This suite asserts the contract:

- ``can_handle`` gating: True only when (a) no in-process task instance exists
  here AND (b) the task type is routed to a worker via task routing
  (``TaskRoutingConfig``).
- REST-path invocation (no ``task_id`` in extra_context) enqueues exactly one
  ``PENDING`` row via ``create_task`` and returns the ``Task``.
- Dispatcher-path invocation (``task_id`` present) refuses to re-enqueue.
- Dedup hit (``create_task`` -> None) returns None.
- Priority ranks below every in-process runner and below ``GcpJobRunner``.
- The worker-routed snapshot refreshes from task routing.
"""

from __future__ import annotations

import uuid as _uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlalchemy.engine import Engine as _SAEngine

from dynastore.modules.tasks.models import RunnerContext


def _fake_engine() -> MagicMock:
    """MagicMock that passes pydantic's ``is_instance[Engine]`` check on
    ``RunnerContext.engine``. Tests don't touch the DB."""
    return MagicMock(spec=_SAEngine)


def _make_context(
    extra_context: Optional[Dict[str, Any]] = None,
    *,
    task_type: str = "gdal",
    dedup_key: Optional[str] = None,
) -> RunnerContext:
    return RunnerContext(
        engine=_fake_engine(),
        task_type=task_type,
        caller_id="system",
        inputs={"inputs": {"asset_id": "a1"}},
        db_schema="s_cat",
        extra_context=extra_context or {},
        dedup_key=dedup_key,
    )


# ---------------------------------------------------------------------------
# can_handle gating
# ---------------------------------------------------------------------------


def test_can_handle_false_when_in_process_instance_exists():
    """If this process can run the task in-process, the fallback runner must
    defer to the higher-priority in-process runners (returns False) — this is
    what makes the worker itself NOT re-enqueue gdal."""
    from dynastore.modules.tasks.runners import WorkerQueueRunner

    runner = WorkerQueueRunner()
    with (
        patch(
            "dynastore.modules.tasks.runners.get_task_instance",
            return_value=MagicMock(),  # instance present
        ),
        patch(
            "dynastore.modules.tasks.runners.get_worker_routed_types_sync",
            return_value={"gdal"},
        ),
    ):
        assert runner.can_handle("gdal") is False


def test_can_handle_true_when_no_instance_and_routed_to_worker():
    """No in-process instance + routed to a worker -> this runner is the
    capable fallback (returns True). This is the dev/on-prem catalog service."""
    from dynastore.modules.tasks.runners import WorkerQueueRunner

    runner = WorkerQueueRunner()
    with (
        patch(
            "dynastore.modules.tasks.runners.get_task_instance",
            return_value=None,  # no instance here
        ),
        patch(
            "dynastore.modules.tasks.runners.get_worker_routed_types_sync",
            return_value={"gdal"},
        ),
    ):
        assert runner.can_handle("gdal") is True


def test_can_handle_false_when_not_routed():
    """No in-process instance but the type is not routed to any service -> a
    remote worker is NOT expected to claim it, so do not enqueue an
    unclaimable row (returns False)."""
    from dynastore.modules.tasks.runners import WorkerQueueRunner

    runner = WorkerQueueRunner()
    with (
        patch(
            "dynastore.modules.tasks.runners.get_task_instance",
            return_value=None,
        ),
        patch(
            "dynastore.modules.tasks.runners.get_worker_routed_types_sync",
            return_value=set(),  # nothing routed
        ),
    ):
        assert runner.can_handle("gdal") is False


# ---------------------------------------------------------------------------
# REST path — enqueue
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rest_path_enqueues_pending_and_returns_task():
    """REST path (no ``task_id`` in extra_context): insert exactly one PENDING
    row via ``create_task`` and return the Task so the processes extension
    responds 201 + Location."""
    from dynastore.modules.tasks.runners import WorkerQueueRunner

    runner = WorkerQueueRunner()
    ctx = _make_context()

    fake_job = MagicMock()
    fake_job.task_id = _uuid.uuid4()

    fake_tasks_mgr = MagicMock()
    fake_tasks_mgr.create_task = AsyncMock(return_value=fake_job)

    with patch(
        "dynastore.tools.protocol_helpers.resolve", return_value=fake_tasks_mgr
    ):
        result = await runner.run(ctx)

    assert result is fake_job, "REST path must return the created Task."
    assert fake_tasks_mgr.create_task.await_count == 1
    kwargs = fake_tasks_mgr.create_task.await_args.kwargs
    assert kwargs.get("initial_status") == "PENDING", (
        "Must enqueue as PENDING so the on_task_insert trigger fires and the "
        "worker dispatcher can claim the row."
    )
    assert kwargs.get("schema") == "s_cat"


@pytest.mark.asyncio
async def test_rest_path_dedup_hit_returns_none():
    """A dedup hit (``create_task`` -> None) must surface as a soft-success
    None so ExecutionEngine short-circuits without trying other runners."""
    from dynastore.modules.tasks.runners import WorkerQueueRunner

    runner = WorkerQueueRunner()
    ctx = _make_context(dedup_key="evt-1")

    fake_tasks_mgr = MagicMock()
    fake_tasks_mgr.create_task = AsyncMock(return_value=None)

    with patch(
        "dynastore.tools.protocol_helpers.resolve", return_value=fake_tasks_mgr
    ):
        result = await runner.run(ctx)

    assert result is None
    assert fake_tasks_mgr.create_task.await_count == 1


# ---------------------------------------------------------------------------
# Dispatcher path — must not re-enqueue
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatcher_path_does_not_re_enqueue():
    """Dispatcher-path invocation (``task_id`` already present) must NOT create
    another row — that would be the #726-class unbounded duplication loop.
    Returns None so the engine falls through without enqueuing."""
    from dynastore.modules.tasks.runners import WorkerQueueRunner

    runner = WorkerQueueRunner()
    ctx = _make_context({
        "task_id": str(_uuid.uuid4()),
        "task_timestamp": datetime(2026, 4, 22, 12, 0, 0, tzinfo=timezone.utc),
    })

    fake_tasks_mgr = MagicMock()
    fake_tasks_mgr.create_task = AsyncMock()

    with patch(
        "dynastore.tools.protocol_helpers.resolve", return_value=fake_tasks_mgr
    ):
        result = await runner.run(ctx)

    assert result is None
    assert fake_tasks_mgr.create_task.await_count == 0, (
        "Dispatcher path must never re-enqueue an already-claimed row."
    )


# ---------------------------------------------------------------------------
# Priority ordering
# ---------------------------------------------------------------------------


def test_priority_ranks_below_in_process_and_gcp_runners():
    """WorkerQueueRunner is the last-resort async fallback: lower priority than
    BackgroundRunner (in-process), FastAPIBackgroundRunner (in-process), and
    GcpJobRunner (Cloud Run)."""
    from dynastore.modules.tasks.runners import (
        BackgroundRunner,
        WorkerQueueRunner,
    )

    wq = WorkerQueueRunner()
    bg = BackgroundRunner()
    assert wq.priority < bg.priority
    # GcpJobRunner priority is 10 (Cloud Run external executor); we must rank
    # below it so a Cloud Run deployment still prefers Cloud Run.
    assert wq.priority < 10
    # FastAPIBackgroundRunner (in-process) is priority 50.
    assert wq.priority < 50


def test_runner_is_asynchronous_mode():
    from dynastore.modules.tasks.models import TaskExecutionMode
    from dynastore.modules.tasks.runners import WorkerQueueRunner

    assert WorkerQueueRunner().mode == TaskExecutionMode.ASYNCHRONOUS


def test_runner_does_not_require_request_context():
    """Pure enqueue — usable from REST and headless callers alike, so it must
    not be filtered out by the request-context capability gate."""
    from dynastore.modules.tasks.runners import WorkerQueueRunner

    caps = WorkerQueueRunner().capabilities
    assert getattr(caps, "requires_request_context", False) is False


# ---------------------------------------------------------------------------
# Worker-routed snapshot refresh
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_refresh_worker_routed_types_from_routing(monkeypatch):
    """``refresh_worker_routed_types`` collects task types whose routing config
    yields a target with a concrete, non-empty consumer list into the snapshot."""
    import dynastore.modules.tasks.runners as runners_mod
    from dynastore.modules.tasks.routing import resolver as routing_resolver
    from dynastore.modules.tasks.routing.model import RunnerTarget

    monkeypatch.setattr(
        runners_mod, "get_loaded_task_types", lambda: ["gdal", "ingestion", "noop"]
    )

    async def _targets(task_key):
        return {
            "gdal": [RunnerTarget(runner="worker_queue", consumers=["worker"])],
            "ingestion": [RunnerTarget(runner="worker_queue", consumers=["worker"])],
            "noop": [RunnerTarget(runner="background", consumers=[])],
        }.get(task_key, [])

    monkeypatch.setattr(routing_resolver, "resolved_targets", _targets)

    await runners_mod.refresh_worker_routed_types()

    snap = runners_mod.get_worker_routed_types_sync()
    assert "gdal" in snap
    assert "ingestion" in snap
    assert "noop" not in snap, "Empty consumer list must NOT be treated as routed."


@pytest.mark.asyncio
async def test_refresh_worker_routed_types_best_effort_on_missing_config():
    """When the config registry is unavailable the refresh must not raise; the
    snapshot is simply left unchanged (can_handle then gates on the in-process
    instance only)."""
    import dynastore.modules.tasks.runners as runners_mod

    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=None
    ):
        # Must not raise.
        await runners_mod.refresh_worker_routed_types()
