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

"""Unit tests for the GcpJobRunner no-double-spawn fix.

Reproduces the 2026-04-27 review-env observation that every ingestion
``POST /processes/.../execution`` spawned TWO Cloud Run job executions
with the same task UUID, started microseconds apart.

Root cause: REST path called ``create_task(initial_status='PENDING')`` →
trigger ``on_task_insert`` (``WHEN NEW.status = 'PENDING'``) fired
``pg_notify('new_task_queued')`` → every dispatcher pod woke and
``claim_batch`` (``FOR UPDATE SKIP LOCKED``) raced the REST handler's
follow-up ``update_task(ACTIVE)``. Whichever dispatcher won the row
spawned Cloud Run RunJob #2; the REST handler then spawned RunJob #1.

Fix verified here:

- REST path INSERTs the row already-claimed (status=ACTIVE, owner_id,
  locked_until) — trigger doesn't fire, dispatcher can't claim, only one
  RunJob is spawned per task.
- Dispatcher path uses ``claim_for_dispatch`` to take ownership only when
  the row is unowned or owned by a peer GcpJobRunner — defends against
  any future regression that re-opens the producer-side race.
- ``run_cloud_run_job_async`` is wrapped in a 3-attempt exponential
  backoff for transient errors (503, timeout, connection); permanent
  errors fail-fast.
- On retry exhaustion or permanent failure the row is released via
  ``fail_task(retry=...)`` so the platform-wide ``hard_retry_cap``
  remains the single circuit breaker.
"""

from __future__ import annotations

import asyncio
import uuid as _uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.tasks.models import DEFERRED_COMPLETION, RunnerContext
from sqlalchemy.engine import Engine as _SAEngine


def _fake_engine() -> MagicMock:
    return MagicMock(spec=_SAEngine)


# ---------------------------------------------------------------------------
# REST path — born claimed, single RunJob, no PENDING window
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rest_path_creates_active_row_with_owner_and_lease():
    """REST path MUST INSERT the row with status='ACTIVE', owner_id, and a
    locked_until lease set in one statement. The trigger ``on_task_insert``
    only fires for PENDING — so an ACTIVE-born row triggers no
    ``pg_notify`` and is invisible to ``claim_batch``. This is what
    closes the producer-side race that previously double-spawned Cloud Run.
    """
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner

    new_task = MagicMock()
    new_task.task_id = _uuid.uuid4()

    runner = GcpJobRunner()
    ctx = RunnerContext(
        engine=_fake_engine(),
        task_type="ingestion",
        caller_id="user@example.com",
        inputs={"asset_id": "aoi_oasis"},
        db_schema="s_test",
        extra_context={},
    )

    create_task_mock = AsyncMock(return_value=new_task)
    run_job_mock = AsyncMock()

    with patch(
        "dynastore.modules.tasks.tasks_module.create_task", create_task_mock
    ), patch(
        "dynastore.modules.gcp.tools.jobs.load_job_config",
        AsyncMock(return_value={"ingestion": "dynastore-ingestion-job"}),
    ), patch(
        "dynastore.modules.gcp.tools.jobs.run_cloud_run_job_async", run_job_mock
    ), patch(
        "dynastore.modules.gcp.tools.jobs.try_load_process_definition", return_value=None
    ):
        await runner.run(ctx)

    # Single create_task call — the row is INSERTed in its final ACTIVE
    # state with owner + lease.  No follow-up update_task to ACTIVE.
    create_task_mock.assert_awaited_once()
    assert create_task_mock.await_args is not None
    kw = create_task_mock.await_args.kwargs
    assert kw["initial_status"] == "ACTIVE"
    assert kw["owner_id"].startswith("gcp_cloud_run_")
    assert kw["locked_until"] is not None

    # Exactly one Cloud Run Job execution.
    run_job_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_rest_path_dedup_hit_returns_none_no_runjob():
    """``create_task`` returning None means a non-terminal task with the
    same dedup_key already exists — caller MUST NOT spawn Cloud Run
    again."""
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner

    runner = GcpJobRunner()
    ctx = RunnerContext(
        engine=_fake_engine(),
        task_type="ingestion",
        caller_id="user@example.com",
        inputs={"asset_id": "aoi_oasis"},
        db_schema="s_test",
        extra_context={"dedup_key": "asset:aoi_oasis"},
    )

    run_job_mock = AsyncMock()
    with patch(
        "dynastore.modules.tasks.tasks_module.create_task",
        AsyncMock(return_value=None),
    ), patch(
        "dynastore.modules.gcp.tools.jobs.load_job_config",
        AsyncMock(return_value={"ingestion": "dynastore-ingestion-job"}),
    ), patch(
        "dynastore.modules.gcp.tools.jobs.run_cloud_run_job_async", run_job_mock
    ):
        result = await runner.run(ctx)

    assert result is None
    run_job_mock.assert_not_called()


# ---------------------------------------------------------------------------
# Dispatcher path — conditional claim, no spawn on lost claim
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatcher_path_defers_when_rest_path_owns():
    """When claim_for_dispatch fails because a REST-path worker owns the task,
    the runner MUST return DEFERRED_COMPLETION without spawning Cloud Run,
    AND extend the task's lease so the liveness reconciler can probe it.
    """
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner

    runner = GcpJobRunner()
    task_id = _uuid.uuid4()
    ctx = RunnerContext(
        engine=_fake_engine(),
        task_type="ingestion",
        caller_id="system",
        inputs={},
        db_schema="s_test",
        extra_context={
            "task_id": str(task_id),
            "task_timestamp": "2026-04-27T12:00:00Z",
        },
    )

    fake_task = MagicMock()
    fake_task.owner_id = "gcp_cloud_run_abc123"

    run_job_mock = AsyncMock()
    heartbeat_mock = AsyncMock(return_value=True)
    with patch(
        "dynastore.modules.tasks.tasks_module.claim_for_dispatch",
        AsyncMock(return_value=False),
    ), patch(
        "dynastore.modules.tasks.tasks_module.get_task_by_id_unscoped",
        AsyncMock(return_value=fake_task),
    ), patch(
        "dynastore.modules.gcp.tools.jobs.load_job_config",
        AsyncMock(return_value={"ingestion": "dynastore-ingestion-job"}),
    ), patch(
        "dynastore.modules.gcp.tools.jobs.run_cloud_run_job_async", run_job_mock
    ), patch(
        "dynastore.modules.tasks.tasks_module.heartbeat_task_if_active",
        heartbeat_mock,
    ):
        result = await runner.run(ctx)

    assert result is DEFERRED_COMPLETION
    run_job_mock.assert_not_called()
    heartbeat_mock.assert_awaited_once()
    assert heartbeat_mock.await_args is not None
    assert heartbeat_mock.await_args.args[1] == task_id


@pytest.mark.asyncio
async def test_dispatcher_path_resets_to_pending_when_unknown_owner():
    """When claim_for_dispatch fails and the task is owned by an unknown worker,
    the runner MUST reset the task to PENDING and return None (no spawn).
    """
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner

    runner = GcpJobRunner()
    task_id = _uuid.uuid4()
    ctx = RunnerContext(
        engine=_fake_engine(),
        task_type="ingestion",
        caller_id="system",
        inputs={},
        db_schema="s_test",
        extra_context={
            "task_id": str(task_id),
            "task_timestamp": "2026-04-27T12:00:00Z",
        },
    )

    fake_task = MagicMock()
    fake_task.owner_id = "unknown-worker-123"

    run_job_mock = AsyncMock()
    reset_mock = AsyncMock()
    with patch(
        "dynastore.modules.tasks.tasks_module.claim_for_dispatch",
        AsyncMock(return_value=False),
    ), patch(
        "dynastore.modules.tasks.tasks_module.get_task_by_id_unscoped",
        AsyncMock(return_value=fake_task),
    ), patch(
        "dynastore.modules.tasks.tasks_module.reset_task_to_pending", reset_mock
    ), patch(
        "dynastore.modules.gcp.tools.jobs.load_job_config",
        AsyncMock(return_value={"ingestion": "dynastore-ingestion-job"}),
    ), patch(
        "dynastore.modules.gcp.tools.jobs.run_cloud_run_job_async", run_job_mock
    ):
        result = await runner.run(ctx)

    assert result is None
    reset_mock.assert_awaited_once()
    run_job_mock.assert_not_called()


@pytest.mark.asyncio
async def test_dispatcher_path_takes_over_predecessor_claim():
    """When the dispatcher pre-claims the row with its own ``_RUNNER_ID``
    (hostname:pid — no ``gcp_cloud_run_`` prefix) and delegates to
    GcpJobRunner, ``claim_for_dispatch`` MUST accept the predecessor as a
    legitimate hand-off via ``prior_owner_id``. Otherwise the runner
    bails with ``DEFERRED_COMPLETION`` and the Cloud Run job never fires
    — the bug that left every reindex task stuck at progress=0 forever
    (closes #217).
    """
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner

    new_task = MagicMock()
    new_task.task_id = _uuid.uuid4()

    runner = GcpJobRunner()
    ctx = RunnerContext(
        engine=_fake_engine(),
        task_type="elasticsearch_indexer",
        caller_id="u",
        inputs={},
        db_schema="s_test",
        extra_context={
            "task_id": str(_uuid.uuid4()),
            "task_timestamp": "2026-05-04T12:00:00Z",
            "prior_owner_id": "host-abc:42",
        },
    )

    claim_mock = AsyncMock(return_value=True)
    run_job_mock = AsyncMock()
    with patch(
        "dynastore.modules.tasks.tasks_module.claim_for_dispatch", claim_mock
    ), patch(
        "dynastore.modules.gcp.tools.jobs.load_job_config",
        AsyncMock(return_value={
            "elasticsearch_indexer": "dynastore-elasticsearch-indexer"
        }),
    ), patch(
        "dynastore.modules.gcp.tools.jobs.run_cloud_run_job_async", run_job_mock
    ), patch(
        "dynastore.modules.gcp.tools.jobs.try_load_process_definition", return_value=None
    ), patch(
        "dynastore.modules.gcp.tools.jobs.get_job_max_retries", return_value=3
    ):
        await runner.run(ctx)

    claim_mock.assert_awaited_once()
    assert claim_mock.await_args.kwargs["prior_owner_id"] == "host-abc:42"
    assert claim_mock.await_args.kwargs["expected_owner_prefix"] == "gcp_cloud_run_"
    run_job_mock.assert_awaited_once()


# ---------------------------------------------------------------------------
# Bounded retry around RunJob — transient errors, permanent fail-fast
# ---------------------------------------------------------------------------


class _Transient503(Exception):
    """Stand-in for google.api_core.exceptions.ServiceUnavailable matched
    by class-name only in ``_is_transient_runjob_error``."""


# Rename the class via __name__ so the matcher's class-name check fires.
_Transient503.__name__ = "ServiceUnavailable"


class _Permanent4xx(Exception):
    """Stand-in for InvalidArgument / PermissionDenied — never retried."""


_Permanent4xx.__name__ = "PermissionDenied"


@pytest.mark.asyncio
async def test_runjob_transient_503_503_200_succeeds_with_two_retries():
    """Two transient 503s followed by a 200 must succeed without releasing
    the row — exactly one logical RunJob from the caller's perspective."""
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner

    new_task = MagicMock()
    new_task.task_id = _uuid.uuid4()

    runner = GcpJobRunner()
    ctx = RunnerContext(
        engine=_fake_engine(),
        task_type="ingestion",
        caller_id="u",
        inputs={},
        db_schema="s_test",
        extra_context={},
    )

    call_count = {"n": 0}

    async def flaky(*_a, **_kw):  # noqa: ARG001 — signature must accept arbitrary args
        call_count["n"] += 1
        if call_count["n"] < 3:
            raise _Transient503("503 Service Unavailable")
        return None

    fail_task_mock = AsyncMock()

    with patch(
        "dynastore.modules.tasks.tasks_module.create_task",
        AsyncMock(return_value=new_task),
    ), patch(
        "dynastore.modules.gcp.tools.jobs.load_job_config",
        AsyncMock(return_value={"ingestion": "dynastore-ingestion-job"}),
    ), patch(
        "dynastore.modules.gcp.tools.jobs.run_cloud_run_job_async", side_effect=flaky
    ), patch(
        "dynastore.modules.gcp.tools.jobs.try_load_process_definition", return_value=None
    ), patch(
        "dynastore.modules.tasks.tasks_module.fail_task", fail_task_mock
    ), patch(
        "asyncio.sleep", AsyncMock()  # no real backoff in unit tests
    ):
        result = await runner.run(ctx)

    assert call_count["n"] == 3
    assert result is new_task
    # Success — row must NOT have been released.
    fail_task_mock.assert_not_called()


@pytest.mark.asyncio
async def test_runjob_transient_exhausted_releases_with_retry_true():
    """Three consecutive transient errors exhaust the bounded retry; the
    runner MUST release the row via ``fail_task(retry=True)`` so the
    platform-wide ``hard_retry_cap`` (not the spawner) makes the
    DLQ decision.  Re-raises the last exception to the caller."""
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner

    new_task = MagicMock()
    new_task.task_id = _uuid.uuid4()

    runner = GcpJobRunner()
    ctx = RunnerContext(
        engine=_fake_engine(),
        task_type="ingestion",
        caller_id="u",
        inputs={},
        db_schema="s_test",
        extra_context={},
    )

    fail_task_mock = AsyncMock()

    with patch(
        "dynastore.modules.tasks.tasks_module.create_task",
        AsyncMock(return_value=new_task),
    ), patch(
        "dynastore.modules.gcp.tools.jobs.load_job_config",
        AsyncMock(return_value={"ingestion": "dynastore-ingestion-job"}),
    ), patch(
        "dynastore.modules.gcp.tools.jobs.run_cloud_run_job_async",
        AsyncMock(side_effect=_Transient503("503")),
    ), patch(
        "dynastore.modules.gcp.tools.jobs.try_load_process_definition", return_value=None
    ), patch(
        "dynastore.modules.tasks.tasks_module.fail_task", fail_task_mock
    ), patch(
        "asyncio.sleep", AsyncMock()
    ):
        with pytest.raises(_Transient503):
            await runner.run(ctx)

    fail_task_mock.assert_awaited_once()
    assert fail_task_mock.await_args is not None
    assert fail_task_mock.await_args.kwargs["retry"] is True


@pytest.mark.asyncio
async def test_runjob_permanent_error_fails_fast_no_retry():
    """A permanent error (PermissionDenied, InvalidArgument…) MUST NOT
    consume retry attempts — fail_fast on first occurrence and release
    via ``fail_task(retry=False)``.  This avoids burning the
    ``hard_retry_cap`` budget on errors that will never resolve.
    """
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner

    new_task = MagicMock()
    new_task.task_id = _uuid.uuid4()

    runner = GcpJobRunner()
    ctx = RunnerContext(
        engine=_fake_engine(),
        task_type="ingestion",
        caller_id="u",
        inputs={},
        db_schema="s_test",
        extra_context={},
    )

    fail_task_mock = AsyncMock()
    sleep_mock = AsyncMock()
    runjob_mock = AsyncMock(side_effect=_Permanent4xx("403"))

    with patch(
        "dynastore.modules.tasks.tasks_module.create_task",
        AsyncMock(return_value=new_task),
    ), patch(
        "dynastore.modules.gcp.tools.jobs.load_job_config",
        AsyncMock(return_value={"ingestion": "dynastore-ingestion-job"}),
    ), patch(
        "dynastore.modules.gcp.tools.jobs.run_cloud_run_job_async", runjob_mock
    ), patch(
        "dynastore.modules.gcp.tools.jobs.try_load_process_definition", return_value=None
    ), patch(
        "dynastore.modules.tasks.tasks_module.fail_task", fail_task_mock
    ), patch(
        "asyncio.sleep", sleep_mock
    ):
        with pytest.raises(_Permanent4xx):
            await runner.run(ctx)

    # Exactly one attempt — no retry budget burned.
    runjob_mock.assert_awaited_once()
    sleep_mock.assert_not_awaited()
    fail_task_mock.assert_awaited_once()
    assert fail_task_mock.await_args is not None
    assert fail_task_mock.await_args.kwargs["retry"] is False


# ---------------------------------------------------------------------------
# Transient classifier — unit-level coverage
# ---------------------------------------------------------------------------


def test_is_transient_runjob_error_recognises_known_classes():
    from dynastore.modules.gcp.gcp_runner import _is_transient_runjob_error

    class _ServiceUnavailable(Exception):
        pass
    _ServiceUnavailable.__name__ = "ServiceUnavailable"

    class _DeadlineExceeded(Exception):
        pass
    _DeadlineExceeded.__name__ = "DeadlineExceeded"

    assert _is_transient_runjob_error(_ServiceUnavailable())
    assert _is_transient_runjob_error(_DeadlineExceeded())
    assert _is_transient_runjob_error(ConnectionError("net"))
    assert _is_transient_runjob_error(TimeoutError("slow"))
    assert _is_transient_runjob_error(asyncio.TimeoutError())


def test_is_transient_runjob_error_treats_unknown_as_permanent():
    """Conservative default: unknown exception classes are NOT retried.
    Avoids hammering Cloud Run with repeated 4xx-style errors that will
    never resolve.
    """
    from dynastore.modules.gcp.gcp_runner import _is_transient_runjob_error

    class _SomeRandom(Exception):
        pass

    assert not _is_transient_runjob_error(_SomeRandom("???"))
    assert not _is_transient_runjob_error(ValueError("bad input"))
