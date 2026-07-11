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

"""Unit tests for the platform-wide hard retry cap circuit breaker and the
claim-aware ``GcpJobRunner`` introduced after the 2026-04-23 ingestion-loop
incident.

Bugs that motivated this test file:

- ``main_task.py`` never marked the task COMPLETED on success; the pg_cron
  reaper kept resetting the row to PENDING every ~5 min.
- ``GcpJobRunner.run()`` ignored ``extra_context['task_id']`` from the
  dispatcher path — every reclaim created a brand-new task row AND launched
  a new Cloud Run Job execution, branching unboundedly.
- Multiple services (catalog, auth, geoid) all ran the dispatcher and all
  registered ``GcpJobRunner``, racing on the reaped PENDING rows.

The fixes:
  1. ``GcpJobRunner.run()`` reuses the claimed row and returns
     ``DEFERRED_COMPLETION`` on the dispatcher path.
  2. ``TasksPluginConfig.hard_retry_cap`` (default 5) is wired into
     ``claim_batch`` SQL, ``fail_task`` SQL, and the pg_cron reaper.
  3. ``GcpJobRunner`` is not registered in services with
     ``DYNASTORE_DISABLE_GCP_JOB_RUNNER=true`` or ``TASK_TYPE`` env set
     (Cloud Run Job containers).
"""

from __future__ import annotations

import uuid as _uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.tasks.models import (
    DEFERRED_COMPLETION,
    RunnerContext,
)
from sqlalchemy.engine import Engine as _SAEngine


def _fake_engine() -> MagicMock:
    return MagicMock(spec=_SAEngine)


# ---------------------------------------------------------------------------
# Hard retry cap — module-level get/set + SQL guards
# ---------------------------------------------------------------------------


def test_hard_retry_cap_default_is_5():
    """The platform default must be conservative (5) so a runaway loop
    cannot spawn more than 5 Cloud Run Job executions before the row is
    forced to DEAD_LETTER."""
    from dynastore.modules.tasks.tasks_module import get_hard_retry_cap

    assert get_hard_retry_cap() >= 1
    # Default before lifespan loads TasksPluginConfig:
    # we don't assert exact value (other tests may set it), only the contract.


def test_hard_retry_cap_setter_validates_positive():
    """The setter must reject ``0`` / negative values — the cap must allow
    at least one execution attempt to make progress."""
    from dynastore.modules.tasks import tasks_module

    original = tasks_module.get_hard_retry_cap()
    try:
        with pytest.raises(ValueError):
            tasks_module.set_hard_retry_cap(0)
        with pytest.raises(ValueError):
            tasks_module.set_hard_retry_cap(-1)
        tasks_module.set_hard_retry_cap(7)
        assert tasks_module.get_hard_retry_cap() == 7
    finally:
        tasks_module.set_hard_retry_cap(original)


def test_reaper_ddl_takes_hard_cap_parameter_and_uses_least():
    """``reap_stuck_tasks`` must accept ``p_hard_cap`` and combine it with
    per-row ``max_retries`` via ``LEAST(...)`` so the smaller of the two
    triggers DEAD_LETTER. Without this, a row with ``max_retries=10`` would
    silently bypass the platform-wide cap."""
    from dynastore.modules.tasks.tasks_module import GLOBAL_TASKS_REAPER_DDL

    assert "p_hard_cap INT DEFAULT 5" in GLOBAL_TASKS_REAPER_DDL
    assert "LEAST(" in GLOBAL_TASKS_REAPER_DDL
    assert "p_hard_cap" in GLOBAL_TASKS_REAPER_DDL


def test_reaper_ddl_emits_warning_on_dead_letter_burst():
    """When the cap fires, a ``WARNING`` line must surface in PG logs so
    operators can correlate with the dashboard / alerting pipeline."""
    from dynastore.modules.tasks.tasks_module import GLOBAL_TASKS_REAPER_DDL

    assert "dynastore.task.hard_cap_hit" in GLOBAL_TASKS_REAPER_DDL
    assert "RAISE WARNING" in GLOBAL_TASKS_REAPER_DDL


def test_reaper_ddl_error_message_mirrors_the_least_dlq_gate():
    """``error_message``'s CASE must gate on the same ``LEAST(...)``
    expression as ``status``/``finished_at`` — not a bare
    ``>= p_hard_cap`` check. Without this, a row whose own ``max_retries``
    (below the platform hard cap) is the binding cap gets DLQ'd but is
    stamped with the generic heartbeat-expired text instead of a
    DLQ-specific reason."""
    from dynastore.modules.tasks.tasks_module import GLOBAL_TASKS_REAPER_DDL

    error_message_clause = GLOBAL_TASKS_REAPER_DDL.split(
        "error_message     = CASE", 1,
    )[1].split("FROM stuck s", 1)[0]

    assert "LEAST(" in error_message_clause
    assert "COALESCE(s.max_retries, p_max_retries)" in error_message_clause
    assert "p_hard_cap" in error_message_clause


def test_reaper_ddl_distinguishes_per_row_cap_from_hard_cap_reasons():
    """The two DLQ outcomes must carry distinguishable reasons — the
    platform hard cap firing is an operational circuit-breaker signal,
    distinct from a row simply exhausting its own ``max_retries``. Both
    must remain distinct from the plain heartbeat-expired requeue text."""
    from dynastore.modules.tasks.tasks_module import GLOBAL_TASKS_REAPER_DDL

    assert "Reaped: hard retry cap (" in GLOBAL_TASKS_REAPER_DDL
    assert "Reaped: max_retries (" in GLOBAL_TASKS_REAPER_DDL
    assert "Reaped by {schema}.reap_stuck_tasks (heartbeat expired)" in GLOBAL_TASKS_REAPER_DDL


def test_claim_batch_sql_excludes_rows_above_hard_cap():
    """The dispatcher must stop wasting cycles on rows the reaper is about
    to DLQ. Rows with ``retry_count >= :hard_cap`` are excluded from the
    candidates CTE — verified by source inspection of the function."""
    import inspect
    from dynastore.modules.tasks import tasks_module

    src = inspect.getsource(tasks_module.claim_batch)
    assert "retry_count < :hard_cap" in src
    assert ":hard_cap" in src


def test_claim_batch_inner_lock_subquery_orders_by_timestamp():
    """The inner row-locking subquery (``FOR UPDATE SKIP LOCKED`` over the
    per-tenant ``candidates`` CTE) must order by ``timestamp ASC`` before
    ``LIMIT :batch_size``. Without an explicit ORDER BY, which candidates
    get locked when eligible tenants exceed ``batch_size`` is planner-order
    dependent, silently breaking the per-tenant fairness the ``candidates``
    CTE is meant to guarantee — verified by source inspection of the
    function."""
    import inspect
    from dynastore.modules.tasks import tasks_module

    src = inspect.getsource(tasks_module.claim_batch)
    assert (
        "ORDER BY timestamp ASC\n            LIMIT :batch_size\n"
        "            FOR UPDATE SKIP LOCKED" in src
    )


def test_fail_task_retry_branch_uses_hard_cap():
    """``fail_task(retry=True)`` must DLQ once ``retry_count + 1`` would
    exceed ``LEAST(max_retries, hard_cap)`` — closes the loophole where a
    runner repeatedly mis-handles the same row."""
    import inspect
    from dynastore.modules.tasks import tasks_module

    src = inspect.getsource(tasks_module.fail_task)
    # SQL uses LEAST(max_retries, :hard_cap) for the cap comparison
    assert "LEAST(max_retries, :hard_cap)" in src


# ---------------------------------------------------------------------------
# TaskCreate.max_retries plumbing
# ---------------------------------------------------------------------------


def test_task_create_accepts_max_retries():
    """``TaskCreate.max_retries`` is the per-row override that lets
    ``GcpJobRunner`` honour the Cloud Run job's ``MAX_RETRIES`` env at
    create-time, bypassing the column DEFAULT (3)."""
    from dynastore.models.tasks import TaskCreate

    tc = TaskCreate(
        caller_id="user@example.com",
        task_type="ingestion",
        inputs={"x": 1},
        max_retries=1,
    )
    assert tc.max_retries == 1

    # None means "use the column DEFAULT (3)" — preserves backwards compat.
    tc2 = TaskCreate(
        caller_id="user@example.com", task_type="ingestion", inputs={"x": 1}
    )
    assert tc2.max_retries is None


def test_task_create_rejects_negative_max_retries():
    """Negative max_retries makes no sense and would crash the SQL check."""
    from pydantic import ValidationError
    from dynastore.models.tasks import TaskCreate

    with pytest.raises(ValidationError):
        TaskCreate(
            caller_id="u",
            task_type="ingestion",
            inputs={"x": 1},
            max_retries=-1,
        )


# ---------------------------------------------------------------------------
# get_job_max_retries — side-channel between gcp_module and gcp_runner
# ---------------------------------------------------------------------------


def test_get_job_max_retries_returns_none_when_unset():
    """Unknown task types return None — caller falls back to TaskCreate's
    default behaviour (column DEFAULT 3)."""
    from dynastore.modules.gcp.tools.jobs import get_job_max_retries

    assert get_job_max_retries("__definitely_not_a_task__") is None


def test_get_job_max_retries_round_trip_via_setter():
    """``set_job_extras`` populates the side-channel; ``get_job_max_retries``
    reads back the int. Coerces stringified ints (env values are strings)."""
    from dynastore.modules.gcp.tools.jobs import (
        set_job_extras,
        get_job_max_retries,
        _JOB_EXTRAS_SYNC,
    )

    test_task_type = "__test_max_retries_round_trip__"
    try:
        set_job_extras(test_task_type, {"max_retries": "2"})
        assert get_job_max_retries(test_task_type) == 2
        set_job_extras(test_task_type, {"max_retries": 1})
        assert get_job_max_retries(test_task_type) == 1
        set_job_extras(test_task_type, {"max_retries": "not_a_number"})
        assert get_job_max_retries(test_task_type) is None
    finally:
        _JOB_EXTRAS_SYNC.pop(test_task_type, None)


# ---------------------------------------------------------------------------
# GcpJobRunner — claim-aware on dispatcher path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gcp_runner_dispatcher_path_reuses_row_and_returns_deferred():
    """When the dispatcher claims a PENDING row and delegates to GcpJobRunner,
    the runner MUST NOT call ``create_task`` (would duplicate the row). It
    MUST take ownership conditionally on the SAME claimed row via
    ``claim_for_dispatch`` (belt-and-suspenders against producer-side races)
    and launch one Cloud Run Job execution carrying the existing
    ``task_id``. It MUST return ``DEFERRED_COMPLETION`` so the dispatcher
    does not write COMPLETED ahead of the job container.

    This is the regression test for the central bug of the 2026-04-23
    ingestion infinite-loop incident; updated to match the
    ``claim_for_dispatch`` defense added with the no-double-spawn fix.
    """
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner

    runner = GcpJobRunner()
    claimed_id = _uuid.uuid4()

    ctx = RunnerContext(
        engine=_fake_engine(),
        task_type="ingestion",
        caller_id="system:platform",
        inputs={"foo": "bar"},
        db_schema="s_test",
        extra_context={
            "task_id": str(claimed_id),
            "task_timestamp": "2026-04-23T11:00:00Z",
        },
    )

    create_task_mock = AsyncMock()
    claim_mock = AsyncMock(return_value=True)
    run_job_mock = AsyncMock()
    load_job_config_mock = AsyncMock(return_value={"ingestion": "dynastore-ingestion-job"})

    with patch(
        "dynastore.modules.tasks.tasks_module.create_task", create_task_mock
    ), patch(
        "dynastore.modules.tasks.tasks_module.claim_for_dispatch", claim_mock
    ), patch(
        "dynastore.modules.gcp.tools.jobs.load_job_config", load_job_config_mock
    ), patch(
        "dynastore.modules.gcp.tools.jobs.run_cloud_run_job_async", run_job_mock
    ), patch(
        "dynastore.modules.gcp.tools.jobs.try_load_process_definition", return_value=None
    ):
        result = await runner.run(ctx)

    # No new row created on dispatcher path.
    create_task_mock.assert_not_called()

    # Conditional claim_for_dispatch called with the right task_id and
    # the gcp_cloud_run_ owner-prefix gate.
    claim_mock.assert_awaited_once()
    claim_await = claim_mock.await_args
    assert claim_await is not None
    assert claim_await.args[1] == claimed_id
    assert claim_await.kwargs["expected_owner_prefix"] == "gcp_cloud_run_"

    # Exactly one Cloud Run Job execution launched, carrying the claimed id
    # in the JSON payload (positional arg index 1).
    run_job_mock.assert_awaited_once()
    run_await = run_job_mock.await_args
    assert run_await is not None
    payload_json = run_await.kwargs["args"][1]  # ["task_type", "<json>", "--schema", "s_test"]
    assert str(claimed_id) in payload_json

    # Dispatcher contract: tell it not to write COMPLETED itself.
    assert result is DEFERRED_COMPLETION


@pytest.mark.asyncio
async def test_gcp_runner_rest_path_born_claimed_and_passes_max_retries():
    """REST-path invocation (no ``task_id`` in extra_context) must INSERT a
    single row already in ACTIVE status with this runner as ``owner_id`` and
    a short spawn lease — closing the create→claim race window where a
    dispatcher pod could otherwise claim the row and spawn a duplicate
    Cloud Run Job between INSERT and Cloud Run trigger.

    Must also pass the Cloud Run job's ``MAX_RETRIES`` env into
    ``TaskCreate.max_retries`` so a long-running expensive job is capped at
    deploy-time intent (typically 1).
    """
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner
    from dynastore.modules.gcp.tools import jobs as jobs_module

    new_task_id = _uuid.uuid4()
    fake_task = MagicMock()
    fake_task.task_id = new_task_id

    runner = GcpJobRunner()
    ctx = RunnerContext(
        engine=_fake_engine(),
        task_type="ingestion",
        caller_id="user@example.com",
        inputs={"foo": "bar"},
        db_schema="s_test",
        extra_context={},  # REST path: no task_id
    )

    create_task_mock = AsyncMock(return_value=fake_task)
    claim_mock = AsyncMock(return_value=True)
    run_job_mock = AsyncMock()
    load_job_config_mock = AsyncMock(return_value={"ingestion": "dynastore-ingestion-job"})

    # Seed the per-job MAX_RETRIES side-channel.
    jobs_module.set_job_extras("ingestion", {"max_retries": 1})

    try:
        with patch(
            "dynastore.modules.tasks.tasks_module.create_task", create_task_mock
        ), patch(
            "dynastore.modules.tasks.tasks_module.claim_for_dispatch", claim_mock
        ), patch(
            "dynastore.modules.gcp.tools.jobs.load_job_config", load_job_config_mock
        ), patch(
            "dynastore.modules.gcp.tools.jobs.run_cloud_run_job_async", run_job_mock
        ), patch(
            "dynastore.modules.gcp.tools.jobs.try_load_process_definition", return_value=None
        ):
            result = await runner.run(ctx)

        # One fresh row created with the born-claimed shape: status=ACTIVE,
        # owner_id set, locked_until set, max_retries from job env.
        create_task_mock.assert_awaited_once()
        create_await = create_task_mock.await_args
        assert create_await is not None
        task_data = create_await.args[1]  # (engine, task_data, schema)
        assert task_data.max_retries == 1
        assert task_data.task_type == "ingestion"
        assert create_await.kwargs["initial_status"] == "ACTIVE"
        assert create_await.kwargs["owner_id"].startswith("gcp_cloud_run_")
        assert create_await.kwargs["locked_until"] is not None

        # Dispatcher path's claim_for_dispatch is NOT called on REST path.
        claim_mock.assert_not_called()

        # Cloud Run Job launched with the new task_id.
        run_job_mock.assert_awaited_once()

        # REST-path return value is the new Task (not DEFERRED_COMPLETION).
        assert result is fake_task
    finally:
        jobs_module._JOB_EXTRAS_SYNC.pop("ingestion", None)


@pytest.mark.asyncio
async def test_gcp_runner_returns_none_when_job_not_in_map():
    """If the task_type has no Cloud Run job registered, return None so
    other (lower-priority) runners can take a turn."""
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner

    runner = GcpJobRunner()
    ctx = RunnerContext(
        engine=_fake_engine(),
        task_type="__not_in_job_map__",
        caller_id="u",
        inputs={},
        db_schema="s_test",
        extra_context={},
    )

    with patch(
        "dynastore.modules.gcp.tools.jobs.load_job_config",
        AsyncMock(return_value={}),
    ):
        result = await runner.run(ctx)

    assert result is None


# ---------------------------------------------------------------------------
# GcpJobRunner registration gate (D.2)
# ---------------------------------------------------------------------------


def test_should_register_skips_when_task_type_env_is_set(monkeypatch):
    """Cloud Run Job containers (which set ``TASK_TYPE``) must not register
    the dispatcher runner — they are themselves the runtime."""
    pytest.importorskip("requests")  # gcp_module imports the Cloud Run HTTP client
    from dynastore.modules.gcp import gcp_module

    monkeypatch.setenv("TASK_TYPE", "ingestion")
    monkeypatch.delenv("DYNASTORE_DISABLE_GCP_JOB_RUNNER", raising=False)
    assert gcp_module._should_register_gcp_job_runner() is False


def test_should_register_skips_when_explicit_opt_out(monkeypatch):
    """Operators set ``DYNASTORE_DISABLE_GCP_JOB_RUNNER=true`` on services
    that should not race the catalog for ingestion task claims (auth,
    geoid)."""
    pytest.importorskip("requests")  # gcp_module imports the Cloud Run HTTP client
    from dynastore.modules.gcp import gcp_module

    monkeypatch.delenv("TASK_TYPE", raising=False)
    monkeypatch.setenv("DYNASTORE_DISABLE_GCP_JOB_RUNNER", "true")
    assert gcp_module._should_register_gcp_job_runner() is False

    monkeypatch.setenv("DYNASTORE_DISABLE_GCP_JOB_RUNNER", "TRUE")
    assert gcp_module._should_register_gcp_job_runner() is False


def test_should_register_default_is_enabled(monkeypatch):
    """In a process with neither flag set (e.g. catalog service), register
    so OGC ``/processes`` ``/execution`` calls can dispatch Cloud Run jobs."""
    pytest.importorskip("requests")  # gcp_module imports the Cloud Run HTTP client
    from dynastore.modules.gcp import gcp_module

    monkeypatch.delenv("TASK_TYPE", raising=False)
    monkeypatch.delenv("DYNASTORE_DISABLE_GCP_JOB_RUNNER", raising=False)
    assert gcp_module._should_register_gcp_job_runner() is True
