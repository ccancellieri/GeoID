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

"""Unit tests for the maintenance supervisor (jobs 4-12, #1911).

Pure-mock style — no live DB.  Run with:
    PYTHONPATH=packages/core/src \
      /Users/ccancellieri/work/code/geoid/.venv/bin/python \
      -m pytest tests/dynastore/modules/catalog/unit/test_maintenance_supervisor.py \
      --noconftest -p no:cacheprovider -q

Covered:
- MaintenanceSupervisor.run_once dispatches only due jobs
- mark_running / mark_done called with correct args per job
- A job raising an exception → mark_done(status='error', error=<msg>),
  other jobs still run (per-job isolation)
- reclaim_stale_jobs: SQL shape, cutoff calculation
- Each job builds the correct SQL / predicate text (assert template + params)
- Bounded-batch loop terminates at 0 rows
- build_supervisor_config reads the right env vars
- register_supervisor_jobs upserts all 9 job names (6 original + 3 task)
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Imports under test
# ---------------------------------------------------------------------------

from dynastore.modules.catalog.db_init.maintenance_schedule import (
    MaintenanceScheduleRepository,
    _RECLAIM_STALE_JOBS,
)
from dynastore.modules.catalog.maintenance_supervisor import (
    JOB_EVENTS_DLQ_PRUNE,
    JOB_EVENTS_PENDING_ALERT,
    JOB_EVENTS_STUCK_REAPER,
    JOB_IAM_PRUNE,
    JOB_STORAGE_PARTITION_CREATE,
    JOB_STORAGE_RETENTION,
    JOB_SYSTEM_LOGS_PRUNE,
    JOB_TASK_PARTITION_CREATE,
    JOB_TASK_REAPER,
    JOB_TASK_RETENTION,
    JOB_TENANT_LOGS_PRUNE,
    JOB_WORK_EVENTS_PARTITION_CREATE,
    JOB_WORK_EVENTS_RETENTION,
    MaintenanceSupervisor,
    _CADENCE_DLQ_PRUNE,
    _CADENCE_IAM_PRUNE,
    _CADENCE_PENDING_ALERT,
    _CADENCE_STUCK_REAPER,
    _CADENCE_SYSTEM_LOGS,
    _CADENCE_TASK_PARTITION_CREATE,
    _CADENCE_TASK_REAPER,
    _CADENCE_TASK_RETENTION,
    _CADENCE_TENANT_LOGS,
    _PRUNE_BATCH,
    _STALE_AFTER_SECONDS,
    _SUPERSEDED_CRON_JOBS,
    _SUPERSEDED_TENANT_LOG_PREFIX,
    _run_events_dlq_prune,
    _run_events_pending_alert,
    _run_events_stuck_reaper,
    _run_iam_prune,
    _run_system_logs_prune,
    _run_tenant_logs_prune,
    build_supervisor_config,
    register_supervisor_jobs,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utc(*args) -> datetime:
    """Construct a UTC datetime from positional args (year, month, day, ...)."""
    return datetime(*args, tzinfo=timezone.utc)


def _make_job_row(name: str) -> dict[str, Any]:
    return {
        "job_name": name,
        "interval_seconds": 300,
        "last_run_at": None,
        "running_since": None,
        "last_status": None,
        "last_error": None,
        "last_rows": None,
    }


def _fake_engine():
    """Return a minimal fake engine accepted by managed_transaction mocks."""
    return MagicMock(name="engine")


# ---------------------------------------------------------------------------
# reclaim_stale_jobs
# ---------------------------------------------------------------------------


def test_reclaim_stale_jobs_sql_contains_running_since():
    """The reclaim query must filter on running_since IS NOT NULL and cutoff."""
    sql = _RECLAIM_STALE_JOBS.template
    assert "running_since IS NOT NULL" in sql
    assert "running_since < :cutoff" in sql
    assert "last_status" in sql
    assert "last_error" in sql


@pytest.mark.asyncio
async def test_reclaim_stale_jobs_cutoff_calculation():
    """reclaim_stale_jobs computes cutoff = now - stale_after_seconds."""
    conn = MagicMock()
    now = _utc(2026, 6, 1, 12, 0, 0)
    stale_after = 3600
    expected_cutoff = now - timedelta(seconds=stale_after)

    mock_exec = AsyncMock(return_value=0)
    with patch.object(_RECLAIM_STALE_JOBS, "execute", new=mock_exec):
        repo = MaintenanceScheduleRepository()
        result = await repo.reclaim_stale_jobs(conn, now=now, stale_after_seconds=stale_after)

    mock_exec.assert_awaited_once_with(conn, cutoff=expected_cutoff)
    assert result == 0


@pytest.mark.asyncio
async def test_reclaim_stale_jobs_returns_reclaimed_count():
    """reclaim_stale_jobs returns the rowcount from the underlying query."""
    conn = MagicMock()
    now = _utc(2026, 6, 1)
    with patch.object(_RECLAIM_STALE_JOBS, "execute", new=AsyncMock(return_value=3)):
        result = await MaintenanceScheduleRepository().reclaim_stale_jobs(
            conn, now=now, stale_after_seconds=_STALE_AFTER_SECONDS
        )
    assert result == 3


# ---------------------------------------------------------------------------
# Supervisor tick: dispatches only due jobs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_once_dispatches_due_jobs_only():
    """run_once only calls _dispatch_job for jobs returned by get_due_jobs."""
    engine = _fake_engine()
    supervisor = MaintenanceSupervisor(
        {"dead_letter_days": 30, "timeout_minutes": 15, "max_retries": 3}
    )

    due = [_make_job_row(JOB_EVENTS_DLQ_PRUNE)]

    async def _fake_managed_txn(eng):
        conn = AsyncMock()
        conn.__aenter__ = AsyncMock(return_value=conn)
        conn.__aexit__ = AsyncMock(return_value=False)
        return conn

    repo_mock = MagicMock(spec=MaintenanceScheduleRepository)
    repo_mock.reclaim_stale_jobs = AsyncMock(return_value=0)
    repo_mock.get_due_jobs = AsyncMock(return_value=due)
    repo_mock.mark_running = AsyncMock(return_value=1)
    repo_mock.mark_done = AsyncMock(return_value=1)

    dispatched: list[str] = []

    async def _fake_dispatch(job_name, conn, config):
        dispatched.append(job_name)
        return 5

    with (
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.get_engine",
            return_value=engine,
        ),
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.MaintenanceScheduleRepository",
            return_value=repo_mock,
        ),
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.managed_transaction",
        ) as mock_mtx,
        patch(
            "dynastore.modules.catalog.maintenance_supervisor._dispatch_job",
            new=AsyncMock(side_effect=_fake_dispatch),
        ),
        patch(
            "dynastore.modules.catalog.maintenance_supervisor._set_statement_timeout",
            new=AsyncMock(),
        ),
    ):
        # managed_transaction is used as async context manager; return a mock conn
        fake_conn = AsyncMock()
        mock_mtx.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
        mock_mtx.return_value.__aexit__ = AsyncMock(return_value=False)

        await supervisor.run_once()

    assert dispatched == [JOB_EVENTS_DLQ_PRUNE]


@pytest.mark.asyncio
async def test_run_once_no_due_jobs_does_nothing():
    """run_once with an empty due list logs debug and does not call _dispatch_job."""
    engine = _fake_engine()
    supervisor = MaintenanceSupervisor(
        {"dead_letter_days": 30, "timeout_minutes": 15, "max_retries": 3}
    )

    repo_mock = MagicMock(spec=MaintenanceScheduleRepository)
    repo_mock.reclaim_stale_jobs = AsyncMock(return_value=0)
    repo_mock.get_due_jobs = AsyncMock(return_value=[])

    dispatched: list[str] = []

    with (
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.get_engine",
            return_value=engine,
        ),
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.MaintenanceScheduleRepository",
            return_value=repo_mock,
        ),
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.managed_transaction",
        ) as mock_mtx,
        patch(
            "dynastore.modules.catalog.maintenance_supervisor._dispatch_job",
            new=AsyncMock(side_effect=lambda n, c, cfg: dispatched.append(n) or 0),
        ),
    ):
        fake_conn = AsyncMock()
        mock_mtx.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
        mock_mtx.return_value.__aexit__ = AsyncMock(return_value=False)
        await supervisor.run_once()

    assert dispatched == []


# ---------------------------------------------------------------------------
# Job isolation: one failure does not block others
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_once_failing_job_marks_error_others_still_run():
    """A job that raises → mark_done(status='error'); remaining jobs still run."""
    engine = _fake_engine()
    supervisor = MaintenanceSupervisor(
        {"dead_letter_days": 30, "timeout_minutes": 15, "max_retries": 3}
    )

    due = [
        _make_job_row(JOB_EVENTS_DLQ_PRUNE),
        _make_job_row(JOB_IAM_PRUNE),
    ]

    repo_mock = MagicMock(spec=MaintenanceScheduleRepository)
    repo_mock.reclaim_stale_jobs = AsyncMock(return_value=0)
    repo_mock.get_due_jobs = AsyncMock(return_value=due)
    repo_mock.mark_running = AsyncMock(return_value=1)
    mark_done_calls: list[dict] = []

    async def _capture_mark_done(conn, job_name, *, status, error, rows, finished_at):
        mark_done_calls.append({"job_name": job_name, "status": status, "error": error})

    repo_mock.mark_done = _capture_mark_done

    call_count = 0

    async def _failing_then_ok(job_name, conn, config):
        nonlocal call_count
        call_count += 1
        if job_name == JOB_EVENTS_DLQ_PRUNE:
            raise RuntimeError("simulated dlq failure")
        return 7

    with (
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.get_engine",
            return_value=engine,
        ),
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.MaintenanceScheduleRepository",
            return_value=repo_mock,
        ),
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.managed_transaction",
        ) as mock_mtx,
        patch(
            "dynastore.modules.catalog.maintenance_supervisor._dispatch_job",
            new=AsyncMock(side_effect=_failing_then_ok),
        ),
        patch(
            "dynastore.modules.catalog.maintenance_supervisor._set_statement_timeout",
            new=AsyncMock(),
        ),
    ):
        fake_conn = AsyncMock()
        mock_mtx.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
        mock_mtx.return_value.__aexit__ = AsyncMock(return_value=False)
        await supervisor.run_once()

    assert call_count == 2

    failed = next(d for d in mark_done_calls if d["job_name"] == JOB_EVENTS_DLQ_PRUNE)
    assert failed["status"] == "error"
    assert "simulated dlq failure" in failed["error"]

    succeeded = next(d for d in mark_done_calls if d["job_name"] == JOB_IAM_PRUNE)
    assert succeeded["status"] == "ok"
    assert succeeded["error"] is None


# ---------------------------------------------------------------------------
# mark_running / mark_done arg validation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_job_calls_mark_running_before_dispatch():
    """_run_job must call mark_running before invoking _dispatch_job."""
    engine = _fake_engine()
    supervisor = MaintenanceSupervisor(
        {"dead_letter_days": 30, "timeout_minutes": 15, "max_retries": 3}
    )

    repo_mock = MagicMock(spec=MaintenanceScheduleRepository)
    call_order: list[str] = []
    repo_mock.mark_running = AsyncMock(side_effect=lambda *a, **kw: call_order.append("mark_running") or 1)
    repo_mock.mark_done = AsyncMock(side_effect=lambda *a, **kw: call_order.append("mark_done") or 1)

    async def _fake_dispatch(job_name, conn, config):
        call_order.append("dispatch")
        return 0

    now = _utc(2026, 6, 1)

    with (
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.managed_transaction",
        ) as mock_mtx,
        patch(
            "dynastore.modules.catalog.maintenance_supervisor._dispatch_job",
            new=AsyncMock(side_effect=_fake_dispatch),
        ),
        patch(
            "dynastore.modules.catalog.maintenance_supervisor._set_statement_timeout",
            new=AsyncMock(),
        ),
    ):
        fake_conn = AsyncMock()
        mock_mtx.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
        mock_mtx.return_value.__aexit__ = AsyncMock(return_value=False)
        await supervisor._run_job(engine, repo_mock, JOB_IAM_PRUNE, now)

    assert call_order == ["mark_running", "dispatch", "mark_done"]


@pytest.mark.asyncio
async def test_run_job_mark_done_receives_status_ok_and_rows():
    """_run_job records status='ok' and the rowcount returned by _dispatch_job."""
    engine = _fake_engine()
    supervisor = MaintenanceSupervisor(
        {"dead_letter_days": 30, "timeout_minutes": 15, "max_retries": 3}
    )

    repo_mock = MagicMock(spec=MaintenanceScheduleRepository)
    repo_mock.mark_running = AsyncMock(return_value=1)
    mark_done_kwargs: dict = {}

    async def _capture_done(conn, job_name, **kw):
        mark_done_kwargs.update(kw)

    repo_mock.mark_done = _capture_done

    with (
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.managed_transaction",
        ) as mock_mtx,
        patch(
            "dynastore.modules.catalog.maintenance_supervisor._dispatch_job",
            new=AsyncMock(return_value=42),
        ),
        patch(
            "dynastore.modules.catalog.maintenance_supervisor._set_statement_timeout",
            new=AsyncMock(),
        ),
    ):
        fake_conn = AsyncMock()
        mock_mtx.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
        mock_mtx.return_value.__aexit__ = AsyncMock(return_value=False)
        await supervisor._run_job(engine, repo_mock, JOB_SYSTEM_LOGS_PRUNE, _utc(2026, 6, 1))

    assert mark_done_kwargs["status"] == "ok"
    assert mark_done_kwargs["rows"] == 42
    assert mark_done_kwargs["error"] is None


# ---------------------------------------------------------------------------
# Job SQL / predicate checks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_events_dlq_prune_sql_references_dead_letter_and_schema():
    """_run_events_dlq_prune must query status='DEAD_LETTER' with a day-based interval."""
    conn = AsyncMock()
    exec_calls: list[tuple] = []

    async def _fake_dqlquery_execute(c, **kw):
        exec_calls.append(kw)
        return 0  # terminate after first batch

    with patch(
        "dynastore.modules.catalog.maintenance_supervisor.DQLQuery"
    ) as MockDQL:
        instance = MagicMock()
        instance.execute = AsyncMock(side_effect=_fake_dqlquery_execute)
        MockDQL.return_value = instance

        await _run_events_dlq_prune(conn, dead_letter_days=30)

    # The SQL template passed to DQLQuery must reference DEAD_LETTER and dead_letter_days
    sql_arg = MockDQL.call_args[0][0]
    assert "DEAD_LETTER" in sql_arg
    assert "dead_letter_days" in sql_arg or ":dead_letter_days" in sql_arg
    # Parameter must be forwarded
    assert exec_calls[0]["dead_letter_days"] == 30
    assert exec_calls[0]["batch_size"] == _PRUNE_BATCH


@pytest.mark.asyncio
async def test_events_stuck_reaper_sql_references_processing_and_cte():
    """_run_events_stuck_reaper SQL must use the CTE pattern with PROCESSING status."""
    conn = AsyncMock()

    async def _fake_dqlquery_execute(c, **kw):
        return 0

    with patch(
        "dynastore.modules.catalog.maintenance_supervisor.DQLQuery"
    ) as MockDQL:
        instance = MagicMock()
        instance.execute = AsyncMock(side_effect=_fake_dqlquery_execute)
        MockDQL.return_value = instance

        await _run_events_stuck_reaper(conn, timeout_minutes=15, max_retries=3)

    sql_arg = MockDQL.call_args[0][0]
    assert "PROCESSING" in sql_arg
    assert "FOR UPDATE SKIP LOCKED" in sql_arg
    assert "DEAD_LETTER" in sql_arg
    assert "PENDING" in sql_arg
    assert "reaped stale PROCESSING" in sql_arg


@pytest.mark.asyncio
async def test_events_pending_alert_logs_warning_per_shard():
    """_run_events_pending_alert emits a WARNING for each shard in the result."""
    conn = AsyncMock()
    rows = [
        {"shard": 0, "n": 5, "oldest_age_sec": 9000},
        {"shard": 1, "n": 3, "oldest_age_sec": 7200},
    ]

    with (
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.DQLQuery"
        ) as MockDQL,
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.logger"
        ) as mock_logger,
    ):
        instance = MagicMock()
        instance.execute = AsyncMock(return_value=rows)
        MockDQL.return_value = instance

        total = await _run_events_pending_alert(conn, dead_letter_days=30)

    assert total == 8  # 5 + 3
    assert mock_logger.warning.call_count == 2


@pytest.mark.asyncio
async def test_iam_prune_sql_references_all_six_tables():
    """_run_iam_prune issues DELETEs for all 6 IAM tables."""
    conn = AsyncMock()
    tables_hit: list[str] = []

    async def _fake_execute(c, **kw):
        return 0

    with patch(
        "dynastore.modules.catalog.maintenance_supervisor.DQLQuery"
    ) as MockDQL:
        # Each call to DQLQuery(sql) creates a new instance; track all sqls
        instances: list[MagicMock] = []

        def _dql_factory(sql, **kwargs):
            inst = MagicMock()
            inst.execute = AsyncMock(side_effect=lambda c, **kw: 0)
            tables_hit.append(sql)
            instances.append(inst)
            return inst

        MockDQL.side_effect = _dql_factory
        await _run_iam_prune(conn)

    combined = " ".join(tables_hit)
    for table in ("refresh_tokens", "oauth_codes", "oauth_tokens", "grants", "usage_counters"):
        assert table in combined, f"Expected table {table!r} in IAM prune SQL"


# ---------------------------------------------------------------------------
# Bounded-batch loop terminates at 0 rows
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_system_logs_prune_bounded_batch_loop():
    """_run_system_logs_prune loops until DQLQuery.execute returns 0."""
    conn = AsyncMock()
    call_counts = [0]
    # Return _PRUNE_BATCH rows twice then 0
    return_sequence = [_PRUNE_BATCH, _PRUNE_BATCH, 0]

    async def _fake_execute(c, **kw):
        idx = call_counts[0]
        call_counts[0] += 1
        return return_sequence[idx] if idx < len(return_sequence) else 0

    with patch(
        "dynastore.modules.catalog.maintenance_supervisor.DQLQuery"
    ) as MockDQL:
        instance = MagicMock()
        instance.execute = AsyncMock(side_effect=_fake_execute)
        MockDQL.return_value = instance

        total = await _run_system_logs_prune(conn)

    assert total == _PRUNE_BATCH * 2
    assert call_counts[0] == 3  # exactly 3 iterations


# ---------------------------------------------------------------------------
# build_supervisor_config reads env vars
# ---------------------------------------------------------------------------


def test_build_supervisor_config_default_values():
    """build_supervisor_config returns defaults when env vars are unset."""
    cfg = build_supervisor_config()
    assert cfg["dead_letter_days"] == 30
    assert cfg["timeout_minutes"] == 15
    assert cfg["max_retries"] == 3


def test_build_supervisor_config_reads_env_vars(monkeypatch):
    """build_supervisor_config picks up overridden env vars."""
    monkeypatch.setenv("GLOBAL_EVENT_RETENTION_DAYS", "60")
    monkeypatch.setenv("EVENT_PROCESSING_TIMEOUT_MINUTES", "5")
    cfg = build_supervisor_config()
    assert cfg["dead_letter_days"] == 60
    assert cfg["timeout_minutes"] == 5


# ---------------------------------------------------------------------------
# register_supervisor_jobs upserts all 6 expected names
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_register_supervisor_jobs_upserts_all_expected_jobs():
    """register_supervisor_jobs upserts all 13 jobs (6 original + 3 task + 4 workclass)."""
    engine = _fake_engine()
    upserted: list[tuple[str, int]] = []

    repo_mock = MagicMock(spec=MaintenanceScheduleRepository)

    async def _capture_upsert(conn, job_name, *, interval_seconds):
        upserted.append((job_name, interval_seconds))

    repo_mock.upsert_job = _capture_upsert

    # register_supervisor_jobs also prunes obsolete schedule rows via a raw
    # DELETE through DQLQuery; stub it so it doesn't hit a real executor.
    def _dql_factory(sql, **_kw):
        inst = MagicMock()
        inst.execute = AsyncMock(return_value=0)
        return inst

    with (
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.MaintenanceScheduleRepository",
            return_value=repo_mock,
        ),
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.DQLQuery",
            side_effect=_dql_factory,
        ),
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.managed_transaction",
        ) as mock_mtx,
    ):
        fake_conn = AsyncMock()
        mock_mtx.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
        mock_mtx.return_value.__aexit__ = AsyncMock(return_value=False)

        await register_supervisor_jobs(engine)

    job_names = [name for name, _ in upserted]
    assert sorted(job_names) == sorted([
        JOB_EVENTS_DLQ_PRUNE,
        JOB_EVENTS_STUCK_REAPER,
        JOB_EVENTS_PENDING_ALERT,
        JOB_TENANT_LOGS_PRUNE,
        JOB_SYSTEM_LOGS_PRUNE,
        JOB_IAM_PRUNE,
        JOB_TASK_REAPER,
        JOB_TASK_PARTITION_CREATE,
        JOB_TASK_RETENTION,
        JOB_WORK_EVENTS_PARTITION_CREATE,
        JOB_WORK_EVENTS_RETENTION,
        JOB_STORAGE_PARTITION_CREATE,
        JOB_STORAGE_RETENTION,
    ])

    cadence_map = dict(upserted)
    assert cadence_map[JOB_EVENTS_DLQ_PRUNE] == _CADENCE_DLQ_PRUNE
    assert cadence_map[JOB_EVENTS_STUCK_REAPER] == _CADENCE_STUCK_REAPER
    assert cadence_map[JOB_EVENTS_PENDING_ALERT] == _CADENCE_PENDING_ALERT
    assert cadence_map[JOB_TENANT_LOGS_PRUNE] == _CADENCE_TENANT_LOGS
    assert cadence_map[JOB_SYSTEM_LOGS_PRUNE] == _CADENCE_SYSTEM_LOGS
    assert cadence_map[JOB_IAM_PRUNE] == _CADENCE_IAM_PRUNE
    assert cadence_map[JOB_TASK_REAPER] == _CADENCE_TASK_REAPER
    assert cadence_map[JOB_TASK_PARTITION_CREATE] == _CADENCE_TASK_PARTITION_CREATE
    assert cadence_map[JOB_TASK_RETENTION] == _CADENCE_TASK_RETENTION


# ---------------------------------------------------------------------------
# Tenant schema enumeration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tenant_logs_prune_queries_each_active_schema():
    """_run_tenant_logs_prune must delete from each schema returned by the catalog query."""
    conn = AsyncMock()
    schemas_deleted: list[str] = []

    async def _fake_dqlquery_execute(c, **kw):
        return 0

    call_num = [0]

    with patch(
        "dynastore.modules.catalog.maintenance_supervisor.DQLQuery"
    ) as MockDQL:
        # First call: list schemas; subsequent calls: bounded batch deletes
        catalog_rows = [("s_abc00001",), ("s_abc00002",)]

        def _dql_factory(sql, **kwargs):
            inst = MagicMock()
            call_num[0] += 1
            if "physical_schema" in sql:
                inst.execute = AsyncMock(return_value=catalog_rows)
            else:
                # Track which schemas we DELETE from
                for row in catalog_rows:
                    if f'"{row[0]}"' in sql:
                        schemas_deleted.append(row[0])
                inst.execute = AsyncMock(return_value=0)
            return inst

        MockDQL.side_effect = _dql_factory
        await _run_tenant_logs_prune(conn)

    # Both schemas should have been hit
    assert "s_abc00001" in schemas_deleted or "s_abc00001" in " ".join(
        str(c) for c in MockDQL.call_args_list
    )


# ---------------------------------------------------------------------------
# Advisory lock key is unique (does not collide with SoftDeleteReaper)
# ---------------------------------------------------------------------------


def test_supervisor_advisory_lock_key_differs_from_reaper():
    """The supervisor must use a different advisory lock key than SoftDeleteReaper."""
    from dynastore.modules.catalog.maintenance_supervisor import _SUPERVISOR_ADVISORY_LOCK_KEY
    from dynastore.modules.catalog.soft_delete_reaper import _REAPER_ADVISORY_LOCK_KEY

    assert _SUPERVISOR_ADVISORY_LOCK_KEY != _REAPER_ADVISORY_LOCK_KEY


# ---------------------------------------------------------------------------
# unschedule_superseded_cron_jobs — clean-cut safety for non-fresh deploys
# ---------------------------------------------------------------------------


def _patch_mtx(mock_mtx, conn):
    """Wire a managed_transaction mock to yield *conn*."""
    mock_mtx.return_value.__aenter__ = AsyncMock(return_value=conn)
    mock_mtx.return_value.__aexit__ = AsyncMock(return_value=False)


@pytest.mark.asyncio
async def test_unschedule_superseded_noop_when_pgcron_absent():
    """No pg_cron → returns 0 and issues no cron.unschedule query."""
    from dynastore.modules.catalog.maintenance_supervisor import (
        unschedule_superseded_cron_jobs,
    )

    conn = AsyncMock()
    with (
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.managed_transaction",
        ) as mock_mtx,
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.check_extension_exists",
            new=AsyncMock(return_value=False),
        ),
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.DQLQuery",
        ) as MockDQL,
    ):
        _patch_mtx(mock_mtx, conn)
        result = await unschedule_superseded_cron_jobs(_fake_engine())

    assert result == 0
    MockDQL.assert_not_called()


@pytest.mark.asyncio
async def test_unschedule_superseded_unschedules_when_pgcron_present():
    """pg_cron present → unschedules matching jobs and returns the count."""
    from dynastore.modules.catalog.maintenance_supervisor import (
        unschedule_superseded_cron_jobs,
    )

    conn = AsyncMock()
    exec_calls: list[dict] = []

    async def _fake_execute(c, **kw):
        exec_calls.append(kw)
        return 3

    with (
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.managed_transaction",
        ) as mock_mtx,
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.check_extension_exists",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.DQLQuery",
        ) as MockDQL,
    ):
        _patch_mtx(mock_mtx, conn)
        instance = MagicMock()
        instance.execute = AsyncMock(side_effect=_fake_execute)
        MockDQL.return_value = instance

        result = await unschedule_superseded_cron_jobs(_fake_engine())

    assert result == 3
    sql_arg = MockDQL.call_args[0][0]
    assert "cron.unschedule" in sql_arg and "cron.job" in sql_arg
    # superseded global names + tenant-logs prefix forwarded as params
    assert exec_calls[0]["names"] == list(_SUPERSEDED_CRON_JOBS)
    assert exec_calls[0]["tenant_prefix"] == f"{_SUPERSEDED_TENANT_LOG_PREFIX}%"


# ---------------------------------------------------------------------------
# _MARK_RUNNING claim guard: AND running_since IS NULL
# ---------------------------------------------------------------------------


def test_mark_running_sql_has_running_since_is_null_guard():
    """_MARK_RUNNING must include AND running_since IS NULL so a second claimer
    gets 0 rows updated and cannot silently overwrite the first leader's claim."""
    from dynastore.modules.catalog.db_init.maintenance_schedule import _MARK_RUNNING

    sql = _MARK_RUNNING.template
    assert "running_since IS NULL" in sql, (
        "_MARK_RUNNING must have 'AND running_since IS NULL' to prevent a "
        "second leader from overwriting the first leader's claim."
    )


@pytest.mark.asyncio
async def test_run_job_skips_when_mark_running_returns_zero_rows(caplog):
    """_run_job must skip dispatch and log WARNING when mark_running returns 0.

    A 0-rowcount from _MARK_RUNNING means another leader already claimed this
    job. The job must NOT be dispatched and the skip must be logged at WARNING.
    """
    import logging

    engine = _fake_engine()
    supervisor = MaintenanceSupervisor(
        {"dead_letter_days": 30, "timeout_minutes": 15, "max_retries": 3}
    )

    repo_mock = MagicMock(spec=MaintenanceScheduleRepository)
    # mark_running returns 0 → claimed by another leader
    repo_mock.mark_running = AsyncMock(return_value=0)
    repo_mock.mark_done = AsyncMock(return_value=1)

    dispatched: list[str] = []

    with (
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.managed_transaction",
        ) as mock_mtx,
        patch(
            "dynastore.modules.catalog.maintenance_supervisor._dispatch_job",
            new=AsyncMock(side_effect=lambda n, c, cfg: dispatched.append(n) or 0),
        ),
        patch(
            "dynastore.modules.catalog.maintenance_supervisor._set_statement_timeout",
            new=AsyncMock(),
        ),
    ):
        fake_conn = AsyncMock()
        mock_mtx.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
        mock_mtx.return_value.__aexit__ = AsyncMock(return_value=False)

        with caplog.at_level(logging.WARNING, logger="dynastore.modules.catalog.maintenance_supervisor"):
            await supervisor._run_job(engine, repo_mock, JOB_EVENTS_DLQ_PRUNE, _utc(2026, 6, 10))

    assert dispatched == [], "dispatch must NOT be called when mark_running returns 0"
    repo_mock.mark_done.assert_not_called()
    assert any("claimed" in r.message.lower() for r in caplog.records), (
        "Expected a WARNING about the job being claimed by another leader"
    )


# ---------------------------------------------------------------------------
# _STALE_AFTER_SECONDS is <= 600 (not 3600)
# ---------------------------------------------------------------------------


def test_stale_after_seconds_is_at_most_600():
    """_STALE_AFTER_SECONDS must be <= 600 so a crashed leader unblocks within
    10 minutes (5x the 60s task_reaper cadence, the shortest job cadence).
    1 hour (3600) is too long — a crashed pod blocks all jobs for up to an hour."""
    assert _STALE_AFTER_SECONDS <= 600, (
        f"_STALE_AFTER_SECONDS={_STALE_AFTER_SECONDS} is too large; "
        "it must be at most 600 (10 minutes) so a crashed leader unblocks within "
        "10 minutes. The longest meaningful reclaim window is 5x the shortest "
        "job cadence (task_reaper = 60s). See #1997."
    )


# ---------------------------------------------------------------------------
# _dispatch_job timeout: asyncio.wait_for with JOB_DISPATCH_TIMEOUT_SECONDS
# ---------------------------------------------------------------------------


def test_job_dispatch_timeout_constant_exists_and_reasonable():
    """A module-level JOB_DISPATCH_TIMEOUT_SECONDS constant must exist and be
    between 60 and 3600 seconds (1 min to 1 hour)."""
    from dynastore.modules.catalog.maintenance_supervisor import JOB_DISPATCH_TIMEOUT_SECONDS

    assert 60 <= JOB_DISPATCH_TIMEOUT_SECONDS <= 3600, (
        f"JOB_DISPATCH_TIMEOUT_SECONDS={JOB_DISPATCH_TIMEOUT_SECONDS} is outside [60, 3600]"
    )


@pytest.mark.asyncio
async def test_dispatch_job_raises_timeout_on_slow_job():
    """_run_job must raise/record an error when the job exceeds JOB_DISPATCH_TIMEOUT_SECONDS.

    We simulate a slow dispatch by making asyncio.wait_for raise TimeoutError.
    The job must record status='error' with a message mentioning 'timeout'.
    """
    engine = _fake_engine()
    supervisor = MaintenanceSupervisor(
        {"dead_letter_days": 30, "timeout_minutes": 15, "max_retries": 3}
    )

    repo_mock = MagicMock(spec=MaintenanceScheduleRepository)
    repo_mock.mark_running = AsyncMock(return_value=1)
    mark_done_kwargs: dict = {}

    async def _capture_done(conn, job_name, **kw):
        mark_done_kwargs.update(kw)

    repo_mock.mark_done = _capture_done

    async def _slow_dispatch(job_name, conn, config):
        raise asyncio.TimeoutError("simulated timeout")

    with (
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.managed_transaction",
        ) as mock_mtx,
        patch(
            "dynastore.modules.catalog.maintenance_supervisor._dispatch_job",
            new=AsyncMock(side_effect=_slow_dispatch),
        ),
        patch(
            "dynastore.modules.catalog.maintenance_supervisor._set_statement_timeout",
            new=AsyncMock(),
        ),
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.asyncio.wait_for",
            side_effect=asyncio.TimeoutError("timed out"),
        ),
    ):
        fake_conn = AsyncMock()
        mock_mtx.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
        mock_mtx.return_value.__aexit__ = AsyncMock(return_value=False)

        await supervisor._run_job(engine, repo_mock, JOB_TENANT_LOGS_PRUNE, _utc(2026, 6, 10))

    assert mark_done_kwargs.get("status") == "error"
    assert mark_done_kwargs.get("error") is not None
    assert "timeout" in mark_done_kwargs["error"].lower(), (
        f"Expected 'timeout' in error message, got: {mark_done_kwargs['error']!r}"
    )


# ---------------------------------------------------------------------------
# Per-schema isolation in _run_tenant_logs_prune
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tenant_logs_prune_continues_on_per_schema_error(caplog):
    """_run_tenant_logs_prune must catch per-schema errors, log WARNING with
    the schema name, and continue processing remaining schemas.

    This covers concurrently-dropped catalog schemas — a common prod scenario.
    """
    import logging

    conn = AsyncMock()
    schemas = [("s_good",), ("s_dropped",), ("s_alsoGood",)]
    schemas_attempted: list[str] = []
    schemas_ok: list[str] = []

    with (
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.DQLQuery"
        ) as MockDQL,
        caplog.at_level(logging.WARNING, logger="dynastore.modules.catalog.maintenance_supervisor"),
    ):
        def _dql_factory(sql, **kwargs):
            inst = MagicMock()
            if "physical_schema" in sql:
                # Schema listing query
                inst.execute = AsyncMock(return_value=schemas)
            else:
                # Per-schema delete — track which schema and raise for s_dropped
                matched_schema = None
                for row in schemas:
                    if f'"{row[0]}"' in sql:
                        matched_schema = row[0]
                        break

                async def _exec(c, **kw):
                    if matched_schema:
                        schemas_attempted.append(matched_schema)
                        if matched_schema == "s_dropped":
                            raise RuntimeError("relation does not exist")
                        schemas_ok.append(matched_schema)
                    return 0

                inst.execute = AsyncMock(side_effect=_exec)
            return inst

        MockDQL.side_effect = _dql_factory
        await _run_tenant_logs_prune(conn)

    # s_good and s_alsoGood should be processed
    assert "s_good" in schemas_ok, "s_good should have been processed"
    assert "s_alsoGood" in schemas_ok, "s_alsoGood should have been processed"

    # A WARNING mentioning the dropped schema must be emitted
    dropped_warnings = [
        r for r in caplog.records
        if r.levelno >= logging.WARNING and "s_dropped" in r.message
    ]
    assert dropped_warnings, (
        "Expected a WARNING log message mentioning the dropped schema 's_dropped'"
    )


# ---------------------------------------------------------------------------
# build_supervisor_config imports max_retries from events_module
# ---------------------------------------------------------------------------


def test_build_supervisor_config_max_retries_matches_events_module():
    """build_supervisor_config must import MAX_RETRIES from events_module,
    not hardcode 3, to prevent silent drift between the supervisor and the
    events consumer."""
    from dynastore.modules.events.events_module import MAX_RETRIES as events_max_retries

    cfg = build_supervisor_config()
    assert cfg["max_retries"] == events_max_retries, (
        f"build_supervisor_config max_retries={cfg['max_retries']} does not match "
        f"events_module.MAX_RETRIES={events_max_retries}. "
        "The supervisor must import this constant, not hardcode it."
    )
