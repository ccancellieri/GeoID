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
- build_supervisor_config provides the task reaper hard_cap
- register_supervisor_jobs upserts all 9 job names (iam + task + events/storage maintenance)

PG log persistence (and its iam_prune / system_logs_prune jobs) was
removed entirely in #2749 — logs are Elasticsearch-only now, so those job
tests are gone rather than skipped.
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
    JOB_ES_LOGS_RETENTION,
    JOB_IAM_PRUNE,
    JOB_STORAGE_PARTITION_CREATE,
    JOB_STORAGE_RETENTION,
    JOB_TASK_PARTITION_CREATE,
    JOB_TASK_REAPER,
    JOB_TASK_RETENTION,
    JOB_EVENTS_PARTITION_CREATE,
    JOB_EVENTS_RETENTION,
    MaintenanceSupervisor,
    _CADENCE_ES_LOGS_RETENTION,
    _CADENCE_IAM_PRUNE,
    _CADENCE_TASK_PARTITION_CREATE,
    _CADENCE_TASK_REAPER,
    _CADENCE_TASK_RETENTION,
    _STALE_AFTER_SECONDS,
    _SUPERSEDED_CRON_JOBS,
    _SUPERSEDED_TENANT_LOG_PREFIX,
    _dispatch_job,
    _run_es_logs_retention,
    _run_health_alert,
    _run_iam_prune,
    build_supervisor_config,
    register_supervisor_jobs,
    HealthAlertConfig,
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
        {"hard_cap": 5}
    )

    due = [_make_job_row(JOB_TASK_REAPER)]

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
            "dynastore.modules.catalog.maintenance_supervisor.background_managed_transaction",
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

    assert dispatched == [JOB_TASK_REAPER]


@pytest.mark.asyncio
async def test_run_once_no_due_jobs_does_nothing():
    """run_once with an empty due list logs debug and does not call _dispatch_job."""
    engine = _fake_engine()
    supervisor = MaintenanceSupervisor(
        {"hard_cap": 5}
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
            "dynastore.modules.catalog.maintenance_supervisor.background_managed_transaction",
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
        {"hard_cap": 5}
    )

    due = [
        _make_job_row(JOB_TASK_REAPER),
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
        if job_name == JOB_TASK_REAPER:
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
            "dynastore.modules.catalog.maintenance_supervisor.background_managed_transaction",
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

    failed = next(d for d in mark_done_calls if d["job_name"] == JOB_TASK_REAPER)
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
        {"hard_cap": 5}
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
            "dynastore.modules.catalog.maintenance_supervisor.background_managed_transaction",
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
        {"hard_cap": 5}
    )

    repo_mock = MagicMock(spec=MaintenanceScheduleRepository)
    repo_mock.mark_running = AsyncMock(return_value=1)
    mark_done_kwargs: dict = {}

    async def _capture_done(conn, job_name, **kw):
        mark_done_kwargs.update(kw)

    repo_mock.mark_done = _capture_done

    with (
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.background_managed_transaction",
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
        await supervisor._run_job(engine, repo_mock, JOB_IAM_PRUNE, _utc(2026, 6, 1))

    assert mark_done_kwargs["status"] == "ok"
    assert mark_done_kwargs["rows"] == 42
    assert mark_done_kwargs["error"] is None


# ---------------------------------------------------------------------------
# Job SQL / predicate checks
# ---------------------------------------------------------------------------


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
# build_supervisor_config reads env vars
# ---------------------------------------------------------------------------


def test_build_supervisor_config_provides_task_reaper_hard_cap():
    """build_supervisor_config returns the task reaper hard_cap.

    The legacy events accumulation knobs (dead_letter_days/timeout_minutes/
    max_retries) were removed with the events plane (#1807 P4): tasks.events
    now owns its own retry/dead-letter and DROP-PARTITION retention.
    """
    cfg = build_supervisor_config()
    assert "hard_cap" in cfg
    assert isinstance(cfg["hard_cap"], int)
    assert cfg["hard_cap"] >= 1
    # The retired events knobs must no longer be present.
    assert "dead_letter_days" not in cfg
    assert "timeout_minutes" not in cfg
    assert "max_retries" not in cfg


# ---------------------------------------------------------------------------
# register_supervisor_jobs upserts all 6 expected names
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_register_supervisor_jobs_upserts_all_expected_jobs():
    """register_supervisor_jobs upserts all 10 jobs (iam + 3 task + 4 events/storage
    partition+retention + health + es_logs_retention)."""
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

    from dynastore.modules.catalog.maintenance_supervisor import JOB_HEALTH_ALERT

    job_names = [name for name, _ in upserted]
    assert sorted(job_names) == sorted([
        JOB_IAM_PRUNE,
        JOB_TASK_REAPER,
        JOB_TASK_PARTITION_CREATE,
        JOB_TASK_RETENTION,
        JOB_EVENTS_PARTITION_CREATE,
        JOB_EVENTS_RETENTION,
        JOB_STORAGE_PARTITION_CREATE,
        JOB_STORAGE_RETENTION,
        JOB_HEALTH_ALERT,
        JOB_ES_LOGS_RETENTION,
    ])

    cadence_map = dict(upserted)
    assert cadence_map[JOB_ES_LOGS_RETENTION] == _CADENCE_ES_LOGS_RETENTION
    assert cadence_map[JOB_IAM_PRUNE] == _CADENCE_IAM_PRUNE
    assert cadence_map[JOB_TASK_REAPER] == _CADENCE_TASK_REAPER
    assert cadence_map[JOB_TASK_PARTITION_CREATE] == _CADENCE_TASK_PARTITION_CREATE
    assert cadence_map[JOB_TASK_RETENTION] == _CADENCE_TASK_RETENTION


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
        {"hard_cap": 5}
    )

    repo_mock = MagicMock(spec=MaintenanceScheduleRepository)
    # mark_running returns 0 → claimed by another leader
    repo_mock.mark_running = AsyncMock(return_value=0)
    repo_mock.mark_done = AsyncMock(return_value=1)

    dispatched: list[str] = []

    with (
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.background_managed_transaction",
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
            await supervisor._run_job(engine, repo_mock, JOB_TASK_REAPER, _utc(2026, 6, 10))

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
        {"hard_cap": 5}
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
            "dynastore.modules.catalog.maintenance_supervisor.background_managed_transaction",
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

        await supervisor._run_job(engine, repo_mock, JOB_IAM_PRUNE, _utc(2026, 6, 10))

    assert mark_done_kwargs.get("status") == "error"
    assert mark_done_kwargs.get("error") is not None
    assert "timeout" in mark_done_kwargs["error"].lower(), (
        f"Expected 'timeout' in error message, got: {mark_done_kwargs['error']!r}"
    )


# ---------------------------------------------------------------------------
# HealthAlertConfig: error_streak_threshold must NOT exist
# ---------------------------------------------------------------------------


def test_health_alert_config_has_no_error_streak_threshold():
    """HealthAlertConfig must not expose error_streak_threshold.

    The maintenance_schedule table stores only last_status/last_error per job
    (no per-run history), so a consecutive-error counter cannot be implemented
    cheaply.  The field was removed to avoid a false promise: callers cannot
    tune a threshold that was never enforced.
    """
    assert not hasattr(HealthAlertConfig.model_fields, "error_streak_threshold"), (
        "HealthAlertConfig must not declare error_streak_threshold — "
        "the table has no consecutive-error counter column."
    )
    cfg = HealthAlertConfig()
    assert not hasattr(cfg, "error_streak_threshold")


def test_health_alert_config_has_pending_age_and_dlq_fields():
    """HealthAlertConfig still exposes the two fields that are actually used."""
    cfg = HealthAlertConfig()
    assert cfg.pending_age_seconds == 3600
    assert cfg.dead_letter_threshold == 100


# ---------------------------------------------------------------------------
# _run_health_alert: alerts on ANY error in the past hour (not a streak)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_health_alert_alerts_on_single_error(caplog):
    """A single job with last_status='error' in the past hour must trigger an alert.

    The check is 'any error in the past hour', not a consecutive-failure count.
    """
    import logging

    conn = AsyncMock()
    emitted_events: list[dict] = []

    # DQLQuery calls in order:
    # 1. error_jobs SELECT → one row with last_status='error'
    # 2. stale_pending COUNT → 0
    # 3. tasks DEAD_LETTER COUNT → 0
    # 4. events DEAD_LETTER COUNT → 0
    call_num = [0]
    error_row = {"job_name": "iam_prune", "last_error": "boom", "last_run_at": "2026-06-26"}

    def _dql_factory(sql, **kwargs):
        inst = MagicMock()
        call_idx = call_num[0]
        call_num[0] += 1
        if call_idx == 0:
            inst.execute = AsyncMock(return_value=[error_row])
        else:
            inst.execute = AsyncMock(return_value=0)
        return inst

    async def _fake_emit(event_type, **kw):
        emitted_events.append({"event_type": event_type, **kw})

    # emit_event is imported lazily inside _run_health_alert; patch via sys.modules.
    fake_event_service = MagicMock(emit_event=AsyncMock(side_effect=_fake_emit))
    with (
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.DQLQuery",
            side_effect=_dql_factory,
        ),
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.load_health_alert_config",
            new=AsyncMock(return_value=HealthAlertConfig()),
        ),
        patch.dict("sys.modules", {"dynastore.modules.catalog.event_service": fake_event_service}),
        caplog.at_level(logging.ERROR, logger="dynastore.modules.catalog.maintenance_supervisor"),
    ):
        alerts = await _run_health_alert(conn)

    assert alerts >= 1, "Expected at least 1 alert for a single error job"
    error_logs = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert any("iam_prune" in r.message for r in error_logs), (
        "Expected an ERROR log mentioning the failing job name"
    )


@pytest.mark.asyncio
async def test_run_health_alert_emits_job_error_alert_type():
    """_run_health_alert must emit alert_type='job_error' (not 'job_error_streak')."""
    conn = AsyncMock()
    emitted_events: list[dict] = []
    error_row = {"job_name": "iam_prune", "last_error": "timeout", "last_run_at": "2026-06-26"}

    call_num = [0]

    def _dql_factory(sql, **kwargs):
        inst = MagicMock()
        call_idx = call_num[0]
        call_num[0] += 1
        if call_idx == 0:
            inst.execute = AsyncMock(return_value=[error_row])
        else:
            inst.execute = AsyncMock(return_value=0)
        return inst

    async def _fake_emit(event_type, **kw):
        emitted_events.append({"event_type": event_type, **kw})

    fake_event_service = MagicMock(emit_event=AsyncMock(side_effect=_fake_emit))
    with (
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.DQLQuery",
            side_effect=_dql_factory,
        ),
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.load_health_alert_config",
            new=AsyncMock(return_value=HealthAlertConfig()),
        ),
        patch.dict("sys.modules", {"dynastore.modules.catalog.event_service": fake_event_service}),
    ):
        await _run_health_alert(conn)

    job_error_events = [e for e in emitted_events if e.get("alert_type") == "job_error"]
    assert job_error_events, (
        "Expected an emitted event with alert_type='job_error'; "
        f"got: {[e.get('alert_type') for e in emitted_events]}"
    )
    streak_events = [e for e in emitted_events if e.get("alert_type") == "job_error_streak"]
    assert not streak_events, (
        "alert_type='job_error_streak' must not be emitted — field was removed"
    )


@pytest.mark.asyncio
async def test_run_health_alert_no_alerts_when_no_errors():
    """_run_health_alert returns 0 when no jobs are in error and counts are below threshold."""
    conn = AsyncMock()

    def _dql_factory(sql, **kwargs):
        inst = MagicMock()
        inst.execute = AsyncMock(return_value=0)
        return inst

    with (
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.DQLQuery",
            side_effect=_dql_factory,
        ),
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.load_health_alert_config",
            new=AsyncMock(return_value=HealthAlertConfig()),
        ),
    ):
        # First DQLQuery call (error_jobs) must return empty list
        call_num = [0]

        def _dql_factory2(sql, **kwargs):
            inst = MagicMock()
            call_idx = call_num[0]
            call_num[0] += 1
            if call_idx == 0:
                inst.execute = AsyncMock(return_value=[])  # no error jobs
            else:
                inst.execute = AsyncMock(return_value=0)
            return inst

        with patch(
            "dynastore.modules.catalog.maintenance_supervisor.DQLQuery",
            side_effect=_dql_factory2,
        ):
            alerts = await _run_health_alert(conn)

    assert alerts == 0, f"Expected 0 alerts, got {alerts}"


# ---------------------------------------------------------------------------
# es_logs_retention job (#2797) — ES-only, no PG connection needed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatch_job_es_logs_retention_calls_run_es_logs_retention():
    """_dispatch_job routes JOB_ES_LOGS_RETENTION to _run_es_logs_retention,
    ignoring the conn/config args every other job branch uses."""
    conn = AsyncMock()
    with patch(
        "dynastore.modules.catalog.maintenance_supervisor._run_es_logs_retention",
        new=AsyncMock(return_value=3),
    ) as mock_run:
        rows = await _dispatch_job(JOB_ES_LOGS_RETENTION, conn, {"hard_cap": 5})

    assert rows == 3
    mock_run.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_run_es_logs_retention_reads_config_and_delegates():
    """_run_es_logs_retention loads LogServiceConfig fresh (hot-reloadable,
    like HealthAlertConfig) and forwards retention_months to the ES driver."""
    from dynastore.modules.catalog.log_service_config import LogServiceConfig

    cfg = LogServiceConfig(retention_months=9)

    with (
        patch(
            "dynastore.modules.catalog.log_service_config.load",
            new=AsyncMock(return_value=cfg),
        ),
        patch(
            "dynastore.modules.elasticsearch.log_retention.run_es_logs_retention",
            new=AsyncMock(return_value=2),
        ) as mock_run,
    ):
        rows = await _run_es_logs_retention()

    assert rows == 2
    mock_run.assert_awaited_once_with(9)


def test_es_logs_retention_job_registered_in_dispatch_table():
    """JOB_ES_LOGS_RETENTION must be a known job name, not fall through to
    the ValueError branch of _dispatch_job."""
    assert JOB_ES_LOGS_RETENTION == "es_logs_retention"
    assert _CADENCE_ES_LOGS_RETENTION == 86400


# ---------------------------------------------------------------------------
# maintenance.health_alert must be a declared event (#2918)
# ---------------------------------------------------------------------------


def test_maintenance_health_alert_event_is_registered():
    """`_run_health_alert` emits ``maintenance.health_alert``, which must be
    declared via ``define_event`` (like every other event type) so
    ``EventRegistry.is_valid`` reflects reality instead of relying on
    ``EventService.emit``'s PLATFORM fallback for unregistered names."""
    from dynastore.modules.catalog.event_service import CatalogEventType, EventScope
    from dynastore.modules.tasks.events.primitives import EventRegistry

    assert EventRegistry.is_valid("maintenance.health_alert")
    assert EventRegistry._events["maintenance.health_alert"] == EventScope.PLATFORM
    assert CatalogEventType.MAINTENANCE_HEALTH_ALERT == "maintenance.health_alert"


