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

"""Unit tests for the task supervisor job registrations (#1911 Option A).

Pure-mock style — no live DB.  Verifies:
- The 3 new task jobs are registered with correct names and intervals.
- register_supervisor_jobs upserts all 9 jobs (6 existing + 3 task).
- ensure_task_storage_exists no longer emits any cron.schedule call.
- The task reaper/partition-create/retention job implementations are
  dispatched correctly by _dispatch_job.
- _SUPERSEDED_CRON_JOBS contains the task-specific pg_cron job names.
- unschedule_superseded_cron_jobs passes task_reaper_prefix to the query.

Run with:
    PYTHONPATH=packages/core/src \\
      /Users/ccancellieri/work/code/geoid/.venv/bin/python \\
      -m pytest tests/dynastore/tasks/unit/test_task_supervisor_jobs.py \\
      --noconftest -p no:cacheprovider -q
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.maintenance_supervisor import (
    JOB_CONTROL_PLANE_RETENTION,
    JOB_OBLIGATION_SWEEP,
    JOB_TASK_PARTITION_CREATE,
    JOB_TASK_REAPER,
    JOB_TASK_RETENTION,
    _CADENCE_CONTROL_PLANE_RETENTION,
    _CADENCE_OBLIGATION_SWEEP,
    _CADENCE_TASK_PARTITION_CREATE,
    _CADENCE_TASK_REAPER,
    _CADENCE_TASK_RETENTION,
    _SUPERSEDED_CRON_JOBS,
    _SUPERSEDED_TASK_REAPER_PREFIX,
    _dispatch_job,
    build_supervisor_config,
    register_supervisor_jobs,
)
from dynastore.modules.catalog.db_init.maintenance_schedule import (
    MaintenanceScheduleRepository,
)


# ---------------------------------------------------------------------------
# Job name / cadence constants
# ---------------------------------------------------------------------------


def test_task_reaper_cadence_is_60s():
    """task_reaper must fire every 60 seconds (mirrors old '* * * * *' pg_cron)."""
    assert _CADENCE_TASK_REAPER == 60


def test_task_partition_create_cadence_is_daily():
    """task_partition_create must run daily (86400 s)."""
    assert _CADENCE_TASK_PARTITION_CREATE == 86400


def test_task_retention_cadence_is_daily():
    """task_retention must run daily (86400 s)."""
    assert _CADENCE_TASK_RETENTION == 86400


# ---------------------------------------------------------------------------
# _SUPERSEDED_CRON_JOBS contains task pg_cron names
# ---------------------------------------------------------------------------


def test_superseded_includes_task_partition_names():
    """prune_tasks_tasks and partcreate_tasks_tasks must be in _SUPERSEDED_CRON_JOBS."""
    assert "prune_tasks_tasks" in _SUPERSEDED_CRON_JOBS
    assert "partcreate_tasks_tasks" in _SUPERSEDED_CRON_JOBS


def test_superseded_task_reaper_prefix():
    """_SUPERSEDED_TASK_REAPER_PREFIX must match the dynastore-task-reaper- format."""
    assert _SUPERSEDED_TASK_REAPER_PREFIX == "dynastore-task-reaper-"


# ---------------------------------------------------------------------------
# register_supervisor_jobs upserts all 9 jobs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_register_supervisor_jobs_includes_task_jobs():
    """register_supervisor_jobs must upsert all 3 task jobs with correct intervals."""
    engine = MagicMock(name="engine")
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
            "dynastore.modules.catalog.maintenance_supervisor._iam_prune_available",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "dynastore.modules.catalog.maintenance_supervisor._es_logs_retention_available",
            return_value=True,
        ),
        patch(
            "dynastore.modules.catalog.maintenance_supervisor.managed_transaction",
        ) as mock_mtx,
    ):
        fake_conn = AsyncMock()
        mock_mtx.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
        mock_mtx.return_value.__aexit__ = AsyncMock(return_value=False)
        await register_supervisor_jobs(engine)

    cadence_map = dict(upserted)
    job_names = list(cadence_map.keys())

    # Total: 3 optional/logs/iam + 3 task + 4 events/storage + control-plane
    # + obligation-sweep = 12
    # (the 3 legacy events.events accumulation jobs were retired in #1807 P4;
    # obligation_sweep was added by #3196).
    assert len(job_names) == 12

    assert JOB_OBLIGATION_SWEEP in cadence_map
    assert JOB_TASK_REAPER in cadence_map
    assert JOB_TASK_PARTITION_CREATE in cadence_map
    assert JOB_TASK_RETENTION in cadence_map
    assert JOB_CONTROL_PLANE_RETENTION in cadence_map

    assert cadence_map[JOB_TASK_REAPER] == _CADENCE_TASK_REAPER
    assert cadence_map[JOB_TASK_PARTITION_CREATE] == _CADENCE_TASK_PARTITION_CREATE
    assert cadence_map[JOB_TASK_RETENTION] == _CADENCE_TASK_RETENTION
    assert cadence_map[JOB_CONTROL_PLANE_RETENTION] == _CADENCE_CONTROL_PLANE_RETENTION
    assert cadence_map[JOB_OBLIGATION_SWEEP] == _CADENCE_OBLIGATION_SWEEP


# ---------------------------------------------------------------------------
# _dispatch_job routes task jobs correctly
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatch_task_reaper_calls_reap_stuck_tasks():
    """_dispatch_job(task_reaper) must issue SELECT {schema}.reap_stuck_tasks(3, hard_cap)."""
    conn = AsyncMock()
    executed_sqls: list[str] = []

    async def _fake_execute(c, **kw):
        return 2

    with patch("dynastore.modules.catalog.maintenance_supervisor.DQLQuery") as MockDQL:
        instance = MagicMock()
        instance.execute = AsyncMock(side_effect=_fake_execute)
        MockDQL.side_effect = lambda sql, **kw: (executed_sqls.append(sql), instance)[1]

        result = await _dispatch_job(
            JOB_TASK_REAPER,
            conn,
            {
                "hard_cap": 7,
                "dead_letter_days": 30,
                "timeout_minutes": 15,
                "max_retries": 3,
            },
        )

    assert result == 2
    assert any("reap_stuck_tasks" in sql for sql in executed_sqls)
    # Must pass hard_cap as a named bind parameter
    call_kwargs = instance.execute.call_args[1]
    assert call_kwargs.get("hard_cap") == 7


@pytest.mark.asyncio
async def test_dispatch_task_partition_create_calls_create_func():
    """_dispatch_job(task_partition_create) must invoke create_partitions_{schema}_tasks."""
    conn = AsyncMock()
    executed_sqls: list[str] = []

    async def _fake_execute(c, **kw):
        return None

    with patch("dynastore.modules.catalog.maintenance_supervisor.DQLQuery") as MockDQL:
        instance = MagicMock()
        instance.execute = AsyncMock(side_effect=_fake_execute)
        MockDQL.side_effect = lambda sql, **kw: (executed_sqls.append(sql), instance)[1]

        result = await _dispatch_job(
            JOB_TASK_PARTITION_CREATE,
            conn,
            {
                "hard_cap": 5,
                "dead_letter_days": 30,
                "timeout_minutes": 15,
                "max_retries": 3,
            },
        )

    assert result == 0
    assert any("create_partitions" in sql for sql in executed_sqls)


@pytest.mark.asyncio
async def test_dispatch_task_retention_calls_maintain_func():
    """_dispatch_job(task_retention) must invoke maintain_partitions_{schema}_tasks."""
    conn = AsyncMock()
    executed_sqls: list[str] = []

    async def _fake_execute(c, **kw):
        return None

    with patch("dynastore.modules.catalog.maintenance_supervisor.DQLQuery") as MockDQL:
        instance = MagicMock()
        instance.execute = AsyncMock(side_effect=_fake_execute)
        MockDQL.side_effect = lambda sql, **kw: (executed_sqls.append(sql), instance)[1]

        result = await _dispatch_job(
            JOB_TASK_RETENTION,
            conn,
            {
                "hard_cap": 5,
                "dead_letter_days": 30,
                "timeout_minutes": 15,
                "max_retries": 3,
            },
        )

    assert result == 0
    assert any("maintain_partitions" in sql for sql in executed_sqls)


# ---------------------------------------------------------------------------
# build_supervisor_config includes hard_cap
# ---------------------------------------------------------------------------


def test_build_supervisor_config_includes_hard_cap():
    """build_supervisor_config must return a 'hard_cap' key."""
    cfg = build_supervisor_config()
    assert "hard_cap" in cfg
    assert isinstance(cfg["hard_cap"], int)
    assert cfg["hard_cap"] >= 1


# ---------------------------------------------------------------------------
# ensure_task_storage_exists emits NO cron.schedule
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ensure_task_storage_no_cron_schedule():
    """ensure_task_storage_exists must not call register_cron_job or cron.schedule."""
    from dynastore.modules.tasks.tasks_module import (
        ensure_task_storage_exists,
        get_task_schema,
    )

    conn = AsyncMock()
    schema = get_task_schema()

    cron_calls: list[str] = []

    async def _fake_ddl_execute(c, **kw):
        return None

    async def _fake_ensure_future_partitions(*a, **kw):
        return None

    with (
        patch(
            "dynastore.modules.tasks.tasks_module.ensure_schema_exists",
            new=AsyncMock(),
        ),
        patch(
            "dynastore.modules.tasks.tasks_module._build_tasks_ddl_batch",
        ) as mock_batch,
        patch(
            "dynastore.modules.db_config.maintenance_tools.ensure_future_partitions",
            new=AsyncMock(),
        ),
        patch(
            "dynastore.modules.tasks.tasks_module.DDLQuery",
        ) as MockDDL,
        patch(
            "dynastore.modules.tasks.tasks_module.DQLQuery",
        ) as MockDQL,
    ):
        # Fake DDLBatch
        fake_batch = MagicMock()
        fake_batch.execute = AsyncMock()
        mock_batch.return_value = fake_batch

        # Track all DDLQuery usages; flag if any contain cron.schedule
        def _dql_factory(sql, **kw):
            if "cron.schedule" in sql:
                cron_calls.append(sql)
            inst = MagicMock()
            inst.execute = AsyncMock()
            return inst

        MockDDL.side_effect = _dql_factory
        dql_inst = MagicMock()
        dql_inst.execute = AsyncMock(return_value="tasks.tasks_default")
        MockDQL.return_value = dql_inst

        await ensure_task_storage_exists(conn, schema)

    assert (
        cron_calls == []
    ), f"ensure_task_storage_exists should not emit cron.schedule; got: {cron_calls}"


# ---------------------------------------------------------------------------
# ensure_task_storage_exists provisions the 3 maintenance helper functions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ensure_task_storage_provisions_maintenance_functions():
    """ensure_task_storage_exists must execute DDLQuery for each of the 3 helper functions."""
    from dynastore.modules.tasks.tasks_module import (
        ensure_task_storage_exists,
        get_task_schema,
    )

    conn = AsyncMock()
    schema = get_task_schema()

    ddl_sqls_executed: list[str] = []

    with (
        patch(
            "dynastore.modules.tasks.tasks_module.ensure_schema_exists",
            new=AsyncMock(),
        ),
        patch(
            "dynastore.modules.tasks.tasks_module._build_tasks_ddl_batch",
        ) as mock_batch,
        patch(
            "dynastore.modules.db_config.maintenance_tools.ensure_future_partitions",
            new=AsyncMock(),
        ),
        patch(
            "dynastore.modules.tasks.tasks_module.DDLQuery",
        ) as MockDDL,
        patch(
            "dynastore.modules.tasks.tasks_module.DQLQuery",
        ) as MockDQL,
    ):
        fake_batch = MagicMock()
        fake_batch.execute = AsyncMock()
        mock_batch.return_value = fake_batch

        def _dql_factory(sql, **kw):
            ddl_sqls_executed.append(sql)
            inst = MagicMock()
            inst.execute = AsyncMock()
            return inst

        MockDDL.side_effect = _dql_factory
        dql_inst = MagicMock()
        dql_inst.execute = AsyncMock(return_value="tasks.tasks_default")
        MockDQL.return_value = dql_inst

        await ensure_task_storage_exists(conn, schema)

    combined = " ".join(ddl_sqls_executed)
    assert "reap_stuck_tasks" in combined, "Reaper DDL must be provisioned"
    assert (
        "maintain_partitions" in combined
    ), "Retention function DDL must be provisioned"
    assert (
        "create_partitions" in combined
    ), "Partition-create function DDL must be provisioned"


# ---------------------------------------------------------------------------
# unschedule_superseded_cron_jobs passes task_reaper_prefix
# ---------------------------------------------------------------------------


def _patch_mtx(mock_mtx, conn):
    """Wire a managed_transaction mock to yield *conn*."""
    mock_mtx.return_value.__aenter__ = AsyncMock(return_value=conn)
    mock_mtx.return_value.__aexit__ = AsyncMock(return_value=False)


@pytest.mark.asyncio
async def test_unschedule_superseded_passes_task_reaper_prefix():
    """unschedule_superseded_cron_jobs must include task_reaper_prefix in its query params."""
    from dynastore.modules.catalog.maintenance_supervisor import (
        unschedule_superseded_cron_jobs,
    )

    conn = AsyncMock()
    exec_calls: list[dict] = []

    async def _fake_execute(c, **kw):
        exec_calls.append(kw)
        return 5

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

        result = await unschedule_superseded_cron_jobs(MagicMock(name="engine"))

    assert result == 5
    assert len(exec_calls) == 1
    assert exec_calls[0]["task_reaper_prefix"] == "dynastore-task-reaper-%"
    # Names list must include the task partition/retention jobs
    assert "prune_tasks_tasks" in exec_calls[0]["names"]
    assert "partcreate_tasks_tasks" in exec_calls[0]["names"]


# ---------------------------------------------------------------------------
# Regression: retention DDL must use date_trunc('day') not 'daily' (#1998)
# ---------------------------------------------------------------------------


def test_retention_ddl_uses_day_not_daily():
    """GLOBAL_TASKS_RETENTION_FUNC_DDL must use date_trunc('day', ...).

    PostgreSQL does not recognise 'daily' as a valid field unit for
    date_trunc.  The function body is deferred by PL/pgSQL, so the
    CREATE OR REPLACE succeeds at startup but every invocation fails
    with "unit 'daily' not recognized" — partitions are never pruned.
    Regression guard for #1998.
    """
    from dynastore.modules.tasks.tasks_module import GLOBAL_TASKS_RETENTION_FUNC_DDL

    assert (
        "date_trunc('day'," in GLOBAL_TASKS_RETENTION_FUNC_DDL
    ), "Retention DDL must call date_trunc('day', ...) — 'daily' is invalid PG syntax"
    # The literal date_trunc call must not use 'daily'.  Comments may reference
    # the old buggy value for documentation purposes — check only the call site.
    assert (
        "date_trunc('daily'" not in GLOBAL_TASKS_RETENTION_FUNC_DDL
    ), "Retention DDL must not call date_trunc('daily', ...) — 'daily' is invalid PG syntax"


def test_retention_ddl_drains_tasks_default():
    """GLOBAL_TASKS_RETENTION_FUNC_DDL must include a DELETE sweep of tasks_default.

    The DEFAULT partition absorbs rows with out-of-range timestamps.
    Without an explicit sweep those rows accumulate forever since the
    monthly partition DROP loop never touches tasks_default.
    """
    from dynastore.modules.tasks.tasks_module import GLOBAL_TASKS_RETENTION_FUNC_DDL

    assert (
        "tasks_default" in GLOBAL_TASKS_RETENTION_FUNC_DDL
    ), "Retention DDL must include a sweep of the tasks_default partition"
    assert (
        "DELETE FROM" in GLOBAL_TASKS_RETENTION_FUNC_DDL
    ), "Retention DDL must DELETE stale rows from tasks_default"


@pytest.mark.asyncio
async def test_tasks_default_partition_repair_creates_when_missing():
    """Warm starts must repair tasks.tasks_default even when the DDLBatch sentinel skips."""
    from dynastore.modules.tasks import tasks_module

    conn = MagicMock()
    executed_ddl: list[str] = []

    def _dql_factory(sql, **kwargs):
        inst = MagicMock()
        inst.execute = AsyncMock(return_value=None)
        return inst

    def _ddl_factory(sql, **kwargs):
        inst = MagicMock()
        inst.execute = AsyncMock(side_effect=lambda c, **kw: executed_ddl.append(sql))
        return inst

    with (
        patch.object(tasks_module, "DQLQuery", side_effect=_dql_factory),
        patch.object(tasks_module, "DDLQuery", side_effect=_ddl_factory),
    ):
        await tasks_module._ensure_tasks_default_partition(conn, "tasks")

    assert tasks_module.GLOBAL_TASKS_DEFAULT_PARTITION_DDL in executed_ddl


def test_retention_ddl_partition_regex_is_single_brace():
    r"""The monthly-partition regex must use single braces (\d{4}_\d{2}).

    DDLQuery substitutes {schema} via str.replace, NOT str.format, so a
    doubled brace (\d{{4}}) is NOT collapsed — PostgreSQL would receive the
    literal "\d{{4}}", which matches no partition name and silently disables
    monthly retention. Regression guard for #2106 (mirrors the work_events /
    storage guard in test_workclass_ddl.py).
    """
    from dynastore.modules.tasks.tasks_module import GLOBAL_TASKS_RETENTION_FUNC_DDL

    assert r"tasks_\d{4}_\d{2}" in GLOBAL_TASKS_RETENTION_FUNC_DDL, (
        "Retention DDL must match monthly partitions with the single-brace "
        r"regex ^tasks_\d{4}_\d{2}$"
    )
    assert r"\d{{4}}" not in GLOBAL_TASKS_RETENTION_FUNC_DDL, (
        r"Retention DDL must not contain doubled braces \d{{4}} — DDLQuery uses "
        "str.replace, so they survive into the SQL and break the regex"
    )


def test_global_tasks_ddl_matches_shared_partition_template():
    """GLOBAL_TASKS_{PARTCREATE,RETENTION}_FUNC_DDL must be exactly what the
    shared workclass_ddl template renders for (table="tasks",
    granularity="month", window=4, retention=1) — guards against
    tasks_module.py drifting from the template it wires up (#2702).

    Retention also passes purge_safe_statuses (#3216): tasks.tasks rows
    carry audit value events/storage don't, so age-based partition pruning
    is status-gated there — see _TASKS_PURGE_SAFE_STATUSES.
    """
    from dynastore.modules.tasks.tasks_module import (
        GLOBAL_TASKS_PARTCREATE_FUNC_DDL,
        GLOBAL_TASKS_RETENTION_FUNC_DDL,
        _TASKS_PURGE_SAFE_STATUSES,
    )
    from dynastore.modules.tasks.workclass_ddl import (
        render_partition_create_ahead_ddl,
        render_partition_retention_ddl,
    )

    assert GLOBAL_TASKS_PARTCREATE_FUNC_DDL == render_partition_create_ahead_ddl(
        table="tasks", granularity="month", window=4
    )
    assert GLOBAL_TASKS_RETENTION_FUNC_DDL == render_partition_retention_ddl(
        table="tasks", granularity="month", retention=1,
        purge_safe_statuses=_TASKS_PURGE_SAFE_STATUSES,
    )
