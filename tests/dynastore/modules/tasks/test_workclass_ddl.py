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

"""Unit tests for workclass_ddl.py (tasks.events + tasks.storage DDL).

All DDL-content and regex tests are pure-Python (no live DB required).
The live-PG tests use a raw ``async_conn`` fixture; they skip cleanly
when no PG is reachable (``asyncpg.connect`` raises ``ConnectionRefusedError``
or ``OSError`` before the test body runs).

Run DB-free tests only:
    PYTHONPATH=packages/core/src \\
      /path/to/.venv/bin/python -m pytest \\
      tests/dynastore/modules/tasks/test_workclass_ddl.py -k "not live_pg" \\
      --noconftest -p no:cacheprovider -n0 -q

Run all (DB-connected):
    PYTHONPATH=packages/core/src \\
      /path/to/.venv/bin/python -m pytest \\
      tests/dynastore/modules/tasks/test_workclass_ddl.py -n0 -q
"""
from __future__ import annotations

import re
import os
from typing import AsyncIterator

import pytest
import pytest_asyncio

from dynastore.modules.tasks.workclass_ddl import (
    EVENTS_TABLE_DDL,
    EVENTS_DEFAULT_PARTITION_DDL,
    EVENTS_INDEXES_DDL,
    EVENTS_PARTCREATE_FUNC_DDL,
    EVENTS_RETENTION_FUNC_DDL,
    STORAGE_TABLE_DDL,
    STORAGE_DEFAULT_PARTITION_DDL,
    STORAGE_INDEXES_DDL,
    STORAGE_PARTCREATE_FUNC_DDL,
    STORAGE_RETENTION_FUNC_DDL,
    _WORKCLASS_CREATE_AHEAD_DAYS,
    _WORKCLASS_RETENTION_DAYS,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _render(template: str, schema: str = "tasks") -> str:
    """Mimic the DDLQuery {schema} substitution for unit tests."""
    return template.replace("{schema}", schema)


# ---------------------------------------------------------------------------
# events DDL content (tasks.events — renamed from tasks.work_events in P3)
# ---------------------------------------------------------------------------


def test_events_table_has_uuid_event_id():
    sql = _render(EVENTS_TABLE_DDL)
    assert "event_id" in sql
    assert "UUID" in sql


def test_events_primary_key_includes_day():
    """Primary key MUST include the partition key (day) — PG requirement."""
    sql = _render(EVENTS_TABLE_DDL)
    assert "PRIMARY KEY (day, event_id)" in sql


def test_events_partition_by_range_day():
    sql = _render(EVENTS_TABLE_DDL)
    assert "PARTITION BY RANGE (day)" in sql


def test_events_claim_version_column():
    """claim_version INTEGER NOT NULL DEFAULT 0 — native claim protocol."""
    sql = _render(EVENTS_TABLE_DDL)
    assert "claim_version" in sql
    assert "INTEGER" in sql
    assert "NOT NULL" in sql
    assert "DEFAULT 0" in sql


def test_events_scope_default_platform():
    """scope must default to 'platform' (lowercase — heed #1804)."""
    sql = _render(EVENTS_TABLE_DDL)
    assert "DEFAULT 'platform'" in sql


def test_events_scope_lowercase_check():
    """scope CHECK constraint must enforce lowercase storage."""
    sql = _render(EVENTS_TABLE_DDL)
    assert "CHECK (scope = lower(scope))" in sql


def test_events_fairness_index_leads_schema_name():
    """Fairness partial index must lead with schema_name and be WHERE status='PENDING'."""
    sql = _render(EVENTS_INDEXES_DDL)
    # The index columns must start with schema_name
    assert "(schema_name, created_at)" in sql
    assert "WHERE status = 'PENDING'" in sql


def test_events_default_partition_ddl_idempotent():
    sql = _render(EVENTS_DEFAULT_PARTITION_DDL)
    assert "IF NOT EXISTS" in sql
    assert "events_default" in sql
    assert "DEFAULT" in sql


# ---------------------------------------------------------------------------
# storage DDL content
# ---------------------------------------------------------------------------


def test_storage_table_has_uuid_op_id():
    sql = _render(STORAGE_TABLE_DDL)
    assert "op_id" in sql
    assert "UUID" in sql


def test_storage_primary_key_includes_day():
    sql = _render(STORAGE_TABLE_DDL)
    assert "PRIMARY KEY (day, op_id)" in sql


def test_storage_partition_by_range_day():
    sql = _render(STORAGE_TABLE_DDL)
    assert "PARTITION BY RANGE (day)" in sql


def test_storage_claim_version_column():
    sql = _render(STORAGE_TABLE_DDL)
    assert "claim_version" in sql
    assert "INTEGER" in sql
    assert "NOT NULL" in sql
    assert "DEFAULT 0" in sql


def test_storage_fairness_index_leads_catalog_id():
    """Fairness partial index must lead with catalog_id and be WHERE status='ready'."""
    sql = _render(STORAGE_INDEXES_DDL)
    assert "(catalog_id, ready_at)" in sql
    assert "WHERE status = 'ready'" in sql


def test_storage_default_partition_ddl_idempotent():
    sql = _render(STORAGE_DEFAULT_PARTITION_DDL)
    assert "IF NOT EXISTS" in sql
    assert "storage_default" in sql
    assert "DEFAULT" in sql


# ---------------------------------------------------------------------------
# Partition-function DDL content
# ---------------------------------------------------------------------------


def test_events_partcreate_func_name():
    """Create-ahead function must follow naming convention."""
    sql = _render(EVENTS_PARTCREATE_FUNC_DDL)
    assert "create_partitions_tasks_events" in sql


def test_events_retention_func_name():
    sql = _render(EVENTS_RETENTION_FUNC_DDL)
    assert "maintain_partitions_tasks_events" in sql


def test_storage_partcreate_func_name():
    sql = _render(STORAGE_PARTCREATE_FUNC_DDL)
    assert "create_partitions_tasks_storage" in sql


def test_storage_retention_func_name():
    sql = _render(STORAGE_RETENTION_FUNC_DDL)
    assert "maintain_partitions_tasks_storage" in sql


def test_partcreate_funcs_use_create_or_replace():
    """Both create-ahead functions must be idempotent via CREATE OR REPLACE."""
    for ddl in (EVENTS_PARTCREATE_FUNC_DDL, STORAGE_PARTCREATE_FUNC_DDL):
        assert "CREATE OR REPLACE FUNCTION" in ddl


def test_retention_funcs_use_create_or_replace():
    for ddl in (EVENTS_RETENTION_FUNC_DDL, STORAGE_RETENTION_FUNC_DDL):
        assert "CREATE OR REPLACE FUNCTION" in ddl


def test_retention_funcs_have_lock_timeout():
    """Retention functions must set LOCAL lock_timeout to protect against long lock waits."""
    for ddl in (EVENTS_RETENTION_FUNC_DDL, STORAGE_RETENTION_FUNC_DDL):
        assert "lock_timeout" in ddl


def test_retention_funcs_drain_default_partition():
    """Retention functions must DELETE stale rows from the DEFAULT partition."""
    assert "events_default" in EVENTS_RETENTION_FUNC_DDL
    assert "DELETE FROM" in EVENTS_RETENTION_FUNC_DDL
    assert "storage_default" in STORAGE_RETENTION_FUNC_DDL
    assert "DELETE FROM" in STORAGE_RETENTION_FUNC_DDL


def test_partcreate_funcs_use_to_char_yyyy_mm_dd():
    """Daily leaf partition names must use YYYY_MM_DD format."""
    for ddl in (EVENTS_PARTCREATE_FUNC_DDL, STORAGE_PARTCREATE_FUNC_DDL):
        assert "YYYY_MM_DD" in ddl


def test_create_ahead_days_constant_matches_loop_bound():
    """The 0-based loop bound in the SQL must equal _WORKCLASS_CREATE_AHEAD_DAYS - 1."""
    # Loop is FOR i IN 0..29 → 30 iterations = _WORKCLASS_CREATE_AHEAD_DAYS
    expected_bound = str(_WORKCLASS_CREATE_AHEAD_DAYS - 1)
    assert f"0..{expected_bound}" in EVENTS_PARTCREATE_FUNC_DDL
    assert f"0..{expected_bound}" in STORAGE_PARTCREATE_FUNC_DDL


def test_retention_days_constant_matches_interval_in_sql():
    """Retention INTERVAL in both functions must match _WORKCLASS_RETENTION_DAYS."""
    expected_interval = f"'{_WORKCLASS_RETENTION_DAYS} days'"
    assert expected_interval in EVENTS_RETENTION_FUNC_DDL
    assert expected_interval in STORAGE_RETENTION_FUNC_DDL


# ---------------------------------------------------------------------------
# Retention regex unit tests
# ---------------------------------------------------------------------------


# The exact daily-leaf regex the retention DDL ships. DDLQuery substitutes
# {schema} via str.replace (NOT str.format), so the regex uses SINGLE braces;
# the constant below must appear verbatim in the DDL source.
_EVENTS_LEAF_REGEX = r"^events_\d{4}_\d{2}_\d{2}$"
_STORAGE_LEAF_REGEX = r"^storage_\d{4}_\d{2}_\d{2}$"


def test_retention_ddl_embeds_the_daily_leaf_regex():
    # Drift guard: assert the regex ACTUALLY shipped in the DDL constant — not a
    # hand-reconstructed copy. A doubled-brace form (\d{{4}}) would reach PG
    # verbatim under .replace substitution, match no leaf, and silently disable
    # retention (verified against live PG). This test fails if that regresses.
    assert _EVENTS_LEAF_REGEX in EVENTS_RETENTION_FUNC_DDL
    assert _STORAGE_LEAF_REGEX in STORAGE_RETENTION_FUNC_DDL
    assert r"\d{{4}}" not in EVENTS_RETENTION_FUNC_DDL
    assert r"\d{{4}}" not in STORAGE_RETENTION_FUNC_DDL


def test_retention_regex_matches_daily_leaf_events():
    pattern = re.compile(_EVENTS_LEAF_REGEX)
    assert pattern.match("events_2026_06_13") is not None
    assert pattern.match("events_2025_01_01") is not None


def test_retention_regex_rejects_parent_table_events():
    pattern = re.compile(_EVENTS_LEAF_REGEX)
    assert pattern.match("events") is None


def test_retention_regex_rejects_default_partition_events():
    pattern = re.compile(_EVENTS_LEAF_REGEX)
    assert pattern.match("events_default") is None


def test_retention_regex_rejects_monthly_partition_events():
    """Regex must NOT match monthly-style names (which tasks.tasks uses)."""
    pattern = re.compile(_EVENTS_LEAF_REGEX)
    assert pattern.match("events_2026_06") is None


def test_retention_regex_rejects_tasks_monthly_partition():
    """Regex must NOT match tasks.tasks monthly leaf names."""
    pattern = re.compile(_EVENTS_LEAF_REGEX)
    assert pattern.match("tasks_2026_06") is None


def test_retention_regex_matches_daily_leaf_storage():
    pattern = re.compile(_STORAGE_LEAF_REGEX)
    assert pattern.match("storage_2026_06_13") is not None


def test_retention_regex_rejects_parent_table_storage():
    pattern = re.compile(_STORAGE_LEAF_REGEX)
    assert pattern.match("storage") is None


def test_retention_regex_rejects_default_partition_storage():
    pattern = re.compile(_STORAGE_LEAF_REGEX)
    assert pattern.match("storage_default") is None


# ---------------------------------------------------------------------------
# MaintenanceSupervisor job registration — workclass jobs
# ---------------------------------------------------------------------------


def test_supervisor_exports_workclass_job_constants():
    from dynastore.modules.catalog.maintenance_supervisor import (
        JOB_EVENTS_PARTITION_CREATE,
        JOB_EVENTS_RETENTION,
        JOB_STORAGE_PARTITION_CREATE,
        JOB_STORAGE_RETENTION,
        _CADENCE_EVENTS_PARTITION_CREATE,
        _CADENCE_EVENTS_RETENTION,
        _CADENCE_STORAGE_PARTITION_CREATE,
        _CADENCE_STORAGE_RETENTION,
    )
    assert JOB_EVENTS_PARTITION_CREATE == "events_partition_create"
    assert JOB_EVENTS_RETENTION == "events_retention"
    assert JOB_STORAGE_PARTITION_CREATE == "storage_partition_create"
    assert JOB_STORAGE_RETENTION == "storage_retention"
    # All four must run daily
    assert _CADENCE_EVENTS_PARTITION_CREATE == 86400
    assert _CADENCE_EVENTS_RETENTION == 86400
    assert _CADENCE_STORAGE_PARTITION_CREATE == 86400
    assert _CADENCE_STORAGE_RETENTION == 86400


@pytest.mark.asyncio
async def test_register_supervisor_jobs_includes_workclass_jobs():
    """register_supervisor_jobs must upsert all 4 workclass jobs."""
    from unittest.mock import AsyncMock, MagicMock, patch
    from dynastore.modules.catalog.maintenance_supervisor import (
        register_supervisor_jobs,
        JOB_TASK_RETENTION,
        JOB_EVENTS_PARTITION_CREATE,
        JOB_EVENTS_RETENTION,
        JOB_STORAGE_PARTITION_CREATE,
        JOB_STORAGE_RETENTION,
        _CADENCE_EVENTS_PARTITION_CREATE,
        _CADENCE_EVENTS_RETENTION,
        _CADENCE_STORAGE_PARTITION_CREATE,
        _CADENCE_STORAGE_RETENTION,
        _OBSOLETE_SCHEDULE_JOBS,
    )
    from dynastore.modules.catalog.db_init.maintenance_schedule import (
        MaintenanceScheduleRepository,
    )

    engine = MagicMock(name="engine")
    upserted: list[tuple[str, int]] = []

    repo_mock = MagicMock(spec=MaintenanceScheduleRepository)

    async def _capture_upsert(conn, job_name, *, interval_seconds):
        upserted.append((job_name, interval_seconds))

    repo_mock.upsert_job = _capture_upsert

    # The obsolete-row prune issues a raw DELETE via DQLQuery; capture its
    # bound params so we can assert it retires exactly the renamed jobs.
    prune_calls: list[dict] = []

    def _dql_factory(sql, **_kw):
        inst = MagicMock()

        async def _exec(_conn, **params):
            prune_calls.append({"sql": sql, **params})
            return 0

        inst.execute = AsyncMock(side_effect=_exec)
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

    cadence_map = dict(upserted)

    # 6 platform/task jobs (tenant+system logs prune, iam prune, task reaper,
    # task partition-create, task retention) + 4 workclass (events/storage
    # partition-create + retention) = 10 total.
    assert len(cadence_map) == 10

    # The tasks-table retention job must be registered — it is what runs the
    # monthly partition prune (the #2106 pre-flight-LOG path).
    assert JOB_TASK_RETENTION in cadence_map
    assert cadence_map[JOB_EVENTS_PARTITION_CREATE] == _CADENCE_EVENTS_PARTITION_CREATE
    assert cadence_map[JOB_EVENTS_RETENTION] == _CADENCE_EVENTS_RETENTION
    assert cadence_map[JOB_STORAGE_PARTITION_CREATE] == _CADENCE_STORAGE_PARTITION_CREATE
    assert cadence_map[JOB_STORAGE_RETENTION] == _CADENCE_STORAGE_RETENTION

    # Obsolete work_index_* schedule rows are pruned with the renamed names.
    assert len(prune_calls) == 1
    assert "DELETE FROM tasks.maintenance_schedule" in prune_calls[0]["sql"]
    assert prune_calls[0]["names"] == list(_OBSOLETE_SCHEDULE_JOBS)


@pytest.mark.asyncio
async def test_dispatch_events_partition_create():
    """_dispatch_job routes events_partition_create to the correct function."""
    from unittest.mock import AsyncMock, MagicMock, patch
    from dynastore.modules.catalog.maintenance_supervisor import (
        _dispatch_job,
        JOB_EVENTS_PARTITION_CREATE,
    )

    conn = AsyncMock()
    executed_sqls: list[str] = []

    async def _fake_execute(c, **kw):
        return None

    with patch(
        "dynastore.modules.catalog.maintenance_supervisor.DQLQuery"
    ) as MockDQL:
        instance = MagicMock()
        instance.execute = AsyncMock(side_effect=_fake_execute)
        MockDQL.side_effect = lambda sql, **kw: (executed_sqls.append(sql), instance)[1]

        result = await _dispatch_job(
            JOB_EVENTS_PARTITION_CREATE, conn,
            {"hard_cap": 5, "dead_letter_days": 30, "timeout_minutes": 15, "max_retries": 3},
        )

    assert result == 0
    assert any("create_partitions" in sql and "events" in sql for sql in executed_sqls)


@pytest.mark.asyncio
async def test_dispatch_events_retention():
    from unittest.mock import AsyncMock, MagicMock, patch
    from dynastore.modules.catalog.maintenance_supervisor import (
        _dispatch_job,
        JOB_EVENTS_RETENTION,
    )

    conn = AsyncMock()
    executed_sqls: list[str] = []

    async def _fake_execute(c, **kw):
        return None

    with patch(
        "dynastore.modules.catalog.maintenance_supervisor.DQLQuery"
    ) as MockDQL:
        instance = MagicMock()
        instance.execute = AsyncMock(side_effect=_fake_execute)
        MockDQL.side_effect = lambda sql, **kw: (executed_sqls.append(sql), instance)[1]

        result = await _dispatch_job(
            JOB_EVENTS_RETENTION, conn,
            {"hard_cap": 5, "dead_letter_days": 30, "timeout_minutes": 15, "max_retries": 3},
        )

    assert result == 0
    assert any("maintain_partitions" in sql and "events" in sql for sql in executed_sqls)


@pytest.mark.asyncio
async def test_dispatch_storage_partition_create():
    from unittest.mock import AsyncMock, MagicMock, patch
    from dynastore.modules.catalog.maintenance_supervisor import (
        _dispatch_job,
        JOB_STORAGE_PARTITION_CREATE,
    )

    conn = AsyncMock()
    executed_sqls: list[str] = []

    async def _fake_execute(c, **kw):
        return None

    with patch(
        "dynastore.modules.catalog.maintenance_supervisor.DQLQuery"
    ) as MockDQL:
        instance = MagicMock()
        instance.execute = AsyncMock(side_effect=_fake_execute)
        MockDQL.side_effect = lambda sql, **kw: (executed_sqls.append(sql), instance)[1]

        result = await _dispatch_job(
            JOB_STORAGE_PARTITION_CREATE, conn,
            {"hard_cap": 5, "dead_letter_days": 30, "timeout_minutes": 15, "max_retries": 3},
        )

    assert result == 0
    assert any("create_partitions" in sql and "storage" in sql for sql in executed_sqls)


@pytest.mark.asyncio
async def test_dispatch_storage_retention():
    from unittest.mock import AsyncMock, MagicMock, patch
    from dynastore.modules.catalog.maintenance_supervisor import (
        _dispatch_job,
        JOB_STORAGE_RETENTION,
    )

    conn = AsyncMock()
    executed_sqls: list[str] = []

    async def _fake_execute(c, **kw):
        return None

    with patch(
        "dynastore.modules.catalog.maintenance_supervisor.DQLQuery"
    ) as MockDQL:
        instance = MagicMock()
        instance.execute = AsyncMock(side_effect=_fake_execute)
        MockDQL.side_effect = lambda sql, **kw: (executed_sqls.append(sql), instance)[1]

        result = await _dispatch_job(
            JOB_STORAGE_RETENTION, conn,
            {"hard_cap": 5, "dead_letter_days": 30, "timeout_minutes": 15, "max_retries": 3},
        )

    assert result == 0
    assert any("maintain_partitions" in sql and "storage" in sql for sql in executed_sqls)


# ---------------------------------------------------------------------------
# ensure_workclass_storage_exists — unit (mock) idempotency test
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ensure_workclass_storage_exists_calls_ddl_query():
    """ensure_workclass_storage_exists must issue DDLQuery for each DDL step."""
    from unittest.mock import AsyncMock, MagicMock, patch
    from dynastore.modules.tasks.workclass_ddl import ensure_workclass_storage_exists

    conn = AsyncMock()
    ddl_sqls: list[str] = []
    dql_sqls: list[str] = []

    def _ddl_factory(sql, **kw):
        ddl_sqls.append(sql)
        inst = MagicMock()
        inst.execute = AsyncMock()
        return inst

    def _dql_factory(sql, **kw):
        dql_sqls.append(sql)
        inst = MagicMock()
        inst.execute = AsyncMock()
        return inst

    with (
        patch("dynastore.modules.tasks.workclass_ddl.DDLQuery", side_effect=_ddl_factory),
        patch("dynastore.modules.tasks.workclass_ddl.DQLQuery", side_effect=_dql_factory),
    ):
        await ensure_workclass_storage_exists(conn, "tasks")

    combined_ddl = " ".join(ddl_sqls)
    # Tables
    assert "events" in combined_ddl
    assert "storage" in combined_ddl
    # IF NOT EXISTS for idempotency
    assert "IF NOT EXISTS" in combined_ddl
    # Maintenance functions
    assert "CREATE OR REPLACE FUNCTION" in combined_ddl

    # The two create-ahead calls must be issued as DQL (SELECT func())
    combined_dql = " ".join(dql_sqls)
    assert "create_partitions" in combined_dql
    assert "events" in combined_dql
    assert "storage" in combined_dql


@pytest.mark.asyncio
async def test_ensure_workclass_storage_exists_twice_no_error():
    """Calling ensure_workclass_storage_exists twice raises no exception (idempotency contract)."""
    from unittest.mock import AsyncMock, MagicMock, patch
    from dynastore.modules.tasks.workclass_ddl import ensure_workclass_storage_exists

    conn = AsyncMock()

    def _ddl_factory(sql, **kw):
        inst = MagicMock()
        inst.execute = AsyncMock()
        return inst

    def _dql_factory(sql, **kw):
        inst = MagicMock()
        inst.execute = AsyncMock()
        return inst

    with (
        patch("dynastore.modules.tasks.workclass_ddl.DDLQuery", side_effect=_ddl_factory),
        patch("dynastore.modules.tasks.workclass_ddl.DQLQuery", side_effect=_dql_factory),
    ):
        await ensure_workclass_storage_exists(conn, "tasks")
        await ensure_workclass_storage_exists(conn, "tasks")  # second call — must not raise


# ---------------------------------------------------------------------------
# Live-PG tests (skip when no PG is available)
# ---------------------------------------------------------------------------

def _asyncpg_url() -> str:
    url = os.getenv(
        "DATABASE_URL",
        "postgresql://testuser:testpassword@localhost:54320/gis_dev",
    )
    return url.replace("postgresql+asyncpg://", "postgresql://")


@pytest_asyncio.fixture
async def workclass_async_conn() -> AsyncIterator[object]:
    """Raw asyncpg connection for workclass live-PG tests."""
    try:
        import asyncpg
        conn = await asyncpg.connect(_asyncpg_url())
    except Exception:
        pytest.skip("No live PG available for workclass DDL live tests")
    try:
        yield conn
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_live_pg_ensure_workclass_creates_tables(workclass_async_conn):
    """ensure_workclass_storage_exists creates both partitioned tables in a throwaway schema."""
    from dynastore.tools.identifiers import generate_id_hex

    conn = workclass_async_conn
    schema = f"wc_t_{generate_id_hex()[:10]}"

    try:
        await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

        # Import and call with a raw asyncpg connection — DDLQuery routes through
        # SQLAlchemy so we need to create a throwaway SA engine to wrap the call.
        # Instead, call the individual DDL strings directly via asyncpg so this
        # test stays independent of the SA layer.
        from dynastore.modules.tasks.workclass_ddl import (
            EVENTS_TABLE_DDL,
            EVENTS_DEFAULT_PARTITION_DDL,
            STORAGE_TABLE_DDL,
            STORAGE_DEFAULT_PARTITION_DDL,
        )

        for ddl_template in (
            EVENTS_TABLE_DDL,
            EVENTS_DEFAULT_PARTITION_DDL,
            STORAGE_TABLE_DDL,
            STORAGE_DEFAULT_PARTITION_DDL,
        ):
            rendered = ddl_template.replace("{schema}", schema)
            await conn.execute(rendered)

        # Verify tables exist
        for table_name in ("events", "storage"):
            row = await conn.fetchrow(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = $1 AND table_name = $2",
                schema,
                table_name,
            )
            assert row is not None, f"Expected table {schema}.{table_name} to exist"

        # Verify default partitions exist
        for partition_name in ("events_default", "storage_default"):
            row = await conn.fetchrow(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = $1 AND table_name = $2",
                schema,
                partition_name,
            )
            assert row is not None, f"Expected partition {schema}.{partition_name} to exist"

        # Idempotency: run again — must not raise
        for ddl_template in (
            EVENTS_TABLE_DDL,
            EVENTS_DEFAULT_PARTITION_DDL,
            STORAGE_TABLE_DDL,
            STORAGE_DEFAULT_PARTITION_DDL,
        ):
            rendered = ddl_template.replace("{schema}", schema)
            await conn.execute(rendered)  # second run — must be no-op

    finally:
        try:
            await conn.execute("RESET search_path")
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        except Exception:
            pass


@pytest.mark.asyncio
async def test_live_pg_events_table_structure(workclass_async_conn):
    """Verify column structure of tasks.events after DDL execution."""
    from dynastore.tools.identifiers import generate_id_hex
    from dynastore.modules.tasks.workclass_ddl import (
        EVENTS_TABLE_DDL,
        EVENTS_DEFAULT_PARTITION_DDL,
    )

    conn = workclass_async_conn
    schema = f"wc_t_{generate_id_hex()[:10]}"

    try:
        await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        await conn.execute(EVENTS_TABLE_DDL.replace("{schema}", schema))
        await conn.execute(EVENTS_DEFAULT_PARTITION_DDL.replace("{schema}", schema))

        cols = await conn.fetch(
            "SELECT column_name, data_type, column_default, is_nullable "
            "FROM information_schema.columns "
            "WHERE table_schema = $1 AND table_name = $2 "
            "ORDER BY ordinal_position",
            schema,
            "events",
        )
        col_names = {r["column_name"] for r in cols}

        expected = {
            "event_id", "day", "shard", "schema_name", "scope", "status",
            "payload", "claim_version", "owner_id", "locked_until",
            "retry_count", "max_retries", "created_at", "processed_at",
        }
        assert expected.issubset(col_names), (
            f"Missing columns: {expected - col_names}"
        )

        # claim_version must be NOT NULL with default 0
        cv = next(r for r in cols if r["column_name"] == "claim_version")
        assert cv["is_nullable"] == "NO"
        assert "0" in (cv["column_default"] or "")

    finally:
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        except Exception:
            pass


@pytest.mark.asyncio
async def test_live_pg_storage_table_structure(workclass_async_conn):
    """Verify column structure of storage after DDL execution."""
    from dynastore.tools.identifiers import generate_id_hex
    from dynastore.modules.tasks.workclass_ddl import (
        STORAGE_TABLE_DDL,
        STORAGE_DEFAULT_PARTITION_DDL,
    )

    conn = workclass_async_conn
    schema = f"wc_t_{generate_id_hex()[:10]}"

    try:
        await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        await conn.execute(STORAGE_TABLE_DDL.replace("{schema}", schema))
        await conn.execute(STORAGE_DEFAULT_PARTITION_DDL.replace("{schema}", schema))

        cols = await conn.fetch(
            "SELECT column_name, data_type, column_default, is_nullable "
            "FROM information_schema.columns "
            "WHERE table_schema = $1 AND table_name = $2 "
            "ORDER BY ordinal_position",
            schema,
            "storage",
        )
        col_names = {r["column_name"] for r in cols}

        expected = {
            "op_id", "day", "catalog_id", "driver_id", "collection_id",
            "entity_kind", "entity_id", "op", "status", "ready_at",
            "op_payload", "idempotency_key", "claim_version", "claimed_by",
            "claimed_at", "attempts", "created_at", "finished_at",
        }
        assert expected.issubset(col_names), (
            f"Missing columns: {expected - col_names}"
        )

        cv = next(r for r in cols if r["column_name"] == "claim_version")
        assert cv["is_nullable"] == "NO"
        assert "0" in (cv["column_default"] or "")

    finally:
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        except Exception:
            pass


@pytest.mark.asyncio
async def test_live_pg_retention_drops_old_daily_leaves_only(workclass_async_conn):
    """End-to-end proof that the retention function actually DROPs old leaves.

    Creates an old leaf (year 2000, well past the 30-day window) and a recent
    leaf (today), runs ``maintain_partitions_<schema>_events()``, and
    asserts the old leaf is gone while the recent leaf, the partitioned parent,
    and the DEFAULT partition all survive.

    This is the test the unit regex checks could not be: it exercises the SQL
    regex inside PostgreSQL. Under the original doubled-brace bug
    (``\\d{{4}}``) the function matched no leaf and dropped nothing, so this
    test fails RED — the old leaf would survive.
    """
    from datetime import date, timedelta
    from dynastore.tools.identifiers import generate_id_hex
    from dynastore.modules.tasks.workclass_ddl import (
        EVENTS_TABLE_DDL,
        EVENTS_DEFAULT_PARTITION_DDL,
        EVENTS_RETENTION_FUNC_DDL,
    )

    conn = workclass_async_conn
    schema = f"wc_t_{generate_id_hex()[:10]}"
    today = date.today()
    tomorrow = today + timedelta(days=1)
    recent_leaf = f"events_{today:%Y_%m_%d}"
    old_leaf = "events_2000_01_01"

    try:
        await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        await conn.execute(EVENTS_TABLE_DDL.replace("{schema}", schema))
        await conn.execute(
            EVENTS_DEFAULT_PARTITION_DDL.replace("{schema}", schema)
        )
        await conn.execute(
            EVENTS_RETENTION_FUNC_DDL.replace("{schema}", schema)
        )

        # An old leaf (far past the 30-day retention window) and a recent leaf.
        await conn.execute(
            f'CREATE TABLE "{schema}".{old_leaf} '
            f'PARTITION OF "{schema}".events '
            f"FOR VALUES FROM ('2000-01-01') TO ('2000-01-02')"
        )
        await conn.execute(
            f'CREATE TABLE "{schema}".{recent_leaf} '
            f'PARTITION OF "{schema}".events '
            f"FOR VALUES FROM ('{today.isoformat()}') TO ('{tomorrow.isoformat()}')"
        )

        # Run retention.
        await conn.execute(
            f'SELECT "{schema}"."maintain_partitions_{schema}_events"()'
        )

        # Parent is relkind 'p'; leaves + default are 'r'.
        existing = {
            r["relname"]
            for r in await conn.fetch(
                "SELECT c.relname FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = $1 AND c.relkind IN ('r', 'p')",
                schema,
            )
        }
        assert old_leaf not in existing, (
            "old daily leaf should have been dropped by retention "
            "(if present, the DROP regex matched nothing — the doubled-brace bug)"
        )
        assert recent_leaf in existing, "recent daily leaf must survive retention"
        assert "events" in existing, "parent table must survive retention"
        assert "events_default" in existing, (
            "DEFAULT partition must survive retention"
        )

    finally:
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        except Exception:
            pass
