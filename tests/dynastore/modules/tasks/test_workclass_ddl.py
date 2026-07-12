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
    render_partition_create_ahead_ddl,
    render_partition_retention_ddl,
    partition_create_ahead_function_name,
    partition_retention_function_name,
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


def test_events_fairness_index_leads_catalog_id():
    """Fairness partial index must lead with catalog_id and be WHERE status='PENDING'."""
    sql = _render(EVENTS_INDEXES_DDL)
    # The index columns must start with catalog_id (the tenant-routing key)
    assert "(catalog_id, created_at)" in sql
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


def test_storage_write_id_column():
    sql = _render(STORAGE_TABLE_DDL)
    assert "write_id" in sql
    assert "TEXT" in sql


def test_storage_table_has_no_op_payload_column():
    """``tasks.storage`` carries no payload column — a row is classified
    structurally by ``entity_id`` / ``write_id`` (async write-id outbox
    slice, #3116)."""
    sql = _render(STORAGE_TABLE_DDL)
    assert "op_payload" not in sql


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


def test_storage_obligation_sweep_lookup_indexes():
    """#2688 lane 1: the obligation sweep's anti-join needs one partial
    index per OR branch (write_id / entity_id), both scoped to
    (catalog_id, driver_id, collection_id), so Postgres can satisfy the
    query via a bitmap-or scan instead of a sequential scan."""
    sql = _render(STORAGE_INDEXES_DDL)
    assert "idx_storage_write_id_lookup" in sql
    assert "(catalog_id, driver_id, collection_id, write_id)" in sql
    assert "WHERE write_id IS NOT NULL" in sql
    assert "idx_storage_entity_id_lookup" in sql
    assert "(catalog_id, driver_id, collection_id, entity_id)" in sql
    assert "WHERE entity_id IS NOT NULL" in sql


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
    """Retention functions must DELETE stale rows from the DEFAULT partition —
    addressed through the parent table pinned by tableoid, never by naming the
    DEFAULT leaf directly (privileges are checked on the named table only, and
    the leaf may be owned by a different role — #3158)."""
    for table, ddl in (("events", EVENTS_RETENTION_FUNC_DDL),
                       ("storage", STORAGE_RETENTION_FUNC_DDL)):
        assert f'DELETE FROM "{{schema}}".{table}\n' in ddl
        assert f"to_regclass('\"{{schema}}\".{table}_default')" in ddl
        assert f'DELETE FROM "{{schema}}".{table}_default' not in ddl


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
        JOB_TASK_REAPER,
        JOB_TASK_PARTITION_CREATE,
        JOB_TASK_RETENTION,
        JOB_EVENTS_PARTITION_CREATE,
        JOB_EVENTS_RETENTION,
        JOB_STORAGE_PARTITION_CREATE,
        JOB_STORAGE_RETENTION,
        JOB_HEALTH_ALERT,
        JOB_CONTROL_PLANE_RETENTION,
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
    # register_supervisor_jobs also routes its availability probes (e.g.
    # _iam_prune_available's per-table ``to_regclass`` checks) through the
    # same patched DQLQuery, so only the prune DELETE itself is recorded here
    # — otherwise those probe calls would inflate this list too.
    prune_calls: list[dict] = []

    def _dql_factory(sql, **_kw):
        inst = MagicMock()

        async def _exec(_conn, **params):
            if "DELETE FROM tasks.maintenance_schedule" in sql:
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

    # register_supervisor_jobs always registers the base platform/task jobs
    # plus the 4 workclass jobs (events/storage partition-create +
    # retention). IAM_PRUNE / ES_LOGS_RETENTION register conditionally on
    # runtime availability so they're excluded from this floor. Asserting a
    # subset rather than an exact count means a newly-added job doesn't
    # require updating this test.
    expected_jobs = {
        JOB_TASK_REAPER,
        JOB_TASK_PARTITION_CREATE,
        JOB_TASK_RETENTION,
        JOB_EVENTS_PARTITION_CREATE,
        JOB_EVENTS_RETENTION,
        JOB_STORAGE_PARTITION_CREATE,
        JOB_STORAGE_RETENTION,
        JOB_HEALTH_ALERT,
        JOB_CONTROL_PLANE_RETENTION,
    }
    assert expected_jobs <= cadence_map.keys()

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


def test_partition_function_names_match_rendered_ddl():
    """The name-builder helpers (#3120) must match the quoted,
    schema-embedded function name each template actually renders — callers
    (schedule registration, tests) rely on the rendered name."""
    assert (
        partition_create_ahead_function_name(table="events", schema="tasks")
        == "create_partitions_tasks_events"
    )
    assert (
        partition_retention_function_name(table="events", schema="tasks")
        == "maintain_partitions_tasks_events"
    )
    # The rendered template is still {schema}-templated (DDLQuery substitutes
    # it later); the un-substituted function name must appear verbatim so
    # the naming convention is guaranteed to match what CREATE OR REPLACE
    # actually names, not just a parallel guess.
    rendered = render_partition_create_ahead_ddl(table="events", granularity="day", window=1)
    templated_name = partition_create_ahead_function_name(table="events", schema="{schema}")
    assert f'"{{schema}}"."{templated_name}"' in rendered


# The former test_ensure_workclass_storage_exists_passes_explicit_check_query
# asserted the #3120 check_query gates. Those gates froze CREATE OR REPLACE
# function bodies at first creation (#3306) and were removed; the inverted
# invariant (no existence gate on any function DDL) is covered by
# tests/dynastore/modules/tasks/unit/test_ensure_task_storage_function_ddl_3306.py.


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
# Shared partition-management template (issue #2702) — pin the generated SQL
# for all three (table, granularity, window, retention) parameterizations so
# a future edit to the template cannot silently change what any of the three
# workclass tables actually gets provisioned with.
# ---------------------------------------------------------------------------


def test_render_events_create_ahead_matches_module_constant():
    rendered = render_partition_create_ahead_ddl(
        table="events", granularity="day", window=_WORKCLASS_CREATE_AHEAD_DAYS
    )
    assert rendered == EVENTS_PARTCREATE_FUNC_DDL


def test_render_events_retention_matches_module_constant():
    rendered = render_partition_retention_ddl(
        table="events", granularity="day", retention=_WORKCLASS_RETENTION_DAYS
    )
    assert rendered == EVENTS_RETENTION_FUNC_DDL


def test_render_storage_create_ahead_matches_module_constant():
    rendered = render_partition_create_ahead_ddl(
        table="storage", granularity="day", window=_WORKCLASS_CREATE_AHEAD_DAYS
    )
    assert rendered == STORAGE_PARTCREATE_FUNC_DDL


def test_render_storage_retention_matches_module_constant():
    rendered = render_partition_retention_ddl(
        table="storage", granularity="day", retention=_WORKCLASS_RETENTION_DAYS
    )
    assert rendered == STORAGE_RETENTION_FUNC_DDL


def test_render_tasks_create_ahead_pinned():
    """Pin the monthly (tasks.tasks) create-ahead rendering — 4-month window."""
    rendered = render_partition_create_ahead_ddl(table="tasks", granularity="month", window=4)
    assert 'CREATE OR REPLACE FUNCTION "{schema}"."create_partitions_{schema}_tasks"()' in rendered
    assert "FOR i IN 0..3 LOOP" in rendered
    assert "date_trunc('month', NOW())" in rendered
    assert "start_date := target_date;" in rendered
    assert "end_date := target_date + INTERVAL '1 month';" in rendered
    assert "part_name   := 'tasks_' || to_char(target_date, 'YYYY_MM');" in rendered
    assert "PARTITION OF \"{schema}\".tasks" in rendered


def test_render_tasks_retention_pinned():
    """Pin the monthly (tasks.tasks) retention rendering — 1-month retention."""
    rendered = render_partition_retention_ddl(table="tasks", granularity="month", retention=1)
    assert 'CREATE OR REPLACE FUNCTION "{schema}"."maintain_partitions_{schema}_tasks"()' in rendered
    # Regression guard for #1998: must use date_trunc('day', ...), never 'daily'.
    assert "date_trunc('day', NOW())" in rendered
    assert "date_trunc('daily'" not in rendered
    assert r"tasks_\d{4}_\d{2}" in rendered
    assert r"\d{{4}}" not in rendered
    # Default-partition drain goes through the parent, pinned by tableoid
    # (#3158) — the age predicate follows the pin.
    assert 'DELETE FROM "{schema}".tasks\n' in rendered
    assert "WHERE tableoid = to_regclass('\"{schema}\".tasks_default')" in rendered
    assert "AND timestamp <" in rendered
    assert 'DELETE FROM "{schema}".tasks_default' not in rendered


def test_render_functions_reject_unknown_granularity():
    with pytest.raises(KeyError):
        render_partition_create_ahead_ddl(table="tasks", granularity="week", window=1)
    with pytest.raises(KeyError):
        render_partition_retention_ddl(table="tasks", granularity="week", retention=1)


# ---------------------------------------------------------------------------
# purge_safe_statuses guard (#3216) — tasks.tasks retention must spare
# non-terminal rows (PENDING/ACTIVE) and terminal-but-graced rows
# (DEAD_LETTER) regardless of partition/row age; events/storage are
# unaffected (no purge_safe_statuses passed for them).
# ---------------------------------------------------------------------------


def test_render_retention_without_purge_safe_statuses_unchanged():
    """Omitting purge_safe_statuses must reproduce the exact prior pure
    age-based rendering — events/storage retention is untouched by #3216."""
    rendered = render_partition_retention_ddl(table="events", granularity="day", retention=30)
    assert "status" not in rendered
    assert "part_is_purgeable" not in rendered


def test_render_tasks_retention_with_purge_safe_statuses_guards_leaf_drop():
    rendered = render_partition_retention_ddl(
        table="tasks", granularity="month", retention=1,
        purge_safe_statuses=("COMPLETED", "FAILED", "DISMISSED"),
    )
    # Leaf partitions are only dropped once no non-purge-safe row remains.
    assert "part_is_purgeable BOOLEAN;" in rendered
    assert "WHERE status NOT IN (''COMPLETED'', ''FAILED'', ''DISMISSED'')" in rendered
    assert "IF NOT part_is_purgeable THEN" in rendered
    assert "CONTINUE;" in rendered


def test_render_tasks_retention_with_purge_safe_statuses_guards_default_delete():
    rendered = render_partition_retention_ddl(
        table="tasks", granularity="month", retention=1,
        purge_safe_statuses=("COMPLETED", "FAILED", "DISMISSED"),
    )
    assert "AND status IN ('COMPLETED', 'FAILED', 'DISMISSED')" in rendered


def test_global_tasks_retention_ddl_is_status_aware():
    """Production wiring: GLOBAL_TASKS_RETENTION_FUNC_DDL (tasks_module.py)
    must actually pass purge_safe_statuses — this is the constant the
    MaintenanceSupervisor provisions and calls."""
    from dynastore.modules.tasks.tasks_module import GLOBAL_TASKS_RETENTION_FUNC_DDL

    assert "part_is_purgeable BOOLEAN;" in GLOBAL_TASKS_RETENTION_FUNC_DDL
    assert "'COMPLETED'" in GLOBAL_TASKS_RETENTION_FUNC_DDL
    assert "'DISMISSED'" in GLOBAL_TASKS_RETENTION_FUNC_DDL
    # DEAD_LETTER is deliberately excluded — it has its own longer grace
    # period (dlq_max_age_days) enforced by TaskRetentionService.
    assert "'DEAD_LETTER'" not in GLOBAL_TASKS_RETENTION_FUNC_DDL


def test_events_and_storage_retention_ddl_remain_status_unaware():
    """Regression guard: the production events/storage constants must not
    pick up the tasks-only status guard."""
    assert "part_is_purgeable" not in EVENTS_RETENTION_FUNC_DDL
    assert "part_is_purgeable" not in STORAGE_RETENTION_FUNC_DDL


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
            "event_id", "day", "shard", "catalog_id", "scope", "status",
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
            "write_id", "idempotency_key", "claim_version",
            "claimed_by", "claimed_at", "attempts", "created_at", "finished_at",
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
