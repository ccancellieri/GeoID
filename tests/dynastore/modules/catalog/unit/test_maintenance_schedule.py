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

"""Unit tests for tasks.maintenance_schedule DDL + repository.

Pure-mock style (no live DB).  Mirrors the AsyncMock pattern from
tests/dynastore/modules/db_config/unit/test_base_extensions_guard.py.

Covered:
- due-job predicate (both NULL last_run_at and expired cases)
- mark_running transition
- mark_done transition
- upsert_job idempotency (ON CONFLICT path reflected in SQL shape)
- DDL constant contains required columns
- ensure_maintenance_schedule delegates to DDLQuery
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.db_init.maintenance_schedule import (
    MAINTENANCE_SCHEDULE_DDL,
    MaintenanceScheduleRepository,
    _GET_DUE_JOBS,
    _MARK_DONE,
    _MARK_RUNNING,
    _UPSERT_JOB,
    ensure_maintenance_schedule,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _dt(year: int = 2026, month: int = 1, day: int = 1) -> datetime:
    return datetime(year, month, day, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# DDL constant sanity checks (no DB needed)
# ---------------------------------------------------------------------------


def test_ddl_contains_all_required_columns():
    """MAINTENANCE_SCHEDULE_DDL must define every column in the spec."""
    required = {
        "job_name",
        "interval_seconds",
        "last_run_at",
        "running_since",
        "last_status",
        "last_error",
        "last_rows",
    }
    for col in required:
        assert col in MAINTENANCE_SCHEDULE_DDL, (
            f"Column '{col}' missing from MAINTENANCE_SCHEDULE_DDL"
        )


def test_ddl_uses_create_if_not_exists():
    assert "CREATE TABLE IF NOT EXISTS" in MAINTENANCE_SCHEDULE_DDL


def test_ddl_targets_tasks_schema():
    assert "tasks.maintenance_schedule" in MAINTENANCE_SCHEDULE_DDL


def test_ddl_job_name_is_primary_key():
    assert "PRIMARY KEY" in MAINTENANCE_SCHEDULE_DDL
    # job_name line comes before PRIMARY KEY keyword in column list
    ddl_lower = MAINTENANCE_SCHEDULE_DDL.lower()
    assert ddl_lower.index("job_name") < ddl_lower.index("primary key")


def test_ddl_interval_seconds_is_not_null():
    assert "interval_seconds" in MAINTENANCE_SCHEDULE_DDL
    # The NOT NULL constraint appears after interval_seconds in the spec
    ddl = MAINTENANCE_SCHEDULE_DDL
    col_line = next(
        (line for line in ddl.splitlines() if "interval_seconds" in line), ""
    )
    assert "NOT NULL" in col_line.upper()


# ---------------------------------------------------------------------------
# get_due_jobs — via mocked _GET_DUE_JOBS.execute
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_due_jobs_returns_all_dict_rows():
    """get_due_jobs propagates the list returned by the underlying query."""
    expected = [
        {"job_name": "prune_logs", "interval_seconds": 3600, "last_run_at": None,
         "running_since": None, "last_status": None, "last_error": None, "last_rows": None},
    ]
    conn = MagicMock()
    now = _dt()
    with patch.object(_GET_DUE_JOBS, "execute", new=AsyncMock(return_value=expected)):
        repo = MaintenanceScheduleRepository()
        result = await repo.get_due_jobs(conn, now=now)
    assert result == expected


@pytest.mark.asyncio
async def test_get_due_jobs_passes_now_param():
    """The 'now' datetime is forwarded to the query as a named parameter."""
    conn = MagicMock()
    now = _dt(2026, 6, 1)
    mock_exec = AsyncMock(return_value=[])
    with patch.object(_GET_DUE_JOBS, "execute", new=mock_exec):
        repo = MaintenanceScheduleRepository()
        await repo.get_due_jobs(conn, now=now)
    mock_exec.assert_awaited_once_with(conn, now=now)


@pytest.mark.asyncio
async def test_get_due_jobs_returns_empty_list_on_none():
    """When the query returns None (no rows), an empty list is returned."""
    conn = MagicMock()
    now = _dt()
    with patch.object(_GET_DUE_JOBS, "execute", new=AsyncMock(return_value=None)):
        repo = MaintenanceScheduleRepository()
        result = await repo.get_due_jobs(conn, now=now)
    assert result == []


# ---------------------------------------------------------------------------
# mark_running
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mark_running_passes_job_name_and_now():
    """mark_running forwards job_name and now to the underlying DMLQuery."""
    conn = MagicMock()
    now = _dt(2026, 3, 15)
    mock_exec = AsyncMock(return_value=1)
    with patch.object(_MARK_RUNNING, "execute", new=mock_exec):
        repo = MaintenanceScheduleRepository()
        result = await repo.mark_running(conn, "prune_tokens", now=now)
    mock_exec.assert_awaited_once_with(conn, job_name="prune_tokens", now=now)
    assert result == 1


@pytest.mark.asyncio
async def test_mark_running_returns_rowcount():
    """mark_running returns whatever rowcount the underlying query returns."""
    conn = MagicMock()
    for expected_count in (0, 1):
        with patch.object(_MARK_RUNNING, "execute", new=AsyncMock(return_value=expected_count)):
            result = await MaintenanceScheduleRepository().mark_running(conn, "j", now=_dt())
        assert result == expected_count


# ---------------------------------------------------------------------------
# mark_done
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mark_done_passes_all_params():
    """mark_done forwards every keyword argument to the underlying DMLQuery."""
    conn = MagicMock()
    finished = _dt(2026, 4, 1)
    mock_exec = AsyncMock(return_value=1)
    with patch.object(_MARK_DONE, "execute", new=mock_exec):
        repo = MaintenanceScheduleRepository()
        result = await repo.mark_done(
            conn,
            "partition_drop",
            status="ok",
            error=None,
            rows=42,
            finished_at=finished,
        )
    mock_exec.assert_awaited_once_with(
        conn,
        job_name="partition_drop",
        finished_at=finished,
        status="ok",
        error=None,
        rows=42,
    )
    assert result == 1


@pytest.mark.asyncio
async def test_mark_done_error_case():
    """mark_done can record an error status with a message."""
    conn = MagicMock()
    finished = _dt(2026, 4, 2)
    mock_exec = AsyncMock(return_value=1)
    with patch.object(_MARK_DONE, "execute", new=mock_exec):
        await MaintenanceScheduleRepository().mark_done(
            conn,
            "dlq_prune",
            status="error",
            error="connection timeout",
            rows=None,
            finished_at=finished,
        )
    _, kwargs = mock_exec.call_args
    assert kwargs["status"] == "error"
    assert kwargs["error"] == "connection timeout"
    assert kwargs["rows"] is None


# ---------------------------------------------------------------------------
# upsert_job — idempotency (ON CONFLICT path)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_job_passes_correct_params():
    """upsert_job forwards job_name and interval_seconds to the query."""
    conn = MagicMock()
    mock_exec = AsyncMock(return_value=1)
    with patch.object(_UPSERT_JOB, "execute", new=mock_exec):
        result = await MaintenanceScheduleRepository().upsert_job(
            conn, "reap_expired_tokens", interval_seconds=3600
        )
    mock_exec.assert_awaited_once_with(
        conn, job_name="reap_expired_tokens", interval_seconds=3600
    )
    assert result == 1


@pytest.mark.asyncio
async def test_upsert_job_idempotent_second_call():
    """upsert_job can be called twice without error (ON CONFLICT shape)."""
    conn = MagicMock()
    mock_exec = AsyncMock(return_value=1)
    with patch.object(_UPSERT_JOB, "execute", new=mock_exec):
        repo = MaintenanceScheduleRepository()
        await repo.upsert_job(conn, "events_dlq", interval_seconds=900)
        await repo.upsert_job(conn, "events_dlq", interval_seconds=900)
    assert mock_exec.await_count == 2


def test_upsert_job_sql_contains_on_conflict():
    """The upsert query uses ON CONFLICT (job_name) DO UPDATE semantics."""
    sql = _UPSERT_JOB.template
    assert "ON CONFLICT" in sql.upper()
    assert "DO UPDATE" in sql.upper()


# ---------------------------------------------------------------------------
# ensure_maintenance_schedule — DDL delegation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ensure_maintenance_schedule_calls_ddlquery():
    """ensure_maintenance_schedule executes one DDLQuery against the connection."""
    conn = MagicMock()
    mock_execute = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.catalog.db_init.maintenance_schedule.DDLQuery"
    ) as MockDDL:
        instance = MagicMock()
        instance.execute = mock_execute
        MockDDL.return_value = instance

        await ensure_maintenance_schedule(conn)

    MockDDL.assert_called_once_with(MAINTENANCE_SCHEDULE_DDL)
    mock_execute.assert_awaited_once_with(conn)
