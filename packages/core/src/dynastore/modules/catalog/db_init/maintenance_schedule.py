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

"""Durable maintenance-schedule table and its async repository.

DDL (``MAINTENANCE_SCHEDULE_DDL``) and the :class:`MaintenanceScheduleRepository`
are co-located so callers import both from a single, predictable module.

The table lives in the ``tasks`` schema (provisioned by ``TasksModule`` at
startup; this function also guards it defensively). It is provisioned at
startup time via ``CREATE TABLE IF NOT EXISTS`` — no runtime ALTER, no inline
DDL in module ``__init__``, no backfill loops.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    DbResource,
    ResultHandler,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Provisioning DDL
# ---------------------------------------------------------------------------

TASKS_SCHEMA_DDL = 'CREATE SCHEMA IF NOT EXISTS "tasks";'

MAINTENANCE_SCHEDULE_DDL = """
CREATE TABLE IF NOT EXISTS tasks.maintenance_schedule (
    job_name         TEXT        PRIMARY KEY,
    interval_seconds INTEGER     NOT NULL,
    last_run_at      TIMESTAMPTZ,
    running_since    TIMESTAMPTZ,
    last_status      TEXT,
    last_error       TEXT,
    last_rows        BIGINT
);
"""

# ---------------------------------------------------------------------------
# Repository queries  (module-level DQLQuery singletons — table path is static)
# ---------------------------------------------------------------------------

_GET_DUE_JOBS = DQLQuery(
    """
    SELECT job_name, interval_seconds, last_run_at, running_since,
           last_status, last_error, last_rows
    FROM tasks.maintenance_schedule
    WHERE running_since IS NULL
      AND (
          last_run_at IS NULL
          OR last_run_at + (interval_seconds * INTERVAL '1 second') <= :now
      )
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

_MARK_RUNNING = DQLQuery(
    """
    UPDATE tasks.maintenance_schedule
    SET running_since = :now
    WHERE job_name = :job_name
      AND running_since IS NULL
    """,
    result_handler=ResultHandler.ROWCOUNT,
)

_MARK_DONE = DQLQuery(
    """
    UPDATE tasks.maintenance_schedule
    SET last_run_at   = :finished_at,
        running_since = NULL,
        last_status   = :status,
        last_error    = :error,
        last_rows     = :rows
    WHERE job_name = :job_name
    """,
    result_handler=ResultHandler.ROWCOUNT,
)

_UPSERT_JOB = DQLQuery(
    """
    INSERT INTO tasks.maintenance_schedule (job_name, interval_seconds)
    VALUES (:job_name, :interval_seconds)
    ON CONFLICT (job_name) DO UPDATE
        SET interval_seconds = EXCLUDED.interval_seconds
    """,
    result_handler=ResultHandler.ROWCOUNT,
)

_RECLAIM_STALE_JOBS = DQLQuery(
    """
    UPDATE tasks.maintenance_schedule
    SET running_since = NULL,
        last_status   = 'error',
        last_error    = 'reclaimed: running_since stale',
        last_rows     = 0
    WHERE running_since IS NOT NULL
      AND running_since < CAST(:now AS TIMESTAMPTZ) - make_interval(secs => (
          LEAST(
              :max_stale_after_seconds,
              GREATEST(:min_stale_after_seconds, interval_seconds * :stale_multiplier)
          )
      ))
    """,
    result_handler=ResultHandler.ROWCOUNT,
)


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------


class MaintenanceScheduleRepository:
    """Thin async data-access layer for ``tasks.maintenance_schedule``.

    All methods accept a live :class:`DbResource` (connection or engine-bound
    object) and delegate SQL to module-level :class:`DQLQuery` singletons.
    No connection is opened here — callers own the transaction boundary.
    """

    async def get_due_jobs(
        self, conn: DbResource, *, now: datetime
    ) -> list[dict[str, Any]]:
        """Return rows whose next run is at or before *now* and not running."""
        return await _GET_DUE_JOBS.execute(conn, now=now) or []

    async def mark_running(
        self, conn: DbResource, job_name: str, *, now: datetime
    ) -> int:
        """Set ``running_since = now`` for *job_name*."""
        return await _MARK_RUNNING.execute(conn, job_name=job_name, now=now)

    async def mark_done(
        self,
        conn: DbResource,
        job_name: str,
        *,
        status: str,
        error: Optional[str],
        rows: Optional[int],
        finished_at: datetime,
    ) -> int:
        """Record completion: clear ``running_since``, set last_run/status/error/rows."""
        return await _MARK_DONE.execute(
            conn,
            job_name=job_name,
            finished_at=finished_at,
            status=status,
            error=error,
            rows=rows,
        )

    async def upsert_job(
        self, conn: DbResource, job_name: str, *, interval_seconds: int
    ) -> int:
        """Idempotently register or update a job's cadence."""
        return await _UPSERT_JOB.execute(
            conn, job_name=job_name, interval_seconds=interval_seconds
        )

    async def reclaim_stale_jobs(
        self,
        conn: DbResource,
        *,
        now: datetime,
        min_stale_after_seconds: int,
        max_stale_after_seconds: int,
        stale_multiplier: int,
    ) -> int:
        """Clear ``running_since`` for rows whose leader crashed mid-run.

        The threshold is derived from each row's cadence and clamped by the
        caller-provided bounds.  Short-cadence jobs recover quickly; daily jobs
        still recover within the max bound.

        Returns the number of rows reclaimed.
        """
        return await _RECLAIM_STALE_JOBS.execute(
            conn,
            now=now,
            min_stale_after_seconds=min_stale_after_seconds,
            max_stale_after_seconds=max_stale_after_seconds,
            stale_multiplier=stale_multiplier,
        )


# Re-export for tests (mirrors existing pattern for _GET_DUE_JOBS, _MARK_RUNNING etc.)
__all__ = [
    "MAINTENANCE_SCHEDULE_DDL",
    "MaintenanceScheduleRepository",
    "ensure_maintenance_schedule",
    "_GET_DUE_JOBS",
    "_MARK_RUNNING",
    "_MARK_DONE",
    "_UPSERT_JOB",
    "_RECLAIM_STALE_JOBS",
]


async def ensure_maintenance_schedule(conn: DbResource) -> None:
    """Apply the ``tasks.maintenance_schedule`` DDL (idempotent).

    The ``tasks`` schema is owned by ``TasksModule`` (which starts before
    ``CatalogModule``), but this function defensively ensures it as well so it
    is self-sufficient on the per-tenant trigger-install path
    (``AssetService.ensure_asset_cleanup_trigger`` →
    ``ensure_stored_procedures`` → here).
    """
    await DDLQuery(TASKS_SCHEMA_DDL).execute(conn)
    await DDLQuery(MAINTENANCE_SCHEDULE_DDL).execute(conn)
    logger.info("tasks.maintenance_schedule table ensured.")
