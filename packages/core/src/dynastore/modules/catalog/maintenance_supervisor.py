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

"""Leader-elected maintenance supervisor.

Drives deadline-insensitive periodic jobs off ``platform.maintenance_schedule``
(jobs 4–12 from the #1911 spec), replacing ALL pg_cron registrations:
events DLQ/reaper/alert, tenant-logs prune, system-logs prune, IAM prune,
and the three task-queue jobs (stuck-task reaper, partition-create, retention).

Architecture contract
---------------------
- One background loop per process; a pg session-level advisory lock (held
  via ``pg_advisory_leadership`` on a dedicated AUTOCOMMIT connection —
  never a pool checkout or an open transaction held across work) ensures
  exactly one pod fleet-wide performs the jobs.
- Tick behaviour: reclaim stale jobs → fetch due jobs → for each due job:
  mark_running, run with a bounded per-job statement_timeout, mark_done.
  A job raising an exception records status='error' and lets others proceed
  (per-job isolation, resilience matrix).
- Bounded-batch DELETEs: all prune jobs delete in batches of at most
  ``_PRUNE_BATCH`` rows, looping until 0 rows affected, so the first prune
  on a large table never creates a long transaction or WAL spike.
- No @cached anywhere in this module: maintenance_schedule reads are the
  mutable source of truth and prune predicates are time-dependent.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

from dynastore.modules.catalog.db_init.maintenance_schedule import (
    MaintenanceScheduleRepository,
)
from dynastore.modules.db_config.locking_tools import (
    check_extension_exists,
    pg_advisory_leadership,
)
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
)
from dynastore.tools.protocol_helpers import get_engine

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Advisory lock key — must not collide with SoftDeleteReaper (0x5D3A7E1F_C2B84961)
# or any other leader-elected loop.
# ---------------------------------------------------------------------------

_SUPERVISOR_ADVISORY_LOCK_KEY = 0x4D41494E_54454E41  # "MAINTENA" in ASCII hex

# ---------------------------------------------------------------------------
# Job names — must match the strings passed to repo.upsert_job at startup.
# ---------------------------------------------------------------------------

JOB_EVENTS_DLQ_PRUNE = "events_dlq_prune"
JOB_EVENTS_STUCK_REAPER = "events_stuck_reaper"
JOB_EVENTS_PENDING_ALERT = "events_pending_alert"
JOB_TENANT_LOGS_PRUNE = "tenant_logs_prune"
JOB_SYSTEM_LOGS_PRUNE = "system_logs_prune"
JOB_IAM_PRUNE = "iam_prune"
JOB_TASK_REAPER = "task_reaper"
JOB_TASK_PARTITION_CREATE = "task_partition_create"
JOB_TASK_RETENTION = "task_retention"
JOB_WORK_EVENTS_PARTITION_CREATE = "work_events_partition_create"
JOB_WORK_EVENTS_RETENTION = "work_events_retention"
JOB_STORAGE_PARTITION_CREATE = "storage_partition_create"
JOB_STORAGE_RETENTION = "storage_retention"

# Obsolete supervisor job names retired by the #1807 work_index -> storage
# rename. An environment that booted a prior build holds these rows in
# platform.maintenance_schedule; register_supervisor_jobs now upserts the
# storage_* names but never overwrites these, so the loop would dispatch an
# unknown job_name and record a recurring error. Prune them once at startup
# (DML on an operational table, not schema DDL). Safe no-op on a fresh DB.
_OBSOLETE_SCHEDULE_JOBS = (
    "work_index_partition_create",
    "work_index_retention",
)

# pg_cron job names this supervisor supersedes. On a non-fresh deploy these may
# already be scheduled in cron.job from a prior boot; we unschedule them once so
# they cannot double-run alongside the supervisor. Per-tenant tenant-logs jobs
# follow the ``monthly_cleanup_logs_<schema>`` shape and are matched by prefix.
_SUPERSEDED_CRON_JOBS = (
    "events_events_retention",
    "events_events_pending_alert",
    "events_events_reaper",
    "monthly_cleanup_system_logs",
    "prune_expired_iam",
    # Tasks pg_cron jobs (format: {policy}_{schema}_{table} / partcreate_{schema}_{table})
    "prune_tasks_tasks",
    "partcreate_tasks_tasks",
)
_SUPERSEDED_TENANT_LOG_PREFIX = "monthly_cleanup_logs_"
# Task reaper jobs use the format "dynastore-task-reaper-{schema}"; match by prefix
_SUPERSEDED_TASK_REAPER_PREFIX = "dynastore-task-reaper-"

# Cadences (seconds)
_CADENCE_DLQ_PRUNE = 86400        # daily
_CADENCE_STUCK_REAPER = 300       # 5 minutes
_CADENCE_PENDING_ALERT = 86400    # daily
_CADENCE_TENANT_LOGS = 2592000    # monthly (30 days)
_CADENCE_SYSTEM_LOGS = 2592000    # monthly (30 days)
_CADENCE_IAM_PRUNE = 86400        # daily
_CADENCE_TASK_REAPER = 60         # every minute (matches old "* * * * *")
_CADENCE_TASK_PARTITION_CREATE = 86400   # daily (idempotent CREATE IF NOT EXISTS)
_CADENCE_TASK_RETENTION = 86400   # daily (idempotent DROP old partitions)
_CADENCE_WORK_EVENTS_PARTITION_CREATE = 86400   # daily
_CADENCE_WORK_EVENTS_RETENTION = 86400          # daily
_CADENCE_STORAGE_PARTITION_CREATE = 86400    # daily
_CADENCE_STORAGE_RETENTION = 86400           # daily

# Bounded-batch DELETE size — no single DELETE removes more than this many rows.
_PRUNE_BATCH = 1000

# Stale-after threshold for reclaim (seconds): a job running for more than
# this long is assumed to belong to a dead leader and its running_since is
# cleared so the job can run again.  Set to 5× the shortest job cadence
# (task_reaper = 60 s) so a crashed pod unblocks all jobs within 10 minutes.
# Using 3600 (1 hour) was too long — it blocked every job for up to an hour
# after a pod crash.
_STALE_AFTER_SECONDS = 600  # 10 minutes (5× the 60 s task_reaper cadence)

# Per-job statement timeout — a hung job resigns rather than wedging the supervisor.
_JOB_STATEMENT_TIMEOUT_MS = 60_000  # 60 seconds

# Total wall-clock cap for a single dispatched job.  Covers jobs that loop
# internally (e.g. _run_tenant_logs_prune across thousands of schemas) or
# stall waiting for IO.  Chosen as 15× the longest per-statement timeout so
# a legitimate slow run across many schemas still completes; an actual hung
# job is cancelled well before it wedges the supervisor for a full cycle.
JOB_DISPATCH_TIMEOUT_SECONDS = 900  # 15 minutes

# Events schema — mirrors events_module._EVENTS_SCHEMA
_EVENTS_SCHEMA = os.getenv("DYNASTORE_EVENTS_SCHEMA", "events")

# IAM schema — always "iam"
_IAM_SCHEMA = "iam"

# Tasks schema — mirrors tasks_module.get_task_schema()
_TASKS_SCHEMA = os.getenv("DYNASTORE_TASK_SCHEMA", "tasks")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _set_statement_timeout(conn: Any, timeout_ms: int) -> None:
    """Apply a bounded per-statement timeout on *conn* for the current job."""
    await DQLQuery(
        f"SET LOCAL statement_timeout = {timeout_ms}",
        result_handler=ResultHandler.NONE,
    ).execute(conn)


async def _bounded_batch_delete(
    conn: Any,
    sql_template: str,
    batch_size: int = _PRUNE_BATCH,
    **params: Any,
) -> int:
    """Loop ``DELETE … WHERE … LIMIT batch_size`` until 0 rows affected.

    The caller must already be inside a ``managed_transaction``; this helper
    does not open its own transaction so it inherits the per-job
    ``statement_timeout``.  Returns total rows deleted.
    """
    total = 0
    while True:
        rows = await DQLQuery(
            sql_template,
            result_handler=ResultHandler.ROWCOUNT,
        ).execute(conn, batch_size=batch_size, **params)
        if not rows:
            break
        total += rows
    return total


# ---------------------------------------------------------------------------
# Individual job implementations
# ---------------------------------------------------------------------------


async def _run_events_dlq_prune(conn: Any, dead_letter_days: int) -> int:
    """Delete DEAD_LETTER events older than *dead_letter_days* days."""
    sql = (
        f"DELETE FROM {_EVENTS_SCHEMA}.events "
        f"WHERE ctid IN ("
        f"  SELECT ctid FROM {_EVENTS_SCHEMA}.events "
        f"  WHERE status = 'DEAD_LETTER' "
        f"    AND created_at < NOW() - (:dead_letter_days * INTERVAL '1 day') "
        f"  LIMIT :batch_size"
        f")"
    )
    return await _bounded_batch_delete(conn, sql, dead_letter_days=dead_letter_days)


async def _run_events_stuck_reaper(
    conn: Any, timeout_minutes: int, max_retries: int
) -> int:
    """Reap PROCESSING events older than *timeout_minutes* minutes.

    Ports the former events DLQ-reaper CTE (previously a pg_cron job, removed
    with the events-module cron registrations). Rows whose retry_count + 1 >=
    max_retries transition to DEAD_LETTER; others reset to PENDING.
    """
    sql = (
        f"WITH expired AS ("
        f"  SELECT shard, event_id, retry_count"
        f"  FROM {_EVENTS_SCHEMA}.events"
        f"  WHERE status = 'PROCESSING'"
        f"    AND processed_at < NOW() - (:timeout_minutes * INTERVAL '1 minute')"
        f"  FOR UPDATE SKIP LOCKED"
        f")"
        f"UPDATE {_EVENTS_SCHEMA}.events e"
        f"  SET status = CASE"
        f"    WHEN expired.retry_count + 1 >= :max_retries THEN 'DEAD_LETTER'"
        f"    ELSE 'PENDING'"
        f"  END,"
        f"  retry_count = expired.retry_count + 1,"
        f"  error_message = 'reaped stale PROCESSING'"
        f"  FROM expired"
        f"  WHERE e.shard = expired.shard AND e.event_id = expired.event_id"
    )
    result = await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(
        conn,
        timeout_minutes=timeout_minutes,
        max_retries=max_retries,
    )
    return result or 0


async def _run_events_pending_alert(conn: Any, dead_letter_days: int) -> int:
    """Emit a WARNING log per shard for stale PENDING events.

    Ports the former events retention alert query (previously a pg_cron job).
    No DB state is changed; last_rows = total stale pending count.
    """
    sql = (
        f"SELECT shard,"
        f"       count(*) AS n,"
        f"       EXTRACT(EPOCH FROM NOW()-min(created_at))::bigint AS oldest_age_sec"
        f"  FROM {_EVENTS_SCHEMA}.events"
        f" WHERE status = 'PENDING'"
        f"   AND created_at < NOW() - (:dead_letter_days * INTERVAL '1 day')"
        f" GROUP BY shard"
    )
    rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
        conn, dead_letter_days=dead_letter_days
    )
    total = 0
    for row in (rows or []):
        n = row["n"]
        total += n
        logger.warning(
            "events.pending_stale shard=%s count=%s oldest_age_sec=%s",
            row["shard"], n, row["oldest_age_sec"],
        )
    return total


async def _list_active_catalog_schemas(conn: Any) -> list[str]:
    """Return physical_schema for all non-deleted catalogs."""
    rows = await DQLQuery(
        "SELECT physical_schema FROM catalog.catalogs WHERE deleted_at IS NULL ORDER BY physical_schema",
        result_handler=ResultHandler.ALL,
    ).execute(conn)
    return [r[0] for r in rows] if rows else []


async def _run_tenant_logs_prune(conn: Any) -> int:
    """Delete tenant log rows older than 1 year across all active catalogs.

    Each schema is pruned in an independent try/except so a concurrently
    dropped catalog schema does not abort the remaining schemas.  A WARNING
    is emitted per failed schema so operators can investigate; the loop
    always continues to the next schema.
    """
    schemas = await _list_active_catalog_schemas(conn)
    total = 0
    for schema in schemas:
        # Schema name is an identifier, not a value; interpolate directly.
        sql = (
            f'DELETE FROM "{schema}".logs '
            f'WHERE ctid IN ('
            f'  SELECT ctid FROM "{schema}".logs '
            f'  WHERE "timestamp" < NOW() - INTERVAL \'1 year\' '
            f'  LIMIT :batch_size'
            f')'
        )
        try:
            total += await _bounded_batch_delete(conn, sql)
        except Exception as exc:
            logger.warning(
                "maintenance_supervisor: tenant_logs_prune skipping schema %r: %s",
                schema, exc,
            )
    return total


async def _run_system_logs_prune(conn: Any) -> int:
    """Delete system_logs rows older than 1 year."""
    sql = (
        'DELETE FROM "catalog"."system_logs" '
        'WHERE ctid IN ('
        '  SELECT ctid FROM "catalog"."system_logs" '
        '  WHERE "timestamp" < NOW() - INTERVAL \'1 year\' '
        '  LIMIT :batch_size'
        ')'
    )
    return await _bounded_batch_delete(conn, sql)


async def _run_iam_prune(conn: Any) -> int:
    """Delete expired IAM tokens, grants, and usage counters.

    Ports the former IAM prune PL/pgSQL body (deleted in #1911 / #1927) into a
    bounded-batch DELETE on each table; returns total rows deleted. The
    usage-counter predicates are not re-inlined here — they are imported from
    ``iam_queries`` so the WHERE clause stays single-sourced with the
    in-process driver (#800 gap #6).
    """
    # Function-local import: keeps this catalog-module function's coupling to
    # the IAM SQL predicates lazy and cycle-proof (constants only, not the
    # AuthorizationProtocol).
    from dynastore.modules.iam.iam_queries import (
        REAP_EXPIRED_USAGE_COUNTERS_WHERE,
        REAP_ORPHAN_USAGE_COUNTERS_WHERE,
    )

    schema = _IAM_SCHEMA
    total = 0

    # Expired refresh tokens
    sql = (
        f'DELETE FROM "{schema}".refresh_tokens '
        f'WHERE ctid IN ('
        f'  SELECT ctid FROM "{schema}".refresh_tokens '
        f'  WHERE expires_at < NOW() LIMIT :batch_size'
        f')'
    )
    total += await _bounded_batch_delete(conn, sql)

    # Expired OAuth2 authorisation codes
    sql = (
        f'DELETE FROM "{schema}".oauth_codes '
        f'WHERE ctid IN ('
        f'  SELECT ctid FROM "{schema}".oauth_codes '
        f'  WHERE expires_at < NOW() LIMIT :batch_size'
        f')'
    )
    total += await _bounded_batch_delete(conn, sql)

    # Expired OAuth2 access/bearer tokens
    sql = (
        f'DELETE FROM "{schema}".oauth_tokens '
        f'WHERE ctid IN ('
        f'  SELECT ctid FROM "{schema}".oauth_tokens '
        f'  WHERE expires_at < NOW() LIMIT :batch_size'
        f')'
    )
    total += await _bounded_batch_delete(conn, sql)

    # Expired grants (valid_until IS NOT NULL AND valid_until < NOW())
    sql = (
        f'DELETE FROM "{schema}".grants '
        f'WHERE ctid IN ('
        f'  SELECT ctid FROM "{schema}".grants '
        f'  WHERE valid_until IS NOT NULL AND valid_until < NOW() LIMIT :batch_size'
        f')'
    )
    total += await _bounded_batch_delete(conn, sql)

    # Expired usage counter buckets (rate-limit windows). The WHERE predicate
    # is the iam_queries SSOT, wrapped in the bounded-batch ctid loop.
    sql = (
        f'DELETE FROM "{schema}".usage_counters '
        f'WHERE ctid IN ('
        f'  SELECT ctid FROM "{schema}".usage_counters '
        f'  WHERE {REAP_EXPIRED_USAGE_COUNTERS_WHERE} LIMIT :batch_size'
        f')'
    )
    total += await _bounded_batch_delete(conn, sql)

    # Orphan lifetime usage counters (parent policy gone). Single unbatched
    # DELETE — the orphan set is tiny and bounded by deleted-policy count.
    # WHERE predicate is the iam_queries SSOT (carries its own {schema} ref).
    sql = (
        f'DELETE FROM "{schema}".usage_counters u '
        "WHERE " + REAP_ORPHAN_USAGE_COUNTERS_WHERE.format(schema=schema)
    )
    rows = await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(conn)
    total += rows or 0

    return total


# ---------------------------------------------------------------------------
# Task maintenance jobs
# ---------------------------------------------------------------------------


async def _run_task_reaper(conn: Any, hard_cap: int) -> int:
    """Invoke ``{schema}.reap_stuck_tasks(3, hard_cap)`` via SQL.

    The function body is provisioned by ``ensure_task_storage_exists`` on
    every boot (CREATE OR REPLACE); we only drive it here.  Uses
    p_max_retries=3 to match the old pg_cron command arg.
    """
    schema = _TASKS_SCHEMA
    result = await DQLQuery(
        f'SELECT "{schema}".reap_stuck_tasks(3, :hard_cap)',
        result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
    ).execute(conn, hard_cap=hard_cap)
    return int(result) if result is not None else 0


async def _run_task_partition_create(conn: Any) -> int:
    """Invoke the partition-creation function for the tasks table.

    The function ``{schema}.create_partitions_{schema}_tasks()`` is
    provisioned by ``ensure_task_storage_exists``; we call it here so
    future monthly partitions are always created ahead of time.
    Returns 0 (the function returns void).
    """
    schema = _TASKS_SCHEMA
    func_name = f"create_partitions_{schema}_tasks"
    await DQLQuery(
        f'SELECT "{schema}"."{func_name}"()',
        result_handler=ResultHandler.NONE,
    ).execute(conn)
    return 0


async def _run_task_retention(conn: Any) -> int:
    """Invoke the partition-retention function for the tasks table.

    The function ``{schema}.maintain_partitions_{schema}_tasks()`` is
    provisioned by ``ensure_task_storage_exists``; it drops monthly
    partitions older than 1 month.  Returns 0 (function returns void).
    """
    schema = _TASKS_SCHEMA
    func_name = f"maintain_partitions_{schema}_tasks"
    await DQLQuery(
        f'SELECT "{schema}"."{func_name}"()',
        result_handler=ResultHandler.NONE,
    ).execute(conn)
    return 0


# ---------------------------------------------------------------------------
# Workclass partition maintenance jobs (work_events + storage)
# ---------------------------------------------------------------------------


async def _run_work_events_partition_create(conn: Any) -> int:
    """Invoke the daily create-ahead function for ``tasks.work_events``.

    The function ``{schema}.create_partitions_{schema}_work_events()`` is
    provisioned by ``ensure_workclass_storage_exists``; it opens a 30-day
    window of daily leaf partitions.  Returns 0 (function returns void).
    """
    schema = _TASKS_SCHEMA
    func_name = f"create_partitions_{schema}_work_events"
    await DQLQuery(
        f'SELECT "{schema}"."{func_name}"()',
        result_handler=ResultHandler.NONE,
    ).execute(conn)
    return 0


async def _run_work_events_retention(conn: Any) -> int:
    """Invoke the daily retention function for ``tasks.work_events``.

    The function ``{schema}.maintain_partitions_{schema}_work_events()`` is
    provisioned by ``ensure_workclass_storage_exists``; it drops daily
    partitions older than 30 days.  Returns 0 (function returns void).
    """
    schema = _TASKS_SCHEMA
    func_name = f"maintain_partitions_{schema}_work_events"
    await DQLQuery(
        f'SELECT "{schema}"."{func_name}"()',
        result_handler=ResultHandler.NONE,
    ).execute(conn)
    return 0


async def _run_storage_partition_create(conn: Any) -> int:
    """Invoke the daily create-ahead function for ``tasks.storage``.

    The function ``{schema}.create_partitions_{schema}_storage()`` is
    provisioned by ``ensure_workclass_storage_exists``; it opens a 30-day
    window of daily leaf partitions.  Returns 0 (function returns void).
    """
    schema = _TASKS_SCHEMA
    func_name = f"create_partitions_{schema}_storage"
    await DQLQuery(
        f'SELECT "{schema}"."{func_name}"()',
        result_handler=ResultHandler.NONE,
    ).execute(conn)
    return 0


async def _run_storage_retention(conn: Any) -> int:
    """Invoke the daily retention function for ``tasks.storage``.

    The function ``{schema}.maintain_partitions_{schema}_storage()`` is
    provisioned by ``ensure_workclass_storage_exists``; it drops daily
    partitions older than 30 days.  Returns 0 (function returns void).
    """
    schema = _TASKS_SCHEMA
    func_name = f"maintain_partitions_{schema}_storage"
    await DQLQuery(
        f'SELECT "{schema}"."{func_name}"()',
        result_handler=ResultHandler.NONE,
    ).execute(conn)
    return 0


# ---------------------------------------------------------------------------
# Job dispatch table
# ---------------------------------------------------------------------------

# Map job_name → coroutine factory(conn) → int (rows affected)
# Each factory receives the open connection inside managed_transaction.
# Config params are captured at upsert time; the supervisor reads them once
# per startup when registering jobs.

async def _dispatch_job(job_name: str, conn: Any, config: dict[str, Any]) -> int:
    """Dispatch a single maintenance job by name.

    Returns the number of rows affected (0 if not applicable).
    Raises on unexpected errors so the caller can record status='error'.
    """
    if job_name == JOB_EVENTS_DLQ_PRUNE:
        return await _run_events_dlq_prune(conn, config["dead_letter_days"])
    if job_name == JOB_EVENTS_STUCK_REAPER:
        return await _run_events_stuck_reaper(
            conn, config["timeout_minutes"], config["max_retries"]
        )
    if job_name == JOB_EVENTS_PENDING_ALERT:
        return await _run_events_pending_alert(conn, config["dead_letter_days"])
    if job_name == JOB_TENANT_LOGS_PRUNE:
        return await _run_tenant_logs_prune(conn)
    if job_name == JOB_SYSTEM_LOGS_PRUNE:
        return await _run_system_logs_prune(conn)
    if job_name == JOB_IAM_PRUNE:
        return await _run_iam_prune(conn)
    if job_name == JOB_TASK_REAPER:
        return await _run_task_reaper(conn, config["hard_cap"])
    if job_name == JOB_TASK_PARTITION_CREATE:
        return await _run_task_partition_create(conn)
    if job_name == JOB_TASK_RETENTION:
        return await _run_task_retention(conn)
    if job_name == JOB_WORK_EVENTS_PARTITION_CREATE:
        return await _run_work_events_partition_create(conn)
    if job_name == JOB_WORK_EVENTS_RETENTION:
        return await _run_work_events_retention(conn)
    if job_name == JOB_STORAGE_PARTITION_CREATE:
        return await _run_storage_partition_create(conn)
    if job_name == JOB_STORAGE_RETENTION:
        return await _run_storage_retention(conn)
    raise ValueError(f"maintenance_supervisor: unknown job_name {job_name!r}")


# ---------------------------------------------------------------------------
# MaintenanceSupervisor
# ---------------------------------------------------------------------------


class MaintenanceSupervisor:
    """Leader-elected supervisor that drives periodic maintenance jobs.

    Call ``start(shutdown_event)`` from CatalogModule.lifespan to schedule the
    background loop.  The supervisor holds a session-level pg advisory lock on
    a dedicated ``managed_transaction`` connection; exactly one pod fleet-wide
    wins the lock per cadence.

    Jobs are registered via ``repo.upsert_job`` at startup; the supervisor
    reads ``platform.maintenance_schedule`` on every tick (no caching — it is
    the mutable source of truth).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        """Initialise with resolved job config values.

        *config* must contain:
          ``dead_letter_days`` (int)   — from EventsModule.accumulation_policy
          ``timeout_minutes`` (int)    — EVENT_PROCESSING_TIMEOUT_MINUTES env var
          ``max_retries`` (int)        — from EventsModule.accumulation_policy
        """
        self._config = config
        self._task: Optional[asyncio.Task[Any]] = None

    def start(self, shutdown_event: asyncio.Event) -> None:
        """Schedule the supervisor loop as an asyncio background task."""
        self._task = asyncio.create_task(
            self._loop(shutdown_event),
            name="maintenance_supervisor",
        )

    async def stop(self) -> None:
        """Cancel and await the background task."""
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def _loop(self, shutdown_event: asyncio.Event) -> None:
        """Leader-elected outer loop — exits when shutdown_event is set."""
        from dynastore.tools.async_utils import run_leader_loop

        def _acquire_leadership():
            return pg_advisory_leadership(
                get_engine(),
                _SUPERVISOR_ADVISORY_LOCK_KEY,
                name="MaintenanceSupervisor",
            )

        async def _on_leader() -> None:
            await self.run_once()
            # Cadence sleep: re-evaluate leadership every 60 s (inner cadence).
            # The outer run_leader_loop cadence_seconds controls the outer retry.
            await asyncio.sleep(60.0)

        await run_leader_loop(
            acquire_leadership=_acquire_leadership,
            on_leader=_on_leader,
            name="MaintenanceSupervisor",
            cadence_seconds=60.0,
            is_shutdown=shutdown_event.is_set,
        )

    async def run_once(self) -> None:
        """One full supervisor tick: reclaim stale, then dispatch due jobs.

        Each job runs in its own transaction with a bounded statement_timeout.
        A job failure records status='error' and does not block other jobs.
        """
        engine = get_engine()
        if engine is None:
            logger.warning("maintenance_supervisor: no DB engine — skipping tick.")
            return

        repo = MaintenanceScheduleRepository()
        now = datetime.now(tz=timezone.utc)

        # Step 1: reclaim stale jobs (crashed leader mid-run).
        try:
            async with managed_transaction(engine) as conn:
                reclaimed = await repo.reclaim_stale_jobs(
                    conn, now=now, stale_after_seconds=_STALE_AFTER_SECONDS
                )
            if reclaimed:
                logger.warning(
                    "maintenance_supervisor: reclaimed %d stale job(s).", reclaimed
                )
        except Exception as exc:
            logger.warning(
                "maintenance_supervisor: reclaim_stale_jobs failed: %s — "
                "continuing tick.", exc,
            )

        # Step 2: fetch due jobs.
        try:
            async with managed_transaction(engine) as conn:
                due_jobs = await repo.get_due_jobs(conn, now=now)
        except Exception as exc:
            logger.warning(
                "maintenance_supervisor: get_due_jobs failed: %s — aborting tick.", exc
            )
            return

        if not due_jobs:
            logger.debug("maintenance_supervisor: no due jobs this tick.")
            return

        # Step 3: run each due job in its own transaction.
        for job in due_jobs:
            job_name: str = job["job_name"]
            await self._run_job(engine, repo, job_name, now)

    async def _run_job(
        self,
        engine: Any,
        repo: MaintenanceScheduleRepository,
        job_name: str,
        tick_now: datetime,
    ) -> None:
        """Execute one maintenance job with full lifecycle tracking.

        mark_running uses AND running_since IS NULL so it returns 0 rows when
        another leader already claimed the job this tick.  When that happens
        we skip dispatch entirely and do not call mark_done — the other leader
        owns the completion record.
        """
        async with managed_transaction(engine) as conn:
            claimed = await repo.mark_running(conn, job_name, now=tick_now)

        if not claimed:
            logger.warning(
                "maintenance_supervisor: job %r already claimed by another leader "
                "— skipping this tick.",
                job_name,
            )
            return

        rows: Optional[int] = None
        status = "ok"
        error: Optional[str] = None

        try:
            async with managed_transaction(engine) as conn:
                await _set_statement_timeout(conn, _JOB_STATEMENT_TIMEOUT_MS)
                rows = await asyncio.wait_for(
                    _dispatch_job(job_name, conn, self._config),
                    timeout=JOB_DISPATCH_TIMEOUT_SECONDS,
                )
            logger.info(
                "maintenance_supervisor: job %r done — rows=%s.", job_name, rows
            )
        except asyncio.TimeoutError:
            status = "error"
            error = (
                f"job {job_name!r} exceeded dispatch timeout "
                f"({JOB_DISPATCH_TIMEOUT_SECONDS}s)"
            )
            logger.error(
                "maintenance_supervisor: %s", error,
            )
        except Exception as exc:
            status = "error"
            error = str(exc)
            logger.exception(
                "maintenance_supervisor: job %r failed: %s", job_name, exc
            )

        finished_at = datetime.now(tz=timezone.utc)
        try:
            async with managed_transaction(engine) as conn:
                await repo.mark_done(
                    conn,
                    job_name,
                    status=status,
                    error=error,
                    rows=rows,
                    finished_at=finished_at,
                )
        except Exception as exc:
            logger.warning(
                "maintenance_supervisor: mark_done for %r failed: %s — "
                "running_since may be stale until next reclaim tick.",
                job_name, exc,
            )


# ---------------------------------------------------------------------------
# Startup: register job cadences into platform.maintenance_schedule
# ---------------------------------------------------------------------------


async def unschedule_superseded_cron_jobs(engine: Any) -> int:
    """Unschedule any pre-existing pg_cron jobs this supervisor now owns.

    Clean-cut safety for a non-fresh deploy: the events/logs/IAM ``pg_cron``
    registrations are gone from the code, but a database that was provisioned
    before this change may still have those jobs scheduled in ``cron.job`` —
    they would then run *alongside* the supervisor (double-run; the stuck-event
    reaper would double-increment ``retry_count``). This unschedules them once.

    No-op when ``pg_cron`` is absent (fresh / on-prem). Idempotent: after the
    first run there are no matching rows, so subsequent boots delete nothing.
    Returns the number of cron jobs unscheduled.
    """
    async with managed_transaction(engine) as conn:
        if not await check_extension_exists(conn, "pg_cron"):
            return 0
        rows = await DQLQuery(
            "SELECT cron.unschedule(jobid) FROM cron.job "
            "WHERE jobname = ANY(:names) "
            "   OR jobname LIKE :tenant_prefix "
            "   OR jobname LIKE :task_reaper_prefix",
            result_handler=ResultHandler.ROWCOUNT,
        ).execute(
            conn,
            names=list(_SUPERSEDED_CRON_JOBS),
            tenant_prefix=f"{_SUPERSEDED_TENANT_LOG_PREFIX}%",
            task_reaper_prefix=f"{_SUPERSEDED_TASK_REAPER_PREFIX}%",
        )
    count = rows or 0
    if count:
        logger.info(
            "maintenance_supervisor: unscheduled %d superseded pg_cron job(s) "
            "(events/logs/IAM/tasks now driven by the supervisor).",
            count,
        )
    return count


async def register_supervisor_jobs(engine: Any) -> None:
    """Upsert all supervisor-owned job rows into ``platform.maintenance_schedule``.

    Idempotent: uses ON CONFLICT … DO UPDATE.  Designed to run once at
    CatalogModule startup before the supervisor loop is started.
    """
    repo = MaintenanceScheduleRepository()
    jobs = [
        (JOB_EVENTS_DLQ_PRUNE, _CADENCE_DLQ_PRUNE),
        (JOB_EVENTS_STUCK_REAPER, _CADENCE_STUCK_REAPER),
        (JOB_EVENTS_PENDING_ALERT, _CADENCE_PENDING_ALERT),
        (JOB_TENANT_LOGS_PRUNE, _CADENCE_TENANT_LOGS),
        (JOB_SYSTEM_LOGS_PRUNE, _CADENCE_SYSTEM_LOGS),
        (JOB_IAM_PRUNE, _CADENCE_IAM_PRUNE),
        (JOB_TASK_REAPER, _CADENCE_TASK_REAPER),
        (JOB_TASK_PARTITION_CREATE, _CADENCE_TASK_PARTITION_CREATE),
        (JOB_TASK_RETENTION, _CADENCE_TASK_RETENTION),
        (JOB_WORK_EVENTS_PARTITION_CREATE, _CADENCE_WORK_EVENTS_PARTITION_CREATE),
        (JOB_WORK_EVENTS_RETENTION, _CADENCE_WORK_EVENTS_RETENTION),
        (JOB_STORAGE_PARTITION_CREATE, _CADENCE_STORAGE_PARTITION_CREATE),
        (JOB_STORAGE_RETENTION, _CADENCE_STORAGE_RETENTION),
    ]
    async with managed_transaction(engine) as conn:
        for job_name, cadence in jobs:
            await repo.upsert_job(conn, job_name, interval_seconds=cadence)
        # Retire schedule rows for jobs this build no longer dispatches (e.g. the
        # #1807 work_index -> storage rename). Without this, get_due_jobs keeps
        # surfacing an orphaned row and _dispatch_job rejects its unknown name.
        pruned = await DQLQuery(
            "DELETE FROM platform.maintenance_schedule WHERE job_name = ANY(:names)",
            result_handler=ResultHandler.ROWCOUNT,
        ).execute(conn, names=list(_OBSOLETE_SCHEDULE_JOBS))
    logger.info(
        "maintenance_supervisor: registered %d job cadences in "
        "platform.maintenance_schedule (pruned %d obsolete row(s)).",
        len(jobs),
        pruned or 0,
    )


def build_supervisor_config() -> dict[str, Any]:
    """Read config values needed by the supervisor from the same sources the old cron code used.

    Mirrors events_module.PostgresEventsDriver.accumulation_policy, the
    EVENT_PROCESSING_TIMEOUT_MINUTES env var, and tasks_module.get_hard_retry_cap()
    — exact same sources, no drift.
    """
    dead_letter_days = int(os.getenv("GLOBAL_EVENT_RETENTION_DAYS", "30"))
    timeout_minutes = int(os.getenv("EVENT_PROCESSING_TIMEOUT_MINUTES", "15"))
    # Import lazily; events_module priority=10 starts before CatalogModule
    # (priority=20).  MAX_RETRIES is the single-source value — importing it
    # here prevents the supervisor's reaper threshold from silently drifting
    # away from the events accumulation policy.
    try:
        from dynastore.modules.events.events_module import MAX_RETRIES
        max_retries = MAX_RETRIES
    except Exception:
        max_retries = 3  # fallback if events module is not loaded
    # Import lazily to avoid circular import; tasks_module priority=15 starts before
    # CatalogModule (priority=20) which hosts the supervisor.
    try:
        from dynastore.modules.tasks.tasks_module import get_hard_retry_cap
        hard_cap = get_hard_retry_cap()
    except Exception:
        hard_cap = 5  # mirrors tasks_module._HARD_RETRY_CAP default
    return {
        "dead_letter_days": dead_letter_days,
        "timeout_minutes": timeout_minutes,
        "max_retries": max_retries,
        "hard_cap": hard_cap,
    }
