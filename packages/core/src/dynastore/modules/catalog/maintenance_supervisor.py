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

Drives deadline-insensitive periodic jobs off ``tasks.maintenance_schedule``
(jobs 4–12 from the #1911 spec), replacing ALL pg_cron registrations:
events DLQ/reaper/alert, IAM prune, and the three task-queue jobs (stuck-task
reaper, partition-create, retention). Tenant-logs / system-logs prune were
retired with PG log persistence (#2749) — logs are Elasticsearch-only now,
and this supervisor also drives their ES-side retention (``es_logs_retention``,
#2797), the one job in this module with no PG work of its own.

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
from typing import Any, ClassVar, Optional, Tuple, Union

from pydantic import Field

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig
from dynastore.modules.catalog.db_init.maintenance_schedule import (
    MaintenanceScheduleRepository,
)
from dynastore.modules.db_config.locking_tools import (
    check_extension_exists,
)
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    background_managed_transaction,
    managed_transaction,
)
from dynastore.tools.background_service import (
    Leadership,
    PeriodicService,
    PodPolicy,
    ServiceContext,
)
from dynastore.tools.protocol_helpers import get_engine

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


class HealthAlertConfig(PluginConfig):
    """Configuration for the maintenance health watchdog job."""

    _address: ClassVar[Tuple[str, ...]] = ("platform", "modules", "catalog")

    pending_age_seconds: Mutable[int] = Field(
        default=3600,
        ge=60,
        description=(
            "Age threshold (seconds) for stale PENDING events. "
            "Default: 3600 (1 hour). Events pending longer trigger an alert."
        ),
    )

    dead_letter_threshold: Mutable[int] = Field(
        default=100,
        ge=0,
        description=(
            "Count threshold for DEAD_LETTER queues. "
            "Default: 100. DLQ sizes exceeding this trigger an alert."
        ),
    )


async def load_health_alert_config() -> HealthAlertConfig:
    """Load ``HealthAlertConfig`` from the platform config store.

    Falls back to the default instance if the store is unavailable or
    the config has not been set.
    """
    try:
        from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        config_mgr = get_protocol(PlatformConfigsProtocol)
        if config_mgr is not None:
            cfg = await config_mgr.get_config(HealthAlertConfig)
            if isinstance(cfg, HealthAlertConfig):
                return cfg
    except Exception as exc:
        logger.warning(
            "maintenance_supervisor: failed to load HealthAlertConfig "
            "(%s) — using defaults.", exc,
        )
    return HealthAlertConfig()


# ---------------------------------------------------------------------------
# Advisory lock key — must not collide with SoftDeleteReaper (0x5D3A7E1F_C2B84961)
# or any other leader-elected loop.
# ---------------------------------------------------------------------------

_SUPERVISOR_ADVISORY_LOCK_KEY = 0x4D41494E_54454E41  # "MAINTENA" in ASCII hex

# ---------------------------------------------------------------------------
# Job names — must match the strings passed to repo.upsert_job at startup.
# ---------------------------------------------------------------------------

JOB_IAM_PRUNE = "iam_prune"
JOB_TASK_REAPER = "task_reaper"
JOB_TASK_PARTITION_CREATE = "task_partition_create"
JOB_TASK_RETENTION = "task_retention"
JOB_EVENTS_PARTITION_CREATE = "events_partition_create"
JOB_EVENTS_RETENTION = "events_retention"
JOB_STORAGE_PARTITION_CREATE = "storage_partition_create"
JOB_STORAGE_RETENTION = "storage_retention"
JOB_HEALTH_ALERT = "health_alert"
JOB_ES_LOGS_RETENTION = "es_logs_retention"

# Obsolete supervisor job names retired by #1807 renames. An environment that
# booted a prior build holds these rows in tasks.maintenance_schedule;
# register_supervisor_jobs now upserts the new names but never overwrites these,
# so the loop would dispatch an unknown job_name and record a recurring error.
# Prune them once at startup (DML on an operational table, not schema DDL).
# Safe no-op on a fresh DB.
_OBSOLETE_SCHEDULE_JOBS = (
    "work_index_partition_create",
    "work_index_retention",
    "work_events_partition_create",
    "work_events_retention",
    # Legacy events.events accumulation jobs, retired with the events plane
    # (#1807 P4): the in-process consumer is gone and tasks.events handles
    # its own retry/dead-letter + DROP-PARTITION retention.
    "events_dlq_prune",
    "events_stuck_reaper",
    "events_pending_alert",
    # PG log persistence removed entirely (#2749) — logs are
    # Elasticsearch-only now, so these prune jobs no longer exist to
    # dispatch. Pruned here so an already-deployed schedule row does not
    # make get_due_jobs surface an unknown job_name forever.
    "tenant_logs_prune",
    "system_logs_prune",
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
_CADENCE_IAM_PRUNE = 86400        # daily
_CADENCE_TASK_REAPER = 60         # every minute (matches old "* * * * *")
_CADENCE_TASK_PARTITION_CREATE = 86400   # daily (idempotent CREATE IF NOT EXISTS)
_CADENCE_TASK_RETENTION = 86400   # daily (idempotent DROP old partitions)
_CADENCE_EVENTS_PARTITION_CREATE = 86400   # daily
_CADENCE_EVENTS_RETENTION = 86400          # daily
_CADENCE_STORAGE_PARTITION_CREATE = 86400    # daily
_CADENCE_STORAGE_RETENTION = 86400           # daily
_CADENCE_HEALTH_ALERT = 300                  # every 5 minutes
_CADENCE_ES_LOGS_RETENTION = 86400           # daily

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
# internally across many schemas or stall waiting for IO.  Chosen as 15× the
# longest per-statement timeout so
# a legitimate slow run across many schemas still completes; an actual hung
# job is cancelled well before it wedges the supervisor for a full cycle.
JOB_DISPATCH_TIMEOUT_SECONDS = 900  # 15 minutes

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


async def _report_reaped_failures(engine: Any) -> int:
    """Emit ``task.failed`` for rows that the SQL reaper moved to DEAD_LETTER.

    The reaper runs entirely in SQL and cannot emit Python events.  This
    leader-only sweep runs right after ``_run_task_reaper`` on the same tick
    and picks up any rows the reaper just dead-lettered.

    Idempotency sentinel: after a successful event emission the row's
    ``error_message`` is suffixed with `` [reported]``.  The SELECT filters
    on ``error_message NOT LIKE '%[reported]%'`` so a restart cannot
    double-emit.  The sentinel is appended in the same DB transaction as the
    SELECT (via UPDATE ... RETURNING), which also acts as a FOR UPDATE lock
    so two concurrent leaders cannot race on the same row.

    Returns the number of rows for which an event was attempted (0 on a quiet
    tick).  All emission is best-effort — a failing event bus never raises
    out of this function.
    """
    schema = _TASKS_SCHEMA
    # Atomically claim unreported reaped rows: mark them [reported] and
    # return the data we need to emit the event.  FOR UPDATE SKIP LOCKED
    # inside the CTE prevents two concurrent leader-pods from double-emitting
    # if the advisory lock is somehow held by two winners simultaneously.
    sql = f"""
        WITH to_report AS (
            SELECT timestamp, task_id
            FROM {schema}.tasks
            WHERE status = 'DEAD_LETTER'
              AND error_message LIKE 'Reaped%%'
              AND error_message NOT LIKE '%%[reported]'
            FOR UPDATE SKIP LOCKED
            LIMIT 200
        )
        UPDATE {schema}.tasks t
        SET error_message = t.error_message || ' [reported]'
        FROM to_report r
        WHERE t.timestamp = r.timestamp AND t.task_id = r.task_id
        RETURNING t.task_id, t.task_type, t.caller_id, t.inputs, t.error_message;
    """
    async with background_managed_transaction(engine) as conn:
        rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(conn)

    if not rows:
        return 0

    import json as _json
    import uuid as _uuid

    for row in rows:
        try:
            task_id_raw = row.get("task_id")
            task_id = _uuid.UUID(str(task_id_raw)) if task_id_raw else None
            error_msg: str = row.get("error_message") or ""
            # Severity: hard-cap rows carry the "hard retry cap" phrase;
            # plain heartbeat-expiry rows are recoverable (work may be replayed).
            severity = (
                "unrecoverable"
                if "hard retry cap" in error_msg
                else "recoverable"
            )
            inputs_raw = row.get("inputs")
            if isinstance(inputs_raw, str):
                try:
                    inputs: Any = _json.loads(inputs_raw)
                except (ValueError, TypeError):
                    inputs = None
            else:
                inputs = inputs_raw
            catalog_id: Optional[str] = (inputs or {}).get("catalog_id") if isinstance(inputs, dict) else None
            task_type: str = row.get("task_type") or "unknown"
            task_id_str = str(task_id) if task_id else "unknown"

            if catalog_id:
                try:
                    from dynastore.modules.catalog.log_manager import log_error
                    await log_error(
                        catalog_id,
                        event_type="task.failed",
                        message=(
                            f"Task '{task_type}' ({task_id_str}) failed [{severity}]: {error_msg}"
                        ),
                        details={"task_type": task_type, "severity": severity},
                    )
                except Exception as log_exc:
                    logger.debug(
                        "maintenance_supervisor: log_manager unavailable for reaped task %s: %s",
                        task_id_str, log_exc,
                    )

            try:
                from dynastore.modules.catalog.event_service import emit_event
                await emit_event(
                    "task.failed",
                    task_id=task_id_str,
                    task_type=task_type,
                    error_message=error_msg,
                    severity=severity,
                    inputs=inputs,
                    originating_event=None,
                    catalog_id=catalog_id,
                )
            except Exception as emit_exc:
                logger.error(
                    "maintenance_supervisor: failed to emit task.failed for reaped task %s: %s",
                    task_id_str, emit_exc,
                )
        except Exception as row_exc:
            logger.error(
                "maintenance_supervisor: unexpected error reporting reaped task %s: %s",
                row.get("task_id"), row_exc,
            )

    return len(rows)


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
# Workclass partition maintenance jobs (events + storage)
# ---------------------------------------------------------------------------


async def _run_events_partition_create(conn: Any) -> int:
    """Invoke the daily create-ahead function for ``tasks.events``.

    The function ``{schema}.create_partitions_{schema}_events()`` is
    provisioned by ``ensure_workclass_storage_exists``; it opens a 30-day
    window of daily leaf partitions.  Returns 0 (function returns void).
    """
    schema = _TASKS_SCHEMA
    func_name = f"create_partitions_{schema}_events"
    await DQLQuery(
        f'SELECT "{schema}"."{func_name}"()',
        result_handler=ResultHandler.NONE,
    ).execute(conn)
    return 0


async def _run_events_retention(conn: Any) -> int:
    """Invoke the daily retention function for ``tasks.events``.

    The function ``{schema}.maintain_partitions_{schema}_events()`` is
    provisioned by ``ensure_workclass_storage_exists``; it drops daily
    partitions older than 30 days.  Returns 0 (function returns void).
    """
    schema = _TASKS_SCHEMA
    func_name = f"maintain_partitions_{schema}_events"
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


async def _run_health_alert(conn: Any) -> int:
    """Check maintenance health and emit alerts for anomalies.

    Restores the watchdogs lost in the pg_cron → MaintenanceSupervisor migration.
    Checks three conditions and emits structured events / logs at ERROR level:
    1. Sustained errors in maintenance_schedule jobs
    2. Stale PENDING events older than threshold
    3. DEAD_LETTER counts over threshold

    Returns the number of alerts emitted (0-3).
    """
    alerts = 0
    schema = _TASKS_SCHEMA
    cfg = await load_health_alert_config()

    # 1. Check for any maintenance job whose last recorded run ended in error
    #    within the past hour.  The schedule table stores only the most-recent
    #    status per job (no per-run history), so the check is "any error in the
    #    past hour", not a consecutive-failure count.
    error_jobs = await DQLQuery(
        """
        SELECT job_name, last_error, last_run_at
        FROM tasks.maintenance_schedule
        WHERE last_status = 'error'
          AND last_run_at IS NOT NULL
          AND last_run_at > NOW() - INTERVAL '1 hour'
        """,
        result_handler=ResultHandler.ALL_DICTS,
    ).execute(conn)

    if error_jobs:
        for job in error_jobs:
            logger.error(
                "maintenance_supervisor: ALERT - job %s in error state: %s",
                job["job_name"],
                job["last_error"],
            )
        alerts += 1

        try:
            from dynastore.modules.catalog.event_service import emit_event
            await emit_event(
                "maintenance.health_alert",
                alert_type="job_error",
                job_errors=[
                    {"job_name": j["job_name"], "last_error": j["last_error"]}
                    for j in error_jobs
                ],
            )
        except Exception as emit_exc:
            logger.error(
                "maintenance_supervisor: failed to emit maintenance.health_alert: %s",
                emit_exc,
            )

    # 2. Check for stale PENDING events (older than threshold)
    pending_threshold = cfg.pending_age_seconds
    stale_pending = await DQLQuery(
        f"""
        SELECT COUNT(*) as cnt
        FROM {schema}.events
        WHERE status = 'PENDING'
          AND created_at < NOW() - INTERVAL '1 second' * :threshold
        """,
        result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
    ).execute(conn, threshold=pending_threshold)

    stale_count = int(stale_pending) if stale_pending else 0
    if stale_count > 0:
        logger.error(
            "maintenance_supervisor: ALERT - %d PENDING events older than %ds",
            stale_count,
            pending_threshold,
        )
        alerts += 1

        try:
            from dynastore.modules.catalog.event_service import emit_event
            await emit_event(
                "maintenance.health_alert",
                alert_type="stale_pending_events",
                stale_count=stale_count,
                threshold_seconds=pending_threshold,
            )
        except Exception as emit_exc:
            logger.error(
                "maintenance_supervisor: failed to emit maintenance.health_alert: %s",
                emit_exc,
            )

    # 3. Check DEAD_LETTER counts in tasks and events
    dlq_threshold = cfg.dead_letter_threshold

    tasks_dlq = await DQLQuery(
        f"""
        SELECT COUNT(*) as cnt FROM {schema}.tasks
        WHERE status = 'DEAD_LETTER'
        """,
        result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
    ).execute(conn)
    tasks_dlq_count = int(tasks_dlq) if tasks_dlq else 0

    events_dlq = await DQLQuery(
        f"""
        SELECT COUNT(*) as cnt FROM {schema}.events
        WHERE status = 'DEAD_LETTER'
        """,
        result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
    ).execute(conn)
    events_dlq_count = int(events_dlq) if events_dlq else 0

    if tasks_dlq_count > dlq_threshold or events_dlq_count > dlq_threshold:
        logger.error(
            "maintenance_supervisor: ALERT - DEAD_LETTER counts exceed threshold: "
            "tasks=%d, events=%d (threshold=%d)",
            tasks_dlq_count,
            events_dlq_count,
            dlq_threshold,
        )
        alerts += 1

        try:
            from dynastore.modules.catalog.event_service import emit_event
            await emit_event(
                "maintenance.health_alert",
                alert_type="dead_letter_overflow",
                tasks_dlq_count=tasks_dlq_count,
                events_dlq_count=events_dlq_count,
                threshold=dlq_threshold,
            )
        except Exception as emit_exc:
            logger.error(
                "maintenance_supervisor: failed to emit maintenance.health_alert: %s",
                emit_exc,
            )

    return alerts


# ---------------------------------------------------------------------------
# ES log index retention (#2797)
# ---------------------------------------------------------------------------


async def _run_es_logs_retention() -> int:
    """Delete monthly ES log indices older than ``LogServiceConfig.retention_months``.

    ES-only work — no PG connection involved, unlike every other job in this
    dispatch table. ``retention_months`` is re-read on every tick (mirrors
    ``_run_health_alert``'s ``load_health_alert_config()`` — Mutable fields
    are meant to take effect without a restart), not captured once at
    startup like the task reaper's ``hard_cap``. Lazy-imported so
    ``modules/catalog`` stays importable on a SCOPE without
    ``module_elasticsearch`` installed.
    """
    from dynastore.modules.catalog.log_service_config import load as load_log_service_config
    from dynastore.modules.elasticsearch.log_retention import run_es_logs_retention

    cfg = await load_log_service_config()
    return await run_es_logs_retention(cfg.retention_months)


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
    if job_name == JOB_IAM_PRUNE:
        return await _run_iam_prune(conn)
    if job_name == JOB_TASK_REAPER:
        return await _run_task_reaper(conn, config["hard_cap"])
    if job_name == JOB_TASK_PARTITION_CREATE:
        return await _run_task_partition_create(conn)
    if job_name == JOB_TASK_RETENTION:
        return await _run_task_retention(conn)
    if job_name == JOB_EVENTS_PARTITION_CREATE:
        return await _run_events_partition_create(conn)
    if job_name == JOB_EVENTS_RETENTION:
        return await _run_events_retention(conn)
    if job_name == JOB_STORAGE_PARTITION_CREATE:
        return await _run_storage_partition_create(conn)
    if job_name == JOB_STORAGE_RETENTION:
        return await _run_storage_retention(conn)
    if job_name == JOB_HEALTH_ALERT:
        return await _run_health_alert(conn)
    if job_name == JOB_ES_LOGS_RETENTION:
        return await _run_es_logs_retention()
    raise ValueError(f"maintenance_supervisor: unknown job_name {job_name!r}")


# ---------------------------------------------------------------------------
# MaintenanceSupervisor
# ---------------------------------------------------------------------------


class MaintenanceSupervisor(PeriodicService):
    """Leader-elected supervisor that drives periodic maintenance jobs.

    Implements ``PeriodicService``: ``BackgroundSupervisor`` handles leadership
    election via ``_SUPERVISOR_ADVISORY_LOCK_KEY`` and the 60 s cadence.  Each
    tick calls ``run_once()`` which reads ``tasks.maintenance_schedule`` (no
    caching — it is the mutable source of truth) and dispatches every due job
    in its own bounded transaction.
    """

    name = "maintenance_supervisor"
    leadership = Leadership.LEADER_ONLY
    pod_policy = PodPolicy.SKIP_EPHEMERAL

    def __init__(self, config: dict[str, Any]) -> None:
        """Initialise with resolved job config values.

        *config* must contain:
          ``hard_cap`` (int)   — task reaper hard retry cap, from
                                 tasks_module.get_hard_retry_cap()
        """
        self._config = config
        self.cadence_seconds = 60.0
        self.lock_key: Optional[Union[int, str]] = _SUPERVISOR_ADVISORY_LOCK_KEY

    async def tick(self, ctx: ServiceContext) -> None:
        """One full supervisor tick: reclaim stale, then dispatch due jobs."""
        await self.run_once()

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
            async with background_managed_transaction(engine) as conn:
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
            async with background_managed_transaction(engine) as conn:
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
        async with background_managed_transaction(engine) as conn:
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
            async with background_managed_transaction(engine) as conn:
                await _set_statement_timeout(conn, _JOB_STATEMENT_TIMEOUT_MS)
                rows = await asyncio.wait_for(
                    _dispatch_job(job_name, conn, self._config),
                    timeout=JOB_DISPATCH_TIMEOUT_SECONDS,
                )
            logger.info(
                "maintenance_supervisor: job %r done — rows=%s.", job_name, rows
            )
            # Post-reaper sweep: emit task.failed for rows the SQL reaper moved
            # to DEAD_LETTER.  Runs in a separate transaction AFTER the reaper
            # transaction commits above so the reaped rows are visible.
            # Best-effort — a reporting hiccup must never brick the maintenance loop.
            if job_name == JOB_TASK_REAPER:
                try:
                    await _report_reaped_failures(engine)
                except Exception as _rep_exc:
                    logger.error(
                        "maintenance_supervisor: _report_reaped_failures failed: %s — "
                        "reaped tasks will be reported on the next tick.",
                        _rep_exc,
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
            async with background_managed_transaction(engine) as conn:
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
# Startup: register job cadences into tasks.maintenance_schedule
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
    """Upsert all supervisor-owned job rows into ``tasks.maintenance_schedule``.

    Idempotent: uses ON CONFLICT … DO UPDATE.  Designed to run once at
    CatalogModule startup before the supervisor loop is started.
    """
    repo = MaintenanceScheduleRepository()
    jobs = [
        (JOB_IAM_PRUNE, _CADENCE_IAM_PRUNE),
        (JOB_TASK_REAPER, _CADENCE_TASK_REAPER),
        (JOB_TASK_PARTITION_CREATE, _CADENCE_TASK_PARTITION_CREATE),
        (JOB_TASK_RETENTION, _CADENCE_TASK_RETENTION),
        (JOB_EVENTS_PARTITION_CREATE, _CADENCE_EVENTS_PARTITION_CREATE),
        (JOB_EVENTS_RETENTION, _CADENCE_EVENTS_RETENTION),
        (JOB_STORAGE_PARTITION_CREATE, _CADENCE_STORAGE_PARTITION_CREATE),
        (JOB_STORAGE_RETENTION, _CADENCE_STORAGE_RETENTION),
        (JOB_HEALTH_ALERT, _CADENCE_HEALTH_ALERT),
        (JOB_ES_LOGS_RETENTION, _CADENCE_ES_LOGS_RETENTION),
    ]
    async with managed_transaction(engine) as conn:
        for job_name, cadence in jobs:
            await repo.upsert_job(conn, job_name, interval_seconds=cadence)
        # Retire schedule rows for jobs this build no longer dispatches (e.g. the
        # #1807 work_index -> storage rename). Without this, get_due_jobs keeps
        # surfacing an orphaned row and _dispatch_job rejects its unknown name.
        pruned = await DQLQuery(
            "DELETE FROM tasks.maintenance_schedule WHERE job_name = ANY(:names)",
            result_handler=ResultHandler.ROWCOUNT,
        ).execute(conn, names=list(_OBSOLETE_SCHEDULE_JOBS))
    logger.info(
        "maintenance_supervisor: registered %d job cadences in "
        "tasks.maintenance_schedule (pruned %d obsolete row(s)).",
        len(jobs),
        pruned or 0,
    )


def build_supervisor_config() -> dict[str, Any]:
    """Read config values needed by the supervisor.

    The only runtime-tunable value left is the task reaper's hard retry cap,
    sourced from tasks_module.get_hard_retry_cap() — the events plane now owns
    its own retry/dead-letter and DROP-PARTITION retention on tasks.events, so
    the supervisor no longer carries the legacy events accumulation knobs.
    """
    # Import lazily to avoid circular import; tasks_module priority=15 starts before
    # CatalogModule (priority=20) which hosts the supervisor.
    try:
        from dynastore.modules.tasks.tasks_module import get_hard_retry_cap
        hard_cap = get_hard_retry_cap()
    except Exception:
        hard_cap = 5  # mirrors tasks_module._HARD_RETRY_CAP default
    return {
        "hard_cap": hard_cap,
    }
