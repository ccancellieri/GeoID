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

"""
tasks/maintenance.py

Administrative tools for the DynaStore task queue.

All queries target the global ``tasks.tasks`` table (task DLQ) or the global
``tasks.events`` table (event DLQ). The ``catalog_id`` parameter refers to
the column value (catalog internal id, or the reserved sentinels 'platform'
/'system'), not a PostgreSQL schema qualifier.

These tools are wired into the existing retention-policy infrastructure and
can be called from admin endpoints or the MaintenanceSupervisor's periodic jobs.
"""

import logging
import re
from datetime import datetime, timezone, timedelta
from typing import List, Mapping, Optional, Dict, Any, Union

from sqlalchemy.ext.asyncio import AsyncEngine

from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
)
from dynastore.models.tasks import Task
from dynastore.modules.tasks.tasks_module import decode_cursor, get_task_schema

logger = logging.getLogger(__name__)

_SAFE_JSONB_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Task statistics
# ---------------------------------------------------------------------------


async def get_task_statistics(
    engine: AsyncEngine, catalog_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Returns a summary of task counts by status for monitoring / health-checks.
    If catalog_id is provided, scopes to that tenant.
    """
    task_schema = get_task_schema()
    schema_filter = ""
    params: Dict[str, Any] = {}
    if catalog_id is not None:
        schema_filter = "WHERE catalog_id = :catalog_id"
        params["catalog_id"] = catalog_id

    sql = f"""
        SELECT status, COUNT(*) AS cnt
        FROM {task_schema}.tasks
        {schema_filter}
        GROUP BY status;
    """
    async with managed_transaction(engine) as conn:
        rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
            conn, **params
        ) or []
    return {r["status"]: r["cnt"] for r in rows}


# ---------------------------------------------------------------------------
# Dead-letter management
# ---------------------------------------------------------------------------

#: Statuses an operator may requeue back to PENDING. DEAD_LETTER = retries
#: exhausted; FAILED = a permanent (retry=False) failure the operator has since
#: addressed. DISMISSED is intentionally excluded (an explicit cancel, not a
#: failure to retry).
_REQUEUEABLE_STATUSES = "('DEAD_LETTER', 'FAILED')"


async def _notify_requeued(engine: AsyncEngine, reason: str) -> None:
    """Wake dispatchers after a committed requeue so PENDING rows are claimed
    immediately instead of waiting for the next ~35s dispatcher poll.

    Mirrors the two-signal wakeup in ``tasks_module._redispatch_stuck_rows``:
    an in-process ``signal_bus`` emit (zero-latency, same pod) plus a
    ``pg_notify`` on ``new_task_queued`` so capable dispatchers on other pods
    also wake. Both are best-effort — a wakeup miss only delays pickup to the
    defensive poll, it never drops the requeued row. Claiming stays safe under
    the resulting wakeup flood because ``claim_batch`` uses FOR UPDATE SKIP
    LOCKED, so each row is still claimed exactly once.
    """
    from dynastore.tools.async_utils import signal_bus
    from dynastore.modules.tasks.queue import NEW_TASK_QUEUED

    try:
        await signal_bus.emit(NEW_TASK_QUEUED)
    except Exception as exc:  # noqa: BLE001
        logger.debug("requeue wakeup: signal_bus emit failed: %s", exc)
    try:
        async with managed_transaction(engine) as conn:
            await DQLQuery(
                "SELECT pg_notify('new_task_queued', :reason)",
                result_handler=ResultHandler.SCALAR,
            ).execute(conn, reason=reason)
    except Exception as exc:  # noqa: BLE001
        logger.debug("requeue wakeup: pg_notify failed: %s", exc)


async def list_dead_letter_tasks(
    engine: AsyncEngine,
    catalog_id: Optional[str] = None,
    *,
    collection_id: Optional[str] = None,
    task_type: Optional[str] = None,
    limit: int = 100,
    cursor: Optional[str] = None,
) -> List[Task]:
    """
    Returns tasks in DEAD_LETTER state for operator review.

    Ordered oldest-first (``timestamp ASC, task_id ASC``) for operator triage.
    Pass ``limit+1`` from the route layer to detect whether a next page exists;
    use :func:`encode_cursor` on the (limit+1)-th row to produce the token.

    When *cursor* is provided the query uses a keyset predicate
    ``(timestamp, task_id) > (cursor_ts, cursor_id)`` — the ASC counterpart
    of the ``<`` predicate used by the DESC task-list routes.

    Args:
        catalog_id: Scope to a single tenant (catalog internal id or reserved
                    sentinels 'system'/'platform'). ``None`` returns DLQ tasks
                    across all tenants (sysadmin-wide listing).
        collection_id: Further scope to a specific STAC collection. Ignored when
                       ``catalog_id`` is ``None`` (system scope has no collections).
        task_type: Optional filter to a specific task_type value.
        limit: Maximum rows to return. Pass ``limit+1`` to detect next page.
        cursor: Opaque keyset cursor from a previous page response.
    """
    task_schema = get_task_schema()
    filters = "WHERE status = 'DEAD_LETTER'"
    params: Dict[str, Any] = {"limit": limit}
    if catalog_id is not None:
        filters += " AND catalog_id = :catalog_id"
        params["catalog_id"] = catalog_id
        if collection_id is not None:
            filters += " AND collection_id = :collection_id"
            params["collection_id"] = collection_id
    if task_type is not None:
        filters += " AND task_type = :task_type"
        params["task_type"] = task_type

    if cursor is not None:
        c_ts, c_id = decode_cursor(cursor)
        params["c_ts"] = c_ts
        params["c_id"] = c_id
        sql = (
            f"SELECT * FROM {task_schema}.tasks "
            f"{filters} AND (timestamp, task_id) > (:c_ts, :c_id) "
            f"ORDER BY timestamp ASC, task_id ASC LIMIT :limit;"
        )
    else:
        sql = (
            f"SELECT * FROM {task_schema}.tasks "
            f"{filters} "
            f"ORDER BY timestamp ASC, task_id ASC LIMIT :limit;"
        )

    async with managed_transaction(engine) as conn:
        rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
            conn, **params
        ) or []
    return [Task.model_validate(r) for r in rows]


async def requeue_dead_letter_task(
    engine: AsyncEngine,
    task_id: str,
    reset_retries: bool = True,
    catalog_id: Optional[str] = None,
    *,
    collection_id: Optional[str] = None,
) -> bool:
    """
    Resets a DEAD_LETTER or FAILED task back to PENDING for another attempt.
    Only an operator with awareness of why it failed should call this.

    DEAD_LETTER = retries exhausted; FAILED = a permanent (retry=False) failure.
    Both are requeueable once the operator has addressed the root cause;
    DISMISSED is excluded (an explicit cancel, not a failure to retry).

    Args:
        reset_retries: If True, resets retry_count to 0 (full fresh start).
                       If False, keeps the count (will fail again on next exhaustion).
        catalog_id: If provided, the UPDATE only matches a task whose tenant tag
                    (the ``catalog_id`` column) equals this value — an atomic
                    tenant guard so a caller scoped to one tenant cannot requeue
                    another tenant's task by id. None = no tenant filter
                    (platform/sysadmin-wide requeue).
        collection_id: If provided together with ``catalog_id``, further guards
                       the UPDATE to tasks whose ``collection_id`` column matches.
                       Ignored when ``catalog_id`` is None.
    Returns:
        True if the task was found and requeued, False otherwise.
    """
    task_schema = get_task_schema()
    retry_clause = "retry_count = 0," if reset_retries else ""
    tenant_clause = "AND catalog_id = :catalog_id" if catalog_id is not None else ""
    collection_clause = (
        "AND collection_id = :collection_id"
        if catalog_id is not None and collection_id is not None
        else ""
    )
    sql = f"""
        UPDATE {task_schema}.tasks
        SET status       = 'PENDING',
            {retry_clause}
            locked_until = NULL,
            finished_at  = NULL,
            error_message = NULL,
            owner_id     = NULL
        WHERE task_id = :task_id
          AND status  IN {_REQUEUEABLE_STATUSES}
          {tenant_clause}
          {collection_clause}
        RETURNING task_id;
    """
    params: Dict[str, Any] = {"task_id": task_id}
    if catalog_id is not None:
        params["catalog_id"] = catalog_id
        if collection_id is not None:
            params["collection_id"] = collection_id
    async with managed_transaction(engine) as conn:
        row = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn, **params
        )
    if row:
        logger.info(f"Maintenance: Task {task_id} re-queued from DEAD_LETTER/FAILED.")
        await _notify_requeued(engine, "dlq_requeue")
        return True
    logger.warning(
        f"Maintenance: Task {task_id} not found in a requeueable "
        f"(DEAD_LETTER/FAILED) state."
    )
    return False


async def requeue_dead_letter_tasks_by_type(
    engine: AsyncEngine,
    task_type: str,
    *,
    since: Optional[datetime] = None,
    limit: int = 1000,
    reset_retries: bool = True,
    inputs_match: Optional[Mapping[str, str]] = None,
) -> int:
    """Bulk-requeue every DEAD_LETTER or FAILED row of ``task_type``
    (optionally filtered by ``finished_at >= since`` and/or JSONB equality on
    selected ``inputs`` keys).

    Companion to the reactive reaper added in #502: after fixing a
    persistent SCOPE drift, operators run this to replay reaped
    ``index_propagation`` rows in one call instead of looping
    :func:`requeue_dead_letter_task`.

    ``inputs_match`` is an AND-joined set of ``inputs->>'key' = value``
    JSONB equality filters. The caller picks the JSONB keys appropriate
    to the given ``task_type`` (e.g. ``{"catalog": "c1"}`` for
    ``index_propagation``). Keys MUST match ``[A-Za-z_][A-Za-z0-9_]*``
    so the literal can be safely inlined; anything else raises
    :class:`ValueError`.

    Returns the count of rows transitioned back to PENDING.
    """
    task_schema = get_task_schema()
    retry_clause = "retry_count = 0," if reset_retries else ""
    since_filter = "AND finished_at >= :since" if since is not None else ""

    extra_filters = ""
    params: Dict[str, Any] = {"task_type": task_type, "lim": limit}
    if since is not None:
        params["since"] = since
    if inputs_match:
        clauses: List[str] = []
        for raw_key, value in inputs_match.items():
            if not _SAFE_JSONB_KEY.match(raw_key):
                raise ValueError(
                    f"requeue_dead_letter_tasks_by_type: unsafe JSONB key "
                    f"{raw_key!r}; expected [A-Za-z_][A-Za-z0-9_]*",
                )
            param_name = f"jm_{raw_key}"
            clauses.append(f"AND inputs->>'{raw_key}' = :{param_name}")
            params[param_name] = value
        extra_filters = "\n              ".join(clauses)

    sql = f"""
        WITH victims AS (
            SELECT task_id, timestamp
            FROM {task_schema}.tasks
            WHERE status    IN {_REQUEUEABLE_STATUSES}
              AND task_type = :task_type
              {since_filter}
              {extra_filters}
            ORDER BY finished_at DESC
            LIMIT :lim
        )
        UPDATE {task_schema}.tasks t
        SET status        = 'PENDING',
            {retry_clause}
            locked_until  = NULL,
            finished_at   = NULL,
            error_message = NULL,
            owner_id      = NULL
        FROM victims v
        WHERE t.task_id   = v.task_id
          AND t.timestamp = v.timestamp
        RETURNING t.task_id;
    """
    async with managed_transaction(engine) as conn:
        rows = await DQLQuery(
            sql, result_handler=ResultHandler.ALL_DICTS,
        ).execute(conn, **params) or []
    count = len(rows)
    logger.info(
        "Maintenance: requeued %d DEAD_LETTER/FAILED row(s) of type %r%s%s.",
        count, task_type,
        f" since {since.isoformat()}" if since else "",
        f" matching {dict(inputs_match)!r}" if inputs_match else "",
    )
    if count:
        await _notify_requeued(engine, "dlq_requeue_bulk")
    return count


# ---------------------------------------------------------------------------
# Event dead-letter management
# ---------------------------------------------------------------------------


async def list_dead_letter_events(
    engine: AsyncEngine, catalog_id: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Returns all events in DEAD_LETTER state for operator review.

    Args:
        catalog_id: If provided, scopes to that tenant (the ``catalog_id``
                    column value). ``None`` returns DEAD_LETTER events across
                    all tenants (platform/sysadmin-wide listing).
    """
    task_schema = get_task_schema()
    schema_filter = ""
    params: Dict[str, Any] = {}
    if catalog_id is not None:
        schema_filter = "AND catalog_id = :catalog_id"
        params["catalog_id"] = catalog_id

    sql = f"""
        SELECT day, event_id, event_type, catalog_id,
               retry_count, max_retries, error_message, created_at
        FROM {task_schema}.events
        WHERE status = 'DEAD_LETTER'
          {schema_filter}
        ORDER BY created_at ASC;
    """
    async with managed_transaction(engine) as conn:
        return await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
            conn, **params
        ) or []


async def requeue_dead_letter_event(
    engine: AsyncEngine,
    event_id: str,
    day: Any,
    reset_retries: bool = True,
) -> bool:
    """Resets a single DEAD_LETTER event back to PENDING for another attempt.

    The composite primary key ``(day, event_id)`` is required because
    ``event_id`` alone is NOT unique across daily partitions.

    After a successful requeue, enqueues one dedup'd ``event_drain`` task on
    the same connection so the ``EventDrainTask`` is woken co-transactionally.

    Args:
        event_id: UUID of the event row (event_id column).
        day: DATE value of the partition key — must match the row's ``day``
             column exactly.
        reset_retries: If True (default), resets ``retry_count`` to 0 for a
                       fresh attempt budget.  If False, keeps the prior count.
    Returns:
        True if the event was found and requeued, False otherwise.
    """
    from dynastore.modules.tasks.events.events_emit import (  # noqa: PLC0415
        _enqueue_event_drain_trigger,
    )

    task_schema = get_task_schema()
    retry_clause = "retry_count = 0," if reset_retries else ""
    sql = f"""
        UPDATE {task_schema}.events
        SET status        = 'PENDING',
            {retry_clause}
            locked_until  = NULL,
            error_message = NULL,
            owner_id      = NULL,
            processed_at  = NULL
        WHERE day      = :day
          AND event_id = CAST(:event_id AS uuid)
          AND status   = 'DEAD_LETTER'
        RETURNING event_id;
    """
    async with managed_transaction(engine) as conn:
        row = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn, event_id=event_id, day=day
        )
        if row:
            await _enqueue_event_drain_trigger(conn)

    if row:
        logger.info(
            "Maintenance: event %s (day=%s) re-queued from DEAD_LETTER.", event_id, day
        )
        return True
    logger.warning(
        "Maintenance: event %s (day=%s) not found in DEAD_LETTER state.", event_id, day
    )
    return False


async def requeue_dead_letter_events_by_type(
    engine: AsyncEngine,
    event_type: str,
    *,
    since: Optional[datetime] = None,
    limit: int = 1000,
    reset_retries: bool = True,
) -> int:
    """Bulk-requeue every DEAD_LETTER event of ``event_type``.

    Selects up to ``limit`` rows (newest-first) and flips them back to
    PENDING in one CTE UPDATE.  After a non-zero requeue count, enqueues one
    dedup'd ``event_drain`` task on the same connection to wake the drain.

    Args:
        event_type: The ``event_type`` column value to replay (e.g.
                    ``"catalog_creation"``).
        since: If set, only replay rows whose ``created_at >= since``.
        limit: Maximum number of rows to requeue in one call (default 1 000).
        reset_retries: If True (default), resets ``retry_count`` to 0 for a
                       fresh attempt budget.  If False, keeps the prior count.
    Returns:
        Count of rows transitioned back to PENDING.
    """
    from dynastore.modules.tasks.events.events_emit import (  # noqa: PLC0415
        _enqueue_event_drain_trigger,
    )

    task_schema = get_task_schema()
    retry_clause = "retry_count = 0," if reset_retries else ""
    since_filter = "AND created_at >= :since" if since is not None else ""

    params: Dict[str, Any] = {"event_type": event_type, "lim": limit}
    if since is not None:
        params["since"] = since

    sql = f"""
        WITH victims AS (
            SELECT day, event_id
            FROM {task_schema}.events
            WHERE status     = 'DEAD_LETTER'
              AND event_type = :event_type
              {since_filter}
            ORDER BY created_at DESC
            LIMIT :lim
        )
        UPDATE {task_schema}.events e
        SET status        = 'PENDING',
            {retry_clause}
            locked_until  = NULL,
            error_message = NULL,
            owner_id      = NULL,
            processed_at  = NULL
        FROM victims v
        WHERE e.day      = v.day
          AND e.event_id = v.event_id
        RETURNING e.event_id;
    """
    async with managed_transaction(engine) as conn:
        rows = await DQLQuery(
            sql, result_handler=ResultHandler.ALL_DICTS,
        ).execute(conn, **params) or []
        count = len(rows)
        if count:
            await _enqueue_event_drain_trigger(conn)

    logger.info(
        "Maintenance: requeued %d DEAD_LETTER event(s) of type %r%s.",
        count,
        event_type,
        f" since {since.isoformat()}" if since else "",
    )
    return count


# ---------------------------------------------------------------------------
# Completed task purge
# ---------------------------------------------------------------------------


async def purge_completed_tasks(
    engine: AsyncEngine,
    catalog_id: Optional[str] = None,
    older_than: timedelta = timedelta(days=30),
) -> int:
    """
    Deletes COMPLETED and FAILED tasks older than the given age.
    Returns the number of rows deleted.
    """
    task_schema = get_task_schema()
    cutoff = _now() - older_than

    schema_filter = ""
    params: Dict[str, Any] = {"cutoff": cutoff}
    if catalog_id is not None:
        schema_filter = "AND catalog_id = :catalog_id"
        params["catalog_id"] = catalog_id

    sql = f"""
        DELETE FROM {task_schema}.tasks
        WHERE status IN ('COMPLETED', 'FAILED')
          AND finished_at < :cutoff
          {schema_filter}
        RETURNING task_id;
    """
    async with managed_transaction(engine) as conn:
        rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
            conn, **params
        ) or []
    count = len(rows)
    logger.info(f"Maintenance: Purged {count} completed/failed task(s).")
    return count


async def purge_dead_letter_tasks(
    engine: AsyncEngine,
    catalog_id: Optional[str] = None,
    older_than: timedelta = timedelta(days=90),
) -> int:
    """Hard-delete DEAD_LETTER tasks older than the given age.

    DEAD_LETTER rows that have exceeded the DLQ retention window are beyond
    operator intervention and should be removed to bound table growth.
    ``COALESCE(finished_at, timestamp)`` is used so rows without a
    ``finished_at`` (e.g. early DLQ without a completion write) are still
    eligible based on their creation timestamp.

    Returns the number of rows deleted.
    """
    task_schema = get_task_schema()
    cutoff = _now() - older_than

    schema_filter = ""
    params: Dict[str, Any] = {"cutoff": cutoff}
    if catalog_id is not None:
        schema_filter = "AND catalog_id = :catalog_id"
        params["catalog_id"] = catalog_id

    sql = f"""
        DELETE FROM {task_schema}.tasks
        WHERE status = 'DEAD_LETTER'
          AND COALESCE(finished_at, timestamp) < :cutoff
          {schema_filter}
        RETURNING task_id;
    """
    async with managed_transaction(engine) as conn:
        rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
            conn, **params
        ) or []
    count = len(rows)
    logger.info(f"Maintenance: Purged {count} stale DEAD_LETTER task(s).")
    return count


# ---------------------------------------------------------------------------
# Stale ACTIVE task cleanup (used by Janitor internally, exposed for tooling)
# ---------------------------------------------------------------------------


async def find_stale_active_tasks(
    engine_or_conn: Union[AsyncEngine, Any],
    catalog_id: Optional[str] = None,
    stale_threshold: timedelta = timedelta(minutes=10),
) -> List[Dict[str, Any]]:
    """
    Returns ACTIVE tasks whose heartbeat is stale or whose lock has expired.
    The Janitor calls this to decide whether to reset or dead-letter a task.

    Args:
        engine_or_conn: Either an AsyncEngine (creates own transaction) or an
            already-open connection (runs inside the caller's transaction).
        catalog_id: If provided, scopes to that tenant.
    """
    task_schema = get_task_schema()
    cutoff = _now() - stale_threshold

    schema_filter = ""
    params: Dict[str, Any] = {"cutoff": cutoff}
    if catalog_id is not None:
        schema_filter = "AND catalog_id = :catalog_id"
        params["catalog_id"] = catalog_id

    sql = f"""
        SELECT task_id, catalog_id, task_type, owner_id, retry_count, max_retries,
               timestamp, locked_until, last_heartbeat_at, inputs
        FROM {task_schema}.tasks
        WHERE status = 'ACTIVE'
          AND (
              locked_until < NOW()
              OR (last_heartbeat_at IS NOT NULL AND last_heartbeat_at < :cutoff)
              OR (last_heartbeat_at IS NULL AND locked_until < NOW())
          )
          {schema_filter};
    """
    query = DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS)

    # If a raw connection is passed, use it directly (keeps advisory lock active)
    if hasattr(engine_or_conn, 'execute'):
        return await query.execute(engine_or_conn, **params) or []

    async with managed_transaction(engine_or_conn) as conn:
        return await query.execute(conn, **params) or []
