#    Copyright 2025 FAO
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

All queries target the global ``tasks.tasks`` table. The ``schema_name``
parameter refers to the column value (tenant schema or 'system'), not a
PostgreSQL schema qualifier.

These tools are wired into the existing retention-policy infrastructure and
can be called from admin endpoints or scheduled pg_cron jobs.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any, Union

from sqlalchemy.ext.asyncio import AsyncEngine

from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    DDLQuery,
    ResultHandler,
    managed_transaction,
)
from dynastore.modules.tasks.models import Task, TaskStatusEnum
from dynastore.modules.tasks.tasks_module import get_task_schema

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Task statistics
# ---------------------------------------------------------------------------


async def get_task_statistics(
    engine: AsyncEngine, schema_name: Optional[str] = None
) -> Dict[str, Any]:
    """
    Returns a summary of task counts by status for monitoring / health-checks.
    If schema_name is provided, scopes to that tenant.
    """
    task_schema = get_task_schema()
    schema_filter = ""
    params: Dict[str, Any] = {}
    if schema_name is not None:
        schema_filter = "WHERE schema_name = :schema_name"
        params["schema_name"] = schema_name

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


async def list_dead_letter_tasks(
    engine: AsyncEngine, schema_name: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Returns all tasks in DEAD_LETTER state for operator review.
    """
    task_schema = get_task_schema()
    schema_filter = ""
    params: Dict[str, Any] = {}
    if schema_name is not None:
        schema_filter = "AND schema_name = :schema_name"
        params["schema_name"] = schema_name

    sql = f"""
        SELECT task_id, schema_name, task_type, owner_id, retry_count, max_retries,
               timestamp, finished_at, error_message, inputs
        FROM {task_schema}.tasks
        WHERE status = 'DEAD_LETTER'
          {schema_filter}
        ORDER BY timestamp ASC;
    """
    async with managed_transaction(engine) as conn:
        return await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
            conn, **params
        ) or []


async def requeue_dead_letter_task(
    engine: AsyncEngine, task_id: str, reset_retries: bool = True
) -> bool:
    """
    Resets a DEAD_LETTER task back to PENDING for another attempt.
    Only an operator with awareness of why it failed should call this.

    Args:
        reset_retries: If True, resets retry_count to 0 (full fresh start).
                       If False, keeps the count (will fail again on next exhaustion).
    Returns:
        True if the task was found and requeued, False otherwise.
    """
    task_schema = get_task_schema()
    retry_clause = "retry_count = 0," if reset_retries else ""
    sql = f"""
        UPDATE {task_schema}.tasks
        SET status       = 'PENDING',
            {retry_clause}
            locked_until = NULL,
            finished_at  = NULL,
            error_message = NULL
        WHERE task_id = :task_id
          AND status  = 'DEAD_LETTER'
        RETURNING task_id;
    """
    async with managed_transaction(engine) as conn:
        row = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn, task_id=task_id
        )
    if row:
        logger.info(f"Maintenance: Task {task_id} re-queued from DEAD_LETTER.")
        return True
    logger.warning(f"Maintenance: Task {task_id} not found in DEAD_LETTER state.")
    return False


# ---------------------------------------------------------------------------
# Completed task purge
# ---------------------------------------------------------------------------


async def purge_completed_tasks(
    engine: AsyncEngine,
    schema_name: Optional[str] = None,
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
    if schema_name is not None:
        schema_filter = "AND schema_name = :schema_name"
        params["schema_name"] = schema_name

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


# ---------------------------------------------------------------------------
# Stale ACTIVE task cleanup (used by Janitor internally, exposed for tooling)
# ---------------------------------------------------------------------------


async def find_stale_active_tasks(
    engine_or_conn: Union[AsyncEngine, Any],
    schema_name: Optional[str] = None,
    stale_threshold: timedelta = timedelta(minutes=10),
) -> List[Dict[str, Any]]:
    """
    Returns ACTIVE tasks whose heartbeat is stale or whose lock has expired.
    The Janitor calls this to decide whether to reset or dead-letter a task.

    Args:
        engine_or_conn: Either an AsyncEngine (creates own transaction) or an
            already-open connection (runs inside the caller's transaction).
        schema_name: If provided, scopes to that tenant.
    """
    task_schema = get_task_schema()
    cutoff = _now() - stale_threshold

    schema_filter = ""
    params: Dict[str, Any] = {"cutoff": cutoff}
    if schema_name is not None:
        schema_filter = "AND schema_name = :schema_name"
        params["schema_name"] = schema_name

    sql = f"""
        SELECT task_id, schema_name, task_type, owner_id, retry_count, max_retries,
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
