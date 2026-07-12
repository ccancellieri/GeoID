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

# dynastore/modules/tasks/tasks_module.py

import asyncio
import base64
import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from typing import List, Optional, Any, Dict, AsyncGenerator, Tuple, Union
from dynastore.tools.cache import cached
from dynastore.models.driver_context import DriverContext
from dynastore.modules import ModuleProtocol
from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DDLBatch,
    DQLQuery,
    managed_transaction,
    background_managed_transaction,
    retry_on_transient_connect,
    ResultHandler,
    DbResource,
    run_in_event_loop,
)
from dynastore.modules.db_config.locking_tools import (
    check_function_exists,
    check_table_exists,
    check_trigger_exists,
    run_startup_ddl_tolerating_lock_timeout,
)
from dynastore.modules.db_config.maintenance_tools import ensure_schema_exists
from dynastore.models.protocols.task_queue import TaskQueueProtocol
from dynastore.modules.processes.protocols import ProcessRegistryProtocol
from dynastore.tools.background_service import (
    Leadership,
    PeriodicService,
    PodPolicy,
    ServiceContext,
)

from .models import Task, TaskCreate, TaskUpdate, TaskStatusEnum
from .workclass_ddl import (
    partition_create_ahead_function_name,
    partition_retention_function_name,
    render_partition_create_ahead_ddl,
    render_partition_retention_ddl,
)

logger = logging.getLogger(__name__)


def _serialize_inputs(inputs: Any) -> Optional[str]:
    """Serialize a task ``inputs`` payload for JSONB storage.

    Uses :class:`dynastore.tools.json.CustomJSONEncoder` so datetime /
    UUID / Decimal / shapely values survive the round-trip to PG.

    Without this, producers that pass ``model_dump()`` of a pydantic
    model containing a ``datetime`` (e.g. ``BulkCatalogReindexInputs``)
    blow up in ``tasks.create_task`` with ``Object of type datetime is
    not JSON serializable``. The task row is never created, the reindex
    never runs, and the catalog silently drifts from its search index.

    Returns ``None`` when ``inputs`` is empty so the DB column stays
    NULL (matches the legacy behaviour).
    """
    if not inputs:
        return None
    from dynastore.tools.json import CustomJSONEncoder
    return json.dumps(inputs, cls=CustomJSONEncoder)


def get_task_schema() -> str:
    """Returns the default schema for global tasks."""
    return os.getenv("DYNASTORE_TASK_SCHEMA", "tasks")


def get_task_lookback() -> timedelta:
    """Returns the lookback window for claim queries.

    Controls which partitions the planner scans — only partitions whose
    timestamp range overlaps [now - lookback, now] are touched.
    Configure via DYNASTORE_TASK_LOOKBACK_DAYS (default: 30).
    Set to match the retention period to avoid scanning pruned partitions.
    """
    days = int(os.getenv("DYNASTORE_TASK_LOOKBACK_DAYS", "30"))
    return timedelta(days=days)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        return max(0.0, float(raw))
    except ValueError:
        logger.warning("Invalid %s=%r; using %.1f.", name, raw, default)
        return default


# --- DDL Definitions ---

# --- Step 1: Table creation only (IF NOT EXISTS safe) ---

GLOBAL_TASKS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.tasks (
    task_id           UUID          NOT NULL,
    catalog_id        VARCHAR(255)  NOT NULL,
    scope             VARCHAR(50)   NOT NULL DEFAULT 'CATALOG',
    caller_id         VARCHAR(255),
    task_type         VARCHAR       NOT NULL,
    type              VARCHAR       NOT NULL DEFAULT 'task',
    execution_mode    VARCHAR       NOT NULL DEFAULT 'ASYNCHRONOUS',
    status            VARCHAR       NOT NULL DEFAULT 'PENDING',
    progress          INT           DEFAULT 0 CHECK (progress >= 0 AND progress <= 100),
    inputs            JSONB,
    outputs           JSONB,
    error_message     TEXT,
    dedup_key         VARCHAR(512),
    timestamp         TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    started_at        TIMESTAMPTZ,
    finished_at       TIMESTAMPTZ,
    dismiss_confirmed_at TIMESTAMPTZ,
    collection_id     VARCHAR(255),
    locked_until      TIMESTAMPTZ,
    last_heartbeat_at TIMESTAMPTZ,
    owner_id          VARCHAR(255),
    runner_ref        TEXT,
    retry_count       INT           NOT NULL DEFAULT 0,
    max_retries       INT           NOT NULL DEFAULT 3,
    PRIMARY KEY (timestamp, task_id)
) PARTITION BY RANGE (timestamp);
"""

# --- Step 2: Indexes and triggers (run AFTER migration so all columns exist) ---

GLOBAL_TASKS_INDEXES_DDL = """
-- Queue claim index: optimizes claim_next() SKIP LOCKED query
CREATE INDEX IF NOT EXISTS idx_tasks_queue
    ON {schema}.tasks (status, task_type, execution_mode, locked_until)
    WHERE status IN ('PENDING', 'ACTIVE');
CREATE INDEX IF NOT EXISTS idx_tasks_schema_status
    ON {schema}.tasks (catalog_id, status);
-- Dedup index: includes timestamp (partition key) as PG requires it for
-- unique indexes on partitioned tables. Per-partition uniqueness.
-- cross-partition dedup enforced at the application layer in enqueue().
CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_dedup
    ON {schema}.tasks (catalog_id, dedup_key, timestamp)
    WHERE dedup_key IS NOT NULL AND status NOT IN ('COMPLETED', 'FAILED', 'DEAD_LETTER');
CREATE INDEX IF NOT EXISTS idx_tasks_caller
    ON {schema}.tasks (caller_id);
CREATE INDEX IF NOT EXISTS idx_tasks_timestamp
    ON {schema}.tasks (timestamp DESC);
-- task_id lookup index: enables complete/fail/heartbeat without full partition scan
CREATE INDEX IF NOT EXISTS idx_tasks_task_id
    ON {schema}.tasks (task_id);
-- Listing indexes for the Tasks API (catalog scope, newest-first keyset pagination)
CREATE INDEX IF NOT EXISTS idx_tasks_scope_listing
    ON {schema}.tasks (catalog_id, timestamp DESC, task_id DESC);
CREATE INDEX IF NOT EXISTS idx_tasks_catalog_type_ts
    ON {schema}.tasks (catalog_id, type, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_tasks_collection
    ON {schema}.tasks (catalog_id, collection_id, timestamp DESC)
    WHERE collection_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_tasks_dead_letter
    ON {schema}.tasks (catalog_id, timestamp DESC)
    WHERE status = 'DEAD_LETTER';

CREATE OR REPLACE FUNCTION {schema}.notify_task_ready()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('new_task_queued', NEW.task_type);
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION {schema}.notify_task_status_changed()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('task_status_changed', NEW.task_type || ':' || NEW.status);
    RETURN NEW;
END;
$$;
"""

# Triggers are kept separate so creation is guarded with pg_trigger existence
# checks. DROP+CREATE TRIGGER takes AccessExclusiveLock on tasks.tasks and
# would deadlock against a concurrently-live dispatcher in another pod
# (RowExclusiveLock from claim_batch DML). Trigger body changes are a
# migration concern — never re-create in place on a hot table.
GLOBAL_TASKS_INSERT_TRIGGER_DDL = """
CREATE TRIGGER on_task_insert
    AFTER INSERT ON {schema}.tasks
    FOR EACH ROW
    WHEN (NEW.status = 'PENDING')
    EXECUTE FUNCTION {schema}.notify_task_ready();
"""

GLOBAL_TASKS_STATUS_TRIGGER_DDL = """
CREATE TRIGGER on_task_status_update
    AFTER UPDATE ON {schema}.tasks
    FOR EACH ROW
    WHEN (OLD.status IS DISTINCT FROM NEW.status)
    EXECUTE FUNCTION {schema}.notify_task_status_changed();
"""

# ---------------------------------------------------------------------------
# stuck-task reaper (driven by the MaintenanceSupervisor)
# ---------------------------------------------------------------------------
#
# Replaces the in-process dispatcher janitor (``_run_janitor``) — a
# DB-scheduled function so coordination happens at the DB layer, not
# through Python pods racing on a pg_try_advisory_xact_lock.
#
# At prod scale (GUNICORN_WORKERS=5 × MAX_SCALE=80 = up to 400 dispatcher
# loops), running the janitor in-process means 400 pods redundantly
# scanning ``tasks.tasks`` on every wakeup and a lot of wasted
# ``pg_try_advisory_xact_lock`` churn.  A pg_cron job is one coordinated
# actor running on the DB: zero pod connections held, no leader election.
#
# Semantics:
#   - Scans ACTIVE rows whose ``locked_until < NOW()`` (heartbeat expired
#     = owner pod died / got SIGKILL / network partition / OOM). Legacy
#     ``RUNNING`` rows are scanned too: builds prior to the ACTIVE cutover
#     stamped claimed rows as RUNNING, and both the heartbeat UPDATE and
#     this reaper used to gate on ACTIVE only, leaving those rows as
#     permanently un-reapable phantoms (#3297). In-process audit rows —
#     the one deliberate RUNNING writer left — never stamp ``locked_until``,
#     so the expiry predicate excludes them by construction.
#   - Resets to PENDING (retry_count+1) unless ``retry_count >= max_retries``,
#     in which case the row is moved to DEAD_LETTER.
#   - Emits ``pg_notify('new_task_queued', 'reaper')`` so live dispatchers
#     wake up immediately instead of waiting for their next signal_timeout.
#   - Uses ``FOR UPDATE SKIP LOCKED`` defensively (the cron job has only
#     one writer, but this prevents any interleaved heartbeat update from
#     blocking the reap pass).
# Platform-wide retry circuit breaker (overridden by TasksPluginConfig at
# lifespan startup). Defaults to 5 — enough to absorb transient cloud
# failures, low enough to bound a runaway loop. Read by claim_batch (rejects
# rows above the cap so dispatchers stop wasting cycles), reaper (DLQs once
# crossed), and fail_task (refuses retry once crossed).
_HARD_RETRY_CAP: int = 5


def get_hard_retry_cap() -> int:
    """Return the active platform-wide hard retry cap."""
    return _HARD_RETRY_CAP


def set_hard_retry_cap(value: int) -> None:
    """Set the active platform-wide hard retry cap (called from lifespan)."""
    global _HARD_RETRY_CAP
    if value < 1:
        raise ValueError(f"hard_retry_cap must be >= 1 (got {value})")
    _HARD_RETRY_CAP = int(value)


# ---------------------------------------------------------------------------
# Maintenance helper functions provisioned into the DB at startup.
# These are called by the MaintenanceSupervisor; pg_cron is NOT used.
# ---------------------------------------------------------------------------

# tasks.tasks partitions are MONTHLY (distinct from the DAILY events/storage
# partitions in workclass_ddl.py). Both function bodies are rendered by the
# shared template in workclass_ddl.py — see that module's docstring for why
# {schema} survives as a literal placeholder and why the regex uses the
# single-brace form \d{4}_\d{2}.
#
# Unlike events/storage (ephemeral queues), tasks.tasks rows carry audit
# value (DEAD_LETTER history, PENDING/ACTIVE in-flight work) that must
# outlive a 1-month partition boundary (#3216). purge_safe_statuses
# restricts age-based pruning — both the leaf DROP TABLE and the
# DEFAULT-partition DELETE — to rows already in a state nothing further
# happens to. DEAD_LETTER is deliberately excluded even though it is
# otherwise terminal: it has its own, longer grace period
# (TasksPluginConfig.dlq_max_age_days, default 90d) enforced row-by-row by
# TaskRetentionService.tick() below, independent of partition age.
_TASKS_PURGE_SAFE_STATUSES = (
    TaskStatusEnum.COMPLETED.value,
    TaskStatusEnum.FAILED.value,
    TaskStatusEnum.DISMISSED.value,
)

GLOBAL_TASKS_RETENTION_FUNC_DDL = render_partition_retention_ddl(
    table="tasks", granularity="month", retention=1,
    purge_safe_statuses=_TASKS_PURGE_SAFE_STATUSES,
)

GLOBAL_TASKS_PARTCREATE_FUNC_DDL = render_partition_create_ahead_ddl(
    table="tasks", granularity="month", window=4
)

# DEFAULT partition DDL: absorbs any timestamp outside the monthly partition range
# so inserts never fail with "no partition of relation found for row".
# Using CREATE TABLE IF NOT EXISTS makes this idempotent on all deploys.
# Adding a DEFAULT partition to an existing partitioned table does NOT require a
# full-table scan — PostgreSQL only checks for a conflicting DEFAULT.  Existing
# deploys also benefit: if a task arrives with an out-of-range timestamp it will
# land here rather than erroring.
GLOBAL_TASKS_DEFAULT_PARTITION_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.tasks_default PARTITION OF {schema}.tasks DEFAULT;
"""

# Drain workclasses get a reclaim grace (#3144 option A): their leases are
# renewed by BatchedHeartbeat DB writes that can lag behind a congested
# pooler while the run itself is healthy. Reaping the row the instant
# ``locked_until`` lapses re-queues work a live run is still processing —
# for the offloaded flavors that means the trigger can spawn a duplicate
# Cloud Run execution. Two heartbeat visibility windows (the runner default
# is 5 minutes — ``execution.py``) bound the reaper's aggressiveness for
# every drain flavor; a genuinely dead worker just recovers those minutes
# later, acceptable for a safety-valve path. The task_type list must match
# the drain task classes' ``task_type`` ClassVars (asserted by unit test).
DRAIN_WORKCLASS_TASK_TYPES: Tuple[str, ...] = (
    "event_drain",
    "storage_drain",
    "storage_drain_offload",
)
DRAIN_RECLAIM_GRACE_SECONDS: int = 600

_DRAIN_TYPES_SQL = ", ".join(f"'{t}'" for t in DRAIN_WORKCLASS_TASK_TYPES)

# NOTE: the 2-arg signature is FROZEN. CREATE OR REPLACE with an added
# parameter would create an *overload* (a second function identity), not a
# replacement — during a mixed-revision rollout the old 2-arg body would
# keep serving ``reap_stuck_tasks(3, N)`` calls indefinitely. Tunables land
# in the body via the module constants above instead.
GLOBAL_TASKS_REAPER_DDL = f"""
CREATE OR REPLACE FUNCTION {{schema}}.reap_stuck_tasks(
    p_max_retries INT DEFAULT 3,
    p_hard_cap INT DEFAULT 5
) RETURNS INTEGER LANGUAGE plpgsql AS $func$
DECLARE
    reaped INT;
    dead_lettered INT;
BEGIN
    WITH stuck AS (
        SELECT timestamp, task_id, retry_count, max_retries
        FROM {{schema}}.tasks
        -- 'RUNNING' is the pre-cutover claim status (kept as a legacy alias):
        -- rows born-claimed by older builds can neither heartbeat (ACTIVE-gated)
        -- nor complete, so they must be reapable here (#3297). Deliberate
        -- RUNNING audit rows carry NULL locked_until and never match the
        -- expiry predicate below.
        WHERE status IN ('ACTIVE', 'RUNNING')
          AND locked_until < NOW() - CASE
                WHEN task_type IN ({_DRAIN_TYPES_SQL})
                    THEN make_interval(secs => {DRAIN_RECLAIM_GRACE_SECONDS})
                ELSE INTERVAL '0 seconds'
            END
        FOR UPDATE SKIP LOCKED
    ),
    reset AS (
        UPDATE {{schema}}.tasks t
        SET status = CASE
                -- Per-row max_retries (typically 1 for Cloud Run jobs) wins
                -- when reached. The platform-wide hard_cap is the circuit
                -- breaker that fires even when per-row config is missing or
                -- mis-configured (defends against re-enqueue loops).
                WHEN s.retry_count + 1 >= LEAST(
                        COALESCE(s.max_retries, p_max_retries),
                        p_hard_cap
                    )
                    THEN 'DEAD_LETTER'
                ELSE 'PENDING'
            END,
            retry_count       = s.retry_count + 1,
            owner_id          = NULL,
            locked_until      = NULL,
            last_heartbeat_at = NULL,
            finished_at       = CASE
                WHEN s.retry_count + 1 >= LEAST(
                        COALESCE(s.max_retries, p_max_retries),
                        p_hard_cap
                    )
                    THEN NOW()
                ELSE NULL
            END,
            error_message     = CASE
                -- Mirrors the same LEAST(...) gate as status/finished_at above:
                -- without it, a per-row max_retries DLQ (the row's own cap
                -- below the platform hard cap) fell through to the generic
                -- heartbeat-expired text even though the row was DLQ'd, not
                -- requeued.
                WHEN s.retry_count + 1 >= LEAST(
                        COALESCE(s.max_retries, p_max_retries),
                        p_hard_cap
                    )
                    THEN CASE
                        WHEN s.retry_count + 1 >= p_hard_cap
                            THEN 'Reaped: hard retry cap (' || p_hard_cap || ') reached'
                        ELSE 'Reaped: max_retries (' ||
                             COALESCE(s.max_retries, p_max_retries) || ') reached'
                    END
                ELSE 'Reaped by {{schema}}.reap_stuck_tasks (heartbeat expired)'
            END
        FROM stuck s
        WHERE t.timestamp = s.timestamp AND t.task_id = s.task_id
        RETURNING t.task_id, t.status
    ),
    counted AS (
        SELECT
            COUNT(*) AS n_reaped,
            SUM(CASE WHEN status = 'DEAD_LETTER' THEN 1 ELSE 0 END) AS n_dead
        FROM reset
    )
    SELECT n_reaped, n_dead INTO reaped, dead_lettered FROM counted;

    IF dead_lettered > 0 THEN
        RAISE WARNING 'dynastore.task.hard_cap_hit: % task(s) moved to DEAD_LETTER in {{schema}} this pass', dead_lettered;
    END IF;

    IF reaped > 0 THEN
        PERFORM pg_notify('new_task_queued', 'reaper');
    END IF;

    RETURN reaped;
END
$func$;
"""



def _build_tasks_ddl_batch(schema: str) -> DDLBatch:
    """Build a module-level DDL batch scoped to *schema*.

    On warm starts, the sentinel (status-update trigger — the last object
    created) short-circuits the batch in one round-trip. Cold starts execute
    the full sequence (table, default partition, indexes + functions, triggers,
    and maintenance helper functions) under a single shared connection with
    nested savepoints.

    The DEFAULT partition is included only on fresh creates (guarded by the
    table existence check).  Attaching it to an already-populated table
    requires a full-table scan + AccessExclusiveLock, which violates the
    never-migrate-DB invariant; existing deploys rely on
    ensure_future_partitions(periods_ahead=12) to stay ahead instead.
    """

    def _check_insert(conn):
        return check_trigger_exists(conn, "on_task_insert", schema, table="tasks")

    def _check_status(conn):
        return check_trigger_exists(
            conn, "on_task_status_update", schema, table="tasks"
        )

    def _check_tasks_table(conn):
        return check_table_exists(conn, "tasks", schema)

    return DDLBatch(
        sentinel=DDLQuery(
            GLOBAL_TASKS_STATUS_TRIGGER_DDL, check_query=_check_status
        ),
        steps=[
            DDLQuery(GLOBAL_TASKS_TABLE_DDL, check_query=_check_tasks_table),
            # DEFAULT partition: absorbs out-of-range timestamps on any schema.
            # Uses CREATE TABLE IF NOT EXISTS (idempotent). Adding a DEFAULT
            # partition to an existing partitioned table does NOT require a
            # full table scan — PostgreSQL only needs to verify no conflicting
            # DEFAULT already exists. Safe on both fresh and existing deploys.
            DDLQuery(GLOBAL_TASKS_DEFAULT_PARTITION_DDL),
            DDLQuery(GLOBAL_TASKS_INDEXES_DDL),
            DDLQuery(GLOBAL_TASKS_INSERT_TRIGGER_DDL, check_query=_check_insert),
            DDLQuery(GLOBAL_TASKS_STATUS_TRIGGER_DDL, check_query=_check_status),
        ],
    )


async def _ensure_tasks_default_partition(conn: DbResource, schema: str) -> None:
    """Repair the default partition even when the warm-start DDL batch skips."""
    fq_name = f'"{schema}"."tasks_default"'
    exists = await DQLQuery(
        "SELECT to_regclass(:fq)",
        result_handler=ResultHandler.SCALAR,
    ).execute(conn, fq=fq_name)
    if exists is not None:
        return
    await DDLQuery(GLOBAL_TASKS_DEFAULT_PARTITION_DDL).execute(conn, schema=schema)


class TasksModule(TaskQueueProtocol, ProcessRegistryProtocol, ModuleProtocol):
    priority: int = 15  # Must start before CatalogModule (20) to create global tables

    # --- TasksProtocol CRUD (backward compat) ---

    async def create_task(
        self, engine: DbResource, task_data: Any, schema: str, initial_status: str = "PENDING"
    ) -> Any:
        return await create_task(engine, task_data, schema, initial_status=initial_status)

    async def update_task(
        self, conn: DbResource, task_id: uuid.UUID, update_data: Any, schema: str
    ) -> Optional[Any]:
        return await update_task(conn, task_id, update_data, schema)

    async def get_task(
        self, conn: DbResource, task_id: uuid.UUID, schema: str
    ) -> Optional[Any]:
        return await get_task(conn, task_id, schema)

    async def list_tasks(
        self, conn: DbResource, schema: str, limit: int = 20, offset: int = 0
    ) -> List[Any]:
        return await list_tasks(conn, schema, limit, offset)

    # Catalog-aware versions
    async def create_task_for_catalog(
        self, engine: DbResource, task_data: Any, catalog_id: str
    ) -> Any:
        return await create_task_for_catalog(engine, task_data, catalog_id)

    async def get_task_for_catalog(
        self, conn: DbResource, task_id: uuid.UUID, catalog_id: str
    ) -> Optional[Any]:
        return await get_task_for_catalog(conn, task_id, catalog_id)

    async def list_tasks_for_catalog(
        self, conn: DbResource, catalog_id: str, limit: int = 20, offset: int = 0
    ) -> List[Any]:
        return await list_tasks_for_catalog(conn, catalog_id, limit, offset)

    # --- TaskQueueProtocol queue operations ---

    async def enqueue(
        self,
        engine: Any,
        task_data: Any,
        schema_name: str,
        dedup_key: Optional[str] = None,
        execution_mode: str = "ASYNCHRONOUS",
        scope: str = "CATALOG",
    ) -> Optional[Any]:
        return await enqueue(engine, task_data, schema_name, dedup_key, execution_mode, scope)

    async def claim_next(
        self,
        engine: Any,
        async_task_types: List[str],
        sync_task_types: List[str],
        visibility_timeout: timedelta,
        owner_id: str,
    ) -> Optional[Dict[str, Any]]:
        return await claim_next(engine, async_task_types, sync_task_types, visibility_timeout, owner_id)

    async def claim_batch_tasks(
        self,
        engine: Any,
        async_task_types: List[str],
        sync_task_types: List[str],
        visibility_timeout: timedelta,
        owner_id: str,
        batch_size: int = 10,
    ) -> List[Dict[str, Any]]:
        return await claim_batch(engine, async_task_types, sync_task_types, visibility_timeout, owner_id, batch_size)

    async def complete(
        self,
        engine: Any,
        task_id: uuid.UUID,
        timestamp: Any,
        outputs: Optional[Any] = None,
    ) -> None:
        # ``complete_task`` now returns a bool (owner-guard match); the
        # TaskQueueProtocol contract is fire-and-forget — discard it.
        await complete_task(engine, task_id, timestamp, outputs)

    async def fail(
        self,
        engine: Any,
        task_id: uuid.UUID,
        timestamp: Any,
        error_message: str,
        retry: bool = True,
    ) -> None:
        # ``fail_task`` now returns a bool (owner-guard match); the
        # TaskQueueProtocol contract is fire-and-forget — discard it.
        await fail_task(engine, task_id, timestamp, error_message, retry)

    async def heartbeat(
        self,
        engine: Any,
        tasks: List[Tuple[uuid.UUID, datetime]],
        visibility_timeout: timedelta,
    ) -> None:
        return await heartbeat_tasks(engine, tasks, visibility_timeout)

    async def find_stale(
        self,
        engine: Any,
        stale_threshold: timedelta,
        schema_name: Optional[str] = None,
    ) -> List[Any]:
        return await find_stale_tasks(engine, stale_threshold, schema_name)

    async def cleanup_orphans(self, engine: Any, grace_period: timedelta) -> int:
        return await cleanup_orphan_tasks(engine, grace_period)

    async def get_capable_task_types(self) -> Dict[str, List[str]]:
        from dynastore.modules.tasks.runners import capability_map
        return {
            "ASYNCHRONOUS": capability_map.async_types,
            "SYNCHRONOUS": capability_map.sync_types,
        }

    # --- ProcessRegistryProtocol ---

    async def list_processes(self, tenant: Optional[str] = None) -> List[Any]:
        """Return all Process definitions from locally-installed tasks."""
        from dynastore.tasks import get_loaded_task_types, discover_tasks
        from dynastore.modules.gcp.tools.jobs import try_load_process_definition

        if not get_loaded_task_types():
            discover_tasks()
        result = []
        for task_type in get_loaded_task_types():
            defn = try_load_process_definition(task_type)
            if defn is not None:
                result.append(defn)
        return result

    async def get_process(self, process_id: str, tenant: Optional[str] = None) -> Optional[Any]:
        for process in await self.list_processes(tenant):
            if process.id == process_id:
                return process
        return None

    @asynccontextmanager
    async def lifespan(self, app_state: object) -> AsyncGenerator[None, None]:
        """
        Full lifecycle for the tasks subsystem:
          1. Initialise task singletons (runners, startup hooks) via manage_tasks.
          2. Start QueueListener and Dispatcher background loops (if a DB engine is available).
          3. On shutdown: signal dispatcher/listener to stop, then teardown singletons.
        """
        import asyncio
        from dynastore.modules.concurrency import get_background_executor
        from dynastore.tasks import manage_tasks

        logger.info("TasksModule: Initialising task singletons …")

        from dynastore.tools.protocol_helpers import resolve
        from dynastore.models.protocols import DatabaseProtocol

        shutdown_event = asyncio.Event()

        try:
            db = resolve(DatabaseProtocol)
            engine = db.get_any_engine()
            logger.debug(f"TasksModule: Resolved engine: {engine}")
        except (RuntimeError, AttributeError) as e:
            logger.warning(f"TasksModule: Failed to resolve engine: {e}")
            engine = None

        async with manage_tasks(app_state):
            from dynastore.modules.tasks.runners import get_all_runners_with_setup
            for _prio, runner in sorted(get_all_runners_with_setup(), key=lambda x: -x[0]):
                try:
                    await runner.setup(app_state)
                except Exception as e:
                    logger.error(
                        f"Runner {type(runner).__name__}.setup failed: {e}",
                        exc_info=True,
                    )
            logger.info("TasksModule: Task singletons active.")

            supervisor = None
            if engine is not None:
                executor = get_background_executor()
                schema = get_task_schema()

                # Load TasksPluginConfig BEFORE storage init so the reaper
                # cron command (registered inside ensure_task_storage_exists)
                # picks up the user-configured hard_retry_cap. claim_batch /
                # fail_task read the same module-level value at runtime.
                from dynastore.tools.discovery import get_protocol
                from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
                from dynastore.modules.tasks.tasks_config import TasksPluginConfig

                # Apply per-deployment JSON defaults (idempotent, advisory-locked)
                # before reading any config — so the seed values are visible to
                # the very first ``get_config`` call below. Safe no-op when the
                # ``defaults/`` folder isn't present (e.g. local dev / tests).
                from dynastore.modules.db_config.config_seeder import seed_default_configs
                try:
                    await seed_default_configs(engine)
                except Exception as e:  # noqa: BLE001 — never fail boot on seeds
                    logger.warning(f"TasksModule: config seeder skipped due to error: {e}")

                poll_interval = 30.0
                hard_cap = get_hard_retry_cap()
                cap_ttl = 60.0
                cap_refresh = 30.0
                sweep_interval = 60.0
                sweep_min_age = 300.0
                retention_sweep_interval = 86400.0
                drain_spawn_interval = 120.0
                config_mgr = get_protocol(PlatformConfigsProtocol)
                if config_mgr:
                    try:
                        tasks_config = await config_mgr.get_config(TasksPluginConfig, ctx=DriverContext(db_resource=engine))
                        if isinstance(tasks_config, TasksPluginConfig):
                            poll_interval = tasks_config.queue_poll_interval
                            hard_cap = tasks_config.hard_retry_cap
                            set_hard_retry_cap(hard_cap)
                            cap_ttl = tasks_config.capability_publisher_ttl_seconds
                            cap_refresh = tasks_config.capability_publisher_refresh_seconds
                            sweep_interval = tasks_config.proactive_sweep_interval_seconds
                            sweep_min_age = tasks_config.proactive_sweep_min_age_seconds
                            retention_sweep_interval = tasks_config.retention_sweep_interval_seconds
                            drain_spawn_interval = tasks_config.drain_spawn_interval_seconds
                    except Exception as e:
                        logger.warning(f"TasksModule: Failed to load TasksPluginConfig, defaulting to {poll_interval}s / hard_cap={hard_cap}: {e}")

                # TaskRoutingConfig is consumed lazily by CapabilityMap.refresh()
                # via PlatformConfigsProtocol; nothing to load eagerly here.
                # Register an apply-handler so live PUT /configs updates trigger
                # a re-narrowing of the dispatcher's capability set without
                # process restart. The handler also validates the new config
                # against this process's loaded task types (warns on routing
                # keys only loaded by other services) and emits an INFO summary
                # of what this service will claim post-refresh.
                from dynastore.modules.tasks.runners import capability_map as _capability_map
                from dynastore.modules.tasks.dispatcher import _SERVICE_NAME
                from dynastore.modules.tasks.routing.model import TaskRoutingConfig

                async def _on_routing_change(cfg, _catalog_id, _collection_id, _conn):
                    logger.info("TaskRoutingConfig changed — refreshing CapabilityMap.")
                    # Typo / cross-service-only diagnostic — informational only,
                    # since each service loads only the task types its SCOPE
                    # pulls in. A WARN here is the cheapest way to surface
                    # routing keys that nothing in this deployment can claim.
                    try:
                        from dynastore.tasks import get_loaded_task_types
                        known = set(get_loaded_task_types())
                        keys = (
                            set(getattr(cfg, "tasks", {}) or {})
                            | set(getattr(cfg, "processes", {}) or {})
                        )
                        unknown = sorted(t for t in keys if t not in known)
                        if unknown:
                            logger.warning(
                                "TaskRoutingConfig: %d routing key(s) not loaded "
                                "on this service ('%s') — likely typos OR types "
                                "only loaded elsewhere: %s",
                                len(unknown), _SERVICE_NAME, unknown,
                            )
                    except Exception as exc:  # noqa: BLE001 — never fail apply
                        logger.debug("Routing-key validation skipped: %s", exc)
                    await _capability_map.refresh()
                    logger.info(
                        "Service '%s' will claim async types: %s",
                        _SERVICE_NAME, _capability_map.async_types,
                    )

                TaskRoutingConfig.register_apply_handler(_on_routing_change)

                async def _on_tasks_config_change(cfg, _catalog_id, _collection_id, _conn):
                    if not isinstance(cfg, TasksPluginConfig):
                        return
                    try:
                        set_hard_retry_cap(cfg.hard_retry_cap)
                    except ValueError as exc:
                        logger.warning(
                            "TasksPluginConfig: rejected hard_retry_cap=%r (%s); "
                            "retaining %d.",
                            cfg.hard_retry_cap, exc, get_hard_retry_cap(),
                        )
                        return
                    logger.info(
                        "TasksPluginConfig changed — hard_retry_cap reapplied to %d.",
                        cfg.hard_retry_cap,
                    )

                TasksPluginConfig.register_apply_handler(_on_tasks_config_change)

                logger.info(f"TasksModule: hard_retry_cap = {hard_cap} (circuit breaker)")

                # Ensure the tasks table + current-month partition exist before
                # the dispatcher starts. The advisory lock must be held on the
                # SAME connection as the DDL, otherwise two concurrent revisions
                # can both observe "table missing" and race to create it (and
                # its partitions). Using the locked_conn yielded by
                # acquire_startup_lock guarantees that. A lock-timeout (a peer
                # pod still holding it, possibly pool-starved — #2333) is
                # tolerated rather than fatal (#2616): the DDL is idempotent,
                # so it is safe to run unlocked instead of crash-looping the
                # foundational module.
                async def _init_tasks_storage(conn: DbResource) -> None:
                    await ensure_task_storage_exists(conn, schema)
                    from dynastore.modules.tasks.workclass_ddl import (
                        ensure_workclass_storage_exists,
                    )
                    await ensure_workclass_storage_exists(conn, schema)

                await run_startup_ddl_tolerating_lock_timeout(
                    engine, f"tasks_storage_init.{schema}", _init_tasks_storage,
                )

                # Ensure configs.task_capability_registry exists before the
                # backstop/sweep loops start querying it. PlatformConfigService
                # owns this DDL but may have skipped it if DBService (priority 10)
                # was not yet up when DBConfigModule (priority 0) ran its lifespan.
                # TasksModule (priority 15) always runs after DBService, so by this
                # point the engine is present and the idempotent CREATE TABLE IF NOT
                # EXISTS is safe.  Advisory lock mirrors the tasks_storage_init
                # namespace pattern: one pod wins per cold-start, others skip.
                # A lock timeout is tolerated the same way as above.
                from dynastore.modules.db_config.typed_store.ddl import (
                    TASK_CAPABILITY_REGISTRY_DDL,
                )

                async def _init_task_capability_registry(conn: DbResource) -> None:
                    await DDLQuery(TASK_CAPABILITY_REGISTRY_DDL).execute(conn)
                    logger.info(
                        "TasksModule: configs.task_capability_registry ensured."
                    )

                await run_startup_ddl_tolerating_lock_timeout(
                    engine,
                    f"tasks_storage_init.{schema}.registry",
                    _init_task_capability_registry,
                )

                # Optional one-shot cleanup of pre-existing per-tenant
                # ``{schema}.tasks`` tables left over from the
                # cellular-safety pattern that this PR removes. Opt-in via
                # ``DYNASTORE_TASKS_DROP_LEGACY_TENANT_TABLES=1`` because it
                # issues DROP TABLE + pg_cron.unschedule per tenant — safe
                # only after a deploy where no service still depends on
                # the old shape. Idempotent: skips schemas without a
                # ``tasks`` table and any pg_cron job names that aren't
                # registered.
                if os.getenv(
                    "DYNASTORE_TASKS_DROP_LEGACY_TENANT_TABLES", "0",
                ).lower() in ("1", "true", "yes"):
                    try:
                        async with managed_transaction(engine) as cleanup_conn:
                            await _drop_legacy_tenant_tasks_tables(
                                cleanup_conn, global_schema=schema,
                            )
                    except Exception as exc:  # noqa: BLE001 — cleanup never blocks boot
                        logger.warning(
                            "TasksModule: legacy tenant-tasks cleanup skipped: %s",
                            exc,
                        )

                # Post-condition: verify current-month partition is visible on a
                # fresh connection. If it's not, crash loud — Cloud Run will
                # restart the pod rather than letting the dispatcher spin on
                # "relation does not exist".
                async with managed_transaction(engine) as probe_conn:
                    await _assert_current_partition_ready(probe_conn, schema)

                # Capability publisher (#502) initial refresh runs SYNCHRONOUSLY
                # before the dispatcher is submitted — otherwise create_task'd
                # coroutines race and the dispatcher could evaluate a row
                # before any pod has published a sentinel, causing a
                # false-positive DLQ. Fail-open if the cache isn't ready.
                from dynastore.modules.tasks.capability_publisher import (
                    _collect_local_capabilities,
                    _refresh_once,
                )
                try:
                    await _refresh_once(
                        _collect_local_capabilities(), ttl_seconds=cap_ttl,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.debug(
                        "TasksModule: initial capability publish skipped: %s",
                        exc,
                    )

                from dynastore.tools.background_service import (
                    BackgroundSupervisor,
                    ServiceContext as _ServiceContext,
                )
                # Task-queue LISTEN channels are registered with the shared
                # notification hub at ``queue`` import time; the single bridge
                # is owned by DBConfigModule's NotificationHubService. Import
                # the module here so its ``register_listen_channel`` calls run
                # before the hub next polls the registry.
                import dynastore.modules.tasks.queue  # noqa: F401  (registers channels)
                from dynastore.modules.tasks.dispatcher import DispatcherService
                from dynastore.modules.tasks.capability_publisher import (
                    CapabilityPublisherService,
                )
                from dynastore.modules.tasks.registry.publisher import (
                    RegistryHeartbeatService,
                )
                from dynastore.modules.tasks.drain_spawner import DrainSpawnerService
                from dynastore.modules.db_config.instance import (
                    get_service_name as _get_service_name,
                )

                bg_ctx = _ServiceContext(
                    engine=engine,
                    shutdown=shutdown_event,
                    is_ephemeral=bool(getattr(app_state, "ephemeral_job", False)),
                    name=_get_service_name() or "unknown",
                )
                supervisor = BackgroundSupervisor(executor)
                supervisor.register(DispatcherService())
                supervisor.register(StuckPendingWarnerService(schema=schema))
                supervisor.register(
                    ProactiveSweepService(
                        schema=schema,
                        interval_s=sweep_interval,
                        min_age_s=sweep_min_age,
                        capability_ttl_s=cap_ttl,
                    )
                )
                supervisor.register(
                    TaskRetentionService(interval_s=retention_sweep_interval)
                )
                supervisor.register(
                    DrainSpawnerService(interval_s=drain_spawn_interval)
                )
                # Async capability-sentinel refresh. Initial publish already ran
                # synchronously above (before the supervisor starts) so
                # dispatcher reactive-reaper checks never see an empty cache
                # during cold start.
                supervisor.register(
                    CapabilityPublisherService(
                        ttl_seconds=cap_ttl,
                        refresh_seconds=cap_refresh,
                    )
                )
                # Durable task-capability registry: self-publish this pod's task
                # inventory (version-gated via the shared cache, so the structural
                # write happens ~once per deploy) and heartbeat last_seen on the
                # same cadence as the capability publisher.
                supervisor.register(RegistryHeartbeatService(refresh_seconds=cap_refresh))
                supervisor.start(bg_ctx)
                logger.info(f"TasksModule: QueueListener (poll_interval={poll_interval}s) and Multi-Tenant Dispatcher launched.")
            else:
                logger.warning(
                    "TasksModule: No database engine available — "
                    "running without Dispatcher/QueueListener (on-premise / test mode)."
                )

            try:
                yield
            finally:
                shutdown_event.set()
                logger.info("TasksModule: Shutdown event set — QueueListener/Dispatcher stopping.")
                if supervisor is not None:
                    await supervisor.stop()


# --- Internal Query Objects ---
# All queries target the global tasks table. The `catalog_id` column
# distinguishes tenants; `get_task_schema()` returns the PostgreSQL schema
# that hosts the global table (default: "tasks").


async def _redispatch_stuck_rows(
    engine: DbResource,
    rows: List[Dict[str, Any]],
) -> None:
    """Re-signal the dispatcher for stuck-PENDING rows that are potentially
    claimable (not confirmed dead-capability).

    Dead-capability rows (``cap_live is False``) are already handled by the
    proactive capability sweep and will be DLQ'd on its next pass — skipping
    them here avoids a redundant wakeup that accomplishes nothing.

    Two signals are emitted so the self-heal works regardless of topology:

    * ``signal_bus.emit`` — wakes the in-process dispatcher immediately on
      the same event loop (most common case: single-pod dev/test env where
      pg_notify was simply not received at enqueue time).
    * ``SELECT pg_notify(...)`` — wakes all other pods' QueueListeners so
      a capable dispatcher on a different pod can also claim the row.

    Cross-pod dedup: ``claim_batch`` uses ``FOR UPDATE SKIP LOCKED`` —
    only one pod claims each row even when many dispatchers wake
    simultaneously. No double-execution is possible.

    Idempotent and fail-open: errors are logged and swallowed; the caller
    continues normally.
    """
    from dynastore.tools.async_utils import signal_bus
    from dynastore.modules.tasks.queue import NEW_TASK_QUEUED

    # Resolve capability liveness for all distinct caps in this batch so we
    # can skip confirmed-dead rows (capability sweep owns those).
    task_instance_cache: Dict[str, Any] = {}
    live_per_cap: Dict[Optional[str], Optional[bool]] = {}
    claimable: List[Dict[str, Any]] = []
    for row in rows:
        cap_id = _resolve_row_capability(row, task_instance_cache)
        if cap_id not in live_per_cap:
            live_per_cap[cap_id] = await _safe_is_live(cap_id) if cap_id else None
        if live_per_cap.get(cap_id) is not False:
            claimable.append(row)

    if not claimable:
        return

    # In-process wakeup — zero latency on the same event loop.
    try:
        await signal_bus.emit(NEW_TASK_QUEUED)
    except Exception as exc:  # noqa: BLE001
        logger.debug("stuck-pending redispatch: signal_bus emit failed: %s", exc)

    # Cross-pod wakeup via pg_notify so capable dispatchers on other pods
    # also wake and attempt to claim.
    try:
        async with managed_transaction(engine) as conn:
            await DQLQuery(
                "SELECT pg_notify('new_task_queued', 'stuck_pending_sweep')",
                result_handler=ResultHandler.SCALAR,
            ).execute(conn)
        logger.info(
            "stuck-pending redispatch: emitted new_task_queued for %d claimable row(s)",
            len(claimable),
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("stuck-pending redispatch: pg_notify failed: %s", exc)


class _BackgroundSlotBusy(Exception):
    """Fast-skip signal: the background DB-slot semaphore was saturated.

    ``background_managed_transaction`` caps its semaphore wait at ~2s and raises
    ``asyncio.TimeoutError`` so a maintenance pass skips rather than blocking.
    But ``asyncio.TimeoutError`` is in ``_TRANSIENT_CONNECT_EXCEPTIONS``, so a
    bare ``retry_on_transient_connect`` would retry the busy-skip 5× (~25s, 5
    warnings) and amplify pool pressure. Callers convert that timeout to this
    non-transient sentinel so only a real mid-query backend drop (08003) retries;
    the busy-skip propagates once and is logged at debug.
    """


class StuckPendingWarnerService(PeriodicService):
    """Periodic read-only scan for stuck PENDING tasks (retry_count=0).

    Elects a single leader pod (LEADER_ONLY) and skips ephemeral Cloud Run
    Job pods (SKIP_EPHEMERAL) — job pods claim one task and exit, never
    managing stuck rows. Resolves #2279 for this loop.

    A single pod scanning is sufficient: the redispatch it triggers
    (``_redispatch_stuck_rows``) emits ``pg_notify('new_task_queued', ...)``,
    which every pod's QueueListener receives — so cross-pod wakeup still
    reaches capable dispatchers on other pods even though only the leader
    runs the scan. Previously RUN_EVERYWHERE, this produced one identical
    scan per pod per cadence and a redundant ``pg_notify`` per stuck event
    at full scale.

    The scan, log, and redispatch body is implemented in tick() — PeriodicService
    supplies the loop, shutdown handling, and the initial tick. Note that
    PeriodicService ticks IMMEDIATELY on startup then on cadence (the old
    hand-rolled loop slept first); this is safe because the min_age guard
    (min_age_s) filters freshly-enqueued rows.
    """

    name = "stuck_pending_warner"
    leadership = Leadership.LEADER_ONLY
    pod_policy = PodPolicy.SKIP_EPHEMERAL
    lock_key: Optional[Union[int, str]] = None

    def __init__(
        self,
        *,
        schema: str,
        interval_s: float = 60.0,
        min_age_s: float = 600.0,
        sample_limit: int = 50,
    ) -> None:
        self._schema = schema
        self.cadence_seconds = interval_s
        self._min_age_s = min_age_s
        self._sample_limit = sample_limit
        # Build the DQLQuery once; it is stateless and safe to share across ticks.
        self._query = DQLQuery(
            (
                f'SELECT task_id, task_type, catalog_id, inputs, '  # nosec
                f'  EXTRACT(EPOCH FROM NOW() - timestamp) AS age_s '
                f'FROM "{schema}".tasks '
                f"WHERE status = 'PENDING' "
                f"  AND retry_count = 0 "
                f"  AND timestamp < NOW() - make_interval(secs => :min_age_s) "
                f"ORDER BY timestamp ASC LIMIT :sample_limit;"
            ),
            result_handler=ResultHandler.ALL_DICTS,
        )

    async def tick(self, ctx: ServiceContext) -> None:
        # Retry the read on a fresh connection when a transaction-mode pooler
        # tears down the backend mid-checkout (08003). Only the DB scan is
        # retried; the downstream emit/redispatch side-effects run once.
        @retry_on_transient_connect()
        async def _scan():
            try:
                async with background_managed_transaction(ctx.engine) as conn:
                    return await self._query.execute(
                        conn,
                        min_age_s=self._min_age_s,
                        sample_limit=self._sample_limit,
                    )
            except asyncio.TimeoutError as exc:
                # Background-slot saturation, not a connection drop: skip this
                # pass instead of retrying it. See _BackgroundSlotBusy.
                raise _BackgroundSlotBusy from exc

        try:
            rows = await _scan()
            rows = rows or []
            await _emit_stuck_pending_logs(rows)
            if rows:
                await _redispatch_stuck_rows(ctx.engine, rows)
        except _BackgroundSlotBusy:
            logger.debug("stuck-pending warner: skipped pass, background DB slots busy")
        except Exception as exc:  # noqa: BLE001 — never crash on diagnostic
            logger.warning("stuck-pending warner: scan failed: %s", exc)


async def sweep_wedged_provisioning_catalogs(
    engine: DbResource,
    min_age_s: float = 600.0,
    sample_limit: int = 50,
) -> int:
    """Drain still-pending checklist steps for catalogs stuck in ``provisioning``
    with no live or queued provisioning task (#1902).

    The normal path is: provisioner task runs → marks each step terminal →
    ``evaluate_checklist`` flips the catalog to ``ready``/``failed``.  When a
    task dies before marking its own steps (crash, SIGKILL, DB unavailable at
    mark time) the catalog is left ``provisioning`` indefinitely.

    This sweep detects that condition — ``provisioning_status = 'provisioning'``
    AND no ``PENDING``/``ACTIVE`` ``gcp_provision_catalog`` or ``catalog_provision``
    task pointing at the same ``catalog_id`` in its ``inputs`` — and calls
    ``drain_pending_checklist_steps`` on each such catalog.  The drain marks
    still-``pending`` steps ``"failed"``: a provisioning task that died without
    completing and is no longer being retried is a failure, so the catalog
    surfaces as ``failed`` (recoverable via reprovision) instead of staying
    wedged in ``provisioning`` or being misreported as ``ready``.

    Returns the number of catalogs drained this pass (0 = nothing to do).

    Idempotent and fail-open: errors per catalog are logged and skipped.
    The SQL is a read-only scan; the actual state mutation goes through the
    catalog service's JSONB update path (SELECT … FOR UPDATE + evaluate).
    """
    task_schema = get_task_schema()
    sql = f"""
        SELECT c.id AS catalog_id
        FROM catalog.catalogs c
        WHERE c.provisioning_status = 'provisioning'
          AND c.deleted_at IS NULL
          AND c.provisioning_checklist IS NOT NULL
          AND c.provisioning_checklist::text != '{{}}'
          AND c.updated_at < NOW() - make_interval(secs => :min_age_s)
          AND NOT EXISTS (
              SELECT 1
              FROM "{task_schema}".tasks t
              WHERE t.status IN ('PENDING', 'ACTIVE')
                AND t.task_type IN ('gcp_provision_catalog', 'catalog_provision')
                AND t.inputs->>'catalog_id' = c.id
          )
        LIMIT :sample_limit;
    """
    try:
        async with background_managed_transaction(engine) as conn:
            if not await check_table_exists(conn, "catalogs", "catalog"):
                return 0
            rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
                conn, min_age_s=min_age_s, sample_limit=sample_limit,
            )
    except TimeoutError:
        logger.warning(
            "sweep_wedged_provisioning_catalogs: background semaphore saturated — skipping pass."
        )
        return 0
    except Exception as exc:  # noqa: BLE001
        logger.warning("sweep_wedged_provisioning_catalogs: scan query failed: %s", exc)
        return 0

    rows = rows or []
    if not rows:
        return 0

    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols.catalogs import CatalogsProtocol

    catalogs = get_protocol(CatalogsProtocol)
    if catalogs is None:
        logger.warning(
            "sweep_wedged_provisioning_catalogs: CatalogsProtocol not available; "
            "skipping drain of %d wedged catalog(s).",
            len(rows),
        )
        return 0

    drained = 0
    for row in rows:
        catalog_id = row.get("catalog_id")
        if not catalog_id:
            continue
        try:
            # A catalog wedged in 'provisioning' with no live/queued task means
            # its provisioning task died without completing (crash / SIGKILL /
            # DB unavailable at mark time) and is not being retried. Under the
            # atomic provisioning contract that is a failure, not a success:
            # drain its pending steps to 'failed' so it surfaces as 'failed'
            # (operator reprovisions to recover) instead of silently 'ready'.
            updated = await catalogs.drain_pending_checklist_steps(
                catalog_id, terminal_status="failed",
            )
            if updated:
                drained += 1
                logger.warning(
                    "sweep_wedged_provisioning_catalogs: drained wedged catalog '%s'.",
                    catalog_id,
                )
        except Exception as exc:  # noqa: BLE001 — one bad catalog must not stop the rest
            logger.warning(
                "sweep_wedged_provisioning_catalogs: drain failed for catalog '%s': %s",
                catalog_id, exc,
            )
    return drained


# Lock namespace for the backstop pass below — see
# modules/tasks/durable/lock_registry.py, the central registry of every
# static lock/lease key.
_MANDATORY_BACKSTOP_LOCK_NAME = "dynastore.mandatory.backstop"


async def _run_mandatory_backstop_pass(
    engine: DbResource, schema: str, *, ttl_grace_seconds: float, min_age_s: float
) -> None:
    """Leader-coordinated (advisory-locked) backstop pass: log mandatory-ownership
    violations and DLQ capability-less unclaimable PENDING rows. Runs on whichever
    pod wins ``pg_try_advisory_xact_lock`` for this pass; others return immediately.
    Fail-open: any error is logged and swallowed."""
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, background_managed_transaction,
        retry_on_transient_connect,
    )
    from dynastore.modules.tasks.dispatcher import (
        _stable_advisory_lock_key, sweep_unclaimable_rows,
        auto_requeue_recovered_mandatory,
    )
    from dynastore.modules.tasks.mandatory import check_mandatory_ownership

    lock_key = _stable_advisory_lock_key(_MANDATORY_BACKSTOP_LOCK_NAME)

    # The whole locked pass is idempotent (a backstop sweep), so on a
    # transaction-mode pooler tearing down the backend mid-pass (08003) we
    # re-run it on a fresh connection rather than waiting a full tick. The
    # advisory xact lock is re-acquired each attempt; a non-transient error
    # falls through to the warning below after the retry budget is spent.
    @retry_on_transient_connect()
    async def _locked_pass() -> None:
        try:
            async with background_managed_transaction(engine) as conn:
                got = await DQLQuery(
                    "SELECT pg_try_advisory_xact_lock(:k) AS got",
                    result_handler=ResultHandler.ONE_DICT,
                ).execute(conn, k=lock_key)
                if not got or not got.get("got"):
                    return  # another pod owns this pass; advisory xact lock held until txn end
                # All three sub-calls receive the locked connection so the whole pass
                # runs on one pool slot instead of opening three additional connections.
                await check_mandatory_ownership(engine, ttl_grace_seconds=ttl_grace_seconds, conn=conn)
                await sweep_unclaimable_rows(
                    engine, schema, ttl_grace_seconds=ttl_grace_seconds, min_age_s=min_age_s, conn=conn,
                )
                await auto_requeue_recovered_mandatory(
                    engine, ttl_grace_seconds=ttl_grace_seconds, conn=conn,
                )
        except asyncio.TimeoutError as exc:
            # Background-slot saturation, not a connection drop: skip this pass
            # instead of retrying it. See _BackgroundSlotBusy.
            raise _BackgroundSlotBusy from exc

    try:
        await _locked_pass()
    except _BackgroundSlotBusy:
        logger.debug("proactive_sweep: mandatory backstop skipped, background DB slots busy")
    except Exception as exc:  # noqa: BLE001 — never crash the sweep loop
        logger.warning("proactive_sweep: mandatory backstop pass failed: %s", exc)


class ProactiveSweepService(PeriodicService):
    """Periodic DLQ sweep for PENDING rows whose required capability has no
    live worker.

    Runs on every pod (RUN_EVERYWHERE) and skips ephemeral Cloud Run Job
    pods (SKIP_EPHEMERAL). Resolves #2279 for this loop.

    The in-body advisory lock (_run_mandatory_backstop_pass /
    pg_try_advisory_xact_lock) is preserved — do NOT add loop-level
    LEADER_ONLY leadership here; the per-pass locking already deduplicates
    across pods for the backstop step. PeriodicService supplies the outer
    loop and shutdown handling. The first pass is delayed by one cadence by
    default so cold-start module registration can settle before the sweep
    starts resolving task routing and collection config.
    """

    name = "proactive_capability_sweep"
    leadership = Leadership.RUN_EVERYWHERE
    pod_policy = PodPolicy.SKIP_EPHEMERAL
    lock_key: Optional[Union[int, str]] = None

    def __init__(
        self,
        *,
        schema: str,
        interval_s: float = 60.0,
        min_age_s: float = 300.0,
        max_caps_per_pass: int = 50,
        capability_ttl_s: float = 90.0,
    ) -> None:
        self._schema = schema
        self.cadence_seconds = interval_s
        self.initial_delay_seconds = _env_float(
            "DYNASTORE_PROACTIVE_SWEEP_INITIAL_DELAY_SECONDS",
            interval_s,
        )
        self._min_age_s = min_age_s
        self._max_caps_per_pass = max_caps_per_pass
        self._capability_ttl_s = capability_ttl_s

    async def tick(self, ctx: ServiceContext) -> None:
        from dynastore.modules.tasks.capability_oracle import (
            TASK_TYPE_CAPABILITY_INPUTS_KEY,
        )
        from dynastore.modules.tasks.dispatcher import sweep_dead_capability_rows

        try:
            for task_type, inputs_key in TASK_TYPE_CAPABILITY_INPUTS_KEY.items():
                if ctx.shutdown.is_set():
                    return
                try:
                    cap_ids = await _distinct_pending_capability_ids(
                        ctx.engine, self._schema, task_type, inputs_key,
                        self._min_age_s, self._max_caps_per_pass,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "proactive_sweep: distinct query failed (task_type=%s): %s",
                        task_type, exc,
                    )
                    continue
                for cap_id in cap_ids:
                    if ctx.shutdown.is_set():
                        return
                    try:
                        dlqed = await sweep_dead_capability_rows(
                            ctx.engine, cap_id, task_type=task_type,
                        )
                        if dlqed > 0:
                            logger.info(
                                "proactive_sweep: DLQ'd %d row(s) "
                                "capability=%s task_type=%s",
                                dlqed, cap_id, task_type,
                            )
                    except Exception as exc:  # noqa: BLE001
                        logger.warning(
                            "proactive_sweep: sweep failed "
                            "(capability=%s task_type=%s): %s",
                            cap_id, task_type, exc,
                        )
            await _run_mandatory_backstop_pass(
                ctx.engine, self._schema,
                ttl_grace_seconds=self._capability_ttl_s,
                min_age_s=self._min_age_s,
            )
            try:
                drained = await sweep_wedged_provisioning_catalogs(
                    ctx.engine, min_age_s=self._min_age_s,
                )
                if drained:
                    logger.info(
                        "proactive_sweep: drained %d wedged provisioning catalog(s).",
                        drained,
                    )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "proactive_sweep: wedged-provisioning sweep failed: %s", exc,
                )
        except Exception as exc:  # noqa: BLE001 — never crash the loop
            logger.warning("proactive_sweep: pass failed: %s", exc)


class TaskRetentionService(PeriodicService):
    """Leader-elected periodic service that enforces task retention policy.

    Runs on a configurable cadence (default daily). On each tick it:
    1. Purges COMPLETED/FAILED tasks older than ``terminal_task_ttl_days``.
    2. Hard-deletes DEAD_LETTER tasks older than ``dlq_max_age_days``.
    3. Emits a ``tasks.health_alert / dead_letter_overflow`` event when the
       global DEAD_LETTER count exceeds ``dlq_alert_threshold``.

    Only one pod runs each tick (LEADER_ONLY) to avoid duplicate DELETEs.
    Skipped on ephemeral Cloud Run Jobs (SKIP_EPHEMERAL).  Config values are
    read live inside tick() so hot-reload via TasksPluginConfig takes effect
    on the next cadence without a pod restart.
    """

    name = "task_retention"
    leadership = Leadership.LEADER_ONLY
    pod_policy = PodPolicy.SKIP_EPHEMERAL
    lock_key: Optional[Union[int, str]] = "dynastore.task_retention"

    def __init__(self, *, interval_s: float = 86400.0) -> None:
        self.cadence_seconds = interval_s

    async def tick(self, ctx: ServiceContext) -> None:
        from dynastore.modules.tasks.tasks_config import TasksPluginConfig
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
        from dynastore.modules.tasks.maintenance import (
            purge_completed_tasks,
            purge_dead_letter_tasks,
            get_task_statistics,
        )
        from dynastore.models.driver_context import DriverContext

        _cfg: Optional[TasksPluginConfig] = None
        config_mgr = get_protocol(PlatformConfigsProtocol)
        if config_mgr:
            try:
                _raw = await config_mgr.get_config(
                    TasksPluginConfig, ctx=DriverContext(db_resource=ctx.engine)
                )
                if isinstance(_raw, TasksPluginConfig):
                    _cfg = _raw
            except Exception as exc:  # noqa: BLE001
                logger.warning("task_retention: failed to load TasksPluginConfig: %s", exc)

        ttl_days = _cfg.terminal_task_ttl_days if _cfg else 30
        dlq_max_days = _cfg.dlq_max_age_days if _cfg else 90
        dlq_threshold = _cfg.dlq_alert_threshold if _cfg else 100

        try:
            purged = await purge_completed_tasks(
                ctx.engine, older_than=timedelta(days=ttl_days)
            )
            if purged:
                logger.info("task_retention: purged %d terminal task(s)", purged)
        except Exception as exc:  # noqa: BLE001
            logger.warning("task_retention: terminal-task purge failed: %s", exc)

        try:
            archived = await purge_dead_letter_tasks(
                ctx.engine, older_than=timedelta(days=dlq_max_days)
            )
            if archived:
                logger.info("task_retention: purged %d stale DEAD_LETTER task(s)", archived)
        except Exception as exc:  # noqa: BLE001
            logger.warning("task_retention: DLQ age-cap purge failed: %s", exc)

        try:
            stats = await get_task_statistics(ctx.engine)
            dlq_count = int(stats.get("DEAD_LETTER", 0))
            if dlq_count > dlq_threshold:
                logger.error(
                    "task_retention: ALERT — DEAD_LETTER count %d exceeds threshold %d",
                    dlq_count,
                    dlq_threshold,
                )
                try:
                    from dynastore.modules.catalog.event_service import emit_event  # noqa: PLC0415
                    await emit_event(
                        "tasks.health_alert",
                        alert_type="dead_letter_overflow",
                        tasks_dlq_count=dlq_count,
                        threshold=dlq_threshold,
                    )
                except Exception as emit_exc:  # noqa: BLE001
                    logger.warning(
                        "task_retention: failed to emit tasks.health_alert: %s", emit_exc
                    )
        except Exception as exc:  # noqa: BLE001
            logger.warning("task_retention: DLQ count check failed: %s", exc)


async def _distinct_pending_capability_ids(
    engine: DbResource,
    schema: str,
    task_type: str,
    inputs_key: str,
    min_age_s: float,
    sample_limit: int,
) -> List[str]:
    """Return distinct capability ids referenced by PENDING/retry=0 rows
    of ``task_type`` older than ``min_age_s``.

    ``inputs_key`` is interpolated into the JSONB extraction; it must
    already have been validated by ``TASK_TYPE_CAPABILITY_INPUTS_KEY``
    membership (SQL identifier safety). The ``sample_limit`` caps a
    single pass — pathological backlogs from many distinct dead caps
    still drain over consecutive passes.
    """
    # ``inputs_key`` is validated SQL identifier — see comment on
    # TASK_TYPE_CAPABILITY_INPUTS_KEY in capability_oracle.py.
    sql = (
        f'SELECT DISTINCT inputs->>\'{inputs_key}\' AS cap_id '  # nosec — validated key
        f'FROM "{schema}".tasks '
        f"WHERE status = 'PENDING' "
        f"  AND retry_count = 0 "
        f"  AND task_type = :task_type "
        f"  AND inputs->>'{inputs_key}' IS NOT NULL "
        f"  AND timestamp < NOW() - make_interval(secs => :min_age_s) "
        f"LIMIT :sample_limit;"
    )
    query = DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS)
    async with background_managed_transaction(engine) as conn:
        rows = await query.execute(
            conn,
            task_type=task_type,
            min_age_s=min_age_s,
            sample_limit=sample_limit,
        )
    return [r["cap_id"] for r in (rows or []) if r.get("cap_id")]


async def _emit_stuck_pending_logs(rows: List[Dict[str, Any]]) -> None:
    """Pre-resolve capability ids with per-cycle memoization, query the
    oracle once per distinct capability, then emit one WARN per row.

    Coalescing matters in the common pathological case: dozens of rows
    share the same dead ``indexer_id`` (a single SCOPE-drift fault
    backlogs many propagations) and we would otherwise hit the cache
    once per row.
    """
    cap_per_row: List[Optional[str]] = []
    task_instance_cache: Dict[str, Any] = {}
    for row in rows:
        cap_per_row.append(_resolve_row_capability(row, task_instance_cache))

    live_cache: Dict[str, Optional[bool]] = {}
    for cap_id in {c for c in cap_per_row if c}:
        live_cache[cap_id] = await _safe_is_live(cap_id)

    for row, cap_id in zip(rows, cap_per_row):
        cap_live = live_cache.get(cap_id) if cap_id else None
        logger.warning(
            "stuck-pending: task '%s' (%s, schema=%s) has been "
            "PENDING for %.0fs with retry_count=0 — %s",
            row["task_id"], row["task_type"], row.get("catalog_id"),
            row["age_s"],
            _stuck_pending_hint(row["task_type"], cap_id, cap_live),
        )


def _resolve_row_capability(
    row: Dict[str, Any], task_instance_cache: Dict[str, Any],
) -> Optional[str]:
    """Return the capability id required to claim ``row`` or ``None``.

    Uses ``task_instance_cache`` to avoid re-walking the task registry
    when many rows share a ``task_type`` (the common case).
    """
    try:
        from dynastore.tasks import get_task_instance
        from dynastore.modules.tasks.capability_oracle import (
            resolve_required_capability,
        )

        task_type = row["task_type"]
        if task_type not in task_instance_cache:
            task_instance_cache[task_type] = get_task_instance(task_type)
        task_instance = task_instance_cache[task_type]
        inputs_raw = row.get("inputs")
        if isinstance(inputs_raw, str):
            try:
                inputs_raw = json.loads(inputs_raw)
            except Exception:  # noqa: BLE001
                inputs_raw = None
        payload = {"inputs": inputs_raw} if inputs_raw is not None else {}
        return resolve_required_capability(task_instance, payload)
    except Exception:  # noqa: BLE001 — diagnostic must never crash
        return None


async def _safe_is_live(capability_id: str) -> Optional[bool]:
    """Wrap :func:`is_capability_live` so the warner never crashes on a
    cache failure. ``None`` falls back to the generic routing hint.
    """
    try:
        from dynastore.modules.tasks.capability_oracle import is_capability_live

        return bool(await is_capability_live(capability_id))
    except Exception:  # noqa: BLE001
        return None


def _stuck_pending_hint(
    task_type: str, capability_id: Optional[str], cap_live: Optional[bool],
) -> str:
    """Produce the actionable tail of the stuck-pending log message.

    When the task declares a required capability and the liveness oracle
    answers ``False``, surface that the reactive reaper (#502) will DLQ
    the row on the next dispatcher pass — the operator should fix SCOPE
    drift, not the routing config. When the oracle says ``True`` the row
    is genuinely starved (transient pool issue), and when no capability
    is declared we fall back to the original generic hint.
    """
    if capability_id and cap_live is False:
        return (
            f"capability={capability_id!r} live=false "
            f"→ reactive reaper will DLQ on next dispatcher pass "
            f"(SCOPE/B6 drift — module not loaded in any reachable pool)"
        )
    if capability_id and cap_live is True:
        return (
            f"capability={capability_id!r} live=true "
            f"→ transient pool starvation; capable workers are advertised "
            f"but none has claimed yet."
        )
    return (
        f"check TaskRoutingConfig.tasks[{task_type!r}] / .processes[{task_type!r}] "
        f"for typos or for a service that should claim it but isn't deployed."
    )


async def _assert_current_partition_ready(conn: DbResource, schema: str) -> None:
    """
    Readiness probe: confirm the current-month partition of {schema}.tasks is
    visible before starting the dispatcher. Raises RuntimeError on failure.

    Uses to_regclass() on the fully-qualified child partition name (tasks_YYYY_MM)
    so that the check is reliable under concurrent DDL — unlike pg_tables, which
    can briefly lag.
    """
    now = datetime.now(timezone.utc)
    partition_name = f"tasks_{now.strftime('%Y_%m')}"
    fq_name = f'"{schema}"."{partition_name}"'
    result = await DQLQuery(
        "SELECT to_regclass(:fq)",
        result_handler=ResultHandler.SCALAR,
    ).execute(conn, fq=fq_name)
    if result is None:
        raise RuntimeError(
            f"TasksModule: current-month partition {schema}.{partition_name} is "
            "missing after ensure_task_storage_exists — refusing to start dispatcher."
        )
    logger.info(f"TasksModule: partition {schema}.{partition_name} is ready.")


async def ensure_task_storage_exists(conn: DbResource, schema: str):
    """
    Provision the global ``tasks.tasks`` partitioned table + its indexes,
    triggers, pg_notify functions, monthly partitions, and maintenance
    helper functions called by the MaintenanceSupervisor.

    There is exactly ONE legitimate caller: ``TasksModule.lifespan`` at app
    startup, with ``schema == get_task_schema()`` (default ``"tasks"``).
    Multi-tenancy is column-based — ``catalog_id`` on each task row carries
    the catalog internal id; the table itself is never duplicated per
    tenant. Callers that pass a catalog/tenant schema are a bug: they would
    create an unread shadow table per tenant.

    All steps are idempotent and must run every time on the global schema:
    table/index DDL use IF NOT EXISTS, partition helpers all check-then-create.
    The table-existence check is NOT used to short-circuit the rest of this
    function — otherwise a restart after a month rollover would never create
    the current-month partition and the dispatcher would hit "relation does
    not exist" on claim_batch.

    pg_cron is NOT used.  The three periodic jobs (reaper, partition-create,
    retention) are driven by the MaintenanceSupervisor leader-elected loop
    via ``tasks.maintenance_schedule`` — registered at CatalogModule startup
    by ``register_supervisor_jobs``.

    Raises ``RuntimeError`` if ``schema`` is anything other than
    ``get_task_schema()``. This is a hard guard against the pattern that
    polluted catalog schemas with empty ``{tenant}.tasks`` tables prior to
    this fix — every read/write path pins the global schema, so a tenant
    copy is dead weight.

    Note: events table is now owned by EventsModule (priority=11).
    """
    from dynastore.modules.db_config import maintenance_tools

    global_schema = get_task_schema()
    if schema != global_schema:
        raise RuntimeError(
            f"ensure_task_storage_exists: refusing DDL on non-global schema "
            f"{schema!r} (expected {global_schema!r}). Tasks live in a single "
            f"global partitioned table; per-tenant tasks tables are a bug "
            f"and were never read. Use catalog_id='{schema}' on the row "
            f"instead (the tenant discriminator column)."
        )

    # Ensure schema exists first
    await ensure_schema_exists(conn, schema)

    # Single module-level batch: on warm starts the sentinel
    # (status-update trigger) short-circuits everything in one round-trip.
    # Cold starts create the table (+ DEFAULT partition), indexes, notify
    # functions, and both triggers in order under nested savepoints.
    await _build_tasks_ddl_batch(schema).execute(conn, schema=schema)
    await _ensure_tasks_default_partition(conn, schema)

    # Ensure current + future partitions exist.
    # Critical path — must succeed for the dispatcher to start.
    await maintenance_tools.ensure_future_partitions(
        conn,
        schema=schema,
        table="tasks",
        interval="monthly",
        periods_ahead=12,
        column="timestamp",
    )

    # Provision maintenance helper functions called by the supervisor.
    # These are CREATE OR REPLACE — idempotent and always up-to-date.
    await DDLQuery(GLOBAL_TASKS_REAPER_DDL).execute(conn, schema=schema)

    # The retention/partition-create function names embed {schema} inside a
    # quoted identifier (e.g. "{schema}"."maintain_partitions_{schema}_tasks"),
    # so the auto-inferred existence check has to resolve that placeholder
    # before checking pg_proc -- the subtle failure fixed in #3117. Pass an
    # explicit check_query so the duplicate-object peer-race recovery on
    # these two statements is self-documenting rather than relying on that
    # inference.
    def _check_tasks_retention(conn):
        return check_function_exists(
            conn, partition_retention_function_name(table="tasks", schema=schema), schema
        )

    def _check_tasks_partcreate(conn):
        return check_function_exists(
            conn, partition_create_ahead_function_name(table="tasks", schema=schema), schema
        )

    await DDLQuery(
        GLOBAL_TASKS_RETENTION_FUNC_DDL, check_query=_check_tasks_retention
    ).execute(conn, schema=schema)
    await DDLQuery(
        GLOBAL_TASKS_PARTCREATE_FUNC_DDL, check_query=_check_tasks_partcreate
    ).execute(conn, schema=schema)

    logger.info(
        "TasksModule: provisioned tasks storage + maintenance helper functions "
        "for schema %r (reaper/partition-create/retention driven by supervisor).",
        schema,
    )


_DISCOVER_LEGACY_TENANT_TASKS_SQL = """
SELECT n.nspname AS schema_name
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = 'tasks'
  AND c.relkind IN ('p', 'r')
  AND n.nspname <> :global_schema
  AND n.nspname NOT IN ('pg_catalog', 'information_schema');
"""


async def _drop_legacy_tenant_tasks_tables(
    conn: DbResource, *, global_schema: str,
) -> int:
    """One-shot cleanup of legacy per-tenant ``{schema}.tasks`` tables.

    Removes the dead-weight tables (and their per-schema pg_cron reaper /
    retention / partition-creation registrations) that earlier revisions of
    ``tiles_preseed`` / ``tiles_export`` / ``dimensions_materialize`` created
    each time they ran on a fresh schema. The current code paths never read
    these tables — every CRUD path pins ``global_schema`` — so dropping them
    is non-destructive to live work; the only effect is that pg_cron stops
    burning a reaper-per-minute on empty tables.

    Idempotent: gracefully skips schemas that have no ``tasks`` table, and
    pg_cron jobs that aren't registered (``cron.unschedule`` raises when the
    job name is unknown — we catch and continue per-job).

    Returns the number of tenant ``tasks`` tables dropped.
    """
    skipped = {global_schema, "pg_catalog", "information_schema"}

    # Discover tenant schemas that currently host a ``tasks`` table.
    # Restrict to partitioned tables (``relkind = 'p'``) so we never touch a
    # hypothetical non-partitioned ``tasks`` that some unrelated extension
    # might own — the legacy pattern always created a RANGE-partitioned one.
    rows = await DQLQuery(
        _DISCOVER_LEGACY_TENANT_TASKS_SQL,
        result_handler=ResultHandler.ALL,
    ).execute(conn, global_schema=global_schema)
    tenant_schemas = [
        r[0] for r in (rows or []) if r and r[0] not in skipped
    ]

    if not tenant_schemas:
        logger.info(
            "TasksModule: no legacy tenant tasks tables found; cleanup is a no-op.",
        )
        return 0

    dropped = 0
    for tenant_schema in tenant_schemas:
        # pg_cron jobs for legacy per-tenant tasks tables are swept globally by
        # ``unschedule_superseded_cron_jobs`` at CatalogModule startup — no
        # per-schema unschedule needed here.

        # Drop the partitioned tasks table itself. ``CASCADE`` removes the
        # monthly child partitions + dependent indexes/triggers in one shot.
        # We trust the discovered schema name (PG returned it from pg_class)
        # but still quote it via ``format(%I)`` for defence-in-depth.
        try:
            await DDLQuery(
                f'DROP TABLE IF EXISTS "{tenant_schema}".tasks CASCADE'
            ).execute(conn)
            dropped += 1
            logger.info(
                "TasksModule: dropped legacy tenant tasks table %s.tasks",
                tenant_schema,
            )
        except Exception as exc:  # noqa: BLE001 — log and continue, never fail boot
            logger.warning(
                "TasksModule: could not drop %s.tasks: %s",
                tenant_schema, exc,
            )

    logger.info(
        "TasksModule: legacy tenant-tasks cleanup complete — %d table(s) dropped, "
        "%d schema(s) examined.",
        dropped, len(tenant_schemas),
    )
    return dropped


# --- Public API Functions ---


# Catalog-aware helper functions using CatalogsProtocol
async def _resolve_catalog_schema(
    catalog_id: str, db_resource: Optional[DbResource] = None
) -> str:
    """
    Resolves the physical schema for a catalog using CatalogsProtocol.
    This decouples tasks from direct catalog module dependencies.
    """
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols import CatalogsProtocol

    catalog_protocol = get_protocol(CatalogsProtocol)
    if not catalog_protocol:
        raise RuntimeError(
            "CatalogsProtocol not available - CatalogModule not initialized"
        )

    schema = await catalog_protocol.resolve_physical_schema(
        catalog_id, ctx=DriverContext(db_resource=db_resource) if db_resource else None
    )
    if not schema:
        raise ValueError(f"Cannot resolve schema for catalog '{catalog_id}'")
    return schema


async def create_task_for_catalog(
    engine: DbResource, task_data: TaskCreate, catalog_id: str
) -> Optional[Task]:
    """
    Creates a new task within a catalog's schema.
    Uses CatalogsProtocol to resolve the physical schema.

    If `task_data.dedup_key` is set and a non-terminal task already exists
    with that key, returns None instead of creating a duplicate. This protects
    every event-driven caller against at-least-once redelivery (Pub/Sub push,
    internal CatalogEvent retries, etc.) with a single consistent contract.
    """
    async with managed_transaction(engine) as conn:
        schema = await _resolve_catalog_schema(catalog_id, conn)
        return await create_task(engine, task_data, schema)


async def get_task_for_catalog(
    conn: DbResource, task_id: uuid.UUID, catalog_id: str
) -> Optional[Task]:
    """
    Retrieves a task from a catalog's schema.
    Uses CatalogsProtocol to resolve the physical schema.
    """
    schema = await _resolve_catalog_schema(catalog_id, conn)
    return await get_task(conn, task_id, schema)


async def list_tasks_for_catalog(
    conn: DbResource,
    catalog_id: str,
    limit: int = 20,
    offset: int = 0,
    kind: Optional[str] = None,
    *,
    status: Optional[str] = None,
    task_type: Optional[str] = None,
    collection_id: Optional[str] = None,
    asset_id: Optional[str] = None,
    created_before: Optional[datetime] = None,
    cursor: Optional[str] = None,
) -> List[Task]:
    """
    Lists tasks from a catalog's schema.
    Uses CatalogsProtocol to resolve the physical schema.

    Pass ``kind`` to narrow results to a specific task type (forwarded to
    :func:`list_tasks`).  All additional keyword filters are forwarded as-is.
    """
    schema = await _resolve_catalog_schema(catalog_id, conn)
    return await list_tasks(
        conn, schema, limit, offset, kind=kind,
        status=status,
        task_type=task_type,
        collection_id=collection_id,
        asset_id=asset_id,
        created_before=created_before,
        cursor=cursor,
    )


async def update_task_for_catalog(
    conn: DbResource, task_id: uuid.UUID, update_data: TaskUpdate, catalog_id: str
) -> Optional[Task]:
    """
    Updates a task in a catalog's schema.
    Uses CatalogsProtocol to resolve the physical schema.
    """
    schema = await _resolve_catalog_schema(catalog_id, conn)
    return await update_task(conn, task_id, update_data, schema)


# --- Low-level functions ---
# The `schema` parameter in these functions refers to the `catalog_id` column
# value (e.g. catalog internal id "s_abc123" or the sentinel "system"), NOT the
# PostgreSQL schema that hosts the table.  The table lives in `get_task_schema()`.tasks.

async def get_active_task_by_dedup_key(
    engine: DbResource, schema: str, dedup_key: str
) -> Optional[Dict[str, Any]]:
    """Return ``{task_id, status}`` of the non-terminal task carrying
    ``dedup_key`` in ``schema`` (catalog_id column), or None.

    Mirrors the dedup pre-check inside :func:`create_task`.  The spawn API uses
    it to return the existing :class:`TaskRef` on a dedup hit, so a retried
    spawn is idempotent instead of failing with 409.
    """
    task_schema = get_task_schema()
    sql = f"""
        SELECT task_id, status FROM {task_schema}.tasks
        WHERE dedup_key = :dedup_key
          AND catalog_id = :catalog_id
          AND status NOT IN ('COMPLETED', 'FAILED', 'DEAD_LETTER')
        ORDER BY timestamp DESC
        LIMIT 1;
    """
    async with managed_transaction(engine) as conn:
        return await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn, dedup_key=dedup_key, catalog_id=schema
        )


async def create_task(
    engine: DbResource,
    task_data: TaskCreate,
    schema: str,
    initial_status: str = "PENDING",
    *,
    owner_id: Optional[str] = None,
    locked_until: Optional[datetime] = None,
) -> Optional[Task]:
    """
    Creates a new task in the global tasks table with catalog_id = `schema`.

    Pass initial_status='RUNNING' to bypass the dispatcher queue (e.g. for
    audit tasks created by BackgroundRunner that are already being executed
    in-process and must not be re-claimed by the dispatcher).

    Pass `owner_id` and `locked_until` together with `initial_status='ACTIVE'`
    to INSERT a row that is *born claimed* — same effect as create_task
    followed by an atomic claim, but in a single statement and without
    firing the `notify_task_ready` trigger (which only fires `WHEN NEW.status =
    'PENDING'`). Used by GcpJobRunner's REST path to close the
    REST↔dispatcher race window where a freshly-created PENDING row could
    be claimed by a dispatcher pod between INSERT and the subsequent
    update_task(ACTIVE), spawning a duplicate Cloud Run Job.

    Dedup: if `task_data.dedup_key` is set, a pre-check rejects insert when
    a non-terminal task already carries the same (catalog_id, dedup_key) —
    this is what lets every event-driven caller survive at-least-once
    redelivery. Returns None on dedup hit.
    """
    from dynastore.tools.identifiers import generate_uuidv7

    task_id = generate_uuidv7()
    creation_time = datetime.now(timezone.utc)
    task_schema = get_task_schema()

    async with managed_transaction(engine) as conn:
        if task_data.dedup_key is not None:
            check_sql = f"""
                SELECT task_id FROM {task_schema}.tasks
                WHERE dedup_key = :dedup_key
                  AND catalog_id = :catalog_id
                  AND status NOT IN ('COMPLETED', 'FAILED', 'DEAD_LETTER')
                LIMIT 1;
            """
            existing = await DQLQuery(
                check_sql, result_handler=ResultHandler.ONE_DICT
            ).execute(conn, dedup_key=task_data.dedup_key, catalog_id=schema)
            if existing:
                return None

        from dynastore.tools.correlation import _INTERNAL_KEY, get_correlation_id
        from dynastore.models.tasks import DEFAULT_TASK_TITLE
        # Fold the title into the inputs dict that will be persisted in the
        # inputs JSONB column under the reserved "title" key.
        # Process executions already stamp their title before reaching here,
        # so we only supply the generic default when the key is missing entirely.
        effective_inputs = dict(task_data.inputs or {})
        explicit_title = (
            task_data.title.model_dump(exclude_none=True) if task_data.title else None
        )
        if explicit_title:
            effective_inputs["title"] = explicit_title
        elif not effective_inputs.get("title"):
            effective_inputs["title"] = DEFAULT_TASK_TITLE.model_dump(exclude_none=True)
        cid = get_correlation_id()
        if cid is not None:
            effective_inputs[_INTERNAL_KEY] = cid
        inputs = effective_inputs

        # Always-present columns + values.
        # max_retries: caller may override the column DEFAULT (3) per-row.
        # Cloud Run Job runners pass the job's MAX_RETRIES env so a long-running
        # ingestion job is capped at the deploy-time intent (typically 1) rather
        # than a generic 3-retry default.
        cols: List[str] = [
            "task_id", "catalog_id", "scope", "caller_id", "task_type", "type",
            "execution_mode", "inputs", "timestamp", "collection_id", "dedup_key",
            "status",
        ]
        # The ``type`` column is a denormalised cache of ``task_kind``: derive
        # it from the registry so every row is labelled consistently regardless
        # of which runner created it (the OGC Processes execution path goes
        # through runners that historically left ``type`` at its default,
        # mislabelling genuine processes such as ``gdal``/``ingestion``).
        from dynastore.tasks import resolve_task_type_kind
        resolved_type = resolve_task_type_kind(task_data.task_type, task_data.type)
        insert_kwargs: Dict[str, Any] = dict(
            task_id=task_id,
            catalog_id=schema,
            scope=task_data.scope,
            caller_id=task_data.caller_id,
            task_type=task_data.task_type,
            type=resolved_type,
            execution_mode=task_data.execution_mode,
            inputs=_serialize_inputs(inputs),
            timestamp=creation_time,
            collection_id=task_data.collection_id,
            dedup_key=task_data.dedup_key,
            status=initial_status,
        )
        if task_data.max_retries is not None:
            cols.append("max_retries")
            insert_kwargs["max_retries"] = task_data.max_retries
        if owner_id is not None:
            cols.append("owner_id")
            insert_kwargs["owner_id"] = owner_id
        if locked_until is not None:
            cols.append("locked_until")
            insert_kwargs["locked_until"] = locked_until
        # Stamp the ACTIVE ownership/liveness fields so the row looks
        # identical to one claim_batch would have produced.
        # started_at is deliberately NOT stamped here (#2893): this branch is
        # the REMOTE born-claimed path (GcpJobRunner's REST dispatch) — the
        # container has not started yet at insert time, so started_at stays
        # NULL until claim_for_execution's COALESCE(started_at, NOW()) stamps
        # the real container-start moment.
        if initial_status == "ACTIVE":
            sql_extra = ", last_heartbeat_at"
            values_extra = ", NOW()"
        else:
            sql_extra = ""
            values_extra = ""
        col_list = ", ".join(cols)
        bind_list = ", ".join(f":{c}" for c in cols)
        sql = (
            f"INSERT INTO {task_schema}.tasks "
            f"({col_list}{sql_extra}) "
            f"VALUES ({bind_list}{values_extra}) "
            f"RETURNING *;"
        )

        task_dict = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn, **insert_kwargs,
        )
        get_task.cache_invalidate(conn, task_id, schema)
        task = Task.model_validate(task_dict)

    return task


async def update_task(
    conn: DbResource, task_id: uuid.UUID, update_data: TaskUpdate, schema: str
) -> Optional[Task]:
    """
    Updates fields of an existing task in the global tasks table.
    """
    task_schema = get_task_schema()
    update_fields = update_data.model_dump(exclude_unset=True)

    if "outputs" in update_fields and update_fields["outputs"] is not None:
        from dynastore.tools.json import CustomJSONEncoder
        update_fields["outputs"] = json.dumps(update_fields["outputs"], cls=CustomJSONEncoder)

    set_clauses = [f"{key} = :{key}" for key in update_fields.keys()]
    if not set_clauses:
        return await get_task(conn, task_id, schema)

    set_sql = ", ".join(set_clauses)

    sql = f'UPDATE {task_schema}.tasks SET {set_sql} WHERE task_id = :task_id AND catalog_id = :catalog_id RETURNING *;'

    query_params = {**update_fields, "task_id": task_id, "catalog_id": schema}

    # Commit the write explicitly. ``conn`` is frequently a bare engine — every
    # BackgroundRunner / GcpJobRunner terminal flip passes ``context.engine`` —
    # and ``DQLQuery.execute`` on an engine routes through the executor's
    # pool-return path, which ROLLS BACK any open transaction before handing the
    # connection back to the pool. The UPDATE's ``RETURNING`` row is therefore
    # read (so a Task is returned) while the status change is silently discarded.
    # Running the write inside ``managed_transaction`` commits via ``conn.begin()``
    # for an engine input, or a savepoint for a live-connection input (the caller
    # still owns the outer commit), so the flip lands for every caller. Mirrors
    # ``create_task`` / ``complete_task``.
    async with managed_transaction(conn) as tx_conn:
        updated_task_dict = await DQLQuery(
            sql, result_handler=ResultHandler.ONE_DICT
        ).execute(tx_conn, **query_params)

    get_task.cache_invalidate(conn, task_id, schema)
    return Task.model_validate(updated_task_dict) if updated_task_dict else None


@cached(
    maxsize=256,
    namespace="tasks",
    ignore=["conn"],
    ttl=60,
    l1_ttl=2,
)
async def get_task(conn: DbResource, task_id: uuid.UUID, schema: str) -> Optional[Task]:
    """Retrieves a single task by its ID from the global tasks table."""
    task_schema = get_task_schema()
    sql = f'SELECT * FROM {task_schema}.tasks WHERE task_id = :task_id AND catalog_id = :catalog_id;'
    task_dict = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
        conn, task_id=task_id, catalog_id=schema
    )
    return Task.model_validate(task_dict) if task_dict else None


async def get_task_by_id_unscoped(
    conn: DbResource, task_id: uuid.UUID
) -> Optional[Task]:
    """Retrieve a task by ``task_id`` alone, ignoring the tenant ``catalog_id``.

    Task IDs are UUIDv7 — globally unique — so a single task_id matches at most
    one row in the partitioned ``tasks`` table. Used by the unscoped OGC
    Processes job-status route so that collection-scope and catalog-scope jobs
    are pollable without requiring the caller to construct the scoped URL.

    Intentionally **not cached**: status polls need to reflect cross-process
    writes (e.g. a Cloud Run Job container's `update_task`) without waiting
    for an in-process cache TTL. The scoped, cached :func:`get_task` remains
    the right choice for hot read paths within a known schema.
    """
    task_schema = get_task_schema()
    sql = f'SELECT * FROM {task_schema}.tasks WHERE task_id = :task_id LIMIT 1;'
    task_dict = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
        conn, task_id=task_id
    )
    return Task.model_validate(task_dict) if task_dict else None


def encode_cursor(task: "Task") -> str:
    """Encode a keyset cursor from the last row of a page.

    The cursor is an opaque, URL-safe base64 string encoding
    ``{timestamp_iso}|{task_id}``.  Pass it as ``?cursor=`` on the next
    request to resume listing from the row after this one.
    """
    raw = f"{task.timestamp.isoformat()}|{task.jobID}"
    return base64.urlsafe_b64encode(raw.encode()).decode()


def decode_cursor(cursor: str) -> tuple:
    """Decode a keyset cursor produced by :func:`encode_cursor`.

    Returns a ``(datetime, uuid.UUID)`` tuple for use in the keyset
    WHERE clause: ``(timestamp, task_id) < (:c_ts, :c_id)``.

    Raises ``ValueError`` on malformed input so the route layer can
    surface a 422 to the caller.
    """
    try:
        raw = base64.urlsafe_b64decode(cursor.encode()).decode()
        ts_str, task_id_str = raw.rsplit("|", 1)
        return datetime.fromisoformat(ts_str), uuid.UUID(task_id_str)
    except Exception as exc:
        raise ValueError(f"Invalid cursor: {exc}") from exc


async def list_tasks(
    conn: DbResource,
    schema: str,
    limit: int = 20,
    offset: int = 0,
    kind: Optional[str] = None,
    *,
    status: Optional[str] = None,
    task_type: Optional[str] = None,
    collection_id: Optional[str] = None,
    asset_id: Optional[str] = None,
    created_before: Optional[datetime] = None,
    cursor: Optional[str] = None,
) -> List[Task]:
    """Lists tasks filtered by catalog_id, ordered newest-first.

    Existing positional callers (``kind`` as keyword, ``offset`` for
    offset-based pagination) are unaffected — all new parameters are
    keyword-only with defaults.

    Keyset pagination: when ``cursor`` is provided (a value from
    :func:`encode_cursor`), offset is ignored and a ``(timestamp, task_id)``
    ``<`` predicate replaces OFFSET, giving stable pagination on
    ``timestamp DESC, task_id DESC``.

    The route layer retrieves ``limit+1`` rows by passing ``limit+1`` to
    detect whether a next page exists, then slices to ``limit`` and encodes
    :func:`encode_cursor` on the (limit+1)-th row if present.

    Args:
        conn:           DB connection or engine.
        schema:         ``catalog_id`` column value (physical schema id, or
                        'system'/'platform' sentinels).
        limit:          Max rows to return.  Caller passes ``limit+1`` to
                        detect next-page existence.
        offset:         Deprecated for keyset callers; kept for offset-based
                        back-compat (processes_service).
        kind:           Filter by the ``type`` column ('task' or 'process').
        status:         Filter by task status string.
        task_type:      Filter by the ``task_type`` column.
        collection_id:  Filter by the ``collection_id`` column.
        asset_id:       Filter by ``inputs->>'asset_id'`` JSONB extraction.
        created_before: Filter rows with ``timestamp < created_before``.
        cursor:         Opaque keyset cursor from :func:`encode_cursor`.
    """
    task_schema = get_task_schema()
    clauses = ["catalog_id = :catalog_id"]
    params: Dict[str, Any] = {"catalog_id": schema, "limit": limit}

    if kind is not None:
        clauses.append("type = :kind")
        params["kind"] = kind
    if status is not None:
        clauses.append("status = :status")
        params["status"] = status
    if task_type is not None:
        clauses.append("task_type = :task_type")
        params["task_type"] = task_type
    if collection_id is not None:
        clauses.append("collection_id = :collection_id")
        params["collection_id"] = collection_id
    if asset_id is not None:
        clauses.append("inputs->>'asset_id' = :asset_id")  # nosec — parameterised
        params["asset_id"] = asset_id
    if created_before is not None:
        clauses.append("timestamp < :created_before")
        params["created_before"] = created_before

    where = " AND ".join(clauses)

    if cursor is not None:
        c_ts, c_id = decode_cursor(cursor)
        params["c_ts"] = c_ts
        params["c_id"] = c_id
        sql = (
            f"SELECT * FROM {task_schema}.tasks "
            f"WHERE {where} AND (timestamp, task_id) < (:c_ts, :c_id) "
            f"ORDER BY timestamp DESC, task_id DESC LIMIT :limit;"
        )
    else:
        params["offset"] = offset
        sql = (
            f"SELECT * FROM {task_schema}.tasks "
            f"WHERE {where} "
            f"ORDER BY timestamp DESC, task_id DESC LIMIT :limit OFFSET :offset;"
        )

    task_dicts = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
        conn, **params
    )
    return [Task.model_validate(t) for t in (task_dicts or [])]


async def list_tasks_system(
    conn: DbResource,
    limit: int = 20,
    *,
    status: Optional[str] = None,
    task_type: Optional[str] = None,
    kind: Optional[str] = None,
    asset_id: Optional[str] = None,
    created_before: Optional[datetime] = None,
    cursor: Optional[str] = None,
) -> List[Task]:
    """Lists tasks whose ``catalog_id`` is one of the system sentinels.

    Covers ``catalog_id IN ('system', 'platform')`` — the two sentinels used
    for cross-tenant platform work.  Accepts the same keyset / filter params
    as :func:`list_tasks` (except ``collection_id`` which is not meaningful
    at system scope).

    The route layer passes ``limit+1`` to detect next-page existence.
    """
    task_schema = get_task_schema()
    clauses = ["catalog_id IN ('system', 'platform')"]
    params: Dict[str, Any] = {"limit": limit}

    if status is not None:
        clauses.append("status = :status")
        params["status"] = status
    if task_type is not None:
        clauses.append("task_type = :task_type")
        params["task_type"] = task_type
    if kind is not None:
        clauses.append("type = :kind")
        params["kind"] = kind
    if asset_id is not None:
        clauses.append("inputs->>'asset_id' = :asset_id")  # nosec — parameterised
        params["asset_id"] = asset_id
    if created_before is not None:
        clauses.append("timestamp < :created_before")
        params["created_before"] = created_before

    where = " AND ".join(clauses)

    if cursor is not None:
        c_ts, c_id = decode_cursor(cursor)
        params["c_ts"] = c_ts
        params["c_id"] = c_id
        sql = (
            f"SELECT * FROM {task_schema}.tasks "
            f"WHERE {where} AND (timestamp, task_id) < (:c_ts, :c_id) "
            f"ORDER BY timestamp DESC, task_id DESC LIMIT :limit;"
        )
    else:
        sql = (
            f"SELECT * FROM {task_schema}.tasks "
            f"WHERE {where} "
            f"ORDER BY timestamp DESC, task_id DESC LIMIT :limit;"
        )

    task_dicts = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
        conn, **params
    )
    return [Task.model_validate(t) for t in (task_dicts or [])]


# --- Synchronous Wrappers for Task Runners ---
def update_task_sync(
    conn: DbResource, task_id: uuid.UUID, update_data: TaskUpdate, schema: str
) -> Optional[Task]:
    """Synchronous wrapper for updating a task."""
    return run_in_event_loop(update_task(conn, task_id, update_data, schema))


# --- TaskQueueProtocol implementation functions ---

async def enqueue(
    engine: DbResource,
    task_data: TaskCreate,
    schema_name: str,
    dedup_key: Optional[str] = None,
    execution_mode: str = "ASYNCHRONOUS",
    scope: str = "CATALOG",
) -> Optional[Task]:
    """
    Enqueue a task into the global task queue.

    If dedup_key is provided and already exists (for a non-terminal task),
    returns None instead of creating a duplicate.
    """
    # Override task_data fields with explicit parameters
    task_data.execution_mode = execution_mode
    task_data.scope = scope
    if dedup_key is not None:
        task_data.dedup_key = dedup_key

    from dynastore.tools.identifiers import generate_uuidv7

    task_id = generate_uuidv7()
    creation_time = datetime.now(timezone.utc)
    task_schema = get_task_schema()

    async with managed_transaction(engine) as conn:
        if dedup_key is not None:
            # Cross-partition dedup check: the UNIQUE index is per-partition
            # (PG requires partition key in unique indexes), so we do an
            # explicit check across all partitions before inserting.
            check_sql = f"""
                SELECT task_id FROM {task_schema}.tasks
                WHERE dedup_key = :dedup_key
                  AND catalog_id = :catalog_id
                  AND status NOT IN ('COMPLETED', 'FAILED', 'DEAD_LETTER')
                LIMIT 1;
            """
            existing = await DQLQuery(
                check_sql, result_handler=ResultHandler.ONE_DICT
            ).execute(conn, dedup_key=dedup_key, catalog_id=schema_name)
            if existing:
                return None

            sql = f"""
                INSERT INTO {task_schema}.tasks
                    (task_id, catalog_id, scope, caller_id, task_type, type,
                     execution_mode, inputs, timestamp, collection_id, dedup_key)
                VALUES
                    (:task_id, :catalog_id, :scope, :caller_id, :task_type, :type,
                     :execution_mode, :inputs, :timestamp, :collection_id, :dedup_key)
                ON CONFLICT (catalog_id, dedup_key, timestamp)
                    WHERE dedup_key IS NOT NULL
                    AND status NOT IN ('COMPLETED', 'FAILED', 'DEAD_LETTER')
                DO NOTHING
                RETURNING *;
            """
        else:
            sql = f"""
                INSERT INTO {task_schema}.tasks
                    (task_id, catalog_id, scope, caller_id, task_type, type,
                     execution_mode, inputs, timestamp, collection_id)
                VALUES
                    (:task_id, :catalog_id, :scope, :caller_id, :task_type, :type,
                     :execution_mode, :inputs, :timestamp, :collection_id)
                RETURNING *;
            """

        # ``type`` is a denormalised cache of ``task_kind`` — derive from the
        # registry (see create_task) so the queue path labels rows identically.
        from dynastore.tasks import resolve_task_type_kind
        resolved_type = resolve_task_type_kind(task_data.task_type, task_data.type)
        task_dict = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn,
            task_id=task_id,
            catalog_id=schema_name,
            scope=scope,
            caller_id=task_data.caller_id,
            task_type=task_data.task_type,
            type=resolved_type,
            execution_mode=execution_mode,
            inputs=_serialize_inputs(task_data.inputs),
            timestamp=creation_time,
            collection_id=task_data.collection_id,
            dedup_key=dedup_key,
        )

        if task_dict is None:
            # Dedup conflict — task already exists
            return None

        get_task.cache_invalidate(conn, task_id, schema_name)
        return Task.model_validate(task_dict)


async def claim_next(
    engine: DbResource,
    async_task_types: List[str],
    sync_task_types: List[str],
    visibility_timeout: timedelta,
    owner_id: str,
) -> Optional[Dict[str, Any]]:
    """
    Atomically claim the next available task matching the given types and
    execution modes using FOR UPDATE SKIP LOCKED.
    """
    if not async_task_types and not sync_task_types:
        return None

    task_schema = get_task_schema()
    locked_until = datetime.now(timezone.utc) + visibility_timeout

    # Build WHERE conditions for execution mode + task type pairs
    conditions = []
    now = datetime.now(timezone.utc)
    # Partition pruning hint: only scan partitions within the lookback window.
    # Configurable via DYNASTORE_TASK_LOOKBACK_DAYS (default: 30).
    lookback = now - get_task_lookback()
    params: Dict[str, Any] = {
        "locked_until": locked_until,
        "owner_id": owner_id,
        "now": now,
        "lookback": lookback,
    }

    if async_task_types:
        conditions.append(
            "(execution_mode = 'ASYNCHRONOUS' AND task_type = ANY(:async_types))"
        )
        params["async_types"] = async_task_types

    if sync_task_types:
        conditions.append(
            "(execution_mode = 'SYNCHRONOUS' AND task_type = ANY(:sync_types))"
        )
        params["sync_types"] = sync_task_types

    mode_filter = " OR ".join(conditions)

    sql = f"""
        UPDATE {task_schema}.tasks
        SET status = 'ACTIVE',
            locked_until = :locked_until,
            owner_id = :owner_id,
            started_at = COALESCE(started_at, NOW()),
            last_heartbeat_at = NOW()
        WHERE (timestamp, task_id) = (
            SELECT timestamp, task_id FROM {task_schema}.tasks
            WHERE status = 'PENDING'
              AND timestamp >= :lookback
              AND (locked_until IS NULL OR locked_until <= :now)
              AND ({mode_filter})
            ORDER BY timestamp ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING task_id, catalog_id, scope, task_type, execution_mode,
                  caller_id, inputs, collection_id, retry_count, max_retries,
                  timestamp, dedup_key, owner_id;
    """

    async with managed_transaction(engine) as conn:
        result = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn, **params
        )

    return result


async def claim_batch(
    engine: DbResource,
    async_task_types: List[str],
    sync_task_types: List[str],
    visibility_timeout: timedelta,
    owner_id: str,
    batch_size: int = 10,
) -> List[Dict[str, Any]]:
    """
    Atomically claim up to ``batch_size`` available tasks matching the given
    types and execution modes using FOR UPDATE SKIP LOCKED.

    Returns a list of claimed task rows (may be empty).
    """
    if not async_task_types and not sync_task_types:
        return []

    task_schema = get_task_schema()
    locked_until = datetime.now(timezone.utc) + visibility_timeout

    conditions = []
    now = datetime.now(timezone.utc)
    lookback = now - get_task_lookback()
    params: Dict[str, Any] = {
        "locked_until": locked_until,
        "owner_id": owner_id,
        "now": now,
        "lookback": lookback,
        "batch_size": batch_size,
        "hard_cap": _HARD_RETRY_CAP,
    }

    if async_task_types:
        conditions.append(
            "(execution_mode = 'ASYNCHRONOUS' AND task_type = ANY(:async_types))"
        )
        params["async_types"] = async_task_types

    if sync_task_types:
        conditions.append(
            "(execution_mode = 'SYNCHRONOUS' AND task_type = ANY(:sync_types))"
        )
        params["sync_types"] = sync_task_types

    mode_filter = " OR ".join(conditions)

    # Fairness: pick the oldest PENDING task per tenant (catalog_id) first,
    # then fill remaining batch slots from those results. This prevents a
    # single high-volume tenant from monopolising all claim slots.
    # DISTINCT ON (catalog_id) ORDER BY catalog_id, timestamp ASC
    # returns exactly one row per tenant — the oldest eligible task.
    # DISTINCT ON and FOR UPDATE SKIP LOCKED cannot be combined in the same
    # SELECT (PostgreSQL forbids FOR UPDATE with DISTINCT). Use a two-step
    # approach: a CTE picks one candidate per tenant (oldest PENDING task via
    # DISTINCT ON), then the outer SELECT locks those specific rows.
    #
    # Circuit breaker: rows whose retry_count has reached the platform-wide
    # ``hard_cap`` are invisible to dispatchers — the reaper will DLQ them on
    # its next pass. This caps the cost of any future re-enqueue regression.
    # `locked` captures error_message as it stood *before* the claim UPDATE
    # clears it below — so the RETURNING clause can still surface the prior
    # failure reason (as `prior_error_message`) for the event emitted after
    # commit (#3225), without weakening the SKIP LOCKED claim itself.
    sql = f"""
        WITH candidates AS (
            SELECT DISTINCT ON (catalog_id) timestamp, task_id
            FROM {task_schema}.tasks
            WHERE status = 'PENDING'
              AND timestamp >= :lookback
              AND (locked_until IS NULL OR locked_until <= :now)
              AND retry_count < :hard_cap
              AND ({mode_filter})
            ORDER BY catalog_id, timestamp ASC
        ),
        locked AS (
            SELECT timestamp, task_id, error_message
            FROM {task_schema}.tasks
            WHERE (timestamp, task_id) IN (SELECT timestamp, task_id FROM candidates)
              AND status = 'PENDING'
            ORDER BY timestamp ASC
            LIMIT :batch_size
            FOR UPDATE SKIP LOCKED
        )
        UPDATE {task_schema}.tasks
        SET status = 'ACTIVE',
            locked_until = :locked_until,
            owner_id = :owner_id,
            started_at = COALESCE(started_at, NOW()),
            last_heartbeat_at = NOW(),
            error_message = NULL
        FROM locked
        WHERE {task_schema}.tasks.timestamp = locked.timestamp
          AND {task_schema}.tasks.task_id = locked.task_id
        RETURNING {task_schema}.tasks.task_id, {task_schema}.tasks.catalog_id,
                  {task_schema}.tasks.scope, {task_schema}.tasks.task_type,
                  {task_schema}.tasks.execution_mode, {task_schema}.tasks.caller_id,
                  {task_schema}.tasks.inputs, {task_schema}.tasks.collection_id,
                  {task_schema}.tasks.retry_count, {task_schema}.tasks.max_retries,
                  {task_schema}.tasks.timestamp, {task_schema}.tasks.dedup_key,
                  {task_schema}.tasks.owner_id, locked.error_message AS prior_error_message;
    """

    async with managed_transaction(engine) as conn:
        result = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
            conn, **params
        )

    result = result or []

    # #3225: the claim above just cleared error_message so a retrying task
    # doesn't carry a stale reason into a successful completion — but that
    # trades away the only place the prior failure was visible while the
    # retry is ACTIVE. Emit it as a task event (best-effort, after commit)
    # so the history survives in the events stream. Skip first-time claims
    # (no prior error) to keep this cheap in the common case.
    for row in result:
        prior_error = row.pop("prior_error_message", None)
        if not prior_error:
            continue
        try:
            from dynastore.modules.catalog.event_service import emit_event
            await emit_event(
                "task.retried",
                task_id=str(row["task_id"]),
                task_type=row.get("task_type"),
                catalog_id=row.get("catalog_id"),
                collection_id=row.get("collection_id"),
                retry_count=row.get("retry_count"),
                prior_error_message=prior_error,
            )
        except Exception as emit_exc:  # noqa: BLE001
            logger.error(
                "claim_batch: failed to emit task.retried event for task %s: %s",
                row["task_id"], emit_exc,
            )

    return result


async def _diagnose_created_at_miss(
    engine: DbResource,
    task_id: uuid.UUID,
    created_at: datetime,
    op: str,
) -> None:
    """Log a loud, distinguishable ERROR when a ``created_at``-guarded
    terminal write matched 0 rows because ``created_at`` does not match the
    row's real partition key — rather than a genuine ``owner_id``/status
    race loss (#3218).

    Only meaningful to call after a 0-row UPDATE that included the
    ``AND timestamp = :created_at`` predicate. Runs a plain ``task_id``-only
    lookup (probes every partition — acceptable here since it only fires on
    the rare 0-row path, never on the hot terminal-write path itself) to
    tell apart:

    - the row exists under a DIFFERENT ``timestamp`` than the caller passed
      — the UPDATE targeted the wrong partition and never had a chance to
      match, regardless of ``owner_id``/status. Logged as an ERROR so this
      is not read as a benign race loss.
    - the row is missing entirely, or exists with the matching ``timestamp``
      — a genuine race loss (owner_id/status already moved on); the caller's
      own "lost race" warning already covers this case, so nothing extra is
      logged here.

    Best-effort: any lookup failure is swallowed rather than raised — this
    diagnostic must never mask the original 0-row result the caller already
    decided to return.
    """
    task_schema = get_task_schema()
    try:
        async with managed_transaction(engine) as conn:
            row = await DQLQuery(
                f"SELECT timestamp FROM {task_schema}.tasks WHERE task_id = :task_id LIMIT 1;",
                result_handler=ResultHandler.ONE_DICT,
            ).execute(conn, task_id=task_id)
    except Exception as exc:  # noqa: BLE001 — diagnostic only, must not raise
        logger.debug(
            "%s: _diagnose_created_at_miss lookup failed for task %s: %s",
            op, task_id, exc,
        )
        return
    if row is None:
        return
    actual_ts = row.get("timestamp")
    if actual_ts is not None and actual_ts != created_at:
        logger.error(
            "%s: WRONG partition key for task %s — caller passed "
            "created_at=%s but the row's actual creation timestamp is %s. "
            "This UPDATE matched 0 rows because it targeted the wrong "
            "monthly partition, NOT because of an owner_id/status race. "
            "Fix the caller to thread the row's real creation timestamp.",
            op, task_id, created_at, actual_ts,
        )


async def complete_task(
    engine: DbResource,
    task_id: uuid.UUID,
    timestamp: Any,
    outputs: Optional[Any] = None,
    *,
    owner_id: Optional[str] = None,
    created_at: Optional[datetime] = None,
) -> bool:
    """Mark a claimed task as COMPLETED.

    Returns ``True`` when a row was updated, ``False`` when none matched.

    ``owner_id`` is an optional race guard: when provided, the UPDATE also
    requires ``owner_id`` to still equal the given value. The liveness
    reconciler passes the ``owner_id`` it probed so it can only complete the
    exact execution attempt it observed — if the pg_cron reaper reclaimed the
    row (``owner_id`` → NULL) and the dispatcher re-claimed it as a fresh
    attempt (``owner_id`` → a different value) between the reconciler's SELECT
    and this write, no row matches and the caller treats the ``False`` return
    as a lost race rather than clobbering the new attempt. See #750.

    ``created_at`` is the row's ``timestamp`` column — its creation time and
    RANGE-partition key — NOT ``timestamp`` above (the completion time
    written to ``finished_at``). When supplied it adds
    ``AND timestamp = :created_at`` to the UPDATE so Postgres prunes to the
    row's own monthly partition instead of probing ``idx_tasks_task_id`` on
    every live partition (#3218). Every production caller has the row's
    creation timestamp in hand from its own claim/create/select and passes
    it; it stays optional only so the handful of callers without it in scope
    keep today's ``task_id``-only match. A 0-row result while ``created_at``
    was supplied is diagnosed by :func:`_diagnose_created_at_miss` so a wrong
    partition key surfaces as a loud, distinguishable error instead of
    reading as a benign owner_id race loss.
    """
    task_schema = get_task_schema()
    # Normalize a TaskReport envelope to a plain dict + error_message pair
    # (#1807 P2).  Non-TaskReport values pass through verbatim.
    from dynastore.tasks.report import normalize_task_result
    outputs, _err = normalize_task_result(outputs)

    serialized_outputs = None
    if outputs is not None:
        from dynastore.tools.json import CustomJSONEncoder
        serialized_outputs = json.dumps(outputs, cls=CustomJSONEncoder)

    owner_guard = " AND owner_id = :owner_id" if owner_id is not None else ""
    created_at_guard = " AND timestamp = :created_at" if created_at is not None else ""
    sql = f"""
        UPDATE {task_schema}.tasks
        SET status = 'COMPLETED',
            progress = 100,
            finished_at = :finished_at,
            outputs = :outputs,
            locked_until = NULL,
            owner_id = NULL
        WHERE task_id = :task_id{created_at_guard}{owner_guard};
    """
    params: Dict[str, Any] = {
        "task_id": task_id,
        "finished_at": timestamp,
        "outputs": serialized_outputs,
    }
    if owner_id is not None:
        params["owner_id"] = owner_id
    if created_at is not None:
        params["created_at"] = created_at
    async with managed_transaction(engine) as conn:
        rowcount = await DQLQuery(
            sql, result_handler=ResultHandler.ROWCOUNT
        ).execute(conn, **params)
    matched = bool(rowcount and rowcount > 0)
    if not matched and created_at is not None:
        await _diagnose_created_at_miss(engine, task_id, created_at, "complete_task")
    return matched


async def update_task_ingestion_offset(
    engine: DbResource,
    task_id: uuid.UUID,
    offset: int,
) -> bool:
    """Stamp a committed-row cursor onto ``inputs.ingestion_request.offset`` (#2820).

    Called by the ingestion loop after every batch commit so a subsequent
    claim of this task row — a dispatcher retry after a timeout/kill, which
    resets status/owner via ``fail_task(retry=True)`` but never touches
    ``inputs`` — rebuilds ``TaskIngestionRequest`` (see ``IngestionTask.run``,
    which reads ``inputs`` fresh from the claimed row on every dispatch)
    starting at the last durably committed offset instead of the original
    request's (almost always 0).

    Scoped by ``task_id`` only, matching ``complete_task`` / ``fail_task`` —
    the ingestion loop already holds ``task_id`` from its own dispatch and
    has no cheap access to the ``catalog_id`` column value here.

    Returns ``True`` when a row was updated, ``False`` when none matched.
    Callers should treat this as best-effort — a missed write degrades to
    the pre-#2820 behaviour (a retry restarts from the original offset)
    rather than aborting an otherwise-successful batch.
    """
    task_schema = get_task_schema()
    sql = f"""
        UPDATE {task_schema}.tasks
        SET inputs = jsonb_set(
            COALESCE(inputs, '{{}}'::jsonb),
            '{{ingestion_request,offset}}',
            to_jsonb(CAST(:offset_value AS bigint)),
            true
        )
        WHERE task_id = :task_id;
    """
    async with managed_transaction(engine) as conn:
        rowcount = await DQLQuery(
            sql, result_handler=ResultHandler.ROWCOUNT
        ).execute(conn, task_id=task_id, offset_value=offset)
    return bool(rowcount and rowcount > 0)


async def update_task_harvest_cursor(
    engine: DbResource,
    task_id: uuid.UUID,
    collection_id: Optional[str],
    items_href: Optional[str],
    done: bool,
) -> bool:
    """Stamp a resume cursor onto ``inputs.inputs.resume`` (#3034).

    Mirrors ``update_task_ingestion_offset`` (#2820) for the ``stac_harvest``
    task: called after each items-page batch write commits so a dispatcher
    retry after a Cloud Run Job timeout/kill resumes the source walk instead
    of restarting it from the first collection. ``StacHarvestTask.run``
    rebuilds ``StacHarvestRequest`` from the claimed row's ``inputs`` on every
    dispatch (a retry resets ``status``/``owner_id`` via
    ``fail_task(retry=True)`` but never touches ``inputs``), so stamping the
    cursor here is sufficient to seed the resumed run.

    ``stac_harvest`` is always submitted via ``execute_process`` (the
    ``stac_harvester`` preset), so the row's ``inputs`` column carries the
    ``ExecuteRequest`` wrapper — the actual ``StacHarvestRequest`` fields
    (and ``resume``) live one level down at ``inputs.inputs``, unlike
    ingestion's flat ``inputs.ingestion_request``. The whole ``resume``
    object is replaced in one ``jsonb_set`` call (rather than patching
    ``collection_id``/``items_href``/``done`` as separate leaf paths) because
    ``inputs.inputs.resume`` does not exist on the row until the first write:
    ``jsonb_set`` only auto-creates the *last* path element even with
    ``create_missing=true``, so a leaf-only path (e.g.
    ``{inputs,resume,collection_id}``) would silently no-op while ``resume``
    itself is still missing. Setting the whole object at
    ``{inputs,resume}`` needs only ``inputs`` (which is always present) to
    already exist.

    ``collection_id`` is the source collection currently in progress (``None``
    while a single-collection harvest has not yet started, or between
    collections). ``items_href`` is the STAC ``rel=next`` page URL to resume
    items from within that collection (``None`` means start it from the
    beginning). ``done`` marks that collection's item walk as fully drained,
    so a resumed catalog walk skips it entirely and moves to the next one.

    Returns ``True`` when a row was updated, ``False`` when none matched.
    Best-effort: a missed write only degrades to a retry restarting the
    affected collection from the beginning, never aborts the harvest.
    """
    task_schema = get_task_schema()
    sql = f"""
        UPDATE {task_schema}.tasks
        SET inputs = jsonb_set(
            COALESCE(inputs, '{{}}'::jsonb),
            '{{inputs,resume}}',
            CAST(:resume_json AS jsonb),
            true
        )
        WHERE task_id = :task_id;
    """
    resume_json = json.dumps(
        {"collection_id": collection_id, "items_href": items_href, "done": done}
    )
    async with managed_transaction(engine) as conn:
        rowcount = await DQLQuery(
            sql, result_handler=ResultHandler.ROWCOUNT
        ).execute(conn, task_id=task_id, resume_json=resume_json)
    return bool(rowcount and rowcount > 0)


async def _emit_task_failed_event(
    task_id: uuid.UUID,
    row: Dict[str, Any],
    severity: str,
) -> None:
    """Emit a ``task.failed`` platform event for a task that reached a terminal
    failure status (``FAILED`` or ``DEAD_LETTER``).

    This is the single emission point for all terminal failure paths that go
    through the DB write functions (``fail_task`` / ``dead_letter_task``).
    Runners no longer emit independently — this avoids double-emission across
    the dispatcher path, the background runner, and the sync runner.

    ``row`` is the dict returned by the RETURNING clause of the terminal UPDATE;
    it must contain ``task_type``, ``caller_id``, ``inputs`` (already decoded to
    a dict or None), and ``error_message``.

    ``severity`` is pre-computed by the caller:
    - ``"unrecoverable"`` — permanent failure (``retry=False``) or hard-cap DLQ.
    - ``"recoverable"``   — retry-exhausted DLQ from a transient error path.

    All emission is best-effort: a logging or event-bus failure must never
    propagate back to the DB write path that already committed the terminal
    status.
    """
    task_id_str = str(task_id)
    task_type: str = row.get("task_type") or "unknown"
    inputs: Optional[Dict[str, Any]] = row.get("inputs")
    if isinstance(inputs, str):
        try:
            inputs = json.loads(inputs)
        except (ValueError, TypeError):
            inputs = None
    catalog_id: Optional[str] = (inputs or {}).get("catalog_id")
    error_msg: str = row.get("error_message") or ""

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
                "log_manager unavailable for task failure logging: %s", log_exc
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
        logger.error("Failed to emit task.failed event for task %s: %s", task_id_str, emit_exc)


async def fail_task(
    engine: DbResource,
    task_id: uuid.UUID,
    timestamp: Any,
    error_message: str,
    retry: bool = True,
    *,
    owner_id: Optional[str] = None,
    created_at: Optional[datetime] = None,
) -> bool:
    """
    Mark a claimed task as failed. If retry=True and retries remain,
    requeue with exponential backoff. Otherwise move to DEAD_LETTER.

    Returns ``True`` when a row was updated, ``False`` when none matched.

    Emits a ``task.failed`` platform event when the row reaches a terminal
    status (``FAILED`` when ``retry=False``, or ``DEAD_LETTER`` when the
    hard-cap is crossed). The PENDING/retry branch does NOT emit.

    ``owner_id`` is an optional race guard: when provided, the UPDATE also
    requires ``owner_id`` to still equal the given value. The liveness
    reconciler passes the ``owner_id`` it probed so it can only fail the exact
    execution attempt it observed — if the pg_cron reaper reclaimed the row
    and the dispatcher re-claimed it as a fresh attempt between the
    reconciler's SELECT and this write, no row matches and the caller treats
    the ``False`` return as a lost race rather than failing a task that is
    legitimately running again. See #750.

    ``created_at`` is the row's ``timestamp`` column — its creation time and
    RANGE-partition key — NOT ``timestamp`` above (the completion time
    written to ``finished_at`` on the terminal branch). When supplied it adds
    ``AND timestamp = :created_at`` to the UPDATE so Postgres prunes to the
    row's own monthly partition instead of probing ``idx_tasks_task_id`` on
    every live partition (#3218). See :func:`complete_task` for the full
    rationale, including how a wrong value is diagnosed via
    :func:`_diagnose_created_at_miss` instead of reading as a benign
    owner_id race loss.
    """
    task_schema = get_task_schema()
    owner_guard = " AND owner_id = :owner_id" if owner_id is not None else ""
    created_at_guard = " AND timestamp = :created_at" if created_at is not None else ""

    if retry:
        # Attempt retry: increment retry_count, reset to PENDING with backoff.
        # The platform-wide hard cap (`hard_retry_cap` from TasksPluginConfig)
        # forces DEAD_LETTER once crossed even if the row's max_retries is
        # generous — defends against runaway loops where a runner repeatedly
        # mis-handles the same row.
        sql = f"""
            UPDATE {task_schema}.tasks
            SET status = CASE
                    WHEN retry_count + 1 < LEAST(max_retries, :hard_cap)
                        THEN 'PENDING'
                    ELSE 'DEAD_LETTER'
                END,
                error_message = CASE
                    WHEN retry_count + 1 >= :hard_cap
                        THEN :error_message || ' [hard retry cap ' || :hard_cap || ' reached]'
                    ELSE :error_message
                END,
                retry_count = retry_count + 1,
                locked_until = CASE
                    WHEN retry_count + 1 < LEAST(max_retries, :hard_cap)
                    THEN NOW() + (POWER(2, retry_count + 1) || ' seconds')::INTERVAL
                    ELSE NULL
                END,
                finished_at = CASE
                    WHEN retry_count + 1 >= LEAST(max_retries, :hard_cap) THEN :finished_at
                    ELSE finished_at
                END,
                owner_id = CASE
                    WHEN retry_count + 1 < LEAST(max_retries, :hard_cap) THEN NULL
                    ELSE owner_id
                END
            WHERE task_id = :task_id{created_at_guard}{owner_guard}
            RETURNING status, task_type, caller_id, inputs, error_message;
        """
        params = {
            "task_id": task_id,
            "error_message": error_message,
            "finished_at": timestamp,
            "hard_cap": _HARD_RETRY_CAP,
        }
    else:
        sql = f"""
            UPDATE {task_schema}.tasks
            SET status = 'FAILED',
                error_message = :error_message,
                finished_at = :finished_at,
                locked_until = NULL,
                owner_id = NULL
            WHERE task_id = :task_id{created_at_guard}{owner_guard}
            RETURNING status, task_type, caller_id, inputs, error_message;
        """
        params = {
            "task_id": task_id,
            "error_message": error_message,
            "finished_at": timestamp,
        }

    if owner_id is not None:
        params["owner_id"] = owner_id
    if created_at is not None:
        params["created_at"] = created_at

    async with managed_transaction(engine) as conn:
        row = await DQLQuery(
            sql, result_handler=ResultHandler.ONE_DICT
        ).execute(conn, **params)

    if row is None:
        if created_at is not None:
            await _diagnose_created_at_miss(engine, task_id, created_at, "fail_task")
        return False

    # Emit task.failed only for terminal statuses — not for PENDING (retry).
    final_status: str = row.get("status") or ""
    if final_status in ("FAILED", "DEAD_LETTER"):
        # FAILED → permanent failure (retry=False); hard-cap DEAD_LETTER is
        # also unrecoverable from the platform's perspective.
        # A DEAD_LETTER via retry=True means retries were exhausted — still
        # "recoverable" in the sense that the error was transient; severity
        # mirrors the retry intent of the call site.
        severity = "unrecoverable" if final_status == "FAILED" else "recoverable"
        try:
            await _emit_task_failed_event(task_id, row, severity)
        except Exception as exc:
            logger.error(
                "fail_task: unexpected error in _emit_task_failed_event for %s: %s",
                task_id, exc,
            )

    return True


async def dead_letter_task(
    engine: DbResource,
    task_id: uuid.UUID,
    timestamp: Any,
    error_message: str,
    *,
    owner_id: Optional[str] = None,
    created_at: Optional[datetime] = None,
) -> bool:
    """Move a claimed task directly to DEAD_LETTER (no retry).

    Distinct from ``fail_task(retry=False)`` (which writes ``FAILED``): this
    parks the row in the dead-letter queue, where it is visible to
    ``requeue_dead_letter_tasks`` for manual/automated replay.  It is the
    terminal write for a timed-out task whose routing ``on_timeout`` action is
    the default ``DEAD_LETTER`` — a timeout is an operational outcome (the work
    may still be valid), not a logic error, so it belongs in the DLQ rather
    than ``FAILED``.

    Returns ``True`` when a row was updated, ``False`` when none matched.
    Emits a ``task.failed`` platform event with severity ``"recoverable"``
    (a timeout is operationally transient — the work may be valid to replay).
    ``owner_id`` is the same optional race guard documented on
    :func:`complete_task` / :func:`fail_task`.

    ``created_at`` is the row's ``timestamp`` column — its creation time and
    RANGE-partition key — NOT ``timestamp`` above (the completion time
    written to ``finished_at``). See :func:`complete_task` for the full
    partition-pruning rationale (#3218) and how a wrong value is diagnosed
    via :func:`_diagnose_created_at_miss`.
    """
    task_schema = get_task_schema()
    owner_guard = " AND owner_id = :owner_id" if owner_id is not None else ""
    created_at_guard = " AND timestamp = :created_at" if created_at is not None else ""
    sql = f"""
        UPDATE {task_schema}.tasks
        SET status = 'DEAD_LETTER',
            error_message = :error_message,
            finished_at = :finished_at,
            locked_until = NULL,
            owner_id = NULL
        WHERE task_id = :task_id{created_at_guard}{owner_guard}
        RETURNING task_type, caller_id, inputs, error_message;
    """
    params: Dict[str, Any] = {
        "task_id": task_id,
        "error_message": error_message,
        "finished_at": timestamp,
    }
    if owner_id is not None:
        params["owner_id"] = owner_id
    if created_at is not None:
        params["created_at"] = created_at
    async with managed_transaction(engine) as conn:
        row = await DQLQuery(
            sql, result_handler=ResultHandler.ONE_DICT
        ).execute(conn, **params)

    if row is None:
        if created_at is not None:
            await _diagnose_created_at_miss(engine, task_id, created_at, "dead_letter_task")
        return False

    # dead_letter_task is always terminal — always emit.
    # Severity: "recoverable" because the caller chose DLQ (timed-out path)
    # rather than permanent FAILED, meaning the work may be valid to replay.
    try:
        await _emit_task_failed_event(task_id, row, "recoverable")
    except Exception as exc:
        logger.error(
            "dead_letter_task: unexpected error in _emit_task_failed_event for %s: %s",
            task_id, exc,
        )

    return True


async def heartbeat_tasks(
    engine: DbResource,
    tasks: List[Tuple[uuid.UUID, datetime]],
    visibility_timeout: timedelta,
) -> None:
    """Extend locked_until for active tasks (batched heartbeat).

    ``tasks`` is a list of ``(task_id, created_at)`` pairs — ``created_at``
    is each row's ``timestamp`` column (creation time, the RANGE-partition
    key). Every caller (``BatchedHeartbeat._flush``, the Cloud Run Job
    in-process heartbeat loop, the liveness reconciler's grace extension)
    already has this value from its own claim/create/select. Matching on
    ``(task_id, created_at)`` pairs — mirroring ``claim_batch``'s existing
    ``(timestamp, task_id)`` join shape — lets Postgres prune to each row's
    own monthly partition instead of probing ``idx_tasks_task_id`` on every
    live partition for every heartbeat tick (#3218).
    """
    if not tasks:
        return

    task_schema = get_task_schema()
    new_locked_until = datetime.now(timezone.utc) + visibility_timeout
    task_ids = [tid for tid, _ in tasks]
    created_ats = [ts for _, ts in tasks]

    sql = f"""
        UPDATE {task_schema}.tasks AS t
        SET locked_until = :locked_until,
            last_heartbeat_at = NOW()
        FROM UNNEST(CAST(:task_ids AS uuid[]), CAST(:created_ats AS timestamptz[]))
            AS batch(task_id, created_at)
        WHERE t.task_id = batch.task_id
          AND t.timestamp = batch.created_at
          AND t.status = 'ACTIVE';
    """
    async with managed_transaction(engine) as conn:
        await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
            conn,
            locked_until=new_locked_until,
            task_ids=task_ids,
            created_ats=created_ats,
        )


async def heartbeat_task_if_active(
    engine: DbResource,
    task_id: uuid.UUID,
    visibility_timeout: timedelta,
    *,
    created_at: Optional[datetime] = None,
) -> bool:
    """Conditionally extend ``locked_until`` for a single task.

    Like :func:`heartbeat_tasks` but single-row and signal-returning. The
    UPDATE is gated on ``status = 'ACTIVE'``; the function returns ``True``
    when the row matched and was extended, ``False`` when it did not — the
    most common cause being a competing process having flipped the row out
    of ``ACTIVE`` between the caller's decision to heartbeat and the UPDATE
    itself.

    The liveness reconciler uses the ``False`` return as the reaper-race
    signal: the reconciler ``SELECT``-commit → probe → heartbeat sequence has
    an accepted gap during which the pg_cron reaper (``reap_stuck_tasks``,
    every minute) can reclaim the row to PENDING. When that happens the
    reconciler's heartbeat finds no ACTIVE row to update; the caller logs a
    WARNING so operators can see how often the race fires in practice and
    tune the reconciler interval down accordingly. See #741 item 3.

    ``created_at`` is the row's ``timestamp`` column — its creation time and
    RANGE-partition key. When supplied it adds ``AND timestamp = :created_at``
    so Postgres prunes to the row's own monthly partition instead of probing
    ``idx_tasks_task_id`` on every live partition (#3218); optional only so
    callers without it in scope keep today's ``task_id``-only match. A 0-row
    result while ``created_at`` was supplied is diagnosed by
    :func:`_diagnose_created_at_miss` so a wrong partition key surfaces as a
    loud, distinguishable error instead of reading as a benign reaper-race
    loss.
    """
    task_schema = get_task_schema()
    new_locked_until = datetime.now(timezone.utc) + visibility_timeout
    created_at_guard = " AND timestamp = :created_at" if created_at is not None else ""
    sql = f"""
        UPDATE {task_schema}.tasks
        SET locked_until = :locked_until,
            last_heartbeat_at = NOW()
        WHERE task_id = :task_id{created_at_guard}
          AND status = 'ACTIVE';
    """
    params: Dict[str, Any] = {"locked_until": new_locked_until, "task_id": task_id}
    if created_at is not None:
        params["created_at"] = created_at
    async with managed_transaction(engine) as conn:
        rowcount = await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(
            conn, **params
        )
    matched = bool(rowcount and rowcount > 0)
    if not matched and created_at is not None:
        await _diagnose_created_at_miss(engine, task_id, created_at, "heartbeat_task_if_active")
    return matched


async def set_runner_ref(
    engine: DbResource,
    task_id: uuid.UUID,
    runner_ref: str,
) -> None:
    """Stamp a runner's opaque execution handle onto a task row.

    Generic and runner-agnostic: ``runner_ref`` is whatever string a runner
    needs to later identify its out-of-process execution. ``GcpJobRunner``
    parks the Cloud Run execution resource name here so the liveness probe can
    query the Executions API. Writes only ``runner_ref`` — no status churn —
    and works identically for the REST and dispatcher spawn paths.
    """
    task_schema = get_task_schema()
    sql = f"""
        UPDATE {task_schema}.tasks
        SET runner_ref = :runner_ref
        WHERE task_id = :task_id;
    """
    async with managed_transaction(engine) as conn:
        await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
            conn, task_id=task_id, runner_ref=runner_ref
        )


async def persist_outputs(
    engine: DbResource,
    task_id: uuid.UUID,
    outputs: Optional[Any],
) -> None:
    """Persist ``outputs`` (and ``progress = 100``) WITHOUT flipping the status.

    The #726-followup hardening: a distinct, idempotent write a runner lands
    *before* the terminal status flip (``complete_task``). Cloud Run reports an
    execution SUCCEEDED only once the container exits 0 — i.e. only after both
    writes — so a liveness reconciler that finds a SUCCEEDED execution on a
    still-``ACTIVE`` row can complete it from the ``outputs`` already on the
    row, instead of recovering an empty result. The status flip stays the
    exclusive job of ``complete_task``.
    """
    task_schema = get_task_schema()
    serialized_outputs = None
    if outputs is not None:
        from dynastore.tools.json import CustomJSONEncoder
        serialized_outputs = json.dumps(outputs, cls=CustomJSONEncoder)

    sql = f"""
        UPDATE {task_schema}.tasks
        SET outputs = :outputs,
            progress = 100
        WHERE task_id = :task_id;
    """
    async with managed_transaction(engine) as conn:
        await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
            conn, task_id=task_id, outputs=serialized_outputs
        )


def _decode_gcp_task_rows_inputs(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Decode the raw JSONB-as-text ``inputs`` column on each row, in place.

    asyncpg hands JSONB back as a JSON *string* under a raw ``text()``/
    ``DQLQuery`` read; ``apply_terminal_action`` only spreads ``inputs`` when
    ``isinstance(inputs, dict)``, so an un-decoded string silently drops the
    original payload (geoid#1743).
    """
    for row in rows:
        inputs_raw = row.get("inputs")
        if isinstance(inputs_raw, str):
            try:
                row["inputs"] = json.loads(inputs_raw)
            except (ValueError, TypeError):
                row["inputs"] = None
    return rows


async def select_lapsed_gcp_tasks(engine: DbResource) -> List[Dict[str, Any]]:
    """Return lapsed-lease Cloud Run task rows for the liveness reconciler.

    A single scan of the global tasks table for rows that are ``ACTIVE`` with
    an expired ``locked_until`` and a ``gcp_cloud_run_*`` owner — i.e. exactly
    the rows the pg_cron reaper would otherwise reclaim blindly. ``FOR UPDATE
    SKIP LOCKED`` so the reconciler and the reaper never fight over a row.

    Surfaces ``runner_ref`` (the probe handle), ``started_at`` (young-row grace
    check) and ``outputs`` (TERMINAL_SUCCEEDED reconciliation), plus the
    routing-continuation columns ``scope``, ``caller_id``, ``inputs`` and
    ``collection_id`` that ``apply_terminal_action`` threads into the
    ``on_success`` ROUTE follow-on — so the caller has everything it needs
    without a second round-trip (geoid#1743). Also surfaces ``timestamp``
    (creation time / RANGE-partition key) so the reconciler can thread it
    into the heartbeat/terminal write it issues for the row, enabling
    partition pruning on that write (#3218).

    ``inputs`` is decoded back to a ``dict`` here: asyncpg hands JSONB back as a
    JSON *string* under a raw ``text()``/``DQLQuery`` read, and the consumer
    ``apply_terminal_action`` only spreads ``inputs`` when ``isinstance(inputs,
    dict)`` — a raw string would silently fall through to ``{}`` and drop the
    original payload (the very data loss geoid#1743 set out to fix).
    """
    task_schema = get_task_schema()
    sql = f"""
        SELECT task_id, catalog_id, task_type, owner_id, runner_ref,
               started_at, locked_until, retry_count, max_retries, outputs,
               scope, caller_id, inputs, collection_id, timestamp
        FROM {task_schema}.tasks
        WHERE status = 'ACTIVE'
          AND locked_until < NOW()
          AND owner_id LIKE 'gcp_cloud_run_%'
        FOR UPDATE SKIP LOCKED
        LIMIT 500;
    """
    async with managed_transaction(engine) as conn:
        rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(conn)
    return _decode_gcp_task_rows_inputs(rows or [])


async def select_stale_gcp_tasks(
    engine: DbResource, grace_seconds: float
) -> List[Dict[str, Any]]:
    """Return lapsed ``ACTIVE`` Cloud Run rows via a plain, non-locking SELECT.

    geoid#2819: :func:`select_lapsed_gcp_tasks` takes ``FOR UPDATE SKIP
    LOCKED`` — correct for avoiding a fight with the pg_cron reaper, but it
    means a row held by a zombie PG session (a SIGKILLed remote backend that
    left an idle-in-transaction lock behind) is silently skipped on *every*
    pass, not just delayed. This query has no ``FOR UPDATE`` clause, so it
    sees such a row regardless of any lock another session holds on it — the
    liveness reconciler diffs this result against
    :func:`select_lapsed_gcp_tasks`'s to tell "naturally lapsed, about to be
    claimed" apart from "stuck behind a lock, invisible to every locking
    scan".

    ``grace_seconds`` is deliberately larger than the locking scan's implicit
    zero-grace ``locked_until < NOW()`` — it filters out rows that merely
    lapsed within the last reconciler tick or two, so this scan only reports
    rows old enough that a healthy locking scan would certainly have reached
    them by now.

    Returns the identical row shape as :func:`select_lapsed_gcp_tasks` so a
    row surfaced here can be handed straight to
    ``GcpLivenessReconciler._reconcile_row`` for a lock-free heal attempt.
    """
    task_schema = get_task_schema()
    sql = f"""
        SELECT task_id, catalog_id, task_type, owner_id, runner_ref,
               started_at, locked_until, retry_count, max_retries, outputs,
               scope, caller_id, inputs, collection_id, timestamp
        FROM {task_schema}.tasks
        WHERE status = 'ACTIVE'
          AND locked_until < NOW() - make_interval(secs => :grace_seconds)
          AND owner_id LIKE 'gcp_cloud_run_%'
        LIMIT 500;
    """
    async with managed_transaction(engine) as conn:
        rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
            conn, grace_seconds=grace_seconds
        )
    return _decode_gcp_task_rows_inputs(rows or [])


async def select_dismissed_unconfirmed_gcp_tasks(
    engine: DbResource,
) -> List[Dict[str, Any]]:
    """Return DISMISSED Cloud Run task rows whose stop is not yet confirmed.

    Selects rows where ``status = 'DISMISSED'`` and ``dismiss_confirmed_at IS
    NULL`` — i.e. an ACTIVE/RUNNING job that was dismissed while compute was
    in flight.  ``locked_until < NOW()`` is intentionally NOT required: a
    just-dismissed ACTIVE job still holds a live lease; the reconciler must
    act immediately regardless.

    ``FOR UPDATE SKIP LOCKED`` so concurrent reconciler pods never double-act
    on the same row.  ``timestamp`` and ``last_heartbeat_at`` are included so
    the caller can derive elapsed time for the force-stop deadline without a
    second round-trip.
    """
    task_schema = get_task_schema()
    sql = f"""
        SELECT task_id, catalog_id, task_type, owner_id, runner_ref,
               timestamp, started_at, last_heartbeat_at, retry_count, max_retries
        FROM {task_schema}.tasks
        WHERE status = 'DISMISSED'
          AND dismiss_confirmed_at IS NULL
          AND owner_id LIKE 'gcp_cloud_run_%'
        FOR UPDATE SKIP LOCKED
        LIMIT 500;
    """
    async with managed_transaction(engine) as conn:
        rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(conn)
    return rows or []


async def stamp_dismiss_confirmed(
    engine: DbResource,
    task_id: uuid.UUID,
) -> bool:
    """Stamp ``dismiss_confirmed_at = NOW()`` on a DISMISSED row.

    Gated on ``status = 'DISMISSED'`` so a race where the row has been
    updated to another status between the reconciler's SELECT and this write
    is handled safely (returns ``False`` with no write).  Returns ``True``
    when the row was updated, ``False`` when it was not found or already had
    ``dismiss_confirmed_at`` set.
    """
    task_schema = get_task_schema()
    sql = f"""
        UPDATE {task_schema}.tasks
        SET dismiss_confirmed_at = NOW()
        WHERE task_id = :task_id
          AND status = 'DISMISSED'
          AND dismiss_confirmed_at IS NULL;
    """
    async with managed_transaction(engine) as conn:
        rowcount = await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(
            conn, task_id=task_id
        )
    return bool(rowcount and rowcount > 0)


async def claim_for_execution(
    engine: DbResource,
    task_id: uuid.UUID,
    schema: str,
    owner_id: str,
    visibility_timeout: timedelta,
) -> Optional[Dict[str, Any]]:
    """Atomically claim a task for in-job execution (``main_task.py``).

    This is the consuming-side counterpart to ``claim_for_dispatch``: the
    Cloud Run Job container calls it once it is up, to take ownership of
    the row the spawner created for it. Unlike the legacy unconditional
    ``update_task(status=ACTIVE)`` it replaced, the claim is status-guarded
    — it matches the row **only if** it is safe to (re-)execute:

    * refuses any terminal row (``COMPLETED`` / ``FAILED`` / ``DISMISSED`` /
      ``DEAD_LETTER``) — re-running a finished task is the #726 regression
      (the reaper reclaimed a still-cold-starting row, a second Cloud Run
      execution spawned, and it re-ran an already-COMPLETED task);
    * refuses an ``ACTIVE`` row whose lease is still live and whose ``owner_id``
      belongs to a *different* execution — that is a concurrent duplicate.

    The happy path still matches: a freshly born-claimed row is ``ACTIVE`` under
    *this* execution's ``owner_id`` (the spawner stamps ``gcp_cloud_run_{id}``
    and passes the same id via ``DYNASTORE_EXECUTION_ID``), and a row the reaper
    reset is back to ``PENDING``.

    Returns the claimed row dict, or ``None`` when the task must not run — the
    caller (``main_task.py``) then exits cleanly without executing it.

    ``RETURNING`` includes ``timestamp`` (the row's creation time / RANGE-
    partition key) so the caller can thread it into every subsequent
    heartbeat / terminal write on this row (``complete_task`` / ``fail_task``
    / ``heartbeat_tasks``), enabling partition pruning on those writes
    without a second round-trip (#3218).
    """
    task_schema = get_task_schema()
    locked_until = datetime.now(timezone.utc) + visibility_timeout
    sql = f"""
        UPDATE {task_schema}.tasks
        SET status = 'ACTIVE',
            owner_id = :owner_id,
            locked_until = :locked_until,
            started_at = COALESCE(started_at, NOW()),
            last_heartbeat_at = NOW()
        WHERE task_id = :task_id
          AND catalog_id = :catalog_id
          AND status NOT IN ('COMPLETED', 'FAILED', 'DISMISSED', 'DEAD_LETTER')
          AND NOT (
                status = 'ACTIVE'
                AND locked_until IS NOT NULL
                AND locked_until > NOW()
                AND owner_id IS DISTINCT FROM :owner_id
          )
        RETURNING task_id, status, owner_id, timestamp;
    """
    async with managed_transaction(engine) as conn:
        return await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn,
            task_id=task_id,
            catalog_id=schema,
            owner_id=owner_id,
            locked_until=locked_until,
        )


async def claim_for_dispatch(
    engine: DbResource,
    task_id: uuid.UUID,
    owner_id: str,
    locked_until: datetime,
    expected_owner_prefix: Optional[str] = None,
    prior_owner_id: Optional[str] = None,
) -> bool:
    """Conditionally take ownership of an ACTIVE task without a fresh claim.

    Used by runners on the dispatcher path to extend the lease and stamp
    themselves as owner *only if* the row is unowned, owned by a peer of
    the same runner family (matched by ``expected_owner_prefix`` LIKE), or
    owned by the immediate dispatcher predecessor (``prior_owner_id``
    exact match — the in-process dispatcher claim that delegated to this
    runner). Returns True when the UPDATE matched a row, False otherwise
    — callers should treat False as "another worker already owns this
    task; do not spawn the side-effect (e.g. Cloud Run Job)".

    Belt-and-suspenders against any future regression that re-opens a
    create→claim race on the producing side.
    """
    task_schema = get_task_schema()
    # Cast `expected_owner_prefix` to ::text for the standalone IS NOT NULL
    # check. asyncpg cannot infer the parameter's type from `IS NOT NULL`
    # alone (the LIKE branch supplies text inference but the conjunction's
    # first arm doesn't), so prepare-time fails with
    # `AmbiguousParameterError: could not determine data type of parameter $4`
    # — observed live in the dispatcher path on dev catalog. The `||`
    # concatenation in the LIKE branch already forces text on the other
    # reference, so casting just the IS-NOT-NULL site is sufficient.
    sql = f"""
        UPDATE {task_schema}.tasks
        SET owner_id = :owner_id,
            locked_until = :locked_until,
            last_heartbeat_at = NOW(),
            started_at = NULL
        WHERE task_id = :task_id
          AND status = 'ACTIVE'
          AND (
              owner_id IS NULL
              OR (CAST(:expected_owner_prefix AS TEXT) IS NOT NULL
                  AND owner_id LIKE :expected_owner_prefix || '%')
              OR owner_id = :owner_id
              OR (CAST(:prior_owner_id AS TEXT) IS NOT NULL
                  AND owner_id = :prior_owner_id)
          )
        RETURNING task_id;
    """
    async with managed_transaction(engine) as conn:
        row = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn,
            task_id=task_id,
            owner_id=owner_id,
            locked_until=locked_until,
            expected_owner_prefix=expected_owner_prefix,
            prior_owner_id=prior_owner_id,
        )
    return row is not None


async def reset_task_to_pending(
    engine: DbResource,
    task_id: uuid.UUID,
    backoff: Optional[timedelta] = None,
) -> None:
    """Requeue an ACTIVE task to PENDING without incrementing retry_count.

    Called when a runner's ``can_claim`` rejects a row (dispatcher back-off
    path) or a Cloud Run Job container is cancelled mid-execution
    (``main_task.py`` on SIGTERM), so the task stays visible for another
    process to pick up rather than being lost.

    ``backoff`` sets ``locked_until = NOW() + backoff`` so the same worker
    that released the claim cannot immediately re-claim on the next poll.
    Without back-off a worker that consistently refuses to handle a row
    (e.g. payload-aware ``can_claim`` returning False) would hot-loop.
    """
    task_schema = get_task_schema()
    params: Dict[str, Any] = {"task_id": task_id}
    if backoff is not None:
        locked_until_clause = "locked_until = :backoff_until"
        params["backoff_until"] = datetime.now(timezone.utc) + backoff
    else:
        locked_until_clause = "locked_until = NULL"
    sql = f"""
        UPDATE {task_schema}.tasks
        SET status = 'PENDING',
            {locked_until_clause},
            owner_id = NULL,
            last_heartbeat_at = NULL
        WHERE task_id = :task_id
          AND status = 'ACTIVE';
    """
    async with managed_transaction(engine) as conn:
        await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
            conn, **params,
        )


async def find_stale_tasks(
    engine: DbResource,
    stale_threshold: timedelta,
    schema_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Find active tasks with expired locks (stuck-task diagnostics).
    If schema_name is provided, scopes to that tenant (matches the catalog_id column value).
    """
    task_schema = get_task_schema()
    cutoff = datetime.now(timezone.utc) - stale_threshold

    schema_filter = ""
    params: Dict[str, Any] = {"cutoff": cutoff}
    if schema_name is not None:
        schema_filter = "AND catalog_id = :catalog_id"
        params["catalog_id"] = schema_name

    sql = f"""
        SELECT task_id, catalog_id, task_type, execution_mode, retry_count, max_retries,
               owner_id, locked_until, last_heartbeat_at
        FROM {task_schema}.tasks
        WHERE status = 'ACTIVE'
          AND locked_until < :cutoff
          {schema_filter}
        ORDER BY locked_until ASC
        LIMIT 500;
    """
    async with managed_transaction(engine) as conn:
        rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
            conn, **params
        )
    return rows or []


async def cleanup_orphan_tasks(
    engine: DbResource,
    grace_period: timedelta,
) -> int:
    """
    Move tasks for deleted catalogs to DEAD_LETTER.

    Checks catalog_id against existing catalog ids. Tasks whose
    catalog_id no longer exists and whose creation timestamp is older
    than grace_period are dead-lettered.
    """
    task_schema = get_task_schema()
    cutoff = datetime.now(timezone.utc) - grace_period

    async with managed_transaction(engine) as conn:
        # catalog.catalogs may not exist on new DBs or partial inits — skip if absent
        if not await check_table_exists(conn, "catalogs", "catalog"):
            return 0

        # Find orphaned tasks: catalog_id not in any active catalog id
        # and task is not already in a terminal state
        sql = f"""
            WITH active_schemas AS (
                SELECT DISTINCT id
                FROM catalog.catalogs
                WHERE deleted_at IS NULL
            )
            UPDATE {task_schema}.tasks t
            SET status = 'DEAD_LETTER',
                error_message = 'Orphaned: catalog no longer exists',
                finished_at = NOW(),
                locked_until = NULL
            WHERE t.status IN ('PENDING', 'ACTIVE')
              AND t.scope = 'CATALOG'
              AND t.timestamp < :cutoff
              AND t.catalog_id NOT IN (SELECT id FROM active_schemas)
              AND t.catalog_id != 'system';
        """

        result = await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(
            conn, cutoff=cutoff
        )
    return result or 0
