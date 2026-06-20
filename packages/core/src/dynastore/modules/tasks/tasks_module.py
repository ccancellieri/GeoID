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

import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from typing import List, Optional, Any, Dict, AsyncGenerator, Union
from dynastore.tools.cache import cached
from dynastore.models.driver_context import DriverContext
from dynastore.modules import ModuleProtocol
from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DDLBatch,
    DQLQuery,
    managed_transaction,
    ResultHandler,
    DbResource,
    run_in_event_loop,
)
from dynastore.modules.db_config.locking_tools import (
    check_table_exists,
    check_trigger_exists,
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

from .models import Task, TaskCreate, TaskUpdate

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

# --- DDL Definitions ---

# --- Step 1: Table creation only (IF NOT EXISTS safe) ---

GLOBAL_TASKS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.tasks (
    task_id           UUID          NOT NULL,
    schema_name       VARCHAR(255)  NOT NULL,
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
    ON {schema}.tasks (schema_name, status);
-- Dedup index: includes timestamp (partition key) as PG requires it for
-- unique indexes on partitioned tables. Per-partition uniqueness.
-- cross-partition dedup enforced at the application layer in enqueue().
CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_dedup
    ON {schema}.tasks (schema_name, dedup_key, timestamp)
    WHERE dedup_key IS NOT NULL AND status NOT IN ('COMPLETED', 'FAILED', 'DEAD_LETTER');
CREATE INDEX IF NOT EXISTS idx_tasks_caller
    ON {schema}.tasks (caller_id);
CREATE INDEX IF NOT EXISTS idx_tasks_timestamp
    ON {schema}.tasks (timestamp DESC);
-- task_id lookup index: enables complete/fail/heartbeat without full partition scan
CREATE INDEX IF NOT EXISTS idx_tasks_task_id
    ON {schema}.tasks (task_id);

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
#     = owner pod died / got SIGKILL / network partition / OOM).
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

# NOTE: {schema} survives until DDLQuery substitutes it via
# str.replace("{schema}", value) — NOT str.format — so a doubled brace
# (\d{{4}}) is NOT collapsed and PostgreSQL would receive the literal
# "\d{{4}}", which matches no partition and silently disables retention.
# The regexes below therefore use the single-brace form \d{4}_\d{2}.
GLOBAL_TASKS_RETENTION_FUNC_DDL = """
CREATE OR REPLACE FUNCTION "{schema}"."maintain_partitions_{schema}_tasks"() RETURNS void AS $$
DECLARE
    row RECORD;
    cutoff_date DATE;
    default_deleted BIGINT;
    prune_count INT;
    prune_list TEXT;
BEGIN
    -- Bound AccessExclusiveLock wait: if a partition is being scanned
    -- fail this DROP fast and let the next supervisor tick retry.
    SET LOCAL lock_timeout = '10s';
    -- 'daily' is not a valid PostgreSQL date_trunc unit; the correct unit is 'day'.
    cutoff_date := date_trunc('day', NOW()) - INTERVAL '1 month';
    -- Pre-flight (#2106): announce, at LOG level, how many monthly leaf
    -- partitions this run will DROP and their names.  The per-partition message
    -- below is NOTICE, which the server log suppresses at the default
    -- log_min_messages=WARNING, so without this the deletion is invisible in
    -- production.  A retention fix applied to a long-running deployment can drop
    -- several months of accumulated partitions on the first tick; this makes
    -- that one-time bulk prune observable instead of silent.
    SELECT count(*), string_agg(c.relname, ', ' ORDER BY c.relname)
      INTO prune_count, prune_list
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = '{schema}' AND c.relkind = 'r'
        AND c.relname ~ '^tasks_\\d{4}_\\d{2}$'
        AND to_date(substring(c.relname from '\\d{4}_\\d{2}$'), 'YYYY_MM') < cutoff_date;
    IF prune_count > 0 THEN
        RAISE LOG 'partition retention [{schema}.tasks]: dropping % monthly partition(s) older than % : %', prune_count, cutoff_date, prune_list;
    END IF;
    FOR row IN SELECT relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = '{schema}' AND c.relkind = 'r' AND c.relname ~ '^tasks_\\d{4}_\\d{2}$' LOOP
        DECLARE
            date_str TEXT;
            part_date DATE;
        BEGIN
            date_str := substring(row.relname from '\\d{4}_\\d{2}$');
            part_date := to_date(date_str, 'YYYY_MM');
            IF part_date < cutoff_date THEN
                RAISE NOTICE 'Pruning old partition: {schema}.%', row.relname;
                EXECUTE format('DROP TABLE "{schema}".%I', row.relname);
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Failed to process partition {schema}.%: %', row.relname, SQLERRM;
        END;
    END LOOP;
    -- Drain rows from the DEFAULT partition (catches clock-skew / far-future
    -- timestamps that never land in a monthly partition).  Idempotent: a
    -- DELETE WHERE nothing matches is a no-op.
    DELETE FROM "{schema}".tasks_default
    WHERE timestamp < (NOW() - INTERVAL '1 month');
    GET DIAGNOSTICS default_deleted = ROW_COUNT;
    IF default_deleted > 0 THEN
        RAISE NOTICE 'Pruned % row(s) from {schema}.tasks_default', default_deleted;
    END IF;
END;
$$ LANGUAGE plpgsql;
"""

GLOBAL_TASKS_PARTCREATE_FUNC_DDL = """
CREATE OR REPLACE FUNCTION "{schema}"."create_partitions_{schema}_tasks"() RETURNS void AS $$
DECLARE
    i INT;
    target_date DATE;
    start_date TIMESTAMPTZ;
    end_date TIMESTAMPTZ;
    part_name TEXT;
BEGIN
    FOR i IN 0..3 LOOP
        target_date := date_trunc('month', NOW()) + (i || ' months')::INTERVAL;
        start_date := target_date;
        end_date := target_date + INTERVAL '1 month';
        part_name := 'tasks_' || to_char(target_date, 'YYYY_MM');
        IF NOT EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = '{schema}' AND c.relname = part_name
        ) THEN
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS "{schema}".%I PARTITION OF "{schema}"."tasks" FOR VALUES FROM (%L) TO (%L)',
                part_name, start_date::TEXT, end_date::TEXT
            );
            RAISE NOTICE 'Created partition: {schema}.%', part_name;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
"""

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

GLOBAL_TASKS_REAPER_DDL = """
CREATE OR REPLACE FUNCTION {schema}.reap_stuck_tasks(
    p_max_retries INT DEFAULT 3,
    p_hard_cap INT DEFAULT 5
) RETURNS INTEGER LANGUAGE plpgsql AS $func$
DECLARE
    reaped INT;
    dead_lettered INT;
BEGIN
    WITH stuck AS (
        SELECT timestamp, task_id, retry_count, max_retries
        FROM {schema}.tasks
        WHERE status = 'ACTIVE'
          AND locked_until < NOW()
        FOR UPDATE SKIP LOCKED
    ),
    reset AS (
        UPDATE {schema}.tasks t
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
                WHEN s.retry_count + 1 >= p_hard_cap
                    THEN 'Reaped: hard retry cap (' || p_hard_cap || ') reached'
                ELSE 'Reaped by {schema}.reap_stuck_tasks (heartbeat expired)'
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
        RAISE WARNING 'dynastore.task.hard_cap_hit: % task(s) moved to DEAD_LETTER in {schema} this pass', dead_lettered;
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
        task_ids: List[uuid.UUID],
        visibility_timeout: timedelta,
    ) -> None:
        return await heartbeat_tasks(engine, task_ids, visibility_timeout)

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
                # acquire_startup_lock guarantees that.
                from dynastore.modules.db_config.locking_tools import acquire_startup_lock
                async with acquire_startup_lock(
                    engine, f"tasks_storage_init.{schema}"
                ) as locked_conn:
                    if locked_conn is None:
                        raise RuntimeError(
                            f"TasksModule: could not acquire startup lock for '{schema}.tasks' "
                            "initialization — refusing to start dispatcher."
                        )
                    await ensure_task_storage_exists(locked_conn, schema)
                    from dynastore.modules.tasks.workclass_ddl import (
                        ensure_workclass_storage_exists,
                    )
                    await ensure_workclass_storage_exists(locked_conn, schema)

                # Ensure configs.task_capability_registry exists before the
                # backstop/sweep loops start querying it. PlatformConfigService
                # owns this DDL but may have skipped it if DBService (priority 10)
                # was not yet up when DBConfigModule (priority 0) ran its lifespan.
                # TasksModule (priority 15) always runs after DBService, so by this
                # point the engine is present and the idempotent CREATE TABLE IF NOT
                # EXISTS is safe.  Advisory lock mirrors the tasks_storage_init
                # namespace pattern: one pod wins per cold-start, others skip.
                from dynastore.modules.db_config.typed_store.ddl import (
                    TASK_CAPABILITY_REGISTRY_DDL,
                )
                async with acquire_startup_lock(
                    engine, f"tasks_storage_init.{schema}.registry"
                ) as reg_conn:
                    if reg_conn is not None:
                        await DDLQuery(TASK_CAPABILITY_REGISTRY_DDL).execute(reg_conn)
                        logger.info(
                            "TasksModule: configs.task_capability_registry ensured."
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
                from dynastore.modules.tasks.queue import QueueListenerService
                from dynastore.modules.tasks.dispatcher import DispatcherService
                from dynastore.modules.tasks.capability_publisher import (
                    CapabilityPublisherService,
                )
                from dynastore.modules.tasks.registry.publisher import (
                    RegistryHeartbeatService,
                )
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
                supervisor.register(QueueListenerService(poll_timeout=poll_interval))
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
# All queries target the global tasks table. The `schema_name` column
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


class StuckPendingWarnerService(PeriodicService):
    """Periodic read-only scan for stuck PENDING tasks (retry_count=0).

    Runs on every pod (RUN_EVERYWHERE) and skips ephemeral Cloud Run Job
    pods (SKIP_EPHEMERAL) — job pods claim one task and exit, never managing
    stuck rows. Resolves #2279 for this loop.

    The scan, log, and redispatch body is implemented in tick() — PeriodicService
    supplies the loop, shutdown handling, and the initial tick. Note that
    PeriodicService ticks IMMEDIATELY on startup then on cadence (the old
    hand-rolled loop slept first); this is safe because the min_age guard
    (min_age_s) filters freshly-enqueued rows.
    """

    name = "stuck_pending_warner"
    leadership = Leadership.RUN_EVERYWHERE
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
                f'SELECT task_id, task_type, schema_name, inputs, '  # nosec
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
        try:
            async with managed_transaction(ctx.engine) as conn:
                rows = await self._query.execute(
                    conn,
                    min_age_s=self._min_age_s,
                    sample_limit=self._sample_limit,
                )
            rows = rows or []
            await _emit_stuck_pending_logs(rows)
            if rows:
                await _redispatch_stuck_rows(ctx.engine, rows)
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
    AND no ``PENDING``/``ACTIVE`` ``gcp_provision_catalog`` task pointing at the
    same ``catalog_id`` in its ``inputs`` — and calls
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
                AND t.task_type = 'gcp_provision_catalog'
                AND t.inputs->>'catalog_id' = c.id
          )
        LIMIT :sample_limit;
    """
    try:
        async with managed_transaction(engine) as conn:
            if not await check_table_exists(conn, "catalogs", "catalog"):
                return 0
            rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
                conn, min_age_s=min_age_s, sample_limit=sample_limit,
            )
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


async def _run_mandatory_backstop_pass(
    engine: DbResource, schema: str, *, ttl_grace_seconds: float, min_age_s: float
) -> None:
    """Leader-coordinated (advisory-locked) backstop pass: log mandatory-ownership
    violations and DLQ capability-less unclaimable PENDING rows. Runs on whichever
    pod wins ``pg_try_advisory_xact_lock`` for this pass; others return immediately.
    Fail-open: any error is logged and swallowed."""
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )
    from dynastore.modules.tasks.dispatcher import (
        _stable_advisory_lock_key, sweep_unclaimable_rows,
        auto_requeue_recovered_mandatory,
    )
    from dynastore.modules.tasks.mandatory import check_mandatory_ownership

    lock_key = _stable_advisory_lock_key("dynastore.mandatory.backstop")
    try:
        async with managed_transaction(engine) as conn:
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
    loop and shutdown handling. Note that PeriodicService ticks IMMEDIATELY
    on startup then on cadence (the old hand-rolled loop slept first); this
    is safe because the min_age guard (min_age_s) filters freshly-enqueued rows.
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
    async with managed_transaction(engine) as conn:
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
            row["task_id"], row["task_type"], row.get("schema_name"),
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
    Multi-tenancy is column-based — ``schema_name`` on each task row carries
    the catalog physical schema; the table itself is never duplicated per
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
            f"and were never read. Use schema_name='{schema}' on the row "
            f"instead (the tenant discriminator column)."
        )

    # Ensure schema exists first
    await ensure_schema_exists(conn, schema)

    # Single module-level batch: on warm starts the sentinel
    # (status-update trigger) short-circuits everything in one round-trip.
    # Cold starts create the table (+ DEFAULT partition), indexes, notify
    # functions, and both triggers in order under nested savepoints.
    await _build_tasks_ddl_batch(schema).execute(conn, schema=schema)

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
    await DDLQuery(GLOBAL_TASKS_RETENTION_FUNC_DDL).execute(conn, schema=schema)
    await DDLQuery(GLOBAL_TASKS_PARTCREATE_FUNC_DDL).execute(conn, schema=schema)

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
) -> List[Task]:
    """
    Lists tasks from a catalog's schema.
    Uses CatalogsProtocol to resolve the physical schema.

    Pass ``kind`` to narrow results to a specific task type (forwarded to
    :func:`list_tasks`).
    """
    schema = await _resolve_catalog_schema(catalog_id, conn)
    return await list_tasks(conn, schema, limit, offset, kind=kind)


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
# The `schema` parameter in these functions refers to the `schema_name` column
# value (e.g. tenant schema "s_abc123" or "system"), NOT the PostgreSQL schema
# that hosts the table.  The actual table lives in `get_task_schema()`.tasks.

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
    Creates a new task in the global tasks table with schema_name = `schema`.

    Pass initial_status='RUNNING' to bypass the dispatcher queue (e.g. for
    audit tasks created by BackgroundRunner that are already being executed
    in-process and must not be re-claimed by the dispatcher).

    Pass `owner_id` and `locked_until` together with `initial_status='ACTIVE'`
    to INSERT a row that is *born claimed* — same effect as create_task →
    claim_by_id, but in a single statement and without firing the
    `notify_task_ready` trigger (which only fires `WHEN NEW.status =
    'PENDING'`). Used by GcpJobRunner's REST path to close the
    REST↔dispatcher race window where a freshly-created PENDING row could
    be claimed by a dispatcher pod between INSERT and the subsequent
    update_task(ACTIVE), spawning a duplicate Cloud Run Job.

    Dedup: if `task_data.dedup_key` is set, a pre-check rejects insert when
    a non-terminal task already carries the same (schema_name, dedup_key) —
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
                  AND schema_name = :schema_name
                  AND status NOT IN ('COMPLETED', 'FAILED', 'DEAD_LETTER')
                LIMIT 1;
            """
            existing = await DQLQuery(
                check_sql, result_handler=ResultHandler.ONE_DICT
            ).execute(conn, dedup_key=task_data.dedup_key, schema_name=schema)
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
            "task_id", "schema_name", "scope", "caller_id", "task_type", "type",
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
            schema_name=schema,
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
        # Stamp ACTIVE timing fields so the row looks identical to one
        # claim_batch / claim_by_id would have produced.
        if initial_status == "ACTIVE":
            sql_extra = ", started_at, last_heartbeat_at"
            values_extra = ", NOW(), NOW()"
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

    sql = f'UPDATE {task_schema}.tasks SET {set_sql} WHERE task_id = :task_id AND schema_name = :schema_name RETURNING *;'

    query_params = {**update_fields, "task_id": task_id, "schema_name": schema}

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


@cached(maxsize=256, namespace="tasks", ignore=["conn"])
async def get_task(conn: DbResource, task_id: uuid.UUID, schema: str) -> Optional[Task]:
    """Retrieves a single task by its ID from the global tasks table."""
    task_schema = get_task_schema()
    sql = f'SELECT * FROM {task_schema}.tasks WHERE task_id = :task_id AND schema_name = :schema_name;'
    task_dict = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
        conn, task_id=task_id, schema_name=schema
    )
    return Task.model_validate(task_dict) if task_dict else None


async def get_task_by_id_unscoped(
    conn: DbResource, task_id: uuid.UUID
) -> Optional[Task]:
    """Retrieve a task by ``task_id`` alone, ignoring the tenant ``schema_name``.

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


async def list_tasks(
    conn: DbResource,
    schema: str,
    limit: int = 20,
    offset: int = 0,
    kind: Optional[str] = None,
) -> List[Task]:
    """Lists tasks filtered by schema_name, ordered by creation date.

    Pass ``kind`` to narrow results to a specific task type (e.g. ``'process'``
    for OGC Process jobs only).  When ``None`` all types are returned — the
    admin Tasks API relies on this full view.
    """
    task_schema = get_task_schema()
    if kind is not None:
        sql = f'SELECT * FROM {task_schema}.tasks WHERE schema_name = :schema_name AND type = :kind ORDER BY timestamp DESC LIMIT :limit OFFSET :offset;'
        task_dicts = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
            conn, schema_name=schema, limit=limit, offset=offset, kind=kind
        )
    else:
        sql = f'SELECT * FROM {task_schema}.tasks WHERE schema_name = :schema_name ORDER BY timestamp DESC LIMIT :limit OFFSET :offset;'
        task_dicts = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
            conn, schema_name=schema, limit=limit, offset=offset
        )
    return [Task.model_validate(t) for t in task_dicts]


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
                  AND schema_name = :schema_name
                  AND status NOT IN ('COMPLETED', 'FAILED', 'DEAD_LETTER')
                LIMIT 1;
            """
            existing = await DQLQuery(
                check_sql, result_handler=ResultHandler.ONE_DICT
            ).execute(conn, dedup_key=dedup_key, schema_name=schema_name)
            if existing:
                return None

            sql = f"""
                INSERT INTO {task_schema}.tasks
                    (task_id, schema_name, scope, caller_id, task_type, type,
                     execution_mode, inputs, timestamp, collection_id, dedup_key)
                VALUES
                    (:task_id, :schema_name, :scope, :caller_id, :task_type, :type,
                     :execution_mode, :inputs, :timestamp, :collection_id, :dedup_key)
                ON CONFLICT (schema_name, dedup_key, timestamp)
                    WHERE dedup_key IS NOT NULL
                    AND status NOT IN ('COMPLETED', 'FAILED', 'DEAD_LETTER')
                DO NOTHING
                RETURNING *;
            """
        else:
            sql = f"""
                INSERT INTO {task_schema}.tasks
                    (task_id, schema_name, scope, caller_id, task_type, type,
                     execution_mode, inputs, timestamp, collection_id)
                VALUES
                    (:task_id, :schema_name, :scope, :caller_id, :task_type, :type,
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
            schema_name=schema_name,
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
        RETURNING task_id, schema_name, scope, task_type, execution_mode,
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

    # Fairness: pick the oldest PENDING task per tenant (schema_name) first,
    # then fill remaining batch slots from those results. This prevents a
    # single high-volume tenant from monopolising all claim slots.
    # DISTINCT ON (schema_name) ORDER BY schema_name, timestamp ASC
    # returns exactly one row per tenant — the oldest eligible task.
    # DISTINCT ON and FOR UPDATE SKIP LOCKED cannot be combined in the same
    # SELECT (PostgreSQL forbids FOR UPDATE with DISTINCT). Use a two-step
    # approach: a CTE picks one candidate per tenant (oldest PENDING task via
    # DISTINCT ON), then the outer SELECT locks those specific rows.
    #
    # Circuit breaker: rows whose retry_count has reached the platform-wide
    # ``hard_cap`` are invisible to dispatchers — the reaper will DLQ them on
    # its next pass. This caps the cost of any future re-enqueue regression.
    sql = f"""
        WITH candidates AS (
            SELECT DISTINCT ON (schema_name) timestamp, task_id
            FROM {task_schema}.tasks
            WHERE status = 'PENDING'
              AND timestamp >= :lookback
              AND (locked_until IS NULL OR locked_until <= :now)
              AND retry_count < :hard_cap
              AND ({mode_filter})
            ORDER BY schema_name, timestamp ASC
        )
        UPDATE {task_schema}.tasks
        SET status = 'ACTIVE',
            locked_until = :locked_until,
            owner_id = :owner_id,
            started_at = COALESCE(started_at, NOW()),
            last_heartbeat_at = NOW()
        WHERE (timestamp, task_id) IN (
            SELECT timestamp, task_id
            FROM {task_schema}.tasks
            WHERE (timestamp, task_id) IN (SELECT timestamp, task_id FROM candidates)
              AND status = 'PENDING'
            LIMIT :batch_size
            FOR UPDATE SKIP LOCKED
        )
        RETURNING task_id, schema_name, scope, task_type, execution_mode,
                  caller_id, inputs, collection_id, retry_count, max_retries,
                  timestamp, dedup_key, owner_id;
    """

    async with managed_transaction(engine) as conn:
        result = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
            conn, **params
        )

    return result or []


async def complete_task(
    engine: DbResource,
    task_id: uuid.UUID,
    timestamp: Any,
    outputs: Optional[Any] = None,
    *,
    owner_id: Optional[str] = None,
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
    sql = f"""
        UPDATE {task_schema}.tasks
        SET status = 'COMPLETED',
            progress = 100,
            finished_at = :finished_at,
            outputs = :outputs,
            locked_until = NULL,
            owner_id = NULL
        WHERE task_id = :task_id{owner_guard};
    """
    params: Dict[str, Any] = {
        "task_id": task_id,
        "finished_at": timestamp,
        "outputs": serialized_outputs,
    }
    if owner_id is not None:
        params["owner_id"] = owner_id
    async with managed_transaction(engine) as conn:
        rowcount = await DQLQuery(
            sql, result_handler=ResultHandler.ROWCOUNT
        ).execute(conn, **params)
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
    """
    task_schema = get_task_schema()
    owner_guard = " AND owner_id = :owner_id" if owner_id is not None else ""

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
            WHERE task_id = :task_id{owner_guard}
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
            WHERE task_id = :task_id{owner_guard}
            RETURNING status, task_type, caller_id, inputs, error_message;
        """
        params = {
            "task_id": task_id,
            "error_message": error_message,
            "finished_at": timestamp,
        }

    if owner_id is not None:
        params["owner_id"] = owner_id

    async with managed_transaction(engine) as conn:
        row = await DQLQuery(
            sql, result_handler=ResultHandler.ONE_DICT
        ).execute(conn, **params)

    if row is None:
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
    """
    task_schema = get_task_schema()
    owner_guard = " AND owner_id = :owner_id" if owner_id is not None else ""
    sql = f"""
        UPDATE {task_schema}.tasks
        SET status = 'DEAD_LETTER',
            error_message = :error_message,
            finished_at = :finished_at,
            locked_until = NULL,
            owner_id = NULL
        WHERE task_id = :task_id{owner_guard}
        RETURNING task_type, caller_id, inputs, error_message;
    """
    params: Dict[str, Any] = {
        "task_id": task_id,
        "error_message": error_message,
        "finished_at": timestamp,
    }
    if owner_id is not None:
        params["owner_id"] = owner_id
    async with managed_transaction(engine) as conn:
        row = await DQLQuery(
            sql, result_handler=ResultHandler.ONE_DICT
        ).execute(conn, **params)

    if row is None:
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
    task_ids: List[uuid.UUID],
    visibility_timeout: timedelta,
) -> None:
    """Extend locked_until for active tasks (batched heartbeat)."""
    if not task_ids:
        return

    task_schema = get_task_schema()
    new_locked_until = datetime.now(timezone.utc) + visibility_timeout

    sql = f"""
        UPDATE {task_schema}.tasks
        SET locked_until = :locked_until,
            last_heartbeat_at = NOW()
        WHERE task_id = ANY(:task_ids)
          AND status = 'ACTIVE';
    """
    async with managed_transaction(engine) as conn:
        await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
            conn, locked_until=new_locked_until, task_ids=list(task_ids)
        )


async def heartbeat_task_if_active(
    engine: DbResource,
    task_id: uuid.UUID,
    visibility_timeout: timedelta,
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
    """
    task_schema = get_task_schema()
    new_locked_until = datetime.now(timezone.utc) + visibility_timeout
    sql = f"""
        UPDATE {task_schema}.tasks
        SET locked_until = :locked_until,
            last_heartbeat_at = NOW()
        WHERE task_id = :task_id
          AND status = 'ACTIVE';
    """
    async with managed_transaction(engine) as conn:
        rowcount = await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(
            conn, locked_until=new_locked_until, task_id=task_id
        )
    return bool(rowcount and rowcount > 0)


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
    without a second round-trip (geoid#1743).

    ``inputs`` is decoded back to a ``dict`` here: asyncpg hands JSONB back as a
    JSON *string* under a raw ``text()``/``DQLQuery`` read, and the consumer
    ``apply_terminal_action`` only spreads ``inputs`` when ``isinstance(inputs,
    dict)`` — a raw string would silently fall through to ``{}`` and drop the
    original payload (the very data loss geoid#1743 set out to fix).
    """
    task_schema = get_task_schema()
    sql = f"""
        SELECT task_id, schema_name, task_type, owner_id, runner_ref,
               started_at, locked_until, retry_count, max_retries, outputs,
               scope, caller_id, inputs, collection_id
        FROM {task_schema}.tasks
        WHERE status = 'ACTIVE'
          AND locked_until < NOW()
          AND owner_id LIKE 'gcp_cloud_run_%'
        FOR UPDATE SKIP LOCKED
        LIMIT 500;
    """
    async with managed_transaction(engine) as conn:
        rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(conn)
    for row in rows or []:
        inputs_raw = row.get("inputs")
        if isinstance(inputs_raw, str):
            try:
                row["inputs"] = json.loads(inputs_raw)
            except (ValueError, TypeError):
                row["inputs"] = None
    return rows or []


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
        SELECT task_id, schema_name, task_type, owner_id, runner_ref,
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


async def claim_by_id(
    engine: DbResource,
    task_id: uuid.UUID,
    visibility_timeout: timedelta,
    owner_id: str,
) -> Optional[Dict[str, Any]]:
    """Atomically claim a specific PENDING task by ID (used by run_ephemeral)."""
    task_schema = get_task_schema()
    locked_until = datetime.now(timezone.utc) + visibility_timeout
    sql = f"""
        UPDATE {task_schema}.tasks
        SET status = 'ACTIVE',
            locked_until = :locked_until,
            owner_id = :owner_id,
            started_at = COALESCE(started_at, NOW()),
            last_heartbeat_at = NOW()
        WHERE task_id = :task_id
          AND status = 'PENDING'
        RETURNING task_id, schema_name, scope, task_type, execution_mode,
                  caller_id, inputs, collection_id, retry_count, max_retries,
                  timestamp, dedup_key, owner_id;
    """
    async with managed_transaction(engine) as conn:
        return await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn, task_id=task_id, locked_until=locked_until, owner_id=owner_id,
        )


async def claim_for_execution(
    engine: DbResource,
    task_id: uuid.UUID,
    schema: str,
    owner_id: str,
    visibility_timeout: timedelta,
) -> Optional[Dict[str, Any]]:
    """Atomically claim a task for in-job execution (``main_task.py``).

    This is the consuming-side counterpart to ``claim_by_id`` /
    ``claim_for_dispatch``: the Cloud Run Job container calls it once it is up,
    to take ownership of the row the spawner created for it. Unlike the legacy
    unconditional ``update_task(status=ACTIVE)`` it replaced, the claim is
    status-guarded — it matches the row **only if** it is safe to (re-)execute:

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
          AND schema_name = :schema_name
          AND status NOT IN ('COMPLETED', 'FAILED', 'DISMISSED', 'DEAD_LETTER')
          AND NOT (
                status = 'ACTIVE'
                AND locked_until IS NOT NULL
                AND locked_until > NOW()
                AND owner_id IS DISTINCT FROM :owner_id
          )
        RETURNING task_id, status, owner_id;
    """
    async with managed_transaction(engine) as conn:
        return await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn,
            task_id=task_id,
            schema_name=schema,
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
            last_heartbeat_at = NOW()
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

    Called by run_ephemeral on CancelledError so the task stays visible
    for another process to pick up rather than being lost on shutdown.

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
    Find active tasks with expired locks (janitor use).
    If schema_name is provided, scopes to that tenant.
    """
    task_schema = get_task_schema()
    cutoff = datetime.now(timezone.utc) - stale_threshold

    schema_filter = ""
    params: Dict[str, Any] = {"cutoff": cutoff}
    if schema_name is not None:
        schema_filter = "AND schema_name = :schema_name"
        params["schema_name"] = schema_name

    sql = f"""
        SELECT task_id, schema_name, task_type, execution_mode, retry_count, max_retries,
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

    Checks schema_name against existing catalog schemas. Tasks whose
    schema_name no longer exists and whose creation timestamp is older
    than grace_period are dead-lettered.
    """
    task_schema = get_task_schema()
    cutoff = datetime.now(timezone.utc) - grace_period

    async with managed_transaction(engine) as conn:
        # catalog.catalogs may not exist on new DBs or partial inits — skip if absent
        if not await check_table_exists(conn, "catalogs", "catalog"):
            return 0

        # Find orphaned tasks: schema_name not in any active catalog schema
        # and task is not already in a terminal state
        sql = f"""
            WITH active_schemas AS (
                SELECT DISTINCT physical_schema
                FROM catalog.catalogs
                WHERE deleted_at IS NULL
            )
            UPDATE {task_schema}.tasks t
            SET status = 'DEAD_LETTER',
                error_message = 'Orphaned: catalog schema no longer exists',
                finished_at = NOW(),
                locked_until = NULL
            WHERE t.status IN ('PENDING', 'ACTIVE')
              AND t.scope = 'CATALOG'
              AND t.timestamp < :cutoff
              AND t.schema_name NOT IN (SELECT physical_schema FROM active_schemas)
              AND t.schema_name != 'system';
        """

        result = await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(
            conn, cutoff=cutoff
        )
    return result or 0
