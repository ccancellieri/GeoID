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

# dynastore/modules/tasks/tasks_module.py

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from typing import List, Optional, Any, Dict, AsyncGenerator
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
from dynastore.modules.db_config.locking_tools import check_table_exists
from dynastore.modules.db_config.partition_tools import (
    ensure_hierarchical_partitions_exist,
    PartitionDefinition,
)
from dynastore.modules.db_config.maintenance_tools import (
    ensure_schema_exists,
    register_retention_policy,
    ensure_future_partitions,
)
from dynastore.modules.db_config.locking_tools import (
    acquire_lock_if_needed,
    check_table_exists,
    check_trigger_exists,
)

from .models import Task, TaskCreate, TaskUpdate

logger = logging.getLogger(__name__)


def _serialize_inputs(inputs: Any) -> Optional[str]:
    """Serialize a task ``inputs`` payload for JSONB storage.

    Uses :class:`dynastore.tools.json.CustomJSONEncoder` so datetime /
    UUID / Decimal / shapely values survive the round-trip to PG.

    Without this, producers that pass ``model_dump()`` of a pydantic
    model containing a ``datetime`` (e.g. ``BulkCatalogReindexInputs``
    emitted by the Elasticsearch module after a ``catalog_metadata_changed``
    event) blow up in ``tasks.create_task`` with::

        ElasticsearchModule: Failed to dispatch task
        elasticsearch_index: Object of type datetime is not JSON serializable

    The task row is never created, the reindex never runs, and the
    catalog silently drifts from its search index.

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
    collection_id     VARCHAR(255),
    locked_until      TIMESTAMPTZ,
    last_heartbeat_at TIMESTAMPTZ,
    owner_id          VARCHAR(255),
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
# pg_cron-driven stuck-task reaper
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
    the full sequence (table, indexes + functions, both triggers) under a
    single shared connection with nested savepoints.
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
            DDLQuery(GLOBAL_TASKS_INDEXES_DDL),
            DDLQuery(GLOBAL_TASKS_INSERT_TRIGGER_DDL, check_query=_check_insert),
            DDLQuery(GLOBAL_TASKS_STATUS_TRIGGER_DDL, check_query=_check_status),
        ],
    )





from dynastore.modules import ModuleProtocol
from dynastore.models.protocols import TasksProtocol
from dynastore.models.protocols.task_queue import TaskQueueProtocol
from dynastore.modules.processes.protocols import ProcessRegistryProtocol


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
        return await complete_task(engine, task_id, timestamp, outputs)

    async def fail(
        self,
        engine: Any,
        task_id: uuid.UUID,
        timestamp: Any,
        error_message: str,
        retry: bool = True,
    ) -> None:
        return await fail_task(engine, task_id, timestamp, error_message, retry)

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
        from dynastore.modules.tasks.queue import start_queue_listener
        from dynastore.modules.tasks.dispatcher import run_dispatcher
        from dynastore.tools.protocol_helpers import get_engine
        from dynastore.tasks import manage_tasks

        logger.info("TasksModule: Initialising task singletons …")

        from dynastore.tools.protocol_helpers import resolve
        from dynastore.models.protocols import DatabaseProtocol
        from dynastore.tasks import manage_tasks

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

            if engine is not None:
                executor = get_background_executor()
                schema = get_task_schema()

                # Load TasksPluginConfig BEFORE storage init so the reaper
                # cron command (registered inside ensure_task_storage_exists)
                # picks up the user-configured hard_retry_cap. claim_batch /
                # fail_task read the same module-level value at runtime.
                from dynastore.tools.discovery import get_protocol
                from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
                from dynastore.modules.tasks.tasks_config import TasksPluginConfig, TaskRoutingConfig

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
                    except Exception as e:
                        logger.warning(f"TasksModule: Failed to load TasksPluginConfig, defaulting to {poll_interval}s / hard_cap={hard_cap}: {e}")

                # TaskRoutingConfig is consumed lazily by CapabilityMap.refresh()
                # via PlatformConfigsProtocol; nothing to load eagerly here.
                # Register an apply-handler so live PUT /configs updates trigger
                # a re-narrowing of the dispatcher's capability set without
                # process restart. The handler also validates the new config
                # against this process's loaded task types (warns on typos /
                # types only loaded by other services) and emits an INFO
                # summary of what this service will claim post-refresh.
                from dynastore.modules.tasks.runners import capability_map as _capability_map
                from dynastore.modules.tasks.dispatcher import _SERVICE_NAME

                async def _on_routing_change(cfg, _catalog_id, _collection_id, _conn):
                    logger.info("TaskRoutingConfig changed — refreshing CapabilityMap.")
                    # Typo / cross-service-only diagnostic — informational only,
                    # since each service loads only the task types its SCOPE
                    # pulls in. A WARN here is the cheapest way to surface
                    # routing keys that nothing in this deployment can claim.
                    try:
                        from dynastore.tasks import get_loaded_task_types
                        known = set(get_loaded_task_types())
                        unknown = sorted(t for t in (getattr(cfg, "routing", {}) or {}) if t not in known)
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
                    run_capability_publisher,
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

                executor.submit(start_queue_listener(engine, shutdown_event, poll_timeout=poll_interval), task_name="service:queue_listener")
                executor.submit(run_dispatcher(engine, None, shutdown_event), task_name="service:dispatcher")
                # Stuck-PENDING warner — periodic read-only scan for tasks that
                # have been PENDING with retry_count=0 for too long. The most
                # common cause is a routing typo or a service that should claim
                # the task but isn't deployed. See _warn_stuck_pending_tasks.
                executor.submit(
                    _warn_stuck_pending_tasks(engine, schema, shutdown_event),
                    task_name="service:stuck_pending_warner",
                )
                # Async refresh loop. Initial publish already ran
                # synchronously above (before run_dispatcher was submitted)
                # so dispatcher reactive-reaper checks never see an empty
                # cache during cold start.
                executor.submit(
                    run_capability_publisher(
                        shutdown_event,
                        ttl_seconds=cap_ttl,
                        refresh_seconds=cap_refresh,
                    ),
                    task_name="service:capability_publisher",
                )
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


# --- Internal Query Objects ---
# All queries target the global tasks table. The `schema_name` column
# distinguishes tenants; `get_task_schema()` returns the PostgreSQL schema
# that hosts the global table (default: "tasks").


async def _warn_stuck_pending_tasks(
    engine: DbResource,
    schema: str,
    shutdown_event: asyncio.Event,
    interval_s: float = 60.0,
    min_age_s: float = 600.0,
    sample_limit: int = 50,
) -> None:
    """Periodic read-only scan that logs WARNINGs for tasks that have been
    PENDING with ``retry_count = 0`` for more than ``min_age_s`` seconds.

    The most common cause is operator misconfiguration of
    :class:`TaskRoutingConfig` — a typo in a service name or a target that
    no deployed service maps to — which leaves tasks sitting unclaimable
    forever. Today the dispatcher's CapabilityMap silently excludes them;
    this coroutine surfaces the silence.

    Read-only by design: we never mutate the row. The pg_cron-driven
    ``reap_stuck_tasks`` SQL function continues to handle stuck *ACTIVE*
    tasks (lock expired); this coroutine handles stuck *PENDING* tasks
    (never claimed). Two orthogonal failure modes, two independent signals.

    Idempotent and crash-safe: any error is logged and swallowed; the loop
    sleeps and retries. Stops cleanly when ``shutdown_event`` is set.
    """
    sql = (
        f'SELECT task_id, task_type, schema_name, inputs, '  # nosec - schema is validated upstream
        f'  EXTRACT(EPOCH FROM NOW() - timestamp) AS age_s '
        f'FROM "{schema}".tasks '
        f"WHERE status = 'PENDING' "
        f"  AND retry_count = 0 "
        f"  AND timestamp < NOW() - make_interval(secs => :min_age_s) "
        f"ORDER BY timestamp ASC LIMIT :sample_limit;"
    )
    query = DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS)

    while not shutdown_event.is_set():
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=interval_s)
            break  # shutdown signalled during sleep
        except asyncio.TimeoutError:
            pass  # normal — periodic wakeup

        try:
            async with managed_transaction(engine) as conn:
                rows = await query.execute(
                    conn, min_age_s=min_age_s, sample_limit=sample_limit,
                )
            await _emit_stuck_pending_logs(rows or [])
        except Exception as exc:  # noqa: BLE001 — never crash on diagnostic
            logger.warning("stuck-pending warner: scan failed: %s", exc)


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
        f"check TaskRoutingConfig.routing[{task_type!r}] for typos or for "
        f"a service that should claim it but isn't deployed."
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
    Ensures that the global tasks table exists in the specified schema.
    Called at every startup. All steps are idempotent and must run every time:
    table/index DDL use IF NOT EXISTS, partition + retention/cron helpers all
    check-then-create. The table-existence check is NOT used to short-circuit
    the rest of this function — otherwise a restart after a month rollover
    would never create the current-month partition and the dispatcher would
    hit "relation does not exist" on claim_batch.

    Note: events table is now owned by EventsModule (priority=11).
    """
    from dynastore.modules.db_config import maintenance_tools

    # Ensure schema exists first
    await ensure_schema_exists(conn, schema)

    # Single module-level batch: on warm starts the sentinel
    # (status-update trigger) short-circuits everything in one round-trip.
    # Cold starts create the table, indexes, notify functions, and both
    # triggers in order under nested savepoints. Advisory locks are
    # auto-derived from each statement's hash.
    await _build_tasks_ddl_batch(schema).execute(conn, schema=schema)

    # Step 3: Ensure current + future partitions exist.
    # Critical path — must succeed for the dispatcher to start.
    await maintenance_tools.ensure_future_partitions(
        conn,
        schema=schema,
        table="tasks",
        interval="monthly",
        periods_ahead=12,
        column="timestamp",
    )

    # Steps 4+: pg_cron registration — operational, not structural.
    # If pg_cron is not installed or misconfigured these steps warn rather than
    # rolling back the table and partition creation above.
    try:
        await maintenance_tools.register_retention_policy(
            conn,
            schema=schema,
            table="tasks",
            policy="prune",
            interval="daily",
            retention_period="1 month",
            column="timestamp",
        )
    except Exception as e:
        logger.warning(
            f"TasksModule: register_retention_policy failed for {schema}.tasks "
            f"(pg_cron unavailable?): {e}"
        )

    try:
        await maintenance_tools.register_partition_creation_policy(
            conn,
            schema=schema,
            table="tasks",
            interval="monthly",
            periods_ahead=3,
        )
    except Exception as e:
        logger.warning(
            f"TasksModule: register_partition_creation_policy failed for {schema}.tasks "
            f"(pg_cron unavailable?): {e}"
        )

    # Reaper function — DB-side replacement for the in-process janitor.
    # Idempotent (CREATE OR REPLACE).
    await DDLQuery(GLOBAL_TASKS_REAPER_DDL).execute(conn, schema=schema)

    # Schedule the reaper via pg_cron.  Every minute — short enough to
    # recover a pod failure within SLA, long enough that live heartbeats
    # (default 30s interval extending locked_until to +5min) always win
    # the race against the reap scan.
    try:
        # Pass the live hard cap (loaded from TasksPluginConfig in the
        # caller's lifespan) as the second arg. Falls back to the function's
        # DEFAULT (5) if the config hasn't been touched.
        hard_cap = get_hard_retry_cap()
        reaper_command = (
            f'SELECT "{schema}".reap_stuck_tasks(3, {int(hard_cap)});'
        )
        await maintenance_tools.register_cron_job(
            conn,
            job_name=f"dynastore-task-reaper-{schema}",
            schedule="* * * * *",
            command=reaper_command,
        )
        logger.info(
            f"TasksModule: registered pg_cron reaper 'dynastore-task-reaper-{schema}' "
            f"(every minute → {schema}.reap_stuck_tasks(3, {hard_cap}))."
        )
    except Exception as e:
        logger.warning(
            f"TasksModule: register_cron_job failed for {schema}.reap_stuck_tasks "
            f"(pg_cron unavailable?): {e} — stuck tasks will accumulate until a "
            f"manual SELECT {schema}.reap_stuck_tasks() or next successful boot."
        )



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
    conn: DbResource, catalog_id: str, limit: int = 20, offset: int = 0
) -> List[Task]:
    """
    Lists tasks from a catalog's schema.
    Uses CatalogsProtocol to resolve the physical schema.
    """
    schema = await _resolve_catalog_schema(catalog_id, conn)
    return await list_tasks(conn, schema, limit, offset)


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
        inputs = dict(task_data.inputs) if task_data.inputs else {}
        cid = get_correlation_id()
        if cid is not None:
            inputs[_INTERNAL_KEY] = cid

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
        insert_kwargs: Dict[str, Any] = dict(
            task_id=task_id,
            schema_name=schema,
            scope=task_data.scope,
            caller_id=task_data.caller_id,
            task_type=task_data.task_type,
            type=task_data.type,
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

    updated_task_dict = await DQLQuery(
        sql, result_handler=ResultHandler.ONE_DICT
    ).execute(conn, **query_params)

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
    conn: DbResource, schema: str, limit: int = 20, offset: int = 0
) -> List[Task]:
    """Lists tasks filtered by schema_name, ordered by creation date."""
    task_schema = get_task_schema()
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

        task_dict = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn,
            task_id=task_id,
            schema_name=schema_name,
            scope=scope,
            caller_id=task_data.caller_id,
            task_type=task_data.task_type,
            type=task_data.type,
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
) -> None:
    """Mark a claimed task as COMPLETED."""
    task_schema = get_task_schema()
    serialized_outputs = None
    if outputs is not None:
        from dynastore.tools.json import CustomJSONEncoder
        serialized_outputs = json.dumps(outputs, cls=CustomJSONEncoder)

    sql = f"""
        UPDATE {task_schema}.tasks
        SET status = 'COMPLETED',
            finished_at = :finished_at,
            outputs = :outputs,
            locked_until = NULL,
            owner_id = NULL
        WHERE task_id = :task_id;
    """
    async with managed_transaction(engine) as conn:
        await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
            conn, task_id=task_id, finished_at=timestamp, outputs=serialized_outputs
        )


async def fail_task(
    engine: DbResource,
    task_id: uuid.UUID,
    timestamp: Any,
    error_message: str,
    retry: bool = True,
) -> None:
    """
    Mark a claimed task as failed. If retry=True and retries remain,
    requeue with exponential backoff. Otherwise move to DEAD_LETTER.
    """
    task_schema = get_task_schema()

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
            WHERE task_id = :task_id;
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
            WHERE task_id = :task_id;
        """
        params = {
            "task_id": task_id,
            "error_message": error_message,
            "finished_at": timestamp,
        }

    async with managed_transaction(engine) as conn:
        await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
            conn, **params,
        )


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
