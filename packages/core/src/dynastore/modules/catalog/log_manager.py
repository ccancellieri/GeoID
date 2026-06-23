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

import logging
import os
from contextlib import asynccontextmanager
import json
from dynastore.tools.json import CustomJSONEncoder
from typing import Optional, Dict, Any, List, AsyncGenerator
from datetime import datetime, timezone
from pydantic import BaseModel, ConfigDict

from dynastore.tools.plugin import ProtocolPlugin
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
    DbResource,
)
from dynastore.models.shared_models import (
    SYSTEM_CATALOG_ID,
    SYSTEM_LOGS_TABLE,
    SYSTEM_SCHEMA,
)
from dynastore.tools.protocol_helpers import get_engine
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
from dynastore.modules.db_config.maintenance_tools import ensure_schema_exists
from dynastore.modules.db_config.locking_tools import (
    check_table_exists,
    safe_drop_relation,
)
from dynastore.models.protocols import LogsProtocol, CatalogsProtocol
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

#
#  Tables:
#   - logs:              partitioned by collection_physical_id (LIST).
#   - logs_default:      DEFAULT partition (FOR VALUES IN ('')) for catalog-tier
#                        logs (no collection) — empty-string semantics preserved.
#
#  Partition key
#  ~~~~~~~~~~~~~
#  The ``logs`` table is partitioned by ``collection_physical_id`` — the
#  immutable physical id (``c_…`` token) stored in the collection registry.
#  This is distinct from ``collection_id``, the mutable logical identifier
#  exposed in the API, which is kept as a plain data column.
#
#  A collection rename updates only the logical ``collection_id``; it touches
#  zero rows in ``logs`` because no key or partition references the logical id.
#
#  Catalog-tier log rows (no collection) use ``collection_physical_id = ''``
#  and land in the default partition exactly as before.
#
#  Maintenance (MaintenanceSupervisor JOB_TENANT_LOGS_PRUNE / JOB_SYSTEM_LOGS_PRUNE):
#   - Monthly: Prune logs older than 1 year.
# ==============================================================================

# ----- Tenant logs (flat parent, partitioned by collection_physical_id LIST) -----
TENANT_LOGS_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.logs (
    id                      BIGSERIAL       NOT NULL,
    timestamp               TIMESTAMPTZ     DEFAULT NOW(),
    catalog_id              VARCHAR         NOT NULL,
    collection_id           VARCHAR         NOT NULL DEFAULT '',
    collection_physical_id  VARCHAR         NOT NULL DEFAULT '',
    event_type              VARCHAR,
    level                   VARCHAR(20),
    message                 TEXT,
    details                 JSONB,
    stacktrace              TEXT,
    request_context         JSONB,
    PRIMARY KEY (collection_physical_id, id)
) PARTITION BY LIST (collection_physical_id);
"""

TENANT_LOGS_DEFAULT_PARTITION_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.logs_default
    PARTITION OF {schema}.logs FOR VALUES IN ('');
"""

# Logs do not use dead-letter tables; old rows are pruned directly by the
# MaintenanceSupervisor (JOB_TENANT_LOGS_PRUNE / JOB_SYSTEM_LOGS_PRUNE jobs).

# ----- System logs (flat, no partition) -----
SYSTEM_LOGS_DDL = f"""
CREATE TABLE IF NOT EXISTS {SYSTEM_SCHEMA}.{SYSTEM_LOGS_TABLE} (
    id              BIGSERIAL       PRIMARY KEY,
    catalog_id      VARCHAR,
    collection_id   VARCHAR,
    event_type      VARCHAR         NOT NULL,
    level           VARCHAR         NOT NULL,
    message         TEXT,
    details         JSONB,
    stacktrace      TEXT,
    request_context JSONB,
    timestamp       TIMESTAMPTZ     DEFAULT NOW()
);
"""

# System logs: flat table (no dead letter) — old rows are pruned directly.
SYSTEM_LOGS_DL_DDL = None



@lifecycle_registry.sync_catalog_initializer(priority=50)
async def _initialize_logs_tenant_slice(conn: DbResource, schema: str, catalog_id: str):
    """Initializes per-tenant log tables and cron jobs (no time partitions)."""

    async def _check_all_logs_tables_exist(active_conn=None, params=None):
        target = active_conn or conn
        exists_logs = await check_table_exists(target, "logs", schema)
        exists_default = await check_table_exists(target, "logs_default", schema)
        return exists_logs and exists_default

    combined_ddl = TENANT_LOGS_DDL + TENANT_LOGS_DEFAULT_PARTITION_DDL

    await DDLQuery(
        combined_ddl,
        check_query=_check_all_logs_tables_exist,
    ).execute(conn, schema=schema)

    # Retention is handled by the maintenance supervisor (job: tenant_logs_prune).


async def initialize_system_logs(conn: DbResource):
    """Initializes the system-level logs table (flat, no partitions)."""
    # Ensure system schema exists (idempotent)
    await ensure_schema_exists(conn, SYSTEM_SCHEMA)

    await DDLQuery(SYSTEM_LOGS_DDL).execute(conn)
    # System logs retention is handled by the maintenance supervisor (job: system_logs_prune).


# ==============================================================================
#  COLLECTION-LEVEL LOG PARTITION LIFECYCLE
#
#  When a collection is created  → attach a dedicated LIST partition for it in logs.
#  When a collection is hard-deleted → archive and drop its log partition.
#  Empty-string DEFAULT partition handles catalog-scoped logs (collection_id='').
# ==============================================================================

from dynastore.modules.catalog.lifecycle_manager import (
    sync_collection_initializer,
    sync_collection_hard_destroyer,
)
from dynastore.modules.db_config.query_executor import DDLQuery
from dynastore.models.driver_context import DriverContext


@sync_collection_initializer()
async def _create_logs_partition(
    conn: DbResource, schema: str, catalog_id: str, collection_id: str, **kwargs
) -> None:
    """Creates a per-collection LIST partition in logs keyed on collection_physical_id."""
    catalogs = get_protocol(CatalogsProtocol)
    collection_physical_id: Optional[str] = None
    if catalogs:
        collection_physical_id = await catalogs.resolve_physical_id(
            catalog_id,
            collection_id,
            ctx=DriverContext(db_resource=conn),
            allow_missing=True,
        )
    if not collection_physical_id:
        logger.warning(
            "Could not resolve physical id for collection '%s' in catalog '%s'; "
            "skipping logs partition creation.",
            collection_id,
            catalog_id,
        )
        return

    # Partition table name is based on the physical id (stable across renames).
    safe_suffix = collection_physical_id.replace("-", "_").replace(".", "_")
    partition_table = f"logs_p_{safe_suffix}"

    async def partition_exists(active_conn=None, params=None):
        return await check_table_exists(active_conn or conn, partition_table, schema)

    create_ddl = (
        f'CREATE TABLE IF NOT EXISTS "{schema}"."{partition_table}" '
        f'PARTITION OF "{schema}".logs '
        f"FOR VALUES IN ('{collection_physical_id}');"
    )

    await DDLQuery(
        create_ddl,
        check_query=partition_exists,
    ).execute(conn)
    logger.info(
        "Created logs partition '%s.%s' for physical id '%s'.",
        schema,
        partition_table,
        collection_physical_id,
    )


@sync_collection_hard_destroyer()
async def _drop_logs_partition(
    conn: DbResource, schema: str, catalog_id: str, collection_id: str
) -> None:
    """Drops the per-collection log partition on hard delete (logs are ephemeral)."""
    catalogs = get_protocol(CatalogsProtocol)
    collection_physical_id: Optional[str] = None
    if catalogs:
        collection_physical_id = await catalogs.resolve_physical_id(
            catalog_id,
            collection_id,
            ctx=DriverContext(db_resource=conn),
            allow_missing=True,
        )
    if not collection_physical_id:
        logger.debug(
            "No physical id found for collection '%s'; no logs partition to drop.",
            collection_id,
        )
        return

    safe_suffix = collection_physical_id.replace("-", "_").replace(".", "_")
    partition_table = f"logs_p_{safe_suffix}"

    exists = await check_table_exists(conn, partition_table, schema)
    if not exists:
        logger.debug(
            "No logs partition '%s' to drop for collection '%s'.",
            partition_table,
            collection_id,
        )
        return

    # Bound AccessExclusiveLock wait — concurrent log appenders on other pods
    # may still be writing to this partition when hard-delete races them.
    await safe_drop_relation(conn, schema, partition_table, kind="table")
    logger.info(
        "Dropped logs partition '%s.%s' for collection '%s'.",
        schema,
        partition_table,
        collection_id,
    )


# --- Log Entry Model (Local Definition) ---


class LogEntryCreate(BaseModel):
    """Pydantic model for creating log entries."""

    catalog_id: str
    collection_id: Optional[str] = None
    event_type: str
    level: str = "INFO"
    message: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    is_system: bool = False

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


# --- Log Buffer ---
# Gone. Replaced by AsyncBufferAggregator from async_utils.



# --- Log Service (similar to StatsService) ---


class LogService(ProtocolPlugin[Any], LogsProtocol):
    """Singleton service for buffered, high-throughput log ingestion."""

    # Protocol attributes — lower means higher precedence in get_protocols()
    priority: int = 10

    def __init__(self):
        self._engine: Optional[DbResource] = None
        self._aggregator: Optional[Any] = None
        self._aggregator_started: bool = False

    @asynccontextmanager
    async def lifespan(self, app_state: Any) -> AsyncGenerator[None, None]:
        """Lifecycle hook for LogService."""
        from dynastore.tools.protocol_helpers import get_engine
        
        self._engine = get_engine()
        if not self._engine:
            logger.warning(
                "LogService: No database engine available. Logging will fall back to stdlib."
            )
            yield
            return

        from dynastore.tools.async_utils import AsyncBufferAggregator
        flush_threshold = int(os.environ.get("LOG_FLUSH_THRESHOLD", 50))
        flush_interval = float(os.environ.get("LOG_FLUSH_INTERVAL", 5.0))

        self._aggregator = AsyncBufferAggregator(
            flush_callback=self._flush_batch,
            threshold=flush_threshold,
            interval=flush_interval,
            name="LogAggregator",
        )
        logger.info(
            "LogService initialized with flush_threshold=%s, flush_interval=%ss",
            flush_threshold,
            flush_interval,
        )
        
        try:
            yield
        finally:
            await self.stop()

    async def start(self, db_resource: Optional[DbResource] = None) -> None:
        """Deprecated: use lifespan instead. Legacy support for manual startup."""
        self._engine = db_resource or get_engine()
        if not self._engine:
            logger.warning(
                "LogService: No database engine available. Logging will fall back to stdlib."
            )
            return

        from dynastore.tools.async_utils import AsyncBufferAggregator
        flush_threshold = int(os.environ.get("LOG_FLUSH_THRESHOLD", 50))
        flush_interval = float(os.environ.get("LOG_FLUSH_INTERVAL", 5.0))

        self._aggregator = AsyncBufferAggregator(
            flush_callback=self._flush_batch,
            threshold=flush_threshold,
            interval=flush_interval,
            name="LogAggregator",
        )
        logger.info(
            "LogService initialized with flush_threshold=%s, flush_interval=%ss",
            flush_threshold,
            flush_interval,
        )

    async def stop(self) -> None:
        """Flushes remaining entries and tears down the aggregator."""
        if self._aggregator:
            logger.info("LogService shutting down...")
            await self._aggregator.stop()
            self._aggregator = None
            self._engine = None
            logger.info("LogService shutdown complete.")

    async def _flush_batch(self, entries: List[LogEntryCreate]):
        """Callback for AsyncBufferAggregator. Writes to PG then dispatches to backends."""
        if not self._engine:
            return

        try:
            async with managed_transaction(self._engine) as conn:
                from dynastore.modules.db_config.query_executor import DbAsyncConnection

                for entry in entries:
                    try:
                        # Fallback: Use a savepoint if in a transaction to avoid poisoning
                        # the main transaction if the logs table doesn't exist yet.
                        if isinstance(conn, DbAsyncConnection) and hasattr(
                            conn, "begin_nested"
                        ):
                            async with conn.begin_nested():
                                await self._write_log_entry(conn, entry)
                        else:
                            await self._write_log_entry(conn, entry)
                    except Exception as e:
                        logger.warning(
                            f"LogService: Failed to write individual log entry: {e}"
                        )
                        pass
            logger.debug(f"Flushed {len(entries)} log entries to database.")
        except Exception as e:
            logger.error(f"Failed to flush log entries: {e}", exc_info=True)

        # Dispatch to registered log backends (ES, GCP Cloud Logging, etc.)
        try:
            from dynastore.models.protocols.logs import LogBackendProtocol

            backends = get_protocol(LogBackendProtocol)
            if backends:
                # get_protocol may return a single instance or a list; normalize to list
                backend_list = [backends] if not isinstance(backends, list) else backends
                for backend in backend_list:
                    try:
                        result = await backend.write_batch(entries)
                        logger.debug(
                            "Log backend '%s' result: %s", backend.name, result
                        )
                    except Exception as exc:
                        logger.warning(
                            "Log backend '%s' failed: %s", backend.name, exc
                        )
        except Exception as exc:
            logger.warning("Failed to dispatch logs to backends: %s", exc)

    async def flush(self):
        """Manually trigger a flush (legacy support)."""
        if self._aggregator:
            await self._aggregator._trigger_flush(wait=True)

    async def _write_log_entry(
        self,
        conn: DbResource,
        entry: LogEntryCreate,
    ) -> Optional[int]:
        """Writes a single log entry, ensuring partition exists. Returns log ID.

        The catalog→schema and collection→physical-id lookups resolve through
        ``resolve_physical_schema`` / ``resolve_physical_id`` *without* a
        ``db_resource``, so they serve from the shared L1/L2 caches
        (``_physical_schema_cache`` / ``_collection_physical_id_cache``) and a
        batch of N entries for one catalog pays the DB round-trip at most once
        per TTL window rather than once per entry.  The flush runs on its own
        connection after the originating write has committed, so the cached
        (committed) view is the correct one.
        """
        # Determine target schema and table.
        catalogs = get_protocol(CatalogsProtocol)
        if entry.is_system or entry.catalog_id == SYSTEM_CATALOG_ID or not catalogs:
            phys_schema = "catalog"
            table_name = SYSTEM_LOGS_TABLE
        else:
            try:
                phys_schema = await catalogs.resolve_physical_schema(entry.catalog_id)
                table_name = "logs"
            except ValueError:
                # Catalog might have been deleted or doesn't exist.
                phys_schema = None

        if not phys_schema:
            logger.warning(
                "LogService: Physical schema not found for catalog '%s'. "
                "Falling back to system_logs.",
                entry.catalog_id,
            )
            phys_schema = "catalog"
            table_name = SYSTEM_LOGS_TABLE

        # Resolve the collection's immutable physical id once for the whole entry.
        # Catalog-tier rows (collection_id is None/empty) use empty-string, which
        # lands in the logs_default partition (FOR VALUES IN ('')).
        collection_physical_id: str = ""
        if table_name == "logs" and entry.collection_id and catalogs:
            resolved = await catalogs.resolve_physical_id(
                entry.catalog_id,
                entry.collection_id,
                allow_missing=True,
            )
            collection_physical_id = resolved or ""

        # Prepare details with stacktrace and request_context if provided.
        details_dict = entry.details or {}
        stacktrace = (
            details_dict.pop("stacktrace", None)
            if isinstance(details_dict, dict)
            else None
        )
        request_context = (
            details_dict.pop("request_context", None)
            if isinstance(details_dict, dict)
            else None
        )

        catalog_id_val = entry.catalog_id

        # The tenant logs table carries collection_physical_id (partition key);
        # system_logs is flat and does not have that column.
        is_tenant_logs = table_name == "logs"

        from dynastore.modules.db_config.query_executor import managed_transaction
        async with managed_transaction(conn) as tx_conn:
            try:
                if is_tenant_logs:
                    # Tenant logs: include collection_physical_id so the row
                    # lands in the correct partition and can be pruned efficiently.
                    # collection_id is kept as a plain data column only.
                    log_id = await DQLQuery(
                        """
                        INSERT INTO {schema}.{table} (timestamp, catalog_id, collection_id, collection_physical_id, event_type, level, message, details, stacktrace, request_context)
                        VALUES (:timestamp, :catalog_id, :collection_id, :collection_physical_id, :event_type, :level, :message, :details, :stacktrace, :request_context)
                        RETURNING id;
                        """,
                        result_handler=ResultHandler.SCALAR_ONE,
                    ).execute(
                        tx_conn,
                        schema=phys_schema,
                        table=table_name,
                        timestamp=datetime.now(timezone.utc),
                        catalog_id=catalog_id_val,
                        collection_id=entry.collection_id or "",
                        collection_physical_id=collection_physical_id,
                        event_type=entry.event_type,
                        level=entry.level,
                        message=entry.message,
                        details=json.dumps(details_dict, cls=CustomJSONEncoder)
                        if details_dict
                        else None,
                        stacktrace=stacktrace,
                        request_context=json.dumps(request_context, cls=CustomJSONEncoder)
                        if request_context
                        else None,
                    )
                else:
                    # System logs: flat table, no partition key column.
                    log_id = await DQLQuery(
                        """
                        INSERT INTO {schema}.{table} (timestamp, catalog_id, collection_id, event_type, level, message, details, stacktrace, request_context)
                        VALUES (:timestamp, :catalog_id, :collection_id, :event_type, :level, :message, :details, :stacktrace, :request_context)
                        RETURNING id;
                        """,
                        result_handler=ResultHandler.SCALAR_ONE,
                    ).execute(
                        tx_conn,
                        schema=phys_schema,
                        table=table_name,
                        timestamp=datetime.now(timezone.utc),
                        catalog_id=catalog_id_val,
                        collection_id=entry.collection_id or "",
                        event_type=entry.event_type,
                        level=entry.level,
                        message=entry.message,
                        details=json.dumps(details_dict, cls=CustomJSONEncoder)
                        if details_dict
                        else None,
                        stacktrace=stacktrace,
                        request_context=json.dumps(request_context, cls=CustomJSONEncoder)
                        if request_context
                        else None,
                    )
                return log_id
            except Exception as e:
                if is_tenant_logs and entry.collection_id and "no partition" in str(e).lower():
                    # Fallback: partition missing (e.g. race at collection create).
                    # Land in the default partition (collection_physical_id='').
                    logger.warning(
                        "Log partition missing for collection '%s' (physical '%s'). "
                        "Falling back to catalog-scoped log.",
                        entry.collection_id,
                        collection_physical_id,
                    )
                    if not details_dict:
                        details_dict = {}
                    details_dict["original_collection_id"] = entry.collection_id

                    log_id = await DQLQuery(
                        """
                        INSERT INTO {schema}.{table} (timestamp, catalog_id, collection_id, collection_physical_id, event_type, level, message, details, stacktrace, request_context)
                        VALUES (:timestamp, :catalog_id, :collection_id, :collection_physical_id, :event_type, :level, :message, :details, :stacktrace, :request_context)
                        RETURNING id;
                        """,
                        result_handler=ResultHandler.SCALAR_ONE,
                    ).execute(
                        tx_conn,
                        schema=phys_schema,
                        table=table_name,
                        timestamp=datetime.now(timezone.utc),
                        catalog_id=catalog_id_val,
                        collection_id="",
                        collection_physical_id="",
                        event_type=entry.event_type,
                        level=entry.level,
                        message=entry.message,
                        details=json.dumps(details_dict, cls=CustomJSONEncoder)
                        if details_dict
                        else None,
                        stacktrace=stacktrace,
                        request_context=json.dumps(request_context, cls=CustomJSONEncoder)
                        if request_context
                        else None,
                    )
                    return log_id
                raise

    async def log_event(
        self,
        catalog_id: str,
        event_type: str,
        level: str = "INFO",
        message: Optional[str] = None,
        collection_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        db_resource: Optional[DbResource] = None,
        immediate: bool = False,
        is_system: bool = False,
    ) -> Optional[int]:
        """
        Main entry point for logging events.

        Args:
            catalog_id: The catalog this event relates to (required).
            event_type: Type of event (e.g., "gcp_bucket_created", "catalog_creation").
            level: Log level (INFO, WARNING, ERROR).
            message: Human-readable message.
            collection_id: Optional collection ID if event is collection-scoped.
            details: Optional structured details dictionary. Can include 'stacktrace' and 'request_context'.
            db_resource: Optional database connection. If provided, writes immediately (bypasses buffer).
            immediate: If True and not under load, flush immediately. Otherwise, buffer for batch write.

        Returns:
            Log ID if db_resource is provided and write succeeds, None otherwise.
        """
        if not self._engine and not db_resource:
            # Fallback to standard logging if no DB available
            safe_msg = f"[LogService] {level} | {catalog_id} | {event_type}: {message}"
            if level.upper() == "ERROR":
                logger.error(safe_msg)
            elif level.upper() == "WARNING":
                logger.warning(safe_msg)
            else:
                logger.info(safe_msg)
            return None

        from dynastore.tools.correlation import get_correlation_id
        cid = get_correlation_id()
        if cid is not None:
            details = dict(details) if details else {}
            details.setdefault("request_context", {})
            if isinstance(details["request_context"], dict):
                details["request_context"].setdefault("correlation_id", cid)

        entry = LogEntryCreate(
            catalog_id=catalog_id,
            collection_id=collection_id,
            event_type=event_type,
            level=level,
            message=message,
            details=details,
            is_system=is_system,
        )

        # If db_resource is provided, write immediately (transactional guarantee) and return ID
        if db_resource or immediate:
            conn = db_resource or self._engine
            if conn is None:
                return None
            return await self._write_log_entry(conn, entry)

        aggregator = self._aggregator
        if aggregator is None:
            return None

        if not self._aggregator_started:
            from dynastore.modules.concurrency import default_executor
            default_executor.submit(aggregator.start(), "log_aggregator_start")
            self._aggregator_started = True

        await aggregator.add(entry)
        return None

    async def log_info(
        self, catalog_id: str, event_type: str, message: str, **kwargs
    ) -> None:
        """Convenience wrapper for INFO level logs."""
        is_system = kwargs.pop("is_system", False)
        await self.log_event(
            catalog_id,
            event_type,
            level="INFO",
            message=message,
            is_system=is_system,
            **kwargs,
        )

    async def log_warning(
        self, catalog_id: str, event_type: str, message: str, **kwargs
    ) -> None:
        """Convenience wrapper for WARNING level logs."""
        is_system = kwargs.pop("is_system", False)
        await self.log_event(
            catalog_id,
            event_type,
            level="WARNING",
            message=message,
            is_system=is_system,
            **kwargs,
        )

    async def log_error(
        self, catalog_id: str, event_type: str, message: str, **kwargs
    ) -> None:
        """Convenience wrapper for ERROR level logs."""
        is_system = kwargs.pop("is_system", False)
        await self.log_event(
            catalog_id,
            event_type,
            level="ERROR",
            message=message,
            is_system=is_system,
            **kwargs,
        )

    async def shutdown(self):
        """Deprecated: use stop() or lifespan."""
        await self.stop()

    async def get_log_by_id(
        self, log_id: int, catalog_id: str, db_resource: Optional[DbResource] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific log entry by ID.

        Args:
            log_id: The log entry ID
            catalog_id: The catalog code (or "_system_")
            db_resource: Optional database connection

        Returns:
            Log entry as dict, or None if not found
        """
        # Determine schema and table
        catalogs = get_protocol(CatalogsProtocol)
        if catalog_id == SYSTEM_CATALOG_ID or catalog_id == "_system_" or not catalogs:
            phys_schema = "catalog"
            table_name = SYSTEM_LOGS_TABLE
        else:
            if db_resource:
                phys_schema = await catalogs.resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=db_resource)
                )
            else:
                async with managed_transaction(self._engine) as conn:
                    phys_schema = await catalogs.resolve_physical_schema(
                        catalog_id, ctx=DriverContext(db_resource=conn)
                    )
            table_name = "logs"

        if not phys_schema:
            return None

        # Query log entry
        async def _query(conn):
            return await DQLQuery(
                """
                SELECT id, timestamp, catalog_id, collection_id, event_type, level, message, details, stacktrace, request_context
                FROM {schema}.{table}
                WHERE id = :log_id
                LIMIT 1;
                """,
                result_handler=ResultHandler.ONE_DICT,
            ).execute(conn, schema=phys_schema, table=table_name, log_id=log_id)

        if db_resource:
            return await _query(db_resource)
        else:
            async with managed_transaction(self._engine) as conn:
                return await _query(conn)

    async def list_logs(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        level: Optional[str] = None,
        event_type: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
        db_resource: Optional[DbResource] = None,
    ) -> List[Dict[str, Any]]:
        """
        List log entries with filtering and pagination.

        Args:
            catalog_id: The catalog code (or "_system_")
            collection_id: Optional collection filter
            level: Optional level filter (ERROR, WARNING, INFO)
            event_type: Optional event type filter
            limit: Maximum number of results (default 50, max 1000)
            offset: Pagination offset
            db_resource: Optional database connection

        Returns:
            List of log entries as dicts
        """
        # Determine schema and table
        catalogs = get_protocol(CatalogsProtocol)
        if catalog_id == "_system_" or not catalogs:
            phys_schema = "catalog"
            table_name = SYSTEM_LOGS_TABLE
        else:
            if db_resource:
                phys_schema = await catalogs.resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=db_resource)
                )
            else:
                async with managed_transaction(self._engine) as conn:
                    phys_schema = await catalogs.resolve_physical_schema(
                        catalog_id, ctx=DriverContext(db_resource=conn)
                    )
            table_name = "logs"

        if not phys_schema:
            return []

        # Build WHERE clause.
        # For tenant logs ({schema}.logs) the schema already scopes to one catalog,
        # so catalog_id is a mutable label that becomes stale after a rename.
        # For the flat system_logs table the catalog_id column is the only scope key.
        if table_name == "logs":
            where_clauses: list = []
            params = {"catalog_id": catalog_id, "limit": limit, "offset": offset}
        else:
            where_clauses = ["catalog_id = :catalog_id"]
            params = {"catalog_id": catalog_id, "limit": limit, "offset": offset}

        if collection_id and table_name == "logs":
            # Resolve the immutable physical id for partition pruning.
            # When resolution fails (collection not found), omit the collection
            # filter rather than matching the stale logical id.
            coll_phys_id: Optional[str] = None
            if catalogs:
                _ctx_conn = db_resource
                if _ctx_conn is None:
                    # conn is not available here yet; use engine for resolution.
                    _ctx_conn = self._engine
                if _ctx_conn:
                    coll_phys_id = await catalogs.resolve_physical_id(
                        catalog_id,
                        collection_id,
                        ctx=DriverContext(db_resource=_ctx_conn),
                        allow_missing=True,
                    )
            if coll_phys_id:
                where_clauses.append(
                    "collection_physical_id = :collection_physical_id"
                )
                params["collection_physical_id"] = coll_phys_id
            # else: physical id unresolved — return catalog-scoped rows without
            # a collection filter rather than matching the stale logical id.
        elif collection_id:
            # system_logs branch: flat table, filter by logical collection_id.
            where_clauses.append("collection_id = :collection_id")
            params["collection_id"] = collection_id

        if level:
            where_clauses.append("level = :level")
            params["level"] = level.upper()

        if event_type:
            where_clauses.append("event_type = :event_type")
            params["event_type"] = event_type

        where_clause = " AND ".join(where_clauses) if where_clauses else "TRUE"

        # Query logs
        async def _query(conn):
            return await DQLQuery(
                f"""
                SELECT id, timestamp, catalog_id, collection_id, event_type, level, message, details, stacktrace, request_context
                FROM {{schema}}.{{table}}
                WHERE {where_clause}
                ORDER BY timestamp DESC
                LIMIT :limit OFFSET :offset;
                """,
                result_handler=ResultHandler.ALL_DICTS,
            ).execute(conn, schema=phys_schema, table=table_name, **params)

        if db_resource:
            return await _query(db_resource)
        else:
            async with managed_transaction(self._engine) as conn:
                return await _query(conn)


# Global instance
LOG_SERVICE = LogService()

# --- Convenience Functions (matches extensions/logs/log_manager.py API) ---


async def log_event(
    catalog_id: str,
    event_type: str,
    level: str = "INFO",
    message: Optional[str] = None,
    collection_id: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
    db_resource: Optional[DbResource] = None,
    immediate: bool = False,
    is_system: bool = False,
) -> Optional[int]:
    """
    Main entry point for logging events to the Catalog Log Service.
    Fails safely (logs to stdout) if service is not initialized.

    Returns:
        Log ID if db_resource is provided and write succeeds, None otherwise.
    """
    service = get_protocol(LogsProtocol)
    if not service:
        # Fallback to standard logging if no service available
        logger.warning(f"[LogService Fallback] {level} | {catalog_id}: {message}")
        return None

    return await service.log_event(
        catalog_id=catalog_id,
        event_type=event_type,
        level=level,
        message=message,
        collection_id=collection_id,
        details=details,
        db_resource=db_resource,
        immediate=immediate,
        is_system=is_system,
    )


async def log_info(catalog_id: str, event_type: str, message: str, **kwargs):
    """Convenience wrapper for INFO level logs."""
    is_system = kwargs.pop("is_system", False)
    await log_event(
        catalog_id,
        event_type,
        level="INFO",
        message=message,
        is_system=is_system,
        **kwargs,
    )


async def log_warning(catalog_id: str, event_type: str, message: str, **kwargs):
    """Convenience wrapper for WARNING level logs."""
    is_system = kwargs.pop("is_system", False)
    await log_event(
        catalog_id,
        event_type,
        level="WARNING",
        message=message,
        is_system=is_system,
        **kwargs,
    )


async def log_error(catalog_id: str, event_type: str, message: str, **kwargs):
    """Convenience wrapper for ERROR level logs."""
    is_system = kwargs.pop("is_system", False)
    await log_event(
        catalog_id,
        event_type,
        level="ERROR",
        message=message,
        is_system=is_system,
        **kwargs,
    )
