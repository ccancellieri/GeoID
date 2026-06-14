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

# dynastore/tasks/workclass_drain/storage_drain_task.py

"""``StorageDrainTask`` — control-plane-native drain for ``tasks.storage``.

Drains the GLOBAL ``tasks.storage`` table for ALL tenants (tenancy is the
``schema_name`` column, not the physical table). This is the storage-plane
counterpart of ``OutboxDrainTask``; the two tasks coexist during the WorkClass
migration window because they use distinct ``task_type`` values
(``"storage_drain"`` vs ``"index_drain"``).

Claim_version fencing (#1945)
-----------------------------
Every claim bumps ``claim_version = claim_version + 1`` on the row.  Terminal
writes (``mark_done`` / ``mark_retry`` / ``mark_dead``) are guarded by::

    AND claimed_by = :owner_id AND claim_version = :claim_version

If a stalled drain worker that was reclaimed by another pod (bumping
``claim_version`` again) later tries to finalize the row, the CAS predicate
matches 0 rows — the stale write is a no-op and the live owner retains
exclusive control.

Drain loop
----------
``run(payload)`` loops ``drain_once()`` until it returns 0, then exits
(one-shot drain-to-empty shape, matching ``OutboxDrainTask``).  The
dispatcher restarts via LISTEN / periodic catch-up; a single ``run`` call
only needs to clear the current backlog.
"""
from __future__ import annotations

import logging
from typing import Any, ClassVar, Dict, List, Optional, Sequence, cast
from uuid import UUID, uuid4

from dynastore.models.protocols.indexing import (
    BulkIndexer,
    BulkIndexResult,
    IndexableOp,
)
from dynastore.models.tasks import TaskPayload
from dynastore.tasks.protocols import TaskProtocol
from dynastore.tools.db import validate_sql_identifier

logger = logging.getLogger(__name__)


# Per-attempt retry backoff in seconds (mirrors pg_outbox._BACKOFF_SECONDS).
# Index by ``attempts`` (0-based); the last entry caps the backoff at ~30 min.
_BACKOFF_SECONDS: List[int] = [1, 5, 30, 5 * 60, 30 * 60]

# Seconds before an in_flight row is considered stale and eligible for
# reclaim by any drain worker.
_DEFAULT_LEASE_SECONDS: int = 300

# Default claim batch size — mirrors OutboxDrainTask.
_DEFAULT_BATCH_SIZE: int = 1500

# The well-known driver_id for the Elasticsearch items secondary-index driver.
# Resolution is config-scoped via the storage driver registry (populated from
# per-collection routing configs at startup). The ES adapter is just one
# adapter — any driver registered under its config-scoped snake_case id resolves
# automatically through the registry.  This constant is used only as a fallback
# construction key when the registry has not yet been populated (e.g. early
# in startup or in test environments without a running app context).
_ES_ITEMS_DRIVER_ID: str = "items_elasticsearch_driver"


def _backoff(attempts: int) -> int:
    """Return the backoff in seconds for the given zero-based attempt count."""
    idx = min(attempts, len(_BACKOFF_SECONDS) - 1)
    return _BACKOFF_SECONDS[idx]


class StorageDrainTask(TaskProtocol):
    """One-shot drain for the global ``tasks.storage`` index outbox.

    Claims ready rows (and stale in_flight rows whose lease expired), fans
    them out to the appropriate ``BulkIndexer`` by ``driver_id``, and
    applies fenced terminal writes (done / retry / dead).  Drains to empty
    then exits; the dispatcher re-enters via NOTIFY.

    Routing: tier-agnostic (``affinity_tier = None``). Placement comes from
    the task routing config; with no override the default matrix routes a
    tier-less system task to the ``catalog`` tier — the service that
    co-locates the dispatcher and the secondary-write driver this drain pushes
    to (and where the legacy outbox drain already runs). An operator can
    repoint it via routing config without a code change.
    """

    task_type: ClassVar[str] = "storage_drain"
    priority: int = 100
    affinity_tier: ClassVar[Optional[str]] = None

    def __init__(
        self,
        app_state: object | None = None,
        *,
        batch_size: int = _DEFAULT_BATCH_SIZE,
        lease_seconds: int = _DEFAULT_LEASE_SECONDS,
    ) -> None:
        self.app_state = app_state
        self.batch_size = batch_size
        self.lease_seconds = lease_seconds
        # driver_id -> resolved BulkIndexer, memoised for this run.
        self._indexer_cache: Dict[str, BulkIndexer] = {}

    async def run(self, payload: TaskPayload) -> Dict[str, Any]:
        """Drain ``tasks.storage`` to empty, then return.

        Loops ``drain_once()`` until it reports zero claimed rows.  The
        dispatcher re-enters via NOTIFY when new rows appear.
        """
        from dynastore.modules.db_config.db_config import DBConfig
        from sqlalchemy.ext.asyncio import create_async_engine
        from sqlalchemy.pool import NullPool

        db_url = DBConfig.database_url
        if not db_url.startswith("postgresql+asyncpg://"):
            db_url = db_url.replace("postgresql://", "postgresql+asyncpg://", 1)

        # One engine for the lifetime of this run — shared across all
        # claim and terminal-write statements so connection overhead is paid
        # once, not per-row.
        engine = create_async_engine(db_url, poolclass=NullPool)
        # Stable owner_id for the lifetime of this run — used as the
        # ``claimed_by`` stamp and the CAS guard on terminal writes.
        owner_id = f"storage_drain:{uuid4()}"
        total = 0
        try:
            while True:
                n = await self.drain_once(engine=engine, owner_id=owner_id)
                total += n
                if n == 0:
                    break
        finally:
            await engine.dispose()

        return {"drained": total}

    async def drain_once(self, *, engine: Any, owner_id: str) -> int:
        """Claim one batch, process, apply fenced outcomes; return rows handled.

        Whole-batch indexer exception: every row is funnelled to the
        retry path so a flaky indexer can never lose data.  Per-row
        poison classification is the indexer's responsibility.

        If a ``driver_id`` in the claimed batch cannot be resolved to a
        ``BulkIndexer``, those rows are treated as transient-retry so
        they are not dropped; they will be retried once a capable pod
        becomes available or the issue is resolved.
        """
        from dynastore.modules.tasks.tasks_module import get_task_schema

        task_schema = get_task_schema()
        validate_sql_identifier(task_schema)

        rows = await self._claim_batch(
            engine=engine,
            task_schema=task_schema,
            owner_id=owner_id,
        )
        if not rows:
            return 0

        # Group claimed rows by driver_id for bulk dispatch.
        by_driver: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            by_driver.setdefault(row["driver_id"], []).append(row)

        for driver_id, driver_rows in by_driver.items():
            indexer = await self._resolve_indexer(driver_id)
            if indexer is None:
                logger.warning(
                    "StorageDrainTask: no BulkIndexer registered for "
                    "driver_id=%r — %d row(s) queued for retry.",
                    driver_id,
                    len(driver_rows),
                )
                await self._apply_retry_all(
                    engine=engine,
                    task_schema=task_schema,
                    rows=driver_rows,
                    owner_id=owner_id,
                    error=f"indexer not registered: {driver_id}",
                )
                continue

            ops = [self._row_to_op(r) for r in driver_rows]
            try:
                result = await indexer.index_bulk(ops)
            except Exception as exc:  # noqa: BLE001 — surface every failure
                logger.warning(
                    "StorageDrainTask[%s]: whole-batch error: %s",
                    driver_id,
                    exc,
                )
                await self._apply_retry_all(
                    engine=engine,
                    task_schema=task_schema,
                    rows=driver_rows,
                    owner_id=owner_id,
                    error=str(exc),
                )
                continue

            await self._apply_outcomes(
                engine=engine,
                task_schema=task_schema,
                rows=driver_rows,
                result=result,
                owner_id=owner_id,
            )

        return len(rows)

    # ------------------------------------------------------------------
    # Claim
    # ------------------------------------------------------------------

    async def _claim_batch(
        self,
        *,
        engine: Any,
        task_schema: str,
        owner_id: str,
    ) -> List[Dict[str, Any]]:
        """Claim a batch of ready/stale rows; return them as raw dicts.

        ``FOR UPDATE SKIP LOCKED`` lets multiple worker pods claim disjoint
        batches concurrently.  Bumps ``claim_version = claim_version + 1``
        on every (re)claim — this is the fence that prevents a stalled drain
        from finalising a row after it has been reclaimed by another worker.
        """
        from dynastore.modules.db_config.query_executor import (
            DQLQuery,
            ResultHandler,
            managed_transaction,
        )

        claim_sql = (
            f"WITH claimed AS ("
            f"    SELECT day, op_id"
            f"    FROM {task_schema}.storage"
            f"    WHERE (status = 'ready'     AND ready_at <= now())"
            f"       OR (status = 'in_flight' AND claimed_at < now() - make_interval(secs => :lease_seconds))"
            f"    ORDER BY ready_at, op_id"
            f"    LIMIT :batch_size"
            f"    FOR UPDATE SKIP LOCKED"
            f")"
            f" UPDATE {task_schema}.storage w"
            f" SET status = 'in_flight', claimed_at = now(), claimed_by = :owner_id,"
            f"     claim_version = w.claim_version + 1"
            f" FROM claimed"
            f" WHERE w.day = claimed.day AND w.op_id = claimed.op_id"
            f" RETURNING w.day, w.op_id, w.driver_id, w.catalog_id, w.collection_id,"
            f"           w.op, w.entity_id, w.op_payload, w.idempotency_key,"
            f"           w.attempts, w.claim_version, w.claimed_by"
        )

        async with managed_transaction(engine) as conn:
            rows = await DQLQuery(
                claim_sql,
                result_handler=ResultHandler.ALL_DICTS,
            ).execute(
                conn,
                lease_seconds=self.lease_seconds,
                batch_size=self.batch_size,
                owner_id=owner_id,
            )

        return rows or []

    # ------------------------------------------------------------------
    # Indexer resolution
    # ------------------------------------------------------------------

    async def _resolve_indexer(self, driver_id: str) -> Optional[BulkIndexer]:
        """Resolve a :class:`BulkIndexer` for ``driver_id``; cached per run.

        Resolution is config-scoped via the storage driver registry
        (``DriverRegistry.collection_index()``), which is populated from
        per-collection routing configs at startup.  The snake_case ``driver_id``
        stamped on each ``tasks.storage`` row matches the registry key
        (``_to_snake(type(driver).__name__)``), so any driver registered under
        its config-scoped id resolves automatically — the ES adapter is just
        one concrete implementation.

        The drain MUST use the :class:`BulkIndexer` protocol
        (``index_bulk(ops) -> BulkIndexResult``), NOT the distinct
        :class:`Indexer` protocol (``index_bulk(ctx, ops) -> BulkResult``) —
        they are different types with different signatures.

        Resolution order
        ----------------
        1. Per-run cache (``self._indexer_cache``) — avoids repeated registry
           lookups within a single drain cycle.
        2. ``DriverRegistry.collection_index().get(driver_id)`` — the process-
           wide L0 registry built from protocol discovery at startup.  Covers
           any registered ``CollectionItemsStore`` driver, not just ES.
        3. ``DriverRegistry.asset_index().get(driver_id)`` — fallback for
           drivers that live in the asset tier rather than the items tier.
        4. For the known ES items driver_id: if the registry yields nothing
           (e.g. early startup or a test environment without a running app
           context), construct ``ItemsElasticsearchDriver()`` directly and wrap
           it.  This preserves the original availability semantics and keeps
           existing tests green.

        ``driver_id``\\s that cannot be resolved through any of the above paths
        return ``None``; the caller funnels those rows to retry — they are
        never dropped.

        Config-scope gating (grouping rows by ``(catalog_id, collection_id,
        driver_id)`` and skipping rows for driver_ids not present in that
        collection's resolved WRITE drivers) is deferred.  The live-PG dispatch
        tests seed rows in a throwaway schema with no routing config, so a hard
        gate would break them.  See issue #1807 (P1.3) to add the gate once
        test fixtures carry per-collection routing config or the gate is guarded
        on a non-empty ``resolve_drivers`` result.
        """
        cached = self._indexer_cache.get(driver_id)
        if cached is not None:
            return cached

        from dynastore.modules.storage.drivers.elasticsearch import (
            ItemsElasticsearchDriver,
        )
        from dynastore.modules.storage.driver_registry import DriverRegistry
        from dynastore.tasks.outbox_drain.es_indexer_adapter import ESBulkIndexer

        # --- Step 1: registry-driven resolution ---
        driver = (
            DriverRegistry.collection_index().get(driver_id)
            or DriverRegistry.asset_index().get(driver_id)
        )

        if driver is not None:
            if isinstance(driver, ItemsElasticsearchDriver):
                if not driver.is_available():
                    logger.warning(
                        "StorageDrainTask: ES driver unavailable (opensearch-py "
                        "missing from worker extras) — rows for driver_id=%r will "
                        "retry until a capable pod drains them.",
                        driver_id,
                    )
                    return None
                indexer = cast(BulkIndexer, ESBulkIndexer(driver))
                self._indexer_cache[driver_id] = indexer
                return indexer
            # Any other driver found in the registry but without a known
            # BulkIndexer adapter yet: treat as unresolved (retry).  Adding
            # new adapters here as they are developed will extend support
            # without touching the drain loop.
            logger.debug(
                "StorageDrainTask: driver_id=%r resolved from registry but "
                "no BulkIndexer adapter is registered for type %r — rows will retry.",
                driver_id,
                type(driver).__name__,
            )
            return None

        # --- Step 2: construction fallback for the known ES driver_id ---
        # Handles the case where the registry is not yet populated (e.g. the
        # elasticsearch extension is installed but discovery hasn't run, or the
        # worker is in a lightweight context that skips protocol registration).
        if driver_id == _ES_ITEMS_DRIVER_ID:
            # cast(Any, ...) mirrors build_es_drain_task: pyright sees the
            # Protocol-mixin as abstract, but runtime instantiation is valid.
            es_driver = cast(Any, ItemsElasticsearchDriver)()
            if not es_driver.is_available():
                logger.warning(
                    "StorageDrainTask: ES driver unavailable (opensearch-py "
                    "missing from worker extras) — rows for driver_id=%r will "
                    "retry until a capable pod drains them.",
                    driver_id,
                )
                return None
            indexer = cast(BulkIndexer, ESBulkIndexer(es_driver))
            self._indexer_cache[driver_id] = indexer
            return indexer

        return None

    # ------------------------------------------------------------------
    # Row-to-op conversion
    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_op(row: Dict[str, Any]) -> IndexableOp:
        """Convert a raw ``tasks.storage`` row dict to an ``IndexableOp``.

        ``driver_instance_id`` is derived deterministically from the
        ``(driver_id, catalog_id, collection_id)`` triple — the ``tasks.storage``
        table has no ``driver_instance_id`` column (unlike ``storage_outbox``).

        Column mapping: ``entity_id`` (storage table) → ``IndexableOp.item_id``.
        # TODO(#1807 P1.3): branch on entity_kind for collection/catalog/asset tiers
        """
        from dynastore.modules.storage.driver_instance_id import (
            compute_driver_instance_id,
        )

        catalog_id = row["catalog_id"]
        collection_id = row.get("collection_id") or ""
        driver_id = row["driver_id"]

        return IndexableOp(
            op_id=UUID(str(row["op_id"])),
            op=row["op"],  # type: ignore[arg-type]
            catalog_id=catalog_id,
            collection_id=collection_id,
            driver_instance_id=compute_driver_instance_id(
                driver_id, catalog_id, collection_id,
            ),
            item_id=row.get("entity_id"),  # entity_id column → IndexableOp.item_id
            payload=dict(row.get("op_payload") or {}),
            idempotency_key=row.get("idempotency_key") or "",
        )

    # ------------------------------------------------------------------
    # Outcome application
    # ------------------------------------------------------------------

    async def _apply_outcomes(
        self,
        *,
        engine: Any,
        task_schema: str,
        rows: Sequence[Dict[str, Any]],
        result: BulkIndexResult,
        owner_id: str,
    ) -> None:
        """Partition rows per BulkIndexResult and apply fenced mark_*."""
        rows_by_id: Dict[UUID, Dict[str, Any]] = {
            UUID(str(r["op_id"])): r for r in rows
        }

        if result.passed:
            for op_id in result.passed:
                row = rows_by_id.get(op_id)
                if row is not None:
                    await self._mark_done(
                        engine=engine,
                        task_schema=task_schema,
                        row=row,
                        owner_id=owner_id,
                    )

        if result.transient:
            for op_id, reason in result.transient:
                row = rows_by_id.get(op_id)
                if row is not None:
                    await self._mark_retry(
                        engine=engine,
                        task_schema=task_schema,
                        row=row,
                        owner_id=owner_id,
                        error=reason,
                    )

        if result.poison:
            for op_id, _reason in result.poison:
                row = rows_by_id.get(op_id)
                if row is not None:
                    await self._mark_dead(
                        engine=engine,
                        task_schema=task_schema,
                        row=row,
                        owner_id=owner_id,
                    )

        # Defence-in-depth: any claimed op_id the indexer omitted from all
        # three result lists would otherwise sit 'in_flight' until its lease
        # expires (up to ``lease_seconds``). Funnel those to retry so a
        # partial/buggy BulkIndexResult can never strand rows. A well-behaved
        # indexer reports every op, so this is normally a no-op.
        categorized: set[UUID] = set(result.passed)
        categorized.update(op_id for op_id, _ in result.transient)
        categorized.update(op_id for op_id, _ in result.poison)
        for op_id, row in rows_by_id.items():
            if op_id not in categorized:
                await self._mark_retry(
                    engine=engine,
                    task_schema=task_schema,
                    row=row,
                    owner_id=owner_id,
                    error="indexer omitted op_id from BulkIndexResult",
                )

    async def _apply_retry_all(
        self,
        *,
        engine: Any,
        task_schema: str,
        rows: Sequence[Dict[str, Any]],
        owner_id: str,
        error: str,
    ) -> None:
        """Funnel all rows in the batch to retry (whole-batch error path)."""
        for row in rows:
            await self._mark_retry(
                engine=engine,
                task_schema=task_schema,
                row=row,
                owner_id=owner_id,
                error=error,
            )

    # ------------------------------------------------------------------
    # Fenced terminal writes (CAS on claimed_by + claim_version)
    # ------------------------------------------------------------------

    async def _mark_done(
        self,
        *,
        engine: Any,
        task_schema: str,
        row: Dict[str, Any],
        owner_id: str,
    ) -> None:
        """Mark a row done; CAS on (claimed_by, claim_version).

        If another worker reclaimed the row (bumping claim_version), this
        UPDATE matches 0 rows — the stale drain's finalization is a no-op.
        """
        from dynastore.modules.db_config.query_executor import (
            DQLQuery, ResultHandler, managed_transaction,
        )

        sql = (
            f"UPDATE {task_schema}.storage"
            f" SET status='done', finished_at=now()"
            f" WHERE day=:day AND op_id=:op_id"
            f"   AND claimed_by=:owner_id AND claim_version=:claim_version"
        )
        async with managed_transaction(engine) as conn:
            await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                conn,
                day=row["day"],
                op_id=str(row["op_id"]),
                owner_id=owner_id,
                claim_version=row["claim_version"],
            )

    async def _mark_retry(
        self,
        *,
        engine: Any,
        task_schema: str,
        row: Dict[str, Any],
        owner_id: str,
        error: str,
    ) -> None:
        """Mark a row for retry with backoff; CAS on (claimed_by, claim_version).

        Bumps ``attempts`` here (not at claim) and pushes ``ready_at`` into
        the future by the backoff curve keyed on the current attempt count.
        If the CAS predicate misses (stale claim), the row is already owned
        by another worker — this is a safe no-op.
        """
        from dynastore.modules.db_config.query_executor import (
            DQLQuery, ResultHandler, managed_transaction,
        )

        attempts = int(row.get("attempts") or 0)
        backoff = _backoff(attempts)
        sql = (
            f"UPDATE {task_schema}.storage"
            f" SET status='ready', attempts=attempts+1,"
            f"     claimed_by=NULL, claimed_at=NULL,"
            f"     ready_at = now() + make_interval(secs => :backoff_seconds)"
            f" WHERE day=:day AND op_id=:op_id"
            f"   AND claimed_by=:owner_id AND claim_version=:claim_version"
        )
        async with managed_transaction(engine) as conn:
            await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                conn,
                day=row["day"],
                op_id=str(row["op_id"]),
                owner_id=owner_id,
                claim_version=row["claim_version"],
                backoff_seconds=backoff,
            )

        logger.debug(
            "StorageDrainTask: retry op_id=%s attempts+1=%d backoff=%ds error=%r",
            row["op_id"],
            attempts + 1,
            backoff,
            error,
        )

    async def _mark_dead(
        self,
        *,
        engine: Any,
        task_schema: str,
        row: Dict[str, Any],
        owner_id: str,
    ) -> None:
        """Mark a poison row as dead (terminal); CAS on (claimed_by, claim_version)."""
        from dynastore.modules.db_config.query_executor import (
            DQLQuery, ResultHandler, managed_transaction,
        )

        sql = (
            f"UPDATE {task_schema}.storage"
            f" SET status='dead', finished_at=now()"
            f" WHERE day=:day AND op_id=:op_id"
            f"   AND claimed_by=:owner_id AND claim_version=:claim_version"
        )
        async with managed_transaction(engine) as conn:
            await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                conn,
                day=row["day"],
                op_id=str(row["op_id"]),
                owner_id=owner_id,
                claim_version=row["claim_version"],
            )
