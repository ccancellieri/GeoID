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
``schema_name`` column, not the physical table). Uses ``task_type``
``"storage_drain"``.

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
(one-shot drain-to-empty shape).  The
dispatcher restarts via LISTEN / periodic catch-up; a single ``run`` call
only needs to clear the current backlog.
"""
from __future__ import annotations

import logging
from dataclasses import replace as _dataclass_replace
from typing import Any, ClassVar, Dict, List, NamedTuple, Optional, Sequence, Tuple, cast
from uuid import UUID, uuid4

from dynastore.models.protocols.indexing import (
    STORAGE_PLANE_ID_ONLY_MARKER_KEY,
    BulkIndexer,
    BulkIndexResult,
    IndexableOp,
)
from dynastore.models.tasks import TaskPayload
from dynastore.tasks.protocols import TaskProtocol
from dynastore.tasks.report import TaskReport
from dynastore.tools.db import validate_sql_identifier

logger = logging.getLogger(__name__)


# Per-attempt retry backoff in seconds.
# Index by ``attempts`` (0-based); the last entry caps the backoff at ~30 min.
_BACKOFF_SECONDS: List[int] = [1, 5, 30, 5 * 60, 30 * 60]

# Seconds before an in_flight row is considered stale and eligible for
# reclaim by any drain worker.
_DEFAULT_LEASE_SECONDS: int = 300

# Default claim batch size. Matches TasksPluginConfig.storage_drain_batch_size:
# id-only rows (#2494 P1) hydrate to full canonical documents for the whole
# claimed batch before the bulk dispatch, so the claim size bounds peak memory.
# The previous 1500 OOM-killed 2Gi containers on MB-scale geometries (#2723).
_DEFAULT_BATCH_SIZE: int = 100

def _backoff(attempts: int) -> int:
    """Return the backoff in seconds for the given zero-based attempt count."""
    idx = min(attempts, len(_BACKOFF_SECONDS) - 1)
    return _BACKOFF_SECONDS[idx]


class _PreparedDriverBatch(NamedTuple):
    """Split of one driver's claimed rows, ready for the outcome pipeline.

    ``ops``/``rows_for_ops`` are parallel to the pre-#2494 behaviour: every
    op sent to ``indexer.index_bulk`` has its claimed row in
    ``rows_for_ops`` so ``_apply_outcomes``/``_apply_retry_all`` can map
    results back. ``auto_done``/``auto_retry`` are resolved WITHOUT calling
    the indexer at all — a re-read-canonical-state outcome for an id-only
    row (#2494 P1) that never needs (or cannot survive) a bulk call.
    """

    ops: List[IndexableOp]
    rows_for_ops: List[Dict[str, Any]]
    auto_done: List[Dict[str, Any]]
    auto_retry: List[Tuple[Dict[str, Any], str]]


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
        # catalog_id -> resolved known-fields set, memoised for this run
        # (#2494 P1 canonical re-read — shared by every id-only group under
        # the same catalog).
        self._known_fields_cache: Dict[str, Any] = {}
        # driver_ids that have already logged the "empty payload, no
        # id-only marker" anomaly warning this run (#2494 P1 dedup).
        self._empty_payload_warned: set = set()
        # Split completion counters for this run (#2731): 'indexed' rows went
        # through a BulkIndexer and were reported passed; 'auto_done' rows were
        # resolved as done WITHOUT the indexer (canonical row verifiably absent
        # — a legitimate deleted-item skip); 'retried' rows were funnelled to
        # backoff for any reason (indexer error, unresolved driver, failed
        # canonical re-read, canonical doc build failure, an indexer-reported
        # transient result, or an indexer omission). Reset at the start of
        # every ``run()``; accumulated by ``drain_once()``.
        self._run_metrics: Dict[str, int] = {
            "indexed": 0, "auto_done": 0, "retried": 0,
        }

    async def run(self, payload: TaskPayload) -> TaskReport:
        """Drain ``tasks.storage`` to empty, then return.

        Loops ``drain_once()`` until it reports zero claimed rows.  The
        dispatcher re-enters via NOTIFY when new rows appear.

        Returns a :class:`~dynastore.tasks.report.TaskReport` so the runner
        persists structured metrics alongside the human-facing message
        (#1807 P2).  ``drain_once`` retains its ``int`` return type so internal
        callers and existing tests are unaffected.
        """
        from dynastore.modules.db_config.db_config import DBConfig
        from dynastore.modules.db_config.tools import normalize_db_url
        from sqlalchemy.ext.asyncio import create_async_engine
        from sqlalchemy.pool import NullPool

        # ``normalize_db_url`` both swaps the prefix to ``postgresql+asyncpg://``
        # AND converts the libpq ``sslmode=`` query parameter to asyncpg's
        # ``ssl=``.  A bare prefix swap leaves ``sslmode=`` in the URL, which
        # makes asyncpg's ``connect()`` raise "unexpected keyword argument
        # 'sslmode'" against a Cloud SQL DSN — failing every drain unrecoverably
        # and leaving the rows stuck.  Mirror the canonical engine build in
        # ``db_service`` rather than re-deriving the URL by hand.
        db_url = normalize_db_url(DBConfig.database_url, is_async=True)

        # One engine for the lifetime of this run — shared across all
        # claim and terminal-write statements so connection overhead is paid
        # once, not per-row.
        engine = create_async_engine(db_url, poolclass=NullPool)
        # Stable owner_id for the lifetime of this run — used as the
        # ``claimed_by`` stamp and the CAS guard on terminal writes.
        owner_id = f"storage_drain:{uuid4()}"
        # Hot-reloaded claim size, resolved once per run: id-only rows
        # hydrate to full canonical documents for the whole claimed batch
        # before the bulk dispatch, so this bounds the run's peak memory
        # (#2723 — 1500 MB-scale features OOM-killed the host container).
        batch_size = await self._resolve_batch_size()
        # Reset the split counters for this run — drain_once accumulates
        # into self._run_metrics as it classifies each claimed batch (#2731).
        self._run_metrics = {"indexed": 0, "auto_done": 0, "retried": 0}
        total = 0
        try:
            while True:
                n = await self.drain_once(
                    engine=engine, owner_id=owner_id, batch_size=batch_size,
                )
                total += n
                if n == 0:
                    break
        finally:
            await engine.dispose()

        # 'drained' stays the total claimed count for backward compat; the
        # split counters distinguish rows actually written to the index from
        # rows resolved without ever reaching it, so a completion message can
        # no longer describe silently-dropped rows as uniformly "processed"
        # (#2731 — a drain once reported 6096 processed while ~5200 of them
        # were auto_done id-only rows whose canonical re-read had swallowed
        # an error, with zero WARNING/ERROR logs).
        report = TaskReport.completed(
            message=f"storage drain completed: {total} row(s) processed",
            metrics={"drained": total, **self._run_metrics},
            correlation={"owner_id": owner_id},
        )

        # Best-effort structured log via the catalog log manager.  No
        # catalog_id is available at the drain level (this task is global,
        # not tenant-scoped), so we skip the tenant log and rely on the
        # standard logger for observability.
        logger.info(
            "StorageDrainTask finished",
            extra={"task_report": report.log_details()},
        )

        return report

    async def _resolve_batch_size(self) -> int:
        """Resolve ``TasksPluginConfig.storage_drain_batch_size``, hot-reloaded.

        Mirrors the resolution pattern of
        ``index_dispatcher._storage_plane_routing_enabled``: falls back to
        the instance default (constructor value, itself matching the field
        default) when the platform configs protocol is unavailable — early
        startup, lightweight worker contexts, tests.
        """
        try:
            from dynastore.models.protocols.platform_configs import (
                PlatformConfigsProtocol,
            )
            from dynastore.modules.tasks.tasks_config import TasksPluginConfig
            from dynastore.tools.discovery import get_protocol

            config_mgr = get_protocol(PlatformConfigsProtocol)
            if config_mgr is not None:
                cfg = await config_mgr.get_config(TasksPluginConfig)
                if isinstance(cfg, TasksPluginConfig):
                    return int(cfg.storage_drain_batch_size)
        except Exception:  # noqa: BLE001 — config read is best-effort
            logger.debug(
                "StorageDrainTask: storage_drain_batch_size unavailable — "
                "falling back to the instance default (%d).",
                self.batch_size,
                exc_info=True,
            )
        return self.batch_size

    async def drain_once(
        self, *, engine: Any, owner_id: str, batch_size: Optional[int] = None,
    ) -> int:
        """Claim one batch, process, apply fenced outcomes; return rows handled.

        ``batch_size`` overrides the instance default for this cycle (the
        hot-reloaded ``TasksPluginConfig.storage_drain_batch_size`` value,
        resolved once per run); ``None`` keeps ``self.batch_size`` so
        internal callers and existing tests are unaffected.

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
            batch_size=batch_size if batch_size is not None else self.batch_size,
        )
        if not rows:
            return 0

        # Group claimed rows by driver_id for bulk dispatch.
        by_driver: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            by_driver.setdefault(row["driver_id"], []).append(row)

        # Per-batch classification counters (#2731): logged below and folded
        # into self._run_metrics so a drain's completion report can never
        # again describe auto_done/retried rows as uniformly "processed".
        batch_indexed = 0
        batch_auto_done = 0
        batch_retried = 0

        for driver_id, driver_rows in by_driver.items():
            indexer = await self._resolve_indexer(driver_id)
            if indexer is None:
                logger.warning(
                    "StorageDrainTask: driver_id=%r is not registered — "
                    "%d row(s) queued for retry. Registration is required: "
                    "the owning driver's module must be installed and "
                    "instantiated for this runtime's SCOPE so it registers "
                    "into the storage driver registry.",
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
                batch_retried += len(driver_rows)
                continue

            prepared = await self._prepare_ops(engine=engine, driver_rows=driver_rows)

            # Rows resolved WITHOUT the indexer (#2494 P1 canonical re-read):
            # a missing PG row on an id-only upsert is a deleted item — skip
            # as success; a re-read that itself raised is a transient infra
            # failure, not a poison classification.
            for row in prepared.auto_done:
                await self._mark_done(
                    engine=engine, task_schema=task_schema,
                    row=row, owner_id=owner_id,
                )
            batch_auto_done += len(prepared.auto_done)
            for row, reason in prepared.auto_retry:
                await self._mark_retry(
                    engine=engine, task_schema=task_schema,
                    row=row, owner_id=owner_id, error=reason,
                )
            batch_retried += len(prepared.auto_retry)

            if not prepared.ops:
                continue

            try:
                result = await indexer.index_bulk(prepared.ops)
            except Exception as exc:  # noqa: BLE001 — surface every failure
                logger.warning(
                    "StorageDrainTask[%s]: whole-batch error: %s",
                    driver_id,
                    exc,
                )
                await self._apply_retry_all(
                    engine=engine,
                    task_schema=task_schema,
                    rows=prepared.rows_for_ops,
                    owner_id=owner_id,
                    error=str(exc),
                )
                batch_retried += len(prepared.rows_for_ops)
                continue

            outcome_counts = await self._apply_outcomes(
                engine=engine,
                task_schema=task_schema,
                rows=prepared.rows_for_ops,
                result=result,
                owner_id=owner_id,
            )
            batch_indexed += outcome_counts["indexed"]
            batch_retried += outcome_counts["retried"]

        self._run_metrics["indexed"] += batch_indexed
        self._run_metrics["auto_done"] += batch_auto_done
        self._run_metrics["retried"] += batch_retried
        logger.info(
            "StorageDrainTask: batch claimed=%d indexed=%d auto_done=%d retried=%d",
            len(rows), batch_indexed, batch_auto_done, batch_retried,
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
        batch_size: Optional[int] = None,
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
                batch_size=batch_size if batch_size is not None else self.batch_size,
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

        There is no construction fallback: a driver that is not registered
        (module not installed for this runtime's SCOPE, or discovery/lifespan
        hasn't populated it yet) resolves to ``None``. Registration is the
        single, symmetric contract every storage driver must satisfy to
        participate in drain — see module entry points under
        ``[project.entry-points."dynastore.modules"]`` (e.g.
        ``storage_elasticsearch = "...:ItemsElasticsearchDriver"``).

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
        from dynastore.tasks.workclass_drain.es_indexer_adapter import ESBulkIndexer

        # --- Registry-driven resolution ---
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

        # Unregistered: no construction fallback. The driver's module must
        # register itself (its entry point instantiated for this runtime's
        # SCOPE) for drain to find it — see the resolution-order note above.
        return None

    # ------------------------------------------------------------------
    # Row-to-op conversion
    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_op(row: Dict[str, Any]) -> IndexableOp:
        """Convert a raw ``tasks.storage`` row dict to an ``IndexableOp``.

        ``driver_instance_id`` is derived deterministically from the
        ``(driver_id, catalog_id, collection_id)`` triple — the ``tasks.storage``
        table has no ``driver_instance_id`` column; it is derived here.

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
    # Canonical re-read for id-only rows (#2494 P1)
    # ------------------------------------------------------------------

    async def _prepare_ops(
        self, *, engine: Any, driver_rows: List[Dict[str, Any]],
    ) -> _PreparedDriverBatch:
        """Split one driver's claimed rows into indexer-bound ops and
        directly-resolved outcomes.

        Delete rows and payload-carrying upsert rows convert to
        ``IndexableOp`` unchanged (:meth:`_row_to_op`). Id-only upsert rows
        — ``op='upsert'`` whose ``op_payload`` carries the explicit
        ``{STORAGE_PLANE_ID_ONLY_MARKER_KEY: true}`` sentinel, written by
        ``IndexDispatcher._enqueue_storage_plane_ids`` when
        ``TasksPluginConfig.items_secondary_via_storage_plane`` is enabled —
        are grouped by ``(catalog_id, collection_id)`` and re-read from
        canonical PG state in ONE batched SELECT per group via
        :func:`read_canonical_index_inputs`:

        * a resolved geoid becomes an ``IndexableOp`` carrying the
          freshly-built canonical document;
        * a geoid absent from PG is treated as a deleted item and marked
          done directly (last-write-wins — the item is gone, indexing it
          would be wrong and re-trying it would never succeed);
        * a group whose re-read itself raises funnels every id-only row in
          that group to retry (a transient infra failure, not a poison
          classification — the geoid's existence is simply unknown).

        Detection keys off the explicit marker, NOT payload emptiness: the
        ``tasks.storage`` DDL default for ``op_payload`` is ALSO
        ``'{}'::jsonb``, so a genuinely empty (unmarked) upsert payload is
        legacy shape, not an id-only obligation — it falls through to the
        normal ``_row_to_op`` conversion below, same as any other
        payload-carrying row, with a one-time-per-driver WARNING since an
        unmarked empty payload is unusual post-#2494 (review finding).
        """
        ops: List[IndexableOp] = []
        rows_for_ops: List[Dict[str, Any]] = []
        auto_done: List[Dict[str, Any]] = []
        auto_retry: List[Tuple[Dict[str, Any], str]] = []

        id_only_by_group: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        for row in driver_rows:
            payload = row.get("op_payload") or {}
            is_id_only = (
                row["op"] == "upsert"
                and isinstance(payload, dict)
                and payload.get(STORAGE_PLANE_ID_ONLY_MARKER_KEY) is True
            )
            if is_id_only:
                key = (row["catalog_id"], row.get("collection_id") or "")
                id_only_by_group.setdefault(key, []).append(row)
                continue
            if row["op"] == "upsert" and not payload:
                driver_id = row.get("driver_id")
                if driver_id not in self._empty_payload_warned:
                    self._empty_payload_warned.add(driver_id)
                    logger.warning(
                        "StorageDrainTask: driver_id=%r has an upsert row "
                        "with an EMPTY op_payload and no id-only marker — "
                        "treating as a legacy payload-carrying row (empty "
                        "body). Future occurrences for this driver_id are "
                        "suppressed this run.",
                        driver_id,
                    )
            ops.append(self._row_to_op(row))
            rows_for_ops.append(row)

        for (catalog_id, collection_id), group_rows in id_only_by_group.items():
            geoids = [r["entity_id"] for r in group_rows if r.get("entity_id")]
            try:
                inputs = await self._read_canonical_inputs(
                    engine=engine, catalog_id=catalog_id,
                    collection_id=collection_id, geoids=geoids,
                )
            except Exception as exc:  # noqa: BLE001 — funnel to retry, never drop
                logger.warning(
                    "StorageDrainTask: canonical re-read failed for %s/%s "
                    "(%d id-only row(s)): %s — funnelling to retry.",
                    catalog_id, collection_id, len(group_rows), exc,
                )
                auto_retry.extend(
                    (r, f"canonical_reread_failed: {exc}") for r in group_rows
                )
                continue

            group_auto_done = 0
            for row in group_rows:
                geoid = row.get("entity_id")
                ci = inputs.get(geoid) if geoid else None
                if ci is None:
                    # No PG row — item deleted after the obligation was
                    # enqueued (or the geoid never existed). Skip as
                    # success rather than indexing a stale/absent doc.
                    auto_done.append(row)
                    group_auto_done += 1
                    continue
                try:
                    doc = await self._build_canonical_doc(
                        catalog_id=catalog_id, collection_id=collection_id, ci=ci,
                    )
                except Exception as exc:  # noqa: BLE001 — funnel to retry
                    logger.warning(
                        "StorageDrainTask: canonical doc build failed for "
                        "%s/%s/%s: %s — funnelling to retry.",
                        catalog_id, collection_id, geoid, exc,
                    )
                    auto_retry.append((row, f"canonical_doc_build_failed: {exc}"))
                    continue
                op = _dataclass_replace(self._row_to_op(row), payload=doc)
                ops.append(op)
                rows_for_ops.append(row)

            # Observability (#2731): auto_done is a legitimate outcome (the
            # canonical row is verifiably absent, not merely unreadable), but
            # it must never be silent — the drain that lost ~5200 items on
            # 2026-07-02 produced zero WARNING/ERROR logs for the entire run.
            if group_auto_done:
                logger.info(
                    "StorageDrainTask: %d id-only row(s) auto_done (canonical "
                    "row absent) for %s/%s",
                    group_auto_done, catalog_id, collection_id,
                )

        return _PreparedDriverBatch(
            ops=ops, rows_for_ops=rows_for_ops,
            auto_done=auto_done, auto_retry=auto_retry,
        )

    async def _read_canonical_inputs(
        self, *, engine: Any, catalog_id: str, collection_id: str,
        geoids: List[str],
    ) -> Dict[str, Any]:
        """Batch-read canonical PG state for ``geoids`` (test seam)."""
        from dynastore.modules.catalog.canonical_index_read import (
            read_canonical_index_inputs,
        )

        return await read_canonical_index_inputs(
            catalog_id, collection_id, geoids, db_resource=engine,
        )

    async def _build_canonical_doc(
        self, *, catalog_id: str, collection_id: str, ci: Any,
    ) -> Dict[str, Any]:
        """Assemble the canonical ES doc for one re-read row (test seam).

        ``known_fields`` is per-catalog and memoised for the lifetime of
        this drain run — every id-only group under the same catalog shares
        it instead of re-resolving per group.

        Couples this generically-named drain task to the ES-shaped
        canonical envelope (``build_canonical_index_doc``); today the only
        ``BulkIndexer`` the drain resolves is the ES adapter
        (``items_elasticsearch_driver``), so this is not a behaviour change —
        a future non-ES ``BulkIndexer`` would need its own re-read
        strategy here.
        """
        from dynastore.modules.elasticsearch.canonical_doc import (
            build_canonical_index_doc,
        )

        known_fields = self._known_fields_cache.get(catalog_id)
        if known_fields is None:
            from dynastore.modules.elasticsearch.items_projection import (
                resolve_catalog_known_fields,
            )

            known_fields = await resolve_catalog_known_fields(catalog_id)
            self._known_fields_cache[catalog_id] = known_fields

        return build_canonical_index_doc(
            ci.row,
            resolved_sidecars=ci.resolved_sidecars,
            known_fields=known_fields,
            catalog_id=catalog_id,
            collection_id=collection_id,
            geometry=ci.geometry,
            bbox=ci.bbox,
            user_properties=ci.user_properties,
            access=ci.access,
            stac_reserved_members=ci.stac_reserved_members,
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
    ) -> Dict[str, int]:
        """Partition rows per BulkIndexResult and apply fenced mark_*.

        Returns ``{"indexed": n, "retried": n}`` — counts of rows actually
        marked done via the indexer path / marked for retry, for the
        per-batch classification summary (#2731). Poison (dead) rows are not
        included; ``TaskReport.metrics`` only splits indexed/auto_done/retried.
        """
        rows_by_id: Dict[UUID, Dict[str, Any]] = {
            UUID(str(r["op_id"])): r for r in rows
        }
        indexed_count = 0
        retried_count = 0

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
                    indexed_count += 1

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
                    retried_count += 1

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
                retried_count += 1

        return {"indexed": indexed_count, "retried": retried_count}

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
