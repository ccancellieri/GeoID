"""``OutboxDrainTask`` — generic per-driver, per-tenant outbox consumer.

Production deployment: one Cloud Run Job per ``driver_id`` (e.g.
``worker_task_elasticsearch_indexer``). The job's main loop:

  1. Enumerate active catalogs.
  2. Spawn a LISTEN coroutine per ``(driver_id, catalog_id)`` channel.
  3. On notification (or periodic catch-up), claim a batch and process.

Tests use :meth:`drain_once` directly against a single catalog.

Failure model
-------------

:class:`BulkIndexResult` partitions per-row outcomes:

* ``passed``    → :meth:`OutboxStore.mark_done`.
* ``transient`` → :meth:`OutboxStore.mark_retry` (PG row goes back to
  ``ready`` with bumped ``attempts`` and a backoff-driven ``ready_at``).
  When the row's NEXT-attempts crosses
  ``retry_visible_threshold`` we also write a ``status='retrying'``
  entry to :class:`IndexFailureLog` so tenants can see rows that are
  actually struggling, not every transient blip.
* ``poison``    → :meth:`OutboxStore.mark_failed` (terminal) plus a
  ``status='failed'`` entry in :class:`IndexFailureLog` regardless of
  threshold (poison is by definition non-recoverable).

Whole-batch exception (e.g. indexer crashed): every row is funnelled
to the transient path so the outbox doesn't lose data, with a single
shared error message.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Mapping, Sequence, Union

from dynastore.models.protocols.indexing import (
    BulkIndexer,
    BulkIndexResult,
    IndexableOp,
    IndexFailureLog,
    OutboxRow,
    OutboxStore,
)
from dynastore.modules.tasks.models import TaskPayload
from dynastore.tasks.protocols import TaskProtocol

logger = logging.getLogger(__name__)


class OutboxDrainTask(TaskProtocol):
    """Drain ``storage_outbox`` for one ``(driver_id, catalog_id)`` pair.

    Holds references to a concrete :class:`BulkIndexer`, an
    :class:`OutboxStore`, and an :class:`IndexFailureLog`. The catalog
    is fixed at construction — production wires one task instance per
    active tenant under the parent Cloud Run Job's main loop.
    """

    priority: int = 100
    task_type = "outbox_drain"

    def __init__(
        self,
        app_state: object | None = None,
        *,
        driver_id: str | None = None,
        indexer: BulkIndexer | None = None,
        store: OutboxStore | None = None,
        failure_log: IndexFailureLog | None = None,
        catalog_id: str | None = None,
        batch_size: int = 1500,
        worker_id: str = "drain",
        retry_visible_threshold: int = 3,
    ) -> None:
        # Two construction paths share this signature:
        # (1) Framework registration at startup — `factory()` or
        #     `factory(app_state=...)` with no other kwargs. Instance is
        #     a placeholder until a per-tenant runner configures it.
        # (2) Production wrapper / tests — explicit kwargs supply a
        #     fully-configured instance ready for ``drain_once()``.
        # ``drain_once()`` validates that the configuration is complete.
        self.app_state = app_state
        self.driver_id = driver_id
        self.indexer = indexer
        self.store = store
        self.failure_log = failure_log
        self.catalog_id = catalog_id
        self.batch_size = batch_size
        self.worker_id = worker_id
        self.retry_visible_threshold = retry_visible_threshold

    async def run(self, payload: TaskPayload) -> Dict[str, Any]:
        """Cloud Run Job entrypoint — drain until empty, then exit.

        The dispatcher invokes this once per claimed task row; we keep
        draining batches until ``drain_once()`` reports zero. The
        Job-supervisor loop in production re-enters via LISTEN, so a
        single ``run`` call only needs to clear the current backlog.
        """
        total = 0
        while True:
            n = await self.drain_once()
            total += n
            if n == 0:
                break
        return {
            "drained": total,
            "driver_id": self.driver_id,
            "catalog_id": self.catalog_id,
        }

    def _require_configured(self) -> tuple[str, BulkIndexer, OutboxStore, IndexFailureLog, str]:
        """Assert all collaborators are set; return them as a non-Optional tuple.

        Used by ``drain_once`` so pyright can narrow the Optional fields
        after a single guard. Raises ``RuntimeError`` if framework-zero-arg
        construction was used without subsequent configuration."""
        missing = [
            name for name, value in (
                ("driver_id", self.driver_id),
                ("indexer", self.indexer),
                ("store", self.store),
                ("failure_log", self.failure_log),
                ("catalog_id", self.catalog_id),
            ) if value is None
        ]
        if missing:
            raise RuntimeError(
                f"OutboxDrainTask not configured: missing {missing}. "
                f"The task framework registers a zero-arg placeholder; "
                f"production callers must construct with explicit kwargs "
                f"(driver_id, indexer, store, failure_log, catalog_id) "
                f"before invoking drain."
            )
        # Cast through assert for pyright narrowing.
        assert self.driver_id is not None
        assert self.indexer is not None
        assert self.store is not None
        assert self.failure_log is not None
        assert self.catalog_id is not None
        return (
            self.driver_id, self.indexer, self.store,
            self.failure_log, self.catalog_id,
        )

    async def drain_once(self) -> int:
        """Claim a single batch and apply outcomes; return rows-handled.

        Whole-batch exception path: forward every row to transient retry
        with the exception message, so a flaky indexer can't move rows
        to the terminal ``failed`` status. Per-row poison classification
        is the indexer's responsibility, not the drain task's.
        """
        driver_id, indexer, store, failure_log, catalog_id = self._require_configured()
        rows = await store.claim_batch(
            driver_id=driver_id,
            catalog_id=catalog_id,
            batch_size=self.batch_size,
            claimed_by=self.worker_id,
        )
        if not rows:
            return 0

        ops = [self._row_to_op(r) for r in rows]
        try:
            result = await indexer.index_bulk(ops)
        except Exception as exc:  # noqa: BLE001 — surface every failure
            logger.warning(
                "OutboxDrainTask[%s/%s]: whole-batch error: %s",
                driver_id,
                catalog_id,
                exc,
            )
            await store.mark_retry(
                catalog_id=catalog_id,
                op_ids=[r.op_id for r in rows],
                error=str(exc),
                attempts_seen=rows[0].attempts,
            )
            await self._record_retry_visible(rows, str(exc), failure_log, catalog_id)
            return len(rows)

        await self._apply_outcomes(rows, result, store, failure_log, catalog_id)
        return len(rows)

    async def _apply_outcomes(
        self,
        rows: Sequence[OutboxRow],
        result: BulkIndexResult,
        store: OutboxStore,
        failure_log: IndexFailureLog,
        catalog_id: str,
    ) -> None:
        """Partition ``rows`` per ``result`` and apply the right
        ``mark_*`` and :class:`IndexFailureLog` writes."""
        rows_by_id = {r.op_id: r for r in rows}

        if result.passed:
            await store.mark_done(
                catalog_id=catalog_id, op_ids=result.passed,
            )

        if result.transient:
            tids = [op_id for op_id, _ in result.transient]
            errs_by_id = {op_id: reason for op_id, reason in result.transient}
            joined = " / ".join(set(errs_by_id.values())) or "transient"
            await store.mark_retry(
                catalog_id=catalog_id,
                op_ids=tids,
                error=joined,
                attempts_seen=rows_by_id[tids[0]].attempts,
            )
            transient_rows = [rows_by_id[op_id] for op_id in tids]
            await self._record_retry_visible(
                transient_rows, errs_by_id, failure_log, catalog_id,
            )

        if result.poison:
            pids = [op_id for op_id, _ in result.poison]
            errs_by_id = {op_id: reason for op_id, reason in result.poison}
            joined = " / ".join(set(errs_by_id.values())) or "poison"
            await store.mark_failed(
                catalog_id=catalog_id, op_ids=pids, error=joined,
            )
            for r in (rows_by_id[op_id] for op_id in pids):
                await failure_log.record(
                    None,
                    catalog_id=catalog_id,
                    collection_id=r.collection_id,
                    driver_instance_id=r.driver_instance_id,
                    driver_id=r.driver_id,
                    op_id=r.op_id,
                    item_id=r.item_id,
                    op=r.op,
                    attempts=r.attempts + 1,
                    error_class="PoisonIndexerError",
                    error_message=errs_by_id[r.op_id],
                    status="failed",
                )

    async def _record_retry_visible(
        self,
        rows: Sequence[OutboxRow],
        errs: Union[str, Mapping[Any, str]],
        failure_log: IndexFailureLog,
        catalog_id: str,
    ) -> None:
        """Write to :class:`IndexFailureLog` only when the row's NEXT
        attempts crosses ``retry_visible_threshold``.

        Keeps the failure log focused on rows that are actually
        struggling, not every transient blip — under a healthy retry
        curve the log stays readable for tenants.
        """
        for r in rows:
            next_attempts = r.attempts + 1
            if next_attempts < self.retry_visible_threshold:
                continue
            err_msg = (
                errs
                if isinstance(errs, str)
                else errs.get(r.op_id, "transient")
            )
            await failure_log.record(
                None,
                catalog_id=catalog_id,
                collection_id=r.collection_id,
                driver_instance_id=r.driver_instance_id,
                driver_id=r.driver_id,
                op_id=r.op_id,
                item_id=r.item_id,
                op=r.op,
                attempts=next_attempts,
                error_class="TransientIndexerError",
                error_message=err_msg,
                status="retrying",
            )

    @staticmethod
    def _row_to_op(r: OutboxRow) -> IndexableOp:
        """Lift a raw-from-DB :class:`OutboxRow` to a typed
        :class:`IndexableOp`.

        ``OutboxRow.op`` is a free-form ``str`` (the table-level CHECK
        constraint enforces values, not the dataclass), while
        :class:`IndexableOp.op` is ``Literal["upsert", "delete"]``.
        Crossing the seam intentionally narrows; if a corrupt row ever
        carries an out-of-domain ``op``, downstream indexer dispatch
        will surface it through normal failure handling.
        """
        return IndexableOp(
            op_id=r.op_id,
            op=r.op,  # type: ignore[arg-type]
            catalog_id=r.catalog_id,
            collection_id=r.collection_id,
            driver_instance_id=r.driver_instance_id,
            item_id=r.item_id,
            payload=r.payload,
            idempotency_key=r.idempotency_key,
        )
