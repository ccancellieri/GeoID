"""ES drain entrypoint — compose :class:`OutboxDrainTask` with
:class:`ESBulkIndexer` for the
``worker_task_elasticsearch_indexer`` Cloud Run Job.

The Job's main loop calls :func:`build_es_drain_task` once per
``(driver_id="items_elasticsearch_driver", catalog_id)`` pair it
needs to drain, then invokes :meth:`OutboxDrainTask.drain_once` on
LISTEN wakeups (or on a periodic catch-up tick) until the
``storage_outbox`` for that pair is empty.

Connection ownership: the caller (Cloud Run Job runtime) owns the
asyncpg connection. :class:`PgOutboxStore` and the failure log keep
the connection alive across calls; the entrypoint does not open or
close it.
"""
from __future__ import annotations

import logging
from typing import Any, List, Literal, Optional, Sequence, cast
from uuid import UUID

from dynastore.models.protocols.indexing import IndexFailureRecord
from dynastore.tasks.outbox_drain.drain_task import OutboxDrainTask

logger = logging.getLogger(__name__)


class _NoopFailureLog:
    """Protocol-conformant no-op stand-in for :class:`IndexFailureLog`.

    Used when the PG-backed implementation is not yet available in
    the running deployment. Calls are logged at warning level so the
    deployment surface still flags poisoned rows even if the durable
    tenant-facing failure table isn't wired up.
    """

    async def record(
        self,
        conn: Any,
        *,
        catalog_id: str,
        collection_id: str,
        driver_instance_id: str,
        driver_id: str,
        op_id: UUID,
        item_id: Optional[str],
        op: str,
        attempts: int,
        error_class: str,
        error_message: str,
        status: Literal["retrying", "failed"],
        correlation_id: Optional[str] = None,
    ) -> None:
        logger.warning(
            "_NoopFailureLog.record(%s/%s/%s op=%s status=%s attempts=%d): %s — %s",
            catalog_id,
            collection_id,
            op_id,
            op,
            status,
            attempts,
            error_class,
            error_message,
        )

    async def list_failures(
        self, **kwargs: Any,
    ) -> Sequence[IndexFailureRecord]:
        return []


async def build_es_drain_task(
    *,
    conn: Any,
    catalog_id: str,
    batch_size: int = 1500,
    worker_id: str = "es-drain",
) -> OutboxDrainTask:
    """Build a configured :class:`OutboxDrainTask` for the ES driver.

    The returned task drains
    ``(driver_id="items_elasticsearch_driver", catalog_id)`` rows
    from ``storage_outbox`` via :class:`ESBulkIndexer` over the
    supplied connection. Caller owns the conn lifecycle.

    Raises :class:`RuntimeError` if the underlying ES driver reports
    ``is_available()`` is False — typically because
    ``opensearch-py`` is missing from the active extras (the worker
    image must include ``module_elasticsearch``).
    """
    from dynastore.modules.storage.drivers.elasticsearch import (
        ItemsElasticsearchDriver,
    )
    from dynastore.modules.storage.pg_outbox import PgOutboxStore
    from dynastore.tasks.outbox_drain.es_indexer_adapter import ESBulkIndexer

    # ``cast(Any, ...)`` mirrors the pattern at
    # ``dynastore/tasks/__init__.py::get_task_instance`` — pyright sees
    # the Protocol-mixin in the class hierarchy as abstract; runtime
    # instantiation is fine because the class fully implements the
    # storage-driver surface it needs here.
    driver = cast(Any, ItemsElasticsearchDriver)()
    if not driver.is_available():
        raise RuntimeError(
            "ES drain entrypoint started without opensearch-py — add "
            "module_elasticsearch to the worker pyproject extras.",
        )

    indexer = ESBulkIndexer(driver)

    # The PG-backed failure log is its own ship; until then a no-op
    # log keeps the drain task wireable end-to-end. The drain task
    # only writes to the failure log on poisoned rows or rows that
    # crossed the retry-visible threshold; misses there cost
    # observability, not correctness.
    # Resolve the PG-backed failure log via importlib so pyright doesn't
    # flag the not-yet-shipped module name; a regular import would fail
    # static resolution even with try/except guards.
    import importlib
    failure_log: Any
    try:
        _pg_failure_log_mod = importlib.import_module(
            "dynastore.modules.storage.pg_index_failure_log",
        )
        failure_log = _pg_failure_log_mod.PgIndexFailureLog(single_conn=conn)
    except ImportError:
        failure_log = _NoopFailureLog()

    return cast(Any, OutboxDrainTask)(
        driver_id="items_elasticsearch_driver",
        indexer=indexer,
        store=PgOutboxStore(single_conn=conn),
        failure_log=failure_log,
        catalog_id=catalog_id,
        batch_size=batch_size,
        worker_id=worker_id,
    )


__all__: List[str] = ["build_es_drain_task"]
