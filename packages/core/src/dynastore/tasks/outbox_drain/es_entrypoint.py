"""ES drain entrypoint — compose :class:`OutboxDrainTask` with
:class:`ESBulkIndexer` for the
``worker_task_elasticsearch_indexer`` Cloud Run Job.

The Job's main loop calls :func:`build_es_drain_task` once per
``(driver_id="items_elasticsearch_driver", catalog_id)`` pair it
needs to drain, then invokes :meth:`OutboxDrainTask.drain_once` on
LISTEN wakeups (or on a periodic catch-up tick) until the
``storage_outbox`` for that pair is empty.

Connection ownership: the caller (Cloud Run Job runtime) owns the
asyncpg connection. :class:`PgOutboxStore` keeps the connection alive
across calls; the entrypoint does not open or close it.

Failure surfacing goes through ``log_event()`` (canonical ``/logs/``
channel) — the drain task itself owns that path; this entrypoint
just wires the indexer + store.
"""
from __future__ import annotations

import logging
from typing import Any, List, cast

from dynastore.tasks.outbox_drain.drain_task import OutboxDrainTask

logger = logging.getLogger(__name__)


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

    return cast(Any, OutboxDrainTask)(
        driver_id="items_elasticsearch_driver",
        indexer=indexer,
        store=PgOutboxStore(single_conn=conn),
        catalog_id=catalog_id,
        batch_size=batch_size,
        worker_id=worker_id,
    )


__all__: List[str] = ["build_es_drain_task"]
