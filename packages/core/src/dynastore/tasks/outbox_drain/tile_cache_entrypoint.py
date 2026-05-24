"""Tile-cache drain entrypoint — compose :class:`OutboxDrainTask` with
:class:`TileCacheBulkIndexer` for the
``worker_task_tile_cache_invalidator`` drain (issue #1292).

Mirrors :mod:`dynastore.tasks.outbox_drain.es_entrypoint`: the drain loop
calls :func:`build_tile_cache_drain_task` once per
``(driver_id="tile_cache_invalidator", catalog_id)`` pair it needs to
drain, then invokes :meth:`OutboxDrainTask.drain_once` on LISTEN wakeups
(or a periodic catch-up tick) until ``storage_outbox`` for that pair is
empty.

Connection ownership: the caller owns the asyncpg connection;
:class:`PgOutboxStore` keeps it alive across calls. The drain deletes the
stale tiles named in each op's payload via the registered
:class:`TileStorageProtocol`; lazy read-miss repopulation then refreshes
them from current data.
"""
from __future__ import annotations

import logging
from typing import Any, List, cast

from dynastore.modules.tiles.tile_cache_sync import TILE_CACHE_DRIVER_ID
from dynastore.tasks.outbox_drain.drain_task import OutboxDrainTask

logger = logging.getLogger(__name__)


async def build_tile_cache_drain_task(
    *,
    conn: Any,
    catalog_id: str,
    batch_size: int = 256,
    worker_id: str = "tile-cache-drain",
) -> OutboxDrainTask:
    """Build a configured :class:`OutboxDrainTask` for tile-cache invalidation.

    The returned task drains
    ``(driver_id="tile_cache_invalidator", catalog_id)`` rows from
    ``storage_outbox`` via :class:`TileCacheBulkIndexer` over the supplied
    connection. Caller owns the conn lifecycle.
    """
    from dynastore.modules.storage.pg_outbox import PgOutboxStore
    from dynastore.tasks.outbox_drain.tile_cache_indexer_adapter import (
        TileCacheBulkIndexer,
    )

    indexer = TileCacheBulkIndexer()

    return cast(Any, OutboxDrainTask)(
        driver_id=TILE_CACHE_DRIVER_ID,
        indexer=indexer,
        store=PgOutboxStore(single_conn=conn),
        catalog_id=catalog_id,
        batch_size=batch_size,
        worker_id=worker_id,
    )


__all__: List[str] = ["build_tile_cache_drain_task"]
