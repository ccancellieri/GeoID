"""``TileCacheInvalidatorTask`` — Cloud Run Job entry point for the
write-reactive tile-cache invalidation drain (issue #1292).

Mirrors how the ES indexer drain is deployed: a generic
:class:`~dynastore.tasks.outbox_drain.drain_task.OutboxDrainTask` is the
machinery, but each ``driver_id`` gets its own deployable image so the
worker discovers and drains exactly one OUTBOX stream. The ES worker image
deploys with ``SCOPE=worker_task_elasticsearch_indexer``; the tile-cache
invalidator image deploys with ``SCOPE=worker_task_tile_cache_invalidator``.

Why a thin subclass instead of reusing ``outbox_drain`` directly:
the SCOPE↔entry-point invariant (``tests/dynastore/tasks/unit/
test_worker_scope_invariants.py``) requires every ``worker_task_<name>``
extras key to map to a ``dynastore.tasks.<name>`` entry point. Pinning a
dedicated ``tile_cache_invalidator`` entry point makes the drain
*discoverable* — the worker registry knows the task type exists, the SCOPE
that carries it, and the modules it needs (``tiles`` for the
``TileStorageProtocol`` provider, ``module_catalog`` for tenant resolution).

Runtime wiring (deployment-side, mirroring ``build_es_drain_task``): the
Cloud Run Job's supervisor loop enumerates active catalogs and, per
``(driver_id="tile_cache_invalidator", catalog_id)``, calls
:func:`~dynastore.tasks.outbox_drain.tile_cache_entrypoint.build_tile_cache_drain_task`
to obtain a fully-configured task, then invokes :meth:`drain_once` on LISTEN
wakeups (or a periodic catch-up tick). The drain deletes the stale tiles
named in each op's payload via the registered ``TileStorageProtocol``; lazy
read-miss repopulation refreshes them from current data.
"""
from __future__ import annotations

from typing import List

from dynastore.modules.tiles.tile_cache_sync import TILE_CACHE_DRIVER_ID
from dynastore.tasks.outbox_drain.drain_task import OutboxDrainTask


class TileCacheInvalidatorTask(OutboxDrainTask):
    """Drain ``(driver_id="tile_cache_invalidator", catalog_id)`` rows.

    Inherits the full drain loop from :class:`OutboxDrainTask`; only the
    ``task_type`` differs so the dispatcher routes tile-cache invalidation
    rows here. Per-tenant configuration (indexer + store + catalog) is
    supplied by ``build_tile_cache_drain_task`` at deploy time, exactly as
    the ES drain is configured by ``build_es_drain_task``.
    """

    priority: int = 100
    task_type = TILE_CACHE_DRIVER_ID  # "tile_cache_invalidator"


__all__: List[str] = ["TileCacheInvalidatorTask"]
