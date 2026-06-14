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

"""Pipeline integration test — catalog mutation → event → dispatch → Indexer.

Goal: a single test that proves the M3.0 + M3.1 + M3.1b + M3.2 chain
connects.  Uses stubs for the durable-storage pieces (outbox, asyncpg)
but exercises the real code paths in between:

1. :func:`catalog_router._emit_catalog_metadata_changed`
   correctly wraps the emission.
2. :meth:`EventService.dispatch_to_listeners` dispatches via registered
   async listeners.  We bypass the EventDrainTask (which needs a real
   ``tasks.events`` row + asyncio scheduling) and invoke the registered
   listener directly, exactly the way ``dispatch_to_listeners`` does per
   claimed row.
3. :func:`handle_catalog_metadata_changed` (the listener adapter)
   extracts the payload, builds a one-event batch, and calls
   :meth:`ReindexWorker.handle_batch`.
4. :class:`ReindexWorker` resolves indexers via an injected async
   resolver (production uses :func:`_resolve_catalog_indexers` which
   queries ``ConfigsProtocol``).
5. :class:`LogCatalogIndexer` receives the dispatch and logs.

What this test would catch that the unit tests would not:

- The listener signature contract with ``dispatch_to_listeners``
  (listener receives payload as kwargs) — no unit test exercises this
  handoff.
- The ``ReindexWorker`` sync/async resolver detection
  (``inspect.iscoroutinefunction`` branch) with the real
  :func:`_resolve_catalog_indexers`.
- Payload round-trip shape: the keys the router emits
  (``catalog_id``, ``domain``, ``operation``) are the keys the
  worker reads.
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

# Fire-and-forget ``asyncio.create_task`` paths in the router under test
# can leave a pending Future at teardown if any awaited link in the
# listener chain never resolves; the per-test SIGALRM timeout below
# guarantees we get a stack trace in seconds instead of stalling the
# whole xdist worker (and the job) for the full 45-min runner timeout.
# ``xdist_group`` keeps both tests on one worker so they don't race a
# sibling test that monkeypatches the same ``event_service`` singleton.
pytestmark = [
    pytest.mark.timeout(30, method="signal"),
    pytest.mark.xdist_group("reindex_pipeline_integration"),
]


# ---------------------------------------------------------------------------
# Fixture: a routing-config override that pins LogCatalogIndexer as a WRITE
# secondary index
# ---------------------------------------------------------------------------


class _FakeConfigsProtocol:
    """Minimal ``ConfigsProtocol`` double that returns a canned routing config.

    Real ``ConfigsProtocol`` walks a 4-tier waterfall through the config
    store; for this integration test we shortcut to "always return the
    override passed at construction time".
    """

    def __init__(self, routing_config):
        self._cfg = routing_config

    async def get_config(self, config_class, *, catalog_id=None):
        return self._cfg


# ---------------------------------------------------------------------------
# End-to-end pipeline
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pipeline_router_emit_listener_worker_indexer(
    caplog, monkeypatch,
):
    """Exercise the catalog-metadata-changed pipeline end-to-end.

    Stages wired:

    - ``catalog_router.upsert_catalog_metadata`` fans out to
      two stub Primary drivers (domain=core, domain=stac), then emits
      ``catalog_metadata_changed`` through ``emit_event``.
    - ``emit_event`` invokes the registered async listener from
      ``register_reindex_listener``.  (We bypass the EventDrainTask
      because it needs a real ``tasks.events`` row; the listener is
      reachable directly through ``event_service`` when
      ``has_listeners()`` is True.)
    - The listener builds a one-event batch and passes it to
      :class:`ReindexWorker`.
    - The worker resolves indexers via the real async
      :func:`_resolve_catalog_indexers` — which queries our fake
      ``ConfigsProtocol`` and returns :class:`LogCatalogIndexer`
      as the sole WRITE secondary-index entry.
    - ``LogCatalogIndexer.upsert_catalog_metadata`` fires and logs.

    Final assertion: a log line from ``LogCatalogIndexer`` names the
    catalog we mutated.  If any link in the chain breaks, this log
    line is missing.
    """
    from dynastore.modules.catalog import event_service as event_service_mod
    from dynastore.modules.catalog.catalog_router import (
        upsert_catalog_metadata,
    )
    from dynastore.modules.catalog.reindex_listener import (
        register_reindex_listener,
    )
    from dynastore.modules.catalog.reindex_worker import ReindexWorker
    from dynastore.modules.storage.drivers.catalog_log_indexer import (
        LogCatalogIndexer,
    )
    from dynastore.modules.storage.routing_config import (
        CatalogRoutingConfig,
        Operation,
        OperationDriverEntry,
    )

    # ------- Set up a fresh EventService so listener state is isolated
    svc = event_service_mod.EventService()
    monkeypatch.setattr(event_service_mod, "event_service", svc)
    monkeypatch.setattr(event_service_mod, "emit_event", svc.emit)

    # ------- Wire the reindex listener with a worker that uses the
    #         real async _resolve_catalog_indexers
    log_indexer = LogCatalogIndexer()
    _base_ops = CatalogRoutingConfig().operations
    routing_cfg = CatalogRoutingConfig(
        operations={
            **_base_ops,
            # Secondary-index sink merged into the WRITE list (#990).
            Operation.WRITE: list(_base_ops.get(Operation.WRITE, [])) + [
                OperationDriverEntry(
                    driver_ref="LogCatalogIndexer", secondary_index=True,
                ),
            ],
        },
    )
    fake_configs = _FakeConfigsProtocol(routing_cfg)

    # Mock get_protocol so ConfigsProtocol returns our fake + the
    # indexer discovery finds LogCatalogIndexer.
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocol",
        lambda proto: fake_configs,
    )
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocols",
        lambda proto: [log_indexer],
    )

    # Inject a hydration callable that returns a non-None envelope
    # so the worker dispatches to ``upsert_catalog_metadata``.  In
    # production the merged envelope from the PG Primary drivers
    # serves this role; in the test we short-circuit because only
    # ``LogCatalogIndexer`` is registered via ``get_protocols``.
    hydrate = AsyncMock(return_value={
        "title": {"en": "Integration test catalog"},
        "stac_version": "1.1.0",
    })
    worker = ReindexWorker(get_catalog_metadata=hydrate)
    register_reindex_listener(svc, worker=worker)

    # ------- Set up two stub Primary drivers for the WRITE fan-out
    core = MagicMock()
    core.upsert_catalog_metadata = AsyncMock()
    stac = MagicMock()
    stac.upsert_catalog_metadata = AsyncMock()

    # ------- Mutate: the router fans out + emits the event.
    # Explicitly pin the LogCatalogIndexer logger — pytest's caplog
    # otherwise doesn't reliably capture async-task log output unless
    # the per-logger level is set.
    caplog.set_level(
        logging.INFO,
        logger="dynastore.modules.storage.drivers.catalog_log_indexer",
    )
    await upsert_catalog_metadata(
        "cat-integration",
        {
            "title": {"en": "Integration test catalog"},
            "stac_version": "1.1.0",
        },
        drivers=[core, stac],
        # NO db_resource — this forces the in-process async-listener
        # path (``emit`` fires registered listeners directly via
        # ``asyncio.create_task`` rather than persisting to the outbox).
        # A separate test would cover the durable-outbox path.
    )

    # Give the asyncio.create_task a turn to fire the listener.
    import asyncio
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    # ------- Primary fan-out ran.
    core.upsert_catalog_metadata.assert_awaited_once()
    stac.upsert_catalog_metadata.assert_awaited_once()

    # ------- The LogCatalogIndexer saw the catalog_id.
    log_messages = [r.message for r in caplog.records]
    indexer_hits = [m for m in log_messages if "cat-integration" in m and
                    "LogCatalogIndexer" in m]
    assert indexer_hits, (
        "pipeline broke: LogCatalogIndexer was never invoked.  "
        "Check listener registration, payload shape, or indexer "
        f"resolution.  All log messages: {log_messages!r}"
    )


@pytest.mark.asyncio
async def test_pipeline_deletes_route_to_delete_catalog_metadata(
    caplog, monkeypatch,
):
    """Delete path: ``delete_catalog_metadata`` → event → worker → indexer.

    Mirrors the upsert pipeline test but exercises the delete branch
    of ``ReindexWorker._dispatch_one`` which calls
    ``driver.delete_catalog_metadata`` instead of ``upsert_catalog_metadata``.
    """
    from dynastore.modules.catalog import event_service as event_service_mod
    from dynastore.modules.catalog.catalog_router import (
        delete_catalog_metadata,
    )
    from dynastore.modules.catalog.reindex_listener import (
        register_reindex_listener,
    )
    from dynastore.modules.catalog.reindex_worker import ReindexWorker
    from dynastore.modules.storage.drivers.catalog_log_indexer import (
        LogCatalogIndexer,
    )
    from dynastore.modules.storage.routing_config import (
        CatalogRoutingConfig,
        Operation,
        OperationDriverEntry,
    )

    svc = event_service_mod.EventService()
    monkeypatch.setattr(event_service_mod, "event_service", svc)
    monkeypatch.setattr(event_service_mod, "emit_event", svc.emit)

    log_indexer = LogCatalogIndexer()
    _base_ops = CatalogRoutingConfig().operations
    routing_cfg = CatalogRoutingConfig(
        operations={
            **_base_ops,
            # Secondary-index sink merged into the WRITE list (#990).
            Operation.WRITE: list(_base_ops.get(Operation.WRITE, [])) + [
                OperationDriverEntry(
                    driver_ref="LogCatalogIndexer", secondary_index=True,
                ),
            ],
        },
    )
    fake_configs = _FakeConfigsProtocol(routing_cfg)

    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocol",
        lambda proto: fake_configs,
    )
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocols",
        lambda proto: [log_indexer],
    )

    # For the delete path, hydrate return value is irrelevant — the
    # worker's dispatch branch checks ``operation == "soft_delete"``
    # before looking at the envelope.  Still inject a non-None
    # callable to avoid hitting the router discovery.
    worker = ReindexWorker(get_catalog_metadata=AsyncMock(return_value=None))
    register_reindex_listener(svc, worker=worker)

    core = MagicMock()
    core.delete_catalog_metadata = AsyncMock()

    caplog.set_level(
        logging.INFO,
        logger="dynastore.modules.storage.drivers.catalog_log_indexer",
    )
    import asyncio
    await delete_catalog_metadata(
        "cat-to-delete", soft=True, drivers=[core],
    )
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    core.delete_catalog_metadata.assert_awaited_once()

    log_messages = [r.message for r in caplog.records]
    delete_hits = [m for m in log_messages if "cat-to-delete" in m and
                   "LogCatalogIndexer" in m and "delete" in m]
    assert delete_hits, (
        "pipeline broke: delete didn't reach LogCatalogIndexer.  "
        f"All log messages: {log_messages!r}"
    )
