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

"""
Async-event-listener adapter for :class:`ReindexWorker` (M3.1b production wiring).

Registers :func:`handle_catalog_metadata_changed` via
``@async_event_listener("catalog_metadata_changed")`` so
:class:`event_service.EventService._consume_shard` — the 16-shard
consumer CatalogModule starts automatically whenever any async
listener is registered — invokes it per event.  The listener's
raise-or-return signals NACK-or-ACK back to the shared outbox
claim machinery: raise → NACK + re-deliver, return → ACK.

Why reuse the shared consumer instead of building a dedicated loop
------------------------------------------------------------------

A second consumer running on the same ``PLATFORM`` scope would
``SKIP LOCKED``-claim from the same ``catalog_metadata_changed``
rows — two consumers interleaving against one outbox is a race we
do not want and have no use for at this scale.  Reusing the 16-shard
consumer sidesteps it entirely.

Wire-up (in :meth:`CatalogModule.__aenter__`)::

    from dynastore.modules.catalog.reindex_listener import (
        register_reindex_listener,
    )
    register_reindex_listener(self.event_service)

Registration is idempotent — calling twice registers two listeners
and events fire through both (harmless but wasteful); the helper
guards against that by tagging the bound function.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional

from dynastore.modules.catalog.reindex_worker import ReindexWorker

logger = logging.getLogger(__name__)


# Canonical event-type string emitted by
# :func:`catalog_router._emit_catalog_metadata_changed`
# (``CatalogEventType.CATALOG_METADATA_CHANGED.value``).  Kept as a
# string here to avoid an import cycle between ``reindex_listener``
# and ``event_service``.
CATALOG_METADATA_CHANGED_EVENT = "catalog_metadata_changed"

_REGISTERED_MARKER = "_dynastore_reindex_listener_registered"


async def handle_catalog_metadata_changed(
    worker: ReindexWorker,
    **kwargs: Any,
) -> None:
    """Dispatch one ``catalog_metadata_changed`` event through ``worker``.

    Shape contract with ``_consume_shard``
    --------------------------------------

    ``event_service.EventService._consume_shard`` invokes registered
    listeners with ``payload["args"]`` as positional args and
    ``payload["kwargs"]`` as keyword args.  The router emits:

    .. code-block:: python

        emit_event(
            CatalogEventType.CATALOG_METADATA_CHANGED,
            catalog_id=catalog_id,
            db_resource=db_resource,
            payload={"catalog_id": ..., "domain": ..., "operation": ...},
        )

    so the listener receives ``catalog_id=..., payload={...}`` as
    kwargs (``db_resource`` is consumed by ``emit`` itself and never
    reaches the payload).  We extract the inner ``payload`` dict —
    that's the shape the worker's ``_handle_one`` expects — and
    build a one-event batch for :meth:`ReindexWorker.handle_batch`.

    ACK/NACK translation
    --------------------

    ``_consume_shard`` wraps each listener call in a try/except:
    return ⇒ ACK, raise ⇒ NACK.  We therefore translate
    :class:`_DispatchResult` back into an exception when any result
    signals ``should_retry=True`` — joining the per-driver error
    messages so the NACK row records exactly what failed.

    Degraded results (``succeeded=True`` with recorded errors from
    an SLA ``degrade`` / ``skip`` branch) do NOT raise — the SLA
    explicitly said "don't retry this, just log".  The errors are
    re-emitted at WARNING level so alerting still fires.

    ``event_id`` passed into the worker is ``None`` because the real
    outbox id lives one layer up in ``_consume_shard``'s local scope
    and doesn't flow through the listener API.  The worker's
    per-result ``event_id`` is therefore informational-only in this
    wiring; ACK/NACK correctness is the shared-consumer's
    responsibility.
    """
    payload = kwargs.get("payload")
    if not isinstance(payload, dict):
        # Defensive: if emission ever changes shape, fail loud at the
        # boundary rather than silently dispatching a nonsense payload.
        raise RuntimeError(
            f"handle_catalog_metadata_changed: missing or non-dict "
            f"'payload' in kwargs (got keys: {sorted(kwargs.keys())})"
        )

    event: Dict[str, Any] = {
        "event_type": CATALOG_METADATA_CHANGED_EVENT,
        "event_id": None,  # see docstring
        "payload": payload,
    }

    results = await worker.handle_batch([event])

    # Translate the three-state outcome into raise-or-return.
    retry_errors: List[str] = []
    degraded_errors: List[str] = []
    for r in results:
        if not r.succeeded and r.should_retry:
            retry_errors.extend(r.errors or ["reindex dispatch failed"])
        elif r.succeeded and r.errors:
            degraded_errors.extend(r.errors)

    if degraded_errors:
        logger.warning(
            "ReindexListener: catalog_metadata_changed dispatched with "
            "degraded errors (catalog_id=%s, errors=%r)",
            payload.get("catalog_id"), degraded_errors,
        )

    if retry_errors:
        # _consume_shard catches this + calls driver.nack(event_id,
        # error=str(e)).  Join errors so the NACK row records every
        # per-driver failure, not just the first.
        raise RuntimeError("; ".join(retry_errors))


def register_reindex_listener(
    event_service: Any,
    *,
    worker: Optional[ReindexWorker] = None,
) -> Callable[..., Any]:
    """Register :func:`handle_catalog_metadata_changed` on ``event_service``.

    Idempotent: a second call is a no-op and returns the previously-
    registered bound callable.  Guarded via a private attribute on
    the event_service instance so reload cycles (module re-init in
    tests, hot-restart in dev) don't accumulate duplicate listeners.

    Returns the bound listener for tests that want to invoke it
    directly or assert on its identity.

    Why ``event_service`` is passed in rather than imported
    -------------------------------------------------------
    Avoids an import cycle — ``event_service.py`` does not import
    from this module at load time; this module imports
    ``ReindexWorker`` but not ``event_service``.  The caller
    (``CatalogModule.__aenter__``) already has ``self.event_service``
    so the injection is cheap.
    """
    existing = getattr(event_service, _REGISTERED_MARKER, None)
    if existing is not None:
        logger.debug(
            "ReindexListener: catalog_metadata_changed listener already "
            "registered on this EventService instance — skipping "
            "duplicate registration",
        )
        return existing

    effective_worker = worker if worker is not None else ReindexWorker()

    async def _listener(**kwargs: Any) -> None:
        await handle_catalog_metadata_changed(effective_worker, **kwargs)

    event_service.async_event_listener(CATALOG_METADATA_CHANGED_EVENT)(_listener)
    setattr(event_service, _REGISTERED_MARKER, _listener)
    logger.info(
        "ReindexListener: registered listener for %r on EventService",
        CATALOG_METADATA_CHANGED_EVENT,
    )
    return _listener
