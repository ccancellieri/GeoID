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
ReindexWorker — M3.1 scaffold of the role-based-driver refactor.

Consumes ``catalog_metadata_changed`` events emitted by the catalog-tier
router (M3.0) and dispatches the mutation payload to registered INDEX
drivers (ES Indexers, vector-DB sinks, …).  The worker is the glue
between the authoritative store (catalog.catalog_metadata_core/_stac
written by the Primary drivers) and search-tier mirrors.

Scope of M3.1 (this file)
-------------------------

- **Batch dispatcher**: given a list of catalog_metadata_changed event
  dicts, hydrate each (via ``catalog_metadata_router.get_catalog_metadata``)
  and fan out to every INDEX-role driver configured in
  ``CatalogRoutingConfig.metadata.operations[INDEX]``.
- **Per-driver SLA**: honours ``DriverSla.timeout_ms`` via
  ``asyncio.wait_for``.  On timeout / raise, applies the entry's
  ``on_timeout`` ∈ {"fail", "degrade", "skip"} policy.
- **Best-effort**: worker-level exceptions in one entry don't poison
  the batch.  The caller (outbox consumer loop — M3.1b) decides
  whether a partial-success batch gets ACKed or NACKed via this
  function's return value.

Not in this file yet (follow-up M3.1b)
--------------------------------------

- LISTEN on ``dynastore_events_channel`` / leader-elected consumer
  loop: that path reuses the existing ``EventBusProtocol`` infrastructure
  (see ``modules/events/events_module.py``) and isn't reinvented here.
- Advisory-lock sharding across worker replicas.
- Catch-up / backfill pass that sweeps catalogs whose split-table
  ``updated_at`` ≥ some cursor.
- Per-Indexer TRANSFORM chain (when ``entry.transformed = True``).
  M3.1 only implements ``transformed = False`` — raw Primary envelope.

"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

from dynastore.models.protocols.driver_roles import DriverSla
from dynastore.models.protocols.entity_store import CatalogStore
from dynastore.modules.storage.routing_config import (
    CatalogRoutingConfig, Operation, OperationDriverEntry,
)
from dynastore.tools.typed_store.base import _to_snake

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result model (returned from ``handle_batch`` so the caller decides ACK/NACK)
# ---------------------------------------------------------------------------


class _DispatchResult:
    """Per-event outcome returned by :meth:`ReindexWorker.handle_batch`.

    Caller semantics:

    - ``succeeded`` : every configured Indexer accepted the payload
      (or was skipped per ``on_timeout=skip``).  ACK the event.
    - ``should_retry`` : at least one Indexer failed with a
      ``on_timeout=fail`` policy or raised synchronously.  NACK the
      event — it'll be re-delivered by the outbox.
    - ``event_id`` : passthrough from the outbox row so the caller can
      build the ACK / NACK list without re-parsing.

    A three-state return (not just success/fail) keeps the interface
    explicit: a "degraded" entry is neither a success (the driver
    didn't actually see the data) nor a hard failure (retrying won't
    help — the SLA said to degrade).  It gets logged + ACKed.
    """

    __slots__ = ("event_id", "succeeded", "should_retry", "errors")

    def __init__(
        self,
        *,
        event_id: Optional[str],
        succeeded: bool,
        should_retry: bool,
        errors: List[str],
    ) -> None:
        self.event_id = event_id
        self.succeeded = succeeded
        self.should_retry = should_retry
        self.errors = errors

    def __repr__(self) -> str:
        return (
            f"_DispatchResult(event_id={self.event_id!r}, "
            f"succeeded={self.succeeded}, should_retry={self.should_retry}, "
            f"errors={self.errors!r})"
        )


# ---------------------------------------------------------------------------
# ReindexWorker
# ---------------------------------------------------------------------------


class ReindexWorker:
    """Dispatcher for ``catalog_metadata_changed`` events → INDEX drivers.

    Stateless by design — every method takes the inputs it needs.  The
    only instance state is the injected ``get_catalog_metadata``
    callable, which defaults to the production catalog-tier router's
    function but can be swapped for tests.

    Today only the catalog-tier ``catalog_metadata_changed`` event flows
    through this worker.  A parallel collection-tier event can be wired
    up by adding a ``COLLECTION_METADATA_CHANGED`` emission in
    :mod:`~dynastore.modules.catalog.collection_metadata_router` when
    INDEX propagation is needed there too.
    """

    def __init__(
        self,
        *,
        resolve_indexers: Optional[Callable[..., Any]] = None,
        get_catalog_metadata: Optional[Callable[..., Any]] = None,
    ) -> None:
        """
        Args:
            resolve_indexers: Callable returning
                ``List[Tuple[OperationDriverEntry, CatalogStore]]``.

                May be either SYNC (``() -> List[...]``) or ASYNC
                (``async def resolve(*, catalog_id=None) -> List[...]``).
                :func:`_resolve_catalog_indexers` — the production
                default — is async because it queries
                ``ConfigsProtocol`` through the 4-tier waterfall and
                honours per-catalog INDEX overrides.  Tests that don't
                need the waterfall can inject a simple synchronous
                lambda returning a canned list.

                :meth:`_handle_one` detects the shape via
                ``inspect.iscoroutinefunction`` and awaits when needed.
            get_catalog_metadata: Hydration callable used to fetch the
                current envelope for a mutated catalog.  Defaults to
                the catalog-tier router's ``get_catalog_metadata``.
        """
        self._resolve_indexers = (
            resolve_indexers or _resolve_catalog_indexers
        )
        if get_catalog_metadata is None:
            from dynastore.modules.catalog.catalog_metadata_router import (
                get_catalog_metadata as _default_hydrate,
            )
            self._get_catalog_metadata = _default_hydrate
        else:
            self._get_catalog_metadata = get_catalog_metadata

    # -- Public API ---------------------------------------------------------

    async def handle_batch(
        self,
        events: List[Dict[str, Any]],
        *,
        db_resource: Optional[Any] = None,
    ) -> List[_DispatchResult]:
        """Process a batch of ``catalog_metadata_changed`` events.

        Returns one :class:`_DispatchResult` per input event.  The
        caller (outbox consumer loop) decides which to ACK vs NACK
        based on ``succeeded`` / ``should_retry``.

        Events of other types are skipped with a log entry — they're
        not in this worker's scope.  A future per-event-type router
        can dispatch to different workers.
        """
        results: List[_DispatchResult] = []
        for event in events:
            event_type = event.get("event_type")
            if event_type != "catalog_metadata_changed":
                logger.debug(
                    "ReindexWorker skipping non-catalog-metadata event "
                    "%r (event_id=%s)",
                    event_type, event.get("event_id"),
                )
                # Still ACK so the caller can prune the outbox — this
                # event is nobody's responsibility here.
                results.append(_DispatchResult(
                    event_id=event.get("event_id"),
                    succeeded=True,
                    should_retry=False,
                    errors=[],
                ))
                continue
            results.append(
                await self._handle_one(event, db_resource=db_resource)
            )
        return results

    # -- Internals ----------------------------------------------------------

    async def _handle_one(
        self,
        event: Dict[str, Any],
        *,
        db_resource: Optional[Any],
    ) -> _DispatchResult:
        """Process a single ``catalog_metadata_changed`` event."""
        event_id = event.get("event_id")
        payload = event.get("payload") or {}
        catalog_id = payload.get("catalog_id")
        operation = payload.get("operation", "upsert")

        if not catalog_id:
            logger.warning(
                "ReindexWorker: catalog_metadata_changed event %s has "
                "no catalog_id in payload; skipping", event_id,
            )
            return _DispatchResult(
                event_id=event_id, succeeded=True, should_retry=False,
                errors=["missing catalog_id"],
            )

        indexers: List[Tuple[OperationDriverEntry, CatalogStore]]
        try:
            # ``resolve_indexers`` may be sync (test-injected callable)
            # or async (production — ``_resolve_catalog_indexers`` awaits
            # ``ConfigsProtocol.get_config`` for per-catalog overrides).
            # ``inspect.iscoroutinefunction`` distinguishes the two
            # without requiring callers to always pass coroutines.
            import inspect

            if inspect.iscoroutinefunction(self._resolve_indexers):
                indexers = await self._resolve_indexers(catalog_id=catalog_id)
            else:
                raw = self._resolve_indexers()
                # Sync path returns the list directly; the type-checker
                # can't narrow the Callable[..., Any] in __init__, so
                # cast here.
                indexers = raw  # type: ignore[assignment]
        except Exception as exc:  # noqa: BLE001 — surface as per-event NACK
            logger.warning(
                "ReindexWorker: Indexer resolution failed for %s: %s",
                catalog_id, exc,
            )
            return _DispatchResult(
                event_id=event_id, succeeded=False, should_retry=True,
                errors=[f"indexer resolution failed: {exc}"],
            )

        if not indexers:
            # No INDEX-role drivers configured — the event still
            # deserves an ACK so the outbox doesn't accumulate.  A
            # deploy that later registers Indexers will pick up new
            # events from that point forward; the catch-up backfill
            # (M3.1b) handles anything already ACKed.
            return _DispatchResult(
                event_id=event_id, succeeded=True, should_retry=False,
                errors=[],
            )

        # Hydrate envelope ONCE per event; every Indexer sees the same
        # merged CORE + STAC envelope at this snapshot in time.  A
        # subsequent mutation on the same catalog will produce a new
        # event and a fresh hydration — each Indexer eventually
        # converges to the latest state.
        try:
            envelope = await self._get_catalog_metadata(
                catalog_id, db_resource=db_resource,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "ReindexWorker: hydrate failed for %s: %s — "
                "will retry on next delivery",
                catalog_id, exc,
            )
            return _DispatchResult(
                event_id=event_id, succeeded=False, should_retry=True,
                errors=[f"hydrate failed: {exc}"],
            )

        fatal_errors: List[str] = []
        degraded_errors: List[str] = []
        for entry, driver in indexers:
            # F3 — surface the transformed=True gap: M3.1 does not
            # implement the TRANSFORM chain.  An entry that asks for
            # the transformed envelope silently gets the raw one,
            # which could look correct at unit-test time and then
            # diverge in production.  Log a WARNING per-event so the
            # gap is visible; feeding raw instead of transformed is
            # still the best-available behaviour (ACK the event so
            # the outbox doesn't stall), but the operator needs to
            # know their routing-config directive is not honoured.
            if getattr(entry, "transformed", False):
                logger.warning(
                    "ReindexWorker: CatalogRoutingConfig INDEX entry %r "
                    "requested transformed=True but the TRANSFORM chain "
                    "is not implemented yet (M3.1 only supports raw "
                    "envelopes) — dispatching the raw envelope for "
                    "catalog_id=%s.  Remove ``transformed=True`` from "
                    "the routing config or wait for a future milestone.",
                    entry.driver_id, catalog_id,
                )
            outcome = await self._dispatch_one(
                entry=entry,
                driver=driver,
                catalog_id=catalog_id,
                envelope=envelope,
                operation=operation,
                db_resource=db_resource,
            )
            if outcome is None:
                # succeeded (or skipped per SLA)
                continue
            is_fatal, message = outcome
            if is_fatal:
                fatal_errors.append(message)
            else:
                degraded_errors.append(message)

        if fatal_errors:
            return _DispatchResult(
                event_id=event_id, succeeded=False, should_retry=True,
                errors=fatal_errors + degraded_errors,
            )
        # No fatal errors — ACK even if some Indexers degraded.
        return _DispatchResult(
            event_id=event_id, succeeded=True, should_retry=False,
            errors=degraded_errors,
        )

    async def _dispatch_one(
        self,
        *,
        entry: OperationDriverEntry,
        driver: CatalogStore,
        catalog_id: str,
        envelope: Optional[Dict[str, Any]],
        operation: str,
        db_resource: Optional[Any],
    ) -> Optional[Tuple[bool, str]]:
        """Invoke ``driver.upsert_catalog_metadata`` honouring ``entry.sla``.

        Returns ``None`` on success or SLA-permitted skip.  Returns
        ``(is_fatal, message)`` on failure:

        - ``is_fatal=True``  → caller should NACK the event (retry).
        - ``is_fatal=False`` → caller should ACK with degraded errors
          recorded (retry won't help — the SLA said to degrade).
        """
        sla = _resolve_entry_sla(entry, driver)
        timeout_s = (sla.timeout_ms / 1000.0) if sla else None

        async def _invoke() -> None:
            if operation in ("delete", "soft_delete"):
                await driver.delete_catalog_metadata(
                    catalog_id,
                    soft=(operation == "soft_delete"),
                    db_resource=db_resource,
                )
            else:  # upsert / unknown-future → default to upsert
                if envelope is None:
                    # Upsert with no envelope is nonsensical — the
                    # Primary store has the catalog gone.  Treat as
                    # a delete so the Indexer stays in sync.
                    await driver.delete_catalog_metadata(
                        catalog_id, soft=False, db_resource=db_resource,
                    )
                else:
                    await driver.upsert_catalog_metadata(
                        catalog_id, envelope, db_resource=db_resource,
                    )

        try:
            if timeout_s is not None:
                await asyncio.wait_for(_invoke(), timeout=timeout_s)
            else:
                await _invoke()
            return None
        except asyncio.TimeoutError:
            return _apply_sla_policy(
                sla=sla,
                driver_id=entry.driver_id,
                catalog_id=catalog_id,
                reason=f"timeout after {sla.timeout_ms if sla else '?'}ms",
            )
        except Exception as exc:  # noqa: BLE001 — Indexer-side failure
            return _apply_sla_policy(
                sla=sla,
                driver_id=entry.driver_id,
                catalog_id=catalog_id,
                reason=f"driver exception: {exc}",
            )


# ---------------------------------------------------------------------------
# SLA / policy helpers
# ---------------------------------------------------------------------------


def _resolve_entry_sla(
    entry: OperationDriverEntry,
    driver: CatalogStore,
) -> Optional[DriverSla]:
    """Pick the effective SLA: per-entry override, then class default."""
    if entry.sla is not None:
        return entry.sla
    return getattr(driver, "sla", None)


def _apply_sla_policy(
    *,
    sla: Optional[DriverSla],
    driver_id: str,
    catalog_id: str,
    reason: str,
) -> Optional[Tuple[bool, str]]:
    """Translate SLA ``on_timeout`` into (is_fatal, message) or None.

    - ``skip``    → log debug, return None (treated as success).
    - ``degrade`` → log warning, return ``(False, message)``.  Caller
      ACKs the event but records the degradation.
    - ``fail``    → log error, return ``(True, message)``.  Caller
      NACKs so the event gets re-delivered.
    - No SLA on entry or class → default to ``fail`` (safe default —
      a silent drop would mask consumer-level failures).
    """
    policy = sla.on_timeout if sla else "fail"
    message = f"Indexer {driver_id}@{catalog_id}: {reason}"
    if policy == "skip":
        logger.debug("%s — SLA says skip", message)
        return None
    if policy == "degrade":
        logger.warning("%s — SLA says degrade; ACKing anyway", message)
        return (False, message)
    logger.error("%s — SLA says fail; NACKing for retry", message)
    return (True, message)


# ---------------------------------------------------------------------------
# Indexer resolution — reads from CatalogRoutingConfig's INDEX operation
# ---------------------------------------------------------------------------


async def _resolve_catalog_indexers(
    *,
    catalog_id: Optional[str] = None,
) -> List[Tuple[OperationDriverEntry, CatalogStore]]:
    """Return the (entry, driver) pairs configured under INDEX for catalogs.

    Resolution order mirrors the collection-tier router
    (:mod:`dynastore.modules.catalog.collection_metadata_router`):

    1. If ``ConfigsProtocol`` is registered, read
       ``CatalogRoutingConfig`` for the given ``catalog_id`` through the
       4-tier waterfall (collection > catalog > platform > code).  This
       is the path operators use to add / override INDEX entries at
       runtime.
    2. On lookup failure (``ConfigsProtocol`` unavailable, config-service
       down, catalog_id absent), fall back to the code-level default
       :class:`CatalogRoutingConfig()` — which today carries no INDEX
       entries, so the worker becomes a no-op rather than crashing.

    Filters to driver_ids that are actually registered via
    ``get_protocols(CatalogStore)``; unregistered entries are
    logged and dropped.
    """
    from dynastore.tools.discovery import get_protocol, get_protocols

    entries: List[OperationDriverEntry] = []
    try:
        from dynastore.models.protocols.configs import ConfigsProtocol

        configs = get_protocol(ConfigsProtocol)
        if configs is not None:
            routing_config = await configs.get_config(
                CatalogRoutingConfig, catalog_id=catalog_id,
            )
            entries = list(
                routing_config.operations.get(Operation.INDEX, [])
            )
    except Exception as exc:  # noqa: BLE001 — diagnostic fallback
        logger.debug(
            "CatalogRoutingConfig platform-override lookup failed for "
            "catalog_id=%r: %s — falling back to code-level default",
            catalog_id, exc,
        )

    if not entries:
        # Fall back to the code-level default — currently empty for INDEX.
        cfg = CatalogRoutingConfig()
        entries = list(cfg.operations.get(Operation.INDEX, []))
    if not entries:
        return []

    driver_index = {
        _to_snake(type(d).__name__): d for d in get_protocols(CatalogStore)
    }
    resolved: List[Tuple[OperationDriverEntry, CatalogStore]] = []
    for entry in entries:
        driver = driver_index.get(entry.driver_id)
        if driver is None:
            logger.warning(
                "CatalogRoutingConfig INDEX entry %r is not a registered "
                "CatalogStore — skipping",
                entry.driver_id,
            )
            continue
        resolved.append((entry, driver))
    return resolved
