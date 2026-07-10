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
Collection-tier metadata router — fan-out across registered drivers.

Mirror of :mod:`catalog_router` at the collection tier.  Every
registered :class:`CollectionStore` driver receives WRITE / DELETE
fan-outs and contributes its slice on READ; the router merges per-domain
dicts into the envelope returned to callers.  Default deployment
registers :class:`CollectionPostgresqlDriver` (the composition wrapper
that fans CRUD across ``collection_core`` + ``collection_stac`` sidecars
internally — PR 1e step 3b); the ES indexer and any future TRANSFORM
contributors slot in alongside without changing call sites.

Each driver's ``upsert_metadata`` filters the incoming payload to its
own domain's columns and no-ops when the filtered slice is empty —
caller passes the full envelope once; domain splitting happens inside
the driver.  Same pattern as the catalog-tier router.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, FrozenSet, List, Literal, Optional, Tuple

from dynastore.models.protocols.entity_store import (
    CollectionStore,
    EntityStoreCapability,
)
from dynastore.modules.storage.hints import Hint
from dynastore.modules.storage.routed_resolver import resolve_routed
from dynastore.modules.storage.routing_config import CollectionRoutingConfig, Operation

logger = logging.getLogger(__name__)

# Per-process latch: log "no drivers registered" once per process, not
# per request.  ES-only / custom deployments that boot briefly without
# PG drivers would otherwise spam the log.
_MISSING_DRIVERS_LOGGED: Dict[str, bool] = {
    "collection": False,
    "collection_write": False,
    "collection_delete": False,
}


def _filter_capable(
    drivers: List[CollectionStore],
    capability: str,
) -> List[CollectionStore]:
    """Keep only drivers declaring ``capability`` — TRANSFORM-only drivers
    (e.g. ``BigQueryMetadataTransformDriver``) must never reach the WRITE
    / DELETE fan-out.  See ``TransformOnlyCollectionStoreMixin`` in
    ``models/protocols/entity_store.py``.

    Load-bearing on the **config-resolved** branch: a pinned
    ``driver_ref`` in ``CollectionRoutingConfig`` may legitimately point
    at a driver that does not declare the operation's capability (mis-
    config, evolving driver, READ-only mirror); without this filter the
    fan-out would raise mid-loop on the first non-capable driver and the
    surviving drivers would never run.  The matching
    ``TransformOnlyCollectionStoreMixin`` raising stubs stay in place as
    a defence-in-depth: anything that slips past this filter still
    fails loudly instead of silently corrupting state.
    """
    kept: List[CollectionStore] = []
    for d in drivers:
        if capability in getattr(d, "capabilities", frozenset()):
            kept.append(d)
        else:
            logger.warning(
                "Driver %s lacks the %s capability — dropped from the "
                "fan-out.  Check the routing config: a pinned driver_ref "
                "must declare the capability for the operation it serves.",
                type(d).__name__, capability,
            )
    return kept


def _resolve_drivers() -> List[CollectionStore]:
    from dynastore.tools.discovery import get_protocols

    drivers = list(get_protocols(CollectionStore))
    if not drivers and not _MISSING_DRIVERS_LOGGED["collection"]:
        logger.error(
            "No CollectionStore drivers registered; collection "
            "router will no-op all operations.  Check that "
            "collection_postgresql imports cleanly and its entry-points "
            "are installed."
        )
        _MISSING_DRIVERS_LOGGED["collection"] = True
    return drivers


async def _routed_drivers(
    operation: str,
    catalog_id: str,
    collection_id: Optional[str],
    *,
    hints: FrozenSet[Hint] = frozenset(),
    db_resource: Optional[Any] = None,
) -> Optional[List[CollectionStore]]:
    """Config-driven driver list for an operation.

    Returns the ordered ``CollectionStore`` instances configured under
    ``CollectionRoutingConfig.operations[operation]``, or ``None`` when the
    routing config could not be consulted (early boot — caller falls back
    to :func:`_resolve_drivers` discovery).  When ``hints`` is non-empty
    the list is pre-filtered by hint overlap (see ``routed_resolver``).
    """
    resolved = await resolve_routed(
        CollectionRoutingConfig, operation, catalog_id, collection_id,
        hints=hints,
        db_resource=db_resource,
    )
    if not resolved:
        return None
    return [driver for _entry, driver in resolved]


def _get_index_dispatcher() -> Any:
    """Indirection seam — lets tests substitute a fake dispatcher."""
    from dynastore.modules.storage.index_dispatcher import get_index_dispatcher
    return get_index_dispatcher()


async def _dispatch_collection_index(
    catalog_id: str,
    collection_id: str,
    metadata: Optional[Dict[str, Any]] = None,
    *,
    op_type: Literal["upsert", "delete"] = "upsert",
    db_resource: Optional[Any] = None,
    lifecycle_status: Optional[str] = None,
) -> None:
    """Fan a collection ``upsert`` / ``delete`` to every Indexer configured
    in ``CollectionRoutingConfig.operations[INDEX]``.

    Mirrors :meth:`item_service.ItemService._dispatch_index_upsert` at the
    collection-envelope tier — the single dispatch call site that replaces
    per-driver ES event listeners.  ``op_type`` selects the verb: an
    ``upsert`` carries the metadata envelope, a ``delete`` carries no
    payload (``metadata`` is ``None``) and asks the Indexer to drop the
    document so a deleted collection does not linger in
    ``dynastore-collections`` until the next full reindex.

    INDEX entries carry no per-entry failure policy: a dispatch failure is
    always absorbed here — the PG mutation has already committed/queued
    and must stand; ES may be stale until the next reindex or drain pass.

    Asymmetry note: there is no ``_dispatch_catalog_index`` analogue in
    ``catalog_router``.  Catalog secondary indexing is event-driven —
    ``catalog_metadata_changed`` is emitted inside the WRITE transaction
    and ``ReindexWorker`` fans it out to ``CatalogRoutingConfig``'s
    INDEX-lane entries.  Same durability plumbing, different trigger.  See
    the "Catalog secondary-index WRITE hop" section in ``catalog_router``'s
    module docstring.
    """
    from dynastore.models.protocols.indexer import IndexContext, IndexOp
    from dynastore.tools.correlation import get_correlation_id

    dispatcher = _get_index_dispatcher()
    ops = [
        IndexOp(
            op_type=op_type,
            entity_type="collection",
            entity_id=collection_id,
            payload=metadata,
        )
    ]
    ctx = IndexContext(
        catalog=catalog_id,
        collection=collection_id,
        correlation_id=get_correlation_id() or "",
        pg_conn=db_resource,
        entity_type="collection",
        lifecycle_status=lifecycle_status,
    )
    try:
        await dispatcher.fan_out_bulk(ctx, ops)
    except Exception as exc:  # noqa: BLE001 — index dispatch never blocks a write
        logger.warning(
            "Collection secondary-index hop %s dispatch failed for %s/%s: "
            "%s — PG mutation stands; ES may be stale until reindex",
            op_type, catalog_id, collection_id, exc,
        )


async def get_collection_metadata(
    catalog_id: str,
    collection_id: str,
    *,
    hints: FrozenSet[Hint] = frozenset(),
    context: Optional[Dict[str, Any]] = None,
    db_resource: Optional[Any] = None,
    drivers: Optional[List[CollectionStore]] = None,
) -> Optional[Dict[str, Any]]:
    """Merge every registered driver's READ slice into a single envelope.

    **Dispatch semantics depend on whether hints are supplied.**

    *No hints (empty frozenset):* existing merge-all behaviour — every driver
    in the resolved list contributes its domain slice; the envelopes are
    merged with ``dict.update`` (last-driver wins on duplicate keys).  This
    is the default path and is preserved byte-identical.

    *Non-empty hints:* first-non-None semantics — the hint-filtered ordered
    driver list is iterated sequentially; the first driver returning a
    truthy (non-None) result wins and is returned immediately.  An ES miss
    (None/empty) causes the iterator to advance to the next driver (PG),
    ensuring PG always answers if ES has no indexed copy.  This is the
    correct contract for per-request geometry-precision routing: a caller
    asking for geometry_simplified on a collection not yet indexed in ES
    should still get data (from PG), not a 404.

    No preset configures more than one collection READ entry today, so the
    merge-all / first-non-None distinction is only observable when a
    deployment has two READ drivers explicitly configured.  Merge-all for
    the no-hint default keeps that theoretical multi-driver preset working
    without modification.

    **Sequential fan-out (load-bearing).**  Earlier versions used
    ``asyncio.gather`` here, but when the caller passes a shared
    ``db_resource`` (a live asyncpg ``Connection``), concurrent driver
    SELECTs race on the single wire and asyncpg deadlocks — the hang
    manifests as pytest's event loop stuck in ``selectors.kqueue.control``
    waiting for the connection to respond.  Same hazard already fixed on
    ``list_catalogs`` (PR #28) and ``get_catalog_metadata`` (PR #32) —
    this is the third occurrence on the symmetric collection-scope path.
    Per-driver latency is additive (~1-2ms each) but dominated by the
    round-trip anyway.  A future refactor that hands each driver its own
    pooled connection can re-enable ``gather`` at that point.
    """
    if drivers is None:
        routed = await _routed_drivers(
            Operation.READ, catalog_id, collection_id,
            hints=hints,
            db_resource=db_resource,
        )
        # READ deliberately does NOT run drivers through ``_filter_capable``.
        # A pinned ``driver_ref`` lacking READ is invoked anyway, because
        # ``_safe_get`` below is forgiving: a driver that doesn't actually
        # serve a READ returns ``None`` (or raises) and the next driver in
        # the merge pipeline supplies the envelope.  Filtering would mask
        # partial-coverage drivers that DO answer for some keys.
        drivers = routed if routed is not None else _resolve_drivers()
    if not drivers:
        return None

    async def _safe_get(d: CollectionStore) -> Optional[Dict[str, Any]]:
        try:
            return await d.get_metadata(
                catalog_id, collection_id,
                context=context, db_resource=db_resource,
            )
        except Exception as exc:  # noqa: BLE001 — degrade, don't fail
            logger.warning(
                "Collection-metadata READ failed via %s for %s/%s: %s — "
                "omitting slice from merged envelope",
                type(d).__name__, catalog_id, collection_id, exc,
            )
            return None

    # Sequential to avoid asyncpg single-wire deadlock when db_resource
    # is a shared Connection (see docstring).
    if hints:
        # Hinted path: first-non-None wins (ES → PG fallback chain).
        for d in drivers:
            result = await _safe_get(d)
            if result:
                return result
        return None

    results: List[Optional[Dict[str, Any]]] = []
    for d in drivers:
        results.append(await _safe_get(d))

    merged: Dict[str, Any] = {}
    any_found = False
    for result in results:
        if result is None:
            continue
        any_found = True
        merged.update(result)
    return merged if any_found else None


async def upsert_collection_metadata(
    catalog_id: str,
    collection_id: str,
    metadata: Dict[str, Any],
    *,
    db_resource: Optional[Any] = None,
    drivers: Optional[List[CollectionStore]] = None,
    lifecycle_status: Optional[str] = None,
) -> None:
    """Fan-out WRITE across every registered driver (sequential, fail-fast).

    When ``db_resource`` is a shared connection every driver participates
    in the same transaction — a failure in any driver rolls back all
    preceding writes.  When ``db_resource is None`` each driver opens
    its own connection; in that case a later-driver failure leaves the
    earlier drivers committed (partial-write).  Callers that need
    all-or-nothing semantics MUST pass a shared ``db_resource``.

    On failure, logs which driver raised and re-raises.  No silent
    suppression — callers at the service layer decide whether the write
    is fatal to their request.

    Only drivers declaring ``EntityStoreCapability.WRITE`` participate in
    the fan-out.  TRANSFORM-only drivers never receive ``upsert_metadata``
    — they'd raise ``NotImplementedError`` from the mixin stub.
    """
    if drivers is None:
        routed = await _routed_drivers(
            Operation.WRITE, catalog_id, collection_id, db_resource=db_resource,
        )
        if routed is not None:
            # Parity with the discovery branch: a config-pinned driver that
            # does not declare WRITE must be dropped, not invoked — invoking
            # it would raise mid-fan-out instead of degrading cleanly.
            drivers = _filter_capable(routed, EntityStoreCapability.WRITE)
        else:
            drivers = _filter_capable(_resolve_drivers(), EntityStoreCapability.WRITE)
    if not drivers:
        if not _MISSING_DRIVERS_LOGGED["collection_write"]:
            logger.warning(
                "No WRITE-capable CollectionStore drivers "
                "registered; upsert_collection_metadata is a no-op."
            )
            _MISSING_DRIVERS_LOGGED["collection_write"] = True
        return
    for driver in drivers:
        try:
            await driver.upsert_metadata(
                catalog_id, collection_id, metadata, db_resource=db_resource,
            )
        except Exception:
            logger.error(
                "Collection-metadata WRITE failed via %s for %s/%s — "
                "aborting fan-out (remaining drivers: %s)",
                type(driver).__name__, catalog_id, collection_id,
                [type(d).__name__ for d in drivers[drivers.index(driver) + 1:]],
            )
            raise

    # secondary-index WRITE hop — propagate the envelope to ES (and any
    # other configured Indexer).  Pure post-write propagation; PG above is
    # the system of record.  db_resource (when a live conn) makes the OUTBOX enqueue
    # atomic with the caller's transaction.  lifecycle_status (when non-None)
    # is threaded into the IndexContext so the ES driver can stamp it on the
    # system container; PG drivers above are NOT passed lifecycle_status —
    # they manage it via a dedicated column.
    await _dispatch_collection_index(
        catalog_id, collection_id, metadata,
        db_resource=db_resource,
        lifecycle_status=lifecycle_status,
    )


async def delete_collection_metadata(
    catalog_id: str,
    collection_id: str,
    *,
    soft: bool = False,
    db_resource: Optional[Any] = None,
    drivers: Optional[List[CollectionStore]] = None,
) -> None:
    """Fan-out DELETE across every registered driver (best-effort).

    Attempts every driver even if an earlier one fails — partial
    deletes are recoverable (idempotent re-delete), so observability
    wins over fail-fast here.  If any driver raised, re-raises the
    first exception after every driver has been attempted.

    Only drivers declaring ``EntityStoreCapability.WRITE`` participate in
    the fan-out (no separate ``DELETE`` capability exists).  TRANSFORM-
    only drivers never receive ``delete_metadata``.
    """
    if drivers is None:
        routed = await _routed_drivers(
            Operation.WRITE, catalog_id, collection_id, db_resource=db_resource,
        )
        if routed is not None:
            # Parity with the discovery branch — see upsert_collection_metadata.
            drivers = _filter_capable(routed, EntityStoreCapability.WRITE)
        else:
            drivers = _filter_capable(_resolve_drivers(), EntityStoreCapability.WRITE)
    if not drivers:
        if not _MISSING_DRIVERS_LOGGED["collection_delete"]:
            logger.warning(
                "No WRITE-capable CollectionStore drivers "
                "registered; delete_collection_metadata is a no-op."
            )
            _MISSING_DRIVERS_LOGGED["collection_delete"] = True
        return
    first_error: Optional[BaseException] = None
    for driver in drivers:
        try:
            await driver.delete_metadata(
                catalog_id, collection_id,
                soft=soft, db_resource=db_resource,
            )
        except Exception as exc:
            logger.error(
                "Collection-metadata DELETE failed via %s for %s/%s: %s",
                type(driver).__name__, catalog_id, collection_id, exc,
            )
            if first_error is None:
                first_error = exc

    if first_error is None:
        # secondary-index WRITE hop — propagate the delete so ES drops the document too.
        # Mirrors the upsert path; fires only on a clean fan-out, because a
        # partial-failure delete left the row in PG (system of record) and
        # ES must keep its copy until a clean retry.
        await _dispatch_collection_index(
            catalog_id, collection_id, op_type="delete", db_resource=db_resource,
        )
    else:
        raise first_error


async def _clear_collection_es_lifecycle_status(
    catalog_id: str,
    collection_id: str,
) -> None:
    """Targeted partial-update: remove ``system.lifecycle_status`` from the
    collection's ES document so it becomes visible in q-based searches.

    Discovers all registered ``CollectionStore`` drivers that expose a
    ``clear_lifecycle_status`` method (i.e. ES-backed drivers) and calls them
    in sequence.  Errors are re-raised after all drivers have been attempted
    so the caller can wrap the whole call in best-effort handling.  The PG
    system-of-record flip is independent of this call.
    """
    drivers = _resolve_drivers()
    first_error: Optional[BaseException] = None
    for driver in drivers:
        clear_fn = getattr(driver, "clear_lifecycle_status", None)
        if clear_fn is None:
            continue
        try:
            await clear_fn(catalog_id, collection_id)
        except Exception as exc:
            logger.warning(
                "_clear_collection_es_lifecycle_status: driver %s failed for "
                "%s/%s: %s",
                type(driver).__name__, catalog_id, collection_id, exc,
            )
            if first_error is None:
                first_error = exc
    if first_error is not None:
        raise first_error


async def search_collection_metadata(
    catalog_id: str,
    *,
    q: Optional[str] = None,
    bbox: Optional[List[float]] = None,
    datetime_range: Optional[str] = None,
    filter_cql: Optional[Dict[str, Any]] = None,
    limit: int = 100,
    offset: int = 0,
    context: Optional[Dict[str, Any]] = None,
    db_resource: Optional[Any] = None,
    drivers: Optional[List[CollectionStore]] = None,
    operation: str = Operation.INDEX,
) -> Tuple[List[Dict[str, Any]], int]:
    """Delegate search to the first driver capable of serving the query shape.

    ``operation`` selects which routing lane supplies the candidate driver
    list (default ``INDEX`` — the search-capable materialization lane).
    Callers pass ``Operation.READ`` to run the same capability-matched
    delegation against the READ-routed drivers — the routing-driven
    fallback the collection service uses when the INDEX lane is ES-only
    and the ES collection index has no rows for the catalog yet (READ is
    PG-backed under every preset, so it always answers from the system of
    record).
    """
    if drivers is None:
        routed = await _routed_drivers(
            operation, catalog_id, collection_id=None, db_resource=db_resource,
        )
        # SEARCH deliberately does NOT pre-filter via ``_filter_capable``:
        # the per-shape capability match below (``required.issubset(caps)``
        # → first hit; partial match → fallback; finally ``drivers[0]``) is
        # richer than a boolean drop and picks the most capable driver for
        # the query shape rather than dropping any candidate up front.
        drivers = routed if routed is not None else _resolve_drivers()
    if not drivers:
        return [], 0

    required: set[str] = set()
    if q is not None:
        required.add(EntityStoreCapability.SEARCH)
    if bbox is not None:
        required.add(EntityStoreCapability.SPATIAL_FILTER)
    if filter_cql is not None:
        required.add(EntityStoreCapability.CQL_FILTER)

    chosen = None
    for driver in drivers:
        caps = getattr(driver, "capabilities", frozenset())
        if required and required.issubset(caps):
            chosen = driver
            break
    if chosen is None:
        for driver in drivers:
            caps = getattr(driver, "capabilities", frozenset())
            if required & caps:
                chosen = driver
                break
    if chosen is None:
        chosen = drivers[0]

    try:
        return await chosen.search_metadata(
            catalog_id,
            q=q, bbox=bbox, datetime_range=datetime_range,
            filter_cql=filter_cql, limit=limit, offset=offset,
            context=context, db_resource=db_resource,
        )
    except Exception as exc:
        logger.warning(
            "Collection-metadata SEARCH failed on %s via %s: %s",
            catalog_id, type(chosen).__name__, exc,
        )
        return [], 0
