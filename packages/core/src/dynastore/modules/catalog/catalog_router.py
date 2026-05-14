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
Catalog-tier metadata router.

Parallels :mod:`dynastore.modules.catalog.collection_router`
but scoped to :class:`CatalogStore` implementations.
Three operations:

- :func:`get_catalog_metadata`      — fan-out READ that merges CORE +
  STAC envelopes into a single dict; later domains slot in here
  without changes at the call site.
- :func:`upsert_catalog_metadata`   — fan-out WRITE that hands the same
  payload to every configured driver (each driver ``_filter_payload``s
  down to its own domain's columns).
- :func:`delete_catalog_metadata`   — fan-out DELETE.

Resolution policy
-----------------

Resolves drivers by scanning every registered
:class:`CatalogStore` via ``get_protocols()``.  This discovery-
only path is deliberate for the WRITE / READ / DELETE operations
exposed here: the M2.3a backfill assumes both CORE and STAC tables
are always populated for catalogs that need them, and fan-out across
every discovered driver satisfies that assumption regardless of
per-catalog routing config.  Callers that need a filtered subset
(e.g. dry-run tooling) inject ``drivers=`` explicitly.

Per-catalog routing-config overrides live on a different code path:
:func:`dynastore.modules.catalog.reindex_worker._resolve_catalog_indexers`
queries ``CatalogRoutingConfig`` through the ``ConfigsProtocol`` 4-tier
waterfall for INDEX entries.  The WRITE / READ / DELETE router
deliberately does NOT honour per-catalog overrides — routing-config
overrides of the canonical Primary store would split writes across
drivers with inconsistent schemas, which is explicitly not supported.

Failure policy
--------------

Driver-level failures bubble up on WRITE / DELETE (data correctness
trumps availability).  On READ, a single domain's missing envelope is
treated as "this catalog has no data in that domain" — the merged dict
simply skips the absent keys.  A domain driver that raises on READ is
logged at WARNING and omitted from the merge; the caller sees a
partial envelope rather than a 5xx.  This matches the pre-refactor
behaviour where a legacy catalog with NULL STAC columns produced an
envelope missing those keys.

Transaction-scope contract (load-bearing)
-----------------------------------------

When a caller passes ``db_resource`` (an already-acquired live
``AsyncConnection``), **every** driver call in this module and every
downstream event emission must observe that same connection — no
driver may silently acquire a fresh pooled connection.  Correctness
hinges on the router's write + ``catalog_metadata_changed`` event
write landing inside the caller's transaction so the transactional-
outbox guarantee holds: if the outer transaction rolls back, the
events disappear with it.

The re-entrancy that makes this work lives in
:func:`dynastore.modules.db_config.query_executor.managed_transaction`:
when handed a live connection already ``in_transaction()``, it opens a
``SAVEPOINT`` via ``begin_nested()`` instead of a new ``BEGIN``.  The
per-domain Primary drivers follow the canonical pattern
``engine = db_resource or _get_engine()`` → ``managed_transaction(engine)``;
passing the caller's connection as ``engine`` takes the SAVEPOINT
branch.  Regressing any of those two invariants — drivers resolving
``_get_engine()`` when they shouldn't, or ``managed_transaction``
losing re-entrancy — silently detaches router writes from the caller's
transaction and breaks the outbox contract.  The router tests pin the
connection-identity propagation
(see ``test_catalog_router.py`` —
``test_upsert_fan_out_shares_db_resource`` and siblings) as the
fuse against that regression.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from dynastore.models.protocols.entity_store import (
    CatalogStore,
    EntityStoreCapability,
)
from dynastore.modules.storage.routed_resolver import resolve_routed
from dynastore.modules.storage.routing_config import CatalogRoutingConfig, Operation

logger = logging.getLogger(__name__)

# Per-process latch so the "no drivers registered" ERROR fires once,
# not on every request.  A deployment that resolves this condition by
# registering a driver later (entry-point reload, dynamic plugin) will
# get subsequent successful resolutions without the log re-firing.
_MISSING_DRIVERS_LOGGED: Dict[str, bool] = {
    "catalog": False,
    "catalog_write": False,
    "catalog_delete": False,
}


def _filter_capable(
    drivers: List[CatalogStore],
    capability: str,
) -> List[CatalogStore]:
    """Keep only drivers declaring ``capability`` — TRANSFORM-only drivers
    (e.g. ``BigQueryMetadataTransformDriver``) must never reach the WRITE
    / DELETE fan-out.  See ``TransformOnlyCatalogStoreMixin`` in
    ``models/protocols/entity_store.py`` — its raising stubs are a
    bug-catcher; routers MUST honour the capability contract before
    invocation.
    """
    return [d for d in drivers
            if capability in getattr(d, "capabilities", frozenset())]


def _resolve_catalog_store_drivers() -> List[CatalogStore]:
    """Return the live :class:`CatalogStore` drivers registered via entry-points.

    Delegates to :func:`dynastore.tools.discovery.get_protocols` so the
    router picks up any driver module loaded by the current scope —
    PG Primaries in the default deployment, an ES indexer if the
    ``elasticsearch`` scope is loaded, etc.  Drivers the entry-point
    layer hasn't loaded are silently absent — no import-time coupling
    between this router and the backends it orchestrates.
    """
    from dynastore.tools.discovery import get_protocols

    drivers = list(get_protocols(CatalogStore))
    if not drivers and not _MISSING_DRIVERS_LOGGED["catalog"]:
        # Log once per process rather than per request to avoid spamming
        # when a deployment legitimately starts up without PG metadata
        # drivers (e.g. ES-only).  A router invocation is proof that a
        # caller expected at least one driver, so ERROR is appropriate —
        # but one ERROR is enough.
        logger.error(
            "No CatalogStore drivers registered; catalog "
            "router will no-op all operations.  Entry-point discovery did "
            "not surface any dynastore.modules entry-point implementing "
            "CatalogStore — check installed package metadata and "
            "that catalog_postgresql imports cleanly in this env."
        )
        _MISSING_DRIVERS_LOGGED["catalog"] = True
    return drivers


async def _routed_catalog_drivers(
    operation: str,
    catalog_id: str,
    *,
    db_resource: Optional[Any] = None,
) -> Optional[List[CatalogStore]]:
    """Config-driven CatalogStore list for an operation.

    Returns the ordered :class:`CatalogStore` instances configured under
    ``CatalogRoutingConfig.operations[operation]``, or ``None`` when the
    routing config could not be consulted (early boot — caller falls back
    to :func:`_resolve_catalog_store_drivers` discovery).
    """
    resolved = await resolve_routed(
        CatalogRoutingConfig, operation, catalog_id, collection_id=None,
        db_resource=db_resource,
    )
    if not resolved:
        return None
    return [driver for _entry, driver in resolved]


async def get_catalog_metadata(
    catalog_id: str,
    *,
    context: Optional[Dict[str, Any]] = None,
    db_resource: Optional[Any] = None,
    drivers: Optional[List[CatalogStore]] = None,
) -> Optional[Dict[str, Any]]:
    """Merged catalog metadata across all registered domain drivers.

    Each driver owns a slice of the envelope (CORE → title/description/…,
    STAC → stac_version/conforms_to/…).  Last-domain-wins on duplicate
    keys — not expected for domain-scoped drivers, but defined for safety.

    Returns ``None`` only when every driver returned ``None`` (i.e. the
    catalog has no metadata in any domain).  Any non-``None`` driver
    response yields a dict (possibly with just a subset of keys).

    ``drivers`` is injectable so callers with a pinned routing config
    can pass a filtered subset; default resolution discovers every
    registered driver.

    **Sequential fan-out (load-bearing).**  Earlier versions used
    ``asyncio.gather`` here, but when the caller passes a shared
    ``db_resource`` (a live asyncpg ``Connection``), concurrent
    ``get_catalog_metadata`` calls race on the single wire and asyncpg
    deadlocks — the hang manifests as pytest's event loop stuck in
    ``selectors.kqueue.control`` waiting for the connection to respond.
    This is the same hazard documented on :func:`list_catalogs` in
    ``catalog_service.py`` — the symmetric fix lives here too.  A
    future refactor that gives each driver its own pooled connection
    can re-enable ``gather`` at that point.
    """
    if drivers is None:
        routed = await _routed_catalog_drivers(
            Operation.READ, catalog_id, db_resource=db_resource,
        )
        drivers = routed if routed is not None else _resolve_catalog_store_drivers()
    if not drivers:
        return None

    async def _safe_get(d: CatalogStore) -> Optional[Dict[str, Any]]:
        try:
            return await d.get_catalog_metadata(
                catalog_id, context=context, db_resource=db_resource,
            )
        except Exception as exc:  # noqa: BLE001 — degrade, don't fail
            logger.warning(
                "Catalog-metadata READ failed for %s on %s: %s — "
                "omitting from merged envelope",
                type(d).__name__, catalog_id, exc,
            )
            return None

    # Sequential to avoid asyncpg single-wire deadlock when db_resource
    # is a shared Connection (see docstring).  Per-driver latency is
    # additive but dominated by the round-trip anyway (~1-2ms each).
    results: List[Optional[Dict[str, Any]]] = []
    for driver in drivers:
        results.append(await _safe_get(driver))

    merged: Dict[str, Any] = {}
    any_found = False
    for result in results:
        if result is None:
            continue
        any_found = True
        merged.update(result)
    return merged if any_found else None


async def upsert_catalog_metadata(
    catalog_id: str,
    metadata: Dict[str, Any],
    *,
    db_resource: Optional[Any] = None,
    drivers: Optional[List[CatalogStore]] = None,
) -> None:
    """Fan-out WRITE across every registered ``CatalogStore`` driver.

    Each driver receives the same ``metadata`` dict; the driver's
    internal ``_filter_payload`` strips keys outside its own domain.
    A caller therefore supplies the full envelope once — no need to
    pre-partition by domain.

    Sequential execution (not ``asyncio.gather``) to keep the driver
    order predictable inside the outer transaction.  The PG CORE and
    STAC drivers share a connection (``db_resource``) so running them
    in parallel over the same connection would cause race conditions
    on the underlying asyncpg cursor.  If a future backend genuinely
    supports per-driver connections, parallelisation can be opt-in.

    M3.0 — ``catalog_metadata_changed`` event emission
    --------------------------------------------------

    After the driver fan-out completes, emit one
    ``catalog_metadata_changed`` event per driver-domain touched.  The
    event is the single signal INDEX / BACKUP consumers
    (``ReindexWorker`` + future export endpoint) listen for to pick
    up mutations on the canonical store.  Emission shares the caller's
    ``db_resource`` so the event write lives in the same transaction
    as the metadata write — transactional-outbox semantics: if the
    enclosing transaction rolls back, the event disappears with it,
    and no consumer wastes work on a never-committed mutation.

    Best-effort: an emission failure is logged but does NOT roll back
    the successful driver writes (the authoritative store already
    has the update; a missing event just means slower propagation
    until the consumer's backfill catches up).

    Only drivers declaring ``EntityStoreCapability.WRITE`` participate in
    the fan-out.  TRANSFORM-only drivers never receive
    ``upsert_catalog_metadata`` — they'd raise ``NotImplementedError``
    from the mixin stub.
    """
    if drivers is None:
        routed = await _routed_catalog_drivers(
            Operation.WRITE, catalog_id, db_resource=db_resource,
        )
        if routed is not None:
            drivers = routed
        else:
            drivers = _filter_capable(
                _resolve_catalog_store_drivers(), EntityStoreCapability.WRITE,
            )
    if not drivers:
        if not _MISSING_DRIVERS_LOGGED["catalog_write"]:
            logger.warning(
                "No WRITE-capable CatalogStore drivers "
                "registered; upsert_catalog_metadata is a no-op."
            )
            _MISSING_DRIVERS_LOGGED["catalog_write"] = True
        return
    for driver in drivers:
        await driver.upsert_catalog_metadata(
            catalog_id, metadata, db_resource=db_resource,
        )
    await _emit_catalog_metadata_changed(
        catalog_id=catalog_id,
        drivers=drivers,
        operation="upsert",
        db_resource=db_resource,
    )


async def delete_catalog_metadata(
    catalog_id: str,
    *,
    soft: bool = False,
    db_resource: Optional[Any] = None,
    drivers: Optional[List[CatalogStore]] = None,
) -> None:
    """Fan-out DELETE across every registered ``CatalogStore`` driver.

    ``soft=True`` pushes a tombstone via ``extra_metadata._deleted_at``
    on drivers that advertise ``SOFT_DELETE``; drivers that don't
    support soft-delete semantics (STAC driver has no extra_metadata
    column) quietly fall through to hard-delete via the base-class
    implementation.  That asymmetry is acceptable for M2.3b — M2.4's
    read flip will pull STAC via the same router, and a hard-deleted
    STAC row is indistinguishable from "never had STAC".

    Only drivers declaring ``EntityStoreCapability.WRITE`` participate in
    the fan-out (no separate ``DELETE`` capability exists).  TRANSFORM-
    only drivers never receive ``delete_catalog_metadata``.
    """
    if drivers is None:
        routed = await _routed_catalog_drivers(
            Operation.WRITE, catalog_id, db_resource=db_resource,
        )
        if routed is not None:
            drivers = routed
        else:
            drivers = _filter_capable(
                _resolve_catalog_store_drivers(), EntityStoreCapability.WRITE,
            )
    if not drivers:
        if not _MISSING_DRIVERS_LOGGED["catalog_delete"]:
            logger.warning(
                "No WRITE-capable CatalogStore drivers "
                "registered; delete_catalog_metadata is a no-op."
            )
            _MISSING_DRIVERS_LOGGED["catalog_delete"] = True
        return
    for driver in drivers:
        await driver.delete_catalog_metadata(
            catalog_id, soft=soft, db_resource=db_resource,
        )
    await _emit_catalog_metadata_changed(
        catalog_id=catalog_id,
        drivers=drivers,
        operation="soft_delete" if soft else "delete",
        db_resource=db_resource,
    )


# ---------------------------------------------------------------------------
# Event-emission helper (M3.0)
# ---------------------------------------------------------------------------


async def _emit_catalog_metadata_changed(
    *,
    catalog_id: str,
    drivers: List[CatalogStore],
    operation: str,
    db_resource: Optional[Any],
) -> None:
    """Emit one ``catalog_metadata_changed`` event per driver-class touched.

    One event per driver class (``CatalogPostgresqlDriver``,
    ``CatalogElasticsearchDriver``, …) rather than one per mutation — the
    composition PG wrapper emits a single event covering all its
    metadata-core + metadata-stac sidecars.  This gives the ReindexWorker per-class
    claim surface (no N-way contention over a single PLATFORM event)
    while staying domain-blind: the router does not need to know which
    driver handles which payload slice.

    Errors here are logged, not raised — the metadata write already
    succeeded and the caller's mutation intent is satisfied.  A
    missing event means INDEX/BACKUP propagation lags until the
    consumer runs its own backfill pass.
    """
    from dynastore.modules.catalog.event_service import (
        CatalogEventType, emit_event,
    )

    emitted_classes: set[str] = set()
    for driver in drivers:
        driver_class = type(driver).__name__
        if driver_class in emitted_classes:
            continue  # de-dup within this call
        emitted_classes.add(driver_class)
        try:
            await emit_event(
                CatalogEventType.CATALOG_METADATA_CHANGED,
                catalog_id=catalog_id,
                db_resource=db_resource,
                payload={
                    "catalog_id": catalog_id,
                    "driver_class": driver_class,
                    "operation": operation,
                },
            )
        except Exception as exc:  # noqa: BLE001 — best-effort
            logger.warning(
                "catalog_metadata_changed event emission failed for "
                "%s / %s: %s — INDEX / BACKUP consumers will pick "
                "this up via their backfill pass",
                catalog_id, driver_class, exc,
            )
