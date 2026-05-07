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

import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple

from dynastore.models.protocols.entity_store import (
    CollectionStore,
    EntityStoreCapability,
)

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
    / DELETE fan-out.  See ``TransformOnlyCollectionStoreMixin``
    in ``models/protocols/entity_store.py`` — its raising stubs are a
    bug-catcher; routers MUST honour the capability contract before
    invocation.
    """
    return [d for d in drivers
            if capability in getattr(d, "capabilities", frozenset())]


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


async def get_collection_metadata(
    catalog_id: str,
    collection_id: str,
    *,
    context: Optional[Dict[str, Any]] = None,
    db_resource: Optional[Any] = None,
    drivers: Optional[List[CollectionStore]] = None,
) -> Optional[Dict[str, Any]]:
    """Merge every registered driver's READ slice into a single envelope.

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
    drivers = drivers if drivers is not None else _resolve_drivers()
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
) -> Tuple[List[Dict[str, Any]], int]:
    """Delegate SEARCH to the first driver capable of serving the query shape."""
    drivers = drivers if drivers is not None else _resolve_drivers()
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
