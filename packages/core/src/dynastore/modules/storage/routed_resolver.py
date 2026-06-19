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

"""Config-driven driver resolution for the collection-envelope and
catalog-envelope tiers.

``collection_router`` / ``catalog_router`` historically fanned out to
every driver discovered via ``get_protocols(CollectionStore|CatalogStore)``,
ignoring the routing config entirely. This module makes them config-driven:
it loads ``CollectionRoutingConfig`` / ``CatalogRoutingConfig`` through the
``ConfigsProtocol``, reads ``operations[operation]``, and maps each
``OperationDriverEntry.driver_ref`` to a concrete driver instance via the
process-wide :class:`DriverRegistry` store indexes.

Parity with ``storage/router.py``:
* a stored config with ``operations={}`` (or missing the requested op)
  falls back to the model's ``default_factory`` for that operation —
  a stored-but-empty row must never be worse than no row at all.
* when ``ConfigsProtocol`` is unavailable (early boot), :func:`resolve_routed`
  returns ``[]`` so the caller can degrade to discovery fan-out.

Every resolution emits one DEBUG line naming the selected drivers — the
single place to watch routing behaviour for the collection/catalog tiers.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, FrozenSet, List, Optional, Tuple, Type

from dynastore.modules.storage.hints import Hint
from dynastore.modules.storage.routing_config import OperationDriverEntry

logger = logging.getLogger(__name__)

# Soak-signal aid for #748 item 2 (discovery-fallback retirement gate).
# The fallback path below logs at DEBUG, which is filtered out by default-INFO
# Cloud Logging — making the "zero unavailable lines" retirement gate
# unobservable in review env. Promote the first occurrence per process to
# WARNING so operators can see it; subsequent occurrences stay at DEBUG to
# avoid spam under sustained ConfigsProtocol unavailability.
_FALLBACK_WARNED = False


async def _load_routing_config(
    routing_plugin_cls: Type[Any],
    catalog_id: str,
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> Any:
    """Load the live routing config via ConfigsProtocol.

    Raises if ConfigsProtocol is not registered — caller treats that as
    "degrade to discovery".

    Note: ``db_resource`` is accepted for interface symmetry but not forwarded
    to ``get_config`` — existing routing-config callers in this codebase pass
    only ``catalog_id`` / ``collection_id`` (the waterfall handles scope
    resolution internally).
    """
    from dynastore.models.protocols.configs import ConfigsProtocol
    from dynastore.tools.discovery import get_protocol

    configs = get_protocol(ConfigsProtocol)
    if configs is None:
        raise RuntimeError("ConfigsProtocol not available")
    return await configs.get_config(
        routing_plugin_cls,
        catalog_id=catalog_id,
        collection_id=collection_id,
    )


def _index_for(routing_plugin_cls: Type[Any]) -> Dict[str, Any]:
    """Return the by-ref driver index appropriate for the routing config class."""
    from dynastore.modules.storage.driver_registry import DriverRegistry
    from dynastore.modules.storage.routing_config import (
        CatalogRoutingConfig,
        CollectionRoutingConfig,
    )

    if routing_plugin_cls is CatalogRoutingConfig:
        return DriverRegistry.catalog_store_index()
    if routing_plugin_cls is CollectionRoutingConfig:
        return DriverRegistry.collection_store_index()
    raise ValueError(
        f"routed_resolver does not handle {routing_plugin_cls.__name__}; "
        "items/asset tiers use storage.router.resolve_drivers"
    )


def _entries_for_operation(
    routing_config: Any,
    routing_plugin_cls: Type[Any],
    operation: str,
) -> List[OperationDriverEntry]:
    """Read operations[operation]; fall back to the model default_factory
    for that operation when the stored config has no entries (parity with
    storage/router.py:_resolve_driver_ids_cached)."""
    ops = getattr(routing_config, "operations", {}) or {}
    entries = list(ops.get(operation, []))
    if entries:
        return entries
    try:
        default_ops = routing_plugin_cls().operations  # fires default_factory
        return list(default_ops.get(operation, []))
    except Exception:  # noqa: BLE001 — defensive; empty list keeps clear semantics
        return []


def _apply_hint_filter(
    resolved: List[Tuple[OperationDriverEntry, Any]],
    hints: FrozenSet[Hint],
    operation: str,
) -> List[Tuple[OperationDriverEntry, Any]]:
    """Apply best-overlap hint matching to an already-resolved driver list.

    Mirrors the matching semantics in ``storage/router.py``:

    * Empty ``hints`` → return the list unchanged (preserve zero-config
      default behaviour; callers that substitute GEOMETRY_EXACT for the
      empty-hint default path rely on this short-circuit).
    * Non-empty ``hints`` → keep entries whose *effective* hint surface
      (``entry.hints`` when set, else driver class ``supported_hints``)
      is a SUPERSET of the requested hints.  Tie-break: longest effective
      surface first, then original entry order.
    * If no entry matches (e.g. hint not declared by any configured driver)
      and the operation is READ or SEARCH → relax: return the full list in
      original order so the request gets data rather than nothing.

    The ``operation`` string is used only for the relax-branch decision and
    the log line; it is not re-validated here.
    """
    if not hints:
        return resolved

    def _effective_hints(entry: OperationDriverEntry, driver: Any) -> FrozenSet[Hint]:
        if entry.hints:
            return frozenset(entry.hints)
        return frozenset(getattr(type(driver), "supported_hints", frozenset()))

    def _entry_matches(entry: OperationDriverEntry, driver: Any) -> bool:
        return hints.issubset(_effective_hints(entry, driver))

    matched = [
        (i, e, d, _effective_hints(e, d))
        for i, (e, d) in enumerate(resolved)
        if _entry_matches(e, d)
    ]
    matched.sort(key=lambda quad: (-len(quad[3]), quad[0]))
    if matched:
        return [(e, d) for _, e, d, _eff in matched]

    from dynastore.modules.storage.routing_config import Operation
    if operation in (Operation.READ, Operation.SEARCH):
        logger.info(
            "routed-resolve: no driver satisfies hints=%s for op=%s; "
            "relaxing to full driver list",
            sorted(hints), operation,
        )
        return resolved
    return []


async def resolve_routed(
    routing_plugin_cls: Type[Any],
    operation: str,
    catalog_id: str,
    collection_id: Optional[str] = None,
    *,
    hints: FrozenSet[Hint] = frozenset(),
    db_resource: Optional[Any] = None,
) -> List[Tuple[OperationDriverEntry, Any]]:
    """Resolve an ordered list of ``(entry, driver)`` for the operation.

    When ``hints`` is non-empty the list is filtered to drivers whose
    effective hint surface (``entry.hints`` when populated, else the driver
    class's ``supported_hints``) is a SUPERSET of the requested hints.
    Best-overlap tie-break: longest effective surface first, then declared
    entry order.  On no match for READ/SEARCH the full list is returned
    (relax — preference, not hard filter).  Empty ``hints`` skips filtering
    entirely and preserves the original declared order.

    Returns ``[]`` when ConfigsProtocol is unavailable — the caller should
    then degrade to discovery-based resolution. Unregistered driver_refs
    are skipped with a WARNING (not fatal — a deploy may legitimately omit
    a driver, e.g. an ES-less stack).
    """
    try:
        routing_config = await _load_routing_config(
            routing_plugin_cls, catalog_id, collection_id, db_resource,
        )
    except Exception as exc:  # noqa: BLE001 — degrade to discovery
        global _FALLBACK_WARNED
        if not _FALLBACK_WARNED:
            _FALLBACK_WARNED = True
            logger.warning(
                "routed-resolve unavailable for %s/%s op=%s (%s); "
                "caller should fall back to discovery — further occurrences "
                "in this process will be logged at DEBUG only",
                catalog_id, collection_id, operation, exc,
            )
        else:
            logger.debug(
                "routed-resolve unavailable for %s/%s op=%s (%s); "
                "caller should fall back to discovery",
                catalog_id, collection_id, operation, exc,
            )
        return []

    entries = _entries_for_operation(routing_config, routing_plugin_cls, operation)
    index = _index_for(routing_plugin_cls)

    resolved: List[Tuple[OperationDriverEntry, Any]] = []
    for entry in entries:
        driver = index.get(entry.driver_ref)
        if driver is None:
            logger.warning(
                "routed-resolve: driver_ref '%s' for %s op=%s on %s/%s is not "
                "registered — skipping",
                entry.driver_ref, routing_plugin_cls.__name__, operation,
                catalog_id, collection_id,
            )
            continue
        resolved.append((entry, driver))

    resolved = _apply_hint_filter(resolved, hints, operation)

    logger.debug(
        "routed-resolve %s op=%s catalog=%s collection=%s hints=%s -> [%s]",
        routing_plugin_cls.__name__, operation, catalog_id, collection_id,
        sorted(hints) or "(none)",
        ", ".join(e.driver_ref for e, _ in resolved) or "(none)",
    )
    return resolved
