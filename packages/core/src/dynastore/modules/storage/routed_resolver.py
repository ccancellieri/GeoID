#    Copyright 2025 FAO
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
from typing import Any, Dict, List, Optional, Tuple, Type

from dynastore.modules.storage.routing_config import OperationDriverEntry

logger = logging.getLogger(__name__)


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


async def resolve_routed(
    routing_plugin_cls: Type[Any],
    operation: str,
    catalog_id: str,
    collection_id: Optional[str] = None,
    *,
    db_resource: Optional[Any] = None,
) -> List[Tuple[OperationDriverEntry, Any]]:
    """Resolve an ordered list of ``(entry, driver)`` for the operation.

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

    logger.debug(
        "routed-resolve %s op=%s catalog=%s collection=%s -> [%s]",
        routing_plugin_cls.__name__, operation, catalog_id, collection_id,
        ", ".join(e.driver_ref for e, _ in resolved) or "(none)",
    )
    return resolved
