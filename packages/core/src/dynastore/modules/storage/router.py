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

"""
Storage Router — resolves drivers for a given operation + catalog/collection.

Resolution is based on ``ItemsRoutingConfig`` (operation → ordered driver
list) with optional hint-based filtering.

For **WRITE**: all matching drivers execute (fan-out), each with its own
``FailurePolicy``.

For **READ/SEARCH**: the first matching driver is returned.

Performance: driver index lookup uses the process-wide ``DriverRegistry``
singleton (L0 cache, built once at startup) so there is no per-request dict
allocation.  Routing resolution is cached (300 s TTL) keyed on
``(routing_config_class_key, catalog_id, collection_id, operation, hints)``.
"""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, FrozenSet, Generic, List, Optional, Type, TypeVar, Union, cast

if TYPE_CHECKING:
    from dynastore.models.protocols.storage_driver import CollectionItemsStore
    from dynastore.models.protocols.asset_driver import AssetStore
    from dynastore.modules.db_config.plugin_config import PluginConfig
    AnyDriver = Union["CollectionItemsStore", "AssetStore"]

_D = TypeVar("_D")

from dynastore.modules.storage.hints import Hint
from dynastore.modules.storage.routing_config import (
    AssetRoutingConfig,
    FailurePolicy,
    Operation,
    ItemsRoutingConfig,
    WriteMode,
)
from dynastore.modules.storage.driver_registry import DriverRegistry
from dynastore.modules.storage.config_cache import get_request_driver_cache
from dynastore.tools.cache import cached

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Resolved driver container
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ResolvedDriver(Generic[_D]):
    """A driver resolved for a specific operation, with its failure policy and write mode."""

    driver: _D
    on_failure: FailurePolicy = FailurePolicy.FATAL
    write_mode: WriteMode = WriteMode.SYNC

    @property
    def driver_ref(self) -> str:
        return type(self.driver).__name__


# ---------------------------------------------------------------------------
# Core resolution
# ---------------------------------------------------------------------------


@cached(maxsize=4096, ttl=300, namespace="storage_router", distributed=True, l1_ttl=2)
async def _resolve_driver_ids_cached(
    routing_plugin_cls: "Type[PluginConfig]",
    catalog_id: str,
    collection_id: Optional[str],
    operation: str,
    hints: FrozenSet[Hint],
) -> List[tuple]:
    """Cached resolution: returns list of (driver_ref, on_failure, write_mode) tuples."""
    from dynastore.models.protocols.configs import ConfigsProtocol
    from dynastore.tools.discovery import get_protocol

    configs = get_protocol(ConfigsProtocol)
    if not configs:
        raise RuntimeError("ConfigsProtocol not available — cannot resolve storage routing")

    from dynastore.modules.storage.routing_config import ItemsRoutingConfig as _RPC
    _raw_config = await configs.get_config(
        routing_plugin_cls,
        catalog_id=catalog_id,
        collection_id=collection_id,
    )
    routing_config = cast(_RPC, _raw_config)

    from dynastore.modules.storage.routing_config import OperationDriverEntry as _ODE
    _ops = cast(Dict[str, List[_ODE]], routing_config.operations)
    entries = _ops.get(operation, [])

    # Fail-safe: if the loaded config has no entries for this operation
    # (e.g. a stored row with `operations: {}` left behind by a config
    # refactor migration, or a partially-seeded routing config), fall back
    # to the model's default_factory operations for the SAME class. This
    # matches the behaviour you'd get with NO stored row at all
    # (configs.get_config returns `cls()` when no row exists, which fires
    # default_factory). Without this fallback, a stored-but-empty config
    # produces a worse outcome than no config at all — silently 500ing
    # `get_collection_config` / `get_asset_driver` etc. Documented regression
    # surfaced 2026-04-29 on review env image :860 for `ingestion`
    # (collection routing READ) and `gdal` (asset routing READ) after a
    # parallel configs-refactor PR rewrote stored-config shape.
    if not entries:
        try:
            _default_ops = cast(
                Dict[str, List[_ODE]],
                routing_plugin_cls().operations,  # type: ignore[call-arg]
            )
            fallback = _default_ops.get(operation, [])
            if fallback:
                entries = list(fallback)
        except Exception:
            # Defensive: if the model default_factory itself fails (no
            # zero-arg constructor, etc.), keep the original empty entries
            # — caller's downstream ValueError preserves the clear "no
            # driver registered" semantics rather than masking with a
            # follow-up exception from the fallback path.
            pass

    if hints:
        # Best-overlap matcher: an entry matches iff the entry's effective
        # hint surface is a SUPERSET of the requested hints (the entry can
        # serve every preference the caller asked for). When ``entry.hints``
        # is empty we defer to the driver class's ``supported_hints`` —
        # drivers self-declare what they serve, so an empty entry-hints set
        # means "this entry does not constrain the hint surface; match
        # whatever the driver itself supports". Preserves zero-config
        # routing while letting operators pin a stricter surface per-entry.
        #
        # Tie-break: on equal match, the entry whose effective hint surface
        # is LONGEST wins (most specific). Final tiebreak is entry order
        # in the configured list.
        if routing_plugin_cls is AssetRoutingConfig:
            driver_index = DriverRegistry.asset_index()
        else:
            driver_index = DriverRegistry.collection_index()

        def _effective_hints(e: _ODE) -> FrozenSet[Hint]:
            if e.hints:
                return frozenset(e.hints)
            drv = driver_index.get(e.driver_ref)
            if drv is None:
                return frozenset()
            return frozenset(getattr(
                type(drv), "supported_hints", frozenset(),
            ))

        def _entry_matches(e: _ODE) -> bool:
            return hints.issubset(_effective_hints(e))

        matched = [(i, e, _effective_hints(e)) for i, e in enumerate(entries) if _entry_matches(e)]
        # Sort by (-len(effective), entry_order) so longest-effective wins,
        # with original-position as the deterministic final tiebreak.
        matched.sort(key=lambda triple: (-len(triple[2]), triple[0]))
        entries = [e for _, e, _eff in matched]

    return [(e.driver_ref, e.on_failure, e.write_mode) for e in entries]


async def resolve_drivers(
    operation: str,
    catalog_id: str,
    collection_id: Optional[str] = None,
    *,
    hints: FrozenSet[Hint] = frozenset(),
    routing_plugin_cls: "Type[PluginConfig]" = ItemsRoutingConfig,
) -> List[ResolvedDriver]:
    """Resolve an ordered list of drivers for the requested operation.

    For **READ/SEARCH**: caller uses the first result.
    For **WRITE**: caller executes all (fan-out), respecting ``on_failure``.

    Resolution layers (fast → slow):
    - **L4** per-request context var — zero-cost within a single request
    - **L1** in-process ``@cached`` LRU — sub-microsecond after first resolution
    - **L2** Valkey-backed shared cache — shared across workers (TTL 300 s)
    - **L3** DB waterfall query — cold path, triggered on cache miss

    Args:
        operation: Required. ``WRITE``, ``READ``, ``SEARCH``, etc.
        catalog_id: Catalog context.
        collection_id: Optional collection context.
        hints: Optional set of preferences. An empty set selects all entries
            (preserves zero-config defaults). Non-empty: only entries whose
            effective hints are a SUPERSET of the request are kept, longest
            effective set wins on tie, then entry order.
        routing_plugin_cls: PluginConfig class — ``ItemsRoutingConfig`` for
            collections, ``AssetRoutingConfig`` for assets.

    Returns:
        Ordered list of :class:`ResolvedDriver`. Empty when the request
        ``hints`` are not satisfiable by any configured driver.
    """
    # L4 — per-request memoisation: if the same resolution was already performed
    # earlier in this request, return the cached result without touching L1/L2/L3.
    l4_key = (routing_plugin_cls, catalog_id, collection_id, operation, hints)
    l4 = get_request_driver_cache()
    if l4_key in l4:
        return l4[l4_key]

    resolved_ids = await _resolve_driver_ids_cached(
        routing_plugin_cls, catalog_id, collection_id, operation, hints,
    )

    if routing_plugin_cls is AssetRoutingConfig:
        driver_index = DriverRegistry.asset_index()
    else:
        driver_index = DriverRegistry.collection_index()

    result = []
    for driver_ref, on_failure, write_mode in resolved_ids:
        driver = driver_index.get(driver_ref)
        if driver:
            result.append(ResolvedDriver(driver=driver, on_failure=on_failure, write_mode=write_mode))
        else:
            logger.warning(
                "Driver '%s' for operation '%s' is not registered. Skipping.",
                driver_ref,
                operation,
            )

    logger.debug(
        "router-resolve %s op=%s catalog=%s collection=%s hints=%s -> [%s]",
        routing_plugin_cls.__name__,
        operation,
        catalog_id,
        collection_id,
        sorted(hints),
        ", ".join(rd.driver_ref for rd in result) or "(none)",
    )

    # Store in L4 for reuse later in the same request
    l4[l4_key] = result
    return result


# ---------------------------------------------------------------------------
# Convenience wrappers — collection drivers
# ---------------------------------------------------------------------------


async def get_driver(
    operation: str,
    catalog_id: str,
    collection_id: Optional[str] = None,
    *,
    hints: FrozenSet[Hint] = frozenset(),
) -> "CollectionItemsStore":
    """Single-driver resolution for collection READ/SEARCH.

    Returns the first matching ``CollectionItemsStore`` or raises.

    ``hints`` selects among multiple drivers configured for the same
    operation. The default routing puts ES (public) first for READ with
    ``hints={Hint.GEOMETRY_SIMPLIFIED}`` and PG second with
    ``hints={Hint.GEOMETRY_EXACT}``. SDK consumers needing exact geometries
    pass the corresponding hint::

        # Default (fast simplified-geom search via ES):
        driver = await get_driver(Operation.READ, catalog_id, collection_id)

        # Exact geometries (falls through to PG):
        driver = await get_driver(
            Operation.READ, catalog_id, collection_id,
            hints=frozenset({Hint.GEOMETRY_EXACT}),
        )

    Hint matching: an entry's effective surface is ``entry.hints`` when
    populated, else the driver class's ``supported_hints``. An entry
    matches when its effective surface is a SUPERSET of the requested
    ``hints``; ties broken by largest effective surface then entry order.
    """
    resolved = await resolve_drivers(
        operation, catalog_id, collection_id, hints=hints,
    )
    if not resolved:
        raise ValueError(
            f"No collection driver found for operation='{operation}', "
            f"hints={sorted(hints)}, catalog='{catalog_id}', collection='{collection_id}'"
        )
    from dynastore.models.protocols.storage_driver import CollectionItemsStore as _CSDP
    return cast(_CSDP, resolved[0].driver)


async def get_items_search_driver(
    catalog_id: str,
    collection_id: Optional[str] = None,
    *,
    hints: FrozenSet[Hint] = frozenset(),
) -> "ResolvedDriver[CollectionItemsStore]":
    """Routing-aware single-driver resolution for items SEARCH.

    Resolution order, mirroring the asset tier
    (:func:`get_asset_search_driver`) and the routing-aware lookup design
    in issue #989:

    1. ``ItemsRoutingConfig.operations[SEARCH]`` — if an operator pinned a
       search-optimised driver for this catalog/collection (e.g. an
       Elasticsearch index, or the tenant-scoped private ES index), use it.
    2. Fall back to ``ItemsRoutingConfig.operations[READ]`` when no SEARCH
       entry resolves. Any READ-capable driver advertises SEARCH via
       :func:`derive_supported_operations` (Capability.READ → {READ, SEARCH}),
       so the read primary (PG by default) serves filtered queries when no
       dedicated search backend is configured.

    Unlike :func:`get_driver` this returns the full :class:`ResolvedDriver`
    so callers can inspect the driver instance (e.g. to decide between the
    index-backed path and the PG hub-scan fallback). Raises ``ValueError``
    when neither operation resolves a registered driver.
    """
    resolved = await resolve_drivers(
        Operation.SEARCH, catalog_id, collection_id, hints=hints,
    )
    if not resolved:
        resolved = await resolve_drivers(
            Operation.READ, catalog_id, collection_id, hints=hints,
        )
    if not resolved:
        raise ValueError(
            f"No items SEARCH/READ driver found for "
            f"hints={sorted(hints)}, catalog='{catalog_id}', collection='{collection_id}'"
        )
    from dynastore.models.protocols.storage_driver import CollectionItemsStore as _CSDP
    return cast("ResolvedDriver[_CSDP]", resolved[0])


async def get_write_drivers(
    catalog_id: str,
    collection_id: Optional[str] = None,
    *,
    hints: FrozenSet[Hint] = frozenset(),
) -> "List[ResolvedDriver[CollectionItemsStore]]":
    """Multi-driver resolution for collection WRITE fan-out.

    Always returns ≥1 entry in a correctly bootstrapped deploy. The waterfall
    has a code-level default (``ItemsRoutingConfig.operations[WRITE] =
    [ItemsPostgresqlDriver]``), so an empty result indicates a deploy/ops
    misconfiguration and is raised as :class:`ConfigResolutionError`.
    """
    from dynastore.models.protocols.storage_driver import CollectionItemsStore as _CSDP
    result = await resolve_drivers(
        Operation.WRITE, catalog_id, collection_id, hints=hints,
    )
    if not result:
        from dynastore.modules.db_config.exceptions import ConfigResolutionError

        raise ConfigResolutionError(
            (
                f"No CollectionItemsStore resolved for WRITE on "
                f"'{catalog_id}/{collection_id}'. Routing waterfall produced "
                f"an empty list — neither ItemsRoutingConfig.operations[WRITE] "
                f"nor its code default is supplying a registered, available driver."
            ),
            missing_key="ItemsRoutingConfig.operations[WRITE]",
            required_fields=[],
            scope_tried=["collection", "catalog", "platform", "code_default"],
            hint=(
                "Register a CollectionItemsStore driver (e.g. "
                "ItemsPostgresqlDriver) or set "
                "ItemsRoutingConfig.operations[WRITE] at platform scope."
            ),
        )
    return cast(List["ResolvedDriver[_CSDP]"], result)


# ---------------------------------------------------------------------------
# Convenience wrappers — asset drivers
# ---------------------------------------------------------------------------


async def get_asset_driver(
    operation: str,
    catalog_id: str,
    collection_id: Optional[str] = None,
    *,
    hints: FrozenSet[Hint] = frozenset(),
):
    """Single-driver resolution for asset READ/SEARCH.

    Returns the first matching ``AssetStore`` or raises.
    """
    resolved = await resolve_drivers(
        operation,
        catalog_id,
        collection_id,
        hints=hints,
        routing_plugin_cls=AssetRoutingConfig,
    )
    if not resolved:
        raise ValueError(
            f"No asset driver found for operation='{operation}', "
            f"hints={sorted(hints)}, catalog='{catalog_id}', collection='{collection_id}'"
        )
    return resolved[0].driver


async def get_asset_search_driver(
    catalog_id: str,
    collection_id: Optional[str] = None,
    *,
    hints: FrozenSet[Hint] = frozenset(),
):
    """Routing-aware single-driver resolution for asset SEARCH.

    Resolution order, mirroring the collection tier (``collection_router``)
    and the routing-aware lookup design in issue #989:

    1. ``AssetRoutingConfig.operations[SEARCH]`` — if an operator pinned a
       search-optimised driver for this catalog/collection (e.g. an
       Elasticsearch index), use it.
    2. Fall back to ``AssetRoutingConfig.operations[READ]`` when no SEARCH
       entry resolves. Any READ-capable driver advertises SEARCH via
       :func:`derive_supported_operations` (Capability.READ → {READ, SEARCH}),
       so the read primary (PG by default) serves filtered queries when no
       dedicated search backend is configured.

    Returns the first matching ``AssetStore`` or raises when neither
    operation resolves a registered driver.
    """
    resolved = await resolve_drivers(
        Operation.SEARCH,
        catalog_id,
        collection_id,
        hints=hints,
        routing_plugin_cls=AssetRoutingConfig,
    )
    if not resolved:
        resolved = await resolve_drivers(
            Operation.READ,
            catalog_id,
            collection_id,
            hints=hints,
            routing_plugin_cls=AssetRoutingConfig,
        )
    if not resolved:
        raise ValueError(
            f"No asset SEARCH/READ driver found for "
            f"hints={sorted(hints)}, catalog='{catalog_id}', collection='{collection_id}'"
        )
    return resolved[0].driver


async def get_asset_write_drivers(
    catalog_id: str,
    collection_id: Optional[str] = None,
    *,
    hints: FrozenSet[Hint] = frozenset(),
) -> "List[ResolvedDriver[AssetStore]]":
    """Multi-driver resolution for asset WRITE fan-out."""
    from dynastore.models.protocols.asset_driver import AssetStore as _ADP
    result = await resolve_drivers(
        Operation.WRITE,
        catalog_id,
        collection_id,
        hints=hints,
        routing_plugin_cls=AssetRoutingConfig,
    )
    return cast(List["ResolvedDriver[_ADP]"], result)


async def get_asset_index_drivers(
    catalog_id: str,
    collection_id: Optional[str] = None,
    *,
    hints: FrozenSet[Hint] = frozenset(),
) -> "List[ResolvedDriver[AssetStore]]":
    """Multi-driver resolution for asset secondary indexes.

    A secondary index is not a distinct operation: it is a ``WRITE`` target
    whose driver implements the ``AssetIndexer`` role (``is_asset_indexer``).
    WRITE is auto-augmented at config-validation time with every discoverable
    ``AssetIndexer`` driver — which is how ``AssetElasticsearchDriver`` gets
    picked up without explicit operator config. This filters the resolved
    WRITE fan-out down to the indexer-role drivers, used by reconcile/sync to
    (re)propagate assets to their search sinks.
    """
    from dynastore.models.protocols.asset_driver import AssetStore as _ADP
    result = await resolve_drivers(
        Operation.WRITE,
        catalog_id,
        collection_id,
        hints=hints,
        routing_plugin_cls=AssetRoutingConfig,
    )
    indexers = [rd for rd in result if getattr(rd.driver, "is_asset_indexer", False)]
    return cast(List["ResolvedDriver[_ADP]"], indexers)


async def get_asset_upload_driver(
    catalog_id: str,
    collection_id: Optional[str] = None,
    *,
    hints: FrozenSet[Hint] = frozenset(),
):
    """Single-driver resolution for asset UPLOAD.

    Reads ``AssetRoutingConfig.operations[UPLOAD]`` (auto-augmented with
    every discoverable ``AssetUploadProtocol`` impl) and returns the first
    matching backend instance. Falls back to the first registered
    ``AssetUploadProtocol`` impl when no UPLOAD entries resolve — preserves
    the previous ``get_protocol(AssetUploadProtocol)`` behaviour for
    deployments that haven't configured per-catalog upload routing.

    Returns ``None`` only when no backend is registered at all.
    """
    from dynastore.models.protocols.asset_upload import AssetUploadProtocol
    from dynastore.tools.discovery import get_protocol, get_protocols

    # Try the routing-config waterfall first.
    try:
        resolved_ids = await _resolve_driver_ids_cached(
            AssetRoutingConfig, catalog_id, collection_id,
            Operation.UPLOAD, hints,
        )
    except Exception as exc:
        logger.debug(
            "Asset upload routing resolution skipped (%s); falling back to "
            "first-registered backend.", exc,
        )
        resolved_ids = []

    from dynastore.tools.typed_store.base import _to_snake
    impls_by_class = {_to_snake(type(d).__name__): d for d in get_protocols(AssetUploadProtocol)}
    for driver_ref, _on_failure, _write_mode in resolved_ids:
        impl = impls_by_class.get(driver_ref)
        if impl is None:
            logger.warning(
                "Asset upload driver '%s' configured but not registered; trying "
                "next entry.", driver_ref,
            )
            continue
        if not _upload_driver_available(impl):
            logger.debug(
                "Asset upload driver '%s' reports unavailable; skipping and "
                "trying next entry.", driver_ref,
            )
            continue
        return impl

    # Fallback: first-registered available backend (matches legacy
    # get_protocol behaviour, but skips backends that report unavailable so an
    # uninitialised GCP module doesn't shadow a ready local backend).
    for impl in get_protocols(AssetUploadProtocol):
        if _upload_driver_available(impl):
            return impl
        logger.debug(
            "Asset upload driver '%s' reports unavailable; skipping in "
            "first-registered fallback.", type(impl).__name__,
        )
    return get_protocol(AssetUploadProtocol)


def _upload_driver_available(impl: object) -> bool:
    """Whether an ``AssetUploadProtocol`` impl is ready to serve uploads.

    Consults the upload-specific ``upload_available()`` hook (distinct from the
    module-wide ``is_available()`` discovery gate, which is already applied
    upstream by ``get_protocols``). A missing hook is treated as available (the
    contract makes it optional). A raising hook is treated as unavailable
    defensively.
    """
    probe = getattr(impl, "upload_available", None)
    if probe is None:
        return True
    try:
        return bool(probe())
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Cache invalidation
# ---------------------------------------------------------------------------


def invalidate_router_cache(
    catalog_id: Optional[str] = None,
    collection_id: Optional[str] = None,
) -> None:
    """Invalidate cached resolution for collection routing.

    Also clears the ``DriverRegistry`` L0 cache so that any driver
    (un)registration events are reflected on the next request.
    """
    try:
        getattr(_resolve_driver_ids_cached, "cache_clear")()
    except Exception:
        pass
    DriverRegistry.clear()


def invalidate_asset_router_cache(
    catalog_id: Optional[str] = None,
    collection_id: Optional[str] = None,
) -> None:
    """Invalidate cached resolution for asset routing.

    Note: shares the same underlying cache as collection routing
    (differentiated by ``routing_plugin_id`` in the cache key).
    Full cache clear is the safest approach.
    """
    invalidate_router_cache(catalog_id, collection_id)
