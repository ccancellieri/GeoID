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
Routing plugin configuration — operation-based driver composition.

Maps **operations** (WRITE, READ, SEARCH) to an ordered list of drivers,
each with optional hints and a failure policy.

Key concepts:

- **Operations** = what the caller wants (WRITE, READ, SEARCH) — defined here
- **Capabilities** = how the driver performs it (SYNC, ASYNC, etc.) — in driver_config.py
- **Hints** = caller-provided preferences to select a specific driver within an operation
- **Failure policy** = per-driver behaviour on error: fatal, warn, or ignore

Resolution semantics:

- **WRITE** (no hint): execute ALL drivers in list (fan-out), respecting ``on_failure``
- **WRITE** (with hint): filter to matching drivers, execute those
- **READ/SEARCH** (no hint): return first driver in list (primary by position)
- **READ/SEARCH** (with hint): filter to matching, return first match
"""

import logging
from enum import StrEnum
from typing import Any, Callable, ClassVar, Dict, FrozenSet, List, Literal, Optional, Set, Tuple, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from dynastore.models.protocols.driver_roles import DriverSla
from dynastore.models.protocols.indexer import (
    AssetIndexer,
    CatalogIndexer,
    CollectionIndexer,
)
from dynastore.modules.db_config.platform_config_service import (
    Immutable,
    PluginConfig,
)
from dynastore.tools.typed_store.base import _to_snake
from dynastore.tools.ui_hints import ui

logger = logging.getLogger(__name__)


class FailurePolicy(StrEnum):
    """Per-driver failure behaviour within an operation."""

    FATAL = "fatal"    # operation fails if this driver fails
    WARN = "warn"      # log warning, continue with other drivers
    IGNORE = "ignore"  # silently skip on failure


class Operation(StrEnum):
    """Standard operations configured in routing configs.

    The four routing configs mirror the four entity tiers, each governing
    CRUD on its own entity row (catalog / collection / items / assets).
    No separate "metadata" routing — every entity has exactly one routing
    config that dispatches every operation on that entity's row.

    Items routing (``ItemsRoutingConfig.operations``) — entity rows for
    collection items / features:
    - WRITE  : fan-out to all configured drivers (position 0 = primary)
    - READ   : single-driver for browsing/pagination (streaming)
    - SEARCH : single-driver for filtered queries (bbox, attributes, fulltext)

    Collection routing (``CollectionRoutingConfig.operations``) — collection
    envelope rows:
    - READ      : first-match driver (PG sidecar / ES wrapper / …).
    - WRITE     : primary driver(s) committing in-transaction.
    - TRANSFORM : ordered chain — drivers (typically items drivers) that
                  enrich the collection row at READ time (extents, counts,
                  summaries derived from items).
    - INDEX     : async post-write propagation to search sinks (ES, vector
                  DB, …).  Entries declare ``transformed: bool``; the
                  ReindexWorker feeds raw or transformed envelopes.
    - BACKUP    : async post-write propagation to export sinks (Parquet via
                  DuckDB, NDJSON, …).  Entries declare ``transformed`` + ``fmt``.

    Catalog routing (``CatalogRoutingConfig.operations``) — same shape on
    catalog rows.

    Asset routing (``AssetRoutingConfig.operations``) — asset rows:
    - WRITE / READ : as above (single-primary + secondaries via INDEX/event sync)
    - INDEX  : auto-augmented with discoverable ``AssetIndexer`` drivers,
               fanned out asynchronously by ``AssetEntitySyncSubscriber``.
    - UPLOAD : single-driver pick of the ``AssetUploadProtocol`` impl that
               handles ``initiate_upload``/``get_upload_status`` (auto-
               augmented from discoverable ``AssetUploadProtocol`` impls;
               operator config can pin a specific backend).
    """

    WRITE = "WRITE"
    READ = "READ"
    SEARCH = "SEARCH"
    TRANSFORM = "TRANSFORM"
    INDEX = "INDEX"
    BACKUP = "BACKUP"
    UPLOAD = "UPLOAD"


class WriteMode(StrEnum):
    """Execution / composition mode for an operation entry.

    Items-routing write semantics:
    - ``sync``   : await result; participates in coordinated rollback
                   (all sync writes run in parallel via ``asyncio.gather``)
    - ``async``  : fire-and-forget after sync phase succeeds

    Collection / catalog-routing composition semantics:
    - ``first``    : return result from the first driver that succeeds
                     (used with ``Operation.READ``)
    - ``fan_out``  : call all drivers independently; merge results
                     (used with ``Operation.WRITE``)
    - ``chain``    : pipe output through drivers in declared order;
                     each driver receives the previous output
                     (used with ``Operation.TRANSFORM``)
    """

    SYNC = "sync"
    ASYNC = "async"
    FIRST = "first"
    FAN_OUT = "fan_out"
    CHAIN = "chain"


# ---------------------------------------------------------------------------
# Capability → Operation mapping
# ---------------------------------------------------------------------------


def derive_supported_operations(capabilities: FrozenSet[str]) -> FrozenSet[str]:
    """Derive which Operations a driver supports from its Capability set.

    Uses :data:`_CAPABILITY_TO_OPERATIONS` to map driver capabilities to the
    operations they can handle.  This is used by apply-handler validation and
    the driver discovery endpoint.

    Role-based driver plan additions (new ops): any driver with ``WRITE`` can
    participate in ``INDEX`` or ``BACKUP`` as an async propagation sink — the
    role is a config assignment, not a driver-internal contract.

    ``Operation.TRANSFORM`` participation is determined by implementing
    :class:`EntityTransformProtocol` rather than via a Capability flag.
    See ``modules/storage/routing_config.py:_self_register_transformers_into``.
    """
    from dynastore.models.protocols.storage_driver import Capability

    mapping: Dict[str, Set[str]] = {
        Capability.WRITE: {Operation.WRITE, Operation.INDEX, Operation.BACKUP},
        Capability.READ: {Operation.READ},
        Capability.FULLTEXT: {Operation.SEARCH},
        Capability.ATTRIBUTE_FILTER: {Operation.SEARCH},
        Capability.SPATIAL_FILTER: {Operation.SEARCH},
        Capability.EXPORT: {Operation.BACKUP},
    }
    ops: Set[str] = set()
    for cap in capabilities:
        if cap in mapping:
            ops.update(mapping[cap])
    return frozenset(ops)


def _items_search_caps() -> FrozenSet[str]:
    """Storage-tier capability strings that qualify a driver for the
    items-tier ``operations[SEARCH]`` operation.

    Lazy: imports the storage ``Capability`` enum on first call so this
    module stays importable when a stripped-down dependency set is
    loaded (e.g. tests that don't pull the storage protocols).
    """
    from dynastore.models.protocols.storage_driver import Capability

    return frozenset({
        Capability.FULLTEXT,
        Capability.SPATIAL_FILTER,
        Capability.ATTRIBUTE_FILTER,
    })


# ---------------------------------------------------------------------------
# Config models
# ---------------------------------------------------------------------------


class OperationDriverEntry(BaseModel):
    """A driver configured for a specific operation.

    ``driver_id`` is immutable — changing which drivers participate in an
    operation is a structural decision.  ``hints`` and ``on_failure`` are
    mutable preferences that can evolve without structural impact.

    Role-based driver plan additions (optional, default-inert):

    - ``sla``         — per-entry SLA override.  When ``None``, the driver's
                         class-level ``sla`` ClassVar (if any) is used.  On
                         TRANSFORM entries, either the entry or the class
                         must provide one; CI guard enforces.
    - ``transformed`` — for INDEX / BACKUP entries: ``True`` means the
                         ReindexWorker feeds this sink the transformed envelope
                         (TRANSFORM chain applied); ``False`` means the raw
                         Primary envelope.  Ignored on other operations.
    - ``fmt``         — BACKUP only: container format the driver should emit
                         (``parquet``, ``ndjson``, …).  A backup driver that
                         supports multiple formats picks the entry matching
                         the request's ``?format=`` query param.
    """

    driver_id: Immutable[str] = Field(
        ..., min_length=1, description="Driver identifier (e.g. 'postgresql')."
    )

    @field_validator("driver_id", mode="before")
    @classmethod
    def _normalize_driver_id(cls, v: Any) -> Any:
        """Coerce driver_id to snake_case (PR-1e cutover convention).

        Accepts both PascalCase (legacy: ``"ItemsPostgresqlDriver"`` from
        auto-augment helpers + persisted configs predating snake_case) and
        snake_case (current canonical form: ``"items_postgresql_driver"``).
        Both forms are idempotent through ``_to_snake``. Normalising here
        means downstream lookup against ``DriverRegistry`` (which keys by
        snake_case) finds entries regardless of input convention.
        """
        if isinstance(v, str) and v:
            from dynastore.tools.typed_store.base import _to_snake
            return _to_snake(v)
        return v

    hints: Set[str] = Field(
        default_factory=set,
        description="Hints this driver responds to for this operation.",
    )
    on_failure: FailurePolicy = Field(
        default=FailurePolicy.FATAL,
        description="What happens if this driver fails: fatal, warn, or ignore.",
    )
    write_mode: WriteMode = Field(
        default=WriteMode.SYNC,
        description=(
            "Execution mode for WRITE operations.  "
            "'sync' = await result (parallel with other sync drivers, participates "
            "in coordinated rollback).  "
            "'async' = fire-and-forget after sync phase succeeds."
        ),
    )
    sla: Optional[DriverSla] = Field(
        default=None,
        description=(
            "Per-entry SLA override.  When None, falls back to the driver's "
            "class-level SLA (if declared).  Mandatory on TRANSFORM entries "
            "— without an SLA, a transform can quietly tax the hot path."
        ),
    )
    transformed: bool = Field(
        default=False,
        description=(
            "For INDEX / BACKUP entries only: True → ReindexWorker feeds the "
            "transformed envelope (TRANSFORM chain applied); False → raw "
            "Primary envelope.  Ignored on other operations."
        ),
    )
    fmt: Optional[str] = Field(
        default=None,
        description=(
            "BACKUP entries only: container format the driver emits "
            "(e.g. 'parquet', 'ndjson').  The export endpoint picks the entry "
            "whose ``fmt`` matches the request's ``?format=`` query param."
        ),
    )
    source: Literal["operator", "auto"] = Field(
        default="operator",
        description=(
            "Provenance of this entry.  ``operator`` (default) means the "
            "entry was written by an operator via the configs API or "
            "constructed by hand.  ``auto`` means the entry was added by "
            "the routing-config self-register helpers "
            "(``_self_register_indexers_into`` / "
            "``_self_register_searchers_into``) because a discoverable "
            "driver matched the marker / capability gate.  Operators can "
            "remove auto-added entries by writing an explicit operations "
            "dict that omits the driver — the next read won't re-add it "
            "as long as the operator-explicit list contains other entries "
            "(self-register is set-default, never overwrite)."
        ),
    )


class ItemsRoutingConfig(PluginConfig):
    """Operation-based routing for **items** storage drivers.

    Each operation maps to an ordered list of :class:`OperationDriverEntry`.
    Position in the list determines priority (first = primary).

    Items routing dispatches `CollectionItemsStore` drivers (PG, ES, BQ,
    Iceberg, DuckDB) for entity-level operations: WRITE / READ / SEARCH /
    INDEX over collection items / features. **Distinct from**
    :class:`CollectionRoutingConfig` which dispatches
    ``CollectionStore`` drivers for collection-envelope metadata.

    Identity is the class itself; see ``class_key()`` in ``platform_config_service.py``.
    """
    _address: ClassVar[Tuple[str, str, Optional[str]]] = ("storage", "routing", None)
    _visibility: ClassVar[Optional[str]] = "collection"


    model_config = ConfigDict(json_schema_extra=ui(category="routing"))

    operations: Immutable[Dict[str, List[OperationDriverEntry]]] = Field(
        default_factory=lambda: {
            # ES (public) primary, PG secondary. PG is authoritative for WRITE
            # (on_failure=fatal — must succeed); ES is the derived index
            # (on_failure=warn — transient outage doesn't block writes,
            # reindex catches up). READ orders ES first for fast simplified-
            # geometry search; PG carries the `geometry_exact` hint so a
            # consumer can request exact geometries — actual hint-aware
            # dispatch is pending PR-1e (today position 0 wins per :660).
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_id="items_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
                OperationDriverEntry(
                    driver_id="items_elasticsearch_driver",
                    on_failure=FailurePolicy.WARN,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_id="items_elasticsearch_driver",
                    hints={"geometry_simplified"},
                    on_failure=FailurePolicy.WARN,
                ),
                OperationDriverEntry(
                    driver_id="items_postgresql_driver",
                    hints={"geometry_exact"},
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
        },
        description=(
            "Operation → ordered driver list for items dispatch.  "
            "Immutable: to change driver mapping, create a new config.  "
            "Hints and on_failure within entries are mutable."
        ),
    )

    @model_validator(mode="after")
    def _augment_items_with_discoverable_indexers_searchers(
        self,
    ) -> "ItemsRoutingConfig":
        """Auto-folds discoverable :class:`ItemIndexer` drivers into
        ``operations[INDEX]`` and :class:`CollectionItemsStore` drivers
        declaring storage ``Capability.{FULLTEXT, SPATIAL_FILTER,
        ATTRIBUTE_FILTER}`` into ``operations[SEARCH]`` — so a deployed
        ``ItemsElasticsearchDriver`` shows up without operator PUT.
        """
        from dynastore.models.protocols.indexer import ItemIndexer
        from dynastore.models.protocols.storage_driver import (
            CollectionItemsStore,
        )

        try:
            _self_register_indexers_into(self.operations, ItemIndexer)
            _self_register_searchers_into(
                self.operations, CollectionItemsStore,
                search_caps=_items_search_caps(),
            )
            _self_register_transformers_into(self.operations)
        except Exception as exc:
            logger.debug(
                "ItemsRoutingConfig: items-tier read-time self-"
                "register skipped (%s); apply-handler will populate on "
                "next write.", exc,
            )
        return self


class CollectionRoutingConfig(PluginConfig):
    """Operation-based routing for **collection metadata** drivers.

    Dispatches ``CollectionStore`` drivers (PG metadata sidecars,
    ES wrapper for collection envelopes) for collection-envelope CRUD and
    metadata indexing. **Distinct from** :class:`ItemsRoutingConfig` which
    dispatches per-entity items drivers.

    Standard operation keys:

    ``READ`` (``write_mode=first``):
        ``CollectionStore`` backends for metadata persistence
        and search.  First available driver wins.
        Empty → auto-discovery fallback (ES if registered, otherwise PG).

    ``WRITE`` (``write_mode=sync``):
        Primary metadata driver(s) committing in-transaction.  Empty →
        defaults to the Primary PG driver.

    ``TRANSFORM`` (``write_mode=chain``, **lazy**):
        Drivers that enrich collection metadata.  **Not** invoked on default
        read paths — only when an endpoint opts in (e.g. STAC derived fields)
        or when the async reindex pipeline is preparing an envelope for an
        INDEX / BACKUP entry marked ``transformed=true``.  Each entry should
        carry an SLA; see role-based driver plan §Routing.

    ``INDEX`` (optional, async):
        Post-write propagation targets for search-capable sinks (ES, Vertex AI,
        vector DBs).  Each entry declares ``transformed: bool`` to pick raw
        vs transformed envelopes.  When absent, search falls through to the
        Primary's ``SEARCH`` capability.

    ``BACKUP`` (optional, async):
        Post-write propagation targets for export sinks (Parquet via DuckDB,
        NDJSON, …).  Each entry declares ``transformed: bool`` and ``fmt``.
        Serves ``GET .../backup?format=<fmt>`` endpoints.

    Identity is the class itself; see ``class_key()`` in ``platform_config_service.py``.
    """
    _address: ClassVar[Tuple[str, str, Optional[str]]] = ("storage", "routing", None)
    _visibility: ClassVar[Optional[str]] = "collection"


    model_config = ConfigDict(json_schema_extra=ui(category="routing"))

    operations: Immutable[Dict[str, List[OperationDriverEntry]]] = Field(
        default_factory=lambda: {},
        description=(
            "Operation → ordered driver list for metadata routing.  "
            "READ  = first-match metadata drivers (CollectionStore).  "
            "TRANSFORM = lazy enrichers.  "
            "INDEX = async search-sink propagation.  "
            "BACKUP = async export-sink propagation."
        ),
    )

    @model_validator(mode="after")
    def _augment_metadata_with_discoverable_indexers_searchers(
        self,
    ) -> "CollectionRoutingConfig":
        """Auto-folds discoverable :class:`CollectionIndexer` drivers into
        ``operations[INDEX]`` and ``CollectionStore`` SEARCH-capable
        drivers into ``operations[SEARCH]``.
        """
        from dynastore.models.protocols.entity_store import (
            CollectionStore,
        )

        try:
            _self_register_indexers_into(
                self.operations, CollectionIndexer,
            )
            _self_register_searchers_into(
                self.operations, CollectionStore,
            )
            _self_register_transformers_into(self.operations)
        except Exception as exc:
            logger.debug(
                "CollectionRoutingConfig: read-time self-register "
                "skipped (%s); apply-handler will populate on next write.",
                exc,
            )
        return self


class AssetRoutingConfig(PluginConfig):
    """Operation-based routing for asset storage drivers.

    Same structure as :class:`ItemsRoutingConfig` but scoped to
    asset-domain drivers.

    Identity is the class itself; see ``class_key()`` in ``platform_config_service.py``.
    """
    _address: ClassVar[Tuple[str, str, Optional[str]]] = ("storage", "routing", None)
    _visibility: ClassVar[Optional[str]] = "collection"


    model_config = ConfigDict(json_schema_extra=ui(category="routing"))

    operations: Immutable[Dict[str, List[OperationDriverEntry]]] = Field(
        default_factory=lambda: {
            # Mirrors ItemsRoutingConfig: ES (public) primary indexer for
            # READ, PG authoritative for WRITE. See companion memory note +
            # ItemsRoutingConfig.operations comment for rationale.
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_id="asset_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
                OperationDriverEntry(
                    driver_id="asset_elasticsearch_driver",
                    on_failure=FailurePolicy.WARN,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_id="asset_elasticsearch_driver",
                    hints={"geometry_simplified"},
                    on_failure=FailurePolicy.WARN,
                ),
                OperationDriverEntry(
                    driver_id="asset_postgresql_driver",
                    hints={"geometry_exact"},
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
        },
        description=(
            "Operation → ordered driver list for asset drivers. "
            "``operations[INDEX]`` is auto-augmented at validation time "
            "with discoverable AssetIndexer drivers; ``operations[UPLOAD]`` "
            "with discoverable AssetUploadProtocol impls."
        ),
    )

    @model_validator(mode="after")
    def _augment_with_discoverable_indexers(self) -> "AssetRoutingConfig":
        """Asset tier has no SEARCH op today (assets aren't searchable
        the way collection items / catalogs are), so only INDEX + UPLOAD
        are augmented.  Mirrors :meth:`CatalogRoutingConfig._augment_*`."""
        try:
            _self_register_indexers_into(self.operations, AssetIndexer)
            from dynastore.models.protocols.asset_upload import AssetUploadProtocol
            _self_register_upload_into(self.operations, AssetUploadProtocol)
            _self_register_transformers_into(self.operations)
        except Exception as exc:
            logger.debug(
                "AssetRoutingConfig: read-time self-register skipped "
                "(%s); apply-handler will populate on next write.", exc,
            )
        return self


class CatalogRoutingConfig(PluginConfig):
    """Operation-based routing for catalog-level metadata drivers.

    Parallels :class:`ItemsRoutingConfig` but scoped to catalog-tier
    drivers (``CatalogStore`` implementations).  Introduced by the
    role-based driver refactor so catalogs follow the same Primary /
    Transformer / Indexer / Backup pattern as collections.

    M2.1 ships the first two concrete drivers
    (``CatalogCorePostgresqlDriver`` / ``CatalogStacPostgresqlDriver``);
    the defaults below pin both under WRITE and READ so a deployment
    with a catalog containing both CORE and STAC envelopes resolves
    correctly without explicit platform config.

    ``operations`` supports the same keys as collection metadata routing:
    ``WRITE``, ``READ``, ``SEARCH``, ``TRANSFORM``, ``INDEX``, ``BACKUP``.
    See :class:`CollectionRoutingConfig` for per-key semantics.

    Identity is the class itself; see ``class_key()`` in ``platform_config_service.py``.
    """
    _address: ClassVar[Tuple[str, str, Optional[str]]] = ("storage", "routing", None)
    _visibility: ClassVar[Optional[str]] = "catalog"


    operations: Immutable[Dict[str, List[OperationDriverEntry]]] = Field(
        default_factory=lambda: {
            # M2.3b: fan-out across the domain-scoped Primary drivers.
            # CORE is first (matches ``_sort_hooks`` priority: required
            # registry data lands before the optional STAC row).  The
            # catalog-metadata router merges both results on READ and
            # parallelises the upsert on WRITE.
            Operation.WRITE: [
                OperationDriverEntry(driver_id="catalog_core_postgresql_driver"),
                OperationDriverEntry(driver_id="catalog_stac_postgresql_driver"),
            ],
            Operation.READ: [
                OperationDriverEntry(driver_id="catalog_core_postgresql_driver"),
                OperationDriverEntry(driver_id="catalog_stac_postgresql_driver"),
            ],
        },
        description=(
            "Operation → ordered driver list for catalog-tier metadata drivers. "
            "Supports WRITE, READ, SEARCH, TRANSFORM, INDEX, BACKUP. "
            "Default fans out to the CORE + STAC PG Primaries; INDEX and "
            "SEARCH entries are auto-augmented at validation time with "
            "every discoverable CatalogIndexer / SEARCH-capable "
            "CatalogStore (typically CatalogElasticsearchDriver). "
            "Operator-explicit entries take precedence; auto-augmentation "
            "is idempotent set-default behaviour, never overwrite."
        ),
    )

    @model_validator(mode="after")
    def _augment_with_discoverable_indexers_searchers(
        self,
    ) -> "CatalogRoutingConfig":
        """Fold discoverable CatalogIndexer + SEARCH-capable
        CatalogStore drivers into ``operations[INDEX]`` and
        ``operations[SEARCH]`` after model validation.

        Closes the gap where the default-state config (no operator
        write) shows neither INDEX nor SEARCH entries even when an ES
        catalog driver is installed.  Mirrors the apply-handler
        self-registration so default-state and apply-time configs
        converge — ``_on_apply_catalog_routing_config`` calls the same
        helpers, with the same idempotent semantics.
        """
        from dynastore.models.protocols.entity_store import (
            CatalogStore,
        )

        try:
            _self_register_indexers_into(self.operations, CatalogIndexer)
            _self_register_searchers_into(self.operations, CatalogStore)
            _self_register_transformers_into(self.operations)
        except Exception as exc:
            # Discovery may not be ready (e.g. test fixtures that
            # validate routing configs before plugins register).  The
            # apply-handler path is the safety net for those cases.
            logger.debug(
                "CatalogRoutingConfig: read-time self-register skipped "
                "(%s); apply-handler will populate on next write.", exc,
            )
        return self


# ---------------------------------------------------------------------------
# on_apply handlers
# ---------------------------------------------------------------------------


def _validate_routing_entries(
    config: "ItemsRoutingConfig | AssetRoutingConfig | CatalogRoutingConfig",
    driver_index: Dict[str, Any],
    label: str,
) -> None:
    """Shared validation for routing config apply handlers.

    Raises ``ValueError`` on:
    1. Unknown ``driver_id``
    2. Hint not in ``driver.supported_hints``
    3. Operation not supported (derived from driver capabilities)
    4. ``write_mode=async`` on a driver without ``DriverCapability.ASYNC``
    """
    from dynastore.modules.storage.driver_config import DriverCapability

    for operation, entries in config.operations.items():
        for entry in entries:
            # 1. Unknown driver. Warn-and-skip aligns with router.py runtime
            # behaviour: an entry whose driver isn't registered is silently
            # skipped at dispatch time. Validation must match — otherwise
            # config-apply on a subset deployment (test fixture, partial
            # rollout, deprecated driver) hard-fails despite the runtime
            # path being safe.
            driver = driver_index.get(entry.driver_id)
            if driver is None:
                logger.warning(
                    "%s: driver '%s' for operation '%s' is not registered. "
                    "Available: %s. Entry will be skipped at dispatch.",
                    label, entry.driver_id, operation, sorted(driver_index),
                )
                continue

            # 2. Hint validation
            driver_hints = getattr(driver, "supported_hints", frozenset())
            invalid_hints = entry.hints - driver_hints
            if invalid_hints:
                raise ValueError(
                    f"{label}: hints {sorted(invalid_hints)} are not supported "
                    f"by driver '{entry.driver_id}'. "
                    f"Supported: {sorted(driver_hints)}"
                )

            # 3. Operation supported (derived from capabilities)
            driver_caps = getattr(driver, "capabilities", frozenset())
            supported_ops = derive_supported_operations(driver_caps)
            if operation not in supported_ops:
                raise ValueError(
                    f"{label}: driver '{entry.driver_id}' does not support "
                    f"operation '{operation}'. "
                    f"Supported operations: {sorted(supported_ops)} "
                    f"(derived from capabilities: {sorted(driver_caps)})"
                )

            # 4. write_mode compatibility — check DriverCapability.ASYNC
            if entry.write_mode == WriteMode.ASYNC:
                # Resolve driver config via naming convention: snake_case
                # ``<class_name>_config`` (matches PluginConfig.class_key()
                # which snake-cases the bound config class name). Pre-PR-1e
                # this used PascalCase ``ClassName + "Config"`` which silently
                # missed the registry — masked by the broad except below.
                try:
                    from dynastore.modules.db_config.platform_config_service import (
                        resolve_config_class,
                    )

                    driver_config_key = _to_snake(type(driver).__name__ + "Config")
                    driver_cls = resolve_config_class(driver_config_key)
                    if driver_cls is not None:
                        driver_config = driver_cls()
                        config_caps = getattr(driver_config, "capabilities", frozenset())
                        if DriverCapability.ASYNC not in config_caps:
                            raise ValueError(
                                f"{label}: write_mode='async' requires "
                                f"DriverCapability.ASYNC on driver '{entry.driver_id}'. "
                                f"Driver capabilities: {sorted(config_caps)}"
                            )
                except ValueError:
                    raise  # re-raise validation errors
                except Exception:
                    pass  # driver config may not exist — skip check

    # 5. Primary driver capability check
    #    Position 0 in WRITE must support WRITE; position 0 in READ/SEARCH
    #    must support READ.  Warn only — don't hard-fail for forward-compat.
    from dynastore.models.protocols.storage_driver import Capability

    _op_required_cap: Dict[str, str] = {
        Operation.WRITE.value: Capability.WRITE,
        Operation.READ.value: Capability.READ,
        Operation.SEARCH.value: Capability.READ,
    }
    for operation, entries in config.operations.items():
        if not entries:
            continue
        primary_id = entries[0].driver_id
        primary_driver = driver_index.get(primary_id)
        if primary_driver is None:
            continue
        required_cap = _op_required_cap.get(operation)
        if required_cap is None:
            continue
        driver_caps = getattr(primary_driver, "capabilities", frozenset())
        if required_cap not in driver_caps:
            logger.warning(
                "%s: primary driver '%s' for operation '%s' lacks capability '%s'. "
                "This may cause runtime errors.",
                label, primary_id, operation, required_cap,
            )


def _self_register_indexers_into(
    target_ops: Dict[str, List["OperationDriverEntry"]],
    marker_proto: type,
) -> None:
    """Auto-append every installed driver satisfying ``marker_proto`` to
    ``target_ops[INDEX]`` with sensible async defaults
    (``write_mode=async``, ``on_failure=warn``).

    Tier-scoped: caller passes the right marker (``CatalogIndexer`` →
    catalog routing, ``CollectionIndexer`` → collection routing,
    ``AssetIndexer`` → asset routing).  Drivers indexing multiple tiers
    opt in to multiple markers and self-register into each tier's
    ``operations[INDEX]`` independently.

    Idempotent: drivers already listed under ``operations[INDEX]`` are
    not re-appended (operator-supplied entries with custom ``on_failure``
    or ``write_mode`` survive).
    """
    from dynastore.tools.discovery import get_protocols

    listed = {entry.driver_id for entry in target_ops.get(Operation.INDEX, [])}
    for driver in get_protocols(marker_proto):
        driver_id = _to_snake(type(driver).__name__)
        if driver_id in listed:
            continue
        target_ops.setdefault(Operation.INDEX, []).append(
            OperationDriverEntry(
                driver_id=driver_id,
                on_failure=FailurePolicy.WARN,
                write_mode=WriteMode.ASYNC,
                source="auto",
            )
        )
        listed.add(driver_id)
        logger.debug(
            "Routing config self-registration: appended %s indexer '%s' "
            "to operations[INDEX] (write_mode=async, on_failure=warn, source=auto)",
            marker_proto.__name__, driver_id,
        )


def _self_register_upload_into(
    target_ops: Dict[str, List["OperationDriverEntry"]],
    marker_proto: type,
) -> None:
    """Auto-append every installed driver satisfying ``marker_proto`` to
    ``target_ops[UPLOAD]`` with single-driver semantics
    (``write_mode=sync``, ``on_failure=fatal``).

    UPLOAD is single-driver per request — the first entry wins unless the
    caller passes a ``hint``.  Multiple registered backends (e.g. GCS +
    local FS) coexist; operator config decides which one is selected for
    each catalog / collection by reordering the entries or pinning one.

    Idempotent: drivers already listed under ``operations[UPLOAD]`` are
    not re-appended (operator-supplied entries with custom hints survive).
    """
    from dynastore.tools.discovery import get_protocols

    listed = {entry.driver_id for entry in target_ops.get(Operation.UPLOAD, [])}
    for driver in get_protocols(marker_proto):
        driver_id = _to_snake(type(driver).__name__)
        if driver_id in listed:
            continue
        target_ops.setdefault(Operation.UPLOAD, []).append(
            OperationDriverEntry(
                driver_id=driver_id,
                on_failure=FailurePolicy.FATAL,
                write_mode=WriteMode.SYNC,
                source="auto",
            )
        )
        listed.add(driver_id)
        logger.debug(
            "Routing config self-registration: appended %s upload driver "
            "'%s' to operations[UPLOAD] (write_mode=sync, on_failure=fatal, source=auto)",
            marker_proto.__name__, driver_id,
        )


def _self_register_searchers_into(
    target_ops: Dict[str, List["OperationDriverEntry"]],
    marker_proto: type,
    *,
    search_caps: Optional[FrozenSet[str]] = None,
) -> None:
    """Auto-append every installed driver declaring a SEARCH-family
    capability to ``target_ops[SEARCH]``.

    Tier-scoped via two parameters:

    - ``marker_proto`` — the structural Protocol to discover against
      (e.g. ``CatalogStore`` for catalog routing,
      ``CollectionStore`` for collection-tier metadata routing,
      ``CollectionItemsStore`` for items-tier routing).
    - ``search_caps`` — the set of capability strings that qualify a
      driver as a SEARCH provider.  When ``None`` (default) the
      metadata-tier set is used (``EntityStoreCapability.SEARCH``,
      ``SEARCH_FULLTEXT``, ``SEARCH_VECTOR``, ``SEARCH_EXACT``).
      Items-tier callers pass the storage ``Capability`` set
      (``FULLTEXT``, ``SPATIAL_FILTER``, ``ATTRIBUTE_FILTER``) via the
      module-level :data:`ITEMS_SEARCH_CAPS` constant.

    Idempotent — drivers already listed under ``operations[SEARCH]`` are
    not re-appended (operator-supplied entries with custom hints survive).
    """
    from dynastore.tools.discovery import get_protocols

    if search_caps is None:
        from dynastore.models.protocols.entity_store import EntityStoreCapability
        search_caps = frozenset({
            EntityStoreCapability.SEARCH,
            EntityStoreCapability.SEARCH_FULLTEXT,
            EntityStoreCapability.SEARCH_VECTOR,
            EntityStoreCapability.SEARCH_EXACT,
        })
    listed = {entry.driver_id for entry in target_ops.get(Operation.SEARCH, [])}
    for driver in get_protocols(marker_proto):
        driver_caps = getattr(driver, "capabilities", frozenset())
        if not (driver_caps & search_caps):
            continue
        driver_id = _to_snake(type(driver).__name__)
        if driver_id in listed:
            continue
        target_ops.setdefault(Operation.SEARCH, []).append(
            OperationDriverEntry(driver_id=driver_id, source="auto")
        )
        listed.add(driver_id)
        logger.debug(
            "Routing config self-registration: appended %s SEARCH "
            "driver '%s' to operations[SEARCH] (source=auto)",
            marker_proto.__name__, driver_id,
        )


def _self_register_store_drivers(
    config: "CollectionRoutingConfig | CatalogRoutingConfig",
    metadata_driver_index: Dict[str, Any],
    *,
    op_keys: Tuple[str, ...] = (Operation.WRITE, Operation.READ),
) -> None:
    """Auto-append every installed store driver missing from ``operations[op]``.

    Closes the "implicit fan-out, invisible to operators" antipattern:
    every protocol-installed driver participates in WRITE/READ unless an
    operator explicitly drops it after the auto-append fires (in which
    case they at least had to see the entry to remove it).

    Mutates ``config`` in place — `Immutable[Dict[...]]` is enforced at the
    Pydantic field level (you can't reassign the dict), but the contents
    are still appendable.  Called from the apply handlers below.
    """
    target_ops = config.operations
    for op in op_keys:
        listed = {entry.driver_id for entry in target_ops.get(op, [])}
        for driver_id in metadata_driver_index:
            if driver_id in listed:
                continue
            target_ops.setdefault(op, []).append(
                OperationDriverEntry(driver_id=driver_id, source="auto")
            )
            logger.debug(
                "Routing config self-registration: appended installed "
                "metadata driver '%s' to operations[%s] (source=auto)",
                driver_id, op,
            )


async def _on_apply_items_routing_config(
    config: ItemsRoutingConfig,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """Called after items routing config is written.

    Validates driver_id, hints, operations, write_mode for items dispatch
    entries (``CollectionItemsStore`` drivers); auto-registers discoverable
    ``ItemIndexer`` drivers and SEARCH-capable items drivers; invalidates
    the router cache.

    NOTE: ensure_storage() for collection WRITE/READ drivers is intentionally
    NOT called here. It is invoked by the collection-creation flow
    (CollectionService._create_collection_internal step 6) on the write driver,
    which is the only correct point because the ItemsPostgresqlDriverConfig
    (physical_table, sidecars) must be fully resolved before storage is
    provisioned.
    """
    from dynastore.models.protocols.indexer import ItemIndexer
    from dynastore.models.protocols.storage_driver import CollectionItemsStore
    from dynastore.tools.discovery import get_protocols

    driver_index = {_to_snake(type(d).__name__): d for d in get_protocols(CollectionItemsStore)}
    _validate_routing_entries(config, driver_index, "Items routing config")

    # Items-tier: auto-register ItemIndexer drivers + items-tier SEARCH-capable
    # CollectionItemsStore drivers — apply-handler parity with the read-time
    # model_validator so operator PUTs also pick up auto-augmentation.
    _self_register_indexers_into(config.operations, ItemIndexer)
    _self_register_searchers_into(
        config.operations, CollectionItemsStore,
        search_caps=_items_search_caps(),
    )

    # Invalidate router cache
    try:
        from dynastore.modules.storage.router import invalidate_router_cache

        invalidate_router_cache(catalog_id, collection_id)
    except Exception:
        pass


async def _on_apply_collection_routing_config(
    config: CollectionRoutingConfig,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """Called after collection-metadata routing config is written.

    Validates entries against the ``CollectionStore`` registry,
    auto-registers installed metadata drivers (READ/WRITE), auto-registers
    discoverable ``CollectionIndexer`` and SEARCH-capable
    ``CollectionStore`` drivers, and calls ``ensure_storage()`` on
    READ drivers.

    The collection-metadata router is cache-free (pure discovery fan-out);
    nothing to invalidate after this apply.
    """
    from dynastore.models.protocols.entity_store import CollectionStore
    from dynastore.models.protocols.storage_driver import CollectionItemsStore
    from dynastore.tools.discovery import get_protocols

    driver_index = {_to_snake(type(d).__name__): d for d in get_protocols(CollectionItemsStore)}
    metadata_driver_index = {_to_snake(type(d).__name__): d for d in get_protocols(CollectionStore)}

    # Auto-register installed store drivers (WRITE/READ) so operators
    # reading ``/configs/...`` see every driver that will run; no implicit
    # fan-out behind the config's back.
    _self_register_store_drivers(config, metadata_driver_index)

    # Validate operations[READ] (CollectionStore drivers)
    for entry in config.operations.get(Operation.READ, []):
        if entry.driver_id not in metadata_driver_index:
            raise ValueError(
                f"Collection metadata routing config: operations[READ] driver "
                f"'{entry.driver_id}' is not registered. "
                f"Available: {sorted(metadata_driver_index)}"
            )

    # Validate operations[TRANSFORM] (CollectionItemsStore drivers — they
    # contribute item-derived metadata at READ time)
    for entry in config.operations.get(Operation.TRANSFORM, []):
        if entry.driver_id not in driver_index:
            raise ValueError(
                f"Collection metadata routing config: operations[TRANSFORM] driver "
                f"'{entry.driver_id}' is not registered. "
                f"Available: {sorted(driver_index)}"
            )

    # Validate operations[INDEX] and [BACKUP] entries (CollectionStore
    # search/export sinks — ES for INDEX, Parquet/DuckDB for BACKUP).
    for op in (Operation.INDEX, Operation.BACKUP):
        for entry in config.operations.get(op, []):
            if entry.driver_id not in metadata_driver_index:
                raise ValueError(
                    f"Collection metadata routing config: operations[{op}] driver "
                    f"'{entry.driver_id}' is not registered. "
                    f"Available: {sorted(metadata_driver_index)}"
                )
            if op == Operation.BACKUP and not entry.fmt:
                logger.info(
                    "Collection metadata routing config: BACKUP entry '%s' "
                    "has no fmt; endpoint ?format= query will not be able to "
                    "target it.",
                    entry.driver_id,
                )

    # Auto-register discoverable indexers + searchers — apply-handler parity
    # with the read-time model_validator on CollectionRoutingConfig.
    _self_register_indexers_into(config.operations, CollectionIndexer)
    _self_register_searchers_into(config.operations, CollectionStore)

    # Call ensure_storage() on READ drivers (idempotent, catalog-scoped).
    if catalog_id:
        for entry in config.operations.get(Operation.READ, []):
            driver = metadata_driver_index.get(entry.driver_id)
            if driver is None:
                continue
            try:
                await driver.ensure_storage(catalog_id)
            except Exception as exc:
                logger.warning(
                    "ensure_storage failed for metadata driver '%s' on catalog '%s': %s",
                    entry.driver_id, catalog_id, exc,
                )


async def _on_apply_asset_routing_config(
    config: AssetRoutingConfig,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """Called after asset routing config is written."""
    from dynastore.models.protocols.asset_driver import AssetStore
    from dynastore.tools.discovery import get_protocols

    driver_index = {_to_snake(type(d).__name__): d for d in get_protocols(AssetStore)}
    _validate_routing_entries(config, driver_index, "Asset routing config")

    # Auto-register installed AssetIndexer drivers under operations[INDEX].
    _self_register_indexers_into(config.operations, AssetIndexer)

    # Auto-register installed AssetUploadProtocol impls under operations[UPLOAD].
    from dynastore.models.protocols.asset_upload import AssetUploadProtocol
    _self_register_upload_into(config.operations, AssetUploadProtocol)

    # Invalidate router cache
    try:
        from dynastore.modules.storage.router import invalidate_asset_router_cache

        invalidate_asset_router_cache(catalog_id, collection_id)
    except Exception:
        pass

    # Call ensure_storage() on all referenced asset drivers (idempotent).
    if catalog_id and collection_id:
        seen_ids: set[str] = set()
        for entries in config.operations.values():
            for entry in entries:
                seen_ids.add(entry.driver_id)
        for did in seen_ids:
            driver = driver_index.get(did)
            if driver is None:
                continue
            try:
                await driver.ensure_storage(catalog_id, collection_id)
            except Exception as exc:
                logger.warning(
                    "ensure_storage failed for asset driver '%s' on %s/%s: %s",
                    did, catalog_id, collection_id, exc,
                )


async def _on_apply_catalog_routing_config(
    config: CatalogRoutingConfig,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """Called after catalog routing config is written.

    Validates ``driver_id``, hints, and operation capability for every entry in
    ``config.operations`` against the ``CatalogStore`` driver registry.
    Mirrors :func:`_on_apply_routing_config` for the catalog tier.

    INDEX / BACKUP entries are validated against the same registry — role is a
    config assignment, not a driver-internal contract (see role-based driver
    plan §Routing).
    """
    from dynastore.models.protocols.entity_store import CatalogStore
    from dynastore.tools.discovery import get_protocols

    driver_index = {_to_snake(type(d).__name__): d for d in get_protocols(CatalogStore)}
    _self_register_store_drivers(config, driver_index)
    _validate_routing_entries(config, driver_index, "Catalog routing config")

    # Auto-register installed CatalogIndexer drivers under operations[INDEX]
    # and SEARCH-capable CatalogStore drivers under operations[SEARCH]
    # for parity with the read-time validator on CatalogRoutingConfig.
    _self_register_indexers_into(config.operations, CatalogIndexer)
    _self_register_searchers_into(config.operations, CatalogStore)

    # Catalog router cache invalidation is wired in M2 when `catalog_router.py`
    # lands.  Until then, config changes are picked up on the next resolution
    # because there's no catalog-tier router to cache.


# Register handlers on the config classes themselves.
_HandlerSig = Callable[[PluginConfig, Optional[str], Optional[str], Optional[Any]], Any]
ItemsRoutingConfig.register_apply_handler(cast(_HandlerSig, _on_apply_items_routing_config))
CollectionRoutingConfig.register_apply_handler(cast(_HandlerSig, _on_apply_collection_routing_config))
AssetRoutingConfig.register_apply_handler(cast(_HandlerSig, _on_apply_asset_routing_config))
CatalogRoutingConfig.register_apply_handler(cast(_HandlerSig, _on_apply_catalog_routing_config))


# ---------------------------------------------------------------------------
# Generic routing-active query helpers — entity-agnostic discovery layer
# ---------------------------------------------------------------------------
#
# Used by every module / OGC service that needs to know which driver(s) are
# active for a given operation on a given entity. Read the routing config
# (single SSOT) and apply the resolution semantics documented at the top
# of this file:
#
#   - INDEX:     fan-out across every listed driver (write side).
#   - SEARCH:    single-driver. Default = first; driver_hint overrides.
#   - TRANSFORM: ordered chain. Composition runtime in
#                modules/storage/transform_runtime.py applies it.
#
# Entity-agnostic: parameterised by ``entity`` ∈ {item, collection, catalog,
# asset}. Each entity reads from a different config:
#
#   item       → ItemsRoutingConfig.operations
#   collection → ItemsRoutingConfig.metadata.operations
#   catalog    → CatalogRoutingConfig.operations
#   asset      → AssetRoutingConfig.operations
# ---------------------------------------------------------------------------

EntityKindLiteral = Literal["item", "collection", "catalog", "asset"]


async def _resolve_entity_operations(
    catalog_id: str,
    *,
    entity: EntityKindLiteral,
    collection_id: Optional[str] = None,
) -> Dict[str, List["OperationDriverEntry"]]:
    """Return the active operations dict for the (entity, catalog, collection).

    Reads the appropriate config class via ConfigsProtocol. Returns an empty
    dict when no config is registered (caller treats as "no drivers active").
    """
    from dynastore.models.protocols.configs import ConfigsProtocol
    from dynastore.tools.discovery import get_protocol

    configs = get_protocol(ConfigsProtocol)
    if not configs:
        return {}

    try:
        if entity == "item":
            if not collection_id:
                return {}
            cfg = await configs.get_config(
                ItemsRoutingConfig,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
            if isinstance(cfg, ItemsRoutingConfig):
                return cfg.operations
        elif entity == "collection":
            if not collection_id:
                return {}
            cfg = await configs.get_config(
                CollectionRoutingConfig,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
            if isinstance(cfg, CollectionRoutingConfig):
                return cfg.operations
        elif entity == "catalog":
            cfg = await configs.get_config(
                CatalogRoutingConfig,
                catalog_id=catalog_id,
            )
            if isinstance(cfg, CatalogRoutingConfig):
                return cfg.operations
        elif entity == "asset":
            cfg = await configs.get_config(
                AssetRoutingConfig,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
            if isinstance(cfg, AssetRoutingConfig):
                return cfg.operations
    except Exception as exc:
        logger.debug(
            "routing_config.resolve: lookup failed for entity=%s catalog=%s "
            "collection=%s: %s",
            entity, catalog_id, collection_id, exc,
        )
    return {}


async def get_active_indexers(
    catalog_id: str,
    *,
    entity: EntityKindLiteral,
    collection_id: Optional[str] = None,
) -> Set[str]:
    """driver_ids of all drivers in operations[INDEX] for this entity.

    Multi-driver fan-out: write side has no merge ambiguity, every listed
    indexer fires. Returns empty set when no INDEX entries exist.
    """
    ops = await _resolve_entity_operations(
        catalog_id, entity=entity, collection_id=collection_id,
    )
    return {entry.driver_id for entry in ops.get(Operation.INDEX, [])}


async def get_search_driver(
    catalog_id: str,
    *,
    entity: EntityKindLiteral,
    collection_id: Optional[str] = None,
    driver_hint: Optional[str] = None,
) -> Optional[str]:
    """driver_id of the driver to use for SEARCH on this entity.

    Single-driver semantics: default = first entry in operations[SEARCH].
    When ``driver_hint`` is given AND present in the routing list, returns
    that driver. When the hint is given but NOT in the list, logs a
    warning and falls back to the default.

    Returns ``None`` when no SEARCH entry is registered.
    """
    ops = await _resolve_entity_operations(
        catalog_id, entity=entity, collection_id=collection_id,
    )
    entries = ops.get(Operation.SEARCH, [])
    if not entries:
        return None

    if driver_hint:
        listed = {e.driver_id for e in entries}
        if driver_hint in listed:
            return driver_hint
        logger.warning(
            "get_search_driver: driver_hint=%r not in operations[SEARCH] "
            "for entity=%s catalog=%s collection=%s; falling back to default. "
            "Available: %s",
            driver_hint, entity, catalog_id, collection_id, sorted(listed),
        )

    return entries[0].driver_id


async def get_active_transformers(
    catalog_id: str,
    *,
    entity: EntityKindLiteral,
    collection_id: Optional[str] = None,
) -> List[Any]:
    """Ordered list of EntityTransformProtocol instances active for this entity.

    Resolves driver_ids in operations[TRANSFORM] (in order) against the
    discovered EntityTransformProtocol implementers. Drivers listed in
    routing but not currently registered are skipped with a debug log.

    Empty list when no TRANSFORM entries exist; the transform runtime treats
    an empty chain as identity.
    """
    from dynastore.models.protocols.entity_transform import EntityTransformProtocol
    from dynastore.tools.discovery import get_protocols

    ops = await _resolve_entity_operations(
        catalog_id, entity=entity, collection_id=collection_id,
    )
    entries = ops.get(Operation.TRANSFORM, [])
    if not entries:
        return []

    by_driver_id = {_to_snake(type(t).__name__): t for t in get_protocols(EntityTransformProtocol)}
    chain: List[Any] = []
    for entry in entries:
        transformer = by_driver_id.get(entry.driver_id)
        if transformer is None:
            logger.debug(
                "get_active_transformers: routing lists '%s' for entity=%s "
                "catalog=%s collection=%s but no EntityTransformProtocol "
                "implementer registered with that class name; skipping. "
                "Available: %s",
                entry.driver_id, entity, catalog_id, collection_id,
                sorted(by_driver_id),
            )
            continue
        chain.append(transformer)
    return chain


def supported_operations(driver: Any) -> FrozenSet[str]:
    """Return the set of :class:`Operation` values this driver participates in.

    Combines the two discovery axes used by the platform:

    1. **Capability-derived operations** — ``derive_supported_operations(
       driver.capabilities)`` translates the driver's ``Capability`` flag set
       into the operations those flags qualify it for (WRITE / READ / SEARCH
       / INDEX / BACKUP).
    2. **Protocol-derived operations** — operations whose participation is
       expressed by implementing a Protocol rather than by setting a flag.
       Currently only ``Operation.TRANSFORM`` (via
       :class:`EntityTransformProtocol`); other future operations may follow
       the same pattern when they have no variant capabilities.

    The asymmetry is intentional: capabilities express variant subsets within
    one structural type (a ``CollectionItemsStore`` may support FULLTEXT but
    not SPATIAL_FILTER); a Protocol expresses an identity with no variants
    (a transformer is a transformer). See
    ``project_geoid_driver_responsibilities.md`` for the full rationale.

    Drivers MAY expose a ``supported_operations`` property forwarding to
    this helper; it is not required.
    """
    from dynastore.models.protocols.entity_transform import EntityTransformProtocol

    caps = getattr(driver, "capabilities", frozenset())
    ops: Set[str] = set(derive_supported_operations(caps))
    if isinstance(driver, EntityTransformProtocol):
        ops.add(Operation.TRANSFORM)
    return frozenset(ops)


def _self_register_transformers_into(
    target_ops: Dict[str, List["OperationDriverEntry"]],
) -> None:
    """Auto-append every installed ``EntityTransformProtocol`` implementer
    to ``target_ops[TRANSFORM]``.

    Mirrors :func:`_self_register_indexers_into` and
    :func:`_self_register_searchers_into`. Idempotent — drivers already
    listed under ``operations[TRANSFORM]`` are not re-appended (operator
    overrides survive).

    Discovery is purely structural: any driver implementing
    ``EntityTransformProtocol`` is eligible. There is no separate capability
    flag — the protocol IS the marker.
    """
    from dynastore.models.protocols.entity_transform import EntityTransformProtocol
    from dynastore.tools.discovery import get_protocols

    listed = {entry.driver_id for entry in target_ops.get(Operation.TRANSFORM, [])}
    for transformer in get_protocols(EntityTransformProtocol):
        driver_id = _to_snake(type(transformer).__name__)
        if driver_id in listed:
            continue
        target_ops.setdefault(Operation.TRANSFORM, []).append(
            OperationDriverEntry(driver_id=driver_id, source="auto")
        )
        listed.add(driver_id)
        logger.debug(
            "Routing config self-registration: appended EntityTransformProtocol "
            "driver '%s' to operations[TRANSFORM] (source=auto)",
            driver_id,
        )
