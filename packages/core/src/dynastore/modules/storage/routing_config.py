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
from dynastore.models.mutability import Immutable, Mutable
from dynastore.modules.db_config.plugin_config import PluginConfig
from dynastore.modules.storage.hints import Hint
from dynastore.tools.typed_store.base import _to_snake
from dynastore.tools.ui_hints import ui

logger = logging.getLogger(__name__)


class FailurePolicy(StrEnum):
    """Per-driver failure behaviour within an operation.

    ``OUTBOX`` is the production-grade durability policy for INDEX entries:
    on synchronous failure (or when the per-indexer circuit breaker is
    open) the dispatcher persists an ``_meta.index_outbox`` row in the
    same PG transaction as the upstream data write.  A background worker
    drains it with exponential backoff.  PG TX commit guarantees neither
    the data nor the obligation-to-index can ever be lost.

    Selection guide:
        ``FATAL``   — caller rolls back if this driver fails.  Use for
                       indexers whose divergence from the source of truth
                       is unacceptable (regulated audit, billing).
        ``OUTBOX``  — eventual consistency, never lost.  Use for the
                       common case of search-backend propagation.
        ``WARN``    — best-effort; failures logged.  Use only for
                       non-critical sinks (telemetry, analytics).
        ``IGNORE``  — silent skip.  Reserved for opt-in development
                       experiments.
    """

    FATAL = "fatal"      # operation fails if this driver fails
    OUTBOX = "outbox"    # on failure, enqueue _meta.index_outbox row in same TX
    WARN = "warn"        # log warning, continue with other drivers
    IGNORE = "ignore"    # silently skip on failure


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
        Capability.READ: {Operation.READ, Operation.SEARCH},
        Capability.EXPORT: {Operation.BACKUP},
    }
    ops: Set[str] = set()
    for cap in capabilities:
        if cap in mapping:
            ops.update(mapping[cap])
    return frozenset(ops)


# ---------------------------------------------------------------------------
# Config models
# ---------------------------------------------------------------------------


class OperationDriverEntry(BaseModel):
    """A driver configured for a specific operation.

    ``driver_ref`` is immutable — changing which drivers participate in
    an operation is a structural decision.  ``hints`` and ``on_failure``
    are mutable preferences that can evolve without structural impact.

    Cycle F.3 renamed the field from ``driver_id`` to ``driver_ref`` to
    align with the F.0-F.2 ``engine_ref`` naming.  Single-instance-per-
    kind: the ref equals the snake_case driver class name (e.g.
    ``"items_postgresql_driver"``).  Multi-instance refs (Cycle F.4c)
    let operators name driver instances explicitly (e.g. ``pg_lean``
    vs ``pg_full``); the routing entry's ref is what the F.4c.2
    ``get_config_by_ref`` lookup keys on at dispatch time.

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

    driver_ref: Immutable[str] = Field(
        ..., min_length=1, description="Driver reference (e.g. 'items_postgresql_driver')."
    )

    @field_validator("driver_ref", mode="before")
    @classmethod
    def _normalize_driver_ref(cls, v: Any) -> Any:
        """Coerce driver_ref to snake_case (PR-1e + F.3 cutover convention).

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

    hints: Set[Hint] = Field(
        default_factory=set,
        description=(
            "Hints this driver responds to for this operation.  Members are "
            "from the canonical ``Hint`` catalogue "
            "(``modules/storage/hints.py``); raw strings still validate via "
            "``StrEnum`` coercion, but unknown strings are rejected at "
            "config-write time so typos surface early."
        ),
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
    input_transformers: Tuple[str, ...] = Field(
        default_factory=tuple,
        description=(
            "Ordered transformer ``driver_ref``s applied to entities going "
            "INTO this driver call. The chain runs left-to-right: each "
            "transformer receives the previous transformer's output. Every "
            "ref must also appear in ``operations[TRANSFORM]`` of the same "
            "routing config — the validator rejects dangling references at "
            "config-build time. Wired hops in this release: ``INDEX``. "
            "Declaring this on other operations emits a one-time WARN "
            "because the hop is not yet active."
        ),
    )
    output_transformers: Tuple[str, ...] = Field(
        default_factory=tuple,
        description=(
            "Ordered transformer ``driver_ref``s applied to entities coming "
            "OUT of this driver call. The inverse chain runs right-to-left "
            "so the output shape matches the client expectation. Same "
            "validation rule as ``input_transformers``. Wired hops in this "
            "release: ``SEARCH``. Declaring this on other operations emits "
            "a one-time WARN."
        ),
    )

    @field_validator("input_transformers", "output_transformers", mode="before")
    @classmethod
    def _normalize_transformer_refs(cls, v: Any) -> Any:
        if v is None:
            return ()
        if isinstance(v, str):
            return (_to_snake(v),)
        if isinstance(v, (list, tuple)):
            return tuple(_to_snake(item) if isinstance(item, str) and item else item for item in v)
        return v


# Operations whose transformer hop is wired in this release. Declaring
# input_transformers / output_transformers on any other (operation, side)
# pair logs a one-time WARN so operators see the silent-no-op early.
_WIRED_INPUT_HOPS: FrozenSet[str] = frozenset({Operation.INDEX})
_WIRED_OUTPUT_HOPS: FrozenSet[str] = frozenset({Operation.SEARCH})
_DEFERRED_HOP_WARNED: Set[Tuple[str, str, str]] = set()


def _warn_deferred_transformer_hops(
    operations: Dict[str, List["OperationDriverEntry"]],
    config_label: str,
) -> None:
    for op_name, entries in operations.items():
        for entry in entries:
            if entry.input_transformers and op_name not in _WIRED_INPUT_HOPS:
                key = (op_name, entry.driver_ref, "input")
                if key not in _DEFERRED_HOP_WARNED:
                    _DEFERRED_HOP_WARNED.add(key)
                    logger.warning(
                        "%s: input_transformers declared on operation '%s' "
                        "for driver '%s' but the %s input-transformer hop "
                        "is not yet wired in this release — declaration is "
                        "a no-op. Wired input hops: %s.",
                        config_label, op_name, entry.driver_ref, op_name,
                        sorted(_WIRED_INPUT_HOPS),
                    )
            if entry.output_transformers and op_name not in _WIRED_OUTPUT_HOPS:
                key = (op_name, entry.driver_ref, "output")
                if key not in _DEFERRED_HOP_WARNED:
                    _DEFERRED_HOP_WARNED.add(key)
                    logger.warning(
                        "%s: output_transformers declared on operation '%s' "
                        "for driver '%s' but the %s output-transformer hop "
                        "is not yet wired in this release — declaration is "
                        "a no-op. Wired output hops: %s.",
                        config_label, op_name, entry.driver_ref, op_name,
                        sorted(_WIRED_OUTPUT_HOPS),
                    )


def _validate_transformer_attachment(
    operations: Dict[str, List["OperationDriverEntry"]],
    config_label: str,
) -> None:
    """Every ref under ``input_transformers`` / ``output_transformers``
    must also appear as a ``driver_ref`` in ``operations[TRANSFORM]``.
    Raises ``ValueError`` listing the dangling refs.
    """
    transform_refs = {
        entry.driver_ref for entry in operations.get(Operation.TRANSFORM, [])
    }
    dangling: List[str] = []
    for op_name, entries in operations.items():
        for entry in entries:
            for ref in entry.input_transformers:
                if ref not in transform_refs:
                    dangling.append(
                        f"{op_name}/{entry.driver_ref}/input_transformers:{ref}"
                    )
            for ref in entry.output_transformers:
                if ref not in transform_refs:
                    dangling.append(
                        f"{op_name}/{entry.driver_ref}/output_transformers:{ref}"
                    )
    if dangling:
        raise ValueError(
            f"{config_label}: transformer driver_ref(s) {dangling} listed in "
            f"input_transformers/output_transformers do not appear in "
            f"operations[TRANSFORM]. Register them as TRANSFORM entries "
            f"(or remove the attachment)."
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
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "items", "routing")
    _visibility: ClassVar[Optional[str]] = "collection"


    model_config = ConfigDict(json_schema_extra=ui(category="routing"))

    operations: Immutable[Dict[str, List[OperationDriverEntry]]] = Field(
        default_factory=lambda: {
            # PG is authoritative for WRITE (on_failure=fatal — must succeed,
            # write_mode=sync — caller awaits the result).
            #
            # ES is a secondary WRITE sink with ASYNC + OUTBOX semantics:
            # the dispatcher's sync phase enqueues an outbox row in the same
            # PG transaction as the data write, then a background drain
            # task pumps the row through the ES driver with retry +
            # exponential backoff.  PG TX commit guarantees neither the
            # data nor the obligation-to-index can be lost.  Putting ES
            # in WRITE with OUTBOX policy is the production-grade
            # replacement for the legacy per-item listener
            # (``_on_item_upsert``); the listener's ``_is_write_driver_for``
            # guard now hits and the listener self-skips when ES is
            # listed here, so there is no double-indexing.
            #
            # See ``feedback_es_indexing_per_item_async_not_bulk.md`` for
            # the historical rationale that produced the listener; the
            # outbox drain task supersedes it.
            #
            # READ is **hint-selected**, not chained: ``get_driver``
            # returns the FIRST entry whose ``hints`` match the caller's
            # ``hint=`` (router.py:289 ``resolved[0]``). ES carries
            # ``GEOMETRY_SIMPLIFIED`` (default fast path); PG carries
            # ``GEOMETRY_EXACT`` / ``TILES`` and is only reached when the
            # consumer asks for that hint. There is no runtime fallback
            # from ES to PG on empty results — an empty ES response is
            # treated as success-with-zero-rows (item_query.py
            # ``_try_driver_dispatch`` returns a non-None ``QueryResponse``
            # with an empty stream). #914 surfaces the consequence: silent
            # upstream indexing failures are invisible at the read path.
            #
            # SEARCH lists ES then PG, but ``get_driver`` still picks the
            # first registered match. The PG entry is a bootstrap-time
            # tie-breaker for when ES driver registration is missing
            # (operator misconfigured deployment), NOT a runtime fallback
            # for ES returning empty. Keeping the explicit pair pins
            # driver order against the auto-discovery dispatch in
            # ``_self_register_searchers_into``, which would otherwise
            # register drivers in arbitrary order.
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                    source="auto",
                ),
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_driver",
                    write_mode=WriteMode.ASYNC,
                    on_failure=FailurePolicy.OUTBOX,
                    source="auto",
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_driver",
                    hints={Hint.GEOMETRY_SIMPLIFIED},
                    on_failure=FailurePolicy.WARN,
                    source="auto",
                ),
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                    hints={Hint.GEOMETRY_EXACT, Hint.TILES},
                    on_failure=FailurePolicy.FATAL,
                    source="auto",
                ),
            ],
            Operation.SEARCH: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_driver",
                    on_failure=FailurePolicy.FATAL,
                    source="auto",
                ),
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                    source="auto",
                ),
            ],
        },
        description=(
            "Operation → ordered driver list for items dispatch.  "
            "Immutable: to change driver mapping, create a new config.  "
            "Hints and on_failure within entries are mutable.  "
            "operations[INDEX] is the source of truth for the items-tier "
            "INDEX hop on item upsert/delete (OGC ingest path through "
            "item_service._dispatch_index_upsert -> IndexDispatcher, and "
            "item_query soft-delete). Pinning a private indexer here is "
            "what the privacy-cascade validator enforces; the entry-aware "
            "default resolver picks this config for entity_type='item'. "
            "See un-fao/GeoID#810 (Option B)."
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
            _self_register_searchers_into(self.operations, CollectionItemsStore)
            _self_register_transformers_into(self.operations)
        except Exception as exc:
            logger.debug(
                "ItemsRoutingConfig: items-tier read-time self-"
                "register skipped (%s); apply-handler will populate on "
                "next write.", exc,
            )
        _validate_transformer_attachment(self.operations, "ItemsRoutingConfig")
        _warn_deferred_transformer_hops(self.operations, "ItemsRoutingConfig")
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
        Primary ``CollectionStore`` driver(s) committing in-transaction.  Empty →
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
    # Collection-envelope routing — 2-tuple under storage (no items/assets fork).
    # CollectionStore drivers are structurally distinct from items-tier drivers,
    # so this routing config lands at ``storage.routing.{class_key}`` rather
    # than under an items/assets sibling.
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "routing")
    _visibility: ClassVar[Optional[str]] = "collection"


    model_config = ConfigDict(json_schema_extra=ui(category="routing"))

    operations: Immutable[Dict[str, List[OperationDriverEntry]]] = Field(
        default_factory=lambda: {
            # Collection-envelope routing. The PG collection driver
            # (collection_postgresql_driver — internally fans CRUD across
            # the collection_core + collection_stac sidecars) is the
            # system of record: primary for both WRITE and READ.
            # Elasticsearch is the *index* — populated asynchronously via
            # the INDEX hop (OUTBOX-durable) and fronting SEARCH. PG is
            # the SEARCH fallback so a deploy without ES still answers
            # collection search from the authoritative store.
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                    source="auto",
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                    source="auto",
                ),
            ],
            Operation.INDEX: [
                OperationDriverEntry(
                    driver_ref="collection_elasticsearch_driver",
                    write_mode=WriteMode.ASYNC,
                    on_failure=FailurePolicy.OUTBOX,
                    source="auto",
                ),
            ],
            Operation.SEARCH: [
                # ES is the primary search backend (fast, simplified
                # geometry). PG is the fallback AND the exact-geometry
                # path: a consumer needing full-precision geometry passes
                # hint=Hint.GEOMETRY_EXACT to route SEARCH to PG.
                OperationDriverEntry(
                    driver_ref="collection_elasticsearch_driver",
                    hints={Hint.GEOMETRY_SIMPLIFIED},
                    source="auto",
                ),
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    hints={Hint.GEOMETRY_EXACT},
                    source="auto",
                ),
            ],
        },
        description=(
            "Operation -> ordered driver list for collection-tier routing. "
            "WRITE/READ = collection_postgresql_driver (system of record). "
            "INDEX = async OUTBOX-durable propagation to Elasticsearch. "
            "SEARCH = Elasticsearch primary (geometry_simplified), "
            "PostgreSQL fallback (geometry_exact). "
            "TRANSFORM/BACKUP entries are auto-augmented at validation time."
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
        _validate_transformer_attachment(self.operations, "CollectionRoutingConfig")
        _warn_deferred_transformer_hops(self.operations, "CollectionRoutingConfig")
        return self


class AssetRoutingConfig(PluginConfig):
    """Operation-based routing for asset storage drivers.

    Same structure as :class:`ItemsRoutingConfig` but scoped to
    asset-domain drivers.

    Identity is the class itself; see ``class_key()`` in ``platform_config_service.py``.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "assets", "routing")
    _visibility: ClassVar[Optional[str]] = "collection"


    model_config = ConfigDict(json_schema_extra=ui(category="routing"))

    operations: Immutable[Dict[str, List[OperationDriverEntry]]] = Field(
        default_factory=lambda: {
            # Assets routing: PG is the canonical system of record;
            # Elasticsearch is the *index* (search/geo/faceted query
            # side), not a parallel storage tier.
            #
            # WRITE: PG primary (FATAL — gates operation success); ES
            # secondary via ASYNC + OUTBOX so a transient ES write
            # failure is durably retried instead of being lost as a
            # single WARN. Matches the items-tier shape at
            # ``ItemsRoutingConfig.operations`` (lines 479–488).
            #
            # READ: PG primary (FATAL). Asset identity / existence /
            # direct-lookup paths (e.g. ``asset_manager.get_asset``)
            # must consult the SOR first — ES-first turned a swallowed
            # ES write failure into a fatal "Asset not found" ingestion
            # raise (#620). ES remains a secondary at WARN with
            # ``GEOMETRY_SIMPLIFIED`` so search-style consumers can opt
            # in via ``get_driver(..., hint=...)``.
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="asset_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                    source="auto",
                ),
                OperationDriverEntry(
                    driver_ref="asset_elasticsearch_driver",
                    write_mode=WriteMode.ASYNC,
                    on_failure=FailurePolicy.OUTBOX,
                    source="auto",
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="asset_postgresql_driver",
                    hints={Hint.GEOMETRY_EXACT},
                    on_failure=FailurePolicy.FATAL,
                    source="auto",
                ),
                OperationDriverEntry(
                    driver_ref="asset_elasticsearch_driver",
                    hints={Hint.GEOMETRY_SIMPLIFIED},
                    on_failure=FailurePolicy.WARN,
                    source="auto",
                ),
            ],
        },
        description=(
            "Operation → ordered driver list for asset drivers. "
            "PG is the system of record (FATAL for both WRITE primary "
            "and READ primary); ES is the index (ASYNC/OUTBOX on WRITE, "
            "WARN secondary on READ for simplified-geometry hints). "
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
        _validate_transformer_attachment(self.operations, "AssetRoutingConfig")
        _warn_deferred_transformer_hops(self.operations, "AssetRoutingConfig")
        return self


class CatalogRoutingConfig(PluginConfig):
    """Operation-based routing for catalog-tier ``CatalogStore`` drivers.

    Parallels :class:`ItemsRoutingConfig` but scoped to catalog-tier
    drivers (``CatalogStore`` implementations).  Introduced by the
    role-based driver refactor so catalogs follow the same Primary /
    Transformer / Indexer / Backup pattern as collections.

    The registered ``CatalogStore`` is ``CatalogPostgresqlDriver`` — a
    composition wrapper that fans CRUD across the ``catalog_core`` and
    ``catalog_stac`` PG sidecars internally. The defaults below pin it
    under WRITE and READ so a deployment resolves correctly without
    explicit platform config.

    ``operations`` supports the same keys as :class:`CollectionRoutingConfig`:
    ``WRITE``, ``READ``, ``SEARCH``, ``TRANSFORM``, ``INDEX``, ``BACKUP``.
    See that class for per-key semantics, with one trigger difference:
    INDEX entries on this config are consumed by
    :class:`~dynastore.modules.catalog.reindex_worker.ReindexWorker` off
    the ``catalog_metadata_changed`` event stream — they are NOT
    invoked directly from ``catalog_router`` the way
    ``CollectionRoutingConfig.operations[INDEX]`` is invoked from
    ``collection_router._dispatch_collection_index``.  Both end at the
    same Indexer drivers through OUTBOX-durable plumbing; the asymmetry
    is in *how* the hop is triggered, not in what runs.  See the
    "Catalog INDEX hop" section in
    ``modules/catalog/catalog_router.py``'s module docstring.

    Identity is the class itself; see ``class_key()`` in ``platform_config_service.py``.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "routing")
    _visibility: ClassVar[Optional[str]] = "catalog"


    operations: Immutable[Dict[str, List[OperationDriverEntry]]] = Field(
        default_factory=lambda: {
            # catalog_postgresql_driver is the registered CatalogStore
            # composition wrapper — it fans CRUD across the catalog_core +
            # catalog_stac PG sidecars internally. It is the system of
            # record (FATAL) for both WRITE and READ. INDEX propagates to
            # Elasticsearch asynchronously (OUTBOX-durable). INDEX is also
            # auto-augmented at validation time with discoverable
            # CatalogIndexer drivers; the explicit entry keeps the default
            # config self-contained.
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="catalog_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                    source="auto",
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="catalog_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                    source="auto",
                ),
            ],
            Operation.INDEX: [
                OperationDriverEntry(
                    driver_ref="catalog_elasticsearch_driver",
                    write_mode=WriteMode.ASYNC,
                    on_failure=FailurePolicy.OUTBOX,
                    source="auto",
                ),
            ],
        },
        description=(
            "Operation -> ordered driver list for catalog-tier CatalogStore "
            "drivers. WRITE/READ = catalog_postgresql_driver (system of "
            "record). INDEX = async OUTBOX-durable propagation to "
            "Elasticsearch; auto-augmented at validation time with every "
            "discoverable CatalogIndexer. Operator-explicit entries take "
            "precedence; auto-augmentation is idempotent set-default."
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
        _validate_transformer_attachment(self.operations, "CatalogRoutingConfig")
        _warn_deferred_transformer_hops(self.operations, "CatalogRoutingConfig")
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
    1. Unknown ``driver_ref``
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
            driver = driver_index.get(entry.driver_ref)
            if driver is None:
                logger.warning(
                    "%s: driver '%s' for operation '%s' is not registered. "
                    "Available: %s. Entry will be skipped at dispatch.",
                    label, entry.driver_ref, operation, sorted(driver_index),
                )
                continue

            # 2. Hint validation
            driver_hints = getattr(driver, "supported_hints", frozenset())
            invalid_hints = entry.hints - driver_hints
            if invalid_hints:
                raise ValueError(
                    f"{label}: hints {sorted(invalid_hints)} are not supported "
                    f"by driver '{entry.driver_ref}'. "
                    f"Supported: {sorted(driver_hints)}"
                )

            # 3. Operation supported (derived from capabilities)
            driver_caps = getattr(driver, "capabilities", frozenset())
            supported_ops = derive_supported_operations(driver_caps)
            if operation not in supported_ops:
                raise ValueError(
                    f"{label}: driver '{entry.driver_ref}' does not support "
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
                    from dynastore.modules.db_config.plugin_config import resolve_config_class

                    driver_config_key = _to_snake(type(driver).__name__ + "Config")
                    driver_cls = resolve_config_class(driver_config_key)
                    if driver_cls is not None:
                        driver_config = driver_cls()
                        config_caps = getattr(driver_config, "capabilities", frozenset())
                        if DriverCapability.ASYNC not in config_caps:
                            raise ValueError(
                                f"{label}: write_mode='async' requires "
                                f"DriverCapability.ASYNC on driver '{entry.driver_ref}'. "
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
        primary_id = entries[0].driver_ref
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


def _is_operator_managed(
    target_ops: Dict[str, List["OperationDriverEntry"]],
    op: str,
) -> bool:
    """Return True when any entry in ``target_ops[op]`` is operator-source.

    Under the list-level operator-override semantic (#889): once an
    operator has touched an operation's driver list, the self-register
    helpers must not append further entries. Boot-time defaults are
    marked ``source="auto"`` so a fresh config still auto-registers
    discoverable drivers; the moment an operator PUTs an explicit
    operations dict, the list is treated as operator-managed and the
    helpers become a no-op for that operation.
    """
    return any(
        entry.source == "operator" for entry in target_ops.get(op, [])
    )


def _self_register_indexers_into(
    target_ops: Dict[str, List["OperationDriverEntry"]],
    marker_proto: type,
) -> None:
    """Auto-append every installed driver satisfying ``marker_proto`` to
    ``target_ops[INDEX]`` with durable async defaults
    (``write_mode=async``, ``on_failure=outbox``).

    Tier-scoped: caller passes the right marker (``CatalogIndexer`` →
    catalog routing, ``CollectionIndexer`` → collection routing,
    ``AssetIndexer`` → asset routing).  Drivers indexing multiple tiers
    opt in to multiple markers and self-register into each tier's
    ``operations[INDEX]`` independently.

    The ``OUTBOX`` default mirrors the WRITE-side ES default: a transient
    indexer failure enqueues a row in ``_meta.index_outbox`` (same PG
    transaction as the upstream write) for the drain task to retry,
    instead of dropping the obligation with a log line.  Operators who
    want a non-durable best-effort sink can override per-entry to
    ``WARN``.

    Operator-override: a no-op when ``target_ops[INDEX]`` contains any
    entry with ``source="operator"`` — operator-managed lists are
    invariant under auto-augmentation (#792 / #889).
    """
    from dynastore.tools.discovery import get_protocols

    if _is_operator_managed(target_ops, Operation.INDEX):
        return
    listed = {entry.driver_ref for entry in target_ops.get(Operation.INDEX, [])}
    for driver in get_protocols(marker_proto):
        # Single gate on the per-Operation auto-default set.  Drivers
        # explicitly declare which Operations they auto-default into via
        # ``auto_register_for_routing: ClassVar[FrozenSet[Operation]]``;
        # ``Operation.INDEX`` membership opts the driver in here.
        # Empty (default) = explicit-pin only.
        opt_in: FrozenSet[str] = getattr(type(driver), "auto_register_for_routing", frozenset())
        if Operation.INDEX not in opt_in:
            continue
        driver_ref = _to_snake(type(driver).__name__)
        if driver_ref in listed:
            continue
        target_ops.setdefault(Operation.INDEX, []).append(
            OperationDriverEntry(
                driver_ref=driver_ref,
                on_failure=FailurePolicy.OUTBOX,
                write_mode=WriteMode.ASYNC,
                source="auto",
            )
        )
        listed.add(driver_ref)
        logger.debug(
            "Routing config self-registration: appended %s indexer '%s' "
            "to operations[INDEX] (write_mode=async, on_failure=outbox, source=auto)",
            marker_proto.__name__, driver_ref,
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

    Operator-override: a no-op when ``target_ops[UPLOAD]`` contains any
    entry with ``source="operator"`` — operator-managed lists are
    invariant under auto-augmentation (#792 / #889).
    """
    from dynastore.tools.discovery import get_protocols

    if _is_operator_managed(target_ops, Operation.UPLOAD):
        return
    listed = {entry.driver_ref for entry in target_ops.get(Operation.UPLOAD, [])}
    for driver in get_protocols(marker_proto):
        driver_ref = _to_snake(type(driver).__name__)
        if driver_ref in listed:
            continue
        target_ops.setdefault(Operation.UPLOAD, []).append(
            OperationDriverEntry(
                driver_ref=driver_ref,
                on_failure=FailurePolicy.FATAL,
                write_mode=WriteMode.SYNC,
                source="auto",
            )
        )
        listed.add(driver_ref)
        logger.debug(
            "Routing config self-registration: appended %s upload driver "
            "'%s' to operations[UPLOAD] (write_mode=sync, on_failure=fatal, source=auto)",
            marker_proto.__name__, driver_ref,
        )


def _self_register_searchers_into(
    target_ops: Dict[str, List["OperationDriverEntry"]],
    marker_proto: type,
) -> None:
    """Auto-append every installed driver opting into SEARCH to
    ``target_ops[SEARCH]``.

    Single gate on the per-Operation auto-default set: a driver is
    auto-augmented into ``operations[SEARCH]`` only if its class
    declares ``Operation.SEARCH`` in
    ``auto_register_for_routing: ClassVar[FrozenSet[Operation]]``.
    Capability-based gating (``FULLTEXT`` / ``SPATIAL_FILTER`` / …)
    has been retired; capabilities are now structural facts only,
    while which read-flavours a driver serves is expressed through
    ``supported_hints``.

    Tier-scoped via ``marker_proto`` — the structural Protocol to
    discover against (``CatalogStore`` for catalog routing,
    ``CollectionStore`` for collection-tier metadata routing,
    ``CollectionItemsStore`` for items-tier routing).

    Operator-override: a no-op when ``target_ops[SEARCH]`` contains any
    entry with ``source="operator"`` — operator-managed lists are
    invariant under auto-augmentation (#792 / #889).
    """
    from dynastore.tools.discovery import get_protocols

    if _is_operator_managed(target_ops, Operation.SEARCH):
        return
    listed = {entry.driver_ref for entry in target_ops.get(Operation.SEARCH, [])}
    for driver in get_protocols(marker_proto):
        opt_in: FrozenSet[str] = getattr(type(driver), "auto_register_for_routing", frozenset())
        if Operation.SEARCH not in opt_in:
            continue
        driver_ref = _to_snake(type(driver).__name__)
        if driver_ref in listed:
            continue
        target_ops.setdefault(Operation.SEARCH, []).append(
            OperationDriverEntry(driver_ref=driver_ref, source="auto")
        )
        listed.add(driver_ref)
        logger.debug(
            "Routing config self-registration: appended %s SEARCH "
            "driver '%s' to operations[SEARCH] (source=auto)",
            marker_proto.__name__, driver_ref,
        )


def _self_register_store_drivers(
    config: "CollectionRoutingConfig | CatalogRoutingConfig",
    store_driver_index: Dict[str, Any],
    *,
    op_keys: Tuple[str, ...] = (Operation.WRITE, Operation.READ),
) -> None:
    """Auto-append every installed store driver missing from ``operations[op]``.

    Closes the "implicit fan-out, invisible to operators" antipattern:
    every protocol-installed driver participates in WRITE/READ unless an
    operator explicitly drops it after the auto-append fires (in which
    case they at least had to see the entry to remove it).

    Operator-override (per-operation): for each ``op`` in ``op_keys``,
    if ``operations[op]`` contains any entry with ``source="operator"``,
    that operation is treated as operator-managed and skipped. Other
    operations in ``op_keys`` are still augmented independently (#889).

    Mutates ``config`` in place — `Immutable[Dict[...]]` is enforced at the
    Pydantic field level (you can't reassign the dict), but the contents
    are still appendable.  Called from the apply handlers below.
    """
    target_ops = config.operations
    for op in op_keys:
        if _is_operator_managed(target_ops, op):
            continue
        listed = {entry.driver_ref for entry in target_ops.get(op, [])}
        for driver_ref in store_driver_index:
            if driver_ref in listed:
                continue
            target_ops.setdefault(op, []).append(
                OperationDriverEntry(driver_ref=driver_ref, source="auto")
            )
            logger.debug(
                "Routing config self-registration: appended installed "
                "metadata driver '%s' to operations[%s] (source=auto)",
                driver_ref, op,
            )


async def _validate_items_routing_config(
    config: ItemsRoutingConfig,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """Validate-phase handler for items routing config (#738).

    Validates driver_ref, hints, operations, write_mode for items dispatch
    entries (``CollectionItemsStore`` drivers) and auto-registers
    discoverable ``ItemIndexer`` drivers and SEARCH-capable items drivers.

    Runs PRE-PERSIST: a failure here propagates as HTTP 4xx and the upsert
    is rolled back.  The ``_self_register_*`` calls mutate ``config.operations``
    in place — running them pre-upsert means the auto-registered
    ``source="auto"`` entries are actually persisted (they were silently
    dropped when this ran post-upsert).
    """
    from dynastore.models.protocols.indexer import ItemIndexer
    from dynastore.models.protocols.storage_driver import CollectionItemsStore
    from dynastore.tools.discovery import get_protocols

    driver_index = {_to_snake(type(d).__name__): d for d in get_protocols(CollectionItemsStore)}
    _validate_routing_entries(config, driver_index, "Items routing config")

    # Items-tier: auto-register ItemIndexer drivers (gated on
    # ``Operation.INDEX in driver.auto_register_for_routing``) +
    # ``CollectionItemsStore`` drivers opting into ``Operation.SEARCH``
    # — parity with the read-time model_validator so operator PUTs also
    # pick up auto-augmentation.
    _self_register_indexers_into(config.operations, ItemIndexer)
    _self_register_searchers_into(config.operations, CollectionItemsStore)


async def _on_apply_items_routing_config(
    config: ItemsRoutingConfig,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """Apply-phase handler for items routing config — side effects only.

    Invalidates the router cache and syncs the catalog-wide DENY policy.
    Validation + self-registration moved to ``_validate_items_routing_config``
    (the validate phase) in #738/#747.

    NOTE: ensure_storage() for collection WRITE/READ drivers is intentionally
    NOT called here. It is invoked by the collection-creation flow
    (CollectionService._create_collection_internal step 6) on the write driver,
    which is the only correct point because the ItemsPostgresqlDriverConfig
    (physical_table, sidecars) must be fully resolved before storage is
    provisioned.
    """
    # Invalidate router cache
    try:
        from dynastore.modules.storage.router import invalidate_router_cache

        invalidate_router_cache(catalog_id, collection_id)
    except Exception:
        pass

    # Auto-fire catalog-wide DENY when this routing pins (or removes) the
    # private items driver. Idempotent — `_apply_deny_policy` re-registers
    # the same `private_deny_{cat}` policy. Skips when no catalog scope.
    if catalog_id:
        await _sync_deny_policy_for_catalog(config, catalog_id)


async def _sync_deny_policy_for_catalog(
    new_routing: "ItemsRoutingConfig", catalog_id: str,
) -> None:
    """Apply or revoke the catalog-wide DENY policy after an items
    routing-config write, depending on whether the catalog still has any
    private collection.

    Issue #480 — covers the missing trigger between provisioning
    (``ensure_storage``) and cold-boot scan (``_restore_deny_policies``):
    flipping an existing public catalog's items routing to pin the private
    driver did not previously install the DENY.
    """
    from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
        ItemsElasticsearchPrivateDriver,
    )

    try:
        if _items_routing_has_private_driver(new_routing):
            await ItemsElasticsearchPrivateDriver._apply_deny_policy(catalog_id)
            return

        from dynastore.models.protocols import CatalogsProtocol
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        catalogs_proto = get_protocol(CatalogsProtocol)
        configs_proto = get_protocol(ConfigsProtocol)
        if catalogs_proto is None or configs_proto is None:
            return
        if not await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
            catalogs_proto, configs_proto, catalog_id,
        ):
            await ItemsElasticsearchPrivateDriver._revoke_deny_policy(catalog_id)
    except Exception as exc:
        logger.warning(
            "routing_config: DENY sync failed for catalog %r after items "
            "routing write: %s (recoverable on next ensure_storage / cold boot)",
            catalog_id, exc,
        )


async def _validate_collection_routing_config(
    config: CollectionRoutingConfig,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """Validate-phase handler for collection-metadata routing config (#738).

    Validates entries against the ``CollectionStore`` registry and
    auto-registers installed metadata drivers (READ/WRITE) plus
    discoverable ``CollectionIndexer`` and SEARCH-capable
    ``CollectionStore`` drivers.  Runs PRE-PERSIST so the
    ``_self_register_*`` ``source="auto"`` entries persist and a bad
    driver_ref propagates as HTTP 4xx.
    """
    from dynastore.models.protocols.entity_store import CollectionStore
    from dynastore.models.protocols.storage_driver import CollectionItemsStore
    from dynastore.tools.discovery import get_protocols

    driver_index = {_to_snake(type(d).__name__): d for d in get_protocols(CollectionItemsStore)}
    store_driver_index = {_to_snake(type(d).__name__): d for d in get_protocols(CollectionStore)}

    # Auto-register installed store drivers (WRITE/READ) so operators
    # reading ``/configs/...`` see every driver that will run; no implicit
    # fan-out behind the config's back.
    _self_register_store_drivers(config, store_driver_index)

    # Validate operations[READ] (CollectionStore drivers)
    for entry in config.operations.get(Operation.READ, []):
        if entry.driver_ref not in store_driver_index:
            raise ValueError(
                f"Collection metadata routing config: operations[READ] driver "
                f"'{entry.driver_ref}' is not registered. "
                f"Available: {sorted(store_driver_index)}"
            )

    # Validate operations[TRANSFORM] (CollectionItemsStore drivers — they
    # contribute item-derived metadata at READ time)
    for entry in config.operations.get(Operation.TRANSFORM, []):
        if entry.driver_ref not in driver_index:
            raise ValueError(
                f"Collection metadata routing config: operations[TRANSFORM] driver "
                f"'{entry.driver_ref}' is not registered. "
                f"Available: {sorted(driver_index)}"
            )

    # Validate operations[INDEX] and [BACKUP] entries (CollectionStore
    # search/export sinks — ES for INDEX, Parquet/DuckDB for BACKUP).
    for op in (Operation.INDEX, Operation.BACKUP):
        for entry in config.operations.get(op, []):
            if entry.driver_ref not in store_driver_index:
                raise ValueError(
                    f"Collection metadata routing config: operations[{op}] driver "
                    f"'{entry.driver_ref}' is not registered. "
                    f"Available: {sorted(store_driver_index)}"
                )
            if op == Operation.BACKUP and not entry.fmt:
                logger.info(
                    "Collection metadata routing config: BACKUP entry '%s' "
                    "has no fmt; endpoint ?format= query will not be able to "
                    "target it.",
                    entry.driver_ref,
                )

    # Auto-register discoverable indexers + searchers — parity with the
    # read-time model_validator on CollectionRoutingConfig.
    _self_register_indexers_into(config.operations, CollectionIndexer)
    _self_register_searchers_into(config.operations, CollectionStore)


async def _on_apply_collection_routing_config(
    config: CollectionRoutingConfig,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """Apply-phase handler for collection-metadata routing config — side
    effects only.

    Calls ``ensure_storage()`` on READ drivers (idempotent, catalog-scoped).
    Validation + self-registration moved to
    ``_validate_collection_routing_config`` in #738/#747.  The
    collection-metadata router is cache-free, so there's nothing else to do.
    """
    if not catalog_id:
        return
    from dynastore.models.protocols.entity_store import CollectionStore
    from dynastore.tools.discovery import get_protocols

    store_driver_index = {_to_snake(type(d).__name__): d for d in get_protocols(CollectionStore)}
    for entry in config.operations.get(Operation.READ, []):
        driver = store_driver_index.get(entry.driver_ref)
        if driver is None:
            continue
        try:
            await driver.ensure_storage(catalog_id)
        except Exception as exc:
            logger.warning(
                "ensure_storage failed for metadata driver '%s' on catalog '%s': %s",
                entry.driver_ref, catalog_id, exc,
            )


async def _validate_asset_routing_config(
    config: AssetRoutingConfig,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """Validate-phase handler for asset routing config (#738).

    Validates entries against the ``AssetStore`` registry and auto-registers
    discoverable ``AssetIndexer`` + ``AssetUploadProtocol`` drivers.  Runs
    PRE-PERSIST so the ``source="auto"`` entries persist and a bad
    driver_ref / hint propagates as HTTP 4xx.
    """
    from dynastore.models.protocols.asset_driver import AssetStore
    from dynastore.models.protocols.asset_upload import AssetUploadProtocol
    from dynastore.tools.discovery import get_protocols

    driver_index = {_to_snake(type(d).__name__): d for d in get_protocols(AssetStore)}
    _validate_routing_entries(config, driver_index, "Asset routing config")

    # Auto-register installed AssetIndexer drivers under operations[INDEX].
    _self_register_indexers_into(config.operations, AssetIndexer)

    # Auto-register installed AssetUploadProtocol impls under operations[UPLOAD].
    _self_register_upload_into(config.operations, AssetUploadProtocol)


async def _on_apply_asset_routing_config(
    config: AssetRoutingConfig,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """Apply-phase handler for asset routing config — side effects only.

    Invalidates the asset router cache and calls ``ensure_storage()`` on
    referenced asset drivers.  Validation + self-registration moved to
    ``_validate_asset_routing_config`` in #738/#747.
    """
    from dynastore.models.protocols.asset_driver import AssetStore
    from dynastore.tools.discovery import get_protocols

    # Invalidate router cache
    try:
        from dynastore.modules.storage.router import invalidate_asset_router_cache

        invalidate_asset_router_cache(catalog_id, collection_id)
    except Exception:
        pass

    # Call ensure_storage() on all referenced asset drivers (idempotent).
    if catalog_id and collection_id:
        driver_index = {_to_snake(type(d).__name__): d for d in get_protocols(AssetStore)}
        seen_ids: set[str] = set()
        for entries in config.operations.values():
            for entry in entries:
                seen_ids.add(entry.driver_ref)
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


async def _validate_catalog_routing_config(
    config: CatalogRoutingConfig,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """Validate-phase handler for catalog routing config (#738).

    Validates ``driver_ref``, hints, and operation capability for every entry in
    ``config.operations`` against the ``CatalogStore`` driver registry, and
    auto-registers installed store drivers + ``CatalogIndexer`` /
    SEARCH-capable ``CatalogStore`` drivers.  Runs PRE-PERSIST.

    INDEX / BACKUP entries are validated against the same registry — role is a
    config assignment, not a driver-internal contract (see role-based driver
    plan §Routing).

    There is no catalog-tier apply handler: the catalog router is cache-free
    until ``catalog_router.py`` lands (M2), so once validation + self-register
    have shaped the config, the upsert is all that remains.
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


# Register handlers on the config classes themselves (#738/#747 — the
# three-phase lifecycle).  Validate handlers run pre-persist and propagate;
# apply handlers run post-persist and are best-effort side effects.
_HandlerSig = Callable[[PluginConfig, Optional[str], Optional[str], Optional[Any]], Any]
ItemsRoutingConfig.register_validate_handler(cast(_HandlerSig, _validate_items_routing_config))
ItemsRoutingConfig.register_apply_handler(cast(_HandlerSig, _on_apply_items_routing_config))
CollectionRoutingConfig.register_validate_handler(cast(_HandlerSig, _validate_collection_routing_config))
CollectionRoutingConfig.register_apply_handler(cast(_HandlerSig, _on_apply_collection_routing_config))
AssetRoutingConfig.register_validate_handler(cast(_HandlerSig, _validate_asset_routing_config))
AssetRoutingConfig.register_apply_handler(cast(_HandlerSig, _on_apply_asset_routing_config))
CatalogRoutingConfig.register_validate_handler(cast(_HandlerSig, _validate_catalog_routing_config))


# ---------------------------------------------------------------------------
# Privacy cascade (#733 — routing-config driven)
# ---------------------------------------------------------------------------
#
# Cascade rule:
#   collection-private REQUIRES items-private.  Reverse direction allowed
#   (items-private + collection-public is a real deployment shape — public
#   collection list with private item geometry).  Items-public + collection-
#   private is rejected: it would leak item geometry on /search bypassing
#   the per-collection DENY policy on the collection envelope index.
#
# Detection (privacy is now a property of routing configs — no more
# ``CollectionPrivacy.is_private`` flag):
#   - Items privacy:      presence of ``items_elasticsearch_private_driver``
#                         in any operation of ``ItemsRoutingConfig.operations``.
#   - Collection privacy: presence of ``collection_elasticsearch_private_driver``
#                         in any operation of ``CollectionRoutingConfig.operations``.
#
# Enforcement points (both registered as validate handlers on the
# routing configs themselves — pure intra-routing invariant):
#   1. On ``CollectionRoutingConfig``: if the new routing pins the
#      collection-private driver, the sibling ``ItemsRoutingConfig`` MUST
#      pin the items-private driver in some operation.
#   2. On ``ItemsRoutingConfig``: if the new routing drops the items-private
#      driver, the sibling ``CollectionRoutingConfig`` must not still pin
#      the collection-private driver.


_PRIVATE_ITEMS_DRIVER_ID = "items_elasticsearch_private_driver"
_PRIVATE_COLLECTION_DRIVER_ID = "collection_elasticsearch_private_driver"


def _items_routing_has_private_driver(routing: "ItemsRoutingConfig") -> bool:
    """Return True iff ``items_elasticsearch_private_driver`` is pinned in
    any operation of the given items routing config."""
    for entries in routing.operations.values():
        for entry in entries:
            if entry.driver_ref == _PRIVATE_ITEMS_DRIVER_ID:
                return True
    return False


def _collection_routing_has_private_driver(
    routing: "CollectionRoutingConfig",
) -> bool:
    """Return True iff ``collection_elasticsearch_private_driver`` is pinned
    in any operation of the given collection routing config."""
    for entries in routing.operations.values():
        for entry in entries:
            if entry.driver_ref == _PRIVATE_COLLECTION_DRIVER_ID:
                return True
    return False


async def _enforce_items_routing_privacy_cascade(
    config: PluginConfig,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """Validate-handler on ``ItemsRoutingConfig`` — reject when items
    routing drops the items-private driver while the sibling
    ``CollectionRoutingConfig`` still pins the collection-private driver.
    """
    if not isinstance(config, ItemsRoutingConfig):
        return
    if not catalog_id or not collection_id:
        return  # platform / catalog scope — N/A
    if _items_routing_has_private_driver(config):
        return  # routing keeps the private driver — cascade satisfied

    from dynastore.models.protocols.configs import ConfigsProtocol
    from dynastore.tools.discovery import get_protocol

    configs = get_protocol(ConfigsProtocol)
    if configs is None:
        return
    coll_routing = await configs.get_config(
        CollectionRoutingConfig,
        catalog_id=catalog_id,
        collection_id=collection_id,
    )
    if not isinstance(coll_routing, CollectionRoutingConfig):
        return
    if _collection_routing_has_private_driver(coll_routing):
        raise ValueError(
            f"Privacy cascade violation: collection {catalog_id!r}/{collection_id!r} "
            f"pins {_PRIVATE_COLLECTION_DRIVER_ID!r} in its CollectionRoutingConfig, "
            f"but the ItemsRoutingConfig being applied does not pin "
            f"{_PRIVATE_ITEMS_DRIVER_ID!r}. Re-add the private items driver "
            f"to operations[INDEX] (and SEARCH/READ as appropriate), or drop "
            f"{_PRIVATE_COLLECTION_DRIVER_ID!r} from the CollectionRoutingConfig first."
        )


async def _enforce_collection_routing_privacy_cascade(
    config: PluginConfig,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """Validate-handler on ``CollectionRoutingConfig`` — reject when a
    collection routing pins the collection-private driver but the sibling
    ``ItemsRoutingConfig`` does not pin the items-private driver.
    """
    if not isinstance(config, CollectionRoutingConfig):
        return
    if not catalog_id or not collection_id:
        return  # platform / catalog scope — N/A
    if not _collection_routing_has_private_driver(config):
        return  # public collection routing — no constraint

    from dynastore.models.protocols.configs import ConfigsProtocol
    from dynastore.tools.discovery import get_protocol

    configs = get_protocol(ConfigsProtocol)
    if configs is None:
        # Discovery not ready (early test fixture, partial deployment) —
        # the items-routing cascade handler catches the violation next
        # time items routing is written.
        return
    items_routing = await configs.get_config(
        ItemsRoutingConfig,
        catalog_id=catalog_id,
        collection_id=collection_id,
    )
    if not isinstance(items_routing, ItemsRoutingConfig):
        return
    if not _items_routing_has_private_driver(items_routing):
        raise ValueError(
            f"Privacy cascade violation: CollectionRoutingConfig for "
            f"{catalog_id!r}/{collection_id!r} pins "
            f"{_PRIVATE_COLLECTION_DRIVER_ID!r}, but the sibling ItemsRoutingConfig "
            f"does not pin {_PRIVATE_ITEMS_DRIVER_ID!r} in any operation. "
            f"Pin the private items driver in operations[INDEX] (and SEARCH/READ "
            f"as appropriate) first, or drop {_PRIVATE_COLLECTION_DRIVER_ID!r} "
            f"from the CollectionRoutingConfig."
        )


# Cascade registrations — both safe at module-load time because each
# handler refers only to classes defined in this module.
ItemsRoutingConfig.register_validate_handler(
    cast(_HandlerSig, _enforce_items_routing_privacy_cascade),
)
CollectionRoutingConfig.register_validate_handler(
    cast(_HandlerSig, _enforce_collection_routing_privacy_cascade),
)


# ---------------------------------------------------------------------------
# Catalog-level routing defaults seeded onto new collections (#733)
# ---------------------------------------------------------------------------


class CatalogRoutingDefaults(BaseModel):
    """Optional routing templates seeded onto newly-created collections.

    Embedded inside :class:`CatalogPrivacy.collection_defaults` (see
    ``modules/catalog/catalog_config.py``). When either template is set,
    ``apply_catalog_default_routing_seed`` writes it as the per-collection
    config at collection-create time.

    Defined here (in ``routing_config.py``) rather than in
    ``catalog_config.py`` to avoid the catalog→routing import cycle:
    ``CatalogPrivacy`` imports this type from us, and our cascade
    handlers don't need to know anything about catalog-tier configs.

    Replaces the deleted ``CollectionPrivacyDefaults`` (whose only field
    was ``is_private: bool``) — privacy is now expressed as the presence
    of private drivers in the routing templates themselves.
    """

    items_routing: Mutable[Optional["ItemsRoutingConfig"]] = Field(
        default=None,
        description=(
            "Template seeded as the per-collection ``ItemsRoutingConfig`` "
            "when a new collection is created in this catalog. When the "
            "template pins ``items_elasticsearch_private_driver`` in any "
            "operation the collection is created as items-private."
        ),
    )

    collection_routing: Mutable[Optional["CollectionRoutingConfig"]] = Field(
        default=None,
        description=(
            "Template seeded as the per-collection ``CollectionRoutingConfig`` "
            "when a new collection is created in this catalog. When the "
            "template pins ``collection_elasticsearch_private_driver`` in any "
            "operation the collection envelope is routed to the per-tenant "
            "private collections index."
        ),
    )


CatalogRoutingDefaults.model_rebuild()


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
#   collection → CollectionRoutingConfig.operations
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
    return {entry.driver_ref for entry in ops.get(Operation.INDEX, [])}


async def get_search_driver(
    catalog_id: str,
    *,
    entity: EntityKindLiteral,
    collection_id: Optional[str] = None,
    driver_hint: Optional[str] = None,
) -> Optional[str]:
    """driver_ref of the driver to use for SEARCH on this entity.

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
        listed = {e.driver_ref for e in entries}
        if driver_hint in listed:
            return driver_hint
        logger.warning(
            "get_search_driver: driver_hint=%r not in operations[SEARCH] "
            "for entity=%s catalog=%s collection=%s; falling back to default. "
            "Available: %s",
            driver_hint, entity, catalog_id, collection_id, sorted(listed),
        )

    return entries[0].driver_ref


async def get_output_transformers_for_search(
    catalog_id: str,
    *,
    entity: EntityKindLiteral,
    collection_id: Optional[str] = None,
    driver_ref: str,
) -> List[Any]:
    """Resolve the ``output_transformers`` declared on the SEARCH entry for
    ``driver_ref`` into live :class:`EntityTransformProtocol` instances.

    Used by SEARCH-side drivers to wrap each hit through
    :func:`restore_transform_chain` so the client-facing shape is the
    inverse of what the indexer wrote. Returns an empty list when no
    matching SEARCH entry exists or when none of its
    ``output_transformers`` resolve to registered instances.
    """
    from dynastore.models.protocols.entity_transform import EntityTransformProtocol
    from dynastore.tools.discovery import get_protocols

    ops = await _resolve_entity_operations(
        catalog_id, entity=entity, collection_id=collection_id,
    )
    search_entries = ops.get(Operation.SEARCH, [])
    target_refs: Tuple[str, ...] = ()
    for entry in search_entries:
        if entry.driver_ref == driver_ref:
            target_refs = entry.output_transformers
            break
    if not target_refs:
        return []
    by_driver_id = {
        _to_snake(type(t).__name__): t
        for t in get_protocols(EntityTransformProtocol)
    }
    chain: List[Any] = []
    for ref in target_refs:
        transformer = by_driver_id.get(ref)
        if transformer is None:
            logger.debug(
                "get_output_transformers_for_search: routing lists '%s' "
                "for entity=%s catalog=%s collection=%s but no "
                "EntityTransformProtocol implementer registered with that "
                "class name; skipping.",
                ref, entity, catalog_id, collection_id,
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
    (a transformer is a transformer).

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
    :func:`_self_register_searchers_into`. Operator-override: a no-op
    when ``target_ops[TRANSFORM]`` contains any entry with
    ``source="operator"`` — operator-managed lists are invariant under
    auto-augmentation (#792 / #889).

    Discovery is purely structural: any driver implementing
    ``EntityTransformProtocol`` is eligible. There is no separate capability
    flag — the protocol IS the marker.
    """
    from dynastore.models.protocols.entity_transform import EntityTransformProtocol
    from dynastore.tools.discovery import get_protocols

    if _is_operator_managed(target_ops, Operation.TRANSFORM):
        return
    listed = {entry.driver_ref for entry in target_ops.get(Operation.TRANSFORM, [])}
    for transformer in get_protocols(EntityTransformProtocol):
        driver_ref = _to_snake(type(transformer).__name__)
        if driver_ref in listed:
            continue
        target_ops.setdefault(Operation.TRANSFORM, []).append(
            OperationDriverEntry(driver_ref=driver_ref, source="auto")
        )
        listed.add(driver_ref)
        logger.debug(
            "Routing config self-registration: appended EntityTransformProtocol "
            "driver '%s' to operations[TRANSFORM] (source=auto)",
            driver_ref,
        )
