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
from typing import Any, Callable, ClassVar, Dict, FrozenSet, List, Literal, Mapping, Optional, Sequence, Set, Tuple, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from dynastore.models.protocols.driver_roles import DriverSla
from dynastore.models.protocols.indexer import (
    AssetIndexer,
    CatalogIndexer,
    CollectionIndexer,
)
from dynastore.models.mutability import Immutable, Mutable
from dynastore.models.plugin_config import PluginConfig
from dynastore.modules.storage.hints import Hint
from dynastore.tools.typed_store.base import _to_snake
from dynastore.tools.ui_hints import ui

logger = logging.getLogger(__name__)


class FailurePolicy(StrEnum):
    """Per-driver failure behaviour within an operation.

    ``OUTBOX`` is the production-grade durability policy for secondary-index
    WRITE entries: on synchronous failure (or when the per-indexer circuit breaker is
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
    - WRITE     : primary driver(s) committing in-transaction plus any
                  secondary indexes. A secondary index is a WRITE entry whose
                  driver implements the tier's Indexer role
                  (``is_*_indexer``); it is propagated async (write_mode=async,
                  on_failure=outbox) by the ReindexWorker. Role — not a
                  distinct operation — distinguishes primary from index (see
                  ``secondary_index_entries``).

    Entity transformers are **not** an operation. They live in the
    sibling ``transformers`` registry (a tuple of
    :class:`TransformerEntry`) and are attached to WRITE / SEARCH entries
    via their ``input_transformers`` / ``output_transformers`` refs.

    Catalog routing (``CatalogRoutingConfig.operations``) — same shape on
    catalog rows.

    Asset routing (``AssetRoutingConfig.operations``) — asset rows:
    - WRITE / READ : as above (single-primary + secondary indexes by role).
    - UPLOAD : single-driver pick of the ``AssetUploadProtocol`` impl that
               handles ``initiate_upload``/``get_upload_status`` (auto-
               augmented from discoverable ``AssetUploadProtocol`` impls;
               operator config can pin a specific backend).
    """

    WRITE = "WRITE"
    READ = "READ"
    SEARCH = "SEARCH"
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
    """

    SYNC = "sync"
    ASYNC = "async"
    FIRST = "first"
    FAN_OUT = "fan_out"


# ---------------------------------------------------------------------------
# Capability → Operation mapping
# ---------------------------------------------------------------------------


def derive_supported_operations(capabilities: FrozenSet[str]) -> FrozenSet[str]:
    """Derive which Operations a driver supports from its Capability set.

    Uses :data:`_CAPABILITY_TO_OPERATIONS` to map driver capabilities to the
    operations they can handle.  This is used by apply-handler validation and
    the driver discovery endpoint.

    Role-based driver plan: a ``WRITE`` entry whose driver implements the
    tier's Indexer marker is a secondary index (``secondary_index=True``),
    propagated asynchronously — the role is derived from the driver, not a
    distinct operation.

    Entity-transform participation is expressed by implementing
    :class:`EntityTransformProtocol`; transformers populate the routing
    config's ``transformers`` registry (not an :class:`Operation`).
    See ``modules/storage/routing_config.py:_self_register_transformers_into``.
    """
    from dynastore.models.protocols.storage_driver import Capability

    mapping: Dict[str, Set[str]] = {
        Capability.WRITE: {Operation.WRITE},
        Capability.READ: {Operation.READ, Operation.SEARCH},
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
                         class-level ``sla`` ClassVar (if any) is used.
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
            "class-level SLA (if declared)."
        ),
    )
    secondary_index: bool = Field(
        default=False,
        description=(
            "Derived role flag for WRITE entries: True iff the driver "
            "implements the tier's Indexer marker "
            "(``is_item_indexer``/``is_collection_indexer``/"
            "``is_catalog_indexer``/``is_asset_indexer``).  A secondary "
            "index is propagated by the ReindexWorker / index dispatcher "
            "rather than committed as the primary store.  Stamped at "
            "config-build time (preset / self-register / validation) from "
            "the driver class, NOT operator-declared — it persists in the "
            "config so a configured-but-not-locally-installed indexer is "
            "still recognised (and OUTBOX-enqueued) in any SCOPE.  Role is "
            "orthogonal to policy (``write_mode`` x ``on_failure``): a "
            "secondary index may be sync/fatal or async/outbox."
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
            "ref must also appear in the routing config's ``transformers`` "
            "registry — the validator rejects dangling references at "
            "config-build time. Wired hops in this release: ``WRITE`` "
            "(secondary-index propagation). Declaring this on other "
            "operations emits a one-time WARN because the hop is not yet "
            "active."
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


class TransformerEntry(BaseModel):
    """A member of a routing config's ``transformers`` registry.

    A transformer is **not** a dispatch operation: it carries no
    ``write_mode`` / ``on_failure`` / ``secondary_index`` semantics.  It is a
    named, ordered registry entry that WRITE / SEARCH operation entries
    reference by ``driver_ref`` through their ``input_transformers`` /
    ``output_transformers`` attachments.  The concrete driver must implement
    :class:`EntityTransformProtocol`; the chain runtime lives in
    ``modules/storage/transform_runtime.py``.

    The registry is auto-populated from discoverable
    ``EntityTransformProtocol`` implementers (see
    :func:`_self_register_transformers_into`) and persists in the config so
    that attachment-ref validation succeeds in any SCOPE — including one where
    the transformer's driver is configured but not locally installed.
    """

    driver_ref: Immutable[str] = Field(
        ...,
        min_length=1,
        description=(
            "Transformer reference — the snake_case class name of an "
            "``EntityTransformProtocol`` implementer "
            "(e.g. ``private_entity_transformer``)."
        ),
    )
    sla: Optional[DriverSla] = Field(
        default=None,
        description=(
            "Optional per-transformer SLA.  Without one a transform runs "
            "unbounded on the hot path; pin an SLA to bound it."
        ),
    )
    source: Literal["operator", "auto"] = Field(
        default="operator",
        description=(
            "Provenance.  ``operator`` = explicitly configured; ``auto`` = "
            "appended by ``_self_register_transformers_into`` from a "
            "discoverable implementer.  An operator-authored registry (any "
            "``source='operator'`` entry present) is invariant under "
            "auto-augmentation."
        ),
    )


# Operations whose transformer hop is wired in this release. Declaring
# input_transformers / output_transformers on any other (operation, side)
# pair logs a one-time WARN so operators see the silent-no-op early.
#
# INPUT (write-side ``apply_transform_chain``) is wired on ``WRITE`` for every
# tier (secondary-index fan-out). OUTPUT (read-side ``restore_from_index``) is
# wired on ``SEARCH`` for every tier whose search driver invokes
# ``restore_transform_chain`` — the four ES-backed tiers (items, collection,
# asset, catalog) since geoid#1574. The per-tier flag
# ``_RoutingConfigBase._wired_output_search_hop`` carries that distinction so a
# SEARCH ``output_transformers`` declared on a tier that does NOT run the
# restore chain (e.g. a non-ES read path) warns instead of silently never
# running. See geoid#1567, geoid#1574; non-ES read-side wiring tracked in
# geoid#1643.
_WIRED_INPUT_HOPS: FrozenSet[str] = frozenset({Operation.WRITE})
_WIRED_OUTPUT_HOPS: FrozenSet[str] = frozenset({Operation.SEARCH})
_DEFERRED_HOP_WARNED: Set[Tuple[str, str, str, str]] = set()


def _warn_deferred_transformer_hops(
    operations: Dict[str, List["OperationDriverEntry"]],
    config_label: str,
    *,
    output_search_wired: bool,
) -> None:
    """Emit a one-time WARN per ``(tier, operation, driver, side)`` for a
    transformer hop the runtime does not invoke, so the silent no-op surfaces
    at config-load instead of as a mysteriously inert transformer.

    ``output_search_wired`` reflects whether *this tier*'s SEARCH path runs the
    read-side restore chain. The four ES-backed tiers (items, collection, asset,
    catalog) do since geoid#1574; for a tier that does not, a SEARCH
    ``output_transformers`` declaration validates but never fires, so it is
    warned as a deferred hop.
    """
    for op_name, entries in operations.items():
        for entry in entries:
            if entry.input_transformers and op_name not in _WIRED_INPUT_HOPS:
                key = (config_label, op_name, entry.driver_ref, "input")
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
            output_hop_wired = op_name in _WIRED_OUTPUT_HOPS and (
                op_name != Operation.SEARCH or output_search_wired
            )
            if entry.output_transformers and not output_hop_wired:
                key = (config_label, op_name, entry.driver_ref, "output")
                if key not in _DEFERRED_HOP_WARNED:
                    _DEFERRED_HOP_WARNED.add(key)
                    if op_name == Operation.SEARCH and not output_search_wired:
                        reason = (
                            "read-side restore_from_index is wired only for the "
                            "asset tier in this release (geoid#1567)"
                        )
                    else:
                        reason = (
                            f"the {op_name} output-transformer hop is not yet "
                            "wired in this release"
                        )
                    logger.warning(
                        "%s: output_transformers declared on operation '%s' "
                        "for driver '%s' but %s — declaration is a no-op.",
                        config_label, op_name, entry.driver_ref, reason,
                    )


def _validate_transformer_attachment(
    operations: Dict[str, List["OperationDriverEntry"]],
    transformers: Sequence["TransformerEntry"],
    config_label: str,
) -> None:
    """Every ref under ``input_transformers`` / ``output_transformers``
    must also appear as a ``driver_ref`` in the ``transformers`` registry.
    Raises ``ValueError`` listing the dangling refs.
    """
    transform_refs = {entry.driver_ref for entry in transformers}
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
            f"input_transformers/output_transformers do not appear in the "
            f"``transformers`` registry. Register them as transformers "
            f"(or remove the attachment)."
        )


class _RoutingConfigBase(PluginConfig):
    """Shared base for the four tier routing configs (#990 P4).

    Collapses what every tier had copied verbatim: the ``transformers``
    registry field, the ``x-ui`` routing category, and the read-time
    model_validator that self-registers discoverable drivers and validates
    transformer attachments.  Each concrete tier supplies only what genuinely
    differs:

    - ``_address`` / ``_freeze_at`` / ``_tiers`` ClassVars (tree
      placement + immutability/view scoping),
    - the ``operations`` field with its tier-specific default driver wiring,
    - :meth:`_self_register_drivers` — folds the tier's discoverable indexer /
      searcher / upload drivers into ``operations`` (the sole behavioural
      variation between tiers; each override does the tier's lazy protocol
      imports to avoid an import cycle at module load).

    A non-generic concrete base is intentional: no field or method signature is
    typed by the tier's Store / Indexer protocols — they are resolved lazily as
    runtime markers inside :meth:`_self_register_drivers` — so a
    ``Generic[StoreT, IndexerT]`` parametrisation would be cosmetic and only
    add Pydantic-generic / config-registry edge cases.
    """

    is_abstract_base: ClassVar[bool] = True

    # Whether THIS tier's SEARCH path invokes the read-side restore chain
    # (``restore_from_index`` via ``restore_transform_chain``). Only the asset
    # tier does today; other tiers leave it False so the validator warns on an
    # inert SEARCH ``output_transformers`` declaration instead of silently
    # dropping it. See geoid#1567.
    _wired_output_search_hop: ClassVar[bool] = False

    model_config = ConfigDict(json_schema_extra=ui(category="routing"))

    operations: Mutable[Dict[str, List[OperationDriverEntry]]] = Field(
        default_factory=dict,
        description=(
            "Operation -> ordered driver list.  Overridden per tier with the "
            "tier's default driver wiring (position 0 = primary)."
        ),
    )
    transformers: Immutable[List[TransformerEntry]] = Field(
        default_factory=list,
        description=(
            "Registry of entity transformers available to this config. "
            "Auto-populated from discoverable EntityTransformProtocol "
            "implementers; WRITE/SEARCH entries reference these by "
            "driver_ref via input_transformers/output_transformers."
        ),
    )

    def _self_register_drivers(self) -> None:
        """Tier hook — fold discoverable indexer / searcher / upload drivers
        into ``self.operations``.

        Overridden by each concrete tier (which does its own lazy protocol
        imports).  The base is a no-op so a tier with no auto-discovery still
        validates cleanly.
        """
        return None

    def _stamp_operator_provenance(
        self, changed_op_keys: Optional[Set[str]] = None
    ) -> None:
        """Stamp ``source='operator'`` on operation-driver entries the operator
        actually changed — the API-boundary half of the Option-A list-level
        operator lock (#792/#889).

        When ``changed_op_keys`` is provided (a set of operation key strings),
        only entries in those operations are stamped.  Operations not in the
        set keep their existing ``source`` values, so auto-augmentation remains
        possible for lists the operator did not touch (#1865).

        When ``changed_op_keys`` is ``None`` (create path or legacy callers),
        all operations present are stamped — the original behaviour, applied
        when there is no stored config to diff against.

        ``_is_operator_managed`` (and the self-register helpers it gates) keys
        on whether any entry in an operation list carries ``source='operator'``.
        Boot defaults and self-registered drivers are stamped ``'auto'``, and
        the configs API serialises that ``'auto'`` back to the operator — so a
        natural GET→edit→PUT round-trip returns lists that still read as
        auto-managed.  Unless we re-assert operator intent for the changed
        lists, the self-register helpers re-append the very driver the operator
        removed (the "deleted driver comes back" symptom).

        This MUST run BEFORE ``_self_register_drivers`` (see
        ``_augment_and_validate_routing``): the stamp only sticks if it
        precedes the re-append pass it is meant to suppress.  Idempotent.
        """
        for op_key, entries in self.operations.items():
            if changed_op_keys is not None and op_key not in changed_op_keys:
                continue
            for i, entry in enumerate(entries):
                if entry.source != "operator":
                    entries[i] = entry.model_copy(update={"source": "operator"})

    @model_validator(mode="after")
    def _augment_and_validate_routing(
        self, info: ValidationInfo
    ) -> "_RoutingConfigBase":
        """Self-register discoverable drivers + transformers, then validate
        transformer attachments and warn on deferred hops.

        Self-registration is best-effort: discovery may not be ready (early
        bootstrap / fixtures that validate before plugins register), in which
        case the apply-handler repopulates on the next write.  Attachment
        validation always runs — a dangling transformer ref is a hard error.

        On an **external operator write** (the configs API stamps
        ``context={"dynastore_external_write": True}`` at deserialisation), only
        the operation lists the operator actually changed are stamped
        ``source='operator'`` before self-registration.  The set of changed
        lists is computed at the service boundary (where the stored config is
        available) and passed via ``context["dynastore_changed_operation_keys"]``
        (#1865).  When that key is absent, all present lists are stamped
        (create path / legacy callers).  Internal DB-load / boot-default
        construction carries no such context, so discoverable drivers still
        auto-register there (#792/#889).
        """
        label = type(self).__name__
        if _is_external_operator_write(info) and "operations" in self.model_fields_set:
            changed_op_keys: Optional[Set[str]] = (info.context or {}).get(
                "dynastore_changed_operation_keys"
            )
            self._stamp_operator_provenance(changed_op_keys)
        try:
            self._self_register_drivers()
            _self_register_transformers_into(self.transformers)
        except Exception as exc:
            logger.debug(
                "%s: read-time self-register skipped (%s); apply-handler "
                "will populate on next write.", label, exc,
            )
        _validate_transformer_attachment(self.operations, self.transformers, label)
        _warn_deferred_transformer_hops(
            self.operations, label,
            output_search_wired=type(self)._wired_output_search_hop,
        )
        return self


class ItemsRoutingConfig(_RoutingConfigBase):
    """Operation-based routing for **items** storage drivers.

    Each operation maps to an ordered list of :class:`OperationDriverEntry`.
    Position in the list determines priority (first = primary).

    Items routing dispatches `CollectionItemsStore` drivers (PG, ES, BQ,
    Iceberg, DuckDB) for entity-level operations: WRITE (including
    secondary-index entries) / READ / SEARCH over collection items /
    features. **Distinct from**
    :class:`CollectionRoutingConfig` which dispatches
    ``CollectionStore`` drivers for collection-envelope metadata.

    Identity is the class itself; see ``class_key()`` in ``platform_config_service.py``.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "items", "routing")
    _freeze_at: ClassVar[Optional[str]] = "collection"
    # Items routing cascades platform → catalog → collection: a catalog-tier
    # default (e.g. a routing preset) must surface in the catalog view even
    # though the immutability gate stays collection-scoped (``_freeze_at``).
    _tiers: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection")
    # ItemsElasticsearchDriver.read_entities and the envelope/private siblings
    # invoke get_output_transformers_for_search + restore_transform_chain, so
    # SEARCH output_transformers are now wired for this tier (geoid#1574).
    _wired_output_search_hop: ClassVar[bool] = True

    operations: Mutable[Dict[str, List[OperationDriverEntry]]] = Field(
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
            # ``hints=`` (router.py ``resolved[0]``). ES carries
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
                # SEARCH entries declare the search-flavour hints each driver
                # serves, so a resolved config is self-documenting and the
                # routing intent is explicit (mirrors the READ defaults and
                # CollectionRoutingConfig SEARCH). Each set is the driver's
                # ``supported_hints`` restricted to search-applicable flavours:
                # operation-specific hints stay out (e.g. ``TILES`` is a READ
                # concern and lives only on the PG READ entry, never here;
                # ``WRITE``/``METADATA``/``JOIN`` are not search flavours).
                #
                # Consequence of declaring these explicitly: the best-overlap
                # matcher (router.py) now ranks by the DECLARED surface rather
                # than the driver-class fallback, so a filtered/sorted search
                # (``attribute_filter``/``spatial_filter``/``sort``) routes to
                # ES first — the search engine — instead of PG (which only won
                # before as an accident of PG's longer total ``supported_hints``
                # surface). Unfiltered search is unaffected (the matcher is
                # skipped on empty request-hints, so declared order ES→PG holds).
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_driver",
                    hints={
                        Hint.SEARCH, Hint.FULLTEXT, Hint.GEOMETRY_SIMPLIFIED,
                        Hint.SPATIAL_FILTER, Hint.ATTRIBUTE_FILTER, Hint.SORT,
                        Hint.AGGREGATION, Hint.COUNT, Hint.STATISTICS,
                    },
                    on_failure=FailurePolicy.FATAL,
                    source="auto",
                ),
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                    hints={
                        Hint.GEOMETRY_EXACT,
                        Hint.SPATIAL_FILTER, Hint.ATTRIBUTE_FILTER, Hint.SORT,
                        Hint.GROUP_BY, Hint.AGGREGATION, Hint.COUNT,
                        Hint.STATISTICS,
                    },
                    on_failure=FailurePolicy.FATAL,
                    source="auto",
                ),
            ],
        },
        description=(
            "Operation → ordered driver list for items dispatch.  "
            "Immutable: to change driver mapping, create a new config.  "
            "Hints and on_failure within entries are mutable.  "
            "operations[WRITE] is the source of truth for the items-tier "
            "secondary-index hop on item upsert/delete (OGC ingest path "
            "through item_service._dispatch_index_upsert -> IndexDispatcher, "
            "and item_query soft-delete): index entries are WRITE entries "
            "with secondary_index=True. Pinning a private indexer here is "
            "what the privacy-cascade validator enforces; the entry-aware "
            "default resolver picks this config for entity_type='item'. "
            "See un-fao/GeoID#810 (Option B)."
        ),
    )
    def _self_register_drivers(self) -> None:
        """Fold discoverable :class:`ItemIndexer` drivers into
        ``operations[WRITE]`` as secondary-index entries
        (``secondary_index=True``) and SEARCH-capable
        :class:`CollectionItemsStore` drivers into ``operations[SEARCH]`` — so
        a deployed ``ItemsElasticsearchDriver`` shows up without operator PUT.
        """
        from dynastore.models.protocols.indexer import ItemIndexer
        from dynastore.models.protocols.storage_driver import CollectionItemsStore

        _self_register_indexers_into(self.operations, ItemIndexer)
        _self_register_searchers_into(self.operations, CollectionItemsStore)


class CollectionRoutingConfig(_RoutingConfigBase):
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

    ``transformers`` registry (**lazy**):
        Entity transformers that enrich collection metadata.  These live in
        the sibling ``transformers`` field — **not** an operation.  A WRITE /
        SEARCH entry opts a transformer in via its ``input_transformers`` /
        ``output_transformers`` refs; the async reindex pipeline applies the
        WRITE entry's ``input_transformers`` before dispatching to a
        secondary-index sink.  Each transformer should carry an SLA.

    ``WRITE`` secondary indexes (optional, typically async):
        Post-write propagation targets for search-capable sinks (ES, Vertex AI,
        vector DBs) live in ``WRITE`` with ``secondary_index=True`` — they are
        not a separate operation.  An entry's ``input_transformers`` decide
        whether the indexer receives a transformed envelope; with none it gets
        the raw Primary envelope.

    Identity is the class itself; see ``class_key()`` in ``platform_config_service.py``.
    """
    # Collection-envelope routing — 2-tuple under storage (no items/assets fork).
    # CollectionStore drivers are structurally distinct from items-tier drivers,
    # so this routing config lands at ``storage.routing.{class_key}`` rather
    # than under an items/assets sibling.
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "routing")
    _freeze_at: ClassVar[Optional[str]] = "collection"
    # Collection routing cascades platform → catalog → collection: a
    # catalog-tier default must surface in the catalog view while the
    # immutability gate stays collection-scoped (``_freeze_at``).
    _tiers: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection")
    # CollectionElasticsearchDriver.get_metadata and search_metadata invoke
    # get_output_transformers_for_search + restore_transform_chain, so
    # SEARCH output_transformers are now wired for this tier (geoid#1574).
    _wired_output_search_hop: ClassVar[bool] = True

    operations: Mutable[Dict[str, List[OperationDriverEntry]]] = Field(
        default_factory=lambda: {
            # Collection-envelope routing. The PG collection driver
            # (collection_postgresql_driver — internally fans CRUD across
            # the collection_core + collection_stac sidecars) is the
            # system of record: primary for both WRITE and READ.
            # Elasticsearch is the *index* — a secondary-index WRITE entry
            # (secondary_index=True) populated asynchronously (OUTBOX-durable)
            # and fronting SEARCH. PG is the SEARCH fallback so a deploy
            # without ES still answers collection search from the
            # authoritative store.
            #
            # The ES secondary-index entry is intentionally NOT hard-coded
            # here: it is supplied by ``_self_register_indexers_into`` at
            # validation time when a CollectionIndexer (ES) driver is
            # registered, and by the routing presets (e.g. public_catalog)
            # for explicit deployments. A PG-only deployment with no ES driver
            # and no outbox-draining worker therefore gets no secondary-index
            # entry, so a plain collection create does not enqueue an OUTBOX
            # row into tasks.tasks that nothing would ever drain
            # (#1069 / #1073).
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
            Operation.SEARCH: [
                # ES is the primary search backend (fast, simplified
                # geometry). PG is the fallback AND the exact-geometry
                # path: a consumer needing full-precision geometry passes
                # hints=frozenset({Hint.GEOMETRY_EXACT}) to route SEARCH to PG.
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
            "The ES secondary index is a WRITE entry (secondary_index=True) "
            "with async OUTBOX-durable propagation, added by self-"
            "registration when an ES CollectionIndexer is registered (or by "
            "a routing preset) — not hard-coded, so a PG-only deployment "
            "enqueues no undrainable outbox rows. "
            "SEARCH = Elasticsearch primary (geometry_simplified), "
            "PostgreSQL fallback (geometry_exact)."
        ),
    )
    def _self_register_drivers(self) -> None:
        """Fold discoverable :class:`CollectionIndexer` drivers into
        ``operations[WRITE]`` as secondary-index entries
        (``secondary_index=True``) and SEARCH-capable ``CollectionStore``
        drivers into ``operations[SEARCH]``.
        """
        from dynastore.models.protocols.entity_store import CollectionStore

        _self_register_indexers_into(self.operations, CollectionIndexer)
        _self_register_searchers_into(self.operations, CollectionStore)


class AssetRoutingConfig(_RoutingConfigBase):
    """Operation-based routing for asset storage drivers.

    Same structure as :class:`ItemsRoutingConfig` but scoped to
    asset-domain drivers.

    Identity is the class itself; see ``class_key()`` in ``platform_config_service.py``.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "assets", "routing")
    _freeze_at: ClassVar[Optional[str]] = "collection"
    # Asset routing cascades platform → catalog → collection: a catalog-tier
    # default must surface in the catalog view while the immutability gate
    # stays collection-scoped (``_freeze_at``).
    _tiers: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection")
    # The asset SEARCH driver (AssetElasticsearchDriver.search_assets) is the
    # only path that invokes the read-side restore chain today, so SEARCH
    # output_transformers actually fire on this tier. See geoid#1567.
    _wired_output_search_hop: ClassVar[bool] = True

    operations: Mutable[Dict[str, List[OperationDriverEntry]]] = Field(
        default_factory=lambda: {
            # Assets routing: PG is the canonical system of record and
            # the only default driver. Elasticsearch is intentionally
            # absent from the hardcoded defaults — an operator pins it
            # via PluginConfig, or it arrives through the auto-augment
            # path below if ``AssetElasticsearchDriver`` is installed
            # and registers itself as an ``AssetIndexer``.
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="asset_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
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
            ],
        },
        description=(
            "Operation → ordered driver list for asset drivers. "
            "Defaults wire PG only (FATAL primary on WRITE and READ); "
            "the ES asset driver is not a default. ``operations[WRITE]`` "
            "is auto-augmented at validation time with discoverable "
            "AssetIndexer drivers (stamped secondary_index=True), so "
            "operators that install an ES asset indexer still get async "
            "fan-out; ``operations[UPLOAD]`` is auto-augmented with "
            "discoverable AssetUploadProtocol impls."
        ),
    )
    def _self_register_drivers(self) -> None:
        """Augment WRITE secondary indexes + UPLOAD with discoverable drivers.

        SEARCH is resolvable on the asset tier but carries no hardcoded
        default and is not auto-augmented: ``get_asset_search_driver``
        resolves ``operations[SEARCH]`` first and falls back to
        ``operations[READ]`` when an operator has not pinned a dedicated
        search backend (see ``modules/storage/router.py`` and #989). This
        keeps the zero-config default behaviour (PG-backed READ serves
        filtered queries) while letting an operator route SEARCH to an
        index driver (e.g. Elasticsearch) per catalog/collection without a
        code change.
        """
        from dynastore.models.protocols.asset_upload import AssetUploadProtocol

        _self_register_indexers_into(self.operations, AssetIndexer)
        _self_register_upload_into(self.operations, AssetUploadProtocol)


class CatalogRoutingConfig(_RoutingConfigBase):
    """Operation-based routing for catalog-tier ``CatalogStore`` drivers.

    Parallels :class:`ItemsRoutingConfig` but scoped to catalog-tier
    drivers (``CatalogStore`` implementations).  Introduced by the
    role-based driver refactor so catalogs follow the same Primary /
    Transformer / Indexer pattern as collections.

    The registered ``CatalogStore`` is ``CatalogPostgresqlDriver`` — a
    composition wrapper that fans CRUD across the ``catalog_core`` and
    ``catalog_stac`` PG sidecars internally. The defaults below pin it
    under WRITE and READ so a deployment resolves correctly without
    explicit platform config.

    ``operations`` supports the same keys as :class:`CollectionRoutingConfig`:
    ``WRITE``, ``READ``, ``SEARCH`` (plus the sibling ``transformers``
    registry).
    See that class for per-key semantics, with one trigger difference:
    secondary-index WRITE entries on this config are consumed by
    :class:`~dynastore.modules.catalog.reindex_worker.ReindexWorker` off
    the ``catalog_metadata_changed`` event stream — they are NOT
    invoked directly from ``catalog_router`` the way the collection
    secondary-index entries are invoked from
    ``collection_router._dispatch_collection_index``.  Both end at the
    same Indexer drivers through OUTBOX-durable plumbing; the asymmetry
    is in *how* the hop is triggered, not in what runs.  See the
    "Catalog secondary-index hop" section in
    ``modules/catalog/catalog_router.py``'s module docstring.

    Identity is the class itself; see ``class_key()`` in ``platform_config_service.py``.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "routing")
    _freeze_at: ClassVar[Optional[str]] = "catalog"
    # Catalog routing applies at platform + catalog only (catalogs don't
    # nest); it must not leak into a collection view.  Explicit so the view
    # no longer depends on the unimplemented ``_freeze_at="catalog"`` hide-
    # at-collection rule.
    _tiers: ClassVar[Tuple[str, ...]] = ("platform", "catalog")
    # CatalogElasticsearchDriver.get_catalog_metadata invokes
    # get_output_transformers_for_search + restore_transform_chain, so
    # SEARCH output_transformers are now wired for this tier (geoid#1574).
    _wired_output_search_hop: ClassVar[bool] = True

    operations: Mutable[Dict[str, List[OperationDriverEntry]]] = Field(
        default_factory=lambda: {
            # catalog_postgresql_driver is the registered CatalogStore
            # composition wrapper — it fans CRUD across the catalog_core +
            # catalog_stac PG sidecars internally. It is the system of
            # record (FATAL) for both WRITE and READ. The ES secondary
            # index is a WRITE entry (secondary_index=True) propagating to
            # Elasticsearch asynchronously (OUTBOX-durable). That entry is
            # NOT hard-coded: it is auto-augmented at validation time with
            # discoverable CatalogIndexer drivers (and supplied by routing
            # presets for explicit deployments). A PG-only deployment with
            # no ES driver and no outbox-draining worker therefore gets no
            # secondary-index entry, so a plain catalog create does not
            # enqueue an OUTBOX row into tasks.tasks that nothing would
            # ever drain (#1069 / #1073).
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
        },
        description=(
            "Operation -> ordered driver list for catalog-tier CatalogStore "
            "drivers. WRITE/READ = catalog_postgresql_driver (system of "
            "record). The ES secondary index is a WRITE entry "
            "(secondary_index=True) with async OUTBOX-durable propagation, "
            "auto-augmented at validation time with every discoverable "
            "CatalogIndexer (or supplied by a routing preset) — not hard-"
            "coded, so a PG-only deployment enqueues no undrainable outbox "
            "rows. Operator-explicit entries take precedence; auto-"
            "augmentation is idempotent set-default."
        ),
    )
    def _self_register_drivers(self) -> None:
        """Fold discoverable CatalogIndexer (as secondary-index WRITE
        entries, ``secondary_index=True``) + SEARCH-capable CatalogStore
        drivers into ``operations[WRITE]`` and ``operations[SEARCH]``.

        Closes the gap where the default-state config (no operator write)
        shows neither a secondary-index WRITE entry nor SEARCH entries even
        when an ES catalog driver is installed.  Mirrors the apply-handler
        self-registration so default-state and apply-time configs converge —
        ``_on_apply_catalog_routing_config`` calls the same helpers with the
        same idempotent semantics.
        """
        from dynastore.models.protocols.entity_store import CatalogStore

        _self_register_indexers_into(self.operations, CatalogIndexer)
        _self_register_searchers_into(self.operations, CatalogStore)


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
                    from dynastore.models.plugin_config import resolve_config_class

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


def _is_external_operator_write(info: ValidationInfo) -> bool:
    """Return True when validation was triggered by an external operator write.

    The configs-API deserialisation boundary (``update_platform_config`` /
    ``update_catalog_config`` / ``update_collection_config``) passes
    ``context={"dynastore_external_write": True}`` to ``model_validate``.
    Internal construction (DB load, boot defaults, config merge/snapshot)
    carries no such context, so this returns False and self-registration runs
    normally. Drives the operator-provenance stamp in
    ``_augment_and_validate_routing`` (#792/#889).
    """
    return bool((info.context or {}).get("dynastore_external_write"))


def _compute_changed_op_keys(
    incoming_ops: Dict[str, Any],
    stored_raw: Optional[Dict[str, Any]],
) -> Optional[Set[str]]:
    """Return the set of operation keys that differ between the incoming PUT
    body and the tier-local stored config row, or ``None`` when there is no
    stored config (create path).

    An operation key is "changed" when its set of ``driver_ref`` values differs
    from the stored list (order-insensitive).  This is the right semantic
    because ``driver_ref`` is the identity of a routing entry; the operator's
    intent is which drivers are present, not their position.

    Called at the **service boundary** (where the stored config is available)
    so the validator does not need DB access (#1865).  Passing ``None`` signals
    the create path: the validator stamps all present lists as operator-managed.
    """
    if stored_raw is None:
        return None
    stored_ops: Dict[str, Any] = stored_raw.get("operations", {})
    changed: Set[str] = set()
    all_keys = set(incoming_ops) | set(stored_ops)
    for op_key in all_keys:
        incoming_refs = {
            e.get("driver_ref") if isinstance(e, dict) else e
            for e in incoming_ops.get(op_key, [])
        }
        stored_refs = {
            e.get("driver_ref") if isinstance(e, dict) else e
            for e in stored_ops.get(op_key, [])
        }
        if incoming_refs != stored_refs:
            changed.add(op_key)
    return changed


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
    ``target_ops[WRITE]`` with durable async defaults
    (``write_mode=async``, ``on_failure=outbox``).

    Secondary indexes are not a distinct operation: an indexer is a WRITE
    target whose driver implements the tier's Indexer marker protocol.  This
    helper also stamps the derived ``secondary_index`` role flag on every
    WRITE entry whose driver is a tier indexer (including operator-pinned
    ones), so a single ``operations[WRITE]`` list carries both primary and
    index entries and ``secondary_index_entries`` can split them by the
    persisted flag — independent of whether the indexer is installed in the
    reading SCOPE.  Stamping is upward-only (never clears a persisted flag),
    so an indexer configured where it is installed stays classified as an
    index in SCOPEs where it is not.

    Tier-scoped: caller passes the right marker (``CatalogIndexer`` →
    catalog routing, ``CollectionIndexer`` → collection routing,
    ``AssetIndexer`` → asset routing).  Drivers indexing multiple tiers
    opt in to multiple markers and self-register into each tier's
    ``operations[WRITE]`` independently.

    The ``OUTBOX`` default means a transient indexer failure enqueues a row
    in ``_meta.index_outbox`` (same PG transaction as the upstream write)
    for the drain task to retry, instead of dropping the obligation with a
    log line.  Operators who want a non-durable best-effort sink can
    override per-entry to ``WARN``, or a strongly-consistent index with
    ``write_mode=sync`` / ``on_failure=fatal``.

    Operator-override: a no-op when ``target_ops[WRITE]`` contains any
    entry with ``source="operator"`` — operator-managed lists are
    invariant under auto-augmentation (#792 / #889).
    """
    from dynastore.tools.discovery import get_protocols

    # Stamp the derived secondary_index role on every existing WRITE entry
    # whose driver is a tier indexer.  Runs before the operator-managed
    # early-return so an operator who pins an indexer still gets the role
    # persisted.  Upward-only: a previously-stamped index stays an index even
    # when the driver is not installed in the reading SCOPE (discovery would
    # not see it), preserving OUTBOX durability.
    indexer_refs = {_to_snake(type(d).__name__) for d in get_protocols(marker_proto)}
    write_entries = target_ops.get(Operation.WRITE)
    if write_entries:
        for i, entry in enumerate(write_entries):
            if entry.driver_ref in indexer_refs and not entry.secondary_index:
                write_entries[i] = entry.model_copy(update={"secondary_index": True})

    if _is_operator_managed(target_ops, Operation.WRITE):
        return
    listed = {entry.driver_ref for entry in target_ops.get(Operation.WRITE, [])}
    for driver in get_protocols(marker_proto):
        # Single gate on the per-Operation auto-default set.  Drivers
        # explicitly declare which Operations they auto-default into via
        # ``auto_register_for_routing: ClassVar[FrozenSet[Operation]]``;
        # ``Operation.WRITE`` membership opts the indexer in here.
        # Empty (default) = explicit-pin only.
        opt_in: FrozenSet[str] = getattr(type(driver), "auto_register_for_routing", frozenset())
        if Operation.WRITE not in opt_in:
            continue
        driver_ref = _to_snake(type(driver).__name__)
        if driver_ref in listed:
            continue
        target_ops.setdefault(Operation.WRITE, []).append(
            OperationDriverEntry(
                driver_ref=driver_ref,
                on_failure=FailurePolicy.OUTBOX,
                write_mode=WriteMode.ASYNC,
                secondary_index=True,
                source="auto",
            )
        )
        listed.add(driver_ref)
        logger.debug(
            "Routing config self-registration: appended %s indexer '%s' "
            "to operations[WRITE] (write_mode=async, on_failure=outbox, source=auto)",
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
        for driver_ref, driver in store_driver_index.items():
            if driver_ref in listed:
                continue
            # Only auto-append a driver into an operation its capabilities
            # actually support. Some drivers satisfy the tier's *Store
            # protocol structurally but declare no WRITE/READ capability —
            # e.g. the diagnostic LogCatalogIndexer (capabilities=frozenset()),
            # which is an INDEX-role driver discoverable as a CatalogStore.
            # Without this gate it would be injected here and then rejected by
            # the capability gate in _validate_routing_entries, making the
            # config impossible to PUT at all (#1179).
            driver_caps = getattr(driver, "capabilities", frozenset())
            if op not in derive_supported_operations(driver_caps):
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

    # Items-tier: auto-register ItemIndexer drivers as secondary-index WRITE
    # entries (gated on ``Operation.WRITE in driver.auto_register_for_routing``)
    # + ``CollectionItemsStore`` drivers opting into ``Operation.SEARCH``
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


async def _resolve_parent_catalog_routing(
    catalog_id: str,
    db_resource: Optional[Any],
) -> Optional["CatalogRoutingConfig"]:
    """Resolve the parent catalog's :class:`CatalogRoutingConfig` (waterfall:
    catalog → platform → defaults) for the composition guard.

    Returns ``None`` when the configs protocol is not available (e.g. early
    bootstrap / test fixtures that validate before plugins register) so the
    caller can decide how to treat an un-resolvable parent. Reads through the
    in-flight ``db_resource`` connection so the lookup is consistent with the
    enclosing config-write transaction.
    """
    from dynastore.models.driver_context import DriverContext
    from dynastore.models.protocols.configs import ConfigsProtocol
    from dynastore.tools.discovery import get_protocol

    configs = get_protocol(ConfigsProtocol)
    if configs is None:
        return None
    ctx = DriverContext(db_resource=db_resource) if db_resource is not None else None
    cfg = await configs.get_config(
        CatalogRoutingConfig, catalog_id=catalog_id, ctx=ctx,
    )
    return cfg if isinstance(cfg, CatalogRoutingConfig) else None


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

    # Validate the transformers registry (CollectionItemsStore drivers — they
    # contribute item-derived metadata at READ time)
    for entry in config.transformers:
        if entry.driver_ref not in driver_index:
            raise ValueError(
                f"Collection metadata routing config: transformer driver "
                f"'{entry.driver_ref}' is not registered. "
                f"Available: {sorted(driver_index)}"
            )

    # Validate operations[WRITE] entries (CollectionStore drivers — primary
    # metadata store plus any secondary-index sinks, e.g. ES). Secondary
    # indexes are WRITE targets distinguished by driver role, not a separate
    # operation.
    for entry in config.operations.get(Operation.WRITE, []):
        if entry.driver_ref not in store_driver_index:
            raise ValueError(
                f"Collection metadata routing config: operations[WRITE] driver "
                f"'{entry.driver_ref}' is not registered. "
                f"Available: {sorted(store_driver_index)}"
            )

    # Auto-register discoverable indexers + searchers — parity with the
    # read-time model_validator on CollectionRoutingConfig.
    _self_register_indexers_into(config.operations, CollectionIndexer)
    _self_register_searchers_into(config.operations, CollectionStore)

    # Composition guard (#1047): a public-ES collection requires a public-ES
    # parent catalog. Cross-tier rule — needs the parent catalog's routing
    # config, which a single-model pydantic validator can't reach. Resolved
    # here, pre-persist, so a violation rolls back the upsert and surfaces as
    # HTTP 400.
    if catalog_id and _collection_routing_is_public(config):
        parent_catalog_routing = await _resolve_parent_catalog_routing(
            catalog_id, db_resource,
        )
        _assert_public_collection_has_public_parent(config, parent_catalog_routing)


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

    # Auto-register installed AssetIndexer drivers as secondary-index
    # entries under operations[WRITE].
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

    Secondary-index WRITE entries are validated against the same registry —
    role is derived from the driver's Indexer marker
    (``secondary_index``), not a distinct operation (see role-based driver
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

    # Auto-register installed CatalogIndexer drivers as secondary-index
    # entries under operations[WRITE] and SEARCH-capable CatalogStore drivers
    # under operations[SEARCH] for parity with the read-time validator.
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
# Privacy detection — items-tier only (#1047 Phase 2)
# ---------------------------------------------------------------------------
#
# After dropping CatalogElasticsearchPrivateDriver and
# CollectionElasticsearchPrivateDriver, privacy is expressed solely by the
# presence of ``items_elasticsearch_private_driver`` in an
# ``ItemsRoutingConfig``.  Catalog and collection envelopes for private
# catalogs live in PostgreSQL only — no ES index at those tiers.


_PRIVATE_ITEMS_DRIVER_ID = "items_elasticsearch_private_driver"

# Public ES envelope drivers — membership in a tier's global public index is
# expressed by pinning these in ``operations[WRITE]`` (#1047 SSOT; post-#990
# canonical shape where the ES indexer rides WRITE with ASYNC + OUTBOX, as the
# items tier already does).  A collection is globally searchable when the
# public collection ES driver is pinned; a catalog is globally navigable when
# the public catalog ES driver is pinned.
_PUBLIC_COLLECTION_ES_DRIVER_ID = "collection_elasticsearch_driver"
_PUBLIC_CATALOG_ES_DRIVER_ID = "catalog_elasticsearch_driver"


def _items_routing_has_private_driver(routing: "ItemsRoutingConfig") -> bool:
    """Return True iff ``items_elasticsearch_private_driver`` is pinned in
    any operation of the given items routing config."""
    for entries in routing.operations.values():
        for entry in entries:
            if entry.driver_ref == _PRIVATE_ITEMS_DRIVER_ID:
                return True
    return False


def _operation_pins_driver(
    config: "PluginConfig",
    operation: str,
    driver_ref: str,
) -> bool:
    """Return True iff ``driver_ref`` is pinned in ``operations[operation]``
    of the given routing config.

    Tolerant of configs that omit ``operations`` (returns False) so callers
    can probe an arbitrary routing config without first proving its shape.
    """
    operations = getattr(config, "operations", None) or {}
    return any(
        entry.driver_ref == driver_ref
        for entry in operations.get(operation, [])
    )


def _collection_routing_is_public(routing: "CollectionRoutingConfig") -> bool:
    """Return True iff the collection routing config pins the public
    collection ES driver in ``operations[WRITE]`` — i.e. the collection
    envelope lands in the global ``{prefix}-collections`` index and is
    therefore globally searchable (#1047)."""
    return _operation_pins_driver(
        routing, Operation.WRITE, _PUBLIC_COLLECTION_ES_DRIVER_ID,
    )


def _catalog_routing_is_public(routing: "CatalogRoutingConfig") -> bool:
    """Return True iff the catalog routing config pins the public catalog ES
    driver in ``operations[WRITE]`` — i.e. the catalog envelope lands in the
    global ``{prefix}-catalogs`` index and the catalog is globally navigable
    (#1047)."""
    return _operation_pins_driver(
        routing, Operation.WRITE, _PUBLIC_CATALOG_ES_DRIVER_ID,
    )


def _assert_public_collection_has_public_parent(
    collection_routing: "CollectionRoutingConfig",
    parent_catalog_routing: Optional["CatalogRoutingConfig"],
) -> None:
    """Composition guard (#1047): a public-ES collection requires a public-ES
    parent catalog.

    Enforces SSOT rule 1 ("public collection ⇒ publicly-visible parent
    catalog"): a globally-searchable collection envelope under a catalog that
    is not itself in the public ``{prefix}-catalogs`` index would leak the
    collection into global search while its parent is not navigable. Rules 2
    (a public catalog may mix public + private collections) and 3 (a private
    catalog has no public children) both fall out of this single check — a
    private collection (no public ES pin) is always accepted, and a public
    collection under a private catalog is always rejected. IAM remains the
    access SSOT; the index split is defense-in-depth.

    No-op when the collection is not public. Raises ``ValueError`` (mapped to
    HTTP 400 by ``run_validate_handlers``) when the collection is public but
    the parent catalog routing config is missing or not public.
    """
    if not _collection_routing_is_public(collection_routing):
        return
    if parent_catalog_routing is not None and _catalog_routing_is_public(
        parent_catalog_routing
    ):
        return
    raise ValueError(
        "Composition guard: a public collection (CollectionRoutingConfig "
        f"pins '{_PUBLIC_COLLECTION_ES_DRIVER_ID}' in operations[WRITE]) "
        "requires its parent catalog to be public (CatalogRoutingConfig "
        f"must pin '{_PUBLIC_CATALOG_ES_DRIVER_ID}' in operations[WRITE]). "
        "A globally-searchable collection under a non-public catalog would "
        "leak the collection envelope into global search while the parent "
        "catalog is not navigable. Apply the 'public_catalog' preset (or "
        "pin the public catalog ES driver) on the parent catalog first, or "
        "keep this collection private (drop the public collection ES driver "
        "from operations[WRITE])."
    )


# ---------------------------------------------------------------------------
# Generic routing-active query helpers — entity-agnostic discovery layer
# ---------------------------------------------------------------------------
#
# Used by every module / OGC service that needs to know which driver(s) are
# active for a given operation on a given entity. Read the routing config
# (single SSOT) and apply the resolution semantics documented at the top
# of this file:
#
#   - WRITE:     fan-out across every listed driver; secondary-index
#                entries (secondary_index=True) propagate to search sinks.
#   - SEARCH:    single-driver. Default = first; driver_hint overrides.
#
# Transformers are not an operation: WRITE/SEARCH entries attach them via
# input_transformers/output_transformers refs into the ``transformers``
# registry; the composition runtime in
# modules/storage/transform_runtime.py applies the chain.
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


def secondary_index_entries(
    operations: Mapping[Any, Sequence["OperationDriverEntry"]],
    *,
    async_outbox_only: bool = False,
) -> List["OperationDriverEntry"]:
    """The secondary-index entries of a write list — role-based (#990).

    A secondary index is not a distinct operation: it is a ``WRITE`` entry
    whose ``secondary_index`` flag is set.  That flag is derived at
    config-build time from the driver's Indexer marker
    (``is_<tier>_indexer``) and persisted, so classification is independent
    of whether the indexer is installed in the current SCOPE — a
    configured-but-not-installed indexer is still recognised and routed to
    the OUTBOX.

    Role is orthogonal to policy: this preserves per-entry ``on_failure``
    (fatal / warn / outbox) and ``write_mode`` (sync / async) — a secondary
    index may be sync+fatal, which a ``write_mode==ASYNC`` proxy could not
    express.

    ``async_outbox_only`` restricts to the outbox-deferred subset
    (``write_mode=ASYNC`` & ``on_failure=OUTBOX``) — used by the in-line
    upsert/delete paths that enqueue outbox rows themselves, as opposed to
    the dispatcher fan-out which handles every failure policy.
    """
    def _keep(entry: "OperationDriverEntry") -> bool:
        if async_outbox_only:
            return (
                entry.write_mode == WriteMode.ASYNC
                and entry.on_failure == FailurePolicy.OUTBOX
            )
        return True

    out: List["OperationDriverEntry"] = []
    seen: Set[str] = set()
    for entry in operations.get(Operation.WRITE, []):
        if (
            entry.secondary_index
            and entry.driver_ref not in seen
            and _keep(entry)
        ):
            seen.add(entry.driver_ref)
            out.append(entry)
    return out


async def get_active_indexers(
    catalog_id: str,
    *,
    entity: EntityKindLiteral,
    collection_id: Optional[str] = None,
) -> Set[str]:
    """driver_ids of all secondary-index drivers for this entity.

    Multi-driver fan-out: write side has no merge ambiguity, every listed
    indexer fires. Returns empty set when no secondary-index entries exist.
    """
    ops = await _resolve_entity_operations(
        catalog_id, entity=entity, collection_id=collection_id,
    )
    return {
        entry.driver_ref
        for entry in secondary_index_entries(ops)
    }


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


def _self_register_transformers_into(
    transformers: List["TransformerEntry"],
) -> None:
    """Auto-append every installed ``EntityTransformProtocol`` implementer to
    the ``transformers`` registry (in place).

    Mirrors :func:`_self_register_indexers_into` and
    :func:`_self_register_searchers_into`. Operator-override: a no-op when the
    registry already contains any entry with ``source="operator"`` — an
    operator-authored registry is invariant under auto-augmentation
    (#792 / #889).

    Discovery is purely structural: any driver implementing
    ``EntityTransformProtocol`` is eligible. There is no separate capability
    flag — the protocol IS the marker.
    """
    from dynastore.models.protocols.entity_transform import EntityTransformProtocol
    from dynastore.tools.discovery import get_protocols

    if any(entry.source == "operator" for entry in transformers):
        return
    listed = {entry.driver_ref for entry in transformers}
    for transformer in get_protocols(EntityTransformProtocol):
        driver_ref = _to_snake(type(transformer).__name__)
        if driver_ref in listed:
            continue
        transformers.append(TransformerEntry(driver_ref=driver_ref, source="auto"))
        listed.add(driver_ref)
        logger.debug(
            "Routing config self-registration: appended EntityTransformProtocol "
            "driver '%s' to transformers registry (source=auto)",
            driver_ref,
        )
