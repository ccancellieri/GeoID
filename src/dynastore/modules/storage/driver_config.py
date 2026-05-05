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
Per-driver plugin configurations.

Each driver config subclass is identified by its class name (``class_key()``
defaults to ``__qualname__``).  The config is stored/retrieved via the existing
config API and 4-tier waterfall (collection > catalog > platform > code defaults).
Identity is the class itself; see ``class_key()`` in ``platform_config_service.py``.

Domain separation uses **class inheritance**:

- ``CollectionDriverConfig`` — base for collection-domain drivers
- ``AssetDriverConfig`` — base for asset-domain drivers

Capabilities describe *how* the driver operates (SYNC, ASYNC, TRANSACTIONAL,
etc.), not *what operation* it performs.  Operations are defined in
``ItemsRoutingConfig`` (see ``routing_config.py``).
"""

import json
import os
from enum import StrEnum
from typing import Annotated, Any, ClassVar, Dict, FrozenSet, List, Optional, Tuple

from pydantic import ConfigDict, Field, field_validator, model_validator

from dynastore.tools.secrets import Secret
from dynastore.tools.ui_hints import ui

from dynastore.models.protocols.typed_driver import _PluginDriverConfig
from dynastore.modules.db_config.platform_config_service import (
    Immutable,
    PluginConfig,
    WriteOnce,
)

# Sidecar machinery is PG-specific — the field type alias and its
# registry-based coercion live in ``pg_sidecars/base.py`` so that other
# storage drivers (DuckDB, Iceberg, Elasticsearch) don't see PG sidecar
# code at all.  Eager imports of the core sidecar config modules below
# trigger their ``SidecarConfigRegistry.register(...)`` side effects so
# extensions (e.g. STAC) can register on top.
from dynastore.modules.storage.drivers.pg_sidecars import (
    attributes_config as _attributes_config,  # noqa: F401 — registry side-effect
    geometries_config as _geometries_config,  # noqa: F401 — registry side-effect
    item_metadata_config as _item_metadata_config,  # noqa: F401 — registry side-effect
)
from dynastore.modules.storage.drivers.pg_sidecars.base import PgSidecarConfig

_PgSidecarConfig = PgSidecarConfig


# ---------------------------------------------------------------------------
# Driver-level configuration — env-var-based, follows DBConfig pattern
# ---------------------------------------------------------------------------


class DuckDBConfig:
    """DuckDB driver-level configuration (env vars).

    Controls connection pooling, resource limits, and extension loading for
    the DuckDB storage driver.  Follows the same pattern as
    ``DBConfig`` (``DB_POOL_*``) and Elasticsearch (``ES_*``).

    Per-collection configs (``ItemsDuckdbDriverConfig``) hold only file
    paths and format overrides; they should be relative to ``data_root``.
    """

    pool_size: int = int(os.getenv("DUCKDB_POOL_SIZE", "4"))
    max_memory: str = os.getenv("DUCKDB_MAX_MEMORY", "4GB")
    threads: int = int(os.getenv("DUCKDB_THREADS", "4"))
    extensions: str = os.getenv("DUCKDB_EXTENSIONS", "spatial")
    read_timeout: int = int(os.getenv("DUCKDB_READ_TIMEOUT", "30"))
    write_timeout: int = int(os.getenv("DUCKDB_WRITE_TIMEOUT", "60"))
    fetch_chunk_size: int = int(os.getenv("DUCKDB_FETCH_CHUNK_SIZE", "500"))
    data_root: str = os.getenv("DUCKDB_DATA_ROOT", "")


_iceberg_catalog_props_env = os.getenv("ICEBERG_CATALOG_PROPERTIES")


class IcebergConfig:
    """Iceberg driver-level configuration (env vars).

    Controls catalog connection, pool sizing, and timeouts for the Iceberg
    storage driver.  All connection-level settings live here; per-collection
    configs (``ItemsIcebergDriverConfig``) hold only table identifiers
    (``namespace``, ``table_name``, ``partition_spec``, etc.).
    """

    catalog_pool_size: int = int(os.getenv("ICEBERG_CATALOG_POOL_SIZE", "4"))
    catalog_timeout: int = int(os.getenv("ICEBERG_CATALOG_TIMEOUT", "30"))
    catalog_name: str = os.getenv("ICEBERG_CATALOG_NAME", "default")
    catalog_type: str = os.getenv("ICEBERG_CATALOG_TYPE", "sql")
    catalog_uri: Optional[str] = os.getenv("ICEBERG_CATALOG_URI")
    warehouse_uri: Optional[str] = os.getenv("ICEBERG_WAREHOUSE_URI")
    warehouse_scheme: Optional[str] = os.getenv("ICEBERG_WAREHOUSE_SCHEME")
    catalog_properties: Optional[Dict[str, Any]] = (
        json.loads(_iceberg_catalog_props_env) if _iceberg_catalog_props_env else None
    )


del _iceberg_catalog_props_env


# ---------------------------------------------------------------------------
# Driver capabilities — describe HOW the driver operates
# ---------------------------------------------------------------------------


class DriverCapability(StrEnum):
    """Standard capabilities describing driver behaviour."""

    SYNC = "SYNC"
    ASYNC = "ASYNC"
    TRANSACTIONAL = "TRANSACTIONAL"
    STREAMING = "STREAMING"
    BATCH = "BATCH"


# ---------------------------------------------------------------------------
# Collection-level write policy — driver-agnostic, applies to all drivers
# ---------------------------------------------------------------------------


class WriteConflictPolicy(StrEnum):
    """Item-level conflict policy — applied per entity when identity already exists.

    Drivers read this policy from ``CollectionWritePolicy`` via the config
    waterfall and apply it during ``write_entities()`` after identity is
    resolved by one of the configured :class:`IdentityMatcher` strategies.

    Actions:
      - ``UPDATE``          — overwrite the existing entity's mutable fields in place
      - ``NEW_VERSION``     — archive the existing row (set validity upper bound)
                              and insert a fresh one with a new ``geoid``
      - ``REFUSE``          — skip this entity, return None, continue the batch
      - ``REFUSE_RETURN``   — skip the insert but return the *existing* matched
                              feature to the caller (idempotent read-through)
      - ``REFUSE_FAIL``     — raise :class:`ConflictError` and abort the batch
    """

    UPDATE = "update"
    NEW_VERSION = "new_version"
    REFUSE = "refuse"
    REFUSE_RETURN = "refuse_return"
    REFUSE_FAIL = "refuse_fail"


class IdentityMatcher(StrEnum):
    """Strategies for deciding whether an incoming feature matches an existing one.

    Matchers are evaluated in the order declared on
    ``CollectionWritePolicy.identity_matchers``; the first one that resolves a
    record wins.  Each sidecar implements the matchers it owns via its
    ``resolve_existing_item(..., matcher=...)`` method.

    - ``EXTERNAL_ID``     — match on ``write_policy.external_id_field`` (attributes sidecar)
    - ``GEOHASH``         — match on geohash of the incoming geometry at
                            ``geohash_precision`` (geometries sidecar, uses ST_GeoHash)
    - ``GEOMETRY_HASH``   — match on SHA256 of the geometry (WKB).  Stored as
                            a hub column today; a follow-up (#220) relocates
                            it to the geometries sidecar as a STORED GENERATED
                            column via ``encode(digest(ST_AsBinary(geom),
                            'sha256'), 'hex')``.
    - ``ATTRIBUTES_HASH`` — match on SHA256 of the canonicalised attributes JSONB.
                            Stored as a STORED GENERATED column on the attributes
                            sidecar via ``encode(digest(attributes::text, 'sha256'), 'hex')``.
                            Recognises items whose attribute combination is identical
                            (regardless of geometry).

    Naming convention: ``<source>_hash`` for SHA256-of-source columns.
    Algorithm-specific spatial indexes (``geohash``, future ``h3``/``s2``)
    use the algorithm name directly.
    """

    EXTERNAL_ID = "external_id"
    GEOHASH = "geohash"
    GEOMETRY_HASH = "geometry_hash"
    ATTRIBUTES_HASH = "attributes_hash"


class AssetConflictPolicy(StrEnum):
    """Asset-level (batch-level) conflict policy — checked before item processing.

    When set, the driver checks whether any entity in the incoming asset batch
    has a duplicate identity before processing items.  A positive match triggers
    the configured response for the entire batch.

    This operates at a different level than ``WriteConflictPolicy`` (per-item).
    Both can be combined: e.g. ``on_conflict=REFUSE`` (skip individual item
    duplicates) and ``on_asset_conflict=REFUSE`` (reject entire batch if any
    duplicate is found).
    """

    REFUSE = "refuse_asset"     # hard stop — reject the entire asset batch


class CollectionWritePolicy(PluginConfig):
    """Collection-level write behaviour, applied by all capable drivers.

    Registered as ``CollectionWritePolicy`` in the config waterfall
    (identity: class_key) — collection > catalog > platform > code default.

    All drivers (PG, ES, Iceberg, DuckDB) read this single config during
    ``write_entities()`` via::

        configs = get_protocol(ConfigsProtocol)
        policy = await configs.get_config(
            CollectionWritePolicy, catalog_id=catalog_id, collection_id=collection_id
        )

    The ``context`` dict passed to ``write_entities()`` carries runtime values
    that override config defaults:

    - ``asset_id``             — source asset reference (from ingestion pipeline)
    - ``external_id_override`` — explicit external_id bypassing field extraction
    - ``valid_from``           — validity range start (ISO-8601 or datetime)
    - ``valid_to``             — validity range end (None = open-ended)

    Composable policies:
      ``on_conflict`` (item-level) and ``on_asset_conflict`` (batch-level) act
      independently — both can be set simultaneously.

    Identity matchers:
      ``identity_matchers`` is an ordered list; the first matcher that
      resolves a record wins. Unknown / unsupported matchers are silently
      skipped so a collection can opt into GEOHASH without requiring every
      sidecar to grow implementations at once. Each matcher is implemented
      by the sidecar that owns the underlying column — see ``IdentityMatcher``.

    Hash-gated versioning:
      When ``skip_if_unchanged_geometry_hash=True`` a match whose
      ``geometry_hash`` equals the incoming feature short-circuits the
      action: ``NEW_VERSION`` degrades to a no-op, ``UPDATE`` degrades to
      ``REFUSE_RETURN``. Enables "new version only when geometry differs".

    Layering vs ``WritePolicyDefaults`` (M8):
      ``CollectionWritePolicy`` is the collection-INTRINSIC config — carries
      field-name bindings (``external_id_field``, ``validity_field``) needed
      by existing write infrastructure. ``WritePolicyDefaults`` (sibling
      class) is the platform/catalog-tier POSTURE config — carries only
      posture flags (``on_conflict``, ``require_identity_key``) without
      field-name references. Field-name binding lives separately in
      ``CollectionSchema.constraints`` (``IdentityKeyConstraint``,
      ``ValidityConstraint``, ``ContentHashConstraint``). Operators
      configuring a NEW collection should set posture in
      ``WritePolicyDefaults`` at the platform tier and field bindings via
      ``CollectionSchema.constraints``; ``CollectionWritePolicy`` remains
      for legacy collections that bound fields here directly.

    Worked scenarios:

    1. **External-id versioning** (track every change as a new version)::

           CollectionWritePolicy(
               identity_matchers=[IdentityMatcher.EXTERNAL_ID],
               on_conflict=WriteConflictPolicy.NEW_VERSION,
               external_id_field="properties.code",
               skip_if_unchanged_geometry_hash=True,
           )

       Each upsert keyed on ``properties.code`` versions the existing row
       UNLESS the geometry_hash is identical (then it's a no-op).

    2. **Geohash dedup at city precision** (drop duplicate POIs)::

           CollectionWritePolicy(
               identity_matchers=[IdentityMatcher.GEOHASH],
               geohash_precision=6,           # ~1.2km
               on_conflict=WriteConflictPolicy.REFUSE_RETURN,
           )

       Incoming features falling within the same ~1.2km tile as an existing
       row are skipped; the existing row is returned.

    3. **Batch idempotency via geometry hash** (reject the whole asset on
       any geometry duplicate)::

           CollectionWritePolicy(
               identity_matchers=[IdentityMatcher.GEOMETRY_HASH],
               on_conflict=WriteConflictPolicy.UPDATE,
               on_asset_conflict=AssetConflictPolicy.REFUSE,
           )

       A re-uploaded asset (same geometries) is rejected entirely; partial
       overlap fails the whole batch with a 409.
    """
    _address: ClassVar[Tuple[str, str, Optional[str]]] = ("storage", "policy", None)
    _visibility: ClassVar[Optional[str]] = "collection"


    on_conflict: WriteConflictPolicy = Field(
        default=WriteConflictPolicy.UPDATE,
        examples=["update", "new_version", "refuse_return", "refuse_fail"],
        description=(
            "Item-level action when identity matches an existing row. "
            "``update`` overwrites mutable fields in place. "
            "``new_version`` archives the old row (sets validity upper bound) "
            "and inserts a fresh one with a new geoid. "
            "``refuse`` skips silently and continues the batch. "
            "``refuse_return`` skips but returns the matched row to the caller "
            "(idempotent read-through). "
            "``refuse_fail`` raises ConflictError and aborts the batch (use for "
            "strict-mode pipelines)."
        ),
    )
    on_asset_conflict: Optional[AssetConflictPolicy] = Field(
        default=None,
        examples=[None, "refuse_asset"],
        description=(
            "Asset-level (batch-level) conflict policy, checked BEFORE item "
            "processing begins. ``None`` (default) skips batch-level checks. "
            "``refuse_asset`` rejects the entire asset batch if any single "
            "entity conflicts — useful for re-upload idempotency where partial "
            "overlap should fail the whole asset."
        ),
    )
    identity_matchers: List[IdentityMatcher] = Field(
        default_factory=lambda: [IdentityMatcher.EXTERNAL_ID],
        examples=[
            ["external_id"],
            ["external_id", "geohash"],
            ["geometry_hash"],
            ["external_id", "attributes_hash"],
        ],
        description=(
            "Ordered matcher chain — first matcher returning a record wins. "
            "Matchers are delegated to the sidecar that owns the underlying "
            "column: ``external_id`` → attributes sidecar (reads "
            "``external_id_field``); ``geohash`` → geometries sidecar (uses "
            "ST_GeoHash at ``geohash_precision``); ``geometry_hash`` → "
            "geometries sidecar (SHA256 of WKB); ``attributes_hash`` → "
            "attributes sidecar (SHA256 of canonicalised attributes JSONB). "
            "Unknown matchers are silently skipped so a collection can opt "
            "into a strategy that requires a sidecar not yet enabled — the "
            "next matcher in the chain takes over."
        ),
    )
    geohash_precision: int = Field(
        default=9,
        ge=1,
        le=12,
        examples=[6, 9, 12],
        description=(
            "ST_GeoHash precision used when ``IdentityMatcher.GEOHASH`` is "
            "active. Cell sizes (latitude-dependent at the equator): "
            "1≈5000km, 4≈40km, 6≈1.2km, 9≈5m, 12≈4cm. Pick by what 'same place' "
            "means in your dataset: 6 for cities, 9 for parcels, 12 for points "
            "of interest."
        ),
    )
    skip_if_unchanged_geometry_hash: bool = Field(
        default=False,
        examples=[False, True],
        description=(
            "If True, matched features whose ``geometry_hash`` equals the "
            "incoming one bypass ``NEW_VERSION`` (treated as a no-op) and "
            "collapse ``UPDATE`` to ``REFUSE_RETURN``. Enables 'new version "
            "only when geometry differs'. Requires the geometries sidecar "
            "to be enabled (it computes geometry_hash on write)."
        ),
    )
    track_asset_id: bool = Field(
        default=True,
        examples=[True, False],
        description=(
            "Store the ``asset_id`` from the write context as a column on the "
            "entity document — provenance tracking. Disable only when source "
            "tracking is intentionally severed (e.g. derived collections "
            "produced from a join)."
        ),
    )
    external_id_field: Optional[str] = Field(
        default=None,
        examples=[None, "id", "properties.code", "properties.src_id"],
        description=(
            "Dot-notation path to extract ``external_id`` from the entity "
            "(e.g. ``id`` for Feature.id, ``properties.code`` for a domain "
            "code). When ``None``, conflict detection uses the geoid directly. "
            "When set, a fresh geoid is always generated on insert and conflict "
            "resolution uses the extracted external_id — the geoid becomes a "
            "stable internal handle while external_id is the operator-facing "
            "natural key."
        ),
    )
    require_external_id: bool = Field(
        default=False,
        examples=[False, True],
        description=(
            "If True, an entity whose ``external_id_field`` resolves to None "
            "or empty is refused with a 422. Pair with ``external_id_field`` "
            "to enforce that every row carries a domain key."
        ),
    )
    enable_validity: bool = Field(
        default=False,
        examples=[False, True],
        description=(
            "Track ``valid_from`` / ``valid_to`` temporal range per entity. "
            "Required for ``NEW_VERSION`` semantics — when False, "
            "``on_conflict=NEW_VERSION`` falls back to ``UPDATE``."
        ),
    )
    validity_field: str = Field(
        default="valid_from",
        examples=["valid_from", "properties.start_date", "valid_time.start"],
        description=(
            "Dot-notation path to extract validity start from the incoming "
            "entity. Used only when ``enable_validity=True``. Validity END is "
            "either provided in ``write_context.valid_to`` or computed as "
            "``None`` (open-ended)."
        ),
    )


# ---------------------------------------------------------------------------
# WritePolicyDefaults — M8: posture-only write policy (no field-name refs)
# ---------------------------------------------------------------------------


class WritePolicyDefaults(PluginConfig):
    """Posture-only write-policy defaults for the platform / catalog waterfall.

    Carries only posture flags — never references specific field names. This
    is the M8 cleanup target: write-policy posture (HOW conflicts are handled)
    is decoupled from field-binding (WHICH columns carry identity, validity,
    geometry hash). Field-binding lives in ``CollectionSchema.constraints``
    as ``IdentityKeyConstraint``, ``ValidityConstraint``, and
    ``GeometryHashConstraint`` instances — owned by the schema, not the
    write-policy config.

    Layering vs ``CollectionWritePolicy`` (sibling class):
      - **At platform / catalog tiers**: set posture defaults via
        ``WritePolicyDefaults``. Operators set "all collections in this
        catalog default to ``on_conflict=REFUSE_FAIL``" once at the catalog
        scope.
      - **At collection tier (legacy / field-name-binding cases)**: use
        ``CollectionWritePolicy`` for the field-name knobs
        (``external_id_field``, ``validity_field``) that pre-date the schema
        constraint model. New collections should declare bindings in
        ``CollectionSchema.constraints`` and leave ``CollectionWritePolicy``
        at code defaults.
      - When both are present, ``CollectionWritePolicy`` at collection tier
        wins for the fields it owns; ``WritePolicyDefaults`` at upstream
        tiers fills in the rest via the standard waterfall.

    Worked scenarios:

    1. **Platform-wide strict mode** (no silent skips anywhere)::

           # at platform tier:
           WritePolicyDefaults(
               on_conflict=WriteConflictPolicy.REFUSE_FAIL,
               require_identity_key=True,
           )

       Every collection refuses with a 409 on any duplicate AND must declare
       an IdentityKeyConstraint in its schema.

    2. **Per-catalog defaults** (loose at catalog A, strict at catalog B)::

           # at catalog A scope (e.g. ingestion landing zone):
           WritePolicyDefaults(on_conflict=WriteConflictPolicy.UPDATE)

           # at catalog B scope (e.g. authoritative master):
           WritePolicyDefaults(
               on_conflict=WriteConflictPolicy.REFUSE_FAIL,
               require_identity_key=True,
           )
    """
    _address: ClassVar[Tuple[str, str, Optional[str]]] = ("storage", "policy", None)
    _visibility: ClassVar[Optional[str]] = "collection"


    on_conflict: WriteConflictPolicy = Field(
        default=WriteConflictPolicy.UPDATE,
        examples=["update", "new_version", "refuse_return", "refuse_fail"],
        description=(
            "Item-level default action when identity matches. Cascades down "
            "the waterfall (platform → catalog → collection). Overridden at "
            "collection scope by ``CollectionWritePolicy.on_conflict`` if set. "
            "Use ``refuse_fail`` for strict-mode catalogs that must surface "
            "every duplicate as a 409."
        ),
    )
    on_asset_conflict: Optional[AssetConflictPolicy] = Field(
        default=None,
        examples=[None, "refuse_asset"],
        description=(
            "Asset-level (batch-level) conflict policy default. ``None`` "
            "disables batch-level checks at this tier. ``refuse_asset`` "
            "rejects the entire asset if any item conflicts. Cascades like "
            "``on_conflict``."
        ),
    )
    require_identity_key: bool = Field(
        default=False,
        examples=[False, True],
        description=(
            "If True, every collection at this scope MUST declare exactly one "
            "``IdentityKeyConstraint`` in its ``CollectionSchema.constraints``. "
            "Collections missing the constraint are rejected at write time. "
            "Set at platform / catalog tier to enforce identity-key discipline "
            "across all owned collections."
        ),
    )


# ---------------------------------------------------------------------------
# Base hierarchy
# ---------------------------------------------------------------------------


class DriverPluginConfig(_PluginDriverConfig):
    """Base for all per-driver configs.

    Inherits :class:`_PluginDriverConfig`, which auto-derives ``class_key()``
    from the ``TypedDriver[X]`` bind on the paired driver class.  Operator-
    facing JSON wire key drops the ``Config`` suffix and matches the routing
    entry's ``driver_id`` byte-for-byte.  No per-class ``class_key()``
    override is needed or allowed.

    Fields shared across all drivers:

    * ``capabilities`` — frozenset of :class:`DriverCapability` strings
      describing how the driver performs operations.
    """

    is_abstract_base: ClassVar[bool] = True

    capabilities: FrozenSet[str] = Field(
        default_factory=frozenset,
        description="How the driver operates: SYNC, ASYNC, TRANSACTIONAL, etc.",
    )


class CollectionDriverConfig(DriverPluginConfig):
    """Base for collection-domain driver configs.

    ``x-ui.category`` is inherited by subclasses and lets the admin
    Configuration Hub group every concrete collection driver under a single
    "Storage Drivers" section in the left rail.
    """

    is_abstract_base: ClassVar[bool] = True

    model_config = ConfigDict(json_schema_extra=ui(category="storage-drivers"))


class AssetDriverConfig(DriverPluginConfig):
    """Base for asset-domain driver configs.

    Grouped under "Asset Drivers" in the admin Configuration Hub.
    """

    is_abstract_base: ClassVar[bool] = True

    model_config = ConfigDict(json_schema_extra=ui(category="asset-drivers"))


# ---------------------------------------------------------------------------
# Collection-domain concrete configs
# ---------------------------------------------------------------------------


class ItemsPostgresqlDriverConfig(CollectionDriverConfig):
    """PostgreSQL collection driver config.

    Absorbs fields previously in ``PostgresStorageLocationConfig`` and
    PG-specific fields from ``CollectionPluginConfig`` (sidecars,
    partitioning).  ``collection_type`` was hoisted out of this class in
    Phase 1.6 — see ``CollectionType`` PluginConfig at collection scope.

    CRITICAL: ``sidecars`` and ``partitioning`` are **Immutable** — they
    cannot be changed once the physical table exists.
    """
    _address: ClassVar[Tuple[str, str, Optional[str]]] = ("storage", "drivers", "items")
    _visibility: ClassVar[Optional[str]] = "collection"


    model_config = ConfigDict(extra="allow")

    capabilities: FrozenSet[str] = Field(
        default=frozenset({DriverCapability.SYNC, DriverCapability.TRANSACTIONAL}),
    )

    # From PostgresStorageLocationConfig — WriteOnce: None → value allowed (set by
    # ensure_storage()); once set to a non-None value, mutation is rejected.
    physical_schema: WriteOnce[Optional[str]] = Field(
        default=None, description="Override auto-resolved schema. Set once by ensure_storage()."
    )
    physical_table: WriteOnce[Optional[str]] = Field(
        default=None, description="Override auto-resolved table. Set once by ensure_storage()."
    )

    # From CollectionPluginConfig — PG-specific structural fields.
    # Discriminated union via `sidecar_type`; default is EMPTY (M1b.2).
    # The old eager `[geometries, attributes]` default is gone — the PG
    # driver resolves sidecar defaults lazily at DDL / read / write time
    # via ``storage.drivers.pg_sidecars.resolver._effective_sidecars(...)``.
    # A default-body ``POST /collections/{id}`` therefore persists zero
    # ``collection_configs`` rows (plan §Principle — default-fast).
    sidecars: Immutable[List[_PgSidecarConfig]] = Field(
        default_factory=list,
        description=(
            "Sidecar table configs — discriminated union on `sidecar_type`. "
            "Empty → PG driver resolves defaults lazily from "
            "``pg_sidecars._effective_sidecars(...)`` (core defaults + "
            "registry injections) at first use."
        ),
    )
    partitioning: Immutable[Any] = Field(
        default_factory=lambda: _default_partitioning(),
        description="Composite partition config for PG tables.",
    )
    # NB: ``collection_type`` field hoisted out of this driver config in
    # Phase 1.6 — it's now a collection-scope ``CollectionType`` PluginConfig
    # (see ``modules/catalog/catalog_config.py``) so EVERY driver reads the
    # same value, not just the PG driver.  Drivers fetch it via
    # ``configs.get_config(CollectionType, catalog_id, collection_id)``.

    # ------------------------------------------------------------------
    # Validators (moved from CollectionPluginConfig)
    # ------------------------------------------------------------------

    # Note: the former ``validate_sidecars_polymorphic`` field-validator is
    # deleted here (M1b.1) — superseded by the ``_PgSidecarConfig``
    # discriminated union declared above, which does the same dispatch
    # natively via Pydantic's ``Discriminator("sidecar_type")``.  Main's
    # ``aa6b8e7`` fix (robust error messages on malformed inputs) is
    # subsumed by Pydantic's own ``ValidationError`` — which also fails
    # loudly on missing discriminators or unknown ``sidecar_type`` values
    # (strictly a tightening: main's version silently fell back to the
    # base ``SidecarConfig`` on unknown types).
    #
    # Phase 1.6: the former ``strip_geometry_for_records`` model_validator
    # is deleted too — it depended on ``collection_type`` being on this
    # class.  The geometry-strip-for-RECORDS logic now lives in
    # ``_effective_sidecars`` (the sidecar resolver), which receives the
    # collection_type from its async callers after they fetch the new
    # ``CollectionType`` PluginConfig.

    @field_validator("partitioning", mode="before")
    @classmethod
    def coerce_partitioning(cls, v: Any) -> Any:
        """Convert plain dict back to CompositePartitionConfig on deserialization."""
        if isinstance(v, dict):
            from dynastore.modules.catalog.catalog_config import CompositePartitionConfig
            return CompositePartitionConfig.model_validate(v)
        return v

    @model_validator(mode="after")
    def validate_composite_partitioning(self) -> "ItemsPostgresqlDriverConfig":
        """Validate partition keys are provided by configured sidecars."""
        if not self.partitioning.enabled:
            return self

        available_keys = {"transaction_time", "geoid"}
        for sidecar in self.sidecars:
            available_keys.update(sidecar.partition_key_contributions.keys())

        missing_keys = [
            k for k in self.partitioning.partition_keys if k not in available_keys
        ]
        if missing_keys:
            raise ValueError(
                f"Partition keys {missing_keys} are not provided by any configured sidecar. "
                f"Available keys: {available_keys}"
            )
        return self

    @model_validator(mode="after")
    def validate_sidecar_partition_mirroring(self) -> "ItemsPostgresqlDriverConfig":
        """Ensure all sidecars mirror the Hub's partition strategy."""
        if self.sidecars and self.partitioning.enabled:
            pass  # Enforced at sidecar DDL generation level
        return self

    def get_column_definitions(self) -> Dict[str, str]:
        """Hub table column definitions (sidecar columns are separate).

        Issue #220 cutover (no dual-write window): ``geometry_hash`` is
        now a STORED GENERATED column on the geometries sidecar
        (``encode(digest(ST_AsBinary(geom), 'sha256'), 'hex')``).
        Postgres maintains it atomically with the geometry — no
        application-side write path, no skew between hub-stored hash and
        actual stored geometry.  The matcher / hash gate JOIN the
        sidecar to read it.
        """
        return {
            "geoid": "UUID PRIMARY KEY",
            "transaction_time": "TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP",
            "deleted_at": "TIMESTAMPTZ",
        }

    def get_all_field_definitions(self) -> Dict[str, Any]:
        """Aggregate field definitions from all enabled sidecars."""
        all_fields: Dict[str, Any] = {}
        from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry

        for sc_config in self.sidecars:
            if not sc_config.enabled:
                continue
            sidecar = SidecarRegistry.get_sidecar(sc_config)
            if sidecar is None:
                continue
            all_fields.update(sidecar.get_field_definitions())
        return all_fields


def _default_partitioning() -> Any:
    """Lazy import to avoid circular dependency with catalog_config."""
    from dynastore.modules.catalog.catalog_config import CompositePartitionConfig

    return CompositePartitionConfig()


class ItemsElasticsearchDriverConfig(CollectionDriverConfig):
    """Elasticsearch collection driver config.

    Uses the stac-fastapi-elasticsearch-opensearch (SFEOS) library by
    convention.  The default ``index_prefix`` matches SFEOS's
    ``STAC_ITEMS_INDEX_PREFIX`` (``items_``), producing per-collection
    indexes like ``items_{collection_id}`` that are natively readable by
    an external SFEOS app running in read-only mode.
    """
    _address: ClassVar[Tuple[str, str, Optional[str]]] = ("storage", "drivers", "items")
    _visibility: ClassVar[Optional[str]] = "collection"


    model_config = ConfigDict(extra="allow")

    capabilities: FrozenSet[str] = Field(
        default=frozenset({DriverCapability.ASYNC}),
    )
    index_prefix: str = Field(
        default="items_",
        description=(
            "Item index name prefix.  Default ``items_`` matches SFEOS convention "
            "(env: STAC_ITEMS_INDEX_PREFIX), producing per-collection indexes "
            "``items_{collection_id}``."
        ),
    )
    mapping: Dict[str, Any] = Field(
        default_factory=dict,
        description="ES index mapping overrides merged with SFEOS defaults.",
    )


class ItemsElasticsearchPrivateDriverConfig(CollectionDriverConfig):
    """Tenant-scoped Elasticsearch driver config (a.k.a. "private").

    Pairs with :class:`~dynastore.modules.storage.drivers.elasticsearch_private.driver.ItemsElasticsearchPrivateDriver`,
    which writes the full feature (geometry + properties + external_id)
    into a per-tenant index named
    ``{index_prefix}-{catalog_id}-private-items`` (catalog-scoped, not
    collection-scoped — the same index is shared across all collections of
    a catalog).  DENY access policies are managed by the driver's own
    lifecycle.

    The driver currently reads its index prefix from the platform-wide
    env var ``STAC_ITEMS_INDEX_PREFIX`` via
    ``modules.elasticsearch.client.get_index_prefix``.  No per-collection
    field is exposed today; the config exists primarily as the TypedDriver
    identity marker so the driver appears in
    :func:`list_registered_configs` and the ``/configs/registry`` /
    ``/configs/.../plugins/items_elasticsearch_private_driver`` deep-view
    routes — fixing the visibility gap that previously hid this driver
    from the operator's deep view.
    """
    _address: ClassVar[Tuple[str, str, Optional[str]]] = ("storage", "drivers", "items")
    _visibility: ClassVar[Optional[str]] = "collection"

    model_config = ConfigDict(extra="allow")


class ItemsDuckdbDriverConfig(CollectionDriverConfig):
    """DuckDB collection driver config.

    Absorbs fields previously in ``FileStorageLocationConfig``.
    """
    _address: ClassVar[Tuple[str, str, Optional[str]]] = ("storage", "drivers", "items")
    _visibility: ClassVar[Optional[str]] = "collection"


    capabilities: FrozenSet[str] = Field(
        default=frozenset({DriverCapability.ASYNC, DriverCapability.BATCH}),
    )
    path: Optional[str] = Field(default=None, description="Read path (file or glob)")
    format: str = Field(default="parquet", description="File format: parquet, csv, json, etc.")
    write_path: Optional[str] = Field(default=None, description="Separate write path (e.g., SQLite file)"
    )
    write_format: Optional[str] = Field(default=None, description="Write format if different from read"
    )


_ICEBERG_CONNECTION_FIELDS: frozenset[str] = frozenset({
    "catalog_name", "catalog_uri", "catalog_type", "catalog_properties",
    "warehouse_uri", "warehouse_scheme",
})


class ItemsIcebergDriverConfig(CollectionDriverConfig):
    """Iceberg per-collection config — table location and DDL hints only.

    Connection-level settings (catalog type, URI, warehouse) must be configured
    via ``IcebergConfig`` environment variables. Use the ``resolve_*`` helpers
    to obtain the effective values at runtime.
    """
    _address: ClassVar[Tuple[str, str, Optional[str]]] = ("storage", "drivers", "items")
    _visibility: ClassVar[Optional[str]] = "collection"


    model_config = ConfigDict(extra="allow")

    @model_validator(mode="before")
    @classmethod
    def _reject_connection_level_fields(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        offenders = sorted(k for k in _ICEBERG_CONNECTION_FIELDS if values.get(k) is not None)
        if offenders:
            raise ValueError(
                f"Connection-level fields {offenders} must be set via IcebergConfig "
                f"environment variables, not per-collection overrides."
            )
        return values

    capabilities: FrozenSet[str] = Field(
        default=frozenset({DriverCapability.ASYNC, DriverCapability.BATCH}),
    )

    # Table location (per-collection identifiers)
    namespace: Optional[str] = Field(default=None, description="OTF namespace/database")
    table_name: Optional[str] = Field(default=None, description="OTF table name")
    uri: Optional[str] = Field(
        default=None, description="Primary URI (s3://, gs://, file://, etc.)"
    )

    # Connection-level overrides. Defaults to IcebergConfig env vars at resolve time.
    catalog_name: Optional[str] = Field(
        default=None,
        description="PyIceberg catalog identifier (overrides ICEBERG_CATALOG_NAME).",
    )
    catalog_type: Optional[str] = Field(
        default=None,
        description="Catalog type: sql | rest | hive | glue | dynamodb | nessie "
                    "(overrides ICEBERG_CATALOG_TYPE).",
    )
    catalog_uri: Optional[Secret] = Field(
        default=None,
        description="Catalog URI — JDBC URL for sql, HTTP endpoint for rest/nessie, "
                    "etc. (overrides ICEBERG_CATALOG_URI). **Treated as a Secret** "
                    "because JDBC URLs commonly embed credentials "
                    "(postgresql+psycopg2://user:pass@host/db).",
    )
    catalog_properties: Optional[Dict[str, Secret]] = Field(
        default=None,
        description="Extra catalog-specific properties merged into PyIceberg's "
                    "load_catalog kwargs (overrides ICEBERG_CATALOG_PROPERTIES). "
                    "Every value is treated as a Secret — these commonly carry "
                    "auth tokens, service-account JSON, or provider API keys.",
    )
    warehouse_uri: Optional[Secret] = Field(
        default=None,
        description="Warehouse location for tables managed by this catalog "
                    "(gs://, s3://, file://, etc.; overrides ICEBERG_WAREHOUSE_URI). "
                    "**Treated as a Secret** because signed / pre-authorized URLs "
                    "carry embedded tokens.",
    )
    warehouse_scheme: Optional[str] = Field(
        default=None,
        description="Scheme hint used when auto-deriving the warehouse from the "
                    "catalog's StorageProtocol (overrides ICEBERG_WAREHOUSE_SCHEME).",
    )

    # Table-level DDL hints (per-collection)
    partition_spec: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Iceberg partition spec as a list of field transforms, e.g. "
                    "[{'name': 'year', 'transform': 'year', 'source': 'event_date'}].",
    )
    sort_order: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Iceberg sort order as a list of sort fields, e.g. "
                    "[{'name': 'id', 'direction': 'asc', 'null_order': 'nulls-last'}].",
    )
    table_properties: Optional[Dict[str, str]] = Field(
        default=None,
        description="Iceberg table properties set on create/update "
                    "(e.g. {'write.format.default': 'parquet'}).",
    )

    def resolve_catalog_name(self) -> str:
        return self.catalog_name or IcebergConfig.catalog_name

    def resolve_catalog_type(self) -> str:
        return (self.catalog_type or IcebergConfig.catalog_type).lower()

    def resolve_catalog_uri(self) -> Optional[str]:
        """Return the plaintext catalog URI for the driver. Callers must never
        log or echo the return value."""
        if self.catalog_uri is not None:
            return self.catalog_uri.reveal()
        return IcebergConfig.catalog_uri

    def resolve_catalog_properties(self) -> Optional[Dict[str, Any]]:
        """Merge env-level and per-collection catalog_properties into a
        plaintext dict for driver consumption.

        Per-collection keys take precedence on collision so a tenant can
        override a platform default while inheriting the rest. Every
        per-collection value is revealed from its Secret wrapper.
        """
        env_props = IcebergConfig.catalog_properties or {}
        col_props = self.catalog_properties or {}
        if not env_props and not col_props:
            return None
        merged: Dict[str, Any] = dict(env_props)
        for k, v in col_props.items():
            merged[k] = v.reveal() if isinstance(v, Secret) else v
        return merged

    def resolve_warehouse_uri(self) -> Optional[str]:
        """Return the plaintext warehouse URI for the driver."""
        if self.warehouse_uri is not None:
            return self.warehouse_uri.reveal()
        return IcebergConfig.warehouse_uri

    def resolve_warehouse_scheme(self) -> Optional[str]:
        return self.warehouse_scheme or IcebergConfig.warehouse_scheme


# ---------------------------------------------------------------------------
# Asset-domain concrete configs
# ---------------------------------------------------------------------------


class AssetPostgresqlDriverConfig(AssetDriverConfig):
    """PostgreSQL asset driver config."""
    _address: ClassVar[Tuple[str, str, Optional[str]]] = ("storage", "drivers", "assets")
    _visibility: ClassVar[Optional[str]] = "collection"


    capabilities: FrozenSet[str] = Field(
        default=frozenset({DriverCapability.SYNC, DriverCapability.TRANSACTIONAL}),
    )


class AssetElasticsearchDriverConfig(AssetDriverConfig):
    """Elasticsearch asset driver config."""
    _address: ClassVar[Tuple[str, str, Optional[str]]] = ("storage", "drivers", "assets")
    _visibility: ClassVar[Optional[str]] = "collection"


    model_config = ConfigDict(extra="allow")

    capabilities: FrozenSet[str] = Field(
        default=frozenset({DriverCapability.ASYNC}),
    )
    index_prefix: str = Field("assets_", description="Asset index name prefix.")


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------

# CollectionWritePolicy / CollectionSchema auto-register via
# PluginConfig.__init_subclass__ — no explicit registration needed.

from dynastore.models.protocols.field_definition import (  # noqa: E402
    FeatureTypeDefinition as _FeatureTypeBase,
    FieldDefinition as _FieldDefinition,
    EntityLevel as _EntityLevel,
)


class CollectionSchema(PluginConfig):
    """Schema definition for a collection — registerable in the config waterfall.

    Defines the field structure and declarative constraints for collection items.
    Replaces the former ``FeatureTypeConfig`` (M8).

    Fields declared here drive DDL generation (PostgreSQL columns, Iceberg schema,
    DuckDB CREATE TABLE) and optional service-layer enforcement.

    ``constraints`` is an open list of :class:`~dynastore.modules.storage.schema_types.FieldConstraint`
    instances, e.g.::

        CollectionSchema(
            fields={"feature_id": FieldDefinition(data_type="text")},
            constraints=[
                IdentityKeyConstraint(geohash_precision=7),
                ValidityConstraint(field="valid_time"),
            ],
        )
    """
    _address: ClassVar[Tuple[str, str, Optional[str]]] = ("storage", "schema", None)
    _visibility: ClassVar[Optional[str]] = "collection"


    level: _EntityLevel = _EntityLevel.ITEM
    fields: Dict[str, _FieldDefinition] = Field(default_factory=dict)
    exclude_fields: Optional[List[str]] = None
    metadata_fields: Optional[Dict[str, Any]] = None
    allow_app_level_enforcement: bool = Field(
        default=False,
        description=(
            "If True, fall back to service-layer enforcement of required/unique "
            "constraints when the primary driver does not advertise "
            "REQUIRED_ENFORCEMENT/UNIQUE_ENFORCEMENT. If False (default), "
            "config admission is rejected for unsupported constraints."
        ),
    )
    strict_unknown_fields: bool = Field(
        default=False,
        description=(
            "When True, refuse writes whose features carry properties not "
            "listed in ``fields``. System fields (id, geoid, geometry, bbox, "
            "properties, etc.) always pass; only user-data fields are "
            "checked. Always service-layer enforcement — drivers that store "
            "JSON properties accept any field by default, so this is the "
            "ONLY way to constrain the schema shape on write. Maps to HTTP "
            "422 with the offending field list. Default False keeps the "
            "current open-schema behavior."
        ),
    )
    materialize_fields_as_columns: bool = Field(
        default=False,
        description=(
            "When True, every field declared in ``fields`` is lifted into a "
            "native column on the PG attributes sidecar — even when the "
            "field carries no required/unique constraint. Enables per-field "
            "indexes, ANALYZE statistics, and column-store query plans. "
            "Default False keeps the historical behaviour of lifting only "
            "constrained fields (plain fields stay in the JSONB properties "
            "blob). Opt-in because materialising every field widens the "
            "table and increases write IO for sparse schemas."
        ),
    )
    constraints: List[Any] = Field(
        default_factory=list,
        description=(
            "Declarative field constraints (FieldConstraint subclass instances). "
            "Examples: IdentityKeyConstraint, ValidityConstraint, GeometryHashConstraint."
        ),
    )


# ---------------------------------------------------------------------------
# Apply handler — write_policy ↔ CollectionSchema cross-validation (Task E)
# ---------------------------------------------------------------------------

import logging as _logging  # noqa: E402

_logger = _logging.getLogger(__name__)

_ALWAYS_VALID_EXTERNAL_ID_FIELDS = frozenset({"geoid", "id"})


async def _on_apply_write_policy(
    config: PluginConfig,
    catalog_id: "Optional[str]",
    collection_id: "Optional[str]",
    db_resource: "Optional[Any]",
) -> None:
    """Cross-validate write_policy.external_id_field against CollectionSchema.fields.

    If ``external_id_field`` is set and a ``CollectionSchema`` exists at the
    same scope, the referenced field must appear in ``CollectionSchema.fields``.

    ``external_id_field = "geoid"`` or ``"id"`` are always accepted (system fields).
    If ``CollectionSchema`` is not yet configured, validation is skipped.
    """
    if not isinstance(config, CollectionWritePolicy):
        return
    ext_id = config.external_id_field
    if not ext_id or ext_id in _ALWAYS_VALID_EXTERNAL_ID_FIELDS:
        return

    if not (catalog_id and collection_id):
        return  # only validate at collection scope

    # Extract leaf field name from dot-path (e.g. "properties.code" → "code")
    field_key = ext_id.split(".")[-1]

    try:
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        configs = get_protocol(ConfigsProtocol)
        if not configs:
            return

        schema = await configs.get_config(
            CollectionSchema,
            catalog_id=catalog_id,
            collection_id=collection_id,
        )
        defined_fields = getattr(schema, "fields", {})
        if not defined_fields:
            return  # no fields defined yet — skip validation

        if field_key not in defined_fields:
            raise ValueError(
                f"write_policy.external_id_field '{ext_id}' (field key: '{field_key}') "
                f"is not defined in CollectionSchema.fields for {catalog_id}/{collection_id}. "
                f"Defined fields: {sorted(defined_fields)}. "
                f"Set 'geoid' or 'id' to use system identity fields without schema restriction."
            )
    except ValueError:
        raise
    except Exception as exc:
        _logger.debug(
            "write_policy cross-validation skipped for %s/%s: %s",
            catalog_id, collection_id, exc,
        )


CollectionWritePolicy.register_apply_handler(_on_apply_write_policy)


# ---------------------------------------------------------------------------
# Apply handler — CollectionSchema required/unique vs primary-driver capabilities
# ---------------------------------------------------------------------------


async def _on_apply_collection_schema(
    config: PluginConfig,
    catalog_id: "Optional[str]",
    collection_id: "Optional[str]",
    db_resource: "Optional[Any]",
) -> None:
    """Validate that constrained fields are supported by the primary write driver.

    If any ``FieldDefinition`` declares ``required=True`` or ``unique=True``,
    the collection's primary WRITE driver must advertise the matching
    ``Capability.REQUIRED_ENFORCEMENT`` / ``UNIQUE_ENFORCEMENT`` — unless
    ``allow_app_level_enforcement=True`` is set, which opts into service-layer
    fallback enforcement.
    """
    if not isinstance(config, CollectionSchema):
        return
    if not (catalog_id and collection_id):
        return

    constrained_required = [n for n, f in config.fields.items() if f.required]
    constrained_unique = [n for n, f in config.fields.items() if f.unique]
    if not constrained_required and not constrained_unique:
        return
    if config.allow_app_level_enforcement:
        return

    try:
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.models.protocols.storage_driver import (
            Capability,
            CollectionItemsStore,
        )
        from dynastore.modules.storage.routing_config import (
            Operation,
            ItemsRoutingConfig,
        )
        from dynastore.tools.discovery import get_protocol, get_protocols

        configs = get_protocol(ConfigsProtocol)
        if not configs:
            return

        routing = await configs.get_config(
            ItemsRoutingConfig,
            catalog_id=catalog_id,
            collection_id=collection_id,
        )
        write_entries = (routing.operations or {}).get(Operation.WRITE) or []
        if not write_entries:
            return
        primary_id = write_entries[0].driver_id

        drivers = get_protocols(CollectionItemsStore) or []
        primary = next(
            (d for d in drivers if getattr(d, "__class__", type(d)).__name__ == primary_id),
            None,
        )
        if primary is None:
            return
        caps = getattr(primary, "capabilities", frozenset())

        if constrained_required and Capability.REQUIRED_ENFORCEMENT not in caps:
            raise ValueError(
                f"CollectionSchema declares required=True fields {constrained_required} "
                f"but primary write driver '{primary_id}' lacks REQUIRED_ENFORCEMENT. "
                f"Switch primary driver, drop the constraints, or set "
                f"allow_app_level_enforcement=True to opt into service-layer fallback."
            )
        if constrained_unique and Capability.UNIQUE_ENFORCEMENT not in caps:
            raise ValueError(
                f"CollectionSchema declares unique=True fields {constrained_unique} "
                f"but primary write driver '{primary_id}' lacks UNIQUE_ENFORCEMENT. "
                f"Switch primary driver, drop the constraints, or set "
                f"allow_app_level_enforcement=True to opt into service-layer fallback."
            )
    except ValueError:
        raise
    except Exception as exc:
        _logger.debug(
            "schema constraint validation skipped for %s/%s: %s",
            catalog_id, collection_id, exc,
        )


CollectionSchema.register_apply_handler(_on_apply_collection_schema)
