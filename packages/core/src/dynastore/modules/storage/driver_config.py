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
from typing import Any, ClassVar, Dict, FrozenSet, List, Optional, Tuple

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from dynastore.tools.secrets import Secret
from dynastore.tools.ui_hints import ui

from dynastore.models.protocols.typed_driver import _PluginDriverConfig
from dynastore.models.mutability import Immutable, Mutable, WriteOnce
from dynastore.modules.db_config.plugin_config import PluginConfig
from dynastore.modules.storage.computed_fields import (
    ComputedField,
    ComputedKind,
    IdentityRule,
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
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    InvalidGeometryPolicy,
    SridMismatchPolicy,
    SimplificationAlgorithm,
)

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

    Drivers read this policy from ``ItemsWritePolicy`` via the config
    waterfall and apply it during ``write_entities()`` after identity is
    resolved by the configured :class:`IdentityRule` chain (each rule's
    ``match_on`` lists :class:`ComputedField` strategies).

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


class GeometriesWriteBehavior(BaseModel):
    """Per-row geometry transform and validation behaviour applied during
    write.

    Lives under :class:`ItemsWritePolicy` as the ``geometries`` sub-block so
    operator-facing geometry write knobs sit alongside identity / conflict
    knobs (single policy plugin, one waterfall lookup, one document to
    teach). Storage-shape concerns (``target_srid``, ``target_dimension``,
    column names, partitioning, statistics) remain on
    :class:`~dynastore.modules.storage.drivers.pg_sidecars.geometries_config.GeometriesSidecarConfig`
    because they affect DDL.

    Consumed by :func:`dynastore.tools.geospatial.process_geometry`, which
    receives both the sidecar config (storage shape) and this behaviour
    block (per-row policy) and applies them in order: SRID transform,
    invalid-geom fix/reject, dimension force, simplification,
    vertex-normalisation, type allow-list.
    """
    model_config = ConfigDict(extra="forbid")

    invalid_geom_policy: InvalidGeometryPolicy = Field(
        default=InvalidGeometryPolicy.ATTEMPT_FIX,
        description=(
            "Action when an incoming geometry fails ``is_valid``. "
            "``attempt_fix`` runs Shapely's ``make_valid``; rows that "
            "remain invalid raise ``UnfixableGeometryError``. ``reject`` "
            "raises ``InvalidGeometryError`` immediately."
        ),
    )
    srid_mismatch_policy: SridMismatchPolicy = Field(
        default=SridMismatchPolicy.TRANSFORM,
        description=(
            "Action when the incoming SRID differs from the sidecar's "
            "``target_srid``. ``transform`` reprojects via pyproj (requires "
            "the ``crs`` extra). ``reject`` raises ``SridMismatchError``."
        ),
    )
    allowed_geometry_types: List[str] = Field(
        default_factory=list,
        examples=[[], ["Polygon", "MultiPolygon"], ["Point"]],
        description=(
            "If non-empty, rejects rows whose Shapely ``geom_type`` is not "
            "in the list (after fix/transform). Empty list (default) "
            "accepts any type."
        ),
    )
    simplification_algorithm: Optional[SimplificationAlgorithm] = Field(
        default=None,
        description=(
            "If set together with ``simplification_tolerance``, simplifies "
            "the geometry on write. ``douglas_peucker`` is faster but can "
            "break topology between adjacent features; "
            "``topology_preserving`` is slower but safe for adjacent "
            "polygons."
        ),
    )
    simplification_tolerance: Optional[float] = Field(
        default=None,
        description=(
            "Tolerance (in target_srid units, usually degrees for 4326 or "
            "metres for projected) for the configured simplification "
            "algorithm. Has no effect when ``simplification_algorithm`` "
            "is None."
        ),
    )
    remove_redundant_vertices: bool = Field(
        default=False,
        description=(
            "When True, runs Shapely ``normalize()`` after simplification, "
            "which also fixes winding order."
        ),
    )


def _default_identity_rules() -> List[IdentityRule]:
    """Default identity chain: single rule matching on ``external_id``.

    Operators who want different identity semantics replace the list; the
    default works for collections that bind ``external_id`` to a properties
    path via a corresponding ``ComputedField(kind=EXTERNAL_ID, name="properties.X")``
    in :attr:`ItemsWritePolicy.compute`.
    """
    return [IdentityRule(match_on=[ComputedField(kind=ComputedKind.EXTERNAL_ID)])]


class ItemsWritePolicy(PluginConfig):
    """Item-level write behaviour, applied by all capable drivers.

    Registered as ``ItemsWritePolicy`` in the config waterfall
    (identity: class_key, ``items_write_policy``) — collection-scoped only.

    All drivers (PG, ES, Iceberg, DuckDB) read this single config during
    ``write_entities()`` via::

        configs = get_protocol(ConfigsProtocol)
        policy = await configs.get_config(
            ItemsWritePolicy, catalog_id=catalog_id, collection_id=collection_id
        )

    Four irreducible concerns, plus three posture flags. See
    ``docs/architecture/items-policy-consolidation-957-950.md``:

    - :attr:`schema` — self-contained JSON Schema describing the wire shape
      of ``properties``. ``type``/``description``/``default``/``required``
      per field. The OpenAPI body schema, admin-UI form, write-time
      validation, and the read-side ``schema_ref`` all consult this single
      object. Setting ``None`` is permissive (no validation, no defaults).
    - :attr:`compute` — ordered list of :class:`ComputedField` entries the
      drivers materialise per row (geometry hash, attribute hash, geohash
      cell, area, centroid, …). The :class:`ComputedKind.EXTERNAL_ID`
      entry's ``name`` carries the source path (e.g. ``properties.code``).
    - :attr:`identity` — ordered list of :class:`IdentityRule`. Each rule
      ANDs its ``match_on`` ComputedFields; rules OR across the list (first
      match wins). Per-rule ``on_match`` overrides :attr:`on_conflict`.
    - :attr:`geometries` — per-row geometry transform / validation block
      (SRID, fix, simplify, allow-list). Runs before :attr:`compute`.

    Posture flags: :attr:`on_conflict`, :attr:`on_asset_conflict`,
    :attr:`enable_validity` / :attr:`validity_field`,
    :attr:`skip_if_unchanged_geometry_hash`, :attr:`track_asset_id`.

    The ``context`` dict passed to ``write_entities()`` carries runtime values
    that override config defaults:

    - ``asset_id``             — source asset reference (from ingestion pipeline)
    - ``external_id_override`` — explicit external_id bypassing field extraction
    - ``valid_from``           — validity range start (ISO-8601 or datetime)
    - ``valid_to``             — validity range end (None = open-ended)

    Hash-gated versioning:
      When ``skip_if_unchanged_geometry_hash=True`` a match whose
      ``geometry_hash`` equals the incoming feature short-circuits the
      action: ``NEW_VERSION`` degrades to a no-op, ``UPDATE`` degrades to
      ``REFUSE_RETURN``. Enables "new version only when geometry differs".

    Worked scenarios:

    1. **External-id versioning** (track every change as a new version)::

           ItemsWritePolicy(
               on_conflict=WriteConflictPolicy.NEW_VERSION,
               compute=[ComputedField(kind=ComputedKind.EXTERNAL_ID, name="properties.code")],
               skip_if_unchanged_geometry_hash=True,
           )

       Each upsert keyed on ``properties.code`` versions the existing row
       UNLESS the geometry_hash is identical (then it's a no-op).

    2. **Geohash dedup at city precision**::

           ItemsWritePolicy(
               compute=[ComputedField(kind=ComputedKind.GEOHASH, resolution=6)],
               identity=[IdentityRule(
                   match_on=[ComputedField(kind=ComputedKind.GEOHASH, resolution=6)],
                   on_match=WriteConflictPolicy.REFUSE_RETURN,
               )],
           )

    3. **Composite identity** ("same geometry AND same attributes is a duplicate")::

           ItemsWritePolicy(
               compute=[
                   ComputedField(kind=ComputedKind.GEOMETRY_HASH),
                   ComputedField(kind=ComputedKind.ATTRIBUTES_HASH),
               ],
               identity=[IdentityRule(match_on=[
                   ComputedField(kind=ComputedKind.GEOMETRY_HASH),
                   ComputedField(kind=ComputedKind.ATTRIBUTES_HASH),
               ], on_match=WriteConflictPolicy.REFUSE_RETURN)],
           )
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "items", "policy")
    _visibility: ClassVar[Optional[str]] = "collection"


    on_conflict: Mutable[WriteConflictPolicy] = Field(
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
    on_asset_conflict: Mutable[Optional[AssetConflictPolicy]] = Field(
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
    schema: Mutable[Optional[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Self-contained JSON Schema (Draft 2020-12) describing the wire "
            "shape of feature ``properties``. Carries ``type``, ``description``, "
            "``default``, ``required``, ``additionalProperties`` per property; "
            "no second source of truth (sidecar, driver, model docstring) "
            "carries any of these. ``None`` = permissive (no validation, no "
            "defaults). Set ``additionalProperties: false`` for strict ingest."
        ),
    )
    compute: Mutable[List[ComputedField]] = Field(
        default_factory=list,
        description=(
            "Per-row derived values materialised by every capable driver. "
            "Each :class:`ComputedField` declares ``kind`` (EXTERNAL_ID, "
            "GEOMETRY_HASH, ATTRIBUTES_HASH, GEOHASH/H3/S2 with resolution, "
            "AREA, PERIMETER, …) and an optional ``name`` override. The "
            "EXTERNAL_ID entry's ``name`` doubles as the source path for "
            "extraction (e.g. ``properties.adm2_pcode``). A "
            ":class:`ComputedKind.GEOHASH` entry with a resolution is the "
            "identity-axis spatial-cell rule; distinct from "
            "``GeometriesSidecarConfig.geohash_precision`` which controls the "
            "stored ``CHAR(N)`` column width (storage layer, not identity)."
        ),
    )
    identity: Mutable[List[IdentityRule]] = Field(
        default_factory=_default_identity_rules,
        description=(
            "Ordered identity rules. Each rule ANDs every ComputedField in "
            "its ``match_on``; rules OR across the list (first whose AND "
            "conjunction matches wins). Per-rule ``on_match`` overrides "
            ":attr:`on_conflict` for that branch. Default is a single rule "
            "matching on EXTERNAL_ID — operators replace the list to express "
            "geometry-hash dedup, composite identity, etc."
        ),
    )
    skip_if_unchanged_geometry_hash: Mutable[bool] = Field(
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
    track_asset_id: Mutable[bool] = Field(
        default=True,
        examples=[True, False],
        description=(
            "Store the ``asset_id`` from the write context as a column on the "
            "entity document — provenance tracking. Disable only when source "
            "tracking is intentionally severed (e.g. derived collections "
            "produced from a join)."
        ),
    )
    enable_validity: Mutable[bool] = Field(
        default=False,
        examples=[False, True],
        description=(
            "Track ``valid_from`` / ``valid_to`` temporal range per entity. "
            "Required for ``NEW_VERSION`` semantics — when False, "
            "``on_conflict=NEW_VERSION`` falls back to ``UPDATE``."
        ),
    )
    validity_field: Mutable[str] = Field(
        default="valid_from",
        examples=["valid_from", "properties.start_date", "valid_time.start"],
        description=(
            "Dot-notation path to extract validity start from the incoming "
            "entity. Used only when ``enable_validity=True``. Validity END is "
            "either provided in ``write_context.valid_to`` or computed as "
            "``None`` (open-ended)."
        ),
    )
    geometries: Mutable[GeometriesWriteBehavior] = Field(
        default_factory=GeometriesWriteBehavior,
        description=(
            "Per-row geometry transform and validation behaviour. See "
            ":class:`GeometriesWriteBehavior` — knobs were previously "
            "co-located on ``GeometriesSidecarConfig`` next to DDL fields; "
            "moved here so operators tune write behaviour from a single "
            "policy plugin while the sidecar config remains "
            "storage-shape-only."
        ),
    )

    # ------------------------------------------------------------------
    # Helpers — derive driver-facing values from compute / schema
    # ------------------------------------------------------------------

    def _all_compute_fields(self) -> List[ComputedField]:
        """Union of explicit ``compute`` entries plus every ``match_on``
        field referenced by identity rules.

        Drivers materialise this set — fields referenced for identity
        resolution have to be present even if the operator forgot to
        repeat them in ``compute``.
        """
        seen: Dict[str, ComputedField] = {}
        for cf in self.compute:
            seen.setdefault(cf.resolved_name, cf)
        for rule in self.identity:
            for cf in rule.match_on:
                seen.setdefault(cf.resolved_name, cf)
        return list(seen.values())

    def find_compute(
        self, kind: ComputedKind, resolution: Optional[int] = None
    ) -> Optional[ComputedField]:
        """Return the first ComputedField matching ``kind`` (and optionally
        ``resolution``), searched across ``compute`` and identity rules."""
        for cf in self._all_compute_fields():
            if cf.kind != kind:
                continue
            if resolution is not None and cf.resolution != resolution:
                continue
            return cf
        return None

    def external_id_path(self) -> Optional[str]:
        """Dot-walk path used to extract the external_id from an incoming
        feature, or None when external_id identity is not configured.

        Reads the ``name`` override on the first ``ComputedField(kind=EXTERNAL_ID)``
        in either ``compute`` or any identity rule's ``match_on``.

        Resolution order applied by extraction callers:
          1. Top-level lookup: ``data[path]`` (e.g. ``id``, ``asset_id``).
          2. Dot-walk: ``a.b.c`` traverses nested dicts (e.g. ``properties.code``).
          3. Properties fallback: when ``path`` has no dot AND
             ``data[path]`` is None, ``data['properties'][path]`` is tried.
        """
        cf = self.find_compute(ComputedKind.EXTERNAL_ID)
        if cf is None:
            return None
        # ``name`` on EXTERNAL_ID is the source path (no leading "properties."
        # collapse — that's resolved at extraction time).
        return cf.name or None

    def external_id_required(self) -> bool:
        """True iff the JSON Schema declares the external-id property required.

        Reads the leaf segment of :meth:`external_id_path` and checks the
        top-level ``required`` list on :attr:`schema`. With no schema or no
        external_id_path, this returns False.
        """
        path = self.external_id_path()
        if not path or not isinstance(self.schema, dict):
            return False
        leaf = path.split(".")[-1]
        required = self.schema.get("required") or []
        return leaf in required


# ---------------------------------------------------------------------------
# Base hierarchy
# ---------------------------------------------------------------------------


class DriverPluginConfig(_PluginDriverConfig):
    """Base for all per-driver configs.

    Inherits :class:`_PluginDriverConfig`, which provides:

    * ``class_key()`` — auto-derived from the ``TypedDriver[X]`` bind on
      the paired driver class.  Operator-facing JSON wire key drops the
      ``Config`` suffix and matches the routing entry's ``driver_id``
      byte-for-byte.
    * ``engine_ref: Optional[str]`` field (Cycle F.2) — name of the
      platform engine this driver instance binds to.
    * ``required_engine_class: ClassVar[str]`` — the ``engine_class``
      discriminator of the platform engine this driver class consumes.
    * Validator that defaults ``engine_ref`` to ``required_engine_class``
      and rejects incompatible refs.

    Class-level traits (NOT Pydantic fields — surface via the driver registry):

    * ``capabilities`` — ``ClassVar`` frozenset of :class:`DriverCapability`
      strings describing how the driver performs operations. Structural to
      the driver class; operators inspect via
      ``GET /configs/registry/{class_key}`` (the ``describedby`` link),
      never via the composed-config payload. See umbrella #665 and #678.
    """

    is_abstract_base: ClassVar[bool] = True

    capabilities: ClassVar[FrozenSet[str]] = frozenset()


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
    Phase 1.6 — see ``CollectionInfo`` PluginConfig at collection scope.

    CRITICAL: ``sidecars`` and ``partitioning`` are **Immutable** — they
    cannot be changed once the physical table exists.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "items", "drivers")
    _visibility: ClassVar[Optional[str]] = "collection"

    required_engine_class: ClassVar[str] = "postgresql_engine"


    model_config = ConfigDict(extra="allow")

    capabilities: ClassVar[FrozenSet[str]] = frozenset(
        {DriverCapability.SYNC, DriverCapability.TRANSACTIONAL}
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
            "Sidecar table configs — discriminated union on ``sidecar_type``. "
            "Lifecycle: empty until the collection is materialised "
            "(``ensure_storage()``).  At materialisation the PG driver runs "
            "``_effective_sidecars(...)`` — core defaults for the "
            "collection_type plus extension-registered sidecars (e.g. "
            "``ItemMetadataSidecarConfig`` from STAC) — and persists the "
            "resolved list onto this field, alongside ``physical_table``.  "
            "After that point this list IS the snapshot of what runs; ``Immutable`` "
            "blocks PATCH from changing it.  Discriminator values: "
            "``geometries`` (GeometriesSidecarConfig — wkb/wkt + ST_GeoHash + "
            "GENERATED geometry_hash; VECTOR/RASTER only); ``attributes`` "
            "(FeatureAttributeSidecarConfig — JSONB or columnar attribute "
            "storage + GENERATED attributes_hash; always-on); "
            "``item_metadata`` (ItemMetadataSidecarConfig — multilingual "
            "title/description/keywords); ``stac_metadata`` "
            "(StacItemsSidecarConfig — STAC external_extensions / "
            "external_assets / extra_fields overlay)."
        ),
    )
    partitioning: Immutable[Any] = Field(
        default_factory=lambda: _default_partitioning(),
        description="Composite partition config for PG tables.",
    )
    # NB: ``collection_type`` field hoisted out of this driver config in
    # Phase 1.6 — it's now a collection-scope ``CollectionInfo`` PluginConfig
    # (see ``modules/catalog/catalog_config.py``) so EVERY driver reads the
    # same value, not just the PG driver.  Drivers fetch it via
    # ``configs.get_config(CollectionInfo, catalog_id, collection_id)``.

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
    # ``CollectionInfo`` PluginConfig.

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

    Items land in the per-tenant index ``{prefix}-{catalog_id}-items``
    (the prefix comes from the platform-wide
    ``modules.elasticsearch.client.get_index_prefix`` — set once at boot
    from ``ES_INDEX_PREFIX``, not per-catalog) keyed by
    ``_routing=collection_id`` so a single index hosts every collection
    of one catalog with shard locality per collection.

    Per-catalog operator knob — :attr:`mapping` is the Tier-2 overlay on
    top of the platform Tier-1 known-fields map (see
    :mod:`dynastore.modules.elasticsearch.items_projection`). Additive
    only: collisions with Tier 1 are rejected at config validate time
    via :func:`validate_tier_2`.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "items", "drivers")
    _visibility: ClassVar[Optional[str]] = "collection"

    required_engine_class: ClassVar[str] = "elasticsearch_engine"


    model_config = ConfigDict(extra="allow")

    capabilities: ClassVar[FrozenSet[str]] = frozenset({DriverCapability.ASYNC})
    mapping: Mutable[Dict[str, Dict[str, Any]]] = Field(
        default_factory=dict,
        description=(
            "Tier-2 known-fields overlay: ``{stac_field: {ES field-type "
            "definition}}``. Merged with the platform Tier-1 set at "
            "index-create time. Additive only — keys cannot collide with "
            "Tier 1 at a different type (rejected by validate_tier_2). "
            "Snapshot at index-create; live edits take effect on next "
            "index rebuild (ES does not allow tightening a live mapping)."
        ),
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
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "items", "drivers")
    _visibility: ClassVar[Optional[str]] = "collection"

    required_engine_class: ClassVar[str] = "elasticsearch_engine"

    model_config = ConfigDict(extra="allow")

    capabilities: ClassVar[FrozenSet[str]] = frozenset({DriverCapability.ASYNC})


class ItemsDuckdbDriverConfig(CollectionDriverConfig):
    """DuckDB collection driver config.

    Absorbs fields previously in ``FileStorageLocationConfig``.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "items", "drivers")
    _visibility: ClassVar[Optional[str]] = "collection"

    required_engine_class: ClassVar[str] = "duckdb_engine"


    capabilities: ClassVar[FrozenSet[str]] = frozenset(
        {DriverCapability.ASYNC, DriverCapability.BATCH}
    )
    path: Mutable[Optional[str]] = Field(default=None, description="Read path (file or glob)")
    format: Mutable[str] = Field(default="parquet", description="File format: parquet, csv, json, etc.")
    write_path: Mutable[Optional[str]] = Field(default=None, description="Separate write path (e.g., SQLite file)"
    )
    write_format: Mutable[Optional[str]] = Field(default=None, description="Write format if different from read"
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
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "items", "drivers")
    _visibility: ClassVar[Optional[str]] = "collection"

    required_engine_class: ClassVar[str] = "iceberg_engine"


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

    capabilities: ClassVar[FrozenSet[str]] = frozenset(
        {DriverCapability.ASYNC, DriverCapability.BATCH}
    )

    # Table location (per-collection identifiers)
    namespace: Mutable[Optional[str]] = Field(default=None, description="OTF namespace/database")
    table_name: Mutable[Optional[str]] = Field(default=None, description="OTF table name")
    uri: Mutable[Optional[str]] = Field(
        default=None, description="Primary URI (s3://, gs://, file://, etc.)"
    )

    # Connection-level overrides. Defaults to IcebergConfig env vars at resolve time.
    catalog_name: Mutable[Optional[str]] = Field(
        default=None,
        description="PyIceberg catalog identifier (overrides ICEBERG_CATALOG_NAME).",
    )
    catalog_type: Mutable[Optional[str]] = Field(
        default=None,
        description="Catalog type: sql | rest | hive | glue | dynamodb | nessie "
                    "(overrides ICEBERG_CATALOG_TYPE).",
    )
    catalog_uri: Mutable[Optional[Secret]] = Field(
        default=None,
        description="Catalog URI — JDBC URL for sql, HTTP endpoint for rest/nessie, "
                    "etc. (overrides ICEBERG_CATALOG_URI). **Treated as a Secret** "
                    "because JDBC URLs commonly embed credentials "
                    "(postgresql+psycopg2://user:pass@host/db).",
    )
    catalog_properties: Mutable[Optional[Dict[str, Secret]]] = Field(
        default=None,
        description="Extra catalog-specific properties merged into PyIceberg's "
                    "load_catalog kwargs (overrides ICEBERG_CATALOG_PROPERTIES). "
                    "Every value is treated as a Secret — these commonly carry "
                    "auth tokens, service-account JSON, or provider API keys.",
    )
    warehouse_uri: Mutable[Optional[Secret]] = Field(
        default=None,
        description="Warehouse location for tables managed by this catalog "
                    "(gs://, s3://, file://, etc.; overrides ICEBERG_WAREHOUSE_URI). "
                    "**Treated as a Secret** because signed / pre-authorized URLs "
                    "carry embedded tokens.",
    )
    warehouse_scheme: Mutable[Optional[str]] = Field(
        default=None,
        description="Scheme hint used when auto-deriving the warehouse from the "
                    "catalog's StorageProtocol (overrides ICEBERG_WAREHOUSE_SCHEME).",
    )

    # Table-level DDL hints (per-collection)
    partition_spec: Mutable[Optional[List[Dict[str, Any]]]] = Field(
        default=None,
        description="Iceberg partition spec as a list of field transforms, e.g. "
                    "[{'name': 'year', 'transform': 'year', 'source': 'event_date'}].",
    )
    sort_order: Mutable[Optional[List[Dict[str, Any]]]] = Field(
        default=None,
        description="Iceberg sort order as a list of sort fields, e.g. "
                    "[{'name': 'id', 'direction': 'asc', 'null_order': 'nulls-last'}].",
    )
    table_properties: Mutable[Optional[Dict[str, str]]] = Field(
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
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "assets", "drivers")
    _visibility: ClassVar[Optional[str]] = "collection"

    required_engine_class: ClassVar[str] = "postgresql_engine"


    capabilities: ClassVar[FrozenSet[str]] = frozenset(
        {DriverCapability.SYNC, DriverCapability.TRANSACTIONAL}
    )


class AssetElasticsearchDriverConfig(AssetDriverConfig):
    """Elasticsearch asset driver config.

    The asset index name (``{prefix}-{catalog_id}-assets``) is composed
    from the platform-wide prefix returned by
    ``modules.elasticsearch.client.get_index_prefix`` (env
    ``ES_INDEX_PREFIX``, set once at boot). No per-catalog override —
    the dead ``index_prefix`` field was dropped per #756 Round 6 (it
    had zero call sites; the actual prefix is module-global).
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "assets", "drivers")
    _visibility: ClassVar[Optional[str]] = "collection"

    required_engine_class: ClassVar[str] = "elasticsearch_engine"


    model_config = ConfigDict(extra="allow")

    capabilities: ClassVar[FrozenSet[str]] = frozenset({DriverCapability.ASYNC})


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------

# ItemsWritePolicy / ItemsSchema auto-register via
# PluginConfig.__init_subclass__ — no explicit registration needed.

from dynastore.models.protocols.field_definition import (  # noqa: E402
    FieldDefinition as _FieldDefinition,
    EntityLevel as _EntityLevel,
)


class ItemsSchema(PluginConfig):
    """Schema definition for a collection — registerable in the config waterfall.

    Defines the field structure and declarative constraints for collection items.
    Replaces the former ``FeatureTypeConfig`` (M8).

    Fields declared here drive DDL generation (PostgreSQL columns, Iceberg schema,
    DuckDB CREATE TABLE) and optional service-layer enforcement.

    ``constraints`` is an open list of :class:`~dynastore.modules.storage.schema_types.FieldConstraint`
    instances, e.g.::

        ItemsSchema(
            fields={"name": FieldDefinition(data_type="text")},
            constraints=[
                RequiredConstraint(field="name"),
                UniqueConstraint(field="name"),
            ],
        )
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "items", "schema")
    _visibility: ClassVar[Optional[str]] = "collection"


    level: Mutable[_EntityLevel] = _EntityLevel.ITEM
    fields: Mutable[Dict[str, _FieldDefinition]] = Field(default_factory=dict)
    exclude_fields: Mutable[Optional[List[str]]] = None
    metadata_fields: Mutable[Optional[Dict[str, Any]]] = None
    allow_app_level_enforcement: Mutable[bool] = Field(
        default=False,
        description=(
            "If True, fall back to service-layer enforcement of required/unique "
            "constraints when the primary driver does not advertise "
            "REQUIRED_ENFORCEMENT/UNIQUE_ENFORCEMENT. If False (default), "
            "config admission is rejected for unsupported constraints."
        ),
    )
    strict_unknown_fields: Mutable[bool] = Field(
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
    materialize_fields_as_columns: Mutable[bool] = Field(
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
    constraints: Mutable[List[Any]] = Field(
        default_factory=list,
        description=(
            "Declarative field constraints (FieldConstraint subclass instances). "
            "Examples: RequiredConstraint, UniqueConstraint."
        ),
    )


# ---------------------------------------------------------------------------
# Apply handler — write_policy ↔ ItemsSchema cross-validation (Task E)
# ---------------------------------------------------------------------------

import logging as _logging  # noqa: E402

_logger = _logging.getLogger(__name__)

_ALWAYS_VALID_EXTERNAL_ID_FIELDS = frozenset({"geoid", "id"})


async def _validate_write_policy(
    config: PluginConfig,
    catalog_id: "Optional[str]",
    collection_id: "Optional[str]",
    db_resource: "Optional[Any]",
) -> None:
    """Cross-validate the EXTERNAL_ID ComputedField path against ItemsSchema.fields.

    If a ``ComputedField(kind=EXTERNAL_ID)`` declares a ``name`` and an
    ``ItemsSchema`` exists at the same scope, the referenced field must
    appear in ``ItemsSchema.fields``.

    Path "geoid" or "id" is always accepted (system fields). If
    ``ItemsSchema`` is not yet configured, validation is skipped.
    """
    if not isinstance(config, ItemsWritePolicy):
        return
    ext_id = config.external_id_path()
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
            ItemsSchema,
            catalog_id=catalog_id,
            collection_id=collection_id,
        )
        defined_fields = getattr(schema, "fields", {})
        if not defined_fields:
            return  # no fields defined yet — skip validation

        if field_key not in defined_fields:
            raise ValueError(
                f"ItemsWritePolicy ComputedField(kind=EXTERNAL_ID, name='{ext_id}') "
                f"(field key: '{field_key}') is not defined in ItemsSchema.fields "
                f"for {catalog_id}/{collection_id}. "
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


ItemsWritePolicy.register_validate_handler(_validate_write_policy)


# ---------------------------------------------------------------------------
# Validate handler — ItemsSchema required/unique vs primary-driver capabilities
# ---------------------------------------------------------------------------


async def _validate_items_schema(
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
    if not isinstance(config, ItemsSchema):
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
        primary_id = write_entries[0].driver_ref

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
                f"ItemsSchema declares required=True fields {constrained_required} "
                f"but primary write driver '{primary_id}' lacks REQUIRED_ENFORCEMENT. "
                f"Switch primary driver, drop the constraints, or set "
                f"allow_app_level_enforcement=True to opt into service-layer fallback."
            )
        if constrained_unique and Capability.UNIQUE_ENFORCEMENT not in caps:
            raise ValueError(
                f"ItemsSchema declares unique=True fields {constrained_unique} "
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


ItemsSchema.register_validate_handler(_validate_items_schema)


# ---------------------------------------------------------------------------
# Validate handler — ItemsElasticsearchDriverConfig.mapping Tier-2 overlay
# ---------------------------------------------------------------------------


async def _validate_items_es_driver_config(
    config: PluginConfig,
    catalog_id: "Optional[str]",
    collection_id: "Optional[str]",
    db_resource: "Optional[Any]",
) -> None:
    """Reject Tier-2 collisions before they corrupt the alias contract.

    The platform-wide alias ``{prefix}-items`` only works as a single
    queryable surface when every member index types the same field name
    the same way. Tier-2 overlay must therefore be additive only — see
    :func:`validate_tier_2` for the exact rule.
    """
    if not isinstance(config, ItemsElasticsearchDriverConfig):
        return
    from dynastore.modules.elasticsearch.items_projection import validate_tier_2

    validate_tier_2(config.mapping)


ItemsElasticsearchDriverConfig.register_validate_handler(_validate_items_es_driver_config)


# Resolve the forward reference on IdentityRule.on_match now that
# WriteConflictPolicy is defined in this module. computed_fields.py declares
# the field as ``Optional["WriteConflictPolicy"]`` to break the otherwise
# circular import (driver_config → computed_fields → driver_config).
IdentityRule.model_rebuild(_types_namespace={"WriteConflictPolicy": WriteConflictPolicy})
