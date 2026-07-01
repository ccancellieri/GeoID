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
import logging
import os
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, ClassVar, Dict, FrozenSet, List, Mapping, Optional, Tuple

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
from dynastore.models.mutability import Computed, Immutable, Mutable
from dynastore.models.plugin_config import PluginConfig
from dynastore.modules.storage.computed_fields import (
    _STATISTIC_STORAGE_KINDS,
    ComputedField,
    ComputedKind,
    DeriveSpec,
    IdentityRule,
)
from dynastore.modules.storage.validity import ValiditySpec

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

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Driver-level configuration — env-var-based, follows DBConfig pattern
# ---------------------------------------------------------------------------


class DuckDBConfig:
    """DuckDB driver-level configuration (env vars).

    Controls resource limits and extension loading for the DuckDB storage
    driver.  Follows the same pattern as ``DBConfig`` (``DB_POOL_*``) and
    Elasticsearch (``ES_*``).

    Per-collection configs (``ItemsDuckdbDriverConfig``) hold only file
    paths and format overrides; they should be relative to ``data_root``.

    Connection-pool sizing is NOT here: it is behavioral (an operator tunes
    it live to relieve read-throughput pressure) rather than a boot-time
    infra constant, so it lives on ``DuckdbEngineConfig.pool_size``
    (``modules/db_config/engine_config.py``) — the hot-reloadable config
    registry entry the driver reads at ``lifespan()`` time. See
    ``drivers/duckdb.py::_read_live_pool_size``.
    """

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


class BatchConflictPolicy(StrEnum):
    """Batch-level conflict policy — an all-or-nothing pre-flight guard.

    When set, before processing any items the driver scans the incoming ingest
    batch and checks whether any feature collides with an existing feature's
    identity. A single positive match aborts the ENTIRE batch — nothing is
    written. This is a coarse, batch-wide gate, not a per-item action.

    This is distinct from, and orthogonal to, two other "conflict" knobs:

    - ``WriteConflictPolicy`` (``ItemsWritePolicy.on_conflict``) — per-item
      identity collision handling (update / new_version / refuse / …). It acts
      row-by-row as items are processed.
    - ``AssetsWritePolicy.on_conflict`` — re-registration of the same physical
      asset/file. It governs whether an already-known asset may be re-ingested,
      and has nothing to do with feature identity.

    Batch-level and per-item policies compose: e.g. ``on_conflict=REFUSE``
    (skip individual duplicate rows) plus ``on_batch_conflict=REFUSE`` (reject
    the whole batch up front if any feature collides).
    """

    REFUSE = "refuse_batch"     # hard stop — reject the entire ingest batch


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
    path via ``DeriveSpec.external_id`` in :attr:`ItemsWritePolicy.derive`.
    The ``"external_id"`` axis is always referenceable even without a path
    (its value then comes from ``write_context.external_id_override`` or the
    feature id).

    A ``geometry_hash`` skip-on-match rule is appended at model-validation
    time when :attr:`ItemsWritePolicy.derive.content_hashes` includes
    ``"geometry"`` (see :meth:`ItemsWritePolicy._append_geometry_hash_default`).
    """
    return [IdentityRule(match_on=["external_id"])]


@dataclass(frozen=True)
class ResolvedIdentityRule:
    """An :class:`IdentityRule` with its ``match_on`` names resolved to the
    engine's :class:`ComputedField` objects.

    Produced by :meth:`ItemsWritePolicy.resolved_identity`; consumed by the
    write-path identity matcher (``item_distributed``) which needs each
    field's ``kind`` to drive sidecar lookups. Not an authored/config type.
    """

    match_on: List[ComputedField]
    on_match: Optional[WriteConflictPolicy] = None


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

    Three irreducible concerns, plus three posture flags. See
    ``docs/architecture/items-policy-consolidation-957-950.md``:

    - :attr:`derive` — the per-row derivations grouped into homogeneous
      buckets (:class:`DeriveSpec`): ``external_id`` (a dotted source path),
      ``content_hashes``, ``spatial_cells``, ``geometry_stats`` and
      ``attribute_stats``. The engine-facing flat list is the read-only
      :attr:`compute` property. Accepts compute-preset shorthand.
    - :attr:`identity` — ordered list of :class:`IdentityRule`. Each rule
      ANDs the derivation *names* in its ``match_on`` (referencing ``derive``
      outputs); rules OR across the list (first match wins). Per-rule
      ``on_match`` overrides :attr:`on_conflict`.
    - :attr:`geometries` — per-row geometry transform / validation block
      (SRID, fix, simplify, allow-list). Runs before the :attr:`derive`
      derivations.

    The wire JSON-Schema for feature ``properties`` is NOT carried on this
    policy. It is derived from ``ItemsSchema`` at read time inside
    ``ItemService.get_collection_schema`` (via ``derive_wire_schema``);
    ``items_schema`` is the single source of truth for field
    types/constraints, and required-ness is enforced on the write path by the
    normal required-field check plus NOT NULL columns.

    Posture flags: :attr:`on_conflict`, :attr:`on_batch_conflict`,
    :attr:`validity` (null-object :class:`ValiditySpec`; :attr:`enable_validity`
    is a derived read-only property), :attr:`track_asset_id`. Validity is a
    driver-abstracted concept — the policy carries where the validity start/end
    values come from, never a physical column name (each driver owns its own
    storage layout).

    Materialisation freeze: the fields that bake into a collection's
    stored shape or identity — :attr:`derive`, :attr:`identity`,
    :attr:`geometries`, :attr:`validity` — are ``Immutable``. The
    materialization-gated enforcer leaves them freely editable until the first
    collection lands, then freezes them so a catalog-tier default change cannot
    silently re-resolve into (and diverge) already-materialised collections.
    The behaviour knobs (:attr:`on_conflict`, :attr:`on_batch_conflict`,
    :attr:`track_asset_id`) stay ``Mutable`` — they alter future write
    behaviour without rewriting existing rows.

    The ``context`` dict passed to ``write_entities()`` carries runtime values
    that override config defaults:

    - ``asset_id``             — source asset reference (from ingestion pipeline)
    - ``external_id_override`` — explicit external_id bypassing field extraction
    - ``valid_from``           — validity range start (ISO-8601 or datetime)
    - ``valid_to``             — validity range end (None = open-ended)

    Hash-gated versioning:
      Authoring ``derive.content_hashes=["geometry"]`` auto-appends a
      ``IdentityRule(match_on=["geometry_hash"], on_match=REFUSE_RETURN)``
      to the default identity chain — a match on the byte-exact
      ``geometry_hash`` of the incoming feature short-circuits the write
      and echoes the existing row. To opt out, author an explicit
      :attr:`identity` list that omits the rule (or set ``on_match`` to a
      different policy).

    Worked scenarios:

    1. **External-id versioning** (track every change as a new version)::

           ItemsWritePolicy(
               on_conflict=WriteConflictPolicy.NEW_VERSION,
               derive=DeriveSpec(
                   external_id="properties.code",
                   content_hashes=["geometry"],
               ),
           )

       Each upsert keyed on ``properties.code`` versions the existing row.
       The auto-appended ``geometry_hash`` rule short-circuits identical
       geometries to ``REFUSE_RETURN``.

    2. **Geohash dedup at city precision**::

           ItemsWritePolicy(
               derive=DeriveSpec(spatial_cells=[SpatialCell(grid="geohash", resolution=6)]),
               identity=[IdentityRule(
                   match_on=["geohash_6"],
                   on_match=WriteConflictPolicy.REFUSE_RETURN,
               )],
           )

    3. **Composite identity** ("same geometry AND same attributes is a duplicate")::

           ItemsWritePolicy(
               derive=DeriveSpec(content_hashes=["geometry", "attributes"]),
               identity=[IdentityRule(
                   match_on=["geometry_hash", "attributes_hash"],
                   on_match=WriteConflictPolicy.REFUSE_RETURN,
               )],
           )
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "items", "policy")
    _freeze_at: ClassVar[Optional[str]] = "collection"


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
    on_batch_conflict: Mutable[Optional[BatchConflictPolicy]] = Field(
        default=None,
        examples=[None, "refuse_batch"],
        description=(
            "Batch-level conflict guard, evaluated BEFORE any item is processed. "
            "``None`` (default) skips the batch-wide pre-flight check. "
            "``refuse_batch`` scans the incoming ingest batch and rejects it in "
            "full if ANY feature collides with an existing feature's identity — "
            "an all-or-nothing gate for high-integrity pipelines where partial "
            "overlap must fail the whole batch. This is unrelated to "
            "``AssetsWritePolicy.on_conflict`` (which governs re-registration of "
            "the same physical asset/file), and orthogonal to ``on_conflict`` "
            "(the per-item identity action applied row-by-row)."
        ),
    )
    derive: Immutable[DeriveSpec] = Field(
        default_factory=DeriveSpec,
        description=(
            "Per-row derivations materialised by every capable driver, grouped "
            "into homogeneous buckets (:class:`DeriveSpec`): ``external_id`` "
            "(dotted source path), ``content_hashes`` "
            "(``geometry``/``attributes``), ``spatial_cells`` "
            "(geohash/h3/s2 + resolution), ``geometry_stats`` "
            "(area/centroid/bbox/…) and ``attribute_stats`` (a property "
            "promoted to the attributes sidecar). Accepts a compute-preset "
            "name (or list of names) as shorthand — it is classified into the "
            "canonical buckets at save. A spatial cell here is the "
            "identity-axis key AND the source for the persisted geohash/h3/s2 "
            "index columns: the stored ``geohash CHAR(N)`` column width is "
            "DERIVED from the geohash cell's resolution (Phase 3 — the former "
            "standalone ``GeometriesSidecarConfig.geohash_precision`` knob is "
            "gone, so the stored precision can never diverge from the identity "
            "axis). "
            "Immutable once a collection is materialised: the derived "
            "columns and identity axis are baked into existing rows, so "
            "changing this after data lands would silently diverge the shape "
            "of already-materialised collections. Tune it before the first "
            "collection is created."
        ),
    )

    @property
    def compute(self) -> List[ComputedField]:
        """Engine view of :attr:`derive`: the flat :class:`ComputedField`
        list every driver/sidecar consumes.

        Derived, read-only — :attr:`derive` is the single authored source of
        truth. Deduped by ``resolved_name`` (last wins).
        """
        return self.derive.to_computed_fields()

    identity: Immutable[List[IdentityRule]] = Field(
        default_factory=_default_identity_rules,
        description=(
            "Ordered identity rules. Each rule ANDs the derivation **names** "
            "in its ``match_on`` (referencing ``derive`` outputs such as "
            "``external_id``, ``geometry_hash``, ``geohash_7``); rules OR "
            "across the list (first whose AND conjunction matches wins). "
            "Per-rule ``on_match`` overrides :attr:`on_conflict` for that "
            "branch. Default is a single rule matching on ``external_id`` — "
            "operators replace the list to express geometry-hash dedup, "
            "composite identity, etc. Every name must resolve to a ``derive`` "
            "output (``external_id`` is always available). Immutable once a "
            "collection is materialised: identity determines how new "
            "writes dedupe against existing rows, so changing it after data "
            "lands would apply different identity semantics to old vs new "
            "rows. Set it before the first collection is created."
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
    enable_batch_insert: Mutable[bool] = Field(
        default=True,
        description=(
            "Enable the batched bulk-insert fast path for Phase 4 chunk writes. "
            "When True, each chunk collapses identity resolution into a single "
            "ANY-lookup and hub+sidecar rows into multi-row INSERT statements, "
            "reducing per-feature round-trips from ~5 to ~3 per chunk. "
            "Enabled by default — validated on live data with ~6x ingestion "
            "speedup. The per-row path is the automatic fallback for the UPDATE "
            "partition, NEW_VERSION archival, geohash/attributes_hash identity "
            "matchers, and any batch exception, so correctness is always "
            "preserved. Set False to revert to per-row for a specific collection."
        ),
    )
    validity: Immutable[Optional[ValiditySpec]] = Field(
        default=None,
        description=(
            "Null-object SSOT for temporal validity (:class:`ValiditySpec`). "
            "Its presence IS the toggle: a non-None spec enables ``valid_from`` "
            "/ ``valid_to`` tracking per entity; ``None`` disables validity "
            "entirely. ``ValiditySpec.column`` NAMES the ``tstzrange`` storage "
            "column (a COLUMN NAME, NOT a source path — the PG driver overlays "
            "it onto the attributes sidecar at ``ensure_storage`` to name and "
            "gate the column), resolving the column-name-vs-source-path "
            "confusion. The validity "
            "VALUES come from ``ValiditySpec.start_from`` / ``end_from``: "
            "``\"context\"`` reads ``write_context.valid_from`` / "
            "``valid_to`` (the default for start), any other string is a dotted "
            "source path walked into the feature, and ``None`` makes that bound "
            "open (``start_from=None`` → open lower bound, ``end_from=None`` → "
            "open upper bound — the bounds are fully independent). Required "
            "(non-None) for ``NEW_VERSION`` semantics — "
            "when ``None``, ``on_conflict=NEW_VERSION`` falls back to "
            "``UPDATE``. Immutable once a collection is materialised: "
            "the column is a physical temporal column, so changing it after the "
            "column exists would orphan the old column and diverge existing "
            "collections. Set it before the first collection is created."
        ),
    )

    @property
    def enable_validity(self) -> bool:
        """True when a :class:`ValiditySpec` is set (null-object pattern).

        Derived, read-only. ``validity`` is the single source of truth; all
        drivers gate temporal-column emission on this property so the bool and
        the spec can never independently diverge.
        """
        return self.validity is not None

    geometries: Immutable[GeometriesWriteBehavior] = Field(
        default_factory=GeometriesWriteBehavior,
        description=(
            "Per-row geometry transform and validation behaviour. See "
            ":class:`GeometriesWriteBehavior` — knobs were previously "
            "co-located on ``GeometriesSidecarConfig`` next to DDL fields; "
            "moved here so operators tune write behaviour from a single "
            "policy plugin while the sidecar config remains "
            "storage-shape-only. Immutable once a collection is materialised: "
            "SRID / simplification settings shape the stored geometry, so "
            "changing them after rows land would diverge the geometry of "
            "existing collections from the new default. Tune it before the "
            "first collection is created."
        ),
    )

    # ------------------------------------------------------------------
    # Helpers — derive driver-facing values from compute / schema
    # ------------------------------------------------------------------

    def _compute_by_name(self) -> Dict[str, ComputedField]:
        """Map ``resolved_name`` -> :class:`ComputedField` over :attr:`compute`."""
        return {cf.resolved_name: cf for cf in self.compute}

    def resolved_identity(self) -> List["ResolvedIdentityRule"]:
        """Resolve each identity rule's ``match_on`` names to ComputedField.

        ``"external_id"`` resolves to the declared external-id derivation, or
        to a path-less ``EXTERNAL_ID`` field when no extraction path is set
        (the value then comes from the write context / feature id). Any other
        name must already be a declared :attr:`derive` output — enforced at
        config-save by :meth:`_validate_identity_refs`, re-checked here
        defensively. Consumed by the write-path identity matcher.
        """
        cmap = self._compute_by_name()
        rules: List[ResolvedIdentityRule] = []
        for rule in self.identity:
            fields: List[ComputedField] = []
            for name in rule.match_on:
                cf = cmap.get(name)
                if cf is None:
                    if name == "external_id":
                        cf = ComputedField(kind=ComputedKind.EXTERNAL_ID)
                    else:
                        raise ValueError(
                            "IdentityRule.match_on references unknown derived "
                            f"field {name!r}; declared: {sorted(cmap)}"
                        )
                fields.append(cf)
            rules.append(
                ResolvedIdentityRule(match_on=fields, on_match=rule.on_match)
            )
        return rules

    def _all_compute_fields(self) -> List[ComputedField]:
        """Union of :attr:`compute` plus every identity ``match_on`` field.

        Drivers materialise this set — an identity axis referenced by name
        (e.g. a path-less ``external_id``) must be present even when it adds
        no stored column of its own.
        """
        seen: Dict[str, ComputedField] = {}
        for cf in self.compute:
            seen.setdefault(cf.resolved_name, cf)
        for rule in self.resolved_identity():
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

    @field_validator("derive", mode="before")
    @classmethod
    def _coerce_derive(cls, v: Any) -> Any:
        """Accept a :class:`DeriveSpec`, a buckets dict, or compute-preset
        shorthand (a preset name or list of names) for ``derive``.

        Preset shorthand expands via the compute-preset registry and is
        classified into the canonical buckets, so ``"geometry_stats"`` and
        ``["spatial_cells", "geometry_full"]`` both normalise to a
        :class:`DeriveSpec`; a dict is validated as a :class:`DeriveSpec`
        directly.
        """
        if v is None:
            return DeriveSpec()
        if isinstance(v, DeriveSpec):
            return v
        if isinstance(v, (str, list)):
            from dynastore.modules.storage.compute_presets import resolve_compute

            return DeriveSpec.from_computed_fields(resolve_compute(v))
        return v

    @model_validator(mode="after")
    def _append_geometry_hash_default(self) -> "ItemsWritePolicy":
        # WHY: geometry_hash is the geometries sidecar's PG-generated
        # CHAR(64) (sha256 of ST_AsBinary(geom)) — byte-exact, not semantic.
        # When the operator authors content_hashes=["geometry"] they are
        # opting in to deriving the column; the natural default is to skip
        # writes that produce an identical geometry. REFUSE_RETURN gives
        # idempotent read-through (echo the existing row), matching the
        # legacy hash-gate semantics.
        if (
            self.identity == [IdentityRule(match_on=["external_id"])]
            and "geometry" in self.derive.content_hashes
            and not any("geometry_hash" in r.match_on for r in self.identity)
        ):
            self.identity = list(self.identity) + [
                IdentityRule(
                    match_on=["geometry_hash"],
                    on_match=WriteConflictPolicy.REFUSE_RETURN,
                )
            ]
        return self

    @model_validator(mode="after")
    def _validate_identity_refs(self) -> "ItemsWritePolicy":
        """Fail-fast: every identity ``match_on`` name must resolve to a
        ``derive`` output (``external_id`` is always available)."""
        allowed = set(self._compute_by_name()) | {"external_id"}
        for rule in self.identity:
            unknown = [n for n in rule.match_on if n not in allowed]
            if unknown:
                raise ValueError(
                    "IdentityRule.match_on references field(s) not produced by "
                    f"ItemsWritePolicy.derive: {unknown}. "
                    f"Available: {sorted(allowed)}."
                )
        return self

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

    def resolve_external_id(self, feature: Mapping[str, Any]) -> Optional[str]:
        """Resolve the external_id from an inbound feature using the configured
        path (if any), otherwise fall back to the feature's top-level ``id``.

        Resolution order mirrors ``_walk_external_id_path``:
          1. When a path is configured: walk the path through the feature dict.
          2. When no path is configured: return ``feature.get("id")``.

        Returns a ``str`` or ``None``; never raises.
        """
        path = self.external_id_path()
        if path is not None:
            val = _walk_external_id_path(feature, path)
        else:
            val = feature.get("id")
        if val is None:
            return None
        return str(val)


def _walk_external_id_path(
    feature: Mapping[str, Any], path: str
) -> Optional[Any]:
    """Walk ``path`` through ``feature`` using the standard three-step rule.

    1. Top-level lookup: ``feature[path]``.
    2. Dot-walk (e.g. ``a.b.c``): traverse nested dicts.
    3. Properties fallback: when ``path`` has no dot AND top-level is ``None``,
       try ``feature["properties"][path]``.

    Returns the value or ``None``; never raises.
    """
    try:
        if "." in path:
            current: Any = feature
            for part in path.split("."):
                if not isinstance(current, Mapping):
                    return None
                current = current.get(part)
                if current is None:
                    return None
            return current
        val = feature.get(path)
        if val is None:
            props = feature.get("properties")
            if isinstance(props, Mapping):
                val = props.get(path)
        return val
    except Exception:
        return None


async def _warn_unstored_unreferenced_stats(
    config: PluginConfig,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[object],
) -> None:
    """Warn on statistic derivations that are computed but stored nowhere.

    A ``geometry_stats`` / ``attribute_stats`` entry with ``store=None`` is
    computed on every write but never persisted. That is legitimate only when
    an identity rule references it (a compute-only match axis). When no identity
    rule does, the value feeds nothing — it cannot be exposed (``ItemsReadPolicy``
    rejects exposing it), queried, or read back — so a ``store: null`` here is
    almost always an authoring foot-gun (the GLOSIS case declared ``area`` this
    way and then referenced it downstream, where it silently never existed).

    Surfaced as a WARNING rather than a hard error: staging a derivation before
    wiring its consumer is a valid intermediate state, and the read-side
    ``expose`` validator already turns the actually-broken case into a 4xx.
    """
    if not isinstance(config, ItemsWritePolicy):
        return
    identity_names = {n for rule in config.identity for n in rule.match_on}
    suspect = sorted(
        cf.resolved_name
        for cf in config.compute
        if cf.kind in _STATISTIC_STORAGE_KINDS
        and cf.storage_mode is None
        and cf.resolved_name not in identity_names
    )
    if suspect:
        _logger.warning(
            "ItemsWritePolicy for %s/%s declares statistic(s) %s with store=None "
            "that no identity rule references: they are computed on every write "
            "but never persisted and feed nothing, so they cannot be exposed or "
            "queried. Set a store (jsonb/columnar) to materialise them, reference "
            "them from an identity rule, or remove them.",
            catalog_id,
            collection_id,
            suspect,
        )


ItemsWritePolicy.register_validate_handler(_warn_unstored_unreferenced_stats)


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

    Phase 3 (sidecars = pure-derived physical realization layer):

    ``sidecars`` is **Computed** — NON-AUTHORABLE.  It is a read-only
    physical projection derived entirely by ``_effective_sidecars(...)``
    from ``(ItemsWritePolicy, ItemsSchema, collection_type, driver
    capabilities)`` at ``ensure_storage()`` time.  An external caller can
    never author it: the config-write path strips it via
    ``restore_system_assigned_fields`` (it is a ``Computed`` field), and
    the internal provisioner stamps the resolved list with
    ``check_immutability=False``.  Operators SEE the resolved plan via the
    composed-view ``physical`` projection (read-only); they SHAPE it
    indirectly through the policy.  ``partitioning`` stays **Immutable** —
    a genuinely-physical operator choice (Decision 5).
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "items", "drivers")
    _freeze_at: ClassVar[Optional[str]] = "collection"

    required_engine_class: ClassVar[str] = "postgresql_engine"


    model_config = ConfigDict(extra="allow")

    capabilities: ClassVar[FrozenSet[str]] = frozenset(
        {DriverCapability.SYNC, DriverCapability.TRANSACTIONAL}
    )

    # ``physical_table`` is machine-assigned by ``ensure_storage()`` at
    # provisioning and flows into SQL identifiers — it must never be supplied
    # or altered by an API caller (would enable identifier injection and
    # cross-collection table targeting; see #1135).  ``Computed`` marks it
    # read-only on the wire AND opts it into the config-write strip
    # (``restore_system_assigned_fields``), so any caller value on the external
    # path is discarded.  The internal provisioner sets it via
    # ``model_copy`` + ``check_immutability=False`` (which bypasses the strip).
    #
    # The catalog's physical schema is resolved from ``CatalogsProtocol``
    # (system-generated, per-tenant); there is intentionally NO per-collection
    # schema override field — a caller-settable schema would break tenant
    # isolation (#1135 Issue 2 — the former ``physical_schema`` override was
    # vestigial/read-nowhere and has been removed).
    physical_table: Computed[Optional[str]] = Field(
        default=None,
        description="Machine-assigned physical table name (set by ensure_storage()). Read-only.",
    )

    @field_validator("physical_table")
    @classmethod
    def _validate_physical_table_identifier(cls, v: Optional[str]) -> Optional[str]:
        """Defense-in-depth: reject any non-identifier ``physical_table`` at
        validation time so a malformed value can never reach SQL even if a path
        bypasses the resolver-level guard.  ``None`` (the default / unset) is
        always allowed; the provisioner assigns a safe generated name."""
        if v is None:
            return v
        from dynastore.tools.db import validate_sql_identifier

        return validate_sql_identifier(v)

    # From CollectionPluginConfig — PG-specific structural fields.
    # Discriminated union via `sidecar_type`; default is EMPTY (M1b.2).
    # The old eager `[geometries, attributes]` default is gone — the PG
    # driver resolves sidecar defaults lazily at DDL / read / write time
    # via ``storage.drivers.pg_sidecars.resolver._effective_sidecars(...)``.
    # A default-body ``POST /collections/{id}`` therefore persists zero
    # ``collection_configs`` rows (plan §Principle — default-fast).
    sidecars: Computed[List[_PgSidecarConfig]] = Field(
        default_factory=list,
        description=(
            "DERIVED, read-only physical realization (Phase 3). Sidecar table "
            "configs — discriminated union on ``sidecar_type``. "
            "NON-AUTHORABLE: ``_effective_sidecars(...)`` is the single source "
            "of truth, deriving the plan from the items policy/schema, the "
            "collection_type and the driver's capabilities. Lifecycle: empty "
            "until the collection is materialised (``ensure_storage()``).  At "
            "materialisation the PG driver runs ``_effective_sidecars(...)`` — "
            "core defaults for the collection_type plus extension-registered "
            "sidecars (e.g. ``ItemMetadataSidecarConfig`` from STAC) — overlays "
            "the policy-derived storage shape, and persists the resolved list "
            "onto this field, alongside ``physical_table``.  ``Computed`` marks "
            "it read-only on the wire AND opts it into the config-write strip "
            "(``restore_system_assigned_fields``): any caller value on the "
            "external path is discarded (the provisioner stamps it with "
            "``check_immutability=False``).  Operators see the resolved plan "
            "via the composed-view read-only ``physical`` projection; they "
            "shape it indirectly through ``items_write_policy``.  "
            "Discriminator values: "
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
            all_fields.update(sidecar.get_queryable_fields())
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
    _freeze_at: ClassVar[Optional[str]] = "collection"

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
    # Issue #1248: geometry simplification is on by default. The driver
    # simplifies oversized geometries to fit the ES byte budget; the
    # PostgreSQL primary always keeps full resolution.
    simplify_geometry: Mutable[bool] = Field(
        default=True,
        description=(
            "When True (default), oversized geometries are simplified to fit "
            "the Elasticsearch per-document byte budget before indexing "
            "(lossy; the PostgreSQL primary always keeps full resolution). "
            "Set False to index exact geometry and reject items whose geometry "
            "exceeds the budget with HTTP 422 before any write."
        ),
    )
    simplify_target_bytes: Mutable[Optional[int]] = Field(
        default=1_048_576,
        ge=1,
        examples=[1_048_576, 5_000_000],
        description=(
            "Target byte budget for ES geometry simplification. Geometry is "
            "simplified to fit under this size before indexing — lower values "
            "keep ES indices smaller and writes faster. The hard 10 MB ES "
            "per-document ceiling is always enforced as an upper bound. "
            "Defaults to 1 MB (1 048 576 bytes). Does not affect the "
            "PostgreSQL primary."
        ),
    )
    snap_to_grid: Mutable[bool] = Field(
        default=False,
        description=(
            "When True, an O(n) coordinate snap-to-grid step runs before the "
            "iterative Douglas-Peucker simplification loop. Snapping is fast "
            "and reduces vertex count cheaply for heavy layers; if snap alone "
            "brings the document under the byte budget the D-P loop is skipped "
            "entirely. Recorded as mode 'snap_to_grid' (or 'snap_to_grid+"
            "tolerance'/'snap_to_grid+bbox' when the D-P loop also runs). "
            "Off by default; useful for collections with very dense geometries."
        ),
    )
    snap_grid_size: Mutable[float] = Field(
        default=1e-5,
        gt=0,
        description=(
            "Grid cell size (in geometry CRS units) for the snap-to-grid "
            "pre-pass. For geographic coordinates (degrees) the default "
            "1e-5 ≈ 1.1 m at the equator — sub-visual at all tile zoom "
            "levels. Only active when snap_to_grid=True."
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
    ``modules.elasticsearch.client.get_index_prefix``.

    Per-tenant operator knob — :attr:`mapping` is a tenant-scoped manual
    mapping overlay merged into the per-tenant private index at
    index-create time. Unlike the public driver's Tier-2 overlay, there
    is no platform-wide Tier-1 set for the private driver: the index is
    not aliased cross-tenant and the operator declares exactly which
    extension keys deserve a typed mapping. Unknown keys keep landing in
    the cap-safe ``properties.extras`` lane + ``_search_text`` root
    field (same defence-in-depth shape as the public per-catalog index,
    #1295). Snapshot at index-create; live edits take effect on next
    index rebuild (ES does not allow tightening a live mapping).

    The freeze tier is ``"catalog"``: the manual mapping governs the
    per-tenant index, not any one collection.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "items", "drivers")
    # Tenant-scoped: the per-tenant index is shared across all collections
    # of a catalog, so the mapping snapshot is meaningful at the catalog
    # tier (not collection). Operators set ``mapping`` at the catalog
    # tier; the immutability gate freezes once the catalog has any
    # collection provisioned.
    _freeze_at: ClassVar[Optional[str]] = "catalog"

    required_engine_class: ClassVar[str] = "elasticsearch_engine"

    model_config = ConfigDict(extra="allow")

    capabilities: ClassVar[FrozenSet[str]] = frozenset({DriverCapability.ASYNC})
    mapping: Mutable[Dict[str, Dict[str, Any]]] = Field(
        default_factory=dict,
        description=(
            "Tenant-scoped manual mapping overlay: ``{stac_field: {ES "
            "field-type definition}}``. Merged into the per-tenant "
            "private index at index-create time, under "
            "``properties.<key>``. When non-empty, the index uses the "
            "strict cap-safe shape: declared keys land at their typed "
            "path, undeclared keys route to ``properties.extras`` "
            "(``flattened``) + the ``_search_text`` root analyzed "
            "catch-all. When empty (default), the legacy fully-dynamic "
            "``properties`` sub-tree is kept for backward compatibility. "
            "Snapshot at index-create; live edits take effect on next "
            "index rebuild (ES does not allow tightening a live mapping)."
        ),
    )
    # Issue #1248: geometry simplification is on by default — see
    # ``ItemsElasticsearchDriverConfig.simplify_geometry`` for the full
    # rationale. The private driver shares the same default-on semantics.
    simplify_geometry: Mutable[bool] = Field(
        default=True,
        description=(
            "When True (default), oversized geometries are simplified to fit "
            "the Elasticsearch per-document byte budget before indexing "
            "(lossy; the PostgreSQL primary always keeps full resolution). "
            "Set False to index exact geometry and reject items whose geometry "
            "exceeds the budget with HTTP 422 before any write."
        ),
    )
    simplify_target_bytes: Mutable[Optional[int]] = Field(
        default=1_048_576,
        ge=1,
        examples=[1_048_576, 5_000_000],
        description=(
            "Target byte budget for ES geometry simplification. Geometry is "
            "simplified to fit under this size before indexing. The hard 10 MB "
            "ES per-document ceiling is always enforced as an upper bound. "
            "Defaults to 1 MB (1 048 576 bytes). Does not affect the "
            "PostgreSQL primary."
        ),
    )
    snap_to_grid: Mutable[bool] = Field(
        default=False,
        description=(
            "When True, an O(n) coordinate snap-to-grid step runs before the "
            "iterative Douglas-Peucker simplification loop. Off by default; "
            "useful for collections with very dense geometries."
        ),
    )
    snap_grid_size: Mutable[float] = Field(
        default=1e-5,
        gt=0,
        description=(
            "Grid cell size for the snap-to-grid pre-pass (geometry CRS "
            "units). Default 1e-5 degrees ≈ 1.1 m at the equator. "
            "Only active when snap_to_grid=True."
        ),
    )


class ItemsElasticsearchEnvelopeDriverConfig(CollectionDriverConfig):
    """Standardized-envelope Elasticsearch driver config.

    Pairs with
    :class:`~dynastore.modules.storage.drivers.elasticsearch_envelope.driver.ItemsElasticsearchEnvelopeDriver`,
    which writes the full feature (geometry + properties + identity) plus a
    canonical *access envelope* (``visibility`` / ``owner``) into a
    per-tenant index named
    ``{index_prefix}-{catalog_id}-envelope-items`` (catalog-scoped, shared
    across all collections of a catalog). The access envelope backs the
    row-level ABAC filter the driver ANDs into every search.

    Unlike the private driver, this config does NOT manage DENY policies —
    document-level scoping is performed by translating the neutral
    ``AccessFilter`` into an ES clause at query time.

    :attr:`mapping` is the Tier-2 overlay (mirrors the public config) so
    operators can type extra tenant attributes. ``simplify_geometry`` shares
    the same default-on semantics as the other ES item drivers (#1248).
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "items", "drivers")
    _freeze_at: ClassVar[Optional[str]] = "collection"

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
    # Issue #1248: geometry simplification is on by default — see
    # ``ItemsElasticsearchDriverConfig.simplify_geometry`` for the full
    # rationale. The envelope driver shares the same default-on semantics.
    simplify_geometry: Mutable[bool] = Field(
        default=True,
        description=(
            "When True (default), oversized geometries are simplified to fit "
            "the Elasticsearch per-document byte budget before indexing "
            "(lossy; the PostgreSQL primary always keeps full resolution). "
            "Set False to index exact geometry and reject items whose geometry "
            "exceeds the budget with HTTP 422 before any write."
        ),
    )
    simplify_target_bytes: Mutable[Optional[int]] = Field(
        default=1_048_576,
        ge=1,
        examples=[1_048_576, 5_000_000],
        description=(
            "Target byte budget for ES geometry simplification. Geometry is "
            "simplified to fit under this size before indexing. The hard 10 MB "
            "ES per-document ceiling is always enforced as an upper bound. "
            "Defaults to 1 MB (1 048 576 bytes). Does not affect the "
            "PostgreSQL primary."
        ),
    )
    snap_to_grid: Mutable[bool] = Field(
        default=False,
        description=(
            "When True, an O(n) coordinate snap-to-grid step runs before the "
            "iterative Douglas-Peucker simplification loop. Off by default; "
            "useful for collections with very dense geometries."
        ),
    )
    snap_grid_size: Mutable[float] = Field(
        default=1e-5,
        gt=0,
        description=(
            "Grid cell size for the snap-to-grid pre-pass (geometry CRS "
            "units). Default 1e-5 degrees ≈ 1.1 m at the equator. "
            "Only active when snap_to_grid=True."
        ),
    )


class ItemsDuckdbDriverConfig(CollectionDriverConfig):
    """DuckDB collection driver config.

    Absorbs fields previously in ``FileStorageLocationConfig``.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "items", "drivers")
    _freeze_at: ClassVar[Optional[str]] = "collection"

    required_engine_class: ClassVar[str] = "duckdb_engine"


    capabilities: ClassVar[FrozenSet[str]] = frozenset(
        {DriverCapability.ASYNC, DriverCapability.BATCH}
    )
    path: Mutable[Optional[str]] = Field(default=None, description="Read path (file or glob)")
    format: Mutable[str] = Field(default="parquet", description="File format: parquet, csv, json, geojson, gpkg, shp, etc.")
    write_path: Mutable[Optional[str]] = Field(default=None, description="Separate write path (e.g., SQLite file)"
    )
    write_format: Mutable[Optional[str]] = Field(default=None, description="Write format if different from read"
    )
    # File-backed collections (#374/#377): bind the driver to a catalog asset
    # instead of a hand-written path. When set, the driver resolves the asset's
    # storage URI via AssetsProtocol at dispatch and reads from it. Preferred
    # over ``path`` when both are present.
    asset_id: Mutable[Optional[str]] = Field(
        default=None,
        description="Catalog asset id to read from. Resolved to the asset's "
                    "storage URI at dispatch; preferred over `path` when set.",
    )
    # Declared id column whose value becomes the deterministic geoid source (fid).
    # When unset, the driver falls back to a content hash of the feature.
    id_column: Mutable[Optional[str]] = Field(
        default=None,
        description="Source column holding the native feature id (fid) used to "
                    "derive a stable geoid. Falls back to a content hash if unset.",
    )
    # GeoParquet geometry column. Only used when format is ``geoparquet`` or ``gpq``.
    # The column contains raw WKB bytes that the driver decodes to a DuckDB GEOMETRY
    # via ST_GeomFromWKB before any spatial operation. Default follows the GeoParquet
    # convention ("geometry"). Set this only when the file uses a non-standard column
    # name such as "geom" or "wkb_geometry".
    geometry_column: Mutable[Optional[str]] = Field(
        default=None,
        description=(
            "Name of the WKB geometry column in GeoParquet files. Defaults to "
            "\"geometry\" (the GeoParquet 1.x convention). Set only when the "
            "file uses a non-standard name such as \"geom\". Ignored for all "
            "other formats."
        ),
    )

    @field_validator("geometry_column")
    @classmethod
    def _validate_geometry_column_identifier(cls, v: Optional[str]) -> Optional[str]:
        """Reject geometry_column values that are not plain SQL identifiers.

        The column name is interpolated into a DuckDB quoted-identifier expression
        (``"<geometry_column>"``), so any value that does not satisfy the plain
        identifier grammar is an injection vector.  ``None`` (the default, which
        causes the driver to fall back to ``"geometry"``) is always accepted.
        """
        if v is None:
            return v
        from dynastore.tools.db import validate_column_identifier, InvalidIdentifierError
        try:
            return validate_column_identifier(v)
        except InvalidIdentifierError as exc:
            raise ValueError(
                f"geometry_column {v!r} is not a valid SQL identifier: {exc}. "
                "The value is interpolated into a DuckDB query; use only letters, "
                "digits, and underscores, starting with a letter or underscore."
            ) from exc

    @field_validator("path", "write_path")
    @classmethod
    def _reject_sql_breakout_chars(cls, v: Optional[str]) -> Optional[str]:
        """Reject paths containing characters that could break out of the
        single-quoted string literal the DuckDB driver interpolates them into.

        ``path``/``write_path`` are operator-set (admin configs / preset apply),
        but they reach the SQL string via ``read_parquet('{path}')`` /
        ``ST_Read('{path}')`` etc., so a single quote, newline, or null byte
        would be an injection vector. Legitimate local paths, cloud URIs
        (``s3://``/``gs://``/``https://``), and glob patterns never contain them.
        """
        if v is not None and any(ch in v for ch in ("'", "\n", "\r", "\x00")):
            raise ValueError(
                "path must not contain single-quotes, newlines, or null bytes "
                "(they are interpolated into a DuckDB query)."
            )
        return v


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
    _freeze_at: ClassVar[Optional[str]] = "collection"

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
    _freeze_at: ClassVar[Optional[str]] = "collection"

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
    _freeze_at: ClassVar[Optional[str]] = "collection"

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
    FieldAccess as _FieldAccess,
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
            fields={"name": FieldDefinition(data_type="string")},
            constraints=[
                RequiredConstraint(field="name"),
                UniqueConstraint(field="name"),
            ],
        )
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "items", "schema")
    _freeze_at: ClassVar[Optional[str]] = "collection"


    version: Mutable[int] = Field(
        default=1,
        ge=1,
        description=(
            "Explicit schema-shape version. An authorable, in-payload "
            "discriminator that travels with the frozen schema (``_freeze_at="
            "'collection'``), complementing the implicit class-name ``class_key`` "
            "and the structural ``schema_id`` hash. Informational today: it is "
            "available for future migration logic but drives no auto-migration. "
            "A version bump is itself a schema-shape change, so it is gated by the "
            "same collection freeze as ``fields`` — bump it BEFORE the first "
            "collection materialises, never after."
        ),
    )
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
    default_access: Mutable[_FieldAccess] = Field(
        default=_FieldAccess.AUTO,
        description=(
            "Schema-wide default access intent for fields that leave their own "
            "``access`` at AUTO. AUTO (the default) lets each driver decide from "
            "the field's declared capabilities — the portable equivalent of the "
            "historical 'lift only constrained/queryable fields' behaviour. FAST "
            "asks the driver to optimise every field for query access (PostgreSQL "
            "lifts each into a native sidecar column with an index; other drivers "
            "use their own mechanism). COMPACT asks the driver to minimise storage. "
            "Replaces the former PG-specific ``materialize_fields_as_columns`` flag."
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
    """Cross-validate the EXTERNAL_ID source path against ItemsSchema.fields.

    The ``external_id`` source path's leaf must reference a field declared in
    the resolved ``ItemsSchema`` at the same scope (or the system fields
    ``geoid`` / ``id``). Enforced consistently at **every** scope where a
    schema is resolvable — catalog and collection alike — so a policy can
    never name an undeclared field at one tier while being rejected at
    another. The check is skipped only when no schema is configured yet at
    the scope (nothing to validate against) or when ``external_id`` is unset.

    Resolution is via the config waterfall, so a collection inherits the
    catalog (and platform) schema. The leaf is taken from the dotted path
    (``properties.code`` → ``code``).
    """
    if not isinstance(config, ItemsWritePolicy):
        return
    ext_id = config.external_id_path()
    if not ext_id or ext_id in _ALWAYS_VALID_EXTERNAL_ID_FIELDS:
        return

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
            return  # no schema defined yet at this scope — nothing to check

        if field_key not in defined_fields:
            scope = (
                f"{catalog_id}/{collection_id}"
                if collection_id
                else (catalog_id or "platform")
            )
            raise ValueError(
                f"ItemsWritePolicy external_id path '{ext_id}' "
                f"(field key: '{field_key}') is not declared in ItemsSchema.fields "
                f"for {scope}. Declared fields: {sorted(defined_fields)}. "
                f"Declare '{field_key}' in items_schema, or set external_id to "
                f"'geoid' / 'id' to use a system identity field."
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


async def _validate_items_schema_reserved_names(
    config: PluginConfig,
    catalog_id: "Optional[str]",
    collection_id: "Optional[str]",
    db_resource: "Optional[Any]",
) -> None:
    """Reject ``ItemsSchema.fields`` keys that collide with reserved root names.

    Reserved names live at the document root of the tenant feature mapping
    (``geoid``, ``geometry``, ``bbox``, ``asset_id`` …) — declaring a property
    by one of these names would either silently shadow the system field or
    fail downstream in opaque ways (DDL collision, JSONB extract returning
    the system value, sidecar config rebuild loops). Fail loud at config-save
    so the operator picks a different name before any write hits the driver.
    """
    if not isinstance(config, ItemsSchema):
        return
    if not config.fields:
        return
    # A field collides whether the reserved name is used as the declared key
    # (``name``) OR as the read-time rename (``alias``): an alias of e.g.
    # ``geometry`` would shadow the system field on the wire exactly as a
    # like-named key would in storage (#1489 item 3). Both are reported in one
    # error so the operator fixes every offender in a single pass.
    collisions = sorted(n for n in config.fields if n in _PRIVATE_RESERVED_ROOT_FIELDS)
    alias_collisions = sorted(
        f"{n} (alias={fd.alias!r})"
        for n, fd in config.fields.items()
        if fd.alias and fd.alias in _PRIVATE_RESERVED_ROOT_FIELDS
    )
    if collisions or alias_collisions:
        offenders = collisions + alias_collisions
        raise ValueError(
            f"ItemsSchema.fields declares reserved root name(s) {offenders} "
            f"— these collide with the tenant feature mapping at the document root. "
            f"Reserved names: {sorted(_PRIVATE_RESERVED_ROOT_FIELDS)}. "
            f"Rename the property/alias (e.g. add a domain prefix)."
        )


ItemsSchema.register_validate_handler(_validate_items_schema_reserved_names)


async def _validate_items_schema_storage_realizable(
    config: PluginConfig,
    catalog_id: "Optional[str]",
    collection_id: "Optional[str]",
    db_resource: "Optional[Any]",
) -> None:
    """Reject ``ItemsSchema`` fields that would silently drop at ingest.

    Defense in depth for the silent-drop class fixed in
    ``bridge_schema_to_attribute_sidecar``: when the PG attributes sidecar
    resolves COLUMNAR-only (explicit COLUMNAR, or AUTOMATIC + any column
    entry), the JSONB blob is NOT DDL'd (``attributes.py`` DDL else-branch).
    Every non-geometry items_schema field MUST therefore have an
    ``AttributeSchemaEntry`` after the bridge runs — otherwise ingest swallows
    the field at the SQL boundary (``_upsert_sidecar_table_raw`` binds only
    known columns).

    The bridge auto-promotes every such field, so this handler should never
    fire under correct code. It is the backstop: if a future regression
    weakens the bridge, the operator hears about it at config-save instead
    of discovering empty MVT tiles weeks later (see #1488 / #1491).

    Coverage holds from the FIRST save (#1489 follow-up): when no PG driver
    config exists yet — the typical first-save case, before the collection
    materialises — the bridge is simulated against a default AUTOMATIC sidecar
    rather than skipped. The realizability invariant is therefore enforced
    before any DDL is ever emitted, not only once a driver config is present.
    Simulation is config-only (pure :func:`bridge_schema_to_attribute_sidecar`,
    no ``ALTER TABLE``), so it honours ``feedback_never_migrate_db``.
    """
    if not isinstance(config, ItemsSchema):
        return
    if not config.fields or not (catalog_id and collection_id):
        return

    try:
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.modules.storage.drivers.collection_postgresql import (
            CollectionPostgresqlDriverConfig,
        )
        from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
            AttributeStorageMode,
            FeatureAttributeSidecarConfig,
        )
        from dynastore.modules.storage.field_constraints import (
            bridge_schema_to_attribute_sidecar,
        )
        from dynastore.tools.discovery import get_protocol

        # Resolve the sidecar config to simulate the bridge against. Prefer the
        # already-persisted PG driver config (post-provisioning reality). When it
        # is absent — first ItemsSchema save, before the collection materialises,
        # or no ConfigsProtocol registered — fall back to a default AUTOMATIC
        # sidecar so the realizability invariant is enforced from the first save.
        # The default is the most permissive resolution (AUTOMATIC + no columns →
        # the bridge either promotes every field under COLUMNAR or resolves JSONB),
        # so this fallback never rejects a schema the real provisioning bridge
        # would have accepted — it only catches a genuine bridge regression.
        sidecar_cfg: "Optional[FeatureAttributeSidecarConfig]" = None
        configs = get_protocol(ConfigsProtocol)
        if configs is not None:
            try:
                driver_cfg = await configs.get_config(
                    CollectionPostgresqlDriverConfig,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                )
            except Exception:
                driver_cfg = None
            for sc in getattr(driver_cfg, "sidecars", None) or []:
                if isinstance(sc, FeatureAttributeSidecarConfig):
                    sidecar_cfg = sc
                    break
        if sidecar_cfg is None:
            sidecar_cfg = FeatureAttributeSidecarConfig()

        # Simulate the bridge against this items_schema; check the resulting
        # resolved mode the same way ``attributes.py`` does at DDL time.
        bridged = bridge_schema_to_attribute_sidecar(config, sidecar_cfg)
        resolved_mode = sidecar_cfg.storage_mode
        if resolved_mode == AttributeStorageMode.AUTOMATIC:
            resolved_mode = (
                AttributeStorageMode.COLUMNAR
                if bridged.attribute_schema
                else AttributeStorageMode.JSONB
            )
        if resolved_mode == AttributeStorageMode.JSONB:
            return  # JSONB blob will catch every non-geometry field.

        promoted = {e.name for e in (bridged.attribute_schema or [])}
        unreachable = [
            name
            for name, fd in config.fields.items()
            if not (fd.data_type or "").lower().startswith("geometry")
            and name not in promoted
        ]
        if unreachable:
            raise ValueError(
                f"ItemsSchema for {catalog_id}/{collection_id}: field(s) "
                f"{sorted(unreachable)} would be silently dropped at ingest. "
                "The PG attributes sidecar resolves to COLUMNAR-only (no JSONB "
                "blob exists on disk), yet the bridge did not promote these "
                "fields to native columns. This signals a regression in "
                "bridge_schema_to_attribute_sidecar — every non-geometry "
                "items_schema field must become an AttributeSchemaEntry under "
                "COLUMNAR resolution. Recover by switching the sidecar "
                "storage_mode to JSONB (so the blob catches plain fields)."
            )
    except ValueError:
        raise
    except Exception as exc:
        _logger.debug(
            "items_schema storage-realizability validation skipped for %s/%s: %s",
            catalog_id,
            collection_id,
            exc,
        )


ItemsSchema.register_validate_handler(_validate_items_schema_storage_realizable)


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


# ---------------------------------------------------------------------------
# Validate handler — ItemsElasticsearchPrivateDriverConfig.mapping overlay
# ---------------------------------------------------------------------------


# Reserved root keys of TENANT_FEATURE_MAPPING that the operator overlay
# must never try to retype — they live at the doc root, not under
# ``properties.``. Kept as a tuple here so the validator doesn't import
# the driver-private mapping module (which would create a config →
# driver layering inversion).
_PRIVATE_RESERVED_ROOT_FIELDS: Tuple[str, ...] = (
    "geoid",
    "id",
    "catalog_id",
    "collection_id",
    "collection",
    "external_id",
    "asset_id",
    "geometry",
    "bbox",
    "properties",
    "system",
    "stats",
    "metadata",
    "access",
    "extras",
)


async def _validate_items_es_private_driver_config(
    config: PluginConfig,
    catalog_id: "Optional[str]",
    collection_id: "Optional[str]",
    db_resource: "Optional[Any]",
) -> None:
    """Reject malformed ``mapping`` overlays on the private driver config.

    The private driver's manual mapping is tenant-scoped (one index per
    catalog), so unlike the public Tier-2 there is no platform-wide
    Tier-1 to collide with. Validation focuses on shape (every entry
    must be a dict carrying ``"type"``) and on rejecting overlays that
    try to retype reserved root keys of ``TENANT_FEATURE_MAPPING``
    (``geoid``, ``catalog_id``, ``geometry``, …) — those keys live at
    the document root, not under ``properties.``.
    """
    if not isinstance(config, ItemsElasticsearchPrivateDriverConfig):
        return
    overlay = getattr(config, "mapping", None)
    if not isinstance(overlay, dict):
        raise ValueError(
            "ItemsElasticsearchPrivateDriverConfig.mapping must be a dict "
            "of {stac_field: {ES field-type definition}}."
        )
    if not overlay:
        return
    for key, value in overlay.items():
        if not isinstance(value, dict) or "type" not in value:
            raise ValueError(
                "ItemsElasticsearchPrivateDriverConfig.mapping["
                f"{key!r}] must be a dict with at least a 'type' field; "
                f"got {type(value).__name__}."
            )
        if key in _PRIVATE_RESERVED_ROOT_FIELDS:
            raise ValueError(
                "ItemsElasticsearchPrivateDriverConfig.mapping["
                f"{key!r}] collides with a reserved root field of the "
                "tenant feature mapping. The overlay applies under "
                "``properties.<key>`` only — pick a property name."
            )


ItemsElasticsearchPrivateDriverConfig.register_validate_handler(
    _validate_items_es_private_driver_config,
)


# Resolve the forward reference on IdentityRule.on_match now that
# WriteConflictPolicy is defined in this module. computed_fields.py declares
# the field as ``Optional["WriteConflictPolicy"]`` to break the otherwise
# circular import (driver_config → computed_fields → driver_config).
IdentityRule.model_rebuild(_types_namespace={"WriteConflictPolicy": WriteConflictPolicy})
