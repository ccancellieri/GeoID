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
Computed field model for the consolidated ItemsWritePolicy / ItemsReadPolicy.

This module is the phase 1 deliverable of the items-policy consolidation
(see ``docs/architecture/items-policy-consolidation-957-950.md``).
It is intentionally additive: nothing here is wired into
``ItemsWritePolicy`` yet; that arrives in phase 2.

Two layers live here:

**Authoring layer (the wire/config shape).** :class:`DeriveSpec` groups
declared derivations into homogeneous, purpose-named buckets
(``external_id``, ``content_hashes``, ``spatial_cells``, ``geometry_stats``,
``attribute_stats``). Each bucket entry carries only the fields its family
needs — no per-kind ``null`` noise, and the registry JSON-Schema advertises
exactly the legal fields per family. :class:`IdentityRule` references those
declared outputs **by name** (``match_on: List[str]``) instead of
re-declaring them. This is what an operator authors and what
``ItemsWritePolicy`` persists.

**Engine layer (the internal value type).** :class:`ComputedField` is the
flat per-derivation value the drivers/sidecars consume. It is produced from
:class:`DeriveSpec` via :meth:`DeriveSpec.to_computed_fields` (and classified
back via :meth:`DeriveSpec.from_computed_fields`); ``ItemsWritePolicy.compute``
is a derived view, not an authored field. Nothing outside this bridge needs
to know the buckets exist.

Public surface:

- :class:`ComputedKind` — enum of every value a driver may derive from an
  incoming feature (identity hashes, spatial-cell keys, statistics).
- :class:`SpatialCell` / :class:`GeometryStat` / :class:`AttributeStat` —
  the homogeneous bucket entries.
- :class:`DeriveSpec` — the authored derivation buckets + the bridge to
  :class:`ComputedField`.
- :class:`ComputedField` — one engine-facing derivation (internal type).
- :class:`IdentityRule` — an AND-composition over declared derivation
  *names* used to resolve "is this incoming feature the same as one already
  stored".
- :class:`FeatureType` — declarative wire-shape contract used by
  ``ItemsReadPolicy``.
"""

import re
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Iterable, List, Literal, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

if TYPE_CHECKING:
    from dynastore.modules.storage.driver_config import WriteConflictPolicy

_SPATIAL_CELL_KINDS: frozenset = frozenset()  # populated after ComputedKind


class StatisticStorageMode(StrEnum):
    """How a derived statistic is materialised on disk.

    Distinct from per-row compute: a :class:`ComputedField` whose
    ``storage_mode`` is ``None`` is computed but not stored as a
    standalone column / JSONB key (e.g. used solely to feed an identity
    rule's match_on). When set, the PG driver emits DDL according to the
    mode:

    - ``JSONB`` — the field's value lands as a key inside a single
      ``geom_stats`` JSONB column; B-tree indexes on JSONB are functional
      indexes on ``(geom_stats->>'<key>')::numeric``.
    - ``COLUMNAR`` — the field gets its own typed column; B-tree indexes
      are direct on that column.

    Mixing modes per field is allowed (one field JSONB, another columnar
    on the same sidecar) — the driver emits the JSONB column iff any
    storage-bearing field uses ``JSONB``.
    """

    JSONB = "jsonb"
    COLUMNAR = "columnar"


class ComputedKind(StrEnum):
    """Every value a driver may derive from an incoming feature.

    Each kind partitions into one of three families:

    - **path-extracted** (``EXTERNAL_ID``) — read from the feature using
      the :class:`ComputedField` ``name`` slot as a dotted JSON path
      (e.g. ``"properties.adm2_pcode"``). No separate
      ``ItemsWritePolicy.external_id_field`` is needed; the
      :class:`ComputedField` is fully self-describing.
    - **content hash** (``GEOMETRY_HASH``, ``ATTRIBUTES_HASH``) —
      deterministic fingerprint of the geometry/properties.
    - **spatial cell** (``GEOHASH``, ``H3``, ``S2``) — discrete index of
      the geometry's centroid at a given resolution. ``resolution`` on
      :class:`ComputedField` is mandatory for these kinds.
    - **statistic** (``AREA``, ``PERIMETER``, ``LENGTH``, ``CENTROID``,
      ``BBOX``, ``VERTEX_COUNT``, ``HOLE_COUNT``) — scalar/array
      derivations from the geometry.
    """

    # Path-extracted (uses ItemsWritePolicy.external_id_field)
    EXTERNAL_ID = "external_id"
    # Content fingerprints
    GEOMETRY_HASH = "geometry_hash"
    ATTRIBUTES_HASH = "attributes_hash"
    # Spatial cell keys (require resolution)
    GEOHASH = "geohash"
    H3 = "h3"
    S2 = "s2"
    # Statistics — scalar/array derivations from the geometry
    AREA = "area"
    VOLUME = "volume"              # 3D geometries only (Shapely .volume)
    PERIMETER = "perimeter"
    LENGTH = "length"
    CENTROID = "centroid"
    BBOX = "bbox"
    VERTEX_COUNT = "vertex_count"
    HOLE_COUNT = "hole_count"
    # Morphological indices — dimensionless 2D shape descriptors
    CIRCULARITY = "circularity"    # (4·π·area) / perimeter²; perfect circle = 1
    CONVEXITY = "convexity"        # area / convex_hull.area; perfect convex = 1
    ASPECT_RATIO = "aspect_ratio"  # bbox width / bbox height
    # JSON-FG 3D place-statistics (sourced from the 'place' member, not 'geometry')
    SURFACE_AREA = "surface_area"
    SURFACE_TO_VOLUME_RATIO = "surface_to_volume_ratio"
    NET_FLOOR_AREA = "net_floor_area"
    CENTROID_3D = "centroid_3d"    # GEOMETRY(POINTZ, 4326) column; distinct from CENTROID
    Z_RANGE = "z_range"
    VERTICAL_GRADIENT = "vertical_gradient"
    TEMPORAL_DURATION = "temporal_duration"  # non-geometry-derived: reads JSON-FG 'time' member
    # Attribute-derived statistic (sourced from the feature's own ``properties``,
    # not the geometry). v1 promotes a single property value — named by the
    # ``source`` dotted path (e.g. ``"properties.population"``) — into the
    # attributes sidecar as a stored, optionally-indexed column / JSONB key.
    # Cross-field expressions are deferred.
    ATTRIBUTE_STAT = "attribute_stat"


# Kinds that may carry ``storage_mode != None``. Identity-style kinds
# (hashes / spatial-cell keys / EXTERNAL_ID) materialise via their own
# columns already managed by the sidecar; only geometry-derived
# scalars/arrays opt into the JSONB-vs-columnar shape switch.
_STATISTIC_STORAGE_KINDS: frozenset = frozenset({
    ComputedKind.AREA,
    ComputedKind.VOLUME,
    ComputedKind.PERIMETER,
    ComputedKind.LENGTH,
    ComputedKind.CENTROID,
    ComputedKind.BBOX,
    ComputedKind.VERTEX_COUNT,
    ComputedKind.HOLE_COUNT,
    ComputedKind.CIRCULARITY,
    ComputedKind.CONVEXITY,
    ComputedKind.ASPECT_RATIO,
    # JSON-FG 3D place-statistics
    ComputedKind.SURFACE_AREA,
    ComputedKind.SURFACE_TO_VOLUME_RATIO,
    ComputedKind.NET_FLOOR_AREA,
    ComputedKind.CENTROID_3D,
    ComputedKind.Z_RANGE,
    ComputedKind.VERTICAL_GRADIENT,
    ComputedKind.TEMPORAL_DURATION,
    # Attribute-derived (sourced from ``properties``, materialised by the
    # attributes sidecar rather than the geometries sidecar).
    ComputedKind.ATTRIBUTE_STAT,
})

# Kinds that live in the JSON-FG ``_place`` sidecar table rather than the
# main geometries sidecar. Declaring any of these in
# ``ItemsWritePolicy.compute`` causes the PG driver to emit a
# ``{table}_place`` table with a FK to the hub.
_PLACE_TABLE_KINDS: frozenset = frozenset({
    ComputedKind.SURFACE_AREA,
    ComputedKind.SURFACE_TO_VOLUME_RATIO,
    ComputedKind.NET_FLOOR_AREA,
    ComputedKind.CENTROID_3D,
    ComputedKind.Z_RANGE,
    ComputedKind.VERTICAL_GRADIENT,
    ComputedKind.TEMPORAL_DURATION,
})


# Resolution-requiring kinds. Geohash 1..12; H3 0..15; S2 0..30.
SPATIAL_CELL_KINDS: frozenset = frozenset(
    {ComputedKind.GEOHASH, ComputedKind.H3, ComputedKind.S2}
)

# Path-extracted kinds: not produced by compute_derived_fields(); identity
# rules referencing them are resolved against the feature directly.
PATH_EXTRACTED_KINDS: frozenset = frozenset({ComputedKind.EXTERNAL_ID})

_RESOLUTION_RANGES: dict = {
    ComputedKind.GEOHASH: (1, 12),
    ComputedKind.H3: (0, 15),
    ComputedKind.S2: (0, 30),
}

# Kinds materialised by the attributes sidecar (sourced from the feature's
# ``properties``, not the geometry). The PG driver splits the storage-bearing
# ``ItemsWritePolicy.compute`` entries by this set: members route to
# ``FeatureAttributeSidecarConfig.compute_fields_overlay``; everything else
# (geometry + JSON-FG place statistics) stays on the geometries sidecar.
_ATTRIBUTE_SIDECAR_KINDS: frozenset = frozenset({ComputedKind.ATTRIBUTE_STAT})


class SidecarTarget(StrEnum):
    """Which PG sidecar materialises a given computed field at write time.

    The PG driver splits ``ItemsWritePolicy.compute`` (storage-bearing entries)
    by this classification: geometry/place statistics land on the geometries
    sidecar, attribute-derived statistics on the attributes sidecar.
    """

    GEOMETRY = "geometry"
    ATTRIBUTES = "attributes"


def target_sidecar(kind: ComputedKind) -> SidecarTarget:
    """Classify a :class:`ComputedKind` to the sidecar that stores it.

    ``ATTRIBUTE_STAT`` (sourced from the feature's ``properties``) routes to the
    attributes sidecar; every other storage-bearing kind — geometry statistics
    and the JSON-FG 3D place statistics, which the geometries sidecar emits into
    its own ``{table}_place`` table — routes to the geometries sidecar.
    """
    if kind in _ATTRIBUTE_SIDECAR_KINDS:
        return SidecarTarget.ATTRIBUTES
    return SidecarTarget.GEOMETRY


class ComputedField(BaseModel):
    """One declared derivation.

    Spatial-cell kinds require ``resolution``; all other kinds must omit
    it. ``name`` defaults to the kind's string value, with the resolution
    appended for spatial cells (e.g. ``"h3_7"``, ``"s2_10"``,
    ``"geohash_8"``). Override ``name`` to control the column/field name
    a driver uses to materialise the value.

    Storage-shape fields (only meaningful for the statistic kinds in
    :data:`_STATISTIC_STORAGE_KINDS`):

    - :attr:`storage_mode` — ``JSONB`` (key in a shared ``geom_stats``
      column) or ``COLUMNAR`` (its own typed column). ``None`` means the
      field is computed but not stored on the sidecar (e.g. used solely
      by an :class:`IdentityRule`).
    - :attr:`indexed` — emit a B-tree index on the resulting column.
      Forbidden together with ``storage_mode == JSONB`` (use a functional
      JSONB index by setting ``indexed=True`` with ``storage_mode=JSONB``
      is rejected; emit two fields if you want both layouts).
    - :attr:`centroid_type` — only consumed by ``ComputedKind.CENTROID``
      to pick column type (``POINT`` 2D vs ``POINTZ`` 3D) and the WKB
      output dimensionality.
    - :attr:`source` — only consumed by ``ComputedKind.ATTRIBUTE_STAT``: a
      dotted path into the feature (e.g. ``"properties.population"``) whose
      value is promoted into the attributes sidecar. Required for that kind,
      forbidden for all others. When ``name`` is omitted the ``resolved_name``
      defaults to the path's final segment (``"population"``).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: ComputedKind
    resolution: Optional[int] = None
    name: Optional[str] = None
    storage_mode: Optional[StatisticStorageMode] = None
    indexed: bool = False
    centroid_type: Optional[Literal["POINT", "POINTZ"]] = None
    source: Optional[str] = None

    @model_validator(mode="after")
    def _check_resolution(self) -> "ComputedField":
        if self.kind in SPATIAL_CELL_KINDS:
            if self.resolution is None:
                raise ValueError(
                    f"ComputedField(kind={self.kind.value}) requires 'resolution'"
                )
            lo, hi = _RESOLUTION_RANGES[self.kind]
            if not (lo <= self.resolution <= hi):
                raise ValueError(
                    f"ComputedField(kind={self.kind.value}) resolution "
                    f"{self.resolution} out of range [{lo}, {hi}]"
                )
        else:
            if self.resolution is not None:
                raise ValueError(
                    f"ComputedField(kind={self.kind.value}) does not accept 'resolution'"
                )
        return self

    @model_validator(mode="after")
    def _check_storage_shape(self) -> "ComputedField":
        if self.storage_mode is not None and self.kind not in _STATISTIC_STORAGE_KINDS:
            raise ValueError(
                f"ComputedField(kind={self.kind.value}) does not accept "
                "'storage_mode' (only statistic kinds materialise to a "
                "sidecar column)."
            )
        if self.indexed and self.storage_mode is None:
            raise ValueError(
                f"ComputedField(kind={self.kind.value}, indexed=True) "
                "requires storage_mode to be set (cannot index a "
                "non-materialised field)."
            )
        if self.indexed and self.storage_mode == StatisticStorageMode.JSONB:
            raise ValueError(
                f"ComputedField(kind={self.kind.value}) cannot combine "
                "storage_mode=JSONB with indexed=True; switch to "
                "storage_mode=COLUMNAR to get a direct B-tree, or declare "
                "two separate fields."
            )
        if self.centroid_type is not None and self.kind != ComputedKind.CENTROID:
            raise ValueError(
                f"ComputedField(kind={self.kind.value}) does not accept "
                "'centroid_type' (only ComputedKind.CENTROID uses it)."
            )
        return self

    @model_validator(mode="after")
    def _check_source(self) -> "ComputedField":
        if self.kind == ComputedKind.ATTRIBUTE_STAT:
            if not self.source:
                raise ValueError(
                    "ComputedField(kind=attribute_stat) requires 'source' "
                    "(a dotted feature path, e.g. 'properties.population')."
                )
        elif self.source is not None:
            raise ValueError(
                f"ComputedField(kind={self.kind.value}) does not accept "
                "'source' (only ATTRIBUTE_STAT reads a property path)."
            )
        return self

    @property
    def resolved_name(self) -> str:
        """Stable identifier used as the dict key in computed-field output.

        For :attr:`ComputedKind.EXTERNAL_ID`, ``name`` carries the dotted
        JSON path into the feature (e.g. ``"properties.adm2_pcode"``),
        NOT the output dict key — the key is always ``"external_id"`` so
        downstream code has a stable handle regardless of the source path.
        """
        if self.kind == ComputedKind.EXTERNAL_ID:
            return "external_id"
        if self.kind == ComputedKind.ATTRIBUTE_STAT and not self.name:
            # Default to the source path's final segment
            # (``"properties.population"`` -> ``"population"``).
            return (self.source or "").rsplit(".", 1)[-1] or "attribute_stat"
        if self.name:
            return self.name
        if self.kind in SPATIAL_CELL_KINDS:
            return f"{self.kind.value}_{self.resolution}"
        return self.kind.value


# ---------------------------------------------------------------------------
# Authoring layer — homogeneous derivation buckets
# ---------------------------------------------------------------------------
#
# Each bucket maps to a slice of ``ComputedKind`` so the authored shape never
# carries a discriminator or per-kind ``null`` slots. The maps below are the
# single bridge between the bucket vocabulary and the engine enum; the
# exhaustiveness test asserts every storage-bearing/identity kind is reachable
# through exactly one bucket.

SpatialGrid = Literal["geohash", "h3", "s2"]
ContentHash = Literal["geometry", "attributes"]

_GRID_KIND: dict[str, ComputedKind] = {
    "geohash": ComputedKind.GEOHASH,
    "h3": ComputedKind.H3,
    "s2": ComputedKind.S2,
}
_KIND_GRID: dict[ComputedKind, str] = {v: k for k, v in _GRID_KIND.items()}

_CONTENT_HASH_KIND: dict[str, ComputedKind] = {
    "geometry": ComputedKind.GEOMETRY_HASH,
    "attributes": ComputedKind.ATTRIBUTES_HASH,
}
_KIND_CONTENT_HASH: dict[ComputedKind, str] = {v: k for k, v in _CONTENT_HASH_KIND.items()}

# Geometry/place statistics usable in :class:`GeometryStat` — every
# storage-bearing kind except the attribute-sourced one (which has its own
# bucket because it reads ``properties`` rather than the geometry).
_GEOMETRY_STAT_KINDS: frozenset = _STATISTIC_STORAGE_KINDS - {ComputedKind.ATTRIBUTE_STAT}


class SpatialCell(BaseModel):
    """A discrete spatial-cell key derived from the geometry centroid.

    ``grid`` selects the scheme; ``resolution`` is required and range-checked
    per scheme (geohash 1..12, h3 0..15, s2 0..30). ``name`` overrides the
    materialised column name (default ``"{grid}_{resolution}"``).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    grid: SpatialGrid
    resolution: int
    name: Optional[str] = None

    @model_validator(mode="after")
    def _check_resolution(self) -> "SpatialCell":
        lo, hi = _RESOLUTION_RANGES[_GRID_KIND[self.grid]]
        if not (lo <= self.resolution <= hi):
            raise ValueError(
                f"SpatialCell(grid={self.grid}) resolution {self.resolution} "
                f"out of range [{lo}, {hi}]"
            )
        return self

    def to_computed_field(self) -> ComputedField:
        return ComputedField(
            kind=_GRID_KIND[self.grid], resolution=self.resolution, name=self.name
        )


class GeometryStat(BaseModel):
    """A scalar/array statistic derived from the geometry (or JSON-FG place).

    ``stat`` is one of the geometry/place statistic kinds (area, perimeter,
    centroid, bbox, …). ``store`` chooses the physical layout (``jsonb`` key
    vs own ``columnar`` column); ``None`` computes the value without storing
    it (e.g. to feed an identity rule). ``indexed`` emits a B-tree (requires
    ``store``; rejected with ``store=jsonb`` — declare two stats for both
    layouts). ``type`` picks ``POINT`` vs ``POINTZ`` and is only valid for
    ``centroid``.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    stat: ComputedKind
    store: Optional[StatisticStorageMode] = None
    indexed: bool = False
    type: Optional[Literal["POINT", "POINTZ"]] = None
    name: Optional[str] = None

    @field_validator("stat")
    @classmethod
    def _check_stat(cls, v: ComputedKind) -> ComputedKind:
        if v not in _GEOMETRY_STAT_KINDS:
            raise ValueError(
                f"GeometryStat.stat={v.value!r} is not a geometry statistic; "
                f"valid kinds: {sorted(k.value for k in _GEOMETRY_STAT_KINDS)}"
            )
        return v

    @model_validator(mode="after")
    def _check_type(self) -> "GeometryStat":
        if self.type is not None and self.stat != ComputedKind.CENTROID:
            raise ValueError(
                f"GeometryStat(stat={self.stat.value}) does not accept 'type' "
                "(only 'centroid' selects POINT vs POINTZ)."
            )
        return self

    def to_computed_field(self) -> ComputedField:
        # Delegates the store/indexed/centroid_type invariants to ComputedField.
        return ComputedField(
            kind=self.stat,
            storage_mode=self.store,
            indexed=self.indexed,
            centroid_type=self.type,
            name=self.name,
        )


class AttributeStat(BaseModel):
    """A single feature ``properties`` value promoted into the attributes
    sidecar as a stored, optionally-indexed column / JSONB key.

    ``source`` is a dotted path into the feature (e.g.
    ``"properties.population"``). ``store``/``indexed`` mirror
    :class:`GeometryStat`. ``name`` overrides the column name (default: the
    source path's final segment).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    source: str
    store: Optional[StatisticStorageMode] = None
    indexed: bool = False
    name: Optional[str] = None

    def to_computed_field(self) -> ComputedField:
        return ComputedField(
            kind=ComputedKind.ATTRIBUTE_STAT,
            source=self.source,
            storage_mode=self.store,
            indexed=self.indexed,
            name=self.name,
        )


class DeriveSpec(BaseModel):
    """Authored, homogeneous buckets of per-row derivations.

    Replaces the old polymorphic ``compute: List[ComputedField]`` authoring
    surface. Each bucket holds only the fields its family needs:

    - :attr:`external_id` — dotted source path for the identity external id,
      or ``None`` (the external-id axis still exists; its value then comes
      from ``write_context.external_id_override`` or the feature id).
    - :attr:`content_hashes` — which deterministic content fingerprints to
      compute (``"geometry"`` / ``"attributes"``).
    - :attr:`spatial_cells` — geohash/h3/s2 keys at a resolution.
    - :attr:`geometry_stats` — geometry/place statistics (area, centroid, …).
    - :attr:`attribute_stats` — feature ``properties`` values promoted to the
      attributes sidecar.

    :meth:`to_computed_fields` flattens these to the engine's
    :class:`ComputedField` list (deduped by ``resolved_name``, last wins);
    :meth:`from_computed_fields` classifies a flat list back into buckets
    (used to accept compute-preset shorthand).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    external_id: Optional[str] = None
    content_hashes: List[ContentHash] = Field(default_factory=list)
    spatial_cells: List[SpatialCell] = Field(default_factory=list)
    geometry_stats: List[GeometryStat] = Field(default_factory=list)
    attribute_stats: List[AttributeStat] = Field(default_factory=list)

    def to_computed_fields(self) -> List["ComputedField"]:
        """Flatten the buckets to the engine's :class:`ComputedField` list.

        Deduped by :attr:`ComputedField.resolved_name`, keeping the last
        occurrence (parity with the compute-preset registry). The
        ``external_id`` field is emitted only when a source path is set — the
        identity axis without a path is materialised lazily by
        :meth:`ItemsWritePolicy.resolved_identity`.
        """
        out: List[ComputedField] = []
        if self.external_id is not None:
            out.append(
                ComputedField(kind=ComputedKind.EXTERNAL_ID, name=self.external_id)
            )
        for h in self.content_hashes:
            out.append(ComputedField(kind=_CONTENT_HASH_KIND[h]))
        for sc in self.spatial_cells:
            out.append(sc.to_computed_field())
        for gs in self.geometry_stats:
            out.append(gs.to_computed_field())
        for a in self.attribute_stats:
            out.append(a.to_computed_field())
        by_name: dict[str, ComputedField] = {}
        for cf in out:
            by_name[cf.resolved_name] = cf
        return list(by_name.values())

    @classmethod
    def from_computed_fields(cls, fields: Iterable["ComputedField"]) -> "DeriveSpec":
        """Classify a flat :class:`ComputedField` list into buckets.

        Lets the policy accept compute-preset shorthand (a preset name or list
        of names expands to a flat list, which this folds into the canonical
        bucket shape).
        """
        external_id: Optional[str] = None
        content_hashes: List[ContentHash] = []
        spatial_cells: List[SpatialCell] = []
        geometry_stats: List[GeometryStat] = []
        attribute_stats: List[AttributeStat] = []
        for cf in fields:
            if cf.kind == ComputedKind.EXTERNAL_ID:
                external_id = cf.name
            elif cf.kind in _KIND_CONTENT_HASH:
                content_hashes.append(_KIND_CONTENT_HASH[cf.kind])  # type: ignore[arg-type]
            elif cf.kind in SPATIAL_CELL_KINDS:
                spatial_cells.append(
                    SpatialCell(
                        grid=_KIND_GRID[cf.kind],  # type: ignore[arg-type]
                        resolution=cf.resolution,  # type: ignore[arg-type]
                        name=cf.name,
                    )
                )
            elif cf.kind == ComputedKind.ATTRIBUTE_STAT:
                attribute_stats.append(
                    AttributeStat(
                        source=cf.source or "",
                        store=cf.storage_mode,
                        indexed=cf.indexed,
                        name=cf.name,
                    )
                )
            else:
                geometry_stats.append(
                    GeometryStat(
                        stat=cf.kind,
                        store=cf.storage_mode,
                        indexed=cf.indexed,
                        type=cf.centroid_type,
                        name=cf.name,
                    )
                )
        return cls(
            external_id=external_id,
            content_hashes=content_hashes,
            spatial_cells=spatial_cells,
            geometry_stats=geometry_stats,
            attribute_stats=attribute_stats,
        )


# Stable authoring order for the platform-default statistics: geometry
# scalars first, then the JSON-FG place family. Order is part of the
# contract — it fixes the column order the sidecar DDL emits.
_DEFAULT_STAT_ORDER: Tuple[ComputedKind, ...] = (
    ComputedKind.AREA,
    ComputedKind.PERIMETER,
    ComputedKind.LENGTH,
    ComputedKind.CENTROID,
    ComputedKind.BBOX,
    ComputedKind.VERTEX_COUNT,
    ComputedKind.HOLE_COUNT,
    ComputedKind.CIRCULARITY,
    ComputedKind.CONVEXITY,
    ComputedKind.ASPECT_RATIO,
    ComputedKind.VOLUME,
    # JSON-FG 3D place statistics — computed only when a feature carries a
    # ``place`` member; materialised on the ``{table}_place`` sidecar.
    ComputedKind.SURFACE_AREA,
    ComputedKind.SURFACE_TO_VOLUME_RATIO,
    ComputedKind.NET_FLOOR_AREA,
    ComputedKind.CENTROID_3D,
    ComputedKind.Z_RANGE,
    ComputedKind.VERTICAL_GRADIENT,
    ComputedKind.TEMPORAL_DURATION,
)

# The render-rank scalars: B-tree indexed by default so
# ``feature_rank_column`` / density ceilings can filter and order on them
# without per-collection configuration. Everything else stays unindexed to
# keep write amplification bounded.
_DEFAULT_INDEXED_STATS: frozenset = frozenset({
    ComputedKind.AREA,
    ComputedKind.LENGTH,
    ComputedKind.VERTEX_COUNT,
})


def default_derive_spec() -> DeriveSpec:
    """The platform-default :class:`DeriveSpec`: every geometry/place
    statistic, materialised COLUMNAR on its sidecar (#3155).

    Applied by ``ItemsWritePolicy.derive`` when a collection doesn't author
    its own spec. Each stat lands as its own typed column — JSONB keys
    can't back the B-tree lookups the render path needs — and the three
    render-rank scalars (``area``, ``length``, ``vertex_count``) are
    indexed. The JSON-FG place statistics are included so 3D sources get
    their stats without extra configuration; they are computed only when a
    feature carries a ``place`` member.

    Identity semantics are untouched: no content hashes or spatial cells
    are enabled, and ``external_id`` keeps its lazy default. Authoring an
    explicit ``derive`` replaces this default entirely (an empty
    ``DeriveSpec`` opts a collection out of all statistics).
    """
    return DeriveSpec(
        geometry_stats=[
            GeometryStat(
                stat=kind,
                store=StatisticStorageMode.COLUMNAR,
                indexed=kind in _DEFAULT_INDEXED_STATS,
            )
            for kind in _DEFAULT_STAT_ORDER
        ]
    )


class IdentityRule(BaseModel):
    """One AND-composition over declared derivations for identity resolution.

    ``match_on`` lists derivation **names** (``ComputedField.resolved_name``
    values produced by ``DeriveSpec`` — e.g. ``"external_id"``,
    ``"geometry_hash"``, ``"geohash_7"``) whose values together identify a
    feature. The fields are *referenced*, not re-declared: every name must
    resolve to a ``DeriveSpec`` output (``"external_id"`` is always available
    even without an extraction path). All listed names must match the same
    existing row for the rule to fire (AND). ``ItemsWritePolicy.identity`` is
    an ordered list of these rules; the first whose conjunction matches wins
    (OR across rules, first-match-wins).

    ``on_match`` lets a rule override the policy-level
    :class:`WriteConflictPolicy` — useful for "match by external_id →
    UPDATE; match by geometry_hash → REFUSE".
    """

    model_config = ConfigDict(extra="forbid")

    match_on: List[str] = Field(min_length=1)
    on_match: Optional["WriteConflictPolicy"] = None


# Identity + lifecycle field names that make up the ``system`` section of the
# report envelope (see ``tasks/ingestion/main_ingestion._build_report_envelope``)
# and of the ``expose_all`` read response. Single source of truth shared by the
# write-side reporter and the read-side feature assembly so the two never drift.
SYSTEM_FIELD_KEYS: tuple[str, ...] = (
    "geoid",
    "external_id",
    "asset_id",
    "geometry_hash",
    "attributes_hash",
    "validity",
    "transaction_time",
    "deleted_at",
    "write_id",
    # #2687: persisted write-time owner (hub column ``access_owner``) — the
    # drain-time source for an access-aware collection's ``_owner`` envelope
    # value. Never a user-facing field; excluded from properties exactly like
    # ``write_id``.
    "access_owner",
)

# The three identity axes live flat at the document root in the canonical ES envelope.
_IDENTITY_FIELD_NAMES: frozenset = frozenset({"external_id", "asset_id", "geoid"})

# System field names are all SYSTEM_FIELD_KEYS except the identity axes.
_SYSTEM_ONLY_FIELD_NAMES: frozenset = frozenset(SYSTEM_FIELD_KEYS) - _IDENTITY_FIELD_NAMES

# Descriptive metadata field names (multilingual title / description / keywords),
# sourced from the ItemMetadataSidecar. Classified into the ``metadata`` container
# so the strict mapping types them as localized text and the read projector resolves
# them per ``?lang=`` (refs #1828). Both the resolved feature names and the
# ``item_``-prefixed queryable aliases are recognised.
_METADATA_FIELD_NAMES: frozenset = frozenset({
    "title",
    "description",
    "keywords",
    "item_title",
    "item_description",
    "item_keywords",
})

# Base names for geometry-derived statistics (from ComputedKind, minus the
# content-hash and identity kinds which land in ``system``). Used by the
# classifier to recognise names like "area", "centroid", "perimeter" as stats.
_GEOMETRY_STAT_BASE_NAMES: frozenset = frozenset({
    ComputedKind.AREA.value,
    ComputedKind.VOLUME.value,
    ComputedKind.PERIMETER.value,
    ComputedKind.LENGTH.value,
    ComputedKind.CENTROID.value,
    ComputedKind.BBOX.value,
    ComputedKind.VERTEX_COUNT.value,
    ComputedKind.HOLE_COUNT.value,
    ComputedKind.CIRCULARITY.value,
    ComputedKind.CONVEXITY.value,
    ComputedKind.ASPECT_RATIO.value,
    ComputedKind.SURFACE_AREA.value,
    ComputedKind.SURFACE_TO_VOLUME_RATIO.value,
    ComputedKind.NET_FLOOR_AREA.value,
    ComputedKind.CENTROID_3D.value,
    ComputedKind.Z_RANGE.value,
    ComputedKind.VERTICAL_GRADIENT.value,
    ComputedKind.TEMPORAL_DURATION.value,
})

# Pattern for spatial-cell resolved names: ``{grid}_{resolution}`` with an
# optional ``res`` resolution token. Matches both the default resolved-name
# shape (``s2_7``, ``h3_10``, ``geohash_6``) and custom names that spell out
# the resolution (``s2_res12``, ``h3_res_10``) — e.g. the geoid preset's
# ``s2_res12``, which would otherwise fall through to ``properties`` and get
# swept into ``properties.extras``.
_SPATIAL_CELL_PATTERN: re.Pattern = re.compile(
    r"^(s2|h3|geohash)_(?:res_?)?\d+$"
)


def classify_container(name: str, field_def: "Any") -> str:
    """Resolve an ES envelope container for a queryable field.

    This is the **single source of truth** consumed by both the ES mapping
    builder (``build_item_mapping``) and the field-path resolvers
    (``resolve_es_field_path``, ``build_es_field_mapping``, ``parse_sort``).
    Having one function guarantees that mapping and resolution never drift —
    a field's ES path and its mapping type are always consistent (refs #1800).

    Classification rules (evaluated in priority order):

    1. **identity** — name in ``_IDENTITY_FIELD_NAMES`` (``external_id``,
       ``asset_id``, ``geoid``). Wins regardless of any explicit tag on the
       ``FieldDefinition``.
    2. **system** — name in ``SYSTEM_FIELD_KEYS`` minus the identity set
       (``geometry_hash``, ``attributes_hash``, ``validity``,
       ``transaction_time``, ``deleted_at``). Wins regardless of any explicit
       tag.
    3. Explicit ``container`` tag on the ``FieldDefinition`` (anything other
       than the default ``"properties"``) — honoured verbatim when neither
       rule 1 nor 2 fires.
    4. **stats** — name matches a known geometry-derived statistic base name
       (``area``, ``centroid``, …) or a spatial-cell resolved-name pattern
       (``s2_*``, ``h3_*``, ``geohash_*``).
    5. **properties** — default for all user / STAC attributes.

    Args:
        name: The queryable field name (e.g. ``"area"``, ``"eo:cloud_cover"``).
        field_def: A :class:`~dynastore.models.protocols.field_definition.FieldDefinition`
            instance (or any object with an optional ``container`` attribute).

    Returns:
        One of ``"identity"``, ``"system"``, ``"metadata"``, ``"stats"``,
        ``"properties"`` (or any explicit container tag honoured by rule 3).
    """
    # Rules 1 & 2 are structural invariants — explicit tags cannot override them.
    if name in _IDENTITY_FIELD_NAMES:
        return "identity"
    if name in _SYSTEM_ONLY_FIELD_NAMES:
        return "system"

    # Rule 3: honour an explicit container tag when the caller (a sidecar or a
    # driver) has already classified the field.
    explicit = getattr(field_def, "container", "properties")
    if explicit != "properties":
        return explicit

    # Rule 4: descriptive multilingual metadata (title/description/keywords).
    if name in _METADATA_FIELD_NAMES:
        return "metadata"

    # Rule 5: derive "stats" from the name pattern.
    if name in _GEOMETRY_STAT_BASE_NAMES:
        return "stats"
    if _SPATIAL_CELL_PATTERN.match(name):
        return "stats"

    # Rule 6: default.
    return "properties"


class FeatureType(BaseModel):
    """Declarative wire-shape contract for the data-oriented read path.

    Used by ``ItemsReadPolicy``. Applies to data-oriented protocols (OGC
    API Features, MVT tiles, EDR, Coverages) that produce GeoJSON-style
    features. **STAC items bypass this policy** — STAC items are metadata
    documents (not properties-projected features), so the full input shape
    passes straight through to the output.

    ``expose`` is the property allowlist with trinary semantics:

      * ``None`` (default) — surface **all** property fields declared by
        ``ItemsSchema.fields``. The read wire-shape mirrors the write
        schema; no computed/derived fields are added.
      * ``[]`` (explicit empty) — surface **no** properties. On MVT this
        yields a geometry-only tile; on /items the response feature has an
        empty ``properties`` object.
      * ``["x", "y", …]`` — surface declared schema fields **plus** the
        listed ``ComputedField.resolved_name`` values from
        ``ItemsWritePolicy.compute``. Listing is **additive** to the
        schema baseline; use ``[]`` to suppress schema fields entirely.

    ``failure_mode`` governs read-failure behaviour: ``best_effort``
    (default) degrades to a bare feature with the missing enriched fields
    silently absent; ``strict`` raises.

    ``external_id_as_feature_id`` controls whether a row's ``external_id``
    (stored by the attributes sidecar) overrides the default ``feature.id``
    (``geoid``); purely a wire-shape decision and **off by default** — the
    stable internal ``geoid`` is the id unless a collection explicitly
    opts in.

    ``expose_created`` surfaces the storage ``transaction_time`` as a
    ``created`` property; **off by default** so the read response mirrors
    the input feature 1:1 (only the id is replaced by the geoid).

    ``expose_geoid`` optionally surfaces the internal geoid as a ``geoid``
    property — useful only when ``external_id`` is the id (otherwise the id
    already is the geoid); a no-op in the default shape.
    """

    model_config = ConfigDict(extra="forbid")

    expose: Optional[List[str]] = Field(
        default=None,
        description=(
            "Property allowlist (data-oriented protocols only; ignored by "
            "STAC). ``None`` (default) surfaces all ``ItemsSchema.fields`` "
            "declared property fields. ``[]`` surfaces no properties (MVT "
            "geometry-only / empty ``properties``). A non-empty list "
            "surfaces declared schema fields PLUS the listed "
            "``ComputedField.resolved_name`` values from "
            "``ItemsWritePolicy.compute`` — listing is additive to the "
            "schema baseline."
        ),
    )
    failure_mode: Literal["strict", "best_effort"] = "best_effort"
    external_id_as_feature_id: bool = False
    expose_created: bool = False
    expose_geoid: bool = False
    expose_all: bool = Field(
        default=False,
        description=(
            "When True, the read response additionally carries the two "
            "platform sections of the ingestion report envelope as top-level "
            "Feature members beside ``properties``: ``stats`` (computed "
            "statistics — geometry/attribute stats, content hashes, spatial "
            "cells) and ``system`` (identity + lifecycle: geoid, external_id, "
            "asset_id, geometry_hash, attributes_hash, validity, "
            "transaction_time, deleted_at). ``properties`` still holds only "
            "user attributes and is still governed by ``expose`` — so "
            "``expose=[]`` + ``expose_all=true`` yields a report-style feature "
            "with an empty ``properties`` plus the ``stats``/``system`` "
            "sections. The sections are attached as GeoJSON foreign members "
            "(RFC 7946 §6.1), so the response stays a valid GeoJSON Feature. "
            "**Off by default** — the default read shape is unchanged."
        ),
    )


__all__ = [
    "ComputedKind",
    "ComputedField",
    "SpatialCell",
    "GeometryStat",
    "AttributeStat",
    "DeriveSpec",
    "SpatialGrid",
    "ContentHash",
    "IdentityRule",
    "FeatureType",
    "SYSTEM_FIELD_KEYS",
    "StatisticStorageMode",
    "SidecarTarget",
    "target_sidecar",
    "SPATIAL_CELL_KINDS",
    "PATH_EXTRACTED_KINDS",
    "_GEOMETRY_STAT_KINDS",
    "_PLACE_TABLE_KINDS",
    "_ATTRIBUTE_SIDECAR_KINDS",
    # Container classification SSOT (refs #1800)
    "classify_container",
    "_IDENTITY_FIELD_NAMES",
    "_SYSTEM_ONLY_FIELD_NAMES",
]
