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
Computed field model for the consolidated ItemsWritePolicy / ItemsReadPolicy.

This module is the phase 1 deliverable of the items-policy consolidation
(see ``docs/architecture/items-policy-consolidation-957-950.md``).
It is intentionally additive: nothing here is wired into
``ItemsWritePolicy`` yet; that arrives in phase 2.

Public surface:

- :class:`ComputedKind` — enum of every value a driver may derive from an
  incoming feature (identity hashes, spatial-cell keys, statistics).
- :class:`ComputedField` — one declared derivation. Spatial-cell kinds
  require a ``resolution``; all others must omit it.
- :class:`IdentityRule` — an AND-composition over computed fields used to
  resolve "is this incoming feature the same as one already stored".
- :class:`FeatureType` — declarative wire-shape contract used by the
  forthcoming ``ItemsReadPolicy``.
"""

from enum import StrEnum
from typing import TYPE_CHECKING, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

if TYPE_CHECKING:
    from dynastore.modules.storage.driver_config import WriteConflictPolicy

_SPATIAL_CELL_KINDS: frozenset = frozenset()  # populated after ComputedKind


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


class ComputedField(BaseModel):
    """One declared derivation.

    Spatial-cell kinds require ``resolution``; all other kinds must omit
    it. ``name`` defaults to the kind's string value, with the resolution
    appended for spatial cells (e.g. ``"h3_7"``, ``"s2_10"``,
    ``"geohash_8"``). Override ``name`` to control the column/field name
    a driver uses to materialise the value.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: ComputedKind
    resolution: Optional[int] = None
    name: Optional[str] = None

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
        if self.name:
            return self.name
        if self.kind in SPATIAL_CELL_KINDS:
            return f"{self.kind.value}_{self.resolution}"
        return self.kind.value


class IdentityRule(BaseModel):
    """One AND-composition over computed fields for identity resolution.

    ``match_on`` lists the fields whose values together identify a
    feature. All listed fields must match an existing row for the rule to
    fire (AND). ``ItemsWritePolicy.identity`` is an ordered list of these
    rules; the first one whose conjunction matches wins (OR across rules,
    first-match-wins).

    ``on_match`` lets a rule override the policy-level
    :class:`WriteConflictPolicy` — useful for "match by external_id →
    UPDATE; match by geometry_hash → REFUSE".
    """

    model_config = ConfigDict(extra="forbid")

    match_on: List[ComputedField] = Field(min_length=1)
    on_match: Optional["WriteConflictPolicy"] = None


class FeatureType(BaseModel):
    """Declarative wire-shape contract for the read path.

    Used by the forthcoming ``ItemsReadPolicy``. ``schema_ref`` points at
    a JSON Schema source — by default the policy's own write-time
    ``schema`` is reused as the response shape. ``expose`` enumerates
    field names from ``ItemsWritePolicy.compute`` (or properties) that
    should be surfaced in responses. ``failure_mode`` controls behaviour
    when an output transformer raises.
    """

    model_config = ConfigDict(extra="forbid")

    schema_ref: str = "items_write_policy.schema"
    expose: List[str] = Field(default_factory=list)
    failure_mode: Literal["strict", "best_effort"] = "best_effort"


__all__ = [
    "ComputedKind",
    "ComputedField",
    "IdentityRule",
    "FeatureType",
    "SPATIAL_CELL_KINDS",
    "PATH_EXTRACTED_KINDS",
]
