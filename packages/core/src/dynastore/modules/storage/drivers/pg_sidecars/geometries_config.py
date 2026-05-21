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
Geometry Sidecar Configuration and Enums.

This module is extracted to avoid circular dependencies between:
- catalog_config
- sidecars/geometry
- tools/geospatial
"""

from typing import List, Optional, Dict, Literal
from enum import Enum
from pydantic import Field
from dynastore.models.mutability import Computed
from dynastore.modules.storage.computed_fields import (
    ComputedField,
    ComputedKind,
)
from dynastore.modules.storage.drivers.pg_sidecars.base import SidecarConfig, SidecarConfigRegistry

# ============================================================================
# ENUMS
# ============================================================================

class TargetDimension(str, Enum):
    FORCE_2D = "force_2d"
    FORCE_3D = "force_3d"


class InvalidGeometryPolicy(str, Enum):
    REJECT = "reject"
    ATTEMPT_FIX = "attempt_fix"


class SridMismatchPolicy(str, Enum):
    REJECT = "reject"
    TRANSFORM = "transform"


class SimplificationAlgorithm(str, Enum):
    DOUGLAS_PEUCKER = "douglas_peucker"
    TOPOLOGY_PRESERVING = "topology_preserving"
    VISVALINGAM_WHYATT = "visvalingam_whyatt"


class GeometryPartitionStrategyPreset(str, Enum):
    """
    Partition strategies supported by the Geometry Sidecar.
    """
    H3_CELL = "h3_cell"        # Partition by H3 cell ID (BIGINT)
    S2_CELL = "s2_cell"        # Partition by S2 cell ID (BIGINT)


# ============================================================================
# MAIN CONFIGURATION
# ============================================================================

class GeometriesSidecarConfig(SidecarConfig):
    """PG sidecar table that stores geometry off-hub.

    Owns the ``{schema}.{table}_geometries`` table — one row per item
    (FK to hub on ``geoid``).  Columns include ``geom`` (and optional
    ``bbox_geom``), a STORED GENERATED ``geohash CHAR(N)`` driven by
    ``ST_GeoHash(geom, N)``, and a STORED GENERATED ``geometry_hash
    CHAR(64)`` (SHA256 of ``ST_AsBinary(geom)``) used by
    ``ComputedKind.GEOMETRY_HASH`` and the
    ``geometries.skip_if_unchanged_geometry_hash`` write-policy gate.

    Attached by default for VECTOR / RASTER collections; not attached
    for RECORDS (no spatial component).
    """
    sidecar_type: Literal["geometries"] = "geometries"
    
    # Geometry storage settings
    target_srid: int = Field(default=4326, description="Target SRID for geometry storage")
    target_dimension: TargetDimension = Field(default=TargetDimension.FORCE_2D)
    
    # Column mapping
    geom_column: str = Field(default="geom", description="Main geometry column name (source and target)")
    bbox_column: Optional[str] = Field(
        default="bbox_geom", 
        description="Bounding box column name. If set, a separate column for the spatial extent is managed. Set to None to disable."
    )

    @property
    def write_bbox(self) -> bool:
        """True when a dedicated bbox column is configured."""
        return self.bbox_column is not None
    
    # Processing policies are operator-tunable write-time behaviour and live
    # on ``ItemsWritePolicy.geometries`` (``GeometriesWriteBehavior``) rather
    # than this storage-shape config — see #941.

    # Spatial-cell index columns are DERIVED from the items policy (Phase 3,
    # Decision 2). The geohash/h3/s2 stored columns and their precisions are
    # NOT an independent sidecar knob — they mirror
    # ``ItemsWritePolicy.derive.spatial_cells`` so the persisted column width
    # can never drift from the identity axis. The PG driver overlays the
    # policy's spatial-cell ComputedFields onto ``spatial_cells_overlay`` at
    # ``ensure_storage`` time; the precision/resolution accessors below read
    # that overlay. (The old authorable ``geohash_precision`` /
    # ``h3_resolutions`` / ``s2_resolutions`` fields are removed — a clean
    # break, no shim.)
    spatial_cells_overlay: Computed[List[ComputedField]] = Field(
        default_factory=list,
        description=(
            "DERIVED, read-only. Storage-shape snapshot of the spatial-cell "
            "entries (geohash/h3/s2) from ``ItemsWritePolicy.derive."
            "spatial_cells`` for this collection. Overwritten by the PG driver "
            "at DDL time; drives the persisted geohash/h3/s2 index columns so "
            "the stored precision can never diverge from the identity axis."
        ),
    )

    @property
    def geohash_precision(self) -> Optional[int]:
        """DERIVED geohash column width (CHAR(N)) from the policy's geohash
        spatial cell, or ``None`` when the policy declares no geohash cell.

        Physical realization of ``ItemsWritePolicy.derive.spatial_cells`` —
        the stored ``geohash CHAR(N)`` column uses the same N as the
        identity-axis geohash so the two can never diverge (Phase 3
        Decision 2; was a standalone authorable knob pre-Phase-3)."""
        for cf in self.spatial_cells_overlay:
            if cf.kind == ComputedKind.GEOHASH and cf.resolution is not None:
                return cf.resolution
        return None

    @property
    def h3_resolutions(self) -> List[int]:
        """DERIVED H3 index resolutions from the policy's H3 spatial cells."""
        return [
            cf.resolution
            for cf in self.spatial_cells_overlay
            if cf.kind == ComputedKind.H3 and cf.resolution is not None
        ]

    @property
    def s2_resolutions(self) -> List[int]:
        """DERIVED S2 index resolutions from the policy's S2 spatial cells."""
        return [
            cf.resolution
            for cf in self.spatial_cells_overlay
            if cf.kind == ComputedKind.S2 and cf.resolution is not None
        ]

    # Partitioning — PHYSICAL realization, not policy (Phase 3 Decision 5):
    # the partition strategy/resolution is a genuinely-physical operator
    # choice that cannot be inferred from policy intent, so it stays an
    # authorable knob on the driver config. ``partition_resolution`` must be
    # one of the DERIVED ``h3_resolutions``/``s2_resolutions`` (which mirror
    # the policy's spatial cells).
    partition_strategy: Optional[GeometryPartitionStrategyPreset] = Field(default=None, description="PHYSICAL: strategy to use for contributing to the global partition key (operator-tunable, not policy)."
    )
    partition_resolution: int = Field(
        default=0, description="PHYSICAL: resolution to use for partitioning (must be one of the derived h3_resolutions or s2_resolutions, which mirror the policy spatial cells)."
    )
    
    # Geometry Statistics — storage-shape mirror of ``ItemsWritePolicy.compute``
    #
    # The PG driver populates this list at ``ensure_storage`` time with the
    # subset of ``ItemsWritePolicy.compute`` whose entries declare a
    # ``storage_mode`` (i.e. should land on disk via this sidecar). DDL
    # emission, ``get_select_fields``, ``get_field_definitions``, and
    # ``prepare_upsert_payload`` all read this single list — there is no
    # second source for "which stats does this sidecar materialise". An
    # empty list means the sidecar carries geometry + spatial-index columns
    # only; no stats are materialised at all.
    #
    # 3D place-statistics (SURFACE_AREA, Z_RANGE, CENTROID_3D, etc.) are
    # declared here using the standard ComputedField / ComputedKind API;
    # the PG driver detects any kind in ``_PLACE_TABLE_KINDS`` and emits a
    # separate ``{table}_place`` sidecar table automatically.
    compute_fields_overlay: List[ComputedField] = Field(
        default_factory=list,
        description=(
            "Storage-shape snapshot of the storage-bearing entries from "
            "``ItemsWritePolicy.compute`` for this collection. Overwritten "
            "by the PG driver at DDL time."
        ),
    )
    
    # ``feature_type_schema`` was retired in #976: the wire shape of Feature
    # ``properties`` is now the SSOT on ``ItemsWritePolicy.resolved_schema``. The
    # geometry contribution stays auto-derived from this sidecar's columns.

    @property
    def partition_key_contributions(self) -> Dict[str, str]:
        if not self.partition_strategy:
            return {}
        
        if self.partition_strategy == GeometryPartitionStrategyPreset.H3_CELL:
            return {f"h3_res{self.partition_resolution}": "BIGINT"}
        
        if self.partition_strategy == GeometryPartitionStrategyPreset.S2_CELL:
            return {f"s2_res{self.partition_resolution}": "BIGINT"}
            
        return {}

# Rebuild model to resolve forward references
GeometriesSidecarConfig.model_rebuild()

# Register for polymorphic resolution
SidecarConfigRegistry.register("geometries", GeometriesSidecarConfig)
