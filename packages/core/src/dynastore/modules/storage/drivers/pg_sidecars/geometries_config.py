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

from typing import List, Optional, Dict, Literal, Any
from enum import Enum
from pydantic import BaseModel, Field, model_validator
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
# STATISTICS CONFIGURATION
# ============================================================================

class MorphologicalIndex(str, Enum):
    """Supported morphological indices for geometry analysis."""
    CIRCULARITY = "circularity"
    CONVEXITY = "convexity"
    ASPECT_RATIO = "aspect_ratio"
    SPHERICITY = "sphericity"  # 3D only
    FLATNESS = "flatness"      # 3D only


class StatisticStorageMode(str, Enum):
    """How to store computed statistics."""
    JSONB = "jsonb"       # All stats in single JSONB column
    COLUMNAR = "columnar" # Individual typed columns


class StatisticIndexConfig(BaseModel):
    """Per-statistic configuration."""
    enabled: bool = Field(default=False, description="Compute this statistic")
    index: bool = Field(default=False, description="Create B-Tree index on this statistic")


class GeometriesStatisticsConfig(BaseModel):
    """
    Configuration for geometry statistics computation and storage.
    
    Supports dual storage modes:
    - JSONB: All stats in single column with functional B-Tree indexes
    - Columnar: Individual typed columns with direct B-Tree indexes
    """
    enabled: bool = Field(default=False, description="Enable statistics computation")
    storage_mode: StatisticStorageMode = Field(
        default=StatisticStorageMode.JSONB,
        description="Storage mode: JSONB or individual columns"
    )
    
    # Basic geometric metrics with indexing control
    area: StatisticIndexConfig = Field(
        default_factory=lambda: StatisticIndexConfig(),
        description="2D surface area or 3D surface area"
    )
    volume: StatisticIndexConfig = Field(
        default_factory=lambda: StatisticIndexConfig(),
        description="Volume (3D closed meshes only)"
    )
    length: StatisticIndexConfig = Field(
        default_factory=lambda: StatisticIndexConfig(),
        description="Perimeter or line length"
    )
    
    # Centroid
    centroid_type: Optional[Literal["geometric", "weighted", "median"]] = Field(default=None,
        description="Type of centroid to compute"
    )
    index_centroid: bool = Field(default=False, description="Create index on centroid coordinates")
    
    # Morphological indices (map of index -> should_index)
    morphological_indices: Dict[MorphologicalIndex, bool] = Field(
        default_factory=dict,
        description="Map of morphological index to whether it should be indexed"
    )
    
    # Topology
    vertex_count: StatisticIndexConfig = Field(
        default_factory=lambda: StatisticIndexConfig(),
        description="Number of vertices in geometry"
    )
    hole_count: StatisticIndexConfig = Field(
        default_factory=lambda: StatisticIndexConfig(),
        description="Number of holes (interior rings)"
    )
    
    # Legacy GIN support (not recommended for large datasets)
    create_gin_index: bool = Field(
        default=False, 
        description="Create GIN index on geom_stats JSONB (WARNING: Large at trillion-row scale)"
    )


class PlaceStatisticsConfig(BaseModel):
    """
    Configuration for JSON-FG 'place' geometry statistics.
    
    These statistics are only meaningful when a 3D precision 'place' member
    is provided (e.g., Solid, Prism, 3D Curve) in a local/engineering CRS.
    """
    enabled: bool = Field(default=False, description="Enable JSON-FG place statistics computation")
    storage_mode: StatisticStorageMode = Field(
        default=StatisticStorageMode.JSONB,
        description="Storage mode: JSONB or individual columns"
    )
    place_column: str = Field(default="place", description="Column name for the JSON-FG place geometry")
    coordRefSys_column: Optional[str] = Field(
        default="coordRefSys",
        description="Column name for the coordinate reference system identifier"
    )
    # Volumetric
    volume: StatisticIndexConfig = Field(
        default_factory=lambda: StatisticIndexConfig(),
        description="Volume in cubic units (3D solids and prisms only)"
    )
    surface_area: StatisticIndexConfig = Field(
        default_factory=lambda: StatisticIndexConfig(),
        description="Total surface area of all exterior faces of a 3D solid"
    )
    surface_to_volume_ratio: StatisticIndexConfig = Field(
        default_factory=lambda: StatisticIndexConfig(),
        description="Ratio of surface area to volume (building efficiency metric)"
    )
    net_floor_area: StatisticIndexConfig = Field(
        default_factory=lambda: StatisticIndexConfig(),
        description="Estimated usable floor area for prism features"
    )
    # 3D Morphological
    centroid_3d: bool = Field(
        default=False,
        description="Compute 3D centroid (center of gravity) of volume"
    )
    z_range: StatisticIndexConfig = Field(
        default_factory=lambda: StatisticIndexConfig(),
        description="Vertical elevation delta (max_z - min_z)"
    )
    vertical_gradient: StatisticIndexConfig = Field(
        default_factory=lambda: StatisticIndexConfig(),
        description="Steepness of a 3D Curve (e.g., slope of a pipe or road)"
    )
    # Temporal
    temporal_duration: StatisticIndexConfig = Field(
        default_factory=lambda: StatisticIndexConfig(),
        description="Computed from the JSON-FG 'time' member interval duration"
    )
    # Legacy GIN
    create_gin_index: bool = Field(
        default=False,
        description="Create GIN index on place_stats JSONB"
    )


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
    ``IdentityMatcher.GEOMETRY_HASH`` and the
    ``skip_if_unchanged_geometry_hash`` write-policy gate.

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
        """Legacy support for write_bbox toggle."""
        return self.bbox_column is not None
    
    # Processing policies are operator-tunable write-time behaviour and live
    # on ``ItemsWritePolicy.geometries`` (``GeometriesWriteBehavior``) rather
    # than this storage-shape config — see #941.

    # Spatial indexes configuration
    h3_resolutions: List[int] = Field(default_factory=list, description="H3 resolutions to index (0-15)")
    s2_resolutions: List[int] = Field(default_factory=list, description="S2 resolutions to index (0-30)")
    geohash_precision: Optional[int] = Field(
        default=8, ge=1, le=12,
        description=(
            "If set, a STORED generated column `geohash CHAR(N)` (N=precision, 1-12) "
            "is added to the geometry sidecar, populated by ST_GeoHash(geom, N), "
            "with a B-tree index.  Used by ItemsWritePolicy.IdentityMatcher.GEOHASH "
            "for spatial-locality-based deduplication on write. Default 8 (≈20 m, ≈38 bits) — "
            "small storage cost, useful out-of-the-box for spatial dedup. Set to None to "
            "opt out for collections where the column would be wasted."
        ),
    )

    # Partitioning
    partition_strategy: Optional[GeometryPartitionStrategyPreset] = Field(default=None, description="Strategy to use for contributing to the global partition key."
    )
    partition_resolution: int = Field(
        default=0, description="Resolution to use for partitioning (must be in h3_resolutions or s2_resolutions)."
    )
    
    # Geometry Statistics
    statistics: Optional[GeometriesStatisticsConfig] = Field(
        default=GeometriesStatisticsConfig(
            enabled= True,
            storage_mode=StatisticStorageMode.COLUMNAR,
            area=StatisticIndexConfig(enabled=True, index=True),
            volume=StatisticIndexConfig(enabled=True, index=True),
            length=StatisticIndexConfig(enabled=True, index=True),
            centroid_type= "geometric",
            index_centroid= True
        ),
        description="Geometry statistics configuration for computing and storing metrics"
    )
    
    # JSON-FG Place Statistics (3D geometry metrics)
    place_statistics: Optional[PlaceStatisticsConfig] = Field(default=None,
        description=(
            "Configuration for JSON-FG 'place' geometry statistics. "
            "Enables 3D metrics like volume, surface area, z-range for Solid/Prism types. "
            "Requires a 'place' column in the geometry sidecar table."
        )
    )
    
    # Protocol-Driven Architecture
    store_bbox: bool = Field(default=True, description="Store bounding box geometry")
    store_centroid: bool = Field(default=False, description="Store centroid point")
    
    feature_type_schema: Optional[Dict[str, Any]] = Field(default=None,
        description="JSON Schema override for geometry contribution to Feature. "
                    "Can include 'bbox' in properties or customize geometry output."
    )

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
