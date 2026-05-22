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
Geometries Sidecar Implementation.

This module provides the GeometriesSidecar which manages:
- Primary geometry storage (geom)
- Bounding box storage (bbox_geom)
- Spatial indexing (H3, S2)
- Geometry validation and fixing logic
"""

import hashlib
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple, Union
from enum import Enum
from pydantic import Field
from shapely.geometry import shape, mapping
from shapely import wkb
from geojson_pydantic import Feature
from geojson_pydantic.geometries import Geometry
from dynastore.models.query_builder import QueryRequest

from dynastore.modules.storage.drivers.pg_sidecars.base import (
    SidecarProtocol,
    SidecarConfig,
    FeaturePipelineContext,
    ValidationResult,
    FieldDefinition,
    FieldCapability,
)
from dynastore.modules.catalog.models import LocalizedText
from dynastore.modules.storage.computed_fields import (
    ComputedField,
    ComputedKind,
    StatisticStorageMode,
    _PLACE_TABLE_KINDS,
)
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesSidecarConfig,
    TargetDimension,
    InvalidGeometryPolicy,
    SridMismatchPolicy,
    SimplificationAlgorithm,
    GeometryPartitionStrategyPreset,
)
from dynastore.modules.db_config.query_executor import DbResource, DQLQuery, ResultHandler
from dynastore.tools.geospatial_exceptions import (
    GeometryProcessingError,
)

logger = logging.getLogger(__name__)


# ============================================================================
# ENUMS & CONSTANTS
# ============================================================================


# ============================================================================
# IMPLEMENTATION
# ============================================================================


class GeometriesSidecar(SidecarProtocol):
    """
    Sidecar for geometry storage and spatial indexing.

    Manages:
    - Primary geometry (geom)
    - Bounding box (bbox_geom)
    - H3 spatial indexes
    - S2 spatial indexes
    """

    # Map of ``ComputedKind`` → SQL column type for COLUMNAR storage. ``CENTROID``
    # is special-cased because it needs the active SRID + 2D/3D selection.
    # ``CENTROID_3D`` always emits GEOMETRY(POINTZ, 4326) regardless of srid.
    _COLUMNAR_SQL_TYPE: Dict[ComputedKind, str] = {
        ComputedKind.AREA: "DOUBLE PRECISION",
        ComputedKind.VOLUME: "DOUBLE PRECISION",
        ComputedKind.PERIMETER: "DOUBLE PRECISION",
        ComputedKind.LENGTH: "DOUBLE PRECISION",
        ComputedKind.BBOX: "DOUBLE PRECISION[]",
        ComputedKind.VERTEX_COUNT: "INTEGER",
        ComputedKind.HOLE_COUNT: "INTEGER",
        ComputedKind.CIRCULARITY: "DOUBLE PRECISION",
        ComputedKind.CONVEXITY: "DOUBLE PRECISION",
        ComputedKind.ASPECT_RATIO: "DOUBLE PRECISION",
        # JSON-FG 3D place-statistics
        ComputedKind.SURFACE_AREA: "DOUBLE PRECISION",
        ComputedKind.SURFACE_TO_VOLUME_RATIO: "DOUBLE PRECISION",
        ComputedKind.NET_FLOOR_AREA: "DOUBLE PRECISION",
        ComputedKind.CENTROID_3D: "GEOMETRY(POINTZ, 4326)",
        ComputedKind.Z_RANGE: "DOUBLE PRECISION",
        ComputedKind.VERTICAL_GRADIENT: "DOUBLE PRECISION",
        ComputedKind.TEMPORAL_DURATION: "INTERVAL",
    }

    _NUMERIC_KINDS: frozenset = frozenset({
        ComputedKind.AREA,
        ComputedKind.VOLUME,
        ComputedKind.PERIMETER,
        ComputedKind.LENGTH,
        ComputedKind.CIRCULARITY,
        ComputedKind.CONVEXITY,
        ComputedKind.ASPECT_RATIO,
    })

    _INTEGER_KINDS: frozenset = frozenset({
        ComputedKind.VERTEX_COUNT,
        ComputedKind.HOLE_COUNT,
    })

    def __init__(self, config: GeometriesSidecarConfig, **_kwargs: Any):
        self.config = config

    # ------------------------------------------------------------------
    # Storage-shape helpers (consume ``compute_fields_overlay``)
    # ------------------------------------------------------------------

    def _storage_fields(self) -> List[ComputedField]:
        """Return the storage-bearing :class:`ComputedField` overlay.

        The PG driver populates this list at ``ensure_storage`` time from
        ``ItemsWritePolicy.compute`` (filtered to entries whose
        ``storage_mode`` is set). Every DDL/projection/upsert site reads
        from here so there is one shape decision per collection.
        """
        return [
            f for f in self.config.compute_fields_overlay
            if f.storage_mode is not None
        ]

    def _has_jsonb_stats(self) -> bool:
        """Any storage-bearing field with ``storage_mode == JSONB``?"""
        return any(
            f.storage_mode == StatisticStorageMode.JSONB for f in self._storage_fields()
        )

    def _columnar_fields(self) -> List[ComputedField]:
        return [
            f for f in self._storage_fields()
            if f.storage_mode == StatisticStorageMode.COLUMNAR
        ]

    def _centroid_sql_type(self, field: ComputedField, srid: int) -> str:
        ct = field.centroid_type or "POINT"
        return f"GEOMETRY({ct}, {srid})"

    def _columnar_sql_type(self, field: ComputedField, srid: int) -> str:
        if field.kind == ComputedKind.CENTROID:
            return self._centroid_sql_type(field, srid)
        return self._COLUMNAR_SQL_TYPE[field.kind]

    def _field_data_type(self, field: ComputedField) -> str:
        """``FieldDefinition.data_type`` for the read API."""
        if field.kind == ComputedKind.CENTROID:
            return "geometry"
        if field.kind == ComputedKind.BBOX:
            return "array"
        if field.kind in self._INTEGER_KINDS:
            return "integer"
        return "numeric"

    def _jsonb_cast_for(self, field: ComputedField) -> str:
        if field.kind in self._INTEGER_KINDS:
            return "integer"
        return "numeric"

    @property
    def sidecar_id(self) -> str:
        return "geometries"

    @property
    def sidecar_type_id(self) -> str:
        """Type ID for protocol-based discovery and config matching."""
        return "geometries"

    @classmethod
    def get_default_config(cls, context: Dict[str, Any]) -> Optional[GeometriesSidecarConfig]:
        """Auto-inject geometries sidecar by default.

        Skipped for RECORDS collections which have no spatial component.
        """
        if context.get("collection_type") == "RECORDS":
            return None
        return GeometriesSidecarConfig()

    @property
    def provides_feature_id(self) -> bool:
        """Returns True if this sidecar provides the feature ID (geoid)."""
        return False

    def get_queryable_fields(self) -> Dict[str, FieldDefinition]:
        """
        Returns all queryable geometry fields.

        Includes geom, bbox, and optionally centroid based on config.
        """
        # Alias must match builder convention
        alias = f"sc_{self.sidecar_id}"

        fields = {}

        # Main geometry - always queryable
        fields[self.config.geom_column] = FieldDefinition(
            name=self.config.geom_column,
            sql_expression=f"{alias}.{self.config.geom_column}",
            capabilities=[FieldCapability.FILTERABLE, FieldCapability.SPATIAL],
            data_type="geometry",
            expose=True,
            title="Geometry",
            description="Primary geometry",
        )

        # Bounding box - always stored, queryable
        if self.config.bbox_column:
            fields["bbox"] = FieldDefinition(
                name="bbox",
                sql_expression=f"{alias}.{self.config.bbox_column}",
                capabilities=[FieldCapability.FILTERABLE, FieldCapability.SPATIAL],
                data_type="box2d",
                expose=False,  # Query-only by default
                title="Bounding Box",
                description="Geometry bounding box",
            )

        # Add index fields from get_field_definitions if needed
        # Just use defaults from get_field_definitions to ensure consistency
        other_fields = self.get_field_definitions(sidecar_alias=alias)
        for k, v in other_fields.items():
            if k not in fields:
                fields[k] = v

        return fields

    def get_feature_type_schema(self) -> Dict[str, Any]:
        """
        Returns JSON Schema for geometry contribution to Feature.

        Auto-derived to the well-known GeoJSON geometry shape; the
        user-data wire shape is derived from ``ItemsSchema`` (the SSOT) and
        overlays sidecar fragments at the service layer (#976).
        """
        return {"geometry": {"type": "object", "description": "GeoJSON geometry"}}

    def get_main_geometry_field(self) -> Optional[str]:
        """Returns the configured main geometry field."""
        return self.config.geom_column

    @property
    def partition_key_contributions(self) -> Dict[str, str]:
        """Expose keys available for partitioning."""
        return self.config.partition_key_contributions

    # --- Partitioning & Capabilities ---

    def get_partition_keys(self) -> List[str]:
        """Returns list of columns this sidecar contributes to composite partitioning."""
        return self.config.partition_keys

    def get_partition_key_types(self) -> Dict[str, str]:
        """Returns mapping of partition keys to their SQL types."""
        return self.config.partition_key_types

    def has_validity(self) -> bool:
        """Returns True if this sidecar manages temporal validity (Geometry does not)."""
        return False

    def get_ddl(
        self,
        physical_table: str,
        partition_keys: List[str] = [],
        partition_key_types: Dict[str, str] = {},
        has_validity: bool = False,
    ) -> str:
        """Generate DDL for geometry sidecar table."""
        geom_modifier = (
            "" if self.config.target_dimension == TargetDimension.FORCE_2D else "Z"
        )
        srid = self.config.target_srid

        # Base columns
        columns = [
            "geoid UUID NOT NULL",
            f"{self.config.geom_column} GEOMETRY(GEOMETRY{geom_modifier}, {srid}) NOT NULL",
            "geom_type VARCHAR(50) NOT NULL",
        ]

        # Shared context for column tracking
        known_columns = {"geoid", self.config.geom_column, "geom_type"}
        if self.config.bbox_column:
            known_columns.add(self.config.bbox_column)

        # Add validity if versioned (has_validity) OR if it's a partition key
        if has_validity or "validity" in partition_keys:
            columns.append("validity TSTZRANGE NOT NULL")
            known_columns.add("validity")

        if self.config.bbox_column:
            columns.append(f"{self.config.bbox_column} GEOMETRY(POLYGON, {srid})")

        # H3/S2 columns (these are potential partition keys provided by THIS sidecar)

        for res in self.config.h3_resolutions:
            col = f"h3_res{res}"
            columns.append(f"{col} BIGINT")
            known_columns.add(col)

        for res in self.config.s2_resolutions:
            col = f"s2_res{res}"
            columns.append(f"{col} BIGINT")
            known_columns.add(col)

        # Geohash generated column (ST_GeoHash is IMMUTABLE → safe for STORED).
        # ST_GeoHash requires inputs in decimal degrees (EPSG:4326). When the
        # geometry column is stored in a projected CRS (e.g. target_srid=3857),
        # PostgreSQL raises "Geohash requires inputs in decimal degrees" on
        # the generated-column compute. Wrap in ST_Transform to 4326 so the
        # geohash is always semantically the WGS84 location of the geometry,
        # regardless of how the source geometry is stored. ST_Transform is a
        # no-op when geom is already in 4326.
        if self.config.geohash_precision:
            gh_prec = self.config.geohash_precision
            columns.append(
                f"geohash CHAR({gh_prec}) GENERATED ALWAYS AS "
                f"(ST_GeoHash(ST_Transform({self.config.geom_column}, 4326), {gh_prec})) STORED"
            )
            known_columns.add("geohash")

        # Geometry hash GENERATED column — SHA256 of the WKB.  Mirrors
        # ``attributes_hash`` on the attributes sidecar; replaces the
        # legacy hub ``geometry_hash`` column (issue #220).  Application
        # code never computes this hash — Postgres maintains it
        # atomically with the geometry, eliminating the skew window
        # between ``hub.geometry_hash`` and the actual stored geometry
        # that the previous design carried.
        #
        # Requires pgcrypto extension (already enabled by ensure_init_db).
        columns.append(
            f"geometry_hash CHAR(64) GENERATED ALWAYS AS "
            f"(encode(digest(ST_AsBinary({self.config.geom_column}), 'sha256'), 'hex')) STORED"
        )
        known_columns.add("geometry_hash")

        # Add Statistics Columns — derived from ``compute_fields_overlay``
        if self._has_jsonb_stats():
            columns.append("geom_stats JSONB")
            known_columns.add("geom_stats")
        for f in self._columnar_fields():
            col_name = f.resolved_name
            col_type = self._columnar_sql_type(f, srid)
            columns.append(f"{col_name} {col_type}")
            known_columns.add(col_name)

        # Composite PK Construction
        # Rule: Partition keys FIRST, then Hub identity (geoid, validity)
        pk_columns = []

        if partition_keys:
            # 1. Add any non-sidecar partition keys (e.g. catalog_id from Hub)
            for key in partition_keys:
                if key not in known_columns:
                    col_type = partition_key_types.get(key, "TEXT")
                    columns.insert(0, f'"{key}" {col_type} NOT NULL')
                    known_columns.add(key)

            # 2. Partitions must be part of the PK
            pk_set = set()
            for key in partition_keys:
                pk_columns.append(f'"{key}"')
                pk_set.add(key)

            # 3. Complete the PK with identity if not already present
            if "geoid" not in pk_set:
                pk_columns.append('"geoid"')
            if (
                has_validity or "validity" in partition_keys
            ) and "validity" not in pk_set:
                pk_columns.append('"validity"')

            partition_clause = (
                f" PARTITION BY LIST ({', '.join([f'"{k}"' for k in partition_keys])})"
            )
        else:
            partition_clause = ""
            # Non-partitioned table: PK is geoid + validity (if has_validity)
            pk_columns = ['"geoid"']
            if has_validity or "validity" in partition_keys:
                pk_columns.append('"validity"')

        # Build DDL
        table_name = f"{physical_table}_{self.sidecar_id}"

        create_sql = f'CREATE TABLE IF NOT EXISTS {{schema}}."{table_name}" ({", ".join(columns)}, PRIMARY KEY ({", ".join(pk_columns)})){partition_clause};'

        # Foreign Key to Hub - only if validity matches or only geoid
        ref_cols = ["geoid"]

        # Determine if validity should be in FK
        # Only if validity is a partition key (shared with Hub)
        if has_validity or "validity" in partition_keys:
            ref_cols.append("validity")

        fk_sql = (
            f'\nALTER TABLE {{schema}}."{table_name}" '
            f'ADD CONSTRAINT "fk_{table_name}_hub" '
            f"FOREIGN KEY ({', '.join([f'''{c}''' for c in ref_cols])}) "
            f'REFERENCES {{schema}}."{physical_table}" ({", ".join([f"""{c}""" for c in ref_cols])}) '
            f"ON DELETE CASCADE;"
        )

        ddl = create_sql + fk_sql

        # Add spatial indexes
        ddl += f'\nCREATE INDEX IF NOT EXISTS "idx_{table_name}_geom" ON {{schema}}."{table_name}" USING GIST({self.config.geom_column});'
        if self.config.bbox_column:
            ddl += f'\nCREATE INDEX IF NOT EXISTS "idx_{table_name}_bbox" ON {{schema}}."{table_name}" USING GIST({self.config.bbox_column});'

        # Add H3/S2 indexes
        for res in self.config.h3_resolutions:
            col = f"h3_res{res}"
            if col in known_columns:
                ddl += f'\nCREATE INDEX IF NOT EXISTS "idx_{table_name}_h3_{res}" ON {{schema}}."{table_name}" ({col});'
        for res in self.config.s2_resolutions:
            col = f"s2_res{res}"
            if col in known_columns:
                ddl += f'\nCREATE INDEX IF NOT EXISTS "idx_{table_name}_s2_{res}" ON {{schema}}."{table_name}" ({col});'

        if self.config.geohash_precision and "geohash" in known_columns:
            ddl += f'\nCREATE INDEX IF NOT EXISTS "idx_{table_name}_geohash" ON {{schema}}."{table_name}" (geohash);'

        # Index for the GEOMETRY_HASH identity matcher (lookup on equality).
        if "geometry_hash" in known_columns:
            ddl += f'\nCREATE INDEX IF NOT EXISTS "idx_{table_name}_geometry_hash" ON {{schema}}."{table_name}" (geometry_hash);'

        # Add Statistics Indexes — derived from ``compute_fields_overlay``.
        # COLUMNAR + indexed=True emits a direct B-tree; JSONB never
        # combines with indexed=True (rejected at ComputedField
        # construction — operators must declare a second COLUMNAR field
        # to get a B-tree alongside the JSONB key).
        for f in self._columnar_fields():
            if not f.indexed:
                continue
            col_name = f.resolved_name
            ddl += (
                f'\nCREATE INDEX IF NOT EXISTS "idx_{table_name}_{col_name}" '
                f'ON {{schema}}."{table_name}" ({col_name});'
            )

        # --- Place Statistics DDL (JSON-FG 'place' column) ---
        # Emitted when any storage-bearing ComputedField in the overlay belongs
        # to _PLACE_TABLE_KINDS. The table always has a 'place' JSONB column
        # (the raw JSON-FG 'place' member) and a 'coordRefSys' TEXT column.
        # Stat columns follow the same JSONB-vs-COLUMNAR logic as geom_stats.
        place_fields = [
            f for f in self._storage_fields() if f.kind in _PLACE_TABLE_KINDS
        ]
        if place_fields:
            place_table = f"{physical_table}_place"
            place_cols = [
                "geoid UUID NOT NULL",
                "place JSONB NOT NULL",
                "coordRefSys TEXT",
            ]
            has_jsonb = any(
                f.storage_mode == StatisticStorageMode.JSONB for f in place_fields
            )
            if has_jsonb:
                place_cols.append("place_stats JSONB")
            for f in place_fields:
                if f.storage_mode != StatisticStorageMode.COLUMNAR:
                    continue
                col_type = self._COLUMNAR_SQL_TYPE.get(f.kind, "DOUBLE PRECISION")
                place_cols.append(f"place_{f.resolved_name} {col_type}")

            ddl += (
                f'\nCREATE TABLE IF NOT EXISTS {{schema}}."{place_table}" '
                f'({", ".join(place_cols)}, PRIMARY KEY ("geoid"));'
            )
            ddl += (
                f'\nALTER TABLE {{schema}}."{place_table}" '
                f'ADD CONSTRAINT "fk_{place_table}_hub" '
                f'FOREIGN KEY (geoid) REFERENCES {{schema}}."{physical_table}" '
                f'(geoid) ON DELETE CASCADE;'
            )
            # Indexes for COLUMNAR place fields
            for f in place_fields:
                if f.storage_mode == StatisticStorageMode.COLUMNAR and f.indexed:
                    col_name = f"place_{f.resolved_name}"
                    ddl += (
                        f'\nCREATE INDEX IF NOT EXISTS "idx_{place_table}_{f.resolved_name}" '
                        f'ON {{schema}}."{place_table}" ({col_name});'
                    )

        return ddl

    async def setup_lifecycle_hooks(
        self, conn: DbResource, schema: str, table_name: str
    ) -> None:
        """Register VACUUM schedule for geometry table."""
        # Geometry tables benefit from regular VACUUM due to spatial index updates
        logger.info(f"Setting up lifecycle hooks for {schema}.{table_name}")
        pass

    async def on_partition_create(
        self,
        conn: DbResource,
        schema: str,
        parent_table: str,
        partition_table: str,
        partition_value: Any,
    ) -> None:
        """Set FILLFACTOR for geometry partitions."""
        logger.info(f"Geometry partition created: {schema}.{partition_table}")
        pass

    def resolve_query_path(self, attr_name: str) -> Optional[Tuple[str, str]]:
        """Resolve geometry-related attributes."""
        # Alias must match builder convention
        alias = f"sc_{self.sidecar_id}"

        # Geometry column access
        if attr_name == self.config.geom_column:
            return (f"{alias}.{self.config.geom_column}", alias)
        if self.config.bbox_column and attr_name == "bbox":
            return (f"{alias}.{self.config.bbox_column}", alias)
        if attr_name == "geom_type":
            return (f"{alias}.geom_type", alias)

        # BBOX components
        if self.config.bbox_column:
            if attr_name == "bbox_xmin":
                return (f"ST_XMin({alias}.{self.config.bbox_column})", alias)
            if attr_name == "bbox_ymin":
                return (f"ST_YMin({alias}.{self.config.bbox_column})", alias)
            if attr_name == "bbox_xmax":
                return (f"ST_XMax({alias}.{self.config.bbox_column})", alias)
            if attr_name == "bbox_ymax":
                return (f"ST_YMax({alias}.{self.config.bbox_column})", alias)

        # H3 indexes
        for res in self.config.h3_resolutions:
            if attr_name == f"h3_res{res}":
                return (f"{alias}.h3_res{res}", alias)

        # S2 indexes
        for res in self.config.s2_resolutions:
            if attr_name == f"s2_res{res}":
                return (f"{alias}.s2_res{res}", alias)

        # Computed attributes
        if attr_name == "area":
            return (f"ST_Area({alias}.{self.config.geom_column})", alias)
        if attr_name == "centroid":
            return (f"ST_Centroid({alias}.{self.config.geom_column})", alias)

        return None

    def get_select_fields(
        self,
        request: Optional[QueryRequest] = None,
        hub_alias: str = "h",
        sidecar_alias: Optional[str] = None,
        include_all: bool = False,
    ) -> List[str]:
        """Returns SELECT field expressions for geometry sidecar."""
        alias = sidecar_alias or f"sc_{self.sidecar_id}"

        fields = []

        if request is not None and not include_all:
            # Selective mode
            requested = {sel.field for sel in request.select} if hasattr(request, "select") and request.select else set()
            filter_fields = {f.field for f in request.filters} if hasattr(request, "filters") and request.filters else set()
            sort_fields = {s.field for s in (request.sort or [])} if hasattr(request, "sort") and request.sort else set()
            all_needed = requested | filter_fields | sort_fields
            
            if self.config.geom_column in all_needed or "geometry" in all_needed or "*" in requested or not requested:
                fields.append(f"ST_AsGeoJSON({alias}.{self.config.geom_column})::jsonb as {self.config.geom_column}")

            if self.config.bbox_column and ("bbox" in all_needed or self.config.bbox_column in all_needed or "*" in requested):
                fields.append(f"ST_AsGeoJSON({alias}.{self.config.bbox_column})::jsonb as {self.config.bbox_column}")

            if self.config.h3_resolutions:
                for res in self.config.h3_resolutions:
                    if f"h3_res{res}" in all_needed or "*" in requested:
                        fields.append(f"{alias}.h3_res{res}")

            if self.config.s2_resolutions:
                for res in self.config.s2_resolutions:
                    if f"s2_res{res}" in all_needed or "*" in requested:
                        fields.append(f"{alias}.s2_res{res}")
                        
            # Add Statistics if requested — overlay-driven, COLUMNAR fields
            # surface per resolved name, JSONB stats project the shared
            # ``geom_stats`` column.
            for f in self._columnar_fields():
                key = f.resolved_name
                if key not in all_needed and "*" not in requested:
                    continue
                if f.kind == ComputedKind.CENTROID:
                    fields.append(f"ST_AsEWKB({alias}.{key}) as {key}")
                else:
                    fields.append(f"{alias}.{key}")
            if self._has_jsonb_stats() and (
                "geom_stats" in all_needed or "*" in requested
            ):
                fields.append(f"{alias}.geom_stats")

        else:
            # Full mode (existing behavior)
            if request is not None:
                # Default to GeoJSON for optimization/consistency in API responses
                fields.append(f"ST_AsGeoJSON({alias}.{self.config.geom_column})::jsonb as {self.config.geom_column}")

            if self.config.bbox_column:
                fields.append(f"ST_AsGeoJSON({alias}.{self.config.bbox_column})::jsonb as {self.config.bbox_column}")

            if self.config.h3_resolutions:
                for res in self.config.h3_resolutions:
                    fields.append(f"{alias}.h3_res{res}")

            if self.config.s2_resolutions:
                for res in self.config.s2_resolutions:
                    fields.append(f"{alias}.s2_res{res}")

            # Add Statistics — full mode projects all storage-bearing fields.
            for f in self._columnar_fields():
                key = f.resolved_name
                if f.kind == ComputedKind.CENTROID:
                    fields.append(f"ST_AsEWKB({alias}.{key}) as {key}")
                else:
                    fields.append(f"{alias}.{key}")
            if self._has_jsonb_stats():
                fields.append(f"{alias}.geom_stats")

        return fields

    def get_join_clause(
        self,
        schema: str,
        hub_table: str,
        hub_alias: str = "h",
        sidecar_alias: Optional[str] = None,
        join_type: str = "LEFT",
        extra_condition: Optional[str] = None,
    ) -> str:
        """Returns JOIN clause for geometry sidecar (geoid-only join)."""
        alias = sidecar_alias or f"sc_{self.sidecar_id}"
        table = f"{hub_table}_geometries"
        on_clause = f"{hub_alias}.geoid = {alias}.geoid"
        if extra_condition:
            on_clause = f"{on_clause} {extra_condition}"
        return f'{join_type} JOIN "{schema}"."{table}" {alias} ON {on_clause}'

    def get_spatial_condition(
        self,
        bbox: Optional[List[float]] = None,
        geometry: Optional[Dict[str, Any]] = None,
        sidecar_alias: Optional[str] = None,
        srid: int = 4326,
    ) -> Optional[str]:
        """
        Returns a spatial filter condition for this sidecar.
        """
        alias = sidecar_alias or f"sc_{self.sidecar_id}"
        conditions = []

        if bbox:
            conditions.append(
                f"ST_Intersects({alias}.{self.config.geom_column}, ST_MakeEnvelope(:bbox_minx, :bbox_miny, :bbox_maxx, :bbox_maxy, :srid))"
            )

        if geometry:
            conditions.append(
                f"ST_Intersects({alias}.{self.config.geom_column}, ST_GeomFromGeoJSON(:geometry))"
            )

        return " AND ".join(conditions) if conditions else None

    def get_geometry_select(
        self, params: Dict[str, Any], sidecar_alias: Optional[str] = None
    ) -> Optional[str]:
        """
        Returns the formatted geometry column expression for SELECT.
        Handles SRID transforms, simplification, and MVT/GeoJSON/GML formatting.
        """
        if params.get("include_geometry") is False:
            return None

        alias = sidecar_alias or f"sc_{self.sidecar_id}"

        # 1. Base Geometry column
        source_srid = params.get("srid", self.config.target_srid)
        target_srid = params.get("target_srid", source_srid)
        geom_col = f"{alias}.{self.config.geom_column}"

        # 2. Transform Logic
        if target_srid != source_srid:
            # ``ST_Transform`` is overloaded — ``(geometry, integer)`` and
            # ``(geometry, text)`` (a proj string). A bare untyped bind param
            # resolves to ``text`` at plan time, so asyncpg rejects the integer
            # SRID ("expected str, got int"). The cast pins the param type to
            # integer (matching the tile-bounds expression below).
            geom_col = f"ST_Transform({geom_col}, CAST(:target_srid AS INTEGER))"

        # 3. Simplification
        simplification = params.get("simplification")
        if simplification and simplification > 0:
            algo = params.get("simplification_algorithm", "ST_SimplifyPreserveTopology")
            geom_col = f"{algo}({geom_col}, :simplification)"

        # 4. Final Formatting
        geom_format = params.get("geom_format", "WKB")
        if geom_format == "GML":
            return f"ST_AsGML(3, {geom_col}, 15, 6)"
        elif geom_format == "GeoJSON":
            return f"ST_AsGeoJSON({geom_col})"
        elif geom_format == "MVT":
            extent = params.get("extent", 4096)
            buffer = params.get("buffer", 256)
            # Use bind param for bounds instead of alias 'b' to avoid scoping issues in ItemService queries
            bounds_expr = (
                "ST_SetSRID(ST_GeomFromWKB(:tile_wkb), CAST(:target_srid AS INTEGER))"
            )
            return f"ST_AsMVTGeom({geom_col}, {bounds_expr}, {extent}, {buffer}, true)"

        # Default WKB
        return f"ST_AsEWKB({geom_col})"

    def get_order_by_fields(self, sidecar_alias: Optional[str] = None) -> List[str]:
        """Returns fields that can be used for ordering."""
        alias = sidecar_alias or f"sc_{self.sidecar_id}"
        fields = []

        # H3 and S2 indexes can be used for spatial ordering
        if self.config.h3_resolutions:
            for res in self.config.h3_resolutions:
                fields.append(f"{alias}.h3_res{res}")

        if self.config.s2_resolutions:
            for res in self.config.s2_resolutions:
                fields.append(f"{alias}.s2_res{res}")

        return fields

    def get_group_by_fields(self, sidecar_alias: Optional[str] = None) -> List[str]:
        """Returns fields that can be used for grouping."""
        alias = sidecar_alias or f"sc_{self.sidecar_id}"
        fields = []

        # H3 and S2 indexes are excellent for spatial grouping
        if self.config.h3_resolutions:
            for res in self.config.h3_resolutions:
                fields.append(f"{alias}.h3_res{res}")

        if self.config.s2_resolutions:
            for res in self.config.s2_resolutions:
                fields.append(f"{alias}.s2_res{res}")

        return fields

    def get_field_definitions(
        self, sidecar_alias: Optional[str] = None
    ) -> Dict[str, FieldDefinition]:
        """Returns capabilities of geometry sidecar fields."""
        alias = sidecar_alias or f"sc_{self.sidecar_id}"
        fields = {}

        # Geometry field - allow all spatial aggregations and transformations by default
        fields["geom"] = FieldDefinition(
            name="geom",
            alias="geometry",  # Standard external name
            title=LocalizedText(en="Geometry", fr="Géométrie", es="Geometría"),
            description=LocalizedText(
                en="The primary spatial geometry of the feature.",
                fr="La géométrie spatiale primaire de l'entité.",
                es="La geometría espacial primaria de la entidad.",
            ),
            sql_expression=f"{alias}.{self.config.geom_column}",
            capabilities=[FieldCapability.FILTERABLE, FieldCapability.SPATIAL],
            data_type="geometry",
            aggregations=None,  # All spatial aggregations allowed (ST_Union, ST_Collect, etc.)
            transformations=None,  # All spatial transformations allowed (ST_AsGeoJSON, ST_Buffer, etc.)
            expose=True,
        )

        if self.config.bbox_column:
            fields["bbox"] = FieldDefinition(
                name="bbox",
                alias="bbox",
                title=LocalizedText(
                    en="Bounding Box", fr="Boîte Englobante", es="Caja de Delimitación"
                ),
                description=LocalizedText(
                    en="The spatial extent (bounding box) of the feature.",
                    fr="L'étendue spatiale (boîte englobante) de l'entité.",
                    es="La extensión espacial (caja de delimitación) de la entidad.",
                ),
                sql_expression=f"{alias}.{self.config.bbox_column}",
                capabilities=[FieldCapability.FILTERABLE, FieldCapability.SPATIAL],
                data_type="geometry",
                aggregations=None,
                transformations=None,
                expose=True,
            )

        # H3 indexes
        for res in self.config.h3_resolutions:
            fields[f"h3_res{res}"] = FieldDefinition(
                name=f"h3_res{res}",
                sql_expression=f"{alias}.h3_res{res}",
                capabilities=[
                    FieldCapability.FILTERABLE,
                    FieldCapability.SORTABLE,
                    FieldCapability.GROUPABLE,
                    FieldCapability.INDEXED,
                ],
                data_type="bigint",
                aggregations=["count", "array_agg"],
                transformations=[],
                expose=True,
            )

        # S2 indexes
        for res in self.config.s2_resolutions:
            fields[f"s2_res{res}"] = FieldDefinition(
                name=f"s2_res{res}",
                sql_expression=f"{alias}.s2_res{res}",
                capabilities=[
                    FieldCapability.FILTERABLE,
                    FieldCapability.SORTABLE,
                    FieldCapability.GROUPABLE,
                    FieldCapability.INDEXED,
                ],
                data_type="bigint",
                aggregations=["count", "array_agg"],
                transformations=[],
                expose=True,
            )

        # Statistics — overlay-driven. COLUMNAR fields expose as queryable
        # FieldDefinitions; JSONB fields land inside ``geom_stats`` and
        # are not directly queryable through this surface (operators can
        # still filter via JSONB path expressions outside the
        # FieldDefinition surface).
        for f in self._columnar_fields():
            key = f.resolved_name
            if f.kind == ComputedKind.CENTROID:
                fields[key] = FieldDefinition(
                    name=key,
                    sql_expression=f"{alias}.{key}",
                    capabilities=[
                        FieldCapability.FILTERABLE,
                        FieldCapability.SPATIAL,
                    ],
                    data_type="geometry",
                    aggregations=None,
                    transformations=None,
                )
            else:
                fields[key] = FieldDefinition(
                    name=key,
                    sql_expression=f"{alias}.{key}",
                    capabilities=[
                        FieldCapability.FILTERABLE,
                        FieldCapability.SORTABLE,
                        FieldCapability.AGGREGATABLE,
                    ],
                    data_type=self._field_data_type(f),
                    aggregations=["sum", "avg", "min", "max", "count"],
                )

        # Virtual Bbox components for STAC - use stored bbox column when available
        bbox_source = (
            f"{alias}.{self.config.bbox_column}" if self.config.write_bbox else f"{alias}.geom"
        )

        fields["bbox_xmin"] = FieldDefinition(
            name="bbox_xmin",
            sql_expression=f"ST_XMin({bbox_source})",
            capabilities=[FieldCapability.FILTERABLE, FieldCapability.SORTABLE],
            data_type="numeric",
        )
        fields["bbox_ymin"] = FieldDefinition(
            name="bbox_ymin",
            sql_expression=f"ST_YMin({bbox_source})",
            capabilities=[FieldCapability.FILTERABLE, FieldCapability.SORTABLE],
            data_type="numeric",
        )
        fields["bbox_xmax"] = FieldDefinition(
            name="bbox_xmax",
            sql_expression=f"ST_XMax({bbox_source})",
            capabilities=[FieldCapability.FILTERABLE, FieldCapability.SORTABLE],
            data_type="numeric",
        )
        fields["bbox_ymax"] = FieldDefinition(
            name="bbox_ymax",
            sql_expression=f"ST_YMax({bbox_source})",
            capabilities=[FieldCapability.FILTERABLE, FieldCapability.SORTABLE],
            data_type="numeric",
        )

        # Hub fields
        fields["transaction_time"] = FieldDefinition(
            name="transaction_time",
            title=LocalizedText(
                en="Transaction Time",
                fr="Temps de Transaction",
                es="Tiempo de Transacción",
            ),
            description=LocalizedText(
                en="The time when this record was recorded in the database.",
                fr="Le moment où cet enregistrement a été consigné dans la base de données.",
                es="El momento en que este registro fue grabado en la base de datos.",
            ),
            sql_expression="h.transaction_time",
            capabilities=[FieldCapability.FILTERABLE, FieldCapability.SORTABLE],
            data_type="timestamptz",
        )

        return fields

    def prepare_geometry_for_upsert(
        self,
        geometry: Any,
        source_srid: int = 4326,
        write_behavior: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Public utility to prepare a geometry for storage.
        Handles SRID transforms, validation, and simplification.

        ``write_behavior`` is the :class:`GeometriesWriteBehavior` block
        from :class:`ItemsWritePolicy`. When None, ``process_geometry``
        falls back to documented defaults (TRANSFORM / ATTEMPT_FIX / no
        simplification / no allow-list).
        """
        try:
            # Convert GeoJSON/Mappings/Shapely to WKB Hex string if needed
            geom_wkb_hex = None

            # Handle Shapely objects
            if hasattr(geometry, "wkb"):
                geom_wkb_hex = geometry.wkb.hex()
            # Handle Dicts / Mappings (GeoJSON, Fiona models)
            elif hasattr(geometry, "__getitem__") and hasattr(geometry, "get"):
                # It's dictionary-like (dict, fiona.model.Geometry, etc.)
                from shapely.geometry import shape

                # Ensure it's a dict for shapely if it's a custom mapping
                if not isinstance(geometry, dict):
                    geometry = dict(geometry)
                geom_wkb_hex = shape(geometry).wkb.hex()
            # Handle Hex String (assume valid if string)
            elif isinstance(geometry, str):
                geom_wkb_hex = geometry
            # Handle Bytes
            elif isinstance(geometry, (bytes, bytearray)):
                geom_wkb_hex = geometry.hex()
            else:
                raise ValueError(f"Unsupported geometry format: {type(geometry)}")

            # Process geometry (validation, fixing, transformation)
            from dynastore.tools.geospatial import process_geometry

            return process_geometry(
                geom_wkb_hex,
                self.config,
                source_srid=source_srid,
                write_behavior=write_behavior,
            )
        except Exception as e:
            raise ValueError(f"Geometry preparation failed: {e}")

    def _get_val(self, obj, key, default=None):
        if isinstance(obj, dict):
            return obj.get(key, default)
        return getattr(obj, key, default)

    def prepare_upsert_payload(
        self, feature: Union[Feature, Dict[str, Any]], context: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Extract and process geometry from raw feature.
        Delegates core logic to prepare_geometry_for_upsert.
        """
        geoid = context.get("geoid")
        if not geoid:
            raise ValueError("geoid is required in context")

        payload = {"geoid": geoid}

        # Add partition key if present
        partition_key_value = context.get("partition_key_value")
        partition_key_name = context.get("partition_key_name")
        if partition_key_value and partition_key_name:
            payload[partition_key_name] = partition_key_value

        # Check for pre-processed geometry (from Ingestion)
        wkb_hex = self._get_val(feature, "wkb_hex_processed")

        geom_data = {}
        shapely_geom = None

        if wkb_hex:
            # Trusted pre-processed path
            feature_srid = self._get_val(feature, "srid", 4326)
            if (
                self.config.target_srid != 4326
                and feature_srid != self.config.target_srid
            ):
                # Ingestion usually targets 3857 or configured SRID.
                # If mismatch, we might need to re-process, but for now assume ingestion did it right.
                pass

            geom_data = {
                "wkb_hex_processed": wkb_hex,
                "geom_type": self._get_val(feature, "geom_type", "UNKNOWN"),
                "bbox_coords": self._get_val(feature, "bbox_coords"),
            }
            # If any storage-bearing computed field is configured we must
            # re-parse the WKB to a Shapely geometry so the runner can
            # derive the values.
            if self._storage_fields():
                from shapely import wkb
                import binascii

                try:
                    shapely_geom = wkb.loads(binascii.unhexlify(wkb_hex))
                    geom_data["shapely_geom"] = shapely_geom
                except Exception:
                    # Fallback or ignore stats
                    pass

        else:
            # Standard processing path
            if isinstance(feature, Feature):
                geometry = feature.geometry.model_dump() if feature.geometry else None
                source_srid = feature.properties.get("srid", 4326) if feature.properties else 4326
            else:
                geometry = feature.get("geometry") or feature.get("geom")
                source_srid = feature.get("srid", 4326)

            if not geometry:
                raise ValueError("Feature must have a geometry")

            logger.debug(f"GeometrySidecar Input Geom: {geometry}")
            policy = context.get("_items_write_policy")
            write_behavior = getattr(policy, "geometries", None) if policy else None
            geom_data = self.prepare_geometry_for_upsert(
                geometry, source_srid=source_srid, write_behavior=write_behavior,
            )
            logger.debug(
                f"GeometrySidecar Processed WKB: {geom_data['wkb_hex_processed']}"
            )
            shapely_geom = geom_data.get("shapely_geom")

        # Compute geometry_hash on the incoming WKB so the
        # GEOMETRY_HASH identity matcher can look up an existing row
        # *before* this write commits.  The PG-generated column on the
        # sidecar (issue #220) handles the stored value; this Python-
        # side hash is for matcher lookup only and uses the same input
        # (final WKB → SHA256) so the two values are guaranteed to
        # match for the same processed geometry.
        wkb_hex_final = geom_data.get("wkb_hex_processed")
        if wkb_hex_final:
            context["geometry_hash"] = hashlib.sha256(
                bytes.fromhex(wkb_hex_final)
            ).hexdigest()

        # Store processed geometry
        payload[self.config.geom_column] = geom_data["wkb_hex_processed"]
        payload["geom_type"] = geom_data["geom_type"]

        # Calculate bbox if configured
        if self.config.bbox_column:
            if geom_data.get("bbox_coords"):
                from shapely.geometry import box
                from shapely import wkb
                payload[self.config.bbox_column] = wkb.dumps(
                    box(*geom_data["bbox_coords"]), hex=True
                )
            elif wkb_hex:
                bbox_val = self._get_val(feature, self.config.bbox_column)
                if bbox_val:  # Optimization: if passed directly
                    payload[self.config.bbox_column] = bbox_val  # Expecting WKB Hex

        # Calculate spatial indices
        # Check if already in feature (ingestion)
        indices_found = False

        # Add H3 indexes
        for res in self.config.h3_resolutions:
            key = f"h3_res{res}"
            val = self._get_val(feature, key)
            if val is not None:
                payload[key] = val
                indices_found = True
            elif shapely_geom:
                # Calc if not found
                pass  # Will be handled below

        # Add S2 indexes
        for res in self.config.s2_resolutions:
            key = f"s2_res{res}"
            val = self._get_val(feature, key)
            if val is not None:
                payload[key] = val
                indices_found = True

        if not indices_found and shapely_geom:
            from dynastore.tools.geospatial import calculate_spatial_indices

            indices = calculate_spatial_indices(
                shapely_geom, self.config.h3_resolutions, self.config.s2_resolutions
            )

            # Add H3 indexes
            for res in self.config.h3_resolutions:
                key = f"h3_res{res}"
                if key in indices:
                    payload[key] = indices[key]

            # Add S2 indexes
            for res in self.config.s2_resolutions:
                key = f"s2_res{res}"
                if key in indices:
                    payload[key] = indices[key]

            # Add S2 indexes
            for res in self.config.s2_resolutions:
                key = f"s2_res{res}"
                if key in indices:
                    payload[key] = indices[key]

        # 3. Calculate storage-bearing Computed Fields
        storage_fields = self._storage_fields()
        if storage_fields and shapely_geom:
            # ``compute_derived_fields`` handles centroid WKB conversion
            # internally when ``centroid_type`` is set on the field.
            props = {}
            if isinstance(feature, Feature):
                props = feature.properties or {}
            elif isinstance(feature, dict):
                props = feature.get("properties") or {}
            from dynastore.tools.geospatial import compute_derived_fields

            derived = compute_derived_fields(shapely_geom, props, storage_fields)
            jsonb_payload: Dict[str, Any] = {}
            for f in storage_fields:
                key = f.resolved_name
                if key not in derived:
                    continue
                if f.storage_mode == StatisticStorageMode.JSONB:
                    jsonb_payload[key] = derived[key]
                else:
                    payload[key] = derived[key]
            if jsonb_payload:
                payload["geom_stats"] = jsonb_payload
        elif storage_fields and not shapely_geom:
            # Fail-fast: if stats are declared we MUST have a valid geometry.
            raise ValueError(
                f"GeometrySidecar: storage-bearing computed fields declared but "
                f"no valid geometry provided for geoid {geoid}. "
                "Discarding feature to maintain data integrity."
            )
        return payload

    def prepare_place_upsert_payload(
        self,
        feature: Union[Feature, Dict[str, Any]],
        context: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Extract the JSON-FG 'place' member and compute declared 3D statistics.

        Returns a payload dict for the ``{table}_place`` sidecar table, or
        ``None`` when no 3D place-stat fields are declared in the overlay or
        the feature has no ``place`` member.
        """
        place_fields = [
            f for f in self._storage_fields() if f.kind in _PLACE_TABLE_KINDS
        ]
        if not place_fields:
            return None

        geoid = context.get("geoid")
        if not geoid:
            raise ValueError("geoid is required in context")

        # Extract place raw value and coordRefSys
        if isinstance(feature, dict):
            place_raw = feature.get("place")
            coordref = feature.get("coordRefSys")
            props = feature.get("properties") or {}
            validity = props.get("validity") if isinstance(props, dict) else None
        else:
            place_raw = getattr(feature, "place", None)
            if place_raw is None and hasattr(feature, "model_extra") and feature.model_extra:
                place_raw = feature.model_extra.get("place")
            coordref = getattr(feature, "coordRefSys", None)
            if coordref is None and hasattr(feature, "model_extra") and feature.model_extra:
                coordref = feature.model_extra.get("coordRefSys")
            raw_props = feature.properties if hasattr(feature, "properties") else {}
            validity = (raw_props or {}).get("validity") if isinstance(raw_props, dict) else None

        if not place_raw:
            return None

        # Normalise to dict
        if hasattr(place_raw, "model_dump"):
            place_dict = place_raw.model_dump()
        elif isinstance(place_raw, dict):
            place_dict = place_raw
        else:
            import json as _json
            try:
                place_dict = _json.loads(place_raw) if isinstance(place_raw, str) else dict(place_raw)  # type: ignore[call-overload]
            except Exception:
                logger.warning(f"Could not parse place geometry for geoid={geoid}: {type(place_raw)}")
                return None

        payload: Dict[str, Any] = {
            "geoid": geoid,
            "place": place_dict,
        }
        if coordref is not None:
            payload["coordRefSys"] = str(coordref)

        # Compute declared statistics via the unified handler
        from dynastore.tools.geospatial import compute_place_derived_fields
        place_stats = compute_place_derived_fields(place_dict, place_fields, validity=validity)

        if place_stats:
            has_jsonb = any(
                f.storage_mode == StatisticStorageMode.JSONB for f in place_fields
            )
            if has_jsonb:
                payload["place_stats"] = place_stats
            else:
                for k, v in place_stats.items():
                    payload[f"place_{k}"] = v

        return payload

    def map_row_to_feature(
        self,
        row: Dict[str, Any],
        feature: Feature,
        context: FeaturePipelineContext,
    ) -> None:
        """Populate Feature geometry from database row."""
        # Publish all raw row values for downstream sidecars (e.g. STAC for bbox).
        context.publish(self.sidecar_id, {k: row[k] for k in self.get_internal_columns() if k in row})

        # 1. Geometry Mapping
        if "geom" in row:
            geom_data = row["geom"]
            from pydantic import TypeAdapter

            if isinstance(geom_data, dict):
                feature.geometry = TypeAdapter(Geometry).validate_python(geom_data)
            elif isinstance(geom_data, str):
                try:
                    # Try GeoJSON string
                    feature.geometry = TypeAdapter(Geometry).validate_json(geom_data)
                except Exception:
                    # Fallback to WKB hex
                    try:
                        import shapely.wkb
                        from shapely.geometry import mapping

                        geom = shapely.wkb.loads(bytes.fromhex(geom_data))
                        feature.geometry = TypeAdapter(Geometry).validate_python(
                            mapping(geom)
                        )
                    except Exception as e:
                        logger.warning(f"Failed to parse geometry string: {e}")
            elif isinstance(geom_data, (bytes, bytearray)):
                # WKB bytes (geom_format="WKB" via raw_params)
                try:
                    import shapely.wkb
                    from shapely.geometry import mapping

                    geom = shapely.wkb.loads(bytes(geom_data))
                    feature.geometry = TypeAdapter(Geometry).validate_python(
                        mapping(geom)
                    )
                except Exception as e:
                    logger.warning(f"Failed to parse geometry bytes: {e}")
            elif hasattr(geom_data, "__geo_interface__") or hasattr(geom_data, "geom_type"):
                # Shapely geometry object
                try:
                    from shapely.geometry import mapping

                    feature.geometry = TypeAdapter(Geometry).validate_python(
                        mapping(geom_data)
                    )
                except Exception as e:
                    logger.warning(f"Failed to convert Shapely geometry: {e}")

        # 2. BBox Mapping
        if self.config.write_bbox and "bbox_geom" in row:
            # bbox_geom is a PostGIS geometry (box), we might want to convert it to a bbox array [minx, miny, maxx, maxy]
            # but usually map_row_to_geojson handles list extraction if it's already a list.
            # If it's a geom, we might need a utility to convert it.
            pass

        # 3. Spatial Metadata (Hidden from properties by default unless configured)
        # These are usually internal but can be exposed if needed.
        if feature.properties is None:
            feature.properties = {}

        # 4. Statistics are projected via ``get_select_fields`` and consumed
        # by the read API as queryable fields (COLUMNAR) or remain inside
        # the ``geom_stats`` JSONB column. They are NOT auto-merged into
        # ``properties``: a collection opts each computed value into the wire
        # output via ``ItemsReadPolicy.feature_type.expose``. The exposure loop
        # runs once at the pipeline level
        # (``SidecarProtocol.apply_exposed_computed_values``); this sidecar only
        # declares which names it owns and how to read them — see
        # ``producible_computed_names`` / ``resolve_computed_value`` below.

    def producible_computed_names(self) -> set:
        """Computed-field resolved names this sidecar can surface at read.

        These are the storage-bearing statistics projected by
        ``get_select_fields`` (COLUMNAR columns, ``geom_stats`` / ``place_stats``
        JSONB entries). The pipeline-level exposure loop
        (``SidecarProtocol.apply_exposed_computed_values``) intersects this with
        ``ItemsReadPolicy.feature_type.expose``.
        """
        return {f.resolved_name for f in self._storage_fields()}

    def resolve_computed_value(
        self, row: Dict[str, Any], resolved_name: str
    ) -> Tuple[bool, Any]:
        """Locate a storage-bearing computed value in a read row.

        Returns ``(found, value)``. Mirrors the storage layout decided by
        ``get_select_fields``: COLUMNAR fields surface as a top-level row key;
        JSONB fields live inside the shared ``geom_stats`` column; 3D place
        statistics live in their own ``{table}_place`` projection (columnar
        ``place_{name}`` keys or a ``place_stats`` JSONB blob). ``found`` is
        ``False`` only when the layout has no slot for the value at all.
        """
        field = next(
            (f for f in self._storage_fields() if f.resolved_name == resolved_name),
            None,
        )
        if field is None:
            return (False, None)

        is_place = field.kind in _PLACE_TABLE_KINDS
        if field.storage_mode == StatisticStorageMode.COLUMNAR:
            key = f"place_{resolved_name}" if is_place else resolved_name
            if key in row:
                return (True, row[key])
            return (False, None)

        # JSONB: value nests inside the shared stats blob for this sidecar.
        blob_key = "place_stats" if is_place else "geom_stats"
        blob = row.get(blob_key)
        if isinstance(blob, (str, bytes, bytearray)):
            import json as _json

            try:
                blob = _json.loads(blob)
            except Exception:
                return (False, None)
        if isinstance(blob, dict) and resolved_name in blob:
            return (True, blob[resolved_name])
        return (False, None)

    async def expire_version(
        self,
        conn: DbResource,
        physical_schema: str,
        physical_table: str,
        geoid: str,
        expire_at: datetime,
    ) -> int:
        """Geometry sidecar usually doesn't manage temporal validity, so this is a no-op."""
        return 0

    def apply_query_context(
        self,
        request: QueryRequest,
        context: Dict[str, Any],
    ) -> None:
        """
        Contribute spatial filters or JOINs based on QueryRequest.
        """
        alias = f"sc_{self.sidecar_id}"
        
        # Use get_select_fields to populate context SELECTs
        fields_to_add = self.get_select_fields(request=request, sidecar_alias=alias)
        
        for expr in fields_to_add:
            if expr not in context.get("select_fields", []):
                context.setdefault("select_fields", []).append(expr)
        
        # Add spatial conditions if present in request (handled by Optimizer, but sidecar can contribute)
        # For now, ItemService handles bbox/geometry filters, but we ensure SELECTs are present.

    def get_identity_columns(self) -> List[str]:
        """Returns identity columns for this sidecar (only geoid)."""
        return ["geoid"]

    async def resolve_existing_item(
        self,
        conn,
        physical_schema: str,
        physical_table: str,
        processing_context: Dict[str, Any],
        matcher: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Resolve by GEOHASH matcher.

        Uses the stored generated column ``geohash`` on the geometry sidecar
        (populated by ``ST_GeoHash(geom, precision)``) to find a prior feature
        co-located at the configured precision.  Returns the active (non-
        soft-deleted) hub row with its ``geometry_hash`` so the caller can
        hash-gate NEW_VERSION / UPDATE decisions.

        Returns ``None`` when:
          - matcher is not GEOHASH (owned by another sidecar)
          - ``geohash_precision`` is not configured on this sidecar
          - the incoming context lacks a usable geometry
          - no co-located record exists
        """
        if matcher != "geohash":
            return None
        if not self.config.geohash_precision:
            return None

        wkb_hex = processing_context.get("wkb_hex_processed")
        if not wkb_hex:
            return None

        sc_table = f"{physical_table}_{self.sidecar_id}"
        # geometry_hash also lives on this sidecar (issue #220) — same
        # JOIN, just project the column from ``s`` instead of ``h``.
        sql = f"""
            SELECT h.geoid, s.geometry_hash
            FROM "{physical_schema}"."{physical_table}" h
            JOIN "{physical_schema}"."{sc_table}" s ON s.geoid = h.geoid
            WHERE h.deleted_at IS NULL
              AND s.geohash = ST_GeoHash(
                  ST_GeomFromEWKB(decode(:wkb, 'hex')),
                  :prec
              )
            ORDER BY h.transaction_time DESC
            LIMIT 1;
        """
        row = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn, wkb=wkb_hex, prec=self.config.geohash_precision
        )
        return row or None

    def get_internal_columns(self) -> set:
        """
        Return the set of DB column names this sidecar manages that are
        never part of the public Feature output properties.

        ``geohash`` is the Postgres-generated ST_GeoHash column used by
        the GEOHASH identity matcher (see ItemsWritePolicy); it is
        queryable at the SQL layer for write policies but is hidden from
        API consumers by default to avoid leaking storage plumbing into
        Feature.properties.
        """
        cols = {"geom", "bbox_geom", "geohash"}
        if self._has_jsonb_stats():
            cols.add("geom_stats")
        for f in self._columnar_fields():
            if f.kind == ComputedKind.CENTROID:
                cols.add(f.resolved_name)

        place_fields = [
            f for f in self._storage_fields() if f.kind in _PLACE_TABLE_KINDS
        ]
        if place_fields:
            cols.add("place")
            cols.add("coordRefSys")
            if any(f.storage_mode == StatisticStorageMode.JSONB for f in place_fields):
                cols.add("place_stats")

        return cols

    def finalize_upsert_payload(
        self,
        sc_payload: Dict[str, Any],
        hub_row: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Finalizes geometry payload (pass-through)."""
        return sc_payload
