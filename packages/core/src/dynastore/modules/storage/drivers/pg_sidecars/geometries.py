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
Geometries Sidecar Implementation.

This module provides the GeometriesSidecar which manages:
- Primary geometry storage (geom)
- Bounding box storage (bbox_geom)
- Spatial indexing (H3, S2)
- Geometry validation and fixing logic
"""

import hashlib
import json
import logging
from datetime import datetime
from typing import ClassVar, Dict, Any, List, Optional, Tuple, Union
from geojson_pydantic import Feature
from geojson_pydantic.geometries import Geometry
from dynastore.models.query_builder import QueryRequest

from dynastore.modules.storage.drivers.pg_sidecars.base import (
    SidecarProtocol,
    FeaturePipelineContext,
    FieldDefinition,
    FieldCapability,
)
from dynastore.modules.catalog.models import LocalizedText
from dynastore.tools.json import CustomJSONEncoder
from dynastore.modules.storage.computed_fields import (
    ComputedField,
    ComputedKind,
    StatisticStorageMode,
    _PLACE_TABLE_KINDS,
)
from dynastore.modules.storage.drivers.pg_sidecars.naming import sidecar_table_name
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesSidecarConfig,
    TargetDimension,
    SimplificationAlgorithm,
)
from dynastore.modules.db_config.query_executor import DbResource, DQLQuery, ResultHandler
from dynastore.tools.db import quote_ident as _canonical_quote_ident
from dynastore.tools.geospatial_exceptions import SridMismatchError

logger = logging.getLogger(__name__)


# ============================================================================
# ENUMS & CONSTANTS
# ============================================================================

# Maps each ``SimplificationAlgorithm`` *value* to its PostGIS function. The tile
# pipeline passes the bare enum value (e.g. ``topology_preserving``), which is NOT
# a SQL identifier — emitting it verbatim raises ``function topology_preserving(
# geometry, unknown) does not exist``. Only names in this table are ever
# interpolated into SQL, so the algorithm string can never inject arbitrary calls.
_SIMPLIFY_SQL_FUNCTIONS: Dict[str, str] = {
    SimplificationAlgorithm.DOUGLAS_PEUCKER.value: "ST_Simplify",
    SimplificationAlgorithm.TOPOLOGY_PRESERVING.value: "ST_SimplifyPreserveTopology",
    SimplificationAlgorithm.VISVALINGAM_WHYATT.value: "ST_SimplifyVW",
    # ST_SnapToGrid(geom, gridsize): O(n) coordinate-snap.  The gridsize maps
    # directly to the ``simplification`` tolerance (source-CRS units), which the
    # per-zoom defaults calibrate to ~half a MVT pixel width — precisely the
    # right grid resolution.  Fastest pre-pass before ST_AsMVTGeom because it
    # requires no recursive distance comparisons; sub-pixel rings collapse to
    # degenerate geometry that ST_AsMVTGeom silently drops.
    SimplificationAlgorithm.SNAP_TO_GRID.value: "ST_SnapToGrid",
}
# Default when no algorithm key matches.  The accurate topology-preserving function
# is the safe fallback: it never produces self-intersecting rings and matches the
# default TilesConfig.simplification_algorithm (TOPOLOGY_PRESERVING).
# ST_SnapToGrid is available as an explicit opt-in via the SNAP_TO_GRID enum value.
_DEFAULT_SIMPLIFY_SQL_FUNCTION = "ST_SimplifyPreserveTopology"


def _quote_ident(name: str) -> str:
    """Double-quote a SQL identifier so Postgres preserves its exact case.

    Unquoted identifiers are folded to lowercase by Postgres, which breaks
    consumers that reference the column by its original mixed/upper-case name.
    Idempotent: an already-quoted name is returned unchanged. See #719.

    Thin wrapper around the canonical ``dynastore.tools.db.quote_ident``
    (#2700) — kept as a local alias so call sites in this module don't churn.
    """
    return _canonical_quote_ident(name.strip())


def _derive_bbox_from_shapely(geom: Any) -> Optional[Tuple[float, float, float, float]]:
    """Derive a (minx, miny, maxx, maxy) bbox tuple from a shapely geometry.

    Returns ``None`` when ``geom`` is ``None``, empty, or raises any error.
    Never raises — degrade-safe for write-path use.
    """
    if geom is None:
        return None
    try:
        bounds = geom.bounds
        # bounds returns (minx, miny, maxx, maxy); empty geometries yield
        # (inf, inf, -inf, -inf) which is not a valid bbox.
        if len(bounds) != 4:
            return None
        minx, miny, maxx, maxy = bounds
        import math
        if any(math.isinf(v) or math.isnan(v) for v in bounds):
            return None
        return (minx, miny, maxx, maxy)
    except Exception:
        return None


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
    _COLUMNAR_SQL_TYPE: ClassVar[Dict[ComputedKind, str]] = {
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
        # Duration is materialised as a numeric count of seconds
        # (``_place_temporal_duration`` returns a float, and the cross-driver
        # ``field_projection`` declares it ``double``), so the column must be
        # numeric — an ``INTERVAL`` column rejects the numeric value.
        ComputedKind.TEMPORAL_DURATION: "DOUBLE PRECISION",
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
            return "jsonb"  # bbox is an array of coordinates; no canonical array type
        if field.kind in self._INTEGER_KINDS:
            return "integer"
        return "numeric"

    def _jsonb_cast_for(self, field: ComputedField) -> str:
        if field.kind in self._INTEGER_KINDS:
            return "integer"
        return "numeric"

    def _centroid_select_field(self, field: ComputedField, alias: str) -> str:
        """SELECT projection for a COLUMNAR ``centroid`` read.

        The centroid is stored in a ``GEOMETRY(POINT[Z], srid)`` column, but
        for output it must surface as a clean coordinate, not raw EWKB. We
        project a native ``double precision[]`` (``ARRAY[ST_X, ST_Y]``); asyncpg
        decodes it to a Python ``[x, y]`` list, so the exposed value matches the
        JSONB-stored centroid (``tools/geospatial`` emits ``[x, y]``) and is
        independent of the storage mode — toggling ``store: columnar`` <->
        ``store: jsonb`` no longer changes the property shape. The raw geometry
        column is still referenced unchanged for spatial filtering via the
        FieldDefinition ``sql_expression``, so only the read-for-output value
        changes here. NULL centroids stay NULL rather than ``[null, null]``.
        """
        col = f"{alias}.{field.resolved_name}"
        coords = f"ST_X({col}), ST_Y({col})"
        if (field.centroid_type or "POINT") == "POINTZ":
            coords = f"{coords}, ST_Z({col})"
        return (
            f"CASE WHEN {col} IS NULL THEN NULL "
            f"ELSE ARRAY[{coords}] END as {_quote_ident(field.resolved_name)}"
        )

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

        Skipped for RECORDS collections which have no spatial component,
        unless the collection's ``CollectionInfo.allow_geometry`` capability
        override (RFC #2550) forces the decision either way:

        - ``allow_geometry is True`` → inject regardless of kind (e.g. a
          RECORDS collection carrying a real footprint geometry).
        - ``allow_geometry is False`` → never inject, regardless of kind.
        - ``allow_geometry is None`` (default/unset) → fall back to the
          kind-derived default below, unchanged from before.
        """
        allow_geometry = context.get("allow_geometry")
        if allow_geometry is True:
            return GeometriesSidecarConfig()
        if allow_geometry is False:
            return None
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

        Single source of truth for geometry field definitions.
        """
        # Alias must match builder convention
        alias = f"sc_{self.sidecar_id}"

        fields: Dict[str, FieldDefinition] = {}

        # Main geometry keyed by config column name (e.g. "geom") — simple entry
        # used by the optimizer for quick capability checks.
        fields[self.config.geom_column] = FieldDefinition(
            name=self.config.geom_column,
            sql_expression=f"{alias}.{self.config.geom_column}",
            capabilities=[FieldCapability.FILTERABLE, FieldCapability.SPATIAL],
            data_type="geometry",
            expose=True,
            title="Geometry",
            description="Primary geometry",
        )

        # Canonical "geom" key with full localisation — callers that key on
        # "geom" (e.g. validation) use this entry.
        if self.config.geom_column != "geom":
            fields["geom"] = FieldDefinition(
                name="geom",
                alias="geometry",
                title=LocalizedText(en="Geometry", fr="Géométrie", es="Geometría"),
                description=LocalizedText(
                    en="The primary spatial geometry of the feature.",
                    fr="La géométrie spatiale primaire de l'entité.",
                    es="La geometría espacial primaria de la entidad.",
                ),
                sql_expression=f"{alias}.{self.config.geom_column}",
                capabilities=[FieldCapability.FILTERABLE, FieldCapability.SPATIAL],
                data_type="geometry",
                aggregations=None,
                transformations=None,
                expose=True,
            )
        else:
            # geom_column IS "geom" — enrich the entry already added above.
            fields["geom"] = FieldDefinition(
                name="geom",
                alias="geometry",
                title=LocalizedText(en="Geometry", fr="Géométrie", es="Geometría"),
                description=LocalizedText(
                    en="The primary spatial geometry of the feature.",
                    fr="La géométrie spatiale primaire de l'entité.",
                    es="La geometría espacial primaria de la entidad.",
                ),
                sql_expression=f"{alias}.{self.config.geom_column}",
                capabilities=[FieldCapability.FILTERABLE, FieldCapability.SPATIAL],
                data_type="geometry",
                aggregations=None,
                transformations=None,
                expose=True,
            )

        # Bounding box
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

        # COLUMNAR stat fields — overlay-driven
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

        # Virtual bbox components for STAC — use stored bbox when available
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

        # Hub temporal field — always present; expression references hub alias "h"
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
            data_type="timestamp",
        )

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
        partition_keys: Optional[List[str]] = None,
        partition_key_types: Optional[Dict[str, str]] = None,
        has_validity: bool = False,
    ) -> str:
        """Generate DDL for geometry sidecar table."""
        partition_keys = partition_keys or []
        partition_key_types = partition_key_types or {}
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
        table_name = sidecar_table_name(physical_table, self.sidecar_id)

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

        return None

    def get_select_fields(
        self,
        request: Optional[QueryRequest] = None,
        sidecar_alias: Optional[str] = None,
        include_all: bool = False,
    ) -> List[str]:
        """Returns SELECT field expressions for geometry sidecar.

        Every emitted column carries an explicit ``as {name}`` alias — geometry,
        bbox, H3/S2 index columns, centroid, COLUMNAR statistics, and the JSONB
        ``geom_stats`` blob. A single canonical output name per field means that
        when the optimizer also projects the same field through an explicit
        ``FieldSelection`` (rendered ``{alias}.{name} as {name}``) the two paths
        emit identical strings and collapse to one column. Emitting a bare
        ``{alias}.{name}`` here instead would survive the optimizer's
        output-name dedup as a second column with the same name and trigger
        PostgreSQL ``column reference "{name}" is ambiguous`` on the wrapping
        MVT SELECT (the original ``area`` tile failure).
        """
        alias = sidecar_alias or f"sc_{self.sidecar_id}"

        # OGC API Features ``skipGeometry=true`` (de-facto pygeoapi convention)
        # asks the driver to omit Feature.geometry from the response. The
        # column is geometry-sidecar territory, so we suppress it here at the
        # SELECT level rather than projecting it away post-fetch. Bbox / H3 /
        # S2 / statistics columns are unaffected — they are distinct
        # OGC outputs that survive a geometry-skip.
        skip_geom = bool(request is not None and getattr(request, "skip_geometry", False))

        fields = []

        if request is not None and not include_all:
            # Selective mode
            requested = {sel.field for sel in request.select} if hasattr(request, "select") and request.select else set()
            filter_fields = {f.field for f in request.filters} if hasattr(request, "filters") and request.filters else set()
            sort_fields = {s.field for s in (request.sort or [])} if hasattr(request, "sort") and request.sort else set()
            all_needed = requested | filter_fields | sort_fields

            # #2829: a GROUP BY request must not pull in geometry/bbox/H3/S2/
            # statistics columns that are neither grouped nor aggregated —
            # PostgreSQL rejects any SELECT-list column that isn't. When
            # ``group_by`` is set, ``_include`` narrows inclusion to exactly
            # those field names and ignores ``default`` (the ordinary
            # per-field requested/filter/sort/wildcard/empty-select rule
            # below, preserved as-is for the non-grouped case).
            group_by_fields = set(request.group_by) if request.group_by else None

            def _include(*names: str, default: bool) -> bool:
                if group_by_fields is not None:
                    return any(n in group_by_fields for n in names)
                return default

            if not skip_geom and _include(
                self.config.geom_column,
                "geometry",
                default=(
                    self.config.geom_column in all_needed
                    or "geometry" in all_needed
                    or "*" in requested
                    or not requested
                ),
            ):
                fields.append(f"ST_AsGeoJSON({alias}.{self.config.geom_column})::jsonb as {self.config.geom_column}")

            if self.config.bbox_column and _include(
                "bbox",
                self.config.bbox_column,
                default=(
                    "bbox" in all_needed
                    or self.config.bbox_column in all_needed
                    or "*" in requested
                ),
            ):
                fields.append(f"ST_AsGeoJSON({alias}.{self.config.bbox_column})::jsonb as {self.config.bbox_column}")

            if self.config.h3_resolutions:
                for res in self.config.h3_resolutions:
                    if _include(
                        f"h3_res{res}",
                        default=(f"h3_res{res}" in all_needed or "*" in requested),
                    ):
                        fields.append(f"{alias}.h3_res{res} as h3_res{res}")

            if self.config.s2_resolutions:
                for res in self.config.s2_resolutions:
                    if _include(
                        f"s2_res{res}",
                        default=(f"s2_res{res}" in all_needed or "*" in requested),
                    ):
                        fields.append(f"{alias}.s2_res{res} as s2_res{res}")

            # Add Statistics if requested — overlay-driven, COLUMNAR fields
            # surface per resolved name, JSONB stats project the shared
            # ``geom_stats`` column.
            for f in self._columnar_fields():
                key = f.resolved_name
                if not _include(
                    key, default=(key in all_needed or "*" in requested)
                ):
                    continue
                if f.kind == ComputedKind.CENTROID:
                    fields.append(self._centroid_select_field(f, alias))
                else:
                    fields.append(f"{alias}.{key} as {_quote_ident(key)}")
            if self._has_jsonb_stats() and _include(
                "geom_stats",
                default=("geom_stats" in all_needed or "*" in requested),
            ):
                fields.append(f"{alias}.geom_stats as geom_stats")

        else:
            # Full mode (existing behavior)
            if request is not None and not skip_geom:
                # Default to GeoJSON for optimization/consistency in API responses
                fields.append(f"ST_AsGeoJSON({alias}.{self.config.geom_column})::jsonb as {self.config.geom_column}")

            if self.config.bbox_column:
                fields.append(f"ST_AsGeoJSON({alias}.{self.config.bbox_column})::jsonb as {self.config.bbox_column}")

            if self.config.h3_resolutions:
                for res in self.config.h3_resolutions:
                    fields.append(f"{alias}.h3_res{res} as h3_res{res}")

            if self.config.s2_resolutions:
                for res in self.config.s2_resolutions:
                    fields.append(f"{alias}.s2_res{res} as s2_res{res}")

            # Add Statistics — full mode projects all storage-bearing fields.
            for f in self._columnar_fields():
                key = f.resolved_name
                if f.kind == ComputedKind.CENTROID:
                    fields.append(self._centroid_select_field(f, alias))
                else:
                    fields.append(f"{alias}.{key} as {_quote_ident(key)}")
            if self._has_jsonb_stats():
                fields.append(f"{alias}.geom_stats as geom_stats")

        return fields

    # get_join_clause: uses the SidecarProtocol base default (plain
    # hub.geoid = sidecar.geoid join against "{hub_table}_geometries",
    # since sidecar_id is always "geometries").

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
            algo = params.get("simplification_algorithm") or ""
            func = _SIMPLIFY_SQL_FUNCTIONS.get(algo, _DEFAULT_SIMPLIFY_SQL_FUNCTION)
            geom_col = f"{func}({geom_col}, :simplification)"

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
            # Preserve the cause (e.g. InvalidGeometryError carrying the
            # explain_validity reason) so it survives the ValueError wrap the
            # write path maps to a 422 / 207 per-item rejection.
            raise ValueError(f"Geometry preparation failed: {e}") from e

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
            # Trusted pre-processed WKB path: the WKB is stored as-is without
            # any re-projection. Contract: the WKB MUST already be in
            # target_srid. When the caller declares an explicit source SRID that
            # differs from target_srid we raise immediately — silently storing a
            # mis-projected geometry would corrupt every subsequent spatial query
            # and pre-computed stat. If srid is absent (the common trusted-path
            # case) the caller asserts the WKB is already correct.
            declared_srid = self._get_val(feature, "srid", None)
            if declared_srid is not None:
                try:
                    declared_srid = int(declared_srid)
                except (TypeError, ValueError) as e:
                    raise SridMismatchError(
                        f"Pre-processed geometry carries an unparseable source SRID "
                        f"{declared_srid!r}; expected an integer EPSG code."
                    ) from e
                if declared_srid != int(self.config.target_srid):
                    raise SridMismatchError(
                        f"Pre-processed geometry declares source SRID {declared_srid} "
                        f"but collection target_srid is {self.config.target_srid}. "
                        "The trusted/pre-processed WKB path stores geometry as-is and "
                        "does NOT re-project; pre-computed stats would otherwise be stale. "
                        "Re-project upstream to the target SRID before ingest, or use "
                        "the standard path (omit wkb_hex_processed) so the pipeline "
                        "transforms it."
                    )
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
            elif shapely_geom is not None:
                derived = _derive_bbox_from_shapely(shapely_geom)
                if derived is not None:
                    from shapely.geometry import box
                    from shapely import wkb
                    payload[self.config.bbox_column] = wkb.dumps(
                        box(*derived), hex=True
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

            # ``shapely_geom`` has been processed into ``target_srid``; pass it
            # so geographic CRSs yield geodesic area/perimeter/length (m²/m)
            # instead of meaningless square-degree planar values.
            derived = compute_derived_fields(
                shapely_geom, props, storage_fields, srid=self.config.target_srid
            )
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
                # Serialize to a JSON string before binding. The sidecar upsert
                # passes payload values straight to the driver with no ``::jsonb``
                # cast or codec, so a raw dict is rejected ("can't adapt type
                # 'dict'" on psycopg2 sync workers; asyncpg's built-in jsonb
                # codec also expects str). Mirrors the attributes sidecar's
                # ``attribute_stats`` handling.
                payload["geom_stats"] = json.dumps(
                    jsonb_payload, cls=CustomJSONEncoder
                )
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
                # JSON string, not a raw dict — see the geom_stats note above.
                payload["place_stats"] = json.dumps(
                    place_stats, cls=CustomJSONEncoder
                )
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

        # 2. BBox Mapping — build [minx, miny, maxx, maxy] from whichever
        # columns the read query projected.
        #
        # Priority A: scalar envelope columns (bbox_xmin / bbox_ymin /
        #   bbox_xmax / bbox_ymax) — projected by the STAC search hydration
        #   query via ST_XMin/YMin/XMax/YMax.
        # Priority B: bbox_geom — the stored PostGIS geometry column written
        #   when write_bbox=True; decoded through Shapely.
        # Priority C: derive from the main geometry already set on the Feature.
        #
        # Sets feature.bbox only when all four coordinate values are available
        # and numeric; leaves it untouched (None) on any failure so the caller
        # can degrade gracefully.
        _bbox_set = False
        if all(k in row and row[k] is not None for k in ("bbox_xmin", "bbox_ymin", "bbox_xmax", "bbox_ymax")):
            try:
                feature.bbox = (
                    float(row["bbox_xmin"]),
                    float(row["bbox_ymin"]),
                    float(row["bbox_xmax"]),
                    float(row["bbox_ymax"]),
                )
                _bbox_set = True
            except (TypeError, ValueError) as e:
                logger.warning("GeometriesSidecar: failed to build bbox from scalar columns: %s", e)

        if not _bbox_set and self.config.write_bbox and row.get("bbox_geom") is not None:
            try:
                import shapely.wkb
                _bg_raw = row["bbox_geom"]
                if isinstance(_bg_raw, str):
                    _bg = shapely.wkb.loads(bytes.fromhex(_bg_raw))
                elif isinstance(_bg_raw, (bytes, bytearray)):
                    _bg = shapely.wkb.loads(bytes(_bg_raw))
                elif hasattr(_bg_raw, "geom_type"):
                    _bg = _bg_raw
                else:
                    _bg = None
                if _bg is not None:
                    _b = _bg.bounds  # (minx, miny, maxx, maxy)
                    feature.bbox = (float(_b[0]), float(_b[1]), float(_b[2]), float(_b[3]))
                    _bbox_set = True
            except Exception as e:
                logger.warning("GeometriesSidecar: failed to build bbox from bbox_geom: %s", e)

        if not _bbox_set and feature.geometry is not None:
            try:
                from shapely.geometry import shape as _shape
                _geom_dict = (
                    feature.geometry
                    if isinstance(feature.geometry, dict)
                    else feature.geometry.model_dump(exclude_none=True)
                    if hasattr(feature.geometry, "model_dump")
                    else None
                )
                if _geom_dict is not None:
                    _b = _shape(_geom_dict).bounds
                    feature.bbox = (float(_b[0]), float(_b[1]), float(_b[2]), float(_b[3]))
            except Exception as e:
                logger.warning("GeometriesSidecar: failed to derive bbox from geometry: %s", e)

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

    def geometry_value_columns(self) -> set:
        """Column names this sidecar writes as PostGIS geometry values.

        The raw sidecar-upsert builder must wrap these with
        ``ST_GeomFromEWKB(decode(:col, 'hex'))`` — a geometry column rejects a
        raw bind. Besides the always-present ``geom``/``bbox_geom`` it includes
        every COLUMNAR ``centroid`` / ``centroid_3d`` field under its
        (possibly renamed) resolved column name, so a renamed or 3D centroid is
        wrapped too — not just a column literally named ``centroid``.
        """
        cols = {self.config.geom_column, "bbox_geom"}
        for f in self._storage_fields():
            if (
                f.storage_mode == StatisticStorageMode.COLUMNAR
                and f.kind in (ComputedKind.CENTROID, ComputedKind.CENTROID_3D)
            ):
                cols.add(f.resolved_name)
        return cols

    def get_internal_columns(self) -> set:
        """
        Return the set of DB column names this sidecar manages that are
        never part of the public Feature output properties.

        ``geohash`` is the Postgres-generated ST_GeoHash column used by
        the GEOHASH identity matcher (see ItemsWritePolicy); it is
        queryable at the SQL layer for write policies but is hidden from
        API consumers by default to avoid leaking storage plumbing into
        Feature.properties.

        ``bbox_xmin``/``bbox_ymin``/``bbox_xmax``/``bbox_ymax`` are the
        scalar ``ST_XMin``/``ST_YMin``/``ST_XMax``/``ST_YMax`` columns
        projected in place of the full geometry when ``skipGeometry=true``
        (#2899), consumed by ``map_row_to_feature`` above to populate
        ``feature.bbox``. Excluding them here keeps them out of the
        attributes sidecar's whole-row context publish, so they don't
        flatten onto the Feature root as foreign members duplicating
        ``bbox``.
        """
        cols = {
            "geom",
            "geom_type",
            "bbox_geom",
            "geohash",
            "bbox_xmin",
            "bbox_ymin",
            "bbox_xmax",
            "bbox_ymax",
        }
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
