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

from dynastore.models.shared_models import OutputFormatEnum
from pydantic import BaseModel, Field
from typing import List, Literal, Optional, Dict
from dynastore.tools.geospatial import SimplificationAlgorithm

class DWHJoinRequestBase(BaseModel):
    """Base Request model for joining features with a Data Warehouse (BigQuery) query."""
    dwh_project_id: str = Field(
        ...,
        json_schema_extra={"example": "dwh-review"},
        description="Google DWH project_id (e.g., BigQuery).",
    )
    dwh_query: str = Field(
        ...,
        json_schema_extra={"example": "SELECT user_id FROM analytics.locations WHERE region = 'emea'"},
        description="SQL query to execute against the DWH (e.g., BigQuery).",
    )
    collection: str = Field(
        ...,
        json_schema_extra={"example": "collection1"},
        description="DynaStore collection to query.",
    )
    with_geometry: bool = Field(default=True, description="If True, include geometry in the response.")
    dwh_join_column: str = Field(
        ...,
        json_schema_extra={"example": "geoid"},
        description="Common column name for joining, present in DWH results and DynaStore table attributes.",
    )
    join_column: str = Field(
        ...,
        json_schema_extra={"example": "geoid"},
        description="Common column name for joining, present in DynaStore table attributes.",
    )
    join_source: Literal["properties", "system", "stats"] = Field(
        default="properties",
        description=(
            "Which section of the DynaStore feature to read the join key from. "
            "'properties' (default) reads feature.properties[join_column] with "
            "a feature.id fallback — preserving existing behavior. "
            "'system' reads the system foreign-member section only "
            "(external_id, geoid, asset_id, etc.), so a system identity can be "
            "used as a join key without colliding with a same-named user property. "
            "'stats' reads the stats foreign-member section only. "
            "See #1827."
        ),
    )
    properties: Optional[List[str]] = Field(
        None,
        json_schema_extra={"example": ["code", "region"]},
        description=(
            "User-facing attribute fields that populate feature.properties. "
            "Storage-agnostic: works for both JSONB and COLUMNAR collections. "
            "Use ['*'] to include all available property fields; omit to include none."
        ),
    )
    stats: Optional[List[str]] = Field(
        None,
        json_schema_extra={"example": ["area_ha", "perimeter_m", "h3_lvl5"]},
        description=(
            "Computed/statistic fields produced by the collection's sidecars "
            "(area, perimeter, centroid, geohash, h3 cells, …). "
            "Use ['*'] to include all producible stats; omit to include none."
        ),
    )
    system: Optional[List[str]] = Field(
        None,
        json_schema_extra={"example": ["external_id", "geoid", "validity"]},
        description=(
            "Identity and lifecycle fields (external_id, geoid, asset_id, "
            "geometry_hash, attributes_hash, validity, transaction_time, deleted_at). "
            "Use ['*'] to include all available system fields; omit to include none."
        ),
    )
    where: Optional[str] = Field(
        None,
        json_schema_extra={"example": "valid_to = 'infinity'"},
        description="Optional SQL WHERE clause to apply directly to the DynaStore query.",
    )
    output_format: OutputFormatEnum = Field(
        OutputFormatEnum.GEOJSON, description="Desired output format."
    )
    output_encoding: str = Field(
        default="utf-8",
        description="Character encoding for the output features (e.g., 'utf-8', 'latin-1')."
    )
    limit: Optional[int] = Field(
        default=None,
        description="Limit the number of returned features."
    )
    offset: Optional[int] = Field(
        default=None,
        description="Skip the first N features."
    )
    destination_crs: int = Field(
        default=4326,
        json_schema_extra={"example": 4326},
        description="Destination CRS EPSG code for the output geometry (default: 4326 - WGS 84).",
    )

class DWHJoinRequest(DWHJoinRequestBase):
    """Legacy Request model including catalog in payload."""
    catalog: str = Field(
        ...,
        json_schema_extra={"example": "stores"},
        description="DynaStore catalog to query.",
    )

class TilesConfig(BaseModel):
    """Configuration for tiled DWH join requests."""
    tileMatrixSetId: str = Field(
        default="WebMercatorQuad",
        description="Tile Matrix Set ID (default: WebMercatorQuad)"
    )
    datetime: Optional[str] = Field(None, description="Temporal filter")
    filter: Optional[str] = Field(None, description="CQL2 filter expression")
    filter_lang: Optional[str] = Field(default="cql2-text", description="Filter language")
    subset: Optional[str] = Field(None, description="Subset parameter")
    simplification: Optional[float] = Field(None, description="Simplification tolerance")
    simplification_by_zoom: Optional[Dict[int, float]] = Field(None, description="Zoom-based simplification rules")
    simplification_algorithm: SimplificationAlgorithm = Field(
        default=SimplificationAlgorithm.TOPOLOGY_PRESERVING,
        description="Simplification algorithm"
    )
    extent: int = Field(default=4096, description="MVT extent")
    buffer: int = Field(default=256, description="MVT buffer")
    enable_cache: bool = Field(default=False, description="Enable tile caching for this request")

class DWHTiledJoinRequest(BaseModel):
    """Request model for tiled DWH join."""
    dwh_project_id: str = Field(..., description="Google DWH project_id (e.g., BigQuery)")
    dwh_query: str = Field(..., description="SQL query to execute against the DWH")
    collection: str = Field(..., description="DynaStore collection to query")
    dwh_join_column: str = Field(..., description="Join column in DWH results")
    join_column: str = Field(..., description="Join column in DynaStore table")
    join_source: Literal["properties", "system", "stats"] = Field(
        default="properties",
        description=(
            "Which section of the DynaStore feature to read the join key from. "
            "See DWHJoinRequestBase.join_source for full semantics. See #1827."
        ),
    )
    tiles: TilesConfig = Field(default_factory=TilesConfig, description="Tiles configuration")  # type: ignore[arg-type]
    format: str = Field(default="mvt", description="Output format (mvt or pbf)")
