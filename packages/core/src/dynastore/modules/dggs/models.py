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

"""Pydantic models for OGC API - DGGS Part 1 responses."""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

from dynastore.models.shared_models import Link


class DGGRSInfo(BaseModel):
    """Metadata for a Discrete Global Grid Reference System."""

    id: str = Field(description="DGGRS identifier (e.g. 'H3')")
    title: str
    description: str
    uri: str = Field(description="OGC DGGRS definition URI")
    links: List[Link] = Field(default_factory=list)
    maxRefinementLevel: int = Field(
        description="Maximum zone refinement level supported"
    )
    defaultRefinementLevel: int = Field(
        description="Default zone refinement level"
    )


class DGGRSList(BaseModel):
    """List of available DGGRS."""

    dggrs: List[DGGRSInfo]
    links: List[Link] = Field(default_factory=list)


class ZoneProperties(BaseModel):
    """Properties of a DGGS zone feature."""

    zone_id: str = Field(alias="zone-id", description="H3 cell index")
    resolution: int = Field(description="H3 resolution level (0-15)")
    count: int = Field(default=0, description="Number of features in zone")
    values: Dict[str, Any] = Field(
        default_factory=dict,
        description="Aggregated numeric values (avg per property name)",
    )

    model_config = {"populate_by_name": True}


class DGGSFeature(BaseModel):
    """GeoJSON Feature representing a DGGS zone with aggregated data."""

    type: Literal["Feature"] = "Feature"
    id: str = Field(description="H3 cell index")
    geometry: Dict[str, Any] = Field(description="GeoJSON Polygon (H3 hexagon boundary)")
    properties: ZoneProperties


class DGGSFeatureCollection(BaseModel):
    """GeoJSON FeatureCollection of DGGS zones."""

    type: Literal["FeatureCollection"] = "FeatureCollection"
    features: List[DGGSFeature] = Field(default_factory=list)
    links: List[Link] = Field(default_factory=list)
    numberMatched: Optional[int] = None
    numberReturned: int = 0
    dggsId: str = Field(description="DGGRS identifier used for this response")
    zoneLevel: int = Field(description="Zone resolution level")
