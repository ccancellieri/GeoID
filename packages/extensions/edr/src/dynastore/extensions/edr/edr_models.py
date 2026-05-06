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

"""Response models for OGC API - Environmental Data Retrieval (EDR)."""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from dynastore.extensions.tools.ogc_common_models import Conformance, LandingPage  # noqa: F401
from dynastore.models.shared_models import Link


class EDRLandingPage(LandingPage):
    """Landing page for OGC API - EDR."""

    title: Optional[str] = "DynaStore OGC API - EDR"
    description: Optional[str] = "Environmental Data Retrieval via OGC API - EDR"


class EDRExtent(BaseModel):
    """Spatial and temporal extent of an EDR collection."""

    spatial: Optional[Dict[str, Any]] = None
    temporal: Optional[Dict[str, Any]] = None


class EDRCollection(BaseModel):
    """Metadata for a single EDR collection."""

    id: str
    title: Optional[str] = None
    description: Optional[str] = None
    extent: Optional[EDRExtent] = None
    data_queries: Optional[Dict[str, Any]] = None
    crs: List[str] = Field(default_factory=lambda: ["CRS84"])
    output_formats: List[str] = Field(
        default_factory=lambda: ["CoverageJSON", "GeoJSON"]
    )
    parameter_names: Optional[Dict[str, Any]] = None
    links: List[Link] = Field(default_factory=list)


class EDRCollections(BaseModel):
    """List of EDR collections."""

    collections: List[EDRCollection] = Field(default_factory=list)
    links: List[Link] = Field(default_factory=list)


class EDRLocations(BaseModel):
    """GeoJSON FeatureCollection of named locations."""

    type: str = "FeatureCollection"
    features: List[Dict[str, Any]] = Field(default_factory=list)
