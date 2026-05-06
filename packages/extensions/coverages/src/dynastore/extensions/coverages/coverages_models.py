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

"""Response models for OGC API - Coverages."""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from dynastore.extensions.tools.ogc_common_models import Conformance, LandingPage
from dynastore.models.shared_models import Link


class CoveragesLandingPage(LandingPage):
    """Landing page for OGC API - Coverages."""

    title: Optional[str] = "DynaStore OGC API - Coverages"
    description: Optional[str] = "Access to coverage data via OGC API - Coverages"


class DomainSet(BaseModel):
    """Describes the domain (axes/CRS) of a coverage."""

    type: str = "DomainSet"
    generalGrid: Optional[Dict[str, Any]] = None


class RangeType(BaseModel):
    """Describes the range (data fields) of a coverage."""

    type: str = "DataRecord"
    field: List[Dict[str, Any]] = Field(default_factory=list)


class CoverageDescription(BaseModel):
    """Metadata describing a coverage resource."""

    id: str
    title: Optional[str] = None
    description: Optional[str] = None
    extent: Optional[Dict[str, Any]] = None
    crs: Optional[List[str]] = None
    domainSet: Optional[DomainSet] = None
    rangeType: Optional[RangeType] = None
    links: List[Link] = Field(default_factory=list)
