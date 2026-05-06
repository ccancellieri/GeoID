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

# dynastore/extensions/stac/stac_aggregation_models.py

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from dynastore.modules.stac.stac_config import AggregationRule


class AggregationRequest(BaseModel):
    """
    Request model for aggregation endpoint.
    Follows OGC STAC Aggregation Extension specification.
    """
    aggregations: List[AggregationRule] = Field(
        ..., 
        description="List of aggregations to execute",
        min_length=1
    )
    filter: Optional[Dict[str, Any]] = Field(
        None, 
        description="CQL-JSON filter to apply before aggregation"
    )
    datetime: Optional[str] = Field(
        None, 
        description="Temporal filter in RFC 3339 format or interval (e.g., '2020-01-01T00:00:00Z/2021-01-01T00:00:00Z')"
    )
    bbox: Optional[List[float]] = Field(
        None, 
        description="Spatial filter as [west, south, east, north] in WGS84",
        min_length=4,
        max_length=6
    )
    collections: Optional[List[str]] = Field(
        None,
        description="Optional list of collection IDs to aggregate across (if not specified in URL)"
    )
