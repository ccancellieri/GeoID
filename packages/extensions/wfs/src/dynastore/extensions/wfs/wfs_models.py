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

from pydantic import BaseModel, Field
from typing import Optional, List
from dynastore.models.shared_models import OutputFormatEnum

class FeaturesQueryRequest(BaseModel):
    """Request model for querying features with ECQL filter and attribute selection."""

    store: str = Field(
        ...,
        json_schema_extra={"example": "stores"},
        description="Store name (e.g., stores).",
    )
    layer: str = Field(
        ...,
        json_schema_extra={"example": "layer"},
        description="Layer name (e.g., roads).",
    )
    select: Optional[List[str]] = Field(
        None,
        json_schema_extra={"example": ["geoid", "name"]},
        description="List of columns/attributes to return. Can be database columns or JSONB attributes.",
    )
    cql_filter: Optional[str] = Field(
        None,
        json_schema_extra={"example": "name LIKE 'A%' AND ST_Intersects(geom, ST_MakeEnvelope(...))"},
        description="ECQL filter string.",
    )
    output_format: OutputFormatEnum = Field(
        OutputFormatEnum.GEOJSON, description="Desired output format."
    )
    limit: int = Field(100, ge=1, description="Maximum number of features to return.")
    offset: int = Field(0, ge=0, description="Offset for pagination.")


class WFSException(Exception):
    """Custom exception for WFS errors that can be caught by the web layer."""
    def __init__(self, message: str, code: str = "OperationProcessingFailed", locator: str = "cql_filter"):
        """
        Initializes the WFS exception.
        :param message: The human-readable exception text.
        :param code: The OGC exception code (e.g., 'InvalidParameterValue').
        :param locator: The parameter or part of the request that caused the error.
        """
        self.message = message
        self.code = code
        self.locator = locator
        super().__init__(self.message)




    
