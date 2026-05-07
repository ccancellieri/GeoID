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

from pydantic import BaseModel, Field, model_validator, field_validator, AliasChoices, ConfigDict
from typing import Optional, Annotated
from datetime import datetime
from enum import Enum
import logging

# We use pyproj to validate WKT adherence to OGC 18-010r11
try:
    from pyproj import CRS as PyprojCRS
    from pyproj.exceptions import CRSError
except ImportError:
    PyprojCRS = None

logger = logging.getLogger(__name__)

class CRSDefinition(BaseModel):
    """
    A model representing the definition of a Coordinate Reference System (CRS).
    Includes strict validation against OGC standards using PROJ.
    """
    class CRSDefinitionType(str, Enum):
        WKT2 = "WKT2"
        PROJ = "PROJ" # Legacy PROJ strings, though WKT2 is preferred for OGC compliance

    name: Optional[str] = Field(None, description="A human-readable name for the CRS. If not provided, will be extracted from WKT.")
    description: Optional[str] = Field(None, description="A detailed description of the CRS.")
    scope: Optional[str] = Field(None, description="The scope of the CRS (e.g., 'Oil and gas exploration'). Extracted from WKT if possible.")
    area_of_use: Optional[str] = Field(None, description="The geographic applicability. Extracted from WKT if possible.")
    definition_type: CRSDefinitionType = Field(..., description="The type of CRS definition provided (e.g., WKT2, PROJ).")
    definition: str = Field(..., description="The full WKT or PROJ string definition of the CRS.")

    @field_validator('definition')
    @classmethod
    def validate_definition_compliance(cls, v: str, info):
        """
        Validates the CRS string against OGC standards using pyproj.
        """
        if PyprojCRS is None:
            logger.warning("pyproj not installed. Skipping OGC WKT validation.")
            return v

        try:
            # Attempt to create a CRS object. This validates syntax and parameters.
            crs_obj = PyprojCRS(v)
            
            # Optional: Enforce WKT2 preference if the type is WKT2
            # We can also re-format the string to ensure it is canonical WKT2
            if info.data.get('definition_type') == cls.CRSDefinitionType.WKT2:
                # This ensures the stored string is clean, canonical OGC WKT2
                return crs_obj.to_wkt(version="WKT2_2019")
            
            return v
        except CRSError as e:
            raise ValueError(f"Invalid CRS definition: {str(e)}")

    @model_validator(mode='after')
    def extract_metadata_from_crs(self) -> 'CRSDefinition':
        """
        Extracts metadata (Name, Scope, Area) from the PROJ/WKT object
        if fields are missing in the input model.
        """
        if PyprojCRS is None:
            return self

        try:
            crs_obj = PyprojCRS(self.definition)
            
            # Populate Name
            if not self.name:
                self.name = crs_obj.name
            
            # Populate Area of Use
            if not self.area_of_use and crs_obj.area_of_use:
                self.area_of_use = str(crs_obj.area_of_use)
            
            # Populate Scope (if available in the object properties)
            if not self.scope and hasattr(crs_obj, 'scope') and crs_obj.scope:
                self.scope = str(crs_obj.scope)

        except Exception as e:
            # We don't fail here because validation happened earlier. 
            # We just skip metadata extraction.
            logger.warning(f"Could not extract metadata from CRS: {e}")
        
        # Fallback for name if extraction failed
        if not self.name:
            self.name = "Unnamed Custom CRS"

        return self

class CRS(BaseModel):
    """
    Represents a CRS entry in the database, linking a CRS definition
    to a specific catalog and URI.
    """
    catalog_id: Annotated[str, Field(description="The catalog to which this CRS definition belongs.")]
    crs_uri: str = Field(..., description="The unique URI identifier for the CRS within the catalog.")
    definition: CRSDefinition = Field(..., description="The detailed definition of the CRS.")
    created_at: datetime = Field(description="The timestamp when the CRS definition was created.")

    # Populated by the validator from the nested definition object for convenience.
    name: Optional[str] = Field(None, description="A human-readable name for the CRS.", exclude=True)
    description: Optional[str] = Field(None, description="A detailed description of the CRS.", exclude=True)

    @model_validator(mode='after')
    def populate_from_definition(self) -> 'CRS':
        """
        Populates the top-level 'name' and 'description' fields from the nested
        'definition' object after the model is initialized.
        """
        self.name = self.definition.name
        self.description = self.definition.description
        return self

class CRSCreate(BaseModel):
    """
    A model for creating a new CRS. It omits database-generated
    fields like 'created_at'.
    """
    crs_uri: str = Field(..., description="The unique URI identifier for the CRS within the catalog.")
    definition: CRSDefinition = Field(..., description="The detailed definition of the CRS.")