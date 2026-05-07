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

# dynastore/extensions/tiles/tiles_models.py

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, ConfigDict
import uuid

class Link(BaseModel):
    href: str
    rel: str
    type: Optional[str] = None
    hreflang: Optional[str] = None
    title: Optional[str] = None

class BoundingBox(BaseModel):
    lowerLeft: List[float]
    upperRight: List[float]
    crs: Optional[str] = None
    orderedAxes: Optional[List[str]] = None

class TileMatrix(BaseModel):
    id: str = Field(..., description="Identifier selecting one of the scales defined in the TileMatrixSet")
    title: Optional[str] = None
    description: Optional[str] = None
    keywords: Optional[List[str]] = None
    scaleDenominator: float
    cellSize: float
    cornerOfOrigin: Optional[str] = "topLeft"
    pointOfOrigin: List[float]
    tileWidth: int
    tileHeight: int
    matrixWidth: int
    matrixHeight: int
    variableMatrixWidths: Optional[List[Dict[str, Any]]] = None

class TileMatrixSet(BaseModel):
    id: str
    title: Optional[str] = None
    description: Optional[str] = None
    keywords: Optional[List[str]] = None
    uri: Optional[str] = None
    orderedAxes: Optional[List[str]] = None
    crs: str
    wellKnownScaleSet: Optional[str] = None
    boundingBox: Optional[BoundingBox] = None
    tileMatrices: List[TileMatrix]

class TileMatrixSetCreate(BaseModel):
    """
    Model for creating a new TileMatrixSet.
    The 'definition' should conform to the OGC API - Tiles - Part 2: TileMatrixSet schema.
    """
    id: str = Field(..., description="The identifier for the custom TileMatrixSet (e.g., 'MyCustomTMS').")
    definition: TileMatrixSet = Field(..., description="The full OGC TileMatrixSet definition object.")

class StoredTileMatrixSet(BaseModel):
    """
    Model representing a stored TileMatrixSet, including its database ID and catalog.
    """
    id: uuid.UUID
    catalog_id: str
    definition: TileMatrixSet

    model_config = ConfigDict(from_attributes=True)

class TileMatrixSetRef(BaseModel):
    id: str
    title: Optional[str] = None
    links: List[Link]

class TileMatrixSetList(BaseModel):
    tileMatrixSets: List[TileMatrixSetRef]