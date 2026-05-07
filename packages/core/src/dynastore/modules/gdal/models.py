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

from pydantic import BaseModel, ConfigDict
from typing import List, Dict, Any, Optional, Tuple, Union

# --- Vector Info Models (from ogrinfo) ---

class VectorFieldInfo(BaseModel):
    """Describes a single field in a vector layer."""
    name: str
    type: str

class VectorCoordinateSystem(BaseModel):
    """Describes the coordinate system of a vector layer."""
    wkt: str

class VectorLayerInfo(BaseModel):
    """Describes a single layer within a vector dataset."""
    name: str
    geometryType: str
    featureCount: int
    extent: Tuple[float, float, float, float]
    coordinateSystem: VectorCoordinateSystem
    fields: List[VectorFieldInfo]

class VectorInfo(BaseModel):
    """The complete information model for a vector dataset."""
    driverShortName: str
    layers: List[VectorLayerInfo]


# --- Raster Info Models (from gdalinfo) ---

class RasterCoordinateSystem(BaseModel):
    """Describes the coordinate system of a raster dataset."""
    wkt: str
    dataAxisToSRSAxisMapping: List[int]

class CornerCoordinates(BaseModel):
    """Describes the corner coordinates of a raster dataset."""
    upperLeft: Tuple[float, float]
    lowerLeft: Tuple[float, float]
    upperRight: Tuple[float, float]
    lowerRight: Tuple[float, float]
    center: Tuple[float, float]

class Wgs84Extent(BaseModel):
    """Describes the WGS84 extent as a GeoJSON Polygon."""
    type: str = "Polygon"
    coordinates: List[List[Tuple[float, float]]]

class Band(BaseModel):
    """Describes a single band within a raster dataset."""
    band: int
    block: Tuple[int, int]
    type: str
    colorInterpretation: str
    description: Optional[str] = None
    noDataValue: Optional[Union[int, float]] = None
    metadata: Dict[str, Any]

class RasterInfo(BaseModel):
    """The complete information model for a raster dataset."""
    description: str
    driverShortName: str
    driverLongName: str
    files: List[str]
    size: Tuple[int, int]
    coordinateSystem: RasterCoordinateSystem
    geoTransform: List[float]
    metadata: Dict[str, Any]
    cornerCoordinates: CornerCoordinates
    wgs84Extent: Wgs84Extent
    bands: List[Band]
    stac: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(extra='ignore')
