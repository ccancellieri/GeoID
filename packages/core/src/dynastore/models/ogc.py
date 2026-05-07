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

from typing import List, Optional, Dict, Any, Union
from pydantic import ConfigDict

try:
    from geojson_pydantic import Feature as _Feature, FeatureCollection as _FeatureCollection  # type: ignore[assignment]
    from geojson_pydantic.geometries import Geometry as GeoJSONGeometry
except ImportError:
    # Minimal fallbacks for core isolation
    from pydantic import BaseModel
    class _Feature(BaseModel):
        type: str = "Feature"
        geometry: Any = None
        properties: Optional[Dict[str, Any]] = None
        id: Optional[Union[str, int]] = None
    class _FeatureCollection(BaseModel):
        type: str = "FeatureCollection"
        features: List[Any] = []
    GeoJSONGeometry = Any

from dynastore.models.shared_models import Link

class Feature(_Feature):
    # Allow extra fields for extension data (like geom_gml, etc.)
    # and custom links mapping
    links: Optional[List[Link]] = None
    
    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)

class FeatureCollection(_FeatureCollection):
    features: List[Feature] = []
    links: Optional[List[Link]] = None
    timeStamp: Optional[str] = None
    numberMatched: Optional[int] = None
    numberReturned: Optional[int] = None
    
    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)
