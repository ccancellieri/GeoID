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

from typing import ClassVar, Dict, List, Optional, Tuple
from pydantic import Field, BaseModel
from dynastore.modules.db_config.platform_config_service import PluginConfig
from dynastore.extensions.tools.exposure_mixin import ExposableConfigMixin
from dynastore.tools.geospatial import SimplificationAlgorithm

class TilesConfig(ExposableConfigMixin, PluginConfig):
    """
    Runtime configuration for the Tiles extension.
    Controls visibility, bounds, and on-the-fly generation settings.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "tiles", None)


    # Global mask/bounds
    bbox: Optional[List[Tuple[float, float, float, float]]] = Field(default=None, 
        description="Global bounding boxes for the collection/catalog. Requests outside these bounds return 404/Empty. If None, assumes world/max extent."
    )
    
    # Zoom limits
    min_zoom: int = Field(default=0, description="Minimum zoom level served.")
    max_zoom: int = Field(default=12, description="Maximum zoom level served.")
    
    # TMS Support
    supported_tms_ids: List[str] = Field(
        default=["WebMercatorQuad"], 
        description="List of supported TileMatrixSet IDs."
    )
    
    # Runtime Generation Settings
    simplification_by_zoom: Optional[Dict[int, float]] = Field(default=None, 
        description="Map of zoom level to simplification tolerance (in degrees/units). Applied during dynamic generation."
    )
    simplification_algorithm: Optional[SimplificationAlgorithm] = Field(
        default=SimplificationAlgorithm.TOPOLOGY_PRESERVING,
        description="Algorithm used for dynamic simplification."
    )

    # Caching
    cache_on_demand: bool = Field(
        default=True,
        description="If True, dynamically generated tiles are saved to the preseed storage for future reuse."
    )


class TilesPreseedConfig(PluginConfig):
    """
    Configuration for the Tiles Pre-seeding Process.
    This configures the background task that generates and stores tiles.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "tiles", None)

    # Implicitly enabled if this config is present and defaults are acceptable, 
    # but explicit flag allows disabling without deleting config.
    enabled: bool = Field(default=True, description="If True, the pre-seeding task will process this configuration.")
    
    # What to seed
    target_tms_ids: List[str] = Field(
        default=["WebMercatorQuad"], 
        description="List of TMS IDs to pre-seed. Must be a subset of TilesConfig.supported_tms_ids."
    )
    formats: List[str] = Field(
        default=["mvt"], 
        description="List of output formats to generate (e.g. 'mvt', 'geojson')."
    )
    
    # Where to seed (Spatial subset)
    bboxes: Optional[List[Tuple[float, float, float, float]]] = Field(default=None, 
        description="Specific areas to pre-seed. Intersected with TilesConfig.bbox."
    )
    
    # Storage Configuration
    storage_priority: List[str] = Field(
        default=["bucket", "pg"], 
        description="Priority list of storage providers to use for saving tiles."
    )
    
    # Generation Overrides
    simplification_by_zoom_override: Optional[Dict[int, float]] = Field(default=None, 
        description="Override runtime simplification settings for pre-seeded tiles."
    )
    
    # Catalog Level specific
    collections_to_preseed: Optional[List[str]] = Field(default=None, 
        description="For Catalog-level config: list of collections to include. If None, applies to all (or logic defined by task)."
    )
