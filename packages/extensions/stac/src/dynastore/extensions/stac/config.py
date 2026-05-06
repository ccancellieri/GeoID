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
import os
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Dict

class StacSettings(BaseSettings):
    """
    Manages configuration for the STAC API service.
    Settings are loaded from environment variables.
    """
    # Type of links to generate: 'relative' or 'absolute'
    # Affects all generated STAC links (self, parent, child, etc.)
    STAC_LINK_TYPE: str = os.getenv("STAC_LINK_TYPE", "relative")

    # A map of geometry complexity (number of vertices) to simplification tolerance.
    # The key is the minimum number of points, and the value is the tolerance to apply.
    # Parsed from a string like "50000:0.1,10000:0.01,1000:0.001"
    GEOMETRY_SIMPLIFICATION_MAP: str = os.getenv("GEOMETRY_SIMPLIFICATION_MAP", "50000:0.1,10000:0.01,1000:0.001")

    # The default tolerance for geometries that don't meet any threshold in the map.
    DEFAULT_GEOMETRY_SIMPLIFICATION_TOLERANCE: float = float(os.getenv("DEFAULT_GEOMETRY_SIMPLIFICATION_TOLERANCE", 0.0001))

    @property
    def simplification_levels(self) -> Dict[int, float]:
        """Parses the simplification map string into a dictionary."""
        levels = {}
        if not self.GEOMETRY_SIMPLIFICATION_MAP:
            return {}
        try:
            for item in self.GEOMETRY_SIMPLIFICATION_MAP.split(','):
                key, value = item.split(':')
                levels[int(key.strip())] = float(value.strip())
            # Sort by key (points) in descending order to build the CASE statement correctly
            return dict(sorted(levels.items(), key=lambda item: item[0], reverse=True))
        except ValueError:
            # Fallback to an empty map if parsing fails
            return {}

    model_config = SettingsConfigDict(
        # Specifies the prefix for environment variables, e.g., STAC_LINK_TYPE
        env_prefix = "",
        case_sensitive = False
    )

# Create a single, importable instance of the settings
settings = StacSettings()

