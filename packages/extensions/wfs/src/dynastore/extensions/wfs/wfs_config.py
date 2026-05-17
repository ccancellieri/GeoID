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

from typing import ClassVar, List, Optional, Tuple
from pydantic import Field
from dynastore.models.mutability import Mutable
from dynastore.modules.db_config.plugin_config import PluginConfig
from dynastore.extensions.tools.exposure_mixin import ExposableConfigMixin

class WFSPluginConfig(ExposableConfigMixin, PluginConfig):
    """
    Runtime configuration for the WFS extension.
    Controls caching and visibility.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "extensions", "wfs")

    # Caching
    cache_on_demand: Mutable[bool] = Field(
        False, # Default to False for WFS as it might handle large geometries
        description="If True, generated WFS responses are saved to the bucket storage for future reuse."
    )

    storage_priority: Mutable[List[str]] = Field(
        default=["bucket"],
        description="Priority list of storage providers to use for saving cached responses."
    )
