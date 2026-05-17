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

from dynastore.models.mutability import Mutable
from dynastore.modules.db_config.plugin_config import PluginConfig
from dynastore.extensions.tools.exposure_mixin import ExposableConfigMixin
from typing import ClassVar, Optional, Tuple


class DGGSConfig(ExposableConfigMixin, PluginConfig):
    """Runtime configuration for the OGC API - DGGS extension."""
    _address: ClassVar[Tuple[str, ...]] = ("platform", "extensions", "dggs")


    # `enabled` inherited from ExposableConfigMixin.
    default_resolution: Mutable[int] = 5
    max_resolution: Mutable[int] = 10
    max_features_per_request: Mutable[int] = 10_000
