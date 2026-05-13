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

from typing import ClassVar, Optional, Tuple

from dynastore.modules.db_config.platform_config_service import Mutable, PluginConfig
from dynastore.extensions.tools.exposure_mixin import ExposableConfigMixin


class CoveragesConfig(ExposableConfigMixin, PluginConfig):
    """Service-exposure and runtime guardrail config for the coverages extension."""
    _address: ClassVar[Tuple[str, ...]] = ("platform", "extensions", "coverages")

    # `enabled` inherited from ExposableConfigMixin.
    max_bands_per_request: Mutable[int] = 16
    request_deadline_soft_s: Mutable[int] = 60
    request_deadline_hard_s: Mutable[int] = 120
    default_block_size: Mutable[int] = 512
    default_style_id: Mutable[Optional[str]] = None


# Backward-compatible alias — historical name used elsewhere in the codebase.
CoveragesPluginConfig = CoveragesConfig
