#    Copyright 2026 FAO
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

from typing import ClassVar, Tuple

from dynastore.extensions.tools.exposure_mixin import ExposableConfigMixin
from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig


class ConnectedSystemsPluginConfig(ExposableConfigMixin, PluginConfig):
    """Service-exposure config for the OGC API - Connected Systems extension.

    Scoped to platform and catalog tiers only (not per-collection): all
    Connected Systems resources are catalog-scoped (systems, datastreams,
    observations are keyed on ``catalog_id``), so there is no meaningful
    collection-level override surface.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "extensions", "connected_systems")
    _tiers: ClassVar[Tuple[str, ...]] = ("platform", "catalog")

    # `enabled` inherited from ExposableConfigMixin.

    # Pagination policy (OGC API - Features Part 1 Core, /req/core/fc-limit-*)
    default_limit: Mutable[int] = 100
    max_limit: Mutable[int] = 1000
