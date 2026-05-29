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

"""
Asset plugin configuration.

``AssetPluginConfig`` is a structural config for per-collection asset
behaviour (e.g. ``enabled``).  Routing (which drivers handle which
operations) is handled by ``AssetRoutingConfig`` in
``dynastore.modules.storage.routing_config``.
"""

from pydantic import ConfigDict

from dynastore.modules.db_config.plugin_config import PluginConfig
from typing import ClassVar, Optional, Tuple

class AssetPluginConfig(PluginConfig):
    """Per-collection asset config (structural only).

    Routing is now managed by ``AssetRoutingConfig``
    (identity: ``AssetRoutingConfig.class_key()``).
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "asset")
    _freeze_at: ClassVar[Optional[str]] = "collection"


    model_config = ConfigDict(extra="allow")


AssetPluginConfig.model_rebuild()
