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
from pydantic import Field
from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig
from dynastore.modules.storage.driver_config import WriteConflictPolicy

# --- Main Ingestion Config ---

class IngestionPluginConfig(PluginConfig):
    """
    Configuration for the Ingestion Module.
    This can be updated at any time via API.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "tasks", "ingestion")

    # 1. Behavior Rules
    on_conflict: Mutable[WriteConflictPolicy] = Field(
        default=WriteConflictPolicy.NEW_VERSION,
        description="Determines how new entities with an existing external_id are handled."
    )

    sync_ingest_max_body_mb: Mutable[int] = Field(
        default=64,
        ge=1,
        description=(
            "Maximum inbound request body (MiB) accepted on the synchronous "
            "bulk item-POST (POST .../collections/{id}/items) before the "
            "body is parsed. Larger loads are rejected with 413 and steered "
            "to the asynchronous ingestion task instead."
        ),
    )