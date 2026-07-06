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

"""Configuration for the Layer A platform-config hot-reload watcher.

Governs ``ConfigReloadService`` (see ``config_reload_service.py``): whether
every long-lived instance reconciles platform-scope config changes made by
another instance without a redeploy, and the self-sufficiency timeout floor
it falls back to when neither a real NOTIFY nor the task-queue LISTEN
bridge's health-beat has woken it.
"""

from typing import ClassVar, Optional, Tuple

from pydantic import Field

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig


class ConfigReloadConfig(PluginConfig):
    """Hot-reloadable configuration for the platform-config reload watcher.

    Address: ``("platform", "db", "config_reload")``.
    """

    _address: ClassVar[Tuple[str, ...]] = ("platform", "db", "config_reload")
    _tiers: ClassVar[Optional[Tuple[str, ...]]] = ("platform",)

    enabled: Mutable[bool] = Field(
        default=True,
        description=(
            "When True (default), every long-lived instance runs "
            "ConfigReloadService: it wakes on the PLATFORM_CONFIG_CHANGED "
            "pg_notify channel (carried by the existing task-queue LISTEN "
            "connection, no dedicated connection of its own) and re-runs "
            "apply handlers for any platform config whose stored row "
            "advanced since this instance last saw it, so in-process state "
            "(e.g. a cached connection) converges without a redeploy. Read "
            "at startup; changing requires a pod restart to take effect."
        ),
    )

    reload_interval_seconds: Mutable[float] = Field(
        default=30.0,
        ge=1.0,
        description=(
            "Self-sufficiency timeout floor for ConfigReloadService's wait "
            "on PLATFORM_CONFIG_CHANGED. A real NOTIFY, or the task-queue "
            "LISTEN bridge's own periodic health-beat, ordinarily wakes the "
            "service well under this interval; this value only bounds the "
            "worst case should the bridge ever be absent. Read at startup; "
            "changing requires a pod restart to take effect."
        ),
    )
