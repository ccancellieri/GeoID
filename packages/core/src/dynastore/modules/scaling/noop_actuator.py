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

"""Fallback ``PlatformScalingProtocol`` for deployments with no
platform-specific actuator (e.g. non-GCP). Logs and no-ops so the control
loop has somewhere safe to land instead of crashing on a missing protocol.

Registered at import time with ``priority=1000`` (lowest), the same pattern
``GcpJobRunner`` uses for its own registration in ``gcp_module.py`` — a real
platform actuator (e.g. ``GCPModule``, priority 30) always wins.
"""

from __future__ import annotations

import logging
from typing import Optional

from dynastore.tools.discovery import register_plugin

logger = logging.getLogger(__name__)


class NoOpPlatformScaling:
    """Lowest-priority fallback actuator — observes, never acts."""

    priority: int = 1000

    async def set_min_instances(self, n: int) -> None:
        logger.debug(
            "NoOpPlatformScaling.set_min_instances(%d): no platform actuator "
            "registered — no-op.",
            n,
        )

    async def get_min_instances(self) -> Optional[int]:
        return None


_noop_platform_scaling = NoOpPlatformScaling()
register_plugin(_noop_platform_scaling)
