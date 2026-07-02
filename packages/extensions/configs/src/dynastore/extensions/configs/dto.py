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

"""Configuration API — Data Transfer Objects.

Typed request/response models for the ``/configs`` extension.

Per-plugin example payloads are NOT carried here — the JSON Schema for
each registered :class:`PluginConfig` (served by ``GET /configs/schemas``)
is the single source of truth for shape, defaults, and inline
``examples``.
"""

from typing import Dict, List

from pydantic import BaseModel, Field


class DriverInfo(BaseModel):
    """Metadata about a registered storage driver."""

    description: Dict[str, str] = Field(
        default_factory=dict,
        description="Multilanguage description of the driver (ClassVar[LocalizedText] from the class).",
    )
    capabilities: List[str] = Field(
        default_factory=list,
        description="What the driver can do (Capability constants: read, write, fulltext, ...).",
    )
    driver_capabilities: List[str] = Field(
        default_factory=list,
        description="How the driver operates (DriverCapability: SYNC, ASYNC, TRANSACTIONAL, ...).",
    )
    supported_operations: List[str] = Field(
        default_factory=list,
        description="Operations this driver supports, derived from its capabilities.",
    )
    supported_hints: List[str] = Field(
        default_factory=list,
        description="Hints this driver accepts in routing config entries.",
    )
    preferred_for: List[str] = Field(
        default_factory=list,
        description="Hints this driver is optimized for (used for auto-selection).",
    )
    available: bool = Field(default=True, description="Whether the driver is currently available.")


class DriverListResponse(BaseModel):
    """All registered storage drivers, grouped by the protocol/domain they serve.

    Outer key is the domain (``"collections"``, ``"assets"``,
    ``"collection_metadata"``) — matches the slot in routing config.
    Inner key is the snake_case ``driver_ref`` (e.g. ``"items_postgresql_driver"``),
    used in ``ItemsRoutingConfig`` / ``AssetRoutingConfig`` operations.
    """

    drivers: Dict[str, Dict[str, DriverInfo]] = Field(
        default_factory=dict,
        description="domain → {driver_ref → driver info}.",
    )
