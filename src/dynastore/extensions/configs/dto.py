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

"""Configuration API — Data Transfer Objects.

Typed request/response models for the ``/configs`` extension.

Per-plugin example payloads are NOT carried here — the JSON Schema for
each registered :class:`PluginConfig` (served by ``GET /configs/schemas``)
is the single source of truth for shape, defaults, and inline
``examples``.
"""

from datetime import datetime
from enum import StrEnum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# (WellKnownPlugin removed — configs are now identified by Pydantic class_key)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Shared sub-models
# ---------------------------------------------------------------------------


class ConfigLevel(StrEnum):
    """Hierarchy tier from which a configuration was resolved."""

    COLLECTION = "collection"
    CATALOG = "catalog"
    PLATFORM = "platform"
    DEFAULT = "default"


class PluginSchemaInfo(BaseModel):
    """Schema metadata for a registered plugin."""

    description: str = Field(..., description="Human-readable description from the model docstring.")
    schema_: Dict[str, Any] = Field(..., alias="schema", description="Full JSON Schema of the config model.")

    model_config = {"populate_by_name": True}


# ---------------------------------------------------------------------------
# Response DTOs
# ---------------------------------------------------------------------------


class ConfigEntry(BaseModel):
    """A single configuration entry as returned by list/search endpoints."""

    plugin_id: str = Field(..., description="Unique plugin identifier (snake_case).", examples=["items_postgresql_driver"])
    config_data: Dict[str, Any] = Field(..., description="The stored configuration payload.")
    updated_at: Optional[datetime] = Field(None, description="Last modification timestamp (UTC).")


class ConfigListResponse(BaseModel):
    """Paginated list of configuration entries."""

    items: List[ConfigEntry] = Field(default_factory=list)
    total: int = Field(0, ge=0, description="Total items matching the query.")
    limit: int = Field(10, ge=1, le=1000)
    offset: int = Field(0, ge=0)


class EffectiveConfigResponse(BaseModel):
    """The resolved (effective) configuration for a plugin at a given scope.

    The ``resolved_from`` field tells the caller which hierarchy tier
    supplied the value, so they know whether it is an explicit override
    or a fallback default.
    """

    plugin_id: str = Field(..., examples=["items_postgresql_driver"])
    config: Dict[str, Any] = Field(..., description="Effective configuration payload.")
    resolved_from: Optional[ConfigLevel] = Field(
        None,
        description="Hierarchy tier that provided this configuration.",
    )


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
    Inner key is the snake_case ``driver_id`` (e.g. ``"items_postgresql_driver"``),
    used in ``CollectionRoutingConfig`` / ``AssetRoutingConfig`` operations.
    """

    drivers: Dict[str, Dict[str, DriverInfo]] = Field(
        default_factory=dict,
        description="domain → {driver_id → driver info}.",
    )


class PluginListResponse(BaseModel):
    """List of registered plugin IDs (without schemas)."""

    plugins: List[str] = Field(
        ...,
        examples=[
            [
                "collection",
                "stac",
                "collection_routing_config",
                "asset_routing_config",
                "items_postgresql_driver",
                "items_elasticsearch_driver",
                "tiles",
                "tasks",
                "features",
                "wfs",
            ]
        ],
    )


class PluginSchemaResponse(BaseModel):
    """All registered plugins with their JSON Schemas."""

    plugins: Dict[str, PluginSchemaInfo]



# ---------------------------------------------------------------------------
# Bulk-apply request / response
# ---------------------------------------------------------------------------


class QuickStartConfigSet(BaseModel):
    """Bundle of plugin configurations applied atomically by the bulk endpoints.

    Each key is a ``plugin_id`` (the ``class_key()`` string of a registered
    PluginConfig); each value is the full payload for that plugin. The bulk
    endpoint iterates the bundle and applies each entry independently —
    failures on one plugin do not abort the rest (see
    :class:`BulkApplyResponse`).
    """

    configs: Dict[str, Dict[str, Any]] = Field(
        ...,
        description="Map of plugin_id to config payload.",
        json_schema_extra={
            "examples": [
                {
                    "collection_routing_config": {
                        "enabled": True,
                        "operations": {
                            "WRITE": [{"driver_id": "items_postgresql_driver_config", "on_failure": "fatal"}],
                            "READ":  [{"driver_id": "items_postgresql_driver_config", "on_failure": "fatal"}],
                        },
                    },
                    "items_postgresql_driver_config": {
                        "enabled": True,
                        "collection_type": "VECTOR",
                    },
                }
            ]
        },
    )


class BulkApplyResultEntry(BaseModel):
    """Result of applying a single plugin config within a bulk operation."""

    plugin_id: str
    status: str = Field(..., description="'ok' or 'error'.")
    detail: Optional[str] = Field(None, description="Error message if status is 'error'.")


class BulkApplyResponse(BaseModel):
    """Response from the bulk-apply endpoint."""

    applied: int = Field(0, description="Number of configs successfully applied.")
    failed:  int = Field(0, description="Number of configs that failed.")
    results: List[BulkApplyResultEntry] = Field(default_factory=list)
