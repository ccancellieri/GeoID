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

"""
Cache module configuration (Valkey timeouts, circuit breaker, etc.).

Registered via PluginConfig framework so settings are:
- Runtime-configurable (no redeploy needed)
- Per-catalog overridable
- Accessible via /configs?plugin_id=module_cache API

This replaces env-var-only configuration (e.g., VALKEY_PROBE_TIMEOUT).
"""

from __future__ import annotations

from typing import ClassVar, Tuple

from pydantic import BaseModel, Field

from dynastore.extensions.tools.exposure_mixin import ExposableConfigMixin
from dynastore.modules.db_config.platform_config_service import PluginConfig


class CachePluginConfig(ExposableConfigMixin, PluginConfig):
    """PluginConfig entry for cache module (Valkey timeouts, circuit breaker).

    Live edits via `PUT /api/catalog/v2/configs?plugin_id=module_cache` apply
    immediately — no redeploy needed. Settings are per-catalog overridable.
    """

    _address: ClassVar[Tuple[str, ...]] = ("platform", "module_cache", None)

    # Re-export CacheConfig fields directly for cleaner API
    probe_timeout_seconds: float = Field(
        default=5.0,
        ge=1,
        le=60,
        description=(
            "Timeout for Valkey connection probe at startup (backend.info() call). "
            "Bounds the app lifespan when Valkey is network-unreachable. "
            "Increase for GCP (TLS handshake + cluster discovery may take 10–20s)."
        ),
    )

    socket_connect_timeout_seconds: float = Field(
        default=10.0,
        ge=1,
        le=60,
        description=(
            "Timeout for individual Valkey socket connection attempts. "
            "Applies to each node in cluster mode. "
            "Increase if cluster discovery or node connectivity is slow."
        ),
    )

    circuit_breaker_threshold: int = Field(
        default=3,
        ge=1,
        le=10,
        description=(
            "Consecutive Valkey operation failures before tripping circuit breaker "
            "(falls back to LocalAsyncCacheBackend, logs ERROR, unregisters backend)."
        ),
    )

    oracle_inner_timeout_seconds: float = Field(
        default=0.5,
        ge=0.05,
        le=5.0,
        description=(
            "Upper bound on the inner is_capability_live re-check inside the "
            "reactive reaper's locked DB transaction. The call holds the "
            "connection + advisory xact lock for its duration; a slow Valkey "
            "response convoys peer dispatchers. On timeout the reaper "
            "fails-open (treats as live, never false-DLQs). "
            "0.5 s matches the db_pool_acquire slow-log threshold."
        ),
    )
