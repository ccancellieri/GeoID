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

"""
Cache module configuration (probe timeout, circuit breaker, oracle timeout).

Registered via PluginConfig framework so settings are:
- Runtime-configurable (no redeploy needed)
- Per-catalog overridable
- Accessible via /configs?plugin_id=cache_plugin_config API

**Migration note (operators):** Connection params (socket timeouts, TCP keepalives,
TLS, IAM, cluster settings) have moved to ``ValkeyEngineConfig`` at
``("platform", "protocols", "storage")``. The engine config supports live reconnect on change
without process restart. This config only holds cache-layer settings that don't
control the underlying connection.
"""

from __future__ import annotations

from typing import ClassVar, Tuple

from pydantic import Field

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig


class CachePluginConfig(PluginConfig):
    """PluginConfig entry for cache module (probe timeout, circuit breaker).

    Live edits via `PUT /api/catalog/v2/configs?plugin_id=cache_plugin_config`
    apply immediately — no redeploy needed. Settings are per-catalog overridable.

    **Connection tuning:** See ``ValkeyEngineConfig`` for socket timeouts,
    TCP keepalives, TLS, IAM auth, and cluster discovery settings.
    """

    _address: ClassVar[Tuple[str, ...]] = ("platform", "modules", "cache")

    probe_timeout_seconds: Mutable[float] = Field(
        default=5.0,
        ge=1,
        le=60,
        description=(
            "Timeout for Valkey connection probe at startup (backend.info() call). "
            "Bounds the app lifespan when Valkey is network-unreachable. "
            "Increase for GCP (TLS handshake + cluster discovery may take 10–20s)."
        ),
    )

    circuit_breaker_threshold: Mutable[int] = Field(
        default=3,
        ge=1,
        le=10,
        description=(
            "Consecutive Valkey operation failures before tripping circuit breaker "
            "(falls back to LocalAsyncCacheBackend, logs ERROR, unregisters backend)."
        ),
    )

    oracle_inner_timeout_seconds: Mutable[float] = Field(
        default=0.5,
        ge=0.05,
        le=5.0,
        description=(
            "Upper bound on the inner is_capability_live re-check inside the "
            "reactive reaper's locked DB transaction. The call holds the "
            "connection + advisory xact lock for its duration; a slow cache "
            "response convoys peer dispatchers. On timeout the reaper "
            "fails-open (treats as live, never false-DLQs). "
            "0.5 s matches the db_pool_acquire slow-log threshold."
        ),
    )

    slow_path_timeout_seconds: Mutable[float] = Field(
        default=30.0,
        ge=1,
        le=120,
        description=(
            "Upper bound on a cache-miss slow path: per-key stampede lock "
            "wait plus the factory rebuild call, for both "
            "LocalCache.get_or_set() and the @cached decorator. Without this "
            "a rebuild stalled behind a starved DB pool rides every queued "
            "waiter to the caller's own timeout. On expiry, a stale value "
            "still within its grace window is served instead (see "
            "@cached(stale_grace=...))."
        ),
    )
