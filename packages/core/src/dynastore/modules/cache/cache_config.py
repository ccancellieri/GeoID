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
Cache module configuration.

``CachePluginConfig`` is the **standard** cache config — backend-agnostic
settings shared by every shared-cache implementation (probe + connect
timeouts, circuit breaker). A concrete backend plugin specialises it by
subclassing and adding only the params that plugin uses — see
``ValkeyCachePluginConfig``. Implementation-specific params must not pollute
the standard config.

Registered via PluginConfig framework so settings are:
- Runtime-configurable (no redeploy needed)
- Per-catalog overridable
- Accessible via /configs?plugin_id=module_cache API

This replaces env-var-only configuration (e.g., VALKEY_PROBE_TIMEOUT).
"""

from __future__ import annotations

from typing import ClassVar, Tuple

from pydantic import Field

from dynastore.extensions.tools.exposure_mixin import ExposableConfigMixin
from dynastore.modules.db_config.platform_config_service import Mutable, PluginConfig


class CachePluginConfig(ExposableConfigMixin, PluginConfig):
    """Standard (backend-agnostic) cache config — timeouts + circuit breaker.

    Holds only settings every shared-cache backend understands. Concrete
    backends (Valkey today; Redis, an in-memory DB, etc. tomorrow) specialise
    this via subclassing and add their own implementation-specific params —
    they must not be declared here.

    Live edits via `PUT /api/catalog/v2/configs?plugin_id=module_cache` apply
    immediately — no redeploy needed. Settings are per-catalog overridable.
    """

    _address: ClassVar[Tuple[str, ...]] = ("platform", "module_cache", None)

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

    socket_connect_timeout_seconds: Mutable[float] = Field(
        default=10.0,
        ge=1,
        le=60,
        description=(
            "Timeout for individual cache-backend socket connection attempts. "
            "Applies to each node in cluster mode. "
            "Increase if cluster discovery or node connectivity is slow."
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


class ValkeyCachePluginConfig(CachePluginConfig):
    """Valkey-specific cache config — specialises the standard cache config.

    Adds the connection knobs that only the Valkey backend uses. These exist
    here, not on ``CachePluginConfig``, so the standard config stays
    backend-agnostic. Inherits every standard field, so ``CacheModule`` can
    load this one class and still read ``probe_timeout_seconds`` etc.
    """

    # Same topic as the standard cache config — exposed as a distinct
    # ClassName under ``module_cache`` (mirrors how gcp_config registers
    # several classes under one address).
    _address: ClassVar[Tuple[str, ...]] = ("platform", "module_cache", None)

    socket_timeout_seconds: Mutable[float] = Field(
        default=15.0,
        ge=1,
        le=120,
        description=(
            "Timeout for individual Valkey socket read/write operations "
            "(once connected). valkey-py defaults this to 5s; on GCP a cold "
            "cluster-topology fetch (CLUSTER SLOTS / COMMAND) can exceed that "
            "and surface as a read TimeoutError. Bounds each op so a slow "
            "node trips the circuit breaker deterministically instead of "
            "convoying callers."
        ),
    )

    tcp_keepalive_idle_seconds: Mutable[int] = Field(
        default=300,
        ge=10,
        le=1200,
        description=(
            "TCP_KEEPIDLE — seconds an idle Valkey socket waits before the "
            "first keepalive probe. Keeps Cloud NAT from silently dropping "
            "idle Cloud Run↔Memorystore sockets (mirrors the DB-pool "
            "keepalive hygiene)."
        ),
    )

    tcp_keepalive_interval_seconds: Mutable[int] = Field(
        default=30,
        ge=5,
        le=300,
        description="TCP_KEEPINTVL — seconds between Valkey keepalive probes.",
    )

    tcp_keepalive_count: Mutable[int] = Field(
        default=5,
        ge=1,
        le=20,
        description=(
            "TCP_KEEPCNT — failed keepalive probes before the Valkey socket "
            "is considered dead."
        ),
    )
