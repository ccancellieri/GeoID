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

"""Connection retry and health management configuration.

Infra dimensioning constants for PostgreSQL connection retry behavior,
provisioning retries, advisory lock management, and pool health observability.
These are genuine infra dimensioning values, not application-layer configs.
They are read at process startup and remain fixed for the lifetime of the
process.  Set DB_* env vars (where applicable) or db_config.json before
starting the server to change them.

Why a module-global snapshot instead of call-site constants
-----------------------------------------------------------
The consumers here are a mix of sync and async call sites — the module globals
make the current defaults readable by both without any async ceremony. Tests
can also override the globals directly to drive retry counts down (see
``tests/dynastore/modules/db_config/integration/test_configurable_retry.py``).

Related issues:
- #2438 — Connection health management architecture
- #2437 — AUTOCOMMIT + managed_transaction fix
- #2343 — DB pool starvation (idle_in_txn)
- #2509 — Pool-pressure-aware semaphore for connection-retry concurrency
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import ClassVar, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class ConnectionRetryConfig:
    """Infra dimensioning for transient connection retry behavior.

    Controls retry attempts for pool checkout, wire-connect, and brief server
    unavailability. Used by ``retry_on_transient_connect`` and
    ``retry_on_lock_conflict``.

    Scope: platform-wide. Not per-tenant.
    """

    max_retries: int = 5
    base_delay_seconds: float = 0.5
    max_delay_seconds: float = 8.0
    jitter: float = 0.25


@dataclass
class ProvisioningRetryConfig:
    """Infra dimensioning for idempotent provisioning retry behavior.

    Controls retries for provisioning operations that may encounter transient
    errors during long GCP API call windows. Used by
    ``provisioning_write_with_retry``.

    Scope: platform-wide.
    """

    max_attempts: int = 3
    lock_backoff_seconds: float = 1.0


@dataclass
class LeadershipConfig:
    """Infra dimensioning for advisory lock and leader-election behavior.

    Controls advisory lock acquisition, leader election cadence, and liveness
    reconciler behavior.

    Scope: platform-wide.
    """

    lock_acquire_timeout_seconds: int = 30
    dismiss_force_delete_after_seconds: int = 600
    leadership_interval_seconds: float = 20.0
    visibility_extend_seconds: int = 300
    unknown_grace_seconds: int = 180


@dataclass
class _ConnectionHealthInfraConfig:
    """Infra dimensioning for connection health observability.

    Controls the slow pool-acquire logging threshold. Fixed at process
    startup; not hot-reloadable. Use ``resolve_slow_pool_acquire_threshold``
    to read the current value.

    Scope: platform-wide.
    """

    slow_pool_acquire_threshold_seconds: float = 0.5


# ---------------------------------------------------------------------------
# Module-level defaults.
#
# Initialised to the dataclass defaults so ``resolve_*`` is correct at
# process startup. Tests may replace these globals directly to drive
# retry counts down without restarting; the _restore_snapshot fixture in
# the integration test suite undoes those mutations after each test.
# ---------------------------------------------------------------------------
_retry_config: ConnectionRetryConfig = ConnectionRetryConfig()
_provisioning_config: ProvisioningRetryConfig = ProvisioningRetryConfig()
_leadership_config: LeadershipConfig = LeadershipConfig()
_health_config: _ConnectionHealthInfraConfig = _ConnectionHealthInfraConfig()

# Fallback for the concurrent connection-acquisition retry cap.
# Mirrors the default of ConnectionHealthConfig.max_concurrent_connection_retries
# so tests can override this module global without requiring a live platform
# config service.  The semaphore in query_executor reads the live
# ConnectionHealthConfig value per-call (central cached getter) and falls back
# to this global when the config service is unavailable.
_max_concurrent_connection_retries: int = 3

# Fallback for the read-disconnect retry attempt budget (1 original + N-1
# retries). Mirrors the default of
# ConnectionHealthConfig.read_disconnect_retry_attempts so the sync execution
# path and tests can read the value without a live platform config service. The
# async execution path reads the live ConnectionHealthConfig value per-call
# (central cached getter) and falls back to this global when the config service
# is unavailable.
_read_disconnect_retry_attempts: int = 2


def resolve_connection_retry_config() -> Tuple[int, float, float, float]:
    """Return the current ``(max_retries, base_delay, max_delay, jitter)``."""
    c = _retry_config
    return (c.max_retries, c.base_delay_seconds, c.max_delay_seconds, c.jitter)


def resolve_provisioning_retry_config() -> Tuple[int, float]:
    """Return the current ``(max_attempts, lock_backoff_seconds)``."""
    c = _provisioning_config
    return (c.max_attempts, c.lock_backoff_seconds)


def resolve_leadership_config() -> Tuple[int, int, float, int, int]:
    """Return the current leadership settings.

    ``(lock_acquire_timeout_seconds, dismiss_force_delete_after_seconds,
    leadership_interval_seconds, visibility_extend_seconds,
    unknown_grace_seconds)``.
    """
    c = _leadership_config
    return (
        c.lock_acquire_timeout_seconds,
        c.dismiss_force_delete_after_seconds,
        c.leadership_interval_seconds,
        c.visibility_extend_seconds,
        c.unknown_grace_seconds,
    )


def resolve_slow_pool_acquire_threshold() -> float:
    """Return the current slow-pool-acquire logging threshold in seconds."""
    return _health_config.slow_pool_acquire_threshold_seconds


def resolve_max_concurrent_connection_retries() -> int:
    """Return the fallback cap on simultaneous connection-acquisition retries.

    Used by ``query_executor._read_live_retry_limit`` as the fallback value
    when the ``PlatformConfigsProtocol`` config service is unavailable.
    The live path reads ``ConnectionHealthConfig.max_concurrent_connection_retries``
    directly from the central cached getter and does not call this function.
    Tests may replace the global ``_max_concurrent_connection_retries`` directly
    to control the fallback value without a live config service.
    """
    return _max_concurrent_connection_retries


def resolve_read_disconnect_retry_attempts() -> int:
    """Return the fallback total-attempt budget for a mid-flight read disconnect.

    Used by the sync execution path (no async config service to await) and as
    the fallback for the async path when the ``PlatformConfigsProtocol`` config
    service is unavailable (tests, early startup). The async live path reads
    ``ConnectionHealthConfig.read_disconnect_retry_attempts`` directly from the
    central cached getter. Tests may replace the global
    ``_read_disconnect_retry_attempts`` directly to control the value without a
    live config service.
    """
    return _read_disconnect_retry_attempts


# ---------------------------------------------------------------------------
# Hot-reloadable leader-liveness probe configuration.
#
# This PluginConfig is stored in the platform configs table and loaded at
# runtime via PlatformConfigService.get_config(ConnectionHealthConfig).
# Changes take effect on the next leader tick without a pod restart.
#
# The probe runs a cheap SELECT 1 on the dedicated AUTOCOMMIT connection that
# holds the session advisory lock before each LEADER_ONLY tick. If the
# connection wire has died (NAT idle reset, server restart), the probe raises
# DatabaseConnectionError, causing the leader loop to resign and hand the lock
# to another pod. Without this check, a dead wire would be invisible to
# pool_pre_ping (the connection is checked out for the whole tenure) and the
# leader would continue ticking against a ghost connection.
# ---------------------------------------------------------------------------

from pydantic import Field  # noqa: E402 — after stdlib/dataclass section

from dynastore.models.mutability import Mutable  # noqa: E402
from dynastore.models.plugin_config import PluginConfig  # noqa: E402


class ConnectionHealthConfig(PluginConfig):
    """Hot-reloadable configuration for connection health management.

    Stored in the platform configs table and read on each LEADER_ONLY tick via
    ``PlatformConfigService.get_config(ConnectionHealthConfig)``.  All fields
    are ``Mutable`` so operators can adjust them without restarting pods.

    Address: ``("platform", "db", "health")``.
    """

    _address: ClassVar[Tuple[str, ...]] = ("platform", "db", "health")
    _tiers: ClassVar[Optional[Tuple[str, ...]]] = ("platform",)

    leader_liveness_probe_enabled: Mutable[bool] = Field(
        default=True,
        description=(
            "When True (default), a cheap SELECT 1 is executed on the advisory "
            "lock connection before each LEADER_ONLY tick. If the wire has died "
            "(NAT idle reset, DB server restart), this probe detects the failure "
            "and resigns leadership so another pod can take over. Set to False "
            "only if the probe itself causes spurious resignations in a particular "
            "network topology."
        ),
    )

    leader_liveness_probe_timeout_seconds: Mutable[float] = Field(
        default=2.0,
        ge=0.5,
        le=30.0,
        description=(
            "Maximum wall-clock seconds the liveness probe may run before it is "
            "considered failed. A dead TCP socket hangs until the OS connect "
            "timeout fires (often 75 s or more); bounding the probe here ensures "
            "a failed wire is detected within this window rather than stalling "
            "the tick. Must be in [0.5, 30.0] seconds."
        ),
    )

    max_concurrent_connection_retries: Mutable[int] = Field(
        default=3,
        ge=1,
        le=32,
        description=(
            "Maximum number of concurrent connection-acquisition retries allowed "
            "at any instant. When the async PG pool wedges, many callers enter "
            "the retry loop simultaneously and hammer the pool, amplifying the "
            "pressure. This semaphore bounds the number of in-flight retry attempts "
            "so excess callers queue gracefully instead of stampeding. The first "
            "(non-retry) attempt is never gated, preserving happy-path latency. "
            "Conservative default of 3; raise only if you have a large pool and "
            "high parallelism. Read per-call via the central cached config getter "
            "— changes take effect immediately without a pod restart. Must be in "
            "[1, 32]."
        ),
    )

    read_disconnect_retry_attempts: Mutable[int] = Field(
        default=2,
        ge=1,
        le=10,
        description=(
            "Total attempts (1 original + N-1 retries) for a read-only query whose "
            "pooled connection dies mid-flight — it passed pool_pre_ping at "
            "checkout but the wire was killed server-side (NAT idle reset, DB "
            "failover) before the SELECT reached PostgreSQL. Only read-only (DQL) "
            "executions retry; writes and DDL never replay, since a half-applied "
            "write must not be re-run. Each retry invalidates the dead wire and "
            "acquires a fresh pooled connection. Default 2 (one retry) clears "
            "virtually all transient TOCTOU disconnects; raise only if the network "
            "drops idle wires aggressively. Read per-call via the central cached "
            "config getter — changes take effect immediately without a pod "
            "restart. Must be in [1, 10]."
        ),
    )
