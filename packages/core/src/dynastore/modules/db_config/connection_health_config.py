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

"""Connection retry and health management configuration (PluginConfig).

Runtime-configurable settings for PostgreSQL connection retry behavior,
provisioning retries, and advisory lock management. Values are tuned at
runtime via the configs API — a ``PUT /configs`` takes effect without a
restart — exactly like every other ``PluginConfig`` in the platform.

Why a module-global snapshot instead of reading the config on every call
--------------------------------------------------------------------------
The configs store is **async-only** (``await mgr.get_config(...)``), but the
consumers here are a mix of sync and async call sites — ``retry_on_lock_conflict``
and ``provisioning_write_with_retry`` are async, while ``sync_acquire_startup_lock``
and the ``GcpLivenessReconciler.__init__`` constructor are synchronous and
cannot ``await``. Awaiting in the retry hot-path would also add a checkout per
attempt.

So we hold the current config instances in module globals, populated once at
startup by :func:`load_connection_health_configs` (async, called from
``DBConfigModule.lifespan``) and refreshed on every change by the apply
handlers registered via :func:`register_connection_health_apply_handlers`.
The ``resolve_*`` helpers then read those globals synchronously. This is the
same live-apply pattern ``cache_module`` and ``gcp_module`` already use for
their configs.

Related issues:
- #2438 — Connection health management architecture
- #2437 — AUTOCOMMIT + managed_transaction fix
- #2343 — DB pool starvation (idle_in_txn)
"""

from __future__ import annotations

import logging
from typing import Any, ClassVar, Optional, Tuple

from pydantic import Field

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig

logger = logging.getLogger(__name__)


class ConnectionRetryConfig(PluginConfig):
    """Runtime-configurable connection retry settings.

    These settings control retry behavior for transient connection failures
    (pool checkout, wire connect, brief server unavailability). Used by
    :func:`retry_on_transient_connect` and :func:`retry_on_lock_conflict`.

    Scope: Platform-only (sysadmin). These are global connection retry settings.

    Address: ``("platform", "db", "retry")``
    """

    _address: ClassVar[Tuple[str, ...]] = ("platform", "db", "retry")

    max_retries: Mutable[int] = Field(
        default=5,
        ge=1,
        le=20,
        description=(
            "Maximum retry attempts for transient connection failures. "
            "Each attempt uses exponential backoff with jitter. "
            "Default: 5 attempts (~15s total budget at default delays)."
        ),
    )

    base_delay_seconds: Mutable[float] = Field(
        default=0.5,
        ge=0.1,
        le=10.0,
        description=(
            "Initial delay between retry attempts in seconds. "
            "Doubles with each attempt (exponential backoff). "
            "Default: 0.5s (first retry after ~0.5s)."
        ),
    )

    max_delay_seconds: Mutable[float] = Field(
        default=8.0,
        ge=1.0,
        le=60.0,
        description=(
            "Maximum delay cap between retries in seconds. "
            "Prevents exponential backoff from growing unbounded. "
            "Default: 8.0s (caps at ~8s even after many attempts)."
        ),
    )

    jitter: Mutable[float] = Field(
        default=0.25,
        ge=0.0,
        le=1.0,
        description=(
            "Random jitter factor (0.0-1.0) applied to retry delays. "
            "Prevents thundering herd on coordinated retries. "
            "Default: 0.25 (±25% randomization)."
        ),
    )


class ProvisioningRetryConfig(PluginConfig):
    """Runtime-configurable provisioning retry settings.

    These settings control retry behavior for idempotent provisioning operations
    that may encounter transient errors during long GCP API call windows.
    Used by :func:`provisioning_write_with_retry`.

    Scope: Platform-only (sysadmin).

    Address: ``("platform", "db", "provisioning_retry")``
    """

    _address: ClassVar[Tuple[str, ...]] = ("platform", "db", "provisioning_retry")

    max_attempts: Mutable[int] = Field(
        default=3,
        ge=1,
        le=10,
        description=(
            "Maximum retry attempts for provisioning operations. "
            "Each retry uses a FRESH connection from the pool. "
            "Default: 3 attempts."
        ),
    )

    lock_backoff_seconds: Mutable[float] = Field(
        default=1.0,
        ge=0.1,
        le=10.0,
        description=(
            "Backoff delay multiplier for lock contention errors (55P03). "
            "Actual delay: ``lock_backoff * attempt`` seconds. "
            "Gives PostgreSQL time to release conflicting locks. "
            "Default: 1.0s (1s, 2s, 3s backoff)."
        ),
    )


class LeadershipConfig(PluginConfig):
    """Runtime-configurable leadership and advisory lock settings.

    These settings control advisory lock acquisition, leader election,
    and liveness reconciler behavior.

    Scope: Platform-only (sysadmin).

    Address: ``("platform", "db", "leadership")``
    """

    _address: ClassVar[Tuple[str, ...]] = ("platform", "db", "leadership")

    lock_acquire_timeout_seconds: Mutable[int] = Field(
        default=30,
        ge=5,
        le=300,
        description=(
            "Maximum time to wait for advisory lock acquisition. "
            "Applied via ``SET LOCAL lock_timeout`` before lock attempt. "
            "Default: 30 seconds."
        ),
    )

    dismiss_force_delete_after_seconds: Mutable[int] = Field(
        default=600,
        ge=60,
        le=3600,
        description=(
            "Grace period before force-deleting dismissed liveness records. "
            "Prevents immediate deletion for audit/observability. "
            "Default: 600 seconds (10 minutes)."
        ),
    )

    leadership_interval_seconds: Mutable[float] = Field(
        default=20.0,
        ge=5.0,
        le=300.0,
        description=(
            "Interval between leadership tick executions. "
            "Controls how often LEADER_ONLY services run. "
            "Default: 20.0 seconds."
        ),
    )

    visibility_extend_seconds: Mutable[int] = Field(
        default=300,
        ge=60,
        le=3600,
        description=(
            "Visibility timeout extension for leadership claims. "
            "Should be > leadership_interval_seconds. "
            "Default: 300 seconds (5 minutes)."
        ),
    )

    unknown_grace_seconds: Mutable[int] = Field(
        default=180,
        ge=30,
        le=900,
        description=(
            "Grace period before marking unknown instances as stale. "
            "Allows for temporary network partitions. "
            "Default: 180 seconds (3 minutes)."
        ),
    )


class ConnectionHealthConfig(PluginConfig):
    """Runtime-configurable connection health observability settings.

    Scope: Platform-only (sysadmin).

    Address: ``("platform", "db", "health")``
    """

    _address: ClassVar[Tuple[str, ...]] = ("platform", "db", "health")

    slow_pool_acquire_threshold_seconds: Mutable[float] = Field(
        default=0.5,
        ge=0.1,
        le=5.0,
        description=(
            "Threshold for slow pool acquisition logging. "
            "Acquisitions slower than this trigger INFO-level logs. "
            "Default: 0.5 seconds (500ms)."
        ),
    )


# ---------------------------------------------------------------------------
# Live snapshot of the current config instances.
#
# Initialised to the validated class defaults so ``resolve_*`` is correct even
# before the configs store is reachable (early boot, tests). Replaced wholesale
# by :func:`load_connection_health_configs` at startup and by the apply handlers
# on every ``PUT /configs``.
# ---------------------------------------------------------------------------
_retry_config: ConnectionRetryConfig = ConnectionRetryConfig()
_provisioning_config: ProvisioningRetryConfig = ProvisioningRetryConfig()
_leadership_config: LeadershipConfig = LeadershipConfig()
_health_config: ConnectionHealthConfig = ConnectionHealthConfig()


def resolve_connection_retry_config() -> Tuple[int, float, float, float]:
    """Return the live ``(max_retries, base_delay, max_delay, jitter)``.

    Reads the module snapshot kept current by the configs apply handlers, so
    a ``PUT /configs`` for ``ConnectionRetryConfig`` is reflected on the next
    call without a restart.
    """
    c = _retry_config
    return (c.max_retries, c.base_delay_seconds, c.max_delay_seconds, c.jitter)


def resolve_provisioning_retry_config() -> Tuple[int, float]:
    """Return the live ``(max_attempts, lock_backoff_seconds)``."""
    c = _provisioning_config
    return (c.max_attempts, c.lock_backoff_seconds)


def resolve_leadership_config() -> Tuple[int, int, float, int, int]:
    """Return the live leadership settings.

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
    """Return the live slow-pool-acquire logging threshold in seconds."""
    return _health_config.slow_pool_acquire_threshold_seconds


# ---------------------------------------------------------------------------
# Apply handlers — fire post-persist on every ``PUT /configs`` for these
# classes (see ``platform_config_service.run_apply_handlers``). Signature is
# the house ``(config, catalog_id, collection_id, conn)``.
# ---------------------------------------------------------------------------
async def _apply_retry_config(config: Any, *_: Any) -> None:
    global _retry_config
    if isinstance(config, ConnectionRetryConfig):
        _retry_config = config
        logger.info(
            "ConnectionRetryConfig live-applied: max_retries=%d", config.max_retries
        )


async def _apply_provisioning_config(config: Any, *_: Any) -> None:
    global _provisioning_config
    if isinstance(config, ProvisioningRetryConfig):
        _provisioning_config = config
        logger.info(
            "ProvisioningRetryConfig live-applied: max_attempts=%d", config.max_attempts
        )


async def _apply_leadership_config(config: Any, *_: Any) -> None:
    global _leadership_config
    if isinstance(config, LeadershipConfig):
        _leadership_config = config
        logger.info(
            "LeadershipConfig live-applied: interval_seconds=%.1f",
            config.leadership_interval_seconds,
        )


async def _apply_health_config(config: Any, *_: Any) -> None:
    global _health_config
    if isinstance(config, ConnectionHealthConfig):
        _health_config = config
        logger.info(
            "ConnectionHealthConfig live-applied: "
            "slow_pool_acquire_threshold_seconds=%.2f",
            config.slow_pool_acquire_threshold_seconds,
        )


_APPLY_HANDLERS = (
    (ConnectionRetryConfig, _apply_retry_config),
    (ProvisioningRetryConfig, _apply_provisioning_config),
    (LeadershipConfig, _apply_leadership_config),
    (ConnectionHealthConfig, _apply_health_config),
)


def register_connection_health_apply_handlers() -> None:
    """Register apply handlers so live config edits update the snapshot.

    Call once from ``DBConfigModule.lifespan``; pair with
    :func:`unregister_connection_health_apply_handlers` on teardown.
    """
    for cls, handler in _APPLY_HANDLERS:
        cls.register_apply_handler(handler)


def unregister_connection_health_apply_handlers() -> None:
    """Detach the apply handlers registered by
    :func:`register_connection_health_apply_handlers`."""
    for cls, handler in _APPLY_HANDLERS:
        cls.unregister_apply_handler(handler)


async def load_connection_health_configs(engine: Optional[Any] = None) -> None:
    """Populate the module snapshot from the configs store at startup.

    Best-effort: any failure (store not yet reachable, storage table absent)
    leaves the validated class defaults in place — the apply handlers will
    catch the values on the next seed/``PUT``. Call from
    ``DBConfigModule.lifespan`` once ``PlatformConfigsProtocol`` is registered.
    """
    global _retry_config, _provisioning_config, _leadership_config, _health_config

    try:
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
    except Exception:  # pragma: no cover - import wiring only
        return

    mgr = get_protocol(PlatformConfigsProtocol)
    if mgr is None:
        return

    ctx = None
    if engine is not None:
        try:
            from dynastore.models.driver_context import DriverContext

            ctx = DriverContext(db_resource=engine)
        except Exception:
            ctx = None

    async def _read(cls):
        try:
            cfg = await mgr.get_config(cls, ctx=ctx)
            return cfg if isinstance(cfg, cls) else None
        except Exception as e:
            logger.debug(
                "connection_health_config: could not load %s: %s", cls.__name__, e
            )
            return None

    retry = await _read(ConnectionRetryConfig)
    if retry is not None:
        _retry_config = retry
    provisioning = await _read(ProvisioningRetryConfig)
    if provisioning is not None:
        _provisioning_config = provisioning
    leadership = await _read(LeadershipConfig)
    if leadership is not None:
        _leadership_config = leadership
    health = await _read(ConnectionHealthConfig)
    if health is not None:
        _health_config = health
