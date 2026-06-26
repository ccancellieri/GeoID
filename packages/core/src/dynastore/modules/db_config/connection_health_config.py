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
provisioning retries, and advisory lock management. These values can be
tuned per-deployment via the configs API without code changes.

Related issues:
- #2438 — Connection health management architecture
- #2437 — AUTOCOMMIT + managed_transaction fix
- #2343 — DB pool starvation (idle_in_txn)
"""

from __future__ import annotations

import logging
import os
from typing import ClassVar, Tuple

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
    """Runtime-configurable connection health check settings.

    These settings control proactive connection health validation,
    pool hygiene, and monitoring thresholds.

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

    advisory_lock_validation_enabled: Mutable[bool] = Field(
        default=False,
        description=(
            "Enable proactive validation of advisory locks on AUTOCOMMIT connections. "
            "When True, checks ``pg_locks`` before using advisory lock connection. "
            "Adds overhead but catches silently-dropped locks. "
            "Default: False (rely on pool_pre_ping)."
        ),
    )

    connection_health_check_interval_seconds: Mutable[int] = Field(
        default=30,
        ge=10,
        le=300,
        description=(
            "Interval between proactive connection health checks. "
            "Checks run in background to detect stale connections early. "
            "Default: 30 seconds."
        ),
    )

    circuit_breaker_threshold: Mutable[int] = Field(
        default=5,
        ge=1,
        le=20,
        description=(
            "Consecutive connection failures before circuit breaker trips. "
            "When tripped, operations fail fast without retrying. "
            "Default: 5 failures."
        ),
    )

    circuit_breaker_recovery_seconds: Mutable[int] = Field(
        default=60,
        ge=10,
        le=300,
        description=(
            "Time to wait before attempting recovery after circuit breaker trips. "
            "Default: 60 seconds."
        ),
    )


def _env_int(name: str, default: int) -> int:
    """Read integer from env, falling back to default."""
    val = os.getenv(name)
    if val is not None:
        try:
            return int(val)
        except ValueError:
            logger.warning(
                "Invalid int value for %s: %s, using default %s", name, val, default
            )
    return default


def _env_float(name: str, default: float) -> float:
    """Read float from env, falling back to default."""
    val = os.getenv(name)
    if val is not None:
        try:
            return float(val)
        except ValueError:
            logger.warning(
                "Invalid float value for %s: %s, using default %s", name, val, default
            )
    return default


def resolve_connection_retry_config() -> Tuple[int, float, float, float]:
    """Resolve connection retry settings with env var override support.

    Priority:
    1. Environment variables (highest - for backwards compat and emergencies)
    2. Defaults from ConnectionRetryConfig

    Returns (max_retries, base_delay_seconds, max_delay_seconds, jitter)
    """
    return (
        _env_int(
            "DB_RETRY_MAX_RETRIES",
            ConnectionRetryConfig.model_fields["max_retries"].default,
        ),
        _env_float(
            "DB_RETRY_BASE_DELAY_SECONDS",
            ConnectionRetryConfig.model_fields["base_delay_seconds"].default,
        ),
        _env_float(
            "DB_RETRY_MAX_DELAY_SECONDS",
            ConnectionRetryConfig.model_fields["max_delay_seconds"].default,
        ),
        _env_float(
            "DB_RETRY_JITTER", ConnectionRetryConfig.model_fields["jitter"].default
        ),
    )


def resolve_provisioning_retry_config() -> Tuple[int, float]:
    """Resolve provisioning retry settings with env var override support.

    Priority:
    1. Environment variables (highest - for backwards compat and emergencies)
    2. Defaults from ProvisioningRetryConfig

    Returns (max_attempts, lock_backoff_seconds)
    """
    return (
        _env_int(
            "DB_PROVISIONING_RETRY_ATTEMPTS",
            ProvisioningRetryConfig.model_fields["max_attempts"].default,
        ),
        _env_float(
            "DB_PROVISIONING_LOCK_BACKOFF_SECONDS",
            ProvisioningRetryConfig.model_fields["lock_backoff_seconds"].default,
        ),
    )


def resolve_leadership_config() -> Tuple[int, int, float, int, int]:
    """Resolve leadership settings with env var override support.

    Priority:
    1. Environment variables (highest - for backwards compat and emergencies)
    2. Defaults from LeadershipConfig

    Returns (lock_acquire_timeout_seconds, dismiss_force_delete_after_seconds,
             leadership_interval_seconds, visibility_extend_seconds,
             unknown_grace_seconds)
    """
    return (
        _env_int(
            "DB_LEADERSHIP_LOCK_TIMEOUT_SECONDS",
            LeadershipConfig.model_fields["lock_acquire_timeout_seconds"].default,
        ),
        _env_int(
            "DB_LEADERSHIP_DISMISS_FORCE_DELETE_SECONDS",
            LeadershipConfig.model_fields["dismiss_force_delete_after_seconds"].default,
        ),
        _env_float(
            "DB_LEADERSHIP_INTERVAL_SECONDS",
            LeadershipConfig.model_fields["leadership_interval_seconds"].default,
        ),
        _env_int(
            "DB_LEADERSHIP_VISIBILITY_EXTEND_SECONDS",
            LeadershipConfig.model_fields["visibility_extend_seconds"].default,
        ),
        _env_int(
            "DB_LEADERSHIP_UNKNOWN_GRACE_SECONDS",
            LeadershipConfig.model_fields["unknown_grace_seconds"].default,
        ),
    )


def resolve_connection_health_config() -> Tuple[float, bool, int, int, int]:
    """Resolve connection health settings with env var override support.

    Priority:
    1. Environment variables (highest - for backwards compat and emergencies)
    2. Defaults from ConnectionHealthConfig

    Returns (slow_pool_acquire_threshold_seconds, advisory_lock_validation_enabled,
             connection_health_check_interval_seconds, circuit_breaker_threshold,
             circuit_breaker_recovery_seconds)
    """
    val = os.getenv("DB_HEALTH_ADVISORY_LOCK_VALIDATION_ENABLED")
    advisory_validation = (
        val.lower() in ("true", "1", "yes")
        if val is not None
        else ConnectionHealthConfig.model_fields[
            "advisory_lock_validation_enabled"
        ].default
    )

    return (
        _env_float(
            "DB_HEALTH_SLOW_POOL_ACQUIRE_SECONDS",
            ConnectionHealthConfig.model_fields[
                "slow_pool_acquire_threshold_seconds"
            ].default,
        ),
        advisory_validation,
        _env_int(
            "DB_HEALTH_CHECK_INTERVAL_SECONDS",
            ConnectionHealthConfig.model_fields[
                "connection_health_check_interval_seconds"
            ].default,
        ),
        _env_int(
            "DB_HEALTH_CIRCUIT_BREAKER_THRESHOLD",
            ConnectionHealthConfig.model_fields["circuit_breaker_threshold"].default,
        ),
        _env_int(
            "DB_HEALTH_CIRCUIT_BREAKER_RECOVERY_SECONDS",
            ConnectionHealthConfig.model_fields[
                "circuit_breaker_recovery_seconds"
            ].default,
        ),
    )
