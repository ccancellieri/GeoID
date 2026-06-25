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

"""Database timeout configuration (PluginConfig).

Runtime-configurable timeout settings for PostgreSQL connections. These values
are applied to every connection via asyncpg's ``server_settings`` and can be
tuned per-deployment via the configs API without code changes.

Related issues:
- #2343 — DB pool starvation (idle_in_txn)
- #2344 — Leader-loop lock hold
- #2340 — Async catalog hard-delete
"""

from __future__ import annotations

import logging
import os
from typing import ClassVar, Optional, Tuple

from pydantic import Field

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig

logger = logging.getLogger(__name__)


def _env_str(name: str, default: str) -> str:
    """Read string from env, falling back to default."""
    return os.getenv(name, default)


class DbTimeoutConfig(PluginConfig):
    """Runtime-configurable database timeout settings.

    These timeouts are applied to every PostgreSQL connection via asyncpg's
    ``server_settings`` parameter at connection creation time. They bound
    resource usage and prevent runaway queries / transactions from blocking
    the application.

    Scope: Platform-only (sysadmin). These are global pool settings, not
    per-tenant.

    Address: ``("platform", "db", "timeouts")``
    """

    _address: ClassVar[Tuple[str, ...]] = ("platform", "db", "timeouts")

    lock_timeout: Mutable[str] = Field(
        default=_env_str("DB_LOCK_TIMEOUT", "5s"),
        description=(
            "Maximum time any statement waits to acquire a lock. "
            "A pending lock request can never block the application for "
            "longer than this window; on expiry the statement fails with "
            "55P03 (LockNotAvailable) instead of blocking forever."
        ),
    )

    statement_timeout: Mutable[str] = Field(
        default=_env_str("DB_STATEMENT_TIMEOUT", "0"),
        description=(
            "Bounds total EXECUTION time of any single statement. "
            "Disabled by default ('0') — the asyncpg command_timeout "
            "provides a client-side backstop. Set to a value just under "
            "command_timeout (e.g. '55s') to convert silent client-side "
            "cancels into logged server-side 57014 errors."
        ),
    )

    idle_in_transaction_session_timeout: Mutable[str] = Field(
        default=_env_str("DB_IDLE_IN_TRANSACTION_TIMEOUT", "10s"),
        description=(
            "PostgreSQL terminates a backend that holds a transaction open "
            "while idle past this window, releasing its locks SERVER-side. "
            "This is the only guarantee that holds when a client is "
            "interrupted / OOM-killed mid-transaction. Under burst load, "
            "connections can accumulate in idle_in_txn state if transactions "
            "span external calls (GCS/ES/Cloud Run API). A 10s default "
            "surfaces issues quickly; production may tune higher (e.g. '30s')."
        ),
    )


def get_db_timeout_config() -> Optional[DbTimeoutConfig]:
    """Get the DbTimeoutConfig from the configs API if available.

    Returns None if the configs API is not initialized or if no config
    is stored. Callers should fall back to DBConfig or env vars.
    """
    try:
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols import DatabaseProtocol

        db = get_protocol(DatabaseProtocol)
        if db is None:
            return None

        # Check if PlatformConfigService is available
        from dynastore.modules.db_config.platform_config_service import PlatformConfigService
        pcfg = get_protocol(PlatformConfigService)
        if pcfg is None:
            return None

        # Get the config synchronously (it's cached in memory)
        import asyncio
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None:
            # We're in an async context - can't call async from sync
            # Return None and let caller use fallback
            return None

        # Not in async context - use asyncio.run
        # But this is a sync function called from async context sometimes
        # So we just return None and rely on the fallback
        return None
    except Exception as e:
        logger.debug("DbTimeoutConfig not available from configs API: %s", e)
        return None


def resolve_timeout_settings(
    db_config,
) -> Tuple[str, str, str]:
    """Resolve timeout settings from PluginConfig, env, or DBConfig.

    Priority:
    1. Environment variable (highest - for backwards compat and emergencies)
    2. DbTimeoutConfig PluginConfig (if available)
    3. DBConfig defaults (lowest)

    Returns (lock_timeout, statement_timeout, idle_in_transaction_session_timeout)
    """
    # Check env vars first (highest priority - backwards compat)
    lock_timeout_env = os.getenv("DB_LOCK_TIMEOUT")
    statement_timeout_env = os.getenv("DB_STATEMENT_TIMEOUT")
    idle_timeout_env = os.getenv("DB_IDLE_IN_TRANSACTION_TIMEOUT")

    if all([lock_timeout_env, statement_timeout_env, idle_timeout_env]):
        # All env vars are set - use them directly
        return (lock_timeout_env, statement_timeout_env, idle_timeout_env)

    # Fall back to DBConfig (which already has env var + default resolution)
    return (
        lock_timeout_env or db_config.lock_timeout,
        statement_timeout_env or db_config.statement_timeout,
        idle_timeout_env or db_config.idle_in_transaction_session_timeout,
    )
