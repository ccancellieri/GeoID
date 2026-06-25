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

import logging
import os
import socket
from contextlib import asynccontextmanager
from typing import Optional, Any, Protocol, runtime_checkable

# Hard-import the async PG driver at module load.  When SCOPE excludes
# ``module_db`` (e.g. Cloud Run jobs that use ``db_sync`` + DatastoreModule
# only), asyncpg is genuinely not installed.  Without this import,
# ``create_async_engine(postgresql+asyncpg://…)`` blows up deep inside
# the SQLAlchemy lifespan with a ModuleNotFoundError that re-raises as
# ``CRITICAL: Foundational module 'DBService' failed during startup``.
# Failing here instead lets the module-discovery layer
# (modules/__init__.py) catch the ImportError on __init__, set
# ``instance=None``, and silently skip the lifespan — exactly the same
# wrong-SCOPE-soft-skip contract used by GCP/ES/dwh/export/gdal/ingestion
# tasks.  Same fix family as project_geoid_task_routing_config v0.5.86–89.
import asyncpg  # noqa: F401  — gate the entry-point on the async driver

from sqlalchemy import event
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy.engine import Engine
from dynastore.modules import ModuleProtocol
from dynastore.modules.db_config.db_config import DBConfig
from dynastore.modules.db_config.db_timeout_config import resolve_timeout_settings
from dynastore.modules.db_config.tools import (
    get_config,
    normalize_db_url,
)

from dynastore.models.protocols import DatabaseProtocol

logger = logging.getLogger(__name__)


def _arm_client_socket_keepalive(engine: AsyncEngine, db_config: DBConfig) -> None:
    """Arm SO_KEEPALIVE + TCP_USER_TIMEOUT on every asyncpg client socket (#710).

    asyncpg exposes no libpq client-side keepalive params, so the keepalive
    GUCs we pass via ``server_settings`` only make the *server* probe the
    *client*. They never arm ``SO_KEEPALIVE`` on the client socket, so a
    connection silently dropped by the VPC-egress path is discovered only when
    ``pool_pre_ping`` borrows the dead socket — and that probe then blocks up
    to ``connect_timeout`` (the seconds-long ``db_pool_acquire`` waits seen in
    review). Arming the client socket lets the kernel detect the dead peer in
    keepalive / ``TCP_USER_TIMEOUT`` time instead, and the periodic probes keep
    the egress mapping warm so idle connections stop dying in the first place.

    Reuses the same ``DBConfig.tcp_keepalives_*`` values as the server-side
    GUCs so one set of knobs governs both directions. Linux-only socket
    options are applied best-effort; options missing on the platform (e.g.
    macOS dev) and any error are swallowed so connection creation never fails.
    """
    socket_opts: list[tuple[int, int]] = []
    for opt_name, value in (
        ("TCP_KEEPIDLE", db_config.tcp_keepalives_idle),
        ("TCP_KEEPINTVL", db_config.tcp_keepalives_interval),
        ("TCP_KEEPCNT", db_config.tcp_keepalives_count),
        ("TCP_USER_TIMEOUT", db_config.tcp_user_timeout_ms),
    ):
        opt = getattr(socket, opt_name, None)
        if opt is not None:
            socket_opts.append((opt, int(value)))

    @event.listens_for(engine.sync_engine, "connect")
    def _on_connect(dbapi_connection: Any, _record: Any) -> None:
        try:
            raw = getattr(dbapi_connection, "driver_connection", None)
            transport = getattr(raw, "_transport", None)
            sock = (
                transport.get_extra_info("socket")
                if transport is not None
                else None
            )
            if sock is None:
                return
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            for opt, value in socket_opts:
                sock.setsockopt(socket.IPPROTO_TCP, opt, value)
        except Exception:
            # Best-effort hardening — never let socket tuning break a
            # connection. The pool_recycle backstop still covers stale slots.
            logger.debug(
                "DBService: client-side TCP keepalive arming skipped",
                exc_info=True,
            )


@runtime_checkable
class DBServiceAppState(Protocol):
    """Shape of `app_state` consumed by DBService.

    db_config / engine are installed by the db_config module before lifespan runs.
    sync_engine may be set by datastore for sync SQLAlchemy fallbacks.
    """
    db_config: DBConfig
    engine: Optional[AsyncEngine]
    sync_engine: Optional[Engine]


class DBService(ModuleProtocol, DatabaseProtocol):
    priority: int = 10
    app_state: DBServiceAppState

    def __init__(self, app_state: DBServiceAppState):
        self.app_state = app_state

    @property
    def engine(self) -> Any:
        """DatabaseProtocol implementation."""
        engine = getattr(self.app_state, "engine", None)
        if engine:
            return engine
        engine = getattr(self.app_state, "sync_engine", None)
        if engine:
            return engine
        raise RuntimeError("No database engine available (sync or async).")

    @property
    def async_engine(self) -> Optional[AsyncEngine]:
        """DatabaseProtocol implementation."""
        return getattr(self.app_state, "engine", None)

    @property
    def sync_engine(self) -> Optional[Any]:
        """DatabaseProtocol implementation."""
        return getattr(self.app_state, "sync_engine", None)

    def get_any_engine(self) -> Optional[Any]:
        """DatabaseProtocol implementation."""
        from dynastore.tools.protocol_helpers import get_engine

        try:
            return get_engine()
        except RuntimeError:
            return None

    def get_engine(self) -> Optional[AsyncEngine]:
        # Legacy method for backward compatibility
        return self.engine

    async def apply_connection_adapters(self, connection: Any) -> None:
        """asyncpg handles JSONB natively — no-op for async connections."""
        return

    @asynccontextmanager
    async def lifespan(self, app_state: DBServiceAppState):
        """
        Manages the lifespan of the async database engine.
        """
        logger.info("DBService: Async database connection startup initiated...")

        if not hasattr(app_state, "db_config"):
            raise RuntimeError(
                "db_config not found in app_state. Ensure 'db_config' module is loaded before 'db'."
            )

        db_config: DBConfig = get_config(app_state)

        # Check if engine is already injected (e.g. by tests)
        existing_engine = getattr(app_state, "engine", None)
        engine_created_by_service = False

        if existing_engine:
            logger.info("DBService: Using existing engine from app_state.")
        else:
            app_state.engine = None

        try:
            if not existing_engine:
                logger.info(
                    f"DBService: Using DB configuration: {db_config.database_url}"
                )

                # Tag every wire-level connection with the logical service
                # name so DB-side ``pg_stat_activity.application_name`` is
                # populated. Without this every connection shows as the empty
                # string and per-service contention cannot be diagnosed from
                # the DB side. See #699 / #655.
                from dynastore.modules.db_config.instance import (
                    get_service_name,
                )
                app_name = get_service_name() or os.getenv(
                    "SERVICE_NAME"
                ) or "dynastore"

                # Resolve timeout settings from PluginConfig, env, or DBConfig
                (
                    lock_timeout,
                    statement_timeout,
                    idle_in_transaction_session_timeout,
                ) = resolve_timeout_settings(db_config)

                # 1. Create Engine
                app_state.engine = create_async_engine(
                    normalize_db_url(db_config.database_url, is_async=True),
                    pool_size=db_config.pool_min_size,
                    max_overflow=db_config.pool_max_overflow,
                    # pool_timeout = max seconds to wait for a free slot from
                    # QueuePool before raising sqlalchemy.exc.TimeoutError
                    # (fail-fast; not the statement/command execution budget).
                    # Previously this was fed pool_command_timeout (60s) which
                    # is the wrong semantic — see DBConfig.pool_acquire_timeout
                    # and #1894.
                    pool_timeout=db_config.pool_acquire_timeout,
                    pool_pre_ping=True,
                    pool_recycle=db_config.pool_recycle,
                    connect_args={
                        "timeout": db_config.connect_timeout,
                        # asyncpg has no libpq client-side keepalive params;
                        # the equivalent server-side GUCs must be passed as
                        # strings via server_settings so Cloud NAT never
                        # silently drops the idle mapping. See #655.
                        "server_settings": {
                            "application_name": app_name,
                            "tcp_keepalives_idle": str(
                                db_config.tcp_keepalives_idle
                            ),
                            "tcp_keepalives_interval": str(
                                db_config.tcp_keepalives_interval
                            ),
                            "tcp_keepalives_count": str(
                                db_config.tcp_keepalives_count
                            ),
                            # Bounded lock windows on every connection so a
                            # stuck DDL or a leaked / interrupted transaction
                            # can never block the whole application. lock_timeout
                            # caps how long any statement waits to acquire a
                            # lock; idle_in_transaction_session_timeout makes
                            # PostgreSQL release a held lock server-side when a
                            # transaction is left open idle — even if the client
                            # was interrupted and never rolled back. See
                            # DBConfig.lock_timeout.
                            "lock_timeout": lock_timeout,
                            "idle_in_transaction_session_timeout": (
                                idle_in_transaction_session_timeout
                            ),
                            # statement_timeout bounds total statement EXECUTION
                            # (not just the lock wait). "0" = disabled (default,
                            # historical behaviour). Set DB_STATEMENT_TIMEOUT just
                            # under pool_command_timeout to turn a silent 60s
                            # client-side cancel into a logged 57014 naming the
                            # slow query. SET LOCAL in long jobs overrides it.
                            "statement_timeout": statement_timeout,
                        },
                    },
                )
                # Arm client-side TCP keepalive on every asyncpg socket so a
                # silently-dropped idle connection is detected fast instead of
                # hanging the next pool_pre_ping for connect_timeout (#710).
                _arm_client_socket_keepalive(app_state.engine, db_config)
                engine_created_by_service = True
                logger.info(
                    "DBService: ASYNC Database connection pool established successfully."
                )

            # Self-heal the base Postgres extensions (postgis et al.) on the
            # async engine before serving traffic. #1748 gated the sync-engine
            # DatastoreModule — the historical owner of this bootstrap — off the
            # API/catalog SCOPE, so on a freshly-provisioned database the catalog
            # service would otherwise have NO path to ``CREATE EXTENSION postgis``
            # and every geometry-typed write fails with "type geometry does not
            # exist". Runs on the asyncpg engine, so it does NOT re-introduce the
            # sync psycopg2 engine #1748 removed from API services. The call is
            # guarded (DB-backed presence check, Valkey-cached positive keyed by
            # database identity), so across the multi-Cloud-Run fleet the steady
            # state is a single cache read, not repeated DDL on every pod boot.
            # Best-effort: a failure here must never abort foundational startup.
            _async_engine = getattr(app_state, "engine", None)
            if _async_engine is not None:
                try:
                    from dynastore.modules.db_config.tools import (
                        ensure_base_extensions,
                    )

                    await ensure_base_extensions(_async_engine)
                except Exception:
                    logger.warning(
                        "DBService: base-extension ensure failed (best-effort) — "
                        "continuing startup; geometry-typed writes may fail until "
                        "the extensions exist.",
                        exc_info=True,
                    )

            yield

        except Exception as e:
            logger.critical(
                f"DBService: FATAL: Failed to create database connection pool: {e}",
                exc_info=True,
            )
            raise
        finally:
            logger.info("DBService: Database connection shutdown initiated...")
            # Only dispose if we created it
            if (
                engine_created_by_service
                and hasattr(app_state, "engine")
                and app_state.engine
            ):
                await app_state.engine.dispose()
                app_state.engine = None
                logger.info("DBService: Database connection pool closed.")
            logger.info("DBService: Database connection shutdown completed.")
