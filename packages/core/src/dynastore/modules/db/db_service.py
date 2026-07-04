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
import time
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, List, Optional, Any, Protocol, runtime_checkable
from uuid import uuid4

if TYPE_CHECKING:
    from dynastore.models.scaling import ScalingSignal

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
from dynastore.modules.db_config.db_timeout_config import (
    clamp_serving_statement_timeout,
    lock_safety_server_settings,
    resolve_timeout_settings,
)
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


class PgPoolSignalProvider:
    """``ScalingSignalProtocol``: this pod's PostgreSQL connection-pool saturation.

    Reports ``checkedout() / (pool_min_size + pool_max_overflow)`` on the
    SQLAlchemy async engine this module builds — the same pool whose
    acquire waits ``query_executor._acquire_async_engine_connection`` logs
    as ``db_pool_acquire slow``. Before this provider existed, the only
    instance-scope signal feeding the autoscaling control loop
    (``modules/scaling``) was ``DuckDbPoolSignalProvider``'s DuckDB
    saturation — a real, CPU/memory-bound pool, but not the one Postgres
    reads/writes actually contend on. A fleet whose hot path is PG-backed
    (the common case) could stay fully saturated on this pool while
    reporting near-zero DuckDB saturation, so the control loop would never
    see a reason to scale out. Registering this alongside the DuckDB
    provider closes that blind spot.

    ``pool_max_overflow`` is read from the same ``DBConfig`` used to build
    the engine rather than introspected off the live ``Pool`` object —
    SQLAlchemy does not expose the configured ``max_overflow`` ceiling
    (only the *current* ``overflow()`` count, which can be negative).
    """

    def __init__(self, engine: AsyncEngine, db_config: DBConfig) -> None:
        self._engine = engine
        self._db_config = db_config

    def scaling_signals(self) -> List["ScalingSignal"]:
        from dynastore.models.scaling import ScalingSignal

        pool = self._engine.pool
        checkedout = getattr(pool, "checkedout", None)
        if checkedout is None:
            return []
        try:
            in_use = checkedout()
        except Exception:
            return []
        capacity = self._db_config.pool_min_size + self._db_config.pool_max_overflow
        if capacity <= 0:
            return []
        saturation = max(0.0, min(1.0, in_use / capacity))
        return [
            ScalingSignal(
                source="pg_pool",
                metric="pool_saturation",
                value=saturation,
                scope="instance",
                ts=time.time(),
            )
        ]


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
        pg_pool_signal_provider: Optional[PgPoolSignalProvider] = None

        if existing_engine:
            logger.info("DBService: Using existing engine from app_state.")
        else:
            app_state.engine = None

        try:
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
                    #
                    # ``application_name`` additionally carries this process's
                    # stable instance id (geoid#2924) so a monitoring/reaper
                    # query can tell individual replicas of the same service
                    # apart — needed to recognize a session left behind by a
                    # specific dead Cloud Run instance rather than the whole
                    # service.
                    from dynastore.modules.db_config.instance import (
                        get_stamped_application_name,
                    )
                    app_name = get_stamped_application_name()

                    # Resolve timeout settings from PluginConfig, env, or DBConfig
                    (
                        lock_timeout,
                        statement_timeout,
                        idle_in_transaction_session_timeout,
                    ) = resolve_timeout_settings(db_config)

                    # Cap the shared serving engine's session statement_timeout
                    # below the load-balancer/Cloud Run deadline (#2898).
                    # DB_STATEMENT_TIMEOUT resolves to "0" (disabled, dev) or
                    # values like "90s" (prod) that sit ABOVE the 60s LB
                    # timeout, so a stuck query holds its connection to that
                    # ceiling instead of being cancelled and reclaimed
                    # server-side. This only affects the shared serving
                    # engine -- task-side engines never apply statement_timeout,
                    # and SET LOCAL overrides within a transaction still win.
                    statement_timeout = clamp_serving_statement_timeout(
                        statement_timeout,
                        db_config.serving_statement_timeout_ceiling_seconds,
                    )

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
                            # Transaction-mode connection poolers (AlloyDB Managed
                            # Connection Pooling / PgBouncer) multiplex one server
                            # backend across many client sessions, so a prepared
                            # statement cached against one backend may be absent —
                            # or its numeric name already taken — on the next one
                            # the pooler hands out. Disabling the driver's prepared
                            # statement cache and giving every prepared statement a
                            # unique name makes the asyncpg engine safe behind such
                            # a pooler. It also avoids InvalidCachedStatementError
                            # when ANOTHER instance emits DDL against shared objects
                            # (per-catalog schema provisioning, CREATE EXTENSION) and
                            # invalidates a cached statement's OIDs fleet-wide. See
                            # SQLAlchemy asyncpg dialect "Prepared Statement Name
                            # with PGBouncer" + asyncpg #837 / sqlalchemy #6467.
                            # Deployment invariant: because each query now PREPAREs a
                            # uniquely-named statement and never DEALLOCATEs it, the
                            # pooler in front of us must reset the backend on release
                            # (AlloyDB Managed Connection Pooling and PgBouncer both
                            # run DISCARD ALL by default). Do NOT set server_reset_query
                            # empty, or orphaned prepared statements accumulate per
                            # backend for the life of the SQLAlchemy pool.
                            "prepared_statement_cache_size": 0,
                            "prepared_statement_name_func": (
                                lambda: f"__asyncpg_{uuid4()}__"
                            ),
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
                                # DBConfig.lock_timeout. Factored into
                                # lock_safety_server_settings() so task-side ad-hoc
                                # engines carry the same pair (#2832).
                                **lock_safety_server_settings(
                                    lock_timeout,
                                    idle_in_transaction_session_timeout,
                                ),
                                # statement_timeout bounds total statement EXECUTION
                                # (not just the lock wait). DB_STATEMENT_TIMEOUT
                                # resolves to "0" (disabled) or a configured value,
                                # but this shared serving engine always applies the
                                # clamped value (see clamp_serving_statement_timeout
                                # above, #2898) so it never exceeds the LB deadline.
                                # SET LOCAL in long jobs overrides it.
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

                if _async_engine is not None:
                    try:
                        from dynastore.tools.discovery import register_plugin

                        pg_pool_signal_provider = PgPoolSignalProvider(_async_engine, db_config)
                        register_plugin(pg_pool_signal_provider)
                    except Exception:
                        logger.warning(
                            "DBService: PG pool signal provider failed to register — "
                            "PostgreSQL pool saturation will not feed the autoscaling "
                            "control loop.",
                            exc_info=True,
                        )
                        pg_pool_signal_provider = None
            except Exception as e:
                logger.critical(
                    f"DBService: FATAL: Failed to create database connection pool: {e}",
                    exc_info=True,
                )
                raise

            try:
                yield
            except Exception as e:
                # The pool was already established above — an exception surfacing
                # here comes from the application body or teardown, not from pool
                # creation, so it must not be relabelled as a connection-pool
                # creation failure (that misleads operators during shutdown).
                logger.critical(
                    f"DBService: Error during database service runtime or shutdown: {e}",
                    exc_info=True,
                )
                raise
        finally:
            if pg_pool_signal_provider is not None:
                from dynastore.tools.discovery import unregister_plugin

                unregister_plugin(pg_pool_signal_provider)
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
