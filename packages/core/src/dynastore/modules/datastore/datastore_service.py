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
from psycopg2.extras import register_default_jsonb, register_default_json
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from contextlib import asynccontextmanager
from dynastore.modules.db_config.db_config import DBConfig
from dynastore.modules.db_config.tools import (
    get_config,
    ensure_init_db,
    normalize_db_url,
)
from dynastore.modules import ModuleProtocol
from typing import Optional, Any, Protocol, runtime_checkable

from dynastore.models.protocols import DatabaseProtocol

logger = logging.getLogger(__name__)


@runtime_checkable
class DatastoreAppState(Protocol):
    """State attributes this module attaches to the shared AppState namespace."""
    sync_engine: Optional[Engine]


class DatastoreModule(ModuleProtocol, DatabaseProtocol):
    priority: int = 7
    app_state: DatastoreAppState

    def __init__(self, app_state: DatastoreAppState):
        self.app_state = app_state

    @property
    def engine(self) -> Any:
        """DatabaseProtocol implementation.

        Prefers the async engine when both are present.  This module is
        designed for sync workers, but on the catalog service the
        ``module_storage_iceberg`` driver pulls ``pyiceberg[sql-postgres]``
        which transitively installs ``psycopg2-binary``.  That lets this
        module's entry-point class load and its lifespan create a sync
        engine alongside the async one.  Without this preference order,
        ``protocol_helpers.get_engine()`` (priority-sorted; this module
        is priority=7 vs DBService priority=10) would return the sync
        engine and break async-only call sites such as
        ``ItemService.stream_items`` (``stream_conn.stream(...)`` requires
        ``AsyncConnection``).
        """
        engine = getattr(self.app_state, "engine", None)
        if engine:
            return engine
        engine = getattr(self.app_state, "sync_engine", None)
        if engine:
            return engine
        raise RuntimeError("No database engine available (sync or async).")

    @property
    def async_engine(self) -> Optional[Any]:
        """DatabaseProtocol implementation - not available in task context."""
        return None

    @property
    def sync_engine(self) -> Optional[Any]:
        """DatabaseProtocol implementation."""
        return getattr(self.app_state, "sync_engine", None)

    def get_any_engine(self) -> Optional[Any]:
        """DatabaseProtocol implementation."""
        from dynastore.tools.protocol_helpers import get_engine

        return get_engine()

    async def apply_connection_adapters(self, connection: Any) -> None:
        """Register psycopg2 type adapters (JSONB, JSON)."""
        register_default_jsonb(connection)
        register_default_json(connection)

    @asynccontextmanager
    async def lifespan(self, app_state: DatastoreAppState):
        """
        Manages the lifespan for the synchronous database engine.
        """
        logger.info("Synchronous database connection startup initiated...")

        try:
            db_config: DBConfig = get_config(app_state)
            app_state.sync_engine = None

            if hasattr(app_state, "sync_engine") and app_state.sync_engine is not None:
                logger.info("Synchronous database engine already exists in app state.")
                raise Exception("Synchronous database engine already initialized.")
            else:
                # Tag wire-level connections with the logical service name so
                # ``pg_stat_activity.application_name`` is populated and
                # per-service contention is visible from the DB side. See
                # #699 / #655. psycopg2 honours libpq ``application_name``
                # in connect_args.
                #
                # Also carries this process's stable instance id (geoid#2924)
                # so a monitoring/reaper query can recognize a session left
                # behind by a specific dead Cloud Run instance, not just the
                # service as a whole — see
                # dynastore.modules.db_config.instance.get_stamped_application_name.
                from dynastore.modules.db_config.instance import (
                    get_stamped_application_name,
                )
                app_name = get_stamped_application_name()

                app_state.sync_engine = create_engine(
                    normalize_db_url(db_config.database_url, is_async=False),
                    pool_size=db_config.pool_min_size,
                    max_overflow=db_config.pool_max_overflow,
                    pool_timeout=db_config.pool_command_timeout,
                    # Pool hygiene parity with the async engine
                    # (db_service.py): recycle stale slots and pre-ping so a
                    # NAT/AlloyDB-dropped idle connection is replaced rather
                    # than handed out dead-at-the-wire. See #655.
                    pool_pre_ping=True,
                    pool_recycle=db_config.pool_recycle,
                    connect_args={
                        "application_name": app_name,
                        # psycopg2 speaks libpq, so client-side TCP keepalive
                        # params apply directly — probes keep Cloud NAT from
                        # silently dropping the idle mapping. See #655.
                        "keepalives": 1,
                        "keepalives_idle": db_config.tcp_keepalives_idle,
                        "keepalives_interval": db_config.tcp_keepalives_interval,
                        "keepalives_count": db_config.tcp_keepalives_count,
                    },
                )
                logger.info("SQLAlchemy SyncEngine established successfully.")

                # Run initialization (extension bootstrap) using the shared
                # maintenance tool; it handles sync engines from an async
                # lifespan context.
                await ensure_init_db(app_state.sync_engine)


        except Exception as e:
            logger.error(
                f"Failed to initialize synchronous database pool: {e}", exc_info=True
            )
            raise

        yield

        # --- Application Shutdown ---
        logger.info("Synchronous database connection shutdown initiated...")

        if app_state.sync_engine:
            app_state.sync_engine.dispose()
            app_state.sync_engine = None
            logger.info("SQLAlchemy SyncEngine disposed.")

        logger.info("Synchronous database connection shutdown complete.")
