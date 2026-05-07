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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

import logging
from contextlib import asynccontextmanager

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

from sqlalchemy.ext.asyncio import create_async_engine
from typing import Optional, Any, Protocol, runtime_checkable
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.engine import Engine
from dynastore.modules import ModuleProtocol
from dynastore.modules.db_config.db_config import DBConfig
from dynastore.modules.db_config.tools import (
    get_config,
    normalize_db_url,
)

from dynastore.models.protocols import DatabaseProtocol

logger = logging.getLogger(__name__)


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

                # 1. Create Engine
                app_state.engine = create_async_engine(
                    normalize_db_url(db_config.database_url, is_async=True),
                    pool_size=db_config.pool_min_size,
                    max_overflow=db_config.pool_max_size - db_config.pool_min_size,
                    pool_timeout=db_config.pool_command_timeout,
                    pool_pre_ping=True,
                    pool_recycle=1800,
                    connect_args={"timeout": db_config.connect_timeout},
                )
                engine_created_by_service = True
                logger.info(
                    "DBService: ASYNC Database connection pool established successfully."
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
