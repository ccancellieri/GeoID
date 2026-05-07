# src/dynastore/modules/notebooks/notebooks_module.py
"""NotebooksModule: service layer + lifespan for platform notebook schema init."""
import logging
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import List, Dict, Any, Optional, Tuple

from dynastore.modules import ModuleProtocol, get_protocol
from dynastore.models.protocols import DatabaseProtocol, CatalogsProtocol
from dynastore.modules.db_config import maintenance_tools
from dynastore.modules.db_config.locking_tools import check_table_exists
from dynastore.modules.db_config.query_executor import managed_transaction, DDLQuery
from dynastore.modules.db_config.shared_queries import DbResource
from dynastore.models.driver_context import DriverContext
from .models import NotebookCreate, Notebook, PlatformNotebookCreate, PlatformNotebook
from .platform_db import PLATFORM_NOTEBOOKS_DDL, seed_platform_notebooks

logger = logging.getLogger(__name__)


class NotebooksModule(ModuleProtocol):
    priority: int = 100

    def __init__(self, app_state: SimpleNamespace):
        self.app_state = app_state
        global _module_instance
        _module_instance = self

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        """Initialize notebooks schema and seed platform notebooks."""
        db = get_protocol(DatabaseProtocol)
        engine = db.engine if db else None

        if not engine:
            logger.critical("NotebooksModule cannot initialize: database engine not found.")
            yield
            return

        logger.info("NotebooksModule: Initializing schema...")
        try:
            async with managed_transaction(engine) as conn:
                table_exists = await check_table_exists(conn, "platform_notebooks", "notebooks")

            if not table_exists:
                async with managed_transaction(engine) as conn:
                    async with maintenance_tools.acquire_startup_lock(conn, "notebooks_module"):
                        await maintenance_tools.ensure_schema_exists(conn, "notebooks")
                        await DDLQuery(PLATFORM_NOTEBOOKS_DDL).execute(conn)

            # Trigger built-in notebook registrations BEFORE seeding so the
            # in-memory registry is populated when `seed_platform_notebooks`
            # reads it. Each submodule registers via `register_platform_notebook`
            # at import time. Wrapped per-module so an ImportError (deps absent
            # in this SCOPE) or any other error (malformed registration, missing
            # helper) just skips that module rather than aborting startup.
            for mod_path in (
                "dynastore.modules.catalog.notebooks",
                "dynastore.modules.storage.notebooks",
                "dynastore.modules.elasticsearch.notebooks",
                "dynastore.tasks.ingestion.notebooks",
            ):
                try:
                    __import__(mod_path)
                    logger.debug(
                        f"NotebooksModule: registered built-in notebooks from "
                        f"'{mod_path}'."
                    )
                except ImportError as e:
                    logger.info(
                        f"NotebooksModule: skipping built-in registrations from "
                        f"'{mod_path}' — module deps not present in this SCOPE: {e}"
                    )
                except Exception as e:
                    # Defensive: a bad registration must not crash startup.
                    logger.warning(
                        f"NotebooksModule: built-in registrations from "
                        f"'{mod_path}' failed; skipping. Error: {e}",
                        exc_info=True,
                    )

            # Seed the platform_notebooks table from the now-populated
            # in-memory registry. The original eager-imports-at-package-init
            # design implicitly ran imports first; deferring imports into
            # lifespan REQUIRES this explicit ordering or the seed runs
            # against an empty registry and built-in notebooks are missing.
            async with managed_transaction(engine) as conn:
                await seed_platform_notebooks(conn)

            logger.info("NotebooksModule: Initialization complete.")
        except Exception as e:
            logger.error(f"CRITICAL: NotebooksModule initialization failed: {e}", exc_info=True)

        yield

    # ------------------------------------------------------------------
    # Tenant notebook service methods
    # ------------------------------------------------------------------

    async def get_notebook(self, catalog_id: str, notebook_id: str, db_resource: Optional[DbResource] = None) -> Notebook:
        catalogs = get_protocol(CatalogsProtocol)
        db = get_protocol(DatabaseProtocol)
        engine = db.engine if db else None
        schema = await catalogs.resolve_physical_schema(catalog_id, ctx=DriverContext(db_resource=db_resource) if db_resource else None) if catalogs else None
        if schema is None:
            raise ValueError(f"Catalog '{catalog_id}' not found.")
        async with managed_transaction(db_resource or engine) as conn:
            from .notebooks_db import get_notebook as db_get_notebook
            data = await db_get_notebook(conn, schema, notebook_id)
        return Notebook(**data)

    async def list_notebooks(
        self,
        catalog_id: str,
        *,
        q: Optional[str] = None,
        tags: Optional[List[str]] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> Tuple[List[Dict[str, Any]], int]:
        catalogs = get_protocol(CatalogsProtocol)
        db = get_protocol(DatabaseProtocol)
        engine = db.engine if db else None
        schema = await catalogs.resolve_physical_schema(catalog_id) if catalogs else None
        if schema is None:
            raise ValueError(f"Catalog '{catalog_id}' not found.")
        async with managed_transaction(engine) as conn:
            from .notebooks_db import list_notebooks as db_list_notebooks
            return await db_list_notebooks(conn, schema, q=q, tags=tags, limit=limit, offset=offset)

    async def delete_notebook(self, catalog_id: str, notebook_id: str) -> None:
        catalogs = get_protocol(CatalogsProtocol)
        db = get_protocol(DatabaseProtocol)
        engine = db.engine if db else None
        schema = await catalogs.resolve_physical_schema(catalog_id) if catalogs else None
        if schema is None:
            raise ValueError(f"Catalog '{catalog_id}' not found.")
        async with managed_transaction(engine) as conn:
            from .notebooks_db import soft_delete_notebook
            await soft_delete_notebook(conn, schema, notebook_id)

    async def save_notebook(self, catalog_id: str, notebook: NotebookCreate,
                            owner_id: str | None = None) -> Notebook:
        catalogs = get_protocol(CatalogsProtocol)
        db = get_protocol(DatabaseProtocol)
        engine = db.engine if db else None
        schema = await catalogs.resolve_physical_schema(catalog_id) if catalogs else None
        if schema is None:
            raise ValueError(f"Catalog '{catalog_id}' not found.")
        async with managed_transaction(engine) as conn:
            from .notebooks_db import save_notebook as db_save_notebook
            data = await db_save_notebook(conn, schema, catalog_id, notebook, owner_id=owner_id)
            return Notebook(**data)

    async def copy_from_platform(self, catalog_id: str, platform_notebook_id: str,
                                 owner_id: str) -> Notebook:
        catalogs = get_protocol(CatalogsProtocol)
        db = get_protocol(DatabaseProtocol)
        engine = db.engine if db else None
        schema = await catalogs.resolve_physical_schema(catalog_id) if catalogs else None
        if schema is None:
            raise ValueError(f"Catalog '{catalog_id}' not found.")
        async with managed_transaction(engine) as conn:
            from .platform_db import get_platform_notebook
            from .notebooks_db import copy_from_platform as db_copy
            platform_nb = await get_platform_notebook(conn, platform_notebook_id)
            data = await db_copy(conn, schema, catalog_id, platform_nb, owner_id)
            return Notebook(**data)

    # ------------------------------------------------------------------
    # Platform notebook service methods
    # ------------------------------------------------------------------

    async def list_platform_notebooks(
        self,
        *,
        q: Optional[str] = None,
        tags: Optional[List[str]] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> Tuple[List[Dict[str, Any]], int]:
        db = get_protocol(DatabaseProtocol)
        engine = db.engine if db else None
        async with managed_transaction(engine) as conn:
            from .platform_db import list_platform_notebooks as db_list
            return await db_list(conn, q=q, tags=tags, limit=limit, offset=offset)

    async def get_platform_notebook(self, notebook_id: str) -> PlatformNotebook:
        db = get_protocol(DatabaseProtocol)
        engine = db.engine if db else None
        async with managed_transaction(engine) as conn:
            from .platform_db import get_platform_notebook as db_get
            data = await db_get(conn, notebook_id)
        return PlatformNotebook(**data)

    async def save_platform_notebook(self, notebook: PlatformNotebookCreate) -> PlatformNotebook:
        db = get_protocol(DatabaseProtocol)
        engine = db.engine if db else None
        async with managed_transaction(engine) as conn:
            from .platform_db import save_platform_notebook as db_save
            data = await db_save(conn, notebook)
        return PlatformNotebook(**data)

    async def delete_platform_notebook(self, notebook_id: str) -> None:
        db = get_protocol(DatabaseProtocol)
        engine = db.engine if db else None
        async with managed_transaction(engine) as conn:
            from .platform_db import soft_delete_platform_notebook as db_delete
            await db_delete(conn, notebook_id)


# --- Public Helper Functions ---

_module_instance: Optional[NotebooksModule] = None


def _get_service() -> NotebooksModule:
    global _module_instance
    if _module_instance:
        return _module_instance
    instance = get_protocol(NotebooksModule)
    if not instance:
        raise RuntimeError("NotebooksModule is not initialized")
    _module_instance = instance
    return instance


async def get_notebook(catalog_id: str, notebook_id: str) -> Notebook:
    return await _get_service().get_notebook(catalog_id, notebook_id)

async def list_notebooks(
    catalog_id: str,
    *,
    q: Optional[str] = None,
    tags: Optional[List[str]] = None,
    limit: int = 20,
    offset: int = 0,
) -> Tuple[List[Dict[str, Any]], int]:
    return await _get_service().list_notebooks(catalog_id, q=q, tags=tags, limit=limit, offset=offset)

async def delete_notebook(catalog_id: str, notebook_id: str) -> None:
    return await _get_service().delete_notebook(catalog_id, notebook_id)

async def save_notebook(catalog_id: str, notebook: NotebookCreate, owner_id: str | None = None) -> Notebook:
    return await _get_service().save_notebook(catalog_id, notebook, owner_id=owner_id)

async def copy_from_platform(catalog_id: str, platform_notebook_id: str, owner_id: str) -> Notebook:
    return await _get_service().copy_from_platform(catalog_id, platform_notebook_id, owner_id)

async def list_platform_notebooks(
    *,
    q: Optional[str] = None,
    tags: Optional[List[str]] = None,
    limit: int = 20,
    offset: int = 0,
) -> Tuple[List[Dict[str, Any]], int]:
    return await _get_service().list_platform_notebooks(q=q, tags=tags, limit=limit, offset=offset)

async def get_platform_notebook(notebook_id: str) -> PlatformNotebook:
    return await _get_service().get_platform_notebook(notebook_id)

async def save_platform_notebook(notebook: PlatformNotebookCreate) -> PlatformNotebook:
    return await _get_service().save_platform_notebook(notebook)

async def delete_platform_notebook(notebook_id: str) -> None:
    return await _get_service().delete_platform_notebook(notebook_id)
