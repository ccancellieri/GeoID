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
import datetime
from typing import AsyncGenerator, Optional, List, Any
from dynastore.modules import ModuleProtocol, get_protocol
from dynastore.models.protocols import ProxyProtocol, DatabaseProtocol
from .storage import AbstractProxyStorage
from dynastore.modules.proxy.models import ShortURL, AnalyticsPage
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
from dynastore.modules.db_config.query_executor import (
    managed_transaction,
    DbResource,
    DDLQuery,
    DDLBatch,
)
from dynastore.modules.db_config.locking_tools import check_table_exists
logger = logging.getLogger(__name__)
import dynastore.modules.db_config.maintenance_tools as maintenance_tools
from contextlib import asynccontextmanager
# ==============================================================================
#  TENANT INITIALIZATION (Proxy Slice)
# ==============================================================================

TENANT_SHORT_URLS_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.short_urls (
    id BIGINT NOT NULL DEFAULT nextval('{schema}.short_url_id_seq'),
    short_key VARCHAR(20) NOT NULL,
    long_url TEXT NOT NULL,
    collection_id VARCHAR(255) NOT NULL DEFAULT '_catalog_',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    is_active BOOLEAN DEFAULT TRUE,
    click_count BIGINT DEFAULT 0,
    last_accessed_at TIMESTAMPTZ,
    comment TEXT,
    PRIMARY KEY (collection_id, short_key)
) PARTITION BY LIST (collection_id);
"""

TENANT_SHORT_URLS_CATALOG_PARTITION_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.short_urls_catalog PARTITION OF {schema}.short_urls
FOR VALUES IN ('_catalog_');
"""

def _build_proxy_tenant_ddl_batch(schema: str) -> DDLBatch:
    """Build a per-tenant DDL batch for the proxy slice.

    Warm path: the ``short_urls_catalog`` default partition (last object
    created for a new tenant) is the sentinel. If it exists, sequence +
    functions + parent table are skipped in a single round-trip. Cold
    path runs all 5 steps under one conn with nested savepoints.
    """
    from .queries import (
        CREATE_SHORT_URL_SEQUENCE,
        CREATE_BASE62_FUNCTION,
        CREATE_OBFUSCATE_FUNCTION,
    )

    def _check_sentinel(conn):
        return check_table_exists(conn, "short_urls_catalog", schema)

    return DDLBatch(
        sentinel=DDLQuery(
            TENANT_SHORT_URLS_CATALOG_PARTITION_DDL, check_query=_check_sentinel
        ),
        steps=[
            CREATE_SHORT_URL_SEQUENCE,
            CREATE_BASE62_FUNCTION,
            CREATE_OBFUSCATE_FUNCTION,
            DDLQuery(TENANT_SHORT_URLS_DDL),
            DDLQuery(
                TENANT_SHORT_URLS_CATALOG_PARTITION_DDL, check_query=_check_sentinel
            ),
        ],
    )


@lifecycle_registry.sync_catalog_initializer()
async def _initialize_proxy_tenant_slice(conn: DbResource, schema: str, catalog_id: str):
    """Initializes the proxy module's slice of the tenant schema (URL shortening only)."""
    logger.info(f"PROXY_INIT: Ensuring short_urls slice for schema: {schema}")
    await _build_proxy_tenant_ddl_batch(schema).execute(conn, schema=schema)
    logger.info(f"Proxy tenant slice initialization complete for schema '{schema}'")


from dynastore.modules.catalog.lifecycle_manager import sync_collection_initializer, sync_collection_destroyer
from dynastore.tools.db import sanitize_for_sql_identifier
from dynastore.models.driver_context import DriverContext


@sync_collection_initializer()
async def _initialize_proxy_collection(conn: DbResource, schema: str, catalog_id: str, collection_id: str, **kwargs):
    """Creates a partition for the collection in the proxy table."""
    from dynastore.modules.db_config.query_executor import DDLQuery

    safe_suffix = sanitize_for_sql_identifier(collection_id)
    # Escape single quotes for the partition value literal (DDL, no bind params available)
    safe_value = collection_id.replace("'", "''")

    # We use {schema} placeholder so DDLQuery quotes it automatically
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {{schema}}.short_urls_{safe_suffix}
    PARTITION OF {{schema}}.short_urls
    FOR VALUES IN ('{safe_value}');
    """
    await DDLQuery(ddl).execute(conn, schema=schema)
    logger.info(f"Created proxy partition for collection '{collection_id}' in schema '{schema}'.")


@sync_collection_destroyer()
async def _destroy_proxy_collection(conn: DbResource, schema: str, catalog_id: str, collection_id: str):
    """Drops the partition for the collection in the proxy table."""
    from dynastore.modules.db_config.locking_tools import safe_drop_relation

    safe_suffix = sanitize_for_sql_identifier(collection_id)
    partition_name = f"short_urls_{safe_suffix}"
    # Proxy-URL lookups are read-heavy; bound the AccessExclusiveLock wait so
    # concurrent resolvers don't pile up behind the DROP.
    await safe_drop_relation(conn, schema, partition_name, kind="table", cascade=True)
    logger.info(f"Dropped proxy partition for collection '{collection_id}' in schema '{schema}'.")


# --- Lifespan Management ---
class ProxyModule(ModuleProtocol, ProxyProtocol):
    priority: int = 100
    storage_driver: AbstractProxyStorage

    def __init__(self):
        # Driver is selected at lifespan time via ProtocolPlugin discovery.
        pass

    async def create_short_url(self, engine: Any, catalog_id: str, long_url: str, custom_key: Optional[str] = None, collection_id: Optional[str] = None, comment: Optional[str] = None) -> ShortURL:
        """ProxyProtocol: Creates a short URL."""
        from dynastore.models.protocols import CatalogsProtocol
        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            raise RuntimeError("CatalogsProtocol not registered")
        async with managed_transaction(engine) as tx_engine:
            schema = await catalogs.resolve_physical_schema(catalog_id, ctx=DriverContext(db_resource=tx_engine))
            if not schema:
                raise ValueError(f"Catalog '{catalog_id}' not found.")
            return await self.storage_driver.insert_short_url(tx_engine, schema, long_url, custom_key, collection_id, comment)

    async def get_urls_by_collection(self, engine: Any, catalog_id: str, collection_id: str, limit: int = 100, offset: int = 0) -> List[ShortURL]:
        """ProxyProtocol: Retrieves a list of short URLs by collection."""
        from dynastore.models.protocols import CatalogsProtocol
        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            raise RuntimeError("CatalogsProtocol not registered")
        schema = await catalogs.resolve_physical_schema(catalog_id)
        if schema is None:
            raise ValueError(f"Catalog '{catalog_id}' not found.")
        async with managed_transaction(engine) as conn:
            return await self.storage_driver.select_urls_by_collection(conn, schema, collection_id, limit, offset)

    async def get_long_url(self, engine: Any, catalog_id: str, short_key: str) -> Optional[str]:
        """ProxyProtocol: Retrieves a long URL."""
        from dynastore.models.protocols import CatalogsProtocol
        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            raise RuntimeError("CatalogsProtocol not registered")
        async with managed_transaction(engine) as tx_engine:
            schema = await catalogs.resolve_physical_schema(catalog_id, ctx=DriverContext(db_resource=tx_engine))
            if not schema:
                return None
            return await self.storage_driver.select_long_url(tx_engine, schema, short_key)

    async def log_redirect(self, engine: Any, catalog_id: str, short_key: str, ip_address: str, user_agent: str, referrer: str, timestamp: datetime.datetime) -> None:
        """ProxyProtocol: Logs a redirect event to Elasticsearch."""
        from dynastore.models.protocols import CatalogsProtocol
        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            raise RuntimeError("CatalogsProtocol not registered")
        async with managed_transaction(engine) as tx_engine:
            schema = await catalogs.resolve_physical_schema(catalog_id, ctx=DriverContext(db_resource=tx_engine))
            if not schema:
                logger.warning(f"Could not log redirect: Catalog '{catalog_id}' not found.")
                return
        await self.storage_driver.insert_redirect_log(schema, short_key, ip_address, user_agent, referrer, timestamp)

    async def get_analytics(self, engine: Any, catalog_id: str, short_key: str, cursor: Optional[str] = None, page_size: int = 100, aggregate: bool = False, start_date: Optional[datetime.datetime] = None, end_date: Optional[datetime.datetime] = None) -> AnalyticsPage:
        """ProxyProtocol: Gets analytics from Elasticsearch."""
        from dynastore.models.protocols import CatalogsProtocol
        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            raise RuntimeError("CatalogsProtocol not registered")
        async with managed_transaction(engine) as tx_engine:
            schema = await catalogs.resolve_physical_schema(catalog_id, ctx=DriverContext(db_resource=tx_engine))
            if not schema:
                return AnalyticsPage(data=[], long_url=None)
            page = await self.storage_driver.select_analytics(schema, short_key, cursor, page_size, aggregate, start_date, end_date)
            # Enrich with long_url from PG
            if not page.long_url:
                page.long_url = await self.storage_driver.select_long_url(tx_engine, schema, short_key)
            return page

    async def delete_short_url(self, engine: Any, catalog_id: str, short_key: str) -> Optional[str]:
        """ProxyProtocol: Deletes a short URL."""
        from dynastore.models.protocols import CatalogsProtocol
        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            raise RuntimeError("CatalogsProtocol not registered")
        async with managed_transaction(engine) as tx_engine:
            schema = await catalogs.resolve_physical_schema(catalog_id, ctx=DriverContext(db_resource=tx_engine))
            if not schema:
                return None
            return await self.storage_driver.drop_short_url(tx_engine, schema, short_key)

    @asynccontextmanager
    async def lifespan(self, app_state: object) -> AsyncGenerator[None, None]:
        from contextlib import AsyncExitStack
        from dynastore.tools.discovery import get_protocols
        import os

        # Discover and activate the highest-priority available proxy storage driver
        target_name = os.environ.get("PROXY_STORAGE_DRIVER")
        
        # Ensure PostgresProxyStorage is registered (manual registration for now to guarantee discovery)
        from .default_storage_driver import PostgresProxyStorage
        from dynastore.tools.discovery import register_plugin
        register_plugin(PostgresProxyStorage())
        
        drivers = get_protocols(AbstractProxyStorage)
        if not drivers:
            logger.critical("ProxyModule: No proxy storage driver registered. Ensure at least one is imported.")
            yield; return

        if target_name:
            driver = next((d for d in drivers if d.name == target_name), None)
            if not driver:
                logger.warning(f"ProxyModule: driver '{target_name}' not found, using highest-priority.")
                driver = drivers[0]
        else:
            driver = drivers[0]  # already sorted by priority desc

        self.storage_driver = driver

        db = get_protocol(DatabaseProtocol)
        engine = db.engine if db else None

        if not engine:
            logger.critical("ProxyModule: database engine not found.")
            yield; return

        async with AsyncExitStack() as stack:
            # Ensure the schema exists before the storage driver tries to use it.
            async with managed_transaction(engine) as conn:
                await maintenance_tools.ensure_schema_exists(conn, "proxy")

            # Let the driver initialise its schema/tables via its own lifespan
            await stack.enter_async_context(self.storage_driver.lifespan(app_state))

            yield

        # Graceful Shutdown: Flush remaining analytics to ES
        if hasattr(self.storage_driver, 'flush'):
            await self.storage_driver.flush()

# --- Public API ---

def _get_proxy_module() -> ProxyProtocol:
    """Retrieves the ProxyProtocol implementation via protocol discovery (lru_cached)."""
    proxy = get_protocol(ProxyProtocol)
    if proxy is None:
        raise Exception("ProxyProtocol implementation not found. Ensure the ProxyModule is properly initialized.")
    return proxy

async def create_short_url(engine: DbResource, catalog_id: str, long_url: str, custom_key: Optional[str] = None, collection_id: Optional[str] = None, comment: Optional[str] = None) -> ShortURL:
    """Public API function to create a short URL."""
    return await _get_proxy_module().create_short_url(engine, catalog_id, long_url, custom_key, collection_id, comment)

# Removed get_urls_by_owner as it is deprecated by the new architecture.

async def get_urls_by_collection(engine: DbResource, catalog_id: str, collection_id: str, limit: int = 100, offset: int = 0) -> List[ShortURL]:
    """Public API function to list short URLs by collection."""
    return await _get_proxy_module().get_urls_by_collection(engine, catalog_id, collection_id, limit, offset)

async def get_long_url(engine: DbResource, catalog_id: str, short_key: str) -> Optional[str]:
    """Public API function to retrieve a long URL."""
    return await _get_proxy_module().get_long_url(engine, catalog_id, short_key)

async def log_redirect(engine: DbResource, catalog_id: str, short_key: str, ip_address: str, user_agent: str, referrer: str, timestamp: datetime.datetime) -> None:
    """Public API function to log a redirect event."""
    await _get_proxy_module().log_redirect(engine, catalog_id, short_key, ip_address, user_agent, referrer, timestamp)

async def get_analytics(engine: DbResource, catalog_id: str, short_key: str, cursor: Optional[str] = None, page_size: int = 100, aggregate: bool = False, start_date: Optional[datetime.datetime] = None, end_date: Optional[datetime.datetime] = None) -> AnalyticsPage:
    """Public API function to get analytics for a short URL."""
    return await _get_proxy_module().get_analytics(engine, catalog_id, short_key, cursor, page_size, aggregate, start_date, end_date)

async def delete_short_url(engine: DbResource, catalog_id: str, short_key: str) -> Optional[str]:
    """Public API function to delete a short URL."""
    return await _get_proxy_module().delete_short_url(engine, catalog_id, short_key)

