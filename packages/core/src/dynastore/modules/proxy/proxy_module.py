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
import datetime
from typing import AsyncGenerator, Optional, List, Any
from dynastore.modules import ModuleProtocol, get_protocol
from dynastore.models.protocols import ProxyProtocol, DatabaseProtocol
from .storage import AbstractProxyStorage
from dynastore.modules.proxy.models import ShortURL, AnalyticsPage
from dynastore.modules.db_config.query_executor import (
    managed_transaction,
    DbResource,
)
from dynastore.models.driver_context import DriverContext
import dynastore.modules.db_config.maintenance_tools as maintenance_tools
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


# ==============================================================================
#  MODULE
# ==============================================================================

class ProxyModule(ModuleProtocol, ProxyProtocol):
    priority: int = 100
    storage_driver: AbstractProxyStorage

    def __init__(self):
        pass

    async def create_short_url(
        self,
        engine: Any,
        catalog_id: str,
        long_url: str,
        custom_key: Optional[str] = None,
        collection_id: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> ShortURL:
        """ProxyProtocol: Creates a short URL in collection_proxy_urls."""
        from dynastore.models.protocols import CatalogsProtocol
        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            raise RuntimeError("CatalogsProtocol not registered")
        async with managed_transaction(engine) as tx_engine:
            schema = await catalogs.resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=tx_engine)
            )
            if not schema:
                raise ValueError(f"Catalog '{catalog_id}' not found.")
            collection_physical_id: Optional[str] = None
            if collection_id:
                collection_physical_id = await catalogs.resolve_physical_id(
                    catalog_id,
                    collection_id,
                    ctx=DriverContext(db_resource=tx_engine),
                    allow_missing=True,
                )
            return await self.storage_driver.insert_short_url(
                tx_engine, schema, long_url, custom_key, collection_id,
                collection_physical_id, comment
            )

    async def get_urls_by_collection(
        self,
        engine: Any,
        catalog_id: str,
        collection_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[ShortURL]:
        """ProxyProtocol: Retrieves a list of short URLs by collection."""
        from dynastore.models.protocols import CatalogsProtocol
        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            raise RuntimeError("CatalogsProtocol not registered")
        async with managed_transaction(engine) as conn:
            schema = await catalogs.resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=conn)
            )
            if schema is None:
                raise ValueError(f"Catalog '{catalog_id}' not found.")
            collection_physical_id = await catalogs.resolve_physical_id(
                catalog_id,
                collection_id,
                ctx=DriverContext(db_resource=conn),
                allow_missing=True,
            )
            return await self.storage_driver.select_urls_by_collection(
                conn, schema, collection_physical_id or collection_id, limit, offset
            )

    async def get_long_url(
        self, engine: Any, catalog_id: str, short_key: str
    ) -> Optional[str]:
        """ProxyProtocol: Retrieves a long URL from collection_proxy_urls."""
        from dynastore.models.protocols import CatalogsProtocol
        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            raise RuntimeError("CatalogsProtocol not registered")
        async with managed_transaction(engine) as tx_engine:
            schema = await catalogs.resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=tx_engine)
            )
            if not schema:
                return None
            return await self.storage_driver.select_long_url(
                tx_engine, schema, short_key
            )

    async def log_redirect(
        self,
        engine: Any,
        catalog_id: str,
        short_key: str,
        ip_address: str,
        user_agent: str,
        referrer: str,
        timestamp: datetime.datetime,
    ) -> None:
        """ProxyProtocol: Logs a redirect event to Elasticsearch."""
        from dynastore.models.protocols import CatalogsProtocol
        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            raise RuntimeError("CatalogsProtocol not registered")
        async with managed_transaction(engine) as tx_engine:
            schema = await catalogs.resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=tx_engine)
            )
            if not schema:
                logger.warning(
                    "Could not log redirect: Catalog '%s' not found.", catalog_id
                )
                return
        await self.storage_driver.insert_redirect_log(
            schema, short_key, ip_address, user_agent, referrer, timestamp
        )

    async def get_analytics(
        self,
        engine: Any,
        catalog_id: str,
        short_key: str,
        cursor: Optional[str] = None,
        page_size: int = 100,
        aggregate: bool = False,
        start_date: Optional[datetime.datetime] = None,
        end_date: Optional[datetime.datetime] = None,
    ) -> AnalyticsPage:
        """ProxyProtocol: Gets analytics from Elasticsearch."""
        from dynastore.models.protocols import CatalogsProtocol
        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            raise RuntimeError("CatalogsProtocol not registered")
        async with managed_transaction(engine) as tx_engine:
            schema = await catalogs.resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=tx_engine)
            )
            if not schema:
                return AnalyticsPage(data=[], long_url=None)
            page = await self.storage_driver.select_analytics(
                schema, short_key, cursor, page_size, aggregate,
                start_date, end_date,
            )
            if not page.long_url:
                page.long_url = await self.storage_driver.select_long_url(
                    tx_engine, schema, short_key
                )
            return page

    async def delete_short_url(
        self, engine: Any, catalog_id: str, short_key: str
    ) -> Optional[str]:
        """ProxyProtocol: Deletes a short URL from collection_proxy_urls."""
        from dynastore.models.protocols import CatalogsProtocol
        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            raise RuntimeError("CatalogsProtocol not registered")
        async with managed_transaction(engine) as tx_engine:
            schema = await catalogs.resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=tx_engine)
            )
            if not schema:
                return None
            return await self.storage_driver.drop_short_url(
                tx_engine, schema, short_key
            )

    @asynccontextmanager
    async def lifespan(self, app_state: object) -> AsyncGenerator[None, None]:
        from contextlib import AsyncExitStack
        from dynastore.tools.discovery import get_protocols
        import os

        target_name = os.environ.get("PROXY_STORAGE_DRIVER")

        from .default_storage_driver import PostgresProxyStorage
        from dynastore.tools.discovery import register_plugin
        register_plugin(PostgresProxyStorage())

        drivers = get_protocols(AbstractProxyStorage)
        if not drivers:
            logger.critical(
                "ProxyModule: No proxy storage driver registered. "
                "Ensure at least one is imported."
            )
            yield
            return

        if target_name:
            driver = next((d for d in drivers if d.name == target_name), None)
            if not driver:
                logger.warning(
                    "ProxyModule: driver '%s' not found, using highest-priority.",
                    target_name,
                )
                driver = drivers[0]
        else:
            driver = drivers[0]

        self.storage_driver = driver

        db = get_protocol(DatabaseProtocol)
        engine = db.engine if db else None

        if not engine:
            logger.critical("ProxyModule: database engine not found.")
            yield
            return

        async with AsyncExitStack() as stack:
            async with managed_transaction(engine) as conn:
                await maintenance_tools.ensure_schema_exists(conn, "proxy")

            await stack.enter_async_context(
                self.storage_driver.lifespan(app_state)
            )

            yield

        if hasattr(self.storage_driver, "flush"):
            await self.storage_driver.flush()


# ==============================================================================
#  Public API
# ==============================================================================

def _get_proxy_module() -> ProxyProtocol:
    proxy = get_protocol(ProxyProtocol)
    if proxy is None:
        raise Exception(
            "ProxyProtocol implementation not found. "
            "Ensure the ProxyModule is properly initialized."
        )
    return proxy


async def create_short_url(
    engine: DbResource,
    catalog_id: str,
    long_url: str,
    custom_key: Optional[str] = None,
    collection_id: Optional[str] = None,
    comment: Optional[str] = None,
) -> ShortURL:
    """Public API: create a short URL in collection_proxy_urls."""
    return await _get_proxy_module().create_short_url(
        engine, catalog_id, long_url, custom_key, collection_id, comment
    )


async def get_urls_by_collection(
    engine: DbResource,
    catalog_id: str,
    collection_id: str,
    limit: int = 100,
    offset: int = 0,
) -> List[ShortURL]:
    """Public API: list short URLs by collection."""
    return await _get_proxy_module().get_urls_by_collection(
        engine, catalog_id, collection_id, limit, offset
    )


async def get_long_url(
    engine: DbResource, catalog_id: str, short_key: str
) -> Optional[str]:
    """Public API: retrieve a long URL."""
    return await _get_proxy_module().get_long_url(engine, catalog_id, short_key)


async def log_redirect(
    engine: DbResource,
    catalog_id: str,
    short_key: str,
    ip_address: str,
    user_agent: str,
    referrer: str,
    timestamp: datetime.datetime,
) -> None:
    """Public API: log a redirect event."""
    await _get_proxy_module().log_redirect(
        engine, catalog_id, short_key, ip_address, user_agent, referrer, timestamp
    )


async def get_analytics(
    engine: DbResource,
    catalog_id: str,
    short_key: str,
    cursor: Optional[str] = None,
    page_size: int = 100,
    aggregate: bool = False,
    start_date: Optional[datetime.datetime] = None,
    end_date: Optional[datetime.datetime] = None,
) -> AnalyticsPage:
    """Public API: get analytics for a short URL."""
    return await _get_proxy_module().get_analytics(
        engine, catalog_id, short_key, cursor, page_size,
        aggregate, start_date, end_date,
    )


async def delete_short_url(
    engine: DbResource, catalog_id: str, short_key: str
) -> Optional[str]:
    """Public API: delete a short URL."""
    return await _get_proxy_module().delete_short_url(engine, catalog_id, short_key)
