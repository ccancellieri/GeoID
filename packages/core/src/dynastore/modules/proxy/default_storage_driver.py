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

from dynastore.modules.db_config.query_executor import DbResource, managed_transaction
from dynastore.modules.db_config import maintenance_tools
from dynastore.modules.proxy import queries
from dynastore.modules.proxy.models import AnalyticsPage, ShortURL
from dynastore.modules.proxy.storage import AbstractProxyStorage
from dynastore.modules.proxy.elasticsearch_storage import ElasticsearchProxyAnalytics
from contextlib import asynccontextmanager
from typing import Optional, List, AsyncGenerator
import datetime
import logging

logger = logging.getLogger(__name__)


class PostgresProxyStorage(AbstractProxyStorage):
    """URL shortening in PG, analytics in Elasticsearch."""

    @property
    def name(self) -> str:
        return "postgres"

    priority = 10

    def __init__(self):
        self._analytics = ElasticsearchProxyAnalytics()

    @asynccontextmanager
    async def lifespan(self, app_state: object) -> AsyncGenerator[None, None]:
        """Initialises PG tables for URL shortening (analytics is in ES)."""
        from dynastore.tools.protocol_helpers import get_engine
        engine = get_engine(app_state)
        if not engine:
            logger.error("PostgresProxyStorage: engine not found during lifespan.")
            yield
            return

        async with managed_transaction(engine) as conn:
            async with maintenance_tools.acquire_startup_lock(conn, "proxy_storage_init"):
                await queries.CREATE_SHORT_URL_SEQUENCE.execute(conn, schema="proxy")
                await queries.CREATE_BASE62_FUNCTION.execute(conn, schema="proxy")
                await queries.CREATE_OBFUSCATE_FUNCTION.execute(conn, schema="proxy")
                await queries.CREATE_SHORT_URLS_TABLE.execute(conn, schema="proxy")

        yield

    async def insert_short_url(self, conn: DbResource, schema: str, long_url: str, custom_key: Optional[str] = None, collection_id: Optional[str] = None, comment: Optional[str] = None) -> ShortURL:
        if custom_key:
            return await queries.INSERT_SHORT_URL_WITH_CUSTOM_KEY.execute(
                conn, schema=schema, custom_key=custom_key, long_url=long_url,
                collection_id=collection_id or '_catalog_', comment=comment,
            )
        return await queries.INSERT_SHORT_URL_WITH_GENERATED_KEY.execute(
            conn, schema=schema, long_url=long_url,
            collection_id=collection_id or '_catalog_', comment=comment,
        )

    async def select_urls_by_collection(self, conn: DbResource, schema: str, collection_id: str, limit: int = 100, offset: int = 0) -> List[ShortURL]:
        return await queries.GET_URLS_BY_COLLECTION.execute(
            conn, schema=schema, collection_id=collection_id, limit=limit, offset=offset,
        )

    async def select_long_url(self, conn: DbResource, schema: str, short_key: str) -> Optional[str]:
        return await queries.GET_LONG_URL.execute(conn, schema=schema, short_key=short_key)

    async def insert_redirect_log(self, schema: str, short_key: str, ip_address: str, user_agent: str, referrer: str, timestamp: datetime.datetime):
        """Buffer redirect log for ES bulk indexing."""
        needs_flush = await self._analytics.buffer_redirect(
            schema=schema, short_key=short_key, ip_address=ip_address,
            user_agent=user_agent, referrer=referrer, timestamp=timestamp,
        )
        if needs_flush:
            await self.flush()

    async def flush(self):
        """Flush buffered analytics to Elasticsearch."""
        await self._analytics.flush()

    async def select_analytics(self, schema: str, short_key: str, cursor: Optional[str] = None, page_size: int = 100, aggregate: bool = False, start_date: Optional[datetime.datetime] = None, end_date: Optional[datetime.datetime] = None) -> AnalyticsPage:
        """Query analytics from Elasticsearch, enrich with long_url from PG."""
        page = await self._analytics.query_analytics(
            schema=schema, short_key=short_key, cursor=cursor,
            page_size=page_size, aggregate=aggregate,
            start_date=start_date, end_date=end_date,
        )
        return page

    async def drop_short_url(self, conn: DbResource, schema: str, short_key: str) -> Optional[str]:
        return await queries.DELETE_SHORT_URL.execute(conn, schema=schema, short_key=short_key)
