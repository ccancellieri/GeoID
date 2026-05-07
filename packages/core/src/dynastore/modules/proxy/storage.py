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

import abc
import datetime
import os
from contextlib import asynccontextmanager
from typing import Optional, List, AsyncGenerator

from dynastore.modules.db_config.query_executor import DbConnection, DbResource
from dynastore.tools.plugin import ProtocolPlugin

from .models import ShortURL, AnalyticsPage


class AbstractProxyStorage(ProtocolPlugin[object]):
    """
    Abstract base class (and ProtocolPlugin category) for all proxy storage drivers.

    Priority is compared **only** among ``AbstractProxyStorage`` subclasses via
    ``get_protocols(AbstractProxyStorage)``.  The driver with the highest
    ``priority`` is used unless ``PROXY_STORAGE_DRIVER`` env var selects by name.

    Each concrete driver must implement ``lifespan`` to perform its one-time
    schema / table initialisation, replacing the old ``initialize(conn)`` method.
    """

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Unique name used for env-var driver selection (PROXY_STORAGE_DRIVER=<name>)."""
        ...

    @asynccontextmanager
    async def lifespan(self, app_state: object) -> AsyncGenerator[None, None]:
        """
        Override to initialise the storage backend (schema, tables, etc.)
        and clean up on exit.  The default is a no-op.
        """
        yield

    @abc.abstractmethod
    async def insert_short_url(self, conn: DbResource, schema: str, long_url: str, custom_key: Optional[str] = None, collection_id: Optional[str] = None, comment: Optional[str] = None) -> ShortURL:
        raise NotImplementedError

    @abc.abstractmethod
    async def select_urls_by_collection(self, conn: DbResource, schema: str, collection_id: str, limit: int = 100, offset: int = 0) -> List[ShortURL]:
        raise NotImplementedError

    @abc.abstractmethod
    async def select_long_url(self, conn: DbResource, schema: str, short_key: str) -> Optional[str]:
        raise NotImplementedError

    @abc.abstractmethod
    async def insert_redirect_log(self, schema: str, short_key: str, ip_address: str, user_agent: str, referrer: str, timestamp: datetime.datetime):
        """Buffer a redirect log. Analytics are stored in Elasticsearch."""
        raise NotImplementedError

    @abc.abstractmethod
    async def flush(self):
        """Flushes any buffered analytics to Elasticsearch."""
        raise NotImplementedError

    @abc.abstractmethod
    async def select_analytics(self, schema: str, short_key: str, cursor: Optional[str] = None, page_size: int = 100, aggregate: bool = False, start_date: Optional[datetime.datetime] = None, end_date: Optional[datetime.datetime] = None) -> AnalyticsPage:
        raise NotImplementedError

    @abc.abstractmethod
    async def drop_short_url(self, conn: DbResource, schema: str, short_key: str) -> Optional[str]:
        raise NotImplementedError