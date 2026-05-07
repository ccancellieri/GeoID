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

from typing import Protocol, Optional, Any, runtime_checkable, List, TYPE_CHECKING
from datetime import datetime

if TYPE_CHECKING:
    from dynastore.modules.proxy.models import ShortURL, AnalyticsPage

@runtime_checkable
class ProxyProtocol(Protocol):
    """Protocol for short URL proxying and analytics."""

    async def create_short_url(self, engine: Any, catalog_id: str, long_url: str, custom_key: Optional[str] = None, collection_id: Optional[str] = None, comment: Optional[str] = None) -> "ShortURL":
        """Public API function to create a short URL."""
        ...

    async def get_long_url(self, engine: Any, catalog_id: str, short_key: str) -> Optional[str]:
        """Public API function to retrieve a long URL."""
        ...

    async def log_redirect(self, engine: Any, catalog_id: str, short_key: str, ip_address: str, user_agent: str, referrer: str, timestamp: datetime) -> None:
        """Public API function to log a redirect event."""
        ...

    async def get_urls_by_collection(self, engine: Any, catalog_id: str, collection_id: str, limit: int = 100, offset: int = 0) -> List["ShortURL"]:
        """Public API function to retrieve a list of short URLs by collection."""
        ...

    async def get_analytics(self, engine: Any, catalog_id: str, short_key: str, cursor: Optional[str] = None, page_size: int = 100, aggregate: bool = False, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> "AnalyticsPage":
        """Public API function to get analytics for a short URL."""
        ...

    async def delete_short_url(self, engine: Any, catalog_id: str, short_key: str) -> Optional[str]:
        """Public API function to delete a short URL."""
        ...

