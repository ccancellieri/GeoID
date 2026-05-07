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
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
from ipaddress import IPv4Address, IPv6Address

from dynastore.tools.plugin import ProtocolPlugin
from pydantic import BaseModel, field_validator, ConfigDict

logger = logging.getLogger(__name__)

# --- Models ---


class AccessRecord(BaseModel):
    id: Optional[int] = None
    timestamp: datetime
    catalog_id: Optional[str] = None
    principal_id: Optional[str] = None
    source_ip: Union[str, IPv4Address, IPv6Address]
    method: str
    path: str
    status_code: int
    processing_time_ms: float
    details: Dict[str, Any] = {}

    @field_validator("source_ip", mode="before")
    @classmethod
    def normalize_ip(cls, v):
        if isinstance(v, (IPv4Address, IPv6Address)):
            return str(v)
        return v


class StatsSummary(BaseModel):
    """Aggregated statistics over a period."""

    total_requests: int
    average_latency_ms: float
    status_code_distribution: Dict[str, int]
    unique_principals: int


class AccessLogPage(BaseModel):
    """A paginated page of access logs."""

    logs: List[AccessRecord]
    summary: Optional[StatsSummary] = None
    next_cursor: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


# --- Abstract Driver ---


class AbstractStatsDriver(ProtocolPlugin[object]):
    """
    Abstract base class (and ProtocolPlugin category) for all stats drivers.

    Priority is evaluated among AbstractStatsDriver subclasses via
    ``get_protocols(AbstractStatsDriver)``.  A higher ``priority`` means the
    driver is preferred when ``STATS_DRIVER`` env var is not explicitly set.
    """

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Unique name used for env-var selection (STATS_DRIVER=<name>)."""
        ...

    @abc.abstractmethod
    async def buffer_record(self, record: AccessRecord): ...

    @abc.abstractmethod
    async def flush(self, conn=None): ...

    @abc.abstractmethod
    async def get_summary(
        self,
        schema: Optional[str] = None,
        catalog_id: Optional[str] = None,
        principal_id: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        **kwargs: Any,
    ) -> StatsSummary: ...

    @abc.abstractmethod
    async def get_logs(
        self,
        schema: Optional[str] = None,
        catalog_id: Optional[str] = None,
        principal_id: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> AccessLogPage: ...


# --- Convenience functions (route through active driver) ---


def _get_driver() -> Optional[AbstractStatsDriver]:
    """Resolve the active stats driver via StatsService."""
    try:
        from dynastore.modules.stats.service import STATS_SERVICE
        return STATS_SERVICE._stats_driver
    except Exception:
        return None


async def get_access_logs(
    engine=None,
    *,
    principal_id: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    cursor: Optional[str] = None,
    page_size: int = 100,
    schema: Optional[str] = None,
) -> AccessLogPage:
    """Convenience function: query access logs via the active stats driver."""
    driver = _get_driver()
    if not driver:
        return AccessLogPage(logs=[])
    return await driver.get_logs(
        schema=schema,
        principal_id=principal_id,
        start_date=start_date,
        end_date=end_date,
        limit=page_size,
        cursor=cursor,
    )


async def get_stats_summary(
    engine=None,
    *,
    schema: Optional[str] = None,
    catalog_id: Optional[str] = None,
    principal_id: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> StatsSummary:
    """Convenience function: get stats summary via the active stats driver."""
    driver = _get_driver()
    if not driver:
        return StatsSummary(
            total_requests=0,
            average_latency_ms=0.0,
            status_code_distribution={},
            unique_principals=0,
        )
    return await driver.get_summary(
        schema=schema,
        catalog_id=catalog_id,
        principal_id=principal_id,
        start_date=start_date,
        end_date=end_date,
    )
