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

import os
import logging
import asyncio
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timezone, date

from dynastore.modules import get_protocol
from dynastore.models.protocols import DatabaseProtocol
from dynastore.modules.stats.storage import (
    AbstractStatsDriver,
    AccessRecord,
)

logger = logging.getLogger(__name__)


from dynastore.tools.plugin import ProtocolPlugin

class StatsService(ProtocolPlugin[object]):
    """
    Singleton facade over the active stats driver.
    """

    _instance = None
    _stats_driver: Optional[AbstractStatsDriver] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(StatsService, cls).__new__(cls)
        return cls._instance

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        """Standard lifecycle hook for the service."""
        driver = self.select_driver()
        if driver:
            # We don't enter the driver's lifespan here because
            # drivers should BE ProtocolPlugins registered & started by discovery.
            # BUT if we want to be safe, we can.
            # Actually, the pattern is: StatsModule starts StatsService,
            # StatsService selects a driver.
            # If the driver is already a started ProtocolPlugin (from discovery),
            # we just use it.
            logger.info(f"StatsService: Started with driver '{driver.name}'.")
        
        yield
        
        if self._stats_driver:
            await self._stats_driver.flush()
        logger.info("StatsService: Stopped.")

    # ------------------------------------------------------------------
    # Driver selection
    # ------------------------------------------------------------------

    def set_driver(self, driver: AbstractStatsDriver) -> None:
        """Explicitly set the active driver."""
        self._stats_driver = driver
        logger.info(f"StatsService: Active driver set to '{driver.name}'.")

    def select_driver(self) -> Optional[AbstractStatsDriver]:
        """
        Discover and activate the best available stats driver.
        """
        if self._stats_driver:
            return self._stats_driver

        target_name = os.environ.get("STATS_DRIVER")
        
        # Use unified discovery
        from dynastore.tools.discovery import get_protocols
        drivers = get_protocols(AbstractStatsDriver)

        if not drivers:
            logger.error("StatsService: No stats drivers registered. Service disabled.")
            return None

        if target_name:
            for d in drivers:
                if d.name == target_name:
                    self._stats_driver = d
                    break
            if not self._stats_driver:
                logger.error(
                    f"StatsService: Driver '{target_name}' not found. "
                    f"Available: {[d.name for d in drivers]}"
                )
        else:
            # Highest priority first
            self._stats_driver = drivers[0]

        if self._stats_driver:
            logger.info(f"StatsService: Selected driver '{self._stats_driver.name}'.")
        return self._stats_driver

    # ------------------------------------------------------------------
    # Public API (unchanged surface for callers)
    # ------------------------------------------------------------------

    async def flush(self):
        if self._stats_driver:
            await self._stats_driver.flush()

    def log_access(
        self,
        request: Any,
        background_tasks: Any,
        status_code: int,
        processing_time_ms: float,
        details: Optional[Dict[str, Any]] = None,
        catalog_id: Optional[str] = None,
    ):
        """
        Logs access for FastAPI route handlers where BackgroundTasks is available.
        """
        if not self._stats_driver:
            return

        record = self._create_record(
            request, status_code, processing_time_ms, details, catalog_id
        )
        background_tasks.add_task(self._stats_driver.buffer_record, record)

    def log_request_completion(
        self,
        request: Any,
        status_code: int,
        processing_time_ms: float,
        details: Optional[Dict[str, Any]] = None,
        catalog_id: Optional[str] = None,
    ):
        """
        Logs access for Middleware where BackgroundTasks is NOT available.
        Uses asyncio.create_task for fire-and-forget execution.
        """
        if not self._stats_driver:
            return

        try:
            record = self._create_record(
                request, status_code, processing_time_ms, details, catalog_id
            )
            from dynastore.modules.concurrency import get_background_executor

            get_background_executor().submit(
                self._stats_driver.buffer_record(record),
                task_name="stats_log_buffering",
            )
        except Exception as e:
            logger.error(f"StatsService: Failed to schedule log buffering: {e}")

    def _create_record(
        self,
        request: Any,
        status_code: int,
        processing_time_ms: float,
        details: Optional[Dict[str, Any]],
        catalog_id: Optional[str] = None,
    ) -> AccessRecord:
        """Internal helper to construct the AccessRecord from request state."""
        from dynastore.extensions.tools.request_state import (
            get_catalog_id,
            get_principal_id,
        )

        return AccessRecord(
            timestamp=datetime.now(timezone.utc),
            catalog_id=catalog_id or get_catalog_id(request),
            principal_id=get_principal_id(request),
            source_ip=request.client.host if request.client else "unknown",
            method=request.method,
            path=request.url.path,
            status_code=status_code,
            processing_time_ms=processing_time_ms,
            details=details or {},
        )

    async def get_summary(self, **kwargs: Any) -> Optional[Any]:
        """Proxies the call to the underlying driver's get_summary."""
        if self._stats_driver:
            return await self._stats_driver.get_summary(**kwargs)
        return None

    async def get_logs(self, **kwargs: Any) -> Optional[Any]:
        """Proxies the call to the underlying driver's get_logs."""
        if self._stats_driver:
            return await self._stats_driver.get_logs(**kwargs)
        return None


STATS_SERVICE = StatsService()
