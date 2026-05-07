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

from typing import Protocol, Any, Dict, Optional, runtime_checkable

@runtime_checkable
class StatsProtocol(Protocol):
    """
    Protocol for recording and retrieving access statistics and logs.
    """

    async def initialize(self, app_state: Any) -> None:
        """Initializes the statistics service and its underlying driver."""
        ...

    async def log_access(
        self,
        request: Any,
        status_code: int,
        processing_time_ms: float,
        details: Optional[Dict[str, Any]] = None,
        schema: str = "catalog"
    ) -> None:
        """Logs an API access event to the statistics backend."""
        ...

    def log_request_completion(
        self,
        request: Any,
        status_code: int,
        processing_time_ms: float,
        details: Optional[Dict[str, Any]] = None,
        catalog_id: Optional[str] = None,
    ) -> None:
        """
        Logs access for Middleware where BackgroundTasks is NOT available.
        Uses fire-and-forget execution (sync wrapper).
        """
        ...

    async def get_summary(self, **kwargs) -> Any:
        """Retrieves aggregated access statistics (e.g. total requests, average latency)."""
        ...

    async def get_logs(self, **kwargs) -> Any:
        """Retrieves raw access logs/events."""
        ...
