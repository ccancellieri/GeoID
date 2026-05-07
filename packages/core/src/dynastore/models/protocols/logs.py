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

"""
Logging protocol definitions.
"""

from typing import Protocol, Optional, Any, List, Dict, runtime_checkable, TYPE_CHECKING

if TYPE_CHECKING:
    from dynastore.modules.catalog.log_manager import LogEntryCreate


@runtime_checkable
class LogsProtocol(Protocol):
    """
    Protocol for logging operations, enabling decoupled access to buffered
    log ingestion and querying.

    This protocol is used by extensions and services to log events and query
    logs in a loosely-coupled manner, supporting the protocol-based discovery pattern.
    """

    async def log_event(
        self,
        catalog_id: str,
        event_type: str,
        level: str = "INFO",
        message: Optional[str] = None,
        collection_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
        immediate: bool = False,
        is_system: bool = False
    ) -> Optional[int]:
        """
        Main entry point for logging events.

        Args:
            catalog_id: The catalog this event relates to
            event_type: Type of event
            level: Log level (INFO, WARNING, ERROR)
            message: Human-readable message
            collection_id: Optional collection ID
            details: Optional structured details
            db_resource: Optional database connection for immediate write
            immediate: If True, flush immediately if buffer is not full
            is_system: Whether this is a system-level log

        Returns:
            Log ID if written immediately, None otherwise
        """
        ...

    async def log_info(self, catalog_id: str, event_type: str, message: str, **kwargs) -> None:
        """Convenience wrapper for INFO level logs."""
        ...

    async def log_warning(self, catalog_id: str, event_type: str, message: str, **kwargs) -> None:
        """Convenience wrapper for WARNING level logs."""
        ...

    async def log_error(self, catalog_id: str, event_type: str, message: str, **kwargs) -> None:
        """Convenience wrapper for ERROR level logs."""
        ...

    async def list_logs(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        level: Optional[str] = None,
        event_type: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
        db_resource: Optional[Any] = None
    ) -> List[Dict[str, Any]]:
        """
        Lists log entries with filtering and pagination.
        """
        ...

    async def get_log_by_id(
        self,
        log_id: int,
        catalog_id: str,
        db_resource: Optional[Any] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific log entry by ID.
        """
        ...

    async def flush(self) -> None:
        """
        Flushes all buffered log entries to the database immediately.
        """
        ...


@runtime_checkable
class LogBackendProtocol(Protocol):
    """
    Protocol for pluggable log backend implementations.

    Modules implement this to receive batched log entries from LogService.
    Multiple backends can coexist (e.g., PostgreSQL + Elasticsearch + GCP Cloud Logging).
    Discovered via get_protocol(LogBackendProtocol).

    Implementations should:
    - Not raise exceptions; log failures internally and return error status
    - Handle gracefully when backend is not initialized (return skipped status)
    - Be idempotent (duplicate entries with same ID are tolerable)
    - Scrub PII as appropriate (details field is NOT forwarded by LogService, but message may contain sensitive data)
    """

    async def write_batch(self, entries: List["LogEntryCreate"]) -> Dict[str, Any]:
        """
        Write a batch of log entries to this backend.

        Args:
            entries: List of LogEntryCreate objects to persist

        Returns:
            Status dict with keys:
            - "status": "success" | "skipped" | "error"
            - "count": number of entries written
            - "backend": name of this backend
            - Optional: "error" (error message if status="error")
        """
        ...

    @property
    def name(self) -> str:
        """
        Unique identifier for this backend (e.g., 'elasticsearch', 'gcp_cloud_logging').
        Used for logging and debugging.
        """
        ...
