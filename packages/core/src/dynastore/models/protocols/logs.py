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

"""
Logging protocol definitions.

#2749: PostgreSQL log persistence has been removed. Logs flow buffer ->
chunk -> ``LogBackendProtocol.write_batch`` only (Elasticsearch in
practice); there is no caller-supplied database connection anywhere in
this protocol anymore. ``log_event``/``list_logs``/``get_log_by_id`` no
longer accept a ``db_resource`` — that concept (force a synchronous write
on the caller's own PG transaction) does not apply to a backend-dispatch
model.
"""

from datetime import datetime
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
        immediate: bool = False,
        is_system: bool = False
    ) -> Optional[str]:
        """
        Main entry point for logging events.

        Args:
            catalog_id: The catalog this event relates to
            event_type: Type of event
            level: Log level (INFO, WARNING, ERROR)
            message: Human-readable message
            collection_id: Optional collection ID
            details: Optional structured details
            immediate: If True, dispatch to the backend now instead of
                waiting for the buffer's threshold/timer flush. Use for
                sparse, high-value events (lifecycle transitions) where the
                buffered timer could lose the row to a scale-to-zero
                instance; do not use on a hot path.
            is_system: Whether this is a system-level log

        Returns:
            The entry's backend id when ``immediate=True`` and a backend is
            available, ``None`` otherwise (a buffered write's id is not
            known until a later flush).
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
    ) -> List[Dict[str, Any]]:
        """
        Lists log entries with filtering and pagination. Reads the backend
        (Elasticsearch); returns ``[]`` when no backend is available.
        """
        ...

    async def get_log_by_id(
        self,
        log_id: str,
        catalog_id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific log entry by its backend-assigned id.
        """
        ...

    async def flush(self) -> None:
        """
        Flushes all buffered log entries to the backend immediately.
        """
        ...


@runtime_checkable
class LogBackendProtocol(Protocol):
    """
    Protocol for pluggable log backend implementations.

    Modules implement this to receive batched log entries from LogService.
    Multiple backends can coexist (e.g., Elasticsearch + GCP Cloud Logging).
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

    async def search_logs(
        self,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        event_type: Optional[str] = None,
        level: Optional[str] = None,
        is_system: Optional[bool] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        Query persisted log entries. Returns ``[]`` when the backend is
        unavailable or the query fails — reads degrade silently, mirroring
        ``write_batch``'s "skipped" status.
        """
        ...

    async def get_log(self, log_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a single log entry by its backend-assigned id (the ``id`` key
        returned by ``search_logs``). Returns ``None`` when unavailable or
        not found.
        """
        ...

    @property
    def name(self) -> str:
        """
        Unique identifier for this backend (e.g., 'elasticsearch', 'gcp_cloud_logging').
        Used for logging and debugging.
        """
        ...
