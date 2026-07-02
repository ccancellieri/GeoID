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
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, AsyncGenerator
from pydantic import BaseModel, ConfigDict

from dynastore.tools.plugin import ProtocolPlugin
from dynastore.models.shared_models import SYSTEM_CATALOG_ID
from dynastore.models.protocols import LogsProtocol
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

# ==============================================================================
#  #2749: PostgreSQL log persistence has been removed entirely.
#
#  Logs flow buffer -> chunk -> LogBackendProtocol.write_batch only
#  (Elasticsearch in practice, see modules/elasticsearch/log_backend.py).
#  There is no {schema}.logs table, no catalog.system_logs table, no
#  per-collection LIST partition, and no lifecycle hook performing DDL —
#  the entire class of "partition-CREATE takes ACCESS EXCLUSIVE inside the
#  collection-creation transaction" wedge this issue started from is
#  retired by removing the DDL, not by isolating it.
#
#  Without a registered LogBackendProtocol, writes degrade to the stdlib
#  logger and reads return an empty result — the same optional-module
#  posture as every other pluggable backend in this codebase (e.g. IAM).
#
#  Existing ``{schema}.logs`` / ``catalog.system_logs`` tables on already
#  deployed databases are left in place as dead weight; nothing in this
#  codebase creates, writes, or reads them anymore. Dropping them is
#  optional offline cleanup (see the PR description), never runtime DDL.
# ==============================================================================


# --- Log Entry Model (Local Definition) ---


class LogEntryCreate(BaseModel):
    """Pydantic model for creating log entries."""

    catalog_id: str
    collection_id: Optional[str] = None
    event_type: str
    level: str = "INFO"
    message: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    is_system: bool = False
    # Stamped at log_event() call time — the event's real occurrence time.
    # Without this, a buffered entry only learns its timestamp at flush
    # time, which can lag the actual event by up to flush_interval_seconds
    # and reorders entries relative to when they actually happened.
    timestamp: Optional[datetime] = None
    # Set only for immediate (unbuffered) writes — see LogService.log_event.
    # The backend uses it as the persisted document id when present so a
    # synchronous caller can round-trip to the exact entry it just wrote.
    id: Optional[str] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


# ---------------------------------------------------------------------------
# Valkey-buffered producer/drainer (#2833)
#
# ``LogService._dispatch_to_backends`` is the single seam both the buffered
# aggregator flush and an ``immediate=True`` write go through. Buffered
# flushes try RPUSHing the batch onto a shared, bounded Valkey list first —
# one leader-elected drainer (``log_drainer.py``) then LPOPs chunks and
# calls ``write_batch_to_backends`` below, giving ES one writer instead of
# one per pod under a write burst. Any Valkey trouble (unavailable, no
# ListCacheBackend registered, RPUSH error) falls through to dispatching
# straight to the backends — the original #2749 behavior — so producers
# never block on Valkey. ``immediate=True`` writes always go straight to
# the backend: they exist for Cloud Run scale-to-zero reliability
# (log_event's docstring), and parking one in a queue that may not drain
# before the instance dies would defeat that purpose.
# ---------------------------------------------------------------------------

_QUEUE_DROP_WARNING_COOLDOWN = 30.0  # seconds — mirrors AsyncBufferAggregator
_queue_dropped_since_warning = 0
_last_queue_drop_warning = 0.0


def _warn_queue_drop(dropped: int) -> None:
    """Rate-limited warning for Valkey queue LTRIM drops (mirrors
    ``AsyncBufferAggregator``'s bounded-buffer drop warning: one summary
    line per cooldown window instead of one per push under sustained
    drainer slowness)."""
    global _queue_dropped_since_warning, _last_queue_drop_warning
    _queue_dropped_since_warning += dropped
    now = time.monotonic()
    if now - _last_queue_drop_warning >= _QUEUE_DROP_WARNING_COOLDOWN:
        logger.warning(
            "LogService: Valkey log queue at cap — dropped %d oldest entr%s "
            "in the last %.0fs (drainer too slow to keep up).",
            _queue_dropped_since_warning,
            "y" if _queue_dropped_since_warning == 1 else "ies",
            _QUEUE_DROP_WARNING_COOLDOWN,
        )
        _last_queue_drop_warning = now
        _queue_dropped_since_warning = 0


async def write_batch_to_backends(entries: List[LogEntryCreate]) -> None:
    """Fan out ``entries`` directly to every registered ``LogBackendProtocol``.

    The actual backend-dispatch primitive: called by
    ``LogService._dispatch_to_backends`` when Valkey is unavailable/errors
    (or for an ``immediate`` write) and by the Valkey drainer
    (``log_drainer.py``) after popping a chunk off the shared queue — one
    code path, so ES sees the same ``write_batch`` behavior regardless of
    which route a batch arrived by.
    """
    if not entries:
        return

    from dynastore.models.protocols.logs import LogBackendProtocol

    backends = get_protocol(LogBackendProtocol)
    if not backends:
        logger.debug(
            "LogService: no LogBackendProtocol registered; %d entries dropped.",
            len(entries),
        )
        return

    # get_protocol may return a single instance or a list; normalize to list
    backend_list = [backends] if not isinstance(backends, list) else backends
    for backend in backend_list:
        try:
            result = await backend.write_batch(entries)
            logger.debug("Log backend '%s' result: %s", backend.name, result)
        except Exception as exc:
            logger.warning("Log backend '%s' failed: %s", backend.name, exc)


async def _push_to_valkey_queue(entries: List[LogEntryCreate]) -> bool:
    """Best-effort producer: RPUSH ``entries`` onto the shared Valkey queue.

    Returns ``True`` if the push succeeded (caller must not also dispatch
    directly), ``False`` if Valkey is unavailable, the active cache backend
    has no list ops, or the push itself failed — the caller falls back to
    direct dispatch in every ``False`` case. Never raises.
    """
    try:
        from dynastore.tools.cache import get_cache_manager
        from dynastore.models.protocols.cache import ListCacheBackend
        from dynastore.modules.catalog.log_service_config import load as load_log_config

        backend = get_cache_manager().get_async_backend()
        if not isinstance(backend, ListCacheBackend):
            return False

        cfg = await load_log_config()
        values = [entry.model_dump_json().encode("utf-8") for entry in entries]
        dropped = await backend.rpush_trimmed(
            cfg.valkey_queue_key, values, max_len=cfg.valkey_queue_max_len
        )
        if dropped:
            _warn_queue_drop(dropped)
        return True
    except Exception as exc:
        logger.debug(
            "LogService: Valkey queue push failed (%s); falling back to "
            "direct-to-backend dispatch.", exc,
        )
        return False


# --- Log Service (similar to StatsService) ---


class LogService(ProtocolPlugin[Any], LogsProtocol):
    """Singleton service for buffered, high-throughput log ingestion.

    Persistence is backend-dispatch only (#2749): entries are buffered in
    an ``AsyncBufferAggregator`` and flushed in chunks to every registered
    ``LogBackendProtocol`` provider. There is no database engine here at
    all — no PG connection, no transaction, no DDL.
    """

    # Protocol attributes — lower means higher precedence in get_protocols()
    priority: int = 10

    def __init__(self):
        self._aggregator: Optional[Any] = None
        self._aggregator_started: bool = False

    async def _build_aggregator(self) -> Any:
        """Build the buffer aggregator from ``LogServiceConfig`` (#2749).

        Resolved once here — the aggregator is built once and cannot be
        rewired live, so a config edit only takes effect on the next
        process start/lifespan (same caveat as ``ElasticsearchClientConfig``).
        """
        from dynastore.tools.async_utils import AsyncBufferAggregator
        from dynastore.modules.catalog.log_service_config import load as load_log_config

        cfg = await load_log_config()
        logger.info(
            "LogService initialized with flush_threshold=%s, flush_interval=%ss, "
            "buffer_max_size=%s",
            cfg.flush_threshold,
            cfg.flush_interval_seconds,
            cfg.buffer_max_size,
        )
        return AsyncBufferAggregator(
            flush_callback=self._flush_batch,
            threshold=cfg.flush_threshold,
            interval=cfg.flush_interval_seconds,
            name="LogAggregator",
            max_size=cfg.buffer_max_size,
        )

    @asynccontextmanager
    async def lifespan(self, app_state: Any) -> AsyncGenerator[None, None]:
        """Lifecycle hook for LogService."""
        self._aggregator = await self._build_aggregator()

        try:
            yield
        finally:
            await self.stop()

    async def start(self) -> None:
        """Deprecated: use lifespan instead. Legacy support for manual startup."""
        self._aggregator = await self._build_aggregator()

    async def stop(self) -> None:
        """Flushes remaining entries and tears down the aggregator."""
        if self._aggregator:
            logger.info("LogService shutting down...")
            await self._aggregator.stop()
            self._aggregator = None
            logger.info("LogService shutdown complete.")

    def _get_backend_protocol(self):
        from dynastore.models.protocols.logs import LogBackendProtocol
        return LogBackendProtocol

    def _backend_available(self) -> bool:
        return bool(get_protocol(self._get_backend_protocol()))

    async def _dispatch_to_backends(
        self, entries: List[LogEntryCreate], *, immediate: bool = False
    ) -> None:
        """Write a chunk of entries to every registered ``LogBackendProtocol``.

        Single seam for backend dispatch — both the buffered flush
        (``_flush_batch``) and an ``immediate=True`` call go through here.

        Producer/drainer split (#2833): a buffered flush (``immediate=
        False``) first tries pushing the batch onto the shared Valkey queue
        (:func:`_push_to_valkey_queue`) so a single leader-elected drainer
        (``log_drainer.py``) does the ES write instead of every pod. Any
        Valkey trouble falls through to :func:`write_batch_to_backends`
        below — the original #2749 direct-dispatch behavior, and always
        the path for ``immediate`` writes (see :meth:`log_event`'s
        docstring for why those must never be queued).
        """
        if not entries:
            return

        if not immediate and await _push_to_valkey_queue(entries):
            return

        await write_batch_to_backends(entries)

    async def _flush_batch(self, entries: List[LogEntryCreate]):
        """Callback for AsyncBufferAggregator. Dispatches to backends."""
        await self._dispatch_to_backends(entries)

    async def flush(self):
        """Manually trigger a flush (legacy support)."""
        if self._aggregator:
            await self._aggregator._trigger_flush(wait=True)

    async def log_event(
        self,
        catalog_id: str,
        event_type: str,
        level: str = "INFO",
        message: Optional[str] = None,
        collection_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        immediate: bool = False,
        is_system: bool = False,
    ) -> Optional[str]:
        """
        Main entry point for logging events.

        Args:
            catalog_id: The catalog this event relates to (required).
            event_type: Type of event (e.g., "gcp_bucket_created", "catalog_creation").
            level: Log level (INFO, WARNING, ERROR).
            message: Human-readable message.
            collection_id: Optional collection ID if event is collection-scoped.
            details: Optional structured details dictionary.
            immediate: If True, dispatch to the backend now instead of
                waiting for the buffer's threshold/timer flush. Use for
                sparse, high-value events (lifecycle transitions) whose row
                would otherwise be lost when a Cloud Run instance scales to
                zero before the timer fires; do not use on a hot path.
            is_system: Whether this is a system-level log.

        Returns:
            The entry's backend id when ``immediate=True`` and a backend is
            available (so a caller can build a deep link to the exact row),
            ``None`` otherwise — a buffered write's id is not known until a
            later flush, so it is never returned (matches pre-#2749 behavior,
            which only ever returned an id for a synchronous write).
        """
        if not self._backend_available():
            # No backend registered (optional-module posture, #2749) —
            # fall back to the stdlib logger so the event is not silently lost.
            safe_msg = f"[LogService] {level} | {catalog_id} | {event_type}: {message}"
            if level.upper() == "ERROR":
                logger.error(safe_msg)
            elif level.upper() == "WARNING":
                logger.warning(safe_msg)
            else:
                logger.info(safe_msg)
            return None

        from dynastore.tools.correlation import get_correlation_id
        cid = get_correlation_id()
        if cid is not None:
            details = dict(details) if details else {}
            details.setdefault("request_context", {})
            if isinstance(details["request_context"], dict):
                details["request_context"].setdefault("correlation_id", cid)

        now = datetime.now(timezone.utc)
        entry_id = f"{catalog_id}:{event_type}:{now.isoformat()}" if immediate else None
        entry = LogEntryCreate(
            catalog_id=catalog_id,
            collection_id=collection_id,
            event_type=event_type,
            level=level,
            message=message,
            details=details,
            is_system=is_system,
            timestamp=now,
            id=entry_id,
        )

        if immediate:
            await self._dispatch_to_backends([entry], immediate=True)
            return entry_id

        aggregator = self._aggregator
        if aggregator is None:
            return None

        if not self._aggregator_started:
            from dynastore.modules.concurrency import default_executor
            default_executor.submit(aggregator.start(), "log_aggregator_start")
            self._aggregator_started = True

        await aggregator.add(entry)
        return None

    async def log_info(
        self, catalog_id: str, event_type: str, message: str, **kwargs
    ) -> None:
        """Convenience wrapper for INFO level logs."""
        is_system = kwargs.pop("is_system", False)
        await self.log_event(
            catalog_id,
            event_type,
            level="INFO",
            message=message,
            is_system=is_system,
            **kwargs,
        )

    async def log_warning(
        self, catalog_id: str, event_type: str, message: str, **kwargs
    ) -> None:
        """Convenience wrapper for WARNING level logs."""
        is_system = kwargs.pop("is_system", False)
        await self.log_event(
            catalog_id,
            event_type,
            level="WARNING",
            message=message,
            is_system=is_system,
            **kwargs,
        )

    async def log_error(
        self, catalog_id: str, event_type: str, message: str, **kwargs
    ) -> None:
        """Convenience wrapper for ERROR level logs."""
        is_system = kwargs.pop("is_system", False)
        await self.log_event(
            catalog_id,
            event_type,
            level="ERROR",
            message=message,
            is_system=is_system,
            **kwargs,
        )

    async def shutdown(self):
        """Deprecated: use stop() or lifespan."""
        await self.stop()

    async def get_log_by_id(
        self, log_id: str, catalog_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific log entry by its backend-assigned id (#2749:
        Elasticsearch-backed). Returns ``None`` when no backend is
        registered, the entry does not exist, or it belongs to a different
        catalog than requested (unless the caller asked for the
        platform-wide ``_system_`` stream).
        """
        backend = get_protocol(self._get_backend_protocol())
        if not backend:
            return None
        backend = backend[0] if isinstance(backend, list) else backend
        get_log = getattr(backend, "get_log", None)
        if get_log is None:
            return None

        entry = await get_log(log_id)
        if entry is None:
            return None
        if catalog_id != SYSTEM_CATALOG_ID and entry.get("catalog_id") != catalog_id:
            return None
        return entry

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
        List log entries with filtering and pagination (#2749:
        Elasticsearch-backed). Returns ``[]`` when no backend is registered.

        ``catalog_id == SYSTEM_CATALOG_ID`` ("_system_") returns the
        platform-wide ``is_system`` stream with no catalog filter; any other
        value filters by that ``catalog_id`` regardless of ``is_system`` —
        matching the pre-#2749 behavior of UNIONing system-tagged and
        tenant-tagged rows for a given catalog.

        Reads consult a single backend (the first one discovered); unlike
        writes, results are not fanned out across multiple backends.
        """
        backend = get_protocol(self._get_backend_protocol())
        if not backend:
            return []
        backend = backend[0] if isinstance(backend, list) else backend
        search_logs = getattr(backend, "search_logs", None)
        if search_logs is None:
            return []

        is_system = True if catalog_id == SYSTEM_CATALOG_ID else None
        filter_catalog_id = None if catalog_id == SYSTEM_CATALOG_ID else catalog_id

        return await search_logs(
            catalog_id=filter_catalog_id,
            collection_id=collection_id,
            event_type=event_type,
            level=level,
            is_system=is_system,
            limit=limit,
            offset=offset,
        )


# Global instance
LOG_SERVICE = LogService()

# --- Convenience Functions (matches extensions/logs/log_manager.py API) ---


async def log_event(
    catalog_id: str,
    event_type: str,
    level: str = "INFO",
    message: Optional[str] = None,
    collection_id: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
    immediate: bool = False,
    is_system: bool = False,
) -> Optional[str]:
    """
    Main entry point for logging events to the Catalog Log Service.
    Fails safely (logs to stdout) if service is not initialized.
    """
    service = get_protocol(LogsProtocol)
    if not service:
        # Fallback to standard logging if no service available
        logger.warning(f"[LogService Fallback] {level} | {catalog_id}: {message}")
        return None

    return await service.log_event(
        catalog_id=catalog_id,
        event_type=event_type,
        level=level,
        message=message,
        collection_id=collection_id,
        details=details,
        immediate=immediate,
        is_system=is_system,
    )


async def log_info(catalog_id: str, event_type: str, message: str, **kwargs):
    """Convenience wrapper for INFO level logs."""
    is_system = kwargs.pop("is_system", False)
    await log_event(
        catalog_id,
        event_type,
        level="INFO",
        message=message,
        is_system=is_system,
        **kwargs,
    )


async def log_warning(catalog_id: str, event_type: str, message: str, **kwargs):
    """Convenience wrapper for WARNING level logs."""
    is_system = kwargs.pop("is_system", False)
    await log_event(
        catalog_id,
        event_type,
        level="WARNING",
        message=message,
        is_system=is_system,
        **kwargs,
    )


async def log_error(catalog_id: str, event_type: str, message: str, **kwargs):
    """Convenience wrapper for ERROR level logs."""
    is_system = kwargs.pop("is_system", False)
    await log_event(
        catalog_id,
        event_type,
        level="ERROR",
        message=message,
        is_system=is_system,
        **kwargs,
    )
