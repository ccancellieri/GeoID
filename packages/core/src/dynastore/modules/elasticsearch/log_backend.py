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
Elasticsearch/OpenSearch log backend for batch log persistence and search.

The ES leg is the SOLE persistence path for application/event logs (#2749) —
there is no PostgreSQL fallback table. When ES is not configured, writes
degrade to the stdlib logger (see ``LogService.log_event``) and reads
degrade to an empty result, the same optional-module posture used
elsewhere in the codebase (e.g. IAM).
"""

import logging
import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from dynastore.extensions.logs.models import LogEntryCreate

from dynastore.models.protocols.logs import LogBackendProtocol
from .client import get_client, get_index_prefix
from .mappings import (
    LOG_INDEX_SETTINGS,
    LOG_MAPPING,
    get_log_index_name,
    get_log_read_index_target,
)

logger = logging.getLogger(__name__)


def _scrub_pii(text: Optional[str]) -> str:
    """Remove email and card-like patterns from text."""
    if not text:
        return ""
    text = re.sub(r"\b[\w.+-]+@[\w-]+\.[a-z]{2,}\b", "[redacted]", text)
    text = re.sub(r"\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b", "[redacted]", text)
    return text


def _scrub_pii_deep(value: Any) -> Any:
    """Recursively apply :func:`_scrub_pii` to every string leaf of *value*.

    ``request_context`` is a nested dict (headers, query params, URL) that
    can carry the same free-text PII patterns ``message`` is scrubbed for
    (#2798) — a shallow scrub would miss anything not at the top level.
    """
    if isinstance(value, str):
        return _scrub_pii(value)
    if isinstance(value, dict):
        return {k: _scrub_pii_deep(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_scrub_pii_deep(v) for v in value]
    return value


class ElasticsearchLogBackend(LogBackendProtocol):
    """Batch log writer to OpenSearch/Elasticsearch."""

    def __init__(self):
        self._prefix = None
        # Names of monthly indices confirmed to exist (created by us or
        # found already present). Avoids an indices.exists() round-trip on
        # every batch — once ensured, a given month's index is never dropped
        # by this backend at runtime, so a positive result stays valid for
        # the rest of that month. Keyed per-index (not a single bool) since
        # the active index name rolls over every calendar month (#2797).
        self._ensured_indices: set = set()

    @property
    def name(self) -> str:
        return "elasticsearch"

    async def _ensure_index(self, es, index_name: str) -> None:
        if index_name in self._ensured_indices:
            return
        if not await es.indices.exists(index=index_name):
            await es.indices.create(
                index=index_name,
                body={"mappings": LOG_MAPPING, "settings": LOG_INDEX_SETTINGS},
            )
            logger.info("Created log index '%s'.", index_name)
        self._ensured_indices.add(index_name)

    async def write_batch(self, entries: "List[LogEntryCreate]") -> Dict[str, Any]:
        """Write a batch of log entries to ES via bulk API."""
        es = get_client()
        if es is None:
            return {"status": "skipped", "count": 0, "backend": self.name}

        if not entries:
            return {"status": "success", "count": 0, "backend": self.name}

        try:
            index_name = get_log_index_name(get_index_prefix())
            await self._ensure_index(es, index_name)

            # Build bulk request: _index [+ _id], then document. Only
            # immediate (unbuffered) writes carry a caller-assigned
            # entry.id (see LogService.log_event) — those get an explicit
            # _id so the caller can round-trip to the exact document. Every
            # other (buffered, high-volume) entry omits _id and lets ES
            # assign one: an explicit _id forces a per-doc version lookup
            # on every bulk op (and risks a silent overwrite on collision),
            # which is wasted cost on the hot path.
            now = datetime.now(timezone.utc)
            bulk_lines = []
            for entry in entries:
                entry_ts: datetime = getattr(entry, "timestamp", None) or now
                entry_id = getattr(entry, "id", None)
                doc = {
                    "catalog_id": entry.catalog_id,
                    "collection_id": entry.collection_id,
                    "event_type": entry.event_type,
                    "level": entry.level,
                    "is_system": entry.is_system,
                    "message": _scrub_pii(entry.message),
                    "timestamp": entry_ts,
                }
                # Structured caller-attached fields (#2798) — exception
                # handlers stash these under `details`; older call sites use
                # the key `traceback` instead of `stacktrace`, so accept both
                # rather than silently dropping their payload.
                details = getattr(entry, "details", None) or {}
                stacktrace = details.get("stacktrace") or details.get("traceback")
                if stacktrace:
                    doc["stacktrace"] = _scrub_pii(stacktrace)
                request_context = details.get("request_context")
                if request_context:
                    doc["request_context"] = _scrub_pii_deep(request_context)
                action: Dict[str, Any] = {"_index": index_name}
                if entry_id:
                    action["_id"] = entry_id
                bulk_lines.append({"index": action})
                bulk_lines.append(doc)

            # Execute bulk
            response = await es.bulk(body=bulk_lines)
            errors = response.get("errors", False)

            if errors:
                error_count = sum(1 for item in response.get("items", []) if "error" in item)
                logger.warning(
                    "ElasticsearchLogBackend: %d/%d bulk errors writing %d entries",
                    error_count,
                    len(entries),
                    len(entries),
                )

            return {"status": "success", "count": len(entries), "backend": self.name}
        except Exception as exc:
            logger.warning("ElasticsearchLogBackend: write_batch failed: %s", exc)
            return {
                "status": "error",
                "count": 0,
                "backend": self.name,
                "error": str(exc),
            }

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
        """Query persisted log entries. Mirrors
        ``ElasticsearchStatsDriver.get_logs`` (access logs) for the event-log
        index. Returns ``[]`` on any failure (unavailable client, missing
        index, query error) — reads degrade silently, matching
        ``write_batch``'s "skipped" posture.

        Reads span every monthly log index plus the pre-#2797 flat index
        (``get_log_read_index_target``, #2797) — ``ignore_unavailable=True``
        means a month with no writes yet (or a fresh cluster with no log
        index at all) yields an empty result instead of an error.
        """
        es = get_client()
        if es is None:
            return []

        index_target = get_log_read_index_target(get_index_prefix())

        must: List[Dict[str, Any]] = []
        if catalog_id:
            must.append({"term": {"catalog_id": catalog_id}})
        if collection_id:
            must.append({"term": {"collection_id": collection_id}})
        if event_type:
            must.append({"term": {"event_type": event_type}})
        if level:
            must.append({"term": {"level": level.upper()}})
        if is_system is not None:
            must.append({"term": {"is_system": is_system}})
        if start_date or end_date:
            range_q: Dict[str, str] = {}
            if start_date:
                range_q["gte"] = start_date.isoformat()
            if end_date:
                range_q["lte"] = end_date.isoformat()
            must.append({"range": {"timestamp": range_q}})

        query = {"bool": {"must": must}} if must else {"match_all": {}}
        body = {
            "size": limit,
            "from": offset,
            "query": query,
            "sort": [{"timestamp": "desc"}],
        }

        try:
            resp = await es.search(
                index=index_target, body=body, params={"ignore_unavailable": "true"}
            )
        except Exception as exc:
            logger.warning("ElasticsearchLogBackend.search_logs failed: %s", exc)
            return []

        hits = resp.get("hits", {}).get("hits", [])
        results: List[Dict[str, Any]] = []
        for hit in hits:
            src = hit.get("_source", {})
            results.append(
                {
                    "id": hit.get("_id"),
                    "catalog_id": src.get("catalog_id"),
                    "collection_id": src.get("collection_id"),
                    "event_type": src.get("event_type"),
                    "level": src.get("level"),
                    "is_system": src.get("is_system"),
                    "message": src.get("message"),
                    "timestamp": src.get("timestamp"),
                    "details": None,
                    "stacktrace": src.get("stacktrace"),
                    "request_context": src.get("request_context"),
                }
            )
        return results

    async def get_log(self, log_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single log entry by its ES document id (the same ``id``
        ``search_logs`` returns). Returns ``None`` if unavailable or not found.

        A monthly-indexed doc's exact index name isn't known from the id
        alone (#2797), so this is a size-1 ``ids`` query against
        :func:`~.mappings.get_log_read_index_target` rather than a
        single-index ``GET`` — that also naturally covers the pre-#2797 flat
        index.
        """
        es = get_client()
        if es is None:
            return None

        index_target = get_log_read_index_target(get_index_prefix())
        try:
            resp = await es.search(
                index=index_target,
                body={"query": {"ids": {"values": [log_id]}}, "size": 1},
                params={"ignore_unavailable": "true"},
            )
        except Exception as exc:
            logger.debug("ElasticsearchLogBackend.get_log: %s not found (%s)", log_id, exc)
            return None

        hits = resp.get("hits", {}).get("hits", [])
        if not hits:
            return None
        hit = hits[0]
        src = hit.get("_source", {})
        return {
            "id": hit.get("_id"),
            "catalog_id": src.get("catalog_id"),
            "collection_id": src.get("collection_id"),
            "event_type": src.get("event_type"),
            "level": src.get("level"),
            "is_system": src.get("is_system"),
            "message": src.get("message"),
            "timestamp": src.get("timestamp"),
            "details": None,
            "stacktrace": src.get("stacktrace"),
            "request_context": src.get("request_context"),
        }
