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
from .mappings import LOG_INDEX_SETTINGS, LOG_MAPPING, get_log_index_name

logger = logging.getLogger(__name__)


def _scrub_pii(text: Optional[str]) -> str:
    """Remove email and card-like patterns from text."""
    if not text:
        return ""
    text = re.sub(r"\b[\w.+-]+@[\w-]+\.[a-z]{2,}\b", "[redacted]", text)
    text = re.sub(r"\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b", "[redacted]", text)
    return text


class ElasticsearchLogBackend(LogBackendProtocol):
    """Batch log writer to OpenSearch/Elasticsearch."""

    def __init__(self):
        self._prefix = None
        # Set once the index is confirmed to exist (created by us or found
        # already present). Avoids an indices.exists() round-trip on every
        # batch — the index is never dropped by this backend at runtime, so
        # a positive result stays valid for the life of the process.
        self._index_ensured = False

    @property
    def name(self) -> str:
        return "elasticsearch"

    async def _ensure_index(self, es, index_name: str) -> None:
        if self._index_ensured:
            return
        if not await es.indices.exists(index=index_name):
            await es.indices.create(
                index=index_name,
                body={"mappings": LOG_MAPPING, "settings": LOG_INDEX_SETTINGS},
            )
            logger.info("Created log index '%s'.", index_name)
        self._index_ensured = True

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

        Note: the current mapping does not index ``details`` /
        ``stacktrace`` / ``request_context`` — only ``message`` survives.
        Callers get those keys back as ``None``.
        """
        es = get_client()
        if es is None:
            return []

        index_name = get_log_index_name(get_index_prefix())
        if not self._index_ensured:
            try:
                if not await es.indices.exists(index=index_name):
                    return []
                self._index_ensured = True
            except Exception as exc:
                logger.warning("ElasticsearchLogBackend.search_logs: exists check failed: %s", exc)
                return []

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
            resp = await es.search(index=index_name, body=body)
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
                    "stacktrace": None,
                    "request_context": None,
                }
            )
        return results

    async def get_log(self, log_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single log entry by its ES document id (the same ``id``
        ``search_logs`` returns). Returns ``None`` if unavailable or not found."""
        es = get_client()
        if es is None:
            return None

        index_name = get_log_index_name(get_index_prefix())
        try:
            resp = await es.get(index=index_name, id=log_id)
        except Exception as exc:
            logger.debug("ElasticsearchLogBackend.get_log: %s not found (%s)", log_id, exc)
            return None

        src = resp.get("_source", {})
        return {
            "id": resp.get("_id"),
            "catalog_id": src.get("catalog_id"),
            "collection_id": src.get("collection_id"),
            "event_type": src.get("event_type"),
            "level": src.get("level"),
            "is_system": src.get("is_system"),
            "message": src.get("message"),
            "timestamp": src.get("timestamp"),
            "details": None,
            "stacktrace": None,
            "request_context": None,
        }
