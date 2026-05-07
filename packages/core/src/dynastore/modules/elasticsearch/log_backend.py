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
Elasticsearch/OpenSearch log backend for batch log persistence.
"""

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from dynastore.extensions.logs.models import LogEntryCreate
from dynastore.models.protocols.logs import LogBackendProtocol
from .client import get_client, get_index_prefix
from .mappings import LOG_MAPPING, get_log_index_name

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

    @property
    def name(self) -> str:
        return "elasticsearch"

    async def write_batch(self, entries: List[LogEntryCreate]) -> Dict[str, Any]:
        """Write a batch of log entries to ES via bulk API."""
        es = get_client()
        if es is None:
            return {"status": "skipped", "count": 0, "backend": self.name}

        if not entries:
            return {"status": "success", "count": 0, "backend": self.name}

        try:
            prefix = get_index_prefix()
            index_name = get_log_index_name(prefix)

            # Ensure index exists
            if not await es.indices.exists(index=index_name):
                await es.indices.create(index=index_name, body={"mappings": LOG_MAPPING})
                logger.info("Created log index '%s'.", index_name)

            # Build bulk request: _index, _id, then document
            bulk_lines = []
            now = datetime.now(timezone.utc)
            for i, entry in enumerate(entries):
                entry_ts: datetime = getattr(entry, "timestamp", None) or now
                doc_id = f"{entry.catalog_id}:{entry.event_type}:{entry_ts.isoformat()}:{i}"
                doc = {
                    "id": getattr(entry, "id", None),
                    "catalog_id": entry.catalog_id,
                    "collection_id": entry.collection_id,
                    "event_type": entry.event_type,
                    "level": entry.level,
                    "is_system": entry.is_system,
                    "message": _scrub_pii(entry.message),
                    "timestamp": entry_ts,
                }
                bulk_lines.append({"index": {"_index": index_name, "_id": doc_id}})
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
