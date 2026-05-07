"""
OpenSearch/Elasticsearch stats driver.

Stores access logs and computes aggregations entirely in OpenSearch,
replacing the previous PostgreSQL partitioned tables.

Index naming:
    {prefix}_access_logs_{catalog_id}   — one index per catalog
    {prefix}_access_logs_system         — platform-level logs

The driver uses the shared async client from
``dynastore.modules.elasticsearch.client``.
"""

import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from dynastore.modules.stats.storage import (
    AbstractStatsDriver,
    AccessLogPage,
    AccessRecord,
    StatsSummary,
)

logger = logging.getLogger(__name__)

# -- Index mapping (created lazily on first write) --

_ACCESS_LOG_MAPPING = {
    "mappings": {
        "properties": {
            "timestamp": {"type": "date"},
            "catalog_id": {"type": "keyword"},
            "principal_id": {"type": "keyword"},
            "source_ip": {"type": "ip"},
            "method": {"type": "keyword"},
            "path": {"type": "keyword"},
            "status_code": {"type": "integer"},
            "processing_time_ms": {"type": "float"},
            "details": {"type": "object", "enabled": False},
        }
    },
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "index.mapping.total_fields.limit": 200,
    },
}


class ElasticsearchStatsDriver(AbstractStatsDriver):
    """Stats driver backed by OpenSearch / Elasticsearch."""

    priority: int = 50

    def __init__(self) -> None:
        from dynastore.tools.async_utils import AsyncBufferAggregator

        self.buffer = AsyncBufferAggregator(
            flush_callback=self._flush_records,
            threshold=int(os.environ.get("STATS_FLUSH_THRESHOLD", 1000)),
            interval=float(os.environ.get("STATS_FLUSH_INTERVAL", 5.0)),
            name="stats_es",
        )
        self._known_indices: set[str] = set()
        self._is_testing = os.environ.get("PYTEST_CURRENT_TEST") is not None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def initialize(self, app_state: object = None) -> None:
        await self.buffer.start()
        logger.info("ElasticsearchStatsDriver: Initialized.")

    async def shutdown(self) -> None:
        await self.buffer.stop()

    # ------------------------------------------------------------------
    # AbstractStatsDriver interface
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        return "elasticsearch"

    async def buffer_record(self, record: AccessRecord) -> None:
        await self.buffer.add(record)

    async def flush(self, conn=None) -> None:
        async with self.buffer._lock:
            records = self.buffer._buffer[:]
            self.buffer._buffer.clear()
        if records:
            await self._flush_records(records)

    async def get_summary(
        self,
        schema: Optional[str] = None,
        catalog_id: Optional[str] = None,
        principal_id: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        **kwargs: Any,
    ) -> StatsSummary:
        es = self._get_client()
        if es is None:
            return StatsSummary(
                total_requests=0,
                average_latency_ms=0.0,
                status_code_distribution={},
                unique_principals=0,
            )

        index = self._resolve_index(catalog_id)
        must: list[dict] = []
        if catalog_id:
            must.append({"term": {"catalog_id": catalog_id}})
        if principal_id:
            must.append({"term": {"principal_id": principal_id}})
        if start_date or end_date:
            range_q: dict = {}
            if start_date:
                range_q["gte"] = start_date.isoformat()
            if end_date:
                range_q["lte"] = end_date.isoformat()
            must.append({"range": {"timestamp": range_q}})

        query = {"bool": {"must": must}} if must else {"match_all": {}}

        body = {
            "size": 0,
            "query": query,
            "aggs": {
                "status_codes": {"terms": {"field": "status_code", "size": 50}},
                "unique_principals": {"cardinality": {"field": "principal_id"}},
                "avg_latency": {"avg": {"field": "processing_time_ms"}},
            },
        }

        try:
            resp = await es.search(index=index, body=body)
        except Exception as exc:
            logger.warning("ElasticsearchStatsDriver.get_summary failed: %s", exc)
            return StatsSummary(
                total_requests=0,
                average_latency_ms=0.0,
                status_code_distribution={},
                unique_principals=0,
            )

        hits_total = resp.get("hits", {}).get("total", {})
        total = hits_total.get("value", 0) if isinstance(hits_total, dict) else int(hits_total)
        aggs = resp.get("aggregations", {})

        status_dist: Dict[str, int] = {}
        for bucket in aggs.get("status_codes", {}).get("buckets", []):
            status_dist[str(bucket["key"])] = bucket["doc_count"]

        return StatsSummary(
            total_requests=total,
            average_latency_ms=aggs.get("avg_latency", {}).get("value", 0.0) or 0.0,
            status_code_distribution=status_dist,
            unique_principals=int(
                aggs.get("unique_principals", {}).get("value", 0)
            ),
        )

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
    ) -> AccessLogPage:
        es = self._get_client()
        if es is None:
            return AccessLogPage(logs=[])

        index = self._resolve_index(catalog_id)
        must: list[dict] = []
        if catalog_id:
            must.append({"term": {"catalog_id": catalog_id}})
        if principal_id:
            must.append({"term": {"principal_id": principal_id}})
        if start_date or end_date:
            range_q: dict = {}
            if start_date:
                range_q["gte"] = start_date.isoformat()
            if end_date:
                range_q["lte"] = end_date.isoformat()
            must.append({"range": {"timestamp": range_q}})

        query = {"bool": {"must": must}} if must else {"match_all": {}}

        body = {
            "size": limit + 1,
            "from": offset,
            "query": query,
            "sort": [{"timestamp": "desc"}],
        }

        try:
            resp = await es.search(index=index, body=body)
        except Exception as exc:
            logger.warning("ElasticsearchStatsDriver.get_logs failed: %s", exc)
            return AccessLogPage(logs=[])

        hits = resp.get("hits", {}).get("hits", [])
        records = [AccessRecord.model_validate(h["_source"]) for h in hits]

        next_cursor = None
        if len(records) > limit:
            records = records[:limit]
            next_cursor = str(offset + limit)

        return AccessLogPage(logs=records, next_cursor=next_cursor)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_client(self):
        from dynastore.modules.elasticsearch.client import get_client

        return get_client()

    def _get_prefix(self) -> str:
        from dynastore.modules.elasticsearch.client import get_index_prefix

        return get_index_prefix()

    def _resolve_index(self, catalog_id: Optional[str] = None) -> str:
        prefix = self._get_prefix()
        if catalog_id:
            safe_id = catalog_id.lower().replace(" ", "_")
            return f"{prefix}_access_logs_{safe_id}"
        return f"{prefix}_access_logs_*"

    def _write_index(self, catalog_id: Optional[str] = None) -> str:
        from dynastore.models.shared_models import SYSTEM_CATALOG_ID

        prefix = self._get_prefix()
        if not catalog_id or catalog_id == SYSTEM_CATALOG_ID:
            return f"{prefix}_access_logs_system"
        safe_id = catalog_id.lower().replace(" ", "_")
        return f"{prefix}_access_logs_{safe_id}"

    async def _ensure_index(self, index_name: str) -> None:
        if index_name in self._known_indices:
            return
        es = self._get_client()
        if es is None:
            return
        try:
            exists = await es.indices.exists(index=index_name)
            if not exists:
                await es.indices.create(index=index_name, body=_ACCESS_LOG_MAPPING)
                logger.info("Created index: %s", index_name)
        except Exception as exc:
            logger.warning("Failed to create index %s: %s", index_name, exc)
        self._known_indices.add(index_name)

    async def _flush_records(self, records: List[AccessRecord], **kwargs) -> None:
        if not records or self._is_testing:
            return

        es = self._get_client()
        if es is None:
            logger.warning(
                "ElasticsearchStatsDriver: ES client unavailable — dropping %d records.",
                len(records),
            )
            return

        # Group by catalog for per-index writes
        groups: Dict[str, list] = defaultdict(list)
        for rec in records:
            idx = self._write_index(rec.catalog_id)
            groups[idx].append(rec)

        for index_name, recs in groups.items():
            await self._ensure_index(index_name)
            bulk_body: list = []
            for rec in recs:
                bulk_body.append({"index": {"_index": index_name}})
                bulk_body.append(
                    {
                        "timestamp": rec.timestamp.isoformat(),
                        "catalog_id": rec.catalog_id,
                        "principal_id": rec.principal_id,
                        "source_ip": str(rec.source_ip),
                        "method": rec.method,
                        "path": rec.path,
                        "status_code": rec.status_code,
                        "processing_time_ms": rec.processing_time_ms,
                        "details": rec.details,
                    }
                )
            try:
                resp = await es.bulk(body=bulk_body)
                if resp.get("errors"):
                    failed = sum(
                        1
                        for item in resp.get("items", [])
                        if item.get("index", {}).get("error")
                    )
                    logger.warning(
                        "ES bulk: %d/%d failed for %s", failed, len(recs), index_name
                    )
                else:
                    logger.debug(
                        "ES bulk: indexed %d docs into %s", len(recs), index_name
                    )
            except Exception as exc:
                logger.error(
                    "ElasticsearchStatsDriver: bulk write failed for %s: %s",
                    index_name,
                    exc,
                )

        logger.info(
            "Flushed %d access logs across %d ES indices.",
            len(records),
            len(groups),
        )
