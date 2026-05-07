"""
OpenSearch/Elasticsearch proxy analytics driver.

Stores redirect logs and computes click aggregations entirely in OpenSearch,
replacing the previous PostgreSQL partitioned ``url_analytics`` and
``proxy_hourly_aggregates`` tables.

Index naming:
    {prefix}_proxy_analytics_{catalog_schema}  — one index per tenant schema

The driver uses the shared async client from
``dynastore.modules.elasticsearch.client``.
"""

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .models import AnalyticsPage, URLAnalytics

logger = logging.getLogger(__name__)

_ANALYTICS_MAPPING = {
    "mappings": {
        "properties": {
            "short_key": {"type": "keyword"},
            "timestamp": {"type": "date"},
            "ip_address": {"type": "ip"},
            "user_agent": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
            "referrer": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
        }
    },
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
    },
}


class ElasticsearchProxyAnalytics:
    """Proxy analytics backed by OpenSearch / Elasticsearch."""

    def __init__(self, flush_threshold: int = 500):
        self._buffer: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()
        self._flush_threshold = flush_threshold
        self._ensured_indices: set = set()

    def _index_name(self, schema: str) -> str:
        from dynastore.modules.elasticsearch.client import get_index_prefix
        prefix = get_index_prefix()
        return f"{prefix}_proxy_analytics_{schema}"

    async def _ensure_index(self, client: Any, schema: str) -> None:
        idx = self._index_name(schema)
        if idx in self._ensured_indices:
            return
        try:
            exists = await client.indices.exists(index=idx)
            if not exists:
                await client.indices.create(index=idx, body=_ANALYTICS_MAPPING)
                logger.info(f"Created proxy analytics index: {idx}")
        except Exception as exc:
            logger.warning(f"Index ensure failed for {idx}: {exc}")
        self._ensured_indices.add(idx)

    async def buffer_redirect(
        self,
        schema: str,
        short_key: str,
        ip_address: str,
        user_agent: str,
        referrer: str,
        timestamp: datetime,
    ) -> bool:
        """Buffer a redirect event. Returns True when flush threshold reached."""
        async with self._lock:
            self._buffer.append({
                "schema": schema,
                "short_key": short_key,
                "ip_address": ip_address,
                "user_agent": user_agent,
                "referrer": referrer,
                "timestamp": timestamp.isoformat() if isinstance(timestamp, datetime) else str(timestamp),
            })
            return len(self._buffer) >= self._flush_threshold

    async def flush(self) -> int:
        """Bulk-index buffered records into ES. Returns count flushed."""
        async with self._lock:
            if not self._buffer:
                return 0
            records = self._buffer[:]
            self._buffer.clear()

        from dynastore.modules.elasticsearch.client import get_client
        client = get_client()
        if not client:
            logger.warning("ES client not available; dropping %d proxy analytics records", len(records))
            return 0

        # Group by schema for per-index bulk ops
        by_schema: Dict[str, List[Dict]] = defaultdict(list)
        for rec in records:
            by_schema[rec["schema"]].append(rec)

        total = 0
        for schema, recs in by_schema.items():
            await self._ensure_index(client, schema)
            idx = self._index_name(schema)

            bulk_body: List[Dict] = []
            for rec in recs:
                bulk_body.append({"index": {"_index": idx}})
                bulk_body.append({
                    "short_key": rec["short_key"],
                    "timestamp": rec["timestamp"],
                    "ip_address": rec["ip_address"],
                    "user_agent": rec["user_agent"],
                    "referrer": rec["referrer"],
                })

            try:
                resp = await client.bulk(body=bulk_body, refresh="false")  # type: ignore[call-arg]
                if resp.get("errors"):
                    failed = sum(1 for item in resp["items"] if "error" in item.get("index", {}))
                    logger.warning(f"Proxy analytics bulk: {failed}/{len(recs)} errors in {idx}")
                total += len(recs)
            except Exception as exc:
                logger.error(f"Proxy analytics bulk failed for {idx}: {exc}")

        if total:
            logger.info(f"Flushed {total} proxy analytics records to ES")
        return total

    async def query_analytics(
        self,
        schema: str,
        short_key: str,
        *,
        cursor: Optional[str] = None,
        page_size: int = 100,
        aggregate: bool = False,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> AnalyticsPage:
        """Query analytics from ES, returning raw logs + optional aggregations."""
        from dynastore.modules.elasticsearch.client import get_client
        client = get_client()
        if not client:
            return AnalyticsPage(data=[], long_url=None)

        idx = self._index_name(schema)

        # Build filter
        must = [{"term": {"short_key": short_key}}]
        if start_date or end_date:
            range_filter: Dict[str, Any] = {}
            if start_date:
                range_filter["gte"] = start_date.isoformat()
            if end_date:
                range_filter["lte"] = end_date.isoformat()
            must.append({"range": {"timestamp": range_filter}})  # type: ignore[arg-type]

        body: Dict[str, Any] = {
            "query": {"bool": {"must": must}},
            "size": page_size,
            "sort": [{"timestamp": "asc"}, "_doc"],
        }

        if cursor:
            body["search_after"] = cursor.split(",")

        if aggregate:
            body["aggs"] = {
                "total_clicks": {"value_count": {"field": "short_key"}},
                "clicks_per_day": {
                    "date_histogram": {
                        "field": "timestamp",
                        "calendar_interval": "day",
                    }
                },
                "top_referrers": {
                    "terms": {"field": "referrer.raw", "size": 5}
                },
                "top_user_agents": {
                    "terms": {"field": "user_agent.raw", "size": 5}
                },
            }

        try:
            resp = await client.search(index=idx, body=body)
        except Exception as exc:
            logger.warning(f"Proxy analytics query failed for {idx}: {exc}")
            return AnalyticsPage(data=[], long_url=None)

        hits = resp.get("hits", {}).get("hits", [])
        data = []
        for i, hit in enumerate(hits):
            src = hit["_source"]
            data.append(URLAnalytics(
                id=i + 1,
                short_key_ref=src["short_key"],
                timestamp=src["timestamp"],
                ip_address=src.get("ip_address"),
                user_agent=src.get("user_agent"),
                referrer=src.get("referrer"),
            ))

        next_cursor = None
        if hits and len(hits) == page_size:
            sort_vals = hits[-1].get("sort", [])
            next_cursor = ",".join(str(v) for v in sort_vals)

        aggregations = None
        if aggregate and "aggregations" in resp:
            aggs = resp["aggregations"]
            aggregations = {
                "total_clicks": aggs.get("total_clicks", {}).get("value", 0),
                "clicks_per_day": [
                    {"day": b["key_as_string"], "clicks": b["doc_count"]}
                    for b in aggs.get("clicks_per_day", {}).get("buckets", [])
                ],
                "top_referrers": [
                    {"referrer": b["key"], "clicks": b["doc_count"]}
                    for b in aggs.get("top_referrers", {}).get("buckets", [])
                ],
                "top_user_agents": [
                    {"user_agent": b["key"], "clicks": b["doc_count"]}
                    for b in aggs.get("top_user_agents", {}).get("buckets", [])
                ],
            }

        return AnalyticsPage(
            data=data,
            long_url=None,  # Caller fills this from PG
            next_cursor=next_cursor,
            aggregations=aggregations,
        )
