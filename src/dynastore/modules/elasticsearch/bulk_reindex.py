"""Driver-level bulk reindex helpers for Elasticsearch.

These functions implement the actual bulk-index orchestration against the
Elasticsearch driver. They live at the **module** level (not in a task
package) so that any consumer — task, extension, or external integration —
can import and call them directly without going through the dispatcher.

The module-level placement matches the pattern established for
:mod:`dynastore.modules.elasticsearch.client`,
:mod:`dynastore.modules.elasticsearch.mappings`, and the rest of the ES
driver: drivers belong to ``modules``, tasks orchestrate them.

Hard runtime dep on ``opensearchpy`` is satisfied transitively via the
client/mappings imports — no extra import gating needed here.
"""
from __future__ import annotations

import json
import logging
from decimal import Decimal
from typing import Any

from pydantic import BaseModel

logger = logging.getLogger(__name__)


def _json_default(obj: Any) -> Any:
    """Fallback serializer for Decimal and other non-JSON types."""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def get_es_client():
    """Return the shared singleton AsyncElasticsearch client.

    Raises ``RuntimeError`` if the client is not initialized — caller is
    responsible for ensuring ElasticsearchModule's lifespan has started.
    """
    from dynastore.modules.elasticsearch.client import get_client

    es = get_client()
    if es is None:
        raise RuntimeError(
            "Elasticsearch client is not initialized. "
            "Ensure ElasticsearchModule is registered and its lifespan has started."
        )
    return es


async def is_es_active_for(catalog_id: str, collection_id: str) -> bool:
    """Whether the regular ES driver is currently routed for this collection.

    Reads ``CollectionRoutingConfig`` via the ConfigsProtocol. Returns False
    on any failure (missing protocol, missing config, malformed config) so
    callers can safely use this as a guard before performing ES operations.
    """
    from dynastore.models.protocols.configs import ConfigsProtocol
    from dynastore.modules.storage.routing_config import CollectionRoutingConfig
    from dynastore.tools.discovery import get_protocol as _get_protocol

    configs = _get_protocol(ConfigsProtocol)
    if not configs:
        return False
    try:
        routing = await configs.get_config(
            CollectionRoutingConfig,
            catalog_id=catalog_id,
            collection_id=collection_id,
        )
    except Exception:
        return False
    return any(
        entry.driver_id == "items_elasticsearch_driver"
        for entries in routing.operations.values()
        for entry in entries
    )


async def reindex_collection_into_index(
    es,
    catalogs_proto,
    catalog_id: str,
    collection_id: str,
    index_name: str,
    page_size: int = 500,
) -> int:
    """Stream every item of a collection from the SoR and bulk-index it
    into the per-tenant items index with ``_routing=collection_id``.

    Returns the number of documents indexed. Skips collections that don't
    currently route through the regular ES driver (per
    :func:`is_es_active_for`).

    This is the driver-level orchestration; both the bulk-reindex tasks
    and any extension that wants to perform a reindex synchronously can
    call this directly.
    """
    if not await is_es_active_for(catalog_id, collection_id):
        logger.debug(
            "Skipping collection %s/%s — elasticsearch not configured as driver.",
            catalog_id,
            collection_id,
        )
        return 0

    total = 0
    offset = 0

    while True:
        result = await catalogs_proto.search(
            catalog_id,
            collection_id,
            limit=page_size,
            offset=offset,
        )
        features = result.get("features", [])
        if not features:
            break

        bulk_body: list = []
        for feature in features:
            item_id = getattr(feature, "id", None) or (
                feature.get("id") if isinstance(feature, dict) else None
            )
            if not item_id:
                continue

            if isinstance(feature, BaseModel):
                doc = feature.model_dump(by_alias=True, exclude_none=True, mode="json")
            else:
                doc = json.loads(json.dumps(dict(feature), default=_json_default))
            doc["catalog_id"] = catalog_id
            doc["collection"] = collection_id

            doc_id = f"{catalog_id}:{collection_id}:{item_id}"
            bulk_body.append(
                {
                    "index": {
                        "_index": index_name,
                        "_id": doc_id,
                        "routing": collection_id,
                    }
                }
            )
            bulk_body.append(doc)

        if bulk_body:
            resp = await es.bulk(body=bulk_body, params={"timeout": "60s"})
            errors = [
                i for i in resp.get("items", []) if "error" in i.get("index", {})
            ]
            if errors:
                logger.warning(
                    "Bulk index: %d errors in collection %s/%s at offset %d.",
                    len(errors),
                    catalog_id,
                    collection_id,
                    offset,
                )
            total += len(bulk_body) // 2

        if len(features) < page_size:
            break
        offset += page_size

    return total
