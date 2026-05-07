"""
Elasticsearch bulk reindex tasks for the regular items driver.

Two task types:
  elasticsearch_bulk_reindex_catalog    — full catalog reindex (Cloud Run Job)
  elasticsearch_bulk_reindex_collection — single collection reindex (Cloud Run Job)

Both target the per-tenant index ``{prefix}-items-{catalog_id}`` (helper
:func:`dynastore.modules.elasticsearch.mappings.get_tenant_items_index`)
keyed by ``_routing=collection_id``. They are designed to be executed by
the ``geospatial-elasticsearch-indexer`` Cloud Run Job (triggered by an
admin reindex endpoint) and also run in the worker for smaller catalogs.

Per-event private tasks (``elasticsearch_private_index`` /
``elasticsearch_private_delete``) live in the private driver
subpackage at
:mod:`dynastore.modules.storage.drivers.elasticsearch_private.tasks`.
A bulk private reindex is intentionally not provided here — the
fresh-start cutover protocol (drop PG + delete ES indexes pre-deploy)
makes operator-triggered bulk reindex unnecessary for the private
driver. If one is needed it belongs in the private subpackage.
"""

import logging
from typing import Any, Dict, Optional

from pydantic import BaseModel

# Hard runtime dep — see modules/elasticsearch/module.py for rationale.
# Forces entry-point load to fail on services without ``opensearch-py`` so
# the CapabilityMap doesn't list these tasks as claimable there.
import opensearchpy  # noqa: F401

from dynastore.tasks.protocols import TaskProtocol
from dynastore.modules.tasks.models import TaskPayload

# Driver-level helpers live at module level so extensions and ad-hoc tools
# can call them directly without going through the dispatcher. The bulk
# reindex tasks below are thin orchestration wrappers around these.
from dynastore.modules.elasticsearch.bulk_reindex import (
    get_es_client as _build_es_client,
    reindex_collection_into_index as _reindex_collection,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Input models
# ---------------------------------------------------------------------------

class BulkCatalogReindexInputs(BaseModel):
    catalog_id: str
    driver: Optional[str] = None


class BulkCollectionReindexInputs(BaseModel):
    catalog_id: str
    collection_id: str
    driver: Optional[str] = None


# ---------------------------------------------------------------------------
# Task: BulkCatalogReindexTask
# ---------------------------------------------------------------------------

class BulkCatalogReindexTask(TaskProtocol):
    """Reindex every collection of a catalog into the per-tenant items index.

    Iterates the catalog's collections, skips those that don't route
    through the regular ES driver, and streams each collection's items
    via the SoR into ``{prefix}-items-{catalog_id}`` with
    ``_routing=collection_id``. Stale items for the catalog are removed
    via ``delete_by_query`` before reindex begins.
    """

    task_type = "elasticsearch_bulk_reindex_catalog"

    async def run(self, payload: TaskPayload) -> Dict[str, Any]:
        from dynastore.models.protocols import CatalogsProtocol
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.elasticsearch.mappings import get_tenant_items_index
        from dynastore.tools.discovery import get_protocol

        inputs = BulkCatalogReindexInputs.model_validate(payload.inputs)
        catalog_id = inputs.catalog_id
        index_name = get_tenant_items_index(_get_index_prefix(), catalog_id)

        catalogs_proto = get_protocol(CatalogsProtocol)
        if not catalogs_proto:
            raise RuntimeError("CatalogsProtocol not available in this process.")

        es = _build_es_client()

        # Wipe stale items for this catalog. delete_by_query is bounded to
        # the per-tenant index — other catalogs are unaffected.
        try:
            await es.delete_by_query(
                index=index_name,
                body={"query": {"match_all": {}}},
                params={"refresh": "false", "ignore_unavailable": "true"},
            )
        except Exception as exc:
            logger.warning(
                "BulkCatalogReindexTask: pre-reindex delete_by_query failed for "
                "%s: %s", catalog_id, exc,
            )

        total_indexed = 0
        offset, batch = 0, 50
        while True:
            collections = await catalogs_proto.list_collections(
                catalog_id, limit=batch, offset=offset
            )
            if not collections:
                break
            for collection in collections:
                collection_id = getattr(collection, "id", None)
                if not collection_id:
                    continue
                count = await _reindex_collection(
                    es, catalogs_proto, catalog_id, collection_id, index_name,
                )
                total_indexed += count
                logger.info(
                    "BulkCatalogReindexTask: %s/%s — %d docs indexed.",
                    catalog_id, collection_id, count,
                )
            if len(collections) < batch:
                break
            offset += batch

        return {
            "catalog_id": catalog_id,
            "total_indexed": total_indexed,
            "status": "done",
        }


# ---------------------------------------------------------------------------
# Task: BulkCollectionReindexTask
# ---------------------------------------------------------------------------

class BulkCollectionReindexTask(TaskProtocol):
    """Reindex one collection into the per-tenant items index.

    Triggered by the admin reindex endpoint at
    ``POST /search/reindex/catalogs/{id}/collections/{cid}``.
    """

    task_type = "elasticsearch_bulk_reindex_collection"

    async def run(self, payload: TaskPayload) -> Dict[str, Any]:
        from dynastore.models.protocols import CatalogsProtocol
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.elasticsearch.mappings import get_tenant_items_index
        from dynastore.tools.discovery import get_protocol

        inputs = BulkCollectionReindexInputs.model_validate(payload.inputs)
        catalog_id = inputs.catalog_id
        collection_id = inputs.collection_id
        index_name = get_tenant_items_index(_get_index_prefix(), catalog_id)

        catalogs_proto = get_protocol(CatalogsProtocol)
        if not catalogs_proto:
            raise RuntimeError("CatalogsProtocol not available.")

        es = _build_es_client()

        # Wipe stale items for just this collection.
        try:
            await es.delete_by_query(
                index=index_name,
                body={"query": {"term": {"collection": collection_id}}},
                params={
                    "routing": collection_id,
                    "refresh": "false",
                    "ignore_unavailable": "true",
                },
            )
        except Exception as exc:
            logger.warning(
                "BulkCollectionReindexTask: pre-reindex delete_by_query failed "
                "for %s/%s: %s", catalog_id, collection_id, exc,
            )

        count = await _reindex_collection(
            es, catalogs_proto, catalog_id, collection_id, index_name,
        )

        return {
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "total_indexed": count,
            "status": "done",
        }
