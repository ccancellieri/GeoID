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
Elasticsearch bulk reindex tasks for the regular items driver.

Two task types:
  elasticsearch_bulk_reindex_catalog    — full catalog reindex (Cloud Run Job)
  elasticsearch_bulk_reindex_collection — single collection reindex (Cloud Run Job)

Both use the routing-resolved source-of-truth reader (PG primary via
GEOMETRY_EXACT hint) and the routing-resolved INDEX-lane writer (the
items ES driver) rather than hardcoded driver references. The task
``driver`` input field selects the target explicitly when supplied;
otherwise the first INDEX-lane driver is used.

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

from pydantic import BaseModel, Field

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
    page_size: Optional[int] = Field(
        default=None,
        description=(
            "Items per read page for each collection reindex. When given, this value "
            "governs the read page verbatim — it is never overridden by the writer's "
            "preference. When omitted, defaults to the writer's preferred_chunk_size "
            "if it declares one, else a module default. Lower this for geometry-heavy "
            "collections where large read pages risk long-lived queries and connection "
            "drops; the ES _bulk write is independently byte-bounded regardless of this "
            "value."
        ),
    )


class BulkCollectionReindexInputs(BaseModel):
    catalog_id: str
    collection_id: str
    driver: Optional[str] = None
    page_size: Optional[int] = Field(
        default=None,
        description=(
            "Items per read page for this collection reindex. When given, this value "
            "governs the read page verbatim — it is never overridden by the writer's "
            "preference. When omitted, defaults to the writer's preferred_chunk_size "
            "if it declares one, else a module default. Lower this for geometry-heavy "
            "collections where large read pages risk long-lived queries and connection "
            "drops; the ES _bulk write is independently byte-bounded regardless of this "
            "value."
        ),
    )


# ---------------------------------------------------------------------------
# Task: BulkCatalogReindexTask
# ---------------------------------------------------------------------------

class BulkCatalogReindexTask(TaskProtocol):
    """Reindex every collection of a catalog via routing-resolved drivers.

    Iterates the catalog's collections, skips those that don't route
    through the regular ES driver, and streams each collection's items
    from the routing-resolved source-of-truth reader (PG primary, via the
    GEOMETRY_EXACT hint) into the routing-resolved INDEX-lane writer
    (the items ES driver). Stale items for the catalog are removed via
    ``delete_by_query`` before reindex begins.

    The optional ``driver`` input field pins the target by ``driver_ref``
    (e.g. ``"items_elasticsearch_driver"``); when omitted the first
    INDEX-lane driver is selected automatically.
    """

    task_type = "elasticsearch_bulk_reindex_catalog"

    async def run(self, payload: TaskPayload) -> Dict[str, Any]:
        from dynastore.models.protocols import CatalogsProtocol
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.elasticsearch.mappings import get_tenant_items_index
        from dynastore.tools.discovery import get_protocol

        inputs = BulkCatalogReindexInputs.model_validate(payload.inputs)
        catalog_id = inputs.catalog_id
        driver_hint = inputs.driver  # optional explicit WRITE target

        index_name = get_tenant_items_index(_get_index_prefix(), catalog_id)

        catalogs_proto = get_protocol(CatalogsProtocol)
        if not catalogs_proto:
            raise RuntimeError("CatalogsProtocol not available in this process.")

        es = _build_es_client()

        # Wipe stale items for this catalog before reindexing. delete_by_query
        # is bounded to the per-tenant index — other catalogs are unaffected.
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
        total_rejected = 0
        rejected_docs: list = []
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
                result = await _reindex_collection(
                    catalog_id,
                    collection_id,
                    driver_hint=driver_hint,
                    page_size=inputs.page_size,
                )
                total_indexed += result.total_written
                total_rejected += result.rejected
                rejected_docs.extend(
                    {"collection_id": collection_id, "id": doc_id, "reason": reason}
                    for doc_id, reason in result.rejected_docs
                )
                logger.info(
                    "BulkCatalogReindexTask: %s/%s — %d docs indexed, %d rejected.",
                    catalog_id, collection_id, result.total_written, result.rejected,
                )
            if len(collections) < batch:
                break
            offset += batch

        return {
            "catalog_id": catalog_id,
            "total_indexed": total_indexed,
            "rejected": total_rejected,
            "rejected_docs": rejected_docs,
            "status": "done",
        }


# ---------------------------------------------------------------------------
# Task: BulkCollectionReindexTask
# ---------------------------------------------------------------------------

class BulkCollectionReindexTask(TaskProtocol):
    """Reindex one collection via routing-resolved drivers.

    Triggered by the admin reindex endpoint at
    ``POST /search/reindex/catalogs/{id}/collections/{cid}``.

    Reads from the routing-resolved source-of-truth (PG primary via the
    GEOMETRY_EXACT hint) and writes to the routing-resolved INDEX-lane
    writer (the items ES driver). The optional ``driver`` input field pins
    the target by ``driver_ref``; when omitted the first INDEX-lane driver
    is selected automatically.
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
        driver_hint = inputs.driver  # optional explicit WRITE target

        # Resolve external ids to internal before computing the wipe's index
        # name / term filter / routing key (#2999) — mirrors the ES items
        # driver's read-path resolution so this pre-reindex wipe targets the
        # same index/routing partition the write path actually indexes into.
        # Scoped to local variables (not reassigning catalog_id/collection_id)
        # so `_reindex_collection` and the returned payload keep echoing the
        # caller-supplied ids unchanged. Passthrough (unchanged input) when
        # CatalogsProtocol is unavailable or the id is already internal.
        wipe_catalog_id = catalog_id
        wipe_collection_id = collection_id
        catalogs_proto = get_protocol(CatalogsProtocol)
        if catalogs_proto is not None:
            internal_cat = await catalogs_proto.resolve_catalog_id(
                wipe_catalog_id, allow_missing=True
            )
            if internal_cat is not None:
                wipe_catalog_id = internal_cat
            internal_col = await catalogs_proto.collections.resolve_collection_id(
                wipe_catalog_id, wipe_collection_id, allow_missing=True
            )
            if internal_col is not None:
                wipe_collection_id = internal_col

        index_name = get_tenant_items_index(_get_index_prefix(), wipe_catalog_id)

        es = _build_es_client()

        # Wipe stale items for just this collection before reindexing.
        try:
            await es.delete_by_query(
                index=index_name,
                body={"query": {"term": {"collection": wipe_collection_id}}},
                params={
                    "routing": wipe_collection_id,
                    "refresh": "false",
                    "ignore_unavailable": "true",
                },
            )
        except Exception as exc:
            logger.warning(
                "BulkCollectionReindexTask: pre-reindex delete_by_query failed "
                "for %s/%s: %s", catalog_id, collection_id, exc,
            )

        result = await _reindex_collection(
            catalog_id,
            collection_id,
            driver_hint=driver_hint,
            page_size=inputs.page_size,
        )

        return {
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "total_indexed": result.total_written,
            "rejected": result.rejected,
            "rejected_docs": [
                {"id": doc_id, "reason": reason}
                for doc_id, reason in result.rejected_docs
            ],
            "status": "done",
        }
