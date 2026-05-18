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

"""
Per-item incremental private indexing tasks.

Two task types:
  - ``elasticsearch_private_index``  — index one item as a full
    tenant-feature doc into the per-tenant index
    ``{prefix}-{catalog_id}-private-items``.
  - ``elasticsearch_private_delete`` — remove one geoid doc from the
    private index.

These tasks live with the private driver (subpackage-private) so a
deployment dropping the ``[private]`` extras group also drops the task
registrations from ``CapabilityMap`` automatically.

Bulk reindex (``BulkCatalogReindexTask`` / ``BulkCollectionReindexTask``)
in :mod:`dynastore.tasks.elasticsearch_indexer.tasks` is regular-driver
only — the private driver does not ship a bulk reindex (the
fresh-start cutover protocol drops state and re-ingests instead). If
needed it would belong here in the subpackage.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

# Hard runtime dep — entry-point load fails on services without
# ``opensearch-py`` so the CapabilityMap doesn't list these tasks as
# claimable there.
import opensearchpy  # noqa: F401
from pydantic import BaseModel

from dynastore.modules.tasks.models import TaskPayload
from dynastore.tasks.protocols import TaskProtocol

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Input models
# ---------------------------------------------------------------------------


class PrivateIndexInputs(BaseModel):
    geoid: str
    catalog_id: str
    collection_id: str


class PrivateDeleteInputs(BaseModel):
    geoid: str
    catalog_id: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_es_client():
    """Return the shared singleton AsyncElasticsearch client."""
    from dynastore.modules.elasticsearch.client import get_client
    es = get_client()
    if es is None:
        raise RuntimeError(
            "Elasticsearch client is not initialized. "
            "Ensure ElasticsearchModule is registered and its lifespan has started."
        )
    return es


# ---------------------------------------------------------------------------
# Task: PrivateIndexTask  (per-item, incremental)
# ---------------------------------------------------------------------------


class PrivateIndexTask(TaskProtocol):
    """Index a single item as a full tenant-feature doc.

    Dispatched per-item via the ``IndexDispatcher`` (PR #261) when the
    collection's ``ItemsRoutingConfig`` pins
    ``items_elasticsearch_private_driver`` in any operation.  Pinning
    this driver in the items routing is required by the cascade rule
    (#733) whenever the sibling ``CollectionRoutingConfig`` pins
    ``collection_elasticsearch_private_driver``.  The full feature
    is fetched via ``ItemCrudProtocol`` so the dispatcher only needs
    to send identifiers.  Geometry simplification is applied via
    ``simplify_to_fit`` to honour the ES 10MB per-doc limit.
    """

    task_type = "elasticsearch_private_index"

    async def run(self, payload: TaskPayload) -> Dict[str, Any]:
        from dynastore.models.protocols.item_crud import ItemCrudProtocol
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.storage.drivers.elasticsearch_private.doc_builder import (
            build_tenant_feature_doc,
        )
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            TENANT_FEATURE_MAPPING,
            get_private_index_name,
        )
        from dynastore.tools.discovery import get_protocol
        from dynastore.tools.geometry_simplify import simplify_to_fit

        inputs = PrivateIndexInputs.model_validate(payload.inputs)
        index_name = get_private_index_name(_get_index_prefix(), inputs.catalog_id)

        es = _build_es_client()
        if not await es.indices.exists(index=index_name):
            await es.indices.create(
                index=index_name,
                body={"mappings": TENANT_FEATURE_MAPPING},
            )

        # Fetch the full feature so we can persist geometry + properties.
        feature: Any = {"id": inputs.geoid}
        items_proto = get_protocol(ItemCrudProtocol)
        if items_proto:
            try:
                fetched = await items_proto.get_item(
                    inputs.catalog_id, inputs.collection_id, inputs.geoid,
                )
                if fetched is not None:
                    feature = fetched
            except Exception as e:
                logger.warning(
                    "PrivateIndexTask: get_item(%s/%s/%s) failed (%s); "
                    "indexing geoid-only stub.",
                    inputs.catalog_id, inputs.collection_id, inputs.geoid, e,
                )

        doc = build_tenant_feature_doc(
            feature, catalog_id=inputs.catalog_id, collection_id=inputs.collection_id,
        )
        doc, factor, mode = simplify_to_fit(doc)
        doc["simplification_factor"] = factor
        doc["simplification_mode"] = mode

        await es.index(index=index_name, id=inputs.geoid, body=doc)
        return {"geoid": inputs.geoid, "index": index_name, "status": "indexed"}


# ---------------------------------------------------------------------------
# Task: PrivateDeleteTask  (per-item, incremental)
# ---------------------------------------------------------------------------


class PrivateDeleteTask(TaskProtocol):
    """Remove a single geoid document from the private index.

    Safe to run even if the collection is not private / the index does not exist (no-op via NotFoundError).
    """

    task_type = "elasticsearch_private_delete"

    async def run(self, payload: TaskPayload) -> Dict[str, Any]:
        from opensearchpy.exceptions import NotFoundError

        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            get_private_index_name,
        )

        inputs = PrivateDeleteInputs.model_validate(payload.inputs)
        index_name = get_private_index_name(_get_index_prefix(), inputs.catalog_id)

        es = _build_es_client()
        try:
            await es.delete(index=index_name, id=inputs.geoid)
        except NotFoundError:
            pass  # safe: document may not be in the private index

        return {"geoid": inputs.geoid, "index": index_name, "status": "deleted"}
