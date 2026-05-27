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
    ``items_elasticsearch_private_driver`` in any operation. The full feature
    is fetched via ``ItemCrudProtocol`` so the dispatcher only needs
    to send identifiers. Geometry is indexed EXACTLY by default (#1248);
    ``simplify_to_fit`` only runs when the private driver's
    ``simplify_geometry`` config flag is enabled.
    """

    task_type = "elasticsearch_private_index"

    async def run(self, payload: TaskPayload) -> Dict[str, Any]:
        from dynastore.models.protocols.item_crud import ItemCrudProtocol
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.storage.drivers.elasticsearch_private.doc_builder import (
            build_tenant_feature_doc,
        )
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            build_private_item_mapping,
            get_private_index_name,
            project_private_doc,
            resolve_catalog_private_known_fields,
        )
        from dynastore.modules.storage.driver_config import (
            ItemsElasticsearchPrivateDriverConfig,
        )
        from dynastore.tools.discovery import get_protocol
        from dynastore.tools.geometry_simplify import maybe_simplify_for_es

        inputs = PrivateIndexInputs.model_validate(payload.inputs)
        index_name = get_private_index_name(_get_index_prefix(), inputs.catalog_id)

        # Tenant-scoped manual mapping overlay (#1295 slice 3). Resolved
        # once up front and reused for both index-create and projection.
        known_fields = await resolve_catalog_private_known_fields(
            inputs.catalog_id,
        )

        es = _build_es_client()
        if not await es.indices.exists(index=index_name):
            await es.indices.create(
                index=index_name,
                body={"mappings": build_private_item_mapping(known_fields)},
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
        # #1248: exact geometry by default — simplification is opt-in via the
        # private driver's ``simplify_geometry`` config flag.
        simplify_geometry = False
        from dynastore.models.protocols.configs import ConfigsProtocol
        configs = get_protocol(ConfigsProtocol)
        if configs is not None:
            private_config = await configs.get_config(
                ItemsElasticsearchPrivateDriverConfig,
                catalog_id=inputs.catalog_id,
                collection_id=inputs.collection_id,
            )
            simplify_geometry = bool(
                getattr(private_config, "simplify_geometry", False)
            )
        doc, factor, mode = maybe_simplify_for_es(doc, simplify=simplify_geometry)
        doc["simplification_factor"] = factor
        doc["simplification_mode"] = mode
        doc = project_private_doc(doc, known_fields)

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
