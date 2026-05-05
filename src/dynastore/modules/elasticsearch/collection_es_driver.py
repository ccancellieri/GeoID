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
Elasticsearch collection driver — implements CollectionStore.

Stores the **full collection object** (not just metadata) in the
platform-wide singleton index ``{prefix}-collections``. Per-catalog
isolation is achieved via ``_routing=catalog_id`` (shard locality) and a
composite document id ``"{catalog_id}:{collection_id}"`` (name collision
across catalogs is impossible).

Provides fulltext search (multi_match on title/description/keywords),
CQL2-JSON filter support, spatial filtering on extent bbox (geo_shape),
and aggregations. The mapping comes from
:data:`dynastore.modules.elasticsearch.mappings.COLLECTION_MAPPING` — a
single source of truth shared with the lifespan bootstrap and the search
service.
"""

import copy
import logging
from typing import Any, ClassVar, Dict, FrozenSet, List, Optional, Tuple

from dynastore.models.driver_context import DriverContext
from dynastore.models.protocols.entity_store import EntityStoreCapability
from dynastore.modules.storage.routing_config import Operation
from dynastore.modules.storage.storage_location import StorageLocation
from dynastore.models.protocols.typed_driver import (
    TypedDriver,
    _PluginDriverConfig,
)
from dynastore.modules.db_config.platform_config_service import (
    Immutable,
    PluginConfig,
)
from pydantic import Field

logger = logging.getLogger(__name__)

class CollectionElasticsearchDriverConfig(_PluginDriverConfig):
    """Configuration for the Elasticsearch collection driver.

    ``index_prefix`` controls the deployment-wide singleton name
    (``{index_prefix}-collections``). ``Immutable`` — once set it cannot
    change, because altering the prefix would orphan existing collections.
    """
    _address: ClassVar[Tuple[str, str, Optional[str]]] = ("storage", "drivers", "collection")
    _visibility: ClassVar[Optional[str]] = "catalog"


    index_prefix: Immutable[str] = Field(
        "dynastore",
        description=(
            "Deployment-wide ES index prefix. "
            "Final singleton index: ``{index_prefix}-collections``. "
            "Immutable once set — changing it would orphan existing collections."
        ),
    )


# CollectionElasticsearchDriverConfig auto-registers via PluginConfig.__init_subclass__.


async def _on_apply_collection_es_driver_config(
    config: PluginConfig,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """No-op apply handler.

    The singleton index ``{prefix}-collections`` is created at
    :meth:`ElasticsearchModule.lifespan` time, so applying the driver
    config to a catalog does not require any per-catalog provisioning.
    Kept registered for symmetry with other driver-config apply handlers
    (and to keep the code path warm for future per-catalog hooks).
    """
    if not isinstance(config, CollectionElasticsearchDriverConfig):
        return
    return


CollectionElasticsearchDriverConfig.register_apply_handler(_on_apply_collection_es_driver_config)


def _doc_id(catalog_id: str, collection_id: str) -> str:
    """Composite document id used in the singleton ``{prefix}-collections``.

    Same-named collections in different catalogs co-exist as distinct
    documents because the catalog scope is encoded in the id itself.
    """
    return f"{catalog_id}:{collection_id}"


def _bbox_to_envelope(bbox: List[float]) -> Optional[Dict[str, Any]]:
    """Convert [west, south, east, north] to ES envelope for geo_shape."""
    if not bbox or len(bbox) < 4:
        return None
    return {
        "type": "envelope",
        "coordinates": [[bbox[0], bbox[3]], [bbox[2], bbox[1]]],
    }


class CollectionElasticsearchDriver(TypedDriver[CollectionElasticsearchDriverConfig]):
    """Elasticsearch implementation of :class:`CollectionStore`.

    Uses opensearch-py client (wire-compatible with ES and OpenSearch).
    Indexes ONE tier — collection metadata, keyed by ``(catalog_id,
    collection_id)`` — so it opts in to :class:`CollectionIndexer` only.
    Catalog-tier indexing is handled by a separate driver class (NEW —
    not part of this rename).
    """

    is_collection_indexer: ClassVar[bool] = True

    # Collection ES is the canonical async indexer + primary SEARCH
    # backend for collection metadata routing.  Auto-defaults into both
    # Operations.
    auto_register_for_routing: ClassVar[FrozenSet[str]] = frozenset({Operation.SEARCH, Operation.INDEX})

    capabilities: FrozenSet[str] = frozenset({
        EntityStoreCapability.READ,
        EntityStoreCapability.WRITE,
        EntityStoreCapability.SEARCH,
        EntityStoreCapability.CQL_FILTER,
        EntityStoreCapability.SPATIAL_FILTER,
        EntityStoreCapability.AGGREGATION,
        EntityStoreCapability.PHYSICAL_ADDRESSING,
    })

    def location(self, catalog_id: str, collection_id: Optional[str] = None) -> StorageLocation:
        prefix = self._get_prefix()
        index = self._index_name()
        routing = catalog_id
        return StorageLocation(
            backend="elasticsearch",
            canonical_uri=f"es://{index}?routing={routing}",
            identifiers={
                "index": index,
                "prefix": prefix,
                "catalog_id": catalog_id,
                "routing": routing,
            },
            display_label=f"{index} (routing={routing})",
        )

    def _get_client(self):
        from dynastore.modules.elasticsearch.client import get_client

        return get_client()

    def _get_prefix(self) -> str:
        from dynastore.modules.elasticsearch.client import get_index_prefix

        return get_index_prefix()

    def _index_name(self) -> str:
        from dynastore.modules.elasticsearch.mappings import get_index_name

        return get_index_name(self._get_prefix(), "collection")

    async def ensure_storage(self, catalog_id: str) -> None:
        """No-op — the singleton ``{prefix}-collections`` is created at
        ``ElasticsearchModule.lifespan`` time and never per catalog.

        Kept on the signature so the apply-handler wiring and any callers
        invoking the protocol method continue to work without branching.
        """
        return None

    @staticmethod
    def _enrich_doc(metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare doc for ES: add bbox_shape and convert temporal interval to date_range format.

        Deep-copies the input — earlier versions did ``doc = dict(metadata)``
        (shallow), which left the nested ``extent.spatial`` / ``extent.temporal``
        dicts shared between caller and the rewritten ES doc.  Mutating
        ``temporal['interval']`` in-place to the ``[{'gte': …, 'lte': …}]``
        shape then leaked into the caller's payload, breaking the
        post-create re-validation in ``CollectionService.create_collection``
        with a 422 ``"Input should be a valid list"`` error.
        """
        doc = copy.deepcopy(metadata)
        extent = doc.get("extent")
        if isinstance(extent, dict):
            spatial = extent.get("spatial")
            if isinstance(spatial, dict):
                bboxes = spatial.get("bbox")
                if isinstance(bboxes, list) and bboxes:
                    first_bbox = bboxes[0] if isinstance(bboxes[0], list) else bboxes
                    envelope = _bbox_to_envelope(first_bbox)
                    if envelope:
                        spatial["bbox_shape"] = envelope

            temporal = extent.get("temporal")
            if isinstance(temporal, dict):
                interval = temporal.get("interval")
                if isinstance(interval, list):
                    # STAC: [[start, end], ...] → ES date_range: [{"gte": start, "lte": end}, ...]
                    # Skip null-null bounds (no useful range for ES date_range queries).
                    date_ranges = []
                    for bounds in interval:
                        if isinstance(bounds, list) and len(bounds) >= 2:
                            start, end = bounds[0], bounds[1]
                            if start is not None or end is not None:
                                range_obj: Dict[str, Any] = {}
                                if start is not None:
                                    range_obj["gte"] = start
                                if end is not None:
                                    range_obj["lte"] = end
                                date_ranges.append(range_obj)
                    if date_ranges:
                        temporal["interval"] = date_ranges
                    else:
                        temporal.pop("interval", None)
        return doc

    @staticmethod
    def _unenrich_doc(source: Dict[str, Any]) -> Dict[str, Any]:
        """Reverse :meth:`_enrich_doc` for read paths: convert ES
        ``date_range`` shape back to STAC ``[[start, end], …]`` and drop
        the synthetic ``bbox_shape`` so the merged Pydantic ``Collection``
        envelope round-trips cleanly.  Without this, the router fan-in
        feeds the ES-shaped extent into ``Collection.model_validate`` and
        Pydantic rejects ``interval[0]`` as a dict where a list is
        expected.
        """
        doc = copy.deepcopy(source)
        extent = doc.get("extent")
        if isinstance(extent, dict):
            spatial = extent.get("spatial")
            if isinstance(spatial, dict):
                spatial.pop("bbox_shape", None)

            temporal = extent.get("temporal")
            if isinstance(temporal, dict):
                interval = temporal.get("interval")
                if isinstance(interval, list):
                    restored: List[List[Any]] = []
                    for bounds in interval:
                        if isinstance(bounds, dict):
                            restored.append(
                                [bounds.get("gte"), bounds.get("lte")]
                            )
                        elif isinstance(bounds, list):
                            # already in STAC shape — pass through.
                            restored.append(bounds)
                    if restored:
                        temporal["interval"] = restored
        return doc

    async def get_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        client = self._get_client()
        if not client:
            return None

        index_name = self._index_name()
        try:
            resp = await client.get(
                index=index_name,
                id=_doc_id(catalog_id, collection_id),
                params={"routing": catalog_id},
            )
            return self._unenrich_doc(resp["_source"])
        except Exception:
            return None

    async def upsert_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        metadata: Dict[str, Any],
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        client = self._get_client()
        if not client:
            raise RuntimeError("Elasticsearch client not available")

        index_name = self._index_name()
        doc = self._enrich_doc(metadata)
        doc["id"] = collection_id
        doc["catalog_id"] = catalog_id

        await client.index(
            index=index_name,
            id=_doc_id(catalog_id, collection_id),
            body=doc,
            params={"routing": catalog_id, "refresh": "wait_for"},
        )

    async def delete_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> None:
        client = self._get_client()
        if not client:
            return

        index_name = self._index_name()
        doc_id = _doc_id(catalog_id, collection_id)
        try:
            if soft:
                await client.update(
                    index=index_name,
                    id=doc_id,
                    body={"doc": {"_deleted": True}},
                    params={"routing": catalog_id, "refresh": "wait_for"},
                )
            else:
                await client.delete(
                    index=index_name,
                    id=doc_id,
                    params={"routing": catalog_id, "refresh": "wait_for"},
                )
        except Exception as e:
            logger.debug("delete_metadata ES error for %s/%s: %s", catalog_id, collection_id, e)

    async def search_metadata(
        self,
        catalog_id: str,
        *,
        q: Optional[str] = None,
        bbox: Optional[List[float]] = None,
        datetime_range: Optional[str] = None,
        filter_cql: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        client = self._get_client()
        if not client:
            return [], 0

        index_name = self._index_name()

        must_clauses: List[Dict[str, Any]] = []
        filter_clauses: List[Dict[str, Any]] = [
            {"term": {"catalog_id": catalog_id}},
            {"bool": {"must_not": [{"term": {"_deleted": True}}]}},
        ]

        # Fulltext search
        if q:
            must_clauses.append({
                "multi_match": {
                    "query": q,
                    "fields": [
                        "title.en^3", "title.en.keyword^2",
                        "description.en^2",
                        "keywords.*.text",
                        "id^2",
                    ],
                    "type": "best_fields",
                    "fuzziness": "AUTO",
                }
            })

        # Spatial filter on extent.spatial.bbox_shape
        if bbox and len(bbox) >= 4:
            envelope = _bbox_to_envelope(bbox)
            if envelope:
                filter_clauses.append({
                    "geo_shape": {
                        "extent.spatial.bbox_shape": {
                            "shape": envelope,
                            "relation": "intersects",
                        }
                    }
                })

        # CQL2-JSON filter — not yet implemented for ES metadata driver
        if filter_cql:
            logger.warning(
                "CQL2-JSON filter on ES metadata index is not implemented; ignoring"
            )

        query_body: Dict[str, Any] = {
            "bool": {
                "must": must_clauses if must_clauses else [{"match_all": {}}],
                "filter": filter_clauses,
            }
        }

        body: Dict[str, Any] = {
            "query": query_body,
            "from": offset,
            "size": limit,
            "sort": [{"_score": "desc"}, {"id": "asc"}],
        }

        try:
            resp = await client.search(
                index=index_name,
                body=body,
                params={"routing": catalog_id},
            )
            hits = resp.get("hits", {})
            total = hits.get("total", {})
            total_count = total.get("value", 0) if isinstance(total, dict) else total
            results = [hit["_source"] for hit in hits.get("hits", [])]
            return results, total_count
        except Exception as e:
            logger.warning("search_metadata ES error for %s: %s", catalog_id, e)
            return [], 0

    async def get_driver_config(
        self,
        catalog_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Any:
        from dynastore.models.protocols import ConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        configs = get_protocol(ConfigsProtocol)
        if not configs:
            return {}
        try:
            return await configs.get_config(
                CollectionElasticsearchDriverConfig,
                catalog_id=catalog_id,
                ctx=DriverContext(db_resource=db_resource),
            )
        except Exception:
            return {}

    async def is_available(self) -> bool:
        client = self._get_client()
        if not client:
            return False
        try:
            info = await client.info()
            return bool(info)
        except Exception:
            return False
