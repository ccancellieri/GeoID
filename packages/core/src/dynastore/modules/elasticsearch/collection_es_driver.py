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
from dynastore.models.mutability import Immutable
from dynastore.modules.db_config.plugin_config import PluginConfig
from pydantic import Field

logger = logging.getLogger(__name__)

class CollectionElasticsearchDriverConfig(_PluginDriverConfig):
    """Configuration for the Elasticsearch collection driver.

    ``index_prefix`` controls the deployment-wide singleton name
    (``{index_prefix}-collections``). ``Immutable`` — once set it cannot
    change, because altering the prefix would orphan existing collections.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "drivers")
    _freeze_at: ClassVar[Optional[str]] = "catalog"

    required_engine_class: ClassVar[str] = "elasticsearch_engine"


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

    # Collection ES is the canonical async secondary index + primary SEARCH
    # backend for collection metadata routing.  It auto-defaults into WRITE
    # (as a secondary index, identified by ``is_collection_indexer``) and SEARCH.
    auto_register_for_routing: ClassVar[FrozenSet[str]] = frozenset({Operation.SEARCH, Operation.WRITE})

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
                # If ``_enrich_doc`` dropped an all-null-bounds interval to
                # keep ES date_range happy, restore the canonical STAC
                # ``[[None, None]]`` so the Pydantic ``Collection`` envelope
                # round-trips cleanly.  Without this, ``extent.temporal``
                # comes back as ``{}`` and validation fails with
                # ``extent.temporal.interval Field required``.
                if "interval" not in temporal:
                    temporal["interval"] = [[None, None]]
        return doc

    async def get_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        from dynastore.modules.storage.routing_config import (
            get_output_transformers_for_search,
        )
        from dynastore.modules.storage.transform_runtime import (
            restore_transform_chain,
        )
        from dynastore.tools.typed_store.base import _to_snake

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
            doc = self._unenrich_doc(resp["_source"])
        except Exception:
            return None

        restore_chain = await get_output_transformers_for_search(
            catalog_id,
            entity="collection",
            collection_id=collection_id,
            driver_ref=_to_snake(type(self).__name__),
        )
        if restore_chain:
            from dynastore.models.protocols.entity_transform import (
                TransformChainContext,
            )
            doc = await restore_transform_chain(
                doc,
                restore_chain,
                catalog_id=catalog_id,
                collection_id=collection_id,
                entity_kind="collection",
                ctx=TransformChainContext(),
            )
        return doc

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

        try:
            await client.index(
                index=index_name,
                id=_doc_id(catalog_id, collection_id),
                body=doc,
                params={"routing": catalog_id, "refresh": "wait_for"},
            )
        except Exception as exc:
            from dynastore.modules.elasticsearch._mapping_errors import (
                maybe_raise_mapping_mismatch,
            )
            # #728: surface the real reason. The opensearchpy transport
            # logger only prints the status line (not the body), so 400s
            # like document_parsing_exception were invisible. exc_info=True
            # writes the response body / cause chain into structured logs.
            logger.warning(
                "CollectionElasticsearchDriver.upsert_metadata failed: "
                "catalog=%r collection=%r index=%r",
                catalog_id, collection_id, index_name,
                exc_info=True,
            )
            maybe_raise_mapping_mismatch(exc, index_name, doc.keys())
            raise

    async def ensure_indexer(self, ctx: Any) -> None:
        """Idempotent bootstrap — delegates to :meth:`ensure_storage`."""
        await self.ensure_storage(ctx.catalog)

    async def index(self, ctx: Any, op: Any) -> None:
        """Apply a single collection-tier op (upsert or delete).

        ``op.entity_id`` is the collection id; ``ctx.catalog`` is the
        owning catalog. ``op.payload`` carries the collection metadata
        for upserts.

        Tier guard (#728): non-collection ops are refused outright rather
        than silently upserted into the singleton ``dynastore-collections``
        index. A misconfigured secondary-index ``WRITE`` entry
        (``secondary_index=True``) in ``ItemsRoutingConfig.operations[WRITE]``
        pointing at the collection driver previously leaked 180 STAC
        items into the collection index and poisoned its dynamic mapping;
        the loud refusal here surfaces that misconfiguration immediately.
        """
        op_entity_type = getattr(op, "entity_type", None)
        if op_entity_type is not None and op_entity_type != "collection":
            payload_type = (op.payload or {}).get("type") if op.op_type == "upsert" else None
            logger.error(
                "CollectionElasticsearchDriver refused non-collection op: "
                "op_entity_type=%r op_type=%r entity_id=%r catalog=%r payload_type=%r — "
                "check routing config for this tier (likely a stale "
                "ItemsRoutingConfig pointing at this driver).",
                op_entity_type, op.op_type, op.entity_id,
                getattr(ctx, "catalog", None), payload_type,
            )
            raise ValueError(
                f"CollectionElasticsearchDriver.index: refused op with "
                f"entity_type={op_entity_type!r}; this driver only accepts "
                f"collection-tier ops."
            )
        if op.op_type == "upsert":
            payload = op.payload or {}
            # Defence-in-depth: even when entity_type is unset (legacy
            # pre-#810 callers fall back to CollectionRoutingConfig), a
            # STAC Feature payload is unambiguously an item.
            if payload.get("type") == "Feature":
                logger.error(
                    "CollectionElasticsearchDriver refused STAC Feature payload: "
                    "entity_id=%r catalog=%r — leaked items pollute the "
                    "singleton collection index's dynamic mapping.",
                    op.entity_id, getattr(ctx, "catalog", None),
                )
                raise ValueError(
                    "CollectionElasticsearchDriver.index: refused payload with "
                    "type='Feature'; STAC items belong on the items-tier index."
                )
            await self.upsert_metadata(ctx.catalog, op.entity_id, payload)
        elif op.op_type == "delete":
            await self.delete_metadata(ctx.catalog, op.entity_id)
        else:
            raise ValueError(
                f"CollectionElasticsearchDriver.index: unsupported op_type "
                f"{op.op_type!r}"
            )

    async def index_bulk(self, ctx: Any, ops: Any) -> Any:
        """Apply a batch of collection-tier ops via per-op :meth:`index`.

        Collection cardinality per catalog is typically moderate; a
        per-op loop is fine. ES ``_bulk`` optimisation can land later if
        a real tenant hits a hot loop.
        """
        from dynastore.models.protocols.indexer import BulkResult

        total = len(ops)
        failures: List[Dict[str, Any]] = []
        succeeded = 0
        for op in ops:
            try:
                await self.index(ctx, op)
                succeeded += 1
            except Exception as exc:
                # #728: log per-op failure with exc_info so the real
                # cause (e.g. document_parsing_exception body, refused
                # tier guard) reaches structured logs. The downstream
                # ``index_propagation`` task currently treats partial
                # failures as a successful "partial" outcome — see #728
                # follow-up to wire failures to the DLQ like the items
                # adapter does.
                logger.warning(
                    "CollectionElasticsearchDriver.index_bulk op failed: "
                    "catalog=%r entity_id=%r op_type=%r",
                    getattr(ctx, "catalog", None),
                    op.entity_id, op.op_type,
                    exc_info=True,
                )
                failures.append({
                    "entity_id": op.entity_id,
                    "op_type": op.op_type,
                    "error": str(exc),
                })
        return BulkResult(
            total=total,
            succeeded=succeeded,
            failed=len(failures),
            failures=failures,
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

        from dynastore.models.protocols.entity_transform import (
            TransformChainContext,
        )
        from dynastore.modules.storage.routing_config import (
            get_output_transformers_for_search,
        )
        from dynastore.modules.storage.transform_runtime import (
            restore_transform_chain,
        )
        from dynastore.tools.typed_store.base import _to_snake

        driver_ref = _to_snake(type(self).__name__)
        try:
            resp = await client.search(
                index=index_name,
                body=body,
                params={"routing": catalog_id},
            )
            hits = resp.get("hits", {})
            total = hits.get("total", {})
            total_count = total.get("value", 0) if isinstance(total, dict) else total
            # Resolve the output-transformer chain once for this search
            # (collection_id is unknown at the catalog-wide search level; each
            # hit carries its own id which could be used for per-collection
            # resolution, but a catalog-level chain suffices for now).
            restore_chain = await get_output_transformers_for_search(
                catalog_id,
                entity="collection",
                collection_id=None,
                driver_ref=driver_ref,
            )
            # One restore context per query — shared cache across the page (#1568).
            restore_ctx = TransformChainContext()
            results: List[Dict[str, Any]] = []
            for hit in hits.get("hits", []):
                doc = self._unenrich_doc(hit["_source"])
                if restore_chain:
                    doc = await restore_transform_chain(
                        doc,
                        restore_chain,
                        catalog_id=catalog_id,
                        collection_id=None,
                        entity_kind="collection",
                        ctx=restore_ctx,
                    )
                results.append(doc)
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
