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

"""``CollectionElasticsearchPrivateDriver`` — collection-envelope tenant-scoped ES driver.

Cycle E.2.b: parallels :class:`ItemsElasticsearchPrivateDriver` but at
the collection-envelope tier.  Stores the full collection object in a
per-tenant private index ``{prefix}-{catalog_id}-collections-private``
instead of the shared platform-wide ``{prefix}-collections``.

Privacy semantics
-----------------
- Per-collection: the cascade rule (Cycle E.2.a) requires
  ``CollectionPrivacy.is_private == True`` whenever this driver is
  pinned in :class:`CollectionRoutingConfig`.
- DENY policy ownership stays with the items-tier private driver —
  ``items_elasticsearch_private_driver`` applies the catalog-wide
  ``private_deny_{catalog_id}`` covering ``/.../catalogs/{cat}/...``,
  which protects the collection-envelope index access paths too.  The
  cascade guarantees items-private is in scope whenever this driver is
  pinned, so no separate DENY needed here.
- Opts out of auto-default routing
  (``auto_register_for_routing = frozenset()``); operators pin it
  explicitly in :class:`CollectionRoutingConfig` for collections whose
  envelope must not appear in the shared platform index.

Index design
------------
- Per-catalog index, doc-id is the bare ``collection_id`` (no
  composite catalog-prefix needed — the tenant scope is already in
  the index name).
- No ``_routing`` shard hint required (single-shard tenant scope).
- Same enrichment as the public collection driver (bbox → geo_shape,
  STAC interval → ES date_range), so existing query bodies work the
  same way against the private index.

Wiring
------
- Registered as ``storage_collection_elasticsearch_private`` via
  ``pyproject.toml`` entry point (parallel to
  ``storage_elasticsearch_private`` for the items-tier driver).
"""
from __future__ import annotations

import logging
from typing import Any, ClassVar, Dict, FrozenSet, List, Optional, Tuple

from dynastore.models.protocols.entity_store import EntityStoreCapability
from dynastore.modules.elasticsearch.collection_es_driver import (
    CollectionElasticsearchDriver,
    _bbox_to_envelope,
)
from dynastore.modules.storage.hints import Hint
from dynastore.modules.storage.storage_location import StorageLocation

logger = logging.getLogger(__name__)


class CollectionElasticsearchPrivateDriver(CollectionElasticsearchDriver):
    """Tenant-scoped Elasticsearch driver for collection envelopes.

    Subclass of the public :class:`CollectionElasticsearchDriver` —
    inherits the full ``CollectionStore`` surface and overrides the
    methods that resolve the index name, swapping the shared
    ``{prefix}-collections`` for the per-tenant
    ``{prefix}-{catalog_id}-collections-private``.
    """

    # Opt out of auto-default routing.  See module docstring.
    auto_register_for_routing: ClassVar[FrozenSet[str]] = frozenset()

    # Declare the full hint surface so hint-filtered routing (router.py
    # _entry_matches) can select this driver when an operator explicitly
    # pins it in a CollectionRoutingConfig.  Mirrors the public driver's
    # capability — same search/filter/aggregation methods, different index.
    preferred_for: FrozenSet[Hint] = frozenset()
    supported_hints: FrozenSet[Hint] = frozenset({
        Hint.SEARCH,
        Hint.FULLTEXT,
        Hint.SPATIAL_FILTER,
        Hint.ATTRIBUTE_FILTER,
        Hint.SORT,
        Hint.AGGREGATION,
        Hint.COUNT,
        Hint.STATISTICS,
    })

    # Collection-tier indexer marker stays True (parent declares it).
    # Extend the parent's capabilities with TENANT_ISOLATED so the privacy
    # cascade can find per-tenant drivers via capability membership instead
    # of isinstance() on the concrete class.
    capabilities: FrozenSet[str] = frozenset(
        CollectionElasticsearchDriver.capabilities
        | {EntityStoreCapability.TENANT_ISOLATED}
    )

    # ------------------------------------------------------------------
    # Index-name resolution — the only structural difference from the public
    # driver.  Each ``catalog_id``-aware method below computes the
    # per-tenant index name and forwards to the same ES client surface.
    # ------------------------------------------------------------------

    def _private_index(self, catalog_id: str) -> str:
        from dynastore.modules.elasticsearch.mappings import (
            get_tenant_collections_private_index,
        )

        return get_tenant_collections_private_index(
            self._get_prefix(), catalog_id,
        )

    def _doc_id(self, collection_id: str) -> str:
        """Per-tenant doc-id is the bare collection_id (no catalog
        prefix — index is already catalog-scoped).
        """
        return collection_id

    # ------------------------------------------------------------------
    # CollectionStore surface — overrides
    # ------------------------------------------------------------------

    def location(
        self, catalog_id: str, collection_id: Optional[str] = None,
    ) -> StorageLocation:
        prefix = self._get_prefix()
        index = self._private_index(catalog_id)
        return StorageLocation(
            backend="elasticsearch_private",
            canonical_uri=f"es://{index}",
            identifiers={
                "index": index,
                "prefix": prefix,
                "catalog_id": catalog_id,
            },
            display_label=index,
        )

    async def ensure_storage(self, catalog_id: str) -> None:
        """Create the per-tenant private collection index with the
        public collection mapping (same shape — privacy is enforced at
        the routing/IAM layer, not via mapping reduction)."""
        client = self._get_client()
        if client is None:
            return
        index_name = self._private_index(catalog_id)
        from dynastore.modules.elasticsearch.mappings import COLLECTION_MAPPING

        try:
            exists = await client.indices.exists(index=index_name)
            if exists:
                return
            try:
                await client.indices.create(
                    index=index_name,
                    body={"mappings": COLLECTION_MAPPING},
                )
            except Exception as exc:
                # Tolerate "already exists" races between concurrent
                # ensure_storage callers. opensearch-py 3.x dropped the
                # ``ignore=`` kwarg, so we catch and filter instead.
                if "resource_already_exists" not in str(exc):
                    raise
            logger.info(
                "CollectionElasticsearchPrivateDriver: created %r.", index_name,
            )
        except Exception as exc:
            logger.warning(
                "CollectionElasticsearchPrivateDriver: ensure_storage(%r) failed: %s",
                catalog_id, exc,
            )

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
        index_name = self._private_index(catalog_id)
        try:
            resp = await client.get(
                index=index_name, id=self._doc_id(collection_id),
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
        # Lazy index creation — Cycle E.2.c gap: the catalog ensure_storage
        # lifecycle hook isn't yet wired to call our ensure_storage(),
        # so without this defensive create the first upsert would either
        # fail (auto_create disabled cluster-wide) or land in an index
        # without our COLLECTION_MAPPING (auto-created with default
        # mapping — geo_shape on extent.spatial.bbox_shape would be
        # missing, breaking spatial search).  Mirrors the items-private
        # driver's pattern (``write_entities`` and ``index_bulk`` both
        # ensure the index exists before bulk-loading).
        await self.ensure_storage(catalog_id)
        index_name = self._private_index(catalog_id)
        doc = self._enrich_doc(metadata)
        doc["id"] = collection_id
        doc["catalog_id"] = catalog_id
        await client.index(
            index=index_name,
            id=self._doc_id(collection_id),
            body=doc,
            params={"refresh": "wait_for"},
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
        index_name = self._private_index(catalog_id)
        doc_id = self._doc_id(collection_id)
        try:
            if soft:
                await client.update(
                    index=index_name,
                    id=doc_id,
                    body={"doc": {"_deleted": True}},
                    params={"refresh": "wait_for"},
                )
            else:
                await client.delete(
                    index=index_name,
                    id=doc_id,
                    params={"refresh": "wait_for"},
                )
        except Exception as exc:
            logger.debug(
                "CollectionElasticsearchPrivateDriver.delete_metadata(%s/%s) failed: %s",
                catalog_id, collection_id, exc,
            )

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
        index_name = self._private_index(catalog_id)

        must_clauses: List[Dict[str, Any]] = []
        # No catalog_id filter needed — the index is already catalog-scoped.
        filter_clauses: List[Dict[str, Any]] = [
            {"bool": {"must_not": [{"term": {"_deleted": True}}]}},
        ]
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
        if filter_cql:
            logger.warning(
                "CQL2-JSON filter on private collection ES index is not implemented; ignoring",
            )

        body: Dict[str, Any] = {
            "query": {
                "bool": {
                    "must": must_clauses if must_clauses else [{"match_all": {}}],
                    "filter": filter_clauses,
                }
            },
            "from": offset,
            "size": limit,
            "sort": [{"_score": "desc"}, {"id": "asc"}],
        }
        try:
            resp = await client.search(index=index_name, body=body)
            hits = resp.get("hits", {})
            total = hits.get("total", {})
            total_count = total.get("value", 0) if isinstance(total, dict) else total
            results = [hit["_source"] for hit in hits.get("hits", [])]
            return results, total_count
        except Exception as exc:
            logger.warning(
                "CollectionElasticsearchPrivateDriver.search_metadata(%r) failed: %s",
                catalog_id, exc,
            )
            return [], 0

    async def drop_storage(self, catalog_id: str, *, soft: bool = False) -> None:
        """Delete the per-tenant private collection index.  Idempotent
        — missing index is treated as a no-op.
        """
        client = self._get_client()
        if client is None:
            return
        index_name = self._private_index(catalog_id)
        try:
            try:
                await client.indices.delete(index=index_name)
            except Exception as exc:
                # Idempotent drop: missing index is fine.
                if "index_not_found" not in str(exc) and "not_found" not in str(exc):
                    raise
            logger.info(
                "CollectionElasticsearchPrivateDriver: dropped %r.", index_name,
            )
        except Exception as exc:
            logger.warning(
                "CollectionElasticsearchPrivateDriver: drop_storage(%r) failed: %s",
                catalog_id, exc,
            )
