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
ItemsElasticsearchPrivateDriver — tenant-scoped ES storage driver.

Writes the full feature (geometry + properties + external_id) into a
per-tenant index whose name is owned by this subpackage
(``{prefix}-{catalog_id}-private-items``). Driver-private mapping
(``TENANT_FEATURE_MAPPING``) lives in :mod:`.mappings`.

Manages DENY access policies in its own lifecycle. Reuses the shared
SFEOS-backed helpers from :class:`_ElasticsearchBase` (parent class
imported from the sibling regular driver module).

Registered as ``storage_elasticsearch_private`` via entry points.
"""

from __future__ import annotations

import logging
import re
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncIterator, ClassVar, Dict, FrozenSet, List, Optional, Union

if TYPE_CHECKING:
    from dynastore.modules.storage.storage_location import StorageLocation

from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.protocols.typed_driver import TypedDriver
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.protocols import ModuleProtocol
from dynastore.modules.storage.driver_config import (
    ItemsElasticsearchPrivateDriverConfig,
)
from dynastore.modules.storage.drivers.elasticsearch import _ElasticsearchBase
from dynastore.modules.storage.errors import SoftDeleteNotSupportedError
from dynastore.modules.storage.hints import Hint

logger = logging.getLogger(__name__)


class ItemsElasticsearchPrivateDriver(
    TypedDriver[ItemsElasticsearchPrivateDriverConfig],
    _ElasticsearchBase,
    ModuleProtocol,
):
    """Tenant-scoped Elasticsearch storage driver (a.k.a. "private").

    Writes the full feature (geometry + properties + external_id) into a
    per-tenant index ``{prefix}-{catalog_id}-private-items`` shared across all
    collections of the catalog. The mapping is `TENANT_FEATURE_MAPPING`
    (root ``dynamic: false`` to reject smuggled fields; ``properties.*``
    is dynamic so tenant attributes index without mapping churn).

    Docs that would exceed the ES 10MB per-doc limit are shrunk by
    `simplify_to_fit` (`tools/geometry_simplify.py`); the resulting
    `simplification_factor` and `simplification_mode` are persisted on
    the doc so clients can tell how much fidelity was lost.

    Search is gated by the tenant-first contract enforced in
    `extensions/search` (catalog_id required; geoid OR (external_id +
    collection_id) — never external_id alone).

    Manages DENY access policies in its own lifecycle.

    Uses the raw ES client from SFEOS settings (not DatabaseLogic) since
    the index has a custom mapping not managed by SFEOS.

    Registered as ``storage_elasticsearch_private`` via entry points.
    """

    # Generic Indexer Protocol — slim per-item / bulk surface used by the
    # ``IndexDispatcher``.  The private driver opts in as ``ItemIndexer``
    # via the existing routing config; this attribute identifies it on
    # the dispatcher side.
    indexer_id: ClassVar[str] = "items_elasticsearch_private_driver"
    is_item_indexer: ClassVar[bool] = True

    # Opt out of items-tier auto-default routing.  The private variant is
    # tenant-isolated DENY-policy indexing; it must only run for collections
    # whose ``CollectionPrivacy.is_private == True`` (Cycle E.2) — the
    # privacy-cascade validator on ``ItemsRoutingConfig`` enforces that the
    # routing pins this driver in some operation whenever the collection
    # claims is_private.  Auto-injecting into every collection's INDEX/SEARCH
    # would silently bypass that gate.
    auto_register_for_routing: ClassVar[FrozenSet[str]] = frozenset()

    priority: int = 51
    preferred_chunk_size: int = 500
    capabilities: FrozenSet[str] = frozenset({
        Capability.READ,
        Capability.WRITE,
        Capability.STREAMING,
        Capability.PHYSICAL_ADDRESSING,
        Capability.INTROSPECTION,
    })
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

    def is_available(self) -> bool:
        # See ItemsElasticsearchDriver.is_available — the driver is
        # available whenever the standalone opensearch-py client is wired
        # up. SFEOS is only needed for write/read full-STAC paths and
        # those raise at call time if it's missing.
        try:
            from dynastore.modules.elasticsearch.client import get_client
        except (ImportError, ModuleNotFoundError):
            return False
        return get_client() is not None

    def _get_client(self):
        """Get the async ES client from SFEOS DatabaseLogic."""
        return self._get_db_logic().client

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        """Restore DENY policies; item-tier propagation runs through the
        IndexDispatcher now (Phase 2d).  No event listeners registered
        on this driver — the dispatcher resolves it via the slim
        :class:`Indexer` Protocol and routing config.
        """
        await self._restore_deny_policies()
        yield

    # ------------------------------------------------------------------
    # StorageDriverProtocol
    # ------------------------------------------------------------------

    async def write_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entities: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]]],
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> List[Feature]:
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.storage.drivers.elasticsearch_private.doc_builder import (
            build_tenant_feature_doc,
        )
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            TENANT_FEATURE_MAPPING,
            get_private_index_name,
        )
        from dynastore.tools.geometry_simplify import simplify_to_fit

        index_name = get_private_index_name(_get_index_prefix(), catalog_id)
        items = self._normalize_entities(entities)
        es = self._get_client()

        if not await es.indices.exists(index=index_name):
            await es.indices.create(
                index=index_name,
                body={"mappings": TENANT_FEATURE_MAPPING},
                ignore=400,
            )

        bulk_body: list = []
        for item in items:
            geoid = self._extract_item_id(item)
            if not geoid:
                continue
            doc = build_tenant_feature_doc(
                item, catalog_id=catalog_id, collection_id=collection_id,
            )
            doc, factor, mode = simplify_to_fit(doc)
            doc["simplification_factor"] = factor
            doc["simplification_mode"] = mode
            bulk_body.append({"index": {"_index": index_name, "_id": geoid}})
            bulk_body.append(doc)

        if bulk_body:
            await es.bulk(body=bulk_body, request_timeout=60)

        return items if isinstance(items, list) else list(items)

    async def read_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        entity_ids: Optional[List[str]] = None,
        request: Optional[QueryRequest] = None,
        context: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        db_resource: Optional[Any] = None,
    ) -> AsyncIterator[Feature]:
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            get_private_index_name,
        )

        if not entity_ids:
            return

        index_name = get_private_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()

        if not await es.indices.exists(index=index_name):
            return

        for geoid in entity_ids:
            try:
                resp = await es.get(index=index_name, id=geoid)
                source = resp["_source"]
                props = dict(source.get("properties") or {})
                # Surface tenant-feature bookkeeping in properties so callers
                # can detect simplification without a separate API.
                if "external_id" in source:
                    props["external_id"] = source["external_id"]
                if "simplification_factor" in source:
                    props["simplification_factor"] = source["simplification_factor"]
                if "simplification_mode" in source:
                    props["simplification_mode"] = source["simplification_mode"]
                props["catalog_id"] = source.get("catalog_id", catalog_id)
                props["collection_id"] = source.get("collection_id", collection_id)
                yield Feature(
                    type="Feature",
                    id=source.get("geoid", geoid),
                    geometry=source.get("geometry"),
                    properties=props,
                    bbox=source.get("bbox"),  # type: ignore[call-arg]
                )
            except Exception:
                pass

    async def delete_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entity_ids: List[str],
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> int:
        if soft:
            raise SoftDeleteNotSupportedError(
                "ItemsElasticsearchPrivateDriver does not support soft delete."
            )
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            get_private_index_name,
        )

        index_name = get_private_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()
        deleted = 0

        for geoid in entity_ids:
            try:
                await es.delete(index=index_name, id=geoid)
                deleted += 1
            except Exception:
                pass

        return deleted

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            TENANT_FEATURE_MAPPING,
            get_private_index_name,
        )

        index_name = get_private_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()

        if not await es.indices.exists(index=index_name):
            await es.indices.create(
                index=index_name,
                body={"mappings": TENANT_FEATURE_MAPPING},
                ignore=400,
            )

        await self._apply_deny_policy(catalog_id)

    async def drop_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        soft: bool = False,
    ) -> None:
        if soft:
            raise SoftDeleteNotSupportedError(
                "ItemsElasticsearchPrivateDriver does not support soft drop."
            )
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            get_private_index_name,
        )

        index_name = get_private_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()
        await es.indices.delete(index=index_name, ignore_unavailable=True)
        await self._revoke_deny_policy(catalog_id)

    async def export_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        format: str = "parquet",
        target_path: str = "",
        db_resource: Optional[Any] = None,
    ) -> str:
        raise NotImplementedError(
            "ItemsElasticsearchPrivateDriver.export_entities: not supported."
        )

    # ------------------------------------------------------------------
    # Generic Indexer Protocol — slim, dispatcher-facing surface
    # ------------------------------------------------------------------

    async def ensure_indexer(self, ctx) -> None:
        """Idempotent bootstrap for the private per-tenant index.

        Creates ``{prefix}-{catalog_id}-private-items`` with
        ``TENANT_FEATURE_MAPPING`` if missing, then re-applies the
        catalog's DENY policies (recovers in-memory IAM state on cold
        boot — same recovery path as :meth:`ensure_storage`).

        No public alias today: the private index is intentionally
        absent from the ``{prefix}-items-public`` discovery alias to
        keep tenant isolation intact.
        """
        await self.ensure_storage(ctx.catalog, ctx.collection)

    async def index(self, ctx, op) -> None:
        """Apply a single item :class:`IndexOp` to the per-tenant private
        index ``{prefix}-{catalog_id}-private-items``.

        The "private" index stores the full feature with reduced search
        surface (geoid + tenant attrs).  When called from the
        ``IndexDispatcher``, ``op.payload`` carries the full STAC item;
        the driver builds the tenant-scoped doc and shrinks oversized
        geometries via ``simplify_to_fit`` before indexing.
        """
        if op.entity_type != "item":
            return
        if not ctx.collection:
            raise ValueError(
                "ItemsElasticsearchPrivateDriver.index: collection required for item ops",
            )

        from dynastore.modules.elasticsearch.client import (
            get_index_prefix as _get_index_prefix,
        )
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            TENANT_FEATURE_MAPPING,
            get_private_index_name,
        )

        index_name = get_private_index_name(_get_index_prefix(), ctx.catalog)
        es = self._get_client()

        if op.op_type == "delete":
            try:
                await es.delete(index=index_name, id=op.entity_id)
            except Exception:
                pass
            return

        # upsert
        from dynastore.modules.storage.drivers.elasticsearch_private.doc_builder import (
            build_tenant_feature_doc,
        )
        from dynastore.tools.geometry_simplify import simplify_to_fit

        if not await es.indices.exists(index=index_name):
            await es.indices.create(
                index=index_name,
                body={"mappings": TENANT_FEATURE_MAPPING},
                ignore=400,
            )

        src = op.payload or {"id": op.entity_id}
        src.setdefault("id", op.entity_id)
        doc = build_tenant_feature_doc(
            src, catalog_id=ctx.catalog, collection_id=ctx.collection,
        )
        doc, factor, mode = simplify_to_fit(doc)
        doc["simplification_factor"] = factor
        doc["simplification_mode"] = mode
        await es.index(index=index_name, id=op.entity_id, document=doc)

    async def index_bulk(self, ctx, ops):
        """Bulk-apply a batch of item ops via the ES ``_bulk`` API."""
        from dynastore.models.protocols.indexer import BulkResult
        from dynastore.modules.elasticsearch.client import (
            get_index_prefix as _get_index_prefix,
        )
        from dynastore.modules.storage.drivers.elasticsearch_private.doc_builder import (
            build_tenant_feature_doc,
        )
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            TENANT_FEATURE_MAPPING,
            get_private_index_name,
        )
        from dynastore.tools.geometry_simplify import simplify_to_fit

        if not ops:
            return BulkResult()
        if not ctx.collection:
            raise ValueError(
                "ItemsElasticsearchPrivateDriver.index_bulk: collection required for item ops",
            )

        index_name = get_private_index_name(_get_index_prefix(), ctx.catalog)
        es = self._get_client()

        if not await es.indices.exists(index=index_name):
            await es.indices.create(
                index=index_name,
                body={"mappings": TENANT_FEATURE_MAPPING},
                ignore=400,
            )

        body: List[dict] = []
        for op in ops:
            if op.entity_type != "item":
                continue
            if op.op_type == "delete":
                body.append({"delete": {"_index": index_name, "_id": op.entity_id}})
                continue
            src = op.payload or {"id": op.entity_id}
            src.setdefault("id", op.entity_id)
            doc = build_tenant_feature_doc(
                src, catalog_id=ctx.catalog, collection_id=ctx.collection,
            )
            doc, factor, mode = simplify_to_fit(doc)
            doc["simplification_factor"] = factor
            doc["simplification_mode"] = mode
            body.append({"index": {"_index": index_name, "_id": op.entity_id}})
            body.append(doc)

        if not body:
            return BulkResult(total=len(ops))

        resp = await es.bulk(body=body, request_timeout=60)
        items = (resp or {}).get("items", []) if isinstance(resp, dict) else []
        succeeded = 0
        failures: List[Dict[str, Any]] = []
        for it in items:
            entry = next(iter(it.values())) if isinstance(it, dict) and it else {}
            err = entry.get("error") if isinstance(entry, dict) else None
            if err:
                failures.append({
                    "id": entry.get("_id"),
                    "reason": str(err.get("reason", err) if isinstance(err, dict) else err),
                })
            else:
                succeeded += 1
        return BulkResult(
            total=len(ops),
            succeeded=succeeded,
            failed=len(failures),
            failures=failures,
        )

    # ------------------------------------------------------------------
    # DENY policy management (self-contained)
    # ------------------------------------------------------------------

    @staticmethod
    async def _apply_deny_policy(catalog_id: str) -> None:
        from dynastore.tools.discovery import get_protocol
        try:
            from dynastore.models.protocols.policies import PermissionProtocol, Policy, Role
        except ImportError:
            return

        perm = get_protocol(PermissionProtocol)
        if not perm:
            return

        policy_id = f"private_deny_{catalog_id}"
        deny_policy = Policy(
            id=policy_id,
            description=f"Blocks public access to private catalog: {catalog_id}",
            actions=["GET"],
            resources=[
                f"/(catalog|stac|features|tiles|wfs|maps)/catalogs/{re.escape(catalog_id)}(/.*)?",
            ],
            effect="DENY",
        )

        perm.register_policy(deny_policy)
        perm.register_role(Role(name="all_users", policies=[policy_id]))

        try:
            await perm.create_policy(deny_policy)
        except Exception:
            try:
                await perm.update_policy(deny_policy)
            except Exception as e:
                logger.error("DENY policy persist failed for '%s': %s", catalog_id, e)

    @staticmethod
    async def _revoke_deny_policy(catalog_id: str) -> None:
        from dynastore.tools.discovery import get_protocol
        try:
            from dynastore.models.protocols.policies import PermissionProtocol
        except ImportError:
            return

        perm = get_protocol(PermissionProtocol)
        if not perm:
            return

        try:
            await perm.delete_policy(f"private_deny_{catalog_id}")
        except Exception:
            pass

    async def _restore_deny_policies(self) -> None:
        """Restore catalog-wide DENY policies at startup for any catalog
        that has at least one private collection.

        Cycle E.2 / F.0d cutover: privacy is per-collection now
        (``CollectionPrivacy.is_private``).  We scan all catalogs,
        list each catalog's collections, and re-apply the DENY policy
        idempotently for any catalog with at least one private collection.

        The DENY pattern stays catalog-wide
        (``private_deny_{catalog_id}`` blocking
        ``/.../catalogs/{cat}/...``) — the cascade validator guarantees
        that any private collection's items go through this driver, so
        the catalog-wide block covers every privacy-sensitive path.
        """
        try:
            from dynastore.models.protocols import CatalogsProtocol
            from dynastore.models.protocols.configs import ConfigsProtocol
            from dynastore.modules.catalog.catalog_config import CollectionPrivacy
            from dynastore.tools.discovery import get_protocol

            catalogs_proto = get_protocol(CatalogsProtocol)
            configs = get_protocol(ConfigsProtocol)
            if not catalogs_proto or not configs:
                return

            offset, batch = 0, 100
            while True:
                catalog_list = await catalogs_proto.list_catalogs(
                    limit=batch, offset=offset,
                )
                if not catalog_list:
                    break
                for catalog in catalog_list:
                    catalog_id = getattr(catalog, "id", None)
                    if not catalog_id:
                        continue
                    if await self._catalog_has_private_collection(
                        catalogs_proto, configs, catalog_id, CollectionPrivacy,
                    ):
                        await self._apply_deny_policy(catalog_id)
                        logger.info(
                            "PrivateDriver: restored DENY for '%s' (cycle E.2 — at least one private collection).",
                            catalog_id,
                        )
                if len(catalog_list) < batch:
                    break
                offset += batch
        except Exception as e:
            logger.warning(
                "PrivateDriver: could not restore DENY policies: %s", e,
            )

    @staticmethod
    async def _catalog_has_private_collection(
        catalogs_proto: Any,
        configs: Any,
        catalog_id: str,
        collection_cls: type,
    ) -> bool:
        """Return True iff any collection of the catalog has
        ``is_private == True``.  Iterates collections in batches."""
        offset, batch = 0, 100
        while True:
            try:
                collections = await catalogs_proto.list_collections(
                    catalog_id, limit=batch, offset=offset,
                )
            except Exception:
                return False
            if not collections:
                return False
            for col in collections:
                col_id = getattr(col, "id", None)
                if not col_id:
                    continue
                try:
                    cfg = await configs.get_config(
                        collection_cls, catalog_id=catalog_id, collection_id=col_id,
                    )
                except Exception:
                    continue
                if isinstance(cfg, collection_cls) and getattr(cfg, "is_private", False):
                    return True
            if len(collections) < batch:
                return False
            offset += batch

    async def location(
        self,
        catalog_id: str,
        collection_id: str,
    ) -> "StorageLocation":
        """Return typed physical storage coordinates for this private index."""
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            get_private_index_name,
        )
        from dynastore.modules.storage.storage_location import StorageLocation

        prefix = _get_index_prefix()
        index_name = get_private_index_name(prefix, catalog_id)
        return StorageLocation(
            backend="elasticsearch_private",
            canonical_uri=f"es://{index_name}",
            identifiers={"index": index_name, "prefix": prefix, "catalog_id": catalog_id},
            display_label=index_name,
        )

    # ------------------------------------------------------------------
    # CollectionItemsStore Protocol — data-side ops (parity with public)
    # ------------------------------------------------------------------
    # Mirror the public driver's implementation against the per-tenant
    # private index ``{prefix}-{catalog_id}-private-items``. The private index
    # holds every collection of the catalog in one place, scoped at
    # query time by the ``collection`` field — same shape as public.

    def _private_index(self, catalog_id: str) -> str:
        from dynastore.modules.elasticsearch.client import get_index_prefix
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            get_private_index_name,
        )
        return get_private_index_name(get_index_prefix(), catalog_id)

    async def count_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        request: Optional[Any] = None,
        db_resource: Optional[Any] = None,
    ) -> int:
        from dynastore.modules.elasticsearch.client import get_client
        from dynastore.modules.elasticsearch.items_es_ops import es_count_items

        es = get_client()
        if es is None:
            return 0
        return await es_count_items(
            es, self._private_index(catalog_id), collection=collection_id,
        )

    async def compute_extents(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        from dynastore.modules.elasticsearch.client import get_client
        from dynastore.modules.elasticsearch.items_es_ops import es_extents

        es = get_client()
        if es is None:
            return None
        return await es_extents(
            es, self._private_index(catalog_id), collection=collection_id,
        )

    async def aggregate(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        aggregation_type: str,
        field: Optional[str] = None,
        request: Optional[Any] = None,
        db_resource: Optional[Any] = None,
    ) -> Any:
        from dynastore.modules.elasticsearch.client import get_client
        from dynastore.modules.elasticsearch.items_es_ops import es_aggregate

        es = get_client()
        if es is None:
            return None
        return await es_aggregate(
            es,
            self._private_index(catalog_id),
            aggregation_type=aggregation_type,
            field=field,
            collection=collection_id,
        )

    async def introspect_schema(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> List[Any]:
        from dynastore.modules.elasticsearch.client import get_client
        from dynastore.modules.elasticsearch.items_es_ops import es_introspect_mapping

        es = get_client()
        if es is None:
            return []
        return await es_introspect_mapping(es, self._private_index(catalog_id))

    async def get_entity_fields(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        entity_level: str = "item",
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Return the introspected field set as a dict keyed by field name.

        Bridges the legacy dict-based contract (used by some queryables
        contributors) over the protocol's list-returning ``introspect_schema``.
        """
        if entity_level != "item" or not collection_id:
            return {}
        fields = await self.introspect_schema(catalog_id, collection_id)
        return {getattr(f, "name", str(f)): f for f in fields}

    # --- Admin ops not supported on this backend ---

    async def rename_storage(
        self,
        catalog_id: str,
        old_collection_id: str,
        new_collection_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        raise NotImplementedError(
            "ItemsElasticsearchPrivateDriver: rename_storage is not "
            "supported. Renaming on this backend would require a full "
            "reindex of the per-tenant private index."
        )

    async def restore_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entity_ids: List[str],
        *,
        db_resource: Optional[Any] = None,
    ) -> int:
        raise SoftDeleteNotSupportedError(
            "ItemsElasticsearchPrivateDriver: restore_entities is not "
            "implemented; deletes on the private index are physical "
            "removals (no soft-delete tombstone)."
        )
