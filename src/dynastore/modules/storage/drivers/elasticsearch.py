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
Elasticsearch Storage Drivers.

Two drivers in this module:

* ``ItemsElasticsearchDriver``  (driver_id ``"elasticsearch"``)
  Items are written directly to a per-tenant index
  ``{prefix}-items-{catalog_id}`` (helper :func:`get_tenant_items_index`)
  with ``_routing=collection_id`` so a single index hosts every collection
  of one catalog while keeping shard locality per collection.  The index
  is enrolled in the platform alias ``{prefix}-items-public`` so OGC
  discovery search routes can target one alias regardless of tenant.
  Catalog and collection documents are still routed via SFEOS until the
  dedicated ``catalog_es_driver`` / ``collection_es_driver`` modules
  fully take over (separate, scheduled migration).

* ``AssetElasticsearchDriver``  (driver_id ``"elasticsearch_assets"``)
  Indexes asset metadata into per-catalog ``{prefix}-assets-{catalog_id}``
  indices.  Driven by ``AssetRoutingConfig.operations[INDEX]`` (auto-augmented
  with discoverable ``AssetIndexer`` impls) and dispatched via
  ``AssetEntitySyncSubscriber`` from the events outbox — single-writer fan-out,
  no per-driver listener block.  Direct programmatic indexing via
  ``index_asset()`` / ``delete_asset()`` remains available.

The private driver (``ItemsElasticsearchPrivateDriver``) lives in
its own self-contained subpackage at
:mod:`dynastore.modules.storage.drivers.elasticsearch_private` so the
``[private]`` extras group can be opted in or out without touching this
file.

All drivers register as async event listeners, checking
``StorageRoutingConfig.secondary_drivers`` before acting.
"""

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncIterator, ClassVar, Dict, FrozenSet, List, Optional, Union

if TYPE_CHECKING:
    from dynastore.modules.storage.driver_config import CollectionWritePolicy
    from dynastore.modules.storage.storage_location import StorageLocation

from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.driver_context import DriverContext
from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.protocols.typed_driver import TypedDriver
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.protocols import ModuleProtocol
from dynastore.modules.storage.driver_config import (
    AssetElasticsearchDriverConfig,
    ItemsElasticsearchDriverConfig,
)
from dynastore.modules.storage.errors import SoftDeleteNotSupportedError

logger = logging.getLogger(__name__)


def _es_client_required() -> Any:
    """Return the ES async client; raise if the platform module hasn't started."""
    from dynastore.modules.elasticsearch.client import get_client

    es = get_client()
    if es is None:
        raise RuntimeError(
            "ES client not initialised — ElasticsearchModule.lifespan must "
            "have started before items operations are invoked."
        )
    return es


def _tenant_items_index(catalog_id: str) -> str:
    """Resolve the per-tenant items index name for the active deployment."""
    from dynastore.modules.elasticsearch.client import get_index_prefix
    from dynastore.modules.elasticsearch.mappings import get_tenant_items_index

    return get_tenant_items_index(get_index_prefix(), catalog_id)


# ---------------------------------------------------------------------------
# Shared base
# ---------------------------------------------------------------------------

class _ElasticsearchBase:
    """Shared helpers for ES storage drivers."""

    _db_logic = None

    @classmethod
    def _get_db_logic(cls):
        """Lazily instantiate SFEOS DatabaseLogic (singleton)."""
        if cls._db_logic is None:
            try:
                from stac_fastapi.elasticsearch.database_logic import DatabaseLogic  # type: ignore[import-not-found]
                cls._db_logic = DatabaseLogic()
            except ImportError:
                raise RuntimeError(
                    "stac-fastapi-elasticsearch not installed. "
                    "Install with: pip install stac-fastapi-elasticsearch"
                )
        return cls._db_logic

    @staticmethod
    def _sfeos_available() -> bool:
        try:
            from stac_fastapi.elasticsearch.database_logic import DatabaseLogic  # type: ignore[import-not-found]  # noqa: F401
            return True
        except ImportError:
            return False

    async def get_driver_config(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        db_resource: Optional[Any] = None,
    ) -> Any:
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.tools.discovery import get_protocol
        from dynastore.modules.storage.driver_config import ItemsElasticsearchDriverConfig

        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            return ItemsElasticsearchDriverConfig()
        config = await configs.get_config(
            ItemsElasticsearchDriverConfig,
            catalog_id=catalog_id,
            collection_id=collection_id,
            ctx=DriverContext(db_resource=db_resource),
        )
        if config is None:
            return ItemsElasticsearchDriverConfig()
        return config

    @staticmethod
    async def _is_secondary_for(
        driver_id: str, catalog_id: str, collection_id: Optional[str],
    ) -> bool:
        """Check if this driver is listed in the routing config for the given scope."""
        try:
            from typing import cast as _cast
            from dynastore.models.protocols.configs import ConfigsProtocol
            from dynastore.tools.discovery import get_protocol
            from dynastore.modules.storage.routing_config import (
                CollectionRoutingConfig,
            )

            configs = get_protocol(ConfigsProtocol)
            if not configs:
                return False
            routing = _cast(
                Optional[CollectionRoutingConfig],
                await configs.get_config(
                    CollectionRoutingConfig,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                ),
            )
            if routing is None:
                return False
            from typing import cast as _cast2
            ops = _cast2(Dict[str, list], routing.operations)
            return any(
                entry.driver_id == driver_id
                for entries in ops.values()
                for entry in entries
            )
        except Exception:
            return False

    @staticmethod
    async def _is_write_driver_for(
        driver_id: str, catalog_id: str, collection_id: Optional[str],
    ) -> bool:
        """Check if this driver is listed in the WRITE operation of the routing config.

        When True, the router fan-out already handles writes for this
        collection — event-driven indexing should be skipped to avoid
        double-indexing.
        """
        try:
            from typing import cast as _cast
            from dynastore.models.protocols.configs import ConfigsProtocol
            from dynastore.tools.discovery import get_protocol
            from dynastore.modules.storage.routing_config import (
                Operation,
                CollectionRoutingConfig,
            )

            configs = get_protocol(ConfigsProtocol)
            if not configs:
                return False
            routing = _cast(
                Optional[CollectionRoutingConfig],
                await configs.get_config(
                    CollectionRoutingConfig,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                ),
            )
            if routing is None:
                return False
            from typing import cast as _cast3
            ops2 = _cast3(Dict[str, list], routing.operations)
            write_entries = ops2.get(Operation.WRITE, [])
            return any(e.driver_id == driver_id for e in write_entries)
        except Exception:
            return False

    @staticmethod
    def _feature_to_stac_item(
        feature: Any, catalog_id: str, collection_id: str,
    ) -> dict:
        """Serialize a Feature to a STAC item dict for SFEOS."""
        if hasattr(feature, "model_dump"):
            doc = feature.model_dump(by_alias=True, exclude_none=True)
        elif isinstance(feature, dict):
            doc = dict(feature)
        else:
            doc = dict(feature)
        doc.setdefault("id", doc.get("id"))
        doc["collection"] = collection_id
        return doc

    @staticmethod
    def _normalize_entities(
        entities: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]]],
    ) -> list:
        if isinstance(entities, FeatureCollection):
            return list(entities.features) if entities.features else []
        if isinstance(entities, list):
            return entities
        return [entities]

    @staticmethod
    def _extract_item_id(entity: Any) -> Optional[str]:
        if hasattr(entity, "id"):
            return entity.id
        if isinstance(entity, dict):
            return entity.get("id")
        return None


# ---------------------------------------------------------------------------
# ItemsElasticsearchDriver — SFEOS-backed full STAC
# ---------------------------------------------------------------------------

class ItemsElasticsearchDriver(
    TypedDriver[ItemsElasticsearchDriverConfig], _ElasticsearchBase, ModuleProtocol,
):
    """Elasticsearch storage driver for STAC items.

    Items are written directly via the async ES client to a single
    per-tenant index ``{prefix}-items-{catalog_id}`` keyed by
    ``_routing=collection_id`` for shard locality. The driver enrolls
    each per-tenant index in the platform alias
    ``{prefix}-items-public`` on first ``ensure_storage`` so OGC
    discovery search routes can target that alias regardless of tenant.

    Catalog and collection serialization on this class still flows
    through SFEOS ``DatabaseLogic`` until the dedicated
    ``catalog_es_driver`` / ``collection_es_driver`` modules supersede
    those paths.

    Registered as ``storage_elasticsearch`` via entry points.

    Indexer marker — opts in to :class:`ItemIndexer` so the items routing
    config auto-registers it under ``operations[INDEX]``.
    """

    is_item_indexer: ClassVar[bool] = True

    priority: int = 50
    preferred_chunk_size: int = 500
    capabilities: FrozenSet[str] = frozenset({
        Capability.READ,
        Capability.WRITE,
        Capability.STREAMING,
        Capability.SPATIAL_FILTER,
        Capability.SORT,
        Capability.FULLTEXT,
        Capability.SOFT_DELETE,
        Capability.ATTRIBUTE_FILTER,
        Capability.EXTERNAL_ID_TRACKING,
        Capability.TEMPORAL_VALIDITY,
        Capability.PHYSICAL_ADDRESSING,
    })
    preferred_for: FrozenSet[str] = frozenset({"search", "geometry_simplified"})
    supported_hints: FrozenSet[str] = frozenset({
        "search", "fulltext",
        "geometry_simplified",  # PR #185 default routing: ES serves the fast simplified-geometry read path
    })

    def is_available(self) -> bool:
        return self._sfeos_available()

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        from dynastore.models.protocols.events import EventsProtocol
        from dynastore.tools.discovery import get_protocol
        from dynastore.modules.catalog.event_service import CatalogEventType

        events = get_protocol(EventsProtocol)
        if events:
            for etype, handler in [
                (CatalogEventType.CATALOG_CREATION, self._on_catalog_upsert),
                (CatalogEventType.CATALOG_UPDATE, self._on_catalog_upsert),
                (CatalogEventType.CATALOG_DELETION, self._on_catalog_delete),
                (CatalogEventType.CATALOG_HARD_DELETION, self._on_catalog_delete),
                (CatalogEventType.COLLECTION_CREATION, self._on_collection_upsert),
                (CatalogEventType.COLLECTION_UPDATE, self._on_collection_upsert),
                (CatalogEventType.COLLECTION_DELETION, self._on_collection_delete),
                (CatalogEventType.COLLECTION_HARD_DELETION, self._on_collection_delete),
                (CatalogEventType.ITEM_CREATION, self._on_item_upsert),
                (CatalogEventType.ITEM_UPDATE, self._on_item_upsert),
                (CatalogEventType.ITEM_DELETION, self._on_item_delete),
                (CatalogEventType.ITEM_HARD_DELETION, self._on_item_delete),
                (CatalogEventType.BULK_ITEM_CREATION, self._on_item_bulk_upsert),
            ]:
                decorator = events.async_event_listener(etype)
                if decorator:
                    decorator(handler)
            logger.info("ItemsElasticsearchDriver: event listeners registered.")
        yield

    # ------------------------------------------------------------------
    # StorageDriverProtocol — Items
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
        """Write/upsert entities to Elasticsearch respecting CollectionWritePolicy.

        Applies ``WriteConflictPolicy`` per entity when ``external_id`` is present.
        Stores ``asset_id``, ``valid_from``, ``valid_to`` from ``context`` in ES ``_source``.

        Conflict policies (item-level via ``on_conflict``):
        - UPDATE: index with stable doc_id (existing ES behaviour).
        - REFUSE: skip if a doc with the same external_id already exists.
        - NEW_VERSION: index with a timestamped doc_id suffix; stores ``valid_from``/``valid_to``.

        Batch-level via ``on_asset_conflict``:
        - REFUSE (``refuse_asset``): raise ``ConflictError`` if any external_id already exists.
        """
        from datetime import datetime, timezone
        from dynastore.tools.geometry_simplify import simplify_to_fit

        items = self._normalize_entities(entities)
        if not items:
            return []
        es = _es_client_required()
        index_name = _tenant_items_index(catalog_id)

        # Service-layer enforcement of FieldDefinition.required / .unique for
        # drivers (like ES) that don't advertise native REQUIRED_ENFORCEMENT /
        # UNIQUE_ENFORCEMENT. Only runs when the collection's
        # CollectionSchema has allow_app_level_enforcement=True; otherwise
        # config admission would have already rejected the constraints.
        await self._enforce_field_constraints(catalog_id, collection_id, items)

        # Resolve write policy from the config waterfall.
        policy = await self._resolve_write_policy(catalog_id, collection_id)

        # Extract context fields (ingestion metadata — not part of feature payload).
        ctx = context or {}
        asset_id: Optional[str] = ctx.get("asset_id")
        valid_from = ctx.get("valid_from")
        valid_to = ctx.get("valid_to")

        written: List = []
        prepped_bulk: list = []

        for item in items:
            stac_doc = self._feature_to_stac_item(item, catalog_id, collection_id)

            # Resolve external_id from the configured field path.
            external_id = self._extract_external_id_from_doc(stac_doc, policy.external_id_field)
            if policy.require_external_id and not external_id:
                logger.warning(
                    "ES write_entities: external_id required but missing for item in %s/%s — skipped",
                    catalog_id, collection_id,
                )
                continue

            # Attach tracking fields to ES document _source.
            if asset_id is not None:
                stac_doc["_asset_id"] = asset_id
            if valid_from is not None:
                stac_doc["_valid_from"] = valid_from
            if valid_to is not None:
                stac_doc["_valid_to"] = valid_to
            if external_id is not None:
                stac_doc["_external_id"] = external_id

            # Build the ES doc_id based on conflict policy.
            from dynastore.modules.storage.driver_config import WriteConflictPolicy

            base_id = external_id or stac_doc.get("id")

            # Asset-level (batch-level) check — runs before item-level; raises on first match.
            if policy.on_asset_conflict is not None and external_id:
                from dynastore.modules.storage.driver_config import AssetConflictPolicy
                if (
                    policy.on_asset_conflict == AssetConflictPolicy.REFUSE
                    and await self._es_doc_exists_by_external_id(
                        es, index_name, collection_id, external_id,
                    )
                ):
                    from dynastore.modules.storage.errors import ConflictError
                    raise ConflictError(
                        f"ES driver: external_id '{external_id}' already exists "
                        f"in {catalog_id}/{collection_id} (policy=refuse_asset)"
                    )

            # Item-level check — skip this entity, continue batch.
            if policy.on_conflict == WriteConflictPolicy.REFUSE:
                if external_id and await self._es_doc_exists_by_external_id(
                    es, index_name, collection_id, external_id,
                ):
                    logger.debug(
                        "ES write_entities(REFUSE): external_id '%s' exists — skipped",
                        external_id,
                    )
                    continue

            if policy.on_conflict == WriteConflictPolicy.NEW_VERSION:
                # Each version gets a unique doc_id. Store validity window.
                ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
                stac_doc["id"] = f"{base_id}_{ts}" if base_id else ts
                if valid_from is None:
                    stac_doc["_valid_from"] = datetime.now(timezone.utc).isoformat()

            # Default (UPDATE): stable doc_id wins; the bulk action below
            # uses ``index`` semantics (upsert in place).

            # Guard against the ES 10MB per-doc limit. For oversize docs
            # the geometry is simplified in place and the ratio is
            # recorded so consumers can detect lossy storage.
            stac_doc, factor, mode = simplify_to_fit(stac_doc)
            if mode != "none":
                stac_doc["_simplification_factor"] = factor
                stac_doc["_simplification_mode"] = mode

            doc_id = stac_doc.get("id") or base_id
            if doc_id is None:
                logger.warning(
                    "ES write_entities: skipping item with no id in %s/%s",
                    catalog_id, collection_id,
                )
                continue
            prepped_bulk.append({
                "action": {"index": {
                    "_index": index_name,
                    "_id": str(doc_id),
                    "routing": collection_id,
                }},
                "doc": stac_doc,
            })
            written.append(item)

        if prepped_bulk:
            body: list = []
            for entry in prepped_bulk:
                body.append(entry["action"])
                body.append(entry["doc"])
            await es.bulk(body=body, params={"refresh": "false"})

        return written

    async def _enforce_field_constraints(
        self,
        catalog_id: str,
        collection_id: str,
        items: List[Any],
    ) -> None:
        """App-level fallback enforcement of FieldDefinition.required / .unique.

        Only runs when the collection's CollectionSchema has
        ``allow_app_level_enforcement=True`` (otherwise admission would have
        rejected any constrained fields). Raises
        ``RequiredFieldMissingError`` (HTTP 400) or
        ``UniqueConstraintViolationError`` (HTTP 409).
        """
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.modules.storage.driver_config import CollectionSchema
        from dynastore.modules.storage.field_constraints import (
            check_required, check_unique,
        )
        from dynastore.tools.discovery import get_protocol

        configs = get_protocol(ConfigsProtocol)
        if not configs:
            return
        try:
            ft = await configs.get_config(
                CollectionSchema,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
        except Exception:
            return
        if not isinstance(ft, CollectionSchema):
            return

        if not ft.allow_app_level_enforcement:
            return

        feature_dicts = [
            it if isinstance(it, dict) else (
                it.model_dump(by_alias=True, exclude_none=False)
                if hasattr(it, "model_dump") else dict(it)
            )
            for it in items
        ]
        check_required(ft.fields, feature_dicts)

        async def _exists(field_name: str, value: Any) -> bool:
            # In-batch dedup only for now; a proper ES term query per unique
            # field is a follow-up (driver-specific CQL2/term construction).
            return False

        await check_unique(ft.fields, feature_dicts, exists=_exists)

    @staticmethod
    async def _resolve_write_policy(
        catalog_id: str, collection_id: str,
    ) -> "CollectionWritePolicy":
        """Resolve CollectionWritePolicy from the config waterfall."""
        from dynastore.modules.storage.driver_config import (
            CollectionWritePolicy,
        )
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        try:
            configs = get_protocol(ConfigsProtocol)
            if configs:
                result = await configs.get_config(
                    CollectionWritePolicy,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                )
                if isinstance(result, CollectionWritePolicy):
                    return result
        except Exception:
            pass
        return CollectionWritePolicy()

    @staticmethod
    def _extract_external_id_from_doc(doc: dict, field_path: Optional[str]) -> Optional[str]:
        """Extract external_id using a dot-notation path from an ES document."""
        if field_path is None:
            return None
        val = doc
        for part in field_path.split("."):
            if isinstance(val, dict):
                val = val.get(part)
            else:
                return None
        return str(val) if val is not None else None

    @staticmethod
    async def _es_doc_exists_by_external_id(
        es: Any, index_name: str, collection_id: str, external_id: str,
    ) -> bool:
        """Check whether any document in the tenant index for this
        collection carries ``_external_id == external_id``.

        Uses ``_routing=collection_id`` so the count hits a single shard.
        """
        try:
            resp = await es.count(
                index=index_name,
                body={
                    "query": {
                        "bool": {
                            "filter": [
                                {"term": {"collection": collection_id}},
                                {"term": {"_external_id": external_id}},
                            ]
                        }
                    }
                },
                params={"routing": collection_id},
            )
            return resp.get("count", 0) > 0
        except Exception:
            return False

    async def get_entity_fields(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        entity_level: str = "item",
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Return FieldDefinition dict from ES index mappings."""
        from dynastore.models.protocols.field_definition import (
            FieldDefinition as ProtocolFieldDefinition,
            FieldCapability,
        )

        if entity_level != "item" or not collection_id:
            return {}

        es_type_map = {
            "text": [FieldCapability.FILTERABLE, FieldCapability.SORTABLE],
            "keyword": [FieldCapability.FILTERABLE, FieldCapability.SORTABLE, FieldCapability.GROUPABLE],
            "long": [FieldCapability.FILTERABLE, FieldCapability.SORTABLE, FieldCapability.AGGREGATABLE],
            "integer": [FieldCapability.FILTERABLE, FieldCapability.SORTABLE, FieldCapability.AGGREGATABLE],
            "float": [FieldCapability.FILTERABLE, FieldCapability.SORTABLE, FieldCapability.AGGREGATABLE],
            "double": [FieldCapability.FILTERABLE, FieldCapability.SORTABLE, FieldCapability.AGGREGATABLE],
            "date": [FieldCapability.FILTERABLE, FieldCapability.SORTABLE],
            "boolean": [FieldCapability.FILTERABLE],
            "geo_point": [FieldCapability.SPATIAL],
            "geo_shape": [FieldCapability.SPATIAL],
        }
        data_type_map = {
            "text": "string", "keyword": "string",
            "long": "integer", "integer": "integer",
            "float": "numeric", "double": "numeric",
            "date": "datetime", "boolean": "boolean",
            "geo_point": "geometry", "geo_shape": "geometry",
        }

        try:
            es = _es_client_required()
            index_name = _tenant_items_index(catalog_id)
            mapping = await es.indices.get_mapping(index=index_name)
            properties: Dict[str, Any] = {}
            for idx_data in mapping.values():
                properties = idx_data.get("mappings", {}).get("properties", {})
                break

            result = {}
            internal = {"_asset_id", "_external_id", "_valid_from", "_valid_to"}
            for name, field_info in properties.items():
                if name.startswith("_") and name in internal:
                    continue
                es_type = field_info.get("type", "object")
                caps = es_type_map.get(es_type, [FieldCapability.FILTERABLE])
                result[name] = ProtocolFieldDefinition(
                    name=name,
                    data_type=data_type_map.get(es_type, "string"),
                    capabilities=caps,
                )
            return result
        except Exception:
            return {}

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
        es = _es_client_required()
        index_name = _tenant_items_index(catalog_id)

        if entity_ids:
            for eid in entity_ids:
                try:
                    resp = await es.get(
                        index=index_name, id=eid,
                        params={"routing": collection_id},
                    )
                    src = resp.get("_source")
                    if src is not None:
                        yield Feature.model_validate(src)
                except Exception:
                    pass
        else:
            base = self._query_request_to_es(request) if request else {"query": {"match_all": {}}}
            # Always scope by collection so a tenant-wide index returns
            # only this collection's hits.
            collection_filter = {"term": {"collection": collection_id}}
            base_query = base.get("query", {"match_all": {}})
            scoped_query = {
                "bool": {
                    "must": [base_query],
                    "filter": [collection_filter],
                }
            }
            size = limit if request is None or request.limit is None else request.limit
            from_ = offset if request is None or request.offset is None else request.offset

            try:
                resp = await es.search(
                    index=index_name,
                    body={"query": scoped_query},
                    params={
                        "routing": collection_id,
                        "size": str(size),
                        "from": str(from_),
                    },
                )
                for hit in resp.get("hits", {}).get("hits", []):
                    try:
                        yield Feature.model_validate(hit["_source"])
                    except Exception:
                        pass
            except Exception as e:
                logger.warning(
                    "ItemsElasticsearchDriver: search failed for %s/%s: %s",
                    catalog_id, collection_id, e,
                )

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
            logger.info(
                "ES delete_entities(soft=True): marking %d entities as deleted",
                len(entity_ids),
            )
        es = _es_client_required()
        index_name = _tenant_items_index(catalog_id)
        deleted = 0
        for eid in entity_ids:
            try:
                await es.delete(
                    index=index_name, id=eid,
                    params={"routing": collection_id, "ignore": "404"},
                )
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
        """Idempotently create the per-tenant items index and enrol it
        in the platform public alias.

        The index ``{prefix}-items-{catalog_id}`` hosts every collection's
        items for this catalog; collection scoping is enforced via
        ``_routing=collection_id`` on every write/read. Membership in
        ``{prefix}-items-public`` makes the data discoverable through OGC
        search routes regardless of tenant.

        ``collection_id`` is accepted for protocol parity but ignored —
        the same tenant index serves all collections of the catalog.
        """
        from dynastore.modules.elasticsearch.aliases import (
            add_index_to_public_alias,
        )
        from dynastore.modules.elasticsearch.mappings import ITEM_MAPPING

        es = _es_client_required()
        index_name = _tenant_items_index(catalog_id)

        try:
            exists = await es.indices.exists(index=index_name)
        except Exception as exc:
            logger.warning(
                "ItemsElasticsearchDriver.ensure_storage: exists() failed for "
                "'%s': %s", index_name, exc,
            )
            exists = False

        if not exists:
            try:
                await es.indices.create(
                    index=index_name,
                    body={"mappings": ITEM_MAPPING},
                )
                logger.info(
                    "ItemsElasticsearchDriver: created tenant items index '%s'.",
                    index_name,
                )
            except Exception as exc:
                logger.warning(
                    "ItemsElasticsearchDriver.ensure_storage: create('%s') "
                    "failed: %s — assuming concurrent create",
                    index_name, exc,
                )

        # Idempotent — safe even if the index was already a member.
        await add_index_to_public_alias(index_name)

    async def drop_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        soft: bool = False,
    ) -> None:
        from dynastore.modules.elasticsearch.aliases import (
            remove_index_from_public_alias,
        )

        es = _es_client_required()
        index_name = _tenant_items_index(catalog_id)
        if collection_id:
            try:
                await es.delete_by_query(
                    index=index_name,
                    body={"query": {"term": {"collection": collection_id}}},
                    params={"routing": collection_id, "refresh": "false"},
                )
            except Exception as e:
                logger.warning(
                    "drop_storage collection delete_by_query failed for %s/%s: %s",
                    catalog_id, collection_id, e,
                )
        else:
            await remove_index_from_public_alias(index_name)
            try:
                await es.indices.delete(
                    index=index_name, params={"ignore_unavailable": "true"},
                )
            except Exception as e:
                logger.warning("drop_storage catalog delete failed: %s", e)

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
            "ItemsElasticsearchDriver.export_entities: not supported. "
            "Export from the primary driver instead."
        )

    # ------------------------------------------------------------------
    # Catalog & Collection support (via SFEOS DatabaseLogic)
    # ------------------------------------------------------------------

    async def write_catalog(self, catalog_id: str, catalog_doc: dict) -> None:
        """Index a catalog document via SFEOS."""
        db = self._get_db_logic()
        catalog_doc.setdefault("id", catalog_id)
        catalog_doc.setdefault("type", "Catalog")
        await db.create_catalog(catalog_doc, refresh=False)

    async def delete_catalog(self, catalog_id: str) -> None:
        """Delete a catalog from ES via SFEOS."""
        db = self._get_db_logic()
        await db.delete_catalog(catalog_id, refresh=False)

    async def write_collection(
        self, catalog_id: str, collection_id: str, collection_doc: dict,
    ) -> None:
        """Index a collection document via SFEOS."""
        db = self._get_db_logic()
        collection_doc.setdefault("id", collection_id)
        collection_doc.setdefault("type", "Collection")
        try:
            await db.create_collection(collection_doc, refresh=False)
        except Exception:
            # Already exists — update
            await db.find_collection(collection_id)
            from stac_fastapi.sfeos_helpers.database import (  # type: ignore[import-not-found]
                update_catalog_in_index_shared,
            )
            await update_catalog_in_index_shared(
                db.client, collection_id, collection_doc,
            )

    async def delete_collection_doc(
        self, catalog_id: str, collection_id: str,
    ) -> None:
        """Delete a collection document from ES via SFEOS."""
        db = self._get_db_logic()
        try:
            await db.delete_collection(collection_id, refresh=False)
        except Exception as e:
            logger.debug("delete_collection_doc: %s", e)

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    async def _on_catalog_upsert(
        self, catalog_id: Optional[str] = None, payload=None, **kwargs,
    ):
        if not catalog_id:
            return
        if not await self._is_secondary_for(type(self).__name__, catalog_id, None):
            return
        try:
            doc = await self._serialize_catalog(catalog_id)
            if doc is None:
                doc = payload if isinstance(payload, dict) else {"id": catalog_id}
            await self.write_catalog(catalog_id, doc)
        except Exception as e:
            logger.error("ES driver: catalog upsert failed for '%s': %s", catalog_id, e)

    async def _on_catalog_delete(self, catalog_id: Optional[str] = None, **kwargs):
        if not catalog_id:
            return
        if not await self._is_secondary_for(type(self).__name__, catalog_id, None):
            return
        try:
            await self.delete_catalog(catalog_id)
        except Exception as e:
            logger.error("ES driver: catalog delete failed for '%s': %s", catalog_id, e)

    async def _on_collection_upsert(
        self, catalog_id: Optional[str] = None, collection_id: Optional[str] = None,
        payload=None, **kwargs,
    ):
        if not catalog_id or not collection_id:
            return
        if not await self._is_secondary_for(type(self).__name__, catalog_id, collection_id):
            return
        try:
            doc = await self._serialize_collection(catalog_id, collection_id)
            if doc is None:
                doc = payload if isinstance(payload, dict) else {}
            await self.write_collection(catalog_id, collection_id, doc)
        except Exception as e:
            logger.error(
                "ES driver: collection upsert failed for '%s/%s': %s",
                catalog_id, collection_id, e,
            )

    async def _on_collection_delete(
        self, catalog_id: Optional[str] = None, collection_id: Optional[str] = None, **kwargs,
    ):
        if not catalog_id or not collection_id:
            return
        if not await self._is_secondary_for(type(self).__name__, catalog_id, collection_id):
            return
        try:
            await self.delete_collection_doc(catalog_id, collection_id)
        except Exception as e:
            logger.error(
                "ES driver: collection delete failed for '%s/%s': %s",
                catalog_id, collection_id, e,
            )

    async def _on_item_upsert(
        self, catalog_id: Optional[str] = None, collection_id: Optional[str] = None,
        item_id: Optional[str] = None, payload=None, **kwargs,
    ):
        if not catalog_id or not collection_id or not item_id:
            return
        if not await self._is_secondary_for(type(self).__name__, catalog_id, collection_id):
            return
        # Skip event-driven indexing when driver is in WRITE routing —
        # the router fan-out already handled this write.
        if await self._is_write_driver_for(type(self).__name__, catalog_id, collection_id):
            return
        try:
            doc = await self._serialize_item(catalog_id, collection_id, item_id)
            if doc is None:
                doc = payload if isinstance(payload, dict) else {}
                doc.update({
                    "id": item_id,
                    "collection": collection_id,
                })
            es = _es_client_required()
            index_name = _tenant_items_index(catalog_id)
            await es.index(
                index=index_name, id=item_id, body=doc,
                params={"routing": collection_id},
            )
        except Exception as e:
            logger.error(
                "ES driver: item upsert failed for %s/%s/%s: %s",
                catalog_id, collection_id, item_id, e,
            )

    async def _on_item_bulk_upsert(
        self, catalog_id: Optional[str] = None, collection_id: Optional[str] = None,
        payload=None, **kwargs,
    ):
        if not catalog_id or not collection_id:
            return
        if not await self._is_secondary_for(type(self).__name__, catalog_id, collection_id):
            return
        # Skip event-driven indexing when driver is in WRITE routing —
        # the router fan-out already handled this write.
        if await self._is_write_driver_for(type(self).__name__, catalog_id, collection_id):
            return

        items_subset = (payload if isinstance(payload, dict) else {}).get("items_subset", [])
        if not items_subset:
            return

        try:
            es = _es_client_required()
            index_name = _tenant_items_index(catalog_id)
            body: list = []
            for item_doc in items_subset:
                item_doc.setdefault("collection", collection_id)
                doc_id = item_doc.get("id")
                if doc_id is None:
                    continue
                body.append({"index": {
                    "_index": index_name,
                    "_id": str(doc_id),
                    "routing": collection_id,
                }})
                body.append(item_doc)
            if body:
                await es.bulk(body=body, params={"refresh": "false"})
        except Exception as e:
            logger.error(
                "ES driver: bulk upsert failed for %s/%s: %s",
                catalog_id, collection_id, e,
            )

    async def _on_item_delete(
        self, catalog_id: Optional[str] = None, collection_id: Optional[str] = None,
        item_id: Optional[str] = None, payload=None, **kwargs,
    ):
        if not item_id:
            _val = (payload if isinstance(payload, dict) else {}).get("geoid")
            item_id = str(_val) if _val is not None else None
        if not catalog_id or not collection_id or not item_id:
            return
        if not await self._is_secondary_for(type(self).__name__, catalog_id, collection_id):
            return
        try:
            es = _es_client_required()
            index_name = _tenant_items_index(catalog_id)
            await es.delete(
                index=index_name, id=item_id,
                params={"routing": collection_id, "ignore": "404"},
            )
        except Exception as e:
            logger.error(
                "ES driver: item delete failed for %s/%s/%s: %s",
                catalog_id, collection_id, item_id, e,
            )

    # ------------------------------------------------------------------
    # Serialization helpers (reuse existing DynaStore services)
    # ------------------------------------------------------------------

    @staticmethod
    async def _serialize_item(
        catalog_id: str, collection_id: str, item_id: str,
    ) -> Optional[dict]:
        try:
            from dynastore.modules.catalog.item_service import ItemService
            from typing import cast as _cast4
            from dynastore.models.protocols import DbProtocol
            from dynastore.modules.db_config.query_executor import DbResource
            from dynastore.tools.discovery import get_protocol

            db = get_protocol(DbProtocol)
            item_svc = get_protocol(ItemService)
            if not item_svc:
                item_svc = ItemService(engine=_cast4(Optional[DbResource], db))

            feature = await item_svc.get_item(catalog_id, collection_id, item_id)
            if feature is None:
                return None

            doc = feature.model_dump(by_alias=True, exclude_none=True)
            doc["collection"] = collection_id
            return doc
        except Exception as e:
            logger.warning("Failed to serialize item %s/%s/%s: %s",
                           catalog_id, collection_id, item_id, e)
            return None

    @staticmethod
    async def _serialize_catalog(catalog_id: str) -> Optional[dict]:
        try:
            from dynastore.models.protocols import CatalogsProtocol
            from dynastore.tools.discovery import get_protocol

            catalogs = get_protocol(CatalogsProtocol)
            if not catalogs:
                return None
            model = await catalogs.get_catalog_model(catalog_id)
            if model is None:
                return None
            doc = model.model_dump(by_alias=True, exclude_none=True) if hasattr(model, "model_dump") else {}
            doc.setdefault("id", catalog_id)
            doc.setdefault("type", "Catalog")
            return doc
        except Exception as e:
            logger.warning("Failed to serialize catalog %s: %s", catalog_id, e)
            return None

    @staticmethod
    async def _serialize_collection(
        catalog_id: str, collection_id: str,
    ) -> Optional[dict]:
        try:
            from dynastore.models.protocols import CatalogsProtocol
            from dynastore.tools.discovery import get_protocol

            catalogs = get_protocol(CatalogsProtocol)
            if not catalogs:
                return None
            model = await catalogs.get_collection_model(catalog_id, collection_id)  # type: ignore[attr-defined]
            if model is None:
                return None
            doc = model.model_dump(by_alias=True, exclude_none=True) if hasattr(model, "model_dump") else {}
            doc.setdefault("id", collection_id)
            doc.setdefault("type", "Collection")
            return doc
        except Exception as e:
            logger.warning("Failed to serialize collection %s/%s: %s",
                           catalog_id, collection_id, e)
            return None

    @staticmethod
    def _query_request_to_es(request: QueryRequest) -> dict:
        """Convert a QueryRequest to an ES query body."""
        must: list = []
        for f in request.filters:
            op = f.operator if isinstance(f.operator, str) else f.operator.value
            if op == "bbox" and f.field == "geometry":
                coords = f.value
                if isinstance(coords, (list, tuple)) and len(coords) >= 4:
                    must.append({
                        "geo_bounding_box": {
                            "geometry": {
                                "top_left": {"lon": coords[0], "lat": coords[3]},
                                "bottom_right": {"lon": coords[2], "lat": coords[1]},
                            }
                        }
                    })
            elif op in ("eq", "="):
                must.append({"term": {f.field: f.value}})
            elif op in ("like", "ilike"):
                must.append({"wildcard": {f.field: f.value}})

        if not must:
            return {"query": {"match_all": {}}}
        return {"query": {"bool": {"must": must}}}

    async def location(
        self,
        catalog_id: str,
        collection_id: str,
    ) -> "StorageLocation":
        """Return typed physical storage coordinates for this collection.

        The tenant index hosts every collection of the catalog; the
        ``routing`` identifier records the per-collection shard key so
        consumers can reproduce reads.
        """
        from dynastore.modules.storage.storage_location import StorageLocation

        index_name = _tenant_items_index(catalog_id)
        return StorageLocation(
            backend="elasticsearch",
            canonical_uri=f"es://{index_name}?routing={collection_id}",
            identifiers={
                "index": index_name,
                "routing": collection_id,
            },
            display_label=f"{index_name} (routing={collection_id})",
        )



# ---------------------------------------------------------------------------
# AssetElasticsearchDriver — per-catalog asset index
# ---------------------------------------------------------------------------

class AssetElasticsearchDriver(
    TypedDriver[AssetElasticsearchDriverConfig], _ElasticsearchBase, ModuleProtocol,
):
    """Elasticsearch storage driver for asset metadata.

    Multi-tier scope today
    ----------------------
    The per-catalog index ``{prefix}-assets-{catalog_id}`` (mapping at
    ``modules/elasticsearch/mappings.py:ASSET_MAPPING``) carries BOTH
    catalog-tier and collection-tier assets — the mapping has both
    ``catalog_id`` (always set) and a nullable ``collection_id``
    (NULL for catalog-tier assets, set for collection-tier).  This is
    NOT a per-tier driver; it serves the catalog/collection asset
    spectrum from one index.

    Indexer marker — opts in to :class:`AssetIndexer` only.  The
    ``AssetIndexer`` marker is documented as tier-spanning at the
    catalog/collection level (see ``models/protocols/indexer.py``); per-tier
    asset routing is unnecessary today because both tiers land in the
    same index.

    Extension axis — future tier expansions
    ---------------------------------------
    Two future tiers are pre-declared as marker Protocols in
    ``models/protocols/indexer.py`` but have no implementer yet:

    * :class:`ItemAssetIndexer` (``is_item_asset_indexer``) — for
      promoting item-embedded assets to first-class index entries.
      Today STAC item docs store ``assets`` as opaque blob
      (``COMMON_PROPERTIES`` declares ``"assets": {"enabled": False}``);
      promotion is a deferred STAC read/write refactor.  When it lands,
      this class will add ``is_item_asset_indexer: ClassVar[bool] = True``
      and start emitting per-item-asset documents to the same per-catalog
      index (with ``item_id`` populated; mapping field already added for
      forward-compat).
    * :class:`PlatformAssetIndexer` (``is_platform_asset_indexer``) —
      for assets above the catalog scope (no design yet; ``AssetBase``
      requires ``catalog_id`` today).

    Both marker opt-ins are deferred until the consumer tier ships;
    the markers themselves exist so future drivers (or this driver's
    future extension) can self-register without a rename.

    Lifecycle wiring
    ----------------
    No lifespan-time wiring is required.  Asset writes flow through the
    ``AssetIndexer`` route in ``AssetRoutingConfig.operations[INDEX]`` —
    invoked by ``AssetService``'s secondary-driver fan-out — and through
    direct programmatic calls to ``index_asset()`` / ``delete_asset()``.

    Registered as ``storage_elasticsearch_assets`` via entry points.
    """

    is_asset_indexer: ClassVar[bool] = True

    priority: int = 52
    capabilities: FrozenSet[str] = frozenset({
        Capability.READ,
        Capability.WRITE,
        Capability.STREAMING,
        Capability.FULLTEXT,
        Capability.PHYSICAL_ADDRESSING,
    })
    preferred_for: FrozenSet[str] = frozenset({"search", "assets"})
    supported_hints: FrozenSet[str] = frozenset({"search", "assets"})

    def is_available(self) -> bool:
        return self._sfeos_available()

    def _get_client(self):
        """Get the async ES client from SFEOS DatabaseLogic."""
        return self._get_db_logic().client

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        # Asset writes flow exclusively through the AssetIndexer route in
        # AssetRoutingConfig.operations[INDEX] — invoked by AssetService's
        # secondary-driver fan-out. The previous CatalogEventType.ASSET_*
        # listener path was retired to eliminate a dual-write race against
        # the same index.
        yield

    # ------------------------------------------------------------------
    # Public API — direct programmatic indexing
    # ------------------------------------------------------------------

    async def index_asset(
        self, catalog_id: str, asset_doc: Dict[str, Any],
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Index a single asset document."""
        from dynastore.modules.elasticsearch.mappings import (
            get_assets_index_name, ASSET_MAPPING,
        )
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()

        if not await es.indices.exists(index=index_name):
            await es.indices.create(
                index=index_name,
                body={"mappings": ASSET_MAPPING},
                ignore=400,
            )

        asset_id = asset_doc.get("asset_id", asset_doc.get("id"))
        await es.index(index=index_name, id=asset_id, document=asset_doc)

    async def delete_asset(
        self, catalog_id: str, asset_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Delete a single asset document from the index."""
        from dynastore.modules.elasticsearch.mappings import get_assets_index_name
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()
        try:
            await es.delete(index=index_name, id=asset_id)
        except Exception:
            pass

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
        from dynastore.modules.elasticsearch.mappings import (
            get_assets_index_name, ASSET_MAPPING,
        )
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        items = self._normalize_entities(entities)
        es = self._get_client()

        if not await es.indices.exists(index=index_name):
            await es.indices.create(
                index=index_name,
                body={"mappings": ASSET_MAPPING},
                ignore=400,
            )

        bulk_body: list = []
        for item in items:
            doc = item if isinstance(item, dict) else (
                item.model_dump(by_alias=True, exclude_none=True)
                if hasattr(item, "model_dump") else dict(item)
            )
            doc.setdefault("catalog_id", catalog_id)
            doc.setdefault("collection_id", collection_id)
            asset_id = doc.get("asset_id", doc.get("id", ""))
            bulk_body.append({"index": {"_index": index_name, "_id": asset_id}})
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
        from dynastore.modules.elasticsearch.mappings import get_assets_index_name
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        if not entity_ids:
            return

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()

        if not await es.indices.exists(index=index_name):
            return

        for asset_id in entity_ids:
            try:
                resp = await es.get(index=index_name, id=asset_id)
                source = resp["_source"]
                yield Feature(
                    type="Feature",
                    id=source.get("asset_id", asset_id),
                    geometry=None,
                    properties=source,
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
                "AssetElasticsearchDriver does not support soft delete."
            )
        from dynastore.modules.elasticsearch.mappings import get_assets_index_name
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()
        deleted = 0
        for asset_id in entity_ids:
            try:
                await es.delete(index=index_name, id=asset_id)
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
        from dynastore.modules.elasticsearch.mappings import (
            get_assets_index_name, ASSET_MAPPING,
        )
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()
        if not await es.indices.exists(index=index_name):
            await es.indices.create(
                index=index_name,
                body={"mappings": ASSET_MAPPING},
                ignore=400,
            )

    async def drop_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        soft: bool = False,
    ) -> None:
        if soft:
            raise SoftDeleteNotSupportedError(
                "AssetElasticsearchDriver does not support soft drop."
            )
        from dynastore.modules.elasticsearch.mappings import get_assets_index_name
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()
        await es.indices.delete(index=index_name, ignore_unavailable=True)

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
            "AssetElasticsearchDriver.export_entities: not supported."
        )

    # ------------------------------------------------------------------
    # AssetStore read methods
    # ------------------------------------------------------------------

    async def get_asset(
        self,
        catalog_id: str,
        asset_id: str,
        *,
        collection_id: Optional[str] = None,
        db_resource=None,
    ) -> Optional[Dict[str, Any]]:
        """Return a single asset document from ES by its ID, or None."""
        from dynastore.modules.elasticsearch.mappings import get_assets_index_name
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()
        try:
            resp = await es.get(index=index_name, id=asset_id)
            return resp["_source"]
        except Exception:
            return None

    # ES DSL top-level query keywords — used to distinguish raw ES DSL from
    # simple {field: value} filter dicts passed by AssetService.
    _ES_DSL_KEYWORDS = frozenset({
        "match_all", "bool", "term", "terms", "match", "range",
        "exists", "prefix", "wildcard", "regexp", "fuzzy",
        "ids", "multi_match", "query_string", "nested",
        "match_phrase", "match_phrase_prefix",
    })

    @classmethod
    def _to_es_query(cls, query: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a simple ``{field: value}`` dict to an ES bool/filter/term query.

        If *query* already looks like ES DSL (contains a known ES keyword as a
        top-level key), it is returned unchanged.  Otherwise each entry becomes
        a ``term`` filter clause, supporting dot-notation for nested fields
        (e.g. ``metadata.license_id``).
        """
        if not query:
            return {"match_all": {}}

        # Detect raw ES DSL — fast path
        if query.keys() & cls._ES_DSL_KEYWORDS:
            return query

        # Convert field=value pairs to ES term filters
        filters = [{"term": {field: value}} for field, value in query.items()]
        if len(filters) == 1:
            return filters[0]
        return {"bool": {"filter": filters}}

    async def search_assets(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        query: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        db_resource=None,
    ) -> List[Dict[str, Any]]:
        """Search asset documents in ES.

        ``query`` may be:
        - A raw ES query DSL dict (detected by presence of ES keywords like
          ``bool``, ``term``, ``match``, etc.)
        - A simple ``{field: value}`` dict — each entry is converted to a
          ``term`` filter.  Dot-notation (e.g. ``metadata.license_id``) is
          supported natively by ES for dynamically mapped nested fields.
        - ``None`` → ``match_all``

        ``collection_id`` is added as an extra ``term`` filter when provided.
        """
        from dynastore.modules.elasticsearch.mappings import get_assets_index_name
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()

        base_query = self._to_es_query(query or {})
        if collection_id:
            base_query = {
                "bool": {
                    "must": [base_query],
                    "filter": [{"term": {"collection_id": collection_id}}],
                }
            }

        try:
            resp = await es.search(
                index=index_name,
                query=base_query,
                size=limit,
                from_=offset,
            )
            return [hit["_source"] for hit in resp["hits"]["hits"]]
        except Exception as e:
            logger.error("AssetElasticsearchDriver.search_assets failed: %s", e)
            return []

    async def location(
        self,
        catalog_id: str,
        collection_id: str,
    ) -> "StorageLocation":
        """Return typed physical storage coordinates for this asset index."""
        from dynastore.modules.storage.storage_location import StorageLocation
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.elasticsearch.mappings import get_assets_index_name

        prefix = _get_index_prefix()
        index_name = get_assets_index_name(prefix, catalog_id)
        return StorageLocation(
            backend="elasticsearch_assets",
            canonical_uri=f"es://{index_name}",
            identifiers={"index": index_name, "prefix": prefix, "catalog_id": catalog_id},
            display_label=index_name,
        )


