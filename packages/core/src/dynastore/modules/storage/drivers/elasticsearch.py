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

* ``ItemsElasticsearchDriver``  (driver_ref ``"elasticsearch"``)
  Items are written directly to a per-tenant index
  ``{prefix}-items-{catalog_id}`` (helper :func:`get_tenant_items_index`)
  with ``_routing=collection_id`` so a single index hosts every collection
  of one catalog while keeping shard locality per collection.  The index
  is enrolled in the platform alias ``{prefix}-items-public`` so OGC
  discovery search routes can target one alias regardless of tenant.
  Catalog and collection documents are owned by the dedicated
  ``catalog_es_driver`` / ``collection_es_driver`` modules (see
  :mod:`dynastore.modules.elasticsearch`).

* ``AssetElasticsearchDriver``  (driver_ref ``"elasticsearch_assets"``)
  Indexes asset metadata into per-catalog ``{prefix}-assets-{catalog_id}``
  indices.  Driven by the secondary-index ``WRITE`` entries
  (``secondary_index=True``) in ``AssetRoutingConfig.operations[WRITE]``
  (auto-augmented with discoverable ``AssetIndexer`` impls) and dispatched via
  ``AssetEntitySyncSubscriber`` from the events outbox — single-writer fan-out,
  no per-driver listener block.  Direct programmatic indexing via
  ``index_asset()`` / ``delete_asset()`` remains available.

The private driver (``ItemsElasticsearchPrivateDriver``) lives in
its own self-contained subpackage at
:mod:`dynastore.modules.storage.drivers.elasticsearch_private` so the
``[private]`` extras group can be opted in or out without touching this
file.

All drivers register as async event listeners, dispatched by the post-PR-#261
operation-based router via the secondary-index ``WRITE`` entries
(``secondary_index=True``) in ``ItemsRoutingConfig.operations[WRITE]`` /
``AssetRoutingConfig.operations[WRITE]``.
"""

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncIterator, ClassVar, Dict, FrozenSet, List, Optional, Union

if TYPE_CHECKING:
    from dynastore.modules.storage.driver_config import ItemsWritePolicy
    from dynastore.modules.storage.storage_location import StorageLocation

from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.driver_context import DriverContext
from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.protocols.typed_driver import TypedDriver
from dynastore.models.query_builder import AssetFilter, QueryRequest
from dynastore.modules.protocols import ModuleProtocol
from dynastore.modules.tools.asset_filters import build_es_query
from dynastore.modules.storage.driver_config import (
    AssetElasticsearchDriverConfig,
    ItemsElasticsearchDriverConfig,
)
from dynastore.modules.storage.errors import SoftDeleteNotSupportedError
from dynastore.modules.storage.hints import Hint
from dynastore.modules.storage.routing_config import Operation

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


# Fields that the items ES mapping models as multilingual ``object`` blocks
# (one ``text`` sub-property per supported locale — see
# ``items_projection._localized_text_field``). The platform's read pipeline
# (``ItemMetadataSidecar.map_row_to_feature``) collapses these to a plain
# string for the requested ``context.lang`` before the Feature is dumped to
# the dispatch payload. Sending a string to an ``object``-typed field would
# trip ``object mapping for [properties.<field>] tried to parse field [..]
# as object, but found a concrete value`` and fail every item write on the
# dispatcher path. Re-wrap the collapsed string back into the canonical
# ``{<lang>: <value>}`` dict so the ES mapping accepts it.
_LOCALIZED_OBJECT_PROPERTIES: tuple = ("title", "description")
# ``keywords`` is mapped as keyword + .text — concrete values are fine
# there, so it intentionally stays out of the wrap list.


def _ensure_localized_object_shape(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Re-wrap localised string fields back into ``{<lang>: <value>}`` dicts.

    The read pipeline collapses multilingual fields (``properties.title``,
    ``properties.description``) into a plain string for the active
    ``context.lang``. The ES items mapping types those fields as ``object``
    with per-locale sub-properties, so a string payload is rejected by ES.

    This helper restores the canonical object shape — non-destructive when
    the field is already a dict or absent. Pure function; the input is not
    mutated. The lang label defaults to ``en`` (matches
    ``FeaturePipelineContext.lang`` default and aligns with the only
    language the dispatch path resolves at present).
    """
    if not isinstance(doc, dict):
        return doc
    props = doc.get("properties")
    if not isinstance(props, dict):
        return doc
    new_props: Optional[Dict[str, Any]] = None
    for field in _LOCALIZED_OBJECT_PROPERTIES:
        value = props.get(field)
        if isinstance(value, str):
            if new_props is None:
                new_props = dict(props)
            new_props[field] = {"en": value}
    if new_props is None:
        return doc
    out = dict(doc)
    out["properties"] = new_props
    return out


# Per-process cache: catalogs whose tenant items index has already been
# added to the platform public alias on this worker. Live indexer paths
# (index() / index_bulk()) consult this before issuing the alias call so
# the cost is bounded to one ES round-trip per (catalog, process) lifetime
# rather than per-write. ``ensure_storage`` and the OUTBOX drainer also
# fire ``add_index_to_public_alias`` (idempotent ES-side) but neither
# runs on the live ASYNC indexer path that ES auto-creation hits, leaving
# new tenant indices unaliased and invisible to alias-scoped search.
_ALIAS_REGISTERED_CATALOGS: set = set()


async def _ensure_in_public_alias_once(catalog_id: str, index_name: str) -> None:
    """Add ``index_name`` to the platform public alias, at most once per
    process. Failures are swallowed-with-log; the OUTBOX drainer and
    ``ensure_storage`` paths cover the same ground so a transient miss
    here is recovered by the next write through one of those routes.
    """
    if catalog_id in _ALIAS_REGISTERED_CATALOGS:
        return
    try:
        from dynastore.modules.elasticsearch.aliases import (
            add_index_to_public_alias,
        )
        await add_index_to_public_alias(index_name)
    except Exception as exc:
        logger.warning(
            "ItemsElasticsearchDriver: alias-add failed for '%s': %s",
            index_name, exc,
        )
        return
    _ALIAS_REGISTERED_CATALOGS.add(catalog_id)


# ---------------------------------------------------------------------------
# Shared base
# ---------------------------------------------------------------------------

class _ElasticsearchBase:
    """Shared helpers for ES storage drivers."""

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
        driver_ref: str, catalog_id: str, collection_id: Optional[str],
    ) -> bool:
        """Check if this driver is listed in the routing config for the given scope."""
        try:
            from typing import cast as _cast
            from dynastore.models.protocols.configs import ConfigsProtocol
            from dynastore.tools.discovery import get_protocol
            from dynastore.modules.storage.routing_config import (
                ItemsRoutingConfig,
            )

            configs = get_protocol(ConfigsProtocol)
            if not configs:
                return False
            routing = _cast(
                Optional[ItemsRoutingConfig],
                await configs.get_config(
                    ItemsRoutingConfig,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                ),
            )
            if routing is None:
                return False
            from typing import cast as _cast2
            ops = _cast2(Dict[str, list], routing.operations)
            return any(
                entry.driver_ref == driver_ref
                for entries in ops.values()
                for entry in entries
            )
        except Exception:
            return False

    @staticmethod
    async def _is_write_driver_for(
        driver_ref: str, catalog_id: str, collection_id: Optional[str],
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
                ItemsRoutingConfig,
            )

            configs = get_protocol(ConfigsProtocol)
            if not configs:
                return False
            routing = _cast(
                Optional[ItemsRoutingConfig],
                await configs.get_config(
                    ItemsRoutingConfig,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                ),
            )
            if routing is None:
                return False
            from typing import cast as _cast3
            ops2 = _cast3(Dict[str, list], routing.operations)
            write_entries = ops2.get(Operation.WRITE, [])
            return any(e.driver_ref == driver_ref for e in write_entries)
        except Exception:
            return False

    @staticmethod
    def _feature_to_stac_item(
        feature: Any, catalog_id: str, collection_id: str,
    ) -> dict:
        """Serialize a Feature to a STAC item dict."""
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
# ItemsElasticsearchDriver — public STAC items index
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

    Catalog and collection documents are owned by the dedicated
    :class:`CatalogElasticsearchDriver` and
    :class:`CollectionElasticsearchDriver` (see
    :mod:`dynastore.modules.elasticsearch`).

    Registered as ``storage_elasticsearch`` via entry points.

    Indexer marker — opts in to :class:`ItemIndexer` so the items routing
    config auto-registers it in ``operations[WRITE]`` as a secondary-index
    entry (``secondary_index=True``).
    """

    is_item_indexer: ClassVar[bool] = True

    # ES (public) is the canonical async secondary index + primary SEARCH
    # backend for items routing.  It auto-defaults into WRITE (as a secondary
    # index, identified by ``is_item_indexer``) and SEARCH.
    auto_register_for_routing: ClassVar[FrozenSet[str]] = frozenset({Operation.SEARCH, Operation.WRITE})

    priority: int = 50
    preferred_chunk_size: int = 500
    capabilities: FrozenSet[str] = frozenset({
        Capability.READ,
        Capability.WRITE,
        Capability.STREAMING,
        Capability.SOFT_DELETE,
        Capability.EXTERNAL_ID_TRACKING,
        Capability.TEMPORAL_VALIDITY,
        Capability.PHYSICAL_ADDRESSING,
        Capability.INTROSPECTION,
    })
    preferred_for: FrozenSet[Hint] = frozenset({Hint.SEARCH, Hint.GEOMETRY_SIMPLIFIED})
    supported_hints: FrozenSet[Hint] = frozenset({
        Hint.SEARCH, Hint.FULLTEXT,
        Hint.GEOMETRY_SIMPLIFIED,  # PR #185 default routing: ES serves the fast simplified-geometry read path
        Hint.SPATIAL_FILTER, Hint.ATTRIBUTE_FILTER, Hint.SORT,
        Hint.AGGREGATION, Hint.COUNT, Hint.STATISTICS,
    })

    def is_available(self) -> bool:
        # Available whenever the shared ES client is wired up.
        try:
            from dynastore.modules.elasticsearch.client import get_client
        except (ImportError, ModuleNotFoundError):
            return False
        return get_client() is not None

    @property
    def es_client(self) -> Any:
        """Shared async ES client.

        The platform's ``ElasticsearchModule.lifespan`` initialises the
        singleton; this property returns the same instance every driver
        and the search extension use. Consumers outside this module
        (notably ``SearchService``) reach the ES engine through this
        property so that nothing imports
        :func:`dynastore.modules.elasticsearch.client.get_client` directly
        — the search extension reuses the platform engine via the
        routing-resolved driver instance.

        Raises ``RuntimeError`` when ``ElasticsearchModule.lifespan``
        has not started yet.
        """
        return _es_client_required()

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        """No-op lifecycle.

        Item-tier propagation runs through the :class:`IndexDispatcher`
        (Phase 2d) — invoked directly from ``item_service.upsert`` and
        ``item_query.delete``. Catalog and collection documents are
        owned by :class:`CatalogElasticsearchDriver` and
        :class:`CollectionElasticsearchDriver` (in
        :mod:`dynastore.modules.elasticsearch`), so this driver does
        not subscribe to any catalog/collection events.
        """
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
        """Write/upsert entities to Elasticsearch respecting ItemsWritePolicy.

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
        from dynastore.modules.elasticsearch.items_projection import (
            project_item_for_es,
            resolve_catalog_known_fields,
        )
        from dynastore.tools.geometry_simplify import simplify_to_fit

        items = self._normalize_entities(entities)
        if not items:
            return []
        es = _es_client_required()
        index_name = _tenant_items_index(catalog_id)
        known_fields = await resolve_catalog_known_fields(catalog_id)

        # Service-layer enforcement of FieldDefinition.required / .unique for
        # drivers (like ES) that don't advertise native REQUIRED_ENFORCEMENT /
        # UNIQUE_ENFORCEMENT. Only runs when the collection's
        # ItemsSchema has allow_app_level_enforcement=True; otherwise
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

            # Resolve external_id from the configured ComputedField path.
            external_id = self._extract_external_id_from_doc(stac_doc, policy.external_id_path())
            if policy.external_id_required() and not external_id:
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
                # An open lower bound (ValiditySpec.start_from is None) keeps
                # ``_valid_from`` unset; otherwise a missing start defaults to
                # the ingestion instant for this new version. (#1172)
                start_is_open = (
                    policy.validity is not None and policy.validity.start_from is None
                )
                if valid_from is None and not start_is_open:
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
            stac_doc = project_item_for_es(stac_doc, known_fields)
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

        Only runs when the collection's ItemsSchema has
        ``allow_app_level_enforcement=True`` (otherwise admission would have
        rejected any constrained fields). Raises
        ``RequiredFieldMissingError`` (HTTP 400) or
        ``UniqueConstraintViolationError`` (HTTP 409).
        """
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.modules.storage.driver_config import ItemsSchema
        from dynastore.modules.storage.field_constraints import (
            check_required, check_unique,
        )
        from dynastore.tools.discovery import get_protocol

        configs = get_protocol(ConfigsProtocol)
        if not configs:
            return
        try:
            ft = await configs.get_config(
                ItemsSchema,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
        except Exception:
            return
        if not isinstance(ft, ItemsSchema):
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
    ) -> "ItemsWritePolicy":
        """Resolve ItemsWritePolicy from the config waterfall."""
        from dynastore.modules.storage.driver_config import (
            ItemsWritePolicy,
        )
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        try:
            configs = get_protocol(ConfigsProtocol)
            if configs:
                result = await configs.get_config(
                    ItemsWritePolicy,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                )
                if isinstance(result, ItemsWritePolicy):
                    return result
        except Exception:
            pass
        return ItemsWritePolicy()

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

        The index ``{prefix}-{catalog_id}-items`` hosts every collection's
        items for this catalog; collection scoping is enforced via
        ``_routing=collection_id`` on every write/read. Membership in the
        platform public alias ``{prefix}-items`` makes the data
        discoverable through OGC search routes regardless of tenant.

        ``collection_id`` is accepted for protocol parity but ignored —
        the same tenant index serves all collections of the catalog.
        """
        from dynastore.modules.elasticsearch.aliases import (
            add_index_to_public_alias,
        )
        from dynastore.modules.elasticsearch.mappings import build_item_mapping
        from dynastore.modules.elasticsearch.items_projection import (
            resolve_catalog_known_fields,
        )
        from dynastore.modules.elasticsearch.index_config import (
            get_items_index_settings,
        )

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
            # Snapshot the per-catalog Tier-1 ∪ Tier-2 known-fields at
            # index-create time. Live edits to ``mapping`` do not
            # retro-patch ES (ES disallows tightening a live mapping);
            # they take effect on the next index rebuild.
            known_fields = await resolve_catalog_known_fields(catalog_id)
            try:
                await es.indices.create(
                    index=index_name,
                    body={
                        "settings": await get_items_index_settings(),
                        "mappings": build_item_mapping(known_fields),
                    },
                )
                logger.info(
                    "ItemsElasticsearchDriver: created tenant items index '%s' "
                    "with %d known fields (Tier 1 ∪ Tier 2).",
                    index_name, len(known_fields),
                )
            except Exception as exc:
                if "resource_already_exists" not in str(exc):
                    raise
                logger.warning(
                    "ItemsElasticsearchDriver.ensure_storage: create('%s') "
                    "lost race to a concurrent create — proceeding",
                    index_name,
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
    # Generic Indexer Protocol — slim, dispatcher-facing surface
    # ------------------------------------------------------------------

    async def ensure_indexer(self, ctx) -> None:
        """Idempotent bootstrap — creates the per-tenant items index
        ``{prefix}-items-{catalog_id}`` with ``ITEM_MAPPING`` if missing
        and enrols it in the platform alias ``{prefix}-items-public``.

        Delegates to :meth:`ensure_storage` so a single code path
        handles both per-collection-creation eager bootstrap and the
        dispatcher's lazy first-write check.
        """
        await self.ensure_storage(ctx.catalog, ctx.collection)

    async def index(self, ctx, op) -> None:
        """Apply a single :class:`IndexOp` to the per-tenant items index.

        Called by :class:`IndexDispatcher` from inside the caller's PG
        transaction.  Failure raises; the dispatcher applies the
        configured ``FailurePolicy`` (FATAL → caller rollback, OUTBOX
        → enqueue retry row in same TX, WARN → log).

        No ``_is_secondary_for`` / ``_is_write_driver_for`` guards: the
        dispatcher only invokes drivers pinned as secondary-index ``WRITE``
        entries (``secondary_index=True``) in ``operations[WRITE]`` for this
        ``(catalog, collection)`` — guard is moved out of the driver into the
        routing layer.
        """
        if op.entity_type != "item":
            # Items driver only handles item-tier ops.  Non-item routes
            # land on a different Indexer registered for that tier.
            return
        if not ctx.collection:
            raise ValueError(
                "ItemsElasticsearchDriver.index: collection is required for item ops",
            )

        es = _es_client_required()
        index_name = _tenant_items_index(ctx.catalog)
        await _ensure_in_public_alias_once(ctx.catalog, index_name)

        if op.op_type == "delete":
            await es.delete(
                index=index_name, id=op.entity_id,
                params={"routing": ctx.collection, "ignore": "404"},
            )
            return

        # op_type == "upsert"
        doc = op.payload or await self._serialize_item(
            ctx.catalog, ctx.collection, op.entity_id,
        )
        if doc is None:
            # Nothing serialisable — skip without raising; this is the
            # "row vanished between write and index" race, not a failure.
            logger.debug(
                "ItemsElasticsearchDriver.index: %s/%s/%s — no doc to index",
                ctx.catalog, ctx.collection, op.entity_id,
            )
            return
        doc.setdefault("id", op.entity_id)
        doc.setdefault("collection", ctx.collection)
        from dynastore.modules.elasticsearch.items_projection import (
            project_item_for_es,
            resolve_catalog_known_fields,
        )
        doc = _ensure_localized_object_shape(doc)
        doc = project_item_for_es(doc, await resolve_catalog_known_fields(ctx.catalog))
        await es.index(
            index=index_name, id=op.entity_id, body=doc,
            params={"routing": ctx.collection},
        )

    async def index_bulk(self, ctx, ops):
        """Apply a batch of :class:`IndexOp` via the ES ``_bulk`` API.

        Returns a :class:`BulkResult` summarising success / failure
        per-op.  An unhandled exception (auth, connection) raises; the
        dispatcher applies the configured ``FailurePolicy`` to the whole
        batch.
        """
        from dynastore.models.protocols.indexer import BulkResult

        if not ops:
            return BulkResult()
        if not ctx.collection:
            raise ValueError(
                "ItemsElasticsearchDriver.index_bulk: collection is required for item ops",
            )

        from dynastore.modules.elasticsearch.items_projection import (
            project_item_for_es,
            resolve_catalog_known_fields,
        )

        es = _es_client_required()
        index_name = _tenant_items_index(ctx.catalog)
        await _ensure_in_public_alias_once(ctx.catalog, index_name)
        known_fields = await resolve_catalog_known_fields(ctx.catalog)

        body: List[dict] = []
        for op in ops:
            if op.entity_type != "item":
                continue
            if op.op_type == "delete":
                body.append({"delete": {
                    "_index": index_name, "_id": op.entity_id,
                    "routing": ctx.collection,
                }})
                continue
            doc = op.payload or await self._serialize_item(
                ctx.catalog, ctx.collection, op.entity_id,
            )
            if doc is None:
                continue
            doc.setdefault("id", op.entity_id)
            doc.setdefault("collection", ctx.collection)
            doc.setdefault("catalog_id", ctx.catalog)
            doc = _ensure_localized_object_shape(doc)
            doc = project_item_for_es(doc, known_fields)
            body.append({"index": {
                "_index": index_name, "_id": op.entity_id,
                "routing": ctx.collection,
            }})
            body.append(doc)

        if not body:
            return BulkResult(total=len(ops))

        resp = await es.bulk(body=body, params={"refresh": "false"})
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
        # #914 — when the parsed result is a silent no-op (succeeded=0 with
        # no per-item failures), log the raw response shape so operators
        # can tell ``items=[]`` (request never hit ES) from a shape we
        # don't parse.
        if succeeded == 0 and not failures and len(ops) > 0:
            logger.warning(
                "ItemsElasticsearchDriver.index_bulk: ES bulk returned a "
                "shape that yielded 0 succeeded / 0 failed for %d ops "
                "(catalog=%s collection=%s index=%s). resp_type=%s "
                "resp_keys=%s items_len=%d errors=%s",
                len(ops), ctx.catalog, ctx.collection, index_name,
                type(resp).__name__,
                list(resp.keys()) if isinstance(resp, dict) else None,
                len(items),
                resp.get("errors") if isinstance(resp, dict) else None,
            )
        return BulkResult(
            total=len(ops),
            succeeded=succeeded,
            failed=len(failures),
            failures=failures,
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
            doc["catalog_id"] = catalog_id
            return doc
        except Exception as e:
            logger.warning("Failed to serialize item %s/%s/%s: %s",
                           catalog_id, collection_id, item_id, e)
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

    # ------------------------------------------------------------------
    # CollectionItemsStore Protocol — data-side ops
    # ------------------------------------------------------------------
    # All four delegate to the shared ``items_es_ops`` helpers (which
    # use the shared async ES client from
    # :mod:`dynastore.modules.elasticsearch.client`). The per-tenant
    # index is shared across all collections of a catalog; the routing
    # key is the collection_id.

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
        query = self._query_request_to_es(request) if request is not None else None
        return await es_count_items(
            es,
            _tenant_items_index(catalog_id),
            query=query,
            collection=collection_id,
            routing=collection_id,
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
            es,
            _tenant_items_index(catalog_id),
            collection=collection_id,
            routing=collection_id,
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
        query = self._query_request_to_es(request) if request is not None else None
        return await es_aggregate(
            es,
            _tenant_items_index(catalog_id),
            aggregation_type=aggregation_type,
            field=field,
            query=query,
            collection=collection_id,
            routing=collection_id,
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
        return await es_introspect_mapping(es, _tenant_items_index(catalog_id))

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
            "ItemsElasticsearchDriver: rename_storage is not supported. "
            "Renaming a collection on this backend would require a full "
            "reindex; perform the reindex explicitly via the "
            "elasticsearch_indexer process."
        )

    async def restore_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entity_ids: List[str],
        *,
        db_resource: Optional[Any] = None,
    ) -> int:
        from dynastore.modules.storage.errors import SoftDeleteNotSupportedError

        raise SoftDeleteNotSupportedError(
            "ItemsElasticsearchDriver: restore_entities is not implemented; "
            "soft-deleted entities are not tracked separately on this "
            "backend (deletes are physical removals from the index)."
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
    ``AssetIndexer`` secondary-index ``WRITE`` entry (``secondary_index=True``)
    in ``AssetRoutingConfig.operations[WRITE]`` —
    invoked by ``AssetService``'s secondary-driver fan-out — and through
    direct programmatic calls to ``index_asset()`` / ``delete_asset()``.

    Registered as ``storage_elasticsearch_assets`` via entry points.
    """

    is_asset_indexer: ClassVar[bool] = True

    # Asset ES is the canonical async secondary index + primary SEARCH
    # backend for asset metadata routing.  It auto-defaults into WRITE (as a
    # secondary index, identified by ``is_asset_indexer``) and SEARCH.
    auto_register_for_routing: ClassVar[FrozenSet[str]] = frozenset({Operation.SEARCH, Operation.WRITE})

    priority: int = 52
    capabilities: FrozenSet[str] = frozenset({
        Capability.READ,
        Capability.WRITE,
        Capability.STREAMING,
        Capability.PHYSICAL_ADDRESSING,
    })
    preferred_for: FrozenSet[Hint] = frozenset({Hint.SEARCH, Hint.ASSETS})
    supported_hints: FrozenSet[Hint] = frozenset({Hint.SEARCH, Hint.ASSETS, Hint.FULLTEXT})

    def is_available(self) -> bool:
        try:
            from dynastore.modules.elasticsearch.client import get_client
        except (ImportError, ModuleNotFoundError):
            return False
        return get_client() is not None

    def _get_client(self):
        """Return the shared async ES client."""
        return _es_client_required()

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        # Asset writes flow exclusively through the AssetIndexer secondary-index
        # WRITE entry (secondary_index=True) in AssetRoutingConfig.operations[WRITE]
        # — invoked by AssetService's
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
        from dynastore.modules.elasticsearch.index_config import (
            get_assets_index_settings,
        )
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()

        if not await es.indices.exists(index=index_name):
            try:
                await es.indices.create(
                    index=index_name,
                    body={
                        "settings": await get_assets_index_settings(),
                        "mappings": ASSET_MAPPING,
                    },
                )
            except Exception as exc:
                if "resource_already_exists" not in str(exc):
                    raise

        asset_id = asset_doc.get("asset_id", asset_doc.get("id"))
        await es.index(index=index_name, id=asset_id, body=asset_doc)

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
    # Generic Indexer Protocol — slim, dispatcher-facing surface
    # ------------------------------------------------------------------

    async def ensure_indexer(self, ctx) -> None:
        """Idempotent bootstrap — creates ``{prefix}-assets-{catalog_id}``
        with ``ASSET_MAPPING`` if missing.  No alias today (assets are
        per-catalog only; no platform-wide assets alias).
        """
        await self.ensure_storage(ctx.catalog, ctx.collection)

    async def index(self, ctx, op) -> None:
        """Apply a single asset :class:`IndexOp` via the existing
        :meth:`index_asset` / :meth:`delete_asset` helpers.

        Skips ops whose ``entity_type`` is not ``"asset"`` — different
        tier; a different Indexer fields it.
        """
        if op.entity_type != "asset":
            return
        if op.op_type == "delete":
            await self.delete_asset(ctx.catalog, op.entity_id)
            return
        # upsert
        doc = dict(op.payload or {})
        doc.setdefault("asset_id", op.entity_id)
        doc.setdefault("catalog_id", ctx.catalog)
        if ctx.collection is not None:
            doc.setdefault("collection_id", ctx.collection)
        await self.index_asset(ctx.catalog, doc)

    async def index_bulk(self, ctx, ops):
        """Bulk-apply a batch of asset ops.

        Delegates per-op to :meth:`index` for now — asset writes are
        rare enough vs item writes that a single ES round-trip per op
        isn't a hot-path concern.  A native ``_bulk`` implementation
        can land later if profiling motivates it.
        """
        from dynastore.models.protocols.indexer import BulkResult

        succeeded = 0
        failures: List[Dict[str, Any]] = []
        for op in ops:
            if op.entity_type != "asset":
                continue
            try:
                await self.index(ctx, op)
                succeeded += 1
            except Exception as exc:  # noqa: BLE001 — surface per-op failures
                failures.append({"id": op.entity_id, "reason": str(exc)})
        return BulkResult(
            total=len(ops),
            succeeded=succeeded,
            failed=len(failures),
            failures=failures,
        )

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
        from dynastore.modules.elasticsearch.index_config import (
            get_assets_index_settings,
        )
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        items = self._normalize_entities(entities)
        es = self._get_client()

        if not await es.indices.exists(index=index_name):
            try:
                await es.indices.create(
                    index=index_name,
                    body={
                        "settings": await get_assets_index_settings(),
                        "mappings": ASSET_MAPPING,
                    },
                )
            except Exception as exc:
                if "resource_already_exists" not in str(exc):
                    raise

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
            await es.bulk(body=bulk_body)

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
        from dynastore.modules.elasticsearch.index_config import (
            get_assets_index_settings,
        )
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()
        if not await es.indices.exists(index=index_name):
            try:
                await es.indices.create(
                    index=index_name,
                    body={
                        "settings": await get_assets_index_settings(),
                        "mappings": ASSET_MAPPING,
                    },
                )
            except Exception as exc:
                if "resource_already_exists" not in str(exc):
                    raise

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
        await es.indices.delete(
            index=index_name, params={"ignore_unavailable": "true"},
        )

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

    async def search_assets(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        filters: Optional[List[AssetFilter]] = None,
        limit: int = 100,
        offset: int = 0,
        all_collections: bool = False,
        db_resource=None,
    ) -> List[Dict[str, Any]]:
        """Search asset documents in ES.

        ``filters`` is an optional list of :class:`AssetFilter`. The operator
        set and ES-clause translation live in
        :func:`dynastore.modules.tools.asset_filters.build_es_query`
        (shared with the PG driver so both backends honour the same operators).
        Dot-notation fields (``metadata.license_id``) resolve natively in ES.
        ``None``/empty → ``match_all``.

        Collection scope mirrors the PG asset driver (tri-state):
        - ``collection_id="<id>"`` → ``term`` filter on that collection.
        - ``collection_id=None`` and ``all_collections=False`` → catalog-tier
          assets only, via a ``must_not exists collection_id`` clause (a doc
          bound to no collection has the field absent/null). This keeps ES and
          PG scoping identical for the default catalog-tier search.
        - ``all_collections=True`` → no collection clause; spans every
          collection plus the catalog tier under the catalog.
        """
        from dynastore.modules.elasticsearch.mappings import get_assets_index_name
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()

        base_query = build_es_query(filters or [])
        if all_collections:
            pass  # no collection clause — span the whole catalog
        elif collection_id:
            base_query = {
                "bool": {
                    "must": [base_query],
                    "filter": [{"term": {"collection_id": collection_id}}],
                }
            }
        else:
            base_query = {
                "bool": {
                    "must": [base_query],
                    "must_not": [{"exists": {"field": "collection_id"}}],
                }
            }

        try:
            resp = await es.search(
                index=index_name,
                query=base_query,
                size=limit,
                from_=offset,
            )
            hits = [hit["_source"] for hit in resp["hits"]["hits"]]
            from dynastore.modules.storage.routing_config import (
                get_output_transformers_for_search,
            )
            from dynastore.modules.storage.transform_runtime import (
                restore_transform_chain,
            )
            from dynastore.tools.typed_store.base import _to_snake

            chain = await get_output_transformers_for_search(
                catalog_id,
                entity="asset",
                collection_id=collection_id,
                driver_ref=_to_snake(type(self).__name__),
            )
            if not chain:
                return hits
            restored: List[Dict[str, Any]] = []
            for hit in hits:
                restored.append(
                    await restore_transform_chain(
                        hit,
                        chain,
                        catalog_id=catalog_id,
                        collection_id=collection_id,
                        entity_kind="asset",
                    )
                )
            return restored
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


