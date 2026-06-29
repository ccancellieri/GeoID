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
ItemsElasticsearchEnvelopeDriver — standardized-envelope ES storage driver.

Writes the full feature (geometry + properties + identity) plus a canonical
*access envelope* (``visibility`` / ``owner``) into a
per-tenant index whose name is owned by this subpackage
(``{prefix}-{catalog_id}-envelope-items``). Driver-private mapping
(``ENVELOPE_FEATURE_MAPPING``) lives in :mod:`.mappings`.

The distinguishing capability of this driver is *row-level access scoping*:
every search ANDs a neutral :class:`AccessFilter` (compiled elsewhere and
arriving on ``QueryRequest.access_filter``) into its query body, translated to
ES Query DSL by the pure :func:`.access_translate.access_filter_to_es`. The
driver imports nothing from the IAM module — it consumes only the neutral
filter type — so authorization and storage stay decoupled.

Unlike the private driver, this driver does NOT manage DENY access policies;
document-level security is enforced by the query-time access filter, not by a
coarse catalog-wide URL DENY rule.

Registered as ``storage_elasticsearch_envelope`` via entry points.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, ClassVar, Dict, FrozenSet, List, Optional, Union

from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.protocols.typed_driver import TypedDriver
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.elasticsearch.items_query import (
    ENVELOPE_FIELDS,
    EnvelopeFields,
)
from dynastore.modules.protocols import ModuleProtocol
from dynastore.modules.storage.driver_config import (
    ItemsElasticsearchEnvelopeDriverConfig,
)
from dynastore.modules.storage.drivers.elasticsearch import (
    _ItemsElasticsearchBase,
    _stamp_simplification,
)
from dynastore.modules.storage.errors import SoftDeleteNotSupportedError
from dynastore.modules.storage.hints import Hint

logger = logging.getLogger(__name__)


class ItemsElasticsearchEnvelopeDriver(
    TypedDriver[ItemsElasticsearchEnvelopeDriverConfig],
    _ItemsElasticsearchBase,
    ModuleProtocol,
):
    """Standardized-envelope Elasticsearch storage driver with row-level ABAC.

    Writes the full feature plus a canonical access envelope into a per-tenant
    index ``{prefix}-{catalog_id}-envelope-items`` shared across all
    collections of the catalog. The mapping is ``ENVELOPE_FEATURE_MAPPING``
    (root ``dynamic: false`` to reject smuggled fields; the access fields
    ``visibility`` / ``owner`` are typed root keywords so
    the row-level filter is reliable; ``properties.*`` stays dynamic so tenant
    attributes index without mapping churn).

    Every search is access-scoped: :meth:`_query_request_to_es` ANDs the
    translated ``QueryRequest.access_filter`` into the query body, so this
    driver can never fall back to an unfiltered scan when a filter is present.

    Uses the shared async ES client from
    :mod:`dynastore.modules.elasticsearch.client`.

    Registered as ``storage_elasticsearch_envelope`` via entry points.
    """

    is_item_indexer: ClassVar[bool] = True

    # Opt in to document-level read scoping (#1285). The search dispatch reads
    # this marker, and only when it is ``True`` does it compile the caller's
    # ``compile_read_filter`` and place the resulting neutral ``AccessFilter``
    # on the request so this driver's ``_query_request_to_es`` ANDs it in. All
    # other drivers keep the base default ``False`` and are unaffected.
    applies_access_filter: ClassVar[bool] = True

    # Opt out of items-tier auto-default routing. This driver is the
    # standardized row-level-scoped variant; it must only run for collections
    # whose ``ItemsRoutingConfig`` explicitly pins it (mirrors the private
    # driver's explicit-pin policy). Auto-injecting it into every collection's
    # WRITE / SEARCH entries would silently change every search's semantics to
    # access-scoped without operator intent.
    auto_register_for_routing: ClassVar[FrozenSet[str]] = frozenset()

    priority: int = 52
    preferred_chunk_size: int = 500
    capabilities: FrozenSet[str] = frozenset({
        Capability.READ,
        Capability.WRITE,
        Capability.STREAMING,
        Capability.PHYSICAL_ADDRESSING,
        Capability.INTROSPECTION,
        Capability.TENANT_ISOLATED,
        # Tracks ``external_id`` as a typed root keyword on every doc.
        Capability.EXTERNAL_ID_TRACKING,
    })
    preferred_for: FrozenSet[Hint] = frozenset()
    supported_hints: FrozenSet[Hint] = frozenset({
        Hint.SEARCH,
        Hint.FULLTEXT,
        Hint.GEOMETRY_SIMPLIFIED,
        Hint.SPATIAL_FILTER,
        Hint.ATTRIBUTE_FILTER,
        Hint.SORT,
        Hint.AGGREGATION,
        Hint.COUNT,
        Hint.STATISTICS,
    })

    # ``is_available`` / ``_get_client`` inherited from ``_ElasticsearchBase``.

    # The envelope doc carries the canonical names (``collection_id`` /
    # ``geoid`` / ``external_id``) at the root, so structural queries built by
    # the shared SSOT must address that shape.
    _envelope_fields: ClassVar[EnvelopeFields] = ENVELOPE_FIELDS

    # Config class resolved by :meth:`_ItemsElasticsearchBase._resolve_simplify_geometry`.
    _driver_config_class: ClassVar[Any] = ItemsElasticsearchEnvelopeDriverConfig

    # Backend label for StorageLocation (used by inherited location() method).
    _location_backend: ClassVar[str] = "elasticsearch_envelope"

    def _items_index_name(self, catalog_id: str) -> str:
        """Per-tenant envelope index ``{prefix}-{catalog_id}-envelope-items``.

        The single index-name seam every CRUD + data-side op routes through.
        """
        from dynastore.modules.elasticsearch.client import get_index_prefix
        from dynastore.modules.storage.drivers.elasticsearch_envelope.mappings import (
            get_envelope_index_name,
        )
        return get_envelope_index_name(get_index_prefix(), catalog_id)

    def _collection_routing(self, collection_id: Optional[str]) -> Optional[str]:
        """The envelope index is not sharded by collection — no ``_routing``."""
        return None

    # ------------------------------------------------------------------
    # Row-level access seam
    # ------------------------------------------------------------------

    def _query_request_to_es(  # type: ignore[override]
        self,
        request: QueryRequest,
        fields: EnvelopeFields = ENVELOPE_FIELDS,
    ) -> dict:
        """Build the ES query body, then AND the row-level access filter in.

        Delegates the structural + CQL2 translation to the shared base, then
        translates the neutral :class:`AccessFilter` on ``request.access_filter``
        into an ES clause and AND-s it into the query via the shared merge
        helper. This is THE seam that guarantees every search through this
        driver is access-scoped: a ``deny_all`` filter translates to
        ``{"match_none": {}}`` which, merged into the body, returns nothing.

        Fail-closed by construction: a ``request`` with NO ``access_filter`` set
        means no caller established a read scope for this access-controlled
        index, so the query returns NOTHING rather than leaking unfiltered rows.
        Any search entry point that forgets to compile a filter therefore
        under-returns (safe) instead of leaking. Trusted server-side callers
        that legitimately need an unrestricted read MUST opt in explicitly by
        setting ``request.access_filter = AccessFilter.allow_everything()``.
        """
        from dynastore.modules.storage.drivers.elasticsearch_envelope.access_translate import (
            access_filter_to_es,
        )
        from dynastore.modules.storage.drivers.es_common import merge_es_filter

        base = super()._query_request_to_es(request, fields)
        access_filter = getattr(request, "access_filter", None)
        if access_filter is None:
            # No read scope was established → deny by default (no leak).
            return {"query": {"match_none": {}}}

        clause = access_filter_to_es(access_filter)
        if clause is None:
            # ``allow_all`` with no deny — an explicit, trusted unrestricted
            # read: no row-level restriction to apply.
            return base

        inner = base.get("query", {"match_all": {}})
        merged = merge_es_filter(inner, clause)
        return {"query": merged}

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        """No-op lifecycle.

        Item-tier propagation runs through the IndexDispatcher (the dispatcher
        resolves this driver via the slim :class:`Indexer` Protocol and the
        routing config), so no event listeners are registered here. Unlike the
        private driver, document-level security is enforced at query time via
        the access filter, so there is no DENY-policy state to restore at
        startup. Discovery happens via the entry-point registration (the
        framework instantiates the module and registers it), exactly like the
        public and private ES item drivers — neither self-registers here.
        """
        yield

    # ------------------------------------------------------------------
    # StorageDriverProtocol
    # ------------------------------------------------------------------

    def _build_doc(
        self,
        item: Any,
        *,
        catalog_id: str,
        collection_id: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Build an envelope doc, sourcing access fields from ``context`` first.

        Access/identity fields are read from the write ``context`` dict (the
        ingestion-context surface) when present, falling back inside
        :func:`build_envelope_feature_doc` to dispatcher-stamped
        ``_``-prefixed source keys.
        """
        from dynastore.modules.storage.drivers.elasticsearch_envelope.doc_builder import (
            build_envelope_feature_doc,
        )

        ctx = context or {}
        return build_envelope_feature_doc(
            item,
            catalog_id=catalog_id,
            collection_id=collection_id,
            external_id=ctx.get("external_id"),
            asset_id=ctx.get("asset_id"),
            visibility=ctx.get("visibility"),
            owner=ctx.get("owner"),
            attrs=ctx.get("attrs"),
        )

    async def write_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entities: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]]],
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> List[Feature]:
        from dynastore.modules.storage.drivers.elasticsearch_envelope.mappings import (
            ENVELOPE_FEATURE_MAPPING,
        )
        from dynastore.modules.elasticsearch.index_config import (
            get_private_items_index_settings,
        )
        from dynastore.tools.geometry_simplify import maybe_simplify_for_es

        index_name = self._items_index_name(catalog_id)
        items = self._normalize_entities(entities)
        es = self._get_client()

        simplify_geometry = await self._resolve_simplify_geometry(
            catalog_id, collection_id, db_resource=db_resource,
        )
        simplify_max_bytes = await self._resolve_simplify_max_bytes(
            catalog_id, collection_id, db_resource=db_resource,
        )
        snap_to_grid, snap_grid_size = await self._resolve_snap_to_grid_config(
            catalog_id, collection_id, db_resource=db_resource,
        )

        await self._ensure_index(es, index_name, ENVELOPE_FEATURE_MAPPING,
                                 get_private_items_index_settings)

        bulk_body: list = []
        submitted_ids: list = []
        skipped_no_id = 0
        for item in items:
            geoid = self._extract_item_id(item)
            if not geoid:
                skipped_no_id += 1
                continue
            doc = self._build_doc(
                item, catalog_id=catalog_id, collection_id=collection_id,
                context=context,
            )
            doc, factor, mode = maybe_simplify_for_es(
                doc, simplify=simplify_geometry, max_bytes=simplify_max_bytes,
                snap_to_grid=snap_to_grid, snap_grid_size=snap_grid_size,
            )
            _stamp_simplification(doc, factor, mode)
            bulk_body.append({"index": {"_index": index_name, "_id": geoid}})
            bulk_body.append(doc)
            submitted_ids.append(geoid)

        if skipped_no_id:
            logger.error(
                "ItemsElasticsearchEnvelopeDriver.write_entities: skipped %d item(s) "
                "with no id in catalog=%s collection=%s — these will NOT be indexed.",
                skipped_no_id, catalog_id, collection_id,
            )

        if bulk_body:
            from dynastore.modules.elasticsearch._mapping_errors import (
                maybe_raise_bulk_mapping_mismatch,
                raise_on_bulk_errors,
            )
            resp = await es.bulk(body=bulk_body)
            maybe_raise_bulk_mapping_mismatch(resp, index_name)
            raise_on_bulk_errors(resp, index_name, submitted_ids)

        return items if isinstance(items, list) else list(items)

    # _ensure_index is inherited from _ItemsElasticsearchBase.

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

        index_name = self._items_index_name(catalog_id)
        es = self._get_client()

        if not await es.indices.exists(index=index_name):
            return

        # Resolve the output-transformer chain once per query (empty → no-op).
        restore_chain = await get_output_transformers_for_search(
            catalog_id,
            entity="item",
            collection_id=collection_id,
            driver_ref=_to_snake(type(self).__name__),
        )
        # One restore context per query — shared cache across the page (#1568).
        restore_ctx = TransformChainContext()

        # --- by-id lookup (geoid token retrieval) -------------------------
        if entity_ids:
            for geoid in entity_ids:
                try:
                    resp = await es.get(index=index_name, id=geoid)
                    feature = self._envelope_source_to_feature(
                        resp["_source"], catalog_id, collection_id, geoid,
                    )
                    if restore_chain:
                        feature = await restore_transform_chain(
                            feature,
                            restore_chain,
                            catalog_id=catalog_id,
                            collection_id=collection_id,
                            entity_kind="item",
                            ctx=restore_ctx,
                        )
                    yield feature
                except Exception:
                    pass
            return

        # --- structural search (routed via Operation.SEARCH) --------------
        # The envelope index is not sharded by collection, so reuse the shared
        # ``_build_read_search_body``. Passing ``self._envelope_fields`` scopes
        # via the canonical ``collection_id`` term, folds any pre-translated
        # CQL2->ES ``request.es_filter`` in, and — via the overridden
        # ``_query_request_to_es`` — ANDs the row-level access filter.
        if request is None:
            return
        body, params = self._build_read_search_body(
            collection_id, request, limit, offset, self._envelope_fields,
        )
        params.pop("routing", None)  # envelope index is not collection-routed
        try:
            resp = await es.search(index=index_name, body=body, params=params)
        except Exception as e:
            logger.warning(
                "ItemsElasticsearchEnvelopeDriver: search failed for %s/%s: %s",
                catalog_id, collection_id, e,
            )
            return
        for hit in resp.get("hits", {}).get("hits", []):
            try:
                src = hit["_source"]
                feature = self._envelope_source_to_feature(
                    src, catalog_id, collection_id, src.get("geoid"),
                )
                if restore_chain:
                    feature = await restore_transform_chain(
                        feature,
                        restore_chain,
                        catalog_id=catalog_id,
                        collection_id=collection_id,
                        entity_kind="item",
                        ctx=restore_ctx,
                    )
                yield feature
            except Exception:
                pass

    @staticmethod
    def _envelope_source_to_feature(
        source: Dict[str, Any],
        catalog_id: str,
        collection_id: str,
        fallback_id: Any,
        lang: Optional[str] = None,
    ) -> Feature:
        """Reconstruct a :class:`Feature` from an envelope doc ``_source``.

        Surfaces feature bookkeeping (``external_id`` and simplification
        markers) in ``properties`` so callers can detect simplification without
        a separate API.

        Simplification is read from the canonical path
        ``source["system"]["geometry_simplification"]`` with a back-compat
        fallback to the old flat root keys (``simplification_factor`` /
        ``simplification_mode``) for docs written by an older driver version.

        When ``source["metadata"]`` is present (multilingual title/description/
        keywords), each field is resolved to the requested language (``lang``,
        defaulting to ``"en"`` when absent) via
        :func:`dynastore.tools.language_utils.resolve_localized_field` and
        surfaced in ``properties``.

        The access-envelope fields (``visibility`` / ``owner`` / ``attrs``)
        are deliberately NOT surfaced into the read contract — they are
        internal authorization metadata, not feature attributes.
        """
        from dynastore.tools.language_utils import resolve_localized_field

        effective_lang = lang or "en"
        props = dict(source.get("properties") or {})
        if "external_id" in source:
            props["external_id"] = source["external_id"]

        # Simplification: canonical path first, flat fallback for old docs.
        system = source.get("system") or {}
        geo_simp = system.get("geometry_simplification")
        if isinstance(geo_simp, dict):
            props["simplification_factor"] = geo_simp.get("factor")
            props["simplification_mode"] = geo_simp.get("mode")
        else:
            # Back-compat: docs written before the canonical system container.
            if "simplification_factor" in source:
                props["simplification_factor"] = source["simplification_factor"]
            if "simplification_mode" in source:
                props["simplification_mode"] = source["simplification_mode"]

        # Multilingual metadata: resolve each field to the requested language.
        metadata = source.get("metadata")
        if isinstance(metadata, dict):
            for key in ("title", "description", "keywords"):
                raw = metadata.get(key)
                if raw is not None:
                    resolved = resolve_localized_field(raw, effective_lang)
                    if resolved is not None:
                        props.setdefault(key, resolved)

        props["catalog_id"] = source.get("catalog_id", catalog_id)
        props["collection_id"] = source.get("collection_id", collection_id)
        return Feature(
            type="Feature",
            id=source.get("geoid", fallback_id),
            geometry=source.get("geometry"),
            properties=props,
            bbox=source.get("bbox"),  # type: ignore[call-arg]
        )

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        from dynastore.modules.storage.drivers.elasticsearch_envelope.mappings import (
            ENVELOPE_FEATURE_MAPPING,
        )
        from dynastore.modules.elasticsearch.index_config import (
            get_private_items_index_settings,
        )

        index_name = self._items_index_name(catalog_id)
        es = self._get_client()
        await self._ensure_index(es, index_name, ENVELOPE_FEATURE_MAPPING,
                                 get_private_items_index_settings)

    async def drop_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        soft: bool = False,
    ) -> None:
        if soft:
            raise SoftDeleteNotSupportedError(
                "ItemsElasticsearchEnvelopeDriver does not support soft drop."
            )

        index_name = self._items_index_name(catalog_id)
        es = self._get_client()
        await es.indices.delete(
            index=index_name, params={"ignore_unavailable": "true"},
        )

    # ------------------------------------------------------------------
    # Generic Indexer Protocol — slim, dispatcher-facing surface
    # ------------------------------------------------------------------
    # export_entities and ensure_indexer are inherited from
    # _ItemsElasticsearchBase.

    async def index(self, ctx, op) -> None:
        """Apply a single item :class:`IndexOp` to the envelope index.

        When called from the ``IndexDispatcher``, ``op.payload`` carries the
        full STAC item (with any dispatcher-stamped ``_visibility`` /
        ``_owner`` access keys); the driver builds the
        envelope doc and shrinks oversized geometries before indexing.
        """
        if op.entity_type != "item":
            return
        if not ctx.collection:
            raise ValueError(
                "ItemsElasticsearchEnvelopeDriver.index: collection required for item ops",
            )

        from dynastore.modules.storage.drivers.elasticsearch_envelope.mappings import (
            ENVELOPE_FEATURE_MAPPING,
        )
        from dynastore.modules.elasticsearch.index_config import (
            get_private_items_index_settings,
        )

        index_name = self._items_index_name(ctx.catalog)
        es = self._get_client()

        if op.op_type == "delete":
            try:
                await es.delete(index=index_name, id=op.entity_id)
            except Exception:
                pass
            return

        # upsert
        from dynastore.tools.geometry_simplify import maybe_simplify_for_es

        await self._ensure_index(es, index_name, ENVELOPE_FEATURE_MAPPING,
                                 get_private_items_index_settings)

        src = op.payload or {"id": op.entity_id}
        src.setdefault("id", op.entity_id)
        doc = self._build_doc(
            src, catalog_id=ctx.catalog, collection_id=ctx.collection,
        )
        simplify_geometry = await self._resolve_simplify_geometry(
            ctx.catalog, ctx.collection,
        )
        simplify_max_bytes = await self._resolve_simplify_max_bytes(
            ctx.catalog, ctx.collection,
        )
        snap_to_grid, snap_grid_size = await self._resolve_snap_to_grid_config(
            ctx.catalog, ctx.collection,
        )
        doc, factor, mode = maybe_simplify_for_es(
            doc, simplify=simplify_geometry, max_bytes=simplify_max_bytes,
            snap_to_grid=snap_to_grid, snap_grid_size=snap_grid_size,
        )
        _stamp_simplification(doc, factor, mode)
        await es.index(index=index_name, id=op.entity_id, body=doc)

    async def index_bulk(self, ctx, ops):
        """Bulk-apply a batch of item ops via the ES ``_bulk`` API."""
        from dynastore.models.protocols.indexer import BulkResult
        from dynastore.modules.storage.drivers.elasticsearch_envelope.mappings import (
            ENVELOPE_FEATURE_MAPPING,
        )
        from dynastore.modules.elasticsearch.index_config import (
            get_private_items_index_settings,
        )
        from dynastore.tools.geometry_simplify import maybe_simplify_for_es

        if not ops:
            return BulkResult()
        if not ctx.collection:
            raise ValueError(
                "ItemsElasticsearchEnvelopeDriver.index_bulk: collection required for item ops",
            )

        simplify_geometry = await self._resolve_simplify_geometry(
            ctx.catalog, ctx.collection,
        )
        simplify_max_bytes = await self._resolve_simplify_max_bytes(
            ctx.catalog, ctx.collection,
        )
        snap_to_grid, snap_grid_size = await self._resolve_snap_to_grid_config(
            ctx.catalog, ctx.collection,
        )

        index_name = self._items_index_name(ctx.catalog)
        es = self._get_client()

        await self._ensure_index(es, index_name, ENVELOPE_FEATURE_MAPPING,
                                 get_private_items_index_settings)

        body: List[dict] = []
        for op in ops:
            if op.entity_type != "item":
                continue
            if op.op_type == "delete":
                body.append({"delete": {"_index": index_name, "_id": op.entity_id}})
                continue
            src = op.payload or {"id": op.entity_id}
            src.setdefault("id", op.entity_id)
            doc = self._build_doc(
                src, catalog_id=ctx.catalog, collection_id=ctx.collection,
            )
            doc, factor, mode = maybe_simplify_for_es(
                doc, simplify=simplify_geometry, max_bytes=simplify_max_bytes,
                snap_to_grid=snap_to_grid, snap_grid_size=snap_grid_size,
            )
            _stamp_simplification(doc, factor, mode)
            body.append({"index": {"_index": index_name, "_id": op.entity_id}})
            body.append(doc)

        if not body:
            return BulkResult(total=len(ops))

        resp = await es.bulk(body=body)
        succeeded, failures = self._tally_bulk_response(
            resp, len(ops),
            driver_name="ItemsElasticsearchEnvelopeDriver",
            catalog=ctx.catalog,
            collection=ctx.collection,
            index_name=index_name,
        )
        return BulkResult(
            total=len(ops),
            succeeded=succeeded,
            failed=len(failures),
            failures=failures,
        )

    # ``location``, ``get_entity_fields``, ``export_entities``,
    # ``rename_storage``, ``restore_entities`` are inherited from
    # :class:`_ItemsElasticsearchBase`.  ``_location_backend`` above supplies
    # the correct backend label for ``location()``.
    #
    # CollectionItemsStore Protocol — data-side ops:
    # ``count_entities`` / ``compute_extents`` / ``aggregate`` /
    # ``introspect_schema`` are inherited from :class:`_ItemsElasticsearchBase`.
    # The envelope index is not sharded by collection, so the
    # :meth:`_collection_routing` override (returns ``None``) keeps the
    # inherited ops from sending a ``_routing`` param. ``count``/``aggregate``
    # route their query through the overridden ``_query_request_to_es``, so the
    # row-level access filter applies to those data-side ops too.
