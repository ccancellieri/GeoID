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
ItemsElasticsearchPrivateDriver — tenant-scoped ES storage driver.

Writes the full feature (geometry + properties + external_id) into a
per-tenant index whose name is owned by this subpackage
(``{prefix}-{catalog_id}-private-items``). The mapping is built by
:func:`~dynastore.modules.storage.drivers.elasticsearch_private.mappings.build_private_item_mapping`
from the tenant-scoped operator overlay
(:attr:`~dynastore.modules.storage.driver_config.ItemsElasticsearchPrivateDriverConfig.mapping`,
#1295 slice 3); an empty overlay keeps the legacy fully-dynamic shape
(:data:`~dynastore.modules.storage.drivers.elasticsearch_private.mappings.TENANT_FEATURE_MAPPING`).

Manages DENY access policies in its own lifecycle. Inherits the shared
:class:`_ItemsElasticsearchBase` surface (structural search, the index-name
seam, and the data-side count/extents/aggregate/introspect ops) with the
public items driver.

Registered as ``storage_elasticsearch_private`` via entry points.
"""

from __future__ import annotations

import logging
import re
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, ClassVar, Dict, FrozenSet, List, Optional, Union

from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.protocols.typed_driver import TypedDriver
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.elasticsearch.items_query import (
    PRIVATE_ENVELOPE_FIELDS,
    EnvelopeFields,
)
from dynastore.modules.protocols import ModuleProtocol
from dynastore.modules.storage.driver_config import (
    ItemsElasticsearchPrivateDriverConfig,
)
from dynastore.modules.storage.drivers.elasticsearch import (
    _ItemsElasticsearchBase,
    _stamp_simplification,
)
from dynastore.modules.storage.errors import SoftDeleteNotSupportedError
from dynastore.modules.storage.hints import Hint

logger = logging.getLogger(__name__)


class ItemsElasticsearchPrivateDriver(
    TypedDriver[ItemsElasticsearchPrivateDriverConfig],
    _ItemsElasticsearchBase,
    ModuleProtocol,
):
    """Tenant-scoped Elasticsearch storage driver (a.k.a. "private").

    Writes the full feature (geometry + properties + external_id) into a
    per-tenant index ``{prefix}-{catalog_id}-private-items`` shared across all
    collections of the catalog. The mapping is built by
    :func:`build_private_item_mapping` from the tenant-scoped operator
    overlay (#1295 slice 3): an empty overlay keeps the legacy fully-dynamic
    ``properties.*`` sub-tree (``TENANT_FEATURE_MAPPING``); a non-empty
    overlay produces a strict typed mapping for declared keys plus a
    cap-safe ``properties.extras`` (``flattened``) + root ``_search_text``
    lane for undeclared keys.

    Docs that would exceed the ES 10MB per-doc limit are shrunk by
    `simplify_to_fit` (`tools/geometry_simplify.py`); the resulting
    `simplification_factor` and `simplification_mode` are persisted on
    the doc so clients can tell how much fidelity was lost.

    Search is gated by the tenant-first contract enforced in
    `extensions/search` (catalog_id required; geoid OR (external_id +
    collection_id) — never external_id alone).

    Manages DENY access policies in its own lifecycle.

    Uses the shared async ES client from
    :mod:`dynastore.modules.elasticsearch.client`; the per-tenant index
    has a custom mapping owned by this subpackage.

    Registered as ``storage_elasticsearch_private`` via entry points.
    """

    index_tiers: ClassVar[FrozenSet[str]] = frozenset({"item"})

    # Opt out of items-tier auto-default routing.  The private variant is
    # tenant-isolated DENY-policy indexing; it must only run for collections
    # whose ``ItemsRoutingConfig`` explicitly pins this driver (#733).
    # The privacy-cascade validator on the routing configs guarantees the
    # items-private driver is pinned whenever the sibling
    # ``CollectionRoutingConfig`` pins the collection-private driver.
    # Auto-injecting into every collection's secondary-index WRITE / SEARCH
    # entries would silently bypass that gate.
    auto_register_for_routing: ClassVar[FrozenSet[str]] = frozenset()

    priority: int = 51
    preferred_chunk_size: int = 500
    capabilities: FrozenSet[str] = frozenset({
        Capability.READ,
        Capability.WRITE,
        Capability.STREAMING,
        Capability.PHYSICAL_ADDRESSING,
        Capability.INTROSPECTION,
        Capability.TENANT_ISOLATED,
    })
    preferred_for: FrozenSet[Hint] = frozenset()
    supported_hints: FrozenSet[Hint] = frozenset({
        Hint.SEARCH,
        Hint.FULLTEXT,
        # The private driver shrinks oversized docs via ``simplify_to_fit``
        # (tools/geometry_simplify.py) and persists the resulting
        # ``simplification_factor`` — it serves the simplified-geometry
        # read path exactly like the public ES sibling, which also lists
        # this hint.  #738: the omission silently failed routing-config
        # PUTs that pinned this driver with a ``geometry_simplified`` hint.
        Hint.GEOMETRY_SIMPLIFIED,
        Hint.SPATIAL_FILTER,
        Hint.ATTRIBUTE_FILTER,
        Hint.SORT,
        Hint.AGGREGATION,
        Hint.COUNT,
        Hint.STATISTICS,
    })

    # ``is_available`` / ``_get_client`` inherited from ``_ElasticsearchBase``.

    # The private doc carries the canonical envelope names (``collection_id`` /
    # ``geoid`` / ``external_id``) instead of the public STAC-flavoured ones, so
    # structural queries built by the shared SSOT must address that shape.
    _envelope_fields: ClassVar[EnvelopeFields] = PRIVATE_ENVELOPE_FIELDS

    # Config class resolved by :meth:`_ItemsElasticsearchBase._resolve_simplify_geometry`.
    _driver_config_class: ClassVar[Any] = ItemsElasticsearchPrivateDriverConfig

    # Backend label for StorageLocation (used by inherited location() method).
    _location_backend: ClassVar[str] = "elasticsearch_private"

    def _items_index_name(self, catalog_id: str) -> str:
        """Private per-tenant index ``{prefix}-{catalog_id}-private-items``.

        The single index-name seam every CRUD + data-side op routes through.
        """
        from dynastore.modules.elasticsearch.client import get_index_prefix
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            get_private_index_name,
        )
        return get_private_index_name(get_index_prefix(), catalog_id)

    def _collection_routing(self, collection_id: Optional[str]) -> Optional[str]:
        """The private index is not sharded by collection — no ``_routing``."""
        return None

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        """Restore DENY policies; item-tier propagation runs through the
        IndexDispatcher now (Phase 2d).  No event listeners registered
        on this driver — the dispatcher resolves it via the slim
        :class:`Indexer` Protocol and routing config.

        The restore scan is skipped in ephemeral job contexts (Cloud Run Jobs,
        local dev, CI) because there is no prior in-memory DENY state to
        recover — the scan only makes sense after a long-running service
        restart.  Detection uses ``K_SERVICE``, which Cloud Run Services
        always set and Cloud Run Jobs never do.
        """
        from dynastore.tools.env import is_running_as_job
        if is_running_as_job():
            logger.debug(
                "PrivateDriver: skipping _restore_deny_policies() — "
                "ephemeral job context has no prior in-memory DENY state to recover."
            )
        else:
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
        from dynastore.modules.storage.drivers.elasticsearch_private.doc_builder import (
            build_tenant_feature_doc,
        )
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            build_private_item_mapping,
            project_private_doc,
            resolve_catalog_private_known_fields,
        )
        from dynastore.modules.elasticsearch.index_config import (
            get_private_items_index_settings,
        )
        from dynastore.tools.geometry_simplify import maybe_simplify_for_es

        index_name = self._items_index_name(catalog_id)
        items = self._normalize_entities(entities)
        es = self._get_client()

        # #1248: simplify geometry by default — exact geometry is the explicit
        # opt-out via ``simplify_geometry: false`` in the driver config.
        simplify_geometry = await self._resolve_simplify_geometry(
            catalog_id, collection_id, db_resource=db_resource,
        )
        simplify_max_bytes = await self._resolve_simplify_max_bytes(
            catalog_id, collection_id, db_resource=db_resource,
        )
        snap_to_grid, snap_grid_size = await self._resolve_snap_to_grid_config(
            catalog_id, collection_id, db_resource=db_resource,
        )

        # Tenant-scoped manual mapping overlay (#1295 slice 3). Empty
        # overlay → legacy fully-dynamic ``properties`` sub-tree.
        known_fields = await resolve_catalog_private_known_fields(catalog_id)

        # Ingestion-context asset identity (mirrors the public driver's
        # ``_asset_id`` tracking field) — projected onto the private doc as
        # a top-level ``asset_id`` keyword for the WRITE path.
        ctx = context or {}
        asset_id = ctx.get("asset_id")

        await self._ensure_index(
            es, index_name,
            build_private_item_mapping(known_fields),
            get_private_items_index_settings,
        )

        bulk_body: list = []
        submitted_ids: list = []
        skipped_no_id = 0
        for item in items:
            geoid = self._extract_item_id(item)
            if not geoid:
                skipped_no_id += 1
                continue
            doc = build_tenant_feature_doc(
                item, catalog_id=catalog_id, collection_id=collection_id,
                asset_id=asset_id,
            )
            doc, factor, mode = maybe_simplify_for_es(
                doc, simplify=simplify_geometry, max_bytes=simplify_max_bytes,
                snap_to_grid=snap_to_grid, snap_grid_size=snap_grid_size,
            )
            _stamp_simplification(doc, factor, mode)
            doc = project_private_doc(doc, known_fields)
            bulk_body.append({"index": {"_index": index_name, "_id": geoid}})
            bulk_body.append(doc)
            submitted_ids.append(geoid)

        if skipped_no_id:
            logger.error(
                "ItemsElasticsearchPrivateDriver.write_entities: skipped %d item(s) "
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
        self._reject_unsupported_group_by(request)

        index_name = self._items_index_name(catalog_id)
        es = self._get_client()

        if not await es.indices.exists(index=index_name):
            return

        # --- by-id lookup (geoid token retrieval) -------------------------
        if entity_ids:
            for geoid in entity_ids:
                try:
                    resp = await es.get(index=index_name, id=geoid)
                    yield self._private_source_to_feature(
                        resp["_source"], catalog_id, collection_id, geoid,
                    )
                except Exception:
                    pass
            return

        # --- structural search (derived: routed via the INDEX lane) -------
        # The private index is not sharded by collection, so reuse the shared
        # ``_build_read_search_body``. Passing ``self._envelope_fields`` makes it
        # scope via the canonical ``collection_id`` term (the private doc shape)
        # and fold any pre-translated CQL2→ES ``request.es_filter`` in via
        # ``_query_request_to_es``. ``_collection_routing`` returns ``None`` for
        # the private index, so the resulting ``routing`` param is harmless.
        if request is None:
            return
        body, params = self._build_read_search_body(
            collection_id, request, limit, offset, self._envelope_fields,
        )
        params.pop("routing", None)  # private index is not collection-routed
        try:
            resp = await es.search(index=index_name, body=body, params=params)
        except Exception as e:
            logger.warning(
                "ItemsElasticsearchPrivateDriver: search failed for %s/%s: %s",
                catalog_id, collection_id, e,
            )
            return
        for hit in resp.get("hits", {}).get("hits", []):
            try:
                src = hit["_source"]
                # Use the ES document ``_id`` (always the geoid) as the
                # caller-supplied fallback so read never produces a None id even
                # when the source lacks both ``geoid`` and ``id`` root keys.
                yield self._private_source_to_feature(
                    src, catalog_id, collection_id, hit.get("_id"),
                )
            except Exception:
                pass

    @staticmethod
    def _private_source_to_feature(
        source: Dict[str, Any],
        catalog_id: str,
        collection_id: str,
        fallback_id: Any,
    ) -> Feature:
        """Reconstruct a :class:`Feature` from a private tenant doc ``_source``.

        Surfaces tenant-feature bookkeeping (``external_id`` and the
        simplification markers) in ``properties`` so callers can detect
        simplification without a separate API — shared by the by-id lookup and
        the structural-search read paths.

        Canonical docs (post-#1800/#1828) carry ``id``=geoid at root with a
        ``geoid`` alias written by ``build_tenant_feature_doc``. Pre-canonical
        docs carry only ``geoid`` at root. Both shapes are handled: prefer
        ``geoid`` (set on all private docs, canonical or legacy), then fall
        back to ``id`` (canonical only), then ``fallback_id`` (caller-supplied,
        typically the ES ``_id``).
        """
        props = dict(source.get("properties") or {})
        if "external_id" in source:
            props["external_id"] = source["external_id"]
        # Read geometry_simplification from canonical system container (#1828).
        # Defensive fallback reads old flat keys for docs written before this change.
        _gs = source.get("system", {}).get("geometry_simplification")
        if _gs:
            props["simplification_factor"] = _gs.get("factor")
            props["simplification_mode"] = _gs.get("mode")
        else:
            # Back-compat: flat keys on docs written before #1828 Phase 2.
            if "simplification_factor" in source:
                props["simplification_factor"] = source["simplification_factor"]
            if "simplification_mode" in source:
                props["simplification_mode"] = source["simplification_mode"]
        props["catalog_id"] = source.get("catalog_id", catalog_id)
        props["collection_id"] = source.get("collection_id", collection_id)
        # Resolve the feature id: prefer the ``geoid`` alias written by
        # build_tenant_feature_doc on all private docs (canonical and legacy),
        # fall back to ``id`` (canonical shape only), then the caller-supplied
        # fallback (typically the ES document ``_id``).
        feature_id = source.get("geoid") or source.get("id") or fallback_id
        return Feature(
            type="Feature",
            id=feature_id,
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
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            build_private_item_mapping,
            resolve_catalog_private_known_fields,
        )
        from dynastore.modules.elasticsearch.index_config import (
            get_private_items_index_settings,
        )

        index_name = self._items_index_name(catalog_id)
        es = self._get_client()

        # Snapshot the tenant-scoped operator overlay at index-create time.
        # Live edits to ``mapping`` do not retro-patch ES (ES disallows
        # tightening a live mapping); they take effect on the next index
        # rebuild — same contract as the public driver's Tier-2 overlay.
        known_fields = await resolve_catalog_private_known_fields(catalog_id)
        await self._ensure_index(
            es, index_name,
            build_private_item_mapping(known_fields),
            get_private_items_index_settings,
        )

        await self._apply_deny_policy(catalog_id)

    async def drop_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        soft: bool = False,
        **kwargs: Any,
    ) -> None:
        if soft:
            raise SoftDeleteNotSupportedError(
                "ItemsElasticsearchPrivateDriver does not support soft drop."
            )

        index_name = self._items_index_name(catalog_id)
        es = self._get_client()
        await es.indices.delete(
            index=index_name, params={"ignore_unavailable": "true"},
        )
        await self._revoke_deny_policy(catalog_id)

    # ------------------------------------------------------------------
    # Generic Indexer Protocol — slim, dispatcher-facing surface
    # ------------------------------------------------------------------
    # ensure_indexer is inherited from _ItemsElasticsearchBase (delegates
    # to ensure_storage); no public alias for the private index.

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
        from dynastore.modules.storage.drivers.elasticsearch_private.doc_builder import (
            build_tenant_feature_doc,
        )
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            build_private_item_mapping,
            project_private_doc,
            resolve_catalog_private_known_fields,
        )
        from dynastore.tools.geometry_simplify import maybe_simplify_for_es

        known_fields = await resolve_catalog_private_known_fields(ctx.catalog)
        await self._ensure_index(
            es, index_name,
            build_private_item_mapping(known_fields),
            get_private_items_index_settings,
        )

        src = op.payload or {"id": op.entity_id}
        src.setdefault("id", op.entity_id)
        doc = build_tenant_feature_doc(
            src, catalog_id=ctx.catalog, collection_id=ctx.collection,
        )
        # #1248: simplify geometry by default — exact geometry is the explicit
        # opt-out via ``simplify_geometry: false`` in the driver config.
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
        doc = project_private_doc(doc, known_fields)
        await es.index(index=index_name, id=op.entity_id, body=doc)

    async def index_bulk(self, ctx, ops):
        """Bulk-apply a batch of item ops via the ES ``_bulk`` API."""
        from dynastore.models.protocols.indexer import BulkResult
        from dynastore.modules.storage.drivers.elasticsearch_private.doc_builder import (
            build_tenant_feature_doc,
        )
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            build_private_item_mapping,
            project_private_doc,
            resolve_catalog_private_known_fields,
        )
        from dynastore.modules.elasticsearch.index_config import (
            get_private_items_index_settings,
        )
        from dynastore.tools.geometry_simplify import maybe_simplify_for_es

        if not ops:
            return BulkResult()
        if not ctx.collection:
            raise ValueError(
                "ItemsElasticsearchPrivateDriver.index_bulk: collection required for item ops",
            )

        # #1248: simplify geometry by default — exact geometry is the explicit
        # opt-out via ``simplify_geometry: false`` in the driver config.
        simplify_geometry = await self._resolve_simplify_geometry(
            ctx.catalog, ctx.collection,
        )
        simplify_max_bytes = await self._resolve_simplify_max_bytes(
            ctx.catalog, ctx.collection,
        )
        snap_to_grid, snap_grid_size = await self._resolve_snap_to_grid_config(
            ctx.catalog, ctx.collection,
        )

        # Tenant-scoped manual mapping overlay (#1295 slice 3).
        known_fields = await resolve_catalog_private_known_fields(ctx.catalog)

        index_name = self._items_index_name(ctx.catalog)
        es = self._get_client()

        await self._ensure_index(
            es, index_name,
            build_private_item_mapping(known_fields),
            get_private_items_index_settings,
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
            doc, factor, mode = maybe_simplify_for_es(
                doc, simplify=simplify_geometry, max_bytes=simplify_max_bytes,
                snap_to_grid=snap_to_grid, snap_grid_size=snap_grid_size,
            )
            _stamp_simplification(doc, factor, mode)
            doc = project_private_doc(doc, known_fields)
            body.append({"index": {"_index": index_name, "_id": op.entity_id}})
            body.append(doc)

        if not body:
            return BulkResult(total=len(ops))

        resp = await es.bulk(body=body)
        succeeded, failures = self._tally_bulk_response(
            resp, len(ops),
            driver_name="ItemsElasticsearchPrivateDriver",
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

    # ------------------------------------------------------------------
    # DENY policy management (self-contained)
    # ------------------------------------------------------------------

    @staticmethod
    async def _apply_deny_policy(catalog_id: str) -> None:
        from dynastore.tools.discovery import get_protocol
        try:
            from dynastore.models.protocols.policies import PermissionProtocol, Policy
        except ImportError:
            return

        perm = get_protocol(PermissionProtocol)
        if not perm:
            return

        from dynastore.extensions.tools.conformance import get_ogc_service_prefixes

        ogc_prefixes = get_ogc_service_prefixes()
        if ogc_prefixes:
            prefix_alt = "|".join(re.escape(p) for p in ogc_prefixes)
            resource_pattern = (
                f"/({prefix_alt})/catalogs/{re.escape(catalog_id)}(/.*)?"
            )
        else:
            resource_pattern = (
                f"/[^/]+/catalogs/{re.escape(catalog_id)}(/.*)?"
            )
            logger.warning(
                "PrivateDriver: no OGCServiceMixin contributors registered; "
                "falling back to wildcard DENY pattern for catalog '%s'.",
                catalog_id,
            )

        policy_id = f"private_deny_{catalog_id}"
        deny_policy = Policy(
            id=policy_id,
            description=f"Blocks public access to private catalog: {catalog_id}",
            actions=["GET"],
            resources=[resource_pattern],
            effect="DENY",
        )

        try:
            await perm.create_policy(deny_policy)
        except Exception:
            try:
                await perm.update_policy(deny_policy)
            except Exception as e:
                logger.error("DENY policy persist failed for '%s': %s", catalog_id, e)
                return

        # Bind the deny policy to the all_users role atomically — no Python R-M-W.
        try:
            from dynastore.modules.iam.iam_service import IamService
            iam = get_protocol(IamService)
            if iam:
                await iam.bind_policy_to_role("all_users", {"id": policy_id})
        except Exception as e:
            logger.warning("Failed to bind deny policy '%s' to all_users role: %s", policy_id, e)

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

        policy_id = f"private_deny_{catalog_id}"

        # Strip the deny policy from the all_users role atomically — no Python R-M-W.
        try:
            from dynastore.modules.iam.iam_service import IamService
            iam = get_protocol(IamService)
            if iam:
                await iam.unbind_policy_from_role("all_users", policy_id)
        except Exception as e:
            logger.warning("Failed to strip deny policy '%s' from all_users role: %s", policy_id, e)

        try:
            await perm.delete_policy(policy_id)
        except Exception:
            pass

    async def _restore_deny_policies(self) -> None:
        """Restore catalog-wide DENY policies at startup for any catalog
        that has at least one private collection.

        #733 cutover: privacy is now expressed as the presence of the
        private driver variants in a collection's routing configs.  We
        scan all catalogs and re-apply the DENY policy idempotently for
        any catalog whose items routing pins a private driver — at
        catalog scope or in any stored collection-level delta.

        #3160: the check reads only what is actually persisted — one
        resolved catalog-scope config plus one query over the stored
        collection-level ``ItemsRoutingConfig`` delta rows per catalog.
        The previous implementation listed every collection and resolved
        the full config waterfall for each, which on a catalog with tens
        of thousands of collections ran the serving worker out of memory
        before it could bind the port.

        The DENY pattern stays catalog-wide
        (``private_deny_{catalog_id}`` blocking
        ``/.../catalogs/{cat}/...``) — the cascade validator guarantees
        that any private collection's items go through this driver, so
        the catalog-wide block covers every privacy-sensitive path.
        """
        try:
            from dynastore.models.protocols import CatalogsProtocol
            from dynastore.models.protocols.configs import ConfigsProtocol
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
                        configs, catalog_id,
                    ):
                        await self._apply_deny_policy(catalog_id)
                        logger.info(
                            "PrivateDriver: restored DENY for '%s' (#733 — items routing pins a private driver).",
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
        configs: Any,
        catalog_id: str,
    ) -> bool:
        """Return True iff the catalog's items routing pins
        ``items_elasticsearch_private_driver`` (#733, #1047) — at catalog
        scope, or in any STORED collection-level delta row (#3160).

        Equivalence with the old per-collection walk: a collection without
        a stored ``ItemsRoutingConfig`` row resolves exactly to the
        catalog-scope config checked first; a stored delta replaces
        ``operations`` wholesale on merge, so a private pin in the resolved
        view implies the pin is visible either at catalog scope or verbatim
        in the delta payload. One deliberate widening: a catalog-scope
        private pin now restores the DENY even before the first collection
        exists (fails closed for privacy).

        Collection-envelope privacy via a separate ES driver is no longer
        supported (#1047 Phase 2). Only items-level private routing is checked.
        """
        from dynastore.modules.storage.routing_config import (
            _PRIVATE_ITEMS_DRIVER_ID,
            ItemsRoutingConfig,
            _items_routing_has_private_driver,
        )

        try:
            catalog_scope = await configs.get_config(
                ItemsRoutingConfig, catalog_id=catalog_id,
            )
        except Exception:
            catalog_scope = None
        if isinstance(catalog_scope, ItemsRoutingConfig) and (
            _items_routing_has_private_driver(catalog_scope)
        ):
            return True

        try:
            deltas = await configs.list_collection_config_deltas(
                ItemsRoutingConfig, catalog_id,
            )
        except Exception:
            return False
        for delta in deltas:
            operations = delta.get("operations") or {}
            for entries in operations.values():
                for entry in entries or []:
                    if (
                        isinstance(entry, dict)
                        and entry.get("driver_ref") == _PRIVATE_ITEMS_DRIVER_ID
                    ):
                        return True
        return False

    # ``location``, ``get_entity_fields``, ``export_entities``,
    # ``rename_storage``, ``restore_entities`` are inherited from
    # :class:`_ItemsElasticsearchBase`.  ``_location_backend`` is set above
    # so ``location()`` emits the correct backend label.
    #
    # CollectionItemsStore Protocol — data-side ops:
    # ``count_entities`` / ``compute_extents`` / ``aggregate`` /
    # ``introspect_schema`` are inherited from :class:`_ItemsElasticsearchBase`.
    # The private index is not sharded by collection, so the
    # :meth:`_collection_routing` override (returns ``None``) keeps the
    # inherited ops from sending a ``_routing`` param.
