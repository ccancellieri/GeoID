"""
Search Service — Elasticsearch-backed implementation of SearchProtocol.

Item-only surface per #819. Catalog/collection keyword search retired
from the router and the protocol; the only remaining endpoints are
``GET|POST /search`` and ``GET|POST /search/catalogs/{catalog_id}``,
plus admin-side reindex triggers.

Multilingual fields (title, description) are stored as JSON objects
with language-code keys, e.g. {"en": "Food security", "fr": "Sécurité alimentaire"}.
Search uses ES wildcard field patterns (title.*, description.*) so the
query covers every language transparently.

Implements ``SearchProtocol`` so consumers discover the search backend
via protocol discovery, not direct imports.
"""
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, FastAPI
from contextlib import asynccontextmanager
from dynastore.extensions.protocols import ExtensionProtocol

from .search_models import ItemCollection, SearchBody, SearchLink

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


# The ES items-query DSL now lives in the core single source of truth
# (``dynastore.modules.elasticsearch.items_query``) so the routing-aware item
# search dispatched through whichever driver a catalog pins for
# ``Operation.SEARCH`` builds the same query as this extension (#989). These
# module-level shims preserve the historical ``search_service`` import surface
# (consumed by ``test_search_item_filters`` and ``test_items_projection``).
from dynastore.modules.elasticsearch.items_query import (  # noqa: E402
    PUBLIC_ENVELOPE_FIELDS,
    EnvelopeFields,
    build_items_query as _build_items_query,
    parse_sort as _parse_sort,
)


def _index_is_canonical(index: Any) -> bool:
    """True when the resolved index carries the canonical envelope shape.

    Both the tenant-private index (``-private-items``) and the standardized
    envelope index (``-envelope-items``, the row-level-ABAC driver, #1285)
    store the canonical identity names (``collection_id`` / ``geoid`` /
    ``external_id``) at the document root — as opposed to the public per-catalog
    index's STAC-flavoured shape (``collection`` / ``id`` / ``_external_id``).
    Structural-field and CQL2 field mapping select the field shape from this
    single predicate so both canonical indexes are addressed consistently. The
    public path is unchanged (neither suffix matches the public per-catalog
    index / platform alias).
    """
    name = str(index)
    return name.endswith("-private-items") or name.endswith("-envelope-items")


def _build_item_query(
    body: SearchBody, fields: EnvelopeFields = PUBLIC_ENVELOPE_FIELDS,
) -> Dict[str, Any]:
    """Build an Elasticsearch query DSL from a STAC SearchBody.

    Thin adapter over the core :func:`build_items_query` SSOT. ``fields``
    selects the envelope field names of the resolved index so a query against
    the tenant-private index addresses its canonical ``collection_id`` /
    ``geoid`` / ``external_id`` shape rather than the public one.
    """
    return _build_items_query(
        q=body.q,
        ids=body.ids,
        geoid=body.geoid,
        external_id=body.external_id,
        collections=body.collections,
        bbox=body.bbox,
        intersects=body.intersects,
        datetime=body.datetime,
        fields=fields,
    )


# ---------------------------------------------------------------------------
# SearchService — implements SearchProtocol (discoverable via get_protocol)
# ---------------------------------------------------------------------------
class SearchService(ExtensionProtocol):
    """Elasticsearch-backed implementation of ``SearchProtocol``."""

    priority: int = 100
    router: APIRouter

    def __init__(self):
        from .router import router as search_router
        self.router = search_router
        logger.info("SearchService: Initializing extension.")

    def _resolve_items_driver(self) -> Any:
        """Discover the platform's items search driver instance."""
        from dynastore.modules.storage.drivers.elasticsearch import (
            ItemsElasticsearchDriver,
        )
        from dynastore.tools.discovery import get_protocol

        driver = get_protocol(ItemsElasticsearchDriver)
        if driver is None:
            raise RuntimeError(
                "SearchService: no ItemsElasticsearchDriver registered. "
                "Ensure storage drivers are loaded before the search "
                "extension queries items."
            )
        return driver

    async def _resolve_items_search_driver_ref(
        self,
        catalog_id: Optional[str],
        collections: Optional[List[str]],
        driver_hint: Optional[str],
    ) -> Optional[str]:
        """Honor a routing-config pin via :func:`get_search_driver`.

        Returns the driver_ref to use for SEARCH dispatch, or ``None``
        when no per-collection ``ItemsRoutingConfig.operations[SEARCH]``
        applies (cross-collection or unscoped queries fall through to
        the platform default — :meth:`_resolve_items_driver`).
        """
        if not catalog_id or not collections or len(collections) != 1:
            return None
        from dynastore.modules.storage.routing_config import get_search_driver

        return await get_search_driver(
            catalog_id,
            entity="item",
            collection_id=collections[0],
            driver_hint=driver_hint,
        )

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        from dynastore.modules.elasticsearch.aliases import (
            ensure_public_alias_exists,
        )
        # Force-materialise the public items alias so a fresh deployment
        # with no catalog onboarded yet returns 200/empty on /search
        # instead of 404. Idempotent.
        await ensure_public_alias_exists()
        yield

    async def _translate_cql_filter(
        self, body: SearchBody, index: str,
    ) -> Optional[Dict[str, Any]]:
        """Translate ``body.filter`` (CQL2) to an Elasticsearch query clause.

        Reuses the shared queryables SSOT (``QueryOptimizer.get_all_queryable_fields``
        via the collection's ``ItemsPostgresqlDriverConfig`` — the config waterfall
        is driver-agnostic) to build the ES field mapping, then converts the CQL2
        filter to a pygeofilter AST and emits a plain ES clause through the shared
        translator. Canonical vs public field mapping is selected from the
        resolved index (the per-tenant private index ends in ``-private-items``
        and the standardized envelope index ends in ``-envelope-items``; both
        carry the canonical shape — see :func:`_index_is_canonical`).

        Returns ``None`` when there is no filter. Raises ``HTTPException(400)`` when
        a filter is present but cannot be honoured (no single catalog+collection
        scope, unknown queryable property, or unsupported operator) — unlike the
        STAC ``/search`` path this REST endpoint queries Elasticsearch directly and
        has no PostgreSQL fallback, so silently dropping the filter (returning the
        unfiltered set) would be wrong.
        """
        if body.filter is None:
            return None

        from fastapi import HTTPException
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
        from dynastore.modules.storage.drivers.pg_sidecars.base import ConsumerType
        from dynastore.modules.catalog.query_optimizer import QueryOptimizer
        from dynastore.modules.storage.drivers.es_common import (
            build_es_field_mapping,
            build_es_fulltext_mapping,
            cql_ast_to_es_query,
            UntranslatableFilterError,
        )
        from dynastore.models.driver_context import DriverContext
        from dynastore.tools.discovery import get_protocol
        from pygeofilter.parsers.cql2_json import parse as parse_cql2_json
        from pygeofilter.parsers.cql2_text import parse as parse_cql2_text

        cols = body.collections or []
        if not body.catalog_id or len(cols) != 1:
            raise HTTPException(
                status_code=400,
                detail=(
                    "A CQL2 'filter' requires the search to be scoped to a single "
                    "catalog and a single collection (its queryables define the "
                    "field mapping)."
                ),
            )

        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            raise HTTPException(status_code=400, detail="Configs protocol unavailable for filter resolution.")
        col_config = await configs.get_config(
            ItemsPostgresqlDriverConfig,
            catalog_id=body.catalog_id,
            collection_id=cols[0],
            ctx=DriverContext(db_resource=None),
        )
        if col_config is None:
            raise HTTPException(
                status_code=400,
                detail=f"No items config for catalog '{body.catalog_id}', collection '{cols[0]}'.",
            )

        optimizer = QueryOptimizer(col_config, consumer=ConsumerType.STAC)
        # Both the private and the standardized envelope index carry the
        # canonical field shape; the CQL→ES field mapping must address that
        # shape for either (#1285). The public per-catalog index keeps the
        # STAC-flavoured mapping.
        private = _index_is_canonical(index)
        queryable_fields = optimizer.get_all_queryable_fields()
        field_mapping = build_es_field_mapping(queryable_fields, private=private)
        fulltext_mapping = build_es_fulltext_mapping(queryable_fields, private=private)

        use_text = (
            isinstance(body.filter, str)
            or (body.filter_lang or "").lower() == "cql2-text"
        )
        try:
            ast_node = (
                parse_cql2_text(body.filter)
                if use_text and isinstance(body.filter, str)
                else parse_cql2_json(body.filter)
            )
            return cql_ast_to_es_query(ast_node, field_mapping, fulltext_mapping)
        except UntranslatableFilterError as exc:
            raise HTTPException(status_code=400, detail=f"Unsupported filter: {exc}") from exc
        except HTTPException:
            raise
        except Exception as exc:  # malformed CQL2, parse failure
            raise HTTPException(status_code=400, detail=f"Invalid CQL2 filter: {exc}") from exc

    async def search_items(
        self,
        body: SearchBody,
        base_url: str = "",
        *,
        scoped: bool = False,
        principals: Optional[List[str]] = None,
        principal: Optional[Any] = None,
    ) -> ItemCollection:
        """Search STAC Items.

        ``scoped`` distinguishes the two route families that share this
        method. The catalog-scoped routes (``/search/catalogs/{cat}``) pass
        ``scoped=True`` and may resolve a catalog's pinned SEARCH driver —
        including a tenant-private items driver — so the datamanager reads
        its own private index. The unscoped public discovery routes
        (``/search``) pass ``scoped=False``: even when a ``catalog_id`` is
        supplied in the body they must only ever address the public per-catalog
        index / platform alias, never a catalog's private index. Honouring the
        private SEARCH pin on the public endpoint would surface private items
        through the cross-tenant discovery surface.

        ``principals`` / ``principal`` carry the caller's identity (set by the
        IAM middleware on ``request.state`` and threaded in by the router).
        They are consumed ONLY when the resolved SEARCH driver opts in to
        row-level ABAC (``applies_access_filter=True`` — the standardized
        envelope driver, #1285): in that case the caller's read scope is
        compiled to a neutral ``AccessFilter`` and ANDed into the ES query so
        the driver returns only documents the principal may read. For every
        other driver no filter is compiled and the query is byte-for-byte what
        it was before — existing search behaviour is unchanged. The list models
        anonymous as the middleware does (a single anonymous-role principal),
        so an unauthenticated caller still gets the correct public-only scope
        rather than skipping the filter (which would leak).
        """
        from dynastore.modules.elasticsearch.items_projection import (
            resolve_catalog_known_fields,
        )

        index = await self._resolve_items_index(
            body.catalog_id, body.collections, body.driver, scoped=scoped,
        )
        # When the request scopes to a single catalog, resolve that
        # catalog's full Tier-1 ∪ Tier-2 known-fields so sort on a
        # Tier-2 field hits the explicit ``properties.<key>`` path
        # instead of routing through ``extras``. Cross-catalog alias
        # queries fall back to Tier 1 only (Tier-2 fields sort gracefully
        # as missing on non-declaring catalogs).
        sort_known = await resolve_catalog_known_fields(body.catalog_id)
        sort = _parse_sort(body.sortby, sort_known)

        # The tenant-private index and the standardized envelope index both
        # carry the canonical envelope names (``collection_id`` / ``geoid`` /
        # ``external_id``); the public index uses the STAC-flavoured shape.
        # Address whichever the resolved index uses so structural filters
        # (collections, ids, external_id) actually match — same canonical-index
        # detection the CQL translator uses below (``_index_is_canonical``).
        from dynastore.modules.elasticsearch.items_query import (
            PRIVATE_ENVELOPE_FIELDS,
            PUBLIC_ENVELOPE_FIELDS,
        )
        envelope_fields = (
            PRIVATE_ENVELOPE_FIELDS
            if _index_is_canonical(index)
            else PUBLIC_ENVELOPE_FIELDS
        )
        query = _build_item_query(body, envelope_fields)

        # CQL2 ``filter`` → ES Query DSL, ANDed into the structural query.
        # Raises 400 when present-but-untranslatable (no PG fallback on this path).
        es_clause = await self._translate_cql_filter(body, index)
        if es_clause is not None:
            from dynastore.modules.storage.drivers.es_common import merge_es_filter
            query = merge_es_filter(query, es_clause)

        # Row-level ABAC (#1285): when — and ONLY when — the resolved SEARCH
        # driver opts in (``applies_access_filter=True``, the standardized
        # envelope driver), compile the caller's read scope to a neutral
        # ``AccessFilter`` and AND it into the query so the driver returns only
        # documents the principal may read. The filter is compiled even for
        # anonymous callers (the router threads the middleware's anonymous
        # principals list) so the public-only scope is enforced rather than
        # skipped — skipping would be a leak. For every other driver this block
        # is a no-op and the query is exactly what it was before.
        query = await self._apply_access_filter(
            query,
            index=index,
            catalog_id=body.catalog_id,
            collections=body.collections,
            driver_hint=body.driver,
            scoped=scoped,
            principals=principals,
            principal=principal,
        )

        es_body: Dict[str, Any] = {
            "query": query,
            "size": body.limit,
            "sort": sort,
        }

        if body.token:
            import json as _json
            try:
                es_body["search_after"] = _json.loads(body.token)
            except Exception:
                logger.warning(f"Ignoring invalid search token: {body.token!r}")

        es = self._resolve_items_driver().es_client
        # ``ignore_unavailable`` covers missing concrete indexes inside an
        # alias; ``allow_no_indices`` covers the alias itself not existing
        # yet on a fresh deployment with zero catalogs onboarded (the public
        # items alias only materialises on the first
        # ``add_index_to_public_alias`` call, per
        # ``ensure_public_alias_exists`` docstring). Together they make
        # ``/search`` return an empty ``ItemCollection`` rather than a 404
        # on a brand-new install — closes the remaining symptom of #803.
        resp = await es.search(
            index=index,
            body=es_body,
            ignore_unavailable=True,
            allow_no_indices=True,
        )  # type: ignore[call-arg]

        hits = resp.get("hits", {})
        total = hits.get("total", {}).get("value", 0)
        raw_hits = hits.get("hits", [])
        features = [h["_source"] for h in raw_hits]

        links: List[SearchLink] = [
            SearchLink(rel="self", href=f"{base_url}/search", type="application/geo+json")
        ]

        if raw_hits and len(raw_hits) == body.limit:
            import json as _json
            last_sort = raw_hits[-1].get("sort", [])
            token = _json.dumps(last_sort)
            links.append(SearchLink(
                rel="next",
                href=f"{base_url}/search",
                type="application/geo+json",
                method="POST",
                body={
                    **body.model_dump(exclude_none=True, exclude={"token"}),
                    "token": token,
                },
                merge=False,
            ))

        return ItemCollection(
            features=features,
            links=links,
            numberMatched=total,
            numberReturned=len(features),
        )

    # NOTE: the structural STAC fast path is no longer a bespoke search method.
    # STAC ``/search`` resolves the items SEARCH driver via routing and streams
    # the structural query through that driver's ``read_entities`` +
    # ``count_entities`` contract (shared ``_ElasticsearchBase`` implementation)
    # — so a catalog routing SEARCH to the tenant-private ES index is honoured,
    # which a public-only singleton could not express. SearchService keeps its
    # own ``/search`` REST router (full-text, sort, search_after tokens) via
    # :meth:`search_items`.

    async def reindex_catalog(
        self,
        catalog_id: str,
        driver: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Dispatch a BulkCatalogReindexTask and return 202 + task_id."""
        from dynastore.models.protocols import DatabaseProtocol
        from dynastore.modules.tasks import tasks_module
        from dynastore.modules.tasks.models import TaskCreate
        from dynastore.tools.discovery import get_protocol

        db = get_protocol(DatabaseProtocol)
        if not db:
            raise RuntimeError("DatabaseProtocol not available.")
        engine = db.engine if isinstance(db, DatabaseProtocol) else db

        inputs: Dict[str, Any] = {"catalog_id": catalog_id}
        if driver:
            inputs["driver"] = driver

        task = await tasks_module.create_task_for_catalog(
            engine=engine,
            task_data=TaskCreate(
                caller_id="system:search",
                task_type="elasticsearch_indexer",
                inputs=inputs,
            ),
            catalog_id=catalog_id,
        )
        if task is None:
            raise RuntimeError("reindex_catalog: create_task returned None (dedup hit on a non-dedup task).")
        return {"task_id": str(task.task_id), "catalog_id": catalog_id, "driver": driver, "status": "queued"}

    async def reindex_collection(
        self,
        catalog_id: str,
        collection_id: str,
        driver: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Dispatch a BulkCollectionReindexTask and return 202 + task_id."""
        from dynastore.models.protocols import DatabaseProtocol
        from dynastore.modules.tasks import tasks_module
        from dynastore.modules.tasks.models import TaskCreate
        from dynastore.tools.discovery import get_protocol

        db = get_protocol(DatabaseProtocol)
        if not db:
            raise RuntimeError("DatabaseProtocol not available.")
        engine = db.engine if isinstance(db, DatabaseProtocol) else db

        inputs: Dict[str, Any] = {"catalog_id": catalog_id, "collection_id": collection_id}
        if driver:
            inputs["driver"] = driver

        task = await tasks_module.create_task_for_catalog(
            engine=engine,
            task_data=TaskCreate(
                caller_id="system:search",
                task_type="elasticsearch_indexer",
                inputs=inputs,
            ),
            catalog_id=catalog_id,
        )
        if task is None:
            raise RuntimeError("reindex_collection: create_task returned None (dedup hit on a non-dedup task).")
        return {
            "task_id": str(task.task_id),
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "driver": driver,
            "status": "queued",
        }

    async def backfill_envelope_attrs(
        self,
        catalog_id: str,
        collection_id: str,
        dry_run: bool = False,
        batch_size: int = 500,
    ) -> Dict[str, Any]:
        """Enqueue an EnvelopeAttrsBackfillTask and return 202 + task_id.

        Sysadmin-only endpoint.  The task stamps ``attrs`` onto existing
        envelope-driver documents that pre-date #1441.
        """
        from dynastore.models.protocols import DatabaseProtocol
        from dynastore.modules.tasks import tasks_module
        from dynastore.modules.tasks.models import TaskCreate
        from dynastore.tools.discovery import get_protocol

        db = get_protocol(DatabaseProtocol)
        if not db:
            raise RuntimeError("DatabaseProtocol not available.")
        engine = db.engine if isinstance(db, DatabaseProtocol) else db

        inputs: Dict[str, Any] = {
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "dry_run": dry_run,
            "batch_size": batch_size,
        }

        task = await tasks_module.create_task_for_catalog(
            engine=engine,
            task_data=TaskCreate(
                caller_id="system:search",
                task_type="envelope_attrs_backfill_collection",
                inputs=inputs,
            ),
            catalog_id=catalog_id,
        )
        if task is None:
            raise RuntimeError(
                "backfill_envelope_attrs: create_task returned None "
                "(dedup hit on a non-dedup task)."
            )
        return {
            "task_id": str(task.task_id),
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "dry_run": dry_run,
            "status": "queued",
        }

    def _resolve_items_driver_by_ref(self, driver_ref: str) -> Any:
        """Resolve a live items ES driver instance by its ``driver_ref``.

        Both ES item drivers (the public per-catalog driver and the
        tenant-private driver) subclass ``_ItemsElasticsearchBase`` and derive
        their ``driver_ref`` from ``_to_snake(type(self).__name__)``. Returns
        ``None`` when no registered driver matches the ref.
        """
        from dynastore.modules.storage.drivers.elasticsearch import (
            _ItemsElasticsearchBase,
        )
        from dynastore.tools.discovery import get_protocols
        from dynastore.tools.typed_store.base import _to_snake

        for d in get_protocols(_ItemsElasticsearchBase):
            if _to_snake(type(d).__name__) == driver_ref:
                return d
        return None

    async def _resolve_search_driver_instance(
        self,
        catalog_id: Optional[str],
        collections: Optional[List[str]],
        driver_hint: Optional[str],
        *,
        scoped: bool,
    ) -> Any:
        """Resolve the live SEARCH driver instance for this request, or ``None``.

        Mirrors the pin resolution in :meth:`_resolve_items_index`: only the
        catalog-scoped route family (``scoped=True``) honours a per-collection
        ``ItemsRoutingConfig.operations[SEARCH]`` pin, so an unscoped public
        request never resolves a pinned (e.g. row-level-ABAC) driver — it falls
        through to the public per-catalog index with no driver instance. Used
        purely to read the driver's ``applies_access_filter`` marker; index
        resolution itself stays in :meth:`_resolve_items_index`.
        """
        if not scoped:
            return None
        driver_ref = await self._resolve_items_search_driver_ref(
            catalog_id, collections, driver_hint,
        )
        if not driver_ref:
            return None
        return self._resolve_items_driver_by_ref(driver_ref)

    async def _apply_access_filter(
        self,
        query: Dict[str, Any],
        *,
        index: str,
        catalog_id: Optional[str],
        collections: Optional[List[str]],
        driver_hint: Optional[str],
        scoped: bool,
        principals: Optional[List[str]],
        principal: Optional[Any],
    ) -> Dict[str, Any]:
        """AND the caller's compiled read scope into ``query`` — gated by driver.

        Returns ``query`` unchanged unless the resolved SEARCH driver opts in to
        row-level ABAC (``applies_access_filter=True``). When it does, the
        caller's read scope is compiled to a neutral ``AccessFilter`` via
        ``get_protocol(PermissionProtocol).compile_read_filter`` and translated
        to an ES clause that is ANDed into ``query``. The compile happens for
        anonymous callers too (the middleware models anonymous as a single
        anonymous-role principal) so the public-only scope is enforced, never
        skipped. The IAM module is reached ONLY through the neutral
        ``PermissionProtocol`` + ``AccessFilter`` contract — no IAM import here.
        """
        driver = await self._resolve_search_driver_instance(
            catalog_id, collections, driver_hint, scoped=scoped,
        )
        if not getattr(driver, "applies_access_filter", False):
            return query

        from dynastore.modules.storage.drivers.elasticsearch_envelope.access_translate import (
            access_filter_to_es,
        )
        from dynastore.modules.storage.access_scope import (
            compile_read_access_filter,
        )
        from dynastore.modules.storage.drivers.es_common import merge_es_filter

        # Single source of truth for the read-scope compilation, shared with the
        # STAC fast path so the two enforcement paths cannot drift. Fails closed
        # (deny-everything) when no PermissionProtocol is registered.
        access_filter = await compile_read_access_filter(
            catalog_id=catalog_id,
            collections=collections,
            principals=principals,
            principal=principal,
        )
        clause = access_filter_to_es(access_filter)
        if clause is None:
            # ``allow_all`` with no deny — no row-level restriction to apply.
            return query
        return merge_es_filter(query, clause)

    async def _resolve_items_index(
        self,
        catalog_id: Optional[str],
        collections: Optional[List[str]],
        driver_hint: Optional[str],
        *,
        scoped: bool = False,
    ) -> str:
        """Resolve the ES index/alias to query for an items SEARCH.

        When a catalog pins ``Operation.SEARCH`` to a specific items ES driver
        (e.g. the tenant-private driver), the index queried must be that
        driver's own index — not the public per-catalog index. Both ES item
        drivers expose the same ``_items_index_name`` seam, so resolve the
        pinned driver instance and ask it. A cross-catalog / unscoped query
        (or a missing pin) falls back to the public per-catalog index / alias.

        The pinned-driver resolution is honoured **only** for the catalog-scoped
        route family (``scoped=True``). The unscoped public discovery routes
        (``scoped=False``) must never address a catalog's private items index,
        even when a ``catalog_id`` is supplied — a private SEARCH pin would
        otherwise leak tenant-private items through the public ``/search``
        surface. Unscoped requests therefore always resolve the public
        per-catalog index (when scoped to one catalog) or the platform alias.
        """
        from dynastore.modules.elasticsearch.client import get_index_prefix
        from dynastore.modules.elasticsearch.mappings import (
            get_public_items_alias,
            get_tenant_items_index,
        )

        driver_ref = (
            await self._resolve_items_search_driver_ref(
                catalog_id, collections, driver_hint,
            )
            if scoped
            else None
        )

        if catalog_id and driver_ref:
            driver = self._resolve_items_driver_by_ref(driver_ref)
            if driver is not None:
                index = driver._items_index_name(catalog_id)
                logger.debug(
                    "SearchService: routing items SEARCH via driver_ref=%s "
                    "index=%s (catalog=%s, collections=%s)",
                    driver_ref, index, catalog_id, collections,
                )
                return index
            logger.warning(
                "SearchService: SEARCH driver_ref=%s did not resolve to a live "
                "items ES driver; falling back to the public index "
                "(catalog=%s, collections=%s).",
                driver_ref, catalog_id, collections,
            )

        prefix = get_index_prefix()
        if catalog_id:
            return get_tenant_items_index(prefix, catalog_id)
        return get_public_items_alias(prefix)
