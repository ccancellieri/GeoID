"""
Search Service — Elasticsearch-backed implementation of SearchProtocol.

Primary goal: search by id, title, description, or any property of items,
             supporting multilingual fields (title, description stored as JSON objects
             with language keys, e.g. {"en": "Food security", "fr": "Sécurité alimentaire"}).

The service uses ES wildcard field patterns (title.*, description.*) to search
across all language variants transparently. Callers can optionally pass `lang`
to boost a specific language.

Implements ``SearchProtocol`` so that the router and other consumers discover
the search backend via protocol discovery, not direct imports.
"""
import logging
import re
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, FastAPI
from contextlib import asynccontextmanager
from dynastore.extensions.protocols import ExtensionProtocol

from .search_models import (
    CatalogSearchBody,
    GenericCollection,
    ItemCollection,
    SearchBody,
    SearchLink,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_sort(sortby: Optional[str]) -> List[Dict[str, Any]]:
    """
    Parse STAC sortby string into ES sort clause.
    
    Examples:
      '+properties.title'  → [{"properties.title.keyword": {"order": "asc"}}]
      '-properties.datetime' → [{"properties.datetime": {"order": "desc"}}]
    """
    if not sortby:
        return [{"_score": {"order": "desc"}}]
    direction = "desc" if sortby.startswith("-") else "asc"
    field = sortby.lstrip("+-")
    # For text fields we sort on the .keyword sub-field
    text_fields = {"properties.title", "title", "description", "properties.description"}
    if field in text_fields:
        field = f"{field}.keyword"
    return [{field: {"order": direction}}, {"_score": {"order": "desc"}}]


def _parse_datetime_filter(datetime_str: Optional[str]) -> Optional[Dict[str, Any]]:
    """Convert STAC datetime string to ES range filter."""
    if not datetime_str:
        return None
    if "/" in datetime_str:
        parts = datetime_str.split("/")
        gte = None if parts[0] == ".." else parts[0]
        lte = None if parts[1] == ".." else parts[1]
        r: Dict[str, Any] = {}
        if gte:
            r["gte"] = gte
        if lte:
            r["lte"] = lte
        return {"range": {"properties.datetime": r}}
    # Single date-time value
    return {
        "bool": {
            "should": [
                {"term": {"properties.datetime": datetime_str}},
                {"range": {"properties.datetime": {"lte": datetime_str}}},
            ]
        }
    }


def _build_item_query(body: SearchBody) -> Dict[str, Any]:
    """
    Build an Elasticsearch query DSL from a STAC SearchBody.
    
    Multilingual fields (title, description, keywords) are stored as objects
    with language-code keys, e.g. {"en": "Food", "fr": "Aliment"}.
    We use wildcard patterns in multi_match to cover all languages.
    """
    must: List[Dict[str, Any]] = []
    filter_: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    # 1. Free-text query (PRIMARY GOAL)
    # Covers id, title.*, description.*, keywords.*, and all properties.*
    # ------------------------------------------------------------------
    if body.q:
        must.append({
            "multi_match": {
                "query": body.q,
                "type": "best_fields",
                # Wildcard field patterns cover all language variants:
                #  title.en, title.fr, title.ar, ...
                #  properties.title.en, properties.title.fr, ...
                "fields": [
                    "id^3",               # Boost exact ID matches
                    "title.*^2",          # Multilingual title at root level
                    "properties.title.*^2",  # Multilingual title inside properties
                    "description.*",
                    "properties.description.*",
                    "keywords.*",
                    "properties.keywords.*",
                    "properties.*",       # Any other property (lower relevance)
                ],
                "fuzziness": "AUTO",
                "minimum_should_match": "1",
            }
        })

    # ------------------------------------------------------------------
    # 2. Catalog filter
    # ------------------------------------------------------------------
    if body.catalog_id:
        filter_.append({"term": {"catalog_id": body.catalog_id}})

    # ------------------------------------------------------------------
    # 3. IDs filter
    # ------------------------------------------------------------------
    if body.ids:
        filter_.append({"terms": {"id": body.ids}})

    # ------------------------------------------------------------------
    # 3. Collections filter
    # ------------------------------------------------------------------
    if body.collections:
        filter_.append({"terms": {"collection": body.collections}})

    # ------------------------------------------------------------------
    # 4. Spatial filter – bbox or intersects (mutually exclusive)
    # ------------------------------------------------------------------
    if body.bbox:
        lon_min, lat_min, lon_max, lat_max = body.bbox[:4]
        filter_.append({
            "geo_shape": {
                "geometry": {
                    "shape": {
                        "type": "envelope",
                        "coordinates": [[lon_min, lat_max], [lon_max, lat_min]],
                    },
                    "relation": "intersects",
                }
            }
        })
    elif body.intersects:
        filter_.append({
            "geo_shape": {
                "geometry": {
                    "shape": body.intersects,
                    "relation": "intersects",
                }
            }
        })

    # ------------------------------------------------------------------
    # 5. Datetime filter
    # ------------------------------------------------------------------
    dt_filter = _parse_datetime_filter(body.datetime)
    if dt_filter:
        filter_.append(dt_filter)

    # ------------------------------------------------------------------
    # Build compound query
    # ------------------------------------------------------------------
    if not must and not filter_:
        query = {"match_all": {}}
    else:
        query = {
            "bool": {
                **({"must": must} if must else {}),
                **({"filter": filter_} if filter_ else {}),
            }
        }

    return query


_GENERIC_SORT_ALIASES: Dict[str, str] = {"code": "id", "label": "title"}
_GENERIC_MULTILINGUAL_FIELDS = frozenset({"title", "description"})


def _parse_sort_generic(
    sortby: Optional[str], lang: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Parse a catalog/collection sortby string into an ES sort clause.

    Aliases:  code → id,  label → title
    Multilingual fields (title, description) sort on title.{lang} — plain keyword
    per the catch-all strings dynamic template (NOT title.{lang}.keyword).
    """
    if not sortby:
        return [{"_score": {"order": "desc"}}]
    direction = "desc" if sortby.startswith("-") else "asc"
    raw_field = sortby.lstrip("+-")
    field = _GENERIC_SORT_ALIASES.get(raw_field, raw_field)
    # Validate lang: 2-3 lowercase letters only
    safe_lang = lang if (lang and re.match(r"^[a-z]{2,3}$", lang)) else "en"
    if field in _GENERIC_MULTILINGUAL_FIELDS:
        es_field = f"{field}.{safe_lang}"
    else:
        es_field = field
    return [{es_field: {"order": direction}}, {"_score": {"order": "desc"}}]


def _build_generic_query(body: CatalogSearchBody) -> Dict[str, Any]:
    """Build a query for catalog/collection search."""
    must: List[Dict[str, Any]] = []
    filter_: List[Dict[str, Any]] = []

    if body.q:
        must.append({
            "multi_match": {
                "query": body.q,
                "type": "best_fields",
                "fields": [
                    "id^3",
                    "title.*^2",
                    "description.*",
                    "keywords.*",
                ],
                "fuzziness": "AUTO",
            }
        })
    if body.ids:
        filter_.append({"terms": {"id": body.ids}})
    if body.catalog_id:
        filter_.append({"term": {"catalog_id": body.catalog_id}})

    if not must and not filter_:
        return {"match_all": {}}
    return {
        "bool": {
            **({"must": must} if must else {}),
            **({"filter": filter_} if filter_ else {}),
        }
    }


# ---------------------------------------------------------------------------
# SearchService — implements SearchProtocol (discoverable via get_protocol)
# ---------------------------------------------------------------------------
class SearchService(ExtensionProtocol):
    """
    Elasticsearch-backed implementation of ``SearchProtocol``.

    Discovered at runtime by the router via ``get_protocol(SearchProtocol)``
    — the router has zero direct imports from this module or from ES.

    To swap to a different backend (Solr, Meilisearch, etc.), create a new
    ``ExtensionProtocol`` that satisfies the same ``SearchProtocol`` contract
    and ensure it is loaded instead of this one.
    """

    priority: int = 100
    router: APIRouter

    def __init__(self):
        from .router import router as search_router
        self.router = search_router
        logger.info("SearchService: Initializing extension.")

    def _resolve_items_driver(self) -> Any:
        """Discover the platform's items search driver instance.

        Returns the registered :class:`ItemsElasticsearchDriver` (or any
        items-tier ES driver of the same shape). The driver instance is
        the entry point to the platform ES engine — callers reach the ES
        client via ``driver.es_client`` rather than importing
        ``dynastore.modules.elasticsearch.client.get_client`` directly.
        Centralising here means swapping the items search backend is a
        single driver-registration change.
        """
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

        ``ItemsRoutingConfig`` is keyed at the (catalog, collection)
        pair, so routing-config dispatch is only meaningful when a
        single concrete collection is in scope.
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
        """
        Protocol-discoverable service that provides STAC-compliant search
        backed by Elasticsearch.

        Implements paginated search for Items, Catalogs, and Collections.
        """
        from .policies import register_search_policies
        from dynastore.modules.elasticsearch.aliases import (
            ensure_public_alias_exists,
        )
        register_search_policies()
        # Force-materialise the platform public items alias so a fresh
        # deployment with no catalog onboarded yet responds 200/empty on
        # the unscoped /search path instead of 404. Idempotent — no-op
        # when the alias already has members.
        await ensure_public_alias_exists()
        yield

    async def search_items(
        self,
        body: SearchBody,
        base_url: str = "",
    ) -> ItemCollection:
        """
        Search STAC Items.

        Supports: q (full-text, multilingual), bbox, intersects, datetime,
                  ids, collections, sortby, and cursor-based pagination via token.
        """
        index = await self._resolve_items_index(body.catalog_id, body.collections, body.driver)
        sort = _parse_sort(body.sortby)
        query = _build_item_query(body)

        es_body: Dict[str, Any] = {
            "query": query,
            "size": body.limit,
            "sort": sort,
        }

        # Cursor-based pagination (search_after)
        if body.token:
            import json as _json
            try:
                es_body["search_after"] = _json.loads(body.token)
            except Exception:
                logger.warning(f"Ignoring invalid search token: {body.token!r}")

        es = self._resolve_items_driver().es_client
        resp = await es.search(index=index, body=es_body, ignore_unavailable=True)  # type: ignore[call-arg]

        hits = resp.get("hits", {})
        total = hits.get("total", {}).get("value", 0)
        raw_hits = hits.get("hits", [])
        features = [h["_source"] for h in raw_hits]

        links: List[SearchLink] = [
            SearchLink(rel="self", href=f"{base_url}/search", type="application/geo+json")
        ]

        # Next page link using search_after token
        if raw_hits and len(raw_hits) == body.limit:
            import json as _json
            last_sort = raw_hits[-1].get("sort", [])
            token = _json.dumps(last_sort)
            import urllib.parse
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

    async def search_items_struct(
        self,
        *,
        catalog_id: Optional[str],
        collections: Optional[List[str]],
        ids: Optional[List[str]],
        bbox: Optional[List[float]],
        intersects: Optional[Dict[str, Any]],
        datetime: Optional[str],
        limit: int,
    ) -> "ItemSearchResult":
        """Backend-agnostic structural search — satisfies
        :class:`~dynastore.models.protocols.item_search.ItemSearchProtocol`.
        """
        from dynastore.models.protocols.item_search import ItemSearchResult

        body = SearchBody(
            q=None,
            token=None,
            sortby=None,
            catalog_id=catalog_id,
            collections=collections,
            ids=ids,
            bbox=bbox,
            intersects=intersects,
            datetime=datetime,
            limit=limit,
        )
        ic = await self.search_items(body)
        return ItemSearchResult(
            features=list(ic.features),
            total=ic.numberMatched or 0,
        )

    async def reindex_catalog(
        self,
        catalog_id: str,
        driver: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Dispatch a BulkCatalogReindexTask and return 202 + task_id.

        If ``driver`` is provided, the task targets only that secondary driver.
        Privacy routing is per-collection
        (``CollectionPrivacy.is_private``); there is no catalog-wide
        "private mode" to resolve here.
        """
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
        """Dispatch a BulkCollectionReindexTask and return 202 + task_id.

        If ``driver`` is provided, the task targets only that secondary driver.
        """
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

    async def search_catalogs(
        self,
        body: CatalogSearchBody,
        base_url: str = "",
    ) -> GenericCollection:
        """Search Catalogs."""
        return await self._search_generic(body, "catalog", base_url)

    async def search_collections(
        self,
        body: CatalogSearchBody,
        base_url: str = "",
    ) -> GenericCollection:
        """Search Collections."""
        return await self._search_generic(body, "collection", base_url)

    async def _resolve_items_index(
        self,
        catalog_id: Optional[str],
        collections: Optional[List[str]],
        driver_hint: Optional[str],
    ) -> str:
        """Resolve the ES index/alias to query for an items SEARCH.

        - Catalog-scoped (``catalog_id`` set): per-tenant index
          ``{prefix}-{catalog}-items`` owned by the items driver.
        - Unscoped (no ``catalog_id``): platform public alias
          ``{prefix}-items`` whose membership is managed by
          :meth:`ItemsElasticsearchDriver.ensure_storage`.

        Honors routing-config pins via
        :meth:`_resolve_items_search_driver_ref` when a single
        collection is in scope — a warn-and-fallback in
        :func:`get_search_driver` covers unknown hints. The returned
        driver_ref is logged for observability; the live ES query goes
        through the platform driver's client either way because all ES
        items live in a single shared cluster.
        """
        from dynastore.modules.elasticsearch.client import get_index_prefix
        from dynastore.modules.elasticsearch.mappings import (
            get_public_items_alias,
            get_tenant_items_index,
        )

        driver_ref = await self._resolve_items_search_driver_ref(
            catalog_id, collections, driver_hint,
        )
        if driver_ref:
            logger.debug(
                "SearchService: routing items SEARCH via driver_ref=%s "
                "(catalog=%s, collections=%s)",
                driver_ref, catalog_id, collections,
            )

        prefix = get_index_prefix()
        if catalog_id:
            return get_tenant_items_index(prefix, catalog_id)
        return get_public_items_alias(prefix)

    async def _search_generic(
        self,
        body: CatalogSearchBody,
        entity_type: str,
        base_url: str = "",
    ) -> GenericCollection:
        from dynastore.modules.elasticsearch.client import get_index_prefix
        from dynastore.modules.elasticsearch.mappings import get_index_name

        index = get_index_name(get_index_prefix(), entity_type)
        query = _build_generic_query(body)
        # Singleton catalogs/collections indexes use catalog_id as the routing
        # key; scoped requests narrow to one shard + filter by catalog_id so
        # results never leak across catalogs even at the index level.
        if entity_type == "collection" and body.catalog_id:
            query = {
                "bool": {
                    "must": [query],
                    "filter": [{"term": {"catalog_id": body.catalog_id}}],
                }
            }

        es_body: Dict[str, Any] = {
            "query": query,
            "size": body.limit,
            "sort": _parse_sort_generic(body.sortby, body.lang),
        }
        if body.token:
            import json as _json
            try:
                es_body["search_after"] = _json.loads(body.token)
            except Exception:
                pass

        search_kwargs: Dict[str, Any] = {
            "index": index,
            "body": es_body,
            "ignore_unavailable": True,
        }
        if entity_type == "collection" and body.catalog_id:
            search_kwargs["params"] = {"routing": body.catalog_id}

        es = self._resolve_items_driver().es_client
        resp = await es.search(**search_kwargs)

        raw_hits = resp.get("hits", {}).get("hits", [])
        entities = [h["_source"] for h in raw_hits]

        links: List[SearchLink] = []
        if raw_hits and len(raw_hits) == body.limit:
            import json as _json
            token = _json.dumps(raw_hits[-1].get("sort", []))
            links.append(SearchLink(
                rel="next",
                href=f"{base_url}/search/{entity_type}s",
                type="application/json",
                method="POST",
                body={**body.model_dump(exclude_none=True, exclude={"token"}), "token": token},
                merge=False,
            ))

        return GenericCollection(
            entities=entities,
            links=links,
            numberReturned=len(entities),
        )
