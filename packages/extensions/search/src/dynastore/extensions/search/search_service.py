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

from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
from dynastore.modules.elasticsearch.mappings import get_search_index
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
        self._es = None  # set during lifespan
        logger.info("SearchService: Initializing extension.")

    def _get_es(self):
        """Return the shared ES client (cached after first lifespan startup)."""
        if self._es is None:
            # Lazy fallback: look up the singleton from the elasticsearch module.
            from dynastore.modules.elasticsearch.client import get_client
            self._es = get_client()
            if self._es is None:
                raise RuntimeError(
                    "Elasticsearch client is not initialized. "
                    "Ensure ElasticsearchModule is registered and its lifespan has started."
                )
        return self._es

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        """
        Protocol-discoverable service that provides STAC-compliant search
        backed by Elasticsearch.

        Implements paginated search for Items, Catalogs, and Collections.
        """
        from .policies import register_search_policies
        from dynastore.modules.elasticsearch.client import get_client
        register_search_policies()
        self._es = get_client()  # cache the module-level singleton for all requests
        yield
        self._es = None

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
        index = get_search_index(_get_index_prefix(), "item", body.catalog_id)
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

        es = self._get_es()
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

    async def _search_generic(
        self,
        body: CatalogSearchBody,
        entity_type: str,
        base_url: str = "",
    ) -> GenericCollection:
        index = get_search_index(_get_index_prefix(), entity_type, body.catalog_id)
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

        es = self._get_es()
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
