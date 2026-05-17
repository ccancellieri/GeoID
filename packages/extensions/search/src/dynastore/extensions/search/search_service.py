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


def _parse_sort(
    sortby: Optional[str],
    known_fields: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Parse STAC sortby string into ES sort clause.

    Sort paths are resolved through
    :func:`dynastore.modules.elasticsearch.items_projection.resolve_es_field_path`
    so a request to sort on an unknown extension field
    (``properties.foo:bar``) targets the ``properties.extras.foo:bar``
    bucket where the projection helper actually wrote it. Tier-1 paths
    (and Tier-2 paths when ``known_fields`` carries the per-catalog
    overlay) pass through unchanged.

    ``known_fields`` defaults to Tier 1 only — used by cross-catalog
    ``/search`` (against the platform alias) where Tier-2 fields are
    not guaranteed to exist on every member; on non-declaring catalogs
    they sort as missing (graceful per-index miss, zero ES cost). The
    per-catalog ``/search`` call site resolves the catalog's full
    known-fields map and threads it in so Tier-2 paths sort on their
    explicit ``properties.<key>`` path.
    """
    if not sortby:
        return [{"_score": {"order": "desc"}}]
    from dynastore.modules.elasticsearch.items_projection import (
        build_known_fields,
        resolve_es_field_path,
    )
    if known_fields is None:
        known_fields = build_known_fields()
    direction = "desc" if sortby.startswith("-") else "asc"
    field = sortby.lstrip("+-")
    field = resolve_es_field_path(field, known_fields)
    # Text fields sort on the .keyword sub-field
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
    return {
        "bool": {
            "should": [
                {"term": {"properties.datetime": datetime_str}},
                {"range": {"properties.datetime": {"lte": datetime_str}}},
            ]
        }
    }


def _build_item_query(body: SearchBody) -> Dict[str, Any]:
    """Build an Elasticsearch query DSL from a STAC SearchBody."""
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
                    "properties.title.*^2",
                    "description.*",
                    "properties.description.*",
                    "keywords.*",
                    "properties.keywords.*",
                    "properties.*",
                ],
                "fuzziness": "AUTO",
                "minimum_should_match": "1",
            }
        })

    if body.catalog_id:
        filter_.append({"term": {"catalog_id": body.catalog_id}})

    if body.ids:
        filter_.append({"terms": {"id": body.ids}})

    if body.geoid:
        # GeoID is conventionally stored under properties.geoid on item docs;
        # the should-clause also matches a root-level mirror when present
        # (defensive — keeps the filter behaving uniformly across migrations).
        filter_.append({
            "bool": {
                "should": [
                    {"terms": {"properties.geoid": body.geoid}},
                    {"terms": {"geoid": body.geoid}},
                ],
                "minimum_should_match": 1,
            }
        })

    if body.external_id:
        # ``_external_id`` is the internal mirror written by the items driver
        # (see ItemsElasticsearchDriver._extract_external_id_from_doc); it is
        # the only reliably-indexed external_id field across writers.
        filter_.append({"terms": {"_external_id": body.external_id}})

    if body.collections:
        filter_.append({"terms": {"collection": body.collections}})

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

    dt_filter = _parse_datetime_filter(body.datetime)
    if dt_filter:
        filter_.append(dt_filter)

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
        from .policies import register_search_policies
        from dynastore.modules.elasticsearch.aliases import (
            ensure_public_alias_exists,
        )
        register_search_policies()
        # Force-materialise the public items alias so a fresh deployment
        # with no catalog onboarded yet returns 200/empty on /search
        # instead of 404. Idempotent.
        await ensure_public_alias_exists()
        yield

    async def search_items(
        self,
        body: SearchBody,
        base_url: str = "",
    ) -> ItemCollection:
        """Search STAC Items."""
        from dynastore.modules.elasticsearch.items_projection import (
            resolve_catalog_known_fields,
        )

        index = await self._resolve_items_index(body.catalog_id, body.collections, body.driver)
        # When the request scopes to a single catalog, resolve that
        # catalog's full Tier-1 ∪ Tier-2 known-fields so sort on a
        # Tier-2 field hits the explicit ``properties.<key>`` path
        # instead of routing through ``extras``. Cross-catalog alias
        # queries fall back to Tier 1 only (Tier-2 fields sort gracefully
        # as missing on non-declaring catalogs).
        sort_known = await resolve_catalog_known_fields(body.catalog_id)
        sort = _parse_sort(body.sortby, sort_known)
        query = _build_item_query(body)

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

    async def _resolve_items_index(
        self,
        catalog_id: Optional[str],
        collections: Optional[List[str]],
        driver_hint: Optional[str],
    ) -> str:
        """Resolve the ES index/alias to query for an items SEARCH."""
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
