"""FastAPI router — STAC Item Search (issue #819 trimmed surface).

Endpoints exposed by this router:

    GET/POST  /search                                                   – Unscoped item search (public alias)
    GET/POST  /search/catalogs/{catalog_id}                             – Catalog-scoped item search
    POST      /search/catalogs/{catalog_id}/reindex                     – Trigger catalog reindex (admin)
    POST      /search/catalogs/{catalog_id}/collections/{cid}/reindex   – Trigger collection reindex (admin)

Per #819 catalog/collection keyword-search routes were removed; the
extension is item-only and dispatches through ``ItemsRoutingConfig.operations[SEARCH]``
(the platform default pins ``items_elasticsearch_driver`` whose
``preferred_for`` includes ``Hint.GEOMETRY_SIMPLIFIED``).

GeoID lookup routes (/search/catalogs/{cat}/geoid/...) are served by the
geoid extension's lookup_router.py (PG-backed, no Elasticsearch dependency).

Conformance class: https://api.stacspec.org/v1.0.0/item-search
"""
import logging
from typing import Any, Dict, Optional
from fastapi import APIRouter, HTTPException, Query, Request

from .search_models import ItemCollection, SearchBody

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/search", tags=["Item Search"])


def _base_url(request: Request) -> str:
    """Derive base URL (scheme + host) from request."""
    return str(request.base_url).rstrip("/")


def _get_search_service():
    """Discover the SearchProtocol implementation at runtime."""
    from dynastore.models.protocols.search import SearchProtocol
    from dynastore.tools.discovery import get_protocol
    svc = get_protocol(SearchProtocol)
    if not svc:
        raise HTTPException(
            status_code=503,
            detail="Search backend not available. No SearchProtocol implementation loaded.",
        )
    return svc


def _split_csv(value: Optional[str]) -> Optional[list[str]]:
    return [v.strip() for v in value.split(",")] if value else None


# ---------------------------------------------------------------------------
# Item Search – unscoped (GET/POST /search)
# ---------------------------------------------------------------------------

@router.get(
    "",
    response_model=ItemCollection,
    summary="Search STAC items (unscoped).",
    response_model_exclude_none=True,
)
async def get_search(
    request: Request,
    q: Optional[str] = Query(None, description="Free-text query over id, title, description, and all item properties."),
    bbox: Optional[str] = Query(None, description="Bounding box: min_lon,min_lat,max_lon,max_lat"),
    datetime: Optional[str] = Query(None, description="RFC 3339 date-time or interval"),
    limit: int = Query(10, ge=1, le=10_000),
    ids: Optional[str] = Query(None, description="Comma-separated list of Item IDs."),
    geoid: Optional[str] = Query(None, description="Comma-separated list of GeoIDs."),
    external_id: Optional[str] = Query(None, description="Comma-separated list of external IDs."),
    collections: Optional[str] = Query(None, description="Comma-separated list of Collection IDs."),
    sortby: Optional[str] = Query(None, description="Sort field. Prefix with '+'/'-' for asc/desc."),
    token: Optional[str] = Query(None, description="Pagination cursor from a previous response's 'next' link."),
    driver: Optional[str] = Query(None, description="Driver hint for ItemsRoutingConfig.operations[SEARCH]."),
) -> ItemCollection:
    body = SearchBody(
        q=q,
        bbox=[float(x) for x in bbox.split(",")] if bbox else None,
        datetime=datetime,
        limit=limit,
        ids=_split_csv(ids),
        geoid=_split_csv(geoid),
        external_id=_split_csv(external_id),
        collections=_split_csv(collections),
        sortby=sortby,
        token=token,
        driver=driver,
    )
    return await _get_search_service().search_items(body, base_url=_base_url(request))


@router.post(
    "",
    response_model=ItemCollection,
    summary="Search STAC items (unscoped, full-featured body).",
    response_model_exclude_none=True,
)
async def post_search(request: Request, body: SearchBody) -> ItemCollection:
    return await _get_search_service().search_items(body, base_url=_base_url(request))


# ---------------------------------------------------------------------------
# Item Search – catalog-scoped (GET/POST /search/catalogs/{catalog_id})
# ---------------------------------------------------------------------------

@router.get(
    "/catalogs/{catalog_id}",
    response_model=ItemCollection,
    summary="Search STAC items scoped to a catalog.",
    response_model_exclude_none=True,
)
async def get_search_items_scoped(
    request: Request,
    catalog_id: str,
    q: Optional[str] = Query(None, description="Free-text query over id, title, description, and all item properties."),
    bbox: Optional[str] = Query(None, description="Bounding box: min_lon,min_lat,max_lon,max_lat"),
    datetime: Optional[str] = Query(None, description="RFC 3339 date-time or interval"),
    limit: int = Query(10, ge=1, le=10_000),
    ids: Optional[str] = Query(None, description="Comma-separated list of Item IDs."),
    geoid: Optional[str] = Query(None, description="Comma-separated list of GeoIDs."),
    external_id: Optional[str] = Query(None, description="Comma-separated list of external IDs."),
    collections: Optional[str] = Query(None, description="Comma-separated list of Collection IDs."),
    sortby: Optional[str] = Query(None, description="Sort field. Prefix with '+'/'-' for asc/desc."),
    token: Optional[str] = Query(None, description="Pagination cursor from a previous response's 'next' link."),
    driver: Optional[str] = Query(None, description="Driver hint for ItemsRoutingConfig.operations[SEARCH]."),
) -> ItemCollection:
    body = SearchBody(
        q=q,
        catalog_id=catalog_id,
        bbox=[float(x) for x in bbox.split(",")] if bbox else None,
        datetime=datetime,
        limit=limit,
        ids=_split_csv(ids),
        geoid=_split_csv(geoid),
        external_id=_split_csv(external_id),
        collections=_split_csv(collections),
        sortby=sortby,
        token=token,
        driver=driver,
    )
    return await _get_search_service().search_items(body, base_url=_base_url(request))


@router.post(
    "/catalogs/{catalog_id}",
    response_model=ItemCollection,
    summary="Search STAC items scoped to a catalog (full-featured body).",
    response_model_exclude_none=True,
)
async def post_search_items_scoped(
    request: Request, catalog_id: str, body: SearchBody,
) -> ItemCollection:
    body = body.model_copy(update={"catalog_id": catalog_id})
    return await _get_search_service().search_items(body, base_url=_base_url(request))


# ---------------------------------------------------------------------------
# Reindex – /search/catalogs/{catalog_id}/reindex
# ---------------------------------------------------------------------------

@router.post(
    "/catalogs/{catalog_id}/reindex",
    response_model=Dict[str, Any],
    summary="Trigger full catalog reindex (admin only).",
    status_code=202,
)
async def post_reindex_catalog_scoped(
    request: Request,
    catalog_id: str,
    driver: Optional[str] = Query(None, description="Hint: which secondary driver to reindex (e.g. 'elasticsearch')."),
) -> Dict[str, Any]:
    return await _get_search_service().reindex_catalog(catalog_id, driver=driver)


@router.post(
    "/catalogs/{catalog_id}/collections/{collection_id}/reindex",
    response_model=Dict[str, Any],
    summary="Trigger single collection reindex (admin only).",
    status_code=202,
)
async def post_reindex_collection_scoped(
    request: Request,
    catalog_id: str,
    collection_id: str,
    driver: Optional[str] = Query(None, description="Hint: which secondary driver to reindex."),
) -> Dict[str, Any]:
    return await _get_search_service().reindex_collection(catalog_id, collection_id, driver=driver)
