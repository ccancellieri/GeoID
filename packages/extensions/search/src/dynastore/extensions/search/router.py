"""
FastAPI router implementing the STAC API Item Search specification.

Path convention: /search/catalogs/{catalog_id}/...
Search actions use {resource}-search (hyphen, not nested /search).

New paths:
  GET/POST /search                                                 – Item search
  GET/POST /search/catalogs                                        – Catalog search
  GET/POST /search/catalogs/{catalog_id}/collections-search        – Collection search scoped to catalog
  POST     /search/catalogs/{catalog_id}/reindex                   – Trigger catalog reindex
  POST     /search/catalogs/{catalog_id}/collections/{cid}/reindex – Trigger collection reindex

Deprecated aliases (kept for backward compat):
  GET/POST /search/collections  → /search/catalogs/{catalog_id}/collections-search
  POST     /search/reindex/catalogs/{catalog_id}
  POST     /search/reindex/catalogs/{catalog_id}/collections/{cid}

GeoID lookup routes (/search/catalogs/{cat}/geoid/...) are served by the
geoid extension's lookup_router.py (PG-backed, no Elasticsearch dependency).

Conformance class: https://api.stacspec.org/v1.0.0/item-search
"""
import logging
from typing import Any, Dict, Optional
from fastapi import APIRouter, HTTPException, Query, Request

from .search_models import (
    CatalogSearchBody,
    GenericCollection,
    ItemCollection,
    SearchBody,
)

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


# ---------------------------------------------------------------------------
# STAC Item Search – GET (Simple)
# ---------------------------------------------------------------------------

@router.get(
    "",
    response_model=ItemCollection,
    summary="Search STAC items with simple filtering.",
    response_model_exclude_none=True,
)
async def get_search(
    request: Request,
    q: Optional[str] = Query(None, description="Free-text query over id, title, description, and all item properties."),
    bbox: Optional[str] = Query(None, description="Bounding box: min_lon,min_lat,max_lon,max_lat"),
    datetime: Optional[str] = Query(None, description="RFC 3339 date-time or interval"),
    limit: int = Query(10, ge=1, le=10_000),
    ids: Optional[str] = Query(None, description="Comma-separated list of Item IDs."),
    collections: Optional[str] = Query(None, description="Comma-separated list of Collection IDs."),
    sortby: Optional[str] = Query(None, description="Sort field. Prefix with '+'/'-' for asc/desc."),
    token: Optional[str] = Query(None, description="Pagination cursor from a previous response's 'next' link."),
) -> ItemCollection:
    body = SearchBody(
        q=q,
        bbox=[float(x) for x in bbox.split(",")] if bbox else None,
        datetime=datetime,
        limit=limit,
        ids=[i.strip() for i in ids.split(",")] if ids else None,
        collections=[c.strip() for c in collections.split(",")] if collections else None,
        sortby=sortby,
        token=token,
    )
    return await _get_search_service().search_items(body, base_url=_base_url(request))


# ---------------------------------------------------------------------------
# STAC Item Search – POST (Full-featured)
# ---------------------------------------------------------------------------

@router.post(
    "",
    response_model=ItemCollection,
    summary="Search STAC items with full-featured filtering.",
    response_model_exclude_none=True,
)
async def post_search(request: Request, body: SearchBody) -> ItemCollection:
    return await _get_search_service().search_items(body, base_url=_base_url(request))


# ---------------------------------------------------------------------------
# STAC Item Search – scoped to catalog: GET/POST /catalogs/{catalog_id}/items-search
# ---------------------------------------------------------------------------

@router.get(
    "/catalogs/{catalog_id}/items-search",
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
    collections: Optional[str] = Query(None, description="Comma-separated list of Collection IDs."),
    sortby: Optional[str] = Query(None, description="Sort field. Prefix with '+'/'-' for asc/desc."),
    token: Optional[str] = Query(None, description="Pagination cursor from a previous response's 'next' link."),
) -> ItemCollection:
    body = SearchBody(
        q=q,
        catalog_id=catalog_id,
        bbox=[float(x) for x in bbox.split(",")] if bbox else None,
        datetime=datetime,
        limit=limit,
        ids=[i.strip() for i in ids.split(",")] if ids else None,
        collections=[c.strip() for c in collections.split(",")] if collections else None,
        sortby=sortby,
        token=token,
    )
    return await _get_search_service().search_items(body, base_url=_base_url(request))


@router.post(
    "/catalogs/{catalog_id}/items-search",
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
# Catalog Search – GET/POST
# ---------------------------------------------------------------------------

@router.get(
    "/catalogs",
    response_model=GenericCollection,
    summary="Search Catalogs.",
    response_model_exclude_none=True,
)
async def get_search_catalogs(
    request: Request,
    q: Optional[str] = Query(None, description="Free-text query over id, title, description."),
    ids: Optional[str] = Query(None, description="Comma-separated list of Catalog IDs."),
    limit: int = Query(10, ge=1, le=10_000),
    token: Optional[str] = Query(None),
    sortby: Optional[str] = Query(None, description="Sort field. Aliases: 'code'=id, 'label'=title."),
    lang: Optional[str] = Query(None, description="Language for multilingual sort (e.g. 'en', 'fr')."),
) -> GenericCollection:
    body = CatalogSearchBody(
        q=q,
        ids=[i.strip() for i in ids.split(",")] if ids else None,
        limit=limit,
        token=token,
        sortby=sortby,
        lang=lang,
    )
    return await _get_search_service().search_catalogs(body, base_url=_base_url(request))


@router.post(
    "/catalogs",
    response_model=GenericCollection,
    summary="Search Catalogs (full-featured body).",
    response_model_exclude_none=True,
)
async def post_search_catalogs(request: Request, body: CatalogSearchBody) -> GenericCollection:
    return await _get_search_service().search_catalogs(body, base_url=_base_url(request))


# ---------------------------------------------------------------------------
# Collection Search – scoped to catalog: GET/POST /catalogs/{catalog_id}/collections-search
# ---------------------------------------------------------------------------

@router.get(
    "/catalogs/{catalog_id}/collections-search",
    response_model=GenericCollection,
    summary="Search Collections scoped to a catalog.",
    response_model_exclude_none=True,
)
async def get_search_collections_scoped(
    request: Request,
    catalog_id: str,
    q: Optional[str] = Query(None, description="Free-text query over id, title, description."),
    ids: Optional[str] = Query(None, description="Comma-separated list of Collection IDs."),
    limit: int = Query(10, ge=1, le=10_000),
    token: Optional[str] = Query(None),
    sortby: Optional[str] = Query(None, description="Sort field. Aliases: 'code'=id, 'label'=title."),
    lang: Optional[str] = Query(None, description="Language for multilingual sort (e.g. 'en', 'fr')."),
) -> GenericCollection:
    body = CatalogSearchBody(
        q=q,
        ids=[i.strip() for i in ids.split(",")] if ids else None,
        catalog_id=catalog_id,
        limit=limit,
        token=token,
        sortby=sortby,
        lang=lang,
    )
    return await _get_search_service().search_collections(body, base_url=_base_url(request))


@router.post(
    "/catalogs/{catalog_id}/collections-search",
    response_model=GenericCollection,
    summary="Search Collections scoped to a catalog (full-featured body).",
    response_model_exclude_none=True,
)
async def post_search_collections_scoped(
    request: Request, catalog_id: str, body: CatalogSearchBody,
) -> GenericCollection:
    body = body.model_copy(update={"catalog_id": catalog_id})
    return await _get_search_service().search_collections(body, base_url=_base_url(request))


# ---------------------------------------------------------------------------
# Collection Search – cross-catalog (deprecated alias, kept for backward compat)
# ---------------------------------------------------------------------------

@router.get(
    "/collections",
    response_model=GenericCollection,
    summary="Search Collections (cross-catalog). Deprecated: use /catalogs/{catalog_id}/collections-search.",
    response_model_exclude_none=True,
    deprecated=True,
)
async def get_search_collections(
    request: Request,
    q: Optional[str] = Query(None),
    ids: Optional[str] = Query(None),
    catalog_id: Optional[str] = Query(None),
    limit: int = Query(10, ge=1, le=10_000),
    token: Optional[str] = Query(None),
    sortby: Optional[str] = Query(None, description="Sort field. Aliases: 'code'=id, 'label'=title."),
    lang: Optional[str] = Query(None, description="Language for multilingual sort."),
) -> GenericCollection:
    body = CatalogSearchBody(
        q=q,
        ids=[i.strip() for i in ids.split(",")] if ids else None,
        catalog_id=catalog_id,
        limit=limit,
        token=token,
        sortby=sortby,
        lang=lang,
    )
    return await _get_search_service().search_collections(body, base_url=_base_url(request))


@router.post(
    "/collections",
    response_model=GenericCollection,
    summary="Search Collections (cross-catalog). Deprecated: use /catalogs/{catalog_id}/collections-search.",
    response_model_exclude_none=True,
    deprecated=True,
)
async def post_search_collections(request: Request, body: CatalogSearchBody) -> GenericCollection:
    return await _get_search_service().search_collections(body, base_url=_base_url(request))


# ---------------------------------------------------------------------------
# Reindex – /catalogs/{catalog_id}/reindex
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


# ---------------------------------------------------------------------------
# Reindex – deprecated aliases
# ---------------------------------------------------------------------------

@router.post(
    "/reindex/catalogs/{catalog_id}",
    response_model=Dict[str, Any],
    summary="Trigger catalog reindex. Deprecated: use /catalogs/{catalog_id}/reindex.",
    status_code=202,
    deprecated=True,
)
async def post_reindex_catalog(
    request: Request,
    catalog_id: str,
) -> Dict[str, Any]:
    return await _get_search_service().reindex_catalog(catalog_id)


@router.post(
    "/reindex/catalogs/{catalog_id}/collections/{collection_id}",
    response_model=Dict[str, Any],
    summary="Trigger collection reindex. Deprecated: use /catalogs/{catalog_id}/collections/{collection_id}/reindex.",
    status_code=202,
    deprecated=True,
)
async def post_reindex_collection(
    request: Request,
    catalog_id: str,
    collection_id: str,
) -> Dict[str, Any]:
    return await _get_search_service().reindex_collection(catalog_id, collection_id)
