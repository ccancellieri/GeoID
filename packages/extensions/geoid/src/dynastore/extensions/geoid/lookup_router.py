"""FastAPI router for /search/catalogs/{cat}/geoid lookup endpoints.

Hosted by the geoid extension. Previously these routes lived in
extensions/search/router.py and were backed by Elasticsearch (the per-tenant
private index). This implementation uses direct PG queries via
ItemsProtocol.search_items so there is no dependency on Elasticsearch
availability.

URL contract is preserved:
  GET  /search/catalogs/{catalog_id}/geoid/{geoid}
  POST /search/catalogs/{catalog_id}/geoid
"""
import logging

from fastapi import APIRouter, HTTPException

from .lookup_models import GeoidCollection, GeoidResult, GeoidSearchBody
from .lookup_service import lookup_by_external_id, lookup_by_geoids

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/search", tags=["Item Search"])


def _to_collection(rows) -> GeoidCollection:
    results = [GeoidResult(**r) for r in rows]
    return GeoidCollection(results=results, numberReturned=len(results))


@router.get(
    "/catalogs/{catalog_id}/geoid/{geoid}",
    response_model=GeoidCollection,
    summary="Look up a single item by geoid in a catalog (PG-backed).",
    response_model_exclude_none=True,
)
async def get_geoid_scoped(catalog_id: str, geoid: str) -> GeoidCollection:
    rows = await lookup_by_geoids(catalog_id, [geoid], limit=1)
    return _to_collection(rows)


@router.post(
    "/catalogs/{catalog_id}/geoid",
    response_model=GeoidCollection,
    summary="Batch geoid lookup OR single (external_id, collection_id) lookup (PG-backed).",
    response_model_exclude_none=True,
)
async def post_geoid_scoped(catalog_id: str, body: GeoidSearchBody) -> GeoidCollection:
    has_geoids = bool(body.geoids)
    has_pair = bool(body.external_id) and bool(body.collection_id)
    if body.external_id and not body.collection_id:
        raise HTTPException(
            status_code=400,
            detail="external_id requires collection_id (cross-collection enumeration is not allowed).",
        )
    if not has_geoids and not has_pair:
        raise HTTPException(
            status_code=400,
            detail="Provide geoids or (external_id, collection_id).",
        )
    if has_geoids:
        rows = await lookup_by_geoids(catalog_id, list(body.geoids), limit=body.limit)
    else:
        rows = await lookup_by_external_id(
            catalog_id, body.collection_id, body.external_id, limit=body.limit,
        )
    return _to_collection(rows)
