"""FastAPI router for the geoid extension's item-search route.

Hosted by the geoid extension. The lookup is PG-backed (geoid resolution is
routing-aware and uses the catalog's private ES index when one is pinned;
external_id resolution runs over PostgreSQL), so the route does not hard-depend
on Elasticsearch availability.

URL contract (#1210 — replaces the former GET/POST .../geoid routes):
  POST /search/catalogs/{catalog_id}/items-search
    body: exactly one of
      {"geoid": ...}                              # resolved catalog-wide
      {"external_id": ..., "collection_id": ...}  # collection_id required
"""
import logging

from fastapi import APIRouter, HTTPException

from .lookup_models import GeoidCollection, GeoidResult, ItemsSearchBody
from .lookup_service import (
    lookup_by_external_id,
    lookup_by_geoids,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/search", tags=["Item Search"])


def _to_collection(rows) -> GeoidCollection:
    results = [GeoidResult(**r) for r in rows]
    return GeoidCollection(results=results, numberReturned=len(results))


@router.post(
    "/catalogs/{catalog_id}/items-search",
    response_model=GeoidCollection,
    summary="Resolve an item in a catalog by exactly one of geoid or external_id.",
    response_model_exclude_none=True,
)
async def items_search(catalog_id: str, body: ItemsSearchBody) -> GeoidCollection:
    # Treat empty strings as absent, then enforce the geoid-xor-external_id rule.
    geoid = body.geoid or None
    external_id = body.external_id or None
    collection_id = body.collection_id or None
    if geoid is not None:
        if external_id is not None:
            raise HTTPException(
                status_code=400,
                detail="Provide exactly one of 'geoid' or 'external_id'.",
            )
        rows = await lookup_by_geoids(catalog_id, [geoid], limit=body.limit)
    elif external_id is not None:
        # external_id is not globally unique, so it is resolved within a single
        # named collection only — a bare external_id would mean a cross-collection
        # scan, which the public lookup contract disallows (un-fao/GeoID#1204 R2).
        if collection_id is None:
            raise HTTPException(
                status_code=400,
                detail="'external_id' requires 'collection_id'.",
            )
        rows = await lookup_by_external_id(
            catalog_id, collection_id, external_id, limit=body.limit,
        )
    else:
        raise HTTPException(
            status_code=400,
            detail="Provide exactly one of 'geoid' or 'external_id'.",
        )
    return _to_collection(rows)
