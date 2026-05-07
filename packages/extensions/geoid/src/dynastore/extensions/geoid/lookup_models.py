"""Pydantic models for the geoid-extension lookup routes.

These shapes are part of the customer-facing contract for
``/search/catalogs/{catalog_id}/geoid`` (PG-backed). They previously lived
in the search extension as a re-export indirection; they were moved here
when the geoid lookup routes stopped depending on Elasticsearch.
"""
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class GeoidSearchBody(BaseModel):
    """Body for tenant-scoped lookup against the per-tenant feature index.

    Either ``geoids`` (lookup across collections of the catalog) or the
    pair ``(external_id, collection_id)`` (resolve a tenant's own item id
    within a known collection) must be supplied. ``external_id`` alone is
    rejected to prevent enumeration across collections.
    """
    geoids: Optional[List[str]] = Field(
        None, min_length=1, description="One or more geoid values to look up.",
    )
    catalog_id: Optional[str] = Field(
        None,
        description=(
            "Tenant identifier. Required at the service layer; the path "
            "variant `/catalogs/{catalog_id}/geoid` injects this for you."
        ),
    )
    external_id: Optional[str] = Field(
        None, description="Tenant's own item id (must be paired with collection_id).",
    )
    collection_id: Optional[str] = Field(
        None, description="Required when external_id is supplied.",
    )
    limit: int = Field(100, ge=1, le=10_000, description="Maximum number of results to return.")


class GeoidResult(BaseModel):
    """A single result from the per-tenant feature index."""
    geoid: str
    catalog_id: str
    collection_id: str
    external_id: Optional[str] = None
    geometry: Optional[Dict[str, Any]] = None
    bbox: Optional[List[float]] = None
    properties: Optional[Dict[str, Any]] = None
    simplification_factor: Optional[float] = None
    simplification_mode: Optional[str] = None


class GeoidCollection(BaseModel):
    """Collection of geoid lookup results."""
    type: str = "GeoidCollection"
    results: List[GeoidResult] = Field(default_factory=list)
    numberReturned: Optional[int] = None


__all__ = ["GeoidCollection", "GeoidResult", "GeoidSearchBody"]
