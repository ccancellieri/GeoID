"""
STAC-compliant search models based on the STAC API Item Search OpenAPI specification.
"""
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field


class SearchBody(BaseModel):
    """STAC API Item Search request body (POST /search)."""
    
    q: Optional[str] = Field(
        None,
        description="Free-text query over id, title, description, and all item properties."
    )
    catalog_id: Optional[str] = Field(
        default=None,
        description="Restrict search to items in a single catalog."
    )
    ids: Optional[List[str]] = Field(
        None,
        description="Array of Item ids to return."
    )
    collections: Optional[List[str]] = Field(
        None,
        description="Array of Collection IDs. Only Items in these collections will be searched."
    )
    bbox: Optional[List[float]] = Field(
        None,
        min_length=4,
        max_length=6,
        description="Bounding box [min_lon, min_lat, max_lon, max_lat]."
    )
    datetime: Optional[str] = Field(
        None,
        description=(
            "Date-time or interval. RFC 3339. E.g. '2021-01-01T00:00:00Z' or "
            "'2021-01-01T00:00:00Z/2021-12-31T23:59:59Z'. Open intervals via '..'."
        )
    )
    intersects: Optional[Dict[str, Any]] = Field(
        default=None,
        description="GeoJSON geometry. Only Items whose geometry intersects are returned."
    )
    limit: int = Field(
        10, ge=1, le=10_000,
        description="Maximum number of Items to return."
    )
    token: Optional[str] = Field(
        None,
        description="Pagination token (search_after value) returned in previous response links."
    )
    sortby: Optional[str] = Field(
        None,
        description=(
            "Sort field. Prefix with '+' for ascending, '-' for descending. "
            "E.g. '+properties.title', '-properties.datetime'. "
            "Sortable fields: 'id', 'properties.title', 'properties.datetime', 'properties.updated'."
        )
    )


class CatalogSearchBody(BaseModel):
    """Body for catalog or collection keyword search."""
    q: Optional[str] = Field(None, description="Free-text query over id, title, description.")
    ids: Optional[List[str]] = Field(None, description="Array of IDs to return.")
    catalog_id: Optional[str] = Field(default=None, description="Restrict to a single catalog.")
    limit: int = Field(10, ge=1, le=10_000)
    token: Optional[str] = Field(None, description="Pagination cursor token.")
    sortby: Optional[str] = Field(
        None,
        description=(
            "Sort field. Prefix with '+' for ascending, '-' for descending. "
            "Aliases: 'code'=id, 'label'=title. "
            "E.g. '+code', '-label'."
        ),
    )
    lang: Optional[str] = Field(
        None,
        description="Language code for multilingual sort (e.g. 'en', 'fr'). Default: 'en'.",
    )


class SearchLink(BaseModel):
    rel: str
    href: str
    type: Optional[str] = None
    title: Optional[str] = None
    method: Optional[str] = None
    body: Optional[Dict[str, Any]] = None
    merge: Optional[bool] = None


class ItemCollection(BaseModel):
    """STAC FeatureCollection returned by the search endpoint."""
    type: str = "FeatureCollection"
    features: List[Dict[str, Any]] = Field(default_factory=list)
    links: List[SearchLink] = Field(default_factory=list)
    numberMatched: Optional[int] = None
    numberReturned: Optional[int] = None


class GenericCollection(BaseModel):
    """Generic collection of catalog/collection records returned by the search endpoint."""
    type: str = "EntityCollection"
    entities: List[Dict[str, Any]] = Field(default_factory=list)
    links: List[SearchLink] = Field(default_factory=list)
    numberReturned: Optional[int] = None


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
