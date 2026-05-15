"""STAC-compliant search models — item-only API surface (issue #819).

Per #819 the search extension now exposes only item-search endpoints.
Catalog/collection keyword search has been retired from the public router
and its bodies/response models (CatalogSearchBody, GenericCollection)
were removed along with the endpoints.
"""
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class SearchBody(BaseModel):
    """STAC API Item Search request body (POST /search)."""

    q: Optional[str] = Field(
        None,
        description="Free-text query over id, title, description, and all item properties.",
    )
    catalog_id: Optional[str] = Field(
        default=None,
        description="Restrict search to items in a single catalog.",
    )
    ids: Optional[List[str]] = Field(
        None,
        description="Array of Item ids to return.",
    )
    geoid: Optional[List[str]] = Field(
        None,
        description=(
            "Array of GeoID values. Matches items whose item-document carries "
            "the given GeoID(s) — typically stored on ``properties.geoid``."
        ),
    )
    external_id: Optional[List[str]] = Field(
        None,
        description=(
            "Array of external_id values. Matches items whose ``_external_id`` "
            "(populated from the configured WriteConflictPolicy.external_id_field) "
            "equals one of the supplied values."
        ),
    )
    collections: Optional[List[str]] = Field(
        None,
        description="Array of Collection IDs. Only Items in these collections will be searched.",
    )
    bbox: Optional[List[float]] = Field(
        None,
        min_length=4,
        max_length=6,
        description="Bounding box [min_lon, min_lat, max_lon, max_lat].",
    )
    datetime: Optional[str] = Field(
        None,
        description=(
            "Date-time or interval. RFC 3339. E.g. '2021-01-01T00:00:00Z' or "
            "'2021-01-01T00:00:00Z/2021-12-31T23:59:59Z'. Open intervals via '..'."
        ),
    )
    intersects: Optional[Dict[str, Any]] = Field(
        default=None,
        description="GeoJSON geometry. Only Items whose geometry intersects are returned.",
    )
    limit: int = Field(
        10, ge=1, le=10_000,
        description="Maximum number of Items to return.",
    )
    token: Optional[str] = Field(
        None,
        description="Pagination token (search_after value) returned in previous response links.",
    )
    sortby: Optional[str] = Field(
        None,
        description=(
            "Sort field. Prefix with '+' for ascending, '-' for descending. "
            "E.g. '+properties.title', '-properties.datetime'. "
            "Sortable fields: 'id', 'properties.title', 'properties.datetime', 'properties.updated'."
        ),
    )
    driver: Optional[str] = Field(
        None,
        description=(
            "Hint: driver_ref of a SEARCH entry pinned in the catalog/collection's "
            "ItemsRoutingConfig.operations[SEARCH]. When omitted (or unknown), the "
            "first entry's driver is used. Unknown hints warn-and-fall-back to default."
        ),
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
