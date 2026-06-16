#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""STAC-compliant search models — item-only API surface (issue #819).

Per #819 the search extension now exposes only item-search endpoints.
Catalog/collection keyword search has been retired from the public router
and its bodies/response models (CatalogSearchBody, GenericCollection)
were removed along with the endpoints.

These models live in ``core`` so that the search protocol
(``dynastore.models.protocols.search``) can reference them without
importing from any extension package.
"""
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, ConfigDict, Field


class SearchBody(BaseModel):
    """STAC API Item Search request body (POST /search)."""

    # Accept both ``filter_lang`` and the STAC/OGC wire spelling ``filter-lang``.
    model_config = ConfigDict(populate_by_name=True)

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
            "Array of external_id values. Matches items whose ``external_id`` "
            "(populated from the configured ItemsWritePolicy.external_id_field) "
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
    filter: Optional[Union[Dict[str, Any], str]] = Field(
        None,
        description=(
            "CQL2 filter, ANDed with the other parameters. A JSON object is read "
            "as cql2-json; a string as cql2-text. Property names resolve against "
            "the collection's queryables (the same source of truth the items "
            "schema and write policy expose). It is translated to the resolved "
            "search backend's native query (Elasticsearch Query DSL). An unknown "
            "queryable property or an unsupported operator is rejected with 400."
        ),
    )
    filter_lang: Optional[str] = Field(
        "cql2-json",
        alias="filter-lang",
        description=(
            "Filter language for ``filter``: 'cql2-json' (default) or 'cql2-text'. "
            "Inferred from the ``filter`` JSON/string type when omitted."
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
