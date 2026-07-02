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

"""TerriaJS region-mapping registry — read-only serving endpoints
(dynastore#443 Phase 1).

Exposes the claims registered by the ``region_mapping`` preset in
TerriaJS's ``regionMapping.json`` shape:

* ``GET /region-mappings/definitions``            — ``{"regionWmsMap": {...}}``
* ``GET /region-mappings/{mapping_id}/regionIds``  — sorted distinct values

Anonymous read for these routes is granted by the ``region_mappings_registry``
platform preset via a direct ``/region-mappings/.*`` policy bound to the
``unauthenticated`` role — these routes are not catalog-path-shaped, so the
usual ``CatalogLookupAudience`` condition handler cannot gate them (see
``presets/region_mappings_registry.py`` for the full two-part rationale).

That grant only opens the *registry* routes, not the *source* catalog's own
data — a claim registered against a private source collection must not
leak that collection's bbox/title/regionIds anonymously. Both handlers gate
per request on ``registry_data.is_catalog_public(src_catalog)`` (the same
``CatalogLookupAudience`` opt-in, checked against the source catalog this
time, not the registry): ``/definitions`` skips such a mapping entirely;
``/regionIds`` returns 404 — never 403, so a private mapping is
indistinguishable from a non-existent one.
"""
from __future__ import annotations

import logging
from collections import OrderedDict
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

from fastapi import APIRouter, FastAPI, HTTPException, Query, Request

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.url import get_root_url
from dynastore.models.protocols.catalogs import CatalogsProtocol
from dynastore.tools.discovery import get_protocol

from .registry_data import (
    fetch_claims_for_mapping,
    fetch_collection_bbox,
    fetch_distinct_region_ids,
    fetch_mapping_primary,
    fetch_primary_records,
    is_catalog_public,
)

logger = logging.getLogger(__name__)

_DEFAULT_LIMIT = 200


async def _build_region_wms_map(
    request: Request,
    *,
    catalog: Optional[str],
    collection: Optional[str],
    alias: Optional[str],
    limit: int,
    offset: int,
) -> Dict[str, Any]:
    """Build the ``regionWmsMap`` dict for ``GET /region-mappings/definitions``."""
    alias_ci = alias.strip().casefold() if alias else None
    records = await fetch_primary_records(catalog, collection, alias_ci)

    # De-dup by mapping_id, preserving the query's sort order.
    by_mapping: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
    for record in records:
        mapping_id = record.get("mapping_id")
        if mapping_id and mapping_id not in by_mapping:
            by_mapping[mapping_id] = record

    # Serve-time visibility gate — registering a claim does not make the
    # SOURCE catalog's data public; only that catalog's own
    # CatalogLookupAudience opt-in does (see the module docstring). Applied
    # before pagination so limit/offset count over the set the caller can
    # actually see.
    visible: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
    for mapping_id, record in by_mapping.items():
        if await is_catalog_public(record.get("src_catalog", "")):
            visible[mapping_id] = record

    page = list(visible.items())[offset: offset + limit]
    base_url = get_root_url(request)

    region_wms_map: Dict[str, Any] = {}
    for mapping_id, record in page:
        claim_records = await fetch_claims_for_mapping(mapping_id)
        aliases = sorted({c["claim"] for c in claim_records if c.get("claim")})

        src_catalog = record.get("src_catalog", "")
        src_collection = record.get("src_collection", "")
        region_prop = record.get("region_prop", "")
        title = record.get("title") or src_collection

        bbox = await fetch_collection_bbox(src_catalog, src_collection)

        key = f"{src_catalog}_{src_collection}".upper()
        region_wms_map[key] = {
            "layerName": "default",
            "server": (
                f"{base_url}/maps/tiles/catalogs/{src_catalog}/tiles/"
                f"{{z}}/{{x}}/{{y}}.mvt?collections={src_collection}"
            ),
            "serverType": "MVT",
            "serverSubdomains": [],
            "serverMinZoom": 0,
            "serverMaxNativeZoom": 12,
            "serverMaxZoom": 28,
            "regionProp": region_prop,
            "uniqueIdProp": region_prop,
            "aliases": aliases,
            "digits": 255,
            "description": title,
            "regionIdsFile": f"{base_url}/region-mappings/{mapping_id}/regionIds",
            "bbox": bbox,
        }
    return region_wms_map


class RegionMappingService(ExtensionProtocol):
    """Read-only TerriaJS region-mapping serving endpoints."""

    priority: int = 200

    router: APIRouter = APIRouter(prefix="/region-mappings", tags=["Region Mapping"])

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        yield

    # -------------------------------------------------------------------
    # GET /region-mappings/definitions
    # -------------------------------------------------------------------

    @router.get(
        "/definitions",
        summary="TerriaJS regionMapping.json-shaped WMS region definitions.",
    )
    async def get_definitions(
        request: Request,  # type: ignore[reportGeneralTypeIssues]
        catalog: Optional[str] = Query(
            None, description="Filter to mappings sourced from this catalog id.",
        ),
        collection: Optional[str] = Query(
            None, description="Filter to mappings sourced from this collection id.",
        ),
        alias: Optional[str] = Query(
            None, description="Exact, case-insensitive claim/alias match.",
        ),
        limit: int = Query(_DEFAULT_LIMIT, ge=1, le=1000, description="Mappings per page."),
        offset: int = Query(0, ge=0, description="Mappings to skip."),
    ) -> Dict[str, Any]:
        """Return ``{"regionWmsMap": {...}}`` — one entry per registered mapping."""
        if get_protocol(CatalogsProtocol) is None:
            raise HTTPException(status_code=503, detail="Catalogs service not available.")
        region_wms_map = await _build_region_wms_map(
            request, catalog=catalog, collection=collection, alias=alias,
            limit=limit, offset=offset,
        )
        return {"regionWmsMap": region_wms_map}

    # -------------------------------------------------------------------
    # GET /region-mappings/{mapping_id}/regionIds
    # -------------------------------------------------------------------

    @router.get(
        "/{mapping_id}/regionIds",
        summary="Sorted distinct region id values for a registered mapping.",
    )
    async def get_region_ids(mapping_id: str):  # type: ignore[reportGeneralTypeIssues]
        """Return ``{"layer", "property", "values"}`` for TerriaJS's regionIds fetch."""
        if get_protocol(CatalogsProtocol) is None:
            raise HTTPException(status_code=503, detail="Catalogs service not available.")
        record = await fetch_mapping_primary(mapping_id)
        if record is None:
            raise HTTPException(
                status_code=404, detail=f"Region mapping {mapping_id!r} not found.",
            )
        # Same visibility gate as /definitions — a private source catalog's
        # mapping is 404, not 403, so it is indistinguishable from a
        # non-existent one (no existence disclosure).
        if not await is_catalog_public(record.get("src_catalog", "")):
            raise HTTPException(
                status_code=404, detail=f"Region mapping {mapping_id!r} not found.",
            )
        region_prop = record.get("region_prop", "")
        values = await fetch_distinct_region_ids(
            record.get("src_catalog", ""), record.get("src_collection", ""), region_prop,
        )
        return {"layer": "default", "property": region_prop, "values": values}
