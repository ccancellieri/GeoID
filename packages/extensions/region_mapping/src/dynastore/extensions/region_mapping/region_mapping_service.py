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

"""region_mapping — a detached, non-OGC CRUD API + TerriaJS serving surface
for WMS region-mapping claims (dynastore#443, dynastore#448, dynastore#2821).

Persistence is a dedicated ``region_mapping.mappings`` table (one row per
claim), provisioned once at extension-lifespan startup -- see
``registry_queries.ensure_mappings_table``. This replaced the earlier
``_region_mappings_`` RECORDS-catalog registry provisioned by two presets
(dynastore#443 Phase 1): the preset/RECORDS/routing/ItemsSchema machinery
was disproportionate to managing one small claims table, and its read path
had an open bug (dynastore#448 Status).

Routes:

* ``POST   /region-mappings``                       -- register/re-apply a mapping
* ``DELETE /region-mappings/{mapping_id}``           -- revoke a mapping (all its claims)
* ``GET    /region-mappings``                        -- list claim rows (CQL2 filter)
* ``GET    /region-mappings/definitions``            -- ``{"regionWmsMap": {...}}``
* ``GET    /region-mappings/{mapping_id}/regionIds``  -- sorted distinct values

REGISTRATION IS PUBLICATION -- unchanged from dynastore#443. Applying a
mapping via ``POST`` is an explicit decision to publish, to anyone who can
reach these routes, the claimed column's distinct values, the source
collection's bbox/title, and its tile URL. There is no separate visibility
check against the source collection's own access posture.

No IAM policy is registered here -- this extension imports nothing from
``AuthorizationProtocol``. On a deployment without the IAM module (e.g.
dev's ``scope_catalog``, which ships without it) every route is open by
default. On an IAM-enabled deployment nothing here is public until an
operator explicitly grants access to ``/region-mappings/.*`` (reads, and
now writes) -- the same way they would grant any other protected route;
this extension does not assume or automate that decision.
"""
from __future__ import annotations

import logging
from collections import OrderedDict
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, FastAPI, HTTPException, Query, Request, status
from pydantic import BaseModel, ConfigDict, Field

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.url import get_root_url
from dynastore.models.protocols.catalogs import CatalogsProtocol
from dynastore.modules.tools.cql import parse_cql_filter
from dynastore.tools.discovery import get_protocol
from dynastore.tools.protocol_helpers import get_engine

from . import registry_queries as _q
from . import registry_store as _store
from .claims import fetch_collection_bbox, fetch_distinct_region_ids
from .templates import render_definitions

logger = logging.getLogger(__name__)

_DEFAULT_LIMIT = 200


# ---------------------------------------------------------------------------
# DTOs
# ---------------------------------------------------------------------------


class RegisterMappingRequest(BaseModel):
    """Body of ``POST /region-mappings``."""

    model_config = ConfigDict(extra="forbid")

    catalog: str = Field(..., description="Source catalog id.")
    collection: str = Field(..., description="Source collection id.")
    column: str = Field(
        ...,
        description=(
            "Source collection property carrying the region-id values "
            "TerriaJS should join against."
        ),
    )
    alias: Optional[str] = Field(
        default=None,
        description="Canonical alias TerriaJS matches this region type against. Defaults to 'column'.",
    )
    extra_aliases: List[str] = Field(
        default_factory=list, description="Additional alias strings TerriaJS should also accept.",
    )
    title: Optional[str] = Field(
        default=None, description="Human-readable description; defaults to the collection id.",
    )


class ClaimOut(BaseModel):
    """One claim row."""

    model_config = ConfigDict(from_attributes=True)

    claim_ci: str
    claim: str
    mapping_id: str
    role: str
    src_catalog: str
    src_collection: str
    region_prop: str
    alias: Optional[str] = None
    title: Optional[str] = None


class RegisterMappingResponse(BaseModel):
    mapping_id: str
    claims: List[ClaimOut]


class ClaimListResponse(BaseModel):
    items: List[ClaimOut]
    limit: int
    offset: int


# ---------------------------------------------------------------------------
# CQL2 helper
# ---------------------------------------------------------------------------


def _parse_cql(filter_text: Optional[str]) -> tuple[str, Dict[str, Any]]:
    """Parse a CQL2-Text ``filter=`` value into a WHERE fragment + bind
    params, restricted to :data:`registry_queries.ALLOWED_COLUMNS`.

    Raises ``HTTPException(400)`` for an invalid filter or an unknown
    property (``parse_cql_filter`` validates against ``valid_props``, which
    defaults to the field mapping's keys).
    """
    if not filter_text:
        return "", {}
    try:
        return parse_cql_filter(
            filter_text, field_mapping=_q.build_cql_field_mapping(), parser_type="cql2",
        )
    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve)) from ve
    except ImportError as e:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail=f"CQL filtering is not available on this server. ({e})",
        ) from e


# ---------------------------------------------------------------------------
# /definitions rendering
# ---------------------------------------------------------------------------


async def _build_definitions(
    request: Request,
    *,
    catalog: Optional[str],
    collection: Optional[str],
    alias: Optional[str],
    cql_where: str,
    cql_params: Dict[str, Any],
    limit: int,
    offset: int,
) -> Dict[str, Any]:
    alias_ci = alias.strip().casefold() if alias else None

    if cql_where:
        # A caller-supplied CQL2 filter always reads through uncached; the
        # alias exact-match (when also given) composes as an extra equality
        # predicate rather than a separate code path.
        records = await _store.list_claims(
            role=None if alias_ci else "primary",
            src_catalog=catalog, src_collection=collection, claim_ci=alias_ci,
            cql_where=cql_where, cql_params=cql_params,
            limit=_store.DEFINITIONS_FETCH_CAP, offset=0,
        )
    else:
        records = await _store.fetch_primary_records(catalog, collection, alias_ci)

    # De-dup by mapping_id, preserving the query's sort order, then paginate
    # over MAPPINGS (not claim rows).
    by_mapping: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
    for record in records:
        mapping_id = record.get("mapping_id")
        if mapping_id and mapping_id not in by_mapping:
            by_mapping[mapping_id] = record

    page = list(by_mapping.items())[offset: offset + limit]
    base_url = get_root_url(request)

    entries = []
    for mapping_id, record in page:
        claim_records = await _store.fetch_claims_for_mapping(mapping_id)
        aliases = sorted({c["claim"] for c in claim_records if c.get("claim")})

        src_catalog = record.get("src_catalog", "")
        src_collection = record.get("src_collection", "")
        region_prop = record.get("region_prop", "")
        title = record.get("title") or src_collection

        bbox = await fetch_collection_bbox(src_catalog, src_collection)

        entries.append({
            "key": f"{src_catalog}_{src_collection}".upper(),
            "server": (
                f"{base_url}/maps/tiles/catalogs/{src_catalog}/tiles/"
                f"{{z}}/{{x}}/{{y}}.mvt?collections={src_collection}"
            ),
            "region_prop": region_prop,
            "aliases": aliases,
            "title": title,
            "region_ids_file": f"{base_url}/region-mappings/{mapping_id}/regionIds",
            "bbox": bbox,
        })
    return render_definitions(entries)


class RegionMappingService(ExtensionProtocol):
    """Detached region-mapping CRUD + TerriaJS serving API."""

    priority: int = 200

    router: APIRouter = APIRouter(prefix="/region-mappings", tags=["Region Mapping"])

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        try:
            await _q.ensure_mappings_table(get_engine())
        except Exception as exc:  # noqa: BLE001 -- never abort app startup
            logger.error(
                "RegionMappingService: failed to provision the %s.%s table: %s. "
                "region_mapping will be unavailable until the next boot.",
                _q.SCHEMA, _q.TABLE, exc,
            )
        yield

    # -------------------------------------------------------------------
    # POST /region-mappings
    # -------------------------------------------------------------------

    @router.post(
        "",
        status_code=status.HTTP_201_CREATED,
        summary="Claim a collection's region-id column for TerriaJS WMS region mapping.",
    )
    async def register_mapping(payload: RegisterMappingRequest) -> RegisterMappingResponse:  # type: ignore[reportGeneralTypeIssues]
        """Register (or idempotently re-apply) one mapping's claim set.

        A claim already owned by a *different* mapping is a genuine PG
        ``23505`` -> HTTP 409 (see ``registry_store.apply_mapping``); a
        re-apply of the SAME mapping is an idempotent update and
        self-cleaning (stale claims from a changed alias set are deleted).
        """
        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            raise HTTPException(status_code=503, detail="Catalogs service not available.")
        engine = get_engine()
        if engine is None:
            raise HTTPException(status_code=503, detail="Database engine not available.")

        collection = await catalogs.get_collection(payload.catalog, payload.collection)
        if collection is None:
            raise HTTPException(
                status_code=404,
                detail=f"Collection {payload.catalog!r}/{payload.collection!r} not found.",
            )

        try:
            mapping_id, claim_rows = await _store.apply_mapping(
                engine,
                catalog_id=payload.catalog, collection_id=payload.collection,
                column=payload.column, alias=payload.alias,
                extra_aliases=tuple(payload.extra_aliases), title=payload.title,
            )
        except ValueError as ve:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve)) from ve

        return RegisterMappingResponse(
            mapping_id=mapping_id,
            claims=[ClaimOut.model_validate(row) for row in claim_rows],
        )

    # -------------------------------------------------------------------
    # DELETE /region-mappings/{mapping_id}
    # -------------------------------------------------------------------

    @router.delete(
        "/{mapping_id}",
        summary="Revoke a mapping -- deletes every claim currently sharing mapping_id.",
    )
    async def revoke_mapping(mapping_id: str) -> Dict[str, Any]:  # type: ignore[reportGeneralTypeIssues]
        engine = get_engine()
        if engine is None:
            raise HTTPException(status_code=503, detail="Database engine not available.")
        try:
            deleted = await _store.delete_mapping(engine, mapping_id)
        except _store.MappingNotFoundError as exc:
            raise HTTPException(
                status_code=404, detail=f"Region mapping {mapping_id!r} not found.",
            ) from exc
        return {"mapping_id": mapping_id, "deleted_claims": deleted}

    # -------------------------------------------------------------------
    # GET /region-mappings
    # -------------------------------------------------------------------

    @router.get("", summary="List registered region-mapping claims.")
    async def list_mappings(
        mapping_id: Optional[str] = Query(None, description="Exact mapping_id match."),  # type: ignore[reportGeneralTypeIssues]
        role: Optional[str] = Query(None, description="Exact role match ('primary' or 'alias')."),
        catalog: Optional[str] = Query(None, description="Filter to claims sourced from this catalog id."),
        collection: Optional[str] = Query(None, description="Filter to claims sourced from this collection id."),
        filter: Optional[str] = Query(  # noqa: A002 -- OGC/CQL2 query-param convention
            None, description="CQL2-Text filter expression over the claim columns.",
        ),
        limit: int = Query(_DEFAULT_LIMIT, ge=1, le=1000, description="Claims per page."),
        offset: int = Query(0, ge=0, description="Claims to skip."),
    ) -> ClaimListResponse:
        cql_where, cql_params = _parse_cql(filter)
        rows = await _store.list_claims(
            mapping_id=mapping_id, role=role, src_catalog=catalog, src_collection=collection,
            cql_where=cql_where, cql_params=cql_params, limit=limit, offset=offset,
        )
        return ClaimListResponse(
            items=[ClaimOut.model_validate(row) for row in rows], limit=limit, offset=offset,
        )

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
        filter: Optional[str] = Query(  # noqa: A002 -- OGC/CQL2 query-param convention
            None, description="CQL2-Text filter expression over the claim columns.",
        ),
        limit: int = Query(_DEFAULT_LIMIT, ge=1, le=1000, description="Mappings per page."),
        offset: int = Query(0, ge=0, description="Mappings to skip."),
    ) -> Dict[str, Any]:
        """Return ``{"regionWmsMap": {...}}`` -- one entry per registered mapping."""
        if get_protocol(CatalogsProtocol) is None:
            raise HTTPException(status_code=503, detail="Catalogs service not available.")
        cql_where, cql_params = _parse_cql(filter)
        return await _build_definitions(
            request, catalog=catalog, collection=collection, alias=alias,
            cql_where=cql_where, cql_params=cql_params, limit=limit, offset=offset,
        )

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
        record = await _store.fetch_mapping_primary(mapping_id)
        if record is None:
            raise HTTPException(
                status_code=404, detail=f"Region mapping {mapping_id!r} not found.",
            )
        region_prop = record.get("region_prop", "")
        values = await fetch_distinct_region_ids(
            record.get("src_catalog", ""), record.get("src_collection", ""), region_prop,
        )
        return {"layer": "default", "property": region_prop, "values": values}
