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

* ``POST   /region-mappings``                           -- register/re-apply a mapping
* ``DELETE /region-mappings/{mapping_id}``               -- revoke a mapping (all its claims)
* ``GET    /region-mappings``                            -- list claim rows (CQL2 filter)
* ``GET    /region-mappings/region.json``                -- ``{"regionWmsMap": {...}}``
* ``GET    /region-mappings/{mapping_id}/regionIds``      -- sorted distinct values
* ``GET    /region-mappings/{mapping_id}/regionIds.csv``  -- same values, as a downloadable CSV

REGISTRATION IS PUBLICATION -- unchanged from dynastore#443. Applying a
mapping via ``POST`` is an explicit decision to publish, to anyone who can
reach these routes, the claimed column's distinct values, the source
collection's bbox/title, and its tile URL. There is no separate visibility
check against the source collection's own access posture.

Referential integrity: a claim row is a weak reference to its source
collection, cleaned up best-effort on that collection's (or its catalog's)
hard-deletion via an async, decoupled event listener -- see ``lifecycle.py``.
The delete path itself never knows this extension exists.

No IAM policy is registered here -- this extension imports nothing from
``AuthorizationProtocol``. On a deployment without the IAM module (e.g.
dev's ``scope_catalog``, which ships without it) every route is open by
default. On an IAM-enabled deployment nothing here is public until an
operator explicitly grants access to ``/region-mappings/.*`` (reads, and
now writes) -- the same way they would grant any other protected route;
this extension does not assume or automate that decision.
"""
from __future__ import annotations

import csv
import io
import logging
from collections import OrderedDict
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlsplit, urlunsplit

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request, Response, status
from pydantic import BaseModel, ConfigDict, Field

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.language_utils import get_language
from dynastore.extensions.tools.query import resolve_queryable_property_names
from dynastore.extensions.tools.response_i18n import resolve_localized
from dynastore.extensions.tools.url import get_root_url
from dynastore.models.protocols.catalogs import CatalogsProtocol
from dynastore.models.protocols.configs import ConfigsProtocol
from dynastore.modules.tools.cql import parse_cql_filter
from dynastore.tools.discovery import get_protocol
from dynastore.tools.protocol_helpers import get_engine

from . import registry_queries as _q
from . import registry_store as _store
from .claims import (
    fetch_collection_bbox,
    fetch_distinct_region_ids,
    fetch_region_ids_by_unique_id,
)
from .config import RegionMappingConfig
from .lifecycle import register_region_mapping_cleanup_subscriber
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
    alias: str = Field(
        ...,
        description=(
            "Canonical alias TerriaJS's region-mapping config matches CSV "
            "column names against (its regionProp/uniqueIdProp). Required: "
            "TerriaJS always declares one, and the raw column name is often "
            "a database-internal identifier a CSV header would not use, so "
            "there is no safe default."
        ),
    )
    extra_aliases: List[str] = Field(
        default_factory=list, description="Additional alias strings TerriaJS should also accept.",
    )
    title: Optional[Union[str, Dict[str, str]]] = Field(
        default=None,
        description=(
            "TerriaJS's regionWmsMap 'description' -- used in GUI elements and "
            "error messages. Defaults to the collection id. A plain string is "
            "stored under the request's language (see the `lang` query "
            "parameter, same convention as the rest of the platform); a "
            "{lang: text} dict is stored as given."
        ),
    )
    layer_name: str = Field(
        default="default",
        description="TerriaJS layerName -- the tile layer within this mapping's server.",
    )
    server_type: str = Field(
        default="MVT", description="TerriaJS serverType, e.g. 'MVT' or 'WMS'.",
    )
    server_subdomains: List[str] = Field(
        default_factory=list,
        description="TerriaJS serverSubdomains, for {s}-templated tile server URLs.",
    )
    server_min_zoom: int = Field(default=0, ge=0, description="TerriaJS serverMinZoom.")
    server_max_native_zoom: int = Field(
        default=12, ge=0, description="TerriaJS serverMaxNativeZoom.",
    )
    server_max_zoom: int = Field(default=28, ge=0, description="TerriaJS serverMaxZoom.")
    unique_id_prop: Optional[str] = Field(
        default=None,
        description=(
            "TerriaJS uniqueIdProp: a numeric, zero-based, sequential feature "
            "index attribute used for positional row lookups -- NOT the region "
            "code (`column`/regionProp). Defaults to `FID` if not given."
        ),
    )
    digits: int = Field(
        default=255,
        description="TerriaJS digits -- left-zero-pad width for numeric region codes.",
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
    title: Optional[Any] = None
    layer_name: Optional[str] = None
    server_type: Optional[str] = None
    server_subdomains: Optional[List[str]] = None
    server_min_zoom: Optional[int] = None
    server_max_native_zoom: Optional[int] = None
    server_max_zoom: Optional[int] = None
    unique_id_prop: Optional[str] = None
    digits: Optional[int] = None


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
# CSV formula-injection guard
# ---------------------------------------------------------------------------


_CSV_FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")


def _escape_csv_cell(value: str) -> str:
    """Neutralize CSV/formula injection (OWASP): a cell whose first
    character would be interpreted as a formula trigger by Excel/Sheets
    gets a leading single quote so it opens as literal text instead.
    """
    if value.startswith(_CSV_FORMULA_PREFIXES):
        return f"'{value}"
    return value


# ---------------------------------------------------------------------------
# /definitions rendering
# ---------------------------------------------------------------------------


def _default_maps_base_url(catalog_base_url: str) -> str:
    """Sibling-path fallback when ``RegionMappingConfig.maps_base_url`` is unset.

    Swaps this request's own last gateway-path segment for ``maps``, e.g.
    ``.../api/catalog`` -> ``.../api/maps``. Only correct when the deployment
    follows that convention; set the config explicitly otherwise.
    """
    scheme, netloc, path, _, _ = urlsplit(catalog_base_url)
    segments = path.rstrip("/").split("/")
    if segments:
        segments[-1] = "maps"
    return urlunsplit((scheme, netloc, "/".join(segments), "", ""))


async def _get_maps_base_url(base_url: str) -> str:
    configs = get_protocol(ConfigsProtocol)
    config = await configs.get_config(RegionMappingConfig) if configs else RegionMappingConfig()
    return config.maps_base_url or _default_maps_base_url(base_url)


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
    language: str,
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
    maps_base_url = await _get_maps_base_url(base_url)

    entries = []
    for mapping_id, record in page:
        claim_records = await _store.fetch_claims_for_mapping(mapping_id)
        aliases = sorted({c["claim"] for c in claim_records if c.get("claim")})

        src_catalog = record.get("src_catalog", "")
        src_collection = record.get("src_collection", "")
        region_prop = record.get("region_prop", "")
        title = resolve_localized(record.get("title"), language) or src_collection

        bbox = await fetch_collection_bbox(src_catalog, src_collection)

        entries.append({
            "key": f"{src_catalog}_{src_collection}",
            "server": (
                f"{maps_base_url}/tiles/catalogs/{src_catalog}/tiles/"
                f"{{z}}/{{x}}/{{y}}.mvt?collections={src_collection}"
            ),
            "region_prop": region_prop,
            "aliases": aliases,
            "title": title,
            "region_ids_file": f"{base_url}/region-mappings/{mapping_id}/regionIds",
            "bbox": bbox,
            "layer_name": record.get("layer_name") or "default",
            "server_type": record.get("server_type") or "MVT",
            "server_subdomains": record.get("server_subdomains") or [],
            "server_min_zoom": record.get("server_min_zoom", 0),
            "server_max_native_zoom": record.get("server_max_native_zoom", 12),
            "server_max_zoom": record.get("server_max_zoom", 28),
            "unique_id_prop": record.get("unique_id_prop") or "FID",
            "digits": record.get("digits", 255),
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
        register_region_mapping_cleanup_subscriber()
        yield

    # -------------------------------------------------------------------
    # POST /region-mappings
    # -------------------------------------------------------------------

    @router.post(
        "",
        status_code=status.HTTP_201_CREATED,
        summary="Claim a collection's region-id column for TerriaJS WMS region mapping.",
    )
    async def register_mapping(
        payload: RegisterMappingRequest,  # type: ignore[reportGeneralTypeIssues]
        lang: str = Depends(get_language),
    ) -> RegisterMappingResponse:
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

        valid_names = await resolve_queryable_property_names(payload.catalog, payload.collection)
        if valid_names and payload.column not in valid_names:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"Property {payload.column!r} is not a queryable property of "
                    f"{payload.catalog!r}/{payload.collection!r} -- see its "
                    "'/queryables' for the supported names."
                ),
            )

        try:
            mapping_id, claim_rows = await _store.apply_mapping(
                engine,
                catalog_id=payload.catalog, collection_id=payload.collection,
                column=payload.column, alias=payload.alias,
                extra_aliases=tuple(payload.extra_aliases), title=payload.title, lang=lang,
                layer_name=payload.layer_name, server_type=payload.server_type,
                server_subdomains=payload.server_subdomains,
                server_min_zoom=payload.server_min_zoom,
                server_max_native_zoom=payload.server_max_native_zoom,
                server_max_zoom=payload.server_max_zoom,
                unique_id_prop=payload.unique_id_prop, digits=payload.digits,
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
        language: str = Depends(get_language),
    ) -> ClaimListResponse:
        cql_where, cql_params = _parse_cql(filter)
        rows = await _store.list_claims(
            mapping_id=mapping_id, role=role, src_catalog=catalog, src_collection=collection,
            cql_where=cql_where, cql_params=cql_params, limit=limit, offset=offset,
        )
        items = []
        for row in rows:
            row = dict(row)
            row["title"] = resolve_localized(row.get("title"), language)
            items.append(ClaimOut.model_validate(row))
        return ClaimListResponse(items=items, limit=limit, offset=offset)

    # -------------------------------------------------------------------
    # GET /region-mappings/region.json
    # -------------------------------------------------------------------

    @router.get(
        "/region.json",
        summary="TerriaJS regionMapping.json-shaped WMS region definitions.",
    )
    async def get_region_json(
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
        language: str = Depends(get_language),
    ) -> Dict[str, Any]:
        """Return ``{"regionWmsMap": {...}}`` -- one entry per registered mapping."""
        if get_protocol(CatalogsProtocol) is None:
            raise HTTPException(status_code=503, detail="Catalogs service not available.")
        cql_where, cql_params = _parse_cql(filter)
        return await _build_definitions(
            request, catalog=catalog, collection=collection, alias=alias,
            cql_where=cql_where, cql_params=cql_params, limit=limit, offset=offset,
            language=language,
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
        unique_id_prop = record.get("unique_id_prop") or "FID"
        values = await fetch_region_ids_by_unique_id(
            record.get("src_catalog", ""), record.get("src_collection", ""),
            region_prop, unique_id_prop,
        )
        return {"layer": "default", "property": region_prop, "values": values}

    # -------------------------------------------------------------------
    # GET /region-mappings/{mapping_id}/regionIds.csv
    # -------------------------------------------------------------------

    @router.get(
        "/{mapping_id}/regionIds.csv",
        summary="Downloadable CSV template of this mapping's region-id values.",
    )
    async def get_region_ids_csv(mapping_id: str):  # type: ignore[reportGeneralTypeIssues]
        """One column of every distinct region-id value, headed by the
        mapping's alias -- the exact CSV column name TerriaJS matches
        against. A ready-to-fill template for testing/seeding a
        region-mapped CSV catalog item, built from the same live values as
        ``GET .../regionIds``.
        """
        if get_protocol(CatalogsProtocol) is None:
            raise HTTPException(status_code=503, detail="Catalogs service not available.")
        record = await _store.fetch_mapping_primary(mapping_id)
        if record is None:
            raise HTTPException(
                status_code=404, detail=f"Region mapping {mapping_id!r} not found.",
            )
        header = record.get("alias") or record.get("region_prop") or "region_id"
        values = await fetch_distinct_region_ids(
            record.get("src_catalog", ""), record.get("src_collection", ""),
            record.get("region_prop", ""),
        )

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow([_escape_csv_cell(header)])
        writer.writerows([_escape_csv_cell(value)] for value in values)

        return Response(
            content=buffer.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{mapping_id}.csv"'},
        )
