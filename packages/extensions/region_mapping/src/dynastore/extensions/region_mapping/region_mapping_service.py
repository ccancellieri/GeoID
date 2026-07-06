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
* ``GET    /region-mappings/region.json``                -- ``{"regionWmsMap": {...}}``,
  silently excluding any mapping whose regionProp/uniqueIdProp don't identify
  one feature per code (see ``/validate`` below)
* ``GET    /region-mappings/{mapping_id}/validate``       -- reasons a mapping is (not) sound
* ``GET    /region-mappings/{mapping_id}/regionIds``      -- feature-positional values (TerriaJS's regionIdsFile)
* ``GET    /region-mappings/{mapping_id}/regionIds.csv``  -- sorted distinct values, as a downloadable CSV

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
    FALLBACK_UNIQUE_ID_PROP,
    evaluate_mapping_soundness,
    fetch_collection_bbox,
    fetch_distinct_region_ids,
    fetch_region_ids_by_unique_id,
    fetch_region_mapping_cardinality,
    mapping_id_for,
    resolve_collection_columns,
    resolve_unique_id_prop,
    slugify,
    uncached_if,
)
from .config import RegionMappingConfig
from .lifecycle import register_region_mapping_cleanup_subscriber
from .templates import render_definitions

logger = logging.getLogger(__name__)

_DEFAULT_LIMIT = 200

_NO_CACHE_DESC = (
    "Bypass the per-pod read cache and read straight through to the database "
    "for this request. Use to see writes immediately (the serving reads are "
    "cached per instance, so a fresh registration can lag on other pods)."
)


# ---------------------------------------------------------------------------
# DTOs
# ---------------------------------------------------------------------------


class RegionMappingRequest(BaseModel):
    """Body of ``POST /region-mappings`` -- one region.json ``regionWmsMap``
    entry, plus the ``catalog``/``collection`` naming its source.

    The fields are the region.json single-mapping object the server itself
    emits at ``/region-mappings/region.json``: registering a mapping is
    declaring that object. ``region_prop`` and every ``alias`` must be a
    globally unique CSV-header identifier (a duplicate is HTTP 409); the
    source collection must expose a columnar ``items_schema`` and both
    ``region_prop`` and the resolved ``unique_id_prop`` must be declared
    columns of it (otherwise HTTP 400).
    """

    model_config = ConfigDict(extra="forbid")

    id: Optional[str] = Field(
        default=None,
        description=(
            "Optional caller-chosen mapping id -- the '/{mapping_id}/...' path "
            "segment for this mapping's regionIds/validate routes, and its key "
            "on the write/list surfaces. Must be a URL-safe slug "
            "([a-z0-9_-], case-insensitive). Defaults to the "
            "'{catalog}_{collection}' slug. Re-posting the same id for the same "
            "source collection is an idempotent update; a different collection "
            "reusing an id is HTTP 409."
        ),
    )
    catalog: str = Field(..., description="Source catalog id.")
    collection: str = Field(..., description="Source collection id.")
    region_prop: str = Field(
        ...,
        description=(
            "The tile property (source collection column) carrying the region "
            "code TerriaJS joins CSV rows against -- region.json 'regionProp'. "
            "Must be a declared column of the collection's items_schema, and "
            "unique across all registered mappings."
        ),
    )
    aliases: List[str] = Field(
        default_factory=list,
        description=(
            "Additional CSV column headers TerriaJS accepts for this layer "
            "(region.json 'aliases'). Each must be unique across all "
            "registered mappings. region_prop is always matchable on its own; "
            "aliases add friendlier header names."
        ),
    )
    unique_id_prop: Optional[str] = Field(
        default=None,
        description=(
            "region.json 'uniqueIdProp': the per-feature index column the "
            "positional regionIds array is keyed by -- NOT the region code. "
            "When omitted, resolves to the collection's external_id column if "
            "one is configured (its values survive a feature's versions), else "
            "'FID'. The resolved value must be a declared column."
        ),
    )
    title: Optional[Union[str, Dict[str, str]]] = Field(
        default=None,
        description=(
            "region.json 'description' -- used in GUI elements and error "
            "messages. Defaults to the collection id. A plain string is stored "
            "under the request's language (`lang` query parameter); a "
            "{lang: text} dict is stored as given."
        ),
    )
    layer_name: str = Field(
        default="default",
        description="region.json 'layerName' -- the tile layer within this mapping's server.",
    )
    server_type: str = Field(
        default="MVT", description="region.json 'serverType', e.g. 'MVT' or 'WMS'.",
    )
    server_subdomains: List[str] = Field(
        default_factory=list,
        description="region.json 'serverSubdomains', for {s}-templated tile server URLs.",
    )
    server_min_zoom: int = Field(default=0, ge=0, description="region.json 'serverMinZoom'.")
    server_max_native_zoom: int = Field(
        default=12, ge=0, description="region.json 'serverMaxNativeZoom'.",
    )
    server_max_zoom: int = Field(default=28, ge=0, description="region.json 'serverMaxZoom'.")
    digits: int = Field(
        default=255,
        description="region.json 'digits' -- left-zero-pad width for numeric region codes.",
    )


class RegionMappingOut(BaseModel):
    """One registered mapping, as the region.json single-object shape.

    The public representation of a mapping on every write/read surface -- the
    internal one-row-per-claim storage never surfaces. ``aliases`` is the
    mapping's alias set (everything TerriaJS matches besides ``region_prop``).
    """

    model_config = ConfigDict(from_attributes=True)

    mapping_id: str
    catalog: str
    collection: str
    region_prop: str
    unique_id_prop: str
    aliases: List[str] = Field(default_factory=list)
    title: Optional[Any] = None
    layer_name: str = "default"
    server_type: str = "MVT"
    server_subdomains: List[str] = Field(default_factory=list)
    server_min_zoom: int = 0
    server_max_native_zoom: int = 12
    server_max_zoom: int = 28
    digits: int = 255


class RegionMappingListResponse(BaseModel):
    items: List[RegionMappingOut]
    limit: int
    offset: int


class MappingValidationResponse(BaseModel):
    mapping_id: str
    valid: bool
    reasons: List[str]
    catalog: Optional[str] = None
    collection: Optional[str] = None
    region_prop: Optional[str] = None
    unique_id_prop: Optional[str] = None
    feature_count: int = 0
    distinct_region_count: int = 0
    distinct_unique_id_count: int = 0
    null_unique_id_count: int = 0


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
    no_cache: bool = False,
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
        records = await uncached_if(_store.fetch_primary_records, no_cache)(
            catalog, collection, alias_ci,
        )

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
        src_catalog = record.get("src_catalog", "")
        src_collection = record.get("src_collection", "")
        region_prop = record.get("region_prop", "")
        unique_id_prop = record.get("unique_id_prop") or FALLBACK_UNIQUE_ID_PROP

        reasons, _stats = await evaluate_mapping_soundness(
            src_catalog, src_collection, region_prop, unique_id_prop, no_cache=no_cache,
        )
        if reasons:
            logger.warning(
                "region_mapping: excluding %r from region.json -- %s",
                mapping_id, "; ".join(reasons),
            )
            continue

        title = resolve_localized(
            _store.coerce_stored_title(record.get("title")), language,
        ) or src_collection

        claim_records = await uncached_if(_store.fetch_claims_for_mapping, no_cache)(mapping_id)
        region_prop_ci = region_prop.casefold()
        aliases = sorted({
            c["claim"] for c in claim_records
            if c.get("claim") and c["claim"].casefold() != region_prop_ci
        })

        bbox = await uncached_if(fetch_collection_bbox, no_cache)(src_catalog, src_collection)

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
            "unique_id_prop": unique_id_prop,
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
        summary="Register a collection's region column for TerriaJS WMS region mapping.",
    )
    async def register_mapping(
        payload: RegionMappingRequest,  # type: ignore[reportGeneralTypeIssues]
        lang: str = Depends(get_language),
    ) -> RegionMappingOut:
        """Register (or idempotently re-apply) one region mapping.

        Validates before writing: the collection must exist and expose a
        columnar ``items_schema`` (JSONB-only collections are refused -- their
        attributes have no fixed columns to back a region layer), and both
        ``region_prop`` and the resolved ``unique_id_prop`` must be declared
        columns of it. ``region_prop`` or an ``alias`` already claimed by a
        *different* mapping is a PG ``23505`` -> HTTP 409; re-applying the SAME
        mapping is idempotent and self-cleaning (stale aliases are dropped).
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

        cols = await resolve_collection_columns(payload.catalog, payload.collection)
        if cols is None or not cols.is_columnar or not cols.declared:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"Collection {payload.catalog!r}/{payload.collection!r} has no columnar "
                    "items_schema. Region mapping requires a declared physical schema: JSONB "
                    "attributes have no fixed columns, so a region column cannot be guaranteed "
                    "to exist or share a type across features. Re-ingest the collection with a "
                    "declared items_schema and retry."
                ),
            )
        if not cols.has_column(payload.region_prop):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"regionProp {payload.region_prop!r} is not a declared column of "
                    f"{payload.catalog!r}/{payload.collection!r}'s items_schema."
                ),
            )
        unique_id_prop = resolve_unique_id_prop(
            payload.unique_id_prop, cols.external_id_path,
            cols.has_column(FALLBACK_UNIQUE_ID_PROP),
        )
        if unique_id_prop is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"Cannot resolve a uniqueIdProp for {payload.catalog!r}/{payload.collection!r}: "
                    "none was supplied, the collection configures no external_id source column, and "
                    f"it declares no {FALLBACK_UNIQUE_ID_PROP!r} column. Pass an explicit uniqueIdProp "
                    "naming a declared per-feature index column."
                ),
            )
        if not cols.has_column(unique_id_prop):
            hint = (
                "."
                if payload.unique_id_prop
                else (
                    f" -- resolved from the collection's external_id-source/{FALLBACK_UNIQUE_ID_PROP} "
                    "fallback. Pass an explicit uniqueIdProp naming a declared column."
                )
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"uniqueIdProp {unique_id_prop!r} is not a declared column of "
                    f"{payload.catalog!r}/{payload.collection!r}'s items_schema{hint}"
                ),
            )

        # Every feature must carry a non-null uniqueIdProp value, or it cannot be
        # positioned in the regionIds array. A mapping over an external_id/index
        # column with NULL gaps (or over an empty collection) is refused here
        # rather than silently served with holes.
        stats = await fetch_region_mapping_cardinality(
            payload.catalog, payload.collection, payload.region_prop, unique_id_prop,
        )
        if stats["feature_count"] == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"No features of {payload.catalog!r}/{payload.collection!r} carry non-null "
                    f"values for both regionProp {payload.region_prop!r} and uniqueIdProp "
                    f"{unique_id_prop!r} -- nothing to map."
                ),
            )
        if stats["null_unique_id_count"] > 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"uniqueIdProp {unique_id_prop!r} is NULL on {stats['null_unique_id_count']} "
                    f"feature(s) of {payload.catalog!r}/{payload.collection!r}. Every feature must "
                    "carry a non-null per-feature index value for a region mapping; an "
                    "external_id/index column with gaps cannot be used."
                ),
            )

        mapping_id = slugify(payload.id) if payload.id else mapping_id_for(
            payload.catalog, payload.collection,
        )
        if payload.id and not mapping_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="id must contain URL-safe characters ([a-z0-9_-]).",
            )
        existing = await _store.fetch_mapping_primary(mapping_id)
        if existing is not None and (
            existing.get("src_catalog") != payload.catalog
            or existing.get("src_collection") != payload.collection
        ):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"Region mapping id {mapping_id!r} is already used by "
                    f"{existing.get('src_catalog')!r}/{existing.get('src_collection')!r}. "
                    "Choose a different id."
                ),
            )

        try:
            _mapping_id, claim_rows = await _store.apply_mapping(
                engine,
                catalog_id=payload.catalog, collection_id=payload.collection,
                region_prop=payload.region_prop, aliases=payload.aliases,
                unique_id_prop=unique_id_prop, title=payload.title,
                mapping_id=mapping_id, lang=lang,
                layer_name=payload.layer_name, server_type=payload.server_type,
                server_subdomains=payload.server_subdomains,
                server_min_zoom=payload.server_min_zoom,
                server_max_native_zoom=payload.server_max_native_zoom,
                server_max_zoom=payload.server_max_zoom, digits=payload.digits,
            )
        except ValueError as ve:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve)) from ve

        obj = _store.mapping_object_from_claims(claim_rows, language=lang)
        return RegionMappingOut.model_validate(obj)

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

    @router.get("", summary="List registered region mappings.")
    async def list_mappings(
        mapping_id: Optional[str] = Query(None, description="Exact mapping_id match."),  # type: ignore[reportGeneralTypeIssues]
        catalog: Optional[str] = Query(None, description="Filter to mappings sourced from this catalog id."),
        collection: Optional[str] = Query(None, description="Filter to mappings sourced from this collection id."),
        filter: Optional[str] = Query(  # noqa: A002 -- OGC/CQL2 query-param convention
            None, description="CQL2-Text filter expression over the claim columns.",
        ),
        limit: int = Query(_DEFAULT_LIMIT, ge=1, le=1000, description="Mappings per page."),
        offset: int = Query(0, ge=0, description="Mappings to skip."),
        language: str = Depends(get_language),
    ) -> RegionMappingListResponse:
        """List registered mappings as region.json single-object entries,
        paginated over MAPPINGS (not the internal claim rows).

        A CQL2 ``filter=`` matches against the claim columns; a mapping is
        included when any of its claims matches, and its ``aliases`` then
        reflect only the matching claims. The default (no filter) path returns
        each mapping's complete alias set.
        """
        cql_where, cql_params = _parse_cql(filter)
        rows = await _store.list_claims(
            mapping_id=mapping_id, src_catalog=catalog, src_collection=collection,
            cql_where=cql_where, cql_params=cql_params,
            limit=_store.DEFINITIONS_FETCH_CAP, offset=0,
        )
        grouped: "OrderedDict[str, List[Dict[str, Any]]]" = OrderedDict()
        for row in rows:
            mid = row.get("mapping_id")
            if mid:
                grouped.setdefault(mid, []).append(dict(row))

        page = list(grouped.values())[offset: offset + limit]
        items = []
        for claim_rows in page:
            obj = _store.mapping_object_from_claims(claim_rows, language=language)
            if obj:
                items.append(RegionMappingOut.model_validate(obj))
        return RegionMappingListResponse(items=items, limit=limit, offset=offset)

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
        no_cache: bool = Query(False, description=_NO_CACHE_DESC),
        language: str = Depends(get_language),
    ) -> Dict[str, Any]:
        """Return ``{"regionWmsMap": {...}}`` -- one entry per registered mapping."""
        if get_protocol(CatalogsProtocol) is None:
            raise HTTPException(status_code=503, detail="Catalogs service not available.")
        cql_where, cql_params = _parse_cql(filter)
        return await _build_definitions(
            request, catalog=catalog, collection=collection, alias=alias,
            cql_where=cql_where, cql_params=cql_params, limit=limit, offset=offset,
            language=language, no_cache=no_cache,
        )

    # -------------------------------------------------------------------
    # GET /region-mappings/{mapping_id}/validate
    # -------------------------------------------------------------------

    @router.get(
        "/{mapping_id}/validate",
        summary="Diagnose whether a registered mapping is sound for TerriaJS.",
    )
    async def validate_mapping(
        mapping_id: str,  # type: ignore[reportGeneralTypeIssues]
        no_cache: bool = Query(False, description=_NO_CACHE_DESC),
    ) -> MappingValidationResponse:
        """Re-check every condition a mapping must satisfy, live against its
        source collection -- the source still exists, still has a columnar
        items_schema, still declares both ``region_prop`` and
        ``unique_id_prop`` as columns, and ``region_prop``/``unique_id_prop``
        still identify one feature per code (external_id is checked against its
        latest-version-per-id set, so legitimate versioning is not flagged).

        Any failing condition is a ``reasons`` entry; a mapping with a
        non-empty ``reasons`` is silently excluded from
        ``GET /region-mappings/region.json``, and this endpoint is how to see
        why.
        """
        record = await uncached_if(_store.fetch_mapping_primary, no_cache)(mapping_id)
        if record is None:
            raise HTTPException(
                status_code=404, detail=f"Region mapping {mapping_id!r} not found.",
            )
        catalog = record.get("src_catalog", "")
        collection = record.get("src_collection", "")
        region_prop = record.get("region_prop", "")
        unique_id_prop = record.get("unique_id_prop") or FALLBACK_UNIQUE_ID_PROP

        reasons, stats = await evaluate_mapping_soundness(
            catalog, collection, region_prop, unique_id_prop, no_cache=no_cache,
        )

        return MappingValidationResponse(
            mapping_id=mapping_id, valid=not reasons, reasons=reasons,
            catalog=catalog, collection=collection,
            region_prop=region_prop, unique_id_prop=unique_id_prop,
            feature_count=stats["feature_count"],
            distinct_region_count=stats["distinct_region_count"],
            distinct_unique_id_count=stats["distinct_unique_id_count"],
            null_unique_id_count=stats.get("null_unique_id_count", 0),
        )

    # -------------------------------------------------------------------
    # GET /region-mappings/{mapping_id}/regionIds
    # -------------------------------------------------------------------

    @router.get(
        "/{mapping_id}/regionIds",
        summary="Feature-positional region id values for TerriaJS's regionIdsFile fetch.",
    )
    async def get_region_ids(  # type: ignore[reportGeneralTypeIssues]
        mapping_id: str,
        no_cache: bool = Query(False, description=_NO_CACHE_DESC),
    ):
        """Return ``{"layer", "property", "values"}`` for TerriaJS's regionIds fetch."""
        if get_protocol(CatalogsProtocol) is None:
            raise HTTPException(status_code=503, detail="Catalogs service not available.")
        record = await uncached_if(_store.fetch_mapping_primary, no_cache)(mapping_id)
        if record is None:
            raise HTTPException(
                status_code=404, detail=f"Region mapping {mapping_id!r} not found.",
            )
        region_prop = record.get("region_prop", "")
        unique_id_prop = record.get("unique_id_prop") or FALLBACK_UNIQUE_ID_PROP
        values = await uncached_if(fetch_region_ids_by_unique_id, no_cache)(
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
    async def get_region_ids_csv(  # type: ignore[reportGeneralTypeIssues]
        mapping_id: str,
        no_cache: bool = Query(False, description=_NO_CACHE_DESC),
    ):
        """One column of every distinct region-id value, headed by the
        mapping's alias -- the exact CSV column name TerriaJS matches
        against. A ready-to-fill template for testing/seeding a
        region-mapped CSV catalog item, built from the same live values as
        ``GET .../regionIds``.
        """
        if get_protocol(CatalogsProtocol) is None:
            raise HTTPException(status_code=503, detail="Catalogs service not available.")
        record = await uncached_if(_store.fetch_mapping_primary, no_cache)(mapping_id)
        if record is None:
            raise HTTPException(
                status_code=404, detail=f"Region mapping {mapping_id!r} not found.",
            )
        header = record.get("alias") or record.get("region_prop") or "region_id"
        values = await uncached_if(fetch_distinct_region_ids, no_cache)(
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
