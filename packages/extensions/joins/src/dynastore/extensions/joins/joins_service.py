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

"""OGC API - Joins service (Phase 4b PR-1).

Ships the OGC-conformant /join/* surface alongside the existing /dwh/*
(which is NOT touched). PR-1 supports `NamedSecondarySpec` only — the
secondary must reference a registered collection.

PR-2 will add `BigQuerySecondarySpec` (per-request target overrides +
Secret-wrapped credentials) on top of this surface.
"""

from __future__ import annotations

from google.cloud import bigquery as _bigquery_scope_gate  # noqa: F401  # SCOPE gate: joins extra requires google-cloud-bigquery
_ = _bigquery_scope_gate  # silence pyright "unused" — load-bearing for SCOPE filtering

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Annotated, Any, AsyncGenerator, Dict, FrozenSet, List, Optional
from urllib.parse import urlencode, urlsplit, urlunsplit

from fastapi import APIRouter, Body, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse  # noqa: E402

from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.query import parse_hints_param  # noqa: E402
from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.joins.bq_secondary import stream_bigquery_secondary
from dynastore.modules.joins.executor import index_secondary, run_join
from dynastore.modules.joins.models import (
    BigQuerySecondarySpec,
    JoinRequest,
    NamedSecondarySpec,
    PagingSpec,
    PrimaryFilterSpec,
)
from dynastore.modules.storage.hints import Hint
from dynastore.modules.storage.router import resolve_drivers

logger = logging.getLogger(__name__)

# Default and ceiling for a single /join page, mirroring OGC API - Features
# `limit` semantics (Features Part 1, Req 19). Kept in sync with PagingSpec's
# own field bounds (default=100, le=10000).
DEFAULT_PAGE_LIMIT = 100
MAX_PAGE_LIMIT = 10_000

# Safety ceiling on the number of primary rows scanned per /join request.
# The join executor streams the primary collection without a row-based pre-cap
# (so sparse inner joins find their full quota of matched features); this
# ceiling prevents runaway scans on very large collections. When hit, the
# response still emits a `next` link so the client can continue paging.
# Operators can observe ceiling hits via the `join_scan_ceiling_hit` warning.
MAX_PRIMARY_SCAN_ROWS = 1_000_000

# GeoJSON media type for join feature collections and pagination links.
GEOJSON_MEDIA_TYPE = "application/geo+json"
# Plain JSON alternative a client can request via `Accept` (see
# `_negotiate_join_response`).
JSON_MEDIA_TYPE = "application/json"


class GeoJSONResponse(JSONResponse):
    """JSON response served with the OGC GeoJSON media type.

    The join FeatureCollection is GeoJSON, so it must be sent as
    ``application/geo+json`` (OGC API - Features Part 1 §7.15.4 /
    ``/req/core/fc-response``), not the FastAPI-default ``application/json``.
    """

    media_type = GEOJSON_MEDIA_TYPE


def _negotiate_join_response(body: Dict[str, Any], accept_header: str):
    """Honor `Accept: application/json` vs the OGC-default `geo+json`.

    Mirrors the substring `Accept` check already used elsewhere in the catalog
    for content negotiation (e.g. ``CRSService.get_crs``): the join
    FeatureCollection defaults to ``application/geo+json`` (OGC API - Features
    Part 1 §7.15.4 fc-response) for any other/absent/wildcard ``Accept``. Only
    an explicit plain-JSON request gets an explicit ``JSONResponse`` here — the
    default case returns ``body`` unwrapped and lets the route's
    ``response_class=GeoJSONResponse`` serve it.
    """
    if JSON_MEDIA_TYPE in accept_header and GEOJSON_MEDIA_TYPE not in accept_header:
        return JSONResponse(content=body)
    return body


async def _resolve_primary_driver(
    catalog_id: str,
    collection_id: str,
    extra_hints: FrozenSet = frozenset(),
) -> Optional[object]:
    """Resolve the first READ driver for the primary collection.

    Returns the driver instance (any CollectionItemsStore impl) or None
    if no driver is registered for this catalog/collection on READ.

    ``extra_hints`` is unioned with the baseline JOIN hint so per-request
    routing preferences (e.g. ``geometry_exact``) can steer driver selection
    without overriding the join-specific routing intent. An empty set (the
    default) preserves existing behaviour exactly.
    """
    # Use a join-specific hint so an operator can ship a deployment where
    # /join routes to a different driver than /features (e.g. BQ for joins,
    # PG for raw features) without affecting the rest of the catalog read
    # surface. Both ItemsPostgresqlDriver and ItemsBigQueryDriver self-
    # declare "join" in their supported_hints, so a zero-config deployment
    # resolves the platform-default items store via the empty-entry-hints
    # fallback in router._resolve_driver_ids_cached.
    hints = frozenset({Hint.JOIN}) | extra_hints
    drivers = await resolve_drivers(
        "READ", catalog_id, collection_id, hints=hints,
    )
    return drivers[0].driver if drivers else None


async def _stream_primary_features(
    driver, *, catalog_id: str, collection_id: str,
    primary_column: str, limit: int = 100_000,
    query_request: Optional[QueryRequest] = None,
) -> AsyncGenerator[Feature, None]:
    """Wrap any CollectionItemsStore driver's read_entities into the
    plain ``AsyncIterator[Feature]`` shape ``run_join`` expects.

    The ``primary_column`` is passed via ``context["id_column"]`` so
    drivers (e.g. BQ) that need to know which column carries the join
    key can use it for projection. Drivers that ignore context just
    return all columns — the executor reads ``primary_column`` from
    ``feature.properties`` either way.

    When ``query_request`` is set, it's forwarded via ``request=`` so
    drivers that honor ``QueryRequest.cql_filter`` apply the primary-side
    filter. Drivers that ignore ``request`` treat it as a no-op.
    """
    async for feat in driver.read_entities(
        catalog_id, collection_id,
        limit=limit,
        request=query_request,
        context={"id_column": primary_column},
    ):
        yield feat


def _build_primary_query_request(
    primary_filter: Optional[PrimaryFilterSpec],
    limit: int,
) -> QueryRequest:
    """Construct the QueryRequest the primary driver will receive.

    The driver parses ``cql_filter`` downstream (see modules/tools/cql.py);
    drivers that don't support CQL2 ignore the field.
    """
    req = QueryRequest(limit=limit)
    if primary_filter is not None:
        req.cql_filter = primary_filter.cql
    return req


def _resolve_paging(
    body: JoinRequest, *, limit: Optional[int], offset: Optional[int],
) -> PagingSpec:
    """Resolve the effective page from query params over body, bounded.

    Query ``?limit=&offset=`` win over ``body.paging`` so a ``next`` link
    (which can only carry a query string) is followable by replaying the same
    POST body. Absent both, default to a bounded page (Features-style) rather
    than an unbounded scan. The result is clamped to ``[1, MAX_PAGE_LIMIT]``.
    """
    eff_limit = (
        limit if limit is not None
        else body.paging.limit if body.paging is not None
        else DEFAULT_PAGE_LIMIT
    )
    eff_offset = (
        offset if offset is not None
        else body.paging.offset if body.paging is not None
        else 0
    )
    eff_limit = max(1, min(eff_limit, MAX_PAGE_LIMIT))
    eff_offset = max(0, eff_offset)
    return PagingSpec(limit=eff_limit, offset=eff_offset)


def _with_paging_query(url: str, *, offset: int, limit: int) -> str:
    """Return ``url`` with ``offset``/``limit`` set in the query string.

    Preserves any other query params already present (e.g. ``?hints=``).
    """
    parts = urlsplit(url)
    kept = [
        (k, v)
        for k, v in (
            tuple(p.split("=", 1)) if "=" in p else (p, "")
            for p in parts.query.split("&") if p
        )
        if k not in ("offset", "limit")
    ]
    kept.extend([("offset", str(offset)), ("limit", str(limit))])
    return urlunsplit(parts._replace(query=urlencode(kept)))


def _join_feature_collection(
    joined: List[Feature], *, request: Request, paging: PagingSpec, has_next: bool,
    number_matched: Optional[int] = None,
) -> Dict[str, Any]:
    """Build an OGC-conformant join FeatureCollection.

    Carries the OGC API - Features response members (``links``, ``timeStamp``,
    ``numberReturned``) instead of a non-standard ``_join_meta`` foreign member.
    ``numberMatched`` is only set when the caller already knows the exact total
    for free (see ``_execute_primary_join``: a terminal page reached after a
    full, untruncated primary scan). The join otherwise streams an inner match
    and never materializes the full matched set, so its total is not known
    cheaply and stays omitted (Features Part 1 permits omitting it) rather than
    paying for a separate count.

    ``has_next`` is computed by the caller (``_execute_primary_join``) which
    uses a match-bounded read: it requests ``limit+1`` matched features from
    the join executor and emits ``next`` when the (limit+1)th match exists or
    the primary-scan safety ceiling was reached before ``limit+1`` matches were
    found. This is correct for both dense and sparse joins.

    ``prev`` is emitted whenever ``paging.offset > 0``, mirroring how ``next``
    is built (same ``_with_paging_query`` helper) — omitted on the first page.
    """
    features = [f.model_dump(by_alias=True, exclude_none=True) for f in joined]
    self_href = str(request.url)
    links: List[Dict[str, Any]] = [
        {"rel": "self", "type": GEOJSON_MEDIA_TYPE, "href": self_href},
    ]
    if paging.offset > 0:
        links.append({
            "rel": "prev",
            "type": GEOJSON_MEDIA_TYPE,
            "href": _with_paging_query(
                self_href,
                offset=max(0, paging.offset - paging.limit),
                limit=paging.limit,
            ),
        })
    if has_next:
        links.append({
            "rel": "next",
            "type": GEOJSON_MEDIA_TYPE,
            "href": _with_paging_query(
                self_href, offset=paging.offset + paging.limit, limit=paging.limit,
            ),
        })
    result: Dict[str, Any] = {
        "type": "FeatureCollection",
        "features": features,
        "numberReturned": len(features),
        "timeStamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "links": links,
    }
    if number_matched is not None:
        result["numberMatched"] = number_matched
    return result


async def _execute_primary_join(
    primary_driver,
    *,
    catalog_id: str,
    collection_id: str,
    body: JoinRequest,
    secondary_index: Dict[Any, Dict[str, Any]],
    request: Request,
) -> Dict[str, Any]:
    """Stream primary features up to the safety ceiling, run the join, page results.

    Uses a match-bounded read strategy: the primary stream is not pre-capped at
    ``offset+limit`` rows. Instead it is scanned up to ``MAX_PRIMARY_SCAN_ROWS``
    rows. The executor is asked for ``limit+1`` matched features; if the
    (limit+1)th match is found, a ``next`` link is emitted. When the ceiling
    truncates the scan before ``limit+1`` matches are found, a ``next`` link is
    still emitted so the client does not stop early on a sparse join.

    A key=value WARNING is logged when the ceiling is hit so sparse-scan
    truncation is observable in production logs.

    Raises:
        ValueError: forwarded from the primary driver when a CQL2 expression
            references an unknown column; the caller maps this to HTTP 400.
    """
    paging = body.paging
    assert paging is not None  # _resolve_paging always sets body.paging before we're called

    query_request = _build_primary_query_request(
        body.primary_filter, limit=MAX_PRIMARY_SCAN_ROWS,
    )

    # Count primary rows emitted so we can detect ceiling hits after the join.
    primary_rows_scanned = 0

    async def _counted_primary() -> AsyncGenerator[Feature, None]:
        nonlocal primary_rows_scanned
        inner = _stream_primary_features(
            primary_driver,
            catalog_id=catalog_id,
            collection_id=collection_id,
            primary_column=body.join.primary_column,
            limit=MAX_PRIMARY_SCAN_ROWS,
            query_request=query_request,
        )
        try:
            async for feat in inner:
                primary_rows_scanned += 1
                yield feat
        finally:
            await inner.aclose()

    # Request limit+1 matched features so the (limit+1)th item acts as a peek
    # for the `next` link decision.  PagingSpec.model_construct bypasses the
    # le=10000 validator because this is an internal peek value; the page
    # exposed to the client is trimmed to paging.limit below.
    peek_paging = PagingSpec.model_construct(
        limit=paging.limit + 1, offset=paging.offset,
    )
    run_join_body = body.model_copy(update={"paging": peek_paging})

    joined = [
        feat async for feat in run_join(
            run_join_body,
            primary_stream=_counted_primary(),
            secondary_index=secondary_index,
        )
    ]

    ceiling_hit = primary_rows_scanned >= MAX_PRIMARY_SCAN_ROWS
    if ceiling_hit and len(joined) < paging.limit + 1:
        logger.warning(
            "join_scan_ceiling_hit primary_rows=%d matches=%d "
            "catalog=%s collection=%s",
            primary_rows_scanned, len(joined), catalog_id, collection_id,
        )

    # A (limit+1)th match in `joined` proves more matches follow.
    # A ceiling hit without limit+1 matches means the scan was truncated;
    # conservatively emit `next` so the client does not stop prematurely.
    has_next = len(joined) > paging.limit or ceiling_hit
    joined = joined[: paging.limit]

    # `numberMatched` for free: reaching a terminal page (`not has_next`) means
    # the primary was scanned to completion without hitting the safety ceiling
    # and no further match exists — so the total is exactly what's already
    # been counted across pages (`offset` + this page's own count), with no
    # extra query. Any other page keeps it omitted (see `_join_feature_collection`).
    number_matched = None if has_next else paging.offset + len(joined)

    return _join_feature_collection(
        joined, request=request, paging=paging, has_next=has_next,
        number_matched=number_matched,
    )


# Draft URIs — OGC API - Joins Part 1 0.0 (working draft).
OGC_API_JOINS_URIS = [
    "http://www.opengis.net/spec/ogcapi-joins-1/0.0/conf/core",
]


class JoinsService(ExtensionProtocol, OGCServiceMixin):
    """OGC API - Joins extension."""

    priority: int = 180  # after Volumes (170)

    conformance_uris = OGC_API_JOINS_URIS
    prefix = "/join"
    protocol_title = "DynaStore OGC API - Joins"
    protocol_description = (
        "Per-request joins between a primary collection and a secondary "
        "(registered or per-request) data source."
    )

    def __init__(self, app: Optional[FastAPI] = None):
        super().__init__()
        self.app = app
        self.router = APIRouter(prefix=self.prefix, tags=["OGC API - Joins"])
        self._register_routes()

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        from dynastore.tools.discovery import register_plugin
        from .link_contrib import JoinsLinkContributor
        register_plugin(JoinsLinkContributor())
        yield

    def get_notebooks(self):
        try:
            from .notebooks import build_contributions
        except Exception:
            return []
        return build_contributions()

    def _register_routes(self) -> None:
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/join",
            self.describe_join, methods=["GET"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/join",
            self.execute_join, methods=["POST"],
            response_class=GeoJSONResponse,
            # Doc-only response schema: the join FeatureCollection carries
            # arbitrary per-request properties/geometry that a real
            # `response_model=FeatureCollection` would re-validate on every
            # request (and reject legitimate `geometry: null` features, since
            # geojson_pydantic's `Feature.geometry` isn't itself nullable-
            # optional) — that's the join *semantics* this issue must not
            # touch. `responses=` documents the schema for the OpenAPI spec
            # without imposing runtime (re-)validation.
            responses={
                200: {
                    "description": (
                        "Joined FeatureCollection page, served as "
                        f"`{GEOJSON_MEDIA_TYPE}` by default or `{JSON_MEDIA_TYPE}` "
                        "when explicitly requested via `Accept`."
                    ),
                    "content": {
                        GEOJSON_MEDIA_TYPE: {"schema": FeatureCollection.model_json_schema()},
                        JSON_MEDIA_TYPE: {"schema": FeatureCollection.model_json_schema()},
                    },
                },
            },
        )

    async def describe_join(
        self, catalog_id: str, collection_id: str, request: Request,
    ):
        """Advertise supported secondary drivers + minimal capability surface."""
        base = str(request.url).rstrip("/")
        return {
            "title": "OGC API - Joins describe",
            "primary": {"catalog": catalog_id, "collection": collection_id},
            "supported_secondary_drivers": ["registered", "bigquery"],
            "links": [
                {"rel": "self", "type": "application/json", "href": base},
            ],
        }

    async def execute_join(
        self, catalog_id: str, collection_id: str, request: Request,
        body: JoinRequest = Body(...),
        request_hints: FrozenSet = Depends(parse_hints_param),
        limit: Annotated[Optional[int], Query(
            ge=1, le=MAX_PAGE_LIMIT,
            description="Page size; overrides body.paging.limit when present.",
        )] = None,
        offset: Annotated[Optional[int], Query(
            ge=0,
            description="Start offset; overrides body.paging.offset when present.",
        )] = None,
    ):
        """Execute the join.

        Accepts ``?hints=`` to steer primary-driver selection — e.g.
        ``?hints=geometry_exact`` routes to the PostgreSQL driver (exact
        full-precision geometry) instead of a simplified-geometry search
        backend. Hints are forwarded to driver resolution and unioned with
        the baseline ``JOIN`` hint; omitting the parameter preserves the
        existing routing behaviour.

        ``?limit=`` / ``?offset=`` override ``body.paging`` so the ``next``
        link in the response is followable by replaying this POST body. The
        response is an OGC API - Features-style FeatureCollection (``links``,
        ``timeStamp``, ``numberReturned``).

        Honors ``Accept: application/json`` for a plain-JSON response body;
        any other (or absent/wildcard) ``Accept`` keeps the OGC-conformant
        ``application/geo+json`` default.

        PR-3: BigQuerySecondarySpec runs the join end-to-end via the
        platform's driver registry for the primary side. NamedSecondarySpec
        stub remains until its own PR.
        """
        accept_header = request.headers.get("Accept", GEOJSON_MEDIA_TYPE)

        # Effective page: query params win over body, bounded to a sane page.
        # The primary read is NOT pre-capped at offset+limit rows — that caused
        # sparse inner joins to under-fill pages and suppress the `next` link
        # prematurely. Instead, _execute_primary_join streams up to
        # MAX_PRIMARY_SCAN_ROWS rows and asks the executor for limit+1 matched
        # features so it can reliably decide whether a next page exists.
        body.paging = _resolve_paging(body, limit=limit, offset=offset)

        if isinstance(body.secondary, BigQuerySecondarySpec):
            # Materialize secondary side via Phase 4a's BQ driver (inline target).
            secondary_index = await index_secondary(
                stream_bigquery_secondary(
                    body.secondary, secondary_column=body.join.secondary_column,
                ),
                secondary_column=body.join.secondary_column,
            )
            # Resolve primary driver via the platform's storage router.
            # Thread request_hints so ?hints=geometry_exact steers routing.
            primary_driver = await _resolve_primary_driver(
                catalog_id, collection_id, extra_hints=request_hints,
            )
            if primary_driver is None:
                raise HTTPException(
                    status_code=404,
                    detail=(
                        f"No READ driver registered for {catalog_id}/{collection_id}. "
                        "Configure a ItemsRoutingConfig before /join."
                    ),
                )
            try:
                result = await _execute_primary_join(
                    primary_driver,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    body=body,
                    secondary_index=secondary_index,
                    request=request,
                )
            except ValueError as e:
                raise HTTPException(
                    status_code=400, detail=f"Invalid primary_filter: {e}",
                ) from e
            return _negotiate_join_response(result, accept_header)

        if isinstance(body.secondary, NamedSecondarySpec):
            # Resolve secondary collection via the platform's driver registry.
            # Secondary reads are always full-scan (no user-geometry preference);
            # hints apply only to the primary driver below.
            secondary_driver = await _resolve_primary_driver(catalog_id, body.secondary.ref)
            if secondary_driver is None:
                raise HTTPException(
                    status_code=404,
                    detail=(
                        f"Secondary collection {body.secondary.ref!r} has no READ "
                        f"driver registered in catalog {catalog_id!r}."
                    ),
                )
            # Drain the secondary into a lookup dict.
            secondary_stream = _stream_primary_features(
                secondary_driver,
                catalog_id=catalog_id,
                collection_id=body.secondary.ref,
                primary_column=body.join.secondary_column,
                limit=100_000,
            )
            secondary_index = await index_secondary(
                secondary_stream, secondary_column=body.join.secondary_column,
            )
            # Resolve primary driver, forwarding request_hints.
            primary_driver = await _resolve_primary_driver(
                catalog_id, collection_id, extra_hints=request_hints,
            )
            if primary_driver is None:
                raise HTTPException(
                    status_code=404,
                    detail=(
                        f"No READ driver registered for primary "
                        f"{catalog_id}/{collection_id}."
                    ),
                )
            try:
                result = await _execute_primary_join(
                    primary_driver,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    body=body,
                    secondary_index=secondary_index,
                    request=request,
                )
            except ValueError as e:
                raise HTTPException(
                    status_code=400, detail=f"Invalid primary_filter: {e}",
                ) from e
            return _negotiate_join_response(result, accept_header)

        raise HTTPException(
            status_code=400,
            detail=f"Unsupported secondary spec: {type(body.secondary).__name__}",
        )
