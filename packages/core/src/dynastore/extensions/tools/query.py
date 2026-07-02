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

from typing import FrozenSet, Optional, List, Dict, Any, Union
from datetime import datetime, timezone
import logging
from fastapi import HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from dynastore.models.query_builder import (
    QueryRequest,
    FilterCondition,
    SortOrder,
    QueryResponse,
)
from dynastore.extensions.tools.formatters import (
    OutputFormatEnum,
    format_response,
    OGCResponseMetadata,
)
from dynastore.tools.geospatial import BboxDimensionality, parse_bbox_string

logger = logging.getLogger(__name__)


#: OpenAPI description shared by the ``?hints=`` parameter on every protocol.
HINTS_QUERY_DESCRIPTION = (
    "Per-request routing hints (repeatable, or comma-separated in one value). "
    "Canonical tokens come from the Hint vocabulary — e.g. "
    "``geometry_exact`` requests full-precision geometry from the exact-capable "
    "driver (today PostgreSQL) instead of the simplified search-backend copy. "
    "Unknown tokens are ignored, so passing an unsupported hint is harmless. "
    "The parametric token ``prefer:<driver>`` (e.g. ``prefer:es``, ``prefer:pg``) "
    "pins a READ/SEARCH to a specific driver regardless of its declared hint "
    "surface — for example ``?hints=prefer:es`` routes a collection metadata "
    "READ to Elasticsearch first with PostgreSQL kept as the fallback."
)


def parse_hints_param(
    hints: Optional[List[str]] = Query(None, description=HINTS_QUERY_DESCRIPTION),
) -> FrozenSet:
    """FastAPI dependency: parse the uniform ``?hints=`` parameter.

    Declaring ``hints: FrozenSet = Depends(parse_hints_param)`` on any protocol
    route both documents the parameter in OpenAPI and yields a validated
    ``frozenset[Hint]`` ready to thread into the routing layer's ``hints=``
    argument. Empty/omitted → ``frozenset()`` (default read path unchanged).
    """
    from dynastore.modules.storage.hints import parse_request_hints

    return parse_request_hints(hints)


def request_hints(request: Request) -> FrozenSet:
    """Parse ``?hints=`` straight from the live request query string.

    For shared helpers (e.g. :func:`dispatch_or_stream_items`) that already
    receive the ``Request`` but whose callers may not have declared the
    :func:`parse_hints_param` dependency: lets every protocol routed through the
    helper honour ``hints`` uniformly without a per-route signature change.
    """
    from dynastore.modules.storage.hints import parse_request_hints

    try:
        values = request.query_params.getlist("hints")
    except Exception:
        return frozenset()
    return parse_request_hints(values)


async def resolve_items_read_policy(
    catalog_id: str,
    collection_id: str,
) -> Optional[Any]:
    """Resolve a collection's :class:`ItemsReadPolicy` for OGC read assembly.

    Shared by the OGC generators (Features / Records) so a raw-row fallback in
    ``map_row_to_feature`` honours the read-time wire-shape contract —
    ``feature_type.expose`` value-merge and ``external_id_as_feature_id``.
    Mirrors ``ItemQueryMixin._resolve_read_policy`` for callers that don't have
    a mixin instance. Returns ``None`` when the configs protocol is unavailable
    or the lookup fails; callers then fall back to the default wire shape.
    """
    try:
        from dynastore.models.protocols import ConfigsProtocol
        from dynastore.modules.storage.read_policy import ItemsReadPolicy
        from dynastore.tools.discovery import get_protocol

        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            return None
        return await configs.get_config(
            ItemsReadPolicy,
            catalog_id=catalog_id,
            collection_id=collection_id,
        )
    except Exception as exc:  # noqa: BLE001 - read assembly must not break on config miss
        logger.debug(
            "read policy resolution skipped for %s/%s: %s",
            catalog_id,
            collection_id,
            exc,
        )
        return None


async def maybe_dispatch_items_to_search_driver(
    catalog_id: str,
    collection_id: str,
    *,
    bbox: Optional[List[float]] = None,
    intersects: Optional[Dict[str, Any]] = None,
    datetime: Optional[str] = None,
    ids: Optional[List[str]] = None,
    filters: Optional[List[Any]] = None,
    limit: int = 10,
    offset: int = 0,
    has_complex_filter: bool = False,
    request: Optional[Request] = None,
) -> Optional[QueryResponse]:
    """Dispatch an OGC ``/items`` listing to the collection's routing-pinned
    items SEARCH driver, when one is configured and search-capable.

    Used by the OGC API - Features and OGC API - Records ``/items`` endpoints
    so they resolve the items SEARCH driver via routing
    (:func:`router.get_items_search_driver` — ``Operation.SEARCH`` then a
    ``READ`` fallback) and dispatch the structural query to **that driver**
    through its streaming ``read_entities`` + ``count_entities`` contract —
    exactly as STAC ``/search`` does. A public-ES catalog, a GEOID-style
    catalog routing SEARCH to its tenant-private ES index, or any future
    ES items driver are all served the same way, with no hardcoded driver
    class (#1047).

    Returns ``None`` (caller falls back to the existing PostgreSQL
    ``stream_items`` path) when the dispatch is not applicable:

    * a CQL2 / shorthand attribute ``filter`` is present (no CQL2→search-backend
      translator yet — same restriction as STAC ``/search``);
    * the resolved driver is a read-primary fallback (PostgreSQL, advertising
      ``Capability.QUERY_FALLBACK_SOURCE``) — i.e. the catalog has no dedicated
      search backend;
    * the resolved driver does not advertise the structural-search capability;
    * driver resolution or the dispatch itself raises (degrade, never 500).

    On a successful dispatch returns a :class:`QueryResponse` whose ``items`` is
    an async iterator of read-contract :class:`~dynastore.models.ogc.Feature`
    objects and whose ``total_count`` carries the backend ``numberMatched`` —
    so paging links stay correct (unlike the ``read_entities`` browse path,
    which cannot report a total).

    Row-level ABAC: when the resolved driver opts in (the standardized envelope
    driver, ``applies_access_filter=True``) and the Starlette ``request`` is
    supplied, the caller's read scope is compiled from request state via the
    shared :func:`compile_read_access_filter` and set on the dispatched
    ``QueryRequest`` — the same single piece of security logic the STAC
    ``/search`` fast path and the search extension use, so the enforcement paths
    cannot drift. The envelope driver fails closed when the filter is absent, so
    an un-wired caller can only under-return, never leak; a non-envelope driver
    ignores the field, so this is behaviour-neutral for existing catalogs.
    """
    # Only basic structural filters are routed to a search backend today;
    # CQL2 / attribute-shorthand filters continue to the PG path.
    if has_complex_filter:
        return None

    from dynastore.modules.storage import router as _router
    from dynastore.modules.catalog.item_query import is_query_fallback_driver
    from dynastore.models.query_builder import QueryRequest

    try:
        resolved = await _router.get_items_search_driver(
            catalog_id, collection_id,
        )
    except Exception:
        return None

    driver: Any = resolved.driver

    # A read-primary fallback (PostgreSQL) advertises QUERY_FALLBACK_SOURCE
    # (and an absent driver is treated the same): no dedicated search backend,
    # so defer to the PG stream_items path. Shared gate with the read_entities
    # browse path (``item_query._try_driver_dispatch``).
    if is_query_fallback_driver(driver):
        return None

    # Only ES items drivers honor the structural QueryRequest dimensions on
    # their streaming read/count path today; anything else defers to the PG
    # ``stream_items`` path. (Marker, not isinstance — same gate the indexer
    # and item_service use, ``is_es_items_driver``.)
    if not getattr(driver, "is_es_items_driver", False):
        return None

    # Routing-honouring fallback: the ES driver is the configured search
    # primary, but if its per-tenant items index does not exist (PG-only
    # catalog, or indexing has not run) it cannot serve. Defer to the PG
    # ``stream_items`` path — the configured read fallback / system of record —
    # instead of returning an empty result that hides PG-resident rows.
    _index_check = getattr(driver, "index_available", None)
    if _index_check is not None and not await _index_check(catalog_id):
        return None

    # Single-collection /items: leave ``collections`` unset so the driver keeps
    # its routed single-collection fast path; the positional collection scopes.
    # ``filters`` carries any caller-supplied structural attribute predicates
    # (e.g. the virtual-asset view's ``asset_id`` equality); the ES driver folds
    # ``eq``/``like`` conditions into the query as additional ``must`` clauses.
    query_request = QueryRequest(
        item_ids=ids,
        bbox=bbox,
        intersects=intersects,
        datetime=datetime,
        filters=filters or [],
        limit=limit,
        offset=offset,
    )

    # Row-level ABAC: an access-aware driver (the envelope driver) AND-s a
    # compiled read scope into its query and fails closed without one. Compile
    # the caller's scope from request state through the shared helper — the same
    # one STAC ``/search`` and the search extension use — so the OGC ``/items``
    # entry points enforce exactly what those paths do. Guarded on the driver
    # marker, so it is a no-op for ordinary ES/PG drivers.
    if request is not None and getattr(driver, "applies_access_filter", False):
        from dynastore.modules.storage.access_scope import (
            compile_read_access_filter,
            principals_from_request_state,
        )

        principals, principal = principals_from_request_state(request)
        query_request.access_filter = await compile_read_access_filter(
            catalog_id=catalog_id,
            collections=[collection_id],
            principals=principals,
            principal=principal,
        )

    try:
        # ``read_entities`` streams read-contract Features (O(1) memory);
        # ``count_entities`` supplies numberMatched so paging links stay correct.
        items = driver.read_entities(
            catalog_id, collection_id,
            request=query_request, limit=limit, offset=offset,
        )
        total = await driver.count_entities(
            catalog_id, collection_id, request=query_request,
        )
    except Exception as exc:  # noqa: BLE001 — degrade to PG path, never 500
        logger.warning(
            "OGC /items → SEARCH-driver dispatch failed "
            "(catalog=%s, collection=%s, driver=%s): %s",
            catalog_id, collection_id, type(driver).__name__, exc,
        )
        return None

    return QueryResponse(
        items=items,
        total_count=total,
        catalog_id=catalog_id,
        collection_id=collection_id,
    )


# OGC API Features query parameters that are handled explicitly by the
# request parser / route handler and therefore must NOT be treated as
# ad-hoc property=value attribute filters.
OGC_RESERVED_QUERY_PARAMS: frozenset = frozenset({
    "bbox",
    "bbox-crs",
    "datetime",
    "limit",
    "offset",
    "filter",
    "filter-lang",
    "filter_lang",
    "crs",
    "sortby",
    "f",
    "lang",
    "language",
    "token",
    "access_token",
    "hints",  # per-request routing hints (e.g. ?hints=geometry_exact); a
    # control parameter accepted uniformly across protocols, never a queryable
    # attribute, so it must not be swept into the ?{property}={value} shorthand.
    # Projection / geometry-control parameters. These are bound as real query
    # parameters by the item endpoints (``properties`` projection, the
    # pygeoapi ``skipGeometry`` and ESRI ``returnGeometry`` geometry switches,
    # and the ``filter-crs`` / ``bbox-crs`` CRS overrides). They must be
    # reserved here too: the single-field equality shorthand sweeps every
    # non-reserved query parameter into a ``?{property}={value}`` CQL filter,
    # so leaving them out makes e.g. ``?returnGeometry=false`` parse as a
    # bogus ``returnGeometry = 'false'`` attribute filter and 400 with
    # "Unknown properties". Both spellings (hyphen alias + snake_case) are
    # listed because either can reach ``request.query_params``.
    "properties",
    "skipgeometry",
    "skipGeometry",
    "skip_geometry",
    "returngeometry",
    "returnGeometry",
    "return_geometry",
    "filter-crs",
    "filter_crs",
    "bbox_crs",
    "_",  # cache-buster appended by browsers/jQuery; never an attribute
})


async def resolve_queryable_property_names(catalog_id: str, collection_id: str) -> set:
    """Return the set of valid queryable property names for a collection.

    Single source of truth for validating any request parameter that names a
    collection attribute against exactly the surface the ``/queryables``
    endpoint advertises — the collection's own field definitions (via
    :pymeth:`ItemsProtocol.get_collection_fields`) merged with
    ``canonical_queryable_properties()`` (the bounded canonical system/stats
    lanes, refs #2230/#2235). Shared by the OGC Features ``properties=``
    projection parameter and the OGC Features / STAC ad-hoc
    ``?{property}={value}`` filter shorthand — no parallel validator.

    Returns an empty set when the introspection protocol is unavailable; the
    caller treats that as "permissive" and skips validation rather than
    failing every request when the introspection backend is offline.
    """
    from dynastore.models.protocols import ItemsProtocol
    from dynastore.tools.discovery import get_protocol

    items_svc = get_protocol(ItemsProtocol)
    if items_svc is None:
        return set()
    try:
        all_fields = await items_svc.get_collection_fields(catalog_id, collection_id)
    except Exception:
        return set()
    names: set = set()
    for fd in all_fields.values():
        if not getattr(fd, "expose", True):
            continue
        final_name = getattr(fd, "alias", None) or getattr(fd, "name", None)
        if not final_name or final_name in ("geoid", "geom"):
            continue
        names.add(final_name)

    from dynastore.modules.elasticsearch.mappings import canonical_queryable_properties

    names |= canonical_queryable_properties().keys()
    return names


def reject_unknown_filter_params(extra_filters: Dict[str, str], valid_names: set) -> None:
    """Reject ad-hoc ``?{property}={value}`` filter params outside ``valid_names``.

    Skips validation when ``valid_names`` is empty (introspection unavailable
    — see :func:`resolve_queryable_property_names`), the same permissive
    fallback the ``properties=`` projection check uses. Otherwise raises
    ``HTTPException`` (400) naming every offending parameter.

    This must run before the shorthand is folded into a CQL filter and
    dispatched to a driver: the PG driver's ``parse_cql_filter`` already 400s
    on an unmapped property name, but the ES SEARCH driver silently dropped
    the unmapped predicate and served the unfiltered listing while the count
    path independently collapsed to zero — the count and the returned
    features must always describe the same selection (#2682). Validating
    here, once, ahead of dispatch keeps both paths consistent regardless of
    which driver ends up serving the request.
    """
    if not valid_names or not extra_filters:
        return
    unknown = sorted(k for k in extra_filters if k not in valid_names)
    if unknown:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unknown filter parameter(s): {', '.join(unknown)}. Not a "
                "queryable property of this collection — see '/queryables' "
                "for the supported filter names."
            ),
        )


def _cql_escape_literal(value: str) -> str:
    """Escape a value for embedding inside a single-quoted CQL2 string literal.

    CQL2-Text escapes a single quote by doubling it. The property name is never
    interpolated into SQL — it is validated against the collection's queryable
    fields by ``parse_cql_filter`` and only the resolved column reference reaches
    the query — and the value here is rendered as a quoted CQL literal that the
    CQL backend turns into a bound parameter. Escaping keeps the generated CQL
    text well-formed for values that themselves contain single quotes.
    """
    return value.replace("'", "''")


def build_attribute_equality_cql(extra_filters: Dict[str, str]) -> Optional[str]:
    """Translate ``{property: value}`` pairs into a CQL2-Text equality filter.

    Each pair becomes ``property = 'value'`` and the clauses are joined with
    ``AND``. The resulting CQL string is parsed and validated downstream by
    :func:`dynastore.modules.tools.cql.parse_cql_filter`, which rejects unknown
    property names (→ 400) and binds the values as query parameters. Returns
    ``None`` when there is nothing to filter on.
    """
    if not extra_filters:
        return None
    clauses = [
        f"{name} = '{_cql_escape_literal(str(value))}'"
        for name, value in extra_filters.items()
        if value is not None
    ]
    if not clauses:
        return None
    return " AND ".join(clauses)


def combine_cql_filters(
    filter: Optional[str] = None,
    extra_filters: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    """Merge an explicit CQL2 ``filter`` with ``?{property}={value}`` shorthand.

    The shorthand pairs are converted to CQL2-Text equality clauses via
    :func:`build_attribute_equality_cql` and AND-combined with the explicit
    ``filter`` into a single CQL2 expression so both go through the same
    validated CQL parsing path downstream. Returns ``None`` when neither side
    contributes anything. This is the single source of truth for building the
    combined CQL string, shared by the OGC Features and STAC items endpoints.
    """
    shorthand_cql = build_attribute_equality_cql(extra_filters or {})
    cql_parts = [c for c in (filter, shorthand_cql) if c]
    if not cql_parts:
        return None
    if len(cql_parts) == 1:
        return cql_parts[0]
    return " AND ".join(f"({c})" for c in cql_parts)


def resolve_geometry_flag(
    skip_geometry: Optional[bool],
    return_geometry: Optional[bool],
) -> bool:
    """
    Resolve the geometry-omission flag from the two accepted query params.

    ``skipGeometry`` (pygeoapi de-facto) and ``returnGeometry`` (ESRI de-facto)
    express the same toggle from opposite poles. Either may be passed; passing
    both is allowed only when they agree. Contradiction → HTTP 400.

    Returns the canonical ``skip_geometry`` boolean (True = omit geometry).
    """
    if skip_geometry is None and return_geometry is None:
        return False
    if skip_geometry is not None and return_geometry is not None:
        if skip_geometry == (not return_geometry):
            return skip_geometry
        raise HTTPException(
            status_code=400,
            detail=(
                f"Conflicting geometry flags: skipGeometry={skip_geometry} "
                f"and returnGeometry={return_geometry}. Pass only one, or "
                "ensure they are mutually consistent "
                "(skipGeometry == not returnGeometry)."
            ),
        )
    if skip_geometry is not None:
        return skip_geometry
    assert return_geometry is not None
    return not return_geometry


def resolve_geometry_flag_from_query(
    skip_geometry: Any,
    return_geometry: Any,
) -> bool:
    """:func:`resolve_geometry_flag`, tolerant of FastAPI ``Query(...)`` sentinels.

    When an OGC listing handler is invoked directly (unit tests bypass FastAPI),
    ``skipGeometry`` / ``returnGeometry`` may still carry their ``Query(...)``
    defaults rather than ``bool`` / ``None``. Coerce any non-bool to ``None``
    before delegating to :func:`resolve_geometry_flag`.
    """
    _sg = skip_geometry if isinstance(skip_geometry, bool) else None
    _rg = return_geometry if isinstance(return_geometry, bool) else None
    return resolve_geometry_flag(_sg, _rg)


def validate_filter_lang(filter_lang: Any) -> str:
    """Normalise and validate the OGC API ``filter-lang`` query parameter.

    OGC API Features Part 3 defines ``cql2-text`` (default) and ``cql2-json``;
    anything else is a client error (HTTP 400). Non-string values — the
    ``Query(...)`` sentinels seen when a handler is invoked directly in unit
    tests — coerce to the documented default. Returns the normalised value.
    """
    normalised = (
        filter_lang.lower()
        if isinstance(filter_lang, str) and filter_lang
        else "cql2-text"
    )
    if normalised not in ("cql2-text", "cql2-json"):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unsupported filter-lang '{filter_lang}'. "
                "Supported: 'cql2-text', 'cql2-json'."
            ),
        )
    return normalised


async def dispatch_or_stream_items(
    items_protocol: Any,
    *,
    catalog_id: str,
    collection_id: str,
    query_request: QueryRequest,
    consumer: Any,
    search_dispatch: Optional[QueryResponse] = None,
    ctx: Any = None,
    request: Optional[Request] = None,
    hints: "FrozenSet" = frozenset(),
) -> QueryResponse:
    """Return the routed SEARCH-driver response, or stream from the items protocol.

    Both OGC listing handlers (Features ``/items``, Records ``/items``) share one
    execution contract: if a routing-aware SEARCH driver already produced a
    :class:`QueryResponse` (``search_dispatch``), use it; otherwise stream via
    ``items_protocol.stream_items`` and map the driver's ``ValueError`` (invalid
    properties / fields) to HTTP 400. Callers supply their own ``consumer``
    (``OGC_FEATURES`` / ``OGC_RECORDS``) and ``ctx`` (Features decouples with
    ``None`` for background streaming; Records threads the request connection).

    ``hints`` is forwarded to ``stream_items`` so callers requesting exact
    geometry (``EXACT_READ_HINTS``) force the PG path even when a simplified-
    geometry ES driver is registered first for READ.  Defaults to ``frozenset()``
    so all existing callers are unaffected.

    Row-level ABAC (PG path): when ``request`` is supplied and the collection
    carries a PG ``access_envelope`` sidecar, the caller's read scope is compiled
    from request state and set on ``query_request.access_filter`` before the PG
    ``stream_items`` call. The envelope sidecar fails closed (zero rows) when the
    filter is absent, so un-wired callers can only under-return, never leak.
    Guarded on ``collection_uses_pg_access_envelope`` (fail-closed to False) so
    this is a no-op for ordinary collections and for system reads (no request).
    """
    if search_dispatch is not None:
        return search_dispatch

    # Uniform ``?hints=`` support: union any caller-supplied hints with the
    # ones parsed from the live request query string. Every protocol routed
    # through this helper (OGC Features, Records, …) honours ``?hints=…``
    # without a per-route signature change; an exact-by-default caller that
    # already passes EXACT_READ_HINTS simply keeps it.
    if request is not None:
        hints = frozenset(hints) | request_hints(request)

    # PG row-level ABAC: compile and inject access_filter when the collection
    # uses the PG access_envelope sidecar and the HTTP request is available.
    if request is not None:
        from dynastore.modules.storage.access_scope import (
            collection_uses_pg_access_envelope,
            compile_read_access_filter,
            principals_from_request_state,
        )

        if await collection_uses_pg_access_envelope(catalog_id, collection_id):
            principals, principal = principals_from_request_state(request)
            query_request.access_filter = await compile_read_access_filter(
                catalog_id=catalog_id,
                collections=[collection_id],
                principals=principals,
                principal=principal,
            )

    try:
        return await items_protocol.stream_items(
            catalog_id=catalog_id,
            collection_id=collection_id,
            request=query_request,
            ctx=ctx,
            consumer=consumer,
            hints=hints,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


def parse_ogc_query_request(
    bbox: Optional[str] = None,
    datetime_param: Optional[str] = None,
    sortby: Optional[str] = None,
    filter: Optional[str] = None,
    item_ids: Optional[Union[str, List[str]]] = None,
    limit: int = 10,
    offset: int = 0,
    bbox_crs_srid: Optional[int] = None,
    include_total_count: bool = True,
    extra_filters: Optional[Dict[str, str]] = None,
    filter_lang: str = "cql2-text",
    filter_crs_srid: Optional[int] = None,
    select_fields: Optional[List[str]] = None,
    skip_geometry: bool = False,
) -> QueryRequest:
    """
    Unifies OGC parameter parsing into a structured QueryRequest.

    ``extra_filters`` carries ad-hoc ``?{property}={value}`` shorthand
    parameters (the simple single-field equality form). They are converted to
    CQL2-Text equality clauses and combined with any explicit ``filter`` so both
    paths share the same validation, identifier safety, and parameter binding.
    """
    from dynastore.models.query_builder import FieldSelection

    select: Optional[List[FieldSelection]] = None
    if select_fields:
        # Narrow the driver-level projection to exactly the requested names.
        # An empty selection is intentionally NOT wired into ``select`` here
        # because :pyfunc:`QueryRequest.validate_select` would re-expand ``[]``
        # to ``[FieldSelection(field="*")]``; the service-layer post-fetch
        # projection handles the empty case by stripping every attribute.
        select = [FieldSelection(field=name) for name in select_fields]

    request_obj = QueryRequest(
        limit=limit,
        offset=offset,
        include_total_count=include_total_count,
        filters=[],
        filter_lang=filter_lang,
        filter_crs_srid=filter_crs_srid,
        skip_geometry=skip_geometry,
    )
    if select is not None:
        request_obj.select = select

    # 0. Item IDs
    if item_ids:
        if isinstance(item_ids, str):
            item_ids = [id.strip() for id in item_ids.split(",")]

        request_obj.filters.append(
            FilterCondition(
                field="geoid",  # Default to geoid; ItemService handles mapping to external_id if configured
                operator="IN",
                value=item_ids,
            )
        )

    if bbox:
        try:
            parsed_bbox = parse_bbox_string(
                bbox,
                dimensionality=BboxDimensionality.STRICT_2D,
                allow_none=False,
                validate_geometry=False,
            )
            assert parsed_bbox is not None  # allow_none=False guarantees this
            srid = bbox_crs_srid or 4326
            request_obj.filters.append(
                FilterCondition(
                    field="geom",
                    operator="&&",
                    value=f"SRID={srid};POLYGON(({parsed_bbox[0]} {parsed_bbox[1]}, {parsed_bbox[0]} {parsed_bbox[3]}, {parsed_bbox[2]} {parsed_bbox[3]}, {parsed_bbox[2]} {parsed_bbox[1]}, {parsed_bbox[0]} {parsed_bbox[1]}))",
                    spatial_op=True,
                )
            )
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid BBOX: {e}") from e

    if datetime_param:
        try:
            if "/" in datetime_param:
                start_str, end_str = datetime_param.split("/")
                start_dt_str = start_str if start_str != ".." else None
                end_dt_str = end_str if end_str != ".." else None
                start_dt = (
                    datetime.fromisoformat(start_dt_str.replace("Z", "+00:00"))
                    if start_dt_str
                    else None
                )
                end_dt = (
                    datetime.fromisoformat(end_dt_str.replace("Z", "+00:00"))
                    if end_dt_str
                    else None
                )

                if start_dt and end_dt:
                    request_obj.filters.append(
                        FilterCondition(
                            field="validity",
                            operator="&&",
                            value=f"[{start_dt.isoformat()},{end_dt.isoformat()})",
                        )
                    )
                elif start_dt:
                    request_obj.filters.append(
                        FilterCondition(field="validity", operator="@>", value=start_dt)
                    )
                elif end_dt:
                    request_obj.filters.append(
                        FilterCondition(field="validity", operator="@>", value=end_dt)
                    )
            else:
                dt = datetime.fromisoformat(datetime_param.replace("Z", "+00:00"))
                request_obj.filters.append(
                    FilterCondition(field="validity", operator="@>", value=dt)
                )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Invalid datetime format: {e}") from e
    else:
        request_obj.filters.append(
            FilterCondition(
                field="validity", operator="@>", value=datetime.now(timezone.utc)
            )
        )

    if sortby:
        request_obj.sort = []
        for v in sortby.split(","):
            v = v.strip()
            if not v:
                continue
            direction = "DESC" if v.startswith("-") else "ASC"
            request_obj.sort.append(
                SortOrder(field=v.lstrip("+-"), direction=direction)
            )

    # Combine the explicit CQL2 ``filter`` with any ``?{property}={value}``
    # shorthand equality filters into a single CQL2 expression. Both go through
    # the same validated CQL parsing path downstream.
    request_obj.cql_filter = combine_cql_filters(filter, extra_filters)

    return request_obj


def stream_ogc_features(
    request: Request,
    query_response: QueryResponse,
    output_format: OutputFormatEnum,
    catalog_id: str,
    collection_id: str,
    target_srid: int = 4326,
    links: Optional[List[Any]] = None,
    language: str = "en",
    offset: Optional[int] = None,
    max_response_bytes: Optional[int] = None,
) -> StreamingResponse:
    """
    Unified streaming response for OGC Features/WFS/DWH.

    ``language`` controls how ``Link.title`` values (``LocalizedText``) are
    resolved before writing to the wire.  Default ``'en'`` collapses each
    title to a plain string; ``'*'`` preserves the full multi-language dict.

    ``offset``/``max_response_bytes`` (#2681): pass both to enable a
    config-driven response byte budget — once the serialized ``features``
    bytes cross ``max_response_bytes`` the GeoJSON/JSON page is cut short and
    the precomputed ``next`` link in ``links`` is replaced with one built
    from the number of items actually served (see
    :func:`dynastore.extensions.tools.formatters._stream_ogc_json`).
    ``numberReturned`` always reflects what was actually streamed;
    ``numberMatched`` is unaffected. Omit both (the default) to keep the
    unbounded legacy behaviour.
    """
    from datetime import datetime, timezone
    from dynastore.extensions.tools.response_i18n import resolve_links

    links_data: Optional[List[Any]] = None
    if links:
        # Dump each Link to a plain dict, then resolve title in-place so the
        # resolved value (plain str or full dict) reaches the wire unchanged.
        # We bypass OGCResponseMetadata's Link validator (which would re-wrap
        # a plain str back to {"en": "..."}) by using model_construct below.
        raw_dicts = [lnk.model_dump(exclude_none=True) if hasattr(lnk, "model_dump") else lnk for lnk in links]
        links_data = resolve_links(raw_dicts, language)

    # model_construct bypasses Pydantic field validators so pre-resolved dicts
    # stored in links_data reach formatters.py without being re-wrapped.
    ogc_metadata = OGCResponseMetadata.model_construct(
        numberMatched=query_response.total_count,
        links=links_data,
        timeStamp=datetime.now(timezone.utc),
    )

    return format_response(
        request=request,
        features=query_response.items,
        output_format=output_format,
        collection_id=collection_id,
        target_srid=target_srid,
        metadata=ogc_metadata,
        max_response_bytes=max_response_bytes,
        offset=offset,
    )
