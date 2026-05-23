#    Copyright 2025 FAO
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

from typing import Optional, List, Dict, Any, Tuple, Union, AsyncIterator
from datetime import datetime, timezone
import logging
import orjson
from fastapi import HTTPException, status, Request
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
from dynastore.tools.json import orjson_default

logger = logging.getLogger(__name__)


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


def _items_hits_to_features(
    raw_features: list,
    catalog_id: str,
    collection_id: str,
) -> list:
    """Turn raw search-driver hits into read-contract OGC ``Feature`` objects.

    ``ItemSearchProtocol.search_items_struct`` returns plain dicts in the
    indexed ``_source`` shape: unknown attributes nested under
    ``properties.extras`` (Tier-3 projection), internal ``_*`` tracking fields
    and leaked echo keys at the top level (which would otherwise surface via the
    ``extra="allow"`` Feature model), and possibly an empty ``{}`` geometry.
    :func:`unproject_item_from_es` restores the GeoJSON/STAC read contract so
    the OGC ``/items`` dispatch returns the same shape as the PostgreSQL
    ``stream_items`` path. Mirrors ``stac/search.py::_es_hits_to_features``.
    """
    from dynastore.models.ogc import Feature
    from dynastore.modules.elasticsearch.items_projection import (
        unproject_item_from_es,
    )

    features: list = []
    for raw in raw_features:
        if isinstance(raw, Feature):
            features.append(raw)
            continue
        clean = unproject_item_from_es(raw) if isinstance(raw, dict) else raw
        try:
            feat = Feature.model_validate(clean)
        except Exception as exc:  # noqa: BLE001 — skip a malformed hit, never 500
            logger.warning(
                "OGC /items → SEARCH-driver dispatch: skipping malformed hit "
                "(catalog=%s, collection=%s): %s",
                catalog_id, collection_id, exc,
            )
            continue
        features.append(feat)
    return features


async def maybe_dispatch_items_to_search_driver(
    catalog_id: str,
    collection_id: str,
    *,
    bbox: Optional[List[float]] = None,
    intersects: Optional[Dict[str, Any]] = None,
    datetime: Optional[str] = None,
    ids: Optional[List[str]] = None,
    limit: int = 10,
    offset: int = 0,
    has_complex_filter: bool = False,
) -> Optional[QueryResponse]:
    """Dispatch an OGC ``/items`` listing to the collection's routing-pinned
    items SEARCH driver, when one is configured and search-capable.

    Used by the OGC API - Features and OGC API - Records ``/items`` endpoints
    so they resolve the items SEARCH driver via routing
    (:func:`router.get_items_search_driver` — ``Operation.SEARCH`` then a
    ``READ`` fallback) and dispatch the structural query to **that driver**
    through the backend-agnostic
    :class:`~dynastore.models.protocols.item_search.ItemSearchProtocol`
    capability — exactly as STAC ``/search`` does after #1257. A public-ES
    catalog, a GEOID-style catalog routing SEARCH to its tenant-private ES
    index, or any future search-capable driver are all served the same way,
    with no hardcoded driver class (#1047).

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
    """
    # Only basic structural filters are routed to a search backend today;
    # CQL2 / attribute-shorthand filters continue to the PG path.
    if has_complex_filter:
        return None

    from dynastore.modules.storage import router as _router
    from dynastore.models.protocols.storage_driver import Capability
    from dynastore.models.protocols.item_search import ItemSearchProtocol

    try:
        resolved = await _router.get_items_search_driver(
            catalog_id, collection_id,
        )
    except Exception:
        return None

    driver: Any = resolved.driver
    if driver is None:
        return None

    # A read-primary fallback (PostgreSQL) advertises QUERY_FALLBACK_SOURCE:
    # no dedicated search backend, so defer to the PG stream_items path.
    if Capability.QUERY_FALLBACK_SOURCE in getattr(
        driver, "capabilities", frozenset()
    ):
        return None

    # The resolved driver must implement the structural-search capability.
    if not isinstance(driver, ItemSearchProtocol):
        return None

    try:
        result = await driver.search_items_struct(
            catalog_id=catalog_id,
            collections=[collection_id],
            ids=ids,
            bbox=bbox,
            intersects=intersects,
            datetime=datetime,
            limit=limit,
            offset=offset,
        )
    except Exception as exc:  # noqa: BLE001 — degrade to PG path, never 500
        logger.warning(
            "OGC /items → SEARCH-driver dispatch failed "
            "(catalog=%s, collection=%s, driver=%s): %s",
            catalog_id, collection_id, type(driver).__name__, exc,
        )
        return None

    features = _items_hits_to_features(
        result.features, catalog_id, collection_id,
    )

    async def _feature_stream():
        for feat in features:
            yield feat

    return QueryResponse(
        items=_feature_stream(),
        total_count=result.total,
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
    "_",  # cache-buster appended by browsers/jQuery; never an attribute
})


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
) -> QueryRequest:
    """
    Unifies OGC parameter parsing into a structured QueryRequest.

    ``extra_filters`` carries ad-hoc ``?{property}={value}`` shorthand
    parameters (the simple single-field equality form). They are converted to
    CQL2-Text equality clauses and combined with any explicit ``filter`` so both
    paths share the same validation, identifier safety, and parameter binding.
    """
    request_obj = QueryRequest(
        limit=limit,
        offset=offset,
        include_total_count=include_total_count,
        filters=[],
    )

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
            parsed_bbox = tuple(map(float, bbox.split(",")))
            if len(parsed_bbox) != 4:
                raise ValueError("BBOX must have 4 coordinates.")
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
            raise HTTPException(status_code=400, detail=f"Invalid BBOX: {e}")

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
            raise HTTPException(status_code=400, detail=f"Invalid datetime format: {e}")
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
) -> StreamingResponse:
    """
    Unified streaming response for OGC Features/WFS/DWH.
    """
    ogc_metadata = OGCResponseMetadata(
        numberMatched=query_response.total_count,
        links=[l.model_dump() if hasattr(l, "model_dump") else l for l in links]
        if links
        else None,
    )

    return format_response(
        request=request,
        features=query_response.items,
        output_format=output_format,
        collection_id=collection_id,
        target_srid=target_srid,
        metadata=ogc_metadata,
    )
