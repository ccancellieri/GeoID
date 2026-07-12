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

import logging
from typing import TYPE_CHECKING, Any, FrozenSet, List, Optional, Tuple

if TYPE_CHECKING:
    from starlette.requests import Request

from dynastore.models.protocols import ItemsProtocol
from dynastore.models.query_builder import FilterCondition, QueryRequest
from dynastore.models.driver_context import DriverContext
from dynastore.tools.discovery import get_protocol
from dynastore.modules.stac.stac_config import StacPluginConfig

logger = logging.getLogger(__name__)


async def get_stac_items_paginated(
    conn: Any,
    catalog_id: str,
    collection_id: str,
    limit: int,
    offset: int,
    stac_config: Optional[StacPluginConfig] = None,
    cql_filter: Optional[str] = None,
    request: "Optional[Request]" = None,
    hints: FrozenSet = frozenset(),
    structural_filters: Optional[List[FilterCondition]] = None,
    bbox: Optional[List[float]] = None,
    datetime_param: Optional[str] = None,
) -> Tuple[list, int]:
    """
    Fetches a paginated list of items for STAC using the optimised QueryOptimizer path.

    Geometries are simplified at the database level based on the StacPluginConfig.
    The total count is derived from COUNT(*) OVER() embedded in the same query,
    so no separate count query is needed.

    ``cql_filter`` is an optional CQL2-Text expression (already combining any
    explicit ``filter`` and ``?{property}={value}`` shorthand). It is validated
    and parameter-bound by the shared QueryOptimizer CQL path; an unknown
    property raises ``ValueError`` (surfaced as 400 by the route handler).

    ``structural_filters`` carries the ``bbox=``/``datetime=`` predicates
    (``geom``/``validity`` :class:`FilterCondition`) built by the route
    handler — the PostgreSQL ``QueryOptimizer`` WHERE-builder consumes
    ``QueryRequest.filters``, not the driver-agnostic top-level ``bbox``/
    ``datetime`` fields. ``bbox``/``datetime_param`` are ALSO set on the
    ``QueryRequest`` (unused by the PG path) so that if ``stream_items``'s own
    routing-aware dispatch resolves a non-PG (Elasticsearch) driver instead of
    running this SQL path, that driver's structural query builder — which
    reads the top-level fields, not ``filters`` — still applies the same
    spatial/temporal predicate rather than silently ignoring it.

    ``request`` is threaded from the route handler for PG row-level ABAC: when
    the collection carries an ``access_envelope`` sidecar, the caller's read
    scope is compiled from request state and injected into the QueryRequest before
    ``stream_items``. System/internal callers pass ``request=None`` and instead
    explicitly set ``access_filter=AccessFilter.allow_everything()`` on the
    QueryRequest they supply to a higher-level call; this path skips the check and
    is thus safe for both user-facing and privileged reads.
    """
    items_svc = get_protocol(ItemsProtocol)
    if not items_svc:
        raise RuntimeError("ItemsProtocol not available")

    simplification: float = 0.0001
    if stac_config and stac_config.simplification:
        simplification = stac_config.simplification.default_tolerance

    query_request = QueryRequest(
        limit=limit,
        offset=offset,
        include_total_count=True,
        cql_filter=cql_filter,
        filters=list(structural_filters) if structural_filters else [],
        bbox=bbox,
        datetime=datetime_param,
        raw_params={
            "geom_format": "WKB",
            "simplification": simplification,
            "lang": "*",  # stac_generator handles specific language selection
        },
    )

    # PG row-level ABAC: compile and inject access_filter when the collection
    # uses the PG access_envelope sidecar and an HTTP request is available.
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

    from dynastore.modules.storage.drivers.pg_sidecars.base import ConsumerType
    query_response = await items_svc.stream_items(
        catalog_id=catalog_id,
        collection_id=collection_id,
        request=query_request,
        ctx=DriverContext(db_resource=conn) if conn is not None else None,
        consumer=ConsumerType.STAC,
        hints=hints,
    )

    if query_response is None:
        return [], 0

    processed_result = [feature async for feature in query_response]
    total_count: int = query_response.total_count or 0

    return processed_result, total_count
