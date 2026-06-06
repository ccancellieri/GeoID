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
from typing import AsyncIterator, FrozenSet, Optional

from fastapi import APIRouter, Body, Depends, FastAPI, HTTPException, Request

from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.query import parse_hints_param  # noqa: E402
from dynastore.models.ogc import Feature
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.joins.bq_secondary import stream_bigquery_secondary
from dynastore.modules.joins.executor import index_secondary, run_join
from dynastore.modules.joins.models import (
    BigQuerySecondarySpec,
    JoinRequest,
    NamedSecondarySpec,
    PrimaryFilterSpec,
)
from dynastore.modules.storage.hints import Hint
from dynastore.modules.storage.router import resolve_drivers

logger = logging.getLogger(__name__)


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
) -> AsyncIterator[Feature]:
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

    def _register_routes(self) -> None:
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/join",
            self.describe_join, methods=["GET"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/join",
            self.execute_join, methods=["POST"],
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
    ):
        """Execute the join.

        Accepts ``?hints=`` to steer primary-driver selection — e.g.
        ``?hints=geometry_exact`` routes to the PostgreSQL driver (exact
        full-precision geometry) instead of a simplified-geometry search
        backend. Hints are forwarded to driver resolution and unioned with
        the baseline ``JOIN`` hint; omitting the parameter preserves the
        existing routing behaviour.

        PR-3: BigQuerySecondarySpec runs the join end-to-end via the
        platform's driver registry for the primary side. NamedSecondarySpec
        stub remains until its own PR.
        """
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
            limit = body.paging.limit if body.paging else 100_000
            query_request = _build_primary_query_request(body.primary_filter, limit=limit)
            try:
                primary_stream = _stream_primary_features(
                    primary_driver,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    primary_column=body.join.primary_column,
                    limit=limit,
                    query_request=query_request,
                )
                joined = [
                    feat async for feat in run_join(
                        body, primary_stream=primary_stream,
                        secondary_index=secondary_index,
                    )
                ]
            except ValueError as e:
                raise HTTPException(
                    status_code=400, detail=f"Invalid primary_filter: {e}",
                ) from e
            return {
                "type": "FeatureCollection",
                "features": [f.model_dump(by_alias=True, exclude_none=True) for f in joined],
                "_join_meta": {
                    "secondary_rows_materialized": len(secondary_index),
                    "joined_features": len(joined),
                },
            }

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
            limit = body.paging.limit if body.paging else 100_000
            query_request = _build_primary_query_request(body.primary_filter, limit=limit)
            try:
                primary_stream = _stream_primary_features(
                    primary_driver,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    primary_column=body.join.primary_column,
                    limit=limit,
                    query_request=query_request,
                )
                joined = [
                    feat async for feat in run_join(
                        body, primary_stream=primary_stream,
                        secondary_index=secondary_index,
                    )
                ]
            except ValueError as e:
                raise HTTPException(
                    status_code=400, detail=f"Invalid primary_filter: {e}",
                ) from e
            return {
                "type": "FeatureCollection",
                "features": [f.model_dump(by_alias=True, exclude_none=True) for f in joined],
                "_join_meta": {
                    "secondary_rows_materialized": len(secondary_index),
                    "joined_features": len(joined),
                    "secondary_ref": body.secondary.ref,
                },
            }

        raise HTTPException(
            status_code=400,
            detail=f"Unsupported secondary spec: {type(body.secondary).__name__}",
        )
