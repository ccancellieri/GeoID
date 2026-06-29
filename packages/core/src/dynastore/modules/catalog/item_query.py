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

"""
Query and search mixin for ItemService.

Extracted from item_service.py to reduce file size.  All methods access
``self.*`` helpers defined on the main ``ItemService`` class, which
inherits from this mixin.
"""

import logging
from contextlib import asynccontextmanager
from typing import List, Optional, Any, Dict, Tuple, AsyncIterator, FrozenSet, TYPE_CHECKING

from sqlalchemy import literal_column, text

if TYPE_CHECKING:
    from dynastore.modules.storage.hints import Hint  # noqa: F401

from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    GeoDQLQuery,
    DbResource,
    ResultHandler,
    managed_transaction,
    is_async_resource,
)
from dynastore.modules.storage.driver_config import (
    ItemsPostgresqlDriverConfig,
)
from dynastore.models.driver_context import DriverContext
from dynastore.models.ogc import Feature
from dynastore.models.protocols import ConfigsProtocol
from dynastore.modules.storage.drivers.pg_sidecars import driver_sidecars
from dynastore.modules.storage.drivers.pg_sidecars.base import (
    FeaturePipelineContext,
    ConsumerType,
)
from dynastore.tools.discovery import get_protocol
from dynastore.tools.db import validate_sql_identifier, validate_column_identifier
from dynastore.models.query_builder import QueryRequest, QueryResponse
from dynastore.modules.catalog.query_optimizer import QueryOptimizer

logger = logging.getLogger(__name__)


async def _run_query(conn, stmt, params=None):
    """Run a statement on either sync or async connection."""
    import inspect as _inspect

    result = conn.execute(stmt, params or {})
    if _inspect.isawaitable(result):
        result = await result
    return result


# ---------------------------------------------------------------------------
# Non-PG driver dispatch helpers
# ---------------------------------------------------------------------------

def _pick_operation(request: Optional[QueryRequest]) -> str:
    """Choose the routing operation based on query content.

    Genuine search predicates (bbox/spatial, attribute filters, CQL2,
    fulltext) → SEARCH operation. A plain browse → READ operation.

    Temporal-validity conditions are NOT search predicates: every OGC
    features browse carries an implicit ``validity @> now()`` default
    (``parse_ogc_query_request`` appends it whenever no ``datetime`` is
    supplied), and an explicit ``?datetime=`` is the standard OGC browse
    parameter. Both are READ modifiers applied uniformly on the read
    backend, so they must not flip a browse onto the SEARCH backend. Were
    they counted, ``request.filters`` would never be empty and every
    ``/items`` browse would be misrouted to the SEARCH primary (ES),
    bypassing the operator's READ routing (PG/geometry-exact) and losing
    the total count.
    """
    from dynastore.modules.storage.routing_config import Operation

    if request is None:
        return Operation.READ
    if getattr(request, "cql_filter", None):
        return Operation.SEARCH
    filters = getattr(request, "filters", None) or []
    if any(getattr(fc, "field", None) != "validity" for fc in filters):
        return Operation.SEARCH
    return Operation.READ


def is_query_fallback_driver(driver: Any) -> bool:
    """True when a resolved items driver is the read-primary PG fallback.

    Both item-listing dispatch paths — the ``read_entities`` browse path
    (:func:`_try_driver_dispatch`) and the structural-search path
    (:func:`dynastore.extensions.tools.query.maybe_dispatch_items_to_search_driver`)
    — must decline to dispatch and let the PostgreSQL ``stream_items`` path
    serve the listing when no dedicated non-PG backend is configured. That is
    the case when driver resolution yielded nothing (``driver is None``) or the
    resolved driver advertises :attr:`Capability.QUERY_FALLBACK_SOURCE` (the PG
    read primary). This is the single thin slice the two otherwise-distinct
    resolvers genuinely share; the resolution and feature-projection steps
    remain path-specific.

    ``capabilities`` is read defensively (``getattr(..., frozenset())``): a real
    driver always declares it, but a missing attribute degrades to "not a
    fallback" rather than raising.
    """
    if driver is None:
        return True
    from dynastore.models.protocols.storage_driver import Capability

    return Capability.QUERY_FALLBACK_SOURCE in getattr(
        driver, "capabilities", frozenset()
    )


async def _try_driver_dispatch(
    catalog_id: str,
    collection_id: str,
    operation: str,
    request: Optional[QueryRequest],
    limit: int,
    offset: int,
    entity_ids: Optional[List[str]] = None,
    hints: "FrozenSet[Hint]" = frozenset(),
) -> Optional[QueryResponse]:
    """Try to resolve and dispatch to a non-PG storage driver.

    Returns a ``QueryResponse`` backed by the driver's streaming
    ``AsyncIterator[Feature]``, or ``None`` when the PG path should be used.

    ``None`` is returned when:
    - No routing config exists for this collection (legacy PG-only setup).
    - The resolved driver is ``postgresql`` (PG path is the correct path).
    - Driver resolution raises any exception.

    ``hints`` are forwarded to driver resolution. A caller that needs the
    full-precision PG geometry path (e.g. the DWH join / feature export) passes
    ``{Hint.JOIN}`` (or ``{Hint.GEOMETRY_EXACT}``): the default READ routing
    declares those on the PG entry, so the best-overlap matcher selects PG,
    ``is_query_fallback_driver`` is True, and this returns ``None`` to hand the
    read to the inline PG SQL path. With the default empty hints the public
    default routing (ES-first) is preserved.
    """
    try:
        from dynastore.modules.storage.router import get_driver
    except ImportError:
        return None

    try:
        resolved = await get_driver(operation, catalog_id, collection_id, hints=hints)
    except Exception:
        return None

    if is_query_fallback_driver(resolved):
        return None

    # Routing-honouring fallback: ``resolved`` is the routing config's
    # read/search primary, but if it is an ES items driver whose per-tenant
    # index does not exist yet (PG-only catalog, or indexing has not run), it
    # cannot serve. Return None so the next configured driver — the PG read
    # path / system of record — handles the query, instead of returning an
    # empty stream that hides PG-resident rows (#914 silent-empty).
    if getattr(resolved, "is_es_items_driver", False):
        _es_driver: Any = resolved
        _index_check = getattr(_es_driver, "index_available", None)
        if _index_check is not None and not await _index_check(catalog_id):
            return None

    effective_limit = (request.limit if request and request.limit else limit) or limit
    effective_offset = (request.offset if request and request.offset else offset) or offset

    items: AsyncIterator[Feature] = resolved.read_entities(
        catalog_id,
        collection_id,
        entity_ids=entity_ids,
        request=request,
        limit=effective_limit,
        offset=effective_offset,
    )

    return QueryResponse(
        items=_apply_item_pipeline(items, catalog_id, collection_id),
        total_count=None,  # non-PG drivers do not provide a total count
        catalog_id=catalog_id,
        collection_id=collection_id,
    )


async def _apply_item_pipeline(
    items: AsyncIterator,
    catalog_id: str,
    collection_id: str,
    context: Optional[Dict[str, Any]] = None,
) -> AsyncIterator:
    """Wrap item stream with ItemPipelineProtocol stages (streaming).

    The ``can_apply()`` guard is evaluated once per query, not per item.
    If no stages are active, the stream passes through with zero overhead.

    Stages can return ``None`` to drop the item from the output stream.
    """
    try:
        from dynastore.models.protocols.item_pipeline import ItemPipelineProtocol
        from dynastore.tools.discovery import get_protocols

        stages = sorted(
            get_protocols(ItemPipelineProtocol), key=lambda s: s.priority,
        )
        active = [s for s in stages if s.can_apply(catalog_id, collection_id)]
    except Exception:
        active = []

    if not active:
        async for item in items:
            yield item
        return

    ctx = context or {}
    async for item in items:
        doc = item
        dropped = False
        for stage in active:
            try:
                result = await stage.apply(catalog_id, collection_id, doc, ctx)
            except Exception as err:
                logger.warning(
                    "ItemPipeline stage '%s' failed for %s/%s: %s",
                    getattr(stage, "stage_id", "?"),
                    catalog_id, collection_id, err,
                )
                continue
            if result is None:
                # Stage dropped the item — short-circuit remaining stages.
                dropped = True
                break
            doc = result
        if dropped:
            continue
        yield doc


class ItemQueryMixin:
    """Query, search and streaming methods for ItemService.

    Concrete ``ItemService`` must provide: engine, _resolve_physical_schema,
    _resolve_physical_table, _get_collection_config, map_row_to_feature.
    """

    # Stubs for attributes provided by ItemService (the concrete class).
    engine: Optional[DbResource] = None

    async def _resolve_physical_schema(
        self, catalog_id: str, db_resource: Optional[DbResource] = None
    ) -> Optional[str]:
        raise NotImplementedError("ItemQueryMixin requires a concrete _resolve_physical_schema")

    async def _resolve_physical_table(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> Optional[str]:
        raise NotImplementedError("ItemQueryMixin requires a concrete _resolve_physical_table")

    async def _get_collection_config(
        self,
        catalog_id: str,
        collection_id: str,
        config_provider: Optional[ConfigsProtocol] = None,
        db_resource: Optional[DbResource] = None,
    ) -> Any:
        raise NotImplementedError("ItemQueryMixin requires a concrete _get_collection_config")

    async def _resolve_read_policy(
        self,
        catalog_id: str,
        collection_id: str,
    ) -> Optional[Any]:
        """Resolve the collection's :class:`ItemsReadPolicy` for read assembly.

        Fetched once per query (not per row) so the row mapper can honour the
        wire-shape contract — ``feature_type.expose`` value-merge and
        ``external_id_as_feature_id``. Returns ``None`` when the configs
        protocol is unavailable; callers then fall back to default wire shape.
        """
        try:
            from dynastore.modules.storage.read_policy import ItemsReadPolicy

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

    async def _resolve_count_config(
        self,
        catalog_id: str,
        collection_id: str,
    ) -> Optional[Any]:
        """Resolve :class:`ItemsCountConfig` for the collection.

        Returns ``None`` when the configs protocol is unavailable; callers
        then use the class defaults (``estimate_count=True``,
        ``exact_count_threshold=50_000``).
        """
        try:
            from dynastore.modules.storage.read_policy import ItemsCountConfig

            configs = get_protocol(ConfigsProtocol)
            if configs is None:
                return None
            return await configs.get_config(
                ItemsCountConfig,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
        except Exception as exc:  # noqa: BLE001 - count config miss must not break the read
            logger.debug(
                "count config resolution skipped for %s/%s: %s",
                catalog_id,
                collection_id,
                exc,
            )
            return None

    async def _resolve_number_matched(
        self,
        conn: Any,
        phys_schema: str,
        phys_table: str,
        count_request: "QueryRequest",
        context: Dict[str, Any],
        catalog_id: str,
        collection_id: str,
        col_config: Any,
        consumer: Any,
    ) -> Optional[int]:
        """Compute ``numberMatched`` via estimate or exact count.

        Decision logic:
        1. Load :class:`ItemsCountConfig` (defaults: ``estimate_count=True``,
           ``exact_count_threshold=50_000``).
        2. Query ``pg_class.reltuples`` for the physical table — O(1).
        3. The planner estimate is only applicable when the request is
           **unfiltered** (no structured filters, raw_where, or CQL2 filter).
           Filtered requests always get an exact count because ``reltuples``
           has no knowledge of the WHERE predicate — returning the table's
           total row-count for a bbox-filtered query that matches 87 rows is
           incorrect and breaks pagination.
        4. For unfiltered requests: if the estimate is below
           ``exact_count_threshold`` OR ``estimate_count=False``, run an exact
           ``SELECT count(*)``.  Otherwise return the planner estimate.
        5. For filtered requests (any filter present): always run an exact
           ``SELECT count(*) FROM (filtered-sql) AS sub``.

        OGC API Features §7.15 permits ``numberMatched`` to be approximate
        only for unfiltered collection listings; filtered results must be exact
        so pagination links and result counts remain correct.
        """
        from dynastore.modules.storage.read_policy import ItemsCountConfig

        count_cfg = await self._resolve_count_config(catalog_id, collection_id)
        if count_cfg is None:
            count_cfg = ItemsCountConfig()

        estimate_count: bool = bool(count_cfg.estimate_count)
        exact_threshold: int = int(count_cfg.exact_count_threshold)

        # Determine whether the request carries any filter predicate.
        # cql_filter, filters (structured), and raw_where are all checked;
        # item_ids is also a filter (restricts the result set).
        has_filters = bool(
            count_request.filters
            or count_request.raw_where
            or count_request.cql_filter
            or count_request.item_ids
            or count_request.bbox
            or count_request.intersects
            or count_request.datetime
        )

        if estimate_count and not has_filters:
            # Fast O(1) planner estimate from pg_class, safe only for
            # unfiltered listings.  ``reltuples`` is updated by ANALYZE; it is
            # a reasonable proxy for the total live-row count of the collection.
            try:
                reltuples: int = await DQLQuery(
                    'SELECT reltuples::bigint FROM pg_class '
                    'WHERE oid = :table_oid::regclass',
                    result_handler=ResultHandler.SCALAR,
                ).execute(conn, table_oid=f'"{phys_schema}"."{phys_table}"')
                if reltuples is None:
                    reltuples = 0
            except Exception as exc:  # noqa: BLE001 - fall back to exact count
                logger.debug(
                    "pg_class reltuples lookup failed for %s.%s: %s — falling back to exact count",
                    phys_schema, phys_table, exc,
                )
                reltuples = 0
                estimate_count = False

            if estimate_count and reltuples >= exact_threshold:
                return reltuples

        # Filtered requests, requests below the size threshold, or when
        # estimation is disabled: run an exact count.
        # Use a fresh copy of context so mutations from the data-query
        # transformation pass do not bleed into the count transformation.
        count_sql, count_params = await self._apply_query_transformations(
            count_request, dict(context), catalog_id, collection_id, col_config,
            db_resource=conn, consumer=consumer,
        )
        count_wrapper = f"SELECT count(*) FROM ({count_sql}) AS sub"
        return await DQLQuery(
            count_wrapper, result_handler=ResultHandler.SCALAR
        ).execute(conn, **(count_params or {}))

    def map_row_to_feature(
        self,
        row: Dict[str, Any],
        col_config: Any,
        lang: str = "en",
        context: Any = None,
        read_policy: Optional[Any] = None,
    ) -> Feature:
        raise NotImplementedError("ItemQueryMixin requires a concrete map_row_to_feature")

    async def get_features(
        self,
        conn: DbResource,
        catalog_id: str,
        collection_id: str,
        col_config: Optional[ItemsPostgresqlDriverConfig] = None,
        item_ids: Optional[List[str]] = None,
        request: Optional[QueryRequest] = None,
        **kwargs,
    ) -> List[Feature]:
        """Retrieves a list of items via the QueryOptimizer path."""
        if not col_config:
            from dynastore.modules.storage.router import get_driver as _get_driver
            from dynastore.modules.storage.routing_config import Operation
            _driver = await _get_driver(Operation.READ, catalog_id, collection_id)
            col_config = await _driver.get_driver_config(
                catalog_id, collection_id, db_resource=conn
            )

        phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
        phys_table = await self._resolve_physical_table(
            catalog_id, collection_id, db_resource=conn
        )

        if request is None:
            from dynastore.models.query_builder import FieldSelection
            request = QueryRequest(
                item_ids=item_ids or None,
                select=[FieldSelection(field="*")],
            )

        if col_config is None or phys_schema is None or phys_table is None:
            return []
        read_policy = await self._resolve_read_policy(catalog_id, collection_id)
        optimizer = QueryOptimizer(col_config)
        sql, params = optimizer.build_optimized_query(request, phys_schema, phys_table)
        rows = await _run_query(conn, text(sql), params)
        return [
            self.map_row_to_feature(
                dict(row._mapping), col_config, read_policy=read_policy
            )
            for row in rows
        ]

    async def get_features_query(
        self,
        conn: DbResource,
        catalog_id: str,
        collection_id: str,
        col_config: ItemsPostgresqlDriverConfig,
        params: Dict[str, Any],
        param_suffix: str = "",
        access_filter: Optional[Any] = None,
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Exposes the underlying query generation for features (used by Tiles/MVT).
        Now uses pluggable query transformations.

        ``access_filter`` is injected by callers that already know the read
        scope.  Privileged system callers (tile rendering, DWH) should pass
        ``AccessFilter.allow_everything()`` so the access_envelope JOIN does not
        fail closed when no user principal is present.  User-facing callers that
        carry an HTTP request should compile the filter via
        ``compile_read_access_filter`` before calling this helper.
        """
        # 1. Build base QueryRequest from params
        query_request = self._build_base_query_request(params, col_config)

        # Inject the caller-supplied access_filter so the query optimizer's
        # access_envelope sidecar enforcement uses it (system reads pass
        # allow_everything(); user-facing reads pass a compiled principal scope).
        if access_filter is not None:
            query_request.access_filter = access_filter

        # 2. Apply transformations and generate SQL
        context = {
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "col_config": col_config,
            **params,
        }
        sql, bind_params = await self._apply_query_transformations(
            query_request, context, catalog_id, collection_id, col_config,
            db_resource=conn,
        )

        # 3. Apply param suffixing for multi-collection queries
        if param_suffix:
            sql, bind_params = self._apply_param_suffix(sql, bind_params, param_suffix)

        return sql, bind_params

    def _build_base_query_request(
        self, params: Dict[str, Any], col_config: ItemsPostgresqlDriverConfig
    ) -> QueryRequest:
        """Build base QueryRequest from params before transformations"""
        from dynastore.models.query_builder import QueryRequest, FieldSelection
        from dynastore.modules.storage.drivers.pg_sidecars import (
            SidecarRegistry,
            driver_sidecars,
        )

        # Resolve the collection's sidecars once. The geometry and property
        # field names come from the sidecars themselves so this base query is
        # agnostic to the physical storage layout (JSONB blob vs COLUMNAR
        # columns, default vs renamed geometry column).
        sidecars = [
            sc
            for sc in (
                SidecarRegistry.get_sidecar(cfg, lenient=True)
                for cfg in driver_sidecars(col_config)
            )
            if sc is not None
        ]

        is_mvt = params.get("geom_format") == "MVT"
        feature_type = params.get("feature_type")

        # MVT honours ``ItemsReadPolicy.feature_type`` as the wire-shape contract:
        # ST_AsMVT emits every selected column as a tile property, so a row-time
        # projection (the /items path) cannot be retro-fitted here — the SELECT
        # list IS the projection. The trinary ``expose`` semantics
        # (None = schema baseline, [] = nothing, list = schema + listed
        # computed) live in ``project_select_for_feature_type``; we pass the
        # collection's declared schema fields so the default surfaces the same
        # property set the write schema declares.
        if is_mvt and feature_type is not None:
            from dynastore.modules.storage.read_policy import (
                project_select_for_feature_type,
            )
            selects = project_select_for_feature_type(
                feature_type, params.get("schema_fields") or None
            )
            # ``geom`` is reserved by ``MVTQueryTransform`` for the per-row
            # ``ST_AsMVTGeom(...) AS geom`` projection that the wrapping
            # ``ST_AsMVT(_, 'geom')`` aggregates. Without ``skip_geometry``,
            # the geometry sidecar additionally emits
            # ``ST_AsGeoJSON(geom)::jsonb AS geom`` whenever ``geom``/``geometry``
            # is referenced by any selection/filter, producing two columns
            # aliased ``geom`` — Postgres then fails the outer
            # ``SELECT "geom" FROM (_mvt_inner)`` with
            # ``AmbiguousColumnError`` (live tile crash, region6/glosisdemo).
            query_req = QueryRequest(
                select=selects,
                limit=params.get("limit"),
                offset=params.get("offset"),
                raw_where=params.get("where"),
                raw_params=params.get("raw_params", {}),
                skip_geometry=True,
            )
            if params.get("cql_filter"):
                query_req.cql_filter = params["cql_filter"]
            return query_req

        # Mandatory ID
        selects = [FieldSelection(field="geoid", alias="id")]

        # Geometry (if requested or by default). Skipped for MVT, where the MVT
        # query transform injects the on-the-fly ST_AsMVTGeom projection.
        if params.get("with_geometry", True) and not is_mvt:
            geom_field = next(
                (
                    name
                    for name in (sc.get_main_geometry_field() for sc in sidecars)
                    if name
                ),
                None,
            )
            if geom_field:
                selects.append(FieldSelection(field=geom_field))

        # Properties — storage-agnostic. Each sidecar reports its property field
        # names (JSONB → the blob column; COLUMNAR → the individual columns);
        # the optimizer resolves each to its SQL expression.
        for sc in sidecars:
            for field_name in sc.get_property_field_names():
                selects.append(FieldSelection(field=field_name))

        query_req = QueryRequest(
            select=selects,
            limit=params.get("limit"),
            offset=params.get("offset"),
            raw_where=params.get("where"),
            raw_params=params.get("raw_params", {}),
        )

        # CQL Filter — carry it through on the QueryRequest. The actual
        # CQL→SQL conversion happens in ``_apply_query_transformations``,
        # which builds the field_mapping from the collection's queryable
        # fields (``get_all_queryable_fields``). Pre-parsing here with
        # ``field_mapping=None`` raised "field_mapping is required for SQL
        # conversion" and, on the tiles/MVT path, that exception dropped the
        # whole collection from the tile (empty 204) — breaking every CQL2
        # tile filter, e.g. per-``asset_id`` rendering.
        if params.get("cql_filter"):
            query_req.cql_filter = params["cql_filter"]

        return query_req

    async def _apply_query_transformations(
        self,
        query_request: QueryRequest,
        context: Dict[str, Any],
        catalog_id: str,
        collection_id: str,
        col_config: ItemsPostgresqlDriverConfig,
        db_resource: Optional[DbResource] = None,
        consumer: ConsumerType = ConsumerType.GENERIC,
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Applies registered query transformations and generates optimized SQL.

        This centralizes the transformation logic used across get_features_query,
        _prepare_search, and stream_items.
        """
        from dynastore.models.protocols import QueryTransformProtocol
        from dynastore.tools.discovery import get_protocols

        # Get and sort transformers by priority
        transformers = sorted(
            get_protocols(QueryTransformProtocol),
            key=lambda t: getattr(t, "priority", 100),
        )

        # Apply QueryRequest-level transformations
        for transformer in transformers:
            if transformer.can_transform(context):
                logger.debug(f"Applying transformation: {transformer.transform_id}")
                query_request = transformer.transform_query(query_request, context)

        # Apply CQL Filter parsing if present
        if query_request.cql_filter:
            from dynastore.modules.tools.cql import parse_cql_filter

            # Use a temporary optimizer to get all available fields for validation.
            # Thread the collection's ``ItemsSchema.fields`` (already carried on
            # the params dict by the MVT path, ``tiles_db._build_collection_subquery``)
            # so JSONB-only schema fields (e.g. ``START_DATE`` on a VECTOR
            # collection with no columnar storage for them) resolve to a typed
            # JSONB extraction instead of being rejected as ``Unknown field``.
            temp_optimizer = QueryOptimizer(
                col_config,
                consumer=consumer,
                schema_fields=context.get("schema_fields"),
            )
            queryable_fields = temp_optimizer.get_all_queryable_fields()

            # Create mapping for CQL parser.
            # ``literal_column`` (not ``text``): pygeofilter's ``to_filter``
            # needs a column-like object with comparison operators to emit a
            # real bound-parameter predicate. A ``TextClause`` has none, so it
            # silently collapses every comparison to an always-false 1=0.
            field_mapping = {
                name: literal_column(field_def.sql_expression)
                for name, field_def in queryable_fields.items()
            }
            # Add implicit fields
            field_mapping.update(
                {
                    "geoid": literal_column("h.geoid"),
                    "deleted_at": literal_column("h.deleted_at"),
                    "transaction_time": literal_column("h.transaction_time"),
                }
            )
            # #974: ``validity`` only exists on the hub when it is a
            # partition key; otherwise it lives on a sidecar (or nowhere at
            # all when ``ItemsWritePolicy.enable_validity`` is False).
            # Defer to the optimizer so CQL filters bind to the correct
            # column reference instead of a non-existent ``h.validity``.
            validity_expr = temp_optimizer.resolve_validity_expression()
            if validity_expr is not None:
                field_mapping["validity"] = literal_column(validity_expr)
            else:
                # Bind ``validity`` to ``NULL::tstzrange`` so CQL filters
                # referencing it parse cleanly and evaluate to NULL (the
                # standard Postgres semantics for a missing temporal axis).
                field_mapping["validity"] = literal_column("NULL::tstzrange")

            try:
                filter_lang = (
                    getattr(query_request, "filter_lang", "cql2-text") or "cql2-text"
                ).lower()
                geometry_srid = getattr(query_request, "filter_crs_srid", None)
                if filter_lang == "cql2-json":
                    # OGC API Features Part 3 allows the JSON encoding of CQL2;
                    # route through the dedicated JSON parser so callers can
                    # POST a structured filter (or pass a JSON string on GET).
                    from dynastore.modules.tools.cql import parse_cql2_json_filter
                    cql_where, cql_params = parse_cql2_json_filter(
                        query_request.cql_filter,
                        field_mapping=field_mapping,
                        geometry_srid=geometry_srid,
                    )
                else:
                    cql_where, cql_params = parse_cql_filter(
                        query_request.cql_filter,
                        field_mapping=field_mapping,
                        parser_type="cql2",  # OGC API Features uses CQL2 by default
                        geometry_srid=geometry_srid,
                    )

                if cql_where:
                    if query_request.raw_where:
                        query_request.raw_where = (
                            f"({query_request.raw_where}) AND ({cql_where})"
                        )
                    else:
                        query_request.raw_where = cql_where
                    query_request.raw_params.update(cql_params)
            except ValueError as e:
                # Re-raise as ValueError so it can be caught and returned as 400
                raise ValueError(f"Invalid CQL filter: {e}") from e

        # Resolve physical storage and generate SQL.  Thread the caller's
        # in-flight conn through so resolution shares the warm transaction
        # state (avoids cold per-pod routing-config lookups that surfaced
        # as intermittent 400 "Could not resolve storage").
        phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=db_resource)
        phys_table = await self._resolve_physical_table(
            catalog_id, collection_id, db_resource=db_resource,
        )
        if not phys_schema or not phys_table:
            # Name the missing leg so a real failure on review tells us
            # whether the catalog row's physical_schema is unset, the
            # driver config's physical_table is unset, or both — without
            # this, every miss surfaced as the same opaque 500.
            raise ValueError(
                f"Could not resolve storage for {catalog_id}:{collection_id} "
                f"(phys_schema={phys_schema!r}, phys_table={phys_table!r}, "
                f"db_resource={'passed' if db_resource is not None else 'absent'})"
            )

        optimizer = QueryOptimizer(
            col_config,
            consumer=consumer,
            schema_fields=context.get("schema_fields"),
        )
        sql, params = optimizer.build_optimized_query(
            query_request, schema=phys_schema, table=phys_table
        )

        # Apply SQL-level transformations
        for transformer in transformers:
            if transformer.can_transform(context):
                sql, params = transformer.post_process_sql(sql, params, context)

        return sql, params

    def _apply_param_suffix(
        self, sql: str, bind_params: Dict[str, Any], suffix: str
    ) -> Tuple[str, Dict[str, Any]]:
        """Applies a suffix to bind parameters to avoid collisions."""
        import re

        new_params = {}
        sql_str = str(sql)
        for key, val in bind_params.items():
            new_key = f"{key}{suffix}"
            new_params[new_key] = val
            sql_str = re.sub(f":{key}(?=\\b)", f":{new_key}", sql_str)
        return sql_str, new_params

    async def get_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        ctx: Optional[DriverContext] = None,
        lang: str = "en",
        context: Optional[Any] = None,
        access_filter: Optional[Any] = None,
    ) -> Optional[Feature]:
        db_resource = ctx.db_resource if ctx else None
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        # --- Non-PG driver dispatch ---
        driver_response = await _try_driver_dispatch(
            catalog_id, collection_id, _pick_operation(None),
            None, 1, 0, entity_ids=[item_id],
        )
        if driver_response is not None:
            async for feature in driver_response.items:
                return feature
            return None

        # --- PG path ---
        async with managed_transaction(db_resource or self.engine) as conn:
            col_config = await self._get_collection_config(
                catalog_id, collection_id, db_resource=conn
            )
            phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
            phys_table = await self._resolve_physical_table(
                catalog_id, collection_id, db_resource=conn
            )
            if not phys_schema or not phys_table:
                return None

            from dynastore.models.query_builder import FieldSelection
            request = QueryRequest(
                item_ids=[str(item_id)],
                limit=1,
                select=[FieldSelection(field="*")],
            )
            # Inject the caller-supplied access_filter (user-facing reads compile
            # this from request state via compile_read_access_filter; system reads
            # pass AccessFilter.allow_everything() to avoid a fail-closed blackout).
            if access_filter is not None:
                request.access_filter = access_filter
            query_ctx: Dict[str, Any] = {
                "catalog_id": catalog_id,
                "collection_id": collection_id,
                "col_config": col_config,
            }
            # Thread the caller's consumer (e.g. STAC) into the SQL build so
            # consumer-gated sidecars — like stac_metadata, which serves only
            # ConsumerType.STAC — are JOINed and their columns selected.
            # Without this the single-item read defaults to GENERIC and the
            # stac_metadata payload (external_assets/extensions/extra_fields)
            # is silently dropped from the row.
            consumer = getattr(context, "consumer", None) or ConsumerType.GENERIC
            sql, params = await self._apply_query_transformations(
                request, query_ctx, catalog_id, collection_id, col_config,
                db_resource=conn, consumer=consumer,
            )

            result = await _run_query(conn, text(sql), params)
            row = result.mappings().first()

            read_policy = await self._resolve_read_policy(catalog_id, collection_id)
            return (
                self.map_row_to_feature(
                    dict(row), col_config, lang=lang, context=context,
                    read_policy=read_policy,
                )
                if row else None
            )

    async def resolve_external_id_by_geoid(
        self,
        catalog_id: str,
        collection_id: str,
        geoid: str,
        ctx: Optional[DriverContext] = None,
    ) -> Optional[str]:
        """Look up the ``external_id`` of the active row whose ``geoid`` matches.

        Symmetric counterpart to the sidecar-based path used by
        ``delete_item``: post-#1212 the canonical surface-level id IS the
        geoid, so write handlers that receive a path-id need a cheap way to
        translate that geoid back to the row's ``external_id`` before they
        delegate to ``upsert`` (whose conflict resolution keys on
        ``external_id``). Without this, a PUT against an existing geoid hits
        the external_id-keyed path with the geoid string, misses every row
        (no row has that geoid stored as its external_id), and inserts a
        duplicate. See #1367.

        Returns ``None`` when:
          * ``geoid`` is not a valid UUID (no UUID-typed row can match),
          * the catalog/collection cannot be physically resolved,
          * the collection has no sidecar carrying a ``feature_id_field_name``,
          * or no active row matches.

        Returns the row's stored ``external_id`` string otherwise. Read-only;
        safe to call on a shared transaction.
        """
        import uuid as _uuid

        try:
            _uuid.UUID(str(geoid))
        except (ValueError, AttributeError, TypeError):
            return None

        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        db_resource = ctx.db_resource if ctx else None
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            phys_table = await self._resolve_physical_table(
                catalog_id, collection_id, db_resource=conn
            )
            if not phys_schema or not phys_table:
                return None

            from dynastore.modules.storage.router import get_driver as _get_driver
            from dynastore.modules.storage.routing_config import Operation
            _driver = await _get_driver(Operation.READ, catalog_id, collection_id)
            col_config = await _driver.get_driver_config(
                catalog_id, collection_id, db_resource=conn,
            )
            if not col_config or not driver_sidecars(col_config):
                return None

            from dynastore.modules.storage.drivers.pg_sidecars.registry import (
                SidecarRegistry,
            )

            for sc in driver_sidecars(col_config):
                if not sc.feature_id_field_name:
                    continue
                sidecar = SidecarRegistry.get_sidecar(sc)
                if sidecar is None:
                    continue
                # Validate identifiers before SQL interpolation (#2314)
                fid_col = validate_column_identifier(sc.feature_id_field_name)
                sc_id = validate_column_identifier(str(sidecar.sidecar_id))
                sc_table = f"{phys_table}_{sc_id}"
                ext_id = await DQLQuery(
                    f'SELECT s.{fid_col} '
                    f'FROM "{phys_schema}"."{phys_table}" h '
                    f'JOIN "{phys_schema}"."{sc_table}" s '
                    f"ON s.geoid = h.geoid "
                    f"WHERE h.geoid = :geoid "
                    f"AND h.deleted_at IS NULL "
                    f"LIMIT 1",
                    result_handler=ResultHandler.SCALAR,
                ).execute(conn, geoid=str(geoid))
                if ext_id is not None:
                    return str(ext_id)
                # First sidecar carrying the identity field is authoritative —
                # mirrors delete_item's single-sidecar resolution loop.
                return None

            return None

    async def _resolve_external_ids_to_geoids(
        self,
        conn: Any,
        phys_schema: str,
        phys_table: str,
        sidecar_config: Any,
        feature_id_field: str,
        ext_ids: List[str],
    ) -> List[str]:
        """Resolve external/producer ids to their immutable ``geoid``(s) (#2314).

        ``feature_id_field_name`` is only the *external* representation of a
        feature. It is NOT unique within a collection — the same external id
        repeats to represent successive versions of the same feature, each with
        its own validity. Internally, sidecars are always joinable by ``geoid``,
        the unique physical id of the item. So instead of matching tile
        prior-bboxes on the external-id column directly (ambiguous under
        versioning, and an identifier-interpolation risk because a column name
        can't be a bound parameter), we resolve external ids to ``geoid`` here,
        once, behind a validated identifier, and let the caller match on
        ``h.geoid`` everywhere else.

        When the sidecar manages validity, only the currently-valid version is
        returned (``validity @> NOW()``); otherwise every matching geoid is
        returned. The caller is degrade-safe, so any error here propagates to an
        empty prior-bbox set rather than a captured-but-wrong one.
        """
        if not ext_ids:
            return []
        # Column/table names are interpolated as SQL identifiers (a parameter
        # cannot bind an identifier), so validate them against the strict
        # allowlist before they ever reach the query string.
        field = validate_sql_identifier(feature_id_field)
        sidecar_id = validate_sql_identifier(
            str(getattr(sidecar_config, "sidecar_id", ""))
        )
        sc_table = f"{phys_table}_{sidecar_id}"
        validity_clause = ""
        validity_column = getattr(sidecar_config, "validity_column", None)
        if getattr(sidecar_config, "has_validity", False) and validity_column:
            vcol = validate_sql_identifier(str(validity_column))
            validity_clause = f" AND s.{vcol} @> NOW()"
        geoids = await DQLQuery(
            f'SELECT DISTINCT s.geoid '
            f'FROM "{phys_schema}"."{sc_table}" s '
            f"WHERE s.{field} = ANY(:_ext_ids){validity_clause}",
            result_handler=ResultHandler.ALL_SCALARS,
        ).execute(conn, _ext_ids=[str(i) for i in ext_ids])
        return [str(g) for g in (geoids or [])]

    async def _fetch_prior_bboxes_bulk(
        self,
        catalog_id: str,
        collection_id: str,
        item_ids: List[str],
    ) -> List[Tuple[float, float, float, float]]:
        """Bulk-read prior bbox extents for a list of item IDs (#1845).

        Issues a single ``WHERE feature_id = ANY(:ids)`` query against the
        physical hub + geometries sidecar tables, reading ONLY the four
        ST_XMin/ST_YMin/ST_XMax/ST_YMax scalar values off the materialized
        ``bbox_geom`` envelope column — never raw geometry. Returns a list of
        ``(west, south, east, north)`` tuples for every item that exists and
        has a non-NULL bbox. Items that don't exist (CREATE path) contribute
        nothing — they simply aren't found in the query.

        This is a pure performance helper; the invalidation result is
        identical to calling ``get_item`` per item. Callers must gate on
        ``is_tile_cache_active`` before calling this.

        The caller supplies the wire-surface ids (``feature.id``) which may be
        UUIDs (internal geoid) OR external/producer ids (e.g. CityJSON GUIDs).
        The query adapts: when the collection has a sidecar with a
        ``feature_id_field_name`` (the external-id column), the WHERE clause
        matches against that text column so any string id is accepted. When no
        such sidecar exists, the ids must be valid UUIDs to match the ``geoid``
        column; non-UUID ids are filtered out and, if none remain, an empty
        list is returned immediately.

        Degrade-safe: any exception returns ``[]`` so the caller's write is
        never blocked. Empty ``item_ids`` short-circuits immediately.
        """
        if not item_ids:
            return []
        try:
            col_config = await self._get_collection_config(catalog_id, collection_id)
            phys_schema = await self._resolve_physical_schema(catalog_id)
            phys_table = await self._resolve_physical_table(catalog_id, collection_id)
            if not phys_schema or not phys_table:
                return []

            import uuid as _uuid

            from dynastore.modules.storage.drivers.pg_sidecars import driver_sidecars

            # Resolve bbox column name from the geometries sidecar config.
            # Default is ``bbox_geom``; fall back to that when no sidecar
            # config is found so the helper is robust to minimal environments.
            bbox_col = "bbox_geom"
            # Find the geometries bbox column and the first sidecar that carries
            # the external feature-id column (``feature_id_field_name``). The
            # external id is only a presentation key; it is mutable and repeats
            # across versions, so it is NEVER used as a match key here (#2314).
            ext_id_field: Optional[str] = None
            ext_id_sidecar: Any = None
            for sc in driver_sidecars(col_config):
                if getattr(sc, "sidecar_type", None) == "geometries":
                    bbox_col = getattr(sc, "bbox_column", None) or bbox_col
                if ext_id_field is None:
                    fid = getattr(sc, "feature_id_field_name", None)
                    if fid:
                        ext_id_field = fid
                        ext_id_sidecar = sc

            # Partition the wire-surface ids: valid UUIDs are already geoids;
            # anything else is an external/producer id (e.g. a CityJSON GUID)
            # that must be resolved to its geoid before matching. We ALWAYS match
            # on ``h.geoid`` — the unique, immutable physical id — never on the
            # external id, which is mutable, repeats across versions (ambiguous),
            # and can't be bound as a parameter when used as a column name
            # (identifier-interpolation risk). This supersedes the #2045
            # text-column match while still accepting external ids (#2314).
            geoids: List[str] = []
            ext_ids: List[str] = []
            for i in item_ids:
                try:
                    _uuid.UUID(str(i))
                    geoids.append(str(i))
                except (ValueError, AttributeError, TypeError):
                    ext_ids.append(str(i))

            can_resolve = bool(
                ext_ids and ext_id_field is not None and ext_id_sidecar is not None
            )
            if not geoids and not can_resolve:
                # Nothing matchable: no UUID geoids and no sidecar to resolve the
                # external ids through. Short-circuit before any DB work.
                return []

            from dynastore.models.protocols.access_filter import AccessFilter

            async with managed_transaction(self.engine) as conn:
                if ext_ids and ext_id_field is not None and ext_id_sidecar is not None:
                    geoids.extend(
                        await self._resolve_external_ids_to_geoids(
                            conn, phys_schema, phys_table,
                            ext_id_sidecar, ext_id_field, ext_ids,
                        )
                    )

                if not geoids:
                    # External ids resolved to nothing (unknown / fully expired).
                    return []

                request = QueryRequest(
                    # item_ids is intentionally None: the id-matching WHERE is
                    # injected via raw_where/raw_params below so the optimizer
                    # never applies its own predicate.
                    item_ids=None,
                    select=[],
                    raw_selects=[
                        "h.geoid",
                        f"ST_XMin(sc_geometries.{bbox_col}) AS _xmin",
                        f"ST_YMin(sc_geometries.{bbox_col}) AS _ymin",
                        f"ST_XMax(sc_geometries.{bbox_col}) AS _xmax",
                        f"ST_YMax(sc_geometries.{bbox_col}) AS _ymax",
                    ],
                    raw_where="h.geoid = ANY(CAST(:_tile_prior_ids AS uuid[]))",
                    raw_params={"_tile_prior_ids": geoids},
                    limit=len(geoids),
                    access_filter=AccessFilter.allow_everything(),
                )
                query_ctx: Dict[str, Any] = {
                    "catalog_id": catalog_id,
                    "collection_id": collection_id,
                    "col_config": col_config,
                }
                sql, params = await self._apply_query_transformations(
                    request, query_ctx, catalog_id, collection_id, col_config,
                )
                rows = await DQLQuery(
                    sql, result_handler=ResultHandler.ALL_DICTS,
                ).execute(conn, **params)

            bboxes: List[Tuple[float, float, float, float]] = []
            for row in (rows or []):
                try:
                    w = row.get("_xmin")
                    s = row.get("_ymin")
                    e = row.get("_xmax")
                    n = row.get("_ymax")
                    if None in (w, s, e, n):
                        continue
                    bboxes.append((float(w), float(s), float(e), float(n)))
                except (TypeError, ValueError):
                    continue
            return bboxes
        except Exception as exc:  # noqa: BLE001 — never break the caller's write
            logger.debug(
                "tile_cache: bulk prior-bbox read failed for %s/%s: %s",
                catalog_id, collection_id, exc,
            )
            return []

    async def _capture_prior_bbox_for_delete(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        ctx: Optional[DriverContext],
    ) -> Optional[Tuple[float, float, float, float]]:
        """Read an item's current extent before a soft-delete (#1297).

        Phase 2 tile-cache invalidation needs the tiles a feature *used to*
        occupy. This reads that extent off the materialized bbox envelope via
        a single targeted query (``_fetch_prior_bboxes_bulk`` with one id —
        never raw geometry) BEFORE the delete removes the row. Gated on
        ``is_tile_cache_active`` so non-tile deployments pay nothing, and
        degrade-safe (returns ``None`` on any error) — capturing the prior
        bbox must never block or fail a delete.
        """
        try:
            from dynastore.modules.tiles.tile_cache_sync import is_tile_cache_active

            if not await is_tile_cache_active(catalog_id, collection_id):
                return None

            bboxes = await self._fetch_prior_bboxes_bulk(
                catalog_id, collection_id, [item_id],
            )
            return bboxes[0] if bboxes else None
        except Exception as exc:  # noqa: BLE001 — never break the delete
            logger.debug(
                "tile_cache: prior-bbox capture skipped for %s/%s/%s: %s",
                catalog_id, collection_id, item_id, exc,
            )
            return None

    async def _capture_prior_bboxes_for_update(
        self,
        catalog_id: str,
        collection_id: str,
        items: List[Any],
    ) -> List[Tuple[float, float, float, float]]:
        """Read existing items' extents before an upsert (#1297 Phase 2b, #1845).

        Phase 2b tile-cache invalidation for the geometry-MOVE case: when a
        feature's geometry moves, Phase 1 already invalidates the NEW bbox but
        the OLD tiles stay stale. This reads the current bbox for ALL incoming
        items in a SINGLE bulk query (``WHERE feature_id = ANY(:ids)`` — never
        raw geometry) BEFORE the upsert overwrites the rows, replacing the
        previous per-item ``get_item`` loop (N+1 reads on bulk ingestion).

        Gated on ``is_tile_cache_active`` so non-tile deployments pay nothing.
        Degrade-safe: a single bulk read failure yields ``[]`` and must never
        block or fail the upsert. Items that don't yet exist (CREATE, not
        UPDATE) are simply absent from the query result and contribute nothing.
        """
        try:
            from dynastore.modules.tiles.tile_cache_sync import is_tile_cache_active

            if not await is_tile_cache_active(catalog_id, collection_id):
                return []
        except Exception:  # noqa: BLE001 — never break the upsert
            return []

        item_ids = [
            str(item.get("id") if isinstance(item, dict) else getattr(item, "id", None))
            for item in items
            if (item.get("id") if isinstance(item, dict) else getattr(item, "id", None)) is not None
        ]
        if not item_ids:
            return []

        try:
            return await self._fetch_prior_bboxes_bulk(catalog_id, collection_id, item_ids)
        except Exception as exc:  # noqa: BLE001 — never break the upsert
            logger.debug(
                "tile_cache: bulk prior-bbox capture failed for %s/%s: %s",
                catalog_id, collection_id, exc,
            )
            return []

    async def delete_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        ctx: Optional[DriverContext] = None,
        caller_id: Optional[str] = None,
    ) -> int:
        from dynastore.modules.catalog.tools import recalculate_and_update_extents

        db_resource = ctx.db_resource if ctx else None
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        # Phase 2 tile-cache (#1297): capture the item's current extent BEFORE
        # the soft-delete so the invalidate task can drop the tiles it used to
        # occupy. Gated + degrade-safe — never blocks the delete.
        prior_bbox = await self._capture_prior_bbox_for_delete(
            catalog_id, collection_id, item_id, ctx,
        )

        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            phys_table = await self._resolve_physical_table(
                catalog_id, collection_id, db_resource=conn
            )
            if not phys_schema or not phys_table:
                return 0

            from dynastore.modules.storage.router import get_driver as _get_driver
            from dynastore.modules.storage.routing_config import Operation
            _driver = await _get_driver(Operation.READ, catalog_id, collection_id)
            col_config = await _driver.get_driver_config(
                catalog_id, collection_id, db_resource=conn,
            )

            # ID Resolution Logic: delete ALL active rows for this external_id
            rows = 0
            # geoid(s) actually soft-deleted — these are the ES document
            # ``_id``s the upsert path indexed under, so index-delete
            # propagation must key on them (not the external/path id).
            deleted_geoids: List[str] = []
            # #2249: external id used to key the ITEM_DELETION event so the
            # item->asset forward cascade (which matches asset_references by
            # ref_id = external id, stamped at harvest) fires regardless of the
            # delete path. On the sidecar path ``item_id`` is already the
            # external id; on the geoid fallback below it is resolved explicitly.
            resolved_external_id: Optional[str] = None
            if col_config and driver_sidecars(col_config):
                from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry

                for sc in driver_sidecars(col_config):
                    if sc.feature_id_field_name:
                        sidecar = SidecarRegistry.get_sidecar(sc)
                        if sidecar is None:
                            continue
                        # Validate identifiers before SQL interpolation (#2314)
                        fid_col = validate_column_identifier(sc.feature_id_field_name)
                        sc_id = validate_column_identifier(str(sidecar.sidecar_id))
                        sc_table = f"{phys_table}_{sc_id}"
                        # Soft-delete ALL hub rows linked to this external_id via the sidecar.
                        # DQLQuery handles both async/sync conns uniformly.
                        deleted_geoids = [
                            str(g) for g in await DQLQuery(
                                f'UPDATE "{phys_schema}"."{phys_table}" h '
                                f"SET deleted_at = NOW() "
                                f'FROM "{phys_schema}"."{sc_table}" s '
                                f"WHERE s.{fid_col} = :ext_id "
                                f"AND h.deleted_at IS NULL "
                                f"AND h.geoid = s.geoid "
                                f"RETURNING h.geoid",
                                result_handler=ResultHandler.ALL_SCALARS,
                            ).execute(conn, ext_id=str(item_id))
                        ]
                        rows = len(deleted_geoids)
                        break

            if not rows:
                # Fallback: try direct geoid match (item_id may already be the geoid).
                # geoid is UUID-typed; if the path id isn't a UUID it cannot match
                # — return 0 instead of asking asyncpg to bind a non-UUID string
                # (which raises DataError before the query reaches PG). See #942:
                # this fires when the row's id surface == external_id but the
                # sidecar lookup above missed (no sidecar, or feature_id_field_name
                # unset for that sidecar).
                import uuid as _uuid

                try:
                    _uuid.UUID(str(item_id))
                except (ValueError, AttributeError, TypeError):
                    return 0

                from dynastore.modules.catalog.item_service import soft_delete_item_query

                # #2249: resolve the external id while the row is still active so
                # the ITEM_DELETION event below is keyed consistently with the
                # sidecar path (the forward cascade matches on the external id).
                # Returns None when the collection has no external-id sidecar
                # mapping, in which case we keep the geoid (existing behaviour).
                resolved_external_id = await self.resolve_external_id_by_geoid(
                    catalog_id, collection_id, str(item_id), ctx,
                )

                rows = await soft_delete_item_query.execute(
                    conn,
                    catalog_id=phys_schema,
                    collection_id=phys_table,
                    geoid=item_id,
                )
                # Here ``item_id`` is itself the geoid (UUID-validated above),
                # so a non-zero rowcount means that geoid was soft-deleted.
                if rows:
                    deleted_geoids = [str(item_id)]

            if rows > 0:
                await recalculate_and_update_extents(conn, catalog_id, collection_id)

                # Propagate the delete to async-OUTBOX index drivers
                # (e.g. public ES) symmetrically with ``upsert_bulk``: one
                # delete row per soft-deleted geoid, enqueued on THIS TX
                # conn and keyed by the geoid — the same ES ``_id`` the
                # upsert path indexed under. A failed enqueue rolls back
                # the soft-delete, so PG and the index can't drift apart.
                await self._enqueue_index_deletes(
                    conn, catalog_id, collection_id, deleted_geoids,
                )

                # Emit event for non-indexer subscribers (audit, telemetry).
                try:
                    from dynastore.models.protocols.event_bus import EventBusProtocol
                    from dynastore.modules.catalog.event_service import CatalogEventType
                    events_protocol = get_protocol(EventBusProtocol)
                    if events_protocol:
                        await events_protocol.emit(
                            event_type=CatalogEventType.ITEM_DELETION,
                            catalog_id=catalog_id,
                            collection_id=collection_id,
                            item_id=resolved_external_id or item_id,
                            payload={"original_id": item_id}
                        )
                except Exception as e:
                    logger.warning(f"Failed to emit item deletion event: {e}")

                # Phase 2 tile-cache invalidation (#1297): drop the tiles the
                # deleted feature used to occupy. The invalidate task row is
                # independent of this delete (its own connection), so enqueuing
                # it here is safe; gated + degrade-safe inside the enqueue, so it
                # never blocks the delete.
                if prior_bbox is not None:
                    try:
                        from dynastore.modules.tiles.tile_cache_sync import (
                            enqueue_tile_invalidation_task,
                        )

                        await enqueue_tile_invalidation_task(
                            catalog_id, collection_id, [],
                            engine=db_resource or self.engine,
                            schema=phys_schema,
                            prior_bboxes=[prior_bbox],
                            caller_id=caller_id,
                        )
                    except Exception as exc:  # noqa: BLE001 — never break delete
                        logger.warning(
                            "tile_cache: delete invalidation failed for "
                            "%s/%s: %s", catalog_id, collection_id, exc,
                        )

        return rows

    async def _enqueue_index_deletes(
        self,
        conn: Any,
        catalog_id: str,
        collection_id: str,
        geoids: List[str],
    ) -> None:
        """Enqueue one ES-delete OUTBOX row per soft-deleted geoid.

        Symmetric counterpart to ``ItemService.upsert_bulk``'s async-OUTBOX
        enqueue: resolve the secondary-index ``WRITE`` entries
        (``secondary_index=True``) in ``ItemsRoutingConfig.operations[WRITE]``
        that are ``write_mode=ASYNC`` + ``on_failure=OUTBOX`` and write one
        ``OutboxRecord(op="delete")`` per (entry, geoid) onto the caller's
        transaction, keyed by the geoid. The drain's delete branch keys the
        ES ``_id`` on ``idempotency_key`` (the geoid), the same value the
        upsert path indexed under, so the document is actually purged.

        Honours the ``_test_routing_resolver`` seam (as ``upsert_bulk`` does)
        so this can be unit-tested without a live ConfigsProtocol; the enqueue
        itself goes through the module-level ``enqueue_storage_op`` (#1807 P4),
        which tests patch directly.
        """
        if not geoids:
            return

        from dynastore.models.protocols.indexing import OutboxRecord
        from dynastore.modules.storage.driver_instance_id import (
            compute_driver_instance_id,
        )
        from dynastore.tools.identifiers import generate_uuidv7

        routing_resolver = getattr(self, "_test_routing_resolver", None)
        if routing_resolver is not None:
            routing = await routing_resolver(catalog_id, collection_id)
        else:
            from dynastore.modules.storage.routing_config import (
                ItemsRoutingConfig,
            )
            configs = get_protocol(ConfigsProtocol)
            if configs is None:
                return
            routing = await configs.get_config(
                ItemsRoutingConfig,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )

        ops_map = getattr(routing, "operations", {}) or {}
        from dynastore.modules.storage.routing_config import (
            secondary_index_entries,
        )
        async_outbox_entries = secondary_index_entries(
            ops_map, async_outbox_only=True,
        )
        if not async_outbox_entries:
            return

        records: List[OutboxRecord] = []
        for entry in async_outbox_entries:
            inst = compute_driver_instance_id(
                entry.driver_ref, catalog_id, collection_id,
            )
            for geoid in geoids:
                gid = str(geoid)
                records.append(OutboxRecord(
                    op_id=generate_uuidv7(),
                    driver_id=entry.driver_ref,
                    driver_instance_id=inst,
                    collection_id=collection_id,
                    op="delete",
                    item_id=gid,
                    # Delete actions carry no source document; the drain's
                    # delete branch ignores ``payload`` and keys ES on
                    # ``idempotency_key``.
                    payload={},
                    idempotency_key=gid,
                ))
        if records:
            # Enqueue storage rows and drain trigger co-transactionally on the
            # caller's conn (delete twin of ItemService.upsert_bulk).
            from dynastore.modules.storage.storage_emit import (
                enqueue_storage_op,
            )
            await enqueue_storage_op(
                conn,
                catalog_id=catalog_id,
                rows=records,
            )

    async def stream_items(
        self,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        config: Optional[ConfigsProtocol] = None,
        ctx: Optional[DriverContext] = None,
        consumer: ConsumerType = ConsumerType.GENERIC,
        hints: "FrozenSet[Hint]" = frozenset(),
    ) -> QueryResponse:
        """
        Stream search results using an async iterator (O(1) Memory).

        Dispatches to the configured storage driver first.  Falls back to the
        PostgreSQL path when no routing config exists or when ``postgresql``
        is the configured driver.

        ``hints`` are forwarded to driver resolution. Geometry exports (the DWH
        join, feature export) pass ``{Hint.JOIN}`` to force the full-precision
        PG path rather than the public ES-first read default — ES only carries
        simplified geometry and cannot run the ``ST_Transform`` projection these
        exports build into the request.
        """
        # --- Non-PG driver dispatch (streaming, O(1) memory) ---
        operation = _pick_operation(request)
        limit = request.limit if request and request.limit else 100
        offset = request.offset if request and request.offset else 0
        driver_response = await _try_driver_dispatch(
            catalog_id, collection_id, operation, request, limit, offset,
            hints=hints,
        )
        if driver_response is not None:
            return driver_response

        # --- PG path (unchanged) ---
        db_resource = ctx.db_resource if ctx else None
        # Metadata Resolution
        async with managed_transaction(db_resource or self.engine) as conn:
            col_config = await self._get_collection_config(catalog_id, collection_id, config, db_resource=conn)
            phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
            phys_table = await self._resolve_physical_table(catalog_id, collection_id, db_resource=conn)

            if not phys_schema:
                raise ValueError(f"Collection '{catalog_id}/{collection_id}' not found.")

            if not phys_table:
                # Pending collection: metadata exists but storage has not been
                # provisioned yet (awaiting first-item lazy activation or an
                # explicit `POST /activate`).  OGC API Features Req. 26
                # permits an empty FeatureCollection response on `/items`;
                # STAC and Records inherit the same semantics.
                async def _empty_stream():
                    if False:
                        yield  # pragma: no cover — empty async generator
                return QueryResponse(
                    items=_apply_item_pipeline(
                        _empty_stream(), catalog_id, collection_id,
                    ),
                    total_count=0 if request.include_total_count else None,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    collection_config=col_config,
                )

            # Apply transformations and generate SQL
            context = {
                "catalog_id": catalog_id,
                "collection_id": collection_id,
                "col_config": col_config,
                **(request.raw_params or {}),
            }

            # Read-policy projection pushdown (config-driven). Resolve the
            # collection's ItemsReadPolicy ONCE here — reused for row assembly
            # below — and narrow a wildcard SELECT to exactly what
            # ``feature_type`` exposes. Without this a wildcard /items read
            # fetches every sidecar column (geometry stats area/perimeter/length,
            # spatial-cell indexes, …) the policy does not surface, joining and
            # computing columns no consumer asked for and leaking them onto the
            # Feature root. The tile path already narrows via
            # ``project_select_for_feature_type``; ``pushdown_read_select`` brings
            # the row-projected path under the same SSOT (and returns None — no
            # change — for STAC, explicit caller projections, expose_all, or
            # schemaless collections; the row-map guard remains the backstop).
            read_policy = await self._resolve_read_policy(catalog_id, collection_id)
            try:
                from dynastore.modules.storage.driver_config import ItemsSchema
                from dynastore.modules.storage.drivers.pg_sidecars import (
                    SidecarRegistry,
                )
                from dynastore.modules.storage.read_policy import (
                    pushdown_read_select,
                )

                _feature_type = getattr(read_policy, "feature_type", None)
                _declared_schema = None
                if _feature_type is not None:
                    _configs = get_protocol(ConfigsProtocol)
                    if _configs is not None:
                        _schema = await _configs.get_config(
                            ItemsSchema,
                            catalog_id=catalog_id,
                            collection_id=collection_id,
                        )
                        _declared_schema = getattr(_schema, "fields", None) or {}
                        # The optimizer needs the declared schema to resolve
                        # JSONB-only field names (no columnar storage) in its
                        # field index — same thread the tile path uses.
                        context["schema_fields"] = _declared_schema
                _geom_field = next(
                    (
                        name
                        for name in (
                            sc.get_main_geometry_field()
                            for sc in (
                                SidecarRegistry.get_sidecar(cfg, lenient=True)
                                for cfg in driver_sidecars(col_config)
                            )
                            if sc is not None
                        )
                        if name
                    ),
                    None,
                )
                _narrowed = pushdown_read_select(
                    request.select,
                    _feature_type,
                    _declared_schema,
                    is_stac=(consumer == ConsumerType.STAC),
                    geometry_field=_geom_field,
                    skip_geometry=bool(getattr(request, "skip_geometry", False)),
                )
                if _narrowed is not None:
                    request.select = _narrowed
            except Exception as exc:  # noqa: BLE001 — pushdown is an optimisation; never break the read
                logger.debug(
                    "read-select pushdown skipped for %s/%s: %s",
                    catalog_id, collection_id, exc,
                )

            # Snapshot a pagination-free copy BEFORE the data transformation
            # runs: _apply_query_transformations mutates the request in place
            # (it folds the parsed CQL filter into raw_where/raw_params), so a
            # copy taken afterwards would re-parse the filter and double the
            # predicate. deep=True isolates the shared raw_params dict. Taken
            # AFTER the projection pushdown so the count query shares the
            # narrowed select.
            count_request = (
                request.model_copy(deep=True, update={"limit": None, "offset": None})
                if request.include_total_count
                else None
            )
            sql, params = await self._apply_query_transformations(
                request, context, catalog_id, collection_id, col_config,
                db_resource=conn, consumer=consumer,
            )

            total_count = None
            if count_request is not None:
                # numberMatched is computed via _resolve_number_matched which
                # chooses between a fast planner estimate (pg_class.reltuples,
                # O(1)) and an exact SELECT count(*) based on ItemsCountConfig.
                # The page query no longer carries COUNT(*) OVER() so it returns
                # in time proportional to the page size, not the collection size.
                total_count = await self._resolve_number_matched(
                    conn=conn,
                    phys_schema=phys_schema,
                    phys_table=phys_table,
                    count_request=count_request,
                    context=context,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    col_config=col_config,
                    consumer=consumer,
                )

        # Stream Generator (O(1) Memory)
        lang = (request.raw_params or {}).get("lang", "en")
        # ``read_policy`` was resolved once above (projection pushdown) and is
        # reused here for row assembly — no second config fetch.
        # Collect explicitly-requested field names (both alias and source column)
        # so the sidecar bridge can surface SYSTEM fields named here. Wildcard
        # selects ("*") are excluded — only opt-in by name exposes system columns.
        # See #1827.
        _sel = request.select or []
        _requested_fields: set = {
            name
            for s in _sel
            if s.field and s.field != "*"
            for name in (s.field, s.alias) if name
        }
        async def feature_stream():
            # Require async engine: I/O-bound worker SCOPEs (export_features,
            # dwh_join) must include module_db so DatastoreModule.engine resolves
            # to the asyncpg AsyncEngine, enabling server-side streaming.
            if self.engine is None or not is_async_resource(self.engine):
                raise RuntimeError(
                    "feature_stream requires async engine; configure `module_db` "
                    "in SCOPE — see #1420"
                )
            # Open a fresh connection/transaction for streaming to ensure isolation and avoid leaks
            async with managed_transaction(self.engine) as stream_conn:
                # AsyncConnection.stream() yields rows server-side without buffering
                stream = await stream_conn.stream(text(sql), params)  # type: ignore[union-attr]
                async for row in stream:
                    feature_ctx = FeaturePipelineContext(
                        lang=lang, consumer=consumer,
                        requested_fields=_requested_fields,
                    )
                    yield self.map_row_to_feature(
                        dict(row._mapping), col_config, context=feature_ctx,
                        read_policy=read_policy,
                    )

        return QueryResponse(
            items=_apply_item_pipeline(feature_stream(), catalog_id, collection_id),
            total_count=total_count,
            catalog_id=catalog_id,
            collection_id=collection_id,
            collection_config=col_config,
        )

    @asynccontextmanager
    async def _prepare_search(
        self,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        config: Optional[ConfigsProtocol] = None,
        db_resource: Optional[DbResource] = None,
        consumer: ConsumerType = ConsumerType.GENERIC,
    ):
        """Standardized preparation for search results."""
        async with managed_transaction(db_resource or self.engine) as conn:
            col_config = await self._get_collection_config(catalog_id, collection_id, config, db_resource=conn)

            context = {
                "catalog_id": catalog_id,
                "collection_id": collection_id,
                "col_config": col_config,
                **(request.raw_params or {}),
            }
            sql, params = await self._apply_query_transformations(
                request, context, catalog_id, collection_id, col_config,
                db_resource=conn, consumer=consumer,
            )

            params.update({
                "col_config": col_config,
                "catalog_id": catalog_id,
                "collection_id": collection_id,
            })

            query = GeoDQLQuery(text(sql), result_handler=ResultHandler.ALL)
            yield query, conn, params

    async def search_items(
        self,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        config: Optional[ConfigsProtocol] = None,
        ctx: Optional[DriverContext] = None,
        consumer: ConsumerType = ConsumerType.GENERIC,
    ) -> List[Feature]:
        """
        Search and retrieve items using optimized query generation.

        Dispatches to the SEARCH-capable storage driver first.  Falls back
        to the PostgreSQL path when no routing config exists or when
        ``postgresql`` is the configured driver.

        Note: returns a list for backwards compatibility.  Callers that need
        streaming should use ``stream_items()`` instead.
        """
        from dynastore.modules.storage.routing_config import Operation

        # --- Non-PG driver dispatch ---
        limit = request.limit if request and request.limit else 100
        offset = request.offset if request and request.offset else 0
        driver_response = await _try_driver_dispatch(
            catalog_id, collection_id, Operation.SEARCH, request, limit, offset,
        )
        if driver_response is not None:
            # Collect the async stream into a list (search_items contract returns List).
            return [feature async for feature in driver_response.items]

        # --- PG path ---
        db_resource = ctx.db_resource if ctx else None

        # Pending collection: metadata exists but storage has not been
        # provisioned yet.  Return an empty result set rather than
        # letting the SQL path fail on a missing table (same semantics
        # as stream_items — OGC Features Part 1 Req. 26).
        phys_table = await self._resolve_physical_table(
            catalog_id, collection_id, db_resource=db_resource
        )
        if not phys_table:
            return []

        async with self._prepare_search(
            catalog_id, collection_id, request, config, db_resource, consumer=consumer,
        ) as (query, conn, params):
            rows = await query.execute(conn, **params)
            col_config = params.get("col_config")
            read_policy = await self._resolve_read_policy(catalog_id, collection_id)
            return [
                self.map_row_to_feature(row, col_config, read_policy=read_policy)
                for row in rows
            ]
