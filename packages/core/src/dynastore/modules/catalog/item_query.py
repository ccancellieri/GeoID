"""
Query and search mixin for ItemService.

Extracted from item_service.py to reduce file size.  All methods access
``self.*`` helpers defined on the main ``ItemService`` class, which
inherits from this mixin.
"""

import logging
from contextlib import asynccontextmanager
from typing import List, Optional, Any, Dict, Tuple, AsyncIterator

from sqlalchemy import text

from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    GeoDQLQuery,
    DbResource,
    ResultHandler,
    managed_transaction,
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
from dynastore.tools.db import validate_sql_identifier
from dynastore.models.query_builder import QueryRequest, QueryResponse
from dynastore.modules.catalog.query_optimizer import QueryOptimizer
from dynastore.modules.catalog.query_orchestrator import CatalogQueryOrchestrator

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

    Filtered requests (bbox, attribute filters, fulltext) → SEARCH operation.
    Browsing/pagination without filters → READ operation.
    """
    from dynastore.modules.storage.routing_config import Operation

    if request and (
        getattr(request, "filters", None)
        or getattr(request, "cql_filter", None)
    ):
        return Operation.SEARCH
    return Operation.READ


async def _try_driver_dispatch(
    catalog_id: str,
    collection_id: str,
    operation: str,
    request: Optional[QueryRequest],
    limit: int,
    offset: int,
    entity_ids: Optional[List[str]] = None,
) -> Optional[QueryResponse]:
    """Try to resolve and dispatch to a non-PG storage driver.

    Returns a ``QueryResponse`` backed by the driver's streaming
    ``AsyncIterator[Feature]``, or ``None`` when the PG path should be used.

    ``None`` is returned when:
    - No routing config exists for this collection (legacy PG-only setup).
    - The resolved driver is ``postgresql`` (PG path is the correct path).
    - Driver resolution raises any exception.
    """
    try:
        from dynastore.modules.storage.router import get_driver
    except ImportError:
        return None

    try:
        resolved = await get_driver(operation, catalog_id, collection_id)
    except Exception:
        return None

    from dynastore.models.protocols.storage_driver import Capability
    if resolved is None or Capability.QUERY_FALLBACK_SOURCE in resolved.capabilities:
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
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Exposes the underlying query generation for features (used by Tiles/MVT).
        Now uses pluggable query transformations.
        """
        # 1. Build base QueryRequest from params
        query_request = self._build_base_query_request(params, col_config)

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

        # Mandatory ID
        selects = [FieldSelection(field="geoid", alias="id")]

        # Geometry (if requested or by default)
        if params.get("with_geometry", True) and params.get("geom_format") != "MVT":
            selects.append(FieldSelection(field="geom"))

        # Attributes (JSONB)
        selects.append(FieldSelection(field="attributes"))

        query_req = QueryRequest(
            select=selects,
            limit=params.get("limit"),
            offset=params.get("offset"),
            raw_where=params.get("where"),
            raw_params=params.get("raw_params", {}),
        )

        # CQL Filter
        if params.get("cql_filter"):
            from dynastore.modules.tools.cql import parse_cql_filter

            cql_where_str, cql_params = parse_cql_filter(
                params["cql_filter"], field_mapping=None, parser_type="ecql"
            )
            if cql_where_str:
                if query_req.raw_where:
                    query_req.raw_where = (
                        f"({query_req.raw_where}) AND ({cql_where_str})"
                    )
                else:
                    query_req.raw_where = cql_where_str
                query_req.raw_params.update(cql_params)

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

            # Use a temporary optimizer to get all available fields for validation
            temp_optimizer = QueryOptimizer(col_config, consumer=consumer)
            queryable_fields = temp_optimizer.get_all_queryable_fields()

            # Create mapping for CQL parser
            # We map field names to their SQL expressions
            field_mapping = {
                name: text(field_def.sql_expression)
                for name, field_def in queryable_fields.items()
            }
            # Add implicit fields
            field_mapping.update(
                {
                    "geoid": text("h.geoid"),
                    "deleted_at": text("h.deleted_at"),
                    "transaction_time": text("h.transaction_time"),
                }
            )
            # #974: ``validity`` only exists on the hub when it is a
            # partition key; otherwise it lives on a sidecar (or nowhere at
            # all when ``ItemsWritePolicy.enable_validity`` is False).
            # Defer to the optimizer so CQL filters bind to the correct
            # column reference instead of a non-existent ``h.validity``.
            validity_expr = temp_optimizer.resolve_validity_expression()
            if validity_expr is not None:
                field_mapping["validity"] = text(validity_expr)
            else:
                # Bind ``validity`` to ``NULL::tstzrange`` so CQL filters
                # referencing it parse cleanly and evaluate to NULL (the
                # standard Postgres semantics for a missing temporal axis).
                field_mapping["validity"] = text("NULL::tstzrange")

            try:
                cql_where, cql_params = parse_cql_filter(
                    query_request.cql_filter,
                    field_mapping=field_mapping,
                    parser_type="cql2",  # OGC API Features uses CQL2 by default
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
                raise ValueError(f"Invalid CQL filter: {e}")

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

        optimizer = QueryOptimizer(col_config, consumer=consumer)
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
            query_ctx: Dict[str, Any] = {
                "catalog_id": catalog_id,
                "collection_id": collection_id,
                "col_config": col_config,
            }
            sql, params = await self._apply_query_transformations(
                request, query_ctx, catalog_id, collection_id, col_config,
                db_resource=conn,
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

    async def delete_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        ctx: Optional[DriverContext] = None,
    ) -> int:
        from dynastore.modules.catalog.tools import recalculate_and_update_extents

        db_resource = ctx.db_resource if ctx else None
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
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
            if col_config and driver_sidecars(col_config):
                from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry

                for sc in driver_sidecars(col_config):
                    if sc.feature_id_field_name:
                        sidecar = SidecarRegistry.get_sidecar(sc)
                        if sidecar is None:
                            continue
                        sc_table = f"{phys_table}_{sidecar.sidecar_id}"
                        # Soft-delete ALL hub rows linked to this external_id via the sidecar.
                        # DQLQuery handles both async/sync conns uniformly.
                        rows = await DQLQuery(
                            f'UPDATE "{phys_schema}"."{phys_table}" h '
                            f"SET deleted_at = NOW() "
                            f'FROM "{phys_schema}"."{sc_table}" s '
                            f"WHERE s.{sc.feature_id_field_name} = :ext_id "
                            f"AND h.deleted_at IS NULL "
                            f"AND h.geoid = s.geoid",
                            result_handler=ResultHandler.ROWCOUNT,
                        ).execute(conn, ext_id=str(item_id))
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

                rows = await soft_delete_item_query.execute(
                    conn,
                    catalog_id=phys_schema,
                    collection_id=phys_table,
                    geoid=item_id,
                )

            if rows > 0:
                await recalculate_and_update_extents(conn, catalog_id, collection_id)

                # Dispatcher fan-out for delete propagation — single call
                # site replacing the per-driver ``_on_item_delete`` event
                # listeners.
                try:
                    from dynastore.models.protocols.indexer import (
                        IndexContext, IndexOp,
                    )
                    from dynastore.modules.storage.index_dispatcher import (
                        get_index_dispatcher,
                    )
                    from dynastore.tools.correlation import get_correlation_id

                    dispatcher = get_index_dispatcher()
                    # Phase 2f atomic OUTBOX: pass the live PG conn from
                    # the delete TX so any OUTBOX enqueue lands in the
                    # same TX as the soft-delete UPDATE — atomic with
                    # the data change.
                    await dispatcher.fan_out_bulk(
                        IndexContext(
                            catalog=catalog_id,
                            collection=collection_id,
                            correlation_id=get_correlation_id() or "",
                            pg_conn=conn,
                            entity_type="item",
                        ),
                        [IndexOp(
                            op_type="delete",
                            entity_type="item",
                            entity_id=item_id,
                            payload=None,
                        )],
                    )
                except Exception as e:
                    logger.warning(
                        "Index dispatcher delete fan-out failed for %s/%s/%s: %s",
                        catalog_id, collection_id, item_id, e,
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
                            item_id=item_id,
                            payload={"original_id": item_id}
                        )
                except Exception as e:
                    logger.warning(f"Failed to emit item deletion event: {e}")

        return rows

    async def stream_items(
        self,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        config: Optional[ConfigsProtocol] = None,
        ctx: Optional[DriverContext] = None,
        consumer: ConsumerType = ConsumerType.GENERIC,
    ) -> QueryResponse:
        """
        Stream search results using an async iterator (O(1) Memory).

        Dispatches to the configured storage driver first.  Falls back to the
        PostgreSQL path when no routing config exists or when ``postgresql``
        is the configured driver.
        """
        # --- Non-PG driver dispatch (streaming, O(1) memory) ---
        operation = _pick_operation(request)
        limit = request.limit if request and request.limit else 100
        offset = request.offset if request and request.offset else 0
        driver_response = await _try_driver_dispatch(
            catalog_id, collection_id, operation, request, limit, offset,
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
            sql, params = await self._apply_query_transformations(
                request, context, catalog_id, collection_id, col_config,
                db_resource=conn, consumer=consumer,
            )

            total_count = None
            if request.include_total_count:
                count_wrapper = f"SELECT count(*) FROM ({sql}) AS sub"
                total_count = await DQLQuery(
                    count_wrapper, result_handler=ResultHandler.SCALAR
                ).execute(conn, **(params or {}))

        # Stream Generator (O(1) Memory)
        lang = (request.raw_params or {}).get("lang", "en")
        read_policy = await self._resolve_read_policy(catalog_id, collection_id)
        async def feature_stream():
            # Open a fresh connection/transaction for streaming to ensure isolation and avoid leaks
            async with managed_transaction(self.engine) as stream_conn:
                # Use a buffer for higher throughput but still O(1) memory
                stream = await stream_conn.stream(text(sql), params)  # type: ignore[union-attr]
                async for row in stream:
                    feature_ctx = FeaturePipelineContext(lang=lang, consumer=consumer)
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

    async def stream_items_from_query(
        self,
        catalog_id: str,
        collection_id: str,
        where_clause: str,
        query_params: Optional[Dict[str, Any]] = None,
        select_columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        db_resource: Optional[Any] = None,
    ) -> AsyncIterator[Feature]:
        """
        Stream search results from a raw SQL WHERE clause, leveraging the QueryOrchestrator.

        This method builds a query with sidecar JOINs. The user provides the WHERE clause content.

        **NOTE**: The `where_clause` must use the correct table aliases:
        - `h` for the main items (hub) table.
        - `g` for the geometry sidecar.
        - `a` for the attributes sidecar.

        Example: `where_clause="a.external_id = :ext_id AND ST_Intersects(g.geom, ST_MakeEnvelope(...))"`

        WARNING: The caller is responsible for ensuring that if `select_columns` is provided,
        it includes all columns whose sidecars are referenced in the `where_clause`.
        If `select_columns` is None, all sidecars are joined, which is safer but may be less performant.

        Args:
            catalog_id: The catalog ID.
            collection_id: The collection ID.
            where_clause: The content of the SQL WHERE clause.
            query_params: A dictionary of parameters to bind to the query.
            select_columns: Optional list of columns to select. If None, selects all available fields.
            limit: Optional limit for the query.
            offset: Optional offset for the query.
            db_resource: Optional database resource.

        Yields:
            An async iterator of result dictionaries.
        """
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        async with managed_transaction(db_resource or self.engine) as conn:
            from dynastore.modules.storage.router import get_driver as _get_driver
            from dynastore.modules.storage.routing_config import Operation
            _driver = await _get_driver(Operation.READ, catalog_id, collection_id)
            col_config = await _driver.get_driver_config(catalog_id, collection_id)
            phys_schema = await self._resolve_physical_schema(catalog_id)
            phys_table = await self._resolve_physical_table(catalog_id, collection_id)

            if not phys_schema or not phys_table:
                raise ValueError(
                    f"Collection '{catalog_id}/{collection_id}' not found."
                )

            orchestrator = CatalogQueryOrchestrator(col_config)

            columns_to_select = select_columns
            if not columns_to_select:
                from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry

                all_fields = {"geoid", "deleted_at"}
                if driver_sidecars(col_config):
                    for sc_config in driver_sidecars(col_config):
                        sidecar = SidecarRegistry.get_sidecar(sc_config)
                        if sidecar is not None:
                            all_fields.update(sidecar.get_field_definitions().keys())
                columns_to_select = list(all_fields)

            where_clauses = [where_clause] if where_clause else []

            query_string = orchestrator.build_select_query(
                schema=phys_schema,
                table=phys_table,
                columns=columns_to_select,
                where_clauses=where_clauses,
                limit=limit,
            )

            bind_params = dict(query_params or {})
            if limit is not None:
                bind_params["_qo_limit"] = limit
            if offset is not None:
                query_string += " OFFSET :_qo_offset"
                bind_params["_qo_offset"] = offset

            read_policy = await self._resolve_read_policy(catalog_id, collection_id)
            query = GeoDQLQuery(text(query_string), result_handler=ResultHandler.ALL)
            async for item in await query.stream(conn, **bind_params):
                yield self.map_row_to_feature(
                    item, col_config, read_policy=read_policy
                )
