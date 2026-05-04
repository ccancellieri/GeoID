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

import asyncio
import logging
import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING, List, Optional, Any, Dict, Union, Tuple, cast, AsyncIterator

if TYPE_CHECKING:
    from dynastore.modules.storage.router import ResolvedDriver
    from dynastore.models.protocols.storage_driver import CollectionItemsStore
from sqlalchemy import text

import inspect as _inspect
from dynastore.models.driver_context import DriverContext
from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    GeoDQLQuery,
    DbResource,
    ResultHandler,
    managed_transaction,
    is_async_resource,
)
from dynastore.modules.catalog.models import ItemDataForDB, Collection, Catalog
from dynastore.modules.catalog.catalog_config import CollectionPluginConfig
from dynastore.modules.storage.driver_config import (
    ItemsPostgresqlDriverConfig,
)
from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol
from dynastore.models.protocols.items import ItemsProtocol
from dynastore.modules.storage.drivers.pg_sidecars.base import SidecarProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.tools.db import validate_sql_identifier
from dynastore.tools.json import CustomJSONEncoder
from dynastore.modules.db_config import shared_queries
from dynastore.models.query_builder import QueryRequest, QueryResponse
from dynastore.modules.catalog.query_optimizer import QueryOptimizer
from dynastore.modules.storage.drivers.pg_sidecars.base import FeaturePipelineContext
# M1b.2: SidecarRegistry is now imported inline, next to each effective-
# sidecars resolution site.  ItemsPostgresqlDriverConfig remains
# imported at module level for type annotations on the bulk-write path
# (see `col_config: ItemsPostgresqlDriverConfig`) — that's a known
# carryover for a future cleanup pass; within M1b, the boundary guard
# targets CollectionService / CatalogService / AssetService only.
from dynastore.modules.catalog.item_query import ItemQueryMixin
from dynastore.modules.catalog.item_distributed import ItemDistributedMixin

logger = logging.getLogger(__name__)


async def _run_query(conn, stmt, params=None):
    """Run a statement on either sync or async connection."""
    result = conn.execute(stmt, params or {})
    if _inspect.isawaitable(result):
        result = await result
    return result


# --- Specialized Queries for ItemService ---



soft_delete_item_query = DQLQuery(
    "UPDATE {catalog_id}.{collection_id} SET deleted_at = NOW() WHERE geoid = :geoid AND deleted_at IS NULL;",
    result_handler=ResultHandler.ROWCOUNT,
)


class ItemService(ItemQueryMixin, ItemDistributedMixin, ItemsProtocol):
    """Service for item-level operations.

    Explicitly declares conformance to ``ItemsProtocol`` so that static
    analysis tools (mypy) can verify method signatures stay in sync with
    the protocol definition.
    """

    priority: int = 10

    def __init__(self, engine: Optional[DbResource] = None):
        self.engine = engine

    def is_available(self) -> bool:
        return self.engine is not None

    async def _resolve_physical_schema(
        self, catalog_id: str, db_resource: Optional[DbResource] = None
    ) -> Optional[str]:
        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            return None
        return await catalogs.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=db_resource)
        )

    async def _enforce_strict_unknown_fields(
        self,
        catalog_id: str,
        collection_id: str,
        items_list: List[Any],
    ) -> None:
        """Reject batches with properties not declared in CollectionSchema.fields.

        Runs at the unified service-layer entry point (item_service.upsert) so
        every write path — OGC routes via _ingest_items, direct CatalogsProtocol
        callers, sidecar fan-outs — passes through it once. Driver-agnostic:
        no JSON-property-storing backend (PG JSONB, ES) can reject unknown
        keys natively. ``UnknownFieldsError`` is mapped to HTTP 422 by the
        global ``UnknownFieldsExceptionHandler``.
        """
        from dynastore.modules.storage.driver_config import CollectionSchema
        from dynastore.modules.storage.field_constraints import (
            check_strict_unknown_fields,
        )

        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            return
        try:
            ft = await configs.get_config(
                CollectionSchema,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
        except Exception:
            return
        if not isinstance(ft, CollectionSchema):
            return
        if not ft.strict_unknown_fields or not ft.fields:
            return

        feature_dicts = [
            it if isinstance(it, dict) else (
                it.model_dump(by_alias=True, exclude_none=False)
                if hasattr(it, "model_dump") else dict(it)
            )
            for it in items_list
        ]
        check_strict_unknown_fields(ft.fields.keys(), feature_dicts)

    async def _resolve_physical_table(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> Optional[str]:
        from dynastore.modules.storage.router import get_driver, get_write_drivers
        from dynastore.modules.storage.routing_config import Operation

        # Try READ driver first; fall back to the primary WRITE driver.
        # When READ is a non-PG driver (e.g. DuckDB), it has no
        # resolve_physical_table — the PG WRITE driver must supply it.
        for _op in (Operation.READ, None):
            try:
                if _op is None:
                    write_drivers = await get_write_drivers(catalog_id, collection_id)
                    driver = write_drivers[0].driver if write_drivers else None
                else:
                    driver = await get_driver(_op, catalog_id, collection_id)
            except Exception:
                continue
            if driver and hasattr(driver, "resolve_physical_table"):
                result = await getattr(driver, "resolve_physical_table")(
                    catalog_id, collection_id, db_resource=db_resource
                )
                if result:
                    return result
        return None

    async def _get_collection_config(
        self,
        catalog_id: str,
        collection_id: str,
        config_provider: Optional[ConfigsProtocol] = None,
        db_resource: Optional[DbResource] = None,
    ):
        """Fetch driver config (sidecars, partitioning, collection_type).

        Uses the READ driver first; falls back to the primary WRITE driver
        when READ is a non-PG driver (e.g. DuckDB) so that the PG path
        receives a ItemsPostgresqlDriverConfig with valid sidecars.
        """
        from dynastore.modules.storage.router import get_driver, get_write_drivers
        from dynastore.modules.storage.routing_config import Operation

        # Try READ driver; if it returns a non-PG config, fall back to WRITE.
        config = None
        for _op in (Operation.READ, None):
            try:
                if _op is None:
                    write_drivers = await get_write_drivers(catalog_id, collection_id)
                    driver = write_drivers[0].driver if write_drivers else None
                else:
                    driver = await get_driver(_op, catalog_id, collection_id)
            except Exception:
                continue
            if driver is None:
                continue
            config = await driver.get_driver_config(
                catalog_id, collection_id, db_resource=db_resource
            )
            # If the driver config has physical_table support (PG), use it.
            if hasattr(config, "physical_table"):
                return config
            # Non-PG config (e.g. DuckDB) — try next driver.
        return config  # Return whatever we got last (may be None)

    def map_row_to_feature(
        self,
        row: Dict[str, Any],
        col_config: Any,
        lang: str = "en",
        context: Optional[FeaturePipelineContext] = None,
    ) -> Feature:
        """
        Canonical row-to-Feature mapper. Runs each configured sidecar's
        ``map_row_to_feature`` in declaration order, producing a fully
        populated GeoJSON/STAC Feature.

        Pipeline (ordered by sidecar config):
          1. **Hub** (this initialiser): ``feature.id`` defaults to ``geoid``.
          2. **Geometry sidecar**: sets ``geometry``, ``bbox``, optional stats
             properties.
          3. **Attributes sidecar**: optionally overrides ``id`` with
             ``external_id``, populates schema-driven ``properties``.
          4. **STAC sidecar** (optional): transforms the GeoJSON Feature into
             a STAC Item — adds ``links``, converts timestamps, attaches the
             asset reference from context.

        Each sidecar receives the *same* ``FeaturePipelineContext`` so sidecars
        can share data (e.g. attributes → STAC via ``asset_id``) without
        direct coupling. Internal-field filtering is delegated to sidecars
        via ``get_internal_columns()``.
        """

        if not row:
            return Feature(type="Feature", geometry=None, properties={})

        _mapping = getattr(row, "_mapping", None)
        row_dict = dict(_mapping) if _mapping is not None else dict(row)

        # Hub contribution: initialise the feature with geoid as the default id.
        # Sidecars (e.g. Attributes) may override this later in the pipeline.
        geoid = row_dict.get("geoid")
        feature = Feature(
            type="Feature",
            geometry=None,
            properties={},
            id=str(geoid) if geoid is not None else None,
        )

        # Shared context propagated through the entire sidecar pipeline.
        if context is None:
            context = FeaturePipelineContext(lang=lang)

        # M1b.2: resolve effective sidecars (core defaults + registry
        # injections) so default-body collections — whose `col_config`
        # now has an empty `sidecars` list (plan §Principle — default-
        # fast invariant) — still get their row-to-feature pipeline
        # materialised.  Explicit caller-supplied sidecars are preserved;
        # the registry layers in any missing types for the collection
        # type.  The helper is cheap enough to call per-row, but most
        # callers iterate many rows with a shared col_config so the
        # resolved list is stable across the loop.
        from dynastore.modules.storage.drivers.pg_sidecars import (
            SidecarRegistry,
            _effective_sidecars,
        )
        sidecar_configs = _effective_sidecars(
            col_config, catalog_id="", collection_id="",
        )

        if sidecar_configs:
            # Gather all internal columns to prevent property leaking across sidecars
            all_internal = set()
            for sc_config in sidecar_configs:
                sidecar = SidecarRegistry.get_sidecar(sc_config, lenient=True)
                if sidecar:
                    all_internal.update(sidecar.get_internal_columns())
            context._all_internal_cols = all_internal

            for sc_config in sidecar_configs:
                sidecar = SidecarRegistry.get_sidecar(sc_config, lenient=True)
                if sidecar:
                    sidecar.map_row_to_feature(row_dict, feature, context=context)

            # Bridge context to feature model_extra for extension generators (e.g. STAC)
            if context:
                # model_extra is already initialized by Pydantic 'extra="allow"'
                sidecar_data = context._sidecar_store
                all_internal = context.all_internal_columns
                # Never overwrite defined model fields (id, geometry, etc.)
                # via __pydantic_extra__ — that would clobber values already
                # set by the sidecar pipeline.
                model_fields = set(Feature.model_fields)
                for sid, data in sidecar_data.items():
                    if isinstance(data, dict):
                        # Merge dicts if it's a standard sidecar publication,
                        # but skip raw internal columns (e.g. item_title,
                        # external_extensions) that are only meant for
                        # inter-sidecar communication.
                        for k, v in data.items():
                            if k not in all_internal and k not in model_fields:
                                if feature.__pydantic_extra__ is not None:
                                    feature.__pydantic_extra__[k] = v
                    else:
                        if sid not in model_fields:
                            if feature.__pydantic_extra__ is not None:
                                feature.__pydantic_extra__[sid] = data
        else:
            logger.warning(
                "No sidecars configured for col_config; returning minimal "
                "feature with geoid-based id."
            )

        return feature

    async def upsert(
        self,
        catalog_id: str,
        collection_id: str,
        items: Union[Dict[str, Any], List[Dict[str, Any]], Any],
        ctx: Optional[DriverContext] = None,
        processing_context: Optional[Dict[str, Any]] = None,
    ) -> Union[Dict[str, Any], List[Dict[str, Any]], Any]:
        """
        Create or update items (single or bulk).

        Args:
            catalog_id: Catalog identifier
            collection_id: Collection identifier
            items: Feature, FeatureCollection, STACItem, or raw dict/list
            ctx: Optional driver context carrying db_resource and processing hints

        Returns:
            Created/Updated item(s) (single or list)
        """
        db_resource = ctx.db_resource if ctx else None
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        # Determine if single or bulk to return consistent type
        is_single = False
        items_list = []

        # Handle input types
        if isinstance(items, list):
            items_list = items
        elif isinstance(items, dict) and items.get("type") == "FeatureCollection":
            items_list = items.get("features", [])
        elif hasattr(items, "features") and getattr(items, "type", None) == "FeatureCollection":
            # Handle FeatureCollection Pydantic model
            items_list = getattr(items, "features")
        else:
            # Handle single item passed as list or other iterable
            # Single item (Feature, STACItem, dict)
            is_single = True
            items_list = [items]

        if not items_list:
            raise ValueError("No features provided. A FeatureCollection must contain at least one feature.")

        # CollectionSchema.strict_unknown_fields enforcement.
        # Service-layer because no JSON-property-storing driver (PG JSONB,
        # ES) can natively reject unknown keys, and Branch B (PG primary)
        # below bypasses driver.write_entities entirely — so this is the
        # single point all writers cross.
        await self._enforce_strict_unknown_fields(catalog_id, collection_id, items_list)

        # ── Branch A: non-PG primary write driver ─────────────────────────
        # When the primary WRITE driver is not postgresql, delegate the entire
        # write to that driver.  PG-specific logic (sidecars, hub table, sidecar
        # payloads, QueryOptimizer) is skipped — the driver owns its own write path.
        # Post-commit fan-out and event emission still run after this branch.
        #
        # Trust the waterfall: ItemsRoutingConfig.operations[WRITE] has a
        # code-level default of [ItemsPostgresqlDriver], so this list is
        # never empty in a correctly bootstrapped deploy. If it is empty, the
        # resolver surfaces ConfigResolutionError → HTTP 500 ops alert.
        from dynastore.modules.storage.router import get_write_drivers
        resolved_drivers = await get_write_drivers(catalog_id, collection_id)
        primary = resolved_drivers[0]

        from dynastore.models.protocols.storage_driver import Capability
        if primary is not None and Capability.QUERY_FALLBACK_SOURCE not in primary.driver.capabilities:
            chunk_size = getattr(primary.driver, "preferred_chunk_size", 0)
            if chunk_size > 0 and len(items_list) > chunk_size:
                results = []
                for i in range(0, len(items_list), chunk_size):
                    chunk = items_list[i : i + chunk_size]
                    chunk_results = await primary.driver.write_entities(
                        catalog_id,
                        collection_id,
                        chunk,
                        context=processing_context,
                    )
                    results.extend(chunk_results or [])
            else:
                results = await primary.driver.write_entities(
                    catalog_id,
                    collection_id,
                    items_list,
                    context=processing_context,
                )
            results = results or []

            # Fan-out to secondary drivers (positions 1+)
            if results:
                await self._fan_out_to_secondary_drivers(
                    catalog_id, collection_id, results, _primary_already_written=True,
                )

            # Single dispatcher call site for index propagation —
            # replaces the per-driver ``_on_item_upsert`` event listeners.
            if results:
                await self._dispatch_index_upsert(catalog_id, collection_id, results)

            # Emit events for non-indexer subscribers (audit, telemetry).
            # Index propagation no longer rides this channel — it goes
            # through the dispatcher above so failures are attributable
            # per-indexer with proper retry semantics.
            if results:
                try:
                    from dynastore.models.protocols.event_bus import EventBusProtocol
                    from dynastore.modules.catalog.event_service import CatalogEventType
                    events_protocol = get_protocol(EventBusProtocol)
                    if events_protocol:
                        if is_single:
                            await events_protocol.emit(
                                event_type=CatalogEventType.ITEM_CREATION,
                                catalog_id=catalog_id,
                                collection_id=collection_id,
                                item_id=str(results[0].id) if results[0].id else None,
                                payload=results[0].model_dump(by_alias=True, exclude_unset=True),
                            )
                        else:
                            await events_protocol.emit(
                                event_type=CatalogEventType.BULK_ITEM_CREATION,
                                catalog_id=catalog_id,
                                collection_id=collection_id,
                                payload={
                                    "count": len(results),
                                    "items_subset": [
                                        r.model_dump(by_alias=True, exclude_none=True)
                                        for r in results[:10]
                                    ],
                                },
                            )
                except Exception as e:
                    logger.warning("Failed to emit item creation events: %s", e)

            return results[0] if is_single else results

        # ── Branch B: PostgreSQL primary ─────────────────────────────────
        # Three-phase ingestion. Each phase opens the narrowest possible
        # connection so no single thread holds a write transaction across
        # the whole batch.
        #
        #   1. Resolve config + sidecars + physical table (read-only, brief)
        #   2. Prepare every item in memory (no DB) and dedupe partition keys
        #   3. DDL: pre-create the unique partitions (separate brief tx)
        #   4. Write: chunked write transactions; each commits its hub +
        #      sidecar inserts before yielding the conn back to the pool.
        #      No per-item read-back inside the write tx — that used to
        #      accumulate AccessShare locks on every iteration.
        #   5. Read-back: one bulk SELECT WHERE geoid = ANY(:geoids) on a
        #      separate read conn, after writes have committed.
        engine = db_resource or self.engine
        from dynastore.tools.identifiers import generate_geoid

        # Phase 1 — config + sidecars + physical table
        async with managed_transaction(engine) as conn:
            _configs = get_protocol(ConfigsProtocol)
            assert _configs is not None, "ConfigsProtocol not registered"
            collection_config = await _configs.get_config(
                CollectionPluginConfig, catalog_id, collection_id,
                ctx=DriverContext(db_resource=conn),
            )
            max_bulk = collection_config.max_bulk_features
            if len(items_list) > max_bulk:
                raise ValueError(
                    f"FeatureCollection contains {len(items_list)} features, "
                    f"exceeding the maximum of {max_bulk}. "
                    f"Split into smaller batches."
                )

            assert primary is not None, "primary driver required for PostgreSQL write path"

            # Lazy activation: a pending collection is activated on its first
            # insert. `activate_collection` is idempotent (no-op if already
            # active) and reuses the current transaction so the routing pin
            # commits atomically with the first item write.
            catalogs_for_activation = get_protocol(CatalogsProtocol)
            assert catalogs_for_activation is not None, "CatalogsProtocol not registered"
            if not await catalogs_for_activation.is_active(
                catalog_id, collection_id, db_resource=conn,
            ):
                await catalogs_for_activation.activate_collection(
                    catalog_id, collection_id,
                    ctx=DriverContext(db_resource=conn),
                )

            col_config = await primary.driver.get_driver_config(
                catalog_id, collection_id, db_resource=conn,
            )

            phys_table = await self._resolve_physical_table(
                catalog_id, collection_id, db_resource=conn,
            )
            if not phys_table:
                # Fallback: activation did not pin a physical_table (e.g. no
                # storage driver registered in test environments).  Run the
                # legacy PG-specific promotion path so the table gets created.
                await self.ensure_physical_table_exists(
                    catalog_id, collection_id, col_config, db_resource=conn,
                )
                phys_table = await self._resolve_physical_table(
                    catalog_id, collection_id, db_resource=conn,
                )

            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn,
            )

            # M1b.2: resolve effective sidecars (core defaults for the
            # collection_type + registry injections) so default-body
            # collections still activate sidecars at first write time.
            # Phase 1.6: collection_type lives on its own PluginConfig.
            from dynastore.modules.catalog.catalog_config import CollectionType
            from dynastore.modules.storage.drivers.pg_sidecars import (
                SidecarRegistry,
                _effective_sidecars,
            )
            configs = get_protocol(ConfigsProtocol)
            ct = await configs.get_config(
                CollectionType, catalog_id=catalog_id, collection_id=collection_id,
            ) if configs else CollectionType()
            sidecar_configs = _effective_sidecars(
                col_config,
                catalog_id=catalog_id,
                collection_id=collection_id,
                collection_type=ct.kind.value,
            )

            sidecars: List[Any] = []
            if sidecar_configs:
                for sc_config in sidecar_configs:
                    sidecars.append(SidecarRegistry.get_sidecar(sc_config))

            # Run item_metadata before attributes so its in-place prune of
            # feature.properties takes effect before attributes captures them.
            _PRUNE_FIRST = {"item_metadata"}
            sidecars_ordered = sorted(
                sidecars, key=lambda s: (0 if s.sidecar_id in _PRUNE_FIRST else 1),
            )

        # Phase 2 — prepare every item in memory; dedupe partition keys
        prepared: List[Dict[str, Any]] = []
        unique_partition_values: set = set()
        for item_data in items_list:
            if hasattr(item_data, "model_dump"):
                raw_item = item_data.model_dump(by_alias=True, exclude_unset=True)  # type: ignore[reportAttributeAccessIssue]
            elif isinstance(item_data, dict):
                raw_item = item_data
            else:
                raise ValueError(f"Unsupported item type: {type(item_data)}")

            geoid = generate_geoid()
            item_context: Dict[str, Any] = {
                "geoid": geoid,
                "operation": "insert",
                "_raw_item": raw_item,
                **(processing_context or {}),
            }
            hub_payload: Dict[str, Any] = {
                "geoid": geoid,
                "transaction_time": datetime.now(timezone.utc),
                "deleted_at": None,
            }
            sidecar_payloads: Dict[str, Dict[str, Any]] = {}
            partition_values: Dict[str, Any] = {}

            for sidecar in sidecars_ordered:
                val_result = sidecar.validate_insert(raw_item, item_context)
                if not val_result.valid:
                    raise ValueError(
                        f"Sidecar {sidecar.sidecar_id} rejected item: {val_result.error}"
                    )
                sc_payload = sidecar.prepare_upsert_payload(raw_item, item_context)
                if sc_payload:
                    sidecar_payloads[sidecar.sidecar_id] = sc_payload
                    for pk in sidecar.get_partition_keys():
                        if pk in sc_payload:
                            partition_values[pk] = sc_payload[pk]
                            item_context[pk] = sc_payload[pk]

            hub_payload.update(partition_values)
            if col_config.partitioning.enabled and partition_values:
                # Single-level partitioning today — first value is the partition key.
                unique_partition_values.add(next(iter(partition_values.values())))

            # geometry_hash is now PG-generated on the geometries sidecar
            # (issue #220) — STORED column ``encode(digest(ST_AsBinary(geom),
            # 'sha256'), 'hex')``.  No longer carried on the hub row; the
            # matcher JOINs the sidecar to read it.

            prepared.append({
                "geoid": geoid,
                "hub_payload": hub_payload,
                "sidecar_payloads": sidecar_payloads,
                "item_context": item_context,
            })

        # Phase 3 — pre-create unique partitions on a brief DDL conn so the
        # write tx isn't holding a write lock during DDL coordination.
        if col_config.partitioning.enabled and unique_partition_values:
            async with managed_transaction(engine) as ddl_conn:
                for p_val in unique_partition_values:
                    await self.ensure_partition_exists(
                        catalog_id, collection_id, col_config, p_val,
                        ctx=DriverContext(db_resource=ddl_conn),
                    )

        # Phase 4 — chunked writes. Each chunk commits its own tx; row locks
        # are released between chunks instead of accumulating across the whole
        # ingestion. Per-collection knob via `CollectionPluginConfig.ingest_chunk_size`
        # (default 50 — safe for geometry-heavy payloads; lightweight
        # attribute-only collections can raise it).
        chunk_size = collection_config.ingest_chunk_size
        write_results: List[Dict[str, Any]] = []
        # Per-row rejection collector. SidecarRejectedError is raised by the
        # distributed upsert's acceptance check BEFORE any DB writes for the
        # offending item, so catching it inside the chunk loop does not poison
        # the enclosing transaction and sibling items in the same chunk still
        # commit. Rejections are surfaced to the caller via
        # ``ctx.extensions["_rejections"]`` so the HTTP layer can build a 207
        # ``IngestionReport`` instead of reporting a 500 on a single bad row.
        from dynastore.modules.storage.errors import SidecarRejectedError
        rejections: List[Dict[str, Any]] = []
        for start in range(0, len(prepared), chunk_size):
            chunk = prepared[start:start + chunk_size]
            async with managed_transaction(engine) as conn:
                for plan in chunk:
                    try:
                        new_row = await self.insert_or_update_distributed(
                            conn,
                            catalog_id,
                            collection_id,
                            plan["hub_payload"],
                            plan["sidecar_payloads"],
                            col_config=col_config,
                            sidecars=sidecars,
                            processing_context=plan["item_context"],
                        )
                    except SidecarRejectedError as rej:
                        rejections.append({
                            "geoid": str(rej.geoid) if rej.geoid else plan["geoid"],
                            "external_id": rej.external_id,
                            "sidecar_id": rej.sidecar_id,
                            "matcher": rej.matcher,
                            "reason": rej.reason,
                            "message": str(rej),
                        })
                        continue
                    if new_row is None:
                        logger.error(
                            f"FATAL: insert_or_update_distributed returned None for geoid: {plan['geoid']}"
                        )
                        raise RuntimeError(
                            f"Failed to upsert item. Geoid: {plan['geoid']}"
                        )
                    write_results.append(new_row)

        # Hand rejections to the caller via the typed DriverContext escape
        # hatch. The OGC mixin seeds an empty list before the call and drains
        # this key after so rejections and accepted rows can be combined into
        # a single 207 IngestionReport.
        if ctx is not None and rejections:
            ctx.extensions["_rejections"] = rejections

        # Phase 5 — bulk read-back on a fresh conn, post-commit. One SELECT
        # for the whole batch (vs N inside the write tx). Preserves input
        # order so callers can correlate results 1:1 with the request.
        result_geoids = [r["geoid"] for r in write_results]
        if not phys_schema or not phys_table:
            raise RuntimeError("Physical schema/table unavailable for bulk read-back")
        async with managed_transaction(engine) as read_conn:
            results = await self.fetch_features_bulk(
                read_conn, phys_schema, phys_table, result_geoids, col_config,
            )

        # ── Post-commit: fan-out to secondary drivers ──────────────────
        if results:
            await self._fan_out_to_secondary_drivers(
                catalog_id, collection_id, results
            )

        # ── Post-commit: dispatcher fan-out for index propagation ──────
        # Single call site replaces the per-driver ``_on_item_upsert``
        # event listeners.
        if results:
            await self._dispatch_index_upsert(catalog_id, collection_id, results)

        # ── Post-commit: emit events for non-indexer subscribers ──────
        if results:
            try:
                from dynastore.models.protocols.event_bus import EventBusProtocol
                from dynastore.modules.catalog.event_service import CatalogEventType
                events_protocol = get_protocol(EventBusProtocol)
                if events_protocol:
                    if is_single:
                        await events_protocol.emit(
                            event_type=CatalogEventType.ITEM_CREATION,
                            catalog_id=catalog_id,
                            collection_id=collection_id,
                            item_id=str(results[0].id) if results[0].id else None,
                            payload=results[0].model_dump(by_alias=True, exclude_unset=True)
                        )
                    else:
                        await events_protocol.emit(
                            event_type=CatalogEventType.BULK_ITEM_CREATION,
                            catalog_id=catalog_id,
                            collection_id=collection_id,
                            payload={"count": len(results), "items_subset": [r.model_dump(by_alias=True, exclude_none=True) for r in results[:10]]}
                        )
            except Exception as e:
                logger.warning(f"Failed to emit item creation events: {e}")

        # Single-item callers expect a bare row or None (when the sole item
        # was rejected by the write policy); bulk callers always get a list,
        # possibly empty if every item was rejected.
        if is_single:
            return results[0] if results else None
        return results

    # ------------------------------------------------------------------
    # Index dispatcher fan-out — replaces ES event-driven listeners
    # ------------------------------------------------------------------

    async def _dispatch_index_upsert(
        self,
        catalog_id: str,
        collection_id: str,
        results: List["Feature"],
    ) -> None:
        """Fan items out to every configured Indexer for the collection.

        Single dispatcher call site for items, replacing the per-driver
        ``_on_item_upsert`` event listeners.  Driver-agnostic: ES public,
        ES private, vector DB, audit log — anything implementing the slim
        :class:`Indexer` Protocol and registered under
        ``operations[INDEX]``.

        Failure surfaces are decided by routing-config ``on_failure`` per
        entry — ``OUTBOX`` enqueues a durable retry row, ``WARN`` logs,
        ``FATAL`` raises out of this method.
        """
        if not results:
            return

        from dynastore.models.protocols.indexer import (
            IndexContext, IndexOp,
        )
        from dynastore.modules.storage.index_dispatcher import (
            IndexerFatal, get_index_dispatcher,
        )
        from dynastore.tools.correlation import get_correlation_id

        dispatcher = get_index_dispatcher()
        ctx = IndexContext(
            catalog=catalog_id,
            collection=collection_id,
            correlation_id=get_correlation_id() or "",
            # No pg_conn: this call site runs post-commit. OUTBOX
            # policy degrades to non-atomic enqueue — Phase 2f will
            # move the call inside the write TX so OUTBOX becomes
            # atomic with the data write.
            pg_conn=None,
        )
        ops = [
            IndexOp(
                op_type="upsert",
                entity_type="item",
                entity_id=str(r.id) if r.id else "",
                payload=r.model_dump(by_alias=True, exclude_none=True),
            )
            for r in results
            if r.id is not None
        ]

        try:
            if len(ops) == 1:
                await dispatcher.fan_out(ctx, ops[0])
            else:
                await dispatcher.fan_out_bulk(ctx, ops)
        except IndexerFatal:
            # FATAL contract: a routing entry with on_failure=FATAL
            # MUST propagate so the caller's TX rolls back.  Don't
            # swallow.
            raise
        except Exception as e:  # noqa: BLE001 — non-FATAL paths absorb errors
            logger.warning(
                "Index dispatcher fan-out failed for %s/%s (%d items): %s",
                catalog_id, collection_id, len(results), e,
            )

    # ------------------------------------------------------------------
    # Multi-driver write fan-out
    # ------------------------------------------------------------------

    async def _fan_out_to_secondary_drivers(
        self,
        catalog_id: str,
        collection_id: str,
        features: List[Feature],
        *,
        _primary_already_written: bool = True,
    ) -> None:
        """Fan-out writes to secondary drivers after the primary commit.

        ``_primary_already_written=True`` (default) skips position 0 (primary)
        since it was already written by the caller (Branch A or B in ``upsert``).

        Sync drivers run in parallel (``asyncio.gather``).  If any sync
        driver fails, drivers that succeeded and declare
        ``DriverCapability.TRANSACTIONAL`` are compensated (delete).

        Async drivers fire after the sync phase succeeds (fire-and-forget).
        """
        from dynastore.modules.storage.driver_config import DriverCapability
        from dynastore.modules.storage.router import get_write_drivers, ResolvedDriver
        from dynastore.modules.storage.routing_config import FailurePolicy, WriteMode

        try:
            resolved = await get_write_drivers(catalog_id, collection_id)
        except Exception:
            return  # no routing configured — nothing to fan out

        # Position 0 is the primary driver — already written by the caller.
        secondaries = resolved[1:] if _primary_already_written else resolved
        if not secondaries:
            return

        sync_drivers = [r for r in secondaries if r.write_mode == WriteMode.SYNC]
        async_drivers = [r for r in secondaries if r.write_mode == WriteMode.ASYNC]

        # ── Sync phase: parallel writes ───────────────────────────────
        if sync_drivers:
            tasks = [
                r.driver.write_entities(catalog_id, collection_id, features)
                for r in sync_drivers
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            succeeded: List[ResolvedDriver] = []
            has_fatal = False
            first_fatal_exc: Optional[Exception] = None

            for r, result in zip(sync_drivers, results):
                if isinstance(result, BaseException):
                    if r.on_failure == FailurePolicy.FATAL:
                        has_fatal = True
                        if first_fatal_exc is None:
                            first_fatal_exc = result if isinstance(result, Exception) else RuntimeError(str(result))
                    elif r.on_failure == FailurePolicy.WARN:
                        logger.warning(
                            "Secondary sync driver '%s' write failed for %s/%s: %s",
                            r.driver_id, catalog_id, collection_id, result,
                        )
                    # IGNORE: silent
                else:
                    succeeded.append(r)

            if has_fatal:
                # Compensate succeeded sync drivers that support rollback
                await self._compensate_drivers(
                    succeeded, catalog_id, collection_id, features
                )
                raise first_fatal_exc  # type: ignore[misc]

        # ── Async phase: fire-and-forget ──────────────────────────────
        for r in async_drivers:
            asyncio.create_task(
                self._async_secondary_write(r, catalog_id, collection_id, features)
            )

    async def _async_secondary_write(
        self,
        resolved: "ResolvedDriver",
        catalog_id: str,
        collection_id: str,
        features: List[Feature],
    ) -> None:
        """Fire-and-forget wrapper with logging on failure."""
        from dynastore.modules.storage.routing_config import FailurePolicy

        try:
            await resolved.driver.write_entities(catalog_id, collection_id, features)
        except Exception as err:
            if resolved.on_failure == FailurePolicy.FATAL:
                logger.error(
                    "Async secondary driver '%s' FATAL write failed for %s/%s: %s",
                    resolved.driver_id, catalog_id, collection_id, err,
                )
            elif resolved.on_failure == FailurePolicy.WARN:
                logger.warning(
                    "Async secondary driver '%s' write failed for %s/%s: %s",
                    resolved.driver_id, catalog_id, collection_id, err,
                )

    async def _compensate_drivers(
        self,
        drivers: List["ResolvedDriver"],
        catalog_id: str,
        collection_id: str,
        features: List[Feature],
    ) -> None:
        """Compensating rollback: delete written entities from drivers that
        support TRANSACTIONAL capability."""
        from dynastore.modules.storage.driver_config import DriverCapability

        entity_ids = [str(f.id) for f in features if f.id]
        if not entity_ids:
            return

        for r in drivers:
            driver_caps = getattr(r.driver, "capabilities", frozenset())
            if DriverCapability.TRANSACTIONAL not in driver_caps:
                continue
            try:
                await r.driver.delete_entities(
                    catalog_id, collection_id, entity_ids
                )
                logger.info(
                    "Compensated driver '%s' for %s/%s (%d entities)",
                    r.driver_id, catalog_id, collection_id, len(entity_ids),
                )
            except Exception as comp_err:
                logger.error(
                    "Compensating rollback failed for driver '%s' on %s/%s: %s",
                    r.driver_id, catalog_id, collection_id, comp_err,
                )

    # Query methods (get_features, get_item, search_items, stream_items, etc.)
    # are provided by ItemQueryMixin.

    # Distributed methods (insert_or_update_distributed, etc.)
    # are provided by ItemDistributedMixin.

    async def delete_item_language(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        lang: str,
        ctx: Optional[DriverContext] = None,
    ) -> int:
        """
        Deletes a specific language variant from an item's attributes.
        Actually, it looks for localized fields in 'attributes' and removes the key.
        """
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

            # Fetch the item to identify localized fields in attributes
            item = await self.get_item(
                catalog_id, collection_id, item_id, ctx=DriverContext(db_resource=conn)
            )
            if not item:
                return 0

            attributes = getattr(item, "attributes", None) or {}
            if isinstance(attributes, str):
                attributes = json.loads(attributes)

            # Identifies fields that are potentially localized (e.g. title, description, or custom)
            # and removes the requested language.
            modified = False
            for key, value in attributes.items():
                if isinstance(value, dict):
                    # Check if it looks like a localized dict
                    from dynastore.models.localization import _LANGUAGE_METADATA

                    if any(k in _LANGUAGE_METADATA for k in value.keys()):
                        if lang in value:
                            if len(value) <= 1:
                                # We might skip deletion if it's the only language,
                                # but usually for items, we can either keep or remove.
                                # Let's follow the 'error if last language' rule if it makes sense.
                                # For STAC items, maybe it's less strict, but let's be consistent.
                                continue

                            del value[lang]
                            modified = True

            if not modified:
                return 0

            update_sql = f'UPDATE "{phys_schema}"."{phys_table}" SET attributes = :attr WHERE geoid = :geoid;'
            rows = await DQLQuery(
                update_sql, result_handler=ResultHandler.ROWCOUNT
            ).execute(
                conn, attr=json.dumps(attributes, cls=CustomJSONEncoder), geoid=item_id
            )
            return rows

    async def ensure_physical_table_exists(
        self,
        catalog_id: str,
        collection_id: str,
        col_config: ItemsPostgresqlDriverConfig,
        db_resource: Optional[DbResource] = None,
    ):
        async with managed_transaction(db_resource or self.engine) as conn:
            from dynastore.modules.db_config.locking_tools import acquire_startup_lock

            async with acquire_startup_lock(
                conn, f"promote_{catalog_id}_{collection_id}"
            ):
                phys_schema = await self._resolve_physical_schema(
                    catalog_id, db_resource=conn
                )
                phys_table = await self._resolve_physical_table(
                    catalog_id, collection_id, db_resource=conn
                )
                force_create = False
                catalogs = get_protocol(CatalogsProtocol)
                if not phys_table:
                    logger.info(
                        f"Promoting collection {catalog_id}:{collection_id} to physical storage."
                    )
                    phys_table = collection_id
                    force_create = True
                    from dynastore.modules.storage.router import get_driver as _get_driver
                    from dynastore.modules.storage.routing_config import Operation as _Op
                    _drv = await _get_driver(_Op.WRITE, catalog_id, collection_id)
                    if hasattr(_drv, "set_physical_table"):
                        await getattr(_drv, "set_physical_table")(
                            catalog_id, collection_id, phys_table, db_resource=conn
                        )

                if catalogs is not None and (force_create or not await shared_queries.table_exists_query.execute(
                    conn, schema=phys_schema, table=phys_table
                )):
                    await getattr(catalogs, "create_physical_collection")(
                        conn,
                        phys_schema,
                        catalog_id,
                        collection_id,
                        physical_table=phys_table,
                        layer_config=col_config.model_dump(),
                    )

    async def ensure_partition_exists(
        self,
        catalog_id: str,
        collection_id: str,
        col_config: ItemsPostgresqlDriverConfig,
        partition_value: Any,
        ctx: Optional[DriverContext] = None,
    ):
        db_resource = ctx.db_resource if ctx else None
        partitioning = col_config.partitioning
        if not partitioning.enabled or not partitioning.partition_keys:
            return
        # Current simplify: we use the first partition key for the physical partition routing
        # In a fully composite world, partition_value should be a list/tuple.
        if partition_value is None:
            return
        if partition_value is None:
            return
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            phys_table = await self._resolve_physical_table(
                catalog_id, collection_id, db_resource=conn
            )
            if not phys_schema or not phys_table:
                return
            # Determine strategy for the tool (simplified)
            # If the value is a date/datetime, use RANGE, else LIST
            from datetime import date, datetime

            tool_strategy = (
                "RANGE" if isinstance(partition_value, (date, datetime)) else "LIST"
            )
            interval = (
                None  # We might need to derive this if we had TimePartitionStrategy
            )

            from dynastore.modules.db_config.partition_tools import (
                ensure_partition_exists as ensure_partition_tool,
            )

            await ensure_partition_tool(
                conn=conn,
                table_name=phys_table,
                strategy=tool_strategy,
                partition_value=partition_value,
                schema=phys_schema,
                interval=interval,
                parent_table_name=phys_table,
                parent_table_schema=phys_schema,
            )

    async def get_collection_fields(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Returns a dictionary of field definitions for the collection.
        Aggregates fields from the physical table introspection and configured sidecars.
        """
        # Resolve physical details internally
        schema = await self._resolve_physical_schema(
            catalog_id, db_resource=db_resource
        )
        table = await self._resolve_physical_table(
            catalog_id, collection_id, db_resource=db_resource
        )

        fields = {}
        from dynastore.modules.storage.drivers.pg_sidecars.base import (
            FieldDefinition,
            FieldCapability,
        )

        # 1. Try to get config-based definitions if logical IDs provided
        if catalog_id and collection_id:
            try:
                col_config = await self._get_collection_config(
                    catalog_id, collection_id, db_resource=db_resource
                )

                if col_config:
                    optimizer = QueryOptimizer(col_config)
                    # A. Aggregate from all sidecars via QueryOptimizer
                    fields.update(optimizer.get_all_queryable_fields())

                    # B. Hub Fields (geoid, transaction_time, etc.)
                    hub_cols = col_config.get_column_definitions()
                    for name in hub_cols.keys():
                        if name not in fields:
                            # Infer simple type from SQL def
                            sql_type = hub_cols[name].upper()
                            d_type = "string"
                            if "TIMESTAMP" in sql_type:
                                d_type = "datetime"
                            elif "UUID" in sql_type:
                                d_type = "uuid"

                            fields[name] = FieldDefinition(
                                name=name,
                                sql_expression=f"h.{name}",
                                data_type=d_type,
                                capabilities=[
                                    FieldCapability.FILTERABLE,
                                    FieldCapability.SORTABLE,
                                    FieldCapability.GROUPABLE,
                                ],
                            )

            except (ValueError, KeyError, AttributeError, LookupError, OSError) as e:
                logger.warning(
                    f"Failed to resolve collection config for fields for {catalog_id}/{collection_id}: {e}"
                )

        # 2. Introspect Physical Table (Fallback & Validation)
        # Use a transaction for introspection
        async with managed_transaction(db_resource or self.engine) as conn:
            from dynastore.modules.db_config.tools import _get_table_columns_query

            rows = await _get_table_columns_query.execute(
                conn, schema=schema, table=table
            )

            for row in rows:
                col_name = row.column_name
                # If already defined by config, skip (config is more authoritative for aliases/sql_expression)
                if col_name in fields:
                    continue

                dtype = str(row.data_type).lower()
                udt = str(row.udt_name).lower()

                # Normalize type name
                if dtype == "user-defined" and udt in ("geometry", "geography"):
                    dtype = "geometry"
                elif dtype in ("character varying", "character", "text"):
                    dtype = "string"
                elif dtype in ("integer", "smallint", "bigint"):
                    dtype = "integer"
                elif dtype in ("double precision", "real", "numeric"):
                    dtype = "float"
                elif dtype.startswith("timestamp"):
                    dtype = "datetime"
                elif dtype == "boolean":
                    dtype = "boolean"
                elif dtype == "jsonb":
                    dtype = "jsonb"
                elif dtype == "uuid":
                    dtype = "uuid"
                elif dtype == "tstzrange":
                    dtype = "tstzrange"

                # Simple object mocking FieldDefinition for introspection-only columns
                # We don't know the alias here, so we assume column name
                fields[col_name] = FieldDefinition(
                    name=col_name,
                    sql_expression=col_name,
                    data_type=dtype,
                    capabilities=[FieldCapability.FILTERABLE],
                )

        return fields

    async def get_collection_schema(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Returns the composed JSON Schema for the collection's Feature output.
        Aggregated from all active sidecars via QueryOptimizer.
        """
        col_config = await self._get_collection_config(
            catalog_id, collection_id, db_resource=db_resource
        )
        if not col_config:
            raise ValueError(f"Collection {catalog_id}/{collection_id} not found.")

        from dynastore.modules.catalog.query_optimizer import QueryOptimizer
        optimizer = QueryOptimizer(col_config)
        return optimizer.get_feature_type_schema()

    # NOTE: map_row_to_feature is defined once at the top of this class.
    # The canonical implementation (using the sidecar pipeline) is the single
    # source of truth — do not add a second definition here.

    @property
    def count_items_by_asset_id_query(self) -> DQLQuery:
        """Query builder for counting items by asset ID."""

        def _builder(conn, params):
            phys_schema = params["catalog_id"]
            phys_table = params["collection_id"]
            asset_id = params["asset_id"]

            sql = f'SELECT count(*) FROM "{phys_schema}"."{phys_table}" WHERE extra_metadata->\'assets\' ? :asset_id AND deleted_at IS NULL'
            return sql, {"asset_id": asset_id}

        return DQLQuery.from_builder(_builder, result_handler=ResultHandler.SCALAR_ONE)

    @property
    def list_items_by_asset_id_query(self) -> DQLQuery:
        """Query builder for listing item IDs that link to a given asset.

        Used by the reverse-cascade path to enumerate dependent items
        before deletion. Returns ``external_id`` rows since that's what
        ``CatalogService.delete_item`` accepts.
        """

        def _builder(conn, params):
            phys_schema = params["catalog_id"]
            phys_table = params["collection_id"]
            asset_id = params["asset_id"]

            sql = (
                f'SELECT external_id FROM "{phys_schema}"."{phys_table}" '
                f"WHERE extra_metadata->'assets' ? :asset_id "
                f"AND deleted_at IS NULL"
            )
            return sql, {"asset_id": asset_id}

        return DQLQuery.from_builder(_builder, result_handler=ResultHandler.ALL_DICTS)

