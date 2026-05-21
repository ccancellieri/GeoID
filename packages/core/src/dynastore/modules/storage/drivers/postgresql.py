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

"""
PostgreSQL Storage Driver — wraps existing ``ItemsProtocol`` services.

Zero SQL rewrite.  All complex PG logic (sidecars, query optimizer,
ON CONFLICT, RETURNING, PostGIS) stays exactly where it is.  This driver
is a thin adapter that maps ``CollectionItemsStore``
to the existing PG-based service layer.
"""

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncIterator, ClassVar, Dict, FrozenSet, List, Optional, Union

if TYPE_CHECKING:
    from dynastore.modules.storage.storage_location import StorageLocation
    from dynastore.modules.storage.driver_config import ItemsWritePolicy

from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.driver_context import DriverContext
from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.protocols.typed_driver import TypedDriver
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.protocols import ModuleProtocol
from dynastore.modules.storage.errors import SoftDeleteNotSupportedError
from dynastore.modules.storage.hints import Hint
from dynastore.modules.storage.routing_config import Operation
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig

logger = logging.getLogger(__name__)


class ItemsPostgresqlDriver(TypedDriver[ItemsPostgresqlDriverConfig], ModuleProtocol):
    """PostgreSQL storage driver — delegates to existing ItemsProtocol.

    Satisfies ``CollectionItemsStore`` by wrapping the existing
    PG-based item services, preserving all sidecar logic, query
    optimization, and streaming.
    """

    # PG is the truth-source for items (WRITE primary in routing
    # defaults) and the ``geometry_exact`` fallback for SEARCH.  WRITE +
    # READ are pinned by explicit defaults in ``ItemsRoutingConfig``;
    # auto-augment opts in only for SEARCH so PG appears in the SEARCH
    # entry list as the precision-fallback backend.
    auto_register_for_routing: ClassVar[FrozenSet[str]] = frozenset({Operation.SEARCH})

    priority: int = 10
    preferred_chunk_size: int = 0
    capabilities: FrozenSet[str] = frozenset({
        Capability.READ,
        Capability.WRITE,
        Capability.STREAMING,
        Capability.SOFT_DELETE,
        Capability.EXPORT,
        Capability.GEOSPATIAL,
        Capability.SPATIAL_INDEX,
        Capability.ASSET_TRACKING,
        Capability.INTROSPECTION,
        Capability.REQUIRED_ENFORCEMENT,
        Capability.UNIQUE_ENFORCEMENT,
        Capability.PHYSICAL_ADDRESSING,
        Capability.SOFT_DELETE_ATOMIC,
        Capability.QUERY_FALLBACK_SOURCE,
        Capability.BULK_COPY,
    })
    preferred_for: FrozenSet[Hint] = frozenset({Hint.FEATURES, Hint.WRITE, Hint.GEOMETRY_EXACT})
    supported_hints: FrozenSet[Hint] = frozenset({
        Hint.FEATURES, Hint.WRITE, Hint.METADATA,
        Hint.JOIN,  # primary side of OGC API - Joins (extensions/joins/joins_service.py)
        Hint.GEOMETRY_EXACT,  # PR #185 default routing: PG returns full-precision geometries
        Hint.TILES,  # ST_AsMVT renderer dispatches here via tiles_module.get_tile_resolution_params
        Hint.SPATIAL_FILTER, Hint.ATTRIBUTE_FILTER, Hint.SORT, Hint.GROUP_BY,
        Hint.AGGREGATION, Hint.COUNT, Hint.STATISTICS,
    })

    def is_available(self) -> bool:
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.items import ItemsProtocol

        return get_protocol(ItemsProtocol) is not None

    async def get_driver_config(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        db_resource: Optional[Any] = None,
    ) -> "ItemsPostgresqlDriverConfig":
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.tools.discovery import get_protocol
        from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig

        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            return ItemsPostgresqlDriverConfig()
        config = await configs.get_config(
            ItemsPostgresqlDriverConfig,
            catalog_id=catalog_id,
            collection_id=collection_id,
            ctx=DriverContext(db_resource=db_resource),
        )
        if not isinstance(config, ItemsPostgresqlDriverConfig):
            return ItemsPostgresqlDriverConfig()
        return config

    async def _get_effective_driver_config(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        db_resource: Optional[Any] = None,
    ) -> "ItemsPostgresqlDriverConfig":
        """Load the driver config and materialise effective sidecars.

        Use this at any call site that iterates ``col_config.sidecars``
        (DDL generation, query composition, introspection, field flags).
        Returns a config whose ``sidecars`` field is the result of
        ``_effective_sidecars(...)`` — core defaults for the collection
        type + registry injections.

        M1b.2: replaces direct ``get_driver_config`` + ``.sidecars``
        iteration, now that ``ItemsPostgresqlDriverConfig.sidecars``
        defaults to empty (plan §Principle — default-fast invariant).
        """
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.modules.catalog.catalog_config import CollectionInfo
        from dynastore.modules.storage.drivers.pg_sidecars import (
            _effective_sidecars,
            resolve_stac_enabled,
        )
        from dynastore.tools.discovery import get_protocol

        config = await self.get_driver_config(
            catalog_id, collection_id, db_resource=db_resource,
        )
        # Phase 1.6: collection_type lives on its own PluginConfig now.
        configs = get_protocol(ConfigsProtocol)
        ct = await configs.get_config(
            CollectionInfo, catalog_id=catalog_id, collection_id=collection_id,
        ) if configs else CollectionInfo()
        stac_enabled = await resolve_stac_enabled(
            catalog_id, collection_id or "",
        )
        effective = _effective_sidecars(
            config,
            catalog_id=catalog_id,
            collection_id=collection_id or "",
            collection_type=ct.kind.value,
            context={"stac_enabled": stac_enabled},
        )
        return config.model_copy(update={"sidecars": effective})

    @staticmethod
    async def _resolve_write_policy(
        catalog_id: str,
        collection_id: Optional[str],
    ) -> "ItemsWritePolicy":
        """Resolve ``ItemsWritePolicy`` from the config waterfall.

        Used by ``ensure_storage`` and ``compute_extents`` to obtain the
        SSOT for ``validity`` (and any other policy-driven
        sidecar shape decisions). Mirrors
        ``IcebergItemsDriver._resolve_write_policy`` — falls back to
        defaults if the configs service or config is missing.
        """
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.modules.storage.driver_config import ItemsWritePolicy
        from dynastore.tools.discovery import get_protocol

        try:
            configs = get_protocol(ConfigsProtocol)
            if configs:
                cfg = await configs.get_config(
                    ItemsWritePolicy,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                )
                if isinstance(cfg, ItemsWritePolicy):
                    return cfg
        except Exception:
            pass
        return ItemsWritePolicy()

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        logger.info("ItemsPostgresqlDriver: started (wraps existing ItemsProtocol)")
        yield
        logger.info("ItemsPostgresqlDriver: stopped")

    async def write_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entities: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]]],
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> List[Feature]:
        items_svc = self._get_crud_protocol()
        result = await items_svc.upsert(
            catalog_id, collection_id, entities, ctx=DriverContext(db_resource=db_resource) if db_resource else None
        )
        if isinstance(result, list):
            return result
        return [result]

    async def read_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        entity_ids: Optional[List[str]] = None,
        request: Optional[QueryRequest] = None,
        context: Optional[Dict[str, Any]] = None,  # noqa: ARG002
        limit: int = 100,
        offset: int = 0,
        db_resource: Optional[Any] = None,
    ) -> AsyncIterator[Feature]:
        if request is None:
            request = QueryRequest(limit=limit, offset=offset, item_ids=entity_ids)
        else:
            if entity_ids is not None and request.item_ids is None:
                request = request.model_copy(update={"item_ids": entity_ids})
            if request.limit is None:
                request = request.model_copy(update={"limit": limit})
            if request.offset is None:
                request = request.model_copy(update={"offset": offset})

        query_svc = self._get_query_protocol()
        response = await query_svc.stream_items(
            catalog_id, collection_id, request, ctx=DriverContext(db_resource=db_resource) if db_resource else None
        )

        async for feature in response.items:
            yield feature

    async def delete_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entity_ids: List[str],
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> int:
        if soft:
            raise SoftDeleteNotSupportedError(
                "ItemsPostgresqlDriver: soft delete for individual entities "
                "is not yet implemented. Use drop_storage(soft=True) for "
                "collection-level soft deletion."
            )
        items_svc = self._get_crud_protocol()
        total = 0
        for eid in entity_ids:
            total += await items_svc.delete_item(
                catalog_id, collection_id, eid, ctx=DriverContext(db_resource=db_resource) if db_resource else None
            )
        return total

    async def restore_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entity_ids: List[str],
        *,
        db_resource: Optional[Any] = None,
    ) -> int:
        raise SoftDeleteNotSupportedError(
            "ItemsPostgresqlDriver: restore_entities not yet implemented."
        )

    async def rename_storage(
        self,
        catalog_id: str,
        old_collection_id: str,
        new_collection_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Rename PG physical table and update driver config."""
        from dynastore.modules.db_config.query_executor import DDLQuery, managed_transaction

        schema = await self._resolve_schema(catalog_id, db_resource=db_resource)
        old_table = await self.resolve_physical_table(
            catalog_id, old_collection_id, db_resource=db_resource
        )
        if not old_table:
            raise ValueError(
                f"No physical table found for {catalog_id}:{old_collection_id}"
            )

        new_table = old_table  # Keep same physical table name, just update config mapping
        await self.set_physical_table(
            catalog_id, new_collection_id, new_table, db_resource=db_resource
        )

    async def _resolve_schema(self, catalog_id: str, db_resource=None) -> str:
        """Resolve the PG schema name for a catalog."""
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.catalogs import CatalogsProtocol

        catalogs = get_protocol(CatalogsProtocol)
        if not catalogs:
            raise RuntimeError("CatalogsProtocol not available")
        schema = await catalogs.resolve_physical_schema(catalog_id, ctx=DriverContext(db_resource=db_resource))
        if not schema:
            raise ValueError(f"No physical schema found for catalog '{catalog_id}'")
        # SQL-boundary guard (#1135): the schema name is interpolated into f-string
        # SQL identifiers throughout this driver — never let an unsafe value through,
        # regardless of how it was stored/resolved.
        from dynastore.tools.db import validate_sql_identifier
        return validate_sql_identifier(schema)

    async def resolve_physical_table(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource=None,
    ) -> Optional[str]:
        """Resolve physical table name from driver config."""
        config = await self.get_driver_config(
            catalog_id, collection_id, db_resource=db_resource
        )
        table = config.physical_table
        if table is None:
            return None
        # SQL-boundary guard (#1135): ``physical_table`` is interpolated into
        # f-string SQL identifiers — validate before returning so a value that
        # bypassed config validation (legacy row, model_copy, extra="allow")
        # can never reach SQL.
        from dynastore.tools.db import validate_sql_identifier
        return validate_sql_identifier(table)

    async def set_physical_table(
        self,
        catalog_id: str,
        collection_id: str,
        physical_table: str,
        *,
        db_resource=None,
    ) -> None:
        """Store physical table name in driver config."""
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig

        config = await self.get_driver_config(
            catalog_id, collection_id, db_resource=db_resource
        )
        updated_config = config.model_copy(update={"physical_table": physical_table})
        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            return
        await configs.set_config(
            ItemsPostgresqlDriverConfig,
            updated_config,
            catalog_id=catalog_id,
            collection_id=collection_id,
            check_immutability=False,
            ctx=DriverContext(db_resource=db_resource),
        )

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Create PG hub table + sidecar tables for a collection.

        Generates a unique ``physical_table`` name, creates the hub table,
        creates sidecar tables, and stores the mapping in the driver config.

        If ``collection_id`` is None, this is a no-op (catalog-level call).

        PG-specific kwargs:
            physical_table: Optional explicit table name. If not provided,
                one is generated automatically.
            layer_config: Optional config overlay merged on top of the
                resolved ``ItemsPostgresqlDriverConfig`` before creating storage.
        """
        if not collection_id:
            return

        db_resource = kwargs.get("db_resource")
        col_config = kwargs.get("col_config")
        physical_table = kwargs.get("physical_table")
        layer_config = kwargs.get("layer_config")
        if db_resource is None:
            raise ValueError("ensure_storage requires db_resource")

        from dynastore.modules.db_config.query_executor import (
            DDLQuery, DQLQuery, ResultHandler, managed_transaction,
        )
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.modules.storage.driver_config import (
            ItemsPostgresqlDriverConfig,
        )
        from dynastore.modules.catalog.catalog_service import generate_physical_name
        from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry
        from dynastore.models.protocols.assets import AssetsProtocol

        schema = await self._resolve_schema(catalog_id, db_resource=db_resource)

        # Resolve col_config if not provided
        if col_config is None:
            col_config = await self.get_driver_config(
                catalog_id, collection_id, db_resource=db_resource,
            )

        # Apply layer_config overlay if provided
        if layer_config:
            base_dump = col_config.model_dump()
            layer_config_dict = (
                layer_config.model_dump()
                if hasattr(layer_config, "model_dump")
                else layer_config
            )

            def deep_update(d, u):
                for k, v in u.items():
                    if isinstance(v, dict):
                        d[k] = deep_update(d.get(k, {}), v)
                    else:
                        d[k] = v
                return d

            merged = deep_update(base_dump, layer_config_dict)
            # ``physical_table`` is machine-assigned; a layer_config overlay must
            # never be able to point this collection at an arbitrary table
            # (#1135).  Pin it to the trusted stored value (or None for a fresh
            # collection) regardless of what the overlay carried.
            merged["physical_table"] = col_config.physical_table
            try:
                col_config = ItemsPostgresqlDriverConfig.model_validate(merged)
            except Exception as e:
                logger.error(
                    "Failed to merge layer_config for %s:%s: %s",
                    catalog_id, collection_id, e,
                )

        # --- Generate physical table name if not provided ---
        # Prefer the name already stored in col_config (idempotent re-runs);
        # only generate a new name if the collection is truly new.
        if not physical_table and col_config and col_config.physical_table:
            physical_table = col_config.physical_table
        if not physical_table:
            physical_table = generate_physical_name("t")

        # --- Resolve effective sidecars (M1b.2) ---
        # The sidecars list on `col_config` may be empty (default-fast path —
        # caller didn't supply any PG-specific layout).  `_effective_sidecars`
        # layers in registry-sourced defaults (geometries + attributes for
        # VECTOR, attributes-only for RECORDS) plus extension-registered
        # sidecars (e.g. item_metadata from STAC).  The result is pinned on
        # col_config so downstream DDL, partition-key aggregation, and
        # introspection all see the same materialised list.
        from dynastore.modules.storage.drivers.pg_sidecars import (
            _effective_sidecars,
            resolve_stac_enabled,
        )
        stac_enabled = await resolve_stac_enabled(catalog_id, collection_id or "")
        effective_sidecars = _effective_sidecars(
            col_config, catalog_id=catalog_id, collection_id=collection_id,
            context={"stac_enabled": stac_enabled},
        )
        col_config = col_config.model_copy(update={"sidecars": effective_sidecars})

        # --- Partition context ---
        partition_keys = []
        partition_key_types = {
            "transaction_time": "TIMESTAMPTZ",
            "validity": "TSTZRANGE",
            "geoid": "UUID",
            "asset_id": "VARCHAR(255)",
        }

        # Bridge ItemsSchema.fields → attributes sidecar so that
        # required=True / unique=True materialize as NOT NULL / UNIQUE in the
        # generated DDL (COLUMNAR mode).
        from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
            FeatureAttributeSidecarConfig,
        )
        try:
            from dynastore.modules.storage.driver_config import (
                ItemsSchema,
            )
            from dynastore.modules.storage.field_constraints import (
                bridge_schema_to_attribute_sidecar,
            )

            configs = get_protocol(ConfigsProtocol)
            schema_cfg = None
            if configs is not None:
                schema_cfg = await configs.get_config(
                    ItemsSchema,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    ctx=DriverContext(db_resource=db_resource),
                )
            if schema_cfg is not None and getattr(schema_cfg, "fields", None):
                new_sidecars = []
                for sc in col_config.sidecars:
                    if isinstance(sc, FeatureAttributeSidecarConfig):
                        new_sidecars.append(
                            bridge_schema_to_attribute_sidecar(schema_cfg, sc)
                        )
                    else:
                        new_sidecars.append(sc)
                col_config = col_config.model_copy(
                    update={"sidecars": new_sidecars}
                )
        except Exception as exc:
            logger.debug(
                "schema → attribute_schema bridge skipped for %s/%s: %s",
                catalog_id, collection_id, exc,
            )

        # ItemsWritePolicy is the SSOT for several sidecar storage-shape
        # decisions. The driver overlays the policy-derived values onto the
        # sidecar config fields at ensure_storage time so the persisted config
        # stays policy-aligned. Every read path that rehydrates the sidecar
        # (query_optimizer, item_query, item_service, …) then sees the correct
        # shape without threading the policy itself.
        #
        # Null-object overlays applied from ItemsWritePolicy (#957, #974, #1043, #1126):
        #   validity_column       ← policy.validity.column (column name or None)
        #   validity_start_from   ← policy.validity.start_from ("context" or path)
        #   validity_end_from     ← policy.validity.end_from   (None/"context"/path)
        #   external_id_field  ← "external_id" when policy has EXTERNAL_ID rule,
        #                        None when absent
        #   asset_id_field     ← "asset_id" when policy.track_asset_id, else None
        #
        # #978: storage-bearing compute entries drive the geometries sidecar.
        write_policy = await self._resolve_write_policy(catalog_id, collection_id)
        from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
            GeometriesSidecarConfig,
        )
        from dynastore.modules.storage.driver_config import ComputedKind
        from dynastore.modules.storage.computed_fields import (
            SidecarTarget,
            target_sidecar,
        )

        policy_external_id_field: Optional[str] = (
            "external_id"
            if write_policy.find_compute(ComputedKind.EXTERNAL_ID) is not None
            else None
        )
        policy_asset_id_field: Optional[str] = (
            "asset_id" if write_policy.track_asset_id else None
        )
        # Storage-bearing compute entries split by target sidecar: geometry/place
        # statistics drive the geometries sidecar; attribute-derived statistics
        # (ATTRIBUTE_STAT) drive the attributes sidecar. #1074
        policy_storage_fields = [
            cf for cf in write_policy.compute if cf.storage_mode is not None
        ]
        geom_storage_fields = [
            cf for cf in policy_storage_fields
            if target_sidecar(cf.kind) == SidecarTarget.GEOMETRY
        ]
        attr_storage_fields = [
            cf for cf in policy_storage_fields
            if target_sidecar(cf.kind) == SidecarTarget.ATTRIBUTES
        ]
        # Phase 3 Decision 2: the persisted geohash/h3/s2 index columns are a
        # pure physical realization of the policy's spatial-cell identity axis.
        # Snapshot the policy's spatial-cell ComputedFields onto the geometries
        # sidecar so its derived geohash_precision / h3_resolutions /
        # s2_resolutions accessors yield the policy-aligned values (no
        # independent sidecar knob can drift from the identity axis).
        from dynastore.modules.storage.computed_fields import SPATIAL_CELL_KINDS
        geom_spatial_cells = [
            cf for cf in write_policy.compute if cf.kind in SPATIAL_CELL_KINDS
        ]
        overlay_sidecars = []
        any_overlay = False
        for sc in col_config.sidecars:
            if isinstance(sc, FeatureAttributeSidecarConfig):
                updates: dict = {}
                # Validity overlay (#1126): column NAME from policy.validity.column;
                # value sources from policy.validity.start_from / end_from.
                policy_validity_column = write_policy.validity_column
                policy_validity_start = (
                    write_policy.validity.start_from
                    if write_policy.validity is not None
                    else "context"
                )
                policy_validity_end = (
                    write_policy.validity.end_from
                    if write_policy.validity is not None
                    else None
                )
                if sc.validity_column != policy_validity_column:
                    updates["validity_column"] = policy_validity_column
                if sc.validity_start_from != policy_validity_start:
                    updates["validity_start_from"] = policy_validity_start
                if sc.validity_end_from != policy_validity_end:
                    updates["validity_end_from"] = policy_validity_end
                if sc.external_id_field != policy_external_id_field:
                    updates["external_id_field"] = policy_external_id_field
                if sc.asset_id_field != policy_asset_id_field:
                    updates["asset_id_field"] = policy_asset_id_field
                if list(sc.compute_fields_overlay) != attr_storage_fields:
                    updates["compute_fields_overlay"] = attr_storage_fields
                if updates:
                    overlay_sidecars.append(sc.model_copy(update=updates))
                    any_overlay = True
                else:
                    overlay_sidecars.append(sc)
            elif isinstance(sc, GeometriesSidecarConfig):
                geom_updates: dict = {}
                if list(sc.compute_fields_overlay) != geom_storage_fields:
                    geom_updates["compute_fields_overlay"] = geom_storage_fields
                if list(sc.spatial_cells_overlay) != geom_spatial_cells:
                    geom_updates["spatial_cells_overlay"] = geom_spatial_cells
                if geom_updates:
                    overlay_sidecars.append(sc.model_copy(update=geom_updates))
                    any_overlay = True
                else:
                    overlay_sidecars.append(sc)
            else:
                overlay_sidecars.append(sc)
        if any_overlay:
            col_config = col_config.model_copy(update={"sidecars": overlay_sidecars})

        if col_config.partitioning.enabled:
            partition_keys = col_config.partitioning.partition_keys
            for sc_config in col_config.sidecars:
                partition_key_types.update(sc_config.partition_key_types)

        # --- Build hub table DDL ---
        hub_cols_map = col_config.get_column_definitions()
        for key in partition_keys:
            if key not in hub_cols_map:
                col_type = partition_key_types.get(key, "TEXT")
                hub_cols_map[key] = f"{col_type} NOT NULL"

        has_validity = "validity" in partition_keys

        hub_columns_ddl = []
        for name, spec in hub_cols_map.items():
            clean_spec = spec.replace(" PRIMARY KEY", "")
            hub_columns_ddl.append(f'"{name}" {clean_spec}')

        pk_hub = ["geoid"]
        if has_validity:
            pk_hub.append("validity")
        pk_all = list(set(pk_hub) | set(partition_keys)) if partition_keys else list(pk_hub)
        quoted_pk = ", ".join(f'"{c}"' for c in pk_all)
        hub_columns_ddl.append(f"PRIMARY KEY ({quoted_pk})")

        partition_clause = ""
        if partition_keys:
            quoted_pk_keys = ", ".join(f'"{k}"' for k in partition_keys)
            partition_clause = f" PARTITION BY LIST ({quoted_pk_keys})"

        create_hub_sql = (
            f'CREATE TABLE IF NOT EXISTS "{schema}"."{physical_table}" '
            f'({", ".join(hub_columns_ddl)}){partition_clause};'
        )

        # Hub + every sidecar DDL must run on a SHARED connection so
        # the FK references emitted by ``sidecar_impl.get_ddl(...)``
        # see the freshly-created hub on the same transaction snapshot.
        # The previous code ran each ``DDLQuery.execute(engine, …)`` on
        # its own ``engine.begin()`` transaction; in prod this surfaced
        # as the hub being silently absent at sidecar-FK time
        # (``relation "<schema>.<hub>" does not exist``).  Architecturally
        # the hub is "the first sidecar" — it MUST commit visibly before
        # any FK-bearing sidecar DDL runs, and the cleanest enforcement is
        # to share one connection across the whole ensure_storage block.
        logger.info(
            "ItemsPostgresqlDriver.ensure_storage: creating hub '%s.%s' "
            "+ %s sidecars",
            schema, physical_table, len(col_config.sidecars),
        )
        async with managed_transaction(db_resource) as conn:
            await DDLQuery(create_hub_sql).execute(conn)

            # --- Create sidecar tables ---
            # ``sidecar_config.validity_column`` is already policy-aligned
            # (overlay above), so the factory can stay policy-agnostic.
            for sidecar_config in col_config.sidecars:
                try:
                    sidecar_impl = SidecarRegistry.get_sidecar(sidecar_config)
                    if sidecar_impl is None:
                        continue
                    sc_has_validity = sidecar_impl.has_validity()
                    ddl_statements = sidecar_impl.get_ddl(
                        physical_table=physical_table,
                        partition_keys=partition_keys,
                        partition_key_types=partition_key_types,
                        has_validity=sc_has_validity,
                    )
                    await DDLQuery(ddl_statements).execute(conn, schema=schema)
                    await sidecar_impl.setup_lifecycle_hooks(
                        conn, schema, f"{physical_table}_{sidecar_impl.sidecar_id}"
                    )
                except ValueError as e:
                    logger.warning("Skipping sidecar table creation: %s", e)

        # --- Store physical_table in driver config ---
        from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig

        configs = get_protocol(ConfigsProtocol)
        updated_config = col_config.model_copy(update={"physical_table": physical_table})
        if configs is not None:
            await configs.set_config(
                ItemsPostgresqlDriverConfig,
                updated_config,
                catalog_id=catalog_id,
                collection_id=collection_id,
                check_immutability=False,
                ctx=DriverContext(db_resource=db_resource),
            )

        # --- Ensure asset cleanup trigger ---
        am = get_protocol(AssetsProtocol)
        if am:
            await am.ensure_asset_cleanup_trigger(schema, physical_table, ctx=DriverContext(db_resource=db_resource) if db_resource is not None else None)

        logger.info(
            "ItemsPostgresqlDriver.ensure_storage: created hub '%s' + sidecars for %s/%s",
            physical_table, catalog_id, collection_id,
        )

    # Collection-metadata CRUD has moved to the domain-scoped drivers +
    # :mod:`dynastore.modules.catalog.collection_router`.  The
    # items driver no longer touches the legacy ``{schema}.collection_metadata``
    # table (dropped by the M2.5 migration) — callers route through the
    # split-domain drivers instead.

    async def drop_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        soft: bool = False,
    ) -> None:
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.catalogs import CatalogsProtocol

        catalogs = get_protocol(CatalogsProtocol)
        if not catalogs:
            raise RuntimeError("CatalogsProtocol not available")

        if soft:
            logger.info(
                "ItemsPostgresqlDriver.drop_storage(soft=True): "
                "catalog=%s collection=%s — marking as deleted via deleted_at",
                catalog_id, collection_id,
            )

        if collection_id:
            await catalogs.delete_collection(catalog_id, collection_id)
        else:
            await catalogs.delete_catalog(catalog_id)

    async def export_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        format: str = "parquet",
        target_path: str = "",
        db_resource: Optional[Any] = None,
    ) -> str:
        raise NotImplementedError(
            "ItemsPostgresqlDriver.export_entities: use ExportFeaturesTask "
            "for async export via the task runner system."
        )

    async def count_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        request: Optional[Any] = None,
        db_resource: Optional[Any] = None,
    ) -> int:
        from dynastore.modules.db_config.query_executor import (
            DQLQuery, ResultHandler, managed_transaction,
        )

        schema = await self._resolve_schema(catalog_id, db_resource=db_resource)
        table = await self.resolve_physical_table(
            catalog_id, collection_id, db_resource=db_resource,
        )
        if not table:
            return 0

        query_sql = f'SELECT COUNT(*) FROM "{schema}"."{table}" WHERE deleted_at IS NULL;'

        async def _query(conn):
            return await DQLQuery(
                query_sql, result_handler=ResultHandler.SCALAR_ONE
            ).execute(conn)

        if db_resource is not None:
            return await _query(db_resource) or 0
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.database import DatabaseProtocol
        db_proto = get_protocol(DatabaseProtocol)
        if not db_proto:
            return 0
        async with managed_transaction(db_proto.engine) as conn:
            return await _query(conn) or 0

    async def introspect_schema(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> List[Any]:
        from dynastore.modules.db_config.query_executor import (
            DQLQuery, ResultHandler, managed_transaction,
        )
        from dynastore.modules.db_config import shared_queries
        from dynastore.models.protocols.field_definition import FieldDefinition as ProtocolFieldDefinition

        schema = await self._resolve_schema(catalog_id, db_resource=db_resource)
        table = await self.resolve_physical_table(
            catalog_id, collection_id, db_resource=db_resource,
        )
        if not table:
            return []

        # Map PG types to driver-agnostic type strings
        pg_type_map = {
            "integer": "integer", "bigint": "integer", "smallint": "integer",
            "numeric": "numeric", "real": "numeric", "double precision": "numeric",
            "boolean": "boolean",
            "character varying": "string", "text": "string", "character": "string",
            "uuid": "string", "varchar": "string",
            "timestamp with time zone": "datetime", "timestamp without time zone": "datetime",
            "date": "datetime", "time with time zone": "datetime",
            "tstzrange": "datetime",
            "jsonb": "json", "json": "json",
            "ARRAY": "array",
            "USER-DEFINED": "geometry",
        }

        query_sql = """
            SELECT column_name, data_type, udt_name
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table
            ORDER BY ordinal_position;
        """

        async def _query(conn):
            # Check table exists first
            exists = await shared_queries.table_exists_query.execute(
                conn, schema=schema, table=table
            )
            if not exists:
                return []

            rows = await DQLQuery(
                query_sql, result_handler=ResultHandler.ALL_DICTS
            ).execute(conn, schema=schema, table=table)

            # Also collect fields from attribute sidecar (JSONB keys).
            # Use the effective-sidecars variant so an empty-config
            # collection still resolves to its core+injected defaults
            # (M1b.2).
            col_config = await self._get_effective_driver_config(
                catalog_id, collection_id, db_resource=conn,
            )

            fields: list[ProtocolFieldDefinition] = []
            internal_cols = {"geoid", "deleted_at", "transaction_time", "geom", "bbox_geom", "asset_id"}
            for row in (rows or []):
                col_name = row["column_name"]
                if col_name in internal_cols:
                    continue
                data_type = row.get("data_type", "unknown")
                mapped = pg_type_map.get(data_type, "unknown")
                if mapped == "geometry":
                    mapped = "geometry"
                fields.append(ProtocolFieldDefinition(name=col_name, data_type=mapped))

            # Add sidecar attribute fields if available
            if col_config and col_config.sidecars:
                attr_table = f"{table}_attributes"
                attr_exists = await shared_queries.table_exists_query.execute(
                    conn, schema=schema, table=attr_table
                )
                if attr_exists:
                    attr_rows = await DQLQuery(
                        query_sql, result_handler=ResultHandler.ALL_DICTS
                    ).execute(conn, schema=schema, table=attr_table)
                    for row in (attr_rows or []):
                        col_name = row["column_name"]
                        if col_name in internal_cols or col_name == "attributes":
                            continue
                        data_type = row.get("data_type", "unknown")
                        mapped = pg_type_map.get(data_type, "unknown")
                        fields.append(ProtocolFieldDefinition(name=col_name, data_type=mapped))

                    # Also extract keys from JSONB attributes column via sample
                    try:
                        sample_sql = f"""
                            SELECT DISTINCT jsonb_object_keys(attributes) AS key
                            FROM "{schema}"."{attr_table}"
                            WHERE attributes IS NOT NULL
                            LIMIT 1000;
                        """
                        key_rows = await DQLQuery(
                            sample_sql, result_handler=ResultHandler.ALL
                        ).execute(conn)
                        existing_names = {f.name for f in fields}
                        for key_row in (key_rows or []):
                            key_name = key_row[0]
                            if key_name not in existing_names and key_name not in internal_cols:
                                fields.append(ProtocolFieldDefinition(name=key_name, data_type="string"))
                    except Exception:
                        pass

            return fields

        if db_resource is not None:
            return await _query(db_resource)
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.database import DatabaseProtocol
        db_proto = get_protocol(DatabaseProtocol)
        if not db_proto:
            return []
        async with managed_transaction(db_proto.engine) as conn:
            return await _query(conn)

    async def get_entity_fields(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        entity_level: str = "item",
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Return FieldDefinition dict for the given entity level.

        For ``item`` level, wraps ``QueryOptimizer.get_all_queryable_fields()``.
        For ``collection``/``catalog``/``asset`` levels, returns STAC-standard
        fields enriched with driver-specific metadata.
        """
        from dynastore.models.protocols.field_definition import (
            FieldDefinition as ProtocolFieldDefinition,
            FieldCapability,
        )
        from dynastore.modules.db_config.query_executor import managed_transaction

        async def _resolve_item_fields(conn):
            if not collection_id:
                return {}
            # M1b.2: effective sidecars (core defaults + registry injections)
            # so introspection round-trips what ensure_storage will actually
            # materialise, even for a default-body (no-explicit-sidecars)
            # collection.
            col_config = await self._get_effective_driver_config(
                catalog_id, collection_id, db_resource=conn,
            )
            if col_config and col_config.sidecars:
                try:
                    from dynastore.modules.catalog.query_optimizer import QueryOptimizer
                    from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
                        FeatureAttributeSidecarConfig,
                    )

                    optimizer = QueryOptimizer(col_config)
                    sidecar_fields = optimizer.get_all_queryable_fields()

                    # Collect NOT NULL / UNIQUE flags from the attributes sidecar
                    # so introspection round-trips what ensure_storage materialised.
                    schema_flags: Dict[str, Dict[str, bool]] = {}
                    for sc in col_config.sidecars:
                        if isinstance(sc, FeatureAttributeSidecarConfig) and sc.attribute_schema:
                            for entry in sc.attribute_schema:
                                schema_flags[entry.name] = {
                                    "required": not entry.nullable,
                                    "unique": bool(entry.unique),
                                }

                    result = {}
                    for name, fd in sidecar_fields.items():
                        flags = schema_flags.get(name, {})
                        result[name] = ProtocolFieldDefinition(
                            name=fd.name,
                            alias=fd.alias,
                            title=fd.title if isinstance(fd.title, (str, dict, type(None))) else None,
                            description=fd.description if isinstance(fd.description, (str, dict, type(None))) else None,
                            capabilities=list(fd.capabilities),
                            data_type=fd.data_type,
                            expose=fd.expose,
                            required=flags.get("required", False),
                            unique=flags.get("unique", False),
                            aggregations=fd.aggregations,
                            transformations=fd.transformations,
                        )

                    # Overlay ItemsSchema-declared flags (authoritative).
                    try:
                        from dynastore.models.protocols.configs import ConfigsProtocol
                        from dynastore.modules.storage.driver_config import (
                            ItemsSchema,
                        )
                        from dynastore.modules.storage.field_constraints import (
                            overlay_schema_flags,
                        )
                        from dynastore.tools.discovery import get_protocol

                        configs = get_protocol(ConfigsProtocol)
                        if configs is not None:
                            schema_cfg = await configs.get_config(
                                ItemsSchema,
                                catalog_id=catalog_id,
                                collection_id=collection_id,
                                ctx=DriverContext(db_resource=conn),
                            )
                            result = overlay_schema_flags(schema_cfg, result)
                    except Exception:
                        pass
                    return result
                except Exception:
                    pass

            # Fallback: use introspect_schema
            schema_fields = await self.introspect_schema(
                catalog_id, collection_id, db_resource=conn,
            )
            return {
                f["name"]: ProtocolFieldDefinition(
                    name=f["name"],
                    data_type=f.get("type", "string"),
                    capabilities=[FieldCapability.FILTERABLE],
                )
                for f in schema_fields
            }

        def _stac_base_fields(level: str) -> Dict[str, ProtocolFieldDefinition]:
            """Return STAC-standard fields for catalog/collection/asset levels."""
            if level == "catalog":
                names = ["id", "title", "description", "type", "conformsTo"]
            elif level == "collection":
                names = [
                    "id", "title", "description", "license", "keywords",
                    "extent", "providers", "summaries", "links",
                ]
            elif level == "asset":
                names = ["href", "type", "title", "description", "roles"]
            else:
                return {}
            return {
                n: ProtocolFieldDefinition(
                    name=n, data_type="string",
                    capabilities=[FieldCapability.FILTERABLE],
                )
                for n in names
            }

        if entity_level == "item":
            if db_resource is not None:
                return await _resolve_item_fields(db_resource)
            from dynastore.tools.discovery import get_protocol
            from dynastore.models.protocols.database import DatabaseProtocol
            db_proto = get_protocol(DatabaseProtocol)
            if not db_proto:
                return {}
            async with managed_transaction(db_proto.engine) as conn:
                return await _resolve_item_fields(conn)

        return _stac_base_fields(entity_level)

    async def compute_extents(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        from dynastore.modules.db_config.query_executor import (
            DQLQuery, ResultHandler, managed_transaction,
        )

        schema = await self._resolve_schema(catalog_id, db_resource=db_resource)
        table = await self.resolve_physical_table(
            catalog_id, collection_id, db_resource=db_resource,
        )
        if not table:
            return None

        async def _query(conn):
            # M1b.2: effective sidecars so compute_extents can see the
            # geometry sidecar's target_srid / attributes sidecar layout
            # even when the caller never persisted a PG-specific config.
            layer_config = await self._get_effective_driver_config(
                catalog_id, collection_id, db_resource=conn,
            )
            if not layer_config:
                return None

            geom_alias = "h"
            geom_col = "geom"
            storage_srid = 4326
            temporal_alias: Optional[str] = None
            joins = []

            if layer_config.sidecars:
                from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
                    GeometriesSidecarConfig,
                )
                from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
                    FeatureAttributeSidecarConfig,
                )

                geom_sc = next(
                    (sc for sc in layer_config.sidecars if isinstance(sc, GeometriesSidecarConfig)),
                    None,
                )
                if geom_sc:
                    geom_source = f"{table}_{geom_sc.sidecar_id}"
                    geom_alias = "g"
                    storage_srid = geom_sc.target_srid
                    joins.append(
                        f'JOIN "{schema}"."{geom_source}" {geom_alias} ON h.geoid = {geom_alias}.geoid'
                    )

                attr_sc = next(
                    (sc for sc in layer_config.sidecars if isinstance(sc, FeatureAttributeSidecarConfig)),
                    None,
                )
                # ``attr_sc.enable_validity`` (derived from ``validity_column``)
                # mirrors ``ItemsWritePolicy`` (overlaid by ``ensure_storage``
                # at DDL time, #957/#974) so reading the sidecar is the SSOT
                # for this collection.
                # Only join the attributes table for temporal extents when
                # validity is actually persisted — otherwise the row carries
                # no ``validity`` column and the SELECT would error.
                if attr_sc and attr_sc.enable_validity:
                    temporal_source = f"{table}_{attr_sc.sidecar_id}"
                    temporal_alias = "a"
                    if temporal_source != (f"{table}_{geom_sc.sidecar_id}" if geom_sc else ""):
                        joins.append(
                            f'JOIN "{schema}"."{temporal_source}" {temporal_alias} ON h.geoid = {temporal_alias}.geoid'
                        )
                    else:
                        temporal_alias = geom_alias

            if storage_srid == 4326:
                spatial_expr = f"ST_Extent({geom_alias}.{geom_col})"
            else:
                spatial_expr = f"ST_Transform(ST_SetSRID(ST_Extent({geom_alias}.{geom_col}),{storage_srid}), 4326)"

            join_clause = "\n" + "\n".join(joins) if joins else ""

            # Temporal extent only when validity is actually persisted on a
            # joined sidecar. With no validity column anywhere, fall back to
            # NULL bounds so the SELECT remains valid.
            if temporal_alias is not None:
                min_validity_expr = f"MIN(lower({temporal_alias}.validity))"
                max_validity_expr = f"MAX(upper({temporal_alias}.validity))"
            else:
                min_validity_expr = "NULL::timestamptz"
                max_validity_expr = "NULL::timestamptz"

            sql = f"""
                WITH calculated_extents AS (
                    SELECT
                        {spatial_expr} AS combined_geom,
                        {min_validity_expr} AS min_validity,
                        {max_validity_expr} AS max_validity
                    FROM "{schema}"."{table}" h
                    {join_clause}
                    WHERE h.deleted_at IS NULL AND {geom_alias}.{geom_col} IS NOT NULL
                )
                SELECT
                    ST_XMin(combined_geom),
                    ST_YMin(combined_geom),
                    ST_XMax(combined_geom),
                    ST_YMax(combined_geom),
                    CASE WHEN min_validity = '-infinity' THEN NULL ELSE min_validity END,
                    CASE WHEN max_validity = 'infinity' THEN NULL ELSE max_validity END
                FROM calculated_extents;
            """

            row = await DQLQuery(sql, result_handler=ResultHandler.ONE).execute(conn)

            if row and row[0] is not None:
                bbox = [
                    max(-180.0, min(180.0, row[0])),
                    max(-90.0, min(90.0, row[1])),
                    max(-180.0, min(180.0, row[2])),
                    max(-90.0, min(90.0, row[3])),
                ]
            else:
                bbox = [-180.0, -90.0, 180.0, 90.0]

            min_time = row[4] if row else None
            max_time = row[5] if row else None

            return {
                "spatial": {"bbox": [list(bbox)]},
                "temporal": {"interval": [[min_time, max_time]]},
            }

        if db_resource is not None:
            return await _query(db_resource)
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.database import DatabaseProtocol
        db_proto = get_protocol(DatabaseProtocol)
        if not db_proto:
            return None
        async with managed_transaction(db_proto.engine) as conn:
            return await _query(conn)

    async def aggregate(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        aggregation_type: str,
        field: Optional[str] = None,
        request: Optional[Any] = None,
        db_resource: Optional[Any] = None,
    ) -> Any:
        from dynastore.modules.db_config.query_executor import (
            DQLQuery, ResultHandler, managed_transaction,
        )

        schema = await self._resolve_schema(catalog_id, db_resource=db_resource)
        table = await self.resolve_physical_table(
            catalog_id, collection_id, db_resource=db_resource,
        )
        if not table:
            return None

        async def _query(conn):
            # M1b.2: effective sidecars so the bbox-aggregation path can
            # resolve the geometry sidecar's target_srid even when the
            # caller never persisted a PG-specific config.
            layer_config = await self._get_effective_driver_config(
                catalog_id, collection_id, db_resource=conn,
            )

            # Resolve attribute sidecar table
            attr_table = f"{table}_attributes"
            attr_alias = "s"
            attr_join = f'JOIN "{schema}"."{attr_table}" {attr_alias} ON h.geoid = {attr_alias}.geoid'

            # Resolve geometry sidecar table
            geom_table = f"{table}_geometries"
            geom_alias = "g"
            geom_join = f'JOIN "{schema}"."{geom_table}" {geom_alias} ON h.geoid = {geom_alias}.geoid'

            base_where = "h.deleted_at IS NULL"

            if aggregation_type == "terms" and field:
                sql = f"""
                    SELECT {attr_alias}.attributes->>'{field}' AS val, COUNT(*) AS cnt
                    FROM "{schema}"."{table}" h
                    {attr_join}
                    WHERE {base_where} AND {attr_alias}.attributes ? '{field}'
                    GROUP BY val
                    ORDER BY cnt DESC
                    LIMIT 100;
                """
                rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(conn)
                return [{"value": r["val"], "count": r["cnt"]} for r in (rows or [])]

            elif aggregation_type == "stats" and field:
                sql = f"""
                    SELECT
                        MIN(CAST({attr_alias}.attributes->>'{field}' AS NUMERIC)) AS min_val,
                        MAX(CAST({attr_alias}.attributes->>'{field}' AS NUMERIC)) AS max_val,
                        AVG(CAST({attr_alias}.attributes->>'{field}' AS NUMERIC)) AS avg_val,
                        SUM(CAST({attr_alias}.attributes->>'{field}' AS NUMERIC)) AS sum_val,
                        COUNT(*) AS cnt
                    FROM "{schema}"."{table}" h
                    {attr_join}
                    WHERE {base_where}
                      AND {attr_alias}.attributes ? '{field}'
                      AND {attr_alias}.attributes->>'{field}' ~ '^-?[0-9]+(\\.[0-9]+)?$'
                """
                row = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(conn)
                return row

            elif aggregation_type == "datetime_range":
                sql = f"""
                    SELECT
                        MIN(lower({attr_alias}.validity)) AS min_dt,
                        MAX(upper({attr_alias}.validity)) AS max_dt
                    FROM "{schema}"."{table}" h
                    {attr_join}
                    WHERE {base_where};
                """
                row = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(conn)
                return row

            elif aggregation_type == "bbox":
                storage_srid = 4326
                geom_source = geom_alias
                join_sql = geom_join
                if layer_config and layer_config.sidecars:
                    from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
                        GeometriesSidecarConfig,
                    )
                    geom_sc = next(
                        (sc for sc in layer_config.sidecars if isinstance(sc, GeometriesSidecarConfig)),
                        None,
                    )
                    if geom_sc:
                        storage_srid = geom_sc.target_srid

                if storage_srid == 4326:
                    expr = f"ST_Extent({geom_source}.geom)"
                else:
                    expr = f"ST_Transform(ST_SetSRID(ST_Extent({geom_source}.geom),{storage_srid}),4326)"

                sql = f"""
                    SELECT
                        ST_XMin(ext), ST_YMin(ext), ST_XMax(ext), ST_YMax(ext)
                    FROM (
                        SELECT {expr} AS ext
                        FROM "{schema}"."{table}" h
                        {join_sql}
                        WHERE {base_where} AND {geom_source}.geom IS NOT NULL
                    ) sub;
                """
                row = await DQLQuery(sql, result_handler=ResultHandler.ONE).execute(conn)
                if row and row[0] is not None:
                    return [row[0], row[1], row[2], row[3]]
                return None

            elif aggregation_type == "count":
                sql = f'SELECT COUNT(*) FROM "{schema}"."{table}" WHERE {base_where};'
                return await DQLQuery(sql, result_handler=ResultHandler.SCALAR_ONE).execute(conn)

            else:
                raise ValueError(f"Unsupported aggregation_type: {aggregation_type}")

        if db_resource is not None:
            return await _query(db_resource)
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.database import DatabaseProtocol
        db_proto = get_protocol(DatabaseProtocol)
        if not db_proto:
            return None
        async with managed_transaction(db_proto.engine) as conn:
            return await _query(conn)

    async def location(
        self,
        catalog_id: str,
        collection_id: str,
    ) -> "StorageLocation":
        """Return typed physical storage coordinates for this collection.

        Raises ValueError when no ``physical_table`` is set on the driver
        config.  Synthesizing ``collection_id`` here masks lifecycle gaps:
        ``ItemQueryMixin._apply_query_transformations`` consults
        ``resolve_physical_table`` directly and gets ``None``, then raises
        the opaque "Could not resolve storage" error from a deeper frame.
        Failing here keeps the two resolution paths in agreement —
        callers like ``tiles_module.get_tile_resolution_params`` already
        catch ValueError and surface an empty meta (→ 204 tile).
        """
        from dynastore.modules.storage.storage_location import StorageLocation

        schema = await self._resolve_schema(catalog_id)
        table = await self.resolve_physical_table(catalog_id, collection_id)
        if not table:
            raise ValueError(
                f"No physical_table configured for {catalog_id}/{collection_id} "
                f"on driver {type(self).__name__}"
            )
        return StorageLocation(
            backend="postgresql",
            canonical_uri=f"postgresql://{schema}.{table}",
            identifiers={"schema": schema, "table": table},
            display_label=f"{schema}.{table}",
        )

    # --- Internal helpers ---

    def _get_crud_protocol(self):
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.item_crud import ItemCrudProtocol

        svc = get_protocol(ItemCrudProtocol)
        if not svc:
            raise RuntimeError("ItemCrudProtocol not available")
        return svc

    def _get_query_protocol(self):
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.item_query import ItemQueryProtocol

        svc = get_protocol(ItemQueryProtocol)
        if not svc:
            raise RuntimeError("ItemQueryProtocol not available")
        return svc


# ---------------------------------------------------------------------------
# PG-driver init_collection hook (M1b.3)
# ---------------------------------------------------------------------------
#
# Registered on import of this module (same pattern as other sidecar/driver
# hooks across the codebase).  Consumes the ``layer_config`` kwarg that
# CollectionService.create_collection passes through to
# lifecycle_registry.init_collection, types/validates it as a PG driver
# config, and persists via the configs waterfall **only when the caller
# explicitly supplied PG-specific fields**.  A default-body
# POST /collections/{id} carries no layer_config → this hook is a no-op →
# zero rows written to collection_configs (plan §Principle — default-fast
# invariant).
#
# Priority 5 matches _pg_asset_driver_init_tenant so the hook runs early
# relative to optional-extension hooks but doesn't race the raw schema
# initialisation.


from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry  # noqa: E402


@lifecycle_registry.sync_collection_initializer(priority=5)
async def _pg_driver_init_collection(
    conn: Any,
    schema: str,
    catalog_id: str,
    collection_id: str,
    **kwargs: Any,
) -> None:
    """Persist a caller-supplied PG driver config — if (and only if) one was given.

    Accepts ``layer_config`` via kwargs (matches the signature
    CollectionService passes).  Shape:

    - ``None`` / absent → no-op.  Default-body collection creation lands
      here; the collection_configs table gets zero rows from this hook.
    - ``dict`` → validated against ``ItemsPostgresqlDriverConfig``.
      If validation succeeds **and** the dump under
      ``exclude_unset=True`` is non-empty, persist via ``configs.set_config``.
      An empty dict is treated as "nothing to persist" — same as absent.
    - ``ItemsPostgresqlDriverConfig`` instance → same as the dict
      path; persist iff ``exclude_unset=True`` dump is non-empty.
    - Any other type → ignored with a debug log.  Other drivers
      register their own hooks.

    When persisted, ``physical_table`` is deliberately **not** set here —
    that's the responsibility of ``ensure_storage()`` (which is invoked
    by the collection-activation path after the first write).  The
    ``WriteOnce[Optional[str]]`` guard on ``physical_table`` admits a
    transition from ``None`` → real value exactly once.
    """
    layer_config = kwargs.get("layer_config")
    if layer_config is None:
        return

    # Shape-check + coerce to a typed PG config instance.
    from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig

    if isinstance(layer_config, dict):
        if not layer_config:
            return  # empty dict == nothing to persist
        try:
            pg_config = ItemsPostgresqlDriverConfig.model_validate(layer_config)
        except Exception as exc:
            logger.debug(
                "PG init_collection: layer_config dict rejected by validation "
                "for %s/%s: %s",
                catalog_id, collection_id, exc,
            )
            return
    elif isinstance(layer_config, ItemsPostgresqlDriverConfig):
        pg_config = layer_config
    else:
        # Layer config was intended for a different driver (DuckDB, Iceberg,
        # a future Primary, …) or is an unrecognised type.  Let the other
        # driver's hook handle it.
        logger.debug(
            "PG init_collection: layer_config of type %s ignored — not PG-shaped "
            "(for %s/%s)",
            type(layer_config).__name__, catalog_id, collection_id,
        )
        return

    # ``physical_table`` is machine-assigned by ``ensure_storage()`` — never
    # honour a caller-supplied value at collection creation (#1135).  Drop it
    # from the explicit field set so it is neither persisted nor counted toward
    # the default-fast guard below.  (The generic config-write strip enforces
    # the same on the PATCH path; this is the create-path belt-and-suspenders.)
    if pg_config.physical_table is not None:
        logger.warning(
            "PG init_collection: ignoring caller-supplied physical_table=%r for "
            "%s/%s — it is machine-assigned.",
            pg_config.physical_table, catalog_id, collection_id,
        )
        pg_config = pg_config.model_copy(update={"physical_table": None})
        pg_config.__pydantic_fields_set__.discard("physical_table")

    # The default-fast guard: persist only when the caller supplied at
    # least one PG-specific field explicitly.  A bare
    # `ItemsPostgresqlDriverConfig()` dumps to `{}` under
    # exclude_unset=True (M1b.1 discriminator fix keeps sidecar_type in
    # entries, but the outer config has no explicitly-set fields).
    explicit_fields = pg_config.model_dump(exclude_unset=True)
    if not explicit_fields:
        return

    from dynastore.models.protocols.configs import ConfigsProtocol
    from dynastore.tools.discovery import get_protocol

    configs = get_protocol(ConfigsProtocol)
    if configs is None:
        # No configs service registered (test env without PluginConfig setup).
        # Silently skip — matches pre-refactor behaviour in CollectionService.
        return

    # Phase 3: ``sidecars`` is a Computed field, so the external config-write
    # path strips it (``restore_system_assigned_fields``). This hook is the
    # trusted internal provisioner persisting the caller's initial PG physical
    # layout at collection creation (analogous to ``ensure_storage`` stamping
    # the resolved plan), so it writes with ``check_immutability=False`` to
    # bypass the strip — otherwise a creation-time ``layer_config`` sidecar
    # layout (e.g. a columnar ``attribute_schema``) would be discarded before
    # ``ensure_storage`` ever reads it. The "non-authorable" guarantee still
    # holds on the operator-facing PATCH path, which goes through the external
    # (strip) path.
    await configs.set_config(
        ItemsPostgresqlDriverConfig,
        pg_config,
        catalog_id=catalog_id,
        collection_id=collection_id,
        check_immutability=False,
        ctx=DriverContext(db_resource=conn),
    )
    logger.debug(
        "PG init_collection: persisted caller-supplied layer_config for %s/%s "
        "(fields: %s)",
        catalog_id, collection_id, sorted(explicit_fields),
    )


