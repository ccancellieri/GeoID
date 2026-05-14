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

import logging
import json
from typing import List, Optional, Any, Dict, Union, Tuple, Set, Callable
from dynastore.tools.cache import cached
from dynastore.models.driver_context import DriverContext

from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    DbResource,
    ResultHandler,
    managed_transaction,
    managed_nested_transaction,
)
from dynastore.modules.catalog.models import Collection, CollectionUpdate, Catalog
from dynastore.modules.catalog.catalog_config import (
    CollectionPluginConfig,
)
from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol, AssetsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.tools.db import validate_sql_identifier
from dynastore.tools.async_utils import signal_bus
from dynastore.tools.json import CustomJSONEncoder
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry, LifecycleContext
from dynastore.modules.db_config import shared_queries

logger = logging.getLogger(__name__)


def _make_collection_exists_query(phys_schema: str) -> DQLQuery:
    """SELECT id FROM ``phys_schema``.collections by id (non-deleted only).

    Used as a pre-check on read/update paths. Factoring keeps the SELECT
    projection and deleted_at filter in one place.
    """
    return DQLQuery(
        f'SELECT id FROM "{phys_schema}".collections '
        "WHERE id = :id AND deleted_at IS NULL;",
        result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
    )


class CollectionService:
    """Service for collection-level operations."""

    priority: int = 10

    def __init__(self, engine: Optional[DbResource] = None):
        self.engine = engine
        # Instance-bound caches (private)
        self._get_collection_model_cached = cached(maxsize=1024, namespace="collection_model")(
            self._get_collection_model_db
        )

    def is_available(self) -> bool:
        return self.engine is not None

    async def _resolve_physical_schema(
        self, catalog_id: str, db_resource: Optional[DbResource] = None
    ) -> Optional[str]:
        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            return None
        return await catalogs.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=db_resource) if db_resource else None
        )

    async def _get_pg_driver(self):
        """Get the query-fallback (PostgreSQL) storage driver instance."""
        from dynastore.tools.discovery import get_protocols
        from dynastore.models.protocols.storage_driver import (
            Capability,
            CollectionItemsStore,
        )

        for driver in get_protocols(CollectionItemsStore):
            if Capability.QUERY_FALLBACK_SOURCE in driver.capabilities:
                return driver
        return None

    async def resolve_physical_table(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> Optional[str]:
        """Resolve physical table via the PG driver config."""
        pg_driver = await self._get_pg_driver()
        if not pg_driver:
            return None
        return await pg_driver.resolve_physical_table(  # type: ignore[attr-defined]
            catalog_id, collection_id, db_resource=db_resource
        )

    async def set_physical_table(
        self,
        catalog_id: str,
        collection_id: str,
        physical_table: str,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        """Store physical table in PG driver config."""
        pg_driver = await self._get_pg_driver()
        if not pg_driver:
            raise RuntimeError("ItemsPostgresqlDriver not available")
        await pg_driver.set_physical_table(  # type: ignore[attr-defined]
            catalog_id, collection_id, physical_table, db_resource=db_resource
        )

    async def is_active(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> bool:
        """True once storage has been provisioned for this collection.

        Activation state is derived from the PG driver config's
        `physical_table` (`WriteOnce[Optional[str]]`) — pinned exactly
        once at `_activate_collection`.
        """
        phys_table = await self.resolve_physical_table(
            catalog_id, collection_id, db_resource=db_resource
        )
        return phys_table is not None

    async def _activate_collection(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        conn: DbResource,
    ) -> None:
        """Provision storage + pin routing for a pending collection.

        Idempotent. Runs `ensure_storage` against the write driver (creates
        hub + sidecar tables / ES index / GCS prefix), then pins
        `ItemsRoutingConfig` at collection scope so future platform
        default changes cannot silently re-route existing data.

        Skipped gracefully when no storage drivers are registered (test
        environments without `StorageModule`).
        """
        from dynastore.modules.storage.router import get_driver
        from dynastore.modules.storage.routing_config import ItemsRoutingConfig

        # Provision storage. `ensure_storage` is idempotent; concurrent
        # first-inserts will both call it safely.  Each driver self-fetches
        # its own config — no cross-driver type confusion possible.
        try:
            write_driver = await get_driver("WRITE", catalog_id, collection_id)
            await write_driver.ensure_storage(
                catalog_id,
                collection_id,
                db_resource=conn,
            )
        except ValueError:
            # No storage drivers registered — PG-native tables are handled
            # separately; nothing to provision.
            return

        # Pin the resolved routing at collection scope. FOR UPDATE row lock
        # serialises concurrent activators; loser sees equal value and the
        # immutability guard short-circuits (equal values accepted).
        try:
            configs = get_protocol(ConfigsProtocol)
            if configs is None:
                raise ValueError("ConfigsProtocol not registered")
            resolved_routing = await configs.get_config(
                ItemsRoutingConfig,
                catalog_id=catalog_id,
                collection_id=collection_id,
                ctx=DriverContext(db_resource=conn),
            )
            if resolved_routing:
                await configs.set_config(
                    ItemsRoutingConfig,
                    resolved_routing,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    ctx=DriverContext(db_resource=conn),
                )
        except Exception as _routing_e:
            logger.warning(
                "_activate_collection: failed to pin routing for %s/%s: %s",
                catalog_id, collection_id, _routing_e,
            )

    async def activate_collection(
        self,
        catalog_id: str,
        collection_id: str,
        ctx: Optional["DriverContext"] = None,
    ) -> None:
        """Ensure the collection is active.

        Idempotent — safe to call on already-active collections. Called
        from the items write path (lazy activation); no REST endpoint
        backs this method (activation happens transparently on the
        first ``POST /items``). Kept on ``CollectionsProtocol`` so
        ``item_service`` can invoke it via the protocol layer.
        """
        db_resource = ctx.db_resource if ctx else None
        async with managed_transaction(db_resource or self.engine) as conn:
            await self._activate_collection(
                catalog_id, collection_id, conn=conn,
            )

    async def _get_collection_model_db(
        self, catalog_id: str, collection_id: str
    ) -> Optional[Collection]:
        async with managed_transaction(self.engine) as conn:
            return await self._get_collection_model_logic(
                catalog_id, collection_id, conn
            )

    async def _get_collection_model_logic(
        self, catalog_id: str, collection_id: str, conn: DbResource
    ) -> Optional[Collection]:
        phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
        if not phys_schema:
            return None

        # 1. Verify existence in thin PG registry (always PG — thin registry is authoritative).
        exists = await _make_collection_exists_query(phys_schema).execute(
            conn, id=collection_id
        )
        if not exists:
            return None

        # 2. Read metadata via the router — fan-out across every registered
        # CollectionStore driver (PG Core + PG Stac by default;
        # ES joins in when the elasticsearch scope is installed).  The
        # router merges the per-domain slices into one dict.
        from dynastore.modules.catalog.collection_router import (
            get_collection_metadata as _route_get_metadata,
        )

        meta_dict = await _route_get_metadata(
            catalog_id, collection_id, db_resource=conn,
        ) or {}

        # Deserialize JSONB columns
        for key in ["title", "description", "keywords", "license", "links", "assets",
                    "extent", "providers", "summaries", "item_assets", "extra_metadata",
                    "stac_extensions"]:
            val = meta_dict.get(key)
            if isinstance(val, str):
                try:
                    meta_dict[key] = json.loads(val)
                except Exception:
                    meta_dict[key] = None

        data = {
            "id": collection_id,
            "title": meta_dict.get("title"),
            "description": meta_dict.get("description"),
            "keywords": meta_dict.get("keywords"),
            "license": meta_dict.get("license"),
            "links": meta_dict.get("links"),
            "assets": meta_dict.get("assets"),
            "extent": meta_dict.get("extent"),
            "providers": meta_dict.get("providers"),
            "summaries": meta_dict.get("summaries"),
            "item_assets": meta_dict.get("item_assets"),
            "extra_metadata": meta_dict.get("extra_metadata"),
        }

        # 3. CollectionMetadataEnricherProtocol pipeline removed in the
        #    role-based driver refactor (plan §Protocols — deleted).  Any
        #    in-process hook that used to enrich the metadata dict here is
        #    now a TRANSFORM driver routed through CollectionRoutingConfig —
        #    invoked lazily when an endpoint opts in or when the async
        #    reindex pipeline is preparing a transformed INDEX/BACKUP
        #    envelope.  Default read path is deliberately transform-free.

        return Collection.model_validate(data)

    async def get_collection(
        self,
        catalog_id: str,
        collection_id: str,
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
    ) -> Optional[Collection]:
        """Retrieves a collection by ID, localized."""
        db_resource = ctx.db_resource if ctx else None
        collection_model = await self.get_collection_model(
            catalog_id, collection_id, db_resource=db_resource
        )
        if not collection_model:
            return None

        # Localize
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.localization import LocalizationProtocol
        loc = get_protocol(LocalizationProtocol)
        if loc:
            collection_model = loc.localize_model(collection_model, lang)
        return collection_model

    async def get_collection_config(
        self, catalog_id: str, collection_id: str, ctx: Optional["DriverContext"] = None
    ) -> CollectionPluginConfig:
        """Retrieves the active driver config for a collection (via routing)."""
        db_resource = ctx.db_resource if ctx else None
        from dynastore.modules.storage.router import get_driver
        from dynastore.modules.storage.routing_config import Operation

        driver = await get_driver(Operation.READ, catalog_id, collection_id)
        return await driver.get_driver_config(
            catalog_id, collection_id, db_resource=db_resource
        )

    async def get_collection_model(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> Optional[Collection]:
        if db_resource:
            async with managed_transaction(db_resource) as conn:
                return await self._get_collection_model_logic(
                    catalog_id, collection_id, conn
                )
        return await self._get_collection_model_cached(catalog_id, collection_id)

    async def get_collection_column_names(
        self,
        catalog_id: str,
        collection_id: str,
        ctx: Optional["DriverContext"] = None,
    ) -> Set[str]:
        """Retrieves the physical column names for a collection."""
        db_resource = ctx.db_resource if ctx else None
        phys_schema = await self._resolve_physical_schema(
            catalog_id, db_resource=db_resource
        )
        phys_table = await self.resolve_physical_table(
            catalog_id, collection_id, db_resource=db_resource
        )
        if not phys_schema or not phys_table:
            return set()

        from dynastore.modules.db_config.shared_queries import get_table_column_names

        async def _execute(conn):
            return await get_table_column_names(conn, phys_schema, phys_table)

        if db_resource:
            return await _execute(db_resource)
        assert self.engine is not None, "engine required"
        from sqlalchemy.ext.asyncio import AsyncEngine as _AsyncEngine
        assert isinstance(self.engine, _AsyncEngine), "engine must be AsyncEngine for get_collection_column_names"
        async with self.engine.connect() as conn:
            return await _execute(conn)

    async def ensure_collection_exists(
        self,
        db_resource: DbResource,
        catalog_id: str,
        collection_id: str,
        lang: str = "en",
    ) -> None:
        if not await self.get_collection_model(
            catalog_id, collection_id, db_resource=db_resource
        ):
            # If lang is not '*', we provide a simple string which create_collection will localize
            # If lang is '*', we provide the default 'en' dictionary
            title = {"en": collection_id} if lang == "*" else collection_id
            await self.create_collection(
                catalog_id,
                {"id": collection_id, "title": title},
                lang=lang,
                ctx=DriverContext(db_resource=db_resource) if db_resource else None,
            )

    async def create_collection(
        self,
        catalog_id: str,
        collection_definition: Union[Dict[str, Any], Collection],
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
        **kwargs,
    ) -> Collection:
        db_resource = ctx.db_resource if ctx else None
        if isinstance(collection_definition, dict):
            from dynastore.models.localization import validate_language_consistency

            validate_language_consistency(collection_definition, lang)

        collection_model = (
            Collection.create_from_localized_input(collection_definition, lang)
            if isinstance(collection_definition, dict)
            else collection_definition
        )
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_model.id)

        async with managed_transaction(db_resource or self.engine) as conn:
            # Check catalog exists
            catalogs = get_protocol(CatalogsProtocol)
            assert catalogs is not None, "CatalogsProtocol not registered"
            if not await catalogs.get_catalog_model(catalog_id, ctx=DriverContext(db_resource=conn)):
                raise ValueError(f"Catalog '{catalog_id}' does not exist.")

            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            if not phys_schema:
                raise ValueError(f"No physical schema found for catalog '{catalog_id}'")

            logger.info(
                f"[LIFECYCLE] Creating collection '{catalog_id}:{collection_model.id}' in schema '{phys_schema}'"
            )

            # Get driver config (default/platform config only - collection doesn't exist yet,
            # so we must NOT pass db_resource here; querying collection_configs in a nested
            # transaction before the table may be ready would poison the outer transaction).
            from dynastore.modules.storage.router import get_driver as _get_driver
            from dynastore.modules.storage.routing_config import Operation
            _meta_driver = await _get_driver(Operation.READ, catalog_id, collection_model.id)
            collection_config = await _meta_driver.get_driver_config(
                catalog_id, collection_model.id,
            )

            # Layer config override from input
            layer_config_override = None
            if isinstance(collection_definition, dict):
                layer_config_override = collection_definition.get("layer_config")
                if not layer_config_override and "sidecars" in collection_definition:
                    layer_config_override = {
                        "sidecars": collection_definition["sidecars"]
                    }
            else:
                if hasattr(collection_definition, "layer_config"):
                    layer_config_override = getattr(collection_definition, "layer_config")

                # Check for sidecars in Pydantic model (extra fields)
                if not layer_config_override and hasattr(
                    collection_definition, "sidecars"
                ):
                    # We wrap it in a dict to be compatible with CollectionPluginConfig input
                    layer_config_override = {"sidecars": getattr(collection_definition, "sidecars")}

            # M1b.3: blocks that used to (a) coerce layer_config_override from
            # dict → ItemsPostgresqlDriverConfig and (b) iterate
            # SidecarRegistry.get_injected_sidecar_configs to mutate the
            # override are both deleted.  Driver-specific typing + defaults
            # now live inside the PG driver's init_collection hook (see
            # `_pg_driver_init_collection` in
            # modules/storage/drivers/postgresql.py) and inside
            # `_effective_sidecars` at DDL/read/write time.  The generic
            # service hands `layer_config` down to lifecycle_registry as an
            # opaque payload.

            # Resolved config for this collection creation — caller-supplied
            # override if any, else the plugin default (unused by the PG
            # driver post-refactor; still passed through for non-PG drivers
            # that may consume it via their own init_collection hook).
            init_config = layer_config_override or collection_config

            # Clean kwargs to avoid multiple values for arguments already passed positionally or explicitly
            init_kwargs = kwargs.copy()
            init_kwargs.pop("physical_table", None)
            init_kwargs.pop("layer_config", None)

            # 3. Insert thin registry row (id + catalog_id only)
            insert_sql = f"""
                INSERT INTO "{phys_schema}".collections (id, catalog_id)
                VALUES (:id, :catalog_id)
                RETURNING id;
            """
            await DQLQuery(insert_sql, result_handler=ResultHandler.SCALAR_ONE).execute(
                conn, id=collection_model.id, catalog_id=catalog_id,
            )

            # 4. Run infrastructure hooks (events partition, logs, proxy —
            #    hub/sidecars are handled by write_driver.ensure_storage() below).
            await lifecycle_registry.init_collection(
                conn,
                phys_schema,
                catalog_id,
                collection_model.id,
                layer_config=init_config,
                **init_kwargs,
            )

            # M1b.3: the unconditional `configs.set_config` that used to
            # persist layer_config_override on EVERY collection create is
            # deleted.  Persistence of PG-driver-specific config now runs
            # inside the PG init_collection hook registered in
            # postgresql.py — and only when the caller actually supplied
            # PG-specific fields (default-fast invariant).  Other driver
            # modules register their own hooks for their own config
            # shapes.

            # 5b. Persist write_policy + schema (ItemsSchema, if provided) BEFORE
            #     ensure_storage so the driver can materialise required/unique
            #     constraints at DDL time (ItemsSchema.fields →
            #     NOT NULL / UNIQUE in the attributes sidecar).
            write_policy_input = None
            schema_input = None
            if isinstance(collection_definition, dict):
                write_policy_input = collection_definition.get("write_policy")
                schema_input = collection_definition.get("schema")
            else:
                _fields = type(collection_definition).model_fields if hasattr(type(collection_definition), "model_fields") else {}
                write_policy_input = getattr(collection_definition, "write_policy", None) if "write_policy" in _fields else None
                schema_input = getattr(collection_definition, "schema", None) if "schema" in _fields else None

            if write_policy_input:
                from dynastore.modules.storage.driver_config import (
                    ItemsWritePolicy,
                )
                policy = (
                    ItemsWritePolicy.model_validate(write_policy_input)
                    if isinstance(write_policy_input, dict)
                    else write_policy_input
                )
                configs = get_protocol(ConfigsProtocol)
                assert configs is not None, "ConfigsProtocol not registered"
                await configs.set_config(
                    ItemsWritePolicy,
                    policy,
                    catalog_id=catalog_id,
                    collection_id=collection_model.id,
                    ctx=DriverContext(db_resource=conn),
                )
            if schema_input:
                from dynastore.modules.storage.driver_config import (
                    ItemsSchema,
                )
                schema_def = (
                    ItemsSchema.model_validate(schema_input)
                    if isinstance(schema_input, dict)
                    else schema_input
                )
                configs = get_protocol(ConfigsProtocol)
                assert configs is not None, "ConfigsProtocol not registered"
                await configs.set_config(
                    ItemsSchema,
                    schema_def,
                    catalog_id=catalog_id,
                    collection_id=collection_model.id,
                    ctx=DriverContext(db_resource=conn),
                )

            # 5c. Cycle E.2.c / F.0d — seed the collection's privacy state
            #     from ``CatalogPrivacy.collection_defaults.is_private``
            #     when no explicit ``is_private`` was passed.  The
            #     helper writes ``ItemsRoutingConfig`` (private driver
            #     pinned) BEFORE ``CollectionPrivacy(is_private=True)``
            #     so the cascade validator on the second write finds
            #     the private driver already in scope.  No-op when the
            #     catalog default is ``is_private=False``.
            explicit_is_private: Optional[bool] = None
            if isinstance(collection_definition, dict):
                _v = collection_definition.get("is_private")
                if isinstance(_v, bool):
                    explicit_is_private = _v
            elif hasattr(collection_definition, "is_private"):
                _v = getattr(collection_definition, "is_private")
                if isinstance(_v, bool):
                    explicit_is_private = _v
            if explicit_is_private is None:
                from dynastore.modules.catalog.catalog_config import (
                    apply_catalog_default_privacy_seed,
                )
                _seed_configs = get_protocol(ConfigsProtocol)
                try:
                    await apply_catalog_default_privacy_seed(
                        catalog_id,
                        collection_model.id,
                        configs=_seed_configs,
                        db_resource=conn,
                    )
                except Exception as e:
                    logger.warning(
                        "Could not apply CatalogPrivacy.collection_defaults.is_private seed for %s/%s: %s",
                        catalog_id, collection_model.id, e,
                    )

            # 6. (Lazy activation) Steps formerly responsible for
            #    `ensure_storage` + routing pin have moved to
            #    `_activate_collection`. A newly-created collection is
            #    **pending** until either:
            #       - the first `POST /items` triggers lazy activation, or
            #       - an operator calls `POST /collections/{col}/activate`.
            #    During the pending window, `PUT /configs/.../ItemsRoutingConfig`
            #    (and any other collection-scope config) can be freely set —
            #    the Immutable guard short-circuits while `current=None`.

            # 7. Store collection metadata via the router — fan-out across
            # every registered CollectionStore driver.  Each driver
            # filters the unified payload to its own domain's columns and
            # no-ops on an empty filtered slice.
            from dynastore.modules.catalog.collection_router import (
                upsert_collection_metadata as _route_upsert_metadata,
            )

            metadata_payload = collection_model.model_dump(
                by_alias=True, exclude_none=True
            )
            await _route_upsert_metadata(
                catalog_id, collection_model.id, metadata_payload,
                db_resource=conn,
            )

            # Steps 8 & 9 (write_policy + schema persistence) moved to
            # step 5b above so ensure_storage can honour constraints.

        # Resolve physical_table for async lifecycle context (PG driver only; None for others).
        # Use db_resource when available so uncommitted catalog/collection rows are visible.
        # Fall back to None gracefully if the table hasn't been registered yet.
        try:
            physical_table = await self.resolve_physical_table(
                catalog_id, collection_model.id, db_resource=db_resource
            )
        except (ValueError, Exception):
            physical_table = None

        # Invalidate caches
        self._get_collection_model_cached.cache_invalidate(
            catalog_id, collection_model.id
        )

        # Trigger async lifecycle
        config_snapshot = {}
        try:
            _cfg = get_protocol(ConfigsProtocol)
            if _cfg is not None:
                config_snapshot.update(await _cfg.list_catalog_configs(catalog_id))
        except Exception:
            pass

        lifecycle_registry.init_async_collection(
            catalog_id,
            collection_model.id,
            LifecycleContext(
                physical_schema=phys_schema,
                physical_table=physical_table,
                config=config_snapshot,
            ),
        )

        # Emit signal to wake up background tasks (Visibility Gap fix)
        await signal_bus.emit(
            "AFTER_COLLECTION_CREATION", identifier=collection_model.id
        )

        result = await self.get_collection_model(
            catalog_id, collection_model.id, db_resource=db_resource
        )
        assert result is not None, f"Collection '{collection_model.id}' not found after creation"
        return result

    async def list_collections(
        self,
        catalog_id: str,
        limit: int = 10,
        offset: int = 0,
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
        q: Optional[str] = None,
    ) -> List[Collection]:
        db_resource = ctx.db_resource if ctx else None
        # Delegate to the router — it picks the most capable registered
        # driver (CORE has SEARCH capability; ES contributes FULLTEXT when
        # present) and falls back to the direct-PG path below if nothing
        # matches the query shape.
        from dynastore.modules.catalog.collection_router import (
            search_collection_metadata as _route_search,
        )

        try:
            rows, _total = await _route_search(
                catalog_id, q=q, limit=limit, offset=offset,
                db_resource=db_resource,
            )
            if rows:
                return [Collection.model_validate(row) for row in rows]
        except Exception as e:
            logger.warning(
                "Collection-metadata router search failed for %s, "
                "falling back to PG: %s",
                catalog_id, e,
            )

        # PG direct fallback
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            if not phys_schema:
                return []

            if not q:
                query_sql = (
                    f'SELECT c.id, mc.title, mc.description, mc.keywords, mc.license, '
                    f'ms.links, ms.assets, ms.extent, ms.providers, ms.summaries, '
                    f'ms.item_assets, mc.extra_metadata '
                    f'FROM "{phys_schema}".collections c '
                    f'LEFT JOIN "{phys_schema}".collection_core mc ON mc.collection_id = c.id '
                    f'LEFT JOIN "{phys_schema}".collection_stac ms ON ms.collection_id = c.id '
                    f'WHERE c.deleted_at IS NULL '
                    f'ORDER BY c.created_at DESC LIMIT :limit OFFSET :offset;'
                )
                result_rows = await DQLQuery(
                    query_sql, result_handler=ResultHandler.ALL_DICTS
                ).execute(conn, limit=limit, offset=offset)
            else:
                query_sql = (
                    f'SELECT c.id, mc.title, mc.description, mc.keywords, mc.license, '
                    f'ms.links, ms.assets, ms.extent, ms.providers, ms.summaries, '
                    f'ms.item_assets, mc.extra_metadata '
                    f'FROM "{phys_schema}".collections c '
                    f'LEFT JOIN "{phys_schema}".collection_core mc ON mc.collection_id = c.id '
                    f'LEFT JOIN "{phys_schema}".collection_stac ms ON ms.collection_id = c.id '
                    f'WHERE c.deleted_at IS NULL '
                    f"AND (c.id ILIKE :q OR mc.title->>'en' ILIKE :q OR mc.description->>'en' ILIKE :q) "
                    f'ORDER BY c.created_at DESC LIMIT :limit OFFSET :offset;'
                )
                result_rows = await DQLQuery(
                    query_sql, result_handler=ResultHandler.ALL_DICTS
                ).execute(conn, limit=limit, offset=offset, q=f"%{q}%")

            results = []
            for row_dict in result_rows:
                for key in ["title", "description", "keywords", "license", "links",
                            "assets", "extent", "providers", "summaries", "item_assets",
                            "extra_metadata"]:
                    val = row_dict.get(key)
                    if isinstance(val, str):
                        try:
                            row_dict[key] = json.loads(val)
                        except Exception:
                            row_dict[key] = None

                data = {
                    "id": row_dict["id"],
                    "title": row_dict.get("title"),
                    "description": row_dict.get("description"),
                    "keywords": row_dict.get("keywords"),
                    "license": row_dict.get("license"),
                    "links": row_dict.get("links"),
                    "assets": row_dict.get("assets"),
                    "extent": row_dict.get("extent"),
                    "providers": row_dict.get("providers"),
                    "summaries": row_dict.get("summaries"),
                    "item_assets": row_dict.get("item_assets"),
                    "extra_metadata": row_dict.get("extra_metadata"),
                }
                results.append(Collection.model_validate(data))
            return results

    async def update_collection(
        self,
        catalog_id: str,
        collection_id: str,
        updates: Dict[str, Any],
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
    ) -> Optional[Collection]:
        db_resource = ctx.db_resource if ctx else None
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        from dynastore.models.localization import validate_language_consistency

        validate_language_consistency(updates, lang)

        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            if not phys_schema:
                return None

            existing_model = await self._get_collection_model_logic(
                catalog_id, collection_id, conn
            )
            if not existing_model:
                return None

            merged_model = existing_model.merge_localized_updates(updates, lang)

            # Store only user-provided extra_metadata content (no envelope)
            user_extra_metadata = (
                json.dumps(
                    merged_model.extra_metadata.model_dump(exclude_none=True),
                    cls=CustomJSONEncoder,
                )
                if merged_model.extra_metadata
                else None
            )

            # First verify the collection exists in thin registry
            exists = await _make_collection_exists_query(phys_schema).execute(
                conn, id=collection_id
            )
            if not exists:
                return None

            # Fan-out metadata writes via the collection-metadata router.
            from dynastore.modules.catalog.collection_router import (
                upsert_collection_metadata as _route_upsert_metadata,
            )

            metadata_payload = merged_model.model_dump(
                by_alias=True, exclude_none=True
            )
            await _route_upsert_metadata(
                catalog_id, collection_id, metadata_payload,
                db_resource=conn,
            )

            # Fetch the post-write state inside the TX so the caller's
            # response includes the freshly-merged data (the conn is the
            # only resource where the uncommitted upsert is visible).
            fresh = await self._get_collection_model_logic(
                catalog_id, collection_id, conn
            )

        # CRITICAL: invalidate the cache AFTER the transaction commits
        # (closes #199). Pre-fix this happened inside the async-with block
        # while the upsert was still uncommitted. Concurrent readers
        # (background lifecycle hooks, sibling requests) could populate
        # the cache with the OLD pre-write data between cache_invalidate
        # firing and the TX actually committing — leaving the cache with
        # stale data that subsequent GETs would happily serve. Mirror
        # what create_collection (line 624) and delete_collection (923)
        # already do: invalidate after the `async with` exits.
        self._get_collection_model_cached.cache_invalidate(
            catalog_id, collection_id
        )

        return fresh

    async def delete_collection(
        self,
        catalog_id: str,
        collection_id: str,
        force: bool = False,
        ctx: Optional["DriverContext"] = None,
    ) -> bool:
        db_resource = ctx.db_resource if ctx else None
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            if not phys_schema:
                return False

            soft_delete_sql = f'UPDATE "{phys_schema}".collections SET deleted_at = NOW() WHERE id = :id AND deleted_at IS NULL;'
            await DQLQuery(
                soft_delete_sql, result_handler=ResultHandler.ROWCOUNT
            ).execute(conn, id=collection_id)

            # Soft delete cascades to collection_configs — configs are bound to
            # the logical collection identity, not the physical table. Keeping
            # them behind a tombstoned collection would leak stale state if the
            # id is later reused.
            await DQLQuery(
                f'DELETE FROM "{phys_schema}".collection_configs WHERE collection_id = :id;',
                result_handler=ResultHandler.NONE,
            ).execute(conn, id=collection_id)

            logger.info(
                f"[LIFECYCLE] Soft deleted collection '{catalog_id}:{collection_id}'"
            )

            config_snapshot = {}
            if force:
                logger.info(
                    f"[LIFECYCLE] Hard deleting collection '{catalog_id}:{collection_id}'"
                )
                phys_table = await self.resolve_physical_table(
                    catalog_id, collection_id, db_resource=conn
                )

                try:
                    configs = get_protocol(ConfigsProtocol)
                    config_snapshot: Dict[str, Any] = {
                        "catalog_id": catalog_id,
                        "collection_id": collection_id,
                    }
                    if configs is not None:
                        coll_config = await configs.get_config(
                            CollectionPluginConfig,
                            catalog_id,
                            collection_id,
                            ctx=DriverContext(db_resource=conn),
                        )
                        if coll_config:
                            config_snapshot["collection_config"] = coll_config.model_dump()
                except Exception as e:
                    logger.warning(
                        f"Failed to capture config snapshot for '{catalog_id}:{collection_id}: {e}"
                    )

                await lifecycle_registry.destroy_collection(
                    conn, phys_schema, catalog_id, collection_id
                )
                await lifecycle_registry.hard_destroy_collection(
                    conn, phys_schema, catalog_id, collection_id
                )

                if phys_table:
                    await shared_queries.delete_table_query.execute(
                        conn, schema=phys_schema, table=phys_table
                    )

                hard_delete_sql = (
                    f'DELETE FROM "{phys_schema}".collections WHERE id = :id;'
                )
                await DDLQuery(hard_delete_sql).execute(conn, id=collection_id)
                # Fan-out metadata deletion via the router.
                from dynastore.modules.catalog.collection_router import (
                    delete_collection_metadata as _route_delete_metadata,
                )

                await _route_delete_metadata(
                    catalog_id, collection_id, db_resource=conn,
                )
                # Soft-delete already cleared collection_configs; hard-delete
                # runs after soft-delete in the same txn, so no second DELETE
                # is needed here.

                logger.info(
                    f"[LIFECYCLE] Hard deleted collection '{catalog_id}:{collection_id}' successfully"
                )

        self._get_collection_model_cached.cache_invalidate(catalog_id, collection_id)

        if force and phys_schema:
            lifecycle_registry.destroy_async_collection(
                catalog_id,
                collection_id,
                LifecycleContext(
                    physical_schema=phys_schema,
                    physical_table=phys_table,
                    config=config_snapshot,
                ),
            )

        return True

    async def delete_collection_language(
        self,
        catalog_id: str,
        collection_id: str,
        lang: str,
        ctx: Optional["DriverContext"] = None,
    ) -> bool:
        """Deletes a specific language variant from a collection."""
        db_resource = ctx.db_resource if ctx else None
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            if not phys_schema:
                return False

            model = await self._get_collection_model_logic(
                catalog_id, collection_id, conn
            )
            if not model:
                raise ValueError(
                    f"Collection '{catalog_id}:{collection_id}' not found."
                )

            can_delete = False
            fields_to_update: Dict[str, Any] = {}

            # Localizable fields — all belong to the CORE domain.
            for field in [
                "title",
                "description",
                "keywords",
                "license",
                "extra_metadata",
            ]:
                val = getattr(model, field, None)
                if val:
                    langs = val.get_available_languages()
                    if lang in langs:
                        if len(langs) <= 1:
                            raise ValueError(
                                f"Cannot delete language '{lang}' from field '{field}': it is the only language available."
                            )

                        data = val.model_dump(exclude_none=True)
                        if lang in data:
                            del data[lang]
                            # Router-direct upsert takes raw dicts; each
                            # driver encodes JSONB itself via _to_json().
                            fields_to_update[field] = data
                            can_delete = True

            if not can_delete:
                return False

            from dynastore.modules.catalog.collection_router import (
                upsert_collection_metadata as _route_upsert_metadata,
            )

            await _route_upsert_metadata(
                catalog_id, collection_id, fields_to_update,
                db_resource=conn,
            )

            self._get_collection_model_cached.cache_invalidate(
                catalog_id, collection_id
            )
            return True

