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
from typing import Optional, Dict, Any, Type, Union, TYPE_CHECKING
from dynastore.tools.cache import cached
from dynastore.modules.storage.router import invalidate_router_cache


def _materialise_ref_row(row: Optional[Dict[str, Any]], ref_key: str) -> Optional["PluginConfig"]:
    """F.4c.2 — turn a ``(class_key, config_data)`` row into a validated ``PluginConfig``.

    Returns ``None`` for an empty row or when the row's ``class_key`` is not in
    the live registry (warning logged so dropped rows surface in operator
    triage).  Inline import resolves the late-binding via
    ``platform_config_service.resolve_config_class``.
    """
    if not row:
        return None
    cls = resolve_config_class(row["class_key"])
    if cls is None:
        logger.warning(
            "get_config_by_ref: ref %r stored class_key %r not in registry",
            ref_key,
            row["class_key"],
        )
        return None
    return cls.model_validate(row["config_data"])


def _maybe_bust_router(cls: Type["PluginConfig"], catalog_id: Optional[str], collection_id: Optional[str]) -> None:
    """Bust the distributed router cache only for routing-config writes.

    Non-routing writes (TilesConfig, FeaturesPluginConfig, ...) don't affect
    driver resolution, so nuking the whole Valkey cache for them is pure waste.
    """
    try:
        from dynastore.modules.storage.routing_config import (
            ItemsRoutingConfig,
            AssetRoutingConfig,
        )
        if not issubclass(cls, (ItemsRoutingConfig, AssetRoutingConfig)):
            return
    except Exception:
        return
    invalidate_router_cache(catalog_id, collection_id)
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
    DbResource,
)
from dynastore.models.driver_context import DriverContext
from dynastore.tools.db import validate_sql_identifier
from dynastore.tools.json import CustomJSONEncoder

# Class-as-identity config API.
from dynastore.modules.db_config.platform_config_service import (
    PluginConfig,
    enforce_config_immutability,
    require_config_class,
    resolve_config_class,
    run_apply_handlers,
    run_validate_handlers,
    _register_schema,
)
from dynastore.modules.db_config.locking_tools import check_table_exists
from dynastore.modules.db_config.typed_store.ddl import (
    CATALOG_CONFIGS_TABLE,
    COLLECTION_CONFIGS_TABLE,
)
from dynastore.modules.db_config.typed_store import config_queries as _cq
from dynastore.models.protocols import ConfigsProtocol, CatalogsProtocol
from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# ==============================================================================
#  LEGACY ALIASES
# ==============================================================================

CatalogConfig = PluginConfig
CollectionConfig = PluginConfig

# ==============================================================================
#  STORAGE & SCHEMAS
# ==============================================================================

# Table name constants live in typed_store/ddl.py — imported above.


# Identity normaliser: accept either a class or a class_key string and return
# (class, class_key).  Endpoints that receive dynamic string path params go
# through this at the service boundary.
def _resolve(config_cls: "Union[str, Type[PluginConfig]]") -> "tuple[Type[PluginConfig], str]":
    cls = require_config_class(config_cls)
    return cls, cls.class_key()


# ==============================================================================
#  CACHES
# ==============================================================================


@cached(maxsize=8192, ttl=300, namespace="catalog_config", ignore=["engine", "catalog_manager"])
async def _catalog_config_cache(
    engine: DbResource,
    catalog_manager: CatalogsProtocol,
    catalog_id: str,
    class_key: str,
) -> Optional[dict]:
    """Database fetcher (returned as dict for immutability)."""
    validate_sql_identifier(catalog_id)
    async with managed_transaction(engine) as conn:
        phys_schema = await catalog_manager.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
        )
        if not phys_schema:
            return None

        if not await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema):
            return None

        return await _cq.select_catalog_config(phys_schema).execute(conn, ref_key=class_key)


@cached(maxsize=16384, ttl=300, namespace="collection_config", ignore=["engine", "catalog_manager"])
async def _collection_config_cache(
    engine: DbResource,
    catalog_manager: CatalogsProtocol,
    catalog_id: str,
    collection_id: str,
    class_key: str,
) -> Optional[dict]:
    """Database fetcher (returned as dict for immutability)."""
    validate_sql_identifier(catalog_id)
    validate_sql_identifier(collection_id)
    async with managed_transaction(engine) as conn:
        phys_schema = await catalog_manager.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
        )
        if not phys_schema:
            return None

        if not await check_table_exists(conn, COLLECTION_CONFIGS_TABLE, phys_schema):
            return None

        return await _cq.select_collection_config(phys_schema).execute(
            conn,
            collection_id=collection_id,
            ref_key=class_key,
        )


# ==============================================================================
#  MANAGER
# ==============================================================================


class ConfigService(ConfigsProtocol):
    """Hierarchical Configuration Manager with framework-level immutability enforcement."""

    priority: int = 10

    def __init__(
        self,
        engine: Optional[DbResource] = None,
        catalog_manager: Optional[CatalogsProtocol] = None,
        platform_config_manager: Optional[PlatformConfigsProtocol] = None,
    ):
        self.engine = engine
        self._catalog_manager = catalog_manager
        self._catalogs_service = catalog_manager
        self._platform_config_service = platform_config_manager

    @property
    def catalog_manager(self) -> CatalogsProtocol:
        return self._get_catalog_manager()

    @catalog_manager.setter
    def catalog_manager(self, value: CatalogsProtocol):
        self._catalogs_service = value

    @property
    def platform_config_service(self) -> PlatformConfigsProtocol:
        return self._get_platform_config_service()

    @platform_config_service.setter
    def platform_config_service(self, value: PlatformConfigsProtocol):
        self._platform_config_service = value

    def is_available(self) -> bool:
        return self.engine is not None

    def _get_catalog_manager(self) -> CatalogsProtocol:
        if self._catalogs_service is None:
            from dynastore.tools.discovery import get_protocol
            self._catalogs_service = get_protocol(CatalogsProtocol)
        assert self._catalogs_service is not None, "CatalogsProtocol not registered"
        return self._catalogs_service

    def _get_platform_config_service(self) -> PlatformConfigsProtocol:
        if self._platform_config_service is None:
            from dynastore.tools.discovery import get_protocol
            self._platform_config_service = get_protocol(PlatformConfigsProtocol)
        if self._platform_config_service is None:
            from dynastore.modules.db_config.platform_config_service import PlatformConfigService
            self._platform_config_service = PlatformConfigService()
        return self._platform_config_service

    async def get_config(
        self,
        config_cls: "Union[str, Type[PluginConfig]]",
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        ctx: Optional[DriverContext] = None,
        config_snapshot: Optional[Dict[str, Any]] = None,
    ) -> PluginConfig:
        """Retrieves configuration with a waterfall MERGE:
        1. Snapshot (if provided, overrides everything)
        2. Start from platform tier (platform row overlaid on code defaults)
        3. Overlay catalog delta (if catalog_id)
        4. Overlay collection delta (if collection_id)

        Each DB row holds only the keys explicitly set at that tier (a delta).
        Fields absent from a delta inherit from the parent tier, so bumping a
        class default or platform value propagates through without rewriting
        per-catalog / per-collection rows.
        """
        cls, class_key = _resolve(config_cls)
        db_resource = ctx.db_resource if ctx else None

        # Tier 0: Snapshot (authoritative override)
        if config_snapshot is not None:
            if class_key in config_snapshot:
                data = config_snapshot[class_key]
                if data:
                    return cls.model_validate(data)
            # Authoritative snapshot: skip DB tiers, fall through to platform.
            catalog_id = None
            collection_id = None

        # Base: platform + code defaults (fully-populated model)
        base = await self._get_platform_config_service().get_config(
            cls, ctx=DriverContext(db_resource=db_resource) if db_resource else None
        )

        if not catalog_id:
            return base

        # Collect per-tier deltas top-down.
        deltas: list[dict] = []

        # Tier 2: Catalog delta
        if not db_resource:
            catalog_delta = await self.get_catalog_config_internal_cached(
                catalog_id, class_key
            )
        else:
            async with managed_transaction(db_resource) as conn:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
                )
                if phys_schema and await check_table_exists(
                    conn, CATALOG_CONFIGS_TABLE, phys_schema
                ):
                    catalog_delta = await _cq.select_catalog_config(phys_schema).execute(
                        conn, ref_key=class_key
                    )
                else:
                    catalog_delta = None
        if catalog_delta:
            deltas.append(catalog_delta)

        # Tier 1: Collection delta
        if collection_id:
            if not db_resource:
                collection_delta = await self.get_collection_config_internal_cached(
                    catalog_id, collection_id, class_key
                )
            else:
                async with managed_transaction(db_resource) as conn:
                    phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                        catalog_id, ctx=DriverContext(db_resource=conn)
                    )
                    if phys_schema and await check_table_exists(
                        conn, COLLECTION_CONFIGS_TABLE, phys_schema
                    ):
                        collection_delta = await _cq.select_collection_config(phys_schema).execute(
                            conn, collection_id=collection_id, ref_key=class_key
                        )
                    else:
                        collection_delta = None
            if collection_delta:
                deltas.append(collection_delta)

        if not deltas:
            return base

        # Merge base.model_dump() with deltas in order (catalog → collection).
        # mode="python" preserves native types for round-trip re-validation.
        merged: Dict[str, Any] = base.model_dump(mode="python")
        for delta in deltas:
            merged.update(delta)
        return cls.model_validate(merged)

    async def get_catalog_config_internal_cached(
        self, catalog_id: str, class_key: str
    ) -> Optional[dict]:
        assert self.engine is not None, "ConfigService.engine not initialised"
        return await _catalog_config_cache(
            self.engine, self._get_catalog_manager(), catalog_id, class_key
        )

    async def get_persisted_config(
        self,
        config_cls: "Union[str, Type[PluginConfig]]",
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
    ) -> Optional[dict]:
        """Return the raw row at the exact tier implied by the ids.

        No waterfall, no class-default filling. ``None`` when the row is
        absent. Used by PATCH handlers to merge partial updates over the
        tier-local delta rather than the resolved view.
        """
        _, class_key = _resolve(config_cls)
        if catalog_id and collection_id:
            return await self.get_collection_config_internal_cached(
                catalog_id, collection_id, class_key
            )
        if catalog_id:
            return await self.get_catalog_config_internal_cached(catalog_id, class_key)
        platform_svc = self._get_platform_config_service()
        getter = getattr(platform_svc, "get_platform_config_internal_cached", None)
        if getter is None:
            return None
        return await getter(class_key)

    async def get_collection_config_internal_cached(
        self, catalog_id: str, collection_id: str, class_key: str
    ) -> Optional[dict]:
        assert self.engine is not None, "ConfigService.engine not initialised"
        return await _collection_config_cache(
            self.engine, self._get_catalog_manager(), catalog_id, collection_id, class_key
        )

    async def set_config(
        self,
        config_cls: "Union[str, Type[PluginConfig]]",
        config: PluginConfig,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        check_immutability: bool = True,
        ctx: Optional[DriverContext] = None,
    ) -> "PluginConfig":
        """Persist a config and return it.

        The returned object is the post-apply config — validate handlers
        and the ``_self_register_*`` augmentation mutate ``config`` in
        place before the upsert, so callers (and the configs API route)
        get the *effective* stored shape rather than ``None``.  #738/#747
        — returning the config is what replaces the silent ``200 + null``.
        """
        cls, class_key = _resolve(config_cls)
        db_resource = ctx.db_resource if ctx else None
        if collection_id is not None:
            if catalog_id is None:
                raise ValueError("catalog_id is required when collection_id is provided")
            await self._set_collection_config(
                catalog_id, collection_id, cls, config,
                check_immutability=check_immutability, db_resource=db_resource,
            )
        elif catalog_id is not None:
            await self._set_catalog_config(
                catalog_id, cls, config,
                check_immutability=check_immutability, db_resource=db_resource,
            )
        else:
            await self._get_platform_config_service().set_config(
                cls, config,
                check_immutability=check_immutability,
                ctx=DriverContext(db_resource=db_resource) if db_resource else None,
            )
        return config

    async def _set_catalog_config(
        self,
        catalog_id: str,
        cls: Type[PluginConfig],
        config: PluginConfig,
        check_immutability: bool = True,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        validate_sql_identifier(catalog_id)
        class_key = cls.class_key()

        async with managed_transaction(db_resource or self.engine) as conn:
            await self._get_catalog_manager().ensure_catalog_exists(
                catalog_id, ctx=DriverContext(db_resource=conn)
            )

            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
            )
            if not phys_schema:
                await self._get_catalog_manager().ensure_catalog_exists(
                    catalog_id, ctx=DriverContext(db_resource=conn)
                )
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn)
                )

            if not phys_schema:
                raise ValueError(
                    f"Could not resolve physical schema for catalog '{catalog_id}'."
                )

            if check_immutability:
                current_data = await _cq.select_catalog_config_for_update(phys_schema).execute(
                    conn, ref_key=class_key
                )
                if current_data:
                    current_config = cls.model_validate(current_data)
                    await enforce_config_immutability(
                        current_config, config,
                        catalog_id=catalog_id, collection_id=None, conn=conn,
                    )

            # Phase 2 — validate (pre-persist).
            await run_validate_handlers(cls, config, catalog_id, None, conn)

            await _register_schema(conn, config)

            # secret_mode="db" → every Secret field serializes to its
            # encrypted envelope before jsonb persistence. See tools/secrets.py.
            # exclude_unset=True → store only fields the caller explicitly sent.
            # Class defaults and parent-tier values are resolved at read time
            # by the ``get_config`` waterfall, not baked in here.
            config_data = (
                config.model_dump(
                    mode="json",
                    context={"secret_mode": "db"},
                    exclude_unset=True,
                )
                if hasattr(config, "model_dump")
                else config
            )
            await _cq.upsert_catalog_config(phys_schema).execute(
                conn,
                ref_key=class_key,
                class_key=class_key,
                schema_id=type(config).schema_id(),
                config_data=json.dumps(config_data, cls=CustomJSONEncoder),
            )

            # Phase 3 — apply (post-persist, best-effort).
            await run_apply_handlers(cls, config, catalog_id, None, conn)

        _catalog_config_cache.cache_invalidate(
            self.engine, self._get_catalog_manager(), catalog_id, class_key
        )
        _maybe_bust_router(cls, catalog_id, None)

    async def _set_collection_config(
        self,
        catalog_id: str,
        collection_id: str,
        cls: Type[PluginConfig],
        config: PluginConfig,
        check_immutability: bool = True,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        class_key = cls.class_key()

        async with managed_transaction(db_resource or self.engine) as conn:
            await self._get_catalog_manager().ensure_catalog_exists(
                catalog_id, ctx=DriverContext(db_resource=conn)
            )

            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
            )
            if not phys_schema:
                await self._get_catalog_manager().ensure_catalog_exists(
                    catalog_id, ctx=DriverContext(db_resource=conn)
                )
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn)
                )

            if not phys_schema:
                raise ValueError(
                    f"Could not resolve physical schema for catalog '{catalog_id}'."
                )

            # JIT-create the thin collection registry row if it doesn't yet
            # exist. Upfront config workflow: `PUT .../collections/{col}/configs/{key}`
            # always succeeds — every sub-config resolves from the waterfall,
            # so a minimal Collection can be materialised without the caller
            # first hitting `POST /collections`.
            await self._get_catalog_manager().ensure_collection_exists(
                catalog_id, collection_id, ctx=DriverContext(db_resource=conn)
            )

            if check_immutability:
                current_data = await _cq.select_collection_config_for_update(phys_schema).execute(
                    conn,
                    collection_id=collection_id,
                    ref_key=class_key,
                )
                if current_data:
                    current_config = cls.model_validate(current_data)
                    await enforce_config_immutability(
                        current_config, config,
                        catalog_id=catalog_id, collection_id=collection_id, conn=conn,
                    )

            # Phase 2 — validate (pre-persist).
            await run_validate_handlers(cls, config, catalog_id, collection_id, conn)

            await _register_schema(conn, config)

            # secret_mode="db" → every Secret field serializes to its
            # encrypted envelope before jsonb persistence. See tools/secrets.py.
            # exclude_unset=True → store only fields the caller explicitly sent.
            # Class defaults and parent-tier values are resolved at read time
            # by the ``get_config`` waterfall, not baked in here.
            config_data = (
                config.model_dump(
                    mode="json",
                    context={"secret_mode": "db"},
                    exclude_unset=True,
                )
                if hasattr(config, "model_dump")
                else config
            )
            await _cq.upsert_collection_config(phys_schema).execute(
                conn,
                collection_id=collection_id,
                ref_key=class_key,
                class_key=class_key,
                schema_id=type(config).schema_id(),
                config_data=json.dumps(config_data, cls=CustomJSONEncoder),
            )

            # Phase 3 — apply (post-persist, best-effort).
            await run_apply_handlers(cls, config, catalog_id, collection_id, conn)

        _collection_config_cache.cache_invalidate(
            self.engine, self._get_catalog_manager(), catalog_id, collection_id, class_key
        )
        _maybe_bust_router(cls, catalog_id, collection_id)

    async def list_configs(
        self,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        ctx: Optional[DriverContext] = None,
    ) -> Dict[str, Any]:
        db_resource = ctx.db_resource if ctx else None
        if collection_id is not None:
            if catalog_id is None:
                raise ValueError("catalog_id is required when collection_id is provided")

            validate_sql_identifier(catalog_id)
            validate_sql_identifier(collection_id)
            async with managed_transaction(db_resource or self.engine) as conn:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn)
                )
                if not phys_schema:
                    return {"total": 0, "results": []}

                if not await check_table_exists(conn, COLLECTION_CONFIGS_TABLE, phys_schema):
                    return {"total": 0, "results": []}

                rows = await _cq.list_collection_configs_paginated(phys_schema).execute(
                    conn,
                    collection_id=collection_id,
                    limit=limit,
                    offset=offset,
                )

            total = rows[0]["total_count"] if rows else 0
            results = []
            for r in rows:
                ck: str = r["class_key"]
                cls = resolve_config_class(ck)
                if cls is None:
                    logger.warning("Skipping collection_configs row for unknown class_key %r", ck)
                    continue
                results.append({
                    "plugin_id": ck,
                    "config": cls.model_validate(r["config_data"]).model_dump(),
                })
            return {"total": total, "results": results}

        elif catalog_id is not None:
            validate_sql_identifier(catalog_id)
            async with managed_transaction(db_resource or self.engine) as conn:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn)
                )
                if not phys_schema:
                    return {"total": 0, "results": []}

                if not await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema):
                    return {"total": 0, "results": []}

                rows = await _cq.list_catalog_configs_paginated(phys_schema).execute(
                    conn, limit=limit, offset=offset
                )

            total = rows[0]["total_count"] if rows else 0
            results = []
            for r in rows:
                ck: str = r["class_key"]
                cls = resolve_config_class(ck)
                if cls is None:
                    logger.warning("Skipping catalog_configs row for unknown class_key %r", ck)
                    continue
                results.append({
                    "plugin_id": ck,
                    "config": cls.model_validate(r["config_data"]).model_dump(),
                })
            return {"total": total, "results": results}
        else:
            configs = await self._get_platform_config_service().list_configs()
            all_results = [
                {"plugin_id": klass.class_key(), "config": cfg.model_dump()}
                for klass, cfg in configs.items()
            ]
            total = len(all_results)
            return {"total": total, "results": all_results[offset : offset + limit]}

    async def list_catalog_configs(
        self, catalog_id: str, ctx: Optional[DriverContext] = None
    ) -> Dict[str, PluginConfig]:
        validate_sql_identifier(catalog_id)
        db_resource = ctx.db_resource if ctx else None

        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=conn)
            )
            if not phys_schema:
                return {}

            if not await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema):
                return {}

            rows = await _cq.list_catalog_configs(phys_schema).execute(conn)

        configs: Dict[str, PluginConfig] = {}
        for row in rows:
            class_key: str = row["class_key"]
            cls = resolve_config_class(class_key)
            if cls is None:
                logger.warning("Skipping catalog_configs row for unknown class_key %r", class_key)
                continue
            configs[class_key] = cls.model_validate(row["config_data"])
        return configs

    # -----------------------------------------------------------------
    # F.4c.2 — ref-keyed read API
    # -----------------------------------------------------------------

    async def list_refs_at_scope(
        self,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        ctx: Optional[DriverContext] = None,
    ) -> Dict[str, str]:
        """Tier-local enumeration of ``{ref_key: class_key}`` at the implied scope."""
        db_resource = ctx.db_resource if ctx else None

        if collection_id is not None:
            if catalog_id is None:
                raise ValueError("catalog_id is required when collection_id is provided")
            validate_sql_identifier(catalog_id)
            validate_sql_identifier(collection_id)
            async with managed_transaction(db_resource or self.engine) as conn:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
                )
                if not phys_schema:
                    return {}
                if not await check_table_exists(conn, COLLECTION_CONFIGS_TABLE, phys_schema):
                    return {}
                rows = await _cq.list_collection_refs(phys_schema).execute(
                    conn, collection_id=collection_id
                )
            return {r["ref_key"]: r["class_key"] for r in rows}

        if catalog_id is not None:
            validate_sql_identifier(catalog_id)
            async with managed_transaction(db_resource or self.engine) as conn:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
                )
                if not phys_schema:
                    return {}
                if not await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema):
                    return {}
                rows = await _cq.list_catalog_refs(phys_schema).execute(conn)
            return {r["ref_key"]: r["class_key"] for r in rows}

        # Platform scope — delegate to PlatformConfigService.list_refs.
        platform_svc = self._get_platform_config_service()
        getter = getattr(platform_svc, "list_refs", None)
        if getter is None:
            return {}
        return await getter()

    async def get_config_by_ref(
        self,
        ref_key: str,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        ctx: Optional[DriverContext] = None,
    ) -> Optional[PluginConfig]:
        """Tier-local read by ``ref_key`` at the implied scope.

        Resolves the dispatch class from the row's ``class_key`` discriminator
        and validates the JSON payload through that class.  Returns ``None``
        when the row is absent or its ``class_key`` is no longer registered
        (warning logged).  Does NOT walk the waterfall — refs are explicit.
        """
        db_resource = ctx.db_resource if ctx else None

        if collection_id is not None:
            if catalog_id is None:
                raise ValueError("catalog_id is required when collection_id is provided")
            validate_sql_identifier(catalog_id)
            validate_sql_identifier(collection_id)
            async with managed_transaction(db_resource or self.engine) as conn:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
                )
                if not phys_schema:
                    return None
                if not await check_table_exists(conn, COLLECTION_CONFIGS_TABLE, phys_schema):
                    return None
                row = await _cq.select_collection_config_by_ref(phys_schema).execute(
                    conn, collection_id=collection_id, ref_key=ref_key
                )
            return _materialise_ref_row(row, ref_key)

        if catalog_id is not None:
            validate_sql_identifier(catalog_id)
            async with managed_transaction(db_resource or self.engine) as conn:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
                )
                if not phys_schema:
                    return None
                if not await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema):
                    return None
                row = await _cq.select_catalog_config_by_ref(phys_schema).execute(
                    conn, ref_key=ref_key
                )
            return _materialise_ref_row(row, ref_key)

        # Platform scope — delegate to PlatformConfigService.get_config_by_ref.
        platform_svc = self._get_platform_config_service()
        getter = getattr(platform_svc, "get_config_by_ref", None)
        if getter is None:
            return None
        return await getter(
            ref_key,
            ctx=DriverContext(db_resource=db_resource) if db_resource else None,
        )

    # -----------------------------------------------------------------
    # F.4c.4 — ref-keyed write API
    # -----------------------------------------------------------------

    async def set_config_by_ref(
        self,
        ref_key: str,
        config: PluginConfig,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        check_immutability: bool = True,
        ctx: Optional[DriverContext] = None,
    ) -> "PluginConfig":
        """Tier-local write at ``(ref_key, scope)`` — see ConfigsProtocol.

        Returns the post-apply config (mutated in place by the validate
        phase) — #738/#747, replaces the silent ``200 + null``.
        """
        if collection_id is not None and catalog_id is None:
            raise ValueError("catalog_id is required when collection_id is provided")

        db_resource = ctx.db_resource if ctx else None
        cls = type(config)
        class_key = cls.class_key()

        if collection_id is not None:
            if catalog_id is None:
                raise ValueError("catalog_id is required when collection_id is provided")
            await self._set_collection_config_by_ref(
                ref_key, catalog_id, collection_id, cls, config,
                check_immutability=check_immutability, db_resource=db_resource,
            )
            return config

        if catalog_id is not None:
            await self._set_catalog_config_by_ref(
                ref_key, catalog_id, cls, config,
                check_immutability=check_immutability, db_resource=db_resource,
            )
            return config

        # Platform scope — delegate.
        platform_svc = self._get_platform_config_service()
        setter = getattr(platform_svc, "set_config_by_ref", None)
        if setter is None:
            raise RuntimeError(
                "set_config_by_ref: platform service does not implement the "
                "ref-keyed write API"
            )
        await setter(
            ref_key, config,
            check_immutability=check_immutability,
            ctx=DriverContext(db_resource=db_resource) if db_resource else None,
        )
        # Class-keyed cache lives on the platform service; nothing to bust here.
        _maybe_bust_router(cls, None, None)
        return config

    async def _set_catalog_config_by_ref(
        self,
        ref_key: str,
        catalog_id: str,
        cls: Type[PluginConfig],
        config: PluginConfig,
        check_immutability: bool = True,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        validate_sql_identifier(catalog_id)
        class_key = cls.class_key()

        async with managed_transaction(db_resource or self.engine) as conn:
            await self._get_catalog_manager().ensure_catalog_exists(
                catalog_id, ctx=DriverContext(db_resource=conn)
            )
            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
            )
            if not phys_schema:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn)
                )
            if not phys_schema:
                raise ValueError(
                    f"Could not resolve physical schema for catalog '{catalog_id}'."
                )

            existing = await _cq.select_catalog_config_by_ref(phys_schema).execute(
                conn, ref_key=ref_key
            )
            if existing:
                stored_class_key = existing["class_key"]
                if stored_class_key != class_key:
                    raise ValueError(
                        f"set_config_by_ref({ref_key!r}) at catalog "
                        f"{catalog_id!r}: row stored as class_key="
                        f"{stored_class_key!r}, refusing to overwrite with "
                        f"class_key={class_key!r}.  Delete the ref first or "
                        f"pick a different name."
                    )
                if check_immutability:
                    await enforce_config_immutability(
                        cls.model_validate(existing["config_data"]), config,
                        catalog_id=catalog_id, collection_id=None, conn=conn,
                    )

            # Phase 2 — validate (pre-persist).
            await run_validate_handlers(cls, config, catalog_id, None, conn)

            await _register_schema(conn, config)

            config_data = (
                config.model_dump(
                    mode="json",
                    context={"secret_mode": "db"},
                    exclude_unset=True,
                )
                if hasattr(config, "model_dump")
                else config
            )
            await _cq.upsert_catalog_config(phys_schema).execute(
                conn,
                ref_key=ref_key,
                class_key=class_key,
                schema_id=type(config).schema_id(),
                config_data=json.dumps(config_data, cls=CustomJSONEncoder),
            )

            # Phase 3 — apply (post-persist, best-effort).
            await run_apply_handlers(cls, config, catalog_id, None, conn)

        _catalog_config_cache.cache_invalidate(
            self.engine, self._get_catalog_manager(), catalog_id, class_key
        )
        _maybe_bust_router(cls, catalog_id, None)

    async def _set_collection_config_by_ref(
        self,
        ref_key: str,
        catalog_id: str,
        collection_id: str,
        cls: Type[PluginConfig],
        config: PluginConfig,
        check_immutability: bool = True,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        class_key = cls.class_key()

        async with managed_transaction(db_resource or self.engine) as conn:
            await self._get_catalog_manager().ensure_catalog_exists(
                catalog_id, ctx=DriverContext(db_resource=conn)
            )
            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
            )
            if not phys_schema:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn)
                )
            if not phys_schema:
                raise ValueError(
                    f"Could not resolve physical schema for catalog '{catalog_id}'."
                )
            await self._get_catalog_manager().ensure_collection_exists(
                catalog_id, collection_id, ctx=DriverContext(db_resource=conn)
            )

            existing = await _cq.select_collection_config_by_ref(phys_schema).execute(
                conn, collection_id=collection_id, ref_key=ref_key
            )
            if existing:
                stored_class_key = existing["class_key"]
                if stored_class_key != class_key:
                    raise ValueError(
                        f"set_config_by_ref({ref_key!r}) at "
                        f"{catalog_id}/{collection_id}: row stored as "
                        f"class_key={stored_class_key!r}, refusing to "
                        f"overwrite with class_key={class_key!r}."
                    )
                if check_immutability:
                    await enforce_config_immutability(
                        cls.model_validate(existing["config_data"]), config,
                        catalog_id=catalog_id, collection_id=collection_id, conn=conn,
                    )

            # Phase 2 — validate (pre-persist).
            await run_validate_handlers(cls, config, catalog_id, collection_id, conn)

            await _register_schema(conn, config)

            config_data = (
                config.model_dump(
                    mode="json",
                    context={"secret_mode": "db"},
                    exclude_unset=True,
                )
                if hasattr(config, "model_dump")
                else config
            )
            await _cq.upsert_collection_config(phys_schema).execute(
                conn,
                collection_id=collection_id,
                ref_key=ref_key,
                class_key=class_key,
                schema_id=type(config).schema_id(),
                config_data=json.dumps(config_data, cls=CustomJSONEncoder),
            )

            # Phase 3 — apply (post-persist, best-effort).
            await run_apply_handlers(cls, config, catalog_id, collection_id, conn)

        _collection_config_cache.cache_invalidate(
            self.engine,
            self._get_catalog_manager(),
            catalog_id,
            collection_id,
            class_key,
        )
        _maybe_bust_router(cls, catalog_id, collection_id)

    async def delete_config_by_ref(
        self,
        ref_key: str,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        ctx: Optional[DriverContext] = None,
    ) -> bool:
        """Tier-local delete at ``(ref_key, scope)`` — see ConfigsProtocol."""
        db_resource = ctx.db_resource if ctx else None

        if collection_id is not None:
            if catalog_id is None:
                raise ValueError("catalog_id is required when collection_id is provided")
            validate_sql_identifier(catalog_id)
            validate_sql_identifier(collection_id)
            async with managed_transaction(db_resource or self.engine) as conn:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
                )
                if not phys_schema:
                    return False
                if not await check_table_exists(conn, COLLECTION_CONFIGS_TABLE, phys_schema):
                    return False
                existing = await _cq.select_collection_config_by_ref(phys_schema).execute(
                    conn, collection_id=collection_id, ref_key=ref_key
                )
                if not existing:
                    return False
                stored_class_key = existing["class_key"]
                await _cq.delete_collection_config(phys_schema).execute(
                    conn, collection_id=collection_id, ref_key=ref_key,
                )
            _collection_config_cache.cache_invalidate(
                self.engine, self._get_catalog_manager(),
                catalog_id, collection_id, stored_class_key,
            )
            cls = resolve_config_class(stored_class_key)
            if cls is not None:
                _maybe_bust_router(cls, catalog_id, collection_id)
            return True

        if catalog_id is not None:
            validate_sql_identifier(catalog_id)
            async with managed_transaction(db_resource or self.engine) as conn:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
                )
                if not phys_schema:
                    return False
                if not await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema):
                    return False
                existing = await _cq.select_catalog_config_by_ref(phys_schema).execute(
                    conn, ref_key=ref_key
                )
                if not existing:
                    return False
                stored_class_key = existing["class_key"]
                await _cq.delete_catalog_config(phys_schema).execute(
                    conn, ref_key=ref_key,
                )
            _catalog_config_cache.cache_invalidate(
                self.engine, self._get_catalog_manager(), catalog_id, stored_class_key,
            )
            cls = resolve_config_class(stored_class_key)
            if cls is not None:
                _maybe_bust_router(cls, catalog_id, None)
            return True

        platform_svc = self._get_platform_config_service()
        deleter = getattr(platform_svc, "delete_config_by_ref", None)
        if deleter is None:
            return False
        return await deleter(
            ref_key,
            ctx=DriverContext(db_resource=db_resource) if db_resource else None,
        )

    async def search(
        self,
        query: Optional[str] = None,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        ctx: Optional["DriverContext"] = None,
    ) -> Dict[str, Any]:
        db_resource = ctx.db_resource if ctx else None
        if catalog_id:
            validate_sql_identifier(catalog_id)
        if collection_id:
            validate_sql_identifier(collection_id)

        async with managed_transaction(db_resource or self.engine) as conn:
            if catalog_id:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn)
                )
                if not phys_schema:
                    return {"total": 0, "results": []}

                catalog_table_exists = await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema)
                collection_table_exists = await check_table_exists(conn, COLLECTION_CONFIGS_TABLE, phys_schema)
                if not catalog_table_exists and not collection_table_exists:
                    return {"total": 0, "results": []}

                if collection_id:
                    if not collection_table_exists:
                        return {"total": 0, "results": []}
                    sql = f"""
                    SELECT COUNT(*) OVER() as total_count, 'collection' as level, collection_id, class_key, config_data
                    FROM \"{phys_schema}\".{COLLECTION_CONFIGS_TABLE}
                    WHERE collection_id = :collection_id
                    """
                else:
                    parts = []
                    if catalog_table_exists:
                        parts.append(f"""
                        SELECT 'catalog' as level, NULL as collection_id, class_key, config_data
                        FROM \"{phys_schema}\".{CATALOG_CONFIGS_TABLE}""")
                    if collection_table_exists:
                        parts.append(f"""
                        SELECT 'collection' as level, collection_id, class_key, config_data
                        FROM \"{phys_schema}\".{COLLECTION_CONFIGS_TABLE}""")
                    sql = f"""
                    SELECT COUNT(*) OVER() as total_count, level, collection_id, class_key, config_data FROM (
                        {' UNION ALL '.join(parts)}
                    ) sub
                    """

                if query:
                    sql += " WHERE class_key ILIKE :query"

                sql += " ORDER BY level, collection_id, class_key LIMIT :limit OFFSET :offset;"

                rows = await DQLQuery(
                    sql, result_handler=ResultHandler.ALL_DICTS
                ).execute(
                    conn,
                    collection_id=collection_id,
                    query=f"%{query}%" if query else None,
                    limit=limit,
                    offset=offset,
                )

                total = rows[0]["total_count"] if rows else 0
                results = []
                for r in rows:
                    class_key: str = r["class_key"]
                    cls = resolve_config_class(class_key)
                    if cls is None:
                        continue
                    results.append(
                        {
                            "level": r["level"],
                            "catalog_id": catalog_id,
                            "collection_id": r.get("collection_id"),
                            "plugin_id": class_key,
                            "config": cls.model_validate(r["config_data"]).model_dump(),
                        }
                    )
                return {"total": total, "results": results}
            else:
                configs = await self._get_platform_config_service().list_configs()
                all_results = []
                for klass, cfg in configs.items():
                    ck = klass.class_key()
                    if not query or query.lower() in ck.lower():
                        all_results.append(
                            {
                                "level": "platform",
                                "plugin_id": ck,
                                "config": cfg.model_dump(),
                            }
                        )
                total = len(all_results)
                return {"total": total, "results": all_results[offset : offset + limit]}

    async def delete_config(
        self,
        config_cls: "Union[str, Type[PluginConfig]]",
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        ctx: Optional[DriverContext] = None,
    ) -> None:
        cls, class_key = _resolve(config_cls)
        db_resource = ctx.db_resource if ctx else None
        if collection_id is not None:
            if catalog_id is None:
                raise ValueError("catalog_id is required when collection_id is provided")
            await self._delete_collection_config(
                catalog_id, collection_id, cls, db_resource=db_resource
            )
        elif catalog_id is not None:
            await self._delete_catalog_config(
                catalog_id, cls, db_resource=db_resource
            )
        else:
            await self._get_platform_config_service().delete_config(
                cls, ctx=DriverContext(db_resource=db_resource) if db_resource else None
            )

    async def _delete_catalog_config(
        self, catalog_id: str, cls: Type[PluginConfig], db_resource: Optional[DbResource] = None
    ) -> bool:
        class_key = cls.class_key()
        validate_sql_identifier(catalog_id)
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=conn)
            )
            if not phys_schema:
                return False

            if not await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema):
                return False

            rows_affected = await _cq.delete_catalog_config(phys_schema).execute(
                conn, ref_key=class_key
            )

            if rows_affected > 0:
                _catalog_config_cache.cache_invalidate(
                    self.engine, self.catalog_manager, catalog_id, class_key
                )
                _maybe_bust_router(cls, catalog_id, None)
                return True
        return False

    async def _delete_collection_config(
        self,
        catalog_id: str,
        collection_id: str,
        cls: Type[PluginConfig],
        db_resource: Optional[DbResource] = None,
    ) -> bool:
        class_key = cls.class_key()
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=conn)
            )
            if not phys_schema:
                return False

            if not await check_table_exists(conn, COLLECTION_CONFIGS_TABLE, phys_schema):
                return False

            rows_affected = await _cq.delete_collection_config(phys_schema).execute(
                conn,
                collection_id=collection_id,
                ref_key=class_key,
            )

            if rows_affected > 0:
                _collection_config_cache.cache_invalidate(
                    self.engine,
                    self.catalog_manager,
                    catalog_id,
                    collection_id,
                    class_key,
                )
                _maybe_bust_router(cls, catalog_id, collection_id)
                return True
        return False
