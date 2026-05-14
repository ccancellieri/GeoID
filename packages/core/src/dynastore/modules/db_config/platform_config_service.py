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

# File: src/dynastore/modules/db_config/platform_config_service.py

"""Platform Config Service.

Class-as-identity model:

- Every :class:`PluginConfig` subclass IS its own identity. ``class_key()``
  (a stable PascalCase string derived from ``__qualname__``, pinnable via
  ``_class_key: ClassVar[str]``) is used as the primary key in
  ``configs.platform_configs`` / ``<tenant>.catalog_configs`` /
  ``<tenant>.collection_configs``, and as the public identifier exposed by
  the ``/configs/*`` endpoints.
- Discovery goes through :class:`TypedModelRegistry` — the process-wide
  class registry populated automatically by ``PersistentModel.__init_subclass__``.
- Apply handlers attach to the class, not to a string key.
"""

import logging
import json
import typing
import inspect
from contextlib import asynccontextmanager
from typing import (
    Any,
    Dict,
    Optional,
    Tuple,
    Type,
    Union,
)

from dataclasses import dataclass, field as dc_field

from dynastore.tools.cache import cached

from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    DDLQuery,
    ResultHandler,
    managed_transaction,
    DbResource,
)
from .maintenance_tools import ensure_schema_exists
from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
from dynastore.models.driver_context import DriverContext

# imported to avoid circular imports
from dynastore.modules.db_config.exceptions import (
    ConfigValidationError,
    ImmutableConfigError,
)
from dynastore.tools.json import CustomJSONEncoder

# --- Mutability framework + PluginConfig base (re-exported) ---
#
# The marker types and the ``PluginConfig`` base were extracted into
# dependency-light leaf modules — ``dynastore.models.mutability`` and
# ``dynastore.modules.db_config.plugin_config`` — so that a
# Protocol-contracts file can subclass ``PluginConfig`` (e.g.
# ``IamRolesConfig`` in ``models/protocols/authorization.py``) without
# dragging this DB-facing service, and the ``models.protocols`` eager
# hub it imports, into a load-order import cycle (#686).  They are
# re-exported here so the existing call sites that import them from
# ``platform_config_service`` keep working unchanged.
from dynastore.models.mutability import (  # noqa: F401
    Computed,
    ComputedMarker,
    Immutable,
    ImmutableMarker,
    Mutable,
    MutableMarker,
    WriteOnce,
    WriteOnceMarker,
    is_immutable_field,
    is_write_once_field,
)
from dynastore.modules.db_config.plugin_config import (  # noqa: F401
    _APPLY_HANDLERS,
    _VALIDATE_HANDLERS,
    PluginConfig,
    _collect_required_fields,
    list_registered_configs,
    require_config_class,
    resolve_config_class,
)

logger = logging.getLogger(__name__)

async def is_materialized(
    cls: Type["PluginConfig"],
    catalog_id: Optional[str],
    collection_id: Optional[str],
    conn: Any,
) -> bool:
    """True iff the physical resource the config governs has been materialized.

    Per-tier dispatch on ``cls._visibility``:

    - ``"collection"`` → at least one row in the collection's physical items table
    - ``"catalog"``    → at least one collection registered in the catalog
    - ``"platform"`` / ``None`` → at least one catalog provisioned in the platform

    Classes may override the default check via a ``_materialization_check``
    classmethod ``(cat, col, conn) -> bool`` for resource-specific triggers
    (e.g. bucket exists, ES index created).  The default dispatch is correct
    for every currently-``Immutable[]`` field; the hook is documented for
    future tightening.

    The check runs only when ``enforce_config_immutability`` has a non-None
    ``current_config`` (i.e. on UPDATES, not first writes) — so the cost is
    one cheap ``EXISTS`` query on the slow path, never on creation.

    On any error (missing catalog/schema, lookup failure) the check returns
    ``False`` — i.e. pre-materialization — so the gate fails *open* to the
    less-restrictive side.  An apply handler or the underlying physical
    layer will surface the real error if one exists.
    """
    override = getattr(cls, "_materialization_check", None)
    if override is not None:
        try:
            res = override(catalog_id, collection_id, conn)
            if inspect.isawaitable(res):
                res = await res
            return bool(res)
        except Exception:
            logger.debug(
                "is_materialized: %s._materialization_check raised; "
                "treating resource as not materialized.",
                cls.__qualname__, exc_info=True,
            )
            return False

    visibility = getattr(cls, "_visibility", None)
    try:
        if visibility == "collection":
            return await _collection_is_materialized(catalog_id, collection_id, conn)
        if visibility == "catalog":
            return await _catalog_is_materialized(catalog_id, conn)
        # platform / None → global catalogs count
        return await _platform_is_materialized(conn)
    except Exception:
        logger.debug(
            "is_materialized: default dispatch for %s raised; "
            "treating resource as not materialized.",
            cls.__qualname__, exc_info=True,
        )
        return False


async def _collection_is_materialized(
    catalog_id: Optional[str], collection_id: Optional[str], conn: Any,
) -> bool:
    """True iff the collection's physical items table has at least one row.

    Resolves the physical schema via the catalog manager + the writer's
    ``physical_table`` from ``ItemsPostgresqlDriverConfig`` (the only
    canonical SOR — ES is an index, not a system of record).  An empty
    or missing table → not materialized → ``Immutable`` not enforced.
    """
    if not catalog_id or not collection_id:
        return False
    # Lazy imports to avoid platform-tier import cycles.
    from dynastore.models.driver_context import DriverContext
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols import CatalogsProtocol
    from dynastore.modules.db_config.locking_tools import check_table_exists

    catalogs = get_protocol(CatalogsProtocol)
    if catalogs is None:
        return False
    catalog_manager = catalogs.catalog_manager  # type: ignore[attr-defined]
    phys_schema = await catalog_manager.resolve_physical_schema(
        catalog_id, ctx=DriverContext(db_resource=conn),
    )
    if not phys_schema:
        return False
    # Resolve the items physical_table from the collection's items PG
    # driver config (the canonical writer).  Lookup is best-effort —
    # if any layer is absent, treat as not materialized.
    from dynastore.modules.storage.driver_config import (
        ItemsPostgresqlDriverConfig,
    )
    cfg_service = get_protocol(__configs_protocol_ref())
    if cfg_service is None:
        return False
    try:
        items_cfg = await cfg_service.get_config(
            ItemsPostgresqlDriverConfig,
            catalog_id=catalog_id, collection_id=collection_id,
            ctx=DriverContext(db_resource=conn),
        )
    except Exception:
        return False
    phys_table = getattr(items_cfg, "physical_table", None) if items_cfg else None
    if not phys_table:
        return False
    if not await check_table_exists(conn, phys_table, phys_schema):
        return False
    row = await conn.fetchrow(
        f'SELECT 1 FROM "{phys_schema}"."{phys_table}" LIMIT 1'
    )
    return row is not None


async def _catalog_is_materialized(
    catalog_id: Optional[str], conn: Any,
) -> bool:
    """True iff the catalog has at least one physically-created collection."""
    if not catalog_id:
        return False
    from dynastore.models.driver_context import DriverContext
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols import CatalogsProtocol
    from dynastore.modules.db_config.locking_tools import check_table_exists

    catalogs = get_protocol(CatalogsProtocol)
    if catalogs is None:
        return False
    catalog_manager = catalogs.catalog_manager  # type: ignore[attr-defined]
    phys_schema = await catalog_manager.resolve_physical_schema(
        catalog_id, ctx=DriverContext(db_resource=conn),
    )
    if not phys_schema:
        return False
    if not await check_table_exists(conn, "collections", phys_schema):
        return False
    row = await conn.fetchrow(
        f'SELECT 1 FROM "{phys_schema}"."collections" LIMIT 1'
    )
    return row is not None


async def _platform_is_materialized(conn: Any) -> bool:
    """True iff the platform has at least one provisioned catalog."""
    from dynastore.modules.db_config.locking_tools import check_table_exists
    if not await check_table_exists(conn, "catalogs", "configs"):
        return False
    row = await conn.fetchrow('SELECT 1 FROM "configs"."catalogs" LIMIT 1')
    return row is not None


def __configs_protocol_ref():
    """Late-bound ConfigsProtocol resolution — avoids a top-of-file import
    cycle (configs → catalog → db_config → configs)."""
    from dynastore.models.protocols.configs import ConfigsProtocol
    return ConfigsProtocol


async def enforce_config_immutability(
    current_config: Optional["PluginConfig"],
    new_config: "PluginConfig",
    *,
    catalog_id: Optional[str] = None,
    collection_id: Optional[str] = None,
    conn: Any = None,
) -> None:
    """Reject Immutable / WriteOnce field mutations once the physical
    resource has been materialized.

    Three gates compose:
    1. ``current_config is None`` → first write, no constraint.
    2. ``not is_materialized(...)`` → physical layer empty; Immutable
       fields are still safely changeable.  No enforcement.
    3. Materialized → Immutable + WriteOnce checks fire as before.

    ``WriteOnce`` semantics are unchanged either way (it's used for
    system-assigned identity like ``physical_table``/``engine_ref`` that
    transition None→value exactly once at provisioning).  Routing /
    schema / storage layout fields marked ``Immutable[]`` become
    editable on empty resources and frozen after data lands — which is
    the operator mental model the markers were meant to encode.
    """
    if current_config is None:
        return
    model_class = type(current_config)
    if not isinstance(new_config, model_class):
        return
    if not await is_materialized(model_class, catalog_id, collection_id, conn):
        return
    for field_name, field_info in model_class.model_fields.items():
        current_val = getattr(current_config, field_name)
        new_val = getattr(new_config, field_name)
        if is_immutable_field(field_info):
            if current_val != new_val:
                raise ImmutableConfigError(
                    f"Configuration field '{field_name}' in '{model_class.__name__}' is Immutable. "
                    f"Modification forbidden: {current_val} -> {new_val}"
                )
        elif is_write_once_field(field_info):
            if current_val is not None and current_val != new_val:
                raise ImmutableConfigError(
                    f"Configuration field '{field_name}' in '{model_class.__name__}' is WriteOnce. "
                    f"Cannot change a non-None value: {current_val!r} -> {new_val!r}"
                )


async def run_validate_handlers(
    cls: Type["PluginConfig"],
    config: "PluginConfig",
    catalog_id: Optional[str],
    collection_id: Optional[str],
    conn: Any,
) -> None:
    """Phase 2 — validate, pre-upsert.

    Runs each registered validate handler in order; first failure
    short-circuits and PROPAGATES so the enclosing ``managed_transaction``
    rolls back the (not-yet-issued) upsert.

    Exception normalization for HTTP mapping:
    - ``ConfigValidationError`` (already → 400) re-raised as-is.
    - ``ImmutableConfigError`` (already → 409) re-raised as-is, preserving
      the distinct 409 path for mutability vs validation.
    - Any other ``ValueError`` is wrapped in ``ConfigValidationError`` so
      it lands on the 400 mapping rather than the catch-all ``ValueError``
      → 422 path (which is reserved for schema / shape violations).
    - Non-ValueError exceptions propagate untouched (real bugs, not
      domain violations).
    """
    for handler in cls.get_validate_handlers():
        try:
            res = handler(config, catalog_id, collection_id, conn)
            if inspect.isawaitable(res):
                await res
        except ConfigValidationError:
            raise
        except ImmutableConfigError:
            raise
        except ValueError as e:
            raise ConfigValidationError(str(e)) from e


async def run_apply_handlers(
    cls: Type["PluginConfig"],
    config: "PluginConfig",
    catalog_id: Optional[str],
    collection_id: Optional[str],
    conn: Any,
) -> None:
    """Phase 3 — apply, post-upsert.

    Side effects only (cache invalidation, auto-registration, ensure_storage,
    deny-policy sync, plugin reconnect/restart).  Best-effort: each handler
    is wrapped in ``try/except Exception`` that logs and continues.  A
    transient side-effect blip does not roll back a valid persisted config.

    Extension point (deferred per the #738 plan): a future
    ``register_apply_handler(handler, *, fatal=True)`` flag would let
    specific handlers opt into propagation+rollback.  No current consumer.
    """
    class_key = cls.class_key()
    for handler in cls.get_apply_handlers():
        try:
            res = handler(config, catalog_id, collection_id, conn)
            if inspect.isawaitable(res):
                await res
        except Exception as e:
            logger.error(
                "apply handler failed for class=%r catalog=%r collection=%r: %s",
                class_key, catalog_id, collection_id, e,
                exc_info=True,
            )


# --- Field-level change detection ---


@dataclass
class ConfigChange:
    """Structured diff emitted to on_apply handlers when config changes."""

    class_key: str
    old_config: Optional["PluginConfig"]
    new_config: "PluginConfig"
    changed_fields: Dict[str, Tuple[Any, Any]] = dc_field(default_factory=dict)
    catalog_id: Optional[str] = None
    collection_id: Optional[str] = None
    db_resource: Optional[Any] = None

    @property
    def is_creation(self) -> bool:
        return self.old_config is None

    def field_changed(self, name: str) -> bool:
        return name in self.changed_fields


def compute_config_diff(
    old_config: Optional["PluginConfig"], new_config: "PluginConfig"
) -> Dict[str, Tuple[Any, Any]]:
    model_cls = type(new_config)
    if old_config is None:
        return {
            name: (None, getattr(new_config, name))
            for name in model_cls.model_fields
        }
    changed: Dict[str, Tuple[Any, Any]] = {}
    for name in model_cls.model_fields:
        old_val = getattr(old_config, name, None)
        new_val = getattr(new_config, name, None)
        if old_val != new_val:
            changed[name] = (old_val, new_val)
    return changed


# --- Startup resilience ---


async def _platform_table_exists(conn: DbResource) -> bool:
    from dynastore.modules.db_config.locking_tools import check_table_exists
    return await check_table_exists(conn, "platform_configs", "configs")


# --- Schema (Platform Level Only) ---

from dynastore.modules.db_config.typed_store.ddl import PLATFORM_SCHEMAS_DDL
from dynastore.modules.db_config.typed_store import config_queries as _cq

# Aliases kept for call-site readability within this module.
get_platform_config_query = _cq.get_platform_config
upsert_platform_config_query = _cq.upsert_platform_config
list_platform_configs_query = _cq.list_platform_configs
delete_platform_config_query = _cq.delete_platform_config
get_platform_config_by_ref_query = _cq.get_platform_config_by_ref
list_platform_refs_query = _cq.list_platform_refs


async def _register_schema(conn: DbResource, config: "PluginConfig") -> None:
    """Upsert the config's current JSON schema into ``configs.schemas``."""
    cls = type(config)
    await _cq.register_schema.execute(
        conn,
        schema_id=cls.schema_id(),
        class_key=cls.class_key(),
        schema_json=json.dumps(cls.model_json_schema(), sort_keys=True),
    )


# --- Manager ---


def _post_commit_router_bust(cls: Type["PluginConfig"]) -> None:
    """Bust the distributed storage-router cache after a platform-tier commit.

    Only routing configs affect that cache; for anything else this is a
    no-op. Lazy import keeps ``modules/db_config`` free of a hard dep on
    ``modules/storage``.
    """
    try:
        from dynastore.modules.storage.routing_config import (
            ItemsRoutingConfig,
            AssetRoutingConfig,
        )
        if not issubclass(cls, (ItemsRoutingConfig, AssetRoutingConfig)):
            return
        from dynastore.modules.storage.router import invalidate_router_cache
        invalidate_router_cache(None, None)
    except Exception:
        pass


from dynastore.tools.plugin import ProtocolPlugin


class PlatformConfigService(ProtocolPlugin[object], PlatformConfigsProtocol):
    """Manages global configuration settings (Level 3)."""

    def __init__(self, engine: Optional[DbResource] = None):
        self._engine = engine
        self._setup_cache()

    @property
    def is_platform_manager(self) -> bool:
        return True

    @property
    def engine(self) -> Optional[DbResource]:
        if self._engine:
            return self._engine
        from dynastore.tools.protocol_helpers import get_engine
        return get_engine()

    def _setup_cache(self):
        self.get_platform_config_internal_cached = cached(
            maxsize=64, ttl=300, namespace="platform_config"
        )(self._get_platform_config_internal_db)

    @asynccontextmanager
    async def lifespan(self, app_state: Any) -> typing.AsyncGenerator[None, None]:
        if not hasattr(self, "get_platform_config_internal_cached"):
            self._setup_cache()
        if self.engine is not None:
            await self.initialize_storage(self.engine)
        logger.info("PlatformConfigService: Started.")
        yield
        logger.info("PlatformConfigService: Stopped.")

    @classmethod
    async def initialize_storage(cls, conn: DbResource):
        try:
            logger.info("Initializing Platform Config Storage (configs schema)...")
            await ensure_schema_exists(conn, "configs")
            await DDLQuery(PLATFORM_SCHEMAS_DDL).execute(conn)
            logger.info("Platform Config Storage initialized successfully.")
        except Exception as e:
            logger.error(
                f"FATAL: PlatformConfigService initialization failed: {e}",
                exc_info=True,
            )
            raise

    async def get_config(
        self,
        config_cls: Union[str, Type[PluginConfig]],
        ctx: Optional[DriverContext] = None,
    ) -> PluginConfig:
        cls = require_config_class(config_cls)
        db_resource = ctx.db_resource if ctx else None
        config = await self._get_platform_config_internal(cls, db_resource=db_resource)
        if config:
            return config
        try:
            return cls()
        except Exception as exc:
            from dynastore.modules.db_config.exceptions import ConfigResolutionError
            required = _collect_required_fields(cls)
            raise ConfigResolutionError(
                f"Config '{cls.class_key()}' has no usable default at any scope "
                f"(platform/code) and cannot be constructed with zero args: {exc}",
                missing_key=cls.class_key(),
                required_fields=required,
                scope_tried=["platform", "code_default"],
            ) from exc

    async def _get_platform_config_internal_db(
        self, class_key: str
    ) -> Optional[dict]:
        async with managed_transaction(self.engine) as conn:
            if not await _platform_table_exists(conn):
                return None
            return await get_platform_config_query.execute(conn, ref_key=class_key)

    async def _get_platform_config_internal(
        self,
        cls: Type[PluginConfig],
        db_resource: Optional[DbResource] = None,
    ) -> Optional[PluginConfig]:
        class_key = cls.class_key()
        if db_resource:
            if not await _platform_table_exists(db_resource):
                return None
            data = await get_platform_config_query.execute(
                db_resource, ref_key=class_key
            )
        else:
            data = await self.get_platform_config_internal_cached(class_key)
        if not data:
            return None
        if isinstance(data, cls):
            return data
        return cls.model_validate(data)

    async def set_config(
        self,
        config_cls: Union[str, Type[PluginConfig]],
        config: PluginConfig,
        check_immutability: bool = True,
        ctx: Optional[DriverContext] = None,
    ) -> None:
        cls = require_config_class(config_cls)
        class_key = cls.class_key()
        db_resource = ctx.db_resource if ctx else None
        async with managed_transaction(db_resource or self.engine) as conn:
            old_config: Optional[PluginConfig] = None
            current_data = await get_platform_config_query.execute(
                conn, ref_key=class_key
            )
            if current_data:
                old_config = cls.model_validate(current_data) if not isinstance(
                    current_data, cls
                ) else current_data
                if check_immutability:
                    await enforce_config_immutability(
                        old_config, config,
                        catalog_id=None, collection_id=None, conn=conn,
                    )

            # Phase 2 — validate (pre-persist).  Propagates on failure;
            # ``managed_transaction`` rolls back the (not-yet-issued) upsert.
            await run_validate_handlers(cls, config, None, None, conn)

            await _register_schema(conn, config)

            # exclude_unset=True → platform row stores only fields the caller
            # explicitly sent. Class defaults are resolved at read time, so
            # bumping a class default propagates without rewriting this row.
            await upsert_platform_config_query.execute(
                conn,
                ref_key=class_key,
                class_key=class_key,
                schema_id=type(config).schema_id(),
                config_data=json.dumps(
                    config.model_dump(
                        mode="json",
                        context={"secret_mode": "db"},
                        exclude_unset=True,
                    ),
                    cls=CustomJSONEncoder,
                ),
            )

            # Phase 3 — apply (post-persist, best-effort).
            await run_apply_handlers(cls, config, None, None, conn)

        self.get_platform_config_internal_cached.cache_invalidate(class_key)
        # Post-commit router bust closes the race where an apply_handler
        # invalidates inside the open transaction and a concurrent reader
        # re-caches the pre-commit row. Mirrors catalog/collection tiers.
        _post_commit_router_bust(cls)

    async def list_configs(self) -> Dict[Type[PluginConfig], PluginConfig]:
        """Return ``{class: config}`` for every persisted platform config."""
        async with managed_transaction(self.engine) as conn:
            rows = await list_platform_configs_query.execute(conn)

        configs: Dict[Type[PluginConfig], PluginConfig] = {}
        for row in rows:
            class_key = row["class_key"]
            cls = resolve_config_class(class_key)
            if cls is None:
                logger.warning(
                    "Skipping platform_configs row for unknown class_key %r",
                    class_key,
                )
                continue
            configs[cls] = cls.model_validate(row["config_data"])
        return configs

    async def list_refs(self) -> Dict[str, str]:
        """F.4c.2 — return ``{ref_key: class_key}`` for every platform-stored row.

        Tier-local: does NOT walk the waterfall.  Returns ``{}`` when the
        platform_configs table has no rows.
        """
        async with managed_transaction(self.engine) as conn:
            if not await _platform_table_exists(conn):
                return {}
            rows = await list_platform_refs_query.execute(conn)
        return {row["ref_key"]: row["class_key"] for row in rows}

    async def get_config_by_ref(
        self,
        ref_key: str,
        ctx: Optional[DriverContext] = None,
    ) -> Optional[PluginConfig]:
        """F.4c.2 — return the stored ``PluginConfig`` for ``ref_key`` at platform scope.

        Resolves the dispatch class from the row's ``class_key`` discriminator.
        Returns ``None`` when the row is absent or its ``class_key`` is no
        longer registered (warning logged).  Tier-local: does NOT walk the
        waterfall.
        """
        db_resource = ctx.db_resource if ctx else None
        async with managed_transaction(db_resource or self.engine) as conn:
            if not await _platform_table_exists(conn):
                return None
            row = await get_platform_config_by_ref_query.execute(
                conn, ref_key=ref_key
            )
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
        data = row["config_data"]
        return cls.model_validate(data)

    async def delete_config(
        self,
        config_cls: Union[str, Type[PluginConfig]],
        ctx: Optional[DriverContext] = None,
    ) -> bool:
        cls = require_config_class(config_cls)
        class_key = cls.class_key()
        db_resource = ctx.db_resource if ctx else None
        async with managed_transaction(db_resource or self.engine) as conn:
            rows_affected = await delete_platform_config_query.execute(
                conn, ref_key=class_key
            )
            if rows_affected > 0:
                self.get_platform_config_internal_cached.cache_invalidate(class_key)
                _post_commit_router_bust(cls)
                return True
        return False

    # F.4c.4 — ref-keyed write API (platform scope)

    async def set_config_by_ref(
        self,
        ref_key: str,
        config: PluginConfig,
        check_immutability: bool = True,
        ctx: Optional[DriverContext] = None,
    ) -> None:
        """F.4c.4 — store ``config`` at ``platform.{ref_key}``.

        ``ref_key == class_key`` collapses to the single-instance path that
        :meth:`set_config` already covers.  Different ref_keys allow multiple
        rows per class (multi-instance).

        Immutability check, if enabled, runs against the row stored at the
        same ref_key (so an operator can't repurpose ``pg_main`` to a
        different class) — uses :func:`enforce_config_immutability` against
        the existing row when present.  Apply-handlers fire post-write.
        """
        cls = type(config)
        class_key = cls.class_key()
        db_resource = ctx.db_resource if ctx else None
        async with managed_transaction(db_resource or self.engine) as conn:
            existing_row = await get_platform_config_by_ref_query.execute(
                conn, ref_key=ref_key
            )
            if existing_row:
                stored_class_key = existing_row["class_key"]
                if stored_class_key != class_key:
                    raise ValueError(
                        f"set_config_by_ref({ref_key!r}): row stored as "
                        f"class_key={stored_class_key!r}, refusing to "
                        f"overwrite with class_key={class_key!r}.  Delete "
                        f"the ref first or pick a different name."
                    )
                if check_immutability:
                    old_config = cls.model_validate(existing_row["config_data"])
                    await enforce_config_immutability(
                        old_config, config,
                        catalog_id=None, collection_id=None, conn=conn,
                    )

            # Phase 2 — validate (pre-persist).
            await run_validate_handlers(cls, config, None, None, conn)

            await _register_schema(conn, config)

            await upsert_platform_config_query.execute(
                conn,
                ref_key=ref_key,
                class_key=class_key,
                schema_id=type(config).schema_id(),
                config_data=json.dumps(
                    config.model_dump(
                        mode="json",
                        context={"secret_mode": "db"},
                        exclude_unset=True,
                    ),
                    cls=CustomJSONEncoder,
                ),
            )

            # Phase 3 — apply (post-persist, best-effort).
            await run_apply_handlers(cls, config, None, None, conn)

        # Invalidate the class-keyed cache for the dispatch class so any
        # waterfall reads pick up the change.  Multi-instance rows still
        # share their class with the single-instance default.
        self.get_platform_config_internal_cached.cache_invalidate(class_key)
        _post_commit_router_bust(cls)

    async def delete_config_by_ref(
        self,
        ref_key: str,
        ctx: Optional[DriverContext] = None,
    ) -> bool:
        """F.4c.4 — delete the platform-stored row at ``ref_key``.

        Returns ``True`` when a row was removed, ``False`` for a no-op.
        Tier-local: does NOT cascade to catalog / collection rows that
        share the ref name (per-tier isolation matches single-instance
        ``delete_config``).
        """
        db_resource = ctx.db_resource if ctx else None
        async with managed_transaction(db_resource or self.engine) as conn:
            existing_row = await get_platform_config_by_ref_query.execute(
                conn, ref_key=ref_key
            )
            if not existing_row:
                return False
            stored_class_key = existing_row["class_key"]
            await delete_platform_config_query.execute(conn, ref_key=ref_key)
        # Best-effort cache + router invalidation.  resolve_config_class
        # returns None when the class has been unregistered (warning
        # logged); skip the router bust in that case.
        self.get_platform_config_internal_cached.cache_invalidate(stored_class_key)
        cls = resolve_config_class(stored_class_key)
        if cls is not None:
            _post_commit_router_bust(cls)
        return True
