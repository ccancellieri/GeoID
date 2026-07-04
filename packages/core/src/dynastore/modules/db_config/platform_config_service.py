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

from pydantic import ValidationError
from typing import (
    Any,
    Dict,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

from dataclasses import dataclass, field as dc_field

from dynastore.tools.cache import cached, DEFAULT_CONFIG_CACHE_TTL, DEFAULT_CONFIG_CACHE_L1_TTL

from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    ResultHandler,
    managed_transaction,
    DbResource,
)
from dynastore.modules.db_config.typed_store.ddl import (
    PLATFORM_SCHEMAS_DDL,
    TASK_CAPABILITY_REGISTRY_DDL,
    LEADER_LEASE_DDL,
    INSTANCE_LIVENESS_DDL,
)
from dynastore.modules.db_config.typed_store import config_queries as _cq
from dynastore.tools.plugin import ProtocolPlugin
from .maintenance_tools import ensure_schema_exists
from .ddl_inference import clear_ddl_existence_cache
from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
from dynastore.models.driver_context import DriverContext

# imported to avoid circular imports
from dynastore.modules.db_config.exceptions import (
    ConfigValidationError,
    ConfigVersionConflictError,
    ImmutableConfigError,
)
from dynastore.modules.db_config.config_version import (
    decode_config_version,
    encode_config_version,
)
from dynastore.tools.json import CustomJSONEncoder

from dynastore.models.mutability import is_immutable_field, is_write_once_field
from dynastore.models.plugin_config import (
    PluginConfig,
    _collect_required_fields,
    require_config_class,
    resolve_config_class,
)
# Re-exported for backward compatibility — callers historically imported
# ``_validate_stored_config`` from this module. The implementation now lives
# in ``stored_config_read`` so the catalog/collection tier (config_service.py)
# can share it instead of duplicating it.
from dynastore.modules.db_config.stored_config_read import (  # noqa: F401
    _validate_stored_config,
)

logger = logging.getLogger(__name__)


# Exceptions that legitimately mean "physical layer absent / not yet
# reachable" — for these the gate correctly fails open to ``False`` so
# Immutable / WriteOnce enforcement stays out of the way on
# pre-materialization edits. Anything outside this tuple — most notably
# ``AttributeError`` / ``TypeError`` / ``ImportError`` / ``NameError``
# — is a code bug and must propagate to the caller. #792 shipped the
# canonical bad shape: a typo'd ``catalogs.catalog_manager.…`` raised
# ``AttributeError``, was swallowed by the broad ``except Exception``
# that used to wrap this dispatch, and silently disabled
# ``Immutable[]`` enforcement for ~14h until a reporter noticed the
# DEBUG traceback. #796 narrows the catch + promotes the legitimate
# fail-open log to WARNING with a structured payload so the same class
# of regression cannot hide again.
# Async-only exception types live behind a guarded import so this module
# stays loadable in sync-only worker images (psycopg2 / SCOPE=db_sync) that
# intentionally do not install asyncpg. Sync workers can never raise these
# exceptions, so an empty tuple in their absence is the correct extension.
# See docs/architecture/database.md "Import-time isolation" + #909.
try:
    import asyncpg as _asyncpg
    _ASYNCPG_ABSENT_EXC: tuple = (
        _asyncpg.UndefinedTableError,
        _asyncpg.UndefinedColumnError,
        _asyncpg.InvalidSchemaNameError,
    )
except ImportError:
    _ASYNCPG_ABSENT_EXC = ()

_EXPECTED_ABSENT_LAYER: tuple = (
    OSError,
    KeyError,
    ValidationError,
    *_ASYNCPG_ABSENT_EXC,
)


async def is_materialized(
    cls: Type["PluginConfig"],
    catalog_id: Optional[str],
    collection_id: Optional[str],
    conn: Any,
) -> bool:
    """True iff the physical resource the config governs has been materialized.

    Per-tier dispatch on ``cls._freeze_at``:

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

    On an *expected* absent-layer error (missing catalog, missing schema,
    missing items table, absent driver config) the check returns ``False``
    so the gate fails *open* to the less-restrictive side and emits a
    structured WARNING (``is_materialized_fail_open``) so the skip is
    visible to log-based metrics. Unexpected exceptions — ``AttributeError``,
    ``TypeError``, ``ImportError`` and friends — are code bugs and are
    propagated to the caller (see ``_EXPECTED_ABSENT_LAYER`` above).
    """
    override = getattr(cls, "_materialization_check", None)
    if override is not None:
        try:
            res = override(catalog_id, collection_id, conn)
            if inspect.isawaitable(res):
                res = await res
            return bool(res)
        except _EXPECTED_ABSENT_LAYER as exc:
            logger.warning(
                "is_materialized_fail_open site=override cls=%s catalog_id=%s "
                "collection_id=%s exception_type=%s",
                cls.__qualname__, catalog_id, collection_id,
                type(exc).__name__, exc_info=True,
            )
            return False

    freeze_at = getattr(cls, "_freeze_at", None)
    try:
        if freeze_at == "collection":
            # When set at the platform/catalog tier (no collection_id, or no
            # catalog_id at all), the "collection has rows" question doesn't
            # apply — fall up the tier. A platform-tier write to a
            # collection-gated class is effectively a default; gate it
            # on whether any catalog/collection has been provisioned so the
            # operator mental model (locked once anything depends on it)
            # still holds.
            if catalog_id and collection_id:
                return await _collection_is_materialized(catalog_id, collection_id, conn)
            if catalog_id:
                return await _catalog_is_materialized(catalog_id, conn)
            return await _platform_is_materialized(conn)
        if freeze_at == "catalog":
            if catalog_id:
                return await _catalog_is_materialized(catalog_id, conn)
            return await _platform_is_materialized(conn)
        # platform / None → global catalogs count
        return await _platform_is_materialized(conn)
    except _EXPECTED_ABSENT_LAYER as exc:
        logger.warning(
            "is_materialized_fail_open site=dispatch cls=%s catalog_id=%s "
            "collection_id=%s freeze_at=%s exception_type=%s",
            cls.__qualname__, catalog_id, collection_id, freeze_at,
            type(exc).__name__, exc_info=True,
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
    phys_schema = await catalogs.resolve_physical_schema(
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
    res = await DQLQuery(
        f'SELECT 1 FROM "{phys_schema}"."{phys_table}" LIMIT 1',
        result_handler=ResultHandler.SCALAR,
    ).execute(conn)
    return res is not None


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
    phys_schema = await catalogs.resolve_physical_schema(
        catalog_id, ctx=DriverContext(db_resource=conn),
    )
    if not phys_schema:
        return False
    if not await check_table_exists(conn, "collections", phys_schema):
        return False
    res = await DQLQuery(
        f'SELECT 1 FROM "{phys_schema}"."collections" LIMIT 1',
        result_handler=ResultHandler.SCALAR,
    ).execute(conn)
    return res is not None


async def _platform_is_materialized(conn: Any) -> bool:
    """True iff the platform has at least one provisioned catalog.

    The catalog row lives in ``catalog.catalogs`` (the catalog tier's
    own schema), not ``configs.catalogs`` (configs registry, which holds
    plugin config rows).
    """
    from dynastore.modules.db_config.locking_tools import check_table_exists
    if not await check_table_exists(conn, "catalogs", "catalog"):
        return False
    res = await DQLQuery(
        'SELECT 1 FROM "catalog"."catalogs" LIMIT 1',
        result_handler=ResultHandler.SCALAR,
    ).execute(conn)
    return res is not None


def __configs_protocol_ref():
    """Late-bound ConfigsProtocol resolution — avoids a top-of-file import
    cycle (configs → catalog → db_config → configs)."""
    from dynastore.models.protocols.configs import ConfigsProtocol
    return ConfigsProtocol


def restore_system_assigned_fields(
    cls: Type["PluginConfig"],
    config: "PluginConfig",
    current_config: Optional["PluginConfig"],
) -> None:
    """Discard caller-supplied values for machine-assigned (``Computed``) fields.

    ``Computed[...]`` fields (e.g. ``physical_table``) are assigned by the
    internal provisioner and flow into SQL identifiers / physical resources —
    an API caller must never set or change them (would enable identifier
    injection and cross-collection/tenant targeting; #1135).  On the external
    write path this restores each such field to the current persisted value, or
    unsets it when there is no current value, in place on ``config``.

    The internal provisioner writes with ``check_immutability=False`` and does
    not call this, so it can still stamp the generated value.
    """
    from dynastore.models.mutability import computed_fields

    sys_fields = computed_fields(cls)
    if not sys_fields:
        return
    fields_set = getattr(config, "__pydantic_fields_set__", None)
    for name in sys_fields:
        current_val = (
            getattr(current_config, name, None) if current_config is not None else None
        )
        # object.__setattr__ writes straight to the instance __dict__ (where
        # pydantic v2 stores field values), bypassing validate_assignment — the
        # value is already-persisted/None, so no re-validation is needed.
        object.__setattr__(config, name, current_val)
        if fields_set is not None:
            if current_val is None:
                fields_set.discard(name)
            else:
                fields_set.add(name)


async def enforce_config_immutability(
    current_config: Optional["PluginConfig"],
    new_config: "PluginConfig",
    *,
    catalog_id: Optional[str] = None,
    collection_id: Optional[str] = None,
    conn: Any = None,
    restrict_to_fields: Optional[Set[str]] = None,
) -> None:
    """Reject Immutable / WriteOnce field mutations.

    Gating:
    1. ``current_config is None`` → first write, no constraint.
    2. ``WriteOnce`` fields lock as soon as they hold a non-None value —
       **independent of materialization**.  They carry system-assigned
       identity (e.g. ``engine_ref``) that must not change once stamped, even
       before any data has landed (#1135).  Only the ``None → value`` first
       transition is permitted.
    3. ``Immutable`` fields are materialization-gated: editable while the
       physical layer is empty, frozen once data lands (PR #738) — the
       operator mental model for storage-layout fields.

    Machine-assigned ``Computed`` fields (``physical_table``) are handled
    upstream by ``restore_system_assigned_fields`` (caller value discarded
    before this runs), so no caller can reach a divergence here.

    ``restrict_to_fields`` (#1198): when set, only these field names are
    compared. The first-write-at-tier path passes the caller's explicitly-set
    fields (``__pydantic_fields_set__``) so a partial override compared against
    an *inherited* baseline cannot raise false positives on Immutable fields a
    parent tier customized but the caller never touched. ``None`` (default)
    compares every field — the behaviour for an update against a stored row at
    the same tier.
    """
    if current_config is None:
        return
    model_class = type(current_config)
    if not isinstance(new_config, model_class):
        return

    materialized = await is_materialized(model_class, catalog_id, collection_id, conn)
    for field_name, field_info in model_class.model_fields.items():
        if restrict_to_fields is not None and field_name not in restrict_to_fields:
            continue
        current_val = getattr(current_config, field_name)
        new_val = getattr(new_config, field_name)
        if is_write_once_field(field_info):
            if current_val is not None and current_val != new_val:
                raise ImmutableConfigError(
                    f"Configuration field '{field_name}' in '{model_class.__name__}' is WriteOnce. "
                    f"Cannot change a non-None value: {current_val!r} -> {new_val!r}"
                )
        elif materialized and is_immutable_field(field_info):
            if current_val != new_val:
                raise ImmutableConfigError(
                    f"Configuration field '{field_name}' in '{model_class.__name__}' is Immutable. "
                    f"Modification forbidden: {current_val} -> {new_val}"
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

# Aliases kept for call-site readability within this module.
get_platform_config_query = _cq.get_platform_config
upsert_platform_config_query = _cq.upsert_platform_config
list_platform_configs_query = _cq.list_platform_configs
delete_platform_config_query = _cq.delete_platform_config
get_platform_config_by_ref_query = _cq.get_platform_config_by_ref
list_platform_refs_query = _cq.list_platform_refs


# --- Manager ---

get_platform_config_versioned_query = _cq.get_platform_config_versioned
cas_update_platform_config_query = _cq.cas_update_platform_config


def _serialize_config_for_db(config: "PluginConfig") -> str:
    """Serialize a config to the JSON string persisted in platform_configs.

    ``secret_mode="db"`` → every Secret field serializes to its encrypted
    envelope before jsonb persistence.  ``exclude_unset=True`` → store only
    the fields the caller explicitly sent; class defaults are resolved at read
    time by the waterfall, not baked in here.

    Single SSOT for the write-side serialization contract shared by
    ``set_config`` and ``set_config_by_ref`` (platform tier).
    """
    config_data = (
        config.model_dump(mode="json", context={"secret_mode": "db"}, exclude_unset=True)
        if hasattr(config, "model_dump")
        else config
    )
    return json.dumps(config_data, cls=CustomJSONEncoder)


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
            maxsize=64,
            ttl=DEFAULT_CONFIG_CACHE_TTL,
            namespace="platform_config",
            l1_ttl=DEFAULT_CONFIG_CACHE_L1_TTL,
        )(self._get_platform_config_internal_db)

    @asynccontextmanager
    async def lifespan(self, app_state: Any) -> typing.AsyncGenerator[None, None]:
        if not hasattr(self, "get_platform_config_internal_cached"):
            self._setup_cache()
        if self.engine is not None:
            await self.initialize_storage(self.engine)
        else:
            logger.warning(
                "PlatformConfigService: no DB engine available at lifespan — expected at "
                "priority-0 startup; configs schema will be initialized by TasksModule.lifespan "
                "once the DB engine is registered downstream."
            )
        logger.info("PlatformConfigService: Started.")
        yield
        logger.info("PlatformConfigService: Stopped.")

    @classmethod
    async def initialize_storage(cls, conn: DbResource):
        try:
            logger.info("Initializing Platform Config Storage (configs schema)...")
            # Re-validate every existence check against the live catalog. A dev
            # DB reset can drop the configs schema while this process keeps
            # running under uvicorn --reload; a stale positive existence cache
            # would otherwise skip the schema/table rebuild and let the DDL below
            # run against a schema that no longer exists.
            clear_ddl_existence_cache()
            await ensure_schema_exists(conn, "configs")
            await DDLQuery(PLATFORM_SCHEMAS_DDL).execute(conn)
            await DDLQuery(TASK_CAPABILITY_REGISTRY_DDL).execute(conn)
            await DDLQuery(LEADER_LEASE_DDL).execute(conn)
            await DDLQuery(INSTANCE_LIVENESS_DDL).execute(conn)
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
        return await self._default_or_raise(cls)

    async def _default_or_raise(self, cls: Type[PluginConfig]) -> PluginConfig:
        """Construct the code-level default, or raise ``ConfigResolutionError``.

        Shared by :meth:`get_config` and :meth:`get_config_versioned` — no
        persisted row means "fall through to the class default", and a
        class that cannot be constructed with zero args is a genuine
        ops misconfiguration in either path.
        """
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

    async def get_config_versioned(
        self,
        config_cls: Union[str, Type[PluginConfig]],
        ctx: Optional[DriverContext] = None,
    ) -> Tuple[PluginConfig, Optional[str]]:
        """Versioned read for CAS writes (#2707).

        Returns ``(config, version)`` where ``config`` is what
        :meth:`get_config` would return (platform row overlaid on the code
        default) and ``version`` is the opaque CAS token for the platform
        row, or ``None`` when no row is persisted yet — nothing to CAS
        against, so a subsequent write should go through unconditionally
        (``expected_version=None``, the default).

        Always reads storage directly, bypassing
        ``get_platform_config_internal_cached`` — the token must reflect
        the true current row, never a stale cached one, or a real
        conflict could be masked (or a phantom one raised).
        """
        cls = require_config_class(config_cls)
        db_resource = ctx.db_resource if ctx else None
        async with managed_transaction(db_resource or self.engine) as conn:
            if not await _platform_table_exists(conn):
                row = None
            else:
                row = await get_platform_config_versioned_query.execute(
                    conn, ref_key=cls.class_key()
                )
        if not row:
            return await self._default_or_raise(cls), None
        data = row["config_data"]
        config = data if isinstance(data, cls) else _validate_stored_config(cls, data)
        return config, encode_config_version(row["updated_at"])

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
        return _validate_stored_config(cls, data)

    async def set_config(
        self,
        config_cls: Union[str, Type[PluginConfig]],
        config: PluginConfig,
        check_immutability: bool = True,
        ctx: Optional[DriverContext] = None,
        expected_version: Optional[str] = None,
    ) -> None:
        """Persist a platform-scope config.

        ``expected_version`` (#2707): when ``None`` (default), writes
        unconditionally — today's behavior, fully backward compatible.
        When set to a token obtained from :meth:`get_config_versioned`,
        the write becomes an atomic compare-and-set: it only lands if the
        stored row's ``updated_at`` still equals that token. On a lost
        race (no row, or a concurrent writer already moved it) this
        raises ``ConfigVersionConflictError`` instead of silently
        overwriting the concurrent change — the caller should re-read via
        ``get_config_versioned`` and retry.
        """
        cls = require_config_class(config_cls)
        class_key = cls.class_key()
        db_resource = ctx.db_resource if ctx else None

        # #2524: a config with no explicitly-set fields serialises to {} under
        # exclude_unset=True.  {} is falsy at read time — the waterfall skips it —
        # so the row provides no value.  Return early to avoid an unnecessary write
        # transaction and spurious config-cache / router-cache invalidation.
        _serialized = _serialize_config_for_db(config)
        if _serialized == "{}":
            logger.debug(
                "%s: skipping empty config write at platform scope; "
                "waterfall defaults apply without a stored row",
                class_key,
            )
            return

        async with managed_transaction(db_resource or self.engine) as conn:
            old_config: Optional[PluginConfig] = None
            current_data = await get_platform_config_query.execute(
                conn, ref_key=class_key
            )
            if current_data:
                old_config = _validate_stored_config(
                    cls, current_data
                ) if not isinstance(current_data, cls) else current_data
            if check_immutability:
                # Discard caller values for machine-assigned (Computed) fields
                # BEFORE enforcement/persist (#1135). Internal provisioning uses
                # check_immutability=False and is unaffected.
                restore_system_assigned_fields(cls, config, old_config)
                if old_config is not None:
                    await enforce_config_immutability(
                        old_config, config,
                        catalog_id=None, collection_id=None, conn=conn,
                    )

            # Phase 2 — validate (pre-persist).  Propagates on failure;
            # ``managed_transaction`` rolls back the (not-yet-issued) upsert.
            await run_validate_handlers(cls, config, None, None, conn)

            # exclude_unset=True → platform row stores only fields the caller
            # explicitly sent. Class defaults are resolved at read time, so
            # bumping a class default propagates without rewriting this row.
            if expected_version is not None:
                rowcount = await cas_update_platform_config_query.execute(
                    conn,
                    ref_key=class_key,
                    class_key=class_key,
                    schema_id=type(config).schema_id(),
                    config_data=_serialized,
                    expected_version=decode_config_version(expected_version),
                )
                if rowcount == 0:
                    raise ConfigVersionConflictError(
                        f"set_config({class_key!r}): expected_version "
                        f"{expected_version!r} no longer matches the stored "
                        f"platform row (or the row is absent) — a concurrent "
                        f"writer changed it. Re-read via get_config_versioned "
                        f"and retry."
                    )
            else:
                await upsert_platform_config_query.execute(
                    conn,
                    ref_key=class_key,
                    class_key=class_key,
                    schema_id=type(config).schema_id(),
                    config_data=_serialized,
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
            configs[cls] = _validate_stored_config(cls, row["config_data"])
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
        return _validate_stored_config(cls, data)

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

        # #2524: skip write when no fields were explicitly set (see set_config).
        _serialized = _serialize_config_for_db(config)
        if _serialized == "{}":
            logger.debug(
                "%s: skipping empty config write (by-ref=%r) at platform scope; "
                "waterfall defaults apply without a stored row",
                class_key, ref_key,
            )
            return

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
                    old_config = _validate_stored_config(
                        cls, existing_row["config_data"]
                    )
                    await enforce_config_immutability(
                        old_config, config,
                        catalog_id=None, collection_id=None, conn=conn,
                    )

            # Phase 2 — validate (pre-persist).
            await run_validate_handlers(cls, config, None, None, conn)

            await upsert_platform_config_query.execute(
                conn,
                ref_key=ref_key,
                class_key=class_key,
                schema_id=type(config).schema_id(),
                config_data=_serialized,
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
