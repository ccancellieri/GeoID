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
    ClassVar,
    Dict,
    FrozenSet,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Callable,
    Union,
    get_origin,
    get_args,
    Annotated,
    List,
    TYPE_CHECKING,
    Tuple,
)

from pydantic.fields import FieldInfo
from dataclasses import dataclass, field as dc_field

from dynastore.tools.cache import cached
from dynastore.tools.typed_store import PersistentModel
from dynastore.tools.typed_store.registry import TypedModelRegistry

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
from dynastore.modules.db_config.exceptions import ImmutableConfigError
from dynastore.tools.json import CustomJSONEncoder

logger = logging.getLogger(__name__)

# --- Immutability Framework ---

T = TypeVar("T")


class _MutabilityKindMixin:
    """Shared ``__get_pydantic_json_schema__`` hook for all four markers.

    Each subclass declares ``kind: ClassVar[str]`` — one of ``"mutable"``,
    ``"write_once"``, ``"immutable"``, ``"computed"``.  The hook injects
    ``x-mutability: <kind>`` into the field's JSON Schema (#665 slice 4)
    and, for everything except ``Mutable``, ``readOnly: true`` (JSON
    Schema 2020-12 standard form-builders honour natively) + the legacy
    ``x-ui.readonly`` advisory the existing admin UI consumes.
    """

    kind: ClassVar[str] = ""

    @classmethod
    def __get_pydantic_json_schema__(cls, schema, handler):
        out = handler(schema)
        out["x-mutability"] = cls.kind
        if cls.kind != "mutable":
            out["readOnly"] = True
            from dynastore.tools.ui_hints import merge_ui
            out = merge_ui(out, readonly=True)
        return out


class MutableMarker(_MutabilityKindMixin):
    """Freely editable across the config's lifetime (the default kind)."""
    kind: ClassVar[str] = "mutable"


class ImmutableMarker(_MutabilityKindMixin):
    """Fixed at class definition; ``enforce_config_immutability`` rejects diffs."""
    kind: ClassVar[str] = "immutable"


class WriteOnceMarker(_MutabilityKindMixin):
    """Settable once from ``None`` → value, locked thereafter."""
    kind: ClassVar[str] = "write_once"


class ComputedMarker(_MutabilityKindMixin):
    """Derived value with no stored override.  Prefer ``@computed_field`` when no value is stored."""
    kind: ClassVar[str] = "computed"


if TYPE_CHECKING:
    type Mutable[T] = T  # pyright-transparent alias
    type WriteOnce[T] = T
    type Immutable[T] = T
    type Computed[T] = T
else:
    class Mutable:
        """Marker for freely-editable fields.

        Usage::

            field: Mutable[int] = Field(default=0, description="...")

        Equivalent to ``field: Annotated[int, MutableMarker] = Field(...)``.
        """

        def __class_getitem__(cls, item: Any) -> Any:
            return Annotated[item, MutableMarker]

    class Immutable:
        """Marker for class-definition-fixed fields.

        Usage::

            field: Immutable[int] = Field(...)
        """

        def __class_getitem__(cls, item: Any) -> Any:
            return Annotated[item, ImmutableMarker]

    class WriteOnce:
        """Marker for write-once fields (``None`` → value transition allowed,
        non-``None`` → anything rejected).

        Usage::

            field: WriteOnce[Optional[str]] = Field(default=None, ...)
        """

        def __class_getitem__(cls, item: Any) -> Any:
            return Annotated[item, WriteOnceMarker]

    class Computed:
        """Marker for derived fields with no stored override.

        Prefer ``@computed_field`` when no stored value is needed; this
        marker is for fields that ARE stored but recomputed externally.
        """

        def __class_getitem__(cls, item: Any) -> Any:
            return Annotated[item, ComputedMarker]


def is_immutable_field(field_info: "FieldInfo") -> bool:
    if get_origin(field_info.annotation) is Annotated:
        args = get_args(field_info.annotation)
        if ImmutableMarker in args or Immutable in args:
            return True
    if any(
        item is ImmutableMarker
        or item is Immutable
        or (isinstance(item, type) and issubclass(item, ImmutableMarker))
        for item in field_info.metadata
    ):
        return True
    return False


def is_write_once_field(field_info: "FieldInfo") -> bool:
    if get_origin(field_info.annotation) is Annotated:
        args = get_args(field_info.annotation)
        if WriteOnceMarker in args or WriteOnce in args:
            return True
    if any(
        item is WriteOnceMarker
        or item is WriteOnce
        or (isinstance(item, type) and issubclass(item, WriteOnceMarker))
        for item in field_info.metadata
    ):
        return True
    return False


def enforce_config_immutability(
    current_config: Optional["PluginConfig"], new_config: "PluginConfig"
) -> None:
    """Reject Immutable / WriteOnce field mutations."""
    if current_config is None:
        return
    model_class = type(current_config)
    if not isinstance(new_config, model_class):
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


# --- Protocols & Models ---

# Module-level apply-handler registry keyed by PluginConfig subclass.
# Populated exclusively by ``PluginConfig.register_apply_handler(cls, handler)``
# called imperatively at module-import time.  The legacy
# ``_on_apply: ClassVar`` declaration pattern was retired in Phase 1.5
# (it was a single-handler-only convenience that didn't compose; the
# imperative call supports multiple handlers per class natively).
_APPLY_HANDLERS: Dict[Type["PluginConfig"], List[Callable[..., Any]]] = {}


class PluginConfig(PersistentModel):
    """Base class for all mutable plugin configurations.

    Identity is the class itself.  The stable string form is ``cls.class_key()``
    — snake_case of ``cls.__name__`` (e.g. ``ItemsSchema`` →
    ``"items_schema"``); pin via ``_class_key: ClassVar[str]``.

    Subclasses must be instantiable without arguments — every field requires
    a default — so defaults can be materialised on demand.

    Abstract intermediate bases (``_PluginDriverConfig``, ``DriverPluginConfig``,
    ``CollectionDriverConfig``, ``AssetDriverConfig``) declare
    ``is_abstract_base = True`` so the composer / publisher filters can hide
    them.  The marker is read via ``cls.__dict__.get("is_abstract_base", False)``
    so concrete subclasses do NOT inherit the True value.
    """

    # NB: ``enabled: bool`` is NOT declared on the base.  It used to be an
    # always-true cargo field that polluted every config response; removed
    # in the Phase 0 cleanup.  Subclasses that need a per-scope kill-switch
    # mix in :class:`ExposableConfigMixin` (extension togglability) or
    # declare their own ``enabled: bool`` field with a tailored description
    # that is actually consumed at runtime (e.g. ``GcpCatalogBucketConfig.
    # enabled`` gates GCS provisioning; per-sidecar ``_PgSidecarConfig.
    # enabled`` gates apply-time activation).

    # Marker for abstract intermediate bases (not concrete configs). Read via
    # ``cls.__dict__.get("is_abstract_base", False)`` so concrete subclasses do
    # not inherit the True value from a base that set it.
    is_abstract_base: ClassVar[bool] = False

    # Explicit placement in the deep-view tree.  Currently 3-tuple
    # ``(scope, topic, sub)`` — e.g. ``("storage", "drivers", "items")`` or
    # ``("platform", "gcp", None)``.  Concrete subclasses MUST declare it;
    # the empty-tuple base sentinel triggers the ``__init_subclass__``
    # enforcement check via ``not addr``.
    #
    # Cycle D (pending) will widen this to variable-length
    # ``Tuple[str, ...]`` so the address can carry any depth (e.g. the
    # tier-first ``("platform", "catalog", "collection", "items", "policy")``
    # path).  Subclass annotations may stay narrower today and migrate
    # incrementally — the base type is intentionally permissive
    # (``Tuple[Optional[str], ...]``) to accept both the current 3-tuple
    # shape (with trailing ``None``) and the post-D variable-length shape.
    _address: ClassVar[Tuple[Optional[str], ...]] = ()

    # Optional scope-visibility filter:
    # - ``None`` (default) → visible at every scope (collection / catalog / platform).
    # - ``"collection"`` → only at collection scope.
    # - ``"catalog"`` → only at catalog and platform scopes (hidden at collection).
    _visibility: ClassVar[Optional[str]] = None

    # NB: ``_on_apply: ClassVar`` declaration pattern retired in Phase 1.5.
    # Concrete subclasses register apply handlers imperatively at module-
    # import time via ``MyConfig.register_apply_handler(my_handler_fn)``.
    # The imperative call supports multiple handlers per class (the
    # ClassVar pattern was single-handler-only) and removes the dual-
    # registration footgun where developers couldn't predict which path
    # was active.

    _priority: ClassVar[int] = 100

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Concrete subclasses must declare ``_address`` — abstract bases
        # opt out via ``is_abstract_base = True``.
        if not cls.__dict__.get("is_abstract_base", False):
            addr = cls.__dict__.get("_address")
            # Sentinel-shape independent: rejects the inherited empty-tuple
            # base default, the explicit retired ``("", "", None)`` form, AND
            # the absence-of-declaration case in one check.  Decouples the
            # validation from the address-tuple shape so Cycle D's widening
            # to variable-length doesn't break the enforcement.
            if not addr:
                raise TypeError(
                    f"{cls.__module__}.{cls.__qualname__} is a concrete PluginConfig "
                    f"but does not declare ``_address``.  Declare e.g. "
                    f"``_address: ClassVar[Tuple[str, ...]] = (\"platform\", \"<topic>\", None)`` "
                    f"or mark it abstract via ``is_abstract_base = True``."
                )
            # Phase 1.5: catch developers who still declare the retired
            # ``_on_apply`` ClassVar pattern — surface a clear migration
            # message instead of silently dropping the handler.
            if "_on_apply" in cls.__dict__:
                raise TypeError(
                    f"{cls.__module__}.{cls.__qualname__} declares the retired "
                    f"``_on_apply: ClassVar`` pattern.  Migrate to the imperative "
                    f"``{cls.__qualname__}.register_apply_handler(handler_fn)`` "
                    f"call at module-import time (Phase 1.5 standardisation — "
                    f"single registration path, multi-handler support)."
                )

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        """Enforce per-field mutability markers (#665 slice 4).

        Pydantic invokes this hook AFTER ``model_fields`` is populated —
        the right moment to walk fields and verify each one carries one
        of the four mutability markers.  Abstract intermediate bases
        (``is_abstract_base = True``) are skipped: they don't render via
        the composed-config endpoint and may declare fields that
        subclasses re-annotate or override.

        Every concrete ``PluginConfig`` subclass MUST annotate each
        Pydantic field with exactly one of ``Mutable[T]`` /
        ``WriteOnce[T]`` / ``Immutable[T]`` / ``Computed[T]``.  The
        framework raises ``TypeError`` at class definition otherwise.
        The slice-4.x migration window's ``DYNASTORE_MUTABILITY_STRICT``
        env-var gate was removed in slice 4.z (#665 closed); every
        existing subclass is annotated.
        """
        super().__pydantic_init_subclass__(**kwargs)
        if cls.__dict__.get("is_abstract_base", False):
            return
        from dynastore.models.mutability import missing_markers
        missing = list(missing_markers(cls))
        if missing:
            raise TypeError(
                f"{cls.__module__}.{cls.__qualname__}: every Pydantic field must "
                f"carry exactly one mutability marker — "
                f"``Mutable[T]`` / ``WriteOnce[T]`` / ``Immutable[T]`` / "
                f"``Computed[T]`` from ``dynastore.models.mutability``.  "
                f"Fields missing a marker: {missing}.  See #665 slice 4."
            )
        # Install the WriteOnce setter guard: every field annotated
        # ``WriteOnce[T]`` rejects post-construction mutation.  The guard
        # is installed once at class creation; runtime construction sets
        # the initial value via Pydantic's normal init path (which does
        # not go through ``__setattr__``).
        from dynastore.models.mutability import mutability_map
        write_once_fields = frozenset(
            name for name, kind in mutability_map(cls).items()
            if kind == "write_once"
        )
        if write_once_fields:
            cls._write_once_fields = write_once_fields  # type: ignore[attr-defined]

    def __setattr__(self, name: str, value: Any) -> None:
        """Reject writes to ``WriteOnce`` fields after construction.

        ``__init_private_attributes__``-style internal writes happen via
        Pydantic's ``BaseModel.__init__`` path and never come through
        here; user-driven ``inst.field = value`` does.  The framework
        relies on Pydantic's existing field-validation + immutability
        machinery for ``Immutable`` (which it spells ``frozen=True`` on
        the field — set in the marker's ``__get_pydantic_json_schema__``
        is not enough; subclasses requiring runtime immutability declare
        ``model_config = ConfigDict(frozen=True)``).
        """
        write_once = getattr(type(self), "_write_once_fields", frozenset())
        if name in write_once and name in self.__dict__:
            raise AttributeError(
                f"{type(self).__qualname__}.{name} is WriteOnce — "
                f"locked after construction."
            )
        super().__setattr__(name, value)

    @classmethod
    def mutability_map(cls) -> Dict[str, str]:
        """Return ``{field_name: 'mutable'|'write_once'|'immutable'|'computed'}``.

        Implements ``MutabilityIntrospectionProtocol``; the composed-config
        renderer depends on the Protocol, not on this concrete method.
        """
        from dynastore.models.mutability import mutability_map as _mm
        return _mm(cls)

    @classmethod
    def register_apply_handler(
        cls, handler: Callable[..., Any]
    ) -> None:
        """Attach an additional apply-handler to this config class."""
        _APPLY_HANDLERS.setdefault(cls, []).append(handler)

    @classmethod
    def get_apply_handlers(cls) -> List[Callable[..., Any]]:
        return list(_APPLY_HANDLERS.get(cls, []))


def resolve_config_class(
    identifier: Union[str, Type[PluginConfig]],
) -> Optional[Type[PluginConfig]]:
    """Resolve a ``class_key`` string *or* :class:`PluginConfig` subclass to the class.

    Returns ``None`` when ``identifier`` is a string that does not match any
    registered class.
    """
    if isinstance(identifier, type):
        return identifier if issubclass(identifier, PluginConfig) else None
    cls = TypedModelRegistry.get(identifier)
    if cls is None or not issubclass(cls, PluginConfig):
        return None
    return cls


def require_config_class(
    identifier: Union[str, Type[PluginConfig]],
) -> Type[PluginConfig]:
    """Like :func:`resolve_config_class` but raises ``PluginNotRegisteredError``
    when the identifier is not known.  Used by endpoints and service methods
    to surface 404 at the edges.
    """
    cls = resolve_config_class(identifier)
    if cls is None:
        from dynastore.modules.db_config.exceptions import PluginNotRegisteredError
        raise PluginNotRegisteredError(
            f"No PluginConfig subclass registered under key {identifier!r}"
        )
    return cls


def list_registered_configs() -> Dict[str, Type[PluginConfig]]:
    """Return ``{class_key: class}`` for every registered PluginConfig subclass."""
    return {cls.class_key(): cls for cls in TypedModelRegistry.subclasses_of(PluginConfig)}


def _collect_required_fields(cls: Type[PluginConfig]) -> List[str]:
    """Return the names of fields declared without a default on ``cls``.

    Used by ``ConfigResolutionError`` to produce an actionable ops hint when
    a config cannot be instantiated with zero args at the end of the waterfall.
    """
    fields = getattr(cls, "model_fields", {}) or {}
    return [name for name, info in fields.items() if info.is_required()]


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
                    enforce_config_immutability(old_config, config)

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

            for apply_handler in cls.get_apply_handlers():
                try:
                    res = apply_handler(config, None, None, conn)
                    if inspect.isawaitable(res):
                        await res
                except Exception as e:
                    logger.error(
                        f"Failed to apply platform configuration for '{class_key}': {e}",
                        exc_info=True,
                    )

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
                    enforce_config_immutability(old_config, config)

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

            for apply_handler in cls.get_apply_handlers():
                try:
                    res = apply_handler(config, None, None, conn)
                    if inspect.isawaitable(res):
                        await res
                except Exception as e:
                    logger.error(
                        f"Failed to apply platform configuration for "
                        f"ref={ref_key!r} class={class_key!r}: {e}",
                        exc_info=True,
                    )

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
