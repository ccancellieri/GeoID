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

"""``PluginConfig`` base class and the class-registry resolution helpers.

This leaf module holds only what a config *declaration* needs: the
``PluginConfig`` base, its module-level apply-handler registry, and the
``resolve_config_class`` / ``require_config_class`` /
``list_registered_configs`` helpers built on ``TypedModelRegistry``.

It is kept dependency-light on purpose.  ``PluginConfig`` previously
lived in ``platform_config_service`` alongside the DB-facing service,
which imports the ``models.protocols`` eager hub — so any
Protocol-contracts module that subclasses ``PluginConfig`` (e.g.
``models/protocols/authorization.py`` housing ``IamRolesConfig``) pulled
the whole heavy stack into a load-order cycle.  Splitting the base out
into this leaf breaks that cycle structurally — see #686.  The marker
types live in the sibling leaf ``dynastore.models.mutability``.
"""

from typing import Any, Callable, ClassVar, Dict, List, Optional, Tuple, Type, Union

from dynastore.tools.typed_store import PersistentModel
from dynastore.tools.typed_store.registry import TypedModelRegistry


# Module-level apply-handler registry keyed by PluginConfig subclass.
# Populated exclusively by ``PluginConfig.register_apply_handler(cls, handler)``
# called imperatively at module-import time.  The legacy
# ``_on_apply: ClassVar`` declaration pattern was retired in Phase 1.5
# (it was a single-handler-only convenience that didn't compose; the
# imperative call supports multiple handlers per class natively).
_APPLY_HANDLERS: Dict[Type["PluginConfig"], List[Callable[..., Any]]] = {}

# Module-level validate-handler registry — mirrors ``_APPLY_HANDLERS``.
# Validate handlers run **pre-persist** (before the upsert, inside the same
# transaction); their exceptions PROPAGATE so the API returns 4xx and the
# upsert is rolled back.  Apply handlers run post-persist and are best-
# effort (log + swallow).  See #738 — putting validation in apply handlers
# wrapped in a blanket ``except Exception: logger.error(...)`` silently
# persists invalid configs and returns 200.  Same callable signature as
# apply handlers: ``(config, catalog_id, collection_id, db_resource) -> None``,
# sync or async.
_VALIDATE_HANDLERS: Dict[Type["PluginConfig"], List[Callable[..., Any]]] = {}


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

    @classmethod
    def register_validate_handler(
        cls, handler: Callable[..., Any]
    ) -> None:
        """Attach a validate-handler to this config class.

        Validate handlers run pre-persist, inside the config-write txn.
        Their exceptions PROPAGATE (and roll back the upsert) — unlike
        apply handlers, which are best-effort post-persist side effects.
        Use for content checks that need runtime discovery (driver
        registry, sibling-config cross-checks) and must surface as 4xx.
        Same signature as apply handlers — ``(config, catalog_id,
        collection_id, db_resource) -> None``, sync or async.
        """
        _VALIDATE_HANDLERS.setdefault(cls, []).append(handler)

    @classmethod
    def get_validate_handlers(cls) -> List[Callable[..., Any]]:
        return list(_VALIDATE_HANDLERS.get(cls, []))


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
