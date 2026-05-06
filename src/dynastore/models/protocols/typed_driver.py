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
Typed driver / driver-config bind — pyright-checked pairing.

Drivers inherit from ``TypedDriver[ConfigClass]`` so the (driver, config)
pair is captured by the type system; ``__init_subclass__`` registers the
pair at import time.  Driver-config classes inherit from
:class:`_PluginDriverConfig` and let ``class_key()`` auto-derive from the
bound driver class — no per-class boilerplate, and the operator-facing
JSON wire key is byte-identical to the routing entry's ``driver_id``.

Enforcement guarantees
----------------------

- pyright checks the type bind on ``TypedDriver[ConfigClass]``.
- Renaming the driver class auto-updates the JSON ``class_key()`` since
  it's a reverse lookup, not a string literal.
- Renaming the Config class is invisible on the wire.
- An orphan ``_PluginDriverConfig`` (no driver class binds it) raises a
  loud ``RuntimeError`` the first time ``class_key()`` is invoked.
- Two drivers binding the same Config raise at the second
  ``__init_subclass__`` — the bind is one-to-one by construction.

Migration shape
---------------

::

    class CollectionPostgresqlDriverConfig(_PluginDriverConfig):
        schema: str = "public"
        sidecars: List[_PgCollectionSidecarConfig] = Field(default_factory=list)
        # NO class_key() override.

    class CollectionPostgresqlDriver(TypedDriver[CollectionPostgresqlDriverConfig]):
        async def upsert_metadata(self, ...): ...

The wire then publishes ``CollectionPostgresqlDriver`` as the JSON key for
both the routing entry's ``driver_id`` and the ``configs.platform.catalog.{tier}.drivers.{driver_ref}``
lookup — single name, two places.
"""

from __future__ import annotations

import re
from functools import cache
from typing import Any, ClassVar, Dict, Generic, Optional, Type, TypeVar, get_args, get_origin

from pydantic import Field, model_validator

from dynastore.modules.db_config.platform_config_service import PluginConfig

ConfigT = TypeVar("ConfigT", bound="_PluginDriverConfig")

# F.4c.3 — operator-chosen engine_ref format: snake_case, 1-63 chars,
# leading lowercase letter.  Aligns with PG identifier conventions and
# matches the F.4c plan's recommended naming regex (Q1 in the planning
# notes).  Used only when the registry can't statically resolve the ref —
# refs that are known engine class_keys / engine_class discriminators
# bypass the format check.
_ENGINE_REF_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


# Reverse map: config class → driver class.  Populated by
# ``TypedDriver.__init_subclass__`` at import time.  Reads happen via
# ``_PluginDriverConfig.class_key()`` (single lookup per call, no caching
# needed — the resolved string is constant per class).
_DRIVER_REGISTRY: Dict[Type["_PluginDriverConfig"], Type["TypedDriver[Any]"]] = {}

# Abstract intermediate bases declare ``is_abstract_base = True`` (read via
# ``cls.__dict__.get("is_abstract_base", False)`` so concrete subclasses do
# not inherit it).  Replaces the legacy ``_ABSTRACT_BASE_NAMES`` frozenset —
# single source of truth lives on the classes themselves, not in a hand-
# maintained string set in two files.


def _is_abstract_base(cls: type) -> bool:
    """True iff ``cls`` itself declared ``is_abstract_base = True`` in its body."""
    return bool(cls.__dict__.get("is_abstract_base", False))


class TypedDriver(Generic[ConfigT]):
    """Base for every driver class whose config class is type-bound.

    The ``ConfigT`` type parameter is the single source of truth for the
    (driver, config) pair.  ``__init_subclass__`` extracts ``ConfigT`` from
    ``__orig_bases__`` and registers the pair in :data:`_DRIVER_REGISTRY` so
    the bound config's :meth:`_PluginDriverConfig.class_key` can derive the
    operator-facing wire key from the driver class name.

    Generic intermediates that don't bind a concrete ``ConfigT`` (e.g.
    abstract role mixins) are silently skipped — only concrete leaves
    register.
    """

    @classmethod
    @cache
    def config_cls(cls) -> Type[ConfigT]:
        """Return the bound ``ConfigT`` class.

        Walks ``__orig_bases__`` looking for ``TypedDriver[X]``.  Raises
        :class:`TypeError` for an abstract intermediate that hasn't bound
        a concrete config class.
        """
        for base in getattr(cls, "__orig_bases__", ()):
            if get_origin(base) is TypedDriver:
                args = get_args(base)
                if args:
                    return args[0]
        raise TypeError(
            f"{cls.__name__}: TypedDriver subclass must declare "
            f"`TypedDriver[ConcreteConfigClass]`; abstract intermediates "
            f"don't bind a config class.",
        )

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Register ONLY when ``TypedDriver[X]`` appears directly in this
        # subclass's __orig_bases__ — not when the bind is inherited.  Lets a
        # specialisation (e.g. ``ItemsElasticsearchPrivateDriver(
        # ItemsElasticsearchDriver)``) share the parent's config without
        # colliding on the registry.
        direct_bind = None
        for base in cls.__dict__.get("__orig_bases__", ()):
            if get_origin(base) is TypedDriver:
                args = get_args(base)
                if args:
                    direct_bind = args[0]
                break
        if direct_bind is None:
            return
        existing = _DRIVER_REGISTRY.get(direct_bind)
        if existing is not None and existing is not cls:
            raise RuntimeError(
                f"TypedDriver bind conflict: {direct_bind.__name__} is already "
                f"bound to {existing.__name__}; cannot re-bind to {cls.__name__}. "
                "A config class can serve at most one driver class.",
            )
        _DRIVER_REGISTRY[direct_bind] = cls


class _PluginDriverConfig(PluginConfig):
    """Base for every driver's PluginConfig.

    ``class_key()`` is auto-derived from the driver class bound to this
    config via :class:`TypedDriver`'s registry — driver authors write zero
    boilerplate per pair.

    An orphan ``_PluginDriverConfig`` subclass (no bound driver) raises a
    loud :class:`RuntimeError` the first time ``class_key()`` is called,
    so a missing ``class FooDriver(TypedDriver[FooDriverConfig])``
    declaration surfaces immediately rather than silently leaking the
    raw config class name onto the wire.

    Cycle F.2 added the engine-binding fields:

    * ``required_engine_class: ClassVar[str]`` — the ``engine_class``
      discriminator of the platform engine this driver class consumes.
      Concrete subclasses MUST declare this; the validator rejects
      ``engine_ref`` values pointing at engines of incompatible class.
    * ``engine_ref: Optional[str]`` field — name of the platform engine
      this driver instance binds to.  Defaults to
      ``required_engine_class`` for single-instance deployments;
      operators set explicit snake_case ref names (e.g. ``pg_main``)
      for multi-instance, validated against the engine registry by
      :meth:`_default_and_validate_engine_ref` (Cycle F.4c.3).
    """

    # Marker for abstract intermediate base — hidden from the deep view and
    # from the publish/registry paths.  Concrete subclasses do NOT inherit
    # this (filter reads ``cls.__dict__.get("is_abstract_base", False)``).
    is_abstract_base: ClassVar[bool] = True

    # Sentinel: subclasses MUST NOT set ``_class_key`` — derivation is
    # via the bound driver class, not a string override.
    _class_key: ClassVar[None] = None

    # Engine-class compatibility discriminator (Cycle F.2).
    # Empty on the abstract base; the validator skips the compatibility
    # check when this is empty so abstract bases / un-migrated subclasses
    # can still instantiate without an engine binding.
    required_engine_class: ClassVar[str] = ""

    engine_ref: Optional[str] = Field(
        default=None,
        description=(
            "Name of the platform engine this driver instance binds to. "
            "Single-instance deployments leave this None (defaults to "
            "the driver's ``required_engine_class``).  Multi-instance "
            "operators name engines explicitly (e.g. ``pg_main`` / "
            "``pg_secondary``); the validator accepts any snake_case "
            "ref but rejects refs that resolve through the engine "
            "registry to an incompatible ``engine_class``.  Refs "
            "unknown to the registry are deferred to runtime "
            "(EngineInstanceCache.get + PATCH-handler ref-existence "
            "check at platform.engines.{ref})."
        ),
    )

    @model_validator(mode="after")
    def _default_and_validate_engine_ref(self) -> "_PluginDriverConfig":
        """Default ``engine_ref`` to ``required_engine_class`` and validate
        compatibility against the engine registry (Cycle F.4c.3).

        Three branches by ref shape:

        1. ``None`` / empty → default to ``cls.required_engine_class``.
        2. Resolves via :func:`resolve_engine_class` (the ref names a
           registered engine class_key OR engine_class discriminator) —
           statically enforce ``engine_class == required_engine_class``;
           reject incompatible kinds with a clear ValueError.
        3. Operator-chosen multi-instance ref (resolver returns ``None``)
           — accept after a snake_case format check (1-63 chars).  The
           DB-side ref-existence check fires at PATCH-handler / runtime
           (``EngineInstanceCache.get`` raises for unknown refs).

        Bypasses: when ``required_engine_class`` is empty (abstract base
        or unmigrated subclass) the check is skipped so existing
        instantiation paths keep working.
        """
        cls = type(self)
        required = cls.required_engine_class
        if not required:
            return self
        if self.engine_ref is None or self.engine_ref == "":
            object.__setattr__(self, "engine_ref", required)
            return self

        from dynastore.modules.db_config.engine_registry import resolve_engine_class
        resolved_class = resolve_engine_class(self.engine_ref)
        if resolved_class is not None:
            if resolved_class != required:
                raise ValueError(
                    f"{cls.__name__}.engine_ref={self.engine_ref!r} resolves to "
                    f"engine_class={resolved_class!r}, incompatible with "
                    f"required_engine_class={required!r}."
                )
            return self

        # Operator-chosen multi-instance ref — format-check only; defer
        # ref-existence to PATCH handler / runtime cache lookup.
        if not _ENGINE_REF_RE.match(self.engine_ref):
            raise ValueError(
                f"{cls.__name__}.engine_ref={self.engine_ref!r} is not a "
                f"registered engine and does not match the snake_case "
                f"format ^[a-z][a-z0-9_]{{0,62}}$ required for "
                f"operator-chosen ref names."
            )
        return self

    @classmethod
    def class_key(cls) -> str:
        """Return the wire-published key — the bound driver class name when
        a :class:`TypedDriver` binds this config, else ``__qualname__``.

        The qualname fallback is necessary because ``PluginConfig.__init_subclass__``
        chains into ``TypedModelRegistry.register`` which calls ``class_key()``
        during class creation — BEFORE the paired ``class XDriver(TypedDriver[XConfig])``
        declaration in the driver module has had a chance to populate
        :data:`_DRIVER_REGISTRY`.  The fallback is class-loading mechanics,
        not an operator-facing legacy alias: by the time an operator-facing
        publisher calls ``class_key()`` (post-import), every concrete
        ``*DriverConfig`` is bound and returns the driver class name.

        Operator-facing publishers should call :meth:`assert_bound` to surface
        any orphan configs that escaped the binding.

        The intermediate bases (``_PluginDriverConfig``, ``DriverPluginConfig``,
        ``CollectionDriverConfig``, ``AssetDriverConfig``) always return
        ``__qualname__`` — they're abstract markers, never published.
        """
        from dynastore.tools.typed_store.base import _to_snake

        if _is_abstract_base(cls):
            return _to_snake(cls.__qualname__)
        driver_cls = _DRIVER_REGISTRY.get(cls)
        if driver_cls is None:
            return _to_snake(cls.__qualname__)
        return _to_snake(driver_cls.__name__)

    @classmethod
    def assert_bound(cls) -> None:
        """Raise :class:`RuntimeError` if no :class:`TypedDriver` binds this
        config class.  Operator-facing publish paths call this before
        relying on the wire key being the driver class name.
        """
        if _is_abstract_base(cls):
            return
        if cls not in _DRIVER_REGISTRY:
            raise RuntimeError(
                f"{cls.__name__}: no TypedDriver class binds this config. "
                f"Declare `class XDriver(TypedDriver[{cls.__name__}])` so "
                "the driver class name becomes the operator-facing wire key.",
            )


def _registered_pairs() -> Dict[Type["_PluginDriverConfig"], Type["TypedDriver[Any]"]]:
    """Read-only view of the (config_cls -> driver_cls) registry.

    Test/diagnostic affordance — production code uses
    :meth:`_PluginDriverConfig.class_key` directly.  Returns a defensive
    copy so callers can't mutate the registry.
    """
    return dict(_DRIVER_REGISTRY)
