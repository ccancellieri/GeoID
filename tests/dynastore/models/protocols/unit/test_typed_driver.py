"""Unit tests for the typed driver / driver-config bind machinery.

Pins the contracts of :class:`TypedDriver` and :class:`_PluginDriverConfig`:

- A ``TypedDriver[ConfigClass]`` declaration registers the (driver, config)
  pair in ``_DRIVER_REGISTRY`` at import time.
- ``_PluginDriverConfig.class_key()`` returns the bound driver class name.
- An orphan ``_PluginDriverConfig`` (no driver bound) raises a loud
  RuntimeError on first ``class_key()`` call.
- A second driver class binding the same config raises at the conflicting
  ``__init_subclass__`` invocation.
- Abstract intermediates (no concrete ``ConfigT`` bind) skip registration
  silently — only concrete leaves register.
"""

from __future__ import annotations

from typing import ClassVar, Optional, Tuple

import pytest

from dynastore.models.protocols.typed_driver import (
    TypedDriver,
    _PluginDriverConfig,
    _registered_pairs,
)


def test_concrete_pair_registers_and_class_key_resolves():
    class _DemoConfigA(_PluginDriverConfig):
        _address: ClassVar[Tuple[str, str, Optional[str]]] = ("platform", "catalog", "collection", "items", "drivers")
        nickname: str = "a"

    class _DemoDriverA(TypedDriver[_DemoConfigA]):
        pass

    # class_key() returns the driver class name in snake_case form.
    assert _DemoConfigA.class_key() == "_demo_driver_a"
    # config_cls() round-trips back to the bound config.
    assert _DemoDriverA.config_cls() is _DemoConfigA
    # Registry contains the pair.
    assert _registered_pairs()[_DemoConfigA] is _DemoDriverA


def test_orphan_config_raises_on_assert_bound():
    """Orphan check is in ``assert_bound``, not ``class_key`` — the latter
    has to fall back to ``__qualname__`` because ``TypedModelRegistry``
    calls ``class_key()`` from the parent's ``__init_subclass__`` chain
    before the driver declaration runs.
    """
    class _OrphanConfig(_PluginDriverConfig):
        # NO `class XDriver(TypedDriver[_OrphanConfig])` declaration.
        _address: ClassVar[Tuple[str, str, Optional[str]]] = ("platform", "catalog", "collection", "items", "drivers")
        nickname: str = "orphan"

    # class_key() falls back to qualname (non-fatal) — snake-cased.
    assert _OrphanConfig.class_key().endswith("_orphan_config")
    # assert_bound() is the operator-facing check that raises.
    with pytest.raises(RuntimeError, match="no TypedDriver class binds this config"):
        _OrphanConfig.assert_bound()


def test_double_bind_raises_on_second_driver():
    class _DemoConfigB(_PluginDriverConfig):
        _address: ClassVar[Tuple[str, str, Optional[str]]] = ("platform", "catalog", "collection", "items", "drivers")
        nickname: str = "b"

    class _DemoDriverB1(TypedDriver[_DemoConfigB]):
        pass

    with pytest.raises(RuntimeError, match="TypedDriver bind conflict"):
        class _DemoDriverB2(TypedDriver[_DemoConfigB]):
            pass


def test_abstract_intermediate_does_not_register():
    """A subclass of TypedDriver that doesn't bind a concrete ConfigT
    (e.g. an abstract role mixin) is silently skipped — only concrete
    leaves register their pair.
    """
    # Abstract intermediate — no concrete ConfigT bind.
    class _AbstractRole(TypedDriver):
        pass

    assert _AbstractRole not in _registered_pairs().values()


def test_class_key_byte_matches_driver_name():
    """The whole point: routing.WRITE[].driver_id and configs.drivers.{key}
    use the SAME string, so an operator copies once and the linker is the
    Python class name itself.
    """
    class _CollectionPostgresqlDriverConfig(_PluginDriverConfig):
        _address: ClassVar[Tuple[str, str, Optional[str]]] = ("platform", "catalog", "collection", "drivers")
        schema: str = "public"

    class _CollectionPostgresqlDriver(TypedDriver[_CollectionPostgresqlDriverConfig]):
        pass

    from dynastore.tools.typed_store.base import _to_snake
    expected = _to_snake(_CollectionPostgresqlDriver.__name__)
    assert _CollectionPostgresqlDriverConfig.class_key() == expected
    # Sanity: the Python class names of the (config, driver) pair only differ
    # by the trailing `Config` suffix (PascalCase convention preserved at the
    # Python layer; the wire identity goes through `_to_snake`).
    assert _CollectionPostgresqlDriver.__name__ + "Config" == _CollectionPostgresqlDriverConfig.__name__
