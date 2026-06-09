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

"""Pin the validate-handler registry — sibling to the apply registry (#738).

Background: putting *validation* logic in ``register_apply_handler`` handlers
made validation errors invisible because the 6 apply loops in
``config_service.py`` / ``platform_config_service.py`` swallow exceptions
post-persist.  The fix introduces a parallel ``register_validate_handler`` /
``get_validate_handlers`` registry on ``PluginConfig``; validate handlers
run **pre-persist** inside the same txn and their exceptions propagate
(rolling back the upsert) so the API returns a real 4xx.

These tests pin:
- The new module-level ``_VALIDATE_HANDLERS`` dict exists and is **distinct**
  from ``_APPLY_HANDLERS`` — registering on one does not leak into the other.
- ``register_validate_handler`` appends per-class; ``get_validate_handlers``
  returns a fresh list copy so caller mutation doesn't corrupt the registry.
- Multi-handler registration preserves insertion order.
"""

from __future__ import annotations

from typing import ClassVar, Optional, Tuple

from dynastore.models.plugin_config import PluginConfig, _APPLY_HANDLERS, _VALIDATE_HANDLERS


# Module-level concrete subclasses so Pydantic's metaclass picks up the
# ClassVar annotation correctly (dynamic ``type(name, bases, ns)`` fails
# the ``__init_subclass__`` ``_address`` check because the annotation
# doesn't survive the metaclass round-trip).  Each test cleans up its
# own ``_VALIDATE_HANDLERS`` / ``_APPLY_HANDLERS`` entries so the
# registries stay isolated across tests.


class _IsolatedConfig(PluginConfig):
    _address: ClassVar[Tuple[Optional[str], ...]] = (
        "test", "validate_handlers", "isolated",
    )


class _OrderConfig(PluginConfig):
    _address: ClassVar[Tuple[Optional[str], ...]] = (
        "test", "validate_handlers", "order",
    )


class _CopyConfig(PluginConfig):
    _address: ClassVar[Tuple[Optional[str], ...]] = (
        "test", "validate_handlers", "copy",
    )


class _UnregisteredConfig(PluginConfig):
    _address: ClassVar[Tuple[Optional[str], ...]] = (
        "test", "validate_handlers", "unregistered",
    )


def test_validate_registry_is_separate_from_apply_registry():
    def _validator(config, cat, col, conn) -> None: ...
    def _applier(config, cat, col, conn) -> None: ...

    try:
        _IsolatedConfig.register_validate_handler(_validator)
        _IsolatedConfig.register_apply_handler(_applier)

        assert _IsolatedConfig.get_validate_handlers() == [_validator]
        assert _IsolatedConfig.get_apply_handlers() == [_applier]
        # Cross-leakage check: registering on one bucket must not show up
        # in the other — this is the #738 regression that motivated the
        # split in the first place.
        assert _validator not in _IsolatedConfig.get_apply_handlers()
        assert _applier not in _IsolatedConfig.get_validate_handlers()
    finally:
        _VALIDATE_HANDLERS.pop(_IsolatedConfig, None)
        _APPLY_HANDLERS.pop(_IsolatedConfig, None)


def test_register_validate_handler_appends_in_order():
    def _v1(config, cat, col, conn) -> None: ...
    def _v2(config, cat, col, conn) -> None: ...
    def _v3(config, cat, col, conn) -> None: ...

    try:
        _OrderConfig.register_validate_handler(_v1)
        _OrderConfig.register_validate_handler(_v2)
        _OrderConfig.register_validate_handler(_v3)
        assert _OrderConfig.get_validate_handlers() == [_v1, _v2, _v3]
    finally:
        _VALIDATE_HANDLERS.pop(_OrderConfig, None)


def test_get_validate_handlers_returns_fresh_copy():
    """Caller mutation of the returned list must not corrupt the registry."""
    def _v(config, cat, col, conn) -> None: ...

    try:
        _CopyConfig.register_validate_handler(_v)
        snapshot = _CopyConfig.get_validate_handlers()
        snapshot.clear()
        assert _CopyConfig.get_validate_handlers() == [_v]
    finally:
        _VALIDATE_HANDLERS.pop(_CopyConfig, None)


def test_get_validate_handlers_empty_for_unregistered_class():
    assert _UnregisteredConfig.get_validate_handlers() == []
