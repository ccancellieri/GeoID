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

"""Unit tests for ``_register_cascade_owners`` (#1469).

The helper at ``dynastore.modules.catalog.catalog_module._register_cascade_owners``
is the only path that wires cascade owners into the registry at startup.
Missed registrations are leak hazards because they cause cleanup work to
be silently skipped on catalog hard-delete. These tests pin the three
distinct outcomes the helper must surface:

1. Module not installed (``ModuleNotFoundError``) → DEBUG, no ERROR. This is
   the expected SCOPE-pruning case and must NOT alert.
2. Module installed but import raises → ERROR ``cascade_owner_import_failed``.
3. Module imports cleanly but ``register_owners`` raises → ERROR
   ``cascade_owner_registration_failed``.

All three outcomes must continue past the failing module so a single bad
owner cannot block the others from registering.
"""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import pytest

from dynastore.modules.catalog.catalog_module import _register_cascade_owners


def _install_fake_module(name: str, **attrs: object) -> types.ModuleType:
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


def _uninstall(name: str) -> None:
    sys.modules.pop(name, None)


class TestRegisterCascadeOwners:
    def test_missing_module_logs_debug_not_error(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        registry = MagicMock()
        with caplog.at_level("DEBUG", logger="dynastore.modules.catalog.catalog_module"):
            _register_cascade_owners(
                registry,
                [("dynastore._absolutely_not_a_real_module_1469", "fake-extension")],
            )

        assert not any(r.levelname == "ERROR" for r in caplog.records), (
            "ModuleNotFoundError must NOT emit ERROR — extensions that are "
            "not in this SCOPE are expected to be absent."
        )
        registry.register.assert_not_called()

    def test_register_owners_failure_emits_error(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        modname = "dynastore._test_cascade_owner_module_1469_a"

        def _boom(_registry: object) -> None:
            raise RuntimeError("owner construction blew up")

        _install_fake_module(modname, register_owners=_boom)
        try:
            registry = MagicMock()
            with caplog.at_level("ERROR", logger="dynastore.modules.catalog.catalog_module"):
                _register_cascade_owners(registry, [(modname, "test-broken-register")])
        finally:
            _uninstall(modname)

        errors = [r for r in caplog.records if r.levelname == "ERROR"]
        assert errors, "register_owners raise must emit ERROR"
        msg = errors[0].getMessage()
        assert "cascade_owner_registration_failed" in msg
        assert modname in msg
        assert "test-broken-register" in msg
        assert "LEAK" in msg

    def test_import_failure_emits_error(
        self, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import importlib

        modname = "dynastore._test_cascade_owner_module_1469_b"
        original = importlib.import_module

        def _flaky_import(name, *a, **kw):  # type: ignore[no-untyped-def]
            if name == modname:
                raise RuntimeError("circular import or side-effect failure")
            return original(name, *a, **kw)

        registry = MagicMock()
        monkeypatch.setattr(importlib, "import_module", _flaky_import)
        with caplog.at_level("ERROR", logger="dynastore.modules.catalog.catalog_module"):
            _register_cascade_owners(registry, [(modname, "test-broken-import")])

        errors = [r for r in caplog.records if r.levelname == "ERROR"]
        assert errors, "import-time raise (non-ModuleNotFoundError) must emit ERROR"
        msg = errors[0].getMessage()
        assert "cascade_owner_import_failed" in msg
        assert modname in msg
        assert "test-broken-import" in msg
        assert "LEAK" in msg

    def test_single_failure_does_not_block_other_owners(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        bad = "dynastore._test_cascade_owner_module_1469_bad"
        good = "dynastore._test_cascade_owner_module_1469_good"

        good_register_calls: list[object] = []

        def _good_register(reg: object) -> None:
            good_register_calls.append(reg)

        def _bad_register(_reg: object) -> None:
            raise RuntimeError("nope")

        _install_fake_module(bad, register_owners=_bad_register)
        _install_fake_module(good, register_owners=_good_register)
        try:
            registry = MagicMock()
            with caplog.at_level("ERROR", logger="dynastore.modules.catalog.catalog_module"):
                _register_cascade_owners(
                    registry,
                    [(bad, "bad-owner"), (good, "good-owner")],
                )
        finally:
            _uninstall(bad)
            _uninstall(good)

        assert good_register_calls == [registry], (
            "good owner must still be registered after bad owner fails — "
            "otherwise a single broken extension brings down every cleanup path"
        )
