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

"""On a slim SCOPE (e.g. maps) the STAC extension isn't installed, so
``SidecarRegistry._ensure_defaults`` hits an ``ImportError`` for
'stac_metadata'. That is by design (#1138) — but ``_ensure_defaults`` runs on
essentially every engine/config snapshot rebuild (per catalog, per rebuild,
per instance boot), so re-attempting the import and re-logging the same
"not installed" fact every time buries real regressions under noise (#2988).

These tests simulate the not-installed condition by pointing the submodule's
entry in ``sys.modules`` at ``None`` (the interpreter's own signal that an
import must fail with ``ImportError`` without re-running the module), and
assert the miss is logged exactly once no matter how many times
``_ensure_defaults`` is subsequently invoked.

On a slim SCOPE the extension PACKAGE itself is absent, so the actual
production signature is ``ModuleNotFoundError`` with ``.name`` set to the
ancestor package (``dynastore.extensions.stac``), not the leaf sidecar
module — Python's import machinery names the deepest package/module it
failed to find, which for a wholly-missing package is that package, not
whatever leaf submodule the `from ... import` statement asked for. A
dedicated test below reproduces that exact signature to guard against
misclassifying it as transitive-dependency breakage.

A separate test covers the opposite case: the extension module itself IS
installed but some OTHER module it imports internally is broken. That
surfaces as a ``ModuleNotFoundError`` whose ``.name`` is not the sidecar
module or any of its ancestor packages, and must keep warning loudly (and
keep retrying) rather than being folded into the "not installed" cache —
otherwise a real regression could get silently swallowed as slim-image
tolerance.
"""

import logging
import sys

from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry

_STAC_ITEMS_SIDECAR_MODULE = "dynastore.extensions.stac.stac_items_sidecar"


class TestOptionalSidecarImportMissDedup:
    def setup_method(self):
        SidecarRegistry.clear_registry()

    def teardown_method(self):
        SidecarRegistry.clear_registry()

    def test_missing_module_logged_once_across_repeated_rebuilds(
        self, monkeypatch, caplog
    ):
        monkeypatch.setitem(sys.modules, _STAC_ITEMS_SIDECAR_MODULE, None)

        with caplog.at_level(
            logging.INFO,
            logger="dynastore.modules.storage.drivers.pg_sidecars.registry",
        ):
            # Simulates several engine/config snapshot rebuilds within the
            # same process, e.g. multiple catalogs each triggering their own
            # rebuild.
            for _ in range(5):
                SidecarRegistry._ensure_defaults()

        not_available_records = [
            r for r in caplog.records if "not available" in r.message
        ]
        assert len(not_available_records) == 1
        assert "stac_metadata" not in SidecarRegistry._registry
        assert "stac_metadata" in SidecarRegistry._unavailable_optional_sidecars

    def test_clear_registry_resets_the_miss_cache(self, monkeypatch):
        monkeypatch.setitem(sys.modules, _STAC_ITEMS_SIDECAR_MODULE, None)
        SidecarRegistry._ensure_defaults()
        assert "stac_metadata" in SidecarRegistry._unavailable_optional_sidecars

        SidecarRegistry.clear_registry()

        assert SidecarRegistry._unavailable_optional_sidecars == set()

    def test_module_available_registers_normally_without_caching_a_miss(self):
        # Sanity check with the real STAC extension importable, as it is in
        # this monorepo's test environment: no miss should ever be cached.
        SidecarRegistry._ensure_defaults()

        assert "stac_metadata" in SidecarRegistry._registry
        assert "stac_metadata" not in SidecarRegistry._unavailable_optional_sidecars

    def test_missing_parent_package_logged_once_and_cached(self, monkeypatch, caplog):
        # Reproduces the actual production signature (#2988): on a slim
        # SCOPE the `dynastore.extensions.stac` package itself was never
        # installed, so the import fails with `ModuleNotFoundError` whose
        # `.name` is that ANCESTOR package, not the leaf sidecar module.
        # This must still be classified as "genuinely missing", cached, and
        # logged exactly once at INFO across repeated rebuilds.
        def _missing_package_import(name, *args, **kwargs):
            if name == _STAC_ITEMS_SIDECAR_MODULE:
                raise ModuleNotFoundError(
                    "No module named 'dynastore.extensions.stac'",
                    name="dynastore.extensions.stac",
                )
            return real_import(name, *args, **kwargs)

        import builtins

        real_import = builtins.__import__
        monkeypatch.setattr(builtins, "__import__", _missing_package_import)

        with caplog.at_level(
            logging.INFO,
            logger="dynastore.modules.storage.drivers.pg_sidecars.registry",
        ):
            for _ in range(5):
                SidecarRegistry._ensure_defaults()

        not_available_records = [
            r for r in caplog.records if "not available" in r.message
        ]
        assert len(not_available_records) == 1
        assert "stac_metadata" not in SidecarRegistry._registry
        assert "stac_metadata" in SidecarRegistry._unavailable_optional_sidecars

    def test_transitive_dep_missing_warns_and_is_not_cached(self, monkeypatch, caplog):
        # The sidecar module itself is installed, but importing it raises
        # ModuleNotFoundError for some OTHER module (a broken transitive
        # dependency), not for `_STAC_ITEMS_SIDECAR_MODULE` itself. That is a
        # real defect signal, so it must warn (not info) and must not be
        # cached into `_unavailable_optional_sidecars`.
        def _broken_import(name, *args, **kwargs):
            if name == _STAC_ITEMS_SIDECAR_MODULE:
                raise ModuleNotFoundError(
                    "No module named 'some_broken_transitive_dep'",
                    name="some_broken_transitive_dep",
                )
            return real_import(name, *args, **kwargs)

        import builtins

        real_import = builtins.__import__
        monkeypatch.setattr(builtins, "__import__", _broken_import)

        with caplog.at_level(
            logging.WARNING,
            logger="dynastore.modules.storage.drivers.pg_sidecars.registry",
        ):
            for _ in range(3):
                SidecarRegistry._ensure_defaults()

        warning_records = [
            r for r in caplog.records if r.levelno == logging.WARNING
        ]
        assert len(warning_records) == 3
        assert "stac_metadata" not in SidecarRegistry._registry
        assert "stac_metadata" not in SidecarRegistry._unavailable_optional_sidecars
