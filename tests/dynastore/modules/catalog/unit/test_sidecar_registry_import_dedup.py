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
