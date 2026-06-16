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

"""Neutral platform preset framework.

This package is the platform-level home for preset infrastructure. It has
no IAM or storage-driver imports — those modules import *from* here.

Public surface:

* ``PresetProtocol``         — structural protocol every preset satisfies.
* ``bootstrap_preset_if_absent`` — cold-boot single-application helper.
* ``load_preset_params``     — load JSON params from
  ``${DYNASTORE_CONFIG_ROOT}/presets/<name>.json``.
* ``PRESETS_DIR``            — resolved path (``CONFIG_ROOT / "presets"``).
* ``ColdBootContributor``    — structural protocol for cold-boot contributors.
* ``register_cold_boot_contributor`` — register a contributor for cold-boot.
* ``get_cold_boot_contributors``     — return contributors sorted by priority desc.
* ``run_cold_boot``                  — run all contributors (fail-soft per contributor).
* ``FilePresetSeederContributor``    — file-backed preset seeder (priority=10).

The registry (``register_preset`` / ``get_preset`` / ``find_preset`` /
``list_presets``) lives in ``modules/storage/presets/registry.py`` and is
re-exported here for convenience.  IAM and storage modules each import the
registry from there; this package provides the unified public alias.
"""
from .protocol import PresetProtocol  # noqa: F401
from .bootstrap import bootstrap_preset_if_absent  # noqa: F401
from .param_loader import load_preset_params, PRESETS_DIR  # noqa: F401
from .cold_boot import (  # noqa: F401
    ColdBootContributor,
    register_cold_boot_contributor,
    get_cold_boot_contributors,
    run_cold_boot,
)
from .preset_seeder import FilePresetSeederContributor  # noqa: F401

# Re-export the shared registry so callers can import from one place.
from dynastore.modules.storage.presets.registry import (  # noqa: F401
    find_preset,
    get_preset,
    list_presets,
    register_preset,
    search_presets,
)

# Auto-register the built-in file-preset seeder so dropping a
# ``${DYNASTORE_CONFIG_ROOT}/presets/<name>.json`` payload is applied on
# cold boot with no per-deployment wiring — the same "drop a file" ergonomics
# as the ``defaults/`` config seeder. This is the one always-on generic
# contributor (priority=10, runs after the foundational auth/role presets).
# It only *executes* via ``run_cold_boot`` on the request-serving app, so
# worker-only entrypoints never trigger it. Guarded so a re-import (e.g. in a
# test that does not clear the registry) is a harmless no-op rather than a
# duplicate-name ValueError.
try:
    register_cold_boot_contributor(FilePresetSeederContributor())
except ValueError:
    pass

__all__ = [
    "PresetProtocol",
    "bootstrap_preset_if_absent",
    "load_preset_params",
    "PRESETS_DIR",
    "ColdBootContributor",
    "register_cold_boot_contributor",
    "get_cold_boot_contributors",
    "run_cold_boot",
    "FilePresetSeederContributor",
    "find_preset",
    "get_preset",
    "list_presets",
    "register_preset",
    "search_presets",
]
