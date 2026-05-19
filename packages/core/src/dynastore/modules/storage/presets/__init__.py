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

"""Named routing presets for one-call catalog provisioning (#847).

A preset is a thin factory that emits a ``PresetBundle`` of validated
routing configs + opt-in audience configs. The admin endpoint
``POST /admin/catalogs/{cat}/presets/{name}`` runs the bundle through the
standard ``ConfigsProtocol.set_config`` lifecycle; the catalog-tier
privacy cascade (#960 scope 4) catches mixed private/public combos.
``DELETE /admin/catalogs/{cat}/presets/{name}`` (#971) is the symmetric
unapply path on the same resource URL.

Built-in presets are auto-registered on import (see imports below).
"""
from .protocol import (  # noqa: F401
    PresetBundle,
    PresetBundleEntry,
    PresetTier,
    RoutingPreset,
)
from .registry import get_preset, list_presets, register_preset  # noqa: F401

# Built-in presets — auto-register on import.
from .private_catalog import PrivateCatalogPreset  # noqa: E402
from .public_catalog import PublicCatalogPreset  # noqa: E402

register_preset(PublicCatalogPreset())
register_preset(PrivateCatalogPreset())

__all__ = [
    "PresetBundle",
    "PresetBundleEntry",
    "PresetTier",
    "PrivateCatalogPreset",
    "PublicCatalogPreset",
    "RoutingPreset",
    "get_preset",
    "list_presets",
    "register_preset",
]
