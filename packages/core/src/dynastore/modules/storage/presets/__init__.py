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

"""Named routing presets for one-call provisioning (#847, #972).

A preset is a thin factory that emits a ``PresetBundle`` of validated
routing configs + opt-in audience configs. Each preset declares a
``tier`` (``PresetTier``) selecting the admin URL family it is reachable
from; the URL encodes the apply scope:

* ``PLATFORM``   — ``/admin/presets/{name}`` (no scope)
* ``CATALOG``    — ``/admin/catalogs/{cat}/presets/{name}``
* ``COLLECTION`` — ``/admin/catalogs/{cat}/collections/{col}/presets/{name}``
* ``ITEMS`` / ``ASSETS`` — collection family always; catalog family when
  ``catalog_scopable=True``.

Each endpoint runs the bundle through the standard
``ConfigsProtocol.set_config`` lifecycle (no validation bypass) — the
privacy cascade (#960) catches mixed private/public combos. ``DELETE``
is the symmetric unapply path (#971) on the same resource URL.

Built-in presets are auto-registered on import (see imports below).
"""
from .protocol import (  # noqa: F401
    PresetBundle,
    PresetBundleEntry,
    PresetTier,
    RoutingPreset,
)
from .preset import (  # noqa: F401
    AppliedDescriptor,
    CompositePreset,
    NoParams,
    Preset,
    PresetContext,
    PresetPlan,
    PresetPlanEntry,
    TaskHandle,
)
from .registry import find_preset, get_preset, list_presets, register_preset, search_presets  # noqa: F401
from .routing_adapter import RoutingPresetAdapter  # noqa: F401

# Built-in presets — auto-register on import.
from .defaults_postgres import DefaultsPostgresPreset  # noqa: E402
from .demo_data import DEMO_DATA_PRESET  # noqa: E402
from .items_es_private import ItemsEsPrivatePreset  # noqa: E402
from .private_catalog import PrivateCatalogPreset  # noqa: E402
from .private_collection import PrivateCollectionPreset  # noqa: E402
from .public_catalog import PublicCatalogPreset  # noqa: E402

register_preset(PublicCatalogPreset())
register_preset(PrivateCatalogPreset())
register_preset(DefaultsPostgresPreset())
register_preset(PrivateCollectionPreset())
register_preset(ItemsEsPrivatePreset())
register_preset(DEMO_DATA_PRESET)

# Curated composite presets — imported after all routing presets so their
# children are already registered when the composites/__init__.py runs.
# The composites subpackage handles its own graceful-degradation: a missing
# child (e.g. IAM extension not installed) logs an info line and skips that
# composite without raising.
try:
    from . import composites as composites  # noqa: F401
except Exception:  # noqa: BLE001
    import logging as _logging
    _logging.getLogger(__name__).info(
        "presets.composites subpackage failed to import — no composites registered"
    )

__all__ = [
    "AppliedDescriptor",
    "CompositePreset",
    "DefaultsPostgresPreset",
    "ItemsEsPrivatePreset",
    "NoParams",
    "Preset",
    "PresetBundle",
    "PresetBundleEntry",
    "PresetContext",
    "PresetPlan",
    "PresetPlanEntry",
    "PresetTier",
    "PrivateCatalogPreset",
    "PrivateCollectionPreset",
    "PublicCatalogPreset",
    "RoutingPreset",
    "RoutingPresetAdapter",
    "TaskHandle",
    "find_preset",
    "get_preset",
    "list_presets",
    "register_preset",
    "search_presets",
]
