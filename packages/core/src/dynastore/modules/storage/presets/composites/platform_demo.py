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

"""``platform_demo`` composite preset.

Applies every default extension preset in dependency order, reproducing the
historical all-extensions-on platform shape in one POST.  Intended for fresh
installs that want the full default behaviour without individually composing
each preset.

Order rationale:
  1. ``default_roles_baseline`` — roles must exist before policy bindings
     reference them.
  2. ``iam_baseline`` — IAM service policies referencing the seeded roles.
  3. Routing preset ``defaults_postgres`` — global PG-only storage defaults.
  4. Per-extension presets in dependency order (web/admin last so the UI can
     reflect the other extensions).

PR-4 of umbrella #1412.
"""
from __future__ import annotations

from typing import ClassVar, Tuple, Type

from pydantic import BaseModel

from dynastore.modules.storage.presets.preset import CompositePreset, NoParams
from dynastore.modules.storage.presets.protocol import PresetTier

# Ordered list of child preset names.  IAM-related names (default_roles_baseline,
# iam_baseline) are optional — if the IAM extension is not installed those presets
# will not be in the registry and registration of this composite must be skipped.
# The composites/__init__.py handles the graceful-degradation wrapper.
_COMPOSE: Tuple[str, ...] = (
    "default_roles_baseline",
    "iam_baseline",
    "defaults_postgres",
    "stac_enable",
    "records_enable",
    "coverages_enable",
    "edr_enable",
    "dggs_enable",
    "events_enable",
    "joins_enable",
    "logs_enable",
    "maps_enable",
    "stats_enable",
    "web_enable",
    "admin_enable",
)


class PlatformDemo(CompositePreset):
    """Apply every default extension preset in one POST.

    Reproduces today's all-extensions-on platform shape for fresh installs
    that want the historical default behaviour with a single operator action.
    Lifecycle (apply / revoke / dry_run) is inherited from ``CompositePreset``.
    """

    name: ClassVar[str] = "platform_demo"
    description: ClassVar[str] = (
        "Apply every default extension preset — reproduces today's all-on "
        "platform shape in one POST."
    )
    keywords: ClassVar[Tuple[str, ...]] = ("composite", "demo", "platform", "all-extensions")
    tier: ClassVar[PresetTier] = PresetTier.PLATFORM
    catalog_scopable: ClassVar[bool] = False
    params_model: ClassVar[Type[BaseModel]] = NoParams
    compose: ClassVar[Tuple[str, ...]] = _COMPOSE
