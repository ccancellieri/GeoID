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

"""``public_open_data`` composite preset.

Opinionated bundle for public-read catalog deployments.  Wires together
roles, IAM policies, public-catalog routing, STAC and web/admin interfaces
in the correct dependency order.

``catalog_scopable = True`` so operators may apply this at a catalog scope
to flip an existing catalog to the public-open shape.

PR-4 of umbrella #1412.
"""
from __future__ import annotations

from typing import ClassVar, Tuple, Type

from pydantic import BaseModel

from dynastore.modules.storage.presets.preset import CompositePreset, NoParams
from dynastore.modules.storage.presets.protocol import PresetTier

_COMPOSE: Tuple[str, ...] = (
    "default_roles_baseline",
    "iam_baseline",
    "public_catalog",
    "stac_enable",
    "web_enable",
    "admin_enable",
)


class PublicOpenData(CompositePreset):
    """Opinionated bundle: public-read catalog with STAC for open-data deployments.

    Applies role seed, IAM platform policies, public-catalog routing, and
    STAC + web/admin interfaces.  Lifecycle inherited from ``CompositePreset``.
    """

    name: ClassVar[str] = "public_open_data"
    description: ClassVar[str] = (
        "Opinionated bundle: public-read catalog with STAC + Features for "
        "open-data deployments."
    )
    keywords: ClassVar[Tuple[str, ...]] = ("composite", "public", "open-data", "stac", "features")
    tier: ClassVar[PresetTier] = PresetTier.PLATFORM
    catalog_scopable: ClassVar[bool] = True
    params_model: ClassVar[Type[BaseModel]] = NoParams
    compose: ClassVar[Tuple[str, ...]] = _COMPOSE
