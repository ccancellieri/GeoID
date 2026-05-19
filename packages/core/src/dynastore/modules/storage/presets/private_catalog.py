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

"""``private_catalog`` preset — PG-first storage + per-tenant private
Elasticsearch indexers on every tier (catalog / collection / items).

Composes the existing ``_build_private_*_routing`` helpers from
``catalog_config`` (added under #733 + #960 scope 2) so the preset
stays in lock-step with the seed defaults the cascade validators
already accept. No audience opt-ins; the items-private driver's
``private_deny_{catalog_id}`` policy gates anonymous reads on the
envelope and the items URL pattern.
"""
from __future__ import annotations

from dynastore.modules.catalog.catalog_config import (
    _build_private_catalog_routing,
    _build_private_collection_routing,
    _build_private_items_routing,
)

from typing import ClassVar

from .protocol import PresetBundle, PresetTier


class PrivateCatalogPreset:
    """PG-first storage + per-tenant private ES indexers on every tier."""

    name = "private_catalog"
    tier: ClassVar[PresetTier] = PresetTier.CATALOG
    description = (
        "PG-first storage + per-tenant private Elasticsearch indexers on "
        "catalog, collection, and items tiers. No anonymous read paths; "
        "IAM DENY (private_deny_{catalog_id}) blocks all_users on the "
        "envelope. No audience opt-ins. Use as the foundation for fully "
        "isolated tenant catalogs."
    )

    def build(self, catalog_id: str) -> PresetBundle:  # noqa: ARG002
        return PresetBundle(
            catalog_routing=_build_private_catalog_routing(),
            collection_template=_build_private_collection_routing(),
            items_template=_build_private_items_routing(),
            audience_configs={},
        )
