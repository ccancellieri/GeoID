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

"""``geoid`` routing preset (#847) — flagship FAO GeoID profile.

Composes the ``private_catalog`` bundle (PG-first storage, private ES
indexers on every tier, IAM DENY on default routes) and opts the catalog
in to two anonymous audiences:

  * ``CatalogLookupAudience.is_public=True`` — opens the lookup-only
    search surface (``/search`` + ``/search/catalogs/{cat}``) that the
    ``lookup_only_search`` policy gates.
  * ``CollectionWriteAudience.allow_anonymous_create=True`` — opens the
    STAC + OGC Features item-POST surfaces that the
    ``collection_write_anonymous_allowed`` policy gates. Applied at the
    catalog tier; individual collections inherit unless they override.

The audience opt-ins are higher-priority ALLOW policies layered over the
``private_catalog`` DENY baseline (#915 priority), so anonymous traffic
reaches the explicitly-opened routes while everything else stays denied
by the private cascade.

Auto-registers on extension import.
"""
from __future__ import annotations

from dynastore.modules.iam.audience_configs import (
    CatalogLookupAudience,
    CollectionWriteAudience,
)
from typing import ClassVar

from dynastore.modules.storage.presets import get_preset, register_preset
from dynastore.modules.storage.presets.protocol import PresetBundle, PresetTier


class GeoidPreset:
    """Flagship FAO GeoID profile: private storage + anonymous lookup + anonymous write."""

    name = "geoid"
    tier: ClassVar[PresetTier] = PresetTier.CATALOG
    description = (
        "FAO GeoID flagship profile. Composes the private_catalog bundle "
        "(PG-first storage + per-tenant private ES indexers, IAM DENY on "
        "default routes) with two anonymous opt-ins: catalog-level "
        "lookup_audience.is_public=True and collection-level "
        "write_audience.allow_anonymous_create=True. The audience-driven "
        "ALLOW policies override the private cascade's DENY via priority. "
        "Use for catalogs that should serve anonymous lookup + intake "
        "while keeping the read/index surface private."
    )

    def build(self, catalog_id: str) -> PresetBundle:
        base = get_preset("private_catalog").build(catalog_id)
        return PresetBundle(
            catalog_routing=base.catalog_routing,
            collection_template=base.collection_template,
            items_template=base.items_template,
            audience_configs={
                "catalog_lookup_audience": CatalogLookupAudience(is_public=True),
                "collection_write_audience": CollectionWriteAudience(
                    allow_anonymous_create=True
                ),
            },
        )


register_preset(GeoidPreset())
