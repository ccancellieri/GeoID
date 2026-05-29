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
in to a single anonymous audience:

  * ``CatalogLookupAudience.is_public=True`` — opens the lookup-only
    search surface (``/search`` + ``/search/catalogs/{cat}``) that the
    ``lookup_only_search`` policy gates, and arms the geoid extension's
    anonymous STAC/Features enumeration DENY so the catalog is reachable
    only by exact geoid / external_id lookup, never by enumeration.

The opt-in is a higher-priority ALLOW layered over the ``private_catalog``
DENY baseline (#915 priority), so anonymous lookup reaches the opened
routes while everything else stays denied by the private cascade.

This profile deliberately does NOT open anonymous writes. Lookup-only mode
(``is_public=True``) and anonymous create cannot coexist on one catalog:
the enumeration DENY arms under ``is_public`` and covers the item-POST
path, and deny-precedence (``PermissionService.evaluate_access``) makes it
beat any anonymous-create ALLOW. A geoid catalog therefore refuses
anonymous inserts by construction — which is the intended posture
(un-fao/GeoID#1204: public users must not insert). Genuine anonymous-intake
catalogs are a different shape: ``is_public=False`` plus a per-collection
``CollectionWriteAudience.allow_anonymous_create=True``, configured
directly rather than via this preset.

Auto-registers on extension import.
"""
from __future__ import annotations

from dynastore.modules.iam.audience_configs import CatalogLookupAudience
from typing import ClassVar

from dynastore.modules.storage.presets import BundlePreset, get_preset, register_preset
from dynastore.modules.storage.presets.protocol import PresetBundle, PresetBundleEntry, PresetTier


class GeoidPreset(BundlePreset):
    """Flagship FAO GeoID profile: private storage + anonymous lookup-only."""

    name = "geoid"
    tier: ClassVar[PresetTier] = PresetTier.CATALOG
    catalog_scopable: ClassVar[bool] = False
    description = (
        "FAO GeoID flagship profile. Composes the private_catalog bundle "
        "(PG-first storage + per-tenant private ES indexers, IAM DENY on "
        "default routes) with one anonymous opt-in: catalog-level "
        "lookup_audience.is_public=True. That arms lookup-only mode — "
        "anonymous callers can resolve a single record by geoid or "
        "external_id but cannot enumerate or insert. Use for catalogs that "
        "serve anonymous lookup while keeping the read/index/write surface "
        "private."
    )

    def build(self, catalog_id: str, **_scope: str) -> PresetBundle:
        base = get_preset("private_catalog").build(catalog_id=catalog_id)
        return PresetBundle(
            entries=(
                *base.entries,
                PresetBundleEntry(
                    slot="audience:catalog_lookup_audience",
                    config_cls=CatalogLookupAudience,
                    instance=CatalogLookupAudience(is_public=True),
                    rollback_priority=100,
                ),
            )
        )

    async def on_applied(self, catalog_id: str, **_scope: str) -> None:
        """Called after the preset bundle is applied to a catalog.
        
        Registers per-catalog geoid IAM policies for the lookup-only access model.
        """
        from .catalog_policies import register_geoid_policies_for_catalog
        
        await register_geoid_policies_for_catalog(catalog_id)


register_preset(GeoidPreset())
