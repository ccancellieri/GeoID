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

"""``items_es_private`` preset — items-tier private Elasticsearch indexing.

An ``ITEMS``-tier preset (#972) with ``catalog_scopable=True``, so it
reaches both admin URL families and a single preset covers the two
items-tier rows from the #972 table:

* **items @ catalog** — ``POST /admin/catalogs/{cat}/presets/items_es_private``
  sets the catalog-tier items template (``ItemsRoutingConfig``) that
  *future* collections inherit. Per the #1079/#1139 snapshot model this is
  inherit-only: already-materialized collections are not retro-mutated.
* **items @ collection** —
  ``POST /admin/catalogs/{cat}/collections/{col}/presets/items_es_private``
  pins the same items routing on one collection, overriding the catalog
  template for that collection only.

The routing pins ``items_postgresql_driver`` (sync, FATAL) as the system
of record and ``items_elasticsearch_private_driver`` (async, OUTBOX) as the
private secondary index — the same template as the ``private_catalog``
preset's items tier, sourced from the single ``_build_private_items_routing``
builder so the private-items wire shape stays defined in one place.

The bundle leaves ``scope`` empty: the admin endpoint layers the
URL-derived ``catalog_id`` (and ``collection_id`` at collection scope) on
top of each entry, so the same ``build`` output applies correctly at either
scope.
"""
from __future__ import annotations

from typing import ClassVar, Tuple

from dynastore.modules.catalog.catalog_config import _build_private_items_routing
from dynastore.modules.storage.routing_config import ItemsRoutingConfig

from .bundle_preset import BundlePreset
from .examples import PresetExample
from .protocol import PresetBundle, PresetBundleEntry, PresetTier


class ItemsEsPrivatePreset(BundlePreset):
    """Items-tier private ES indexing, applicable at catalog or collection scope."""

    name = "items_es_private"
    tier: ClassVar[PresetTier] = PresetTier.ITEMS
    catalog_scopable: ClassVar[bool] = True
    keywords: ClassVar[Tuple[str, ...]] = (
        "routing", "items", "private", "tenant", "elasticsearch",
    )
    description = (
        "Items-tier private Elasticsearch indexing. Pins "
        "items_postgresql_driver (system of record) + "
        "items_elasticsearch_private_driver (private secondary index). "
        "Applied at catalog scope it sets the items template future "
        "collections inherit (inherit-only, no retro-apply); applied at "
        "collection scope it overrides the items routing for one collection."
    )

    examples: ClassVar[Tuple[PresetExample, ...]] = (
        PresetExample(
            name="private-items-catalog-template",
            summary=(
                "Set the items template that future collections of a catalog inherit: "
                "PostgreSQL is the system of record and items are indexed into the "
                "per-tenant private ES index. Inherit-only — already-materialized "
                "collections are not retro-mutated. Apply at catalog scope via "
                "POST /admin/catalogs/{catalog_id}/presets/items_es_private. Takes no "
                "parameters."
            ),
            params={},
        ),
        PresetExample(
            name="private-items-single-collection",
            summary=(
                "Override the items routing for one collection so its items are "
                "indexed privately, regardless of the catalog template. Apply at "
                "collection scope via POST "
                "/admin/catalogs/{catalog_id}/collections/{collection_id}/presets/items_es_private. "
                "Identical bundle to the catalog-scope example — the admin endpoint "
                "layers the URL-derived scope on top."
            ),
            params={},
        ),
    )

    def build(self, **_scope: str) -> PresetBundle:
        return PresetBundle(
            entries=(
                PresetBundleEntry(
                    slot="items_template",
                    config_cls=ItemsRoutingConfig,
                    instance=_build_private_items_routing(),
                    rollback_priority=10,
                ),
            )
        )
