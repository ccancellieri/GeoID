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

"""``items_es_public`` preset — items-tier public Elasticsearch routing.

An ``ITEMS``-tier preset with ``catalog_scopable=True``, applicable at
catalog or collection scope.  It is the items-only counterpart used by
the STAC harvest task so the cumulative ``stac_routing`` preset's
collection-tier ES routing (which references the unregistered
``collection_elasticsearch_driver``) is avoided on ES-primary deployments.

Pins the items tier to the public ES-only routing (``items_elasticsearch_driver``
for WRITE / READ / SEARCH) sourced from the single ``_items_routing_es``
builder in ``dynastore.modules.storage.presets.stac``.

* **items @ catalog** — sets the catalog-tier items template
  (``ItemsRoutingConfig``) that future collections inherit (inherit-only;
  already-materialised collections are not retro-mutated).
* **items @ collection** — overrides items routing for one collection,
  independent of the catalog template.

The bundle leaves ``scope`` empty: the admin endpoint (or lifecycle caller)
layers the URL-derived ``catalog_id`` / ``collection_id`` on top of each
entry, so the same ``build`` output applies correctly at either scope.
"""
from __future__ import annotations

from typing import ClassVar, Tuple

from dynastore.modules.storage.presets.stac import _items_routing_es
from dynastore.modules.storage.routing_config import ItemsRoutingConfig

from .bundle_preset import BundlePreset
from .examples import PresetExample
from .protocol import PresetBundle, PresetBundleEntry, PresetTier


class ItemsEsPublicPreset(BundlePreset):
    """Items-tier public ES-only routing, applicable at catalog or collection scope.

    Pins the items tier to the public Elasticsearch-only routing so that
    STAC harvest's ES-primary write path is wired correctly without touching
    the collection-tier routing (where ``collection_elasticsearch_driver`` may
    not be registered).  The items-only counterpart to ``items_es_private``.
    """

    name = "items_es_public"
    tier: ClassVar[PresetTier] = PresetTier.ITEMS
    catalog_scopable: ClassVar[bool] = True
    keywords: ClassVar[Tuple[str, ...]] = (
        "routing", "items", "public", "stac", "elasticsearch",
    )
    description = (
        "Items-tier public Elasticsearch routing. Pins items_elasticsearch_driver "
        "for WRITE / READ / SEARCH — the ES-primary items routing used by STAC "
        "harvest on ES-only deployments. Applied at catalog scope it sets the items "
        "template future collections inherit (inherit-only, no retro-apply); applied "
        "at collection scope it overrides items routing for one collection."
    )

    examples: ClassVar[Tuple[PresetExample, ...]] = (
        PresetExample(
            name="public-items-catalog-template",
            summary=(
                "Set the items template that future collections of a catalog inherit: "
                "all items operations are routed to the public ES index only. "
                "Inherit-only — already-materialised collections are not retro-mutated. "
                "Apply at catalog scope via "
                "POST /admin/catalogs/{catalog_id}/presets/items_es_public. "
                "Takes no parameters."
            ),
            params={},
        ),
        PresetExample(
            name="public-items-single-collection",
            summary=(
                "Override the items routing for one collection so its items are "
                "served from the public ES index only, regardless of the catalog "
                "template. Apply at collection scope via POST "
                "/admin/catalogs/{catalog_id}/collections/{collection_id}/presets/items_es_public. "
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
                    instance=_items_routing_es(),
                    rollback_priority=10,
                ),
            )
        )
