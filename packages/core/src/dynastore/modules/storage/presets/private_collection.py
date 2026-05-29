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

"""``private_collection`` preset — collection-tier private items routing.

A ``COLLECTION``-tier preset (#972): applied at one collection via
``POST /admin/catalogs/{cat}/collections/{col}/presets/private_collection``,
it pins the per-tenant private items indexer (``items_elasticsearch_private_driver``)
on a single collection without touching catalog-tier templates. Use to
make one collection inside an otherwise-public catalog private, or to
override a public collection template for a single collection.

The bundle entries set config at the collection scope — the endpoint
supplies ``catalog_id`` + ``collection_id`` from the URL, so the preset
itself leaves ``scope`` empty and the endpoint layers the scope on.
"""
from __future__ import annotations

from typing import ClassVar

from dynastore.modules.catalog.catalog_config import _build_private_items_routing
from dynastore.modules.storage.routing_config import ItemsRoutingConfig

from .bundle_preset import BundlePreset
from .protocol import PresetBundle, PresetBundleEntry, PresetTier


class PrivateCollectionPreset(BundlePreset):
    """Per-collection private items routing override."""

    name = "private_collection"
    tier: ClassVar[PresetTier] = PresetTier.COLLECTION
    catalog_scopable: ClassVar[bool] = False
    description = (
        "Collection-tier private items routing. Pins the per-tenant "
        "private Elasticsearch indexer (items_elasticsearch_private_driver) "
        "on a single collection, layering over the catalog's collection "
        "template. Use to make one collection private inside an otherwise "
        "public catalog."
    )

    def build(self, catalog_id: str, collection_id: str, **_scope: str) -> PresetBundle:  # noqa: ARG002
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
