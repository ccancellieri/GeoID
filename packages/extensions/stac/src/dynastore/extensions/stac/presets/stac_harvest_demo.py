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

"""``stac_harvest_demo`` preset — one-click STAC harvest demo (PLATFORM tier).

Applies at platform scope (``POST /configs/presets/stac_harvest_demo``):

  1. Creates the ``demo_harvest`` catalog and a ``demo_harvest_index``
     placeholder collection (skip-if-exists) so the harvest has a
     pre-existing target. The ``stac_harvest`` task creates source-catalog
     collections alongside this placeholder automatically.

  2. Submits an async ``stac_harvest`` job that pulls one collection and
     25 items from the Earth Search v1 public STAC API
     (``https://earth-search.aws.element84.com/v1``) into the
     ``demo_harvest`` catalog. Items are routed to public Elasticsearch
     so they are immediately searchable.

Source: ``https://earth-search.aws.element84.com/v1``
Limits: ``max_collections=1``, ``max_items=25`` — fast enough for CI.

Revoke: removes the ``demo_harvest_index`` placeholder collection and the
``demo_harvest`` catalog if it is empty after collection removal. Collections
and items the async harvest job wrote to Elasticsearch are not undone —
re-applying re-syncs them idempotently (all upserts keyed on STAC id).
"""
from __future__ import annotations

from typing import Iterable

from dynastore.modules.storage.presets.multi_contributor import MultiContributorPreset
from dynastore.modules.storage.presets.preset import DataSeed, TaskSeed
from dynastore.modules.storage.presets.registry import register_preset

_CATALOG_ID = "demo_harvest"
_COLLECTION_ID = "demo_harvest_index"

_CATALOG_DATA = {
    "id": _CATALOG_ID,
    "title": {"en": "STAC Harvest Demo"},
    "description": {
        "en": (
            "Demo catalog populated by the stac_harvest_demo preset. "
            "Collections and items are harvested from a public STAC API "
            "into this catalog."
        ),
    },
    "keywords": ["demo", "stac", "harvest", "platform"],
    "license": "CC-BY-4.0",
}

_COLLECTION_DATA = {
    "id": _COLLECTION_ID,
    "title": {"en": "Harvest Demo Index"},
    "description": {
        "en": (
            "Placeholder collection created by the stac_harvest_demo preset "
            "to anchor the demo_harvest catalog. The stac_harvest job "
            "creates source-catalog collections alongside this one."
        ),
    },
}

# Earth Search v1 — small, reliable, publicly accessible STAC catalog hosted
# by Element 84 (AWS Open Data).  max_collections=1 + max_items=25 keeps the
# demo harvest fast (two to three HTTP round-trips total).
_EARTH_SEARCH_URL = "https://earth-search.aws.element84.com/v1"


class _StacHarvestDemoContributor:
    """Data + task contributor for the stac_harvest_demo preset."""

    def get_data(self) -> Iterable[DataSeed]:
        # Pre-create the target catalog + a placeholder collection so
        # ``stac_harvest`` can call create_collection on the catalog without
        # raising a missing-catalog error.
        yield DataSeed(
            catalog_id=_CATALOG_ID,
            collection_id=_COLLECTION_ID,
            catalog_data=_CATALOG_DATA,
            collection_data=_COLLECTION_DATA,
            items=(),
            manage_catalog=True,
            manage_collection=True,
        )

    def get_tasks(self) -> Iterable[TaskSeed]:
        yield TaskSeed(
            process_id="stac_harvest",
            inputs={
                "catalog_url": _EARTH_SEARCH_URL,
                "target_catalog": _CATALOG_ID,
                "max_collections": 1,
                "max_items": 25,
                "with_assets": True,
                "drivers": "es",
            },
            async_mode=True,
            dedup_key="preset:stac_harvest_demo:stac_harvest",
        )


register_preset(MultiContributorPreset(
    name="stac_harvest_demo",
    description=(
        "Create the demo_harvest catalog and submit an async stac_harvest job "
        "that harvests one collection / 25 items from the Earth Search v1 public "
        "STAC API (https://earth-search.aws.element84.com/v1) into that catalog. "
        "Items land in public Elasticsearch and are immediately searchable. "
        "Designed for CI / demo use: small limits keep the run fast."
    ),
    keywords=("demo", "data", "platform", "stac", "harvest"),
    contributors_factory=lambda: [_StacHarvestDemoContributor()],
))
