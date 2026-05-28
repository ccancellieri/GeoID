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

"""Demo-data preset — canonical data-contributor example (dynastore#307).

Seeds a ``demo_catalog`` / ``demo_collection`` with two point features
(Rome and Amsterdam) as a reversible platform preset.  This is the
recommended way to deliver built-in seed data: declare a
``DataContributor``, wrap it in a ``MultiContributorPreset``, and
register it.

This preset supersedes the manual ``tools/demo/populate.py`` CLI path
(which carried a TODO to expose the same data through the preset API).
Running ``apply`` is idempotent — the catalog and collection are created
only when absent, and items are upserted.  Running ``revoke`` removes the
items and, because ``manage_catalog`` / ``manage_collection`` are both
``True``, deletes the catalog and collection only when this preset
created them.
"""
from __future__ import annotations

from typing import Iterable

from .multi_contributor import MultiContributorPreset
from .preset import DataSeed

# ---------------------------------------------------------------------------
# Catalog / collection / item payloads — lifted verbatim from
# tools/demo/populate.py so the two paths stay consistent.
# ---------------------------------------------------------------------------

_CATALOG_DATA = {
    "id": "demo_catalog",
    "title": {"en": "Demo Catalog", "it": "Catalogo Demo"},
    "description": {
        "en": "A catalog for demonstration purposes.",
        "it": "Un catalogo per scopi dimostrativi.",
    },
    "keywords": ["demo", "dynastore", "geospatial"],
    "license": "CC-BY-4.0",
}

_COLLECTION_DATA = {
    "id": "demo_collection",
    "title": {"en": "Demo Collection"},
    "description": {"en": "A demo collection of points."},
    "type": "Feature",
}

_ITEM_ROME = {
    "id": "item_1",
    "type": "Feature",
    "geometry": {"type": "Point", "coordinates": [12.4964, 41.9028]},
    "properties": {"name": "Rome", "description": "The capital of Italy"},
}

_ITEM_AMSTERDAM = {
    "id": "item_2",
    "type": "Feature",
    "geometry": {"type": "Point", "coordinates": [4.8952, 52.3702]},
    "properties": {"name": "Amsterdam", "description": "The capital of the Netherlands"},
}


# ---------------------------------------------------------------------------
# Data contributor
# ---------------------------------------------------------------------------

class _DemoDataContributor:
    """Yields the single demo seed that ``MultiContributorPreset`` will apply."""

    def get_data(self) -> Iterable[DataSeed]:
        yield DataSeed(
            catalog_id="demo_catalog",
            collection_id="demo_collection",
            catalog_data=_CATALOG_DATA,
            collection_data=_COLLECTION_DATA,
            items=(_ITEM_ROME, _ITEM_AMSTERDAM),
            manage_catalog=True,
            manage_collection=True,
        )


# ---------------------------------------------------------------------------
# Preset instance — registered in presets/__init__.py
# ---------------------------------------------------------------------------

DEMO_DATA_PRESET = MultiContributorPreset(
    name="demo_data",
    description=(
        "Seed a demo catalog (demo_catalog/demo_collection) with two example "
        "point features — the data-contributor demonstration preset."
    ),
    keywords=("demo", "data", "platform", "catalog", "seed"),
    contributors_factory=lambda: [_DemoDataContributor()],
)
