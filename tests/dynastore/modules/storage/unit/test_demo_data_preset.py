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

"""Unit tests for the demo_data preset (dynastore#307).

Pure-Python tests — no DB, no FastAPI, no network.  The suite covers:

- ``_DemoDataContributor.get_data()`` yields exactly one ``DataSeed`` with the
  expected catalog/collection IDs, the 6-tile Italy grid items, and
  ``manage_catalog=True``.
- ``DEMO_DATA_PRESET`` has the expected name, description, keywords, and PLATFORM
  tier.
- After importing ``dynastore.modules.storage.presets`` the registry contains a
  preset named ``"demo_data"`` at ``PresetTier.PLATFORM``.
"""
from __future__ import annotations

from typing import List

from dynastore.modules.storage.presets.demo_data import (
    DEMO_DATA_PRESET,
    _DemoDataContributor,
)
from dynastore.modules.storage.presets.preset import DataSeed
from dynastore.modules.storage.presets.protocol import PresetTier
from dynastore.modules.storage.presets.registry import find_preset


# ---------------------------------------------------------------------------
# _DemoDataContributor.get_data()
# ---------------------------------------------------------------------------

def test_contributor_yields_exactly_one_seed() -> None:
    """get_data() returns an iterable with exactly one DataSeed."""
    contributor = _DemoDataContributor()
    seeds: List[DataSeed] = list(contributor.get_data())
    assert len(seeds) == 1


def test_contributor_seed_catalog_and_collection_ids() -> None:
    """The single seed targets demo_catalog / demo_collection."""
    seed = list(_DemoDataContributor().get_data())[0]
    assert seed.catalog_id == "demo_catalog"
    assert seed.collection_id == "demo_collection"


def test_contributor_seed_has_six_tiles() -> None:
    """The seed carries the full 2×3 Italy tile grid."""
    seed = list(_DemoDataContributor().get_data())[0]
    assert len(seed.items) == 6


def test_contributor_seed_item_ids() -> None:
    """Item IDs are the 6 tile ids in (row, col) declaration order."""
    seed = list(_DemoDataContributor().get_data())[0]
    ids = [item["id"] for item in seed.items]
    assert ids == [
        "tile_west_south", "tile_east_south",
        "tile_west_centre", "tile_east_centre",
        "tile_west_north", "tile_east_north",
    ]


def test_contributor_seed_items_are_polygons() -> None:
    """Every tile is a closed Polygon Feature over Italy."""
    seed = list(_DemoDataContributor().get_data())[0]
    for item in seed.items:
        assert item["type"] == "Feature"
        assert item["geometry"]["type"] == "Polygon"
        ring = item["geometry"]["coordinates"][0]
        assert ring[0] == ring[-1]  # closed ring


def test_contributor_seed_item_names() -> None:
    """Item names follow the 'Italy – <Row> <Col>' pattern."""
    seed = list(_DemoDataContributor().get_data())[0]
    names = [item["properties"]["name"] for item in seed.items]
    assert names[0] == "Italy – South West"
    assert names[-1] == "Italy – North East"
    assert all(n.startswith("Italy – ") for n in names)


def test_contributor_seed_manage_catalog_true() -> None:
    """manage_catalog is True — the preset owns demo_catalog."""
    seed = list(_DemoDataContributor().get_data())[0]
    assert seed.manage_catalog is True


def test_contributor_seed_manage_collection_true() -> None:
    """manage_collection is True — the preset owns demo_collection."""
    seed = list(_DemoDataContributor().get_data())[0]
    assert seed.manage_collection is True


def test_contributor_seed_declares_es_secondary_routing() -> None:
    """The seed carries an items routing with an Elasticsearch secondary WRITE
    entry (source="auto"), so demo_collection items are searchable rather than
    PG-only — regression cover for #2241."""
    from dynastore.modules.storage.routing_config import Operation

    seed = list(_DemoDataContributor().get_data())[0]
    assert seed.items_routing is not None
    write_entries = seed.items_routing.operations[Operation.WRITE]
    es = [e for e in write_entries if e.driver_ref == "items_elasticsearch_driver"]
    assert es, "demo seed must declare an ES secondary write entry"
    assert es[0].secondary_index is True
    assert es[0].source == "auto"
    # A SEARCH entry routed to ES is what makes /stac/.../items return matches.
    assert seed.items_routing.operations[Operation.SEARCH][0].driver_ref == (
        "items_elasticsearch_driver"
    )


# ---------------------------------------------------------------------------
# DEMO_DATA_PRESET metadata
# ---------------------------------------------------------------------------

def test_preset_name() -> None:
    assert DEMO_DATA_PRESET.name == "demo_data"


def test_preset_tier_is_platform() -> None:
    assert DEMO_DATA_PRESET.tier == PresetTier.PLATFORM


def test_preset_description_non_empty() -> None:
    assert DEMO_DATA_PRESET.description


def test_preset_keywords_contain_expected() -> None:
    kws = set(DEMO_DATA_PRESET.keywords)
    assert {"demo", "data", "platform", "catalog", "seed"} <= kws


# ---------------------------------------------------------------------------
# Registry presence
# ---------------------------------------------------------------------------

def test_demo_data_preset_registered_in_registry() -> None:
    """Importing the presets package registers demo_data in the global registry."""
    import dynastore.modules.storage.presets  # noqa: F401 — side-effect: registers preset

    preset = find_preset("demo_data")
    assert preset.name == "demo_data"


def test_demo_data_preset_registry_tier_is_platform() -> None:
    """The registered preset exposes PresetTier.PLATFORM."""
    import dynastore.modules.storage.presets  # noqa: F401

    preset = find_preset("demo_data")
    assert preset.tier == PresetTier.PLATFORM
