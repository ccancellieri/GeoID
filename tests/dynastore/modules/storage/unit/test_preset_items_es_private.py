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

"""items_es_private preset (#972) — items-tier private ES, catalog_scopable.

Covers the bundle shape and, crucially, the previously-untested
``catalog_scopable`` dispatch branch in
``presets_api._preset_reachable_at`` (#972: the branch existed but no
shipped ITEMS-tier preset exercised it).
"""
from __future__ import annotations

from dynastore.extensions.configs.presets_api import (
    _preset_reachable_at,
    _resolve_preset_for_scope,
)
from dynastore.modules.storage.presets import PresetTier, get_preset
from dynastore.modules.storage.routing_config import (
    ItemsRoutingConfig,
    _items_routing_has_private_driver,
)
from fastapi import HTTPException

import pytest


def test_items_es_private_registered_as_catalog_scopable_items_tier():
    p = get_preset("items_es_private")
    assert p.name == "items_es_private"
    assert p.tier == PresetTier.ITEMS
    assert p.catalog_scopable is True
    assert p.description, "preset must carry a non-empty description"


def test_items_es_private_bundle_has_single_items_entry():
    bundle = get_preset("items_es_private").build()
    entries = list(bundle.iter_apply())
    assert len(entries) == 1
    entry = entries[0]
    assert entry.slot == "items_template"
    assert entry.config_cls is ItemsRoutingConfig
    assert isinstance(entry.instance, ItemsRoutingConfig)
    # The preset leaves scope empty — the admin endpoint layers the
    # URL-derived catalog_id / collection_id on top.
    assert dict(entry.scope) == {}


def test_items_es_private_pins_private_es_driver():
    bundle = get_preset("items_es_private").build()
    items_template = bundle.items_template
    assert isinstance(items_template, ItemsRoutingConfig)
    assert _items_routing_has_private_driver(items_template)
    refs = [
        e.driver_ref
        for entries in items_template.operations.values()
        for e in entries
    ]
    assert "items_elasticsearch_private_driver" in refs
    assert "items_postgresql_driver" in refs
    assert "items_elasticsearch_driver" not in refs


# --- catalog_scopable dispatch branch (presets_api) --------------------------


def test_reachable_at_collection_and_catalog_when_scopable():
    """A catalog_scopable ITEMS preset reaches BOTH URL families."""
    preset = get_preset("items_es_private")
    assert _preset_reachable_at(preset, PresetTier.COLLECTION) is True
    assert _preset_reachable_at(preset, PresetTier.CATALOG) is True


def test_not_reachable_at_platform_scope():
    preset = get_preset("items_es_private")
    assert _preset_reachable_at(preset, PresetTier.PLATFORM) is False


def test_non_scopable_items_preset_is_collection_only():
    """An ITEMS preset with catalog_scopable=False reaches collection only —
    the catalog family must reject it (the False half of the branch)."""

    class _CollectionOnlyItems:
        name = "items_collection_only_stub"
        description = "stub"
        tier = PresetTier.ITEMS
        catalog_scopable = False

    stub = _CollectionOnlyItems()
    assert _preset_reachable_at(stub, PresetTier.COLLECTION) is True
    assert _preset_reachable_at(stub, PresetTier.CATALOG) is False


def test_resolve_preset_for_scope_allows_catalog_for_scopable():
    preset = _resolve_preset_for_scope("items_es_private", PresetTier.CATALOG)
    assert preset.name == "items_es_private"


def test_resolve_preset_for_scope_409_at_platform():
    with pytest.raises(HTTPException) as exc:
        _resolve_preset_for_scope("items_es_private", PresetTier.PLATFORM)
    assert exc.value.status_code == 409
    assert "items" in exc.value.detail.lower()


def test_resolve_preset_for_scope_404_unknown():
    with pytest.raises(HTTPException) as exc:
        _resolve_preset_for_scope("no_such_preset", PresetTier.CATALOG)
    assert exc.value.status_code == 404
