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

"""Privacy-cascade smoke — the C0 lookup-only-anonymous-write demo path.

The demo pins items-tier private Elasticsearch indexing on a collection and
relies on the privacy-cascade detection to treat that collection as private.
After the ``is_private`` flag was retired (privacy is now derived solely from
the presence of ``items_elasticsearch_private_driver`` in
``ItemsRoutingConfig``), the contract the demo exercises is:

* Applying the ``items_es_private`` preset (or its underlying
  ``_build_private_items_routing`` builder) produces an items routing config
  that pins the private driver and is recognised as private.
* Dropping the private driver from that routing config flips the collection
  back to public — privacy is the presence of the private driver, nothing
  more. There is no longer a separate flag that could remain "private" once
  the driver is removed.

These are pure-unit assertions on the demo's real preset/builder output and
the detection helper; no DB, no app lifespan. They smoke-verify that the
demo's private-routing step resolves to a private collection and that the
inverse holds, so a regression in the preset, the builder, or the detection
helper surfaces here rather than only in the live demo notebook.
"""
from __future__ import annotations

from dynastore.modules.catalog.catalog_config import _build_private_items_routing
from dynastore.modules.storage.presets.items_es_private import ItemsEsPrivatePreset
from dynastore.modules.storage.routing_config import (
    ItemsRoutingConfig,
    Operation,
    _items_routing_has_private_driver,
    _PRIVATE_ITEMS_DRIVER_ID,
)


def test_private_items_builder_pins_private_driver_across_operations():
    """The demo's private-routing builder pins the private driver so the
    collection is detected as private. The private driver rides the INDEX
    lane (materialization + derived search); PG is the system of record
    for WRITE/READ."""
    routing = _build_private_items_routing()

    assert _items_routing_has_private_driver(routing) is True

    write_refs = [e.driver_ref for e in routing.operations[Operation.WRITE]]
    index_refs = [e.driver_ref for e in routing.operations[Operation.INDEX]]
    assert write_refs == ["items_postgresql_driver"]
    assert _PRIVATE_ITEMS_DRIVER_ID in index_refs


def test_items_es_private_preset_resolves_to_private_routing():
    """Applying the ``items_es_private`` preset (the admin endpoint the demo
    POSTs) yields an items routing template that the privacy-cascade detection
    treats as private — the preset is sourced from the same builder."""
    bundle = ItemsEsPrivatePreset().build()

    entries = [e for e in bundle.entries if e.config_cls is ItemsRoutingConfig]
    assert len(entries) == 1, "preset must contribute exactly one items routing template"

    instance = entries[0].instance
    assert isinstance(instance, ItemsRoutingConfig)
    assert _items_routing_has_private_driver(instance) is True


def test_dropping_private_driver_flips_collection_to_public():
    """Privacy is the presence of the private driver. Removing it from every
    operation flips detection back to public — there is no residual
    ``is_private`` state to keep the collection private (retired in #733)."""
    private_routing = _build_private_items_routing()
    assert _items_routing_has_private_driver(private_routing) is True

    deprivatised_operations = {
        op: [e for e in entries if e.driver_ref != _PRIVATE_ITEMS_DRIVER_ID]
        for op, entries in private_routing.operations.items()
    }
    public_routing = ItemsRoutingConfig(operations=deprivatised_operations)

    assert _items_routing_has_private_driver(public_routing) is False
