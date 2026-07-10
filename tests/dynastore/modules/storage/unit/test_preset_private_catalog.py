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

"""private_catalog preset (#847) — PG-only envelopes + items-tier private ES (#1047)."""
from __future__ import annotations

from dynastore.modules.storage.presets import get_preset
from dynastore.modules.storage.routing_config import (
    AssetRoutingConfig,
    CatalogRoutingConfig,
    CollectionRoutingConfig,
    ItemsRoutingConfig,
    _items_routing_has_private_driver,
)


def test_private_catalog_preset_registered():
    p = get_preset("private_catalog")
    assert p.name == "private_catalog"
    assert p.description, "preset must carry a non-empty description"


def test_private_catalog_bundle_shape():
    bundle = get_preset("private_catalog").build(catalog_id="cat-priv")
    assert isinstance(bundle.catalog_routing, CatalogRoutingConfig)
    assert isinstance(bundle.collection_template, CollectionRoutingConfig)
    assert isinstance(bundle.items_template, ItemsRoutingConfig)
    assert isinstance(bundle.asset_template, AssetRoutingConfig)
    assert bundle.audience_configs == {}


def test_private_catalog_catalog_routing_is_pg_only():
    """Catalog envelopes are PG-only for private catalogs — no ES private index."""
    bundle = get_preset("private_catalog").build(catalog_id="cat-priv")
    cat_refs = [
        e.driver_ref
        for entries in bundle.catalog_routing.operations.values()
        for e in entries
    ]
    assert "catalog_postgresql_driver" in cat_refs
    assert "catalog_elasticsearch_private_driver" not in cat_refs
    assert "catalog_elasticsearch_driver" not in cat_refs


def test_private_catalog_collection_routing_is_pg_only():
    """Collection envelopes are PG-only for private catalogs — no ES private index."""
    bundle = get_preset("private_catalog").build(catalog_id="cat-priv")
    coll_refs = [
        e.driver_ref
        for entries in bundle.collection_template.operations.values()
        for e in entries
    ]
    assert "collection_postgresql_driver" in coll_refs
    assert "collection_elasticsearch_private_driver" not in coll_refs
    assert "collection_elasticsearch_driver" not in coll_refs


def test_private_catalog_items_pins_private_driver():
    """Items tier pins the private ES driver."""
    bundle = get_preset("private_catalog").build(catalog_id="cat-priv")
    assert _items_routing_has_private_driver(bundle.items_template)
    items_refs = [
        e.driver_ref
        for entries in bundle.items_template.operations.values()
        for e in entries
    ]
    assert "items_elasticsearch_private_driver" in items_refs
    assert "items_elasticsearch_driver" not in items_refs


def test_private_catalog_asset_routing_is_pg_only():
    """Assets are PG-only for private catalogs — they must NOT inherit the
    public asset ES driver and leak into public search (Part B)."""
    bundle = get_preset("private_catalog").build(catalog_id="cat-priv")
    asset_refs = [
        e.driver_ref
        for entries in bundle.asset_template.operations.values()
        for e in entries
    ]
    assert "asset_postgresql_driver" in asset_refs
    assert "asset_elasticsearch_driver" not in asset_refs
    assert "asset_elasticsearch_private_driver" not in asset_refs


def test_private_catalog_asset_routing_has_write_and_read():
    """The PG-only asset routing must cover both WRITE and READ; UPLOAD is
    left to validation-time auto-augmentation (not forced to an ES driver)."""
    from dynastore.modules.storage.routing_config import Operation

    bundle = get_preset("private_catalog").build(catalog_id="cat-priv")
    ops = bundle.asset_template.operations
    assert Operation.WRITE in ops
    assert Operation.READ in ops


# ── #1102 item 1 / #1047: stay PG-only even when ES drivers are discoverable ──
# The catalog/collection PG-only assertions above pass trivially in unit
# isolation because no ES driver is registered. The nightly *full* run, where an
# ES catalog/collection driver IS discoverable, exposed the real leak: with no
# operator-pinned INDEX entry, ``_self_register_indexers_into`` would auto-append
# the discoverable ES driver (it declares ``auto_register_for_routing`` ⊇
# {INDEX}) to the INDEX lane — routing a *private* catalog/collection's tier
# search at the public ES index. Pinning WRITE operator-sourced blocks this
# (self-registration gates on WRITE's operator-managed status). These tests
# reproduce the discoverable-public-driver condition and assert the private
# preset never grows an ES hop in ANY operation.


def _patch_es_drivers_discoverable(monkeypatch):
    """Make a catalog + collection ES driver discoverable to the routing-config
    self-register helpers, simulating the nightly full-run registry state."""
    import dynastore.tools.discovery as discovery
    from dynastore.modules.storage.routing_config import Operation

    class CatalogElasticsearchDriver:  # __name__ → catalog_elasticsearch_driver
        index_tiers = frozenset({"catalog"})
        auto_register_for_routing = frozenset({Operation.INDEX})

    class CollectionElasticsearchDriver:  # → collection_elasticsearch_driver
        index_tiers = frozenset({"collection"})
        auto_register_for_routing = frozenset({Operation.INDEX})

    catalog_es = CatalogElasticsearchDriver()
    collection_es = CollectionElasticsearchDriver()

    def fake_get_protocols(marker):
        name = getattr(marker, "__name__", "")
        if name in ("IndexTierDriver", "CatalogStore", "CollectionStore"):
            return [catalog_es, collection_es]
        return []

    monkeypatch.setattr(discovery, "get_protocols", fake_get_protocols)


def test_private_catalog_catalog_routing_pg_only_under_es_pollution(monkeypatch):
    _patch_es_drivers_discoverable(monkeypatch)
    bundle = get_preset("private_catalog").build(catalog_id="cat-priv")
    cat_refs = [
        e.driver_ref
        for entries in bundle.catalog_routing.operations.values()
        for e in entries
    ]
    assert "catalog_postgresql_driver" in cat_refs
    assert "catalog_elasticsearch_driver" not in cat_refs
    assert "catalog_elasticsearch_private_driver" not in cat_refs


def test_private_catalog_collection_routing_pg_only_under_es_pollution(monkeypatch):
    _patch_es_drivers_discoverable(monkeypatch)
    bundle = get_preset("private_catalog").build(catalog_id="cat-priv")
    coll_refs = [
        e.driver_ref
        for entries in bundle.collection_template.operations.values()
        for e in entries
    ]
    assert "collection_postgresql_driver" in coll_refs
    assert "collection_elasticsearch_driver" not in coll_refs
    assert "collection_elasticsearch_private_driver" not in coll_refs


# --- items tier (#1336) -----------------------------------------------------
# The items INDEX entries are ``source="auto"``, so with the public
# ``ItemsElasticsearchDriver`` discoverable (it opts into the INDEX lane via
# ``auto_register_for_routing``) ``_self_register_indexers_into`` would append
# it to a *private* catalog's items INDEX list — pointing item search at the
# shared public index instead of the per-tenant private index. Pinning WRITE
# operator-sourced blocks the append (self-registration gates on WRITE's
# operator-managed status). These tests reproduce the discoverable-public-
# driver condition and assert no public ES hop appears.


def _patch_items_es_driver_discoverable(monkeypatch):
    """Make the public items ES driver discoverable to the items-tier
    self-register helper (via ``IndexTierDriver``/``index_tiers``),
    simulating the full-run registry state."""
    import dynastore.tools.discovery as discovery
    from dynastore.modules.storage.routing_config import Operation

    class ItemsElasticsearchDriver:  # __name__ → items_elasticsearch_driver
        index_tiers = frozenset({"item"})
        auto_register_for_routing = frozenset({Operation.INDEX})

    items_es = ItemsElasticsearchDriver()

    def fake_get_protocols(marker):
        name = getattr(marker, "__name__", "")
        if name == "IndexTierDriver":
            return [items_es]
        return []

    monkeypatch.setattr(discovery, "get_protocols", fake_get_protocols)


def test_private_catalog_items_index_lane_is_private_only_under_es_pollution(monkeypatch):
    _patch_items_es_driver_discoverable(monkeypatch)
    from dynastore.modules.storage.routing_config import Operation

    bundle = get_preset("private_catalog").build(catalog_id="cat-priv")
    index_refs = [
        e.driver_ref
        for e in bundle.items_template.operations.get(Operation.INDEX, [])
    ]
    # Private items INDEX lane stays isolated to the private ES driver — PG
    # remains available via the READ-lane fallback in the derived search
    # pool; it is not itself an INDEX-lane entry.
    assert index_refs == ["items_elasticsearch_private_driver"], (
        f"public items ES driver must not be appended to a private catalog's "
        f"items INDEX lane; got {index_refs}"
    )


def test_private_catalog_items_no_public_es_in_any_operation_under_pollution(monkeypatch):
    _patch_items_es_driver_discoverable(monkeypatch)
    bundle = get_preset("private_catalog").build(catalog_id="cat-priv")
    all_refs = [
        e.driver_ref
        for entries in bundle.items_template.operations.values()
        for e in entries
    ]
    # WRITE is operator-managed via the FATAL PG entry, which also gates
    # INDEX self-registration off. No public items ES driver in ANY operation.
    assert "items_elasticsearch_driver" not in all_refs, (
        f"private items routing must never grow a public ES hop; got {all_refs}"
    )
