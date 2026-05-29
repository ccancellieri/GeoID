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
# operator-pinned SEARCH op, ``_self_register_searchers_into`` auto-appends the
# discoverable ES driver (it declares ``auto_register_for_routing`` ⊇ {SEARCH})
# to a freshly-created SEARCH op — routing a *private* catalog/collection's
# tier search at the public ES index. These tests reproduce that condition
# deterministically by making an ES driver discoverable, and assert the private
# preset never grows an ES hop in ANY operation.


def _patch_es_drivers_discoverable(monkeypatch):
    """Make a catalog + collection ES driver discoverable to the routing-config
    self-register helpers, simulating the nightly full-run registry state."""
    import dynastore.tools.discovery as discovery
    from dynastore.modules.storage.routing_config import Operation

    class CatalogElasticsearchDriver:  # __name__ → catalog_elasticsearch_driver
        auto_register_for_routing = frozenset({Operation.SEARCH, Operation.WRITE})

    class CollectionElasticsearchDriver:  # → collection_elasticsearch_driver
        auto_register_for_routing = frozenset({Operation.SEARCH, Operation.WRITE})

    catalog_es = CatalogElasticsearchDriver()
    collection_es = CollectionElasticsearchDriver()

    def fake_get_protocols(marker):
        name = getattr(marker, "__name__", "")
        if name in ("CatalogIndexer", "CatalogStore"):
            return [catalog_es]
        if name in ("CollectionIndexer", "CollectionStore"):
            return [collection_es]
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
# The items SEARCH entries previously declared ``source="auto"``, so with the
# public ``ItemsElasticsearchDriver`` discoverable (it opts into SEARCH via
# ``auto_register_for_routing``) ``_self_register_searchers_into`` appended it
# to a *private* catalog's items SEARCH list — pointing item search at the
# shared public index instead of the per-tenant private index. Pinning those
# entries operator-sourced blocks the append. These tests reproduce the
# discoverable-public-driver condition and assert no public ES hop appears.


def _patch_items_es_driver_discoverable(monkeypatch):
    """Make the public items ES driver discoverable to the items-tier
    self-register helpers (``ItemIndexer`` for WRITE, ``CollectionItemsStore``
    for SEARCH), simulating the full-run registry state."""
    import dynastore.tools.discovery as discovery
    from dynastore.modules.storage.routing_config import Operation

    class ItemsElasticsearchDriver:  # __name__ → items_elasticsearch_driver
        auto_register_for_routing = frozenset({Operation.SEARCH, Operation.WRITE})

    items_es = ItemsElasticsearchDriver()

    def fake_get_protocols(marker):
        name = getattr(marker, "__name__", "")
        if name in ("ItemIndexer", "CollectionItemsStore"):
            return [items_es]
        return []

    monkeypatch.setattr(discovery, "get_protocols", fake_get_protocols)


def test_private_catalog_items_search_pg_and_private_only_under_es_pollution(monkeypatch):
    _patch_items_es_driver_discoverable(monkeypatch)
    from dynastore.modules.storage.routing_config import Operation

    bundle = get_preset("private_catalog").build(catalog_id="cat-priv")
    search_refs = [
        e.driver_ref
        for e in bundle.items_template.operations.get(Operation.SEARCH, [])
    ]
    # Private items search stays isolated: PG + private ES only, no public hop.
    assert "items_postgresql_driver" in search_refs
    assert "items_elasticsearch_private_driver" in search_refs
    assert "items_elasticsearch_driver" not in search_refs, (
        f"public items ES driver must not be appended to a private catalog's "
        f"items SEARCH; got {search_refs}"
    )


def test_private_catalog_items_no_public_es_in_any_operation_under_pollution(monkeypatch):
    _patch_items_es_driver_discoverable(monkeypatch)
    bundle = get_preset("private_catalog").build(catalog_id="cat-priv")
    all_refs = [
        e.driver_ref
        for entries in bundle.items_template.operations.values()
        for e in entries
    ]
    # WRITE is already operator-managed via the FATAL PG entry; SEARCH is now
    # too. No public items ES driver in ANY operation.
    assert "items_elasticsearch_driver" not in all_refs, (
        f"private items routing must never grow a public ES hop; got {all_refs}"
    )
