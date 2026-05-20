"""private_catalog preset (#847) — PG-only envelopes + items-tier private ES (#1047)."""
from __future__ import annotations

from dynastore.modules.storage.presets import get_preset
from dynastore.modules.storage.routing_config import (
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
    bundle = get_preset("private_catalog").build("cat-priv")
    assert isinstance(bundle.catalog_routing, CatalogRoutingConfig)
    assert isinstance(bundle.collection_template, CollectionRoutingConfig)
    assert isinstance(bundle.items_template, ItemsRoutingConfig)
    assert bundle.audience_configs == {}


def test_private_catalog_catalog_routing_is_pg_only():
    """Catalog envelopes are PG-only for private catalogs — no ES private index."""
    bundle = get_preset("private_catalog").build("cat-priv")
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
    bundle = get_preset("private_catalog").build("cat-priv")
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
    bundle = get_preset("private_catalog").build("cat-priv")
    assert _items_routing_has_private_driver(bundle.items_template)
    items_refs = [
        e.driver_ref
        for entries in bundle.items_template.operations.values()
        for e in entries
    ]
    assert "items_elasticsearch_private_driver" in items_refs
    assert "items_elasticsearch_driver" not in items_refs
