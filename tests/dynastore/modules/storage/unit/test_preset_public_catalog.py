"""Public-catalog preset (#847)."""
from __future__ import annotations

from dynastore.modules.storage.presets import get_preset
from dynastore.modules.storage.routing_config import (
    CatalogRoutingConfig,
    CollectionRoutingConfig,
    ItemsRoutingConfig,
    _items_routing_has_private_driver,
)


def test_public_catalog_preset_registered():
    p = get_preset("public_catalog")
    assert p.name == "public_catalog"
    assert p.description, "preset must carry a non-empty description"


def test_public_catalog_bundle_shape():
    bundle = get_preset("public_catalog").build("cat-pub")
    assert isinstance(bundle.catalog_routing, CatalogRoutingConfig)
    assert isinstance(bundle.collection_template, CollectionRoutingConfig)
    assert isinstance(bundle.items_template, ItemsRoutingConfig)
    assert bundle.audience_configs == {}


def test_public_catalog_items_not_private():
    bundle = get_preset("public_catalog").build("cat-pub")
    assert not _items_routing_has_private_driver(bundle.items_template)


def test_public_catalog_uses_expected_driver_ids():
    bundle = get_preset("public_catalog").build("cat-pub")
    cat_refs = [
        e.driver_ref
        for entries in bundle.catalog_routing.operations.values()
        for e in entries
    ]
    assert "catalog_elasticsearch_driver" in cat_refs
    assert "catalog_postgresql_driver" in cat_refs

    items_refs = [
        e.driver_ref
        for entries in bundle.items_template.operations.values()
        for e in entries
    ]
    assert "items_elasticsearch_driver" in items_refs
    assert "items_postgresql_driver" in items_refs
