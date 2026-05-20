"""Geoid routing preset (#847) — flagship FAO profile composing
``private_catalog`` + two anonymous-audience opt-ins.
"""
from __future__ import annotations

# Importing the extension auto-registers the "geoid" preset.
import dynastore.extensions.geoid  # noqa: F401

from dynastore.modules.iam.audience_configs import (
    CatalogLookupAudience,
    CollectionWriteAudience,
)
from dynastore.modules.storage.presets import get_preset
from dynastore.modules.storage.routing_config import _items_routing_has_private_driver


def test_geoid_preset_registered():
    p = get_preset("geoid")
    assert p.name == "geoid"
    assert p.description


def test_geoid_bundle_inherits_private_catalog_routing():
    """Storage tiers must match the private_catalog baseline — geoid only
    layers audience opt-ins on top; it does not relax the cascade."""
    geoid = get_preset("geoid").build("cat-fao")
    priv = get_preset("private_catalog").build("cat-fao")

    assert geoid.catalog_routing == priv.catalog_routing
    assert geoid.collection_template == priv.collection_template
    assert geoid.items_template == priv.items_template


def test_geoid_items_routing_is_private():
    """Items tier must pin the private ES driver — catalog/collection envelopes
    are PG-only (#1047)."""
    bundle = get_preset("geoid").build("cat-fao")
    assert _items_routing_has_private_driver(bundle.items_template)


def test_geoid_catalog_collection_routing_pg_only():
    """Catalog and collection envelopes are PG-only for the geoid preset."""
    bundle = get_preset("geoid").build("cat-fao")
    cat_refs = [
        e.driver_ref
        for entries in bundle.catalog_routing.operations.values()
        for e in entries
    ]
    assert "catalog_elasticsearch_private_driver" not in cat_refs
    coll_refs = [
        e.driver_ref
        for entries in bundle.collection_template.operations.values()
        for e in entries
    ]
    assert "collection_elasticsearch_private_driver" not in coll_refs


def test_geoid_audience_configs_open_anonymous_lookup_and_write():
    """The two opt-ins are the whole point of the geoid profile —
    private storage, anonymous lookup/intake. Pin the dict keys
    (= plugin names the apply endpoint dispatches on) and the values."""
    bundle = get_preset("geoid").build("cat-fao")

    assert set(bundle.audience_configs) == {
        "catalog_lookup_audience",
        "collection_write_audience",
    }

    lookup = bundle.audience_configs["catalog_lookup_audience"]
    assert isinstance(lookup, CatalogLookupAudience)
    assert lookup.is_public is True

    write = bundle.audience_configs["collection_write_audience"]
    assert isinstance(write, CollectionWriteAudience)
    assert write.allow_anonymous_create is True
