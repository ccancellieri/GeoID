"""Pin the private-catalog collection-routing seed shape.

``collection_router.py`` is config-driven: it resolves WRITE/READ/SEARCH
drivers from ``CollectionRoutingConfig``, falling back to the model
defaults (public PG + public ES) when a collection has no explicit routing
config.  The private-catalog seed must therefore pin ALL FOUR operations so
that WRITE/READ go to PG and INDEX/SEARCH both go to the PRIVATE ES driver.

Without an explicit SEARCH entry, a private collection's
``search_collection_metadata`` call would fall through to the model default
``collection_elasticsearch_driver`` — the PUBLIC ES index — leaking private
collection envelopes into public search results.
"""
from __future__ import annotations


def test_private_collection_routing_pins_all_four_operations():
    """A config-driven collection_router resolves WRITE/READ/SEARCH from
    CollectionRoutingConfig. The private-catalog seed must therefore pin
    every operation: WRITE/READ -> PG, INDEX/SEARCH -> the PRIVATE ES
    driver. Without explicit SEARCH, a private collection's search would
    fall back to the public ES default and leak into public search."""
    from dynastore.modules.catalog.catalog_config import (
        _build_private_collection_routing,
    )
    from dynastore.modules.storage.routing_config import Operation

    crc = _build_private_collection_routing()
    ops = crc.operations
    assert [e.driver_ref for e in ops[Operation.WRITE]] == ["collection_postgresql_driver"]
    assert [e.driver_ref for e in ops[Operation.READ]] == ["collection_postgresql_driver"]
    assert ops[Operation.INDEX][0].driver_ref == "collection_elasticsearch_private_driver"
    assert ops[Operation.SEARCH][0].driver_ref == "collection_elasticsearch_private_driver"
