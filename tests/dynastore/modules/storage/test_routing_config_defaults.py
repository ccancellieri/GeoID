"""Pin routing defaults so a future edit can't silently flip ES back
from OUTBOX without explicit test update. PG must remain FATAL —
non-negotiable for authoritative writes."""
from __future__ import annotations


def test_default_pg_write_is_sync_fatal():
    from dynastore.modules.storage.routing_config import (
        FailurePolicy, ItemsRoutingConfig, Operation, WriteMode,
    )
    cfg = ItemsRoutingConfig()
    pg = next(
        e for e in cfg.operations[Operation.WRITE]
        if e.driver_ref == "items_postgresql_driver"
    )
    assert pg.on_failure == FailurePolicy.FATAL
    assert pg.write_mode == WriteMode.SYNC


def test_default_es_write_is_async_outbox():
    from dynastore.modules.storage.routing_config import (
        FailurePolicy, ItemsRoutingConfig, Operation, WriteMode,
    )
    cfg = ItemsRoutingConfig()
    es = next(
        e for e in cfg.operations[Operation.WRITE]
        if e.driver_ref == "items_elasticsearch_driver"
    )
    assert es.on_failure == FailurePolicy.OUTBOX
    assert es.write_mode == WriteMode.ASYNC


def test_default_read_routing_unchanged():
    """Sanity guard — Task 11 only touches WRITE; READ entries
    (ES geometry_simplified primary, PG geometry_exact) must remain."""
    from dynastore.modules.storage.routing_config import (
        FailurePolicy, ItemsRoutingConfig, Operation,
    )
    cfg = ItemsRoutingConfig()
    read = cfg.operations[Operation.READ]
    es_read = next(e for e in read if e.driver_ref == "items_elasticsearch_driver")
    pg_read = next(e for e in read if e.driver_ref == "items_postgresql_driver")
    assert "geometry_simplified" in es_read.hints
    assert "geometry_exact" in pg_read.hints
    # READ defaults retain their existing failure semantics — flag if changed.
    assert pg_read.on_failure == FailurePolicy.FATAL


def test_collection_routing_default_write_is_pg_fatal():
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, FailurePolicy, Operation, WriteMode,
    )
    cfg = CollectionRoutingConfig()
    write = cfg.operations[Operation.WRITE]
    assert [e.driver_ref for e in write] == ["collection_postgresql_driver"]
    assert write[0].on_failure == FailurePolicy.FATAL
    assert write[0].write_mode == WriteMode.SYNC


def test_collection_routing_default_read_is_pg_primary():
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, FailurePolicy, Operation,
    )
    cfg = CollectionRoutingConfig()
    read = cfg.operations[Operation.READ]
    assert [e.driver_ref for e in read] == ["collection_postgresql_driver"]
    assert read[0].on_failure == FailurePolicy.FATAL


def test_collection_routing_default_index_is_es_async_outbox():
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, FailurePolicy, Operation, WriteMode,
    )
    cfg = CollectionRoutingConfig()
    index = cfg.operations[Operation.INDEX]
    es = next(e for e in index if e.driver_ref == "collection_elasticsearch_driver")
    assert es.write_mode == WriteMode.ASYNC
    assert es.on_failure == FailurePolicy.OUTBOX


def test_collection_routing_default_search_is_es_first_pg_fallback():
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, Operation,
    )
    cfg = CollectionRoutingConfig()
    search = cfg.operations[Operation.SEARCH]
    refs = [e.driver_ref for e in search]
    assert refs[0] == "collection_elasticsearch_driver"
    assert "collection_postgresql_driver" in refs


def test_collection_routing_default_search_carries_geometry_hints():
    """SEARCH entries declare which geometry precision they serve so a
    consumer can route to PG via hint='geometry_exact'. ES carries
    GEOMETRY_SIMPLIFIED (fast, lossy); PG carries GEOMETRY_EXACT (full WKB).
    Mirrors ItemsRoutingConfig READ defaults."""
    from dynastore.modules.storage.hints import Hint
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, Operation,
    )
    cfg = CollectionRoutingConfig()
    search = cfg.operations[Operation.SEARCH]
    es = next(e for e in search if e.driver_ref == "collection_elasticsearch_driver")
    pg = next(e for e in search if e.driver_ref == "collection_postgresql_driver")
    assert Hint.GEOMETRY_SIMPLIFIED in es.hints
    assert Hint.GEOMETRY_EXACT in pg.hints


def test_catalog_routing_default_refs_are_registered():
    """The default WRITE/READ entries must reference an actually-registered
    driver_ref. catalog_core_postgresql_driver / catalog_stac_postgresql_driver
    were never registered as entry-points — the registered wrapper is
    catalog_postgresql_driver."""
    from dynastore.modules.storage.routing_config import (
        CatalogRoutingConfig, Operation,
    )
    cfg = CatalogRoutingConfig()
    write_refs = [e.driver_ref for e in cfg.operations[Operation.WRITE]]
    read_refs = [e.driver_ref for e in cfg.operations[Operation.READ]]
    assert write_refs == ["catalog_postgresql_driver"]
    assert read_refs == ["catalog_postgresql_driver"]
    for ref in write_refs + read_refs:
        assert ref not in (
            "catalog_core_postgresql_driver",
            "catalog_stac_postgresql_driver",
        ), f"{ref} is not a registered entry-point"


def test_catalog_routing_default_write_is_fatal():
    from dynastore.modules.storage.routing_config import (
        CatalogRoutingConfig, FailurePolicy, Operation,
    )
    cfg = CatalogRoutingConfig()
    assert cfg.operations[Operation.WRITE][0].on_failure == FailurePolicy.FATAL
