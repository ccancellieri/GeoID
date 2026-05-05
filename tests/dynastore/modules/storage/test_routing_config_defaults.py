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
        if e.driver_id == "items_postgresql_driver"
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
        if e.driver_id == "items_elasticsearch_driver"
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
    es_read = next(e for e in read if e.driver_id == "items_elasticsearch_driver")
    pg_read = next(e for e in read if e.driver_id == "items_postgresql_driver")
    assert "geometry_simplified" in es_read.hints
    assert "geometry_exact" in pg_read.hints
    # READ defaults retain their existing failure semantics — flag if changed.
    assert pg_read.on_failure == FailurePolicy.FATAL
