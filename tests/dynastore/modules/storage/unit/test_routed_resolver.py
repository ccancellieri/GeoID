from __future__ import annotations

import logging
import pytest


class _FakePgDriver:
    pass


class _FakeEsDriver:
    pass


@pytest.mark.asyncio
async def test_resolve_routed_uses_config_entries(monkeypatch, caplog):
    from dynastore.modules.storage import routed_resolver
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, Operation, OperationDriverEntry,
    )

    cfg = CollectionRoutingConfig(
        operations={
            Operation.READ: [
                OperationDriverEntry(driver_ref="collection_postgresql_driver"),
            ],
        }
    )

    async def _fake_load(rpc, catalog_id, collection_id, db_resource):
        return cfg

    monkeypatch.setattr(routed_resolver, "_load_routing_config", _fake_load)
    monkeypatch.setattr(
        routed_resolver, "_index_for",
        lambda rpc: {"collection_postgresql_driver": _FakePgDriver()},
    )

    with caplog.at_level(logging.DEBUG, logger=routed_resolver.__name__):
        resolved = await routed_resolver.resolve_routed(
            CollectionRoutingConfig, Operation.READ, "cat", "coll",
        )
    assert [e.driver_ref for e, _ in resolved] == ["collection_postgresql_driver"]
    assert isinstance(resolved[0][1], _FakePgDriver)
    assert any("routed-resolve" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_resolve_routed_falls_back_to_model_default_when_empty(monkeypatch):
    """A stored config with operations={} must fall back to the model's
    default_factory for the requested operation (parity with storage/router.py)."""
    from dynastore.modules.storage import routed_resolver
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, Operation,
    )

    empty = CollectionRoutingConfig.model_construct(operations={})

    async def _fake_load(rpc, catalog_id, collection_id, db_resource):
        return empty

    monkeypatch.setattr(routed_resolver, "_load_routing_config", _fake_load)
    monkeypatch.setattr(
        routed_resolver, "_index_for",
        lambda rpc: {"collection_postgresql_driver": _FakePgDriver()},
    )
    resolved = await routed_resolver.resolve_routed(
        CollectionRoutingConfig, Operation.WRITE, "cat", "coll",
    )
    # default_factory WRITE = [collection_postgresql_driver]
    assert [e.driver_ref for e, _ in resolved] == ["collection_postgresql_driver"]


@pytest.mark.asyncio
async def test_resolve_routed_skips_unregistered_ref(monkeypatch, caplog):
    from dynastore.modules.storage import routed_resolver
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, Operation, OperationDriverEntry,
    )

    cfg = CollectionRoutingConfig(
        operations={
            Operation.READ: [
                OperationDriverEntry(driver_ref="nonexistent_driver"),
                OperationDriverEntry(driver_ref="collection_postgresql_driver"),
            ],
        }
    )

    async def _fake_load(rpc, catalog_id, collection_id, db_resource):
        return cfg

    monkeypatch.setattr(routed_resolver, "_load_routing_config", _fake_load)
    monkeypatch.setattr(
        routed_resolver, "_index_for",
        lambda rpc: {"collection_postgresql_driver": _FakePgDriver()},
    )
    with caplog.at_level(logging.WARNING, logger=routed_resolver.__name__):
        resolved = await routed_resolver.resolve_routed(
            CollectionRoutingConfig, Operation.READ, "cat", "coll",
        )
    assert [e.driver_ref for e, _ in resolved] == ["collection_postgresql_driver"]
    assert any("nonexistent_driver" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_resolve_routed_returns_empty_when_configs_unavailable(monkeypatch):
    """No ConfigsProtocol (early boot) -> resolver returns [] so callers can
    fall back to discovery."""
    from dynastore.modules.storage import routed_resolver
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, Operation,
    )

    async def _raise(*a, **kw):
        raise RuntimeError("ConfigsProtocol not available")

    monkeypatch.setattr(routed_resolver, "_load_routing_config", _raise)
    resolved = await routed_resolver.resolve_routed(
        CollectionRoutingConfig, Operation.READ, "cat", "coll",
    )
    assert resolved == []


@pytest.mark.asyncio
async def test_fallback_first_occurrence_warns_then_demotes_to_debug(
    monkeypatch, caplog,
):
    """#748 item-2 soak signal: first ConfigsProtocol-unavailable per process
    emits WARNING (so it survives default-INFO Cloud Logging); subsequent
    occurrences demote to DEBUG to avoid spam."""
    from dynastore.modules.storage import routed_resolver
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, Operation,
    )

    monkeypatch.setattr(routed_resolver, "_FALLBACK_WARNED", False)

    async def _raise(*a, **kw):
        raise RuntimeError("ConfigsProtocol not available")

    monkeypatch.setattr(routed_resolver, "_load_routing_config", _raise)

    with caplog.at_level(logging.DEBUG, logger=routed_resolver.__name__):
        await routed_resolver.resolve_routed(
            CollectionRoutingConfig, Operation.READ, "cat", "coll",
        )
        await routed_resolver.resolve_routed(
            CollectionRoutingConfig, Operation.WRITE, "cat", "coll",
        )
        await routed_resolver.resolve_routed(
            CollectionRoutingConfig, Operation.WRITE, "cat2", "coll2",
        )

    fallback_records = [
        r for r in caplog.records if "routed-resolve unavailable" in r.message
    ]
    assert len(fallback_records) == 3
    levels = [r.levelname for r in fallback_records]
    assert levels == ["WARNING", "DEBUG", "DEBUG"], (
        f"first emission must be WARNING then demote: got {levels}"
    )
    assert routed_resolver._FALLBACK_WARNED is True


@pytest.mark.asyncio
async def test_fallback_warn_gate_is_module_level_not_per_call(monkeypatch):
    """Guard against a regression that scopes the gate to a local variable —
    the gate must persist across calls (module-level), otherwise every call
    re-emits the WARNING."""
    import inspect
    from dynastore.modules.storage import routed_resolver

    src = inspect.getsource(routed_resolver)
    assert "_FALLBACK_WARNED = False" in src, (
        "module-level _FALLBACK_WARNED gate must exist"
    )
    assert "global _FALLBACK_WARNED" in src, (
        "resolve_routed must declare `global _FALLBACK_WARNED` so the first-"
        "occurrence promotion to WARNING persists across calls"
    )
