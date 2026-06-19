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

"""Tests for config-driven driver resolution in collection_router.

Verifies that when ``resolve_routed`` returns entries the router uses the
config-ordered drivers, and that it falls back to ``_resolve_drivers``
discovery when ``resolve_routed`` returns an empty list (e.g. early boot
with ConfigsProtocol absent).
"""

from __future__ import annotations

from dynastore.models.protocols.entity_store import EntityStoreCapability


class _RecordingDriver:
    """Minimal CollectionStore stand-in that records calls."""

    capabilities = frozenset({
        EntityStoreCapability.READ,
        EntityStoreCapability.WRITE,
        EntityStoreCapability.SEARCH,
    })

    def __init__(self, name: str):
        self.name = name
        self.calls: list[str] = []

    async def get_metadata(self, catalog_id, collection_id, *, context=None, db_resource=None):
        self.calls.append("get")
        return {"id": collection_id, "_src": self.name}

    async def upsert_metadata(self, catalog_id, collection_id, metadata, *, db_resource=None):
        self.calls.append("upsert")

    async def delete_metadata(self, catalog_id, collection_id, *, soft=False, db_resource=None):
        self.calls.append("delete")

    async def search_metadata(
        self,
        catalog_id,
        *,
        q=None,
        bbox=None,
        datetime_range=None,
        filter_cql=None,
        limit=100,
        offset=0,
        context=None,
        db_resource=None,
    ):
        self.calls.append("search")
        return ([{"id": "c1", "_src": self.name}], 1)


async def test_read_uses_config_resolved_drivers(monkeypatch):
    from dynastore.modules.catalog import collection_router
    from dynastore.modules.storage.routing_config import Operation, OperationDriverEntry

    pg = _RecordingDriver("pg")

    async def _fake_resolve(rpc, operation, catalog_id, collection_id=None, *, hints=frozenset(), db_resource=None):
        if operation == Operation.READ:
            return [(OperationDriverEntry(driver_ref="collection_postgresql_driver"), pg)]
        return []

    monkeypatch.setattr(collection_router, "resolve_routed", _fake_resolve)
    result = await collection_router.get_collection_metadata("cat", "coll")
    assert result == {"id": "coll", "_src": "pg"}
    assert pg.calls == ["get"]


async def test_read_falls_back_to_discovery_when_resolver_empty(monkeypatch):
    from dynastore.modules.catalog import collection_router

    disco = _RecordingDriver("disco")

    async def _empty_resolve(*a, **kw):
        return []

    monkeypatch.setattr(collection_router, "resolve_routed", _empty_resolve)
    monkeypatch.setattr(collection_router, "_resolve_drivers", lambda: [disco])
    result = await collection_router.get_collection_metadata("cat", "coll")
    assert result == {"id": "coll", "_src": "disco"}


async def test_write_fans_out_to_config_resolved_drivers(monkeypatch):
    from dynastore.modules.catalog import collection_router
    from dynastore.modules.storage.routing_config import Operation, OperationDriverEntry

    pg = _RecordingDriver("pg")

    async def _fake_resolve(rpc, operation, catalog_id, collection_id=None, *, hints=frozenset(), db_resource=None):
        if operation == Operation.WRITE:
            return [(OperationDriverEntry(driver_ref="collection_postgresql_driver"), pg)]
        return []

    # no-op INDEX dispatch — exercised in Task 6
    async def _noop_dispatch(*a, **kw):
        return None

    monkeypatch.setattr(collection_router, "resolve_routed", _fake_resolve)
    monkeypatch.setattr(collection_router, "_dispatch_collection_index", _noop_dispatch)
    await collection_router.upsert_collection_metadata("cat", "coll", {"id": "coll"})
    assert pg.calls == ["upsert"]


async def test_search_uses_first_config_resolved_driver(monkeypatch):
    from dynastore.modules.catalog import collection_router
    from dynastore.modules.storage.routing_config import Operation, OperationDriverEntry

    es = _RecordingDriver("es")
    pg = _RecordingDriver("pg")

    async def _fake_resolve(rpc, operation, catalog_id, collection_id=None, *, hints=frozenset(), db_resource=None):
        if operation == Operation.SEARCH:
            return [
                (OperationDriverEntry(driver_ref="collection_elasticsearch_driver"), es),
                (OperationDriverEntry(driver_ref="collection_postgresql_driver"), pg),
            ]
        return []

    monkeypatch.setattr(collection_router, "resolve_routed", _fake_resolve)
    rows, total = await collection_router.search_collection_metadata("cat", q="x")
    assert total == 1 and rows[0]["_src"] == "es"
    assert es.calls == ["search"] and pg.calls == []


import pytest  # noqa: E402 — appended after existing non-pytest tests


@pytest.mark.asyncio
async def test_search_operation_kwarg_selects_read_routed_drivers(monkeypatch):
    """``operation=READ`` resolves the READ-routed driver list (PG, the system
    of record) instead of the SEARCH slice — the routing-driven fallback the
    collection service uses when an ES-only SEARCH slice has no rows yet."""
    from dynastore.modules.catalog import collection_router
    from dynastore.modules.storage.routing_config import Operation, OperationDriverEntry

    es = _RecordingDriver("es")
    pg = _RecordingDriver("pg")

    async def _fake_resolve(rpc, operation, catalog_id, collection_id=None, *, hints=frozenset(), db_resource=None):
        if operation == Operation.SEARCH:
            return [(OperationDriverEntry(driver_ref="collection_elasticsearch_driver"), es)]
        if operation == Operation.READ:
            return [(OperationDriverEntry(driver_ref="collection_postgresql_driver"), pg)]
        return []

    monkeypatch.setattr(collection_router, "resolve_routed", _fake_resolve)
    rows, total = await collection_router.search_collection_metadata(
        "cat", q="x", operation=Operation.READ,
    )
    assert total == 1 and rows[0]["_src"] == "pg"
    assert pg.calls == ["search"] and es.calls == []


@pytest.mark.asyncio
async def test_upsert_dispatches_index_hop(monkeypatch):
    """After a successful WRITE fan-out, upsert_collection_metadata must
    dispatch the envelope to the INDEX hop so ES gets populated."""
    from dynastore.modules.catalog import collection_router
    from dynastore.modules.storage.routing_config import Operation, OperationDriverEntry

    pg = _RecordingDriver("pg")
    dispatched: list[tuple] = []

    async def _fake_resolve(rpc, operation, catalog_id, collection_id=None, *, hints=frozenset(), db_resource=None):
        if operation == Operation.WRITE:
            return [(OperationDriverEntry(driver_ref="collection_postgresql_driver"), pg)]
        return []

    class _FakeDispatcher:
        async def fan_out_bulk(self, ctx, ops):
            dispatched.append((ctx.catalog, ctx.collection, len(ops)))

    monkeypatch.setattr(collection_router, "resolve_routed", _fake_resolve)
    monkeypatch.setattr(
        collection_router, "_get_index_dispatcher", lambda: _FakeDispatcher(),
    )
    await collection_router.upsert_collection_metadata("cat", "coll", {"id": "coll"})
    assert pg.calls == ["upsert"]
    assert dispatched == [("cat", "coll", 1)]


@pytest.mark.asyncio
async def test_index_dispatch_failure_does_not_break_write(monkeypatch, caplog):
    """A non-FATAL INDEX dispatch failure is logged, not raised — the PG
    WRITE already succeeded and must stand."""
    import logging

    from dynastore.modules.catalog import collection_router
    from dynastore.modules.storage.routing_config import Operation, OperationDriverEntry

    pg = _RecordingDriver("pg")

    async def _fake_resolve(rpc, operation, catalog_id, collection_id=None, *, hints=frozenset(), db_resource=None):
        if operation == Operation.WRITE:
            return [(OperationDriverEntry(driver_ref="collection_postgresql_driver"), pg)]
        return []

    class _BoomDispatcher:
        async def fan_out_bulk(self, ctx, ops):
            raise RuntimeError("synthetic ES outage")

    monkeypatch.setattr(collection_router, "resolve_routed", _fake_resolve)
    monkeypatch.setattr(
        collection_router, "_get_index_dispatcher", lambda: _BoomDispatcher(),
    )
    with caplog.at_level(logging.WARNING, logger=collection_router.__name__):
        await collection_router.upsert_collection_metadata("cat", "coll", {"id": "coll"})
    assert pg.calls == ["upsert"]
    assert any("index" in r.message.lower() for r in caplog.records)


@pytest.mark.asyncio
async def test_delete_dispatches_index_hop(monkeypatch):
    """After a clean DELETE fan-out, delete_collection_metadata must dispatch
    a ``delete`` op to the INDEX hop so ES drops the doc too — without it a
    deleted collection lingers in ``dynastore-collections`` until reindex."""
    from dynastore.modules.catalog import collection_router
    from dynastore.modules.storage.routing_config import Operation, OperationDriverEntry

    pg = _RecordingDriver("pg")
    dispatched: list[tuple] = []

    async def _fake_resolve(rpc, operation, catalog_id, collection_id=None, *, hints=frozenset(), db_resource=None):
        if operation == Operation.WRITE:
            return [(OperationDriverEntry(driver_ref="collection_postgresql_driver"), pg)]
        return []

    class _FakeDispatcher:
        async def fan_out_bulk(self, ctx, ops):
            dispatched.append(
                (ctx.catalog, ctx.collection, ops[0].op_type, ops[0].entity_type)
            )

    monkeypatch.setattr(collection_router, "resolve_routed", _fake_resolve)
    monkeypatch.setattr(
        collection_router, "_get_index_dispatcher", lambda: _FakeDispatcher(),
    )
    await collection_router.delete_collection_metadata("cat", "coll")
    assert pg.calls == ["delete"]
    assert dispatched == [("cat", "coll", "delete", "collection")]


@pytest.mark.asyncio
async def test_delete_skips_index_hop_when_driver_failed(monkeypatch):
    """A failed DELETE fan-out must NOT dispatch the INDEX hop — the PG
    delete didn't land, so ES must keep the doc until a clean retry."""
    from dynastore.modules.catalog import collection_router
    from dynastore.modules.storage.routing_config import Operation, OperationDriverEntry

    class _BoomDriver(_RecordingDriver):
        async def delete_metadata(self, catalog_id, collection_id, *, soft=False, db_resource=None):
            raise RuntimeError("synthetic PG outage")

    boom = _BoomDriver("boom")
    dispatched: list = []

    async def _fake_resolve(rpc, operation, catalog_id, collection_id=None, *, hints=frozenset(), db_resource=None):
        if operation == Operation.WRITE:
            return [(OperationDriverEntry(driver_ref="collection_postgresql_driver"), boom)]
        return []

    class _FakeDispatcher:
        async def fan_out_bulk(self, ctx, ops):
            dispatched.append(ops)

    monkeypatch.setattr(collection_router, "resolve_routed", _fake_resolve)
    monkeypatch.setattr(
        collection_router, "_get_index_dispatcher", lambda: _FakeDispatcher(),
    )
    with pytest.raises(RuntimeError, match="synthetic PG outage"):
        await collection_router.delete_collection_metadata("cat", "coll")
    assert dispatched == []


@pytest.mark.asyncio
async def test_write_filters_non_capable_config_resolved_driver(monkeypatch, caplog):
    """A config-pinned driver lacking WRITE capability must be dropped from
    the fan-out (parity with the discovery path) — not invoked, which would
    raise ``AttributeError``/``NotImplementedError`` mid-fan-out."""
    import logging

    from dynastore.modules.catalog import collection_router
    from dynastore.modules.storage.routing_config import Operation, OperationDriverEntry

    class _ReadOnlyDriver(_RecordingDriver):
        capabilities = frozenset({EntityStoreCapability.READ})

    write = _RecordingDriver("write")
    readonly = _ReadOnlyDriver("readonly")

    async def _fake_resolve(rpc, operation, catalog_id, collection_id=None, *, hints=frozenset(), db_resource=None):
        if operation == Operation.WRITE:
            return [
                (OperationDriverEntry(driver_ref="collection_postgresql_driver"), write),
                (OperationDriverEntry(driver_ref="collection_readonly_driver"), readonly),
            ]
        return []

    async def _noop_dispatch(*a, **kw):
        return None

    monkeypatch.setattr(collection_router, "resolve_routed", _fake_resolve)
    monkeypatch.setattr(collection_router, "_dispatch_collection_index", _noop_dispatch)
    with caplog.at_level(logging.WARNING, logger=collection_router.__name__):
        await collection_router.upsert_collection_metadata("cat", "coll", {"id": "coll"})
    assert write.calls == ["upsert"]
    assert readonly.calls == []
    assert any("capability" in r.message.lower() for r in caplog.records)


@pytest.mark.asyncio
async def test_delete_filters_non_capable_config_resolved_driver(monkeypatch, caplog):
    """The DELETE fan-out must apply ``_filter_capable`` on the config-resolved
    branch too (item-3 parity) — a config-pinned driver lacking WRITE
    capability is dropped, not invoked.  Without the filter ``delete_metadata``
    would dispatch to a non-WRITE driver and raise mid-fan-out."""
    import logging

    from dynastore.modules.catalog import collection_router
    from dynastore.modules.storage.routing_config import Operation, OperationDriverEntry

    class _ReadOnlyDriver(_RecordingDriver):
        capabilities = frozenset({EntityStoreCapability.READ})

    write = _RecordingDriver("write")
    readonly = _ReadOnlyDriver("readonly")

    async def _fake_resolve(rpc, operation, catalog_id, collection_id=None, *, hints=frozenset(), db_resource=None):
        if operation == Operation.WRITE:
            return [
                (OperationDriverEntry(driver_ref="collection_postgresql_driver"), write),
                (OperationDriverEntry(driver_ref="collection_readonly_driver"), readonly),
            ]
        return []

    async def _noop_dispatch(*a, **kw):
        return None

    monkeypatch.setattr(collection_router, "resolve_routed", _fake_resolve)
    monkeypatch.setattr(collection_router, "_dispatch_collection_index", _noop_dispatch)
    with caplog.at_level(logging.WARNING, logger=collection_router.__name__):
        await collection_router.delete_collection_metadata("cat", "coll")
    assert write.calls == ["delete"]
    assert readonly.calls == []
    assert any("capability" in r.message.lower() for r in caplog.records)
