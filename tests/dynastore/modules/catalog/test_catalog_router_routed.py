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

"""Tests for config-driven driver resolution in catalog_router.

Verifies that when ``resolve_routed`` returns entries the router uses the
config-ordered drivers, and that it falls back to
``_resolve_catalog_store_drivers`` discovery when ``resolve_routed`` returns
an empty list (e.g. early boot with ConfigsProtocol absent).
"""

from __future__ import annotations

import pytest

from dynastore.models.protocols.entity_store import EntityStoreCapability


class _RecordingCatalogDriver:
    """Minimal CatalogStore stand-in that records calls."""

    capabilities = frozenset({EntityStoreCapability.READ, EntityStoreCapability.WRITE})

    def __init__(self, name: str):
        self.name = name
        self.calls: list[str] = []

    async def get_catalog_metadata(self, catalog_id, *, context=None, db_resource=None):
        self.calls.append("get")
        return {"id": catalog_id, "_src": self.name}

    async def upsert_catalog_metadata(self, catalog_id, metadata, *, db_resource=None):
        self.calls.append("upsert")

    async def delete_catalog_metadata(self, catalog_id, *, soft=False, db_resource=None):
        self.calls.append("delete")


@pytest.mark.asyncio
async def test_catalog_read_uses_config_resolved_drivers(monkeypatch):
    from dynastore.modules.catalog import catalog_router
    from dynastore.modules.storage.routing_config import Operation, OperationDriverEntry

    pg = _RecordingCatalogDriver("pg")

    async def _fake_resolve(rpc, operation, catalog_id, collection_id=None, *, hints=frozenset(), db_resource=None):
        if operation == Operation.READ:
            return [(OperationDriverEntry(driver_ref="catalog_postgresql_driver"), pg)]
        return []

    monkeypatch.setattr(catalog_router, "resolve_routed", _fake_resolve)
    result = await catalog_router.get_catalog_metadata("cat")
    assert result == {"id": "cat", "_src": "pg"}
    assert pg.calls == ["get"]


@pytest.mark.asyncio
async def test_catalog_read_falls_back_to_discovery(monkeypatch):
    from dynastore.modules.catalog import catalog_router

    disco = _RecordingCatalogDriver("disco")

    async def _empty_resolve(*a, **kw):
        return []

    monkeypatch.setattr(catalog_router, "resolve_routed", _empty_resolve)
    monkeypatch.setattr(catalog_router, "_resolve_catalog_store_drivers", lambda: [disco])
    result = await catalog_router.get_catalog_metadata("cat")
    assert result == {"id": "cat", "_src": "disco"}


@pytest.mark.asyncio
async def test_catalog_write_fans_out_to_config_resolved_drivers(monkeypatch):
    from dynastore.modules.catalog import catalog_router
    from dynastore.modules.storage.routing_config import Operation, OperationDriverEntry

    pg = _RecordingCatalogDriver("pg")

    async def _fake_resolve(rpc, operation, catalog_id, collection_id=None, *, hints=frozenset(), db_resource=None):
        if operation == Operation.WRITE:
            return [(OperationDriverEntry(driver_ref="catalog_postgresql_driver"), pg)]
        return []

    # no-op event emission — exercised separately
    async def _noop_emit(**kw):
        return None

    monkeypatch.setattr(catalog_router, "resolve_routed", _fake_resolve)
    monkeypatch.setattr(catalog_router, "_emit_catalog_metadata_changed", _noop_emit)
    await catalog_router.upsert_catalog_metadata("cat", {"id": "cat"})
    assert pg.calls == ["upsert"]


@pytest.mark.asyncio
async def test_catalog_write_filters_non_capable_config_resolved_driver(monkeypatch, caplog):
    """A config-pinned CatalogStore driver lacking WRITE capability must be
    dropped from the fan-out (parity with the discovery path)."""
    import logging

    from dynastore.modules.catalog import catalog_router
    from dynastore.modules.storage.routing_config import Operation, OperationDriverEntry

    class _ReadOnlyCatalogDriver(_RecordingCatalogDriver):
        capabilities = frozenset({EntityStoreCapability.READ})

    write = _RecordingCatalogDriver("write")
    readonly = _ReadOnlyCatalogDriver("readonly")

    async def _fake_resolve(rpc, operation, catalog_id, collection_id=None, *, hints=frozenset(), db_resource=None):
        if operation == Operation.WRITE:
            return [
                (OperationDriverEntry(driver_ref="catalog_postgresql_driver"), write),
                (OperationDriverEntry(driver_ref="catalog_readonly_driver"), readonly),
            ]
        return []

    async def _noop_emit(**kw):
        return None

    monkeypatch.setattr(catalog_router, "resolve_routed", _fake_resolve)
    monkeypatch.setattr(catalog_router, "_emit_catalog_metadata_changed", _noop_emit)
    with caplog.at_level(logging.WARNING, logger=catalog_router.__name__):
        await catalog_router.upsert_catalog_metadata("cat", {"id": "cat"})
    assert write.calls == ["upsert"]
    assert readonly.calls == []
    assert any("capability" in r.message.lower() for r in caplog.records)


@pytest.mark.asyncio
async def test_catalog_delete_filters_non_capable_config_resolved_driver(monkeypatch, caplog):
    """The DELETE fan-out must apply ``_filter_capable`` on the config-resolved
    branch too (item-3 parity) — a config-pinned CatalogStore driver lacking
    WRITE capability is dropped, not invoked."""
    import logging

    from dynastore.modules.catalog import catalog_router
    from dynastore.modules.storage.routing_config import Operation, OperationDriverEntry

    class _ReadOnlyCatalogDriver(_RecordingCatalogDriver):
        capabilities = frozenset({EntityStoreCapability.READ})

    write = _RecordingCatalogDriver("write")
    readonly = _ReadOnlyCatalogDriver("readonly")

    async def _fake_resolve(rpc, operation, catalog_id, collection_id=None, *, hints=frozenset(), db_resource=None):
        if operation == Operation.WRITE:
            return [
                (OperationDriverEntry(driver_ref="catalog_postgresql_driver"), write),
                (OperationDriverEntry(driver_ref="catalog_readonly_driver"), readonly),
            ]
        return []

    async def _noop_emit(**kw):
        return None

    monkeypatch.setattr(catalog_router, "resolve_routed", _fake_resolve)
    monkeypatch.setattr(catalog_router, "_emit_catalog_metadata_changed", _noop_emit)
    with caplog.at_level(logging.WARNING, logger=catalog_router.__name__):
        await catalog_router.delete_catalog_metadata("cat")
    assert write.calls == ["delete"]
    assert readonly.calls == []
    assert any("capability" in r.message.lower() for r in caplog.records)
