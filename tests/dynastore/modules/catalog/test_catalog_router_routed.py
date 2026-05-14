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

    async def _fake_resolve(rpc, operation, catalog_id, collection_id=None, *, db_resource=None):
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

    async def _fake_resolve(rpc, operation, catalog_id, collection_id=None, *, db_resource=None):
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
