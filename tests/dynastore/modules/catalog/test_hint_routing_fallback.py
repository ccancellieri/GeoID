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

"""Task D.3 — metadata router hint-routing + fallback tests.

Verifies that:
- With hints={GEOMETRY_SIMPLIFIED}, get_collection_metadata / get_catalog_metadata
  resolves ES first.
- When ES returns None, the router falls through to PG and returns its result.
- With no hints, only PG is consulted (ES.get_metadata NOT called).
"""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock

from dynastore.models.protocols.entity_store import EntityStoreCapability
from dynastore.modules.storage.hints import Hint


class _FakeCollectionDriver:
    """Minimal CollectionStore stand-in."""
    capabilities = frozenset({EntityStoreCapability.READ, EntityStoreCapability.WRITE})

    def __init__(self, name: str, return_value=None):
        self.name = name
        self._return_value = return_value
        self.call_count = 0

    async def get_metadata(self, catalog_id, collection_id, *, context=None, db_resource=None):
        self.call_count += 1
        return self._return_value

    async def upsert_metadata(self, catalog_id, collection_id, metadata, *, db_resource=None):
        pass

    async def delete_metadata(self, catalog_id, collection_id, *, soft=False, db_resource=None):
        pass

    async def search_metadata(self, catalog_id, *, q=None, bbox=None, datetime_range=None,
                               filter_cql=None, limit=100, offset=0, context=None, db_resource=None):
        return ([], 0)


class _FakeCatalogDriver:
    """Minimal CatalogStore stand-in."""
    capabilities = frozenset({EntityStoreCapability.READ, EntityStoreCapability.WRITE})

    def __init__(self, name: str, return_value=None):
        self.name = name
        self._return_value = return_value
        self.call_count = 0

    async def get_catalog_metadata(self, catalog_id, *, context=None, db_resource=None):
        self.call_count += 1
        return self._return_value

    async def upsert_catalog_metadata(self, catalog_id, metadata, *, db_resource=None):
        pass

    async def delete_catalog_metadata(self, catalog_id, *, soft=False, db_resource=None):
        pass


# ---------------------------------------------------------------------------
# collection_router.get_collection_metadata
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_collection_metadata_hinted_resolves_es_first(monkeypatch):
    """With hints={GEOMETRY_SIMPLIFIED}, the router resolves ES first; when ES
    returns a document the result is returned immediately without calling PG."""
    from dynastore.modules.catalog import collection_router
    from dynastore.modules.storage.routing_config import Operation, OperationDriverEntry

    es = _FakeCollectionDriver("es", return_value={"id": "col1", "_src": "es"})
    pg = _FakeCollectionDriver("pg", return_value={"id": "col1", "_src": "pg"})

    async def _fake_resolve(rpc, operation, catalog_id, collection_id=None, *, hints=frozenset(), db_resource=None):
        if operation == Operation.READ:
            # ES first (matched), PG as fallback tail
            return [
                (OperationDriverEntry(driver_ref="collection_elasticsearch_driver"), es),
                (OperationDriverEntry(driver_ref="collection_postgresql_driver"), pg),
            ]
        return []

    monkeypatch.setattr(collection_router, "resolve_routed", _fake_resolve)
    result = await collection_router.get_collection_metadata(
        "cat", "col1", hints=frozenset({Hint.GEOMETRY_SIMPLIFIED}),
    )
    assert result == {"id": "col1", "_src": "es"}
    assert es.call_count == 1
    # ES returned data → PG must NOT be consulted
    assert pg.call_count == 0


@pytest.mark.asyncio
async def test_collection_metadata_hinted_falls_through_to_pg_when_es_returns_none(monkeypatch):
    """When ES returns None for a hinted READ, the router falls through to PG
    and returns PG's result."""
    from dynastore.modules.catalog import collection_router
    from dynastore.modules.storage.routing_config import Operation, OperationDriverEntry

    es = _FakeCollectionDriver("es", return_value=None)
    pg = _FakeCollectionDriver("pg", return_value={"id": "col1", "_src": "pg"})

    async def _fake_resolve(rpc, operation, catalog_id, collection_id=None, *, hints=frozenset(), db_resource=None):
        if operation == Operation.READ:
            return [
                (OperationDriverEntry(driver_ref="collection_elasticsearch_driver"), es),
                (OperationDriverEntry(driver_ref="collection_postgresql_driver"), pg),
            ]
        return []

    monkeypatch.setattr(collection_router, "resolve_routed", _fake_resolve)
    result = await collection_router.get_collection_metadata(
        "cat", "col1", hints=frozenset({Hint.GEOMETRY_SIMPLIFIED}),
    )
    # ES missed → fell through to PG
    assert result == {"id": "col1", "_src": "pg"}
    assert es.call_count == 1
    assert pg.call_count == 1


@pytest.mark.asyncio
async def test_collection_metadata_no_hints_only_uses_pg(monkeypatch):
    """With no hints, the router uses merge-all semantics on the declared list.
    Callers that pass no hints get PG's data; ES is not called if PG is the only
    resolved driver."""
    from dynastore.modules.catalog import collection_router
    from dynastore.modules.storage.routing_config import Operation, OperationDriverEntry

    pg = _FakeCollectionDriver("pg", return_value={"id": "col1", "_src": "pg"})
    es = _FakeCollectionDriver("es", return_value={"id": "col1", "_src": "es"})

    async def _fake_resolve(rpc, operation, catalog_id, collection_id=None, *, hints=frozenset(), db_resource=None):
        if operation == Operation.READ:
            # No-hint path: only PG in the resolved list (as in a PG-only deployment)
            return [
                (OperationDriverEntry(driver_ref="collection_postgresql_driver"), pg),
            ]
        return []

    monkeypatch.setattr(collection_router, "resolve_routed", _fake_resolve)
    result = await collection_router.get_collection_metadata("cat", "col1")
    assert result == {"id": "col1", "_src": "pg"}
    assert pg.call_count == 1
    # ES was not in the resolved list — must not have been called
    assert es.call_count == 0


# ---------------------------------------------------------------------------
# catalog_router.get_catalog_metadata
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_catalog_metadata_hinted_resolves_es_first(monkeypatch):
    """With hints={GEOMETRY_SIMPLIFIED}, catalog_router resolves ES first; when ES
    returns a document the result is returned without calling PG."""
    from dynastore.modules.catalog import catalog_router
    from dynastore.modules.storage.routing_config import Operation, OperationDriverEntry

    es = _FakeCatalogDriver("es", return_value={"id": "cat", "_src": "es"})
    pg = _FakeCatalogDriver("pg", return_value={"id": "cat", "_src": "pg"})

    async def _fake_resolve(rpc, operation, catalog_id, collection_id=None, *, hints=frozenset(), db_resource=None):
        if operation == Operation.READ:
            return [
                (OperationDriverEntry(driver_ref="catalog_elasticsearch_driver"), es),
                (OperationDriverEntry(driver_ref="catalog_postgresql_driver"), pg),
            ]
        return []

    monkeypatch.setattr(catalog_router, "resolve_routed", _fake_resolve)
    result = await catalog_router.get_catalog_metadata(
        "cat", hints=frozenset({Hint.GEOMETRY_SIMPLIFIED}),
    )
    assert result == {"id": "cat", "_src": "es"}
    assert es.call_count == 1
    assert pg.call_count == 0


@pytest.mark.asyncio
async def test_catalog_metadata_hinted_falls_through_to_pg_when_es_returns_none(monkeypatch):
    """When ES returns None for a hinted catalog READ, the router falls through to PG."""
    from dynastore.modules.catalog import catalog_router
    from dynastore.modules.storage.routing_config import Operation, OperationDriverEntry

    es = _FakeCatalogDriver("es", return_value=None)
    pg = _FakeCatalogDriver("pg", return_value={"id": "cat", "_src": "pg"})

    async def _fake_resolve(rpc, operation, catalog_id, collection_id=None, *, hints=frozenset(), db_resource=None):
        if operation == Operation.READ:
            return [
                (OperationDriverEntry(driver_ref="catalog_elasticsearch_driver"), es),
                (OperationDriverEntry(driver_ref="catalog_postgresql_driver"), pg),
            ]
        return []

    monkeypatch.setattr(catalog_router, "resolve_routed", _fake_resolve)
    result = await catalog_router.get_catalog_metadata(
        "cat", hints=frozenset({Hint.GEOMETRY_SIMPLIFIED}),
    )
    assert result == {"id": "cat", "_src": "pg"}
    assert es.call_count == 1
    assert pg.call_count == 1


@pytest.mark.asyncio
async def test_catalog_metadata_no_hints_only_uses_pg(monkeypatch):
    """With no hints, catalog_router uses merge-all on the resolved list. When only PG
    is resolved, ES is never called."""
    from dynastore.modules.catalog import catalog_router
    from dynastore.modules.storage.routing_config import Operation, OperationDriverEntry

    pg = _FakeCatalogDriver("pg", return_value={"id": "cat", "_src": "pg"})
    es = _FakeCatalogDriver("es", return_value={"id": "cat", "_src": "es"})

    async def _fake_resolve(rpc, operation, catalog_id, collection_id=None, *, hints=frozenset(), db_resource=None):
        if operation == Operation.READ:
            return [
                (OperationDriverEntry(driver_ref="catalog_postgresql_driver"), pg),
            ]
        return []

    monkeypatch.setattr(catalog_router, "resolve_routed", _fake_resolve)
    result = await catalog_router.get_catalog_metadata("cat")
    assert result == {"id": "cat", "_src": "pg"}
    assert pg.call_count == 1
    assert es.call_count == 0
