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

"""Regression coverage for #2677 — ``update_collection`` (generic metadata
PATCH) must emit ``COLLECTION_UPDATE`` / ``AFTER_COLLECTION_UPDATE``,
matching the parity ``CatalogService.update_catalog`` already had for
catalogs. Prior to this change ``update_collection`` emitted nothing at
all, leaving collection metadata edits unobservable.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.catalog import collection_service as collection_service_mod
from dynastore.modules.catalog.collection_service import CollectionService
from dynastore.modules.catalog.event_service import CatalogEventType


class _FakeConn:
    async def execute(self, *a, **k):
        return None


@asynccontextmanager
async def _stub_txn(_engine):
    yield _FakeConn()


@pytest.fixture
def record_emit(monkeypatch):
    calls: list[tuple] = []

    async def _recorder(event_type, *args, **kwargs):
        calls.append((event_type, kwargs))

    monkeypatch.setattr(
        collection_service_mod, "emit_event", AsyncMock(side_effect=_recorder)
    )
    monkeypatch.setattr(collection_service_mod, "managed_transaction", _stub_txn)
    return calls


@pytest.fixture
def svc(monkeypatch):
    s = CollectionService(engine=MagicMock())

    class _AnyDriverContext:
        def __init__(self, db_resource=None, **kwargs):
            self.db_resource = db_resource

    monkeypatch.setattr(collection_service_mod, "DriverContext", _AnyDriverContext)

    class _FakeCatalogsProtocol:
        async def resolve_catalog_id(self, external_id, allow_missing=False):
            if allow_missing:
                return None
            raise ValueError(f"Catalog '{external_id}' not found.")

    monkeypatch.setattr(
        collection_service_mod, "get_protocol", lambda proto: _FakeCatalogsProtocol()
    )

    async def _resolve_collection_id(_self, catalog_id, collection_id, allow_missing=False):
        return None

    monkeypatch.setattr(
        CollectionService, "resolve_collection_id", _resolve_collection_id
    )

    async def _phys_schema(_self, catalog_id, db_resource=None):
        return "phys_sch_test"

    monkeypatch.setattr(CollectionService, "_resolve_physical_schema", _phys_schema)

    existing = MagicMock()

    def _merge_localized_updates(updates, lang):
        merged = MagicMock()
        merged.model_dump.return_value = {"title": "Updated"}
        return merged

    existing.merge_localized_updates.side_effect = _merge_localized_updates

    async def _get_collection_model_logic(_self, catalog_id, collection_id, conn):
        return existing

    monkeypatch.setattr(
        CollectionService, "_get_collection_model_logic", _get_collection_model_logic
    )

    class _NoopQuery:
        def __init__(self, *a, **k):
            pass

        async def execute(self, *a, **k):
            return True  # collection exists

    monkeypatch.setattr(collection_service_mod, "_make_collection_exists_query", lambda *_a, **_k: _NoopQuery())

    import dynastore.modules.catalog.collection_router as col_router_mod

    monkeypatch.setattr(col_router_mod, "upsert_collection_metadata", AsyncMock())

    monkeypatch.setattr(
        collection_service_mod, "_invalidate_collection_model_cache",
        lambda *a, **k: None,
    )

    return s


@pytest.mark.asyncio
async def test_update_collection_emits_mid_and_after(svc, record_emit):
    await svc.update_collection("cat_a", "col_a", {"title": "Updated"})

    event_types = [c[0] for c in record_emit]
    assert CatalogEventType.COLLECTION_UPDATE in event_types, (
        "update_collection did not emit COLLECTION_UPDATE — parity with "
        "update_catalog's CATALOG_UPDATE emission is broken (#2677)."
    )
    assert CatalogEventType.AFTER_COLLECTION_UPDATE in event_types, (
        "update_collection did not emit AFTER_COLLECTION_UPDATE (#2677)."
    )


@pytest.mark.asyncio
async def test_update_collection_emit_payload_shape(svc, record_emit):
    await svc.update_collection("cat_b", "col_b", {"title": "Updated"})

    for evt in (
        CatalogEventType.COLLECTION_UPDATE,
        CatalogEventType.AFTER_COLLECTION_UPDATE,
    ):
        matches = [kwargs for e, kwargs in record_emit if e == evt]
        assert len(matches) == 1, f"{evt} must be emitted exactly once"
        kwargs = matches[0]
        assert kwargs["catalog_id"] == "cat_b"
        assert kwargs["collection_id"] == "col_b"
        assert kwargs["operation"] == "metadata_update"
        assert kwargs.get("db_resource") is not None
