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

"""Regression coverage for #2256 gap #1 — create_collection must emit
``COLLECTION_CREATION`` inside the open transaction so the
``_on_collection_creation`` listener in tasks/event_driver.py fires,
writing a ``collection_creation`` outbox row and giving /events + /logs
parity with catalog_creation.

Two layers of coverage:

1. Source-shape — pins the literal emit reference so a future refactor
   that drops it fails loudly (mirrors
   ``test_collection_delete_emits_events.py`` and
   ``test_catalog_delete_emits_metadata_changed.py``).

2. Runtime — patches the DB plumbing and asserts the patched
   ``emit_event`` symbol receives ``COLLECTION_CREATION`` with the right
   ``catalog_id`` / ``collection_id`` kwargs, and that the call is placed
   inside the same transaction block (i.e. ``db_resource`` is the open
   connection, not None).
"""

from __future__ import annotations

import inspect
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.catalog import collection_service as collection_service_mod
from dynastore.modules.catalog.collection_service import CollectionService
from dynastore.modules.catalog.event_service import CatalogEventType


# ---------------------------------------------------------------------------
# Source-shape: cheap, deterministic regression guard
# ---------------------------------------------------------------------------


def _create_collection_source() -> str:
    return inspect.getsource(CollectionService.create_collection)


def test_create_collection_source_emits_collection_creation() -> None:
    src = _create_collection_source()
    assert "CatalogEventType.COLLECTION_CREATION" in src, (
        "create_collection no longer emits COLLECTION_CREATION — the "
        "_on_collection_creation listener in tasks/event_driver.py will "
        "never fire, leaving /events and /logs without collection_creation "
        "rows (#2256 gap #1). Re-add the emit_event(COLLECTION_CREATION, "
        "catalog_id=..., collection_id=..., db_resource=conn) call inside "
        "the managed_transaction block."
    )


def test_create_collection_source_passes_db_resource_conn() -> None:
    src = _create_collection_source()
    # The emit must be inside the transaction so db_resource=conn (not None).
    # A simple way to pin this: check that the emit appears BEFORE the
    # closing of the `async with managed_transaction(...)` block (i.e. that
    # `db_resource=conn` is present as a kwarg near the emit call).
    assert "db_resource=conn" in src, (
        "create_collection must pass db_resource=conn to emit_event so the "
        "outbox row is written in the same transaction as the collection "
        "INSERT — removing this makes the event non-atomic (#2256 gap #1)."
    )


# ---------------------------------------------------------------------------
# Runtime: assert emit_event is invoked with the correct shape
# ---------------------------------------------------------------------------


class _FakeConn:
    """Minimal stand-in for the asyncpg connection/transaction object."""

    async def execute(self, *a, **k):
        return None


@asynccontextmanager
async def _stub_txn(_engine):
    yield _FakeConn()


@pytest.fixture
def record_emit(monkeypatch):
    """Replace ``emit_event`` (imported by name in collection_service) with a
    recorder that captures ``(event_type, kwargs)`` per call.
    """
    calls: list[tuple] = []

    async def _recorder(event_type, *args, **kwargs):
        calls.append((event_type, kwargs))

    monkeypatch.setattr(
        collection_service_mod, "emit_event", AsyncMock(side_effect=_recorder)
    )
    monkeypatch.setattr(collection_service_mod, "managed_transaction", _stub_txn)

    # Stub DQLQuery / DDLQuery to no-ops so the in-txn SQL (tombstone check,
    # INSERT, etc.) passes without a real DB.
    class _NoopQuery:
        def __init__(self, *a, **k):
            pass

        async def execute(self, *a, **k):
            return None

    monkeypatch.setattr(collection_service_mod, "DQLQuery", _NoopQuery)
    monkeypatch.setattr(collection_service_mod, "DDLQuery", _NoopQuery)

    return calls


@pytest.fixture
def svc(monkeypatch):
    """A CollectionService with all external collaborators stubbed so
    ``create_collection`` can run to completion without a real DB or
    module registry.
    """
    s = CollectionService(engine=MagicMock())

    # DriverContext validates that db_resource is a real SQLAlchemy type.
    # In the unit context _FakeConn is not, so replace DriverContext with a
    # pass-through dataclass that accepts any value.
    class _AnyDriverContext:
        def __init__(self, db_resource=None, **kwargs):
            self.db_resource = db_resource

    monkeypatch.setattr(collection_service_mod, "DriverContext", _AnyDriverContext)

    # Catalog existence check
    class _FakeCatalogsProtocol:
        async def get_catalog_model(self, catalog_id, ctx=None):
            return MagicMock()  # truthy → catalog exists

        async def list_catalog_configs(self, catalog_id):
            return {}

    monkeypatch.setattr(
        collection_service_mod,
        "get_protocol",
        lambda proto: _FakeCatalogsProtocol(),
    )

    # Physical schema resolver
    async def _phys_schema(_self, catalog_id, db_resource=None):
        return "phys_sch_test"

    monkeypatch.setattr(
        CollectionService, "_resolve_physical_schema", _phys_schema
    )

    # validate_sql_identifier is a pure guard; keep it as-is (it's cheap).

    # lifecycle_registry.has_async_collection_initializers → no async init
    monkeypatch.setattr(
        collection_service_mod.lifecycle_registry,
        "has_async_collection_initializers",
        lambda: False,
    )
    # lifecycle_registry.init_collection → no-op
    monkeypatch.setattr(
        collection_service_mod.lifecycle_registry,
        "init_collection",
        AsyncMock(),
    )
    # lifecycle_registry.init_async_collection → no-op
    monkeypatch.setattr(
        collection_service_mod.lifecycle_registry,
        "init_async_collection",
        lambda *a, **kw: None,
    )

    # Storage driver resolved by the router (Operation.READ) → minimal stub
    class _FakeDriver:
        async def get_driver_config(self, catalog_id, collection_id):
            return {}

    async def _get_driver(*a, **kw):
        return _FakeDriver()

    # Patch the lazy import inside create_collection
    import dynastore.modules.storage.router as storage_router_mod

    monkeypatch.setattr(storage_router_mod, "get_driver", _get_driver)

    # collection_router.upsert_collection_metadata → no-op
    import dynastore.modules.catalog.collection_router as col_router_mod

    monkeypatch.setattr(
        col_router_mod,
        "upsert_collection_metadata",
        AsyncMock(),
    )

    # signal_bus.emit → no-op (the internal wakeup bus, NOT the domain bus)
    monkeypatch.setattr(
        collection_service_mod.signal_bus, "emit", AsyncMock()
    )

    # get_collection_model (called after the txn to build the return value)
    async def _get_col_model(_self, catalog_id, collection_id, db_resource=None):
        m = MagicMock()
        m.id = collection_id
        return m

    monkeypatch.setattr(CollectionService, "get_collection_model", _get_col_model)

    # Cache invalidation helper — pure process-local; no-op is fine.
    monkeypatch.setattr(
        collection_service_mod,
        "_invalidate_collection_lifecycle_caches",
        lambda *a, **kw: None,
    )

    return s


@pytest.mark.asyncio
async def test_create_collection_emits_collection_creation(svc, record_emit):
    """create_collection must emit COLLECTION_CREATION with the right
    catalog_id/collection_id so _on_collection_creation in event_driver
    can write the outbox row (#2256 gap #1).
    """
    await svc.create_collection("cat_a", {"id": "col_a", "title": "Col A"})

    event_types = [c[0] for c in record_emit]
    assert CatalogEventType.COLLECTION_CREATION in event_types, (
        "create_collection did not emit COLLECTION_CREATION — "
        "_on_collection_creation will never fire (#2256 gap #1)."
    )


@pytest.mark.asyncio
async def test_create_collection_emits_correct_ids(svc, record_emit):
    await svc.create_collection("cat_b", {"id": "col_b", "title": "Col B"})

    creation_calls = [
        kwargs
        for evt, kwargs in record_emit
        if evt == CatalogEventType.COLLECTION_CREATION
    ]
    assert len(creation_calls) == 1, (
        "COLLECTION_CREATION must be emitted exactly once per create_collection call."
    )
    kwargs = creation_calls[0]
    assert kwargs["catalog_id"] == "cat_b"
    assert kwargs["collection_id"] == "col_b"


@pytest.mark.asyncio
async def test_create_collection_emit_carries_db_resource(svc, record_emit):
    """The emit must carry a non-None db_resource (the open conn) so the
    outbox INSERT is atomic with the collection INSERT.
    """
    await svc.create_collection("cat_c", {"id": "col_c", "title": "Col C"})

    creation_calls = [
        kwargs
        for evt, kwargs in record_emit
        if evt == CatalogEventType.COLLECTION_CREATION
    ]
    assert creation_calls, "COLLECTION_CREATION was not emitted"
    assert creation_calls[0].get("db_resource") is not None, (
        "emit_event for COLLECTION_CREATION must receive db_resource=conn "
        "(the open transaction connection) — passing None makes the outbox "
        "write non-atomic with the collection row (#2256 gap #1)."
    )
