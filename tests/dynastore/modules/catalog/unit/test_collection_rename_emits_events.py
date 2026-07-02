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

"""Regression coverage for #2677 — ``rename_collection`` must emit
``BEFORE_COLLECTION_UPDATE`` / ``AFTER_COLLECTION_UPDATE`` so consumers that
derive data from a collection's external id (e.g. the region-mapping
registry, dynastore#443) can react to a rename instead of silently going
stale.
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


class _Row:
    """Minimal stand-in for a SQLAlchemy result row exposing ``_mapping``."""

    def __init__(self, **kwargs):
        self._mapping = kwargs


def _make_query_stub(current_row, conflict_row=None):
    """Builds a ``DQLQuery`` stand-in that dispatches on the SQL text so the
    three sequential queries in ``rename_collection`` (existence fetch,
    conflict check, UPDATE) each get the right canned response.
    """

    class _QueryStub:
        def __init__(self, sql, *a, **k):
            self._sql = sql

        async def execute(self, *a, **k):
            sql = self._sql
            if sql.strip().startswith("SELECT id, external_id"):
                return current_row
            if sql.strip().startswith("SELECT id FROM"):
                return conflict_row
            if sql.strip().startswith("UPDATE"):
                return 1
            raise AssertionError(f"Unexpected query in rename_collection: {sql}")

    return _QueryStub


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

    async def _phys_schema(_self, catalog_id, db_resource=None):
        return "phys_sch_test"

    monkeypatch.setattr(CollectionService, "_resolve_physical_schema", _phys_schema)

    monkeypatch.setattr(
        collection_service_mod, "_invalidate_collection_external_id_cache",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        collection_service_mod,
        "_invalidate_collection_internal_to_external_id_cache",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        collection_service_mod, "_invalidate_collection_model_cache",
        lambda *a, **k: None,
    )
    return s


@pytest.mark.asyncio
async def test_rename_collection_emits_before_and_after(svc, record_emit, monkeypatch):
    current_row = _Row(id="col_a", external_id="old-label")
    monkeypatch.setattr(
        collection_service_mod, "DQLQuery", _make_query_stub(current_row)
    )

    result = await svc.rename_collection("cat_a", "col_a", "new-label")

    assert result == ("old-label", "new-label")
    event_types = [c[0] for c in record_emit]
    assert CatalogEventType.BEFORE_COLLECTION_UPDATE in event_types, (
        "rename_collection did not emit BEFORE_COLLECTION_UPDATE — consumers "
        "that key off a collection's external id (e.g. a region-mapping "
        "registry) cannot veto/observe the rename before it lands (#2677)."
    )
    assert CatalogEventType.AFTER_COLLECTION_UPDATE in event_types, (
        "rename_collection did not emit AFTER_COLLECTION_UPDATE — consumers "
        "cannot react to a committed rename (#2677)."
    )


@pytest.mark.asyncio
async def test_rename_collection_emit_payload_carries_old_and_new_ids(
    svc, record_emit, monkeypatch
):
    current_row = _Row(id="col_b", external_id="old-label")
    monkeypatch.setattr(
        collection_service_mod, "DQLQuery", _make_query_stub(current_row)
    )

    await svc.rename_collection("cat_b", "col_b", "new-label")

    for evt in (
        CatalogEventType.BEFORE_COLLECTION_UPDATE,
        CatalogEventType.AFTER_COLLECTION_UPDATE,
    ):
        matches = [kwargs for e, kwargs in record_emit if e == evt]
        assert len(matches) == 1, f"{evt} must be emitted exactly once per rename"
        kwargs = matches[0]
        assert kwargs["catalog_id"] == "cat_b"
        assert kwargs["collection_id"] == "col_b"
        assert kwargs["old_external_id"] == "old-label"
        assert kwargs["new_external_id"] == "new-label"
        assert kwargs["operation"] == "rename"
        assert kwargs.get("db_resource") is not None, (
            "rename events must carry the open transaction connection so "
            "the outbox write is atomic with the rename UPDATE."
        )


@pytest.mark.asyncio
async def test_rename_collection_noop_does_not_emit(svc, record_emit, monkeypatch):
    """Renaming to the same label is a no-op and must not spuriously fire
    listeners (mirrors the analogous guard on soft-delete)."""
    current_row = _Row(id="col_c", external_id="same-label")
    monkeypatch.setattr(
        collection_service_mod, "DQLQuery", _make_query_stub(current_row)
    )

    result = await svc.rename_collection("cat_c", "col_c", "same-label")

    assert result == ("same-label", "same-label")
    assert record_emit == []


@pytest.mark.asyncio
async def test_rename_collection_survives_raising_listener(svc, monkeypatch):
    """A listener exception must not corrupt the rename — matches the
    existing emit() posture (sync listener errors are logged and swallowed
    unless raise_on_error=True is explicitly requested)."""
    current_row = _Row(id="col_d", external_id="old-label")
    monkeypatch.setattr(
        collection_service_mod, "DQLQuery", _make_query_stub(current_row)
    )
    monkeypatch.setattr(collection_service_mod, "managed_transaction", _stub_txn)

    # Use the real EventService.emit (not a recorder) so the listener
    # dispatch/error-handling path is genuinely exercised.
    from dynastore.modules.catalog.event_service import event_service

    async def _boom(*a, **k):
        raise RuntimeError("listener exploded")

    event_service.register(CatalogEventType.BEFORE_COLLECTION_UPDATE, _boom)
    try:
        result = await svc.rename_collection("cat_d", "col_d", "new-label")
    finally:
        event_service.unregister(CatalogEventType.BEFORE_COLLECTION_UPDATE, _boom)

    assert result == ("old-label", "new-label"), (
        "A raising BEFORE_COLLECTION_UPDATE listener must not prevent the "
        "rename from completing and returning the correct (old, new) pair."
    )
