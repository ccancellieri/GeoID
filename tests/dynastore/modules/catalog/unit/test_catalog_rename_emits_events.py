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

"""Regression coverage for #2677 — ``rename_catalog`` must emit
``BEFORE_CATALOG_UPDATE`` / ``AFTER_CATALOG_UPDATE`` so consumers that
derive data from a catalog's external id can react to a rename instead of
silently going stale.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.catalog import catalog_service as catalog_service_mod
from dynastore.modules.catalog.catalog_service import CatalogService
from dynastore.modules.catalog.event_service import CatalogEventType


class _FakeConn:
    async def execute(self, *a, **k):
        return None


@asynccontextmanager
async def _stub_txn(_engine):
    yield _FakeConn()


class _Row:
    def __init__(self, **kwargs):
        self._mapping = kwargs


def _make_query_stub(current_row, conflict_row=None):
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
            raise AssertionError(f"Unexpected query in rename_catalog: {sql}")

    return _QueryStub


@pytest.fixture
def record_emit(monkeypatch):
    calls: list[tuple] = []

    async def _recorder(event_type, *args, **kwargs):
        calls.append((event_type, kwargs))

    monkeypatch.setattr(
        catalog_service_mod, "emit_event", AsyncMock(side_effect=_recorder)
    )
    monkeypatch.setattr(catalog_service_mod, "managed_transaction", _stub_txn)
    monkeypatch.setattr(
        catalog_service_mod, "get_catalog_engine", lambda db_resource=None: MagicMock()
    )
    return calls


@pytest.fixture
def svc(monkeypatch):
    s = CatalogService(engine=MagicMock())
    monkeypatch.setattr(
        catalog_service_mod, "_invalidate_catalog_external_id_cache",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        catalog_service_mod, "_invalidate_catalog_internal_to_external_id_cache",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        catalog_service_mod, "_invalidate_catalog_model_cache",
        lambda *a, **k: None,
    )
    return s


@pytest.mark.asyncio
async def test_rename_catalog_emits_before_and_after(svc, record_emit, monkeypatch):
    current_row = _Row(id="cat_a", external_id="old-label")
    monkeypatch.setattr(catalog_service_mod, "DQLQuery", _make_query_stub(current_row))

    result = await svc.rename_catalog("cat_a", "new-label")

    assert result == ("old-label", "new-label")
    event_types = [c[0] for c in record_emit]
    assert CatalogEventType.BEFORE_CATALOG_UPDATE in event_types, (
        "rename_catalog did not emit BEFORE_CATALOG_UPDATE (#2677)."
    )
    assert CatalogEventType.AFTER_CATALOG_UPDATE in event_types, (
        "rename_catalog did not emit AFTER_CATALOG_UPDATE (#2677)."
    )


@pytest.mark.asyncio
async def test_rename_catalog_emit_payload_carries_old_and_new_ids(
    svc, record_emit, monkeypatch
):
    current_row = _Row(id="cat_b", external_id="old-label")
    monkeypatch.setattr(catalog_service_mod, "DQLQuery", _make_query_stub(current_row))

    await svc.rename_catalog("cat_b", "new-label")

    for evt in (
        CatalogEventType.BEFORE_CATALOG_UPDATE,
        CatalogEventType.AFTER_CATALOG_UPDATE,
    ):
        matches = [kwargs for e, kwargs in record_emit if e == evt]
        assert len(matches) == 1, f"{evt} must be emitted exactly once per rename"
        kwargs = matches[0]
        assert kwargs["catalog_id"] == "cat_b"
        assert kwargs["old_external_id"] == "old-label"
        assert kwargs["new_external_id"] == "new-label"
        assert kwargs["operation"] == "rename"
        assert kwargs.get("db_resource") is not None, (
            "rename events must carry the open transaction connection so "
            "the outbox write is atomic with the rename UPDATE."
        )


@pytest.mark.asyncio
async def test_rename_catalog_noop_does_not_emit(svc, record_emit, monkeypatch):
    current_row = _Row(id="cat_c", external_id="same-label")
    monkeypatch.setattr(catalog_service_mod, "DQLQuery", _make_query_stub(current_row))

    result = await svc.rename_catalog("cat_c", "same-label")

    assert result == ("same-label", "same-label")
    assert record_emit == []


@pytest.mark.asyncio
async def test_rename_catalog_survives_raising_listener(svc, monkeypatch):
    """A listener exception must not corrupt the rename — matches the
    existing emit() posture (sync listener errors are logged and swallowed
    unless raise_on_error=True is explicitly requested)."""
    current_row = _Row(id="cat_d", external_id="old-label")
    monkeypatch.setattr(catalog_service_mod, "DQLQuery", _make_query_stub(current_row))
    monkeypatch.setattr(catalog_service_mod, "managed_transaction", _stub_txn)
    monkeypatch.setattr(
        catalog_service_mod, "get_catalog_engine", lambda db_resource=None: MagicMock()
    )

    from dynastore.modules.catalog.event_service import event_service

    async def _boom(*a, **k):
        raise RuntimeError("listener exploded")

    event_service.register(CatalogEventType.BEFORE_CATALOG_UPDATE, _boom)
    try:
        result = await svc.rename_catalog("cat_d", "new-label")
    finally:
        event_service.unregister(CatalogEventType.BEFORE_CATALOG_UPDATE, _boom)

    assert result == ("old-label", "new-label"), (
        "A raising BEFORE_CATALOG_UPDATE listener must not prevent the "
        "rename from completing and returning the correct (old, new) pair."
    )
