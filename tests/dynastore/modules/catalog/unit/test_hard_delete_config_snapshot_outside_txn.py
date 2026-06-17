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

"""Regression: the hard-delete config snapshot must be captured BEFORE the
delete transaction opens and WITHOUT the transaction's connection.

Background
----------
``delete_collection(force=True)`` used to read the collection config inside the
open delete transaction, via the shared ``conn``. Resolving a config performs
distributed-cache I/O and, on a cache miss, acquires a *second* pooled
connection for a fallback DB read. While that I/O ran, the delete connection
sat idle inside its transaction. Under cache latency / pool contention — the
exact conditions of the background ``collection_hard_delete`` runner — that
idle window exceeded ``idle_in_transaction_session_timeout`` (30s). PostgreSQL
terminated the backend, and the very next statement on ``conn`` (the cascade
``describe_scope`` SELECT) failed with::

    InterfaceError: cannot call PreparedStatement.fetch():
    the underlying connection is closed

which fail-closed the cascade and rolled back the entire hard-delete.

The fix moves the snapshot capture out of the transaction and off the shared
connection. These tests pin both properties so a future refactor that pushes
the read back inside the transaction fails loudly.
"""

from __future__ import annotations

import inspect
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.catalog import cascade_runtime as cascade_runtime_mod
from dynastore.modules.catalog import collection_service as collection_service_mod
from dynastore.modules.catalog.collection_service import CollectionService
from dynastore.models.protocols.entity_store import CollectionLifecycle


# ---------------------------------------------------------------------------
# Source-shape guard — cheap, deterministic
# ---------------------------------------------------------------------------


def test_snapshot_capture_is_not_passed_the_txn_connection() -> None:
    """The capture helper must NOT thread the delete transaction's ``conn``
    into ``get_config`` — that reintroduces the idle-in-transaction I/O.
    """
    src = inspect.getsource(CollectionService._capture_collection_config_snapshot)
    assert "db_resource" not in src and "DriverContext" not in src, (
        "_capture_collection_config_snapshot must resolve the config via the "
        "cache path (no shared connection). Passing db_resource/DriverContext "
        "holds the delete transaction idle during distributed-cache I/O and "
        "trips idle_in_transaction_session_timeout, killing the connection "
        "before the cascade describe_scope runs."
    )


def test_delete_collection_captures_snapshot_before_opening_txn() -> None:
    """Source ordering: the snapshot capture call appears before the
    ``managed_transaction`` block in ``delete_collection``.
    """
    src = inspect.getsource(CollectionService.delete_collection)
    capture_at = src.find("_capture_collection_config_snapshot")
    txn_at = src.find("managed_transaction(")
    assert capture_at != -1, "snapshot capture call missing from delete_collection"
    assert txn_at != -1, "managed_transaction block missing from delete_collection"
    assert capture_at < txn_at, (
        "config snapshot must be captured BEFORE managed_transaction opens, so "
        "the delete transaction never holds its connection idle on cache I/O."
    )


# ---------------------------------------------------------------------------
# Runtime — capture runs before the txn opens, with no shared connection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_runtime_snapshot_runs_before_txn_and_without_conn(monkeypatch):
    events: list[str] = []
    received: dict = {}

    @asynccontextmanager
    async def _recording_txn(_engine):
        events.append("txn_enter")
        try:
            yield object()
        finally:
            events.append("txn_exit")

    monkeypatch.setattr(collection_service_mod, "managed_transaction", _recording_txn)
    monkeypatch.setattr(
        collection_service_mod, "emit_event", AsyncMock(return_value=None)
    )

    class _FakeConfig:
        def model_dump(self):
            return {"some": "config"}

    class _FakeConfigs:
        async def get_config(
            self, cls, catalog_id=None, collection_id=None, ctx=None, **kw
        ):
            events.append("get_config")
            received["ctx"] = ctx
            received["kw"] = kw
            return _FakeConfig()

    def _get_protocol(proto, *a, **kw):
        name = getattr(proto, "__name__", "")
        return _FakeConfigs() if "Configs" in name else None

    monkeypatch.setattr(collection_service_mod, "get_protocol", _get_protocol)

    # Cascade orchestrator: record that describe/enqueue ran inside the txn
    # (after txn_enter) and that the captured snapshot already exists.
    class _FakeOrchestrator:
        async def snapshot_and_enqueue(self, conn, scope_ref, mode):
            events.append("snapshot_and_enqueue")
            return "task-id"

    monkeypatch.setattr(cascade_runtime_mod, "CascadeOrchestrator", _FakeOrchestrator)

    s = CollectionService(engine=MagicMock())

    async def _phys_schema(_self, catalog_id, db_resource=None):
        return "phys_sch_test"

    async def _purge(_self, conn, phys_schema, catalog_id, collection_id):
        events.append("purge")
        return "phys_table_test"

    async def _alive(_self, catalog_id, collection_id, db_resource=None):
        return CollectionLifecycle.ACTIVE

    async def _noop_set_status(_self, catalog_id, collection_id, status, db_resource=None):
        return True

    captured_config: dict = {}

    def _record_destroy(catalog_id, collection_id, lifecycle_ctx):
        captured_config["config"] = lifecycle_ctx.config

    monkeypatch.setattr(CollectionService, "_resolve_physical_schema", _phys_schema)
    monkeypatch.setattr(CollectionService, "_purge_collection_storage", _purge)
    monkeypatch.setattr(CollectionService, "_get_lifecycle", _alive)
    monkeypatch.setattr(CollectionService, "_set_lifecycle_status", _noop_set_status)
    monkeypatch.setattr(
        collection_service_mod.lifecycle_registry,
        "destroy_async_collection",
        _record_destroy,
    )

    ok = await s.delete_collection("cat_a", "col_a", force=True)
    assert ok is True

    # 1. The snapshot was captured BEFORE the transaction opened.
    assert "get_config" in events and "txn_enter" in events
    assert events.index("get_config") < events.index("txn_enter"), (
        f"config snapshot ran inside/after the txn: {events}"
    )

    # 2. The capture was NOT handed the delete transaction's connection.
    assert received["ctx"] is None, (
        "get_config must be called without a DriverContext/db_resource so it "
        "resolves via the cache path, not the open delete transaction."
    )

    # 3. The cascade describe/enqueue still ran inside the transaction, after
    #    it opened — and after the snapshot was already in hand.
    assert events.index("txn_enter") < events.index("snapshot_and_enqueue")

    # 4. The captured config still reaches the post-commit async destroyer.
    assert captured_config["config"]["collection_config"] == {"some": "config"}
    assert captured_config["config"]["catalog_id"] == "cat_a"
