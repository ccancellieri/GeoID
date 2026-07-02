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

"""#2667: catalog hard-delete must report an async status link instead of
blocking the DELETE request on the schema-drop teardown.

``delete_catalog(force=True)`` already hands the schema drop + external
teardown off to the durable ``catalog_provision`` task (checklist-driven
deprovision, #2340/#2421) rather than running it inline — the request never
blocks on the drop itself. What was missing is a way for the HTTP layer to
report that hand-off: ``CatalogsProtocol.get_hard_delete_task`` looks up the
in-flight deprovision task by its deterministic ``dedup_key`` so
``_ogc_delete_catalog`` can return 202 + a status link (matching the
collection hard-delete contract) instead of a bare 204 that gives the caller
no way to know teardown is still running.

These tests cover the lookup helper's resolution logic (live row, tombstoned
row, and the miss/none cases) plus a source-shape pin that the enqueue site
carries the same dedup_key the lookup queries for.
"""

from __future__ import annotations

import inspect
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.catalog import catalog_service as catalog_service_mod
from dynastore.modules.catalog.catalog_service import CatalogService


def _make_service_with_txn(monkeypatch, conn: object) -> CatalogService:
    """Return a CatalogService whose managed_transaction yields ``conn``."""

    @asynccontextmanager
    async def _txn(_engine):
        yield conn

    monkeypatch.setattr(catalog_service_mod, "managed_transaction", _txn)
    monkeypatch.setattr(catalog_service_mod, "get_catalog_engine", lambda: MagicMock())
    return CatalogService(engine=MagicMock())


@pytest.mark.asyncio
async def test_get_hard_delete_task_none_when_catalog_unresolvable(monkeypatch):
    """Neither a live nor a tombstoned row exists -> None, no DB query attempted."""
    svc = CatalogService(engine=MagicMock())
    svc.resolve_catalog_id = AsyncMock(return_value=None)
    svc._get_tombstoned_catalog_id_by_external_id_db = AsyncMock(return_value=None)

    result = await svc.get_hard_delete_task("nonexistent")

    assert result is None
    svc._get_tombstoned_catalog_id_by_external_id_db.assert_awaited_once_with(
        "nonexistent"
    )


@pytest.mark.asyncio
async def test_get_hard_delete_task_uses_live_internal_id(monkeypatch):
    """A live (not-yet-tombstoned) row resolves via resolve_catalog_id and the
    dedup_key query is built from the resolved internal id, not the raw
    external path param.
    """
    svc = _make_service_with_txn(monkeypatch, conn=AsyncMock())
    svc.resolve_catalog_id = AsyncMock(return_value="s_internal123")
    svc._get_tombstoned_catalog_id_by_external_id_db = AsyncMock(
        side_effect=AssertionError("must not be called when the live lookup hits")
    )

    monkeypatch.setattr(
        "dynastore.modules.tasks.tasks_module.get_task_schema",
        lambda: "tasks",
    )

    captured: dict = {}

    class _FakeQuery:
        def __init__(self, sql, result_handler=None):
            captured["sql"] = sql

        async def execute(self, conn, **kwargs):
            captured["kwargs"] = kwargs
            return {
                "task_id": "11111111-1111-1111-1111-111111111111",
                "catalog_id": "s_internal123",
                "task_type": "catalog_provision",
                "status": "PENDING",
                "dedup_key": "catalog_provision:deprovision_hard:s_internal123",
            }

    monkeypatch.setattr(catalog_service_mod, "DQLQuery", _FakeQuery)

    result = await svc.get_hard_delete_task("external-cat")

    assert result is not None
    assert str(result.jobID) == "11111111-1111-1111-1111-111111111111"
    assert captured["kwargs"]["catalog_id"] == "s_internal123"
    assert (
        captured["kwargs"]["dedup_key"]
        == "catalog_provision:deprovision_hard:s_internal123"
    )


@pytest.mark.asyncio
async def test_get_hard_delete_task_falls_back_to_tombstoned_lookup(monkeypatch):
    """Right after delete_catalog(force=True) commits, the row is already
    tombstoned — resolve_catalog_id(allow_missing=True) returns None and the
    lookup must fall back to the tombstone-inclusive resolver.
    """
    svc = _make_service_with_txn(monkeypatch, conn=AsyncMock())
    svc.resolve_catalog_id = AsyncMock(return_value=None)
    svc._get_tombstoned_catalog_id_by_external_id_db = AsyncMock(
        return_value="s_internal123"
    )

    monkeypatch.setattr(
        "dynastore.modules.tasks.tasks_module.get_task_schema",
        lambda: "tasks",
    )

    class _FakeQuery:
        def __init__(self, sql, result_handler=None):
            pass

        async def execute(self, conn, **kwargs):
            assert kwargs["catalog_id"] == "s_internal123"
            return {
                "task_id": "22222222-2222-2222-2222-222222222222",
                "catalog_id": "s_internal123",
                "task_type": "catalog_provision",
                "status": "ACTIVE",
                "dedup_key": "catalog_provision:deprovision_hard:s_internal123",
            }

    monkeypatch.setattr(catalog_service_mod, "DQLQuery", _FakeQuery)

    result = await svc.get_hard_delete_task("external-cat")

    assert result is not None
    assert str(result.jobID) == "22222222-2222-2222-2222-222222222222"
    assert result.status == "ACTIVE"


@pytest.mark.asyncio
async def test_get_hard_delete_task_none_when_no_matching_row(monkeypatch):
    """A resolvable catalog with no non-terminal deprovision task (already
    completed, or force=True purged inline with no active provisioners) ->
    None so the HTTP layer falls back to 204.
    """
    svc = _make_service_with_txn(monkeypatch, conn=AsyncMock())
    svc.resolve_catalog_id = AsyncMock(return_value="s_internal123")

    monkeypatch.setattr(
        "dynastore.modules.tasks.tasks_module.get_task_schema",
        lambda: "tasks",
    )

    class _FakeQuery:
        def __init__(self, sql, result_handler=None):
            pass

        async def execute(self, conn, **kwargs):
            return None

    monkeypatch.setattr(catalog_service_mod, "DQLQuery", _FakeQuery)

    result = await svc.get_hard_delete_task("external-cat")

    assert result is None


# ---------------------------------------------------------------------------
# Source-shape guard — the enqueue site must carry the same dedup_key format
# the lookup above queries for, and idempotency must not regress: a repeat
# DELETE on a catalog with an in-flight deprovision must not double-enqueue.
# ---------------------------------------------------------------------------


def test_delete_catalog_enqueues_deprovision_with_dedup_key() -> None:
    src = inspect.getsource(CatalogService.delete_catalog)
    assert 'dedup_key=f"catalog_provision:deprovision_hard:{catalog_id}"' in src, (
        "the catalog_provision deprovision_hard TaskCreate must carry a "
        "deterministic dedup_key so (a) a client retry after a slow/dropped "
        "response re-enqueues idempotently instead of spawning a second "
        "concurrent teardown, and (b) get_hard_delete_task can look the task "
        "back up for the HTTP 202 status link."
    )
