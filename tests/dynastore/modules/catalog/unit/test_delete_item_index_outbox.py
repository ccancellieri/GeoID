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

"""Unit tests for delete index-propagation symmetry.

Background: ``ItemQueryMixin.delete_item`` previously dispatched a delete
``IndexOp`` keyed by the *external* path id through the index dispatcher,
while ``ItemService.upsert_bulk`` indexes the ES document under the
*geoid* (the persisted feature's default ``id``) via an async-OUTBOX
``OutboxRecord``. The two paths derived the ES ``_id`` independently, so a
delete targeted a non-existent ``_id`` and never purged the document.

These tests pin the fixed contract: delete enqueues one
``OutboxRecord(op="delete")`` per soft-deleted geoid, keyed by the geoid,
through the same async-OUTBOX path the upsert uses — so the drain (which
keys ES ``_id`` on ``idempotency_key``) removes the same document the
upsert wrote.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, Dict, List
from unittest.mock import AsyncMock, patch

from dynastore.modules.catalog.item_service import ItemService
from dynastore.modules.storage.routing_config import (
    FailurePolicy,
    Operation,
    OperationDriverEntry,
    WriteMode,
)

# Where the delete path imports the storage-emit function (patched per test).
_ENQUEUE = "dynastore.modules.storage.storage_emit.enqueue_storage_op"


def _service_with(routing_entries: List[OperationDriverEntry]) -> ItemService:
    """Build an ItemService with the routing test seam injected.

    ``__new__`` skips ``__init__`` — ``_enqueue_index_deletes`` only reads the
    routing resolver plus module-level imports, so no engine/state is needed.
    The enqueue goes through ``enqueue_storage_op`` (#1807 P4), which the tests
    patch, so no outbox-store seam is needed.
    """
    svc = ItemService.__new__(ItemService)

    async def _resolver(_catalog_id: str, _collection_id: str):
        return SimpleNamespace(operations={Operation.WRITE: list(routing_entries)})

    svc._test_routing_resolver = _resolver  # type: ignore[attr-defined]
    return svc


def _async_es_entry() -> OperationDriverEntry:
    return OperationDriverEntry(
        driver_ref="items_elasticsearch_driver",
        on_failure=FailurePolicy.OUTBOX,
        write_mode=WriteMode.ASYNC,
        secondary_index=True,
    )


def test_delete_enqueues_one_record_per_geoid_keyed_by_geoid():
    """Each soft-deleted geoid yields one delete OutboxRecord whose
    ``idempotency_key`` (the ES ``_id``) is that geoid — never the
    external/path id."""
    svc = _service_with([_async_es_entry()])
    geoids = [
        "11111111-1111-7111-8111-111111111111",
        "22222222-2222-7222-8222-222222222222",
    ]
    conn = object()
    captured: Dict[str, Any] = {}

    async def _fake_enqueue(c: Any, *, catalog_id: str, rows: Any) -> None:
        captured["conn"] = c
        captured["catalog_id"] = catalog_id
        captured["rows"] = list(rows)

    with patch(_ENQUEUE, _fake_enqueue):
        asyncio.run(svc._enqueue_index_deletes(conn, "cat-x", "col-y", geoids))

    assert captured["conn"] is conn, "must enqueue on the caller's TX conn (atomicity)"
    assert captured["catalog_id"] == "cat-x"
    assert len(captured["rows"]) == len(geoids)
    for rec, gid in zip(captured["rows"], geoids, strict=True):
        assert rec.op == "delete"
        assert rec.idempotency_key == gid, (
            "ES _id must be the geoid the upsert indexed under, not the "
            f"external id; got {rec.idempotency_key!r}"
        )
        assert rec.item_id == gid
        assert rec.collection_id == "col-y"
        assert rec.driver_id == "items_elasticsearch_driver"
        assert rec.payload == {}, "delete actions carry no source document"


def test_delete_is_noop_when_no_geoids_resolved():
    """No soft-deleted rows ⇒ no enqueue (a missed external id must not
    fan out a phantom delete)."""
    svc = _service_with([_async_es_entry()])
    enqueue = AsyncMock()
    with patch(_ENQUEUE, enqueue):
        asyncio.run(svc._enqueue_index_deletes(object(), "c", "k", []))
    enqueue.assert_not_awaited()


def test_delete_skips_non_async_outbox_index_entries():
    """Only ASYNC+OUTBOX secondary-index WRITE entries are enqueued here — a
    SYNC/FATAL primary write entry must not produce an outbox delete row."""
    sync_entry = OperationDriverEntry(
        driver_ref="items_postgresql_driver",
        on_failure=FailurePolicy.FATAL,
        write_mode=WriteMode.SYNC,
    )
    svc = _service_with([sync_entry])
    enqueue = AsyncMock()
    with patch(_ENQUEUE, enqueue):
        asyncio.run(svc._enqueue_index_deletes(object(), "c", "k", ["g1"]))
    enqueue.assert_not_awaited()
