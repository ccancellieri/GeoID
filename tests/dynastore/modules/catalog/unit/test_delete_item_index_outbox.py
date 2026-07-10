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

"""Unit tests for delete async-write ledger symmetry.

Background: ``ItemQueryMixin.delete_item`` previously dispatched a delete
``IndexOp`` keyed by the *external* path id through the index dispatcher,
while ``ItemService.upsert_bulk`` indexes the ES document under the
*geoid* (the persisted feature's default ``id``) via an async-durable
``OutboxRecord``. The two paths derived the ES ``_id`` independently, so a
delete targeted a non-existent ``_id`` and never purged the document.

These tests pin the fixed contract: delete enqueues one lightweight
``WriteIdOutboxRecord(op="delete")`` per INDEX-lane target, keyed by the
same logical write id stamped on the tombstoned hub rows. The drain then
reads tombstoned geoids by write id and removes the same documents the
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
)

# Where the delete path imports the storage-emit function (patched per test).
_ENQUEUE = "dynastore.modules.storage.storage_emit.enqueue_storage_op_write_id"


class _CapableDriver:
    """Minimal stub exposing the #3116 primary-driver write-id read
    capability (``read_indexable_write_batch``) — without it,
    ``_enqueue_index_deletes``'s capability guard skips the async delete
    enqueue entirely (see ``driver_supports_write_id_reads``)."""

    async def read_indexable_write_batch(self, **kwargs: Any) -> List[Any]:
        return []


_DEFAULT_WRITE_ENTRY = OperationDriverEntry(
    driver_ref="items_postgresql_driver", on_failure=FailurePolicy.FATAL,
)


def _service_with(index_entries_: List[OperationDriverEntry]) -> ItemService:
    """Build an ItemService with the routing test seam injected.

    ``__new__`` skips ``__init__`` — ``_enqueue_index_deletes`` only reads the
    routing resolver plus module-level imports, so no engine/state is needed.
    The enqueue goes through ``enqueue_storage_op_write_id``, which the tests
    patch, so no live tasks table is needed. The driver registry seam always
    resolves to a write-id-capable stub so the #3116 guard never silently
    skips the enqueue in these tests. A default WRITE-lane primary entry is
    always present (``index_entries`` reads ``Operation.INDEX`` only; the
    WRITE lane is read separately for the primary write-id capability
    check).
    """
    svc = ItemService.__new__(ItemService)

    async def _resolver(_catalog_id: str, _collection_id: str):
        return SimpleNamespace(operations={
            Operation.WRITE: [_DEFAULT_WRITE_ENTRY],
            Operation.INDEX: list(index_entries_),
        })

    async def _registry(_driver_ref: str) -> _CapableDriver:
        return _CapableDriver()

    svc._test_routing_resolver = _resolver  # type: ignore[attr-defined]
    svc._test_driver_registry = _registry  # type: ignore[attr-defined]
    return svc


def _index_entry() -> OperationDriverEntry:
    return OperationDriverEntry(driver_ref="items_elasticsearch_driver", source="auto")


def test_delete_enqueues_one_write_id_record_per_index_target():
    """A delete batch yields one lightweight ledger row per INDEX-lane target."""
    svc = _service_with([_index_entry()])
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
        asyncio.run(
            svc._enqueue_index_deletes(
                conn, "cat-x", "col-y", geoids, write_id="w-delete-1",
            ),
        )

    assert captured["conn"] is conn, "must enqueue on the caller's TX conn (atomicity)"
    assert captured["catalog_id"] == "cat-x"
    assert len(captured["rows"]) == 1
    for rec in captured["rows"]:
        assert rec.op == "delete"
        assert rec.write_id == "w-delete-1"
        assert rec.idempotency_key == "w-delete-1"
        assert rec.collection_id == "col-y"
        assert rec.driver_id == "items_elasticsearch_driver"


def test_delete_is_noop_when_no_geoids_resolved():
    """No soft-deleted rows ⇒ no enqueue (a missed external id must not
    fan out a phantom delete)."""
    svc = _service_with([_index_entry()])
    enqueue = AsyncMock()
    with patch(_ENQUEUE, enqueue):
        asyncio.run(
            svc._enqueue_index_deletes(object(), "c", "k", [], write_id="w-empty"),
        )
    enqueue.assert_not_awaited()


def test_delete_skips_when_no_index_lane_entries():
    """Only INDEX-lane entries are enqueued here — a WRITE-lane primary
    entry (even the same driver_ref) must not produce an outbox delete
    row when the INDEX lane itself is empty."""
    svc = _service_with([])  # no INDEX-lane entries at all
    enqueue = AsyncMock()
    with patch(_ENQUEUE, enqueue):
        asyncio.run(
            svc._enqueue_index_deletes(object(), "c", "k", ["g1"], write_id="w-sync"),
        )
    enqueue.assert_not_awaited()
