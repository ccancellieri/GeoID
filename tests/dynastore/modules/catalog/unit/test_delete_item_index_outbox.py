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
from typing import Any, List

from dynastore.modules.catalog.item_service import ItemService
from dynastore.modules.storage.routing_config import (
    FailurePolicy,
    Operation,
    OperationDriverEntry,
    WriteMode,
)


class _FakeOutbox:
    """Captures ``enqueue_bulk`` calls without touching a DB."""

    def __init__(self) -> None:
        self.calls: List[Any] = []

    async def enqueue_bulk(self, conn: Any, *, catalog_id: str, rows: Any) -> None:
        self.calls.append(
            SimpleNamespace(conn=conn, catalog_id=catalog_id, rows=list(rows))
        )


def _service_with(routing_entries: List[OperationDriverEntry], outbox: _FakeOutbox) -> ItemService:
    """Build an ItemService with the routing/outbox test seams injected.

    ``__new__`` skips ``__init__`` — ``_enqueue_index_deletes`` only reads
    the two seams plus module-level imports, so no engine/state is needed.
    """
    svc = ItemService.__new__(ItemService)

    async def _resolver(_catalog_id: str, _collection_id: str):
        return SimpleNamespace(operations={Operation.INDEX: list(routing_entries)})

    svc._test_routing_resolver = _resolver  # type: ignore[attr-defined]
    svc._test_outbox_store = outbox  # type: ignore[attr-defined]
    return svc


def _async_es_entry() -> OperationDriverEntry:
    return OperationDriverEntry(
        driver_ref="items_elasticsearch_driver",
        on_failure=FailurePolicy.OUTBOX,
        write_mode=WriteMode.ASYNC,
    )


def test_delete_enqueues_one_record_per_geoid_keyed_by_geoid():
    """Each soft-deleted geoid yields one delete OutboxRecord whose
    ``idempotency_key`` (the ES ``_id``) is that geoid — never the
    external/path id."""
    outbox = _FakeOutbox()
    svc = _service_with([_async_es_entry()], outbox)
    geoids = [
        "11111111-1111-7111-8111-111111111111",
        "22222222-2222-7222-8222-222222222222",
    ]

    conn = object()
    asyncio.run(svc._enqueue_index_deletes(conn, "cat-x", "col-y", geoids))

    assert len(outbox.calls) == 1
    call = outbox.calls[0]
    assert call.conn is conn, "must enqueue on the caller's TX conn (atomicity)"
    assert call.catalog_id == "cat-x"
    assert len(call.rows) == len(geoids)
    for rec, gid in zip(call.rows, geoids, strict=True):
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
    outbox = _FakeOutbox()
    svc = _service_with([_async_es_entry()], outbox)
    asyncio.run(svc._enqueue_index_deletes(object(), "c", "k", []))
    assert outbox.calls == []


def test_delete_skips_non_async_outbox_index_entries():
    """Only ASYNC+OUTBOX INDEX entries are enqueued here — a SYNC/FATAL
    write entry must not produce an outbox delete row."""
    outbox = _FakeOutbox()
    sync_entry = OperationDriverEntry(
        driver_ref="items_postgresql_driver",
        on_failure=FailurePolicy.FATAL,
        write_mode=WriteMode.SYNC,
    )
    svc = _service_with([sync_entry], outbox)
    asyncio.run(svc._enqueue_index_deletes(object(), "c", "k", ["g1"]))
    assert outbox.calls == []
