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

"""Routing tests for the #3116 primary-driver write-id capability guard.

A write-id ledger row can only ever be hydrated from the collection's
primary WRITE driver (``StorageDrainTask._resolve_primary_write_source``
resolves the FIRST resolved WRITE driver). When that driver does not expose
``read_indexable_write_batch`` (or the ``read_active_rows_by_write_id`` /
``read_tombstoned_ids_by_write_id`` chunk-read pair), a write-id row can
never hydrate and would retry forever — so every producer must fall back to
an id-only row (which re-reads canonical PG state and is always hydratable)
instead of a grouped write-id row.

These are pure unit tests — no DB, no app graph — covering the three seams
that implement the guard:

* ``dynastore.modules.storage.storage_emit.driver_supports_write_id_reads``
* ``dynastore.modules.storage.index_dispatcher._build_storage_plane_records``
  / ``_primary_supports_write_id_reads``
* ``ItemService.upsert_bulk`` / ``ItemQueryMixin._enqueue_index_deletes``
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Sequence
from uuid import uuid4

import pytest

from dynastore.models.protocols.indexer import IndexContext
from dynastore.models.protocols.indexing import IndexableOp, WriteIdOutboxRecord
from dynastore.modules.catalog.item_service import ItemService
from dynastore.modules.storage.routing_config import (
    FailurePolicy,
    Operation,
    OperationDriverEntry,
)


# ---------------------------------------------------------------------------
# driver_supports_write_id_reads
# ---------------------------------------------------------------------------


def test_driver_supports_write_id_reads_false_for_capability_lacking_driver():
    from dynastore.modules.storage.storage_emit import driver_supports_write_id_reads

    class _BareDriver:
        async def write_entities(self, *a, **kw):
            return []

    assert driver_supports_write_id_reads(_BareDriver()) is False
    assert driver_supports_write_id_reads(None) is False


def test_driver_supports_write_id_reads_true_for_capable_driver():
    from dynastore.modules.storage.storage_emit import driver_supports_write_id_reads

    class _BatchReader:
        async def read_indexable_write_batch(self, **kwargs):
            return []

    assert driver_supports_write_id_reads(_BatchReader()) is True


# ---------------------------------------------------------------------------
# index_dispatcher._build_storage_plane_records — write_id_supported gate
# ---------------------------------------------------------------------------


def _write_id_op(*, write_id: str, entity_id: str = "item-1") -> IndexableOp:
    op = IndexableOp(
        op_id=uuid4(),
        op="upsert",
        catalog_id="cat-x",
        collection_id="col-y",
        driver_instance_id="primary-di",
        item_id=entity_id,
        payload={"foo": "bar"},
        idempotency_key=f"{write_id}:{entity_id}",
    )
    object.__setattr__(op, "write_id", write_id)
    return op


def test_build_storage_plane_records_forces_id_only_when_write_id_unsupported():
    """A write-id-carrying op still produces an id-only row (write_id None,
    entity_id set) when the primary driver lacks the capability — never a
    grouped write-id row that can never hydrate."""
    from dynastore.modules.storage.index_dispatcher import (
        _build_storage_plane_records,
    )

    ctx = IndexContext(catalog="cat-x", collection="col-y", correlation_id="cid-1")
    op = _write_id_op(write_id="w-1", entity_id="item-1")

    grouped, id_only = _build_storage_plane_records(
        driver_id="items_elasticsearch_driver", ctx=ctx, ops=[op],
        write_id_supported=False,
    )

    assert grouped == [], "no grouped write-id row may be produced"
    assert len(id_only) == 1
    row = id_only[0]
    assert row.item_id == "item-1"
    assert row.op == "upsert"


def test_build_storage_plane_records_groups_write_id_when_supported():
    """The same op, with the capability present, produces a grouped
    write-id row instead (control case)."""
    from dynastore.modules.storage.index_dispatcher import (
        _build_storage_plane_records,
    )

    ctx = IndexContext(catalog="cat-x", collection="col-y", correlation_id="cid-1")
    op = _write_id_op(write_id="w-1", entity_id="item-1")

    grouped, id_only = _build_storage_plane_records(
        driver_id="items_elasticsearch_driver", ctx=ctx, ops=[op],
        write_id_supported=True,
    )

    assert id_only == []
    assert len(grouped) == 1
    assert grouped[0].write_id == "w-1"


# ---------------------------------------------------------------------------
# index_dispatcher._primary_supports_write_id_reads
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_primary_supports_write_id_reads_false_when_resolved_driver_lacks_capability(
    monkeypatch,
):
    import dynastore.modules.storage.router as router_mod
    from dynastore.modules.storage.index_dispatcher import (
        _primary_supports_write_id_reads,
    )

    class _BareDriver:
        pass

    async def _fake_get_write_drivers(catalog_id, collection_id):
        return [SimpleNamespace(driver=_BareDriver())]

    monkeypatch.setattr(router_mod, "get_write_drivers", _fake_get_write_drivers)

    assert await _primary_supports_write_id_reads("cat-x", "col-y") is False


@pytest.mark.asyncio
async def test_primary_supports_write_id_reads_false_on_resolution_failure(monkeypatch):
    """Any resolution failure (no routing, no registered driver) fails
    closed to unsupported — never raises into the dispatch path."""
    import dynastore.modules.storage.router as router_mod
    from dynastore.modules.storage.index_dispatcher import (
        _primary_supports_write_id_reads,
    )

    async def _raising_get_write_drivers(catalog_id, collection_id):
        raise RuntimeError("no routing config for this catalog")

    monkeypatch.setattr(router_mod, "get_write_drivers", _raising_get_write_drivers)

    assert await _primary_supports_write_id_reads("cat-x", "col-y") is False


# ---------------------------------------------------------------------------
# ItemService.upsert_bulk — fall back to per-entity id-only enqueue
# ---------------------------------------------------------------------------


class _FakeConn:
    """Marker connection identity for the wrapping TX."""


class _FakeEngine:
    def __init__(self) -> None:
        self.conn = _FakeConn()


@asynccontextmanager
async def _fake_managed_transaction(engine: _FakeEngine):
    yield engine.conn


class _CapabilityLackingPrimary:
    """Stub primary WRITE driver — writes entities but exposes none of the
    write-id chunk-read methods (#3116 guard must trip)."""

    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []

    async def write_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entities: List[Dict[str, Any]],
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> List[Dict[str, Any]]:
        self.calls.append({"entities": list(entities)})
        return list(entities)


def _make_upsert_routing_resolver():
    operations = {
        Operation.WRITE: [
            OperationDriverEntry(
                driver_ref="items_postgresql_driver",
                on_failure=FailurePolicy.FATAL,
            ),
        ],
        Operation.INDEX: [
            OperationDriverEntry(
                driver_ref="items_elasticsearch_driver",
                source="auto",
            ),
        ],
    }

    @dataclass
    class _StubRouting:
        operations: Dict[str, List[OperationDriverEntry]]

    routing = _StubRouting(operations=operations)

    async def resolve(catalog_id: str, collection_id: Optional[str]):
        return routing

    return resolve


@pytest.mark.asyncio
async def test_upsert_bulk_falls_back_to_id_only_enqueue_when_primary_lacks_write_id_capability():
    engine = _FakeEngine()
    primary = _CapabilityLackingPrimary()
    write_id_enqueued: List[Sequence[WriteIdOutboxRecord]] = []
    id_only_enqueued: List[Sequence[Any]] = []

    async def _fake_write_id_enqueue(conn, *, catalog_id: str, rows) -> None:
        write_id_enqueued.append(list(rows))

    async def _fake_id_only_enqueue(conn, *, catalog_id: str, rows) -> None:
        id_only_enqueued.append(list(rows))

    svc = ItemService(engine=engine)  # type: ignore[arg-type]
    svc._test_routing_resolver = _make_upsert_routing_resolver()  # type: ignore[attr-defined]

    async def _registry(driver_ref: str):
        return {"items_postgresql_driver": primary}.get(driver_ref)

    svc._test_driver_registry = _registry  # type: ignore[attr-defined]
    svc._test_managed_transaction = _fake_managed_transaction  # type: ignore[attr-defined]

    import unittest.mock as mock

    with mock.patch(
        "dynastore.modules.storage.storage_emit.enqueue_storage_op_write_id",
        _fake_write_id_enqueue,
    ), mock.patch(
        "dynastore.modules.storage.storage_emit.enqueue_storage_op_id_only",
        _fake_id_only_enqueue,
    ):
        await svc.upsert_bulk("cat", "col", [{"id": "i1", "v": 1}])

    assert len(primary.calls) == 1, "the primary PG write must still happen"
    assert write_id_enqueued == [], (
        "no grouped write-id row may be enqueued when the primary WRITE "
        "driver lacks write-id read capability"
    )
    assert len(id_only_enqueued) == 1
    id_only_rows = id_only_enqueued[0]
    assert len(id_only_rows) == 1
    assert id_only_rows[0].item_id == "i1"
    assert id_only_rows[0].op == "upsert"


# ---------------------------------------------------------------------------
# ItemQueryMixin._enqueue_index_deletes — same fallback, delete path
# ---------------------------------------------------------------------------


def _async_es_entry() -> OperationDriverEntry:
    return OperationDriverEntry(driver_ref="items_elasticsearch_driver", source="auto")


def test_enqueue_index_deletes_falls_back_to_id_only_when_primary_lacks_write_id_capability():
    svc = ItemService.__new__(ItemService)

    async def _resolver(_catalog_id: str, _collection_id: str):
        return SimpleNamespace(operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.INDEX: [_async_es_entry()],
        })

    svc._test_routing_resolver = _resolver  # type: ignore[attr-defined]

    async def _registry(_driver_ref: str):
        return _CapabilityLackingPrimary()

    svc._test_driver_registry = _registry  # type: ignore[attr-defined]

    write_id_enqueued: List[Any] = []
    id_only_enqueued: List[Any] = []

    async def _fake_write_id_enqueue(conn, *, catalog_id: str, rows) -> None:
        write_id_enqueued.append(list(rows))

    async def _fake_id_only_enqueue(conn, *, catalog_id: str, rows) -> None:
        id_only_enqueued.append(list(rows))

    import unittest.mock as mock

    with mock.patch(
        "dynastore.modules.storage.storage_emit.enqueue_storage_op_write_id",
        _fake_write_id_enqueue,
    ), mock.patch(
        "dynastore.modules.storage.storage_emit.enqueue_storage_op_id_only",
        _fake_id_only_enqueue,
    ):
        asyncio.run(
            svc._enqueue_index_deletes(
                object(), "cat-x", "col-y", ["g1"], write_id="w-delete-1",
            ),
        )

    assert write_id_enqueued == [], (
        "no grouped write-id row may be enqueued when the primary WRITE "
        "driver lacks write-id read capability"
    )
    assert len(id_only_enqueued) == 1
    id_only_rows = id_only_enqueued[0]
    assert len(id_only_rows) == 1
    assert id_only_rows[0].item_id == "g1"
    assert id_only_rows[0].op == "delete"
