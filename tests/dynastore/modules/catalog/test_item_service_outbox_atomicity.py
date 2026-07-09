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

"""``ItemService.upsert_bulk`` atomic outbox enqueue + same-item coalescing.

Scenarios covered:

1. **Coalesce same-item ops in-chunk** — three ops with the same ``id``
   produce a single lightweight write-id ledger row for the async target.
   The primary write still receives the latest payload.
2. **Atomicity on ledger enqueue failure** — when ``enqueue_storage_op_write_id``
   raises inside the wrapping TX, the FATAL driver's bulk write is
   rolled back. ``upsert_bulk`` re-raises so the caller knows nothing
   landed.
3. **Atomicity on PG failure** — when the FATAL driver raises before
   the outbox enqueue runs, the outbox enqueue is never invoked (the
   TX context manager exits with the exception before the outbox
   step).
4. **Primary context propagation** — the generated write id is passed to the
   primary driver context so PG can persist it on the hub rows.

The tests inject a fake routing resolver, a fake driver registry, and a stub
TX/db resource via the ``ItemService.upsert_bulk`` injection seams, and patch
the module-level ``enqueue_storage_op_write_id`` writer to capture the
enqueued rows. They never construct the full app graph — keeping the suite
cheap and targeted at the atomic-enqueue contract.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence
from unittest.mock import patch

import pytest

from dynastore.models.protocols.indexing import WriteIdOutboxRecord
from dynastore.modules.catalog.item_service import ItemService
from dynastore.modules.storage.routing_config import (
    FailurePolicy,
    Operation,
    OperationDriverEntry,
    WriteMode,
)

# Where ``upsert_bulk`` imports the storage-emit function (patched per test).
_ENQUEUE = "dynastore.modules.storage.storage_emit.enqueue_storage_op_write_id"


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeConn:
    """Marker connection — identity passed through ``managed_transaction``
    so tests can assert the same conn reaches PG write + outbox enqueue."""


@dataclass
class _TxState:
    """Records lifecycle of the wrapping TX so tests can assert rollback."""

    entered: bool = False
    committed: bool = False
    rolled_back: bool = False


class _FakeEngine:
    """Engine sentinel paired with ``_fake_managed_transaction``."""

    def __init__(self) -> None:
        self.tx_state = _TxState()
        self.conn = _FakeConn()


@asynccontextmanager
async def _fake_managed_transaction(engine: _FakeEngine):
    engine.tx_state.entered = True
    try:
        yield engine.conn
    except BaseException:
        engine.tx_state.rolled_back = True
        raise
    else:
        engine.tx_state.committed = True


class _RecordingDriver:
    """Stub PG bulk driver — records calls and the conn it was given.

    Carries ``read_indexable_write_batch`` so it passes the #3116 primary-
    driver write-id capability guard in ``upsert_bulk`` — without it the
    guard would skip the async ledger enqueue entirely (see
    ``driver_supports_write_id_reads``).
    """

    def __init__(self, *, raise_on_write: bool = False) -> None:
        self.calls: List[Dict[str, Any]] = []
        self.raise_on_write = raise_on_write

    async def read_indexable_write_batch(self, **kwargs: Any) -> List[Any]:
        return []

    async def write_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entities: List[Dict[str, Any]],
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> List[Dict[str, Any]]:
        self.calls.append({
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "entities": list(entities),
            "context": dict(context or {}),
            "db_resource": db_resource,
        })
        if self.raise_on_write:
            raise RuntimeError("driver bulk write failed (test)")
        return list(entities)


class _EnqueueCapture:
    """Stand-in for ``enqueue_storage_op_write_id`` — records writes.

    Patched over the module-level function the write path imports, so the
    tests can assert on the enqueued rows without a live DB. Set
    ``raise_on_enqueue`` to simulate a failure inside the wrapping TX."""

    def __init__(self, *, raise_on_enqueue: bool = False) -> None:
        self.enqueued_calls: List[Dict[str, Any]] = []
        self.raise_on_enqueue = raise_on_enqueue

    async def __call__(
        self,
        conn: Any = None,
        *,
        catalog_id: str,
        rows: Sequence[WriteIdOutboxRecord],
    ) -> None:
        self.enqueued_calls.append({
            "conn": conn,
            "catalog_id": catalog_id,
            "rows": list(rows),
        })
        if self.raise_on_enqueue:
            raise RuntimeError("outbox enqueue failed (test)")


def _make_routing_resolver(
    *,
    fatal_drivers: List[str],
    outbox_drivers: List[str],
):
    """Return an async callable shaped like the routing resolver."""

    operations = {
        Operation.WRITE: [
            OperationDriverEntry(
                driver_ref=d,
                on_failure=FailurePolicy.FATAL,
                write_mode=WriteMode.SYNC,
            )
            for d in fatal_drivers
        ] + [
            # Secondary-index sinks live in the same WRITE list, role-tagged
            # via secondary_index=True (#990).
            OperationDriverEntry(
                driver_ref=d,
                on_failure=FailurePolicy.OUTBOX,
                write_mode=WriteMode.ASYNC,
                secondary_index=True,
            )
            for d in outbox_drivers
        ],
    }

    @dataclass
    class _StubRouting:
        operations: Dict[str, List[OperationDriverEntry]]

    routing = _StubRouting(operations=operations)

    async def resolve(catalog_id: str, collection_id: Optional[str]):
        return routing

    return resolve


def _make_registry(driver_map: Dict[str, Any]):
    async def lookup(driver_ref: str):
        return driver_map.get(driver_ref)
    return lookup


def _service_with(
    *,
    engine: _FakeEngine,
    driver_map: Dict[str, Any],
    fatal_drivers: List[str],
    outbox_drivers: List[str],
) -> ItemService:
    svc = ItemService(engine=engine)  # type: ignore[arg-type]
    # Inject test seams. The new ``upsert_bulk`` honours these slots when
    # they are not None, bypassing the production routing/registry resolution
    # helpers. The async ledger enqueue goes through the patched module-level
    # ``enqueue_storage_op_write_id``, so no live tasks table is needed.
    svc._test_routing_resolver = _make_routing_resolver(  # type: ignore[attr-defined]
        fatal_drivers=fatal_drivers, outbox_drivers=outbox_drivers,
    )
    svc._test_driver_registry = _make_registry(driver_map)  # type: ignore[attr-defined]
    svc._test_managed_transaction = _fake_managed_transaction  # type: ignore[attr-defined]
    return svc


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_bulk_coalesces_same_item_ops_in_chunk():
    """Same-id input collapses to one primary row and one write-id ledger row."""
    engine = _FakeEngine()
    driver = _RecordingDriver()
    capture = _EnqueueCapture()
    svc = _service_with(
        engine=engine,
        driver_map={"items_postgresql_driver": driver},
        fatal_drivers=["items_postgresql_driver"],
        outbox_drivers=["items_elasticsearch_driver"],
    )

    items = [
        {"id": "i1", "v": 1},
        {"id": "i1", "v": 2},  # latest must win for both PG and outbox
        {"id": "i1", "v": 3},
    ]

    with patch(_ENQUEUE, capture):
        await svc.upsert_bulk("cat", "col", items)

    assert engine.tx_state.committed
    assert not engine.tx_state.rolled_back

    # PG receives the coalesced single item with the latest payload.
    assert len(driver.calls) == 1
    assert driver.calls[0]["entities"] == [{"id": "i1", "v": 3}]
    assert driver.calls[0]["db_resource"] is engine.conn

    # The ledger receives one lightweight row for the ASYNC OUTBOX entry,
    # carrying only the write id. No feature payload is copied into tasks.storage.
    assert len(capture.enqueued_calls) == 1
    rows = capture.enqueued_calls[0]["rows"]
    assert len(rows) == 1
    assert rows[0].driver_id == "items_elasticsearch_driver"
    assert rows[0].op == "upsert"
    assert rows[0].collection_id == "col"
    assert rows[0].write_id
    assert rows[0].idempotency_key == rows[0].write_id
    assert driver.calls[0]["context"]["write_id"] == rows[0].write_id
    assert capture.enqueued_calls[0]["conn"] is engine.conn


@pytest.mark.asyncio
async def test_upsert_bulk_rolls_back_on_outbox_failure():
    """An outbox enqueue failure rolls back the wrapping TX."""
    engine = _FakeEngine()
    driver = _RecordingDriver()
    capture = _EnqueueCapture(raise_on_enqueue=True)
    svc = _service_with(
        engine=engine,
        driver_map={"items_postgresql_driver": driver},
        fatal_drivers=["items_postgresql_driver"],
        outbox_drivers=["items_elasticsearch_driver"],
    )

    items = [{"id": "i1", "v": 1}]

    with pytest.raises(RuntimeError, match="outbox enqueue failed"), patch(_ENQUEUE, capture):
        await svc.upsert_bulk("cat", "col", items)

    # PG bulk write was attempted (it ran before the outbox enqueue),
    # but the wrapping TX rolled back so nothing is durable.
    assert len(driver.calls) == 1
    assert engine.tx_state.entered
    assert engine.tx_state.rolled_back
    assert not engine.tx_state.committed


@pytest.mark.asyncio
async def test_upsert_bulk_does_not_enqueue_when_pg_fails():
    """A FATAL driver failure prevents the outbox enqueue from running."""
    engine = _FakeEngine()
    driver = _RecordingDriver(raise_on_write=True)
    capture = _EnqueueCapture()
    svc = _service_with(
        engine=engine,
        driver_map={"items_postgresql_driver": driver},
        fatal_drivers=["items_postgresql_driver"],
        outbox_drivers=["items_elasticsearch_driver"],
    )

    items = [{"id": "i1", "v": 1}]

    with pytest.raises(RuntimeError, match="driver bulk write failed"), patch(_ENQUEUE, capture):
        await svc.upsert_bulk("cat", "col", items)

    # PG was attempted; outbox enqueue must NOT run.
    assert len(driver.calls) == 1
    assert capture.enqueued_calls == []
    assert engine.tx_state.entered
    assert engine.tx_state.rolled_back
    assert not engine.tx_state.committed


@pytest.mark.asyncio
async def test_upsert_bulk_write_id_is_shared_across_async_targets():
    """One logical primary write id fans out as one row per async target."""
    engine = _FakeEngine()
    driver = _RecordingDriver()
    capture = _EnqueueCapture()
    svc = _service_with(
        engine=engine,
        driver_map={"items_postgresql_driver": driver},
        fatal_drivers=["items_postgresql_driver"],
        outbox_drivers=["items_elasticsearch_driver", "items_audit_driver"],
    )

    items = [
        {"id": "i1", "properties": {"CODE": "ITA_01"}},
        {"id": "i2", "properties": {"CODE": "ITA_02"}},
    ]

    with patch(_ENQUEUE, capture):
        await svc.upsert_bulk(
            "cat", "col", items, processing_context={"asset_id": "asset-xyz"},
        )

    rows = capture.enqueued_calls[0]["rows"]
    assert len(rows) == 2
    assert {r.driver_id for r in rows} == {
        "items_elasticsearch_driver",
        "items_audit_driver",
    }
    assert {r.write_id for r in rows} == {driver.calls[0]["context"]["write_id"]}
    assert all(r.op == "upsert" for r in rows)
