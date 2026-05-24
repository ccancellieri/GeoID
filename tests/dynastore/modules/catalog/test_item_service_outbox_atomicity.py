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

"""``ItemService.upsert_bulk`` atomic outbox enqueue + same-item coalescing.

Scenarios covered:

1. **Coalesce same-item ops in-chunk** — three ops with the same ``id``
   produce a single outbox row (latest payload wins). Halves outbox
   volume for hot-update collections without losing fidelity, since a
   consumer applying the original ops in order would converge on the
   same final state.
2. **Atomicity on outbox failure** — when ``OutboxStore.enqueue_bulk``
   raises inside the wrapping TX, the FATAL driver's bulk write is
   rolled back. ``upsert_bulk`` re-raises so the caller knows nothing
   landed.
3. **Atomicity on PG failure** — when the FATAL driver raises before
   the outbox enqueue runs, the outbox enqueue is never invoked (the
   TX context manager exits with the exception before the outbox
   step).
4. **Identity stamping on OUTBOX records** — ``_external_id`` / ``_asset_id``
   are stamped onto each outbox payload (parity with the read-back dispatch
   path, #1287), applied to a per-record copy so the FATAL primary write keeps
   the unstamped items.
5. **Access-envelope stamping** — when the collection routes WRITE to an
   access-aware driver, the outbox payload also carries ``_visibility`` /
   ``_owner`` / ``_grant_subjects`` (#1285/#1287).

The tests inject a fake routing resolver, a fake driver registry, a
fake outbox store, and a stub TX/db resource via the new
``ItemService.upsert_bulk`` injection seams. They never construct the
full app graph — keeping the suite cheap and targeted at the new
contract introduced for Phase 9 (atomic outbox enqueue).
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence
from uuid import UUID

import pytest

from dynastore.models.protocols.indexing import OutboxRecord
from dynastore.modules.catalog.item_service import ItemService
from dynastore.modules.storage.routing_config import (
    FailurePolicy,
    Operation,
    OperationDriverEntry,
    WriteMode,
)


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
    """Stub PG bulk driver — records calls and the conn it was given."""

    def __init__(self, *, raise_on_write: bool = False) -> None:
        self.calls: List[Dict[str, Any]] = []
        self.raise_on_write = raise_on_write

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
            "db_resource": db_resource,
        })
        if self.raise_on_write:
            raise RuntimeError("driver bulk write failed (test)")
        return list(entities)


class _RecordingOutbox:
    """Stub :class:`OutboxStore` — records enqueue_bulk calls.

    Implements the full ``OutboxStore`` runtime_checkable Protocol surface
    (six methods); tests only exercise ``enqueue_bulk``."""

    def __init__(self, *, raise_on_enqueue: bool = False) -> None:
        self.enqueued_calls: List[Dict[str, Any]] = []
        self.raise_on_enqueue = raise_on_enqueue

    async def enqueue_bulk(
        self,
        conn: Any = None,
        *,
        catalog_id: str,
        rows: Sequence[OutboxRecord],
    ) -> None:
        self.enqueued_calls.append({
            "conn": conn,
            "catalog_id": catalog_id,
            "rows": list(rows),
        })
        if self.raise_on_enqueue:
            raise RuntimeError("outbox enqueue failed (test)")

    async def claim_batch(
        self, *, driver_ref: str, catalog_id: str,
        batch_size: int, claimed_by: str,
    ) -> List[Any]:
        return []

    async def mark_done(self, *, catalog_id: str, op_ids: Sequence[UUID]) -> None:
        return None

    async def mark_retry(
        self, *, catalog_id: str, op_ids: Sequence[UUID],
        error: str, attempts_seen: int,
    ) -> None:
        return None

    async def mark_failed(
        self, *, catalog_id: str, op_ids: Sequence[UUID], error: str,
    ) -> None:
        return None

    def listen(self, *, driver_ref: str, catalog_id: str):  # type: ignore[no-untyped-def]
        async def _empty():
            if False:
                yield  # pragma: no cover
        return _empty()


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
    outbox: Any,
    fatal_drivers: List[str],
    outbox_drivers: List[str],
) -> ItemService:
    svc = ItemService(engine=engine)  # type: ignore[arg-type]
    # Inject test seams. The new ``upsert_bulk`` honours these slots when
    # they are not None, bypassing the production routing/registry/outbox
    # resolution helpers.
    svc._test_routing_resolver = _make_routing_resolver(  # type: ignore[attr-defined]
        fatal_drivers=fatal_drivers, outbox_drivers=outbox_drivers,
    )
    svc._test_driver_registry = _make_registry(driver_map)  # type: ignore[attr-defined]
    svc._test_outbox_store = outbox  # type: ignore[attr-defined]
    svc._test_managed_transaction = _fake_managed_transaction  # type: ignore[attr-defined]
    return svc


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_bulk_coalesces_same_item_ops_in_chunk():
    """Three ops with the same id collapse to one outbox row (latest wins)."""
    engine = _FakeEngine()
    driver = _RecordingDriver()
    outbox = _RecordingOutbox()
    svc = _service_with(
        engine=engine,
        driver_map={"items_postgresql_driver": driver},
        outbox=outbox,
        fatal_drivers=["items_postgresql_driver"],
        outbox_drivers=["items_elasticsearch_driver"],
    )

    items = [
        {"id": "i1", "v": 1},
        {"id": "i1", "v": 2},  # latest must win for both PG and outbox
        {"id": "i1", "v": 3},
    ]

    await svc.upsert_bulk("cat", "col", items)

    assert engine.tx_state.committed
    assert not engine.tx_state.rolled_back

    # PG receives the coalesced single item with the latest payload.
    assert len(driver.calls) == 1
    assert driver.calls[0]["entities"] == [{"id": "i1", "v": 3}]
    assert driver.calls[0]["db_resource"] is engine.conn

    # Outbox receives one row for the (single) ASYNC OUTBOX entry,
    # carrying the latest payload.
    assert len(outbox.enqueued_calls) == 1
    rows = outbox.enqueued_calls[0]["rows"]
    assert len(rows) == 1
    assert rows[0].driver_id == "items_elasticsearch_driver"
    assert rows[0].item_id == "i1"
    assert rows[0].payload == {"id": "i1", "v": 3}
    assert outbox.enqueued_calls[0]["conn"] is engine.conn


@pytest.mark.asyncio
async def test_upsert_bulk_rolls_back_on_outbox_failure():
    """An outbox enqueue failure rolls back the wrapping TX."""
    engine = _FakeEngine()
    driver = _RecordingDriver()
    outbox = _RecordingOutbox(raise_on_enqueue=True)
    svc = _service_with(
        engine=engine,
        driver_map={"items_postgresql_driver": driver},
        outbox=outbox,
        fatal_drivers=["items_postgresql_driver"],
        outbox_drivers=["items_elasticsearch_driver"],
    )

    items = [{"id": "i1", "v": 1}]

    with pytest.raises(RuntimeError, match="outbox enqueue failed"):
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
    outbox = _RecordingOutbox()
    svc = _service_with(
        engine=engine,
        driver_map={"items_postgresql_driver": driver},
        outbox=outbox,
        fatal_drivers=["items_postgresql_driver"],
        outbox_drivers=["items_elasticsearch_driver"],
    )

    items = [{"id": "i1", "v": 1}]

    with pytest.raises(RuntimeError, match="driver bulk write failed"):
        await svc.upsert_bulk("cat", "col", items)

    # PG was attempted; outbox enqueue must NOT run.
    assert len(driver.calls) == 1
    assert outbox.enqueued_calls == []
    assert engine.tx_state.entered
    assert engine.tx_state.rolled_back
    assert not engine.tx_state.committed


@pytest.mark.asyncio
async def test_upsert_bulk_stamps_identity_on_outbox_records():
    """OUTBOX payloads carry the canonical ``_external_id`` (from the write
    policy's ``external_id_path``) and ``_asset_id`` (from
    ``processing_context``) — parity with the read-back dispatch path (#1287).
    The inline FATAL primary write keeps the UNSTAMPED items."""
    engine = _FakeEngine()
    driver = _RecordingDriver()
    outbox = _RecordingOutbox()
    svc = _service_with(
        engine=engine,
        driver_map={"items_postgresql_driver": driver},
        outbox=outbox,
        fatal_drivers=["items_postgresql_driver"],
        outbox_drivers=["items_elasticsearch_driver"],
    )

    # Stub the per-collection derivations the stamp context resolves through
    # (no ConfigsProtocol / routing in this unit graph).
    async def _ext_path(catalog_id, collection_id):
        return "properties.CODE"

    async def _no_envelope(catalog_id, collection_id, processing_context):
        return None  # non-access-aware collection → no access fields

    svc._resolve_external_id_path = _ext_path  # type: ignore[assignment]
    svc._resolve_access_envelope = _no_envelope  # type: ignore[assignment]

    items = [
        {"id": "i1", "properties": {"CODE": "ITA_01"}},
        {"id": "i2", "properties": {"CODE": "ITA_02"}},
    ]

    await svc.upsert_bulk(
        "cat", "col", items, processing_context={"asset_id": "asset-xyz"},
    )

    # OUTBOX rows carry stamped identity, per item.
    rows = outbox.enqueued_calls[0]["rows"]
    by_id = {r.item_id: r.payload for r in rows}
    assert by_id["i1"]["_external_id"] == "ITA_01"
    assert by_id["i1"]["_asset_id"] == "asset-xyz"
    assert by_id["i2"]["_external_id"] == "ITA_02"
    assert by_id["i2"]["_asset_id"] == "asset-xyz"
    # Non-access-aware collection: no access-envelope fields leaked in.
    assert "_visibility" not in by_id["i1"]
    assert "_owner" not in by_id["i1"]

    # The inline FATAL primary write received the UNSTAMPED items: stamping is
    # applied to a per-record copy, never the shared store payload.
    pg_entities = driver.calls[0]["entities"]
    assert all("_external_id" not in e for e in pg_entities)
    assert all("_asset_id" not in e for e in pg_entities)


@pytest.mark.asyncio
async def test_upsert_bulk_stamps_access_envelope_for_access_aware_driver():
    """When the collection routes WRITE to an access-aware driver, OUTBOX
    payloads also carry the access envelope (``_visibility`` / ``_owner`` /
    ``_grant_subjects``) — #1285/#1287. The primary write stays clean."""
    engine = _FakeEngine()
    driver = _RecordingDriver()
    outbox = _RecordingOutbox()
    svc = _service_with(
        engine=engine,
        driver_map={"items_postgresql_driver": driver},
        outbox=outbox,
        fatal_drivers=["items_postgresql_driver"],
        outbox_drivers=["items_elasticsearch_envelope_driver"],
    )

    async def _no_ext(catalog_id, collection_id):
        return None

    async def _envelope(catalog_id, collection_id, processing_context):
        return {
            "_visibility": "private",
            "_owner": "alice",
            "_grant_subjects": [],
        }

    svc._resolve_external_id_path = _no_ext  # type: ignore[assignment]
    svc._resolve_access_envelope = _envelope  # type: ignore[assignment]

    items = [{"id": "i1", "properties": {}}]

    await svc.upsert_bulk("cat", "col", items)

    payload = outbox.enqueued_calls[0]["rows"][0].payload
    assert payload["_visibility"] == "private"
    assert payload["_owner"] == "alice"
    assert payload["_grant_subjects"] == []
    # Primary store row is untouched by the envelope stamping.
    assert "_visibility" not in driver.calls[0]["entities"][0]
