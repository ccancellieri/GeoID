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
   produce a single outbox row (latest payload wins). Halves outbox
   volume for hot-update collections without losing fidelity, since a
   consumer applying the original ops in order would converge on the
   same final state.
2. **Atomicity on outbox failure** — when ``enqueue_storage_op``
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
   ``_owner`` (#1285/#1287).

The tests inject a fake routing resolver, a fake driver registry, an
outbox-store presence sentinel, and a stub TX/db resource via the
``ItemService.upsert_bulk`` injection seams, and patch the module-level
``enqueue_storage_op`` (the single storage write path since #1807 P4) to
capture the enqueued rows. They never construct the full app graph —
keeping the suite cheap and targeted at the atomic-enqueue contract.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence
from unittest.mock import patch

import pytest

from dynastore.models.protocols.indexing import OutboxRecord
from dynastore.modules.catalog.item_service import ItemService
from dynastore.modules.storage.routing_config import (
    FailurePolicy,
    Operation,
    OperationDriverEntry,
    WriteMode,
)

# Where ``upsert_bulk`` imports the storage-emit function (patched per test).
_ENQUEUE = "dynastore.modules.storage.storage_emit.enqueue_storage_op"


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


class _EnqueueCapture:
    """Stand-in for ``enqueue_storage_op`` — records (conn, catalog_id, rows).

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
        rows: Sequence[OutboxRecord],
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
    # helpers. The async-OUTBOX enqueue goes through the patched module-level
    # ``enqueue_storage_op`` (#1807 P4), so no outbox-store seam is needed.
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
    """Three ops with the same id collapse to one outbox row (latest wins)."""
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

    # Outbox receives one row for the (single) ASYNC OUTBOX entry,
    # carrying the latest payload.
    assert len(capture.enqueued_calls) == 1
    rows = capture.enqueued_calls[0]["rows"]
    assert len(rows) == 1
    assert rows[0].driver_id == "items_elasticsearch_driver"
    assert rows[0].item_id == "i1"
    assert rows[0].payload == {"id": "i1", "v": 3}
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
async def test_upsert_bulk_stamps_identity_on_outbox_records():
    """OUTBOX payloads carry the canonical ``_external_id`` (from the write
    policy's ``external_id_path``) and ``_asset_id`` (from
    ``processing_context``) — parity with the read-back dispatch path (#1287).
    The inline FATAL primary write keeps the UNSTAMPED items."""
    engine = _FakeEngine()
    driver = _RecordingDriver()
    capture = _EnqueueCapture()
    svc = _service_with(
        engine=engine,
        driver_map={"items_postgresql_driver": driver},
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

    with patch(_ENQUEUE, capture):
        await svc.upsert_bulk(
            "cat", "col", items, processing_context={"asset_id": "asset-xyz"},
        )

    # OUTBOX rows carry stamped identity, per item.
    rows = capture.enqueued_calls[0]["rows"]
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
    payloads also carry the access envelope (``_visibility`` / ``_owner``) —
    #1285/#1287. The primary write stays clean."""
    engine = _FakeEngine()
    driver = _RecordingDriver()
    capture = _EnqueueCapture()
    svc = _service_with(
        engine=engine,
        driver_map={"items_postgresql_driver": driver},
        fatal_drivers=["items_postgresql_driver"],
        outbox_drivers=["items_elasticsearch_envelope_driver"],
    )

    async def _no_ext(catalog_id, collection_id):
        return None

    async def _envelope(catalog_id, collection_id, processing_context):
        return {
            "_visibility": "private",
            "_owner": "alice",
        }

    svc._resolve_external_id_path = _no_ext  # type: ignore[assignment]
    svc._resolve_access_envelope = _envelope  # type: ignore[assignment]

    items = [{"id": "i1", "properties": {}}]

    with patch(_ENQUEUE, capture):
        await svc.upsert_bulk("cat", "col", items)

    payload = capture.enqueued_calls[0]["rows"][0].payload
    assert payload["_visibility"] == "private"
    assert payload["_owner"] == "alice"
    # Primary store row is untouched by the envelope stamping.
    assert "_visibility" not in driver.calls[0]["entities"][0]
