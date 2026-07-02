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

"""Indexing Protocols and value types — frozen contract for the
multi-driver bulk-write architecture.

* IndexableOp           — one durable op (one row of tasks.storage)
* BulkIndexResult       — per-row outcome from BulkIndexer.index_bulk
* OutboxRecord/OutboxRow — DTOs for OutboxStore

All Protocols are runtime_checkable so `isinstance(obj, BulkIndexer)`
works for discovery.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import (
    Any, AsyncIterator, List, Literal, Optional, Protocol, Sequence, Tuple,
    runtime_checkable,
)
from uuid import UUID

# #2494 P1 — the explicit marker key stamped into ``op_payload`` by
# ``storage_emit.enqueue_storage_op_id_only`` so ``StorageDrainTask``
# can distinguish a deliberate id-only obligation from a row that merely
# has an EMPTY payload (the ``tasks.storage`` DDL default, ``'{}'::jsonb``,
# which a producer can also arrive at by omitting the payload). Detecting
# id-only status from emptiness alone collides with that DDL default;
# the explicit key removes the ambiguity. Shared by both call sites so
# neither can drift from the other.
STORAGE_PLANE_ID_ONLY_MARKER_KEY: str = "_id_only"


@dataclass(frozen=True)
class IndexableOp:
    op_id: UUID
    op: Literal["upsert", "delete"]
    catalog_id: str
    collection_id: str
    driver_instance_id: str
    item_id: Optional[str]
    payload: dict[str, Any]
    idempotency_key: str


@dataclass
class BulkIndexResult:
    passed: List[UUID]
    transient: List[Tuple[UUID, str]]   # (op_id, reason)
    poison: List[Tuple[UUID, str]]


@dataclass(frozen=True)
class OutboxRecord:
    op_id: UUID
    driver_id: str
    driver_instance_id: str
    collection_id: str
    op: Literal["upsert", "delete"]
    item_id: Optional[str]
    payload: dict[str, Any]
    idempotency_key: str


@dataclass(frozen=True)
class OutboxRow:
    op_id: UUID
    driver_id: str
    driver_instance_id: str
    catalog_id: str
    collection_id: str
    op: str  # raw-from-DB TEXT; CHECK constraint enforces values, not the dataclass
    item_id: Optional[str]
    payload: dict[str, Any]
    idempotency_key: str
    attempts: int


@dataclass(frozen=True)
class Notification:
    driver_id: str
    catalog_id: str
    op_id: UUID


@runtime_checkable
class BulkIndexer(Protocol):
    indexer_id: str
    preferred_chunk_size: int

    async def index_bulk(self, ops: Sequence[IndexableOp]) -> BulkIndexResult: ...


@runtime_checkable
class FailureClassifier(Protocol):
    def classify(self, exc: Exception) -> Literal["transient", "poison"]: ...


@runtime_checkable
class OutboxStore(Protocol):
    async def enqueue_bulk(
        self, conn: Any = None, *, catalog_id: str, rows: Sequence[OutboxRecord],
    ) -> None:
        """Enqueue a batch of outbox rows.

        ``conn`` may be passed by callers that want the enqueue to join an
        existing transaction (e.g. ``item_service.upsert_bulk`` writing items
        + outbox rows atomically). When ``None``, the implementation acquires
        its own connection from a pool. Implementations targeting a single
        backend can disregard the parameter; implementations that support
        transaction-sharing use it when non-None."""
        ...

    async def claim_batch(
        self, *, driver_id: str, catalog_id: str,
        batch_size: int, claimed_by: str,
    ) -> List[OutboxRow]: ...

    async def mark_done(
        self, *, catalog_id: str, op_ids: Sequence[UUID],
    ) -> None: ...

    async def mark_retry(
        self, *, catalog_id: str, op_ids: Sequence[UUID],
        error: str, attempts_seen: int,
    ) -> None: ...

    async def mark_failed(
        self, *, catalog_id: str, op_ids: Sequence[UUID], error: str,
    ) -> None: ...

    def listen(
        self, *, driver_id: str, catalog_id: str,
    ) -> AsyncIterator[Notification]: ...
