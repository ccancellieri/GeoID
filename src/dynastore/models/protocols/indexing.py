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

"""Indexing Protocols and value types — frozen contract for the
multi-driver bulk-write architecture.

* IndexableOp           — one durable op (one row of storage_outbox)
* BulkIndexResult       — per-row outcome from BulkIndexer.index_bulk
* OutboxRecord/OutboxRow — DTOs for OutboxStore
* IndexFailureRecord    — DTO for IndexFailureLog

All Protocols are runtime_checkable so `isinstance(obj, BulkIndexer)`
works for discovery.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import (
    Any, AsyncIterator, List, Literal, Optional, Protocol, Sequence, Tuple,
    runtime_checkable,
)
from uuid import UUID


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
class IndexFailureRecord:
    failure_id: UUID
    occurred_at: datetime
    collection_id: str
    driver_id: str
    driver_instance_id: str
    op_id: Optional[UUID]
    item_id: Optional[str]
    op: str  # raw-from-DB TEXT; CHECK constraint enforces values, not the dataclass
    attempts: int
    error_class: str
    error_message: str
    status: Literal["retrying", "failed"]
    correlation_id: Optional[str]


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
        backend can disregard the parameter; production ``PgOutboxStore``
        uses it for transaction-sharing."""
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


@runtime_checkable
class IndexFailureLog(Protocol):
    async def record(
        self, conn, *,
        catalog_id: str,
        collection_id: str,
        driver_instance_id: str,
        driver_id: str,
        op_id: UUID,
        item_id: Optional[str],
        op: str,
        attempts: int,
        error_class: str,
        error_message: str,
        status: Literal["retrying", "failed"],
        correlation_id: Optional[str] = None,
    ) -> None: ...

    async def list_failures(
        self, *,
        catalog_id: str,
        collection_id: Optional[str] = None,
        driver_id: Optional[str] = None,
        since: Optional[datetime] = None,
        status: Optional[Literal["retrying", "failed"]] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Sequence[IndexFailureRecord]: ...
