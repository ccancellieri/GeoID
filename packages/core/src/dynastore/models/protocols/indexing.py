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
* OutboxRecord          — DTO for the durable outbox enqueue path

All Protocols are runtime_checkable so `isinstance(obj, BulkIndexer)`
works for discovery.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import (
    Any, List, Literal, Optional, Protocol, Sequence, Tuple,
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
    """Id-only ledger row: the drain re-reads canonical state by ``item_id``.

    ``tasks.storage`` carries no payloads — a row is either id-only
    (``item_id`` set) or a write-id batch reference
    (:class:`WriteIdOutboxRecord`).
    """
    op_id: UUID
    driver_id: str
    driver_instance_id: str
    collection_id: str
    op: Literal["upsert", "delete"]
    item_id: Optional[str]
    idempotency_key: str


@dataclass(frozen=True)
class WriteIdOutboxRecord:
    op_id: UUID
    driver_id: str
    driver_instance_id: str
    collection_id: str
    op: Literal["upsert", "delete"]
    write_id: str
    idempotency_key: str


@runtime_checkable
class BulkIndexer(Protocol):
    indexer_id: str
    preferred_chunk_size: int

    async def index_bulk(self, ops: Sequence[IndexableOp]) -> BulkIndexResult: ...


@runtime_checkable
class FailureClassifier(Protocol):
    def classify(self, exc: Exception) -> Literal["transient", "poison"]: ...
