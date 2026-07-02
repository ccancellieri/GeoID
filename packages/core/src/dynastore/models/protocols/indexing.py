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


@runtime_checkable
class BulkIndexer(Protocol):
    indexer_id: str
    preferred_chunk_size: int

    async def index_bulk(self, ops: Sequence[IndexableOp]) -> BulkIndexResult: ...


@runtime_checkable
class FailureClassifier(Protocol):
    def classify(self, exc: Exception) -> Literal["transient", "poison"]: ...
