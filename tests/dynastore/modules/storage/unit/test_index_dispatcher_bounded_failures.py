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

"""Regression test: IndexDispatcher's per-chunk aggregation stays bounded.

Contract verified (#2657):
  ``_dispatch_bulk_chunked`` splits a bulk of ops into
  ``INLINE_DISPATCH_CHUNK_SIZE``-sized chunks and aggregates the per-chunk
  ``BulkResult``s SEQUENTIALLY. Before this fix the aggregation concatenated
  ``failures`` across every chunk with no bound — on a large ingest against a
  degraded secondary index (every chunk returning per-doc failures) the
  accumulated list grew to O(dataset) instead of O(chunk), contributing to
  the OOM described in #2657. This asserts the aggregated ``failures`` list
  never exceeds ``MAX_ACCUMULATED_FAILURE_SAMPLES`` while the integer counts
  stay exact.
"""
from __future__ import annotations

from typing import List, Sequence

import pytest

from dynastore.models.protocols.indexer import (
    MAX_ACCUMULATED_FAILURE_SAMPLES,
    BulkResult,
    IndexContext,
    IndexOp,
)
from dynastore.modules.storage.index_dispatcher import (
    INLINE_DISPATCH_CHUNK_SIZE,
    IndexDispatcher,
)
from dynastore.modules.storage.routing_config import (
    FailurePolicy,
    Operation,
    OperationDriverEntry,
    WriteMode,
)
from dynastore.tools.execution_context import task_run_scope


class _AllFailIndexer:
    """Every op in every chunk comes back as a per-doc failure — models a
    secondary index degraded under load (e.g. ES returning per-doc errors).
    """

    async def ensure_indexer(self, ctx: IndexContext) -> None:
        return None

    async def index_bulk(
        self, ctx: IndexContext, ops: Sequence[IndexOp],
    ) -> BulkResult:
        return BulkResult(
            total=len(ops),
            failed=len(ops),
            failures=[{"id": op.entity_id, "error": "x" * 32} for op in ops],
        )


def _entry(driver_ref: str) -> OperationDriverEntry:
    return OperationDriverEntry(
        driver_ref=driver_ref,
        on_failure=FailurePolicy.WARN,
        write_mode=WriteMode.SYNC,
        secondary_index=True,
        source="auto",
    )


def _ctx() -> IndexContext:
    return IndexContext(catalog="cat-x", collection="col-y", correlation_id="cid-1")


def _ops(n: int) -> List[IndexOp]:
    return [
        IndexOp(op_type="upsert", entity_type="item", entity_id=f"i{i}", payload={})
        for i in range(n)
    ]


class _StubRouting:
    def __init__(self, entries):
        self.operations = {Operation.WRITE: entries}


def _make_dispatcher(entries, indexers) -> IndexDispatcher:
    routing = _StubRouting(entries)

    async def routing_resolver(catalog, collection):
        return routing

    async def indexer_registry(indexer_id):
        return indexers.get(indexer_id)

    return IndexDispatcher(
        routing_resolver=routing_resolver,
        indexer_registry=indexer_registry,
    )


@pytest.mark.asyncio
async def test_chunked_aggregation_bounds_failures_across_many_chunks():
    """A batch spanning several chunks, every chunk failing every op, must
    not accumulate an unbounded ``failures`` list — the aggregated sample
    stays capped while the exact counts remain correct.
    """
    n_chunks = 5
    n = INLINE_DISPATCH_CHUNK_SIZE * n_chunks
    dispatcher = _make_dispatcher(
        entries=[_entry("a")],
        indexers={"a": _AllFailIndexer()},
    )
    with task_run_scope():
        results = await dispatcher.fan_out_bulk(_ctx(), _ops(n))

    result = results["a"]
    # Exact counts survive the bound.
    assert result.total == n
    assert result.failed == n
    # The detail sample is capped — not the n (2500) failures a naive
    # unbounded concat across chunks would hold.
    assert len(result.failures) <= MAX_ACCUMULATED_FAILURE_SAMPLES
    assert n > MAX_ACCUMULATED_FAILURE_SAMPLES  # sanity: the bound is actually exercised
