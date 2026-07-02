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

"""Coverage for ``IndexDispatcher._handle_missing``.

When ``_resolve_indexer`` returns ``None`` (the indexer driver isn't
registered locally), the dispatcher must honour the routing entry's
``FailurePolicy`` rather than silently skipping the op.  This makes
"driver down" and "driver not in this SCOPE" indistinguishable from
the operator's perspective — both routes through the same policy.
"""

from __future__ import annotations

import logging
from uuid import uuid4

import pytest

from dynastore.models.protocols.indexer import IndexContext
from dynastore.models.protocols.indexing import IndexableOp
from dynastore.modules.storage.index_dispatcher import (
    IndexDispatcher, IndexerFatal,
)
from dynastore.modules.storage.routing_config import (
    FailurePolicy, Operation, OperationDriverEntry, WriteMode,
)


class _StubRouting:
    def __init__(self, entries):
        self.operations = {Operation.WRITE: entries}


def _dispatcher(entries, *, outbox=None):
    async def routing(c, col):
        return _StubRouting(entries)

    async def registry(driver_ref):
        return None

    return IndexDispatcher(
        routing_resolver=routing,
        indexer_registry=registry,
        outbox=outbox,
    )


@pytest.fixture
def ctx():
    return IndexContext(catalog="c", collection="cc")


@pytest.fixture
def op():
    return IndexableOp(
        op_id=uuid4(),
        op="upsert",
        catalog_id="c",
        collection_id="cc",
        driver_instance_id="x",
        item_id="i1",
        payload={"id": "i1"},
        idempotency_key="i1",
    )


@pytest.mark.asyncio
async def test_missing_fatal_raises(ctx, op):
    entry = OperationDriverEntry(
        driver_ref="d", on_failure=FailurePolicy.FATAL, secondary_index=True,
    )
    with pytest.raises(IndexerFatal):
        await _dispatcher([entry]).fan_out_bulk(ctx, [op])


@pytest.mark.asyncio
async def test_missing_warn_logs_once(ctx, op, caplog):
    entry = OperationDriverEntry(
        driver_ref="d", on_failure=FailurePolicy.WARN, secondary_index=True,
    )
    d = _dispatcher([entry])
    with caplog.at_level(
        logging.WARNING,
        logger="dynastore.modules.storage.index_dispatcher",
    ):
        await d.fan_out_bulk(ctx, [op])
        await d.fan_out_bulk(ctx, [op])
    warns = [r for r in caplog.records if "indexer 'd'" in r.message]
    assert len(warns) == 1


@pytest.mark.asyncio
async def test_missing_ignore_silent(ctx, op, caplog):
    entry = OperationDriverEntry(
        driver_ref="d", on_failure=FailurePolicy.IGNORE, secondary_index=True,
    )
    with caplog.at_level(
        logging.WARNING,
        logger="dynastore.modules.storage.index_dispatcher",
    ):
        await _dispatcher([entry]).fan_out_bulk(ctx, [op])
    assert not [r for r in caplog.records if "indexer 'd'" in r.message]


@pytest.mark.asyncio
async def test_missing_outbox_falls_through_to_enqueue_or_warn(ctx, op, caplog):
    """#2732 step 4: ``_enqueue_outbox_record`` no longer special-cases a
    bulk ``OutboxStore`` — zero concrete implementations exist in this
    codebase, so the dead ``isinstance`` branch was removed. Even an outbox
    stub implementing the full ``OutboxStore`` surface is bypassed;
    ``fan_out_bulk`` always routes an ``IndexableOp`` OUTBOX op through
    :meth:`IndexDispatcher._enqueue_or_warn`, whose legacy singular-enqueue
    path only accepts ``IndexOp`` — so the op is dropped with a WARNING
    rather than silently vanishing.
    """
    enq = []
    legacy_enqueue_calls = []

    # Stub satisfies the full ``OutboxStore`` runtime_checkable Protocol
    # surface AND the legacy singular ``enqueue`` method, so
    # ``_enqueue_or_warn`` proceeds past its "no enqueue method" guard and
    # reaches the IndexableOp-vs-IndexOp filter below. ``enqueue_bulk`` must
    # never be called post-#2732 step 4 — the bulk OutboxStore path is dead
    # code with zero concrete implementations.
    class _Stub:
        async def enqueue_bulk(self, conn, *, catalog_id, rows):
            enq.extend(rows)

        async def enqueue(self, *, indexer_id, ctx, ops, last_error=None):
            legacy_enqueue_calls.append(ops)

        async def claim_batch(self, *, driver_id, catalog_id, batch_size, claimed_by):
            return []

        async def mark_done(self, *, catalog_id, op_ids):
            return None

        async def mark_retry(self, *, catalog_id, op_ids, error, attempts_seen):
            return None

        async def mark_failed(self, *, catalog_id, op_ids, error):
            return None

        def listen(self, *, driver_id, catalog_id):
            async def _empty():
                if False:
                    yield  # pragma: no cover
            return _empty()

    entry = OperationDriverEntry(
        driver_ref="d",
        write_mode=WriteMode.ASYNC,
        on_failure=FailurePolicy.OUTBOX,
        secondary_index=True,
    )
    with caplog.at_level(
        logging.WARNING, logger="dynastore.modules.storage.index_dispatcher",
    ):
        await _dispatcher([entry], outbox=_Stub()).fan_out_bulk(ctx, [op])

    assert enq == [], "enqueue_bulk (the removed bulk OutboxStore path) must never be called"
    assert legacy_enqueue_calls == [], (
        "the legacy singular enqueue() only accepts IndexOp — an all-IndexableOp "
        "batch must be filtered out before reaching it, not passed through empty-handed"
    )
    assert any(
        "IndexableOp" in r.message and "dropping" in r.message
        for r in caplog.records
    ), "an IndexableOp OUTBOX op with no bulk sink must warn, not vanish silently"
