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
        self.operations = {Operation.INDEX: entries}


def _dispatcher(entries, *, outbox=None):
    async def routing(c, col):
        return _StubRouting(entries)

    async def registry(driver_id):
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
    entry = OperationDriverEntry(driver_id="d", on_failure=FailurePolicy.FATAL)
    with pytest.raises(IndexerFatal):
        await _dispatcher([entry]).fan_out(ctx, op)


@pytest.mark.asyncio
async def test_missing_warn_logs_once(ctx, op, caplog):
    entry = OperationDriverEntry(driver_id="d", on_failure=FailurePolicy.WARN)
    d = _dispatcher([entry])
    with caplog.at_level(
        logging.WARNING,
        logger="dynastore.modules.storage.index_dispatcher",
    ):
        await d.fan_out(ctx, op)
        await d.fan_out(ctx, op)
    warns = [r for r in caplog.records if "indexer 'd'" in r.message]
    assert len(warns) == 1


@pytest.mark.asyncio
async def test_missing_ignore_silent(ctx, op, caplog):
    entry = OperationDriverEntry(driver_id="d", on_failure=FailurePolicy.IGNORE)
    with caplog.at_level(
        logging.WARNING,
        logger="dynastore.modules.storage.index_dispatcher",
    ):
        await _dispatcher([entry]).fan_out(ctx, op)
    assert not [r for r in caplog.records if "indexer 'd'" in r.message]


@pytest.mark.asyncio
async def test_missing_outbox_enqueues(ctx, op):
    enq = []

    # Stub satisfies the full ``OutboxStore`` runtime_checkable Protocol
    # surface — the dispatcher narrows via ``isinstance`` so all six
    # methods must be present even when the test only exercises
    # ``enqueue_bulk``.
    class _Stub:
        async def enqueue_bulk(self, conn, *, catalog_id, rows):
            enq.extend(rows)

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
        driver_id="d",
        write_mode=WriteMode.ASYNC,
        on_failure=FailurePolicy.OUTBOX,
    )
    await _dispatcher([entry], outbox=_Stub()).fan_out(ctx, op)
    assert len(enq) == 1
    assert enq[0].driver_id == "d"


@pytest.mark.asyncio
async def test_handle_missing_with_pg_outbox_persists(async_conn, async_schema, ctx, op):
    """End-to-end: missing indexer + OUTBOX policy + PgOutboxStore writes
    a row to ``storage_outbox`` in the tenant schema.

    Exercises the full ``_handle_missing`` → ``_enqueue_outbox_record`` →
    ``PgOutboxStore.enqueue_bulk`` chain against a real PG schema. The
    dispatcher passes ``conn=None`` to ``enqueue_bulk``; the store's
    ``single_conn`` fallback writes through the test's session so the
    fixture's ``search_path`` is honoured.
    """
    from dynastore.modules.storage.outbox_ddl import ensure_storage_outbox_asyncpg
    from dynastore.modules.storage.pg_outbox import PgOutboxStore

    await ensure_storage_outbox_asyncpg(async_conn, async_schema)

    async def routing(c, col):
        return _StubRouting([
            OperationDriverEntry(
                driver_id="missing", write_mode=WriteMode.ASYNC,
                on_failure=FailurePolicy.OUTBOX,
            ),
        ])

    async def registry(driver_id):
        return None

    # IndexableOp.catalog_id must match the test schema so the outbox
    # row lands in the right table — the dispatcher passes
    # ``op.catalog_id`` through to ``OutboxStore.enqueue_bulk`` and the
    # store pins ``search_path`` to that value when acquiring a conn.
    op_for_schema = IndexableOp(
        op_id=op.op_id, op=op.op,
        catalog_id=async_schema, collection_id="cc",
        driver_instance_id="x", item_id="i1",
        payload={"id": "i1"}, idempotency_key="i1",
    )

    dispatcher = IndexDispatcher(
        routing_resolver=routing, indexer_registry=registry,
        outbox=PgOutboxStore(single_conn=async_conn),
    )
    await dispatcher.fan_out(ctx, op_for_schema)

    n = await async_conn.fetchval(  # type: ignore[attr-defined]
        "SELECT count(*) FROM storage_outbox WHERE driver_id='missing'"
    )
    assert n == 1
