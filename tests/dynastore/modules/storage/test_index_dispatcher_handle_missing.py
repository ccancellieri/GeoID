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
registered locally), the dispatcher must durably enqueue the op rather
than silently dropping it — a configured-but-not-installed indexer is
still recognised and routed to the drain (see the method's docstring:
this call site runs BEFORE any proactive obligation would otherwise be
written, so dropping here would lose the write forever). INDEX entries
carry no per-entry failure policy any more — the enqueue is unconditional,
and the WARN log is deduped once per ``(driver_id, catalog, collection)``
so a deliberately-omitted driver doesn't flood the log on every op.
"""

from __future__ import annotations

import logging
from uuid import uuid4

import pytest

from dynastore.models.protocols.indexer import IndexContext
from dynastore.models.protocols.indexing import IndexableOp
from dynastore.modules.storage.index_dispatcher import IndexDispatcher
from dynastore.modules.storage.routing_config import Operation, OperationDriverEntry


class _StubRouting:
    def __init__(self, entries):
        self.operations = {Operation.INDEX: entries}


class _RecordingWriter:
    def __init__(self) -> None:
        self.rows: list = []

    async def enqueue(self, *, indexer_id, ctx, ops, last_error=None, chunk_size=None) -> None:
        for op in ops:
            self.rows.append({"indexer_id": indexer_id, "op": op})


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
    return IndexContext(catalog="c", collection="cc", pg_conn=object())


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
async def test_missing_indexer_durably_enqueues(ctx, op):
    entry = OperationDriverEntry(driver_ref="d", source="auto")
    writer = _RecordingWriter()
    # A missing indexer is durably enqueued via _handle_missing but never
    # gets a BulkResult entry — fan_out_bulk `continue`s before building one
    # for this entry (there is no in-process dispatch outcome to report).
    results = await _dispatcher([entry], outbox=writer).fan_out_bulk(ctx, [op])
    assert len(writer.rows) == 1
    assert writer.rows[0]["indexer_id"] == "d"
    assert "d" not in results


@pytest.mark.asyncio
async def test_missing_indexer_warns_once_per_triple(ctx, op, caplog):
    entry = OperationDriverEntry(driver_ref="d", source="auto")
    writer = _RecordingWriter()
    d = _dispatcher([entry], outbox=writer)
    with caplog.at_level(
        logging.WARNING,
        logger="dynastore.modules.storage.index_dispatcher",
    ):
        await d.fan_out_bulk(ctx, [op])
        await d.fan_out_bulk(ctx, [op])
    warns = [r for r in caplog.records if "indexer 'd'" in r.message]
    assert len(warns) == 1
    # Both dispatches still durably enqueue — the dedup is log-only.
    assert len(writer.rows) == 2


@pytest.mark.asyncio
async def test_missing_indexer_without_outbox_writer_does_not_raise(ctx, op, caplog):
    """No OutboxWriter wired: the enqueue drops (a one-time WARN is logged
    by ``_enqueue_obligation`` itself) but the dispatch never raises."""
    entry = OperationDriverEntry(driver_ref="d", source="auto")
    with caplog.at_level(logging.WARNING, logger="dynastore.modules.storage.index_dispatcher"):
        results = await _dispatcher([entry]).fan_out_bulk(ctx, [op])
    assert "d" not in results
