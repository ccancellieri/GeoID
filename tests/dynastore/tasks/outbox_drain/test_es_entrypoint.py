"""``ESBulkIndexer`` adapter tests — classifies passed / transient /
poison from synthetic ES bulk responses without a live cluster.

Skipped on missing ``opensearchpy`` so the suite still runs in
slim-stack environments where the ES extras aren't installed.
"""
from __future__ import annotations

from uuid import uuid4

import pytest

opensearchpy = pytest.importorskip("opensearchpy")


def _make_op(i: int):
    """Build a minimal upsert :class:`IndexableOp`."""
    from dynastore.models.protocols.indexing import IndexableOp
    return IndexableOp(
        op_id=uuid4(),
        op="upsert",
        catalog_id="cat1",
        collection_id="cc",
        driver_instance_id="x",
        item_id=f"i{i}",
        payload={"id": f"i{i}"},
        idempotency_key=f"i{i}",
    )


@pytest.mark.asyncio
async def test_es_adapter_classifies_passed_transient_poison():
    """3 ops → 1 ok, 1 429 (transient), 1 mapper_parsing (poison)."""
    from dynastore.tasks.outbox_drain.es_indexer_adapter import ESBulkIndexer

    class _StubDriver:
        def _get_client(self):
            return _StubES()

        def _get_index_name(self, catalog_id):
            return f"test-items-{catalog_id}"

    class _StubES:
        async def bulk(self, *, operations):
            return {"items": [
                {"index": {"status": 200, "result": "created"}},
                {"index": {
                    "status": 429,
                    "error": {
                        "type": "es_rejected_execution_exception",
                        "reason": "rate limit",
                    },
                }},
                {"index": {
                    "status": 400,
                    "error": {
                        "type": "mapper_parsing_exception",
                        "reason": "field type mismatch",
                    },
                }},
            ]}

    indexer = ESBulkIndexer(_StubDriver())
    ops = [_make_op(i) for i in range(3)]
    result = await indexer.index_bulk(ops)
    assert len(result.passed) == 1
    assert len(result.transient) == 1
    assert len(result.poison) == 1
    # Specific reasons should mention the ES error type.
    assert "mapper_parsing_exception" in result.poison[0][1]
    assert "429" in result.transient[0][1]


@pytest.mark.asyncio
async def test_es_adapter_treats_connection_error_as_transient():
    """A client-level exception means nothing reached the cluster — every
    op in the batch should be retried, none poisoned."""
    from dynastore.tasks.outbox_drain.es_indexer_adapter import ESBulkIndexer

    class _StubDriver:
        def _get_client(self):
            return _BoomES()

        def _get_index_name(self, catalog_id):
            return f"test-items-{catalog_id}"

    class _BoomES:
        async def bulk(self, *, operations):
            raise ConnectionError("nope")

    indexer = ESBulkIndexer(_StubDriver())
    result = await indexer.index_bulk([_make_op(0)])
    assert len(result.passed) == 0
    assert len(result.poison) == 0
    assert len(result.transient) == 1
    assert "ConnectionError" in result.transient[0][1]
