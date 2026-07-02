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

"""ESBulkIndexer wire-contract tests.

The adapter talks to an **opensearch-py** ``AsyncOpenSearch`` client whose
``bulk`` method takes a keyword-only ``body`` argument — NOT the
elasticsearch-py 8.x ``operations=`` keyword. The fake client below mirrors
that exact signature, so any regression back to ``operations=`` (or a
positional call) fails these tests with a ``TypeError`` instead of silently
funnelling every batch to transient retry in production (the zero-``_bulk``
drain incident).
"""
from __future__ import annotations

import logging
from typing import Any
from uuid import uuid4

import pytest

from dynastore.models.protocols.indexing import IndexableOp
from dynastore.tasks.workclass_drain.es_indexer_adapter import ESBulkIndexer


class _FakeAsyncClient:
    """Mimics opensearch-py ``AsyncOpenSearch.bulk`` keyword-only contract."""

    def __init__(self) -> None:
        self.calls: list[Any] = []

    async def bulk(
        self, *, body: Any, index: Any = None, params: Any = None,
        headers: Any = None,
    ) -> Any:
        self.calls.append(body)
        items = []
        for line in body:
            if isinstance(line, dict) and ("index" in line or "delete" in line):
                action = "index" if "index" in line else "delete"
                items.append({action: {"_id": line[action]["_id"], "status": 200}})
        return {"errors": False, "items": items}


class _RaisingClient:
    async def bulk(self, *, body: Any, **kwargs: Any) -> Any:
        raise ConnectionError("wire down")


class _FakeDriver:
    def __init__(self, client: Any) -> None:
        self._client = client

    def _get_index_name(self, catalog_id: str) -> str:
        return f"test-{catalog_id}-items"

    def _get_client(self) -> Any:
        return self._client


def _op(kind: str) -> IndexableOp:
    oid = uuid4()
    return IndexableOp(
        op_id=oid,
        op=kind,  # type: ignore[arg-type]
        catalog_id="cat1",
        collection_id="col1",
        driver_instance_id="es-default",
        item_id=str(oid),
        payload={"id": str(oid)} if kind == "upsert" else {},
        idempotency_key=f"cat1/col1/{oid}",
    )


@pytest.mark.asyncio
async def test_index_bulk_calls_client_with_body_kwarg() -> None:
    client = _FakeAsyncClient()
    indexer = ESBulkIndexer(_FakeDriver(client))
    ops = [_op("upsert"), _op("upsert"), _op("delete")]

    result = await indexer.index_bulk(ops)

    assert len(client.calls) == 1, "bulk must be called exactly once"
    body = client.calls[0]
    # 2 upserts (action + source) + 1 delete (action only) = 5 lines.
    assert len(body) == 5
    assert result.passed == [op.op_id for op in ops]
    assert result.transient == []
    assert result.poison == []


@pytest.mark.asyncio
async def test_index_bulk_whole_batch_failure_is_transient_and_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    indexer = ESBulkIndexer(_FakeDriver(_RaisingClient()))
    ops = [_op("upsert"), _op("delete")]

    with caplog.at_level(logging.WARNING):
        result = await indexer.index_bulk(ops)

    assert result.passed == []
    assert {op_id for op_id, _ in result.transient} == {op.op_id for op in ops}
    assert any(
        "whole-batch bulk call failed" in rec.message for rec in caplog.records
    ), "a whole-batch failure must never be silent"
