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
async def test_index_bulk_delete_chunk_uses_single_bulk_call() -> None:
    client = _FakeAsyncClient()
    indexer = ESBulkIndexer(_FakeDriver(client))
    ops = [_op("delete"), _op("delete")]

    result = await indexer.index_bulk(ops)

    assert len(client.calls) == 1
    assert client.calls[0] == [
        {"delete": {
            "_index": "test-cat1-items",
            "_id": ops[0].idempotency_key,
            "routing": "col1",
        }},
        {"delete": {
            "_index": "test-cat1-items",
            "_id": ops[1].idempotency_key,
            "routing": "col1",
        }},
    ]
    assert result.passed == [op.op_id for op in ops]


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


# ---------------------------------------------------------------------------
# Drain-path parity with inline/rebuild writes (#2769)
# ---------------------------------------------------------------------------


class _CapableFakeDriver(_FakeDriver):
    """Fake driver additionally exposing the simplify-config resolvers and
    ``ensure_storage`` so the adapter's #2769 parity paths engage."""

    def __init__(self, client: Any, *, simplify: bool = False, max_bytes: int = 10_000_000) -> None:
        super().__init__(client)
        self.ensure_storage_calls: list[str] = []
        self._simplify = simplify
        self._max_bytes = max_bytes

    async def ensure_storage(self, catalog_id: str, collection_id: Any = None) -> None:
        self.ensure_storage_calls.append(catalog_id)

    async def _resolve_simplify_geometry(self, catalog_id: str, collection_id: str) -> bool:
        return self._simplify

    async def _resolve_simplify_max_bytes(self, catalog_id: str, collection_id: str) -> int:
        return self._max_bytes

    async def _resolve_snap_to_grid_config(self, catalog_id: str, collection_id: str):
        return False, 1e-5


@pytest.mark.asyncio
async def test_index_bulk_calls_ensure_storage_once_per_catalog() -> None:
    client = _FakeAsyncClient()
    driver = _CapableFakeDriver(client)
    indexer = ESBulkIndexer(driver)

    await indexer.index_bulk([_op("upsert")])
    await indexer.index_bulk([_op("upsert")])

    assert driver.ensure_storage_calls == ["cat1"], (
        "ensure_storage must run at most once per catalog for this adapter's lifetime"
    )


@pytest.mark.asyncio
async def test_index_bulk_missing_driver_capabilities_is_a_no_op() -> None:
    """A driver without ensure_storage/_resolve_simplify_* (the pre-#2769
    fake used by the wire-contract tests above) must not raise — the
    adapter degrades to its previous behaviour."""
    client = _FakeAsyncClient()
    indexer = ESBulkIndexer(_FakeDriver(client))

    result = await indexer.index_bulk([_op("upsert")])

    assert result.poison == []
    assert result.transient == []


class _OversizedRejectingClient:
    """Mimics an ES cluster that rejects any oversized (unsimplified)
    upsert body but accepts a simplified one."""

    def __init__(self, size_limit: int) -> None:
        self.calls: list = []
        self._size_limit = size_limit

    async def bulk(self, *, body: Any, index: Any = None, params: Any = None,
                    headers: Any = None) -> Any:
        import json

        self.calls.append(body)
        items = []
        i = 0
        while i < len(body):
            line = body[i]
            if isinstance(line, dict) and "index" in line:
                doc = body[i + 1]
                doc_id = line["index"]["_id"]
                if len(json.dumps(doc)) > self._size_limit:
                    items.append({"index": {
                        "_id": doc_id, "status": 400,
                        "error": {"type": "document_parsing_exception", "reason": "too big"},
                    }})
                else:
                    items.append({"index": {"_id": doc_id, "status": 200}})
                i += 2
            else:
                i += 1
        return {"errors": any("error" in v.get("index", {}) for v in items), "items": items}


@pytest.mark.asyncio
async def test_index_bulk_applies_byte_budget_simplification() -> None:
    """A large geometry payload the driver's config says to simplify must
    be shrunk before the drain writes it — this adapter previously wrote
    op.payload verbatim, bypassing the byte-budget simplifier entirely."""
    from shapely.geometry import mapping
    from shapely.geometry.polygon import Polygon
    import math

    big_ring = [
        (math.cos(2 * math.pi * i / 4000), math.sin(2 * math.pi * i / 4000))
        for i in range(4000)
    ]
    big_ring.append(big_ring[0])
    big_geometry = mapping(Polygon(big_ring))

    client = _OversizedRejectingClient(size_limit=20_000)
    driver = _CapableFakeDriver(client, simplify=True, max_bytes=20_000)
    indexer = ESBulkIndexer(driver)

    oid = uuid4()
    op = IndexableOp(
        op_id=oid,
        op="upsert",
        catalog_id="cat1",
        collection_id="col1",
        driver_instance_id="es-default",
        item_id=str(oid),
        payload={"id": str(oid), "geometry": big_geometry},
        idempotency_key=f"cat1/col1/{oid}",
    )

    result = await indexer.index_bulk([op])

    assert result.poison == []
    assert result.passed == [oid]


@pytest.mark.asyncio
async def test_index_bulk_recovers_geo_shape_rejection_via_ladder() -> None:
    """A poison-classified geo_shape rejection must be retried through the
    degradation ladder before being reported to the drain."""

    class _RejectBulkAcceptRetryClient:
        def __init__(self) -> None:
            self.index_calls: list = []

        async def bulk(self, *, body: Any, index: Any = None, params: Any = None,
                        headers: Any = None) -> Any:
            action = body[0]["index"]
            return {
                "errors": True,
                "items": [{"index": {
                    "_id": action["_id"], "status": 400,
                    "error": {
                        "type": "document_parsing_exception",
                        "reason": "failed to parse field [geometry] of type [geo_shape]",
                    },
                }}],
            }

        async def index(self, *, index, id, body, params=None):
            self.index_calls.append((index, id, body, params))
            return {"result": "created"}

    client = _RejectBulkAcceptRetryClient()
    driver = _CapableFakeDriver(client)
    indexer = ESBulkIndexer(driver)

    oid = uuid4()
    op = IndexableOp(
        op_id=oid,
        op="upsert",
        catalog_id="cat1",
        collection_id="col1",
        driver_instance_id="es-default",
        item_id=str(oid),
        payload={
            "id": str(oid),
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
            },
        },
        idempotency_key=f"cat1/col1/{oid}",
    )

    result = await indexer.index_bulk([op])

    assert result.poison == []
    assert result.passed == [oid]
    assert client.index_calls, "the ladder must have retried via a single-doc index() call"


# ---------------------------------------------------------------------------
# #2687 — index-name / routing generalization for the envelope driver
# ---------------------------------------------------------------------------


class _ItemsIndexNameOnlyDriver:
    """Driver exposing only ``_items_index_name`` (the real shared seam on
    ``_ItemsElasticsearchBase``) — no ``_get_index_name``/``get_index_name``.
    Mirrors both concrete ES items drivers today."""

    def __init__(self, client: Any, index_name: str, routing: Any = "col1") -> None:
        self._client = client
        self._index_name_value = index_name
        self._routing_value = routing

    def _items_index_name(self, catalog_id: str) -> str:
        return self._index_name_value

    def _get_client(self) -> Any:
        return self._client

    def _collection_routing(self, collection_id: Any) -> Any:
        return self._routing_value


@pytest.mark.asyncio
async def test_index_bulk_resolves_index_name_via_items_index_name() -> None:
    """A driver that only exposes ``_items_index_name`` (both concrete ES
    items drivers) must have it used — not the generic per-catalog fallback,
    which would resolve the WRONG index for the envelope driver (#2687)."""
    client = _FakeAsyncClient()
    driver = _ItemsIndexNameOnlyDriver(client, "prefix-cat1-envelope-items")
    indexer = ESBulkIndexer(driver)

    await indexer.index_bulk([_op("upsert")])

    body = client.calls[0]
    assert body[0]["index"]["_index"] == "prefix-cat1-envelope-items"


@pytest.mark.asyncio
async def test_index_bulk_omits_routing_when_driver_collection_routing_is_none() -> None:
    """The envelope driver's index is not collection-routed
    (``_collection_routing`` returns ``None``) — the bulk action must omit
    ``routing`` entirely so a drain-issued write/delete lands on the same
    (default, id-hashed) shard ``write_entities`` used (#2687)."""
    client = _FakeAsyncClient()
    driver = _ItemsIndexNameOnlyDriver(client, "prefix-cat1-envelope-items", routing=None)
    indexer = ESBulkIndexer(driver)
    op = _op("delete")

    await indexer.index_bulk([op])

    body = client.calls[0]
    assert body == [{"delete": {
        "_index": "prefix-cat1-envelope-items",
        "_id": op.idempotency_key,
    }}]
    assert "routing" not in body[0]["delete"]


@pytest.mark.asyncio
async def test_index_bulk_keeps_routing_when_driver_collection_routing_returns_collection_id() -> None:
    """A driver whose ``_collection_routing`` returns the collection id
    (the standard/private driver's shape) keeps ``routing`` on the action —
    unchanged behaviour."""
    client = _FakeAsyncClient()
    driver = _ItemsIndexNameOnlyDriver(client, "prefix-cat1-items", routing="col1")
    indexer = ESBulkIndexer(driver)

    await indexer.index_bulk([_op("delete")])

    body = client.calls[0]
    assert body[0]["delete"]["routing"] == "col1"
