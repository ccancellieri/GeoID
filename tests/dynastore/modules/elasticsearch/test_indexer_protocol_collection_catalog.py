"""Unit tests for the Indexer Protocol implementations on
:class:`CollectionElasticsearchDriver` and
:class:`CatalogElasticsearchDriver` — closes the gap that caused #491.

These drivers opt into ``Operation.INDEX`` auto-registration via the
marker Protocols (``is_collection_indexer`` / ``is_catalog_indexer``)
but, prior to #503, did not implement the dispatcher-facing
``index`` / ``index_bulk`` / ``ensure_indexer`` surface.  The
dispatcher then raised an AttributeError at dispatch time.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List

import pytest

from dynastore.modules.elasticsearch.catalog_es_driver import (
    CatalogElasticsearchDriver,
)
from dynastore.modules.elasticsearch.collection_es_driver import (
    CollectionElasticsearchDriver,
)


def _ctx(catalog: str = "cat-x", collection: str | None = "col-y") -> Any:
    return SimpleNamespace(
        catalog=catalog, collection=collection, correlation_id="cid-1",
        pg_conn=None,
    )


def _op(op_type: str = "upsert", entity_id: str = "e-1",
        payload: Dict[str, Any] | None = None) -> Any:
    return SimpleNamespace(
        op_type=op_type,
        entity_type="collection",
        entity_id=entity_id,
        payload=payload if payload is not None else {"title": "T"},
    )


class _CollectionDriverStub(CollectionElasticsearchDriver):
    """Capture upsert/delete/ensure invocations instead of hitting ES."""

    def __init__(self) -> None:
        self.upserts: List[tuple] = []
        self.deletes: List[tuple] = []
        self.ensured: List[str] = []

    async def ensure_storage(self, catalog_id: str) -> None:  # type: ignore[override]
        self.ensured.append(catalog_id)

    async def upsert_metadata(  # type: ignore[override]
        self, catalog_id: str, collection_id: str,
        metadata: Dict[str, Any], *, db_resource: Any = None,
    ) -> None:
        self.upserts.append((catalog_id, collection_id, metadata))

    async def delete_metadata(  # type: ignore[override]
        self, catalog_id: str, collection_id: str, *,
        soft: bool = False, db_resource: Any = None,
    ) -> None:
        self.deletes.append((catalog_id, collection_id))


class _CatalogDriverStub(CatalogElasticsearchDriver):
    def __init__(self) -> None:
        self.upserts: List[tuple] = []
        self.deletes: List[str] = []
        self.ensured: List[Any] = []

    async def ensure_storage(self, catalog_id: Any = None) -> None:  # type: ignore[override]
        self.ensured.append(catalog_id)

    async def upsert_catalog_metadata(  # type: ignore[override]
        self, catalog_id: str, metadata: Dict[str, Any],
        *, db_resource: Any = None,
    ) -> None:
        self.upserts.append((catalog_id, metadata))

    async def delete_catalog_metadata(  # type: ignore[override]
        self, catalog_id: str, *, soft: bool = False, db_resource: Any = None,
    ) -> None:
        self.deletes.append(catalog_id)


@pytest.mark.asyncio
async def test_collection_index_upsert_routes_to_upsert_metadata():
    d = _CollectionDriverStub()
    await d.index(_ctx(), _op("upsert", "col-y", {"title": "Hello"}))
    assert d.upserts == [("cat-x", "col-y", {"title": "Hello"})]
    assert d.deletes == []


@pytest.mark.asyncio
async def test_collection_index_delete_routes_to_delete_metadata():
    d = _CollectionDriverStub()
    await d.index(_ctx(), _op("delete", "col-y"))
    assert d.deletes == [("cat-x", "col-y")]
    assert d.upserts == []


@pytest.mark.asyncio
async def test_collection_index_rejects_unknown_op_type():
    d = _CollectionDriverStub()
    with pytest.raises(ValueError, match="unsupported op_type"):
        await d.index(_ctx(), _op("noop", "col-y"))


@pytest.mark.asyncio
async def test_collection_index_bulk_per_op_failure_reported():
    """A failing op in the middle of a batch must not abort the batch;
    failure is reported in ``BulkResult.failures`` while successes
    continue."""
    d = _CollectionDriverStub()

    async def boom(catalog_id, collection_id, metadata, **kw):
        if collection_id == "boom":
            raise RuntimeError("ES timeout")
        d.upserts.append((catalog_id, collection_id, metadata))

    d.upsert_metadata = boom  # type: ignore[assignment]

    result = await d.index_bulk(_ctx(), [
        _op("upsert", "col-a"),
        _op("upsert", "boom"),
        _op("upsert", "col-c"),
    ])
    assert result.total == 3
    assert result.succeeded == 2
    assert result.failed == 1
    assert result.failures[0]["entity_id"] == "boom"
    assert "ES timeout" in result.failures[0]["error"]


@pytest.mark.asyncio
async def test_collection_ensure_indexer_delegates_to_ensure_storage():
    d = _CollectionDriverStub()
    await d.ensure_indexer(_ctx("cat-z"))
    assert d.ensured == ["cat-z"]


@pytest.mark.asyncio
async def test_catalog_index_upsert_routes_to_upsert_catalog_metadata():
    d = _CatalogDriverStub()
    op = _op("upsert", "cat-x", {"title": "C"})
    await d.index(_ctx(), op)
    assert d.upserts == [("cat-x", {"title": "C"})]
    assert d.deletes == []


@pytest.mark.asyncio
async def test_catalog_index_delete_routes_to_delete_catalog_metadata():
    d = _CatalogDriverStub()
    await d.index(_ctx(), _op("delete", "cat-x"))
    assert d.deletes == ["cat-x"]
    assert d.upserts == []


@pytest.mark.asyncio
async def test_catalog_index_bulk_aggregates_per_op_results():
    d = _CatalogDriverStub()
    result = await d.index_bulk(_ctx(), [
        _op("upsert", "cat-a"),
        _op("upsert", "cat-b"),
    ])
    assert result.total == 2
    assert result.succeeded == 2
    assert result.failed == 0
    assert {u[0] for u in d.upserts} == {"cat-a", "cat-b"}
