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
from dynastore.modules.storage.drivers.elasticsearch import (
    AssetElasticsearchDriver,
)
from dynastore.modules.storage.drivers.elasticsearch_private.collection_driver import (
    CollectionElasticsearchPrivateDriver,
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


# ---------------------------------------------------------------------------
# CollectionElasticsearchPrivateDriver — #559: assert dispatch through the
# *inherited* index/index_bulk/ensure_indexer lands on the per-tenant
# overrides (ensure_storage / upsert_metadata / delete_metadata).
# ---------------------------------------------------------------------------


class _CollectionPrivateDriverStub(CollectionElasticsearchPrivateDriver):
    """Mirror of :class:`_CollectionDriverStub` for the private subclass.

    Asserts the inherited Indexer Protocol methods reach the *private*
    storage hooks rather than the public parent's, i.e. that the
    subclass's per-tenant index resolution is what actually runs.
    """

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


@pytest.mark.asyncio
async def test_collection_private_index_upsert_routes_via_inherited_dispatch():
    d = _CollectionPrivateDriverStub()
    await d.index(_ctx(), _op("upsert", "col-y", {"title": "Priv"}))
    assert d.upserts == [("cat-x", "col-y", {"title": "Priv"})]
    assert d.deletes == []


@pytest.mark.asyncio
async def test_collection_private_index_delete_routes_via_inherited_dispatch():
    d = _CollectionPrivateDriverStub()
    await d.index(_ctx(), _op("delete", "col-y"))
    assert d.deletes == [("cat-x", "col-y")]
    assert d.upserts == []


@pytest.mark.asyncio
async def test_collection_private_index_rejects_unknown_op_type():
    d = _CollectionPrivateDriverStub()
    with pytest.raises(ValueError, match="unsupported op_type"):
        await d.index(_ctx(), _op("noop", "col-y"))


@pytest.mark.asyncio
async def test_collection_private_index_bulk_aggregates_per_op_results():
    d = _CollectionPrivateDriverStub()
    result = await d.index_bulk(_ctx(), [
        _op("upsert", "col-a"),
        _op("delete", "col-b"),
    ])
    assert result.total == 2
    assert result.succeeded == 2
    assert result.failed == 0
    assert d.upserts == [("cat-x", "col-a", {"title": "T"})]
    assert d.deletes == [("cat-x", "col-b")]


@pytest.mark.asyncio
async def test_collection_private_ensure_indexer_delegates_to_ensure_storage():
    d = _CollectionPrivateDriverStub()
    await d.ensure_indexer(_ctx("cat-z"))
    assert d.ensured == ["cat-z"]


# ---------------------------------------------------------------------------
# AssetElasticsearchDriver — #559: ensure_indexer + index/index_bulk surface,
# unknown-op_type behaviour (asset driver currently treats anything that is
# not "delete" as upsert; we pin that behaviour explicitly), and silent-skip
# for non-asset entity_type ops.
# ---------------------------------------------------------------------------


def _asset_op(op_type: str = "upsert", entity_id: str = "a-1",
              payload: Dict[str, Any] | None = None,
              entity_type: str = "asset") -> Any:
    return SimpleNamespace(
        op_type=op_type,
        entity_type=entity_type,
        entity_id=entity_id,
        payload=payload if payload is not None else {"href": "s3://x"},
    )


class _AssetDriverStub(AssetElasticsearchDriver):
    """Capture index_asset / delete_asset / ensure_storage invocations."""

    def __init__(self) -> None:
        self.indexed: List[tuple] = []
        self.deleted: List[tuple] = []
        self.ensured: List[tuple] = []

    async def ensure_storage(  # type: ignore[override]
        self, catalog_id: Any = None, collection_id: Any = None,
    ) -> None:
        self.ensured.append((catalog_id, collection_id))

    async def index_asset(  # type: ignore[override]
        self, catalog_id: str, asset_doc: Dict[str, Any],
        *, db_resource: Any = None,
    ) -> None:
        self.indexed.append((catalog_id, asset_doc))

    async def delete_asset(  # type: ignore[override]
        self, catalog_id: str, asset_id: str,
        *, db_resource: Any = None,
    ) -> None:
        self.deleted.append((catalog_id, asset_id))


@pytest.mark.asyncio
async def test_asset_ensure_indexer_delegates_to_ensure_storage():
    d = _AssetDriverStub()
    await d.ensure_indexer(_ctx("cat-z", "col-y"))
    assert d.ensured == [("cat-z", "col-y")]


@pytest.mark.asyncio
async def test_asset_index_upsert_routes_to_index_asset_with_defaults():
    d = _AssetDriverStub()
    await d.index(_ctx("cat-x", "col-y"), _asset_op("upsert", "a-7", {"href": "s3://k"}))
    assert len(d.indexed) == 1
    catalog_id, doc = d.indexed[0]
    assert catalog_id == "cat-x"
    # asset_id / catalog_id / collection_id are auto-populated when absent
    assert doc["asset_id"] == "a-7"
    assert doc["catalog_id"] == "cat-x"
    assert doc["collection_id"] == "col-y"
    assert doc["href"] == "s3://k"
    assert d.deleted == []


@pytest.mark.asyncio
async def test_asset_index_upsert_without_collection_omits_collection_id():
    d = _AssetDriverStub()
    await d.index(_ctx("cat-x", None), _asset_op("upsert", "a-7", {}))
    _, doc = d.indexed[0]
    assert "collection_id" not in doc
    assert doc["catalog_id"] == "cat-x"
    assert doc["asset_id"] == "a-7"


@pytest.mark.asyncio
async def test_asset_index_delete_routes_to_delete_asset():
    d = _AssetDriverStub()
    await d.index(_ctx("cat-x"), _asset_op("delete", "a-7"))
    assert d.deleted == [("cat-x", "a-7")]
    assert d.indexed == []


@pytest.mark.asyncio
async def test_asset_index_skips_non_asset_entity_type():
    """Asset driver's :meth:`index` is silently a no-op for non-asset ops —
    a different Indexer fields collection-/catalog-tier rows on the same
    bus.  This pins that boundary so future refactors can't quietly
    change it without a test edit."""
    d = _AssetDriverStub()
    await d.index(_ctx("cat-x"), _asset_op("upsert", "x", entity_type="collection"))
    assert d.indexed == [] and d.deleted == []


@pytest.mark.asyncio
async def test_asset_index_bulk_per_op_failure_isolated():
    """One failing op must not poison the rest — failure recorded with the
    asset driver's ``{"id", "reason"}`` failure shape."""
    d = _AssetDriverStub()

    original_index = d.index_asset

    async def boom(catalog_id, asset_doc, **kw):
        if asset_doc.get("asset_id") == "boom":
            raise RuntimeError("ES timeout")
        await original_index(catalog_id, asset_doc, **kw)

    d.index_asset = boom  # type: ignore[assignment]

    result = await d.index_bulk(_ctx("cat-x", "col-y"), [
        _asset_op("upsert", "a-a"),
        _asset_op("upsert", "boom"),
        _asset_op("upsert", "a-c"),
    ])
    assert result.total == 3
    assert result.succeeded == 2
    assert result.failed == 1
    assert result.failures[0]["id"] == "boom"
    assert "ES timeout" in result.failures[0]["reason"]


@pytest.mark.asyncio
async def test_asset_index_bulk_skips_non_asset_ops_in_total_but_not_succeeded():
    """Non-asset ops in a mixed batch are silently skipped — neither
    counted as succeeded nor failed, but ``total`` reflects the input
    length.  This matches the per-op :meth:`index` skip behaviour."""
    d = _AssetDriverStub()
    result = await d.index_bulk(_ctx("cat-x", "col-y"), [
        _asset_op("upsert", "a-a"),
        _asset_op("upsert", "skip", entity_type="collection"),
    ])
    assert result.total == 2
    assert result.succeeded == 1
    assert result.failed == 0
    assert len(d.indexed) == 1
