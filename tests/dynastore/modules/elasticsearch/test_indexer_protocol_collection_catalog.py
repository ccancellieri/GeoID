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

"""Unit tests for the Indexer Protocol implementations on
:class:`CollectionElasticsearchDriver` and
:class:`CatalogElasticsearchDriver` — closes the gap that caused #491.

These drivers opt into ``Operation.INDEX`` materialization
auto-registration via ``index_tiers`` (``"collection"`` / ``"catalog"``)
but, prior to #503, did not implement the dispatcher-facing ``index`` /
``index_bulk`` / ``ensure_indexer`` surface.  The dispatcher then raised
an AttributeError at dispatch time.
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
        lifecycle_status: Any = None,
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


# ---------------------------------------------------------------------------
# Tier guards (#728) — drivers refuse off-tier ops rather than poison the
# singleton ``dynastore-collections`` / ``dynastore-catalogs`` indexes.
# ---------------------------------------------------------------------------


def _item_op(payload: Dict[str, Any] | None = None) -> Any:
    return SimpleNamespace(
        op_type="upsert",
        entity_type="item",
        entity_id="item-1",
        payload=payload if payload is not None else {"type": "Feature", "id": "item-1"},
    )


@pytest.mark.asyncio
async def test_collection_index_refuses_item_entity_type():
    """An ItemsRoutingConfig misconfigured to point at the collection driver
    leaks STAC items into ``dynastore-collections`` (#728). The guard turns
    the silent leak into a loud refusal."""
    d = _CollectionDriverStub()
    with pytest.raises(ValueError, match="refused op with entity_type='item'"):
        await d.index(_ctx(), _item_op())
    # No upsert happened — guard fired before upsert_metadata.
    assert d.upserts == []


@pytest.mark.asyncio
async def test_collection_index_refuses_feature_payload_legacy_ctx():
    """Legacy pre-#810 callers may leave ``op.entity_type=None``. A
    ``type='Feature'`` payload is unambiguously an item and must still be
    refused (defence-in-depth)."""
    d = _CollectionDriverStub()
    legacy_op = SimpleNamespace(
        op_type="upsert",
        entity_type=None,
        entity_id="leaked-item",
        payload={"type": "Feature", "id": "leaked-item"},
    )
    with pytest.raises(ValueError, match="type='Feature'"):
        await d.index(_ctx(), legacy_op)
    assert d.upserts == []


@pytest.mark.asyncio
async def test_collection_index_accepts_collection_entity_type():
    """Regression pin: the guard must NOT reject the happy path."""
    d = _CollectionDriverStub()
    await d.index(_ctx(), _op("upsert", "col-y", {"title": "Hello"}))
    assert d.upserts == [("cat-x", "col-y", {"title": "Hello"})]


@pytest.mark.asyncio
async def test_catalog_index_refuses_collection_entity_type():
    d = _CatalogDriverStub()
    coll_op = SimpleNamespace(
        op_type="upsert",
        entity_type="collection",
        entity_id="cat-1",
        payload={"title": "T"},
    )
    with pytest.raises(ValueError, match="refused op with entity_type='collection'"):
        await d.index(_ctx(), coll_op)
    assert d.upserts == []


@pytest.mark.asyncio
async def test_catalog_index_refuses_feature_payload_legacy_ctx():
    d = _CatalogDriverStub()
    legacy_op = SimpleNamespace(
        op_type="upsert",
        entity_type=None,
        entity_id="leaked-item",
        payload={"type": "Feature", "id": "leaked-item"},
    )
    with pytest.raises(ValueError, match="type='Feature'"):
        await d.index(_ctx(), legacy_op)
    assert d.upserts == []


@pytest.mark.asyncio
async def test_catalog_index_accepts_catalog_entity_type():
    d = _CatalogDriverStub()
    cat_op = SimpleNamespace(
        op_type="upsert",
        entity_type="catalog",
        entity_id="cat-1",
        payload={"title": "Catalog 1"},
    )
    await d.index(_ctx(), cat_op)
    assert d.upserts == [("cat-1", {"title": "Catalog 1"})]


def _catalog_op(op_type: str = "upsert", entity_id: str = "cat-1",
                payload: Dict[str, Any] | None = None) -> Any:
    """Catalog-tier op fixture — separate from :func:`_op` which stamps
    ``entity_type='collection'``. The driver-side tier guards (#728) now
    enforce that ``entity_type`` matches the driver's tier."""
    return SimpleNamespace(
        op_type=op_type,
        entity_type="catalog",
        entity_id=entity_id,
        payload=payload if payload is not None else {"title": "T"},
    )


@pytest.mark.asyncio
async def test_catalog_index_upsert_routes_to_upsert_catalog_metadata():
    d = _CatalogDriverStub()
    op = _catalog_op("upsert", "cat-x", {"title": "C"})
    await d.index(_ctx(), op)
    assert d.upserts == [("cat-x", {"title": "C"})]
    assert d.deletes == []


@pytest.mark.asyncio
async def test_catalog_index_delete_routes_to_delete_catalog_metadata():
    d = _CatalogDriverStub()
    await d.index(_ctx(), _catalog_op("delete", "cat-x"))
    assert d.deletes == ["cat-x"]
    assert d.upserts == []


@pytest.mark.asyncio
async def test_catalog_index_bulk_aggregates_per_op_results():
    d = _CatalogDriverStub()
    result = await d.index_bulk(_ctx(), [
        _catalog_op("upsert", "cat-a"),
        _catalog_op("upsert", "cat-b"),
    ])
    assert result.total == 2
    assert result.succeeded == 2
    assert result.failed == 0
    assert {u[0] for u in d.upserts} == {"cat-a", "cat-b"}



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


class _StubAssetIndices:
    """Minimal ``es.indices`` stub — index already exists, nothing to create."""

    async def exists(self, *, index, **kwargs):
        return True

    async def create(self, *, index, body=None, **kwargs):
        pass


class _StubAssetBulkEs:
    """ES client stub for :meth:`AssetElasticsearchDriver.index_bulk`
    (#2863) — the real code path now builds one ``_bulk`` request instead
    of delegating per-op to ``index_asset``/``delete_asset``, so exercising
    it needs an ES double rather than driver-method overrides."""

    def __init__(self, bulk_response: Dict[str, Any] | None = None) -> None:
        self.indices = _StubAssetIndices()
        self.bulk_calls: List[Dict[str, Any]] = []
        self.bulk_response = bulk_response if bulk_response is not None else {"items": []}

    async def bulk(self, *, body, **kwargs):
        self.bulk_calls.append({"body": body})
        return self.bulk_response


class _AssetDriverStub(AssetElasticsearchDriver):
    """Capture index_asset / delete_asset / ensure_storage invocations for
    the single-op ``index()`` path; stubs the ES client for ``index_bulk``,
    which goes straight to a real ``_bulk`` call (#2863)."""

    def __init__(self, bulk_response: Dict[str, Any] | None = None) -> None:
        self.indexed: List[tuple] = []
        self.deleted: List[tuple] = []
        self.ensured: List[tuple] = []
        self.es = _StubAssetBulkEs(bulk_response=bulk_response)

    def _get_client(self):  # type: ignore[override]
        return self.es

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
    asset driver's ``{"id", "reason"}`` failure shape.

    Refs #2863: ``index_bulk`` now builds a single ``_bulk`` request
    instead of delegating per-op to ``index_asset``, so per-op isolation
    is driven by the ES bulk response's per-item ``error``, not a raised
    exception from an overridden driver method."""
    d = _AssetDriverStub(bulk_response={
        "errors": True,
        "items": [
            {"index": {"_id": "a-a", "status": 201}},
            {"index": {
                "_id": "boom", "status": 400,
                "error": {"type": "mapper_parsing_exception", "reason": "ES timeout"},
            }},
            {"index": {"_id": "a-c", "status": 201}},
        ],
    })

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
    # One _bulk call for the whole batch — not one PUT per op.
    assert len(d.es.bulk_calls) == 1


@pytest.mark.asyncio
async def test_asset_index_bulk_skips_non_asset_ops_in_total_but_not_succeeded():
    """Non-asset ops in a mixed batch are silently skipped — neither
    counted as succeeded nor failed, but ``total`` reflects the input
    length.  This matches the per-op :meth:`index` skip behaviour."""
    d = _AssetDriverStub(bulk_response={
        "errors": False,
        "items": [{"index": {"_id": "a-a", "status": 201}}],
    })
    result = await d.index_bulk(_ctx("cat-x", "col-y"), [
        _asset_op("upsert", "a-a"),
        _asset_op("upsert", "skip", entity_type="collection"),
    ])
    assert result.total == 2
    assert result.succeeded == 1
    assert result.failed == 0
    # Only the asset op was submitted to _bulk — action + doc for "a-a".
    assert len(d.es.bulk_calls[0]["body"]) == 2
