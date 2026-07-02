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

"""Regression tests for un-fao/GeoID#810 (Option B).

Cover the entity-aware default routing resolver and the dispatcher's
``IndexContext.entity_type`` plumbing:

* The default resolver loads :class:`ItemsRoutingConfig` for items-tier
  ops and :class:`CollectionRoutingConfig` for collection-tier ops
  (and the back-compat ``None`` branch).
* :func:`_call_resolver` tolerates legacy 2-arg resolver stubs so
  pre-existing test fixtures continue to dispatch correctly.
* :class:`IndexDispatcher` routes per-call via ``ctx.entity_type`` —
  pinning a secondary-index driver in
  :class:`ItemsRoutingConfig.operations[WRITE]` and dispatching with
  ``entity_type='item'`` calls the indexer; pinning the same entry in
  :class:`CollectionRoutingConfig` and dispatching with
  ``entity_type='item'`` does NOT (the bug pre-#810).
"""

from __future__ import annotations

from typing import List, Sequence

import pytest

from dynastore.models.protocols.indexer import (
    BulkResult, IndexContext, IndexOp,
)
from dynastore.modules.storage.index_dispatcher import (
    IndexDispatcher,
    _call_resolver,
    _make_default_routing_resolver,
)
from dynastore.modules.storage.routing_config import (
    CollectionRoutingConfig,
    FailurePolicy,
    ItemsRoutingConfig,
    Operation,
    OperationDriverEntry,
    WriteMode,
)


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------


class _StubIndexer:
    def __init__(self, indexer_id: str) -> None:
        self.indexer_id = indexer_id
        self.bulk_calls: List[Sequence[IndexOp]] = []
        self.ensure_calls: List[IndexContext] = []

    async def ensure_indexer(self, ctx: IndexContext) -> None:
        self.ensure_calls.append(ctx)

    async def index_bulk(
        self, ctx: IndexContext, ops: Sequence[IndexOp],
    ) -> BulkResult:
        self.bulk_calls.append(list(ops))
        return BulkResult(total=len(ops), succeeded=len(ops))


def _entry(driver_ref: str) -> OperationDriverEntry:
    return OperationDriverEntry(
        driver_ref=driver_ref,
        on_failure=FailurePolicy.WARN,
        write_mode=WriteMode.SYNC,
        secondary_index=True,
        source="auto",
    )


# ---------------------------------------------------------------------------
# _make_default_routing_resolver — config-class dispatch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_default_resolver_returns_items_routing_for_item_entity_type():
    """entity_type='item' resolves to a default ItemsRoutingConfig
    instance when ConfigsProtocol is unregistered (the no-platform
    branch). This is the #810 fix: pre-Option-B, the items path read
    CollectionRoutingConfig, which is why operator-pinned items-WRITE secondary-index
    entries silently no-opped."""
    resolver = _make_default_routing_resolver()
    cfg = await resolver("cat", "col", entity_type="item")
    assert isinstance(cfg, ItemsRoutingConfig)


@pytest.mark.asyncio
async def test_default_resolver_returns_collection_routing_for_collection_entity_type():
    resolver = _make_default_routing_resolver()
    cfg = await resolver("cat", "col", entity_type="collection")
    assert isinstance(cfg, CollectionRoutingConfig)


@pytest.mark.asyncio
async def test_default_resolver_none_entity_type_back_compat_collection():
    """Pre-#810 callers that don't set entity_type must keep getting
    CollectionRoutingConfig so existing collection / unknown-tier
    dispatch paths preserve behaviour."""
    resolver = _make_default_routing_resolver()
    cfg = await resolver("cat", "col", entity_type=None)
    assert isinstance(cfg, CollectionRoutingConfig)


# ---------------------------------------------------------------------------
# _call_resolver — legacy 2-arg stub tolerance
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_call_resolver_falls_back_to_legacy_two_arg_signature():
    """A test stub that predates the entity_type kwarg must still work —
    the TypeError fallback is what keeps the existing test suite green
    without forcing every fixture to grow a parameter it doesn't read."""
    calls: List[tuple] = []

    async def legacy_resolver(catalog, collection):
        calls.append((catalog, collection))
        return CollectionRoutingConfig()

    result = await _call_resolver(legacy_resolver, "cat", "col", "item")
    assert isinstance(result, CollectionRoutingConfig)
    assert calls == [("cat", "col")]


@pytest.mark.asyncio
async def test_call_resolver_passes_entity_type_to_new_signature():
    calls: List[dict] = []

    async def new_resolver(catalog, collection, *, entity_type=None):
        calls.append({
            "catalog": catalog,
            "collection": collection,
            "entity_type": entity_type,
        })
        return ItemsRoutingConfig()

    result = await _call_resolver(new_resolver, "cat", "col", "item")
    assert isinstance(result, ItemsRoutingConfig)
    assert calls == [{"catalog": "cat", "collection": "col", "entity_type": "item"}]


# ---------------------------------------------------------------------------
# IndexDispatcher — ctx.entity_type routes per-call
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatcher_reads_items_routing_when_entity_type_is_item():
    """The dispatcher MUST pick ItemsRoutingConfig entries for items-tier
    dispatch — the central #810 fix. The resolver receives entity_type
    and returns different configs; the dispatcher acts on whichever it
    got. Asserts the indexer pinned under ItemsRoutingConfig fires when
    ctx.entity_type='item'."""

    items_indexer = _StubIndexer("items_indexer")
    collection_indexer = _StubIndexer("collection_indexer")

    items_routing = ItemsRoutingConfig(
        operations={Operation.WRITE: [_entry("items_indexer")]},
    )
    collection_routing = CollectionRoutingConfig(
        operations={Operation.WRITE: [_entry("collection_indexer")]},
    )

    async def resolver(catalog, collection, *, entity_type=None):
        return items_routing if entity_type == "item" else collection_routing

    async def registry(indexer_id):
        return {
            "items_indexer": items_indexer,
            "collection_indexer": collection_indexer,
        }.get(indexer_id)

    dispatcher = IndexDispatcher(
        routing_resolver=resolver, indexer_registry=registry,
    )

    item_ctx = IndexContext(
        catalog="cat", collection="col",
        correlation_id="cid", entity_type="item",
    )
    item_op = IndexOp(
        op_type="upsert", entity_type="item", entity_id="i-1", payload={"k": "v"},
    )
    await dispatcher.fan_out_bulk(item_ctx, [item_op])

    assert len(items_indexer.bulk_calls) == 1, (
        "items_indexer must fire when entity_type='item' picks "
        "ItemsRoutingConfig.operations[WRITE] secondary index — the #810 contract"
    )
    assert items_indexer.bulk_calls[0][0].entity_id == "i-1"
    assert collection_indexer.bulk_calls == [], (
        "collection_indexer must NOT fire when entity_type='item' — that "
        "was the pre-#810 bug (silent no-op when only ItemsRoutingConfig "
        "had the pin)"
    )


@pytest.mark.asyncio
async def test_dispatcher_reads_collection_routing_when_entity_type_is_collection():
    items_indexer = _StubIndexer("items_indexer")
    collection_indexer = _StubIndexer("collection_indexer")

    items_routing = ItemsRoutingConfig(
        operations={Operation.WRITE: [_entry("items_indexer")]},
    )
    collection_routing = CollectionRoutingConfig(
        operations={Operation.WRITE: [_entry("collection_indexer")]},
    )

    async def resolver(catalog, collection, *, entity_type=None):
        return items_routing if entity_type == "item" else collection_routing

    async def registry(indexer_id):
        return {
            "items_indexer": items_indexer,
            "collection_indexer": collection_indexer,
        }.get(indexer_id)

    dispatcher = IndexDispatcher(
        routing_resolver=resolver, indexer_registry=registry,
    )

    coll_ctx = IndexContext(
        catalog="cat", collection="col",
        correlation_id="cid", entity_type="collection",
    )
    coll_op = IndexOp(
        op_type="upsert", entity_type="collection", entity_id="col",
        payload={"k": "v"},
    )
    await dispatcher.fan_out_bulk(coll_ctx, [coll_op])

    assert len(collection_indexer.bulk_calls) == 1
    assert items_indexer.bulk_calls == []


@pytest.mark.asyncio
async def test_dispatcher_back_compat_none_entity_type_reads_collection_routing():
    """An IndexContext built without entity_type (pre-#810 caller) must
    keep dispatching against CollectionRoutingConfig so old callers stay
    on the same path until they are migrated."""
    items_indexer = _StubIndexer("items_indexer")
    collection_indexer = _StubIndexer("collection_indexer")

    items_routing = ItemsRoutingConfig(
        operations={Operation.WRITE: [_entry("items_indexer")]},
    )
    collection_routing = CollectionRoutingConfig(
        operations={Operation.WRITE: [_entry("collection_indexer")]},
    )

    async def resolver(catalog, collection, *, entity_type=None):
        return items_routing if entity_type == "item" else collection_routing

    async def registry(indexer_id):
        return {
            "items_indexer": items_indexer,
            "collection_indexer": collection_indexer,
        }.get(indexer_id)

    dispatcher = IndexDispatcher(
        routing_resolver=resolver, indexer_registry=registry,
    )

    # entity_type defaults to None — the back-compat branch.
    ctx = IndexContext(catalog="cat", collection="col", correlation_id="cid")
    op = IndexOp(
        op_type="upsert", entity_type="item", entity_id="i-1", payload={"k": "v"},
    )
    await dispatcher.fan_out_bulk(ctx, [op])

    assert len(collection_indexer.bulk_calls) == 1
    assert items_indexer.bulk_calls == []


@pytest.mark.asyncio
async def test_dispatcher_works_with_legacy_two_arg_resolver_stub():
    """Existing tests in test_index_dispatcher.py define
    ``routing_resolver(catalog, collection)`` without the entity_type
    kwarg. _call_resolver's TypeError fallback must keep them green."""
    indexer = _StubIndexer("a")

    routing = CollectionRoutingConfig(
        operations={Operation.WRITE: [_entry("a")]},
    )

    async def legacy_resolver(catalog, collection):
        return routing

    async def registry(indexer_id):
        return indexer if indexer_id == "a" else None

    dispatcher = IndexDispatcher(
        routing_resolver=legacy_resolver, indexer_registry=registry,
    )

    ctx = IndexContext(
        catalog="cat", collection="col",
        correlation_id="cid", entity_type="item",
    )
    op = IndexOp(
        op_type="upsert", entity_type="item", entity_id="i-1", payload={"k": "v"},
    )
    await dispatcher.fan_out_bulk(ctx, [op])

    assert len(indexer.bulk_calls) == 1


# ---------------------------------------------------------------------------
# Items call sites stamp entity_type='item' on IndexContext
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatch_index_upsert_sets_entity_type_item_on_ctx():
    """item_service._do_dispatch (called by _dispatch_index_upsert) must
    set ctx.entity_type='item' so the dispatcher routes through
    ItemsRoutingConfig instead of the legacy CollectionRoutingConfig
    branch. Without this, B1's resolver change is dormant."""
    from dynastore.modules.catalog.item_service import ItemService

    captured_ctxs: List[IndexContext] = []

    class _CapturingDispatcher:
        async def fan_out_bulk(self, ctx, ops, *, tx_factory=None):
            captured_ctxs.append(ctx)
            return {}

    svc = ItemService.__new__(ItemService)  # bypass __init__
    op = IndexOp(
        op_type="upsert", entity_type="item", entity_id="i-1", payload={"x": 1},
    )
    await svc._do_dispatch(
        _CapturingDispatcher(),
        catalog_id="cat",
        collection_id="col",
        ops=[op],
        pg_conn=None,
    )

    assert len(captured_ctxs) == 1
    assert captured_ctxs[0].entity_type == "item"
    assert captured_ctxs[0].catalog == "cat"
    assert captured_ctxs[0].collection == "col"
