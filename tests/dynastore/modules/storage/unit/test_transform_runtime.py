"""transform_runtime — chain composition semantics.

apply_transform_chain runs left-to-right; restore_transform_chain runs
right-to-left so the output shape matches the original entity. Empty chain
is identity. Exceptions propagate after a warning log.

Also exercises the per-(operation, driver) attachment introduced by #501:
input_transformers on WRITE secondary-index entries, output_transformers on
SEARCH entries, validator rejection of dangling refs, deferred-hop WARN logs,
and per-item failure isolation inside IndexDispatcher.fan_out_bulk.
"""

from __future__ import annotations

import logging
from typing import Any, List
from unittest.mock import patch

import pytest

from dynastore.modules.storage.transform_runtime import (
    apply_transform_chain,
    restore_transform_chain,
)


class _AppendTransformer:
    """Test transformer that appends `tag` on apply, strips on restore."""

    def __init__(self, transform_id: str, tag: str):
        self.transform_id = transform_id
        self.tag = tag

    async def transform_for_index(self, entity, *, catalog_id, collection_id, entity_kind):
        return f"{entity}|{self.tag}"

    async def restore_from_index(self, doc, *, catalog_id, collection_id, entity_kind):
        suffix = f"|{self.tag}"
        assert doc.endswith(suffix), f"restore expected suffix {suffix}, got {doc!r}"
        return doc[: -len(suffix)]


class _RaisingTransformer:
    transform_id = "boom"

    async def transform_for_index(self, entity, **_):
        raise RuntimeError("apply boom")

    async def restore_from_index(self, doc, **_):
        raise RuntimeError("restore boom")


@pytest.mark.asyncio
async def test_apply_chain_runs_left_to_right():
    chain = [_AppendTransformer("a", "A"), _AppendTransformer("b", "B")]
    result = await apply_transform_chain(
        "raw", chain, catalog_id="c", collection_id=None, entity_kind="item",
    )
    assert result == "raw|A|B"


@pytest.mark.asyncio
async def test_restore_chain_runs_right_to_left():
    chain = [_AppendTransformer("a", "A"), _AppendTransformer("b", "B")]
    # Restore B first (matches its tag suffix), then A
    result = await restore_transform_chain(
        "raw|A|B", chain, catalog_id="c", collection_id=None, entity_kind="item",
    )
    assert result == "raw"


@pytest.mark.asyncio
async def test_apply_empty_chain_is_identity():
    result = await apply_transform_chain(
        {"x": 1}, [], catalog_id="c", collection_id=None, entity_kind="item",
    )
    assert result == {"x": 1}


@pytest.mark.asyncio
async def test_restore_empty_chain_is_identity():
    result = await restore_transform_chain(
        {"x": 1}, [], catalog_id="c", collection_id=None, entity_kind="item",
    )
    assert result == {"x": 1}


@pytest.mark.asyncio
async def test_apply_propagates_exceptions():
    chain: List[Any] = [_RaisingTransformer()]
    with pytest.raises(RuntimeError, match="apply boom"):
        await apply_transform_chain(
            "raw", chain, catalog_id="c", collection_id=None, entity_kind="item",
        )


@pytest.mark.asyncio
async def test_restore_propagates_exceptions():
    chain: List[Any] = [_RaisingTransformer()]
    with pytest.raises(RuntimeError, match="restore boom"):
        await restore_transform_chain(
            "doc", chain, catalog_id="c", collection_id=None, entity_kind="item",
        )


@pytest.mark.asyncio
async def test_apply_then_restore_round_trips():
    """Composition guarantee: restore inverts apply for the same chain."""
    chain = [_AppendTransformer("a", "A"), _AppendTransformer("b", "B")]
    transformed = await apply_transform_chain(
        "raw", chain, catalog_id="c", collection_id=None, entity_kind="item",
    )
    restored = await restore_transform_chain(
        transformed, chain, catalog_id="c", collection_id=None, entity_kind="item",
    )
    assert restored == "raw"


# ---------------------------------------------------------------------------
# #501 — per-(operation, driver) transformer attachment
# ---------------------------------------------------------------------------


class PayloadAppendTransformer:  # noqa: D401 — test fixture
    """Variant of ``_AppendTransformer`` that works on dict payloads.

    Mutates ``payload["tag"]`` so the dispatcher secondary-index hop tests
    can verify the per-item payload reshape end-to-end.
    """

    def __init__(self, tag: str) -> None:
        self.tag = tag

    async def transform_for_index(self, entity, **_):
        out = dict(entity)
        existing = out.get("tag", "")
        out["tag"] = f"{existing}|{self.tag}" if existing else self.tag
        return out

    async def restore_from_index(self, doc, **_):
        out = dict(doc)
        current = out.get("tag", "")
        suffix = f"|{self.tag}"
        if current.endswith(suffix):
            out["tag"] = current[: -len(suffix)]
        elif current == self.tag:
            out.pop("tag")
        return out


class _Append:
    """Distinct class for the dangling-ref / chain composition tests."""

    async def transform_for_index(self, entity, **_): return entity
    async def restore_from_index(self, doc, **_): return doc


@pytest.mark.asyncio
async def test_input_on_index_only_runs_at_index_hop_not_at_search():
    """When only the WRITE secondary-index entry declares
    input_transformers, the indexed doc is transformed; the SEARCH-side
    restore does not fire (no output_transformers declared)."""
    from dynastore.modules.storage.routing_config import (
        Operation,
        OperationDriverEntry,
        get_output_transformers_for_search,
    )

    index_entry = OperationDriverEntry(
        driver_ref="items_indexer",
        input_transformers=("payload_append_transformer",),
        secondary_index=True,
    )
    search_entry = OperationDriverEntry(driver_ref="items_indexer")
    ops = {
        Operation.WRITE: [index_entry],
        Operation.SEARCH: [search_entry],
    }
    assert index_entry.input_transformers == ("payload_append_transformer",)
    assert search_entry.output_transformers == ()

    appender = PayloadAppendTransformer("X")
    # SEARCH entry has no output_transformers → empty chain at SEARCH hop.
    with patch(
        "dynastore.modules.storage.routing_config._resolve_entity_operations",
        return_value=ops,
    ), patch(
        "dynastore.tools.discovery.get_protocols",
        return_value=[appender],
    ):
        chain = await get_output_transformers_for_search(
            "cat", entity="item", collection_id="col", driver_ref="items_indexer",
        )
    assert chain == []


@pytest.mark.asyncio
async def test_output_on_search_only_resolves_inverse_chain():
    """SEARCH entry's output_transformers resolves to an instance chain
    even when the INDEX entry carries no input_transformers."""
    from dynastore.modules.storage.routing_config import (
        Operation,
        OperationDriverEntry,
        get_output_transformers_for_search,
    )

    ops = {
        Operation.SEARCH: [
            OperationDriverEntry(
                driver_ref="items_indexer",
                output_transformers=("payload_append_transformer",),
            ),
        ],
    }
    appender = PayloadAppendTransformer("X")
    with patch(
        "dynastore.modules.storage.routing_config._resolve_entity_operations",
        return_value=ops,
    ), patch(
        "dynastore.tools.discovery.get_protocols",
        return_value=[appender],
    ):
        chain = await get_output_transformers_for_search(
            "cat", entity="item", collection_id="col", driver_ref="items_indexer",
        )
    assert chain == [appender]
    # Inverse strips the tag back out — what the SEARCH wrap would yield.
    restored = await restore_transform_chain(
        {"tag": "X"}, chain, catalog_id="c", collection_id="col", entity_kind="item",
    )
    assert restored == {}


@pytest.mark.asyncio
async def test_input_and_output_symmetric_round_trip():
    """The PrivateEntityTransformer pattern: same transformer attached to
    INDEX (input) and SEARCH (output). Round-trip restores the original."""
    appender = PayloadAppendTransformer("priv")
    raw = {"id": "1"}
    indexed = await apply_transform_chain(
        raw, [appender], catalog_id="c", collection_id="col", entity_kind="item",
    )
    assert indexed == {"id": "1", "tag": "priv"}
    restored = await restore_transform_chain(
        indexed, [appender], catalog_id="c", collection_id="col", entity_kind="item",
    )
    assert restored == raw


@pytest.mark.asyncio
async def test_chain_composition_two_transformers_left_to_right_then_right_to_left():
    a = PayloadAppendTransformer("A")
    b = PayloadAppendTransformer("B")
    indexed = await apply_transform_chain(
        {}, [a, b], catalog_id="c", collection_id=None, entity_kind="item",
    )
    assert indexed == {"tag": "A|B"}
    restored = await restore_transform_chain(
        indexed, [a, b], catalog_id="c", collection_id=None, entity_kind="item",
    )
    assert restored == {}


def test_validator_rejects_dangling_input_transformer_ref():
    """A driver_ref listed in input_transformers that is not present in
    the ``transformers`` registry makes the routing config raise ValueError
    at build time."""
    from dynastore.modules.storage.routing_config import (
        AssetRoutingConfig,
        Operation,
        OperationDriverEntry,
    )

    with pytest.raises(ValueError, match="dangling_ref"):
        AssetRoutingConfig(
            operations={
                Operation.WRITE: [
                    OperationDriverEntry(
                        driver_ref="asset_indexer",
                        input_transformers=("dangling_ref",),
                        secondary_index=True,
                    ),
                ],
            },
        )


def test_deferred_hop_warns_once_for_read_input_transformers(caplog):
    """Declaring input_transformers on a non-wired hop (READ) is a no-op
    until that hop is wired in a future PR. The config-load path emits
    exactly one WARN per (operation, driver, side) tuple.

    WRITE is the wired input hop (secondary-index propagation), so it does
    NOT warn — READ stands in here for the still-deferred case."""
    from dynastore.modules.storage.routing_config import (
        AssetRoutingConfig,
        Operation,
        OperationDriverEntry,
        TransformerEntry,
        _DEFERRED_HOP_WARNED,
    )

    _DEFERRED_HOP_WARNED.clear()
    with caplog.at_level(
        logging.WARNING, logger="dynastore.modules.storage.routing_config",
    ):
        AssetRoutingConfig(
            operations={
                Operation.READ: [
                    OperationDriverEntry(
                        driver_ref="asset_indexer",
                        input_transformers=("noop_attached_transformer",),
                    ),
                ],
            },
            transformers=[
                TransformerEntry(driver_ref="noop_attached_transformer"),
            ],
        )
    matches = [
        rec for rec in caplog.records
        if "input_transformers declared on operation 'READ'" in rec.message
    ]
    assert len(matches) == 1, (
        "Expected exactly one deferred-hop WARN; got %d" % len(matches)
    )

    caplog.clear()
    with caplog.at_level(
        logging.WARNING, logger="dynastore.modules.storage.routing_config",
    ):
        AssetRoutingConfig(
            operations={
                Operation.READ: [
                    OperationDriverEntry(
                        driver_ref="asset_indexer",
                        input_transformers=("noop_attached_transformer",),
                    ),
                ],
            },
            transformers=[
                TransformerEntry(driver_ref="noop_attached_transformer"),
            ],
        )
    matches = [
        rec for rec in caplog.records
        if "input_transformers declared on operation 'READ'" in rec.message
    ]
    assert matches == [], "Second load must not re-emit; dedupe failed"


def test_search_output_transformers_silent_on_collection_tier(caplog):
    """geoid#1574: CollectionElasticsearchDriver.get_metadata and search_metadata
    now invoke the restore chain, so ``_wired_output_search_hop=True`` on
    CollectionRoutingConfig. A SEARCH ``output_transformers`` declaration there
    must NOT emit a deferred-hop WARN — it is fully wired."""
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig,
        Operation,
        OperationDriverEntry,
        TransformerEntry,
        _DEFERRED_HOP_WARNED,
    )

    _DEFERRED_HOP_WARNED.clear()
    with caplog.at_level(
        logging.WARNING, logger="dynastore.modules.storage.routing_config",
    ):
        CollectionRoutingConfig(
            operations={
                Operation.SEARCH: [
                    OperationDriverEntry(
                        driver_ref="es_collection_searcher",
                        output_transformers=("noop_attached_transformer",),
                    ),
                ],
            },
            transformers=[
                TransformerEntry(driver_ref="noop_attached_transformer"),
            ],
        )
    matches = [
        rec for rec in caplog.records
        if "output_transformers declared on operation 'SEARCH'" in rec.message
    ]
    assert matches == [], (
        "Collection tier SEARCH output_transformers is wired (geoid#1574) — "
        "must not warn; got %d WARN(s)" % len(matches)
    )


def test_search_output_transformers_silent_on_asset_tier(caplog):
    """geoid#1567: the asset tier's SEARCH path DOES invoke restore_from_index
    (``_wired_output_search_hop=True``), so a SEARCH ``output_transformers``
    declaration there is wired — it must NOT emit a deferred-hop WARN."""
    from dynastore.modules.storage.routing_config import (
        AssetRoutingConfig,
        Operation,
        OperationDriverEntry,
        TransformerEntry,
        _DEFERRED_HOP_WARNED,
    )

    _DEFERRED_HOP_WARNED.clear()
    with caplog.at_level(
        logging.WARNING, logger="dynastore.modules.storage.routing_config",
    ):
        AssetRoutingConfig(
            operations={
                Operation.SEARCH: [
                    OperationDriverEntry(
                        driver_ref="asset_es_searcher",
                        output_transformers=("noop_attached_transformer",),
                    ),
                ],
            },
            transformers=[
                TransformerEntry(driver_ref="noop_attached_transformer"),
            ],
        )
    matches = [
        rec for rec in caplog.records
        if "output_transformers declared on operation 'SEARCH'" in rec.message
    ]
    assert matches == [], (
        "Asset tier SEARCH output_transformers is wired — must not warn; "
        "got %d WARN(s)" % len(matches)
    )


@pytest.mark.asyncio
async def test_per_item_failure_isolation_with_transformer_at_index_hop():
    """When the input_transformers chain raises for one item in a bulk,
    that item is rejected; the rest reach the indexer and succeed."""
    from typing import Sequence as _Sequence

    from dynastore.models.protocols.indexer import (
        BulkResult, IndexContext, IndexOp,
    )
    from dynastore.modules.storage.index_dispatcher import IndexDispatcher
    from dynastore.modules.storage.routing_config import (
        Operation, OperationDriverEntry,
    )

    class SelectiveRaise:
        async def transform_for_index(self, entity, **_):
            if entity.get("id") == "bad":
                raise RuntimeError("boom on bad")
            out = dict(entity)
            out["seen"] = True
            return out

        async def restore_from_index(self, doc, **_):
            return doc

    class _Indexer:
        def __init__(self) -> None:
            self.received: List[IndexOp] = []

        async def ensure_indexer(self, ctx: IndexContext) -> None:
            return None

        async def index(self, ctx, op) -> None:
            self.received.append(op)

        async def index_bulk(self, ctx, ops: _Sequence[IndexOp]) -> BulkResult:
            self.received.extend(ops)
            return BulkResult(total=len(ops), succeeded=len(ops))

    entry = OperationDriverEntry(
        driver_ref="stub_indexer",
        input_transformers=("selective_raise",),
        secondary_index=True,
    )

    class _Routing:
        operations = {
            Operation.WRITE: [entry],
        }

    indexer = _Indexer()
    transformer = SelectiveRaise()

    async def routing_resolver(catalog, collection):
        return _Routing()

    async def indexer_registry(driver_id):
        if driver_id == "stub_indexer":
            return indexer
        return None

    dispatcher = IndexDispatcher(
        routing_resolver=routing_resolver,
        indexer_registry=indexer_registry,
    )

    ctx = IndexContext(catalog="cat", collection="col", correlation_id="cid")
    ops = [
        IndexOp(op_type="upsert", entity_type="item", entity_id="ok-1", payload={"id": "ok-1"}),
        IndexOp(op_type="upsert", entity_type="item", entity_id="bad", payload={"id": "bad"}),
        IndexOp(op_type="upsert", entity_type="item", entity_id="ok-2", payload={"id": "ok-2"}),
    ]

    with patch(
        "dynastore.tools.discovery.get_protocols",
        return_value=[transformer],
    ):
        results = await dispatcher.fan_out_bulk(ctx, ops)

    result = results["stub_indexer"]
    assert result.total == 3
    assert result.failed == 1
    assert any(
        "input_transformer_failed" in (f.get("reason") or "")
        and f.get("entity_id") == "bad"
        for f in result.failures
    )
    received_ids = [o.entity_id for o in indexer.received]
    assert received_ids == ["ok-1", "ok-2"]
    for op in indexer.received:
        assert op.payload is not None and op.payload.get("seen") is True


# ---------------------------------------------------------------------------
# geoid#1574 — Wire read-side restore into collection/catalog/items ES drivers
# ---------------------------------------------------------------------------


class _PrefixTransformer:
    """Test transformer: adds/removes a sentinel key on apply/restore.

    Operates on dict entities: ``apply`` adds ``{"_t": tag}``; ``restore``
    removes it. Used to verify the restore hook fires (key gone from result)
    vs. does not fire (key still present).
    """

    def __init__(self, tag: str) -> None:
        self.tag = tag

    async def transform_for_index(
        self, entity, *, catalog_id, collection_id, entity_kind,
    ):
        out = dict(entity)
        out["_t"] = self.tag
        return out

    async def restore_from_index(
        self, doc, *, catalog_id, collection_id, entity_kind,
    ):
        out = dict(doc)
        out.pop("_t", None)
        return out


# ---  ItemsElasticsearchDriver.read_entities (entity="item") ---------------


@pytest.mark.asyncio
async def test_items_es_driver_read_entities_no_op_when_chain_empty():
    """Empty output_transformer chain leaves each yielded item byte-identical."""
    from unittest.mock import AsyncMock, MagicMock, patch

    raw_hit = {"id": "item-1", "type": "Feature", "geometry": None, "properties": {}}

    fake_es = MagicMock()
    fake_es.indices = MagicMock()
    fake_es.indices.exists = AsyncMock(return_value=True)
    fake_es.search = AsyncMock(return_value={
        "hits": {"hits": [{"_source": raw_hit}]},
    })

    with patch(
        "dynastore.modules.storage.routing_config.get_output_transformers_for_search",
        new_callable=lambda: lambda *a, **kw: _async_return([]),
    ) as _mock, patch(
        "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
        return_value=fake_es,
    ), patch(
        "dynastore.modules.storage.drivers.elasticsearch.ItemsElasticsearchDriver"
        "._resolve_read_policy",
        new_callable=AsyncMock,
        return_value=None,
    ), patch(
        "dynastore.modules.storage.drivers.elasticsearch.ItemsElasticsearchDriver"
        "._build_read_search_body",
        return_value=({"query": {}}, {}),
    ), patch(
        "dynastore.modules.storage.drivers.elasticsearch.ItemsElasticsearchDriver"
        "._es_source_to_feature",
        side_effect=lambda src, _rp: src,
    ):
        from dynastore.modules.storage.drivers.elasticsearch import ItemsElasticsearchDriver
        from dynastore.models.query_builder import QueryRequest

        driver = ItemsElasticsearchDriver.__new__(ItemsElasticsearchDriver)
        request = MagicMock(spec=QueryRequest)
        results = [
            item async for item in driver.read_entities(
                "cat", "col", request=request, limit=10,
            )
        ]

    # No transformer fired — raw_hit must come back unchanged.
    assert results == [raw_hit]


@pytest.mark.asyncio
async def test_items_es_driver_read_entities_applies_restore_chain():
    """Configured output_transformer.restore_from_index runs per yielded item."""
    from unittest.mock import AsyncMock, MagicMock, patch

    raw_hit = {"id": "item-1", "_t": "X", "type": "Feature", "geometry": None, "properties": {}}
    transformer = _PrefixTransformer("X")

    fake_es = MagicMock()
    fake_es.indices = MagicMock()
    fake_es.indices.exists = AsyncMock(return_value=True)
    fake_es.search = AsyncMock(return_value={
        "hits": {"hits": [{"_source": raw_hit}]},
    })

    with patch(
        "dynastore.modules.storage.routing_config.get_output_transformers_for_search",
        new_callable=lambda: lambda *a, **kw: _async_return([transformer]),
    ), patch(
        "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
        return_value=fake_es,
    ), patch(
        "dynastore.modules.storage.drivers.elasticsearch.ItemsElasticsearchDriver"
        "._resolve_read_policy",
        new_callable=AsyncMock,
        return_value=None,
    ), patch(
        "dynastore.modules.storage.drivers.elasticsearch.ItemsElasticsearchDriver"
        "._build_read_search_body",
        return_value=({"query": {}}, {}),
    ), patch(
        "dynastore.modules.storage.drivers.elasticsearch.ItemsElasticsearchDriver"
        "._es_source_to_feature",
        side_effect=lambda src, _rp: src,
    ):
        from dynastore.modules.storage.drivers.elasticsearch import ItemsElasticsearchDriver
        from dynastore.models.query_builder import QueryRequest

        driver = ItemsElasticsearchDriver.__new__(ItemsElasticsearchDriver)
        request = MagicMock(spec=QueryRequest)
        results = [
            item async for item in driver.read_entities(
                "cat", "col", request=request, limit=10,
            )
        ]

    assert len(results) == 1
    # restore_from_index must have stripped the sentinel "_t" key.
    assert "_t" not in results[0]


# ---  ItemsElasticsearchEnvelopeDriver.read_entities (entity="item") -------


@pytest.mark.asyncio
async def test_envelope_driver_read_entities_no_op_when_chain_empty():
    """Empty chain leaves items unchanged in the envelope driver."""
    from unittest.mock import AsyncMock, MagicMock, patch

    raw_src = {"id": "e-1", "type": "Feature", "geoid": "e-1", "geometry": None}

    fake_es = MagicMock()
    fake_es.indices = MagicMock()
    fake_es.indices.exists = AsyncMock(return_value=True)
    fake_es.search = AsyncMock(return_value={
        "hits": {"hits": [{"_source": raw_src}]},
    })

    with patch(
        "dynastore.modules.storage.routing_config.get_output_transformers_for_search",
        new_callable=lambda: lambda *a, **kw: _async_return([]),
    ), patch(
        "dynastore.modules.storage.drivers.elasticsearch_envelope.driver"
        ".ItemsElasticsearchEnvelopeDriver._items_index_name",
        return_value="test-cat-envelope-items",
    ), patch(
        "dynastore.modules.storage.drivers.elasticsearch_envelope.driver"
        ".ItemsElasticsearchEnvelopeDriver._get_client",
        return_value=fake_es,
    ), patch(
        "dynastore.modules.storage.drivers.elasticsearch_envelope.driver"
        ".ItemsElasticsearchEnvelopeDriver._build_read_search_body",
        return_value=({"query": {}}, {}),
    ), patch(
        "dynastore.modules.storage.drivers.elasticsearch_envelope.driver"
        ".ItemsElasticsearchEnvelopeDriver._envelope_source_to_feature",
        side_effect=lambda src, cat, col, geoid: src,
    ):
        from dynastore.modules.storage.drivers.elasticsearch_envelope.driver import (
            ItemsElasticsearchEnvelopeDriver,
        )
        from dynastore.models.query_builder import QueryRequest

        driver = ItemsElasticsearchEnvelopeDriver.__new__(ItemsElasticsearchEnvelopeDriver)
        request = MagicMock(spec=QueryRequest)
        results = [
            item async for item in driver.read_entities(
                "cat", "col", request=request, limit=10,
            )
        ]

    assert results == [raw_src]


@pytest.mark.asyncio
async def test_envelope_driver_read_entities_applies_restore_chain():
    """Configured transformer's restore_from_index runs per yielded item."""
    from unittest.mock import AsyncMock, MagicMock, patch

    raw_src = {"id": "e-1", "_t": "X", "geoid": "e-1"}
    transformer = _PrefixTransformer("X")

    fake_es = MagicMock()
    fake_es.indices = MagicMock()
    fake_es.indices.exists = AsyncMock(return_value=True)
    fake_es.search = AsyncMock(return_value={
        "hits": {"hits": [{"_source": raw_src}]},
    })

    with patch(
        "dynastore.modules.storage.routing_config.get_output_transformers_for_search",
        new_callable=lambda: lambda *a, **kw: _async_return([transformer]),
    ), patch(
        "dynastore.modules.storage.drivers.elasticsearch_envelope.driver"
        ".ItemsElasticsearchEnvelopeDriver._items_index_name",
        return_value="test-cat-envelope-items",
    ), patch(
        "dynastore.modules.storage.drivers.elasticsearch_envelope.driver"
        ".ItemsElasticsearchEnvelopeDriver._get_client",
        return_value=fake_es,
    ), patch(
        "dynastore.modules.storage.drivers.elasticsearch_envelope.driver"
        ".ItemsElasticsearchEnvelopeDriver._build_read_search_body",
        return_value=({"query": {}}, {}),
    ), patch(
        "dynastore.modules.storage.drivers.elasticsearch_envelope.driver"
        ".ItemsElasticsearchEnvelopeDriver._envelope_source_to_feature",
        side_effect=lambda src, cat, col, geoid: src,
    ):
        from dynastore.modules.storage.drivers.elasticsearch_envelope.driver import (
            ItemsElasticsearchEnvelopeDriver,
        )
        from dynastore.models.query_builder import QueryRequest

        driver = ItemsElasticsearchEnvelopeDriver.__new__(ItemsElasticsearchEnvelopeDriver)
        request = MagicMock(spec=QueryRequest)
        results = [
            item async for item in driver.read_entities(
                "cat", "col", request=request, limit=10,
            )
        ]

    assert len(results) == 1
    assert "_t" not in results[0]


# ---  CollectionElasticsearchDriver.get_metadata (entity="collection") -----


@pytest.mark.asyncio
async def test_collection_es_driver_get_metadata_no_op_when_chain_empty():
    """Empty chain → collection metadata passes through _unenrich_doc unchanged."""
    from unittest.mock import AsyncMock, MagicMock, patch

    raw_doc = {"id": "col-1", "description": "desc"}

    fake_client = MagicMock()
    fake_client.get = AsyncMock(return_value={"_source": raw_doc})

    with patch(
        "dynastore.modules.storage.routing_config.get_output_transformers_for_search",
        new_callable=lambda: lambda *a, **kw: _async_return([]),
    ), patch(
        "dynastore.modules.elasticsearch.collection_es_driver"
        ".CollectionElasticsearchDriver._get_client",
        return_value=fake_client,
    ), patch(
        "dynastore.modules.elasticsearch.collection_es_driver"
        ".CollectionElasticsearchDriver._index_name",
        return_value="test-collections",
    ), patch(
        "dynastore.modules.elasticsearch.collection_es_driver"
        ".CollectionElasticsearchDriver._unenrich_doc",
        side_effect=lambda d: d,
    ):
        from dynastore.modules.elasticsearch.collection_es_driver import CollectionElasticsearchDriver

        driver = CollectionElasticsearchDriver.__new__(CollectionElasticsearchDriver)
        result = await driver.get_metadata("cat", "col-1")

    assert result == raw_doc


@pytest.mark.asyncio
async def test_collection_es_driver_get_metadata_applies_restore_chain():
    """Configured transformer restores each collection metadata document."""
    from unittest.mock import AsyncMock, MagicMock, patch

    raw_doc = {"id": "col-1", "_t": "X", "description": "desc"}
    transformer = _PrefixTransformer("X")

    fake_client = MagicMock()
    fake_client.get = AsyncMock(return_value={"_source": raw_doc})

    with patch(
        "dynastore.modules.storage.routing_config.get_output_transformers_for_search",
        new_callable=lambda: lambda *a, **kw: _async_return([transformer]),
    ), patch(
        "dynastore.modules.elasticsearch.collection_es_driver"
        ".CollectionElasticsearchDriver._get_client",
        return_value=fake_client,
    ), patch(
        "dynastore.modules.elasticsearch.collection_es_driver"
        ".CollectionElasticsearchDriver._index_name",
        return_value="test-collections",
    ), patch(
        "dynastore.modules.elasticsearch.collection_es_driver"
        ".CollectionElasticsearchDriver._unenrich_doc",
        side_effect=lambda d: d,
    ):
        from dynastore.modules.elasticsearch.collection_es_driver import CollectionElasticsearchDriver

        driver = CollectionElasticsearchDriver.__new__(CollectionElasticsearchDriver)
        result = await driver.get_metadata("cat", "col-1")

    assert result is not None
    assert "_t" not in result


# ---  CollectionElasticsearchDriver.search_metadata (entity="collection") --


@pytest.mark.asyncio
async def test_collection_es_driver_search_metadata_no_op_when_chain_empty():
    """Empty chain → search_metadata results pass through unchanged."""
    from unittest.mock import AsyncMock, MagicMock, patch

    raw_doc = {"id": "col-1", "description": "desc"}

    fake_client = MagicMock()
    fake_client.search = AsyncMock(return_value={
        "hits": {"hits": [{"_source": raw_doc}], "total": {"value": 1}},
    })

    with patch(
        "dynastore.modules.storage.routing_config.get_output_transformers_for_search",
        new_callable=lambda: lambda *a, **kw: _async_return([]),
    ), patch(
        "dynastore.modules.elasticsearch.collection_es_driver"
        ".CollectionElasticsearchDriver._get_client",
        return_value=fake_client,
    ), patch(
        "dynastore.modules.elasticsearch.collection_es_driver"
        ".CollectionElasticsearchDriver._index_name",
        return_value="test-collections",
    ), patch(
        "dynastore.modules.elasticsearch.collection_es_driver"
        ".CollectionElasticsearchDriver._unenrich_doc",
        side_effect=lambda d: d,
    ):
        from dynastore.modules.elasticsearch.collection_es_driver import CollectionElasticsearchDriver

        driver = CollectionElasticsearchDriver.__new__(CollectionElasticsearchDriver)
        results, total = await driver.search_metadata("cat", limit=10)

    assert results == [raw_doc]
    assert total == 1


@pytest.mark.asyncio
async def test_collection_es_driver_search_metadata_applies_restore_chain():
    """Configured transformer restores each hit returned by search_metadata."""
    from unittest.mock import AsyncMock, MagicMock, patch

    raw_doc = {"id": "col-1", "_t": "X", "description": "desc"}
    transformer = _PrefixTransformer("X")

    fake_client = MagicMock()
    fake_client.search = AsyncMock(return_value={
        "hits": {"hits": [{"_source": raw_doc}], "total": {"value": 1}},
    })

    with patch(
        "dynastore.modules.storage.routing_config.get_output_transformers_for_search",
        new_callable=lambda: lambda *a, **kw: _async_return([transformer]),
    ), patch(
        "dynastore.modules.elasticsearch.collection_es_driver"
        ".CollectionElasticsearchDriver._get_client",
        return_value=fake_client,
    ), patch(
        "dynastore.modules.elasticsearch.collection_es_driver"
        ".CollectionElasticsearchDriver._index_name",
        return_value="test-collections",
    ), patch(
        "dynastore.modules.elasticsearch.collection_es_driver"
        ".CollectionElasticsearchDriver._unenrich_doc",
        side_effect=lambda d: d,
    ):
        from dynastore.modules.elasticsearch.collection_es_driver import CollectionElasticsearchDriver

        driver = CollectionElasticsearchDriver.__new__(CollectionElasticsearchDriver)
        results, total = await driver.search_metadata("cat", limit=10)

    assert len(results) == 1
    assert "_t" not in results[0]


# ---  CatalogElasticsearchDriver.get_catalog_metadata (entity="catalog") ---


@pytest.mark.asyncio
async def test_catalog_es_driver_get_metadata_no_op_when_chain_empty():
    """Empty chain → catalog metadata passes through unchanged."""
    from unittest.mock import AsyncMock, MagicMock, patch

    raw_doc = {"id": "cat-1", "description": "catalog"}

    fake_client = MagicMock()
    fake_client.indices = MagicMock()
    fake_client.indices.exists = AsyncMock(return_value=True)
    fake_client.get = AsyncMock(return_value={"_source": raw_doc})

    with patch(
        "dynastore.modules.storage.routing_config.get_output_transformers_for_search",
        new_callable=lambda: lambda *a, **kw: _async_return([]),
    ), patch(
        "dynastore.modules.elasticsearch.catalog_es_driver"
        ".CatalogElasticsearchDriver._get_client",
        return_value=fake_client,
    ), patch(
        "dynastore.modules.elasticsearch.catalog_es_driver"
        ".CatalogElasticsearchDriver._index_name",
        return_value="test-catalog-meta",
    ):
        from dynastore.modules.elasticsearch.catalog_es_driver import CatalogElasticsearchDriver

        driver = CatalogElasticsearchDriver.__new__(CatalogElasticsearchDriver)
        result = await driver.get_catalog_metadata("cat-1")

    assert result == raw_doc


@pytest.mark.asyncio
async def test_catalog_es_driver_get_metadata_applies_restore_chain():
    """Configured transformer restores the catalog metadata document."""
    from unittest.mock import AsyncMock, MagicMock, patch

    raw_doc = {"id": "cat-1", "_t": "X", "description": "catalog"}
    transformer = _PrefixTransformer("X")

    fake_client = MagicMock()
    fake_client.indices = MagicMock()
    fake_client.indices.exists = AsyncMock(return_value=True)
    fake_client.get = AsyncMock(return_value={"_source": raw_doc})

    with patch(
        "dynastore.modules.storage.routing_config.get_output_transformers_for_search",
        new_callable=lambda: lambda *a, **kw: _async_return([transformer]),
    ), patch(
        "dynastore.modules.elasticsearch.catalog_es_driver"
        ".CatalogElasticsearchDriver._get_client",
        return_value=fake_client,
    ), patch(
        "dynastore.modules.elasticsearch.catalog_es_driver"
        ".CatalogElasticsearchDriver._index_name",
        return_value="test-catalog-meta",
    ):
        from dynastore.modules.elasticsearch.catalog_es_driver import CatalogElasticsearchDriver

        driver = CatalogElasticsearchDriver.__new__(CatalogElasticsearchDriver)
        result = await driver.get_catalog_metadata("cat-1")

    assert result is not None
    assert "_t" not in result


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

async def _async_return(value):
    """Coroutine that returns ``value`` — used as a patch side_effect."""
    return value
