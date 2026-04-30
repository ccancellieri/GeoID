"""PR-2b smoke tests for the regular-driver bulk reindex tasks.

Covers shape only — does not exercise live ES. Stubs:
  * ``get_client`` → fake ES with capture lists
  * ``get_index_prefix`` → constant
  * ``get_protocol(CatalogsProtocol)`` → fake catalogs proto returning a
    fixed page of features then EOF
  * ``ConfigsProtocol`` lookup of ``CollectionRoutingConfig`` → routing
    that lists ``ItemsElasticsearchDriver`` (so the collection is
    eligible for reindex) or doesn't (so it skips)
"""

from __future__ import annotations

from typing import Dict, List
from unittest.mock import patch

import pytest

from dynastore.tasks.elasticsearch_indexer.tasks import (
    BulkCatalogReindexInputs,
    BulkCatalogReindexTask,
    BulkCollectionReindexInputs,
    BulkCollectionReindexTask,
)


class _FakeIndices:
    pass


class _FakeEs:
    def __init__(self):
        self.indices = _FakeIndices()
        self.bulk_calls: list = []
        self.delete_by_query_calls: list = []

    async def bulk(self, *, body, params=None, **kwargs):
        self.bulk_calls.append({"body": body, "params": params})
        return {"items": []}

    async def delete_by_query(self, *, index, body, params=None, **kwargs):
        self.delete_by_query_calls.append({
            "index": index, "body": body, "params": params,
        })
        return {"deleted": 0}


class _FakeCatalogs:
    """Returns a single page of features then EOF."""

    def __init__(self, features_per_collection: Dict[str, List[Dict]]):
        self._features_per_collection = features_per_collection
        self._served = set()

    async def search(self, catalog_id, collection_id, *, limit, offset):
        if collection_id in self._served:
            return {"features": []}
        self._served.add(collection_id)
        return {"features": self._features_per_collection.get(collection_id, [])}

    async def list_collections(self, catalog_id, *, limit, offset):
        if offset > 0:
            return []
        # Simple object with .id
        return [
            type("C", (), {"id": cid})
            for cid in self._features_per_collection.keys()
        ]


def _routing_with_es(driver_id: str = "items_elasticsearch_driver"):
    """Fake CollectionRoutingConfig listing the regular ES driver."""
    return type("Routing", (), {
        "operations": {"INDEX": [
            type("Entry", (), {"driver_id": driver_id})()
        ]},
    })()


def _routing_without_es():
    return type("Routing", (), {
        "operations": {"INDEX": [
            type("Entry", (), {"driver_id": "other_driver"})()
        ]},
    })()


def _make_payload(model_inputs):
    """Wrap pydantic inputs in the TaskPayload shape the run() expects."""
    from uuid import uuid4
    from dynastore.modules.tasks.models import TaskPayload

    return TaskPayload(
        task_id=uuid4(),
        caller_id="tests:bulk-reindex",
        inputs=model_inputs.model_dump(),
    )


@pytest.mark.asyncio
async def test_collection_reindex_targets_tenant_index_with_routing(monkeypatch):
    es = _FakeEs()
    catalogs = _FakeCatalogs({"col1": [{"id": "f1"}, {"id": "f2"}]})

    async def _get_config(model, *, catalog_id, collection_id=None):
        return _routing_with_es()

    fake_configs = type("C", (), {"get_config": staticmethod(_get_config)})()

    def _get_protocol(proto):
        name = getattr(proto, "__name__", str(proto))
        if "ConfigsProtocol" in name:
            return fake_configs
        if "CatalogsProtocol" in name:
            return catalogs
        return None

    with patch(
        "dynastore.modules.elasticsearch.client.get_client", return_value=es,
    ), patch(
        "dynastore.modules.elasticsearch.client.get_index_prefix",
        return_value="dynastore",
    ), patch(
        "dynastore.tools.discovery.get_protocol", side_effect=_get_protocol,
    ):
        task = BulkCollectionReindexTask()
        result = await task.run(_make_payload(
            BulkCollectionReindexInputs(catalog_id="cat1", collection_id="col1"),
        ))

    assert result["status"] == "done"
    assert result["total_indexed"] == 2

    # Pre-reindex delete_by_query was issued with collection scope.
    assert es.delete_by_query_calls
    dbq = es.delete_by_query_calls[0]
    assert dbq["index"] == "dynastore-cat1-items"
    assert dbq["body"] == {"query": {"term": {"collection": "col1"}}}
    assert dbq["params"]["routing"] == "col1"

    # Bulk action shape: every action carries _index + _id + routing.
    body = es.bulk_calls[0]["body"]
    assert len(body) == 4  # 2 features × (action, doc)
    for action_idx in (0, 2):
        action = body[action_idx]["index"]
        assert action["_index"] == "dynastore-cat1-items"
        assert action["routing"] == "col1"
    for doc_idx in (1, 3):
        doc = body[doc_idx]
        assert doc["catalog_id"] == "cat1"
        assert doc["collection"] == "col1"


@pytest.mark.asyncio
async def test_collection_reindex_skips_when_es_not_in_routing(monkeypatch):
    es = _FakeEs()
    catalogs = _FakeCatalogs({"col1": [{"id": "f1"}]})

    async def _get_config(model, *, catalog_id, collection_id=None):
        return _routing_without_es()

    fake_configs = type("C", (), {"get_config": staticmethod(_get_config)})()

    def _get_protocol(proto):
        name = getattr(proto, "__name__", str(proto))
        if "ConfigsProtocol" in name:
            return fake_configs
        if "CatalogsProtocol" in name:
            return catalogs
        return None

    with patch(
        "dynastore.modules.elasticsearch.client.get_client", return_value=es,
    ), patch(
        "dynastore.modules.elasticsearch.client.get_index_prefix",
        return_value="dynastore",
    ), patch(
        "dynastore.tools.discovery.get_protocol", side_effect=_get_protocol,
    ):
        task = BulkCollectionReindexTask()
        result = await task.run(_make_payload(
            BulkCollectionReindexInputs(catalog_id="cat1", collection_id="col1"),
        ))

    assert result["total_indexed"] == 0
    assert es.bulk_calls == []
    # delete_by_query still ran (pre-reindex wipe is unconditional) — that's
    # acceptable since the index is the right one and the wipe is bounded.
    assert es.delete_by_query_calls


@pytest.mark.asyncio
async def test_catalog_reindex_iterates_collections(monkeypatch):
    es = _FakeEs()
    catalogs = _FakeCatalogs({
        "col1": [{"id": "f1"}],
        "col2": [{"id": "f2"}, {"id": "f3"}],
    })

    async def _get_config(model, *, catalog_id, collection_id=None):
        return _routing_with_es()

    fake_configs = type("C", (), {"get_config": staticmethod(_get_config)})()

    def _get_protocol(proto):
        name = getattr(proto, "__name__", str(proto))
        if "ConfigsProtocol" in name:
            return fake_configs
        if "CatalogsProtocol" in name:
            return catalogs
        return None

    with patch(
        "dynastore.modules.elasticsearch.client.get_client", return_value=es,
    ), patch(
        "dynastore.modules.elasticsearch.client.get_index_prefix",
        return_value="dynastore",
    ), patch(
        "dynastore.tools.discovery.get_protocol", side_effect=_get_protocol,
    ):
        task = BulkCatalogReindexTask()
        result = await task.run(_make_payload(
            BulkCatalogReindexInputs(catalog_id="cat1"),
        ))

    assert result["status"] == "done"
    assert result["total_indexed"] == 3

    # Pre-reindex wipe is catalog-scope (no routing).
    dbq = es.delete_by_query_calls[0]
    assert dbq["index"] == "dynastore-cat1-items"
    assert dbq["body"] == {"query": {"match_all": {}}}
    assert "routing" not in dbq["params"]

    # Both collections produced bulk batches; routing keyed per collection.
    routings: list = []
    for call in es.bulk_calls:
        for entry in call["body"]:
            if "index" in entry:
                routings.append(entry["index"]["routing"])
    assert sorted(set(routings)) == ["col1", "col2"]


@pytest.mark.asyncio
async def test_inputs_drop_mode_field():
    """Schema regression — the mode field must be gone."""
    inputs = BulkCatalogReindexInputs(catalog_id="cat1")
    assert "mode" not in inputs.model_dump()
    inputs2 = BulkCollectionReindexInputs(catalog_id="cat1", collection_id="col1")
    assert "mode" not in inputs2.model_dump()


@pytest.mark.asyncio
async def test_is_es_active_for_matches_snake_case_driver_id():
    """PR-1e regression guard: ``OperationDriverEntry.driver_id`` is always
    snake_case after the validator coerces it. ``is_es_active_for`` must
    compare against ``"items_elasticsearch_driver"`` — pre-PR-1e it compared
    against ``"ItemsElasticsearchDriver"`` and silently returned False for
    every collection, breaking ES-aware downstream code paths.
    """
    from dynastore.modules.elasticsearch.bulk_reindex import is_es_active_for
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig,
        OperationDriverEntry,
        Operation,
    )
    from dynastore.tools import discovery

    routing = CollectionRoutingConfig(
        operations={Operation.READ: [OperationDriverEntry(driver_id="items_elasticsearch_driver")]},
    )

    async def _get_config(model, *, catalog_id, collection_id=None):
        return routing

    fake_configs = type("C", (), {"get_config": staticmethod(_get_config)})()

    def _get_protocol(proto):
        name = getattr(proto, "__name__", str(proto))
        if "ConfigsProtocol" in name:
            return fake_configs
        return None

    with patch.object(discovery, "get_protocol", side_effect=_get_protocol):
        assert await is_es_active_for("cat1", "col1") is True

    # And conversely: a routing without ES returns False.
    routing_pg_only = CollectionRoutingConfig(
        operations={Operation.READ: [OperationDriverEntry(driver_id="items_postgresql_driver")]},
    )

    async def _get_config_pg(model, *, catalog_id, collection_id=None):
        return routing_pg_only

    fake_configs_pg = type("C", (), {"get_config": staticmethod(_get_config_pg)})()

    def _get_protocol_pg(proto):
        name = getattr(proto, "__name__", str(proto))
        if "ConfigsProtocol" in name:
            return fake_configs_pg
        return None

    with patch.object(discovery, "get_protocol", side_effect=_get_protocol_pg):
        assert await is_es_active_for("cat1", "col1") is False
