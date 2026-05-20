"""ES asset-search collection scope (tri-state) — #1095.

``AssetElasticsearchDriver.search_assets`` must build the same three
collection scopes the PG driver does:

- ``collection_id="<id>"`` → ``term`` filter on that collection.
- ``collection_id=None`` (default) → catalog-tier only, via a
  ``must_not exists collection_id`` clause. Previously ES treated ``None``
  as "match everything", silently diverging from PG's catalog-tier scope.
- ``all_collections=True`` → no collection clause; the whole catalog.
"""

from __future__ import annotations

import json

import pytest

from dynastore.modules.storage.drivers import elasticsearch as es_mod


class _FakeEs:
    def __init__(self):
        self.captured: dict = {}

    async def search(self, *, index, query=None, size=None, from_=None, **kwargs):
        self.captured = {"index": index, "query": query, "size": size, "from_": from_}
        return {"hits": {"hits": []}}


@pytest.fixture
def driver_and_es(monkeypatch):
    drv = es_mod.AssetElasticsearchDriver()
    fake = _FakeEs()
    monkeypatch.setattr(drv, "_get_client", lambda: fake)

    monkeypatch.setattr(
        "dynastore.modules.elasticsearch.mappings.get_assets_index_name",
        lambda prefix, catalog_id: f"{prefix}-{catalog_id}-assets",
        raising=True,
    )
    monkeypatch.setattr(
        "dynastore.modules.elasticsearch.client.get_index_prefix",
        lambda: "dynastore",
        raising=True,
    )

    async def _no_transformers(*_a, **_k):
        return []

    monkeypatch.setattr(
        "dynastore.modules.storage.routing_config.get_output_transformers_for_search",
        _no_transformers,
        raising=True,
    )
    return drv, fake


@pytest.mark.asyncio
async def test_single_collection_emits_term_filter(driver_and_es):
    drv, fake = driver_and_es
    await drv.search_assets("cat-a", collection_id="coll-1")
    q = fake.captured["query"]
    assert q["bool"]["filter"] == [{"term": {"collection_id": "coll-1"}}]
    assert "must_not" not in q["bool"]


@pytest.mark.asyncio
async def test_catalog_tier_default_emits_must_not_exists(driver_and_es):
    drv, fake = driver_and_es
    await drv.search_assets("cat-a")
    q = fake.captured["query"]
    assert q["bool"]["must_not"] == [{"exists": {"field": "collection_id"}}]
    assert "filter" not in q["bool"]


@pytest.mark.asyncio
async def test_all_collections_emits_no_collection_clause(driver_and_es):
    drv, fake = driver_and_es
    await drv.search_assets("cat-a", collection_id="ignored", all_collections=True)
    q = fake.captured["query"]
    assert "collection_id" not in json.dumps(q)
