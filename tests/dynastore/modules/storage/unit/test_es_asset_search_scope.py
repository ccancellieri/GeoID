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

"""ES asset-search scoping + operator coverage — #1095 / #1096.

``AssetElasticsearchDriver.search_assets`` must build the same three
collection scopes the PG driver does:

- ``collection_id="<id>"`` → ``term`` filter on that collection.
- ``collection_id=None`` (default) → catalog-tier only, via a
  ``must_not exists collection_id`` clause. Previously ES treated ``None``
  as "match everything", silently diverging from PG's catalog-tier scope.
- ``all_collections=True`` → no collection clause; the whole catalog.

It must also honour the full asset-filter operator set (#1096): positive
operators land in ``bool.filter``, negations in ``bool.must_not``. The
translation is shared with the PG driver via
``dynastore.modules.tools.asset_filters.build_es_query``.
"""

from __future__ import annotations

import json

import pytest

from dynastore.models.query_builder import AssetFilter, FilterOperator
from dynastore.modules.storage.drivers import elasticsearch as es_mod


class _FakeEs:
    """Mirrors opensearch-py's ``AsyncOpenSearch.search`` signature exactly
    (keyword-only ``body/index/params/headers``, no ``**kwargs``) so a call
    using elasticsearch-py 8.x kwargs (``query=``/``size=``/``from_=``)
    fails here the same way it fails against the real client (#3318)."""

    def __init__(self):
        self.captured: dict = {}
        self.hits: list = []

    async def search(self, *, body=None, index=None, params=None, headers=None):
        self.captured = {
            "index": index,
            "query": (body or {}).get("query"),
            "size": (body or {}).get("size"),
            "from": (body or {}).get("from"),
        }
        return {"hits": {"hits": self.hits}}


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


# ---------------------------------------------------------------------------
# Operator coverage beyond EQ — #1096
# (use all_collections=True so the query is the raw filter bool, unwrapped)
# ---------------------------------------------------------------------------


async def _search(drv, fake, filters):
    await drv.search_assets("cat-a", all_collections=True, filters=filters)
    return fake.captured["query"]


@pytest.mark.asyncio
async def test_eq_filter_emits_term(driver_and_es):
    drv, fake = driver_and_es
    q = await _search(drv, fake, [AssetFilter(field="asset_type", op=FilterOperator.EQ, value="RASTER")])
    assert q["bool"]["filter"] == [{"term": {"asset_type": "RASTER"}}]


@pytest.mark.asyncio
@pytest.mark.parametrize("op", [FilterOperator.GT, FilterOperator.GTE, FilterOperator.LT, FilterOperator.LTE])
async def test_range_filters(driver_and_es, op):
    drv, fake = driver_and_es
    q = await _search(drv, fake, [AssetFilter(field="size_bytes", op=op, value=100)])
    assert q["bool"]["filter"] == [{"range": {"size_bytes": {op.value: 100}}}]


@pytest.mark.asyncio
async def test_like_emits_wildcard_with_translated_pattern(driver_and_es):
    drv, fake = driver_and_es
    q = await _search(drv, fake, [AssetFilter(field="filename", op=FilterOperator.LIKE, value="%scene_?.tif")])
    # SQL %->* (any run), SQL _->? (one char); the literal ES '?' in the input
    # is escaped (\\?) so it keeps its literal meaning.
    assert q["bool"]["filter"] == [{"wildcard": {"filename": {"value": "*scene?\\?.tif"}}}]


@pytest.mark.asyncio
async def test_ilike_sets_case_insensitive(driver_and_es):
    drv, fake = driver_and_es
    q = await _search(drv, fake, [AssetFilter(field="filename", op=FilterOperator.ILIKE, value="A%")])
    wc = q["bool"]["filter"][0]["wildcard"]["filename"]
    assert wc["case_insensitive"] is True
    assert wc["value"] == "A*"


@pytest.mark.asyncio
async def test_in_emits_terms(driver_and_es):
    drv, fake = driver_and_es
    q = await _search(drv, fake, [AssetFilter(field="asset_id", op=FilterOperator.IN, value=["a", "b"])])
    assert q["bool"]["filter"] == [{"terms": {"asset_id": ["a", "b"]}}]


@pytest.mark.asyncio
async def test_ne_nin_isnull_go_to_must_not(driver_and_es):
    drv, fake = driver_and_es
    q = await _search(
        drv,
        fake,
        [
            AssetFilter(field="asset_type", op=FilterOperator.NE, value="X"),
            AssetFilter(field="provider", op=FilterOperator.NIN, value=["a", "b"]),
            AssetFilter(field="href", op=FilterOperator.IS_NULL, value=None),
        ],
    )
    must_not = q["bool"]["must_not"]
    assert {"term": {"asset_type": "X"}} in must_not
    assert {"terms": {"provider": ["a", "b"]}} in must_not
    assert {"exists": {"field": "href"}} in must_not
    assert "filter" not in q["bool"]


@pytest.mark.asyncio
async def test_is_not_null_goes_to_filter(driver_and_es):
    drv, fake = driver_and_es
    q = await _search(drv, fake, [AssetFilter(field="href", op=FilterOperator.IS_NOT_NULL, value=None)])
    assert q["bool"]["filter"] == [{"exists": {"field": "href"}}]


# ---------------------------------------------------------------------------
# opensearch-py calling convention — #3318
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_uses_opensearch_body_convention(driver_and_es):
    """``size``/``from`` must travel inside ``body`` and hits must reach the
    caller. Before #3318 the driver passed elasticsearch-py 8.x kwargs, every
    search raised TypeError, and the except-all turned it into ``[]``."""
    drv, fake = driver_and_es
    fake.hits = [{"_source": {"asset_id": "a-1"}}]
    out = await drv.search_assets("cat-a", collection_id="coll-1", limit=7, offset=3)
    assert out == [{"asset_id": "a-1"}]
    assert fake.captured["size"] == 7
    assert fake.captured["from"] == 3


@pytest.mark.asyncio
async def test_unsupported_operator_raises(driver_and_es):
    drv, fake = driver_and_es
    with pytest.raises(ValueError, match="unsupported operator"):
        await drv.search_assets(
            "cat-a",
            all_collections=True,
            filters=[AssetFilter(field="geom", op=FilterOperator.INTERSECTS, value="x")],
        )
