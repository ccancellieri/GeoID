#    Copyright 2025 FAO
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
"""``_ElasticsearchBase.search_items_struct`` — the shared, routing-aware
structural item search used by STAC ``/search`` dispatch (#989).

Both the public ``ItemsElasticsearchDriver`` and the tenant-scoped
``ItemsElasticsearchPrivateDriver`` inherit one implementation that builds the
canonical ES DSL (``build_items_query`` SSOT) and only differs in which index
it targets. These tests pin the query body, index resolution, pagination
(size/from), and the empty-on-no-client degrade path with a stub ES client.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from dynastore.modules.elasticsearch.items_query import build_items_query
from dynastore.modules.storage.drivers.elasticsearch import ItemsElasticsearchDriver
from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
    ItemsElasticsearchPrivateDriver,
)
from dynastore.modules.elasticsearch.mappings import get_tenant_items_index
from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
    get_private_index_name,
)

_PREFIX = "dynastore"


class _SearchStubEs:
    """Minimal async ES double recording the last ``search`` call."""

    def __init__(self, hits, total):
        self._hits = hits
        self._total = total
        self.search_calls: list = []

    async def search(self, *, index, body, **kwargs):
        self.search_calls.append({"index": index, "body": body, "kwargs": kwargs})
        return {
            "hits": {
                "total": {"value": self._total},
                "hits": [{"_source": h} for h in self._hits],
            }
        }


def _patches(es):
    return (
        patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es
        ),
        patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value=_PREFIX,
        ),
    )


@pytest.mark.asyncio
async def test_public_driver_search_items_struct_query_index_and_pagination():
    src = {"id": "i1", "type": "Feature", "collection": "c1", "geometry": None}
    es = _SearchStubEs(hits=[src], total=7)
    p1, p2 = _patches(es)
    with p1, p2:
        driver = ItemsElasticsearchDriver()
        result = await driver.search_items_struct(
            catalog_id="cat1",
            collections=["c1"],
            ids=None,
            bbox=[-10.0, -5.0, 10.0, 5.0],
            intersects=None,
            datetime="2020-01-01T00:00:00Z/..",
            limit=20,
            offset=40,
        )

    assert result.total == 7
    assert result.features == [src]
    assert len(es.search_calls) == 1
    call = es.search_calls[0]
    assert call["index"] == get_tenant_items_index(_PREFIX, "cat1")
    expected_query = build_items_query(
        collections=["c1"],
        bbox=[-10.0, -5.0, 10.0, 5.0],
        datetime="2020-01-01T00:00:00Z/..",
    )
    assert call["body"]["query"] == expected_query
    assert call["body"]["size"] == 20
    assert call["body"]["from"] == 40


@pytest.mark.asyncio
async def test_private_driver_search_items_struct_targets_private_index():
    src = {"id": "p1", "type": "Feature", "collection": "c9", "geometry": None}
    es = _SearchStubEs(hits=[src], total=1)
    p1, p2 = _patches(es)
    with p1, p2:
        driver = ItemsElasticsearchPrivateDriver()
        result = await driver.search_items_struct(
            catalog_id="geoidcat",
            collections=["c9"],
            ids=None,
            bbox=None,
            intersects=None,
            datetime=None,
            limit=10,
            offset=0,
        )

    assert result.total == 1
    assert result.features == [src]
    assert es.search_calls[0]["index"] == get_private_index_name(_PREFIX, "geoidcat")


@pytest.mark.asyncio
async def test_search_items_struct_degrades_to_empty_without_client():
    with patch(
        "dynastore.modules.elasticsearch.client.get_client", return_value=None
    ):
        driver = ItemsElasticsearchDriver()
        result = await driver.search_items_struct(
            catalog_id="cat1",
            collections=["c1"],
            ids=None,
            bbox=None,
            intersects=None,
            datetime=None,
            limit=10,
            offset=0,
        )
    assert result.features == []
    assert result.total == 0
