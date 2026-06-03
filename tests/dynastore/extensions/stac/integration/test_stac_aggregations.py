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
#    See the License for the specific language governing permissions and
#    limitations under the License.

import pytest
import uuid

from httpx import AsyncClient

from dynastore.extensions.stac.stac_models import STACItem

# Mark all tests in this module as asyncio
pytestmark = pytest.mark.asyncio


@pytest.mark.enable_extensions("stac", "assets", "features")
async def test_stac_aggregations(sysadmin_in_process_client, in_process_client: AsyncClient, setup_aggregation_data):
    """
    Integration test for the STAC aggregation endpoints.
    Tests both the dedicated /aggregate endpoint and aggregations within /search.
    """
    catalog_id, collection_id = setup_aggregation_data

    # --- Test 1: Dedicated /aggregate endpoint ---
    aggregation_request = {
        "collections": [collection_id],
        "aggregations": [
            {"name": "value_stats", "type": "stats", "property": "properties.value"},
            {"name": "categories", "type": "term", "property": "properties.category", "limit": 5},
        ]
    }

    response = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}/aggregate",
        json=aggregation_request,
    )

    assert response.status_code == 200
    results = response.json()
    aggs = results.get("aggregations", {})

    # Assert Stats
    stats = aggs.get("value_stats")
    assert stats is not None
    assert stats["min"] == 10.0
    assert stats["max"] == 50.0
    assert stats["sum"] == 150.0
    assert stats["avg"] == 30.0
    assert stats["count"] == 5

    # Assert Terms
    terms = aggs.get("categories")
    assert terms is not None
    buckets = {b["key"]: b["doc_count"] for b in terms["buckets"]}
    assert buckets == {"A": 2, "B": 2, "C": 1}

    # --- Test 2: Aggregations via /search endpoint ---
    search_request = {
        "catalog_id": catalog_id,
        "collections": [collection_id],
        "limit": 1,
        "aggregate": [ # Using alias for test coverage
            {"name": "value_stats", "type": "stats", "property": "properties.value"},
            {"name": "categories", "type": "term", "property": "properties.category", "limit": 5},
        ]
    }

    search_response = await sysadmin_in_process_client.post(f"/stac/catalogs/{catalog_id}/search", json=search_request)
    assert search_response.status_code == 200
    search_results = search_response.json()

    # Assert items are returned
    assert search_results["numberMatched"] == 5
    assert len(search_results["features"]) == 1

    # Assert aggregations are present and correct
    search_aggs = search_results.get("aggregations")
    assert search_aggs is not None
    
    # Assert stats from search
    search_stats = search_aggs.get("value_stats")
    assert search_stats is not None
    assert search_stats["min"] == 10.0
    assert search_stats["max"] == 50.0
    assert search_stats["count"] == 5

    # Assert terms from search
    search_terms = search_aggs.get("categories")
    assert search_terms is not None
    search_buckets = {b["key"]: b["doc_count"] for b in search_terms["buckets"]}
    assert search_buckets == {"A": 2, "B": 2, "C": 1}

@pytest.mark.enable_extensions("stac", "assets", "features")
async def test_stac_unique_values_aggregation(sysadmin_in_process_client, in_process_client: AsyncClient, setup_aggregation_data):
    """
    Integration test for unique value aggregation in STAC.
    """
    catalog_id, collection_id = setup_aggregation_data

    # Aggregate by unique values of 'category'
    aggregation_request = {
        "collections": [collection_id],
        "aggregations": [
            {"name": "unique_categories", "type": "term", "property": "properties.category", "limit": 100}
        ]
    }

    response = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}/aggregate",
        json=aggregation_request,
    )

    assert response.status_code == 200
    results = response.json()
    aggs = results.get("aggregations", {})

    terms = aggs.get("unique_categories")
    assert terms is not None
    
    # Verify we got unique keys
    keys = [b["key"] for b in terms["buckets"]]
    assert len(keys) == 3
    assert set(keys) == {"A", "B", "C"}
    
    # Verify counts
    buckets = {b["key"]: b["doc_count"] for b in terms["buckets"]}
    assert buckets["A"] == 2
    assert buckets["B"] == 2
    assert buckets["C"] == 1