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

import pytest

from httpx import AsyncClient
from tests.dynastore.test_utils import generate_test_id

from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
from dynastore.tasks.ingestion.ingestion_config import IngestionPluginConfig
from dynastore.tools.discovery import get_protocol

# Increase timeout for the endpoint specifically if needed, 
# or use a custom client with longer timeout.
@pytest.mark.asyncio
@pytest.mark.enable_extensions("features", "assets", "stac")
async def test_bulk_item_creation_success(sysadmin_in_process_client: AsyncClient, test_data_loader):
    catalog_id = f"c_{generate_test_id()}"
    collection_id = "test_bulk_collection"
    
    # 1. Create Catalog
    catalog_data = test_data_loader("catalog.json")
    catalog_data["id"] = catalog_id
    catalog_response = await sysadmin_in_process_client.post(
        f"/features/catalogs", 
        json=catalog_data,
        timeout=60.0
    )
    assert catalog_response.status_code == 201
    
    # 2. Create Collection
    collection_data = test_data_loader("collection.json")
    collection_data["id"] = collection_id
    collection_response = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections", 
        json=collection_data,
        timeout=60.0
    )
    assert collection_response.status_code == 201
    
    # 3. Bulk Create Items
    bulk_payload = test_data_loader("bulk_items.json")
    
    response = await sysadmin_in_process_client.post(f"/features/catalogs/{catalog_id}/collections/{collection_id}/items", json=bulk_payload, timeout=60.0)
    assert response.status_code == 201
    data = response.json()
    assert "ids" in data
    assert len(data["ids"]) == 2
    
    # 4. Verify items exist
    items_response = await sysadmin_in_process_client.get(f"/features/catalogs/{catalog_id}/collections/{collection_id}/items")
    assert items_response.status_code == 200
    items_data = items_response.json()
    assert len(items_data["features"]) == 2

@pytest.mark.asyncio
@pytest.mark.enable_extensions("features", "assets", "stac")
async def test_bulk_item_creation_atomicity(sysadmin_in_process_client: AsyncClient, test_data_loader):
    catalog_id = f"c_{generate_test_id()}"
    collection_id = "test_atomic_collection"
    
    # Create Catalog and Collection
    catalog_data = test_data_loader("catalog_atomic.json")
    catalog_data["id"] = catalog_id
    catalog_resp = await sysadmin_in_process_client.post(
        f"/features/catalogs", 
        json=catalog_data,
        timeout=60.0
    )
    assert catalog_resp.status_code == 201, f"Failed to create catalog: {catalog_resp.text}"
    collection_data = test_data_loader("collection_atomic.json")
    collection_data["id"] = collection_id
    collection_resp = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections", 
        json=collection_data,
        timeout=60.0
    )
    assert collection_resp.status_code == 201, f"Failed to create collection: {collection_resp.text}"
    
    # Payload with one invalid item (invalid geometry type)
    bulk_payload = test_data_loader("bulk_items_invalid.json")
    
    response = await sysadmin_in_process_client.post(f"/features/catalogs/{catalog_id}/collections/{collection_id}/items", json=bulk_payload, timeout=60.0)
    # Validation should fail
    assert response.status_code >= 400
    
    # Verify NO items exist (atomicity)
    items_response = await sysadmin_in_process_client.get(f"/features/catalogs/{catalog_id}/collections/{collection_id}/items")
    assert items_response.status_code == 200
    items_data = items_response.json()
    # It might return an empty FeatureCollection
    assert len(items_data.get("features", [])) == 0

@pytest.mark.asyncio
@pytest.mark.enable_extensions("features", "assets", "stac")
async def test_single_feature_post_returns_geojson(sysadmin_in_process_client: AsyncClient, test_data_loader):
    """POST /items with a single Feature must return a valid GeoJSON Feature (not a string repr)."""
    catalog_id = f"c_{generate_test_id()}"
    collection_id = "test_single_response"

    catalog_data = test_data_loader("catalog.json")
    catalog_data["id"] = catalog_id
    r = await sysadmin_in_process_client.post("/features/catalogs", json=catalog_data, timeout=60.0)
    assert r.status_code == 201

    collection_data = test_data_loader("collection.json")
    collection_data["id"] = collection_id
    r = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections", json=collection_data, timeout=60.0
    )
    assert r.status_code == 201

    single_payload = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [12.5, 41.9]},
        "properties": {"name": "Single Item", "value": 99},
    }

    r = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=single_payload,
        timeout=60.0,
    )
    assert r.status_code == 201
    assert "Location" in r.headers

    data = r.json()
    # Must be a valid GeoJSON Feature dict, not a string
    assert isinstance(data, dict), f"Expected dict, got {type(data)}: {str(data)[:200]}"
    assert data["type"] == "Feature"
    assert "id" in data
    assert "geometry" in data
    assert "properties" in data
    assert data["geometry"]["type"] == "Point"
    assert data["properties"]["name"] == "Single Item"
    assert data["properties"]["value"] == 99


@pytest.mark.asyncio
@pytest.mark.enable_extensions("features", "assets", "stac")
async def test_feature_collection_post_returns_ids(sysadmin_in_process_client: AsyncClient, test_data_loader):
    """POST /items with a FeatureCollection must return a BulkCreationResponse with ids."""
    catalog_id = f"c_{generate_test_id()}"
    collection_id = "test_bulk_response"

    catalog_data = test_data_loader("catalog.json")
    catalog_data["id"] = catalog_id
    r = await sysadmin_in_process_client.post("/features/catalogs", json=catalog_data, timeout=60.0)
    assert r.status_code == 201

    collection_data = test_data_loader("collection.json")
    collection_data["id"] = collection_id
    r = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections", json=collection_data, timeout=60.0
    )
    assert r.status_code == 201

    bulk_payload = test_data_loader("bulk_items.json")

    r = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=bulk_payload,
        timeout=60.0,
    )
    assert r.status_code == 201

    data = r.json()
    assert isinstance(data, dict), f"Expected dict, got {type(data)}: {str(data)[:200]}"
    assert "ids" in data
    assert isinstance(data["ids"], list)
    assert len(data["ids"]) == 2
    # Each id must be a non-empty string
    for fid in data["ids"]:
        assert isinstance(fid, str) and len(fid) > 0

    # Verify the created items are retrievable and match
    for fid in data["ids"]:
        r = await sysadmin_in_process_client.get(
            f"/features/catalogs/{catalog_id}/collections/{collection_id}/items/{fid}"
        )
        assert r.status_code == 200
        item = r.json()
        assert item["type"] == "Feature"
        assert item["id"] == fid


@pytest.mark.asyncio
@pytest.mark.enable_extensions("features", "assets", "stac")
async def test_oversize_feature_collection_rejected_before_ingest(
    sysadmin_in_process_client: AsyncClient, test_data_loader,
):
    """A FeatureCollection whose body exceeds ``sync_ingest_max_body_mb``
    is rejected with 413 by ``SyncIngestBodyLimitMiddleware`` (#2657)
    before the route ever parses it, and nothing is written."""
    catalog_id = f"c_{generate_test_id()}"
    collection_id = "test_oversize_collection"

    catalog_data = test_data_loader("catalog.json")
    catalog_data["id"] = catalog_id
    r = await sysadmin_in_process_client.post("/features/catalogs", json=catalog_data, timeout=60.0)
    assert r.status_code == 201

    collection_data = test_data_loader("collection.json")
    collection_data["id"] = collection_id
    r = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections", json=collection_data, timeout=60.0
    )
    assert r.status_code == 201

    config_mgr = get_protocol(PlatformConfigsProtocol)
    assert config_mgr is not None
    original_cfg = await config_mgr.get_config(IngestionPluginConfig)
    await config_mgr.set_config(
        IngestionPluginConfig,
        IngestionPluginConfig(sync_ingest_max_body_mb=1),
    )
    try:
        # A single oversize property pushes the encoded body past 1 MiB
        # without needing a large feature count.
        big_collection = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [12.5, 41.9]},
                    "properties": {"blob": "x" * (1024 * 1024 + 1024)},
                }
            ],
        }

        r = await sysadmin_in_process_client.post(
            f"/features/catalogs/{catalog_id}/collections/{collection_id}/items",
            json=big_collection,
            timeout=60.0,
        )
        assert r.status_code == 413, r.text
        detail = r.json()["detail"]
        assert detail["max_body_mb"] == 1
        assert "ingest-body-too-large" in detail["type"]
    finally:
        await config_mgr.set_config(IngestionPluginConfig, original_cfg)

    # Nothing was written.
    items_response = await sysadmin_in_process_client.get(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items"
    )
    assert items_response.status_code == 200
    assert len(items_response.json().get("features", [])) == 0


@pytest.mark.asyncio
@pytest.mark.enable_extensions("features", "assets", "stac")
async def test_single_item_creation_with_extra_fields(sysadmin_in_process_client: AsyncClient, test_data_loader):
    catalog_id = f"c_{generate_test_id()}"
    collection_id = "test_fix_collection"
    
    catalog_data = test_data_loader("catalog_fix.json")
    catalog_data["id"] = catalog_id
    catalog_resp = await sysadmin_in_process_client.post(
        f"/features/catalogs", 
        json=catalog_data,
        timeout=60.0
    )
    assert catalog_resp.status_code == 201, f"Failed to create catalog: {catalog_resp.text}"
 
    collection_data = test_data_loader("collection_fix.json")
    collection_data["id"] = collection_id
    collection_resp = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections", 
        json=collection_data,
        timeout=60.0
    )
    assert collection_resp.status_code == 201, f"Failed to create collection: {collection_resp.text}"
    
    single_payload = test_data_loader("single_item_extra.json")
    
    response = await sysadmin_in_process_client.post(f"/features/catalogs/{catalog_id}/collections/{collection_id}/items", json=single_payload, timeout=60.0)
    assert response.status_code == 201
    assert "Location" in response.headers

@pytest.mark.asyncio
@pytest.mark.enable_extensions("features", "assets", "stac")
async def test_filter_with_unknown_property(sysadmin_in_process_client: AsyncClient, test_data_loader):
    catalog_id = f"c_{generate_test_id()}"
    collection_id = "test_filter_collection"
    
    catalog_data = test_data_loader("catalog_filter.json")
    catalog_data["id"] = catalog_id
    catalog_resp = await sysadmin_in_process_client.post(
        f"/features/catalogs", 
        json=catalog_data,
        timeout=60.0
    )
    assert catalog_resp.status_code == 201, f"Failed to create catalog: {catalog_resp.text}"
    collection_data = test_data_loader("collection_filter.json")
    collection_data["id"] = collection_id
    collection_resp = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections", 
        json=collection_data,
        timeout=60.0
    )
    assert collection_resp.status_code == 201, f"Failed to create collection: {collection_resp.text}"
    
    # Add a feature first so the collection table exists
    item_payload = test_data_loader("item_test.json")
    item_resp = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=item_payload,
        timeout=30.0
    )
    assert item_resp.status_code == 201, f"Failed to create item: {item_resp.text}"
    
    # Request with unknown property in CQL2 filter
    # Should return 400 Bad Request with error about unknown property
    response = await sysadmin_in_process_client.get(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items",
        params={"filter": "unknown_prop='value'"}
    )
    
    assert response.status_code == 400
    error_detail = response.json()["detail"]
    assert "Unknown property" in error_detail or "unknown" in error_detail.lower()
