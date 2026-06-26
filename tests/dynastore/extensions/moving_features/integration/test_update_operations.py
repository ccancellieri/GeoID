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


@pytest.mark.asyncio
@pytest.mark.enable_extensions("moving_features", "stac")
async def test_update_moving_feature_properties(
    sysadmin_in_process_client,
    in_process_client,
    setup_catalog,
    setup_collection,
):
    """Test PUT endpoint updates moving feature properties and updated_at timestamp."""
    catalog_id = setup_catalog
    collection_id = setup_collection
    
    # Create a moving feature
    mf_data = {
        "feature_type": "Feature",
        "properties": {"vehicle_type": "car", "speed_avg": 60.0},
    }
    r = await sysadmin_in_process_client.post(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=mf_data,
    )
    assert r.status_code == 201
    mf_id = r.json()["id"]
    created_at = r.json()["created_at"]
    
    # Update the moving feature
    update_data = {
        "properties": {"vehicle_type": "truck", "speed_avg": 50.5, "new_prop": "value"},
    }
    r = await sysadmin_in_process_client.put(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items/{mf_id}",
        json=update_data,
    )
    assert r.status_code == 200
    updated = r.json()
    
    # Verify properties updated
    assert updated["properties"]["vehicle_type"] == "truck"
    assert updated["properties"]["speed_avg"] == 50.5
    assert updated["properties"]["new_prop"] == "value"
    
    # Verify updated_at changed
    assert updated["updated_at"] != created_at
    
    # Verify changes persisted
    r = await in_process_client.get(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items/{mf_id}",
    )
    assert r.status_code == 200
    fetched = r.json()
    assert fetched["properties"]["vehicle_type"] == "truck"


@pytest.mark.asyncio
@pytest.mark.enable_extensions("moving_features", "stac")
async def test_update_moving_feature_not_found(
    sysadmin_in_process_client,
    setup_catalog,
    setup_collection,
):
    """Test PUT returns 404 for non-existent moving feature."""
    catalog_id = setup_catalog
    collection_id = setup_collection
    
    import uuid
    fake_id = str(uuid.uuid4())
    
    update_data = {"properties": {"test": "value"}}
    r = await sysadmin_in_process_client.put(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items/{fake_id}",
        json=update_data,
    )
    assert r.status_code == 404


@pytest.mark.asyncio
@pytest.mark.enable_extensions("moving_features", "stac")
async def test_update_moving_feature_wrong_collection(
    sysadmin_in_process_client,
    setup_catalog,
    setup_collection,
):
    """Test PUT returns 404 when mf_id belongs to different collection."""
    catalog_id = setup_catalog
    collection_id = setup_collection
    
    # Create a moving feature
    mf_data = {"feature_type": "Feature", "properties": {"name": "test"}}
    r = await sysadmin_in_process_client.post(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=mf_data,
    )
    assert r.status_code == 201
    mf_id = r.json()["id"]
    
    # Try to update with wrong collection
    wrong_collection = "wrong_collection"
    update_data = {"properties": {"test": "value"}}
    r = await sysadmin_in_process_client.put(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{wrong_collection}/items/{mf_id}",
        json=update_data,
    )
    assert r.status_code == 404


@pytest.mark.asyncio
@pytest.mark.enable_extensions("moving_features", "stac")
async def test_patch_temporal_geometry(
    sysadmin_in_process_client,
    in_process_client,
    setup_catalog,
    setup_collection,
):
    """Test PATCH endpoint updates temporal geometry sequence."""
    catalog_id = setup_catalog
    collection_id = setup_collection
    
    # Create a moving feature
    mf_data = {
        "feature_type": "Feature",
        "properties": {"name": "test_vehicle"},
    }
    r = await sysadmin_in_process_client.post(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=mf_data,
    )
    assert r.status_code == 201
    mf_id = r.json()["id"]
    
    # Add temporal geometry
    tg_data = {
        "datetimes": [
            "2024-01-01T10:00:00Z",
            "2024-01-01T11:00:00Z",
        ],
        "coordinates": [
            [10.0, 45.0],
            [11.0, 46.0],
        ],
        "interpolation": "Linear",
    }
    r = await sysadmin_in_process_client.post(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items/{mf_id}/tgsequence",
        json=tg_data,
    )
    assert r.status_code == 201
    tg_id = r.json()["id"]
    
    # Patch temporal geometry (update coordinates only)
    patch_data = {
        "coordinates": [
            [12.0, 47.0],
            [13.0, 48.0],
        ],
    }
    r = await sysadmin_in_process_client.patch(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items/{mf_id}/tgsequence/{tg_id}",
        json=patch_data,
    )
    assert r.status_code == 200
    updated = r.json()
    
    # Verify coordinates updated
    assert updated["coordinates"] == [[12.0, 47.0], [13.0, 48.0]]
    
    # Verify datetimes unchanged
    assert len(updated["datetimes"]) == 2
    
    # Verify changes persisted
    r = await in_process_client.get(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items/{mf_id}/tgsequence",
    )
    assert r.status_code == 200
    sequences = r.json()
    assert len(sequences) == 1
    assert sequences[0]["coordinates"] == [[12.0, 47.0], [13.0, 48.0]]


@pytest.mark.asyncio
@pytest.mark.enable_extensions("moving_features", "stac")
async def test_patch_temporal_geometry_both_datetimes_and_coordinates(
    sysadmin_in_process_client,
    setup_catalog,
    setup_collection,
):
    """Test PATCH with both datetimes and coordinates validates length match."""
    catalog_id = setup_catalog
    collection_id = setup_collection
    
    # Create a moving feature
    mf_data = {"feature_type": "Feature", "properties": {"name": "test"}}
    r = await sysadmin_in_process_client.post(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=mf_data,
    )
    assert r.status_code == 201
    mf_id = r.json()["id"]
    
    # Add temporal geometry
    tg_data = {
        "datetimes": ["2024-01-01T10:00:00Z", "2024-01-01T11:00:00Z"],
        "coordinates": [[10.0, 45.0], [11.0, 46.0]],
        "interpolation": "Linear",
    }
    r = await sysadmin_in_process_client.post(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items/{mf_id}/tgsequence",
        json=tg_data,
    )
    assert r.status_code == 201
    tg_id = r.json()["id"]
    
    # Patch with mismatched lengths (should fail)
    patch_data = {
        "datetimes": ["2024-01-01T10:00:00Z", "2024-01-01T11:00:00Z", "2024-01-01T12:00:00Z"],
        "coordinates": [[10.0, 45.0], [11.0, 46.0]],
    }
    r = await sysadmin_in_process_client.patch(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items/{mf_id}/tgsequence/{tg_id}",
        json=patch_data,
    )
    assert r.status_code == 400
    assert "must match" in r.json()["detail"]


@pytest.mark.asyncio
@pytest.mark.enable_extensions("moving_features", "stac")
async def test_patch_temporal_geometry_not_found(
    sysadmin_in_process_client,
    setup_catalog,
    setup_collection,
):
    """Test PATCH returns 404 for non-existent temporal geometry."""
    catalog_id = setup_catalog
    collection_id = setup_collection
    
    # Create a moving feature
    mf_data = {"feature_type": "Feature", "properties": {"name": "test"}}
    r = await sysadmin_in_process_client.post(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=mf_data,
    )
    assert r.status_code == 201
    mf_id = r.json()["id"]
    
    import uuid
    fake_tg_id = str(uuid.uuid4())
    
    patch_data = {"coordinates": [[10.0, 45.0]]}
    r = await sysadmin_in_process_client.patch(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items/{mf_id}/tgsequence/{fake_tg_id}",
        json=patch_data,
    )
    assert r.status_code == 404


@pytest.mark.asyncio
@pytest.mark.enable_extensions("moving_features", "stac")
async def test_patch_temporal_geometry_wrong_mf(
    sysadmin_in_process_client,
    setup_catalog,
    setup_collection,
):
    """Test PATCH returns 404 when tg_id belongs to different moving feature."""
    catalog_id = setup_catalog
    collection_id = setup_collection
    
    # Create two moving features
    mf_data1 = {"feature_type": "Feature", "properties": {"name": "mf1"}}
    r = await sysadmin_in_process_client.post(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=mf_data1,
    )
    assert r.status_code == 201
    mf_id1 = r.json()["id"]
    
    mf_data2 = {"feature_type": "Feature", "properties": {"name": "mf2"}}
    r = await sysadmin_in_process_client.post(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=mf_data2,
    )
    assert r.status_code == 201
    mf_id2 = r.json()["id"]
    
    # Add temporal geometry to mf1
    tg_data = {
        "datetimes": ["2024-01-01T10:00:00Z"],
        "coordinates": [[10.0, 45.0]],
        "interpolation": "Linear",
    }
    r = await sysadmin_in_process_client.post(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items/{mf_id1}/tgsequence",
        json=tg_data,
    )
    assert r.status_code == 201
    tg_id = r.json()["id"]
    
    # Try to patch using mf_id2 (should fail)
    patch_data = {"coordinates": [[12.0, 47.0]]}
    r = await sysadmin_in_process_client.patch(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items/{mf_id2}/tgsequence/{tg_id}",
        json=patch_data,
    )
    assert r.status_code == 404


@pytest.mark.asyncio
@pytest.mark.enable_extensions("moving_features", "stac")
async def test_patch_temporal_geometry_properties(
    sysadmin_in_process_client,
    setup_catalog,
    setup_collection,
):
    """Test PATCH can update properties field."""
    catalog_id = setup_catalog
    collection_id = setup_collection
    
    # Create a moving feature with temporal geometry
    mf_data = {"feature_type": "Feature", "properties": {"name": "test"}}
    r = await sysadmin_in_process_client.post(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=mf_data,
    )
    assert r.status_code == 201
    mf_id = r.json()["id"]
    
    tg_data = {
        "datetimes": ["2024-01-01T10:00:00Z"],
        "coordinates": [[10.0, 45.0]],
        "interpolation": "Linear",
        "properties": {"speed": 50.0},
    }
    r = await sysadmin_in_process_client.post(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items/{mf_id}/tgsequence",
        json=tg_data,
    )
    assert r.status_code == 201
    tg_id = r.json()["id"]
    
    # Patch properties
    patch_data = {"properties": {"speed": 60.0, "heading": 90}}
    r = await sysadmin_in_process_client.patch(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items/{mf_id}/tgsequence/{tg_id}",
        json=patch_data,
    )
    assert r.status_code == 200
    updated = r.json()
    assert updated["properties"]["speed"] == 60.0
    assert updated["properties"]["heading"] == 90


@pytest.mark.asyncio
@pytest.mark.enable_extensions("moving_features", "stac")
async def test_patch_temporal_geometry_interpolation(
    sysadmin_in_process_client,
    setup_catalog,
    setup_collection,
):
    """Test PATCH can update interpolation method."""
    catalog_id = setup_catalog
    collection_id = setup_collection
    
    # Create a moving feature with temporal geometry
    mf_data = {"feature_type": "Feature", "properties": {"name": "test"}}
    r = await sysadmin_in_process_client.post(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=mf_data,
    )
    assert r.status_code == 201
    mf_id = r.json()["id"]
    
    tg_data = {
        "datetimes": ["2024-01-01T10:00:00Z", "2024-01-01T11:00:00Z"],
        "coordinates": [[10.0, 45.0], [11.0, 46.0]],
        "interpolation": "Linear",
    }
    r = await sysadmin_in_process_client.post(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items/{mf_id}/tgsequence",
        json=tg_data,
    )
    assert r.status_code == 201
    tg_id = r.json()["id"]
    
    # Patch interpolation
    patch_data = {"interpolation": "Step"}
    r = await sysadmin_in_process_client.patch(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items/{mf_id}/tgsequence/{tg_id}",
        json=patch_data,
    )
    assert r.status_code == 200
    updated = r.json()
    assert updated["interpolation"] == "Step"
