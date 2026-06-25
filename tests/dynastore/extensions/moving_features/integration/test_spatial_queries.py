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
async def test_spatial_bbox_filter(
    sysadmin_in_process_client,
    in_process_client,
    setup_catalog,
    setup_collection,
):
    """Test bbox filtering for moving features."""
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
    
    # Add temporal geometry with trajectory
    tg_data = {
        "datetimes": [
            "2024-01-01T10:00:00Z",
            "2024-01-01T11:00:00Z",
            "2024-01-01T12:00:00Z",
        ],
        "coordinates": [
            [10.0, 45.0],  # Inside bbox
            [11.0, 46.0],  # Inside bbox
            [20.0, 55.0],  # Outside bbox
        ],
        "interpolation": "Linear",
    }
    r = await sysadmin_in_process_client.post(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items/{mf_id}/tgsequence",
        json=tg_data,
    )
    assert r.status_code == 201
    
    # Create another moving feature outside the bbox
    mf_data2 = {
        "feature_type": "Feature",
        "properties": {"name": "test_vehicle_2"},
    }
    r = await sysadmin_in_process_client.post(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=mf_data2,
    )
    assert r.status_code == 201
    mf_id2 = r.json()["id"]
    
    tg_data2 = {
        "datetimes": [
            "2024-01-01T10:00:00Z",
            "2024-01-01T11:00:00Z",
        ],
        "coordinates": [
            [30.0, 65.0],  # Outside bbox
            [31.0, 66.0],  # Outside bbox
        ],
        "interpolation": "Linear",
    }
    r = await sysadmin_in_process_client.post(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items/{mf_id2}/tgsequence",
        json=tg_data2,
    )
    assert r.status_code == 201
    
    # Test bbox filter (should return only first feature)
    r = await in_process_client.get(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items",
        params={"bbox": "9,44,12,47"},
    )
    assert r.status_code == 200
    items = r.json()
    assert len(items) == 1
    assert items[0]["id"] == mf_id


@pytest.mark.asyncio
@pytest.mark.enable_extensions("moving_features", "stac")
async def test_spatial_intersects_filter(
    sysadmin_in_process_client,
    in_process_client,
    setup_catalog,
    setup_collection,
):
    """Test geometry intersection filter for moving features."""
    catalog_id = setup_catalog
    collection_id = setup_collection
    
    # Create a moving feature
    mf_data = {
        "feature_type": "Feature",
        "properties": {"name": "test_ship"},
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
    
    # Test intersects filter with polygon
    intersects_wkt = "POLYGON((9 44, 9 47, 12 47, 12 44, 9 44))"
    r = await in_process_client.get(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items",
        params={"intersects": intersects_wkt},
    )
    assert r.status_code == 200
    items = r.json()
    assert len(items) == 1
    assert items[0]["id"] == mf_id
    
    # Test with non-intersecting geometry
    non_intersect_wkt = "POLYGON((20 50, 20 52, 22 52, 22 50, 20 50))"
    r = await in_process_client.get(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items",
        params={"intersects": non_intersect_wkt},
    )
    assert r.status_code == 200
    items = r.json()
    assert len(items) == 0


@pytest.mark.asyncio
@pytest.mark.enable_extensions("moving_features", "stac")
async def test_bbox_and_intersects_mutually_exclusive(
    in_process_client,
    setup_catalog,
    setup_collection,
):
    """Test that bbox and intersects cannot both be specified."""
    catalog_id = setup_catalog
    collection_id = setup_collection
    
    r = await in_process_client.get(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items",
        params={"bbox": "10,45,11,46", "intersects": "POINT(10 45)"},
    )
    assert r.status_code == 400
    assert "Only one of" in r.json()["detail"]


@pytest.mark.asyncio
@pytest.mark.enable_extensions("moving_features", "stac")
async def test_invalid_bbox_format(
    in_process_client,
    setup_catalog,
    setup_collection,
):
    """Test that invalid bbox format returns 400."""
    catalog_id = setup_catalog
    collection_id = setup_collection
    
    r = await in_process_client.get(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items",
        params={"bbox": "10,45,11"},  # Only 3 values
    )
    assert r.status_code == 400


@pytest.mark.asyncio
@pytest.mark.enable_extensions("moving_features", "stac")
async def test_invalid_intersects_wkt(
    in_process_client,
    setup_catalog,
    setup_collection,
):
    """Test that invalid WKT geometry returns 400."""
    catalog_id = setup_catalog
    collection_id = setup_collection
    
    r = await in_process_client.get(
        f"/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items",
        params={"intersects": "NOT A VALID WKT"},
    )
    assert r.status_code == 400
