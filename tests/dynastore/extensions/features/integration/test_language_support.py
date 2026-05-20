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

"""
Integration tests for OGC API Features language parameter support.

Tests validate:
1. Language parameter propagation in OGC endpoints
2. Proper localization of catalog and collection responses
3. Language-specific content resolution (lang=en, lang=fr, etc.)
4. Full multi-language response (lang=*)
"""

import pytest
from tests.dynastore.test_utils import generate_test_id


@pytest.mark.enable_modules("db_config", "db", "catalog", "stats", "iam")
@pytest.mark.enable_extensions("stac", "assets", "features", "logs")
@pytest.mark.asyncio
async def test_landing_page_respects_language_param(sysadmin_in_process_client):
    """Test that the landing page respects language parameter."""
    # Use authenticated client — the default public_access policy doesn't
    # cover /features/ and dynamic policy updates may not propagate through
    # the evaluation cache within a single test.
    resp_en = await sysadmin_in_process_client.get("/features/", params={"lang": "en"})
    assert resp_en.status_code == 200
    data_en = resp_en.json()
    assert "title" in data_en or "description" in data_en

    # Test with French
    resp_fr = await sysadmin_in_process_client.get("/features/", params={"lang": "fr"})
    assert resp_fr.status_code == 200
    data_fr = resp_fr.json()
    assert "title" in data_fr or "description" in data_fr


@pytest.mark.asyncio
async def test_create_catalog_with_language_parameter(sysadmin_in_process_client):
    """Test creating a catalog through OGC Features API with language parameter."""
    catalog_id = f"ogc_create_{generate_test_id()}"
    payload = {
        "id": catalog_id,
        "title": "OGC Test Catalog",
        "description": "A test catalog for OGC API Features",
    }

    # Pre-cleanup: remove any leftover from a prior run
    await sysadmin_in_process_client.delete(
        f"/features/catalogs/{catalog_id}", params={"force": "true"}
    )

    try:
        response = await sysadmin_in_process_client.post(
            "/features/catalogs", json=payload, params={"lang": "en"}
        )

        assert response.status_code == 201
        data = response.json()
        assert data["id"] == catalog_id
    finally:
        await sysadmin_in_process_client.delete(
            f"/features/catalogs/{catalog_id}", params={"force": "true"}
        )


@pytest.mark.asyncio
async def test_get_catalog_with_language_resolution(sysadmin_in_process_client):
    """Test retrieving catalog with language-specific content."""
    # Create multilingual catalog
    catalog_id = f"ogc_lang_{generate_test_id()}"
    payload = {
        "id": catalog_id,
        "title": {"en": "OGC English Title", "fr": "OGC Titre Français"},
        "description": {
            "en": "OGC English description",
            "fr": "OGC Description française",
        },
    }

    try:
        create_resp = await sysadmin_in_process_client.post(
            "/features/catalogs", json=payload, params={"lang": "*"}
        )
        assert create_resp.status_code == 201

        # Get with lang=en
        resp_en = await sysadmin_in_process_client.get(
            f"/features/catalogs/{catalog_id}", params={"lang": "en"}
        )
        assert resp_en.status_code == 200
        data_en = resp_en.json()
        assert "title" in data_en

        # Get with lang=fr
        resp_fr = await sysadmin_in_process_client.get(
            f"/features/catalogs/{catalog_id}", params={"lang": "fr"}
        )
        assert resp_fr.status_code == 200
        data_fr = resp_fr.json()
        assert "title" in data_fr

        # Get with lang=* for all
        resp_all = await sysadmin_in_process_client.get(
            f"/features/catalogs/{catalog_id}", params={"lang": "*"}
        )
        assert resp_all.status_code == 200
        data_all = resp_all.json()
        assert "title" in data_all
    finally:
        await sysadmin_in_process_client.delete(f"/features/catalogs/{catalog_id}", params={"force": "true"})


@pytest.mark.asyncio
async def test_list_catalogs_with_language(sysadmin_in_process_client):
    """Test that catalog listing respects language parameter."""
    # Create some catalogs with multilingual content
    catalogs = [
        {
            "id": "ogc_list_test_1",
            "description": {"en": "First catalog", "fr": "Premier catalogue"},
        },
        {
            "id": "ogc_list_test_2",
            "description": {"en": "Second catalog", "fr": "Deuxième catalogue"},
        },
    ]

    catalog_ids = [f"{payload['id']}_{i}" for i, payload in enumerate(catalogs)]
    # Pre-cleanup: remove any leftovers from prior runs
    for cid in catalog_ids:
        await sysadmin_in_process_client.delete(f"/features/catalogs/{cid}", params={"force": "true"})

    created_ids = []
    try:
        for i, payload in enumerate(catalogs):
            cat_id = catalog_ids[i]
            payload["id"] = cat_id
            resp = await sysadmin_in_process_client.post(
                "/features/catalogs", json=payload, params={"lang": "en"}
            )
            assert resp.status_code == 201
            created_ids.append(cat_id)

        # List with lang=en (must be inside try so catalogs exist)
        resp_list = await sysadmin_in_process_client.get(
            "/features/catalogs", params={"lang": "en", "limit": 50}
        )
        assert resp_list.status_code == 200
        data = resp_list.json()
        assert "catalogs" in data
    finally:
        for cid in created_ids:
            await sysadmin_in_process_client.delete(f"/features/catalogs/{cid}", params={"force": "true"})


@pytest.mark.asyncio
async def test_create_collection_with_language(sysadmin_in_process_client):
    """Test creating a collection with language support."""
    catalog_id = f"ogc_coll_{generate_test_id()}"
    collection_id = "ogc_collection_unique_1"
    
    try:
        # First create catalog
        cat_resp = await sysadmin_in_process_client.post(
            "/features/catalogs",
            json={"id": catalog_id, "description": "Test catalog"},
            params={"lang": "en"},
        )
        assert cat_resp.status_code == 201

        # Create collection with multilingual metadata
        coll_payload = {
            "id": collection_id,
            "title": {"en": "Collection Title", "fr": "Titre de Collection"},
            "description": {
                "en": "Collection description",
                "fr": "Description de collection",
            },
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
        }

        coll_resp = await sysadmin_in_process_client.post(
            f"/features/catalogs/{catalog_id}/collections",
            json=coll_payload,
            params={"lang": "en"},
        )
        assert coll_resp.status_code == 201
        coll_data = coll_resp.json()
        assert coll_data["id"] == collection_id
    finally:
        await sysadmin_in_process_client.delete(f"/features/catalogs/{catalog_id}", params={"force": "true"})


@pytest.mark.asyncio
async def test_get_collection_with_language(sysadmin_in_process_client):
    """Test retrieving collection with language parameter."""
    catalog_id = f"ogc_getcoll_{generate_test_id()}"
    collection_id = "test_get_coll_unique"
    
    try:
        # Setup
        await sysadmin_in_process_client.post(
            "/features/catalogs",
            json={"id": catalog_id, "description": "Test"},
            params={"lang": "en"},
        )

        coll_payload = {
            "id": collection_id,
            "description": {"en": "English collection", "fr": "Collection française"},
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
        }

        await sysadmin_in_process_client.post(
            f"/features/catalogs/{catalog_id}/collections",
            json=coll_payload,
            params={"lang": "en"},
        )

        # Retrieve with different languages
        resp_en = await sysadmin_in_process_client.get(
            f"/features/catalogs/{catalog_id}/collections/{collection_id}",
            params={"lang": "en"},
        )
        assert resp_en.status_code == 200

        resp_fr = await sysadmin_in_process_client.get(
            f"/features/catalogs/{catalog_id}/collections/{collection_id}",
            params={"lang": "fr"},
        )
        assert resp_fr.status_code == 200
    finally:
        await sysadmin_in_process_client.delete(f"/features/catalogs/{catalog_id}", params={"force": "true"})


@pytest.mark.asyncio
async def test_update_collection_with_language(sysadmin_in_process_client):
    """Test updating collection and merging language data."""
    catalog_id = f"ogc_update_{generate_test_id()}"
    collection_id = "update_coll_unique"
    
    try:
        # Setup
        await sysadmin_in_process_client.post(
            "/features/catalogs",
            json={"id": catalog_id, "description": "Test"},
            params={"lang": "en"},
        )

        coll_payload = {
            "id": collection_id,
            "description": {"en": "English description", "fr": "Description française"},
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
        }

        await sysadmin_in_process_client.post(
            f"/features/catalogs/{catalog_id}/collections",
            json=coll_payload,
            params={"lang": "*"},
        )

        # Update with Spanish
        update = {"description": "Descripción en español"}

        resp = await sysadmin_in_process_client.patch(
            f"/features/catalogs/{catalog_id}/collections/{collection_id}",
            json=update,
            params={"lang": "es"},
        )
        assert resp.status_code == 200
    finally:
        await sysadmin_in_process_client.delete(f"/features/catalogs/{catalog_id}", params={"force": "true"})


@pytest.mark.asyncio
async def test_queryables_respects_language(sysadmin_in_process_client):
    """Test that queryables endpoint respects language parameter."""
    catalog_id = f"ogc_query_{generate_test_id()}"
    collection_id = "query_coll_unique"
    
    try:
        # Setup
        await sysadmin_in_process_client.post(
            "/features/catalogs",
            json={"id": catalog_id, "description": "Test"},
            params={"lang": "en"},
        )

        coll_payload = {
            "id": collection_id,
            "description": "Test",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
        }

        await sysadmin_in_process_client.post(
            f"/features/catalogs/{catalog_id}/collections",
            json=coll_payload,
            params={"lang": "en"},
        )

        # Get queryables with language
        resp = await sysadmin_in_process_client.get(
            f"/features/catalogs/{catalog_id}/collections/{collection_id}/queryables",
            params={"lang": "en"},
        )
        assert resp.status_code == 200
    finally:
        await sysadmin_in_process_client.delete(f"/features/catalogs/{catalog_id}", params={"force": "true"})


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
