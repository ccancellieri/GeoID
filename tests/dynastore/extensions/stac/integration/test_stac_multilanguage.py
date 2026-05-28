import pytest


@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features", "logs")
async def test_create_catalog_with_language(sysadmin_in_process_client, catalog_id):
    """Test creating a catalog with language-specific input."""
    # Create with English
    payload_en = {
        "id": catalog_id,
        "title": "English Title",
        "description": "English Description",
        "license": "MIT"
    }

    r = await sysadmin_in_process_client.post("/stac/catalogs?lang=en", json=payload_en)
    assert r.status_code == 201
    data = r.json()
    assert data["id"] == catalog_id
    assert data["title"] == "English Title"
    assert data["description"] == "English Description"


@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features", "logs")
async def test_add_french_translation_to_catalog(sysadmin_in_process_client, in_process_client, catalog_id):
    """Test adding a French translation to an existing catalog."""
    # Create with English
    payload_en = {
        "id": catalog_id,
        "title": "English Title",
        "description": "English Description",
        "license": "MIT"
    }
    r = await sysadmin_in_process_client.post("/stac/catalogs?lang=en", json=payload_en)
    assert r.status_code == 201
    
    # Add the French translation while preserving English. Layering a single
    # language onto an existing catalog is PATCH (merge) semantics; PUT would
    # replace the whole resource and drop the English variant.
    payload_fr = {
        "title": "Titre Français",
        "description": "Description Française",
        "license": "MIT"
    }
    r = await sysadmin_in_process_client.patch(f"/stac/catalogs/{catalog_id}?lang=fr", json=payload_fr)
    assert r.status_code == 200
    data = r.json()
    assert data["title"] == "Titre Français"  # Returns French when lang=fr
    
    # Verify English is still there
    r = await in_process_client.get(f"/stac/catalogs/{catalog_id}?lang=en")
    assert r.status_code == 200
    data = r.json()
    assert data["title"] == "English Title"
    
    # Verify French is there
    r = await in_process_client.get(f"/stac/catalogs/{catalog_id}?lang=fr")
    assert r.status_code == 200
    data = r.json()
    assert data["title"] == "Titre Français"

@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features", "logs")
async def test_create_collection_with_language(sysadmin_in_process_client, in_process_client, setup_catalog, collection_id):
    """Test creating a collection with language-specific input."""
    catalog_id = setup_catalog
    
    payload = {
        "id": collection_id,
        "title": "Italian Collection",
        "description": "Collezione Italiana",
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [[None, None]]}
        }
    }
    
    r = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections?lang=it", 
        json=payload
    )
    assert r.status_code == 201
    data = r.json()
    assert data["id"] == collection_id
    assert data["title"] == "Italian Collection"
    assert data["description"] == "Collezione Italiana"


@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features", "logs")
async def test_multilingual_collection_update(sysadmin_in_process_client, in_process_client, setup_catalog, collection_id):
    """Test updating a collection with multiple languages."""
    catalog_id = setup_catalog
    
    # Create with English
    payload_en = {
        "id": collection_id,
        "title": "English Collection",
        "description": "English Description",
        "license": "MIT",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [[None, None]]}
        }
    }
    r = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections?lang=en",
        json=payload_en
    )
    assert r.status_code == 201
    
    # Add Spanish translation. This layers a new language onto the existing
    # collection while preserving English, which is PATCH (merge) semantics;
    # PUT replaces the whole resource and would drop the English variant.
    payload_es = {
        "title": "Colección Española",
        "description": "Descripción Española",
        "license": "MIT"
    }
    r = await sysadmin_in_process_client.patch(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}?lang=es",
        json=payload_es
    )
    assert r.status_code == 200
    
    # Verify both languages exist
    r = await in_process_client.get(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}?lang=en"
    )
    assert r.status_code == 200
    assert r.json()["title"] == "English Collection"
    
    r = await in_process_client.get(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}?lang=es"
    )
    assert r.status_code == 200
    assert r.json()["title"] == "Colección Española"


@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features", "logs")
async def test_language_conflict_detection(sysadmin_in_process_client, catalog_id):
    """Test that conflicting language parameters return an error."""
    payload = {
        "id": catalog_id,
        "title": "Test",
        "description": "Test",
        "license": "MIT"
    }

    # Send conflicting lang param and Accept-Language header
    r = await sysadmin_in_process_client.post(
        "/stac/catalogs?lang=es",
        json=payload,
        headers={"Accept-Language": "fr-FR"}
    )

    assert r.status_code == 400
    assert "conflict" in r.json()["detail"].lower()


@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features", "logs")
async def test_accept_language_header_only(sysadmin_in_process_client, catalog_id):
    """Test using Accept-Language header without query param."""
    payload = {
        "id": catalog_id,
        "title": "German Title",
        "description": "German Description",
        "license": "MIT"
    }

    r = await sysadmin_in_process_client.post(
        "/stac/catalogs",
        json=payload,
        headers={"Accept-Language": "de-DE,de;q=0.9"}
    )

    assert r.status_code == 201

    # Retrieve with German header (authenticated — DENY-by-default baseline)
    r = await sysadmin_in_process_client.get(
        f"/stac/catalogs/{catalog_id}",
        headers={"Accept-Language": "de-DE"}
    )
    assert r.status_code == 200
    assert r.json()["title"] == "German Title"


@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features", "logs")
async def test_keywords_localization(sysadmin_in_process_client, in_process_client, catalog_id, collection_id):
    """Test that keywords are properly localized."""
    payload = {
        "id": catalog_id,
        "title": "Test Catalog",
        "description": "Test",
        "license": "MIT"
    }
    from dynastore.extensions.stac.stac_models import STACCatalogRequest
    STACCatalogRequest.model_validate(payload)

    r = await sysadmin_in_process_client.post("/stac/catalogs?lang=en", json=payload)
    assert r.status_code == 201

    # Add English collection
    payload_en = {
        "id": collection_id,
        "title": "English Collection",
        "description": "English Description",
        "keywords": ["geospatial", "data", "test"],
        "license": "MIT",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [[None, None]]}
        }
    }

    r = await sysadmin_in_process_client.post(f"/stac/catalogs/{catalog_id}/collections?lang=en", json=payload_en)
    assert r.status_code == 201
    assert r.json()["title"] == "English Collection"
    
    # Add French keywords. Layering a single language onto an existing
    # collection is PATCH semantics (partial body, lang-scoped); PUT would
    # require the full collection body (including ``extent``) and replace it.
    payload_fr = {
        "title": "French Collection",
        "description": "French Description",
        "keywords": ["géospatial", "données", "test"],
        "license": "MIT"
    }

    r = await sysadmin_in_process_client.patch(f"/stac/catalogs/{catalog_id}/collections/{collection_id}?lang=fr", json=payload_fr)
    assert r.status_code == 200
    
    # Verify English keywords
    r = await in_process_client.get(f"/stac/catalogs/{catalog_id}/collections/{collection_id}?lang=en")
    assert r.status_code == 200
    assert r.json()["keywords"] == ["geospatial", "data", "test"]
    
    # Verify French keywords
    r = await in_process_client.get(f"/stac/catalogs/{catalog_id}/collections/{collection_id}?lang=fr")
    assert r.status_code == 200
    assert r.json()["keywords"] == ["géospatial", "données", "test"]
