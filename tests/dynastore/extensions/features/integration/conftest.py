import pytest
from dynastore.tools.identifiers import generate_geoid, generate_id_hex

# --- Test Data Fixtures ---


@pytest.fixture
def catalog_id():
    """Generate a unique catalog ID for testing."""
    return f"cat_{generate_id_hex()}"


@pytest.fixture
def collection_id():
    """Generate a unique collection ID for testing."""
    return f"coll_{generate_id_hex()}"


@pytest.fixture
def item_id():
    """Generate a unique item ID for testing."""
    return generate_geoid()


@pytest.fixture
def catalog_data(catalog_id):
    """Fixture providing test data for catalog creation."""
    return {
        "id": catalog_id,
        "title": "Test Catalog",
        "description": "A test catalog for OGC API Features",
    }


@pytest.fixture
def collection_data(collection_id):
    """Fixture providing test data for collection creation."""
    return {
        "id": collection_id,
        "description": "Test Collection",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [[None, None]]},
        },
    }


@pytest.fixture
def item_raw_data(item_id):
    """Fixture providing test GeoJSON feature data."""
    return {
        "type": "Feature",
        "id": item_id,
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "bbox": [0, 0, 0, 0],
        "properties": {"name": "Test Item"},
    }


@pytest.fixture
def config_catalog_data():
    """Fixture for collection plugin config."""
    return {"geometry_storage": {"target_srid": 4326, "geometry_column": "geom"}}


# --- Setup/Cleanup Fixtures (Features Extension Level) ---


@pytest.fixture
async def setup_catalog(sysadmin_in_process_client, catalog_data, catalog_id):
    """Fixture to ensure a catalog exists and is cleaned up using the API."""
    # Cleanup any existing catalog with hard delete to avoid duplicate key constraints
    await sysadmin_in_process_client.delete(f"/features/catalogs/{catalog_id}?force=true")

    # Create catalog
    r = await sysadmin_in_process_client.post("/features/catalogs", json=catalog_data)
    if r.status_code != 201:
        # Check if it already exists (409 maybe? or just proceed)
        pass

    yield catalog_id

    # Cleanup with hard delete handled by session fixture
    # await in_process_client.delete(f"/features/catalogs/{catalog_id}?force=true")


@pytest.fixture
async def setup_collection(
    sysadmin_in_process_client, setup_catalog, collection_data, collection_id
):
    """Fixture to ensure a collection exists and is cleaned up using the API."""
    catalog_id = setup_catalog

    # Cleanup with hard delete to avoid duplicate key constraints
    await sysadmin_in_process_client.delete(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}?force=true"
    )

    # Create collection
    r = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections", json=collection_data
    )

    yield collection_id

    # Cleanup with hard delete handled by session fixture
    # await in_process_client.delete(
    #     f"/features/catalogs/{catalog_id}/collections/{collection_id}?force=true"
    # )
