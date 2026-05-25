import asyncio

import pytest
from dynastore.tools.identifiers import generate_id_hex


@pytest.fixture
def catalog_id(data_id):
    # Use unique ID per test to avoid partition overlapping issues during rapid create/delete
    return f"cat_{generate_id_hex()}"


@pytest.fixture
def collection_id():
    return "region"


@pytest.fixture
def catalog_data(catalog_id):
    return {
        "id": catalog_id,
        "title": f"Title for {catalog_id}",
        "description": "Test Catalog for WFS",
    }


@pytest.fixture
def collection_data(collection_id):
    return {
        "id": collection_id,
        "title": f"Title for {collection_id}",
        "description": "Test Collection for WFS",
        "extent": {
            "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
            "temporal": {"interval": [[None, None]]},
        },
        "links": [],
    }


@pytest.fixture
def config_data():
    # Phase 1.6: ``collection_type`` was hoisted off the PG driver config
    # into a standalone ``CollectionInfo`` PluginConfig at collection scope.
    # The PG driver config now ONLY carries driver-local concerns
    # (sidecars, partitioning, etc.).
    return {
        "sidecars": [
            {
                "sidecar_type": "geometries",
                "target_srid": 4326,
            },
            {
                "sidecar_type": "attributes",
            },
        ],
    }


@pytest.fixture
async def setup_catalog(in_process_client_module, catalog_data, catalog_id):
    r = await in_process_client_module.post("/features/catalogs", json=catalog_data)
    assert r.status_code == 201
    yield catalog_id
    await in_process_client_module.delete(f"/features/catalogs/{catalog_id}?force=true")


@pytest.fixture
async def setup_collection(
    in_process_client_module, setup_catalog, collection_data, collection_id, config_data
):
    catalog_id = setup_catalog
    r = await in_process_client_module.post(
        f"/features/catalogs/{catalog_id}/collections", json=collection_data
    )
    assert r.status_code == 201

    # The config storage for a freshly-created collection can briefly return
    # 409 while provisioning settles (the "config PUT right after create"
    # race). It is harmless and self-clears, but surfaces under xdist parallel
    # load and flakes this fixture. Poll the PUT until it stops racing — the
    # same retry pattern operators use against the live configs API.
    config_url = (
        f"/configs/catalogs/{catalog_id}/collections/{collection_id}"
        f"/plugins/collection_plugin_config"
    )
    for attempt in range(10):
        r = await in_process_client_module.put(config_url, json=config_data)
        if r.status_code != 409:
            break
        await asyncio.sleep(0.2 * (attempt + 1))
    assert r.status_code in [200, 204], (
        f"config PUT failed after retries: {r.status_code} {r.text}"
    )

    yield collection_id
    # Cleanup is handled by setup_catalog (delete catalog deletes collections)
