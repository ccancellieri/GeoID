import os

import pytest
from tests.dynastore.test_utils import generate_test_id

# Enables the `gcp` module, which triggers `gcp_provision_catalog` on catalog POST.
# That task calls `get_self_url()` which reads `K_SERVICE` / `SERVICE_URL` from the
# environment and raises `RuntimeError` outside Cloud Run, so the catalog POST 409s
# and every downstream assertion in the test (collection POST, config PUT, ingestion)
# is unreachable. Skip when neither env var is set so this file stops red-flagging on
# local and non-Cloud-Run CI runners. Refs #771 (#755 audit fallout).
pytestmark = pytest.mark.skipif(
    not (os.getenv("K_SERVICE") or os.getenv("SERVICE_URL")),
    reason="Requires Cloud Run environment (K_SERVICE / SERVICE_URL) for gcp_provision_catalog",
)


@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features", "processes")
@pytest.mark.enable_modules(
    "db_config", "db", "catalog", "stac", "processes", "tasks", "proxy", "gcp", "iam", "collection_postgresql", "catalog_postgresql"
)
@pytest.mark.enable_tasks("ingestion")
async def test_virtual_stac_endpoints(sysadmin_in_process_client, in_process_client, test_data_loader, base_url):
    """
    Integration test for STAC Virtual View and Source Tracking using STAC API inputs.
    """
    run_id = generate_test_id()
    catalog_id = f"cat_{run_id}"
    collection_id = f"col_{run_id}"
    asset_id = f"test_asset_{run_id}"

    # Setup Web Extension to serve the test data
    # from tests.dynastore.extensions.tools.web_utils import register_static_data_provider

    data_dir = "tests/dynastore/extensions/stac/integration/data"
    # Use absolute file path to avoid network requests during in-process testing
    file_uri = os.path.abspath(os.path.join(data_dir, "feature_item.json"))

    # 1. Create Catalog (STAC API)
    cat_data = test_data_loader("catalog.json")
    cat_data["id"] = catalog_id
    resp = await sysadmin_in_process_client.post("/stac/catalogs", json=cat_data)
    assert resp.status_code in [201, 200]

    # 2. Create Main Collection (STAC API)
    col_data = test_data_loader("collection.json")
    col_data["id"] = collection_id
    resp = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections", json=col_data
    )
    assert resp.status_code in [201, 200]

    # 3. Insert an Asset record manually (Simulating ingestion)
    asset_payload = test_data_loader("asset.json")
    asset_payload.update({"asset_id": asset_id, "uri": file_uri})
    resp = await sysadmin_in_process_client.post(
        f"/assets/catalogs/{catalog_id}/collections/{collection_id}",
        json=asset_payload,
    )
    assert resp.status_code == 201

    # 4. Feature Tracking Config
    stac_config_payload = test_data_loader("config.json")
    resp = await sysadmin_in_process_client.put(
        f"/configs/catalogs/{catalog_id}/collections/{collection_id}/plugins/stac_plugin_config",
        json=stac_config_payload,
    )
    assert resp.status_code in [200, 204]

    # 5. Ingestion Task
    ingestion_task = test_data_loader("ingestion_task.json")
    ingestion_task["catalog_id"] = catalog_id
    ingestion_task["collection_id"] = collection_id

    ingestion_task["ingestion_request"]["asset"]["asset_id"] = asset_id

    # Use the correct generic process endpoint
    resp = await sysadmin_in_process_client.post(
        f"/processes/catalogs/{catalog_id}/collections/{collection_id}/processes/ingestion/execution",
        json={"inputs": ingestion_task, "outputs": {}},
        headers={"Prefer": "wait=true"},
    )
    if resp.status_code not in [201, 200]:
        print(f"ERROR RESPONSE: {resp.text}")
        pytest.fail(f"Process execution failed with {resp.status_code}: {resp.text}")
    assert resp.status_code in [201, 200]

    # Verify ingestion task response
    job_data = resp.json()
    assert job_data["status"] == "successful"

    # 6. Verify Feature Tracking Config
    resp = await sysadmin_in_process_client.get(
        f"/configs/catalogs/{catalog_id}/collections/{collection_id}/plugins/stac_plugin_config"
    )
    assert resp.status_code == 200
    config_data = resp.json()
    assert config_data["asset_tracking"]["enabled"] is True
    assert config_data["asset_tracking"]["access_mode"] == "proxy"

    # 7. Verify Virtual STAC Endpoints

    # A. Virtual Asset List: /stac/virtual/assets/catalogs/{id}/collections/{col}
    resp = await in_process_client.get(
        f"/stac/virtual/assets/catalogs/{catalog_id}/collections/{collection_id}"
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == f"{collection_id}_assets"

    # Check for child link to asset
    links = data.get("links", [])
    child_link = next(
        (
            l
            for l in links
            if l["href"].endswith(
                f"/stac/virtual/assets/{asset_id}/catalogs/{catalog_id}/collections/{collection_id}"
            )
        ),
        None,
    )
    if child_link is None:
        pytest.fail(f"Virtual Asset {asset_id} child link not found. Links: {links}")
    assert child_link is not None
    assert child_link["rel"] == "child"

    # B. Virtual Asset Collection: .../asset/{asset_id}
    resp = await in_process_client.get(
        f"/stac/virtual/assets/{asset_id}/catalogs/{catalog_id}/collections/{collection_id}"
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == asset_id
    # The href should point to the dynamic resolution endpoint
    asset_href = data["assets"]["source_file"]["href"]
    assert "/stac/catalogs/" in asset_href
    assert "/assets/" in asset_href
    assert "/source" in asset_href

    # Test the resolution endpoint itself
    # Since it returns a RedirectResponse, we can check where it points
    # By default, config.json likely has PROXY enabled or default.
    resp = await in_process_client.get(asset_href, follow_redirects=False)
    assert resp.status_code == 307  # Temporary Redirect for RedirectResponse
    assert "Location" in resp.headers

    # C. Virtual Asset Items: .../asset/{asset_id}/items
    resp = await in_process_client.get(
        f"/stac/virtual/assets/{asset_id}/catalogs/{catalog_id}/collections/{collection_id}/items"
    )
    assert resp.status_code == 200
    data = resp.json()
    features = resp.json().get("features", [])
    assert len(features) > 0
    assert features[0]["properties"]["prop"] == "value"

    # Check for parent link pointing back to the virtual asset collection
    parent_link = next(
        (l for l in features[0]["links"] if l["rel"] == "parent"), None
    )
    if parent_link is None:
        pytest.fail(f"Virtual Asset Item {features[0]['id']} missing parent link. Links: {features[0]['links']}")
    assert parent_link is not None
    assert parent_link["href"].endswith(
        f"/stac/virtual/assets/{asset_id}/catalogs/{catalog_id}/collections/{collection_id}"
    )
