import pytest
from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.catalog.asset_service import (
    AssetBase,
    AssetTypeEnum,
)
from tests.dynastore.test_utils import generate_test_id

@pytest.mark.asyncio
@pytest.mark.enable_extensions("assets")
async def test_assets_api_crud(
    sysadmin_in_process_client,
    catalog_obj,
    catalog_id,
    collection_obj,
    collection_id,
):
    """
    Integration test for Assets API.
    Verifies CRUD operations and Partitioning logic (implicitly).
    """
    # Sysadmin client is required for asset writes after PR #149 IAM tightening.
    client = sysadmin_in_process_client

    catalogs = get_protocol(CatalogsProtocol)

    await catalogs.delete_catalog(catalog_id, force=True)
    await catalogs.create_catalog(catalog_obj)
    await catalogs.create_collection(catalog_id, collection_obj)

    try:
        # --- Catalog Asset ---
        cat_asset_id = f"CAT_ASSET_{generate_test_id()}"
        cat_asset_payload = {
            "asset_id": cat_asset_id,
            "uri": "gs://bucket/cat_asset.tif",
            "asset_type": "ASSET",
            "metadata": {"type": "report"}
        }

        # Create
        resp = await client.post(f"/assets/catalogs/{catalog_id}", json=cat_asset_payload)
        assert resp.status_code == 201
        cat_asset = resp.json()
        assert cat_asset["asset_id"] == cat_asset_id
        assert cat_asset["collection_id"] is None

        # Get
        resp = await client.get(f"/assets/catalogs/{catalog_id}/assets/{cat_asset_id}")
        assert resp.status_code == 200
        assert resp.json()["asset_id"] == cat_asset_id

        # List
        resp = await client.get(f"/assets/catalogs/{catalog_id}")
        assert resp.status_code == 200
        assets = resp.json()
        assert any(a["asset_id"] == cat_asset_id for a in assets)

        # Update (Metadata only)
        update_payload = {"metadata": {"type": "report", "updated": True}}
        resp = await client.put(f"/assets/catalogs/{catalog_id}/assets/{cat_asset_id}", json=update_payload)
        assert resp.status_code == 200
        assert resp.json()["metadata"]["updated"] is True

        # --- Collection Asset ---
        coll_asset_id = f"COLL_ASSET_{generate_test_id()}"
        coll_asset_payload = {
            "asset_id": coll_asset_id,
            "uri": "gs://bucket/coll_asset.tif",
            "asset_type": "RASTER",
            "metadata": {"sensor": "S2"}
        }

        # Create (triggers partition creation)
        resp = await client.post(f"/assets/catalogs/{catalog_id}/collections/{collection_id}", json=coll_asset_payload)
        assert resp.status_code == 201
        coll_asset = resp.json()
        assert coll_asset["asset_id"] == coll_asset_id
        assert coll_asset["collection_id"] == collection_id

        # Get
        resp = await client.get(f"/assets/catalogs/{catalog_id}/collections/{collection_id}/assets/{coll_asset_id}")
        assert resp.status_code == 200
        assert resp.json()["asset_id"] == coll_asset_id

        # Search
        search_payload = {
            "filters": [
                {"field": "asset_id", "op": "eq", "value": coll_asset_id}
            ],
            "collection_id": collection_id,
            "limit": 10
        }
        resp = await client.post(f"/assets/catalogs/{catalog_id}/assets-search", json=search_payload)
        assert resp.status_code == 200
        results = resp.json()
        assert len(results) == 1
        assert results[0]["asset_id"] == coll_asset_id

        # Helper for search error logic
        search_payload_err = {
            "filters": [
                {"field": "non_existent_field", "op": "eq", "value": "check"}
            ],
            "limit": 10
        }
        resp = await client.post(f"/assets/catalogs/{catalog_id}/assets-search", json=search_payload_err)
        # Depending on implementation, might 400 or 500. SQL injection protection throws value error?
        # Basic validation passes, but SQL execution might fail if column doesn't exist?
        # actually validate_sql_identifier catches it if it has weird chars, but "non_existent_field" is valid identifier.
        # Postgres will throw generic error. AssetService catches Exception and returns 400.
        assert resp.status_code == 400

        # Delete Collection Asset
        resp = await client.delete(f"/assets/catalogs/{catalog_id}/collections/{collection_id}/assets/{coll_asset_id}")
        assert resp.status_code == 204

        # Verify Deletion
        resp = await client.get(f"/assets/catalogs/{catalog_id}/collections/{collection_id}/assets/{coll_asset_id}")
        assert resp.status_code == 404

        # Delete Catalog Asset
        resp = await client.delete(f"/assets/catalogs/{catalog_id}/assets/{cat_asset_id}")
        assert resp.status_code == 204

    finally:
        await catalogs.delete_catalog(catalog_id, force=True)
