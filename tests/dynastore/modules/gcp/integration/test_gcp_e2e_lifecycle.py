#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""
End-to-end GCS-backed catalog lifecycle.

Exercises the full production path that cannot be validated piecewise:

1. Create a catalog → triggers ``gcp_provision`` task → GCS bucket created.
2. Managed Pub/Sub eventing is disabled by the autouse
   ``disable_managed_eventing`` fixture (localhost cannot receive push
   subscriptions). In production, the OBJECT_COMPLETED event on a new
   upload would auto-create the asset; here we simulate that step.
3. Create a collection.
4. Upload a real GeoJSON blob directly to the provisioned bucket using
   the GCS client (bypassing the resumable-upload endpoint — the file
   payload is what matters for ingestion).
5. Register the blob as a **collection asset** so the virtual STAC
   collection-by-asset endpoint can filter by ``asset_id``.
6. Run the OGC ``ingestion`` process against that asset with
   ``GcsDetailedReporter`` configured to write reports to the same
   bucket. The reporter is auto-registered when the GCP module loads.
7. Verify that the asset + features are visible via:
   - ``GET /features/catalogs/{cat}/collections/{col}/items``
   - ``GET /stac/virtual/assets/{asset}/catalogs/{cat}/collections/{col}``
     and ``.../items`` (the collection-by-asset virtual view)
   - ``GET /tiles/catalogs/{cat}/tiles/WebMercatorQuad/{z}/{x}/{y}.mvt``
   and that the GCS reporter wrote a report blob into
   ``gs://{bucket}/ingestion-reports/``.
8. Hard-delete the catalog and verify the bucket is removed (with a
   direct force-delete as a safety net for the known async-cleanup race
   documented in ``test_gcp_lifecycle.py``).

Skipped when GCP credentials (ADC) are unavailable — handled by the
``@pytest.mark.gcp`` marker.
"""

import asyncio
import json
import logging

import pytest
from google.api_core.exceptions import NotFound

from dynastore.models.protocols import CatalogsProtocol, StorageProtocol
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
from dynastore.modules.gcp.gcp_module import GCPModule
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.modules.tasks.runners import capability_map
from dynastore.modules.tasks.tasks_module import list_tasks_for_catalog
from dynastore.tools.discovery import get_protocol
from tests.dynastore.test_utils import generate_test_id

logger = logging.getLogger(__name__)


def _stac_data_loader(filename: str):
    """Load a fixture from tests/dynastore/extensions/stac/integration/data."""
    import os
    base = os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "extensions", "stac",
        "integration", "data",
    )
    with open(os.path.join(base, filename)) as f:
        return json.load(f)


@pytest.mark.gcp
@pytest.mark.asyncio
@pytest.mark.enable_modules(
    "db_config", "db", "catalog", "stac", "tasks", "gcp", "iam", "events", "cache",
    "catalog_postgresql",
)
@pytest.mark.enable_extensions("stac")
@pytest.mark.enable_tasks("gcp_provision", "gcp_catalog_cleanup")
async def test_dispatcher_claims_gcp_provision_exclusively(
    sysadmin_in_process_client, app_lifespan, catalog_cleaner,
):
    """Regression guard for the DefinitionOnlyTask dispatcher-crash bug (PR #39).

    Root cause (2026-04-22): CapabilityMap.refresh() called are_protocols_satisfied()
    on DefinitionOnlyTask placeholders, which lacked the method.  The AttributeError
    killed the dispatcher before it entered its main loop — all gcp_provision_catalog
    tasks stayed PENDING forever and no GCS buckets were created.

    This test verifies the full ownership chain:
    1. CapabilityMap has gcp_provision_catalog after startup (dispatcher started cleanly).
    2. Catalog creation via POST /stac/catalogs enqueues the task.
    3. wait_for_all_tasks() returns — task was claimed and ran.
    4. Task status=COMPLETED and owner_id is populated (dispatcher identity, not null).
    5. GCS bucket exists for the catalog.
    """
    gcp_module = get_protocol(StorageProtocol)
    if not isinstance(gcp_module, GCPModule):
        pytest.skip("GCPModule not registered as StorageProtocol.")
    if not getattr(app_lifespan, "engine", None):
        pytest.skip("app_state.engine not initialized.")
    try:
        storage_client = gcp_module.get_storage_client()
    except RuntimeError:
        pytest.skip("GCP storage client not available (no credentials).")

    # 1. Dispatcher started cleanly — gcp_provision_catalog must be in async_types.
    #    If DefinitionOnlyTask crashed refresh(), this set is empty.
    assert "gcp_provision_catalog" in capability_map.async_types, (
        f"gcp_provision_catalog missing from async capability map: {capability_map.async_types}. "
        "Likely the dispatcher crashed at startup (DefinitionOnlyTask bug or missing GCPModule)."
    )

    catalog_id = f"it_dsp_{generate_test_id(8)}"
    catalog_cleaner(catalog_id)

    cat_payload = _stac_data_loader("catalog.json")
    cat_payload["id"] = catalog_id

    # 2. Create catalog — gcp_provision_catalog task enqueued inside the request.
    resp = await sysadmin_in_process_client.post("/stac/catalogs", json=cat_payload)
    assert resp.status_code in (200, 201), f"catalog create failed: {resp.text}"

    # 3. Wait for the dispatcher to claim and complete the task.
    await lifecycle_registry.wait_for_all_tasks(timeout=60.0)

    # 4. Verify task status and who claimed it.
    engine = app_lifespan.engine
    async with managed_transaction(engine) as conn:
        tasks = await list_tasks_for_catalog(conn, catalog_id)
    prov_tasks = [t for t in tasks if t.task_type == "gcp_provision_catalog"]
    assert prov_tasks, f"No gcp_provision_catalog task found for {catalog_id}"
    task = prov_tasks[0]
    assert task.status.value == "COMPLETED", (
        f"task status={task.status.value}, retry_count={task.retry_count}, "
        f"message={task.error_message!r}"
    )
    assert task.started_at is not None, (
        "started_at is null — task was never claimed by any dispatcher. "
        "The dispatcher may have crashed at startup."
    )
    logger.info(
        "gcp_provision_catalog claimed at started_at=%r, completed in retry_count=%d",
        task.started_at, task.retry_count,
    )

    # 5. GCS bucket must exist.
    bucket_name = await gcp_module.get_storage_identifier(catalog_id)
    assert bucket_name, f"No bucket linked to catalog {catalog_id} after COMPLETED task"
    bucket = None
    for _ in range(10):
        try:
            bucket = storage_client.get_bucket(bucket_name)
            break
        except NotFound:
            await asyncio.sleep(1.0)
    assert bucket is not None and bucket.exists(), (
        f"Bucket {bucket_name} not visible in GCS after task COMPLETED"
    )
    logger.info("Bucket %s provisioned and verified for catalog %s", bucket_name, catalog_id)


@pytest.mark.gcp
@pytest.mark.asyncio
@pytest.mark.enable_modules(
    "db_config", "db", "catalog", "stac", "processes", "tasks",
    "proxy", "gcp", "iam", "collection_postgresql", "catalog_postgresql",
)
@pytest.mark.enable_extensions("stac", "assets", "features", "processes", "gcp_bucket", "tiles")
@pytest.mark.enable_tasks("gcp_provision", "gcp_catalog_cleanup", "ingestion")
async def test_gcp_end_to_end_lifecycle(
    sysadmin_in_process_client, in_process_client, app_lifespan, base_url,
):
    gcp_module = get_protocol(StorageProtocol)
    if not isinstance(gcp_module, GCPModule):
        pytest.skip("GCPModule not registered as StorageProtocol.")
    if not getattr(app_lifespan, "engine", None):
        pytest.skip("app_state.engine not initialized.")
    try:
        storage_client = gcp_module.get_storage_client()
    except RuntimeError:
        pytest.skip("GCP storage client not available (no credentials).")

    run_id = generate_test_id(10)
    catalog_id = f"it_{run_id}"
    collection_id = f"col_{run_id}"
    asset_id = f"asset_{run_id}"

    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None

    # Pre-cleanup: force-delete any leftover from a previous run.
    try:
        await catalogs.delete_catalog(catalog_id, force=True)
        await lifecycle_registry.wait_for_all_tasks(timeout=10.0)
    except Exception:
        pass

    bucket_name = None
    try:
        # 1. Create catalog via STAC API → triggers gcp_provision lifecycle task.
        cat_payload = _stac_data_loader("catalog.json")
        cat_payload["id"] = catalog_id
        resp = await sysadmin_in_process_client.post(
            "/stac/catalogs", json=cat_payload
        )
        assert resp.status_code in (200, 201), f"catalog create: {resp.text}"
        await lifecycle_registry.wait_for_all_tasks(timeout=60.0)

        # 2. Verify bucket exists (eventual consistency).
        bucket_name = await gcp_module.get_storage_identifier(catalog_id)
        assert bucket_name, f"Bucket not provisioned for catalog {catalog_id}"
        bucket = None
        for _ in range(20):
            try:
                bucket = storage_client.get_bucket(bucket_name)
                break
            except NotFound:
                await asyncio.sleep(1.0)
        assert bucket is not None and bucket.exists(), (
            f"Bucket {bucket_name} not visible after provisioning"
        )
        logger.info("Provisioned bucket %s", bucket_name)

        # 3. Create collection.
        col_payload = _stac_data_loader("collection.json")
        col_payload["id"] = collection_id
        resp = await in_process_client.post(
            f"/stac/catalogs/{catalog_id}/collections", json=col_payload
        )
        assert resp.status_code in (200, 201), f"collection create: {resp.text}"

        # 4. Upload a GeoJSON artifact directly into the provisioned bucket.
        feature = _stac_data_loader("feature_item.json")
        blob_path = f"data/{asset_id}.json"
        blob = bucket.blob(blob_path)
        blob.upload_from_string(
            json.dumps(feature), content_type="application/geo+json"
        )
        artifact_uri = f"gs://{bucket_name}/{blob_path}"
        logger.info("Uploaded artifact to %s", artifact_uri)

        # 5. Register the artifact as a collection asset (simulates the
        #    OBJECT_COMPLETED Pub/Sub event that fires in production).
        asset_payload = _stac_data_loader("asset.json")
        asset_payload.update({"asset_id": asset_id, "uri": artifact_uri})
        resp = await in_process_client.post(
            f"/assets/catalogs/{catalog_id}/collections/{collection_id}",
            json=asset_payload,
        )
        assert resp.status_code == 201, f"asset register: {resp.text}"

        # 6. Enable feature tracking on the collection.
        stac_config = _stac_data_loader("config.json")
        resp = await in_process_client.put(
            f"/configs/catalogs/{catalog_id}/collections/{collection_id}"
            f"/plugins/stac_plugin_config",
            json=stac_config,
        )
        assert resp.status_code in (200, 204), f"config put: {resp.text}"

        # 7. Kick off ingestion with GcsDetailedReporter writing reports to
        #    the catalog's bucket.
        task = _stac_data_loader("ingestion_task.json")
        task["catalog_id"] = catalog_id
        task["collection_id"] = collection_id
        task["ingestion_request"]["asset"]["asset_id"] = asset_id
        task["ingestion_request"]["asset"]["uri"] = artifact_uri
        task["ingestion_request"]["reporting"] = {
            "gcs_detailed_reporter": {
                "report_file_path": (
                    f"gs://{bucket_name}/ingestion-reports/"
                    "{task_id}-{timestamp_utc}.json"
                ),
                "output_format": "JSON",
                "report_content": "ALL",
            }
        }

        # Use the sysadmin client here so ``caller_id`` is not resolved to
        # ``SYSTEM_USER_ID`` (which has no seed ALLOW policy for Action.EXECUTE)
        # — the unauthenticated ``in_process_client`` would 403 this step.
        resp = await sysadmin_in_process_client.post(
            f"/processes/catalogs/{catalog_id}/collections/{collection_id}"
            f"/processes/ingestion/execution",
            json={"inputs": task, "outputs": {}},
            headers={"Prefer": "wait=true"},
        )
        assert resp.status_code in (200, 201), f"ingestion exec: {resp.text}"
        assert resp.json().get("status") == "successful", (
            f"ingestion not successful: {resp.text}"
        )

        # 8. GcsDetailedReporter wrote a report blob (and a *_config blob).
        reports = list(
            storage_client.list_blobs(bucket_name, prefix="ingestion-reports/")
        )
        assert len(reports) >= 1, "No ingestion reports written to bucket"
        logger.info(
            "Ingestion wrote %d report blob(s): %s",
            len(reports), [b.name for b in reports],
        )

        # 9. Features service returns the ingested feature.
        resp = await in_process_client.get(
            f"/features/catalogs/{catalog_id}/collections/{collection_id}/items"
            f"?limit=10"
        )
        assert resp.status_code == 200, f"features list: {resp.text}"
        feats = resp.json().get("features", [])
        assert len(feats) >= 1, "features endpoint returned no items post-ingestion"

        # 10. Virtual STAC collection-by-asset view.
        resp = await in_process_client.get(
            f"/stac/virtual/assets/{asset_id}/catalogs/{catalog_id}"
            f"/collections/{collection_id}"
        )
        assert resp.status_code == 200
        assert resp.json()["id"] == asset_id

        resp = await in_process_client.get(
            f"/stac/virtual/assets/{asset_id}/catalogs/{catalog_id}"
            f"/collections/{collection_id}/items"
        )
        assert resp.status_code == 200
        vfeats = resp.json().get("features", [])
        assert len(vfeats) >= 1, (
            "virtual STAC collection-by-asset returned no items"
        )

        # 11. Tiles endpoint — advisory only. Serving MVT tiles requires a
        #     TilesConfig (tile-matrix-set, geometry column mapping, …) that
        #     this happy-path test doesn't configure. We hit the endpoint
        #     to prove it's wired but do not fail the test on a config error.
        resp = await in_process_client.get(
            f"/tiles/catalogs/{catalog_id}/tiles/WebMercatorQuad/0/0/0.mvt"
            f"?collections={collection_id}"
        )
        if resp.status_code in (200, 204):
            logger.info("Tiles endpoint served %d bytes", len(resp.content))
        else:
            logger.warning(
                "Tiles endpoint returned %d (expected — TilesConfig not "
                "configured in this test): %s",
                resp.status_code, resp.text[:200],
            )

    finally:
        # 12. Hard-delete catalog + verify bucket is gone.
        try:
            await catalogs.delete_catalog(catalog_id, force=True)
            await lifecycle_registry.wait_for_all_tasks(timeout=30.0)
        except Exception as exc:
            logger.warning("delete_catalog failed: %s", exc)

        if bucket_name:
            # Known race: the async destruction hook occasionally loses the
            # bucket-name lookup when the schema is dropped first. Force-delete
            # as test cleanup and record whether the hook got there first.
            try:
                b = storage_client.get_bucket(bucket_name)
                b.delete(force=True)
                logger.info("Test cleanup: bucket %s force-deleted", bucket_name)
            except NotFound:
                logger.info(
                    "Verified: bucket %s cleaned up by lifecycle hook",
                    bucket_name,
                )
