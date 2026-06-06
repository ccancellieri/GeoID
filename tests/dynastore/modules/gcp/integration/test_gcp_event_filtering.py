import asyncio
import logging
import os

import pytest

from tests.dynastore.test_utils import generate_test_id

logger = logging.getLogger(__name__)
from dynastore.modules.gcp.gcp_module import GCPModule
from dynastore.modules.gcp.gcp_config import (
    GcpCatalogBucketConfig,
    GcpEventingConfig,
    ManagedBucketEventing,
)
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
from google.cloud import storage


@pytest.mark.gcp
@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "tasks", "gcp")
@pytest.mark.enable_tasks("gcp_provision", "gcp_catalog_cleanup")
async def test_gcp_event_filtering_multiple_prefixes(app_lifespan, monkeypatch):
    """
    Verifies that Managed Eventing creates multiple GCS notifications for the configured prefixes.

    Push subscriptions require HTTPS endpoints, which are not available locally or in CI.
    When SERVICE_URL is plain-HTTP (or absent), setup_push_subscription silently falls back
    to a pull subscription — acceptable here because the test only verifies GCS notification
    prefixes, not message delivery.  Both the background lifecycle call and the explicit
    setup_catalog_gcp_resources call are guarded by the create_topic retry loop so a
    concurrent 409 "raced" response is retried rather than propagated.
    """
    catalog_id = f"it_evtm_{generate_test_id(12)}"
    from dynastore.models.protocols import StorageProtocol, EventingProtocol, ConfigsProtocol
    from dynastore.modules import get_protocol
    gcp_module = get_protocol(StorageProtocol)
    if not gcp_module:
        pytest.skip("GCPModule (StorageProtocol) not initialized.")

    try:
        gcp_module.get_storage_client()
        gcp_module.get_publisher_client()
        gcp_module.get_subscriber_client()
    except RuntimeError:
        from unittest.mock import MagicMock, AsyncMock
        gcp_module.get_storage_client = MagicMock()
        gcp_module.get_publisher_client = MagicMock()
        gcp_module.get_subscriber_client = MagicMock()
        # Mock actual bucket provisioning so it doesn't return None on failures
        mock_bucket = gcp_module.get_bucket_service()
        mock_bucket.ensure_storage_for_catalog = AsyncMock(return_value=f"bucket_{catalog_id}")
        gcp_module.get_self_url = AsyncMock(return_value="http://localhost:8080")

        # Mock bucket notifications
        mock_gcs_bucket = MagicMock()
        mock_gcs_bucket.list_notifications.return_value = [
            MagicMock(blob_name_prefix="catalog/", notification_id="mock_id"),
            MagicMock(blob_name_prefix="collections/", notification_id="mock_id")
        ]
        # Ensure notification objects created during setup_catalog_gcp_resources
        # also carry "mock_id" so the ID comparison succeeds in the assertion.
        mock_notification = MagicMock()
        mock_notification.notification_id = "mock_id"
        mock_gcs_bucket.notification.return_value = mock_notification
        # The module uses storage_client.bucket() (not get_bucket()) to obtain the bucket handle
        gcp_module.get_storage_client.return_value.bucket.return_value = mock_gcs_bucket
        gcp_module.get_storage_client.return_value.get_bucket.return_value = mock_gcs_bucket

    # Ensure provisioning is allowed for this integration test
    monkeypatch.setenv("DYNASTORE_GCP_FORCE_PROVISIONING", "true")

    # Provide a plain-HTTP endpoint so setup_push_subscription falls back to a
    # pull subscription — Pub/Sub rejects plain-HTTP push endpoints.  These tests
    # only verify GCS notification prefixes, not push delivery, so a pull
    # subscription is the correct CI-viable variant.
    if not os.getenv("K_SERVICE") and not os.getenv("SERVICE_URL"):
        monkeypatch.setenv("SERVICE_URL", "http://localhost:8080")

    # Override the conftest default: enable eventing for this test
    configs = get_protocol(ConfigsProtocol)
    if configs:
        await configs.set_config(
            GcpEventingConfig,
            GcpEventingConfig(managed_eventing=ManagedBucketEventing(enabled=True)),
        )

    # 1. Create the catalog in the database first (mandatory for linkage)
    from dynastore.models.protocols import CatalogsProtocol
    catalogs = get_protocol(CatalogsProtocol)
    catalog_def = {
        "id": catalog_id,
        "title": {"en": "Test Event Multi"},
        "description": {"en": "Test Event Multi Description"},
        "conformsTo": [],
        "links": []
    }
    await catalogs.create_catalog(catalog_def, lang="*")

    try:
        # 2. Ensure catalog GCP setup matches expectations.
        # prepare_upload_target triggers the background lifecycle setup;
        # setup_catalog_gcp_resources is called explicitly immediately after.
        # Both may attempt create_topic concurrently on the shared GCP project —
        # the retry-on-Aborted loop in setup_managed_eventing_channel handles this.
        await gcp_module.prepare_upload_target(catalog_id)

        # Wait for background initialization to complete (the default one)
        await lifecycle_registry.wait_for_all_tasks()

        # Trigger/Verify resource setup
        bucket_name, updated_config = await gcp_module.setup_catalog_gcp_resources(
            catalog_id
        )

        if not bucket_name or not updated_config:
            pytest.skip("GCS bucket provisioning unavailable (no real GCP project)")

        managed_config = updated_config.managed_eventing

        if not managed_config or not managed_config.enabled:
            pytest.skip("Managed eventing not enabled (GCS operations unavailable)")

        assert len(managed_config.blob_name_prefixes) == 2
        assert "catalog/" in managed_config.blob_name_prefixes
        assert "collections/" in managed_config.blob_name_prefixes
        assert len(managed_config.gcs_notification_ids) == 2

        # 3. Verify notifications on the bucket
        storage_client = gcp_module.get_storage_client()

        # Handle GCS eventual consistency (404 NotFound can occur briefly between clients)
        import time
        from google.api_core.exceptions import NotFound

        bucket = None
        max_retries = 20
        retry_interval = 1.0
        for i in range(max_retries):
            try:
                bucket = storage_client.get_bucket(bucket_name)
                break
            except NotFound:
                if i == max_retries - 1:
                    raise
                logger.info(f"Test: Bucket {bucket_name} not yet visible, retrying... ({i+1}/{max_retries})")
                time.sleep(retry_interval)

        notifications = list(bucket.list_notifications())

        prefixes_found = [
            n.blob_name_prefix
            for n in notifications
            if n.notification_id in managed_config.gcs_notification_ids
        ]
        assert "catalog/" in prefixes_found
        assert "collections/" in prefixes_found

        logger.info("Verified multiple GCS notifications for prefixes: %s", prefixes_found)

    finally:
        # Force-delete the catalog so GCP resources (topic, subscription,
        # notifications, bucket) are cleaned up and do not accumulate on the
        # shared project between runs.
        try:
            await catalogs.delete_catalog(catalog_id, force=True)
            await lifecycle_registry.wait_for_all_tasks(timeout=30.0)
        except Exception as exc:
            logger.warning("Teardown: delete_catalog(%s) failed: %s", catalog_id, exc)


@pytest.mark.gcp
@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "tasks", "gcp")
@pytest.mark.enable_tasks("gcp_provision", "gcp_catalog_cleanup")
async def test_gcp_event_filtering_custom_prefixes(app_lifespan, monkeypatch):
    """
    Verifies that Managed Eventing creates GCS notifications for custom configured prefixes.

    Push subscriptions require HTTPS endpoints, which are not available locally or in CI.
    When SERVICE_URL is plain-HTTP (or absent), setup_push_subscription silently falls back
    to a pull subscription — acceptable here because the test only verifies GCS notification
    prefixes, not message delivery.
    """
    catalog_id = f"it_evtc_{generate_test_id(12)}"
    from dynastore.models.protocols import StorageProtocol, ConfigsProtocol
    from dynastore.modules import get_protocol
    gcp_module = get_protocol(StorageProtocol)
    if not gcp_module:
        pytest.skip("GCPModule (StorageProtocol) not initialized.")

    try:
        gcp_module.get_storage_client()
        gcp_module.get_publisher_client()
        gcp_module.get_subscriber_client()
    except RuntimeError:
        from unittest.mock import MagicMock, AsyncMock
        gcp_module.get_storage_client = MagicMock()
        gcp_module.get_publisher_client = MagicMock()
        gcp_module.get_subscriber_client = MagicMock()
        # Mock actual bucket provisioning so it doesn't return None on failures
        mock_bucket = gcp_module.get_bucket_service()
        mock_bucket.ensure_storage_for_catalog = AsyncMock(return_value=f"bucket_{catalog_id}")
        gcp_module.get_self_url = AsyncMock(return_value="http://localhost:8080")

        # Mock bucket notifications for custom prefixes test
        mock_gcs_bucket = MagicMock()
        mock_gcs_bucket.list_notifications.return_value = [
            MagicMock(blob_name_prefix="data/input/", notification_id="mock_id"),
            MagicMock(blob_name_prefix="data/output/", notification_id="mock_id")
        ]
        # Ensure notification objects created during setup_catalog_gcp_resources
        # also carry "mock_id" so the ID comparison succeeds in the assertion.
        mock_notification = MagicMock()
        mock_notification.notification_id = "mock_id"
        mock_gcs_bucket.notification.return_value = mock_notification
        # The module uses storage_client.bucket() (not get_bucket()) to obtain the bucket handle
        gcp_module.get_storage_client.return_value.bucket.return_value = mock_gcs_bucket
        gcp_module.get_storage_client.return_value.get_bucket.return_value = mock_gcs_bucket

    # Ensure provisioning is allowed for this integration test
    monkeypatch.setenv("DYNASTORE_GCP_FORCE_PROVISIONING", "true")

    # Provide a plain-HTTP endpoint so setup_push_subscription falls back to a
    # pull subscription — Pub/Sub rejects plain-HTTP push endpoints.  These tests
    # only verify GCS notification prefixes, not push delivery, so a pull
    # subscription is the correct CI-viable variant.
    if not os.getenv("K_SERVICE") and not os.getenv("SERVICE_URL"):
        monkeypatch.setenv("SERVICE_URL", "http://localhost:8080")

    # Override the conftest default: enable eventing for this test
    configs = get_protocol(ConfigsProtocol)
    if configs:
        await configs.set_config(
            GcpEventingConfig,
            GcpEventingConfig(managed_eventing=ManagedBucketEventing(enabled=True)),
        )

    # 1. Create the catalog in the database first (mandatory for linkage)
    from dynastore.models.protocols import CatalogsProtocol
    catalogs = get_protocol(CatalogsProtocol)
    catalog_def = {
        "id": catalog_id,
        "title": {"en": "Test Event Custom"},
        "description": {"en": "Test Event Custom Description"},
        "conformsTo": [],
        "links": []
    }
    await catalogs.create_catalog(catalog_def, lang="*")

    try:
        # 2. Ensure catalog GCP setup exists (triggers background setup)
        await gcp_module.prepare_upload_target(catalog_id)

        # Wait for default setup to complete
        await lifecycle_registry.wait_for_all_tasks()

        # Verify GCS is actually functional before proceeding
        bucket_check, config_check = await gcp_module.setup_catalog_gcp_resources(catalog_id)
        if not bucket_check or not config_check or not getattr(config_check.managed_eventing, "enabled", False):
            pytest.skip("GCS bucket provisioning unavailable (no real GCP project)")

        # Configure custom prefixes
        custom_prefixes = ["data/input/", "data/output/"]
        eventing_config = GcpEventingConfig(
            managed_eventing=ManagedBucketEventing(
                enabled=True, blob_name_prefixes=custom_prefixes
            )
        )

        from dynastore.modules.db_config.query_executor import managed_transaction

        async with managed_transaction(gcp_module.engine) as conn:
            await gcp_module.get_config_service().set_config(
                GcpEventingConfig,
                eventing_config,
                catalog_id=catalog_id,
                db_resource=conn,
            )

        # Trigger resource update with new config
        bucket_name, updated_config = await gcp_module.setup_catalog_gcp_resources(
            catalog_id
        )
        managed_config = updated_config.managed_eventing

        assert managed_config is not None
        assert managed_config.blob_name_prefixes == custom_prefixes
        assert len(managed_config.gcs_notification_ids) == 2

        # Verify notifications on the bucket
        storage_client = gcp_module.get_storage_client()

        # Handle GCS eventual consistency (404 NotFound can occur briefly between clients)
        import time
        from google.api_core.exceptions import NotFound

        bucket = None
        max_retries = 20
        retry_interval = 1.0
        for i in range(max_retries):
            try:
                bucket = storage_client.get_bucket(bucket_name)
                break
            except NotFound:
                if i == max_retries - 1:
                    raise
                logger.info(f"Test: Bucket {bucket_name} not yet visible, retrying... ({i+1}/{max_retries})")
                time.sleep(retry_interval)

        notifications = list(bucket.list_notifications())

        prefixes_found = [
            n.blob_name_prefix
            for n in notifications
            if n.notification_id in managed_config.gcs_notification_ids
        ]
        for p in custom_prefixes:
            assert p in prefixes_found

        logger.info("Verified custom GCS notifications for prefixes: %s", prefixes_found)

    finally:
        # Force-delete the catalog so GCP resources (topic, subscription,
        # notifications, bucket) are cleaned up and do not accumulate on the
        # shared project between runs.
        try:
            await catalogs.delete_catalog(catalog_id, force=True)
            await lifecycle_registry.wait_for_all_tasks(timeout=30.0)
        except Exception as exc:
            logger.warning("Teardown: delete_catalog(%s) failed: %s", catalog_id, exc)
