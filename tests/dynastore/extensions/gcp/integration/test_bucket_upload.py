import pytest
import logging
import asyncio
from httpx import AsyncClient, ASGITransport
from unittest.mock import MagicMock, patch
from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols import StorageProtocol
from dynastore.modules.gcp.gcp_module import GCPModule
from dynastore.modules.gcp.gcp_config import GcpEventingConfig, ManagedBucketEventing

logger = logging.getLogger(__name__)

@pytest.mark.gcp
@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "stac", "gcp", "tasks", "iam", "collection_postgresql", "catalog_postgresql")
@pytest.mark.enable_extensions("gcp_bucket", "stac")
async def test_init_upload_acl_enum(sysadmin_in_process_client, app_lifespan, catalog_id, catalog_cleaner, test_data_loader, caplog, base_url):
    """
    Verifies that initiating an upload with a predefined ACL
    correctly passes the string value of the Enum to the GCS client.
    Uses real modules, fixtures for payloads, and in-process execution.
    """
    caplog.set_level(logging.DEBUG)

    # 1. Load payloads from fixtures
    catalog_payload = test_data_loader("catalog_payload.json")
    catalog_payload["id"] = catalog_id

    upload_payload = test_data_loader("init_upload_payload.json")
    upload_payload["catalog_id"] = catalog_id

    # 2. Get the bootstrapped gcp_module instance
    storage_proto = get_protocol(StorageProtocol)
    gcp_module = storage_proto if isinstance(storage_proto, GCPModule) else None

    # The engine is injected into app_state by db_config/db modules
    if not gcp_module or not getattr(app_lifespan, "engine", None):
        pytest.skip("GCPModule or app_state.engine not initialized.")

    # 3. Setup Mocks for external GCS/PubSub clients
    mock_storage_client = MagicMock()
    mock_publisher_client = MagicMock()
    mock_subscriber_client = MagicMock()

    # Set up the mock chain for blob upload
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    mock_blob.create_resumable_upload_session.return_value = (
        "https://example.com/mock-upload-session"
    )

    # Configure mocks to return strings for Pydantic validation
    mock_publisher_client.topic_path.return_value = (
        "projects/test-project/topics/test-topic"
    )
    mock_subscriber_client.subscription_path.return_value = (
        "projects/test-project/subscriptions/test-sub"
    )

    mock_notification = MagicMock()
    mock_notification.notification_id = "notification-123"
    mock_bucket.notification.return_value = mock_notification

    # Injecting mocks into the singleton module instance
    mock_creds = MagicMock()
    mock_creds.service_account_email = "test-sa@example.com"

    with (
        patch.object(gcp_module, "_storage_client", mock_storage_client),
        patch.object(gcp_module, "_publisher_client", mock_publisher_client),
        patch.object(gcp_module, "_subscriber_client", mock_subscriber_client),
        patch.object(
            gcp_module,
            "_identity",
            {
                "account_email": "test-sa@example.com",
                "project_id": "test-project",
                "region": "europe-west1",
            },
        ),
        patch.object(gcp_module, "_credentials", mock_creds),
    ):
        # Manually sync BucketService with the mocked state, as patching gcp_module attributes
        # doesn't automatically propagate to the encapsulated BucketService instance.
        if gcp_module._bucket_service:
            gcp_module._bucket_service.storage_client = mock_storage_client
            gcp_module._bucket_service.project_id = "test-project"
            gcp_module._bucket_service.region = "europe-west1"

        # 4. Use in-process AsyncClient to hit the bootstrapped app
        app = app_lifespan.app
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url=base_url
        ) as client:
            # A. Pre-cleanup + Create Catalog
            from dynastore.models.protocols import CatalogsProtocol
            from dynastore.modules.gcp import gcp_db
            from dynastore.modules.db_config.query_executor import managed_transaction as _mt
            catalogs_proto = get_protocol(CatalogsProtocol)
            if catalogs_proto:
                try:
                    await catalogs_proto.delete_catalog(catalog_id, force=True)
                except Exception:
                    pass
            create_resp = await sysadmin_in_process_client.post("/stac/catalogs", json=catalog_payload)
            assert create_resp.status_code in [201, 200], (
                f"Failed to create catalog: {create_resp.text}"
            )
            catalog_cleaner(catalog_id)

            # B. Force catalog into "ready" state with a linked (fake) bucket.
            # The catalog creation puts it in "provisioning" when GCP module is active,
            # but these tests focus on upload behavior — not provisioning.
            phys_schema = await catalogs_proto.resolve_physical_schema(catalog_id)
            bucket_name = gcp_module.get_bucket_service().generate_bucket_name(
                catalog_id, physical_schema=phys_schema
            )
            async with _mt(app_lifespan.engine) as db_conn:
                await gcp_db.link_bucket_to_catalog_query.execute(
                    db_conn, catalog_id=catalog_id, bucket_name=bucket_name
                )
            await catalogs_proto.update_provisioning_status(catalog_id, "ready")

            # C. Perform the upload initiation request
            max_retries = 5
            retry_interval = 0.5
            response = None
            
            for i in range(max_retries):
                response = await client.post(
                    "/gcp/buckets/init-upload", json=upload_payload
                )
                if response.status_code != 503:
                    break
                logger.info(f"Test: Catalog still provisioning (503), retrying in {retry_interval}s... ({i+1}/{max_retries})")
                await asyncio.sleep(retry_interval)

            print(f"DEBUG: Response Status: {response.status_code}")
            print(f"DEBUG: Response Body: {response.text}")

            if response.status_code != 200:
                for record in caplog.records:
                    print(f"LOG: {record.levelname}: {record.message}")

            assert response.status_code == 200
            assert "mock-upload-session" in response.text

            # 5. Verify GCS call arguments (Specifically Enum serialization)
            assert mock_blob.create_resumable_upload_session.called, (
                "GCS create_resumable_upload_session was not called"
            )

            args, kwargs = mock_blob.create_resumable_upload_session.call_args

            predefined_acl = kwargs.get("predefined_acl")
            assert predefined_acl == "publicRead"
            assert isinstance(predefined_acl, str), (
                f"Expected str, got {type(predefined_acl)}"
            )

            assert kwargs.get("if_generation_match") == 0

            # Wait for any background tasks (like eventing setup) to finish while mocks are still active
            from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry

            await lifecycle_registry.wait_for_all_tasks()

            print(
                "Integration Test SUCCESS: Real module lifecycle, fixtures, and Enum correctly serialized."
            )


@pytest.mark.gcp
@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "stac", "gcp", "tasks", "iam", "collection_postgresql", "catalog_postgresql")
@pytest.mark.enable_extensions("gcp_bucket", "stac")
async def test_init_upload_precondition_failed(
    sysadmin_in_process_client, app_lifespan, catalog_id, catalog_cleaner, test_data_loader, caplog, base_url
):
    """
    Verifies that GCS PreconditionFailed is caught and returned as a clear 400 error.
    """
    import google.api_core.exceptions
    from dynastore.models.protocols import CatalogsProtocol
    from dynastore.modules.gcp import gcp_db
    from dynastore.modules.db_config.query_executor import managed_transaction as _mt

    # 1. Load payloads
    catalog_payload = test_data_loader("catalog_payload.json")
    catalog_payload["id"] = catalog_id

    upload_payload = test_data_loader("init_upload_payload.json")
    upload_payload["catalog_id"] = catalog_id

    storage_proto = get_protocol(StorageProtocol)
    gcp_module = storage_proto if isinstance(storage_proto, GCPModule) else None
    if not gcp_module:
        pytest.skip("GCPModule not initialized.")

    # 2. Mock GCS to raise PreconditionFailed
    mock_storage_client = MagicMock()
    mock_publisher_client = MagicMock()
    mock_subscriber_client = MagicMock()
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    # Simulate a PreconditionFailed error (e.g. if_generation_match=0 but file exists)
    mock_blob.create_resumable_upload_session.side_effect = (
        google.api_core.exceptions.PreconditionFailed(
            "At least one of the pre-conditions you specified did not hold."
        )
    )

    # Configure mocks to return strings for Pydantic validation
    mock_publisher_client.topic_path.return_value = (
        "projects/test-project/topics/test-topic"
    )
    mock_subscriber_client.subscription_path.return_value = (
        "projects/test-project/subscriptions/test-sub"
    )

    mock_notification = MagicMock()
    mock_notification.notification_id = "notification-123"
    mock_bucket.notification.return_value = mock_notification

    mock_creds = MagicMock()
    mock_creds.get_service_account_email.return_value = "test-sa@example.com"

    with (
        patch.object(gcp_module, "_storage_client", mock_storage_client),
        patch.object(gcp_module, "_publisher_client", mock_publisher_client),
        patch.object(gcp_module, "_subscriber_client", mock_subscriber_client),
        patch.object(
            gcp_module,
            "_identity",
            {
                "account_email": "test-sa@example.com",
                "project_id": "test-project",
                "region": "europe-west1",
            },
        ),
        patch.object(gcp_module, "_credentials", mock_creds),
    ):
        # Manually sync BucketService with the mocked state
        if gcp_module._bucket_service:
            gcp_module._bucket_service.storage_client = mock_storage_client
            gcp_module._bucket_service.project_id = "test-project"
            gcp_module._bucket_service.region = "europe-west1"

        app = app_lifespan.app
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url=base_url
        ) as client:
            # Pre-cleanup + Create Catalog
            catalogs_proto = get_protocol(CatalogsProtocol)
            if catalogs_proto:
                try:
                    await catalogs_proto.delete_catalog(catalog_id, force=True)
                except Exception:
                    pass
            create_resp = await sysadmin_in_process_client.post("/stac/catalogs", json=catalog_payload)
            assert create_resp.status_code in [201, 200]
            catalog_cleaner(catalog_id)

            # Force catalog into "ready" state with a linked (fake) bucket.
            assert gcp_module is not None
            phys_schema = await catalogs_proto.resolve_physical_schema(catalog_id)
            bucket_name = gcp_module.get_bucket_service().generate_bucket_name(
                catalog_id, physical_schema=phys_schema
            )
            async with _mt(app_lifespan.engine) as db_conn:
                await gcp_db.link_bucket_to_catalog_query.execute(
                    db_conn, catalog_id=catalog_id, bucket_name=bucket_name
                )
            await catalogs_proto.update_provisioning_status(catalog_id, "ready")

            # Initiate Upload
            max_retries = 5
            retry_interval = 0.5
            response = None

            for i in range(max_retries):
                response = await client.post(
                    "/gcp/buckets/init-upload", json=upload_payload
                )
                if response.status_code != 503:
                    break
                logger.info(f"Test: Catalog still provisioning (503), retrying in {retry_interval}s... ({i+1}/{max_retries})")
                await asyncio.sleep(retry_interval)
            
            assert response.status_code == 400, f"Failed after {max_retries} retries: {response.text}"
            assert "Precondition failed" in response.json()["detail"]
            assert "if_generation_match=0" in response.json()["detail"]

            # Wait for background tasks while mocks are active
            from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
            await lifecycle_registry.wait_for_all_tasks()

            print("Precondition Failed Test SUCCESS: Error correctly caught and clarified.")
