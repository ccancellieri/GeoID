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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

import asyncio
import logging
from typing import Optional, Tuple, Any, TYPE_CHECKING

from dynastore.tools.discovery import get_protocol
from dynastore.models.driver_context import DriverContext
from dynastore.modules.db_config.query_executor import (
    DbResource,
    managed_transaction,
    DQLQuery,
    ResultHandler,
)
from dynastore.models.protocols import (
    ConfigsProtocol,
    CatalogsProtocol,
)
from dynastore.modules.gcp import gcp_db
from dynastore.modules.gcp.gcp_config import (
    GcpCatalogBucketConfig,
    GcpEventingConfig,
    ManagedBucketEventing,
    TriggeredAction,
)
from dynastore.modules.gcp.models import PushSubscriptionConfig
from dynastore.modules.catalog.lifecycle_manager import LifecycleContext
from dynastore.modules.catalog.log_manager import log_info, log_error, log_warning

logger = logging.getLogger(__name__)

_CATALOG_EXISTS_QUERY = DQLQuery(
    "SELECT 1 FROM catalog.catalogs WHERE id = :catalog_id AND deleted_at IS NULL",
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
)


def _get_catalog_visibility_tunables():
    """Get current retry tunables from the gcp_module module-level variables."""
    from dynastore.modules.gcp import gcp_module as _mod
    return _mod._CATALOG_VISIBILITY_MAX_RETRIES, _mod._CATALOG_VISIBILITY_RETRY_INTERVAL


class GcpCatalogOpsMixin:
    """Mixin providing catalog lifecycle hooks and GCP resource orchestration for GCPModule."""

    # --- Host interface contract ---
    # These attributes/methods are provided by sibling mixins on ``GCPModule``
    # (``GcpEventingOpsMixin``, ``GcpStorageOpsMixin``). They are declared
    # here ONLY as type hints under TYPE_CHECKING so static analysis still
    # sees them — a previous version used `def foo(self) -> T: ...` bodies,
    # which are real no-op methods and shadowed the sibling implementations
    # via MRO (GCPModule → GcpCatalogOpsMixin → GcpEventingOpsMixin →
    # GcpStorageOpsMixin). That made e.g. ``generate_default_subscription_id``
    # silently return ``None``, breaking ``PushSubscriptionConfig`` with a
    # Pydantic ValidationError during ``gcp_provision``.
    if TYPE_CHECKING:
        def get_bucket_service(self) -> Any: ...
        def get_config_service(self) -> ConfigsProtocol: ...
        async def get_eventing_config(self, *args: Any, **kw: Any) -> Any: ...
        async def set_eventing_config(self, *args: Any, **kw: Any) -> Any: ...
        def generate_default_subscription_id(self, *args: Any, **kw: Any) -> str: ...
        async def setup_managed_eventing_channel(self, *args: Any, **kw: Any) -> Any: ...
        async def teardown_managed_eventing_channel(self, *args: Any, **kw: Any) -> Any: ...
        async def delete_storage_for_catalog(self, *args: Any, **kw: Any) -> Any: ...

        @property
        def engine(self) -> DbResource: ...

    async def _on_sync_init_catalog(self, conn: DbResource, physical_schema: str, catalog_id: str):
        """
        Sync hook to initialize GCP resources (Bucket, Eventing) when a catalog is created.
        Executed within the catalog creation transaction.
        """
        try:
            logger.info(
                f"GCP Module: Sync initialization for catalog '{catalog_id}' started."
            )

            # 1. Resolve configuration (with defaults if missing)
            # Since this is a sync hook, we don't have LifecycleContext.config pre-calculated.
            # We fetch it from the DB using the hierarchical get_config.
            from dynastore.models.protocols.configs import ConfigsProtocol
            bucket_config = GcpCatalogBucketConfig() # Default

            config_mgr = get_protocol(ConfigsProtocol)
            if config_mgr:
                # get_config follows the waterfall: Collection -> Catalog -> Platform -> Defaults
                bucket_config = await config_mgr.get_config(GcpCatalogBucketConfig, catalog_id=catalog_id, ctx=DriverContext(db_resource=conn))

            # 2. Check if provisioning is enabled for this catalog
            if not bucket_config.enabled:
                logger.info(
                    f"GCP Module: Provisioning disabled for catalog '{catalog_id}' via configuration. Linking bucket name and marking ready."
                )

                # Mark catalog as ready immediately
                from dynastore.models.protocols import CatalogsProtocol
                catalogs_svc = get_protocol(CatalogsProtocol)
                if catalogs_svc:
                    await catalogs_svc.update_provisioning_status(catalog_id, "ready", ctx=DriverContext(db_resource=conn))

                # Link the deterministic bucket name in the DB to allow uploads
                from dynastore.modules.gcp import gcp_db
                bucket_name = self.get_bucket_service().generate_bucket_name(catalog_id, physical_schema=physical_schema)
                # Catalog existence check is not strictly needed here as the lifecycle hook
                # only runs if the catalog was successfully created.
                await gcp_db.link_bucket_to_catalog_query.execute(
                    conn, catalog_id=catalog_id, bucket_name=bucket_name
                )
                return

            # Log to Tenant Log
            try:
                from dynastore.modules.catalog.log_manager import log_info
                await log_info(
                    catalog_id, "gcp.init.start", "Starting GCP resource initialization.", db_resource=conn
                )
            except Exception as e:
                logger.debug(f"Could not log init start for catalog '{catalog_id}' (it might be gone): {e}")

            try:
                from dynastore.modules.tasks.models import TaskCreate
                from dynastore.modules.tasks.tasks_module import create_task
                from dynastore.modules.catalog.event_service import CatalogEventType

                task_request = TaskCreate(
                    task_type="gcp_provision_catalog",
                    inputs={"catalog_id": catalog_id},
                    caller_id="system",
                    type="task",
                    # originating_event lets CatalogModule route rollback without knowing about GCP
                    extra_context={"originating_event": CatalogEventType.CATALOG_CREATION},
                )

                # Enqueue the task within the transaction
                await create_task(conn, task_request, physical_schema)
                logger.info(
                    f"GCP Module: Provisioning task enqueued for catalog '{catalog_id}'."
                )

            except Exception as e:
                logger.error(
                    f"GCP Module: Failed to enqueue provisioning task for '{catalog_id}': {e}",
                    exc_info=True,
                )
                raise

        except Exception as e:
            logger.error(
                f"GCP Module: Sync initialization for catalog '{catalog_id}' failed: {e}",
                exc_info=True,
            )
            # We don't necessarily want to fail the whole catalog creation if GCP setup fails to QUEUE,
            # but usually this implies a DB error which would fail the TX anyway.
            from dynastore.modules.catalog.log_manager import log_error
            try:
                await log_error(
                    catalog_id, "gcp.init.failure", f"GCP initialization failed: {str(e)}", db_resource=conn
                )
            except Exception as log_e:
                logger.debug(f"Could not log init failure for catalog '{catalog_id}': {log_e}")
            raise

    async def _on_async_destroy_catalog(
        self, catalog_id: str, context: LifecycleContext
    ):
        """
        Async hook to tear down GCP resources when a catalog is hard-deleted.
        """
        logger.info(
            f"GCP Module: Async destruction for catalog '{catalog_id}' started."
        )
        # Note: We cannot log to Tenant Logs here if the schema is already dropped.
        # But we can log to System Logs using the same function (it handles fallback).
        await log_info(
            catalog_id, "gcp.destroy.start", "Starting GCP resource teardown."
        )

        try:
            eventing_data = context.config.get(GcpEventingConfig.class_key())

            if eventing_data:
                try:
                    eventing_config = GcpEventingConfig.model_validate(eventing_data)

                    if (
                        isinstance(eventing_config, GcpEventingConfig)
                        and eventing_config.managed_eventing
                    ):
                        logger.info(
                            f"Tearing down managed eventing for catalog '{catalog_id}'."
                        )
                        await self.teardown_managed_eventing_channel(
                            catalog_id, eventing_config.managed_eventing
                        )
                        await log_info(
                            catalog_id,
                            "gcp.eventing.teardown",
                            "Managed eventing torn down.",
                        )

                except Exception as e:
                    logger.error(
                        f"Failed to teardown eventing for catalog '{catalog_id}': {e}"
                    )
                    await log_warning(
                        catalog_id,
                        "gcp.eventing.failure",
                        f"Eventing teardown failed: {e}",
                    )

            # Bucket deletion logic (optional/configurable) would go here
            # For now, we force delete the bucket if it exists to satisfy the lifecycle contract.
            # In a production system, we might want a 'retain_bucket' flag in the config.
            bucket_manager = self.get_bucket_service()
            bucket_name = await bucket_manager.get_storage_identifier(catalog_id)
            if bucket_name:
                logger.info(
                    f"Deleting bucket '{bucket_name}' for catalog '{catalog_id}'..."
                )
                await bucket_manager.delete_storage_for_catalog(catalog_id)
                await log_info(
                    catalog_id, "gcp.bucket.deleted", f"Bucket {bucket_name} deleted."
                )
            else:
                logger.warning(
                    f"No bucket found for catalog '{catalog_id}' during teardown."
                )

            await log_info(
                catalog_id, "gcp.destroy.success", "GCP resource teardown completed."
            )
            logger.info(
                f"GCP Module: Async destruction for catalog '{catalog_id}' completed."
            )

        except Exception as e:
            logger.error(
                f"GCP Module: Async destruction for catalog '{catalog_id}' failed: {e}",
                exc_info=True,
            )
            await log_error(
                catalog_id, "gcp.destroy.failure", f"GCP teardown failed: {e}"
            )

    async def _on_async_init_collection(
        self, catalog_id: str, collection_id: str, context: LifecycleContext
    ):
        # GCP doesn't have explicit collection resources (just folders).
        # Eventing is catalog-level.
        # Nothing critical to do here yet.
        pass

    async def _on_async_destroy_collection(
        self,
        catalog_id: str,
        collection_id: str,
        context: LifecycleContext,
    ):
        # GCP doesn't have explicit collection resources (just folders).
        # Eventing is catalog-level.
        # Nothing critical to do here yet.
        pass

    async def setup_catalog_gcp_resources(
        self, catalog_id: str, context: Optional[LifecycleContext] = None
    ) -> Tuple[str, GcpEventingConfig]:
        """
        High-level orchestrator to ensure all necessary GCP resources for a catalog
        (bucket, eventing) are created just-in-time. This method is idempotent.

        IMPORTANT: This method deliberately does NOT hold a single DB transaction open
        across GCP API calls (bucket creation, Pub/Sub, IAM, GCS notifications).
        asyncpg will close idle connections, causing ConnectionDoesNotExistError if
        a long-running gRPC call is made while a connection is held open.
        """
        if not self.engine:
            raise RuntimeError("Database engine not available in GCPModule.")

        # ── Phase 1: DB reads (short transaction, released before any GCP API call) ──
        async with managed_transaction(self.engine) as conn:
            existing_bucket_name = await gcp_db.get_bucket_for_catalog_query.execute(
                conn, catalog_id=catalog_id
            )
            existing_eventing_config = await self.get_eventing_config(
                catalog_id, conn=conn, context=context
            )

        # ── Phase 2: GCP API calls (no DB connection held) ──
        # We track provisioned resources to ensure cleanup on ANY failure until DB commit.
        provisioned_bucket = None
        provisioned_topic = None

        try:
            # 2a. Ensure the bucket exists (creates it if needed, returns name)
            # This method already queries the DB (short) then makes GCP calls (no DB)
            bucket_name = await self.get_bucket_service().ensure_storage_for_catalog(
                catalog_id,
                conn=None,  # No connection — manages its own short transaction
                context=context,
            )

            # CRITICAL CHECK: ensure storage was actually provisioned
            if bucket_name is None:
                 msg = f"Failed to provision storage for catalog '{catalog_id}': Bucket name returned as None."
                 logger.error(msg)
                 raise RuntimeError(msg)

            provisioned_bucket = bucket_name

            # 2b. Determine the eventing config to apply
            eventing_config = existing_eventing_config
            if eventing_config is None:
                logger.info(
                    f"No GcpEventingConfig found for catalog '{catalog_id}'. Creating default managed eventing system with ingestion template."
                )
                default_ingestion_template = TriggeredAction(
                    process_id="ingestion",
                    execute_request_template={
                        "catalog_id": "{catalog_id}",
                        "collection_id": "{collection_id}",
                        "ingestion_request": {
                            "database_batch_size": 1000,
                            "asset": {
                                "asset_id": "{asset_code}",
                                "uri": "gs://{bucket_id}/{object_id}",
                            },
                            "reporting": {
                                "gcs_detailed": {
                                    "enabled": True,
                                    "report_file_path": "gs://{bucket_id}/ingestion_reports/report_{asset_code}.json",
                                }
                            },
                            "column_mapping": {
                                "external_id": "CODE",
                                "attributes_source_type": "all",
                            },
                        },
                    },
                )
                eventing_config = GcpEventingConfig(
                    managed_eventing=ManagedBucketEventing(enabled=True),
                    action_templates={"ingestion": default_ingestion_template},
                )

            # 2c. Setup Topic and Notifications if managed eventing is enabled
            if (
                eventing_config.managed_eventing
                and eventing_config.managed_eventing.enabled
            ):
                eventing_config.managed_eventing.subscription = PushSubscriptionConfig(
                    subscription_id=self.generate_default_subscription_id(catalog_id)
                )
                updated_managed_eventing = await self.setup_managed_eventing_channel(
                    catalog_id,
                    eventing_config.managed_eventing,
                    bucket_name=bucket_name,
                    context=context,
                )
                logger.debug(
                    f"updated_managed_eventing.topic_path being saved: {updated_managed_eventing.topic_path}"
                )
                eventing_config.managed_eventing = updated_managed_eventing
                provisioned_topic = updated_managed_eventing.topic_path

            # ── Phase 3: DB writes (short transaction, after all GCP API calls complete) ──
            max_retries, retry_interval = _get_catalog_visibility_tunables()
            catalog_exists = None
            for attempt in range(max_retries):
                # Use a fresh connection/transaction for each check to avoid snapshot isolation issues
                async with managed_transaction(self.engine) as conn:
                    catalog_exists = await _CATALOG_EXISTS_QUERY.execute(conn, catalog_id=catalog_id)

                if catalog_exists:
                    break

                logger.warning(
                    f"Catalog '{catalog_id}' not visible yet "
                    f"(attempt {attempt + 1}/{max_retries}). Retrying in {retry_interval}s..."
                )
                await asyncio.sleep(retry_interval)

            if not catalog_exists:
                logger.warning(
                    f"Catalog '{catalog_id}' not found or deleted during GCP resource provisioning. Aborting DB registration and triggering teardown."
                )
                # This will trigger the catch-all cleanup in 'finally' below
                raise asyncio.CancelledError(f"Catalog {catalog_id} not found during provisioning.")

            async with managed_transaction(self.engine) as conn:
                # Persist eventing config if managed eventing was set up
                if (
                    eventing_config.managed_eventing
                    and eventing_config.managed_eventing.enabled
                ):
                    saved_config = await self.set_eventing_config(
                        catalog_id, eventing_config, conn=conn
                    )
                    logger.debug(
                        f"saved_config.managed_eventing.topic_path from DB: {saved_config.managed_eventing.topic_path}"
                    )

            # SUCCESS - Resources committed to DB. Clear provisioning tracking.
            provisioned_bucket = None
            provisioned_topic = None
            return bucket_name, eventing_config

        except BaseException as e:
            # Catch-all cleanup for Phase 2/3 failures (including CancelledError and Exception)
            if not isinstance(e, (Exception, asyncio.CancelledError)):
                # Re-raise immediately for things like SystemExit, KeyboardInterrupt
                raise

            logger.error(f"GCP Provisioning failed for catalog '{catalog_id}': {e}")

            # Orphaned resource cleanup
            if provisioned_bucket:
                logger.info(f"Cleanup: Deleting orphaned bucket {provisioned_bucket}...")
                try:
                    await self.delete_storage_for_catalog(catalog_id)
                except Exception as cleanup_e:
                    logger.warning(f"Failed to cleanup orphaned bucket: {cleanup_e}")

            if provisioned_topic:
                logger.info(f"Cleanup: Tearing down orphaned eventing topic/channel...")
                try:
                    # We need the config object to teardown topics/subscriptions
                    # Ensure eventing_config is defined and has managed_eventing
                    if eventing_config and eventing_config.managed_eventing:
                        await self.teardown_managed_eventing_channel(catalog_id, eventing_config.managed_eventing)
                except Exception as cleanup_e:
                    logger.warning(f"Failed to cleanup orphaned eventing resources: {cleanup_e}")

            # Re-raise to let the caller handle the failure
            raise

    async def get_catalog_bucket_config(
        self, catalog_id: str
    ) -> Optional[GcpCatalogBucketConfig]:
        """Internal helper to fetch and parse a catalog's bucket config."""
        config_service = self.get_config_service()
        config = await config_service.get_config(
            GcpCatalogBucketConfig, catalog_id
        )
        return config if isinstance(config, GcpCatalogBucketConfig) else None

    async def set_catalog_bucket_config(
        self, catalog_id: str, config: GcpCatalogBucketConfig
    ) -> GcpCatalogBucketConfig:
        """Persists the bucket configuration for a catalog."""
        config_service = self.get_config_service()
        await config_service.set_config(
            GcpCatalogBucketConfig, config, catalog_id=catalog_id
        )
        return await config_service.get_config(GcpCatalogBucketConfig, catalog_id)

    async def apply_storage_config(
        self, catalog_id: str, config: GcpCatalogBucketConfig
    ):
        """StorageProtocol: Applies bucket configuration changes (CORS, Lifecycle)."""
        bucket_manager = self.get_bucket_service()
        await bucket_manager.update_bucket_config(catalog_id, config)
