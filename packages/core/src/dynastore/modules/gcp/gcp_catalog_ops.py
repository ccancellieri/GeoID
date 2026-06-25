#    Copyright 2026 FAO
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
    provisioning_write_with_retry,
    DQLQuery,
    ResultHandler,
)
from dynastore.models.protocols import (
    ConfigsProtocol,
)
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
        async def teardown_catalog_eventing(self, *args: Any, **kw: Any) -> Any: ...
        async def drop_storage(self, *args: Any, **kw: Any) -> Any: ...

        @property
        def engine(self) -> DbResource: ...

    async def provisioner_is_active(
        self, catalog_id: str, conn: Optional[Any] = None
    ) -> bool:
        """Provisioning-checklist predicate (#1175): does GCP have async setup
        work the new catalog must wait for?

        GCP contributes the ``gcp_bucket`` checklist item only when bucket
        provisioning is enabled for this catalog (``provision_enabled``). Whether
        the host can actually authenticate to GCP is resolved later, in the
        provision task: if credentials are missing the task marks the step
        ``skipped`` (→ catalog becomes ready) rather than ``failed``, so a
        config-enabled-but-unauthorized on-prem deployment is never wedged.

        A read failure is treated as inactive so a config glitch can't block
        catalog readiness.
        """
        try:
            config_mgr = get_protocol(ConfigsProtocol)
            bucket_config = GcpCatalogBucketConfig()
            if config_mgr:
                bucket_config = await config_mgr.get_config(
                    GcpCatalogBucketConfig,
                    catalog_id=catalog_id,
                    ctx=DriverContext(db_resource=conn),
                )
            return bool(bucket_config.provision_enabled)
        except Exception:  # noqa: BLE001 — never block readiness on a config glitch
            logger.warning(
                "GCP Module: provisioner_is_active check failed for catalog '%s'; "
                "treating GCP as inactive.", catalog_id, exc_info=True,
            )
            return False

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
            # Recreation guard (#2298): this teardown runs un-awaited in the
            # background, so the catalog may have been hard-deleted and rapidly
            # recreated under a NEW physical_schema while we were queued. The
            # default Pub/Sub topic name is deterministic per catalog_id
            # (``ds-{catalog_id}-events``), so the new catalog adopts the exact
            # same topic; tearing eventing down here would silently destroy the
            # live catalog's eventing channel. Compare the schema captured at
            # delete time against the one currently registered for this id: a
            # mismatch means the catalog is a different, live instance.
            recreated = False
            try:
                from dynastore.models.protocols import CatalogsProtocol

                catalogs_svc = get_protocol(CatalogsProtocol)
                if catalogs_svc is not None:
                    current_schema = await catalogs_svc.resolve_physical_schema(
                        catalog_id, allow_missing=True
                    )
                    recreated = (
                        current_schema is not None
                        and current_schema != context.physical_schema
                    )
            except Exception as e:
                # Never let the recreation probe abort teardown; default to the
                # original (non-recreated) behaviour on any lookup failure.
                logger.warning(
                    f"Recreation check failed for catalog '{catalog_id}' "
                    f"(treating as not recreated): {e}"
                )

            if recreated:
                logger.warning(
                    f"Catalog '{catalog_id}' was recreated under a new schema "
                    f"(deleted '{context.physical_schema}', current differs) while its "
                    f"async teardown was in flight. Skipping eventing teardown to "
                    f"protect the new catalog's adopted Pub/Sub topic; deleting only "
                    f"the old, orphaned bucket."
                )
                try:
                    await log_warning(
                        catalog_id,
                        "gcp.destroy.recreated",
                        "Catalog recreated during teardown; eventing preserved, "
                        "only the old bucket is removed.",
                    )
                except Exception as e:
                    # The tenant schema may already be dropped; a failed log must
                    # never abort the bucket cleanup that still has to run below.
                    logger.warning(
                        f"Could not record recreation notice for '{catalog_id}': {e}"
                    )

            eventing_data = context.config.get(GcpEventingConfig.class_key())

            if eventing_data and not recreated:
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

            # Belt-and-braces: a catalog whose provisioning crashed before
            # topic_path was persisted leaves an orphan default Pub/Sub topic
            # that the config-driven teardown above skips (it only deletes
            # topic_path when set), or has no eventing config at all. Force-clean
            # the deterministic default topic/subscription by name — this is
            # NotFound-safe and idempotent, so it never double-deletes resources
            # the managed teardown already removed, and guarantees no Pub/Sub
            # resource survives a catalog hard-delete to collide on recreate.
            # Skipped on recreation (#2298): the deterministic topic is now owned
            # by the new, live catalog.
            if not recreated:
                try:
                    await self.teardown_catalog_eventing(catalog_id, config=None)
                except Exception as e:
                    logger.warning(
                        f"Best-effort default eventing cleanup failed for "
                        f"'{catalog_id}' (non-fatal): {e}"
                    )

            # Bucket deletion: always target the OLD catalog's bucket (#2298),
            # never a catalog_id-keyed DB lookup — after recreation that resolves
            # to the NEW catalog's bucket and would delete live data. Prefer the
            # authoritative name persisted on the deleted catalog's config
            # snapshot (correct even for legacy catalogs whose name predates the
            # schema-embedding convention); fall back to reconstructing it from
            # the captured physical_schema. ``drop_storage`` is NotFound-safe and
            # leaves the (recreated) catalog's DB config link untouched.
            bucket_manager = self.get_bucket_service()
            old_bucket_name: Optional[str] = None
            bucket_snapshot = context.config.get(GcpCatalogBucketConfig.class_key())
            if isinstance(bucket_snapshot, dict):
                old_bucket_name = bucket_snapshot.get("bucket_name")
            if not old_bucket_name:
                try:
                    old_bucket_name = bucket_manager.generate_bucket_name(
                        catalog_id, physical_schema=context.physical_schema
                    )
                except Exception:
                    old_bucket_name = None
            logger.info(
                f"Deleting old bucket '{old_bucket_name}' (schema "
                f"'{context.physical_schema}') for catalog '{catalog_id}'..."
            )
            await bucket_manager.drop_storage(
                catalog_id,
                physical_schema=context.physical_schema,
                bucket_name=old_bucket_name,
            )
            await log_info(
                catalog_id,
                "gcp.bucket.deleted",
                f"Bucket {old_bucket_name} deleted."
                if old_bucket_name
                else "Bucket teardown attempted.",
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
            # Bucket existence is (re)checked idempotently inside
            # ``ensure_storage_for_catalog`` below, so we don't read it here.
            existing_eventing_config = await self.get_eventing_config(
                catalog_id, conn=conn, context=context
            )

        # ── Phase 2: GCP API calls (no DB connection held) ──
        # We track provisioned resources to ensure cleanup on ANY failure until DB commit.
        provisioned_bucket = None
        provisioned_topic = None
        # Bound before the try so the cleanup block can reference it even when a
        # failure occurs before the eventing config is resolved below.
        eventing_config = None

        # Distinguishes a genuine orphan (catalog row disappeared mid-provision)
        # from any other failure such as an eventing IAM/Pub/Sub error.
        # Only a genuine orphan allows the bucket to be deleted: the bucket is
        # committed durable state the moment ensure_storage_for_catalog returns,
        # and deleting it on a soft eventing failure produces "ready with no
        # bucket" — the catalog reports ready but every upload fails with a
        # missing bucket error.
        catalog_vanished = False

        try:
            # 2a. Ensure the bucket exists (creates it if needed, returns name)
            # This method already queries the DB (short) then makes GCP calls (no DB)
            bucket_name = await self.get_bucket_service().ensure_storage_for_catalog(
                catalog_id,
                conn=None,  # No connection — manages its own short transaction
                context=context,
                # Provisioning treats a missing bucket as a hard failure: propagate
                # the real GCS / DB exception instead of collapsing it to None and
                # losing the cause behind a generic "Bucket name returned as None".
                raise_on_failure=True,
            )

            # Defensive belt-and-suspenders: with raise_on_failure=True a real
            # failure raises above, so reaching here with None is not expected.
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
                                "gcs_detailed_reporter": {
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
                # Each iteration acquires a fresh connection. provisioning_write_with_retry
                # adds one retry on transient closed-connection / lock-timeout errors so a
                # single dead wire does not abort the whole visibility wait loop.
                async def _check_catalog_exists(conn, _cid=catalog_id):
                    return await _CATALOG_EXISTS_QUERY.execute(conn, catalog_id=_cid)

                catalog_exists = await provisioning_write_with_retry(
                    self.engine, _check_catalog_exists, attempts=2
                )

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
                # Signal that the catalog row is gone so the exception handler
                # knows it is safe to delete the bucket (genuine orphan path).
                catalog_vanished = True
                raise asyncio.CancelledError(f"Catalog {catalog_id} not found during provisioning.")

            # Persist eventing config in a short committed transaction with retry so a
            # connection closed during the preceding GCP API calls does not abort the write.
            async def _write_eventing_config(conn, _cid=catalog_id, _cfg=eventing_config):
                if _cfg.managed_eventing and _cfg.managed_eventing.enabled:
                    saved_config = await self.set_eventing_config(_cid, _cfg, conn=conn)
                    logger.debug(
                        f"saved_config.managed_eventing.topic_path from DB: {saved_config.managed_eventing.topic_path}"
                    )
                return None

            await provisioning_write_with_retry(self.engine, _write_eventing_config)

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

            # ── Bucket cleanup contract ──
            #
            # The bucket is HARD state: once ensure_storage_for_catalog has
            # returned successfully the bucket exists in GCS and its name is
            # committed (or being committed) to the catalog config. Deleting
            # it on any failure other than a genuine catalog-vanish produces the
            # "ready with no bucket" failure mode — the catalog reports ready
            # but every upload fails because the backing bucket is gone, and
            # reprovision just repeats the cycle (recreate bucket → eventing
            # fails → bucket deleted again).
            #
            # Eventing (topic/subscription) is handled the SAME way as the
            # bucket: GCP resources are only torn down on a genuine
            # catalog-vanish (true orphan). On any other failure — including a
            # transient DB error AFTER the topic/subscription were created
            # successfully — they are preserved. Tearing them down here would
            # destroy possibly-working eventing infrastructure and force a
            # reprovision for a failure that was purely transient; reprovision is
            # idempotent (create_topic/subscription adopt AlreadyExists), so
            # leaving them in place lets recovery reconcile without churn. The
            # caller (the provisioning task) classifies the propagated error:
            # transient → retry, permanent → catalog 'failed'. It never reports
            # the catalog 'ready' on a failure here.
            #
            # Therefore, for both bucket and eventing:
            #   • catalog_vanished=True  → genuine orphan; delete the resources.
            #   • catalog_vanished=False → eventing/IAM/DB/other failure; preserve
            #     everything so reprovision can recover without data loss.
            if catalog_vanished:
                if provisioned_bucket:
                    logger.info(
                        f"Cleanup: catalog '{catalog_id}' vanished mid-provision; "
                        f"deleting orphaned bucket '{provisioned_bucket}'."
                    )
                    try:
                        await self.drop_storage(catalog_id)
                    except Exception as cleanup_e:
                        logger.warning(f"Failed to cleanup orphaned bucket: {cleanup_e}")
                if provisioned_topic:
                    logger.info(
                        f"Cleanup: catalog '{catalog_id}' vanished mid-provision; "
                        f"tearing down orphaned eventing topic/channel."
                    )
                    try:
                        if eventing_config and eventing_config.managed_eventing:
                            await self.teardown_managed_eventing_channel(
                                catalog_id, eventing_config.managed_eventing
                            )
                    except Exception as cleanup_e:
                        logger.warning(
                            f"Failed to cleanup orphaned eventing resources: {cleanup_e}"
                        )
            elif provisioned_bucket or provisioned_topic:
                logger.warning(
                    f"Provisioning failure ({type(e).__name__}) for catalog '{catalog_id}' "
                    f"after GCP resources were committed (bucket={provisioned_bucket}, "
                    f"topic={'yes' if provisioned_topic else 'no'}) — resources are "
                    f"preserved and the error is propagated to the provisioning task "
                    f"(transient → retry, permanent → catalog 'failed'). Re-run "
                    f"provisioning via POST /catalog/catalogs/{catalog_id}/reprovision "
                    f"once the underlying cause (e.g. transient DB error, or a missing "
                    f"Pub/Sub IAM grant) is resolved."
                )

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
