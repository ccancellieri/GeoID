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

import logging
import asyncio
import os
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, ClassVar, Optional, AsyncIterator, Dict, Any, List

if TYPE_CHECKING:
    from google.api_core import exceptions as google_exceptions
    from google.api_core.exceptions import Aborted
    from google.cloud import storage, pubsub_v1, run_v2
else:
    try:
        from google.api_core import exceptions as google_exceptions
        from google.api_core.exceptions import Aborted
    except ImportError:
        google_exceptions = None
        Aborted = None
from dynastore.tools.cache import cached
from dynastore.modules import ModuleProtocol
from dynastore.modules.concurrency import run_in_thread
from dynastore.tools.discovery import get_protocol
from dynastore.modules.db_config.query_executor import (
    DbResource,
    DQLQuery,
    ResultHandler,
)
from dynastore.models.protocols import (
    ConfigsProtocol,
    StorageProtocol,
    JobExecutionProtocol,
    CloudStorageClientProtocol,
    CloudIdentityProtocol,
    EventingProtocol,
    AssetUploadProtocol,
)
from dynastore.modules.gcp.tools.service_account import get_credentials
from dynastore.modules.gcp.gcp_config import (
    GcpModuleConfig,
)
from dynastore.modules.processes.protocols import ProcessRegistryProtocol

if not TYPE_CHECKING:
    try:
        from google.cloud import storage
    except ImportError:
        storage = None
    try:
        from google.cloud import pubsub_v1
    except ImportError:
        pubsub_v1 = None
    try:
        from google.cloud import run_v2
    except ImportError:
        run_v2 = None
from dynastore.modules.gcp.bucket_service import BucketService
from .gcp_catalog_ops import GcpCatalogOpsMixin
from .gcp_eventing_ops import GcpEventingOpsMixin
from .gcp_storage_ops import GcpStorageOpsMixin

logger = logging.getLogger(__name__)


def _task_type_from_scope_token(token: str) -> Optional[str]:
    """Extract a task entry-point name from a Cloud Run Job's SCOPE env token.

    Handles both the current canonical form and the legacy hyphenated form so
    jobs deployed before the ``worker_task_*`` rename can still be discovered.

    Canonical  : ``worker_task_gdal``           → ``gdal``
    Legacy     : ``task-gdal-job``              → ``gdal``
                 ``task_export_features_job``   → ``export_features``
    Infra noise: ``task_base``, ``core``, …     → ``None`` (skip)
    """
    # Canonical form: worker_task_<name>
    if token.startswith("worker_task_"):
        name = token[len("worker_task_"):]
        return name or None

    # Legacy form: task[-_]<name>[-_]job (normalize separators first)
    normalized = token.replace("-", "_").lower()
    if normalized.startswith("task_") and normalized.endswith("_job"):
        inner = normalized[len("task_"):-len("_job")]
        return inner or None

    return None


# ---------------------------------------------------------------------------
# Provisioning retry tunables
# ---------------------------------------------------------------------------
# Provisioning retry tunables - will be initialized from GcpModuleConfig if available
_CATALOG_VISIBILITY_MAX_RETRIES: int = 20
_CATALOG_VISIBILITY_RETRY_INTERVAL: float = 0.2
_CATALOG_EXISTS_QUERY = DQLQuery(
    "SELECT 1 FROM catalog.catalogs WHERE id = :catalog_id AND deleted_at IS NULL",
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
)

# ---------------------------------------------------------------------------
# GCS retry and per-bucket circuit-breaker tunables
# ---------------------------------------------------------------------------
# Updated from GcpModuleConfig during lifespan startup (hot-reload on restart).
# Read at call time via gcs_retry._get_gcs_retry_tunables().
_GCS_RETRY_MAX_ATTEMPTS: int = 3
_GCS_BREAKER_FAILURE_THRESHOLD: int = 5
_GCS_BREAKER_COOLDOWN_SECONDS: float = 30.0

class GCPModule(
    GcpCatalogOpsMixin,
    GcpEventingOpsMixin,
    GcpStorageOpsMixin,
    ModuleProtocol,
    StorageProtocol,
    JobExecutionProtocol,
    CloudStorageClientProtocol,
    CloudIdentityProtocol,
    EventingProtocol,
    AssetUploadProtocol,
    ProcessRegistryProtocol,
):
    _credentials: Optional[Any] = None
    _identity: Optional[Dict[str, Any]] = None
    _engine: Optional[DbResource] = None
    _config_service: Optional[ConfigsProtocol] = None
    _module_config: Optional[GcpModuleConfig] = None
    # Per-bucket circuit breaker for GCS operations.  Created in lifespan with
    # thresholds from GcpModuleConfig; None until lifespan runs (e.g. in tests
    # that instantiate GCPModule without running lifespan).
    _gcs_breaker: Optional[Any] = None
    # In-memory upload ticket store: ticket_id → {asset_id, catalog_id, collection_id, expires_at}
    _upload_tickets: ClassVar[Dict[str, Dict[str, Any]]] = {}

    priority: int = 30  # Priority for protocol implementation (lower means higher priority)

    @property
    def engine(self) -> DbResource:
        if self._engine:
            return self._engine
        from dynastore.tools.protocol_helpers import get_engine
        eng = get_engine()
        assert eng is not None, "No DB engine available"
        return eng

    @engine.setter
    def engine(self, value: DbResource):
        self._engine = value

    ################################################
    # Synchronous clients
    _storage_client: Optional["storage.Client"] = None
    _publisher_client: Optional["pubsub_v1.PublisherClient"] = None
    _subscriber_client: Optional["pubsub_v1.SubscriberClient"] = None

    ################################################
    # Asynchronous clients (lazy; bound to the running event loop on first
    # use — see get_jobs_client/get_run_client). Must NOT be created from
    # sync __init__, because google-api-core grpc.aio channels capture
    # ``asyncio.get_event_loop()`` at construction time. If that loop is a
    # throwaway (as happens during module discovery before uvicorn starts),
    # every later await from the lifespan loop raises
    # ``RuntimeError: got Future ... attached to a different loop``.
    # Mirrors the loop-rebind pattern used by
    # :class:`dynastore.tools.async_utils.WaitableSignal`.
    _jobs_client: Optional["run_v2.JobsAsyncClient"] = None
    _run_client: Optional["run_v2.ServicesAsyncClient"] = None
    _executions_client: Optional["run_v2.ExecutionsAsyncClient"] = None
    _async_clients_loop: Optional[asyncio.AbstractEventLoop] = None
    _bucket_service: Optional[BucketService] = None
    # Background reconciler that probes lapsed-lease Cloud Run task rows for
    # real execution liveness (#735). Started in lifespan, gated by
    # _should_register_gcp_job_runner(); None on hosts that don't run it.
    _liveness_reconciler: Optional[Any] = None

    def __init__(self, app_state: object) -> None:
        logger.info("GCPModule.__init__: START")
        super().__init__()
        logger.info("GCPModule.__init__: identifying active GCP credentials...")

        # Stage 1: credentials. If ADC is genuinely unavailable the module
        # stays disabled and every getter raises a clear RuntimeError.
        try:
            self._credentials, self._identity = get_credentials()
            logger.info(
                "GCPModule.__init__: credentials OK (identity=%s).",
                self.get_account_email(),
            )
        except Exception as e:
            logger.warning(
                f"GCPModule.__init__: no usable GCP credentials ({e}). "
                "Module disabled; clients will remain unavailable until credentials "
                "are provided. Module stays REGISTERED as StorageProtocol provider — "
                "runtime calls will raise clear RuntimeError, not silent None."
            )
            self._credentials = None
            self._identity = None
            logger.info("GCPModule.__init__: END (credentials missing, disabled)")
            return

        # Stage 2: clients. A failure here is recoverable — keep the valid
        # credentials so get_jobs_client() / get_run_client() can lazily retry
        # on first use instead of raising an opaque "not available" error.
        try:
            self.reinitialize_clients()
        except Exception as e:
            logger.error(
                f"GCPModule.__init__: sync clients failed ({e}). "
                "Module stays registered; async clients will still build lazily on "
                "first use.",
                exc_info=True,
            )
        logger.info("GCPModule.__init__: END (registered with StorageProtocol)")

    def reinitialize_clients(self) -> None:
        """
        (Re)instantiates shared Google Cloud clients using current credentials.
        """
        # Defensive: when google-cloud-storage / google-cloud-pubsub aren't
        # installed (SCOPE excludes module_gcp), the conditional imports at the
        # top of this file leave ``storage`` / ``pubsub_v1`` as None. Calling
        # ``storage.Client(...)`` would then crash lifespan with
        # ``AttributeError: 'NoneType' object has no attribute 'Client'``,
        # which silently takes BigQueryService registration with it. Bail
        # cleanly so other modules' lifespans still run.
        if storage is None or pubsub_v1 is None:
            logger.warning(
                "GCPModule.reinitialize_clients: google-cloud-storage/pubsub "
                "not installed; storage/BigQuery clients will be unavailable. "
                "Add module_gcp to this service's SCOPE if it needs them."
            )
            self._storage_client = None
            self._publisher_client = None
            self._subscriber_client = None
            return
        project_id = self.get_project_id()
        if self._credentials:
            logger.info(
                f"Instantiating shared Google Cloud clients for project: {project_id or 'default'}"
            )
            # Pass credentials explicitly to ensure consistent identity
            self._storage_client = storage.Client(
                project=project_id, credentials=self._credentials
            )
            self._publisher_client = pubsub_v1.PublisherClient(
                credentials=self._credentials
            )
            self._subscriber_client = pubsub_v1.SubscriberClient(
                credentials=self._credentials
            )

            # Async clients are built lazily in get_jobs_client/get_run_client/
            # get_executions_client on the running event loop. Reinitializing
            # sync clients invalidates any previously-bound async clients so the
            # next accessor rebuilds them against fresh credentials.
            self._jobs_client = None
            self._run_client = None
            self._executions_client = None
            self._async_clients_loop = None

            # Update BucketService if it exists
            if self._bucket_service:
                self._bucket_service.storage_client = self._storage_client
                if project_id:
                    self._bucket_service.project_id = project_id
                region = self.get_region()
                if region:
                    self._bucket_service.region = region
        else:
            logger.warning("No credentials available to reinitialize clients.")

    """
    A foundational module to manage GCP credentials and state for the application.
    """

    @asynccontextmanager
    async def lifespan(self, app_state: object) -> AsyncIterator[None]:
        logger.info("GCP Module: Entering lifespan - initializing clients.")

        # 1. Retrieve ConfigManager and Global Module Config
        self._config_service = get_protocol(ConfigsProtocol)
        if self._config_service:
            cfg = await self._config_service.get_config(GcpModuleConfig)
            self._module_config = cfg if isinstance(cfg, GcpModuleConfig) else None
            
            # Synchronize local tunables with global config
            if self._module_config:
                global _CATALOG_VISIBILITY_MAX_RETRIES, _CATALOG_VISIBILITY_RETRY_INTERVAL
                _CATALOG_VISIBILITY_MAX_RETRIES = self._module_config.catalog_visibility_max_retries
                _CATALOG_VISIBILITY_RETRY_INTERVAL = self._module_config.catalog_visibility_retry_interval
                # Sync GCS retry tunables and rebuild the per-bucket circuit breaker.
                global _GCS_RETRY_MAX_ATTEMPTS, _GCS_BREAKER_FAILURE_THRESHOLD, _GCS_BREAKER_COOLDOWN_SECONDS
                _GCS_RETRY_MAX_ATTEMPTS = self._module_config.gcs_retry_max_attempts
                _GCS_BREAKER_FAILURE_THRESHOLD = self._module_config.gcs_breaker_failure_threshold
                _GCS_BREAKER_COOLDOWN_SECONDS = self._module_config.gcs_breaker_cooldown_seconds
                from dynastore.modules.storage.circuit_breaker import CircuitBreaker as _GcsCB
                self._gcs_breaker = _GcsCB(
                    failure_threshold=_GCS_BREAKER_FAILURE_THRESHOLD,
                    cooldown_seconds=_GCS_BREAKER_COOLDOWN_SECONDS,
                )
        else:
            logger.warning(
                "GCP Module: ConfigsProtocol not available. Global settings will use defaults/env."
            )
            self._module_config = GcpModuleConfig()

        # 2. Ensure synchronous clients are open (re-open if closed from previous lifespan)
        self.reinitialize_clients()

        # Retrieve ConfigsProtocol via dm.get_protocol
        self._config_service = get_protocol(ConfigsProtocol)
        if not self._config_service:
            logger.warning(
                "GCP Module: ConfigsProtocol not available. Configuration management disabled."
            )

        from dynastore.tools.background_service import (
            BackgroundSupervisor,
            ServiceContext as _GcpServiceContext,
        )
        from dynastore.modules.db_config.instance import (
            get_service_name as _gcp_get_service_name,
        )
        _gcp_bg_shutdown = asyncio.Event()
        _gcp_supervisor = BackgroundSupervisor()

        try:
            # Async clients (_jobs_client/_run_client) are built lazily by
            # get_jobs_client/get_run_client on first use, bound to the current
            # event loop. No eager reinit here — it would re-bind them to the
            # lifespan loop pre-emptively and defeats the loop-id guard.

            # Initialize BucketService (Safe even if clients are None)
            self._bucket_service = BucketService(
                engine=self._engine, # Explicitly pass current engine check
                config_service=self._config_service,
                storage_client=self._storage_client,  # type: ignore[arg-type]
                project_id=self.get_project_id() or "",
                region=self.get_region() or "",
                breaker=self._gcs_breaker,
            )

            # The GCP module no longer owns any database schema: the catalog →
            # bucket-name link lives on ``GcpCatalogBucketConfig.bucket_name``
            # (config-backed), and the dead ``gcp.reconciliation_events`` table
            # has been retired. There is no ``gcp`` schema to initialize.

            # --- Register Lifecycle Hooks ---
            # (No post-create lifecycle hook: both provisioning paths are now
            # registered as provisioner checklist steps — gcp_config for
            # provision_enabled=False and gcp_bucket/gcp_eventing for
            # provision_enabled=True.  The old _on_post_create_catalog hook is
            # removed to eliminate the double-provision trap it created.)

            # #1175: register GCP as a catalog provisioner so bucket setup is a
            # checklist step the catalog waits on, instead of GCP directly owning
            # ``provisioning_status``. Both 'gcp_bucket' (hard) and 'gcp_eventing'
            # (soft/best-effort) steps are contributed only when provisioning is
            # enabled (see ``provisioner_is_active``). The provision task marks each
            # terminal: bucket → complete/skipped/failed; eventing → complete/degraded.
            #
            # All three GCP steps are registered ``deferrable=True``: by default
            # they still run at catalog creation (no behaviour change), but a
            # create with ``?hints=defer`` holds them back so the catalog is
            # created (tenant schema only, via catalog_core) and can be
            # configured — e.g. GcpCatalogBucketConfig.provision_enabled set to
            # false for a records-only catalog — before any GCS bucket is created.
            # On a deferred create the catalog reaches ``ready`` on catalog_core
            # alone, bucket-free; an explicit ``catalog_provision`` task (spawned
            # via POST /task/catalogs/{id}) then runs these steps, each still
            # gated by ``provisioner_is_active``/``provision_enabled``.
            # A 'degraded' eventing step does NOT block readiness — see
            # ``evaluate_checklist`` and the fail-soft eventing path in
            # tasks/gcp_provision/task.py.
            #
            # 'gcp_config' (priority 1) runs whenever GCP is installed AND
            # provision_enabled=False.  It persists the deterministic bucket name
            # onto GcpCatalogBucketConfig so uploads can resolve it without a real
            # GCS bucket.  When provision_enabled=True this step is skipped (not in
            # the checklist) because gcp_bucket's ensure_storage_for_catalog already
            # writes bucket_name in its Phase 3 _write_bucket_name call.
            from dynastore.modules.catalog.provisioning_registry import (
                provisioning_registry,
                SCOPE_CATALOG,
            )

            async def _gcp_config_is_active(catalog_id: str, conn=None) -> bool:
                """Active when GCP is installed AND provision_enabled=False.

                This is the logical complement of provisioner_is_active: the
                gcp_config step runs exactly when the real bucket/eventing steps
                do NOT, ensuring bucket_name is always persisted on the async path
                regardless of whether provisioning is enabled.
                """
                return not await self.provisioner_is_active(catalog_id, conn)

            async def _gcp_config_provision(
                catalog_id: str,
                external_id=None,
                scope: str = "catalog",
                operation: str = "provision",
                collection_id=None,
                **_kw,
            ) -> None:
                """Persist the deterministic bucket name when provision_enabled=False.

                Writes GcpCatalogBucketConfig.bucket_name via the provisioner
                path so that the value is recorded even when full GCS bucket
                provisioning is disabled.

                The catalog schema already exists when this hook runs because
                catalog_core (priority 0) completes before this group (priority 1).
                """
                from dynastore.models.protocols.configs import ConfigsProtocol
                from dynastore.modules.db_config.query_executor import managed_transaction
                from dynastore.modules.gcp.gcp_config import GcpCatalogBucketConfig
                from dynastore.models.driver_context import DriverContext

                config_mgr = get_protocol(ConfigsProtocol)
                if config_mgr is None:
                    logger.warning(
                        "gcp_config provisioner: ConfigsProtocol not available for catalog '%s'; "
                        "skipping bucket-name persistence.",
                        catalog_id,
                    )
                    return

                engine = self._engine
                if engine is None:
                    raise RuntimeError(
                        f"gcp_config provisioner: DB engine not available for catalog '{catalog_id}'"
                    )

                async with managed_transaction(engine) as conn:
                    bucket_config = await config_mgr.get_config(
                        GcpCatalogBucketConfig,
                        catalog_id=catalog_id,
                        ctx=DriverContext(db_resource=conn),
                    )
                    bucket_name = self.get_bucket_service().generate_bucket_name(
                        catalog_id, physical_schema=catalog_id
                    )
                    bucket_config.bucket_name = bucket_name
                    await config_mgr.set_config(
                        GcpCatalogBucketConfig,
                        bucket_config,
                        catalog_id=catalog_id,
                        check_immutability=False,
                        ctx=DriverContext(db_resource=conn),
                    )
                logger.info(
                    "gcp_config provisioner: bucket name '%s' persisted for catalog '%s'.",
                    bucket_name, catalog_id,
                )

            provisioning_registry.register(
                "gcp_config",
                _gcp_config_is_active,
                priority=1,
                scope=SCOPE_CATALOG,
                deferrable=True,
                name="GCP bucket config link",
                description=(
                    "Persists the deterministic GCS bucket name onto GcpCatalogBucketConfig "
                    "when provision_enabled=False, so uploads can resolve the bucket without "
                    "a real GCS provisioning step."
                ),
                provision=_gcp_config_provision,
            )

            async def _gcp_bucket_provision(
                catalog_id: str,
                external_id=None,
                scope: str = "catalog",
                operation: str = "provision",
                collection_id=None,
                **_kw,
            ) -> None:
                """Provision the GCS bucket for a catalog.

                Called by CatalogProvisionTask via call_hook(**ctx).  Delegates
                to the same _provision_bucket_hard helper used by the standalone
                gcp_provision_catalog task so the idempotency and error-handling
                contract is identical.  CatalogProvisionTask marks the
                gcp_bucket checklist step after this hook returns successfully.
                """
                from dynastore.tasks.gcp_provision.task import _provision_bucket_hard, _get_storage_protocol

                storage = _get_storage_protocol()
                await _provision_bucket_hard(storage, catalog_id)

            async def _gcp_eventing_provision(
                catalog_id: str,
                external_id=None,
                scope: str = "catalog",
                operation: str = "provision",
                collection_id=None,
                **_kw,
            ) -> None:
                """Set up managed eventing for a catalog.

                Called by CatalogProvisionTask via call_hook(**ctx) after
                gcp_bucket completes.  Delegates to _provision_eventing which
                encodes the skip (managed_eventing disabled) / permanent-fail
                (IAM) / transient-fail contract.

                When managed_eventing is disabled, _provision_eventing returns
                'skipped'.  Both 'complete' and 'skipped' are terminal-good per
                evaluate_checklist, so the executor marking this step 'complete'
                (its default on a successful hook) is acceptable — the catalog
                still reaches 'ready'.  Operator visibility of the 'skipped'
                distinction is a follow-up concern; the correctness invariant
                (catalog becomes ready, no step left pending) holds either way.
                """
                from dynastore.tasks.gcp_provision.task import _provision_eventing, _get_storage_protocol

                storage = _get_storage_protocol()
                await _provision_eventing(storage, catalog_id)
                # executor marks gcp_eventing 'complete' after this returns

            async def _gcp_bucket_deprovision(
                catalog_id: str,
                external_id=None,
                scope: str = "catalog",
                operation: str = "deprovision_hard",
                collection_id=None,
                config_snapshot=None,
                **_kw,
            ) -> None:
                """Deprovision the GCS bucket for a catalog (#2340).

                Called by CatalogProvisionTask with operation='deprovision_hard'.
                Deletes the bucket using the pre-captured config snapshot
                (passed via task inputs) so cleanup works even though
                catalog_core already dropped the schema.

                For the PostgreSQL driver, catalog_id IS the physical schema name.

                Idempotent: drop_storage is NotFound-safe.
                """
                from dynastore.modules.gcp.gcp_config import GcpCatalogBucketConfig

                bucket_manager = self.get_bucket_service()
                old_bucket_name: Optional[str] = None

                if isinstance(config_snapshot, dict):
                    bucket_snapshot = config_snapshot.get(GcpCatalogBucketConfig.class_key())
                    if isinstance(bucket_snapshot, dict):
                        old_bucket_name = bucket_snapshot.get("bucket_name")

                if not old_bucket_name:
                    try:
                        # For PG driver, catalog_id IS the physical schema
                        old_bucket_name = bucket_manager.generate_bucket_name(
                            catalog_id, physical_schema=catalog_id
                        )
                    except Exception:
                        old_bucket_name = None

                logger.info(
                    "gcp_bucket deprovision: deleting bucket '%s' for catalog '%s'",
                    old_bucket_name, catalog_id,
                )
                await bucket_manager.drop_storage(
                    catalog_id,
                    physical_schema=catalog_id,
                    bucket_name=old_bucket_name,
                )

            async def _gcp_eventing_deprovision(
                catalog_id: str,
                external_id=None,
                scope: str = "catalog",
                operation: str = "deprovision_hard",
                collection_id=None,
                config_snapshot=None,
                **_kw,
            ) -> None:
                """Deprovision managed eventing for a catalog (#2340).

                Called by CatalogProvisionTask with operation='deprovision_hard'.
                Tears down Pub/Sub topic and subscription using the pre-captured
                config snapshot.

                Idempotent: teardown methods are NotFound-safe.
                """
                from dynastore.modules.gcp.gcp_config import GcpEventingConfig

                if not isinstance(config_snapshot, dict):
                    logger.debug(
                        "gcp_eventing deprovision: no config snapshot for catalog '%s'; skipping",
                        catalog_id,
                    )
                    return

                eventing_data = config_snapshot.get(GcpEventingConfig.class_key())
                if not eventing_data:
                    logger.debug(
                        "gcp_eventing deprovision: no eventing config for catalog '%s'; skipping",
                        catalog_id,
                    )
                    return

                try:
                    eventing_config = GcpEventingConfig.model_validate(eventing_data)

                    if (
                        isinstance(eventing_config, GcpEventingConfig)
                        and eventing_config.managed_eventing
                    ):
                        logger.info(
                            "gcp_eventing deprovision: tearing down managed eventing for catalog '%s'",
                            catalog_id,
                        )
                        await self.teardown_managed_eventing_channel(
                            catalog_id, eventing_config.managed_eventing
                        )

                except Exception as e:
                    logger.error(
                        "gcp_eventing deprovision: failed for catalog '%s': %s",
                        catalog_id, e, exc_info=True,
                    )
                    raise

                # Belt-and-braces: force-clean the deterministic default topic/subscription.
                # This is NotFound-safe and guarantees no Pub/Sub resource survives.
                try:
                    await self.teardown_catalog_eventing(catalog_id, config=None)
                except Exception as e:
                    logger.warning(
                        "gcp_eventing deprovision: best-effort default cleanup failed for '%s' (non-fatal): %s",
                        catalog_id, e,
                    )

            provisioning_registry.register(
                "gcp_bucket",
                self.provisioner_is_active,
                deferrable=True,
                provision=_gcp_bucket_provision,
                deprovision=_gcp_bucket_deprovision,
            )
            provisioning_registry.register(
                "gcp_eventing",
                self.provisioner_is_active,
                deferrable=True,
                provision=_gcp_eventing_provision,
                deprovision=_gcp_eventing_deprovision,
            )
            from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry

            # We keep these as async because they don't block the core creation flow
            # and don't cause race conditions in tests as easily as the creation one.
            lifecycle_registry.async_catalog_destroyer()(self._on_async_destroy_catalog)
            lifecycle_registry.async_collection_destroyer()(
                self._on_async_destroy_collection
            )

            # Register BigQueryService and the BQ metadata TRANSFORM driver.
            # Role-based driver refactor: the old CollectionMetadataEnricherProtocol-based
            # BigQueryCollectionEnricher is now a CollectionStore with
            # the TRANSFORM capability (plan §Protocols).  The attribute name
            # and plugin registration are preserved for now so downstream
            # lifespan teardown paths don't need changes in this PR.
            from dynastore.modules.gcp.bigquery_service import BigQueryService
            from dynastore.modules.gcp.bq_collection_enricher import (
                BigQueryMetadataTransformDriver,
            )
            from dynastore.tools.discovery import register_plugin

            self._bq_service = BigQueryService()
            self._bq_collection_enricher = BigQueryMetadataTransformDriver()
            register_plugin(self._bq_service)
            register_plugin(self._bq_collection_enricher)

            # Register the GCS asset-download backend (signed-URL resolver).
            from dynastore.modules.gcp.asset_downloads import GcsAssetDownload

            self._asset_download_process = GcsAssetDownload(
                client_provider=self,
                identity_provider=self if hasattr(self, "get_fresh_token") else None,
            )
            register_plugin(self._asset_download_process)

            # Register GCS-backed tile storage providers
            from dynastore.modules.gcp.tiles_storage import (
                TileBucketPreseedStorage,
                StorageBackedTileArchive,
            )
            self._tile_bucket_storage = TileBucketPreseedStorage()
            self._tile_archive_storage = StorageBackedTileArchive()
            register_plugin(self._tile_bucket_storage)
            register_plugin(self._tile_archive_storage)

            from dynastore.modules.gcp.asset_sync import (
                register_bucket_annotation_patcher,
                register_asset_blob_reaper,
            )
            register_bucket_annotation_patcher()
            register_asset_blob_reaper()

            # --- Liveness reconciler (#735) ---
            # Replaces the fixed spawn-lease guess: probe lapsed-lease Cloud Run
            # task rows for real execution liveness before the MaintenanceSupervisor
            # task_reaper blindly reclaims them. Gated by _should_register_gcp_job_runner()
            # — Cloud Run Job containers and opted-out services must NOT run it
            # (they would compete needlessly and never own a gcp_cloud_run_ row).
            #
            # #778 — resolve the engine through ``self.engine`` (the property,
            # with ``get_engine()`` fallback), NOT raw ``self._engine``:
            # ``@engine.setter`` has no production callers (no code does
            # ``gcp_module.engine = …``), so the raw attribute stays ``None``
            # forever and the gate silently False'd on every Cloud Run service
            # since #735 shipped. The schema-init right above already uses the
            # property; the reconciler must too.
            try:
                reconciler_engine = self.engine
            except AssertionError:
                # No DB engine available at all; schema-init's warning above
                # already covers operator visibility. Reconciler stays inert,
                # MaintenanceSupervisor task_reaper remains the backstop.
                reconciler_engine = None
            if _should_register_gcp_job_runner() and reconciler_engine is not None:
                try:
                    from dynastore.modules.gcp.liveness_reconciler import (
                        GcpLivenessReconciler,
                    )
                    cfg = self._module_config
                    _gcp_supervisor.register(
                        GcpLivenessReconciler(
                            interval_seconds=getattr(
                                cfg, "liveness_reconciler_interval_seconds", 20
                            ),
                            extend_visibility_seconds=getattr(
                                cfg, "liveness_extend_visibility_seconds", 300
                            ),
                            unknown_grace_seconds=getattr(
                                cfg, "liveness_unknown_grace_seconds", 180
                            ),
                        )
                    )
                    logger.info("GCP Module: liveness reconciler registered.")
                except Exception as e:
                    logger.error(
                        "GCP Module: failed to register liveness reconciler "
                        "(%s). MaintenanceSupervisor task_reaper remains the backstop.", e,
                        exc_info=True,
                    )
            _gcp_bg_ctx = _GcpServiceContext(
                engine=reconciler_engine,
                shutdown=_gcp_bg_shutdown,
                is_ephemeral=bool(getattr(app_state, "ephemeral_job", False)),
                name=_gcp_get_service_name() or "unknown",
            )
            _gcp_supervisor.start(_gcp_bg_ctx)

            yield
        finally:
            logger.info("GCP Module: Exiting lifespan - closing all clients.")
            # Stop the liveness reconciler supervisor before tearing down clients it uses.
            _gcp_bg_shutdown.set()
            await _gcp_supervisor.stop()
            # Unregister BigQuery plugins
            from dynastore.tools.discovery import unregister_plugin

            for attr in (
                "_bq_service",
                "_bq_collection_enricher",
                "_asset_download_process",
                "_tile_bucket_storage",
                "_tile_archive_storage",
            ):
                obj = getattr(self, attr, None)
                if obj:
                    unregister_plugin(obj)
                    setattr(self, attr, None)
            await self.close()
            logger.info("GCP Module: Lifespan shutdown complete.")

    async def close(self) -> None:
        """
        Explicitly closes all GCP client instances and clears their references.
        This is critical for preventing 'Closed Channel' errors during test isolation.
        """
        clients_to_close = [
            ("_storage_client", self._storage_client),
            ("_publisher_client", self._publisher_client),
            ("_subscriber_client", self._subscriber_client),
            ("_jobs_client", self._jobs_client),
            ("_run_client", self._run_client),
            ("_executions_client", self._executions_client),
        ]

        for attr, client in clients_to_close:
            if client:
                try:
                    # Generic close for both sync and async clients
                    if hasattr(client, "close"):
                        if asyncio.iscoroutinefunction(client.close):
                            await client.close()
                        else:
                            client.close()
                    setattr(self, attr, None)
                except Exception as e:
                    logger.debug(f"GCP Module: Error closing {attr}: {e}")

        self._bucket_service = None
        self._config_service = None
        # Invalidate the loop reference so the next get_jobs_client /
        # get_run_client rebuilds cleanly on whatever loop is running.
        self._async_clients_loop = None
        # self._module_config is kept to preserve settings across lifespans if needed,
        # but re-fetched on start anyway.

    def get_config_service(self) -> ConfigsProtocol:
        if not self._config_service:
            from dynastore.tools.discovery import get_protocol
            self._config_service = get_protocol(ConfigsProtocol)
        if not self._config_service:
            raise RuntimeError("GCPModule: ConfigsProtocol not available.")
        return self._config_service

    def get_storage_client(self) -> "storage.Client":
        """
        Returns the shared, thread-safe Google Cloud Storage client instance.
        """
        # Google Cloud client libraries automatically handle token refresh using the
        # credentials object. We do not need to manually check/refresh here.
        if not self._storage_client:
            raise RuntimeError(
                "GCPModule has not been initialized or failed to create a storage client."
            )
        return self._storage_client

    def get_publisher_client(self) -> "pubsub_v1.PublisherClient":
        """Returns the shared Pub/Sub Publisher client instance."""
        # Google Cloud client libraries automatically handle token refresh.
        if not self._publisher_client:
            raise RuntimeError(
                "GCPModule has not been initialized or failed to create a publisher client."
            )
        return self._publisher_client

    def get_bucket_service(self) -> BucketService:
        """Returns the initialized BucketService."""
        if not self._bucket_service:
            raise RuntimeError("GCPModule has not been initialized.")
        return self._bucket_service

    def get_subscriber_client(self) -> "pubsub_v1.SubscriberClient":
        """Returns the shared Pub/Sub Subscriber client instance."""
        # Google Cloud client libraries automatically handle token refresh.
        if not self._subscriber_client:
            raise RuntimeError(
                "GCPModule has not been initialized or failed to create a subscriber client."
            )
        return self._subscriber_client

    def _ensure_async_clients_for_current_loop(self) -> None:
        """Build (or rebuild) the async gRPC clients on the running loop.

        google-api-core ``*AsyncClient`` constructors create a grpc.aio channel
        that captures ``asyncio.get_event_loop()`` at construction time. If the
        cached client was bound to a different loop (e.g. a throwaway loop used
        during module discovery), every later await raises
        ``RuntimeError: got Future ... attached to a different loop``.

        Follows the same rebind-on-loop-change pattern as
        :meth:`dynastore.tools.async_utils.WaitableSignal._ensure_event`.
        Called only from the sync accessors ``get_jobs_client`` /
        ``get_run_client``, which are in turn only invoked from ``async def``
        frames — so there is always a running loop.
        """
        if not self._credentials:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running loop — we cannot build loop-bound clients here.
            # Defer: accessor will raise the "unavailable" error below, the
            # caller is already sync, and no await will happen.
            return

        if (
            self._jobs_client is not None
            and self._run_client is not None
            and self._executions_client is not None
            and self._async_clients_loop is loop
        ):
            return

        # Drop clients bound to a different (possibly dead) loop without
        # awaiting close() — we are in a sync frame and closing a channel
        # attached to a foreign loop is unsafe. Garbage collection will
        # tear down the orphaned grpc transport.
        self._jobs_client = run_v2.JobsAsyncClient(credentials=self._credentials)
        self._run_client = run_v2.ServicesAsyncClient(credentials=self._credentials)
        self._executions_client = run_v2.ExecutionsAsyncClient(
            credentials=self._credentials
        )
        self._async_clients_loop = loop
        logger.info("GCP async clients built on event loop %r.", loop)

    def get_jobs_client(self) -> "run_v2.JobsAsyncClient":
        """
        Returns the async Cloud Run Jobs client, bound to the current event loop.

        The client is built (or rebuilt) on first access from a new loop —
        essential because google-api-core async clients capture the event loop
        at construction and raise ``Future attached to a different loop`` on
        reuse across loops.
        """
        self._ensure_async_clients_for_current_loop()
        if not self._jobs_client:
            raise RuntimeError(
                "GCPModule jobs client unavailable: "
                + ("credentials missing" if not self._credentials else "no running event loop")
            )
        return self._jobs_client

    def get_executions_client(self) -> "run_v2.ExecutionsAsyncClient":
        """
        Returns the async Cloud Run Executions client, bound to the current loop.

        Used by the liveness probe (#735) to ask the Executions API whether the
        execution backing a lapsed-lease task is still alive. Built lazily and
        rebound on loop change for the same reason as ``get_jobs_client`` — see
        :meth:`_ensure_async_clients_for_current_loop`.
        """
        self._ensure_async_clients_for_current_loop()
        if not self._executions_client:
            raise RuntimeError(
                "GCPModule executions client unavailable: "
                + ("credentials missing" if not self._credentials else "no running event loop")
            )
        return self._executions_client

    # --- JobExecutionProtocol Implementation ---

    async def run_job(
        self,
        job_name: str,
        args: Optional[List[str]] = None,
        env_vars: Optional[Dict[str, str]] = None,
    ) -> Any:
        """
        JobExecutionProtocol: Triggers a serverless job (Cloud Run job) asynchronously.
        """
        project_id = self.get_project_id()
        region = self.get_region()
        client = self.get_jobs_client()
        name = f"projects/{project_id}/locations/{region}/jobs/{job_name}"

        request = run_v2.RunJobRequest(name=name)

        if args or env_vars:
            container_overrides = run_v2.RunJobRequest.Overrides.ContainerOverride()
            if args:
                container_overrides.args.extend(args)
            if env_vars:
                for key, value in env_vars.items():
                    container_overrides.env.append(run_v2.EnvVar(name=key, value=value))
            request.overrides.container_overrides.append(container_overrides)

        try:
            operation = await client.run_job(request=request)
            logger.info(
                f"GCP Job '{job_name}' triggered asynchronously, operation name: {operation.operation.name if hasattr(operation, 'operation') else 'unknown'}"
            )
            return operation
        except Exception as e:
            logger.error(f"Error triggering GCP job '{job_name}': {e}", exc_info=True)
            raise

    async def get_job_config(self) -> Dict[str, str]:
        """
        JobExecutionProtocol: Discovers deployed jobs and returns mapping of task_type -> job_name.
        """
        job_map = {}
        project_id = self.get_project_id()
        region = self.get_region()

        if not project_id:
            logger.warning("GCP Project ID not available. Cannot discover jobs.")
            return job_map

        # Use common region fallback if metadata server fails or local
        region = region or os.getenv("REGION", "europe-west1")

        # dev and review environments share the same GCP project
        # (fao-aip-geospatial-review). Without environment-scoped filtering,
        # both fleets list each other's jobs and the last enumerated job wins
        # for any given TASK_TYPE — causing dev to dispatch review's jobs.
        # When ENVIRONMENT is set on this service we keep only jobs carrying
        # the same value; jobs with a different or absent ENVIRONMENT tag are
        # skipped. When ENVIRONMENT is unset (on-prem, local, single-env
        # deployments) all APP=dynastore jobs are accepted — back-compat path.
        own_env = os.getenv("ENVIRONMENT", "").strip()
        if not own_env:
            logger.info(
                "ENVIRONMENT is not set; Cloud Run job discovery is environment-unfiltered."
            )

        try:
            client = self.get_jobs_client()
            parent = f"projects/{project_id}/locations/{region}"
            request = run_v2.ListJobsRequest(parent=parent)

            logger.info(f"Discovering GCP jobs in {parent}...")
            async for job in await client.list_jobs(request=request):
                job_name = job.name.split("/")[-1]

                if not (job.template and job.template.template and job.template.template.containers):
                    continue

                env_map = {}
                for container in job.template.template.containers:
                    for env_var in container.env:
                        env_map[env_var.name] = env_var.value
                    break  # first container only

                # Only process jobs marked as dynastore task runners
                if env_map.get("APP") != "dynastore":
                    continue

                # When this service declares an environment, skip any job whose
                # ENVIRONMENT tag does not match exactly — including untagged
                # jobs. This prevents cross-environment pollution when dev and
                # review share a GCP project.
                if own_env:
                    job_env = env_map.get("ENVIRONMENT", "").strip()
                    if job_env != own_env:
                        logger.info(
                            f"Skipping job '{job_name}': ENVIRONMENT='{job_env}' "
                            f"does not match service ENVIRONMENT='{own_env}'."
                        )
                        continue

                # Strategy 1 (explicit): TASK_TYPE env var — canonical task name
                task_type = env_map.get("TASK_TYPE", "").strip() or None

                # Strategy 2 (fallback): derive task type from SCOPE env var
                if not task_type:
                    for token in (
                        s.strip() for s in env_map.get("SCOPE", "").split(",") if s.strip()
                    ):
                        task_type = _task_type_from_scope_token(token)
                        if task_type:
                            break

                if task_type:
                    job_map[task_type] = job_name
                    # Side-channel: capture per-job MAX_RETRIES env so
                    # GcpJobRunner can stamp it on the task row at create-time
                    # (caps long-running expensive jobs at deploy-time intent
                    # rather than the column DEFAULT of 3 retries).
                    max_retries_raw = env_map.get("MAX_RETRIES")
                    extras: Dict[str, object] = {}
                    if max_retries_raw is not None:
                        try:
                            extras["max_retries"] = int(max_retries_raw)
                        except (TypeError, ValueError):
                            logger.warning(
                                f"Job '{job_name}' has non-integer MAX_RETRIES='{max_retries_raw}'; ignoring."
                            )
                    if extras:
                        from dynastore.modules.gcp.tools.jobs import set_job_extras
                        set_job_extras(task_type, extras)
                    logger.info(
                        f"Discovered GCP job: task '{task_type}' -> job '{job_name}' "
                        f"(extras={extras or '{}'})"
                    )
        except Exception as e:
            logger.error(
                f"Error discovering GCP jobs (Project: {project_id}, Region: {region}): {e}",
                exc_info=True,
            )

        return job_map

    # --- ProcessRegistryProtocol Implementation ---

    async def list_processes(self, tenant: Optional[str] = None) -> List[Any]:
        """ProcessRegistryProtocol: list all Cloud Run Job process definitions.

        Includes both explicitly-defined processes and synthetic entries for
        Cloud Run jobs without Process definitions, making all jobs discoverable
        and executable as external services.
        """
        from dynastore.modules.gcp.tools.jobs import load_job_config, try_load_process_definition
        from dynastore.modules.processes.models import Process, ProcessScope, JobControlOptions, TransmissionMode

        job_map = await load_job_config()
        result = []
        seen_ids = set()

        for task_type, job_name in job_map.items():
            # Try to load explicit Process definition
            defn = try_load_process_definition(task_type)
            if defn is not None:
                result.append(defn)
                seen_ids.add(defn.id)
            else:
                # No definition found; use task_type (already in hyphenated form) as process id
                process_id = task_type
                if process_id not in seen_ids:
                    synthetic = Process(
                        id=process_id,
                        title=f"Cloud Run Job: {job_name}",
                        description=f"External Cloud Run job deployed as {job_name}",
                        version="1.0.0",
                        scopes=[ProcessScope.PLATFORM],
                        jobControlOptions=[JobControlOptions.ASYNC_EXECUTE],
                        outputTransmission=[TransmissionMode.VALUE],
                        inputs={},
                        outputs={},
                        links=[],
                    )
                    result.append(synthetic)
                    seen_ids.add(process_id)
                    logger.info(
                        f"Created synthetic Process for Cloud Run job '{job_name}' (task_type={task_type})"
                    )

        return result

    async def get_process(self, process_id: str, tenant: Optional[str] = None) -> Optional[Any]:
        """ProcessRegistryProtocol: look up a single process definition by id."""
        return next((p for p in await self.list_processes(tenant) if p.id == process_id), None)

    def get_run_client(self) -> "run_v2.ServicesAsyncClient":
        """
        Returns the async Cloud Run Services client, bound to the current loop.

        See :meth:`_ensure_async_clients_for_current_loop` for why this is lazy.
        """
        self._ensure_async_clients_for_current_loop()
        if not self._run_client:
            raise RuntimeError(
                "GCPModule run client unavailable: "
                + ("credentials missing" if not self._credentials else "no running event loop")
            )
        return self._run_client

    def _refresh_credentials(self) -> bool:
        """
        Synchronously checks and refreshes GCP credentials if they are expired or invalid.

        Note: This is primarily used by get_fresh_token() where we need the raw token string.
        Standard clients (storage, pubsub) handle refresh automatically.
        """
        if not self._credentials:
            logger.warning("Cannot refresh: no credentials object available.")
            return False

        import google.auth.transport.requests

        # Check 'expired' explicitly. 'valid' might be True even if the token is stale
        # but not yet wiped. 'expired' checks the actual expiry timestamp.
        if not self._credentials.valid or self._credentials.expired:
            request = google.auth.transport.requests.Request()
            self._credentials.refresh(request)
            logger.info("GCP credentials successfully refreshed.")
        return True

    async def get_fresh_token(self) -> str:
        """
        Asynchronously ensures credentials are valid and returns a fresh access token.
        This method is safe to call from the event loop as it offloads the refresh
        to a background thread.
        """
        await run_in_thread(self._refresh_credentials)
        assert self._credentials is not None
        return self._credentials.token

    def get_credentials_object(self) -> Any:
        """Returns the shared google.auth.credentials object."""
        if not self._credentials:
            raise RuntimeError(
                "GCPModule has not been initialized or failed to find credentials."
            )
        return self._credentials

    def get_identity_info(self) -> Optional[Dict[str, Any]]:
        """
        CloudIdentityProtocol: Returns the full GCP identity dictionary discovered at startup.
        """
        return self._identity

    def get_identity(self) -> Optional[Dict[str, Any]]:
        """Legacy identity getter."""
        return self.get_identity_info()

    def get_project_id(self) -> Optional[str]:
        """
        Returns the discovered GCP Project ID from the active credentials.
        Falls back to environment variables if no identity is available.
        """
        if self._identity and self._identity.get("project_id"):
            return self._identity.get("project_id")
        
        if self._module_config:
            return self._module_config.project_id

        # Legacy/Bootstrap fallback
        return os.getenv("PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")

    def get_project_number(self) -> Optional[str]:
        """
        Returns the discovered GCP Project number from the active credentials.
        """
        if not self._identity:
            return None
        return self._identity.get("project_number")

    def get_account_email(self) -> Optional[str]:
        """
        Returns the email of the active GCP service account or user.
        """
        if not self._identity:
            return None
        return self._identity.get("account_email")

    def get_region(self) -> Optional[str]:
        """
        Returns the auto-detected GCP region, if available.
        """
        if self._identity and self._identity.get("region"):
            return self._identity.get("region")

        if self._module_config:
            return self._module_config.region

        return os.getenv("REGION")

    def get_service_name(self) -> Optional[str]:
        """
        Returns the name of the current Cloud Run service, if available.
        Relies on the K_SERVICE environment variable automatically set by Cloud Run.
        """
        return os.getenv("K_SERVICE")

    @cached(maxsize=1, distributed=False)
    async def get_self_url(self) -> str:
        """
        Returns the public URL of the running Cloud Run service, used as the
        Pub/Sub push-subscription endpoint base.

        Resolution order:
        1. If K_SERVICE is set (i.e. we are inside Cloud Run), query the Cloud Run
           Admin API for the canonical ``uri`` of this specific service revision.
           SERVICE_URL is intentionally NOT consulted in this path — a misconfigured
           SERVICE_URL pointing at a different environment (e.g. prod) would wire new
           push subscriptions to the wrong service, causing GCS finalize events to be
           lost on the service that actually owns the catalog.
        2. If K_SERVICE is absent (local dev / test), fall back to SERVICE_URL, then
           raise if neither is set.

        The result is cached per process instance (maxsize=1, no distributed backend).
        """
        service_name = self.get_service_name()
        if not service_name:
            # Not running inside Cloud Run — accept SERVICE_URL override for local dev/test.
            service_url = os.getenv("SERVICE_URL")
            if service_url:
                logger.info(
                    f"K_SERVICE not set; using SERVICE_URL override: {service_url}"
                )
                return service_url
            raise RuntimeError(
                "Cannot determine self URL: K_SERVICE environment variable is not set "
                "and SERVICE_URL is missing. This is not a Cloud Run environment."
            )

        # Running inside Cloud Run: discover the canonical URI via the Admin API.
        # SERVICE_URL is explicitly skipped here — if it were accepted it could
        # silently route push subscriptions to a different service (e.g. prod),
        # causing GCS finalize events to be delivered there instead of this instance.
        project_id = self.get_project_id()
        region = self.get_region()
        logger.info(
            f"Discovering public URL for Cloud Run service '{service_name}' "
            f"in project '{project_id}' region '{region}'."
        )
        # Resolve the client outside the try: a credentials failure raises its own
        # RuntimeError ("run client unavailable: credentials missing"), which must
        # propagate undecorated rather than being re-wrapped below as a
        # run.services.get permission problem.
        client = self.get_run_client()
        try:
            service_path = client.service_path(project_id or "", region or "", service_name)
            service_details = await client.get_service(name=service_path)
            discovered_url = service_details.uri
            logger.info(f"Discovered and cached self URL: {discovered_url}")
            return discovered_url
        except Exception as e:
            # The Admin API call failed (missing run.services.get permission, network
            # issue, etc.).  We cannot safely fall back to SERVICE_URL here — it might
            # point at a different environment and silently misroute push subscriptions.
            # Raise so the provisioning task retries rather than wiring a bad endpoint.
            raise RuntimeError(
                f"Failed to discover Cloud Run service URL for '{service_name}' "
                f"(project={project_id}, region={region}). "
                f"Grant the service account 'run.services.get' or set the correct "
                f"PROJECT_ID / REGION environment variables. "
                f"Original error: {e}"
            ) from e


def _should_register_gcp_job_runner() -> bool:
    """Decide whether this process should host the Cloud Run Job dispatcher runner.

    Skip registration in Cloud Run Job containers (TASK_TYPE env set) — they
    are themselves the runtime, they don't dispatch other jobs. Skip in
    services explicitly opting out (DYNASTORE_DISABLE_GCP_JOB_RUNNER=true) so
    auth / geoid services don't compete with catalog for ingestion claims.

    Without this gate, every service that imports modules.gcp registers a
    GcpJobRunner whose can_handle() answers True for any task type with a
    matching Cloud Run Job. Combined with the dispatcher's lack of scope
    filter, that causes cross-service races on PENDING ingestion rows.
    """
    if os.environ.get("TASK_TYPE", "").strip():
        # Cloud Run Job container — never act as a dispatcher.
        logger.info(
            "GcpJobRunner: skipping registration (TASK_TYPE env set — this is a Cloud Run Job container)."
        )
        return False
    if os.environ.get("DYNASTORE_DISABLE_GCP_JOB_RUNNER", "").lower() in ("1", "true", "yes"):
        logger.info(
            "GcpJobRunner: skipping registration (DYNASTORE_DISABLE_GCP_JOB_RUNNER=true)."
        )
        return False
    return True


try:
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner
    from dynastore.tools.discovery import register_plugin as _reg_runner
    if _should_register_gcp_job_runner():
        _reg_runner(GcpJobRunner())
        logger.info("GcpJobRunner: registered as task runner.")
except ImportError:
    pass
