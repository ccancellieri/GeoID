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

import logging
from typing import Any, Dict, Optional

# Hard runtime dep — fail entry-point load on services without ``module_gcp``
# installed (e.g. SCOPEs that don't include the GCP module). Without this, the
# task class would load anyway via lazy ``get_protocol(StorageProtocol)`` calls
# inside ``run()``; routing config not yet PUT → CapabilityMap surfaces the
# task → claim → runtime crash. The hard import keeps the symmetry with the
# elasticsearch task module fix (commit 54d1cdc).
import google.cloud.storage  # noqa: F401

from pydantic import BaseModel
from dynastore.tasks.protocols import TaskProtocol
from dynastore.modules.tasks.models import (
    RunnerContext,
    TaskPayload,
    PermanentTaskFailure,
)

from dynastore.modules import get_protocol
from dynastore.models.protocols import (
    StorageProtocol,
    EventingProtocol,
    CatalogsProtocol,
    GcpCatalogProvisioning,
)

logger = logging.getLogger(__name__)

def _get_catalog_protocol() -> CatalogsProtocol:
    protocol = get_protocol(CatalogsProtocol)
    if not protocol:
        raise RuntimeError("CatalogsProtocol not available")
    return protocol

def _get_storage_protocol() -> StorageProtocol:
    protocol = get_protocol(StorageProtocol)
    if not protocol:
        # Self-diagnostic: dump the full plugin + module registry so we can
        # tell whether GCPModule was unregistered, whether some other
        # StorageProtocol provider superseded it, or whether the module was
        # simply not loaded in this worker.  Paired with the fix in
        # commit ab5ca1b (GCP async clients no longer bound to the wrong
        # event loop) — if this ever fires again, the log captures exactly
        # which registry state caused it.
        try:
            from dynastore.tools.discovery import (
                _DYNASTORE_PLUGINS,
                get_all_protocols,
            )
            from dynastore.modules import _DYNASTORE_MODULES

            plugin_types = sorted({type(p).__name__ for p in _DYNASTORE_PLUGINS})
            loaded_modules = sorted(
                name for name, cfg in _DYNASTORE_MODULES.items() if cfg.instance
            )
            # get_all_protocols includes is_available=False — tells us whether
            # a provider existed but was filtered out at discovery.
            all_storage = [
                type(p).__name__ for p in get_all_protocols(StorageProtocol)
            ]
            logger.error(
                "_get_storage_protocol: no StorageProtocol resolved. "
                "plugins=%s loaded_modules=%s all_storage_providers=%s",
                plugin_types, loaded_modules, all_storage,
            )
        except Exception as diag_err:  # pragma: no cover — diagnostic best-effort
            logger.error(
                "_get_storage_protocol: diagnostic dump failed: %s", diag_err,
            )
        # Raise RuntimeError (NOT PermanentTaskFailure) so the dispatcher
        # retries.  Each Cloud Run instance has its own in-process registry;
        # if GCPModule failed to load on THIS instance (transient ADC/startup
        # issue), retrying resets the task to PENDING in the DB.  The
        # dispatcher's claim_batch() is instance-agnostic — a healthy instance
        # can pick up the task on the next cycle.
        raise RuntimeError("StorageProtocol not available - GCP module not loaded")
    return protocol

class GcpProvisionInputs(BaseModel):
    catalog_id: str

class ProvisioningTask(TaskProtocol):
    priority: int = 100
    """
    Durable task for creating and configuring a GCS bucket for a catalog.
    
    Idempotent: checks if resources exist before creating.
    If it fails after max retries, the janitor will move it to DEAD_LETTER
    and the catalog will remain in 'provisioning' or 'failed' state.
    """
    task_type = "gcp_provision_catalog"

    async def run(self, payload: TaskPayload[GcpProvisionInputs]) -> Dict[str, Any]:
        try:
            inputs = payload.inputs
            catalog_id = inputs.catalog_id
            if not catalog_id:
                raise ValueError("Missing 'catalog_id' in task inputs")

            logger.info(f"GcpProvisionCatalogTask: Provisioning resources for catalog '{catalog_id}'...")

            # 1. Resolve storage protocol (GCP)
            storage = _get_storage_protocol()

            # 2. Setup the bucket and eventing idempotently. A storage
            # implementation that satisfies GcpCatalogProvisioning provisions
            # bucket + eventing in one call (more efficient on GCS where the
            # two resources are interleaved). Mocks and non-GCP backends fall
            # back to the cross-vendor sequence.
            if isinstance(storage, GcpCatalogProvisioning):
                bucket_name, _ = await storage.setup_catalog_gcp_resources(catalog_id)
            else:
                bucket_name = await storage.ensure_storage_for_catalog(catalog_id)
                eventing = get_protocol(EventingProtocol)
                if eventing:
                    await eventing.setup_catalog_eventing(catalog_id)

            logger.info(f"GcpProvisionCatalogTask: Bucket '{bucket_name}' ensured for catalog '{catalog_id}'.")

            # 4. Mark catalog as ready
            catalogs = _get_catalog_protocol()
            await catalogs.update_provisioning_status(catalog_id, "ready")
            
            logger.info(f"GcpProvisionCatalogTask: Catalog '{catalog_id}' marked as READY.")
            
            return {
                "catalog_id": catalog_id,
                "bucket_name": bucket_name,
                "status": "ready"
            }
        except PermanentTaskFailure:
            await self._mark_failed(catalog_id)
            raise
        except RuntimeError as e:
            msg = str(e).lower()
            # Only truly unrecoverable credential/client-init failures get
            # permanent treatment.  Everything else (GCP module not yet loaded,
            # lifespan not finished, transient GCS conflict/quota) is retryable
            # — don't mark the catalog failed so a successful retry can flip it
            # to "ready" instead of leaving it stuck in "failed" state.
            _permanent_indicators = (
                "failed to create a storage client",
                "credentials",
            )
            if any(indicator in msg for indicator in _permanent_indicators):
                await self._mark_failed(catalog_id)
                raise PermanentTaskFailure(
                    f"GCP unavailable, cannot provision '{catalog_id}': {e}"
                ) from e
            logger.error(
                "GcpProvisionCatalogTask FAILED for %r (retryable): %s",
                catalog_id, e, exc_info=True,
            )
            raise
        except Exception as e:
            logger.error(f"CRITICAL: GcpProvisionCatalogTask FAILED for {catalog_id}: {e}", exc_info=True)
            await self._mark_failed(catalog_id)
            raise

    async def _mark_failed(self, catalog_id: Optional[str]) -> None:
        """Flip ``catalog.catalogs.provisioning_status`` → ``'failed'``.

        Called from every failure path in :meth:`run` so the fail-fast
        guard at the API layer
        (``OGCServiceMixin._require_catalog_ready``) can reject
        subsequent write operations with a 409 instead of letting them
        500 deep inside a driver.

        Best-effort: if the update itself raises (DB down, protocol
        unavailable), we swallow — the task's own FAILED status lands
        via ``fail_task`` in the runner, and the pg_cron reaper / a
        later ``gcp.init.*`` log line will still surface the problem.
        Silently proceeding is preferable to masking the original
        exception that is about to be re-raised.
        """
        if not catalog_id:
            return
        try:
            catalogs = _get_catalog_protocol()
            await catalogs.update_provisioning_status(catalog_id, "failed")
            logger.info(
                f"GcpProvisionCatalogTask: catalog '{catalog_id}' marked FAILED "
                f"(API writes will 409 via fail-fast guard)."
            )
        except Exception as mark_err:  # pragma: no cover — diagnostic best-effort
            logger.error(
                f"GcpProvisionCatalogTask: failed to mark catalog '{catalog_id}' "
                f"provisioning_status='failed': {mark_err}"
            )

class GcpDestroyCatalogTask(TaskProtocol):
    priority: int = 100
    """
    Durable task for tearing down GCS resources associated with a catalog.
    
    Idempotent: safe to run multiple times even if resources are already gone.
    """
    task_type = "gcp_destroy_catalog"

    async def run(self, payload: TaskPayload[GcpProvisionInputs]) -> Dict[str, Any]:
        inputs = payload.inputs
        catalog_id = inputs.catalog_id
        if not catalog_id:
            raise ValueError("Missing 'catalog_id' in task inputs")

        logger.info(f"GcpDestroyCatalogTask: Tearing down resources for catalog '{catalog_id}'...")

        # 1. Teardown eventing (optional). Calls the typed EventingProtocol
        # method directly — previously this dispatched via getattr to a
        # non-existent `teardown_catalog_notifications` method, silently
        # no-opping every destroy. Real teardown now executes.
        eventing = get_protocol(EventingProtocol)
        if eventing:
            try:
                await eventing.teardown_catalog_eventing(catalog_id)
                logger.info(f"GcpDestroyCatalogTask: Eventing removed for catalog '{catalog_id}'.")
            except Exception as e:
                logger.warning(f"GcpDestroyCatalogTask: Failed to teardown eventing for '{catalog_id}': {e}")

        # 2. Delete/cleanup storage. Same fix: was dispatching via getattr to
        # a non-existent `delete_catalog_bucket`; now calls the typed
        # StorageProtocol.delete_storage_for_catalog directly.
        try:
            storage = _get_storage_protocol()
            await storage.delete_storage_for_catalog(catalog_id)
            logger.info(f"GcpDestroyCatalogTask: Bucket resources for '{catalog_id}' deleted.")
        except Exception as e:
            logger.warning(f"GcpDestroyCatalogTask: Failed to delete storage for '{catalog_id}': {e}")

        logger.info(f"GcpDestroyCatalogTask: Cleanup completed for catalog '{catalog_id}'.")
        
        return {
            "catalog_id": catalog_id,
            "status": "destroyed"
        }
