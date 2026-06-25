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
from typing import Any, Dict, Optional, Tuple

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
    TaskPayload,
    PermanentTaskFailure,
)

from dynastore.modules import get_protocol
from dynastore.models.protocols import (
    StorageProtocol,
    EventingProtocol,
    GcpCatalogProvisioning,
)
from dynastore.modules.gcp.tools.bucket import BucketConflictError
from dynastore.modules.gcp.gcp_eventing_ops import OrphanSubscriptionClash
from dynastore.modules.catalog.log_manager import log_error
from dynastore.tasks._helpers import _get_catalog_protocol

logger = logging.getLogger(__name__)

# Eventing failures that indicate a permanent IAM/permission problem — the
# service account will not spontaneously gain access, so retrying is futile.
# When the bounded retry inside ``setup_catalog_eventing`` exhausts these, the
# exception propagates here and we downgrade to "eventing degraded" rather than
# failing the whole provisioning task.
_EVENTING_PERMISSION_ERRORS: Tuple[type, ...] = ()

try:
    from google.api_core import exceptions as _google_exceptions

    _EVENTING_PERMISSION_ERRORS = (
        _google_exceptions.PermissionDenied,
        _google_exceptions.Forbidden,
    )
except Exception:  # pragma: no cover — google-cloud not installed
    pass

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


async def _provision_bucket_hard(
    storage: StorageProtocol,
    catalog_id: str,
) -> str:
    """Provision only the GCS bucket, using the appropriate protocol path.

    Returns the bucket name. Raises on any failure — bucket is HARD.
    """
    if isinstance(storage, GcpCatalogProvisioning):
        # The combined call does bucket + eventing; we call it here for the
        # GcpCatalogProvisioning path and extract just the bucket name.  The
        # eventing portion is handled separately in ``_provision_eventing_soft``
        # for the fail-soft contract. Because ``setup_catalog_gcp_resources``
        # is idempotent, calling it a second time in the eventing step is safe;
        # but to avoid a redundant GCP round-trip we instead call the
        # cross-vendor bucket-only path when available.
        bucket_name = await storage.ensure_storage_for_catalog(catalog_id)
        if bucket_name is None:
            raise RuntimeError(
                f"ensure_storage_for_catalog returned None for catalog '{catalog_id}'"
            )
        return bucket_name
    else:
        bucket_name = await storage.ensure_storage_for_catalog(catalog_id)
        if bucket_name is None:
            raise RuntimeError(
                f"ensure_storage_for_catalog returned None for catalog '{catalog_id}'"
            )
        return bucket_name


async def _provision_eventing(
    storage: StorageProtocol,
    catalog_id: str,
) -> str:
    """Set up managed eventing for a catalog.

    Atomic provisioning contract (eventing is NOT best-effort): this either
    sets eventing up or raises. It never swallows a failure into a "degraded"
    success, because a catalog whose eventing did not provision must not be
    reported ``ready``.

    Returns:
        ``"complete"`` when eventing was set up successfully.
        ``"skipped"``  when managed eventing is disabled for this catalog
                       (``managed_eventing.enabled is False``) — a deliberate
                       configuration choice, so the catalog still reaches
                       ``ready`` as a bucket-only catalog.

    Raises the underlying exception on any failure. The caller classifies it:
    a permission/structural error is permanent (catalog → ``failed``; operator
    grants the IAM role and reprovisions); anything else is treated as transient
    and retried by the task queue.
    """
    if isinstance(storage, GcpCatalogProvisioning):
        _, eventing_config = await storage.setup_catalog_gcp_resources(
            catalog_id
        )
        managed = getattr(eventing_config, "managed_eventing", None)
        if managed is None or not getattr(managed, "enabled", False):
            return "skipped"
        return "complete"
    eventing = get_protocol(EventingProtocol)
    if eventing:
        await eventing.setup_catalog_eventing(catalog_id)
    return "complete"


class GcpProvisionInputs(BaseModel):
    catalog_id: str

class ProvisioningTask(TaskProtocol):
    priority: int = 100
    """
    Durable task for creating and configuring a GCS bucket for a catalog.

    Idempotent: checks if resources exist before creating.
    If it fails after max retries, the janitor will move it to DEAD_LETTER
    and the catalog will be marked 'failed'.

    Atomic failure contract — the catalog is 'ready' only when every step
    actually provisioned; otherwise it is 'failed' (never silently 'ready'):
      - Bucket failure → catalog 'failed' (HARD requirement). The committed
        bucket is never deleted on a soft/transient failure (deleting it is the
        "ready with no bucket" data-loss bug); only a genuine orphan (catalog
        row vanished mid-provision) is cleaned up.
      - Eventing disabled (managed_eventing.enabled=False) → step 'skipped' →
        catalog 'ready' as a bucket-only catalog (deliberate config choice).
      - Eventing permission/structural failure → permanent: catalog 'failed';
        grant pubsub.topics.attachSubscription, then POST
        /catalog/catalogs/{id}/reprovision (idempotent) to recover.
      - Eventing transient failure → retried by the task queue; on exhaustion
        the catalog is marked 'failed'.
    Recovery is always the same: fix the cause, then reprovision — the task
    re-ensures the bucket and re-attaches eventing and overwrites the checklist.
    """
    task_type = "gcp_provision_catalog"

    async def run(self, payload: TaskPayload[GcpProvisionInputs]) -> Dict[str, Any]:
        catalog_id: Optional[str] = None
        try:
            inputs = payload.inputs
            catalog_id = inputs.catalog_id
            if not catalog_id:
                raise ValueError("Missing 'catalog_id' in task inputs")

            logger.info(
                "GcpProvisionCatalogTask: Provisioning resources for catalog '%s'...",
                catalog_id,
            )

            # 1. Resolve storage protocol (GCP)
            storage = _get_storage_protocol()

            # 2. Provision the bucket — HARD requirement. Any failure here
            # propagates to the outer except handlers, which mark the catalog
            # failed or retryable as appropriate.
            bucket_name = await _provision_bucket_hard(storage, catalog_id)
            logger.info(
                "GcpProvisionCatalogTask: Bucket '%s' ensured for catalog '%s'.",
                bucket_name, catalog_id,
            )

            # 3. Mark bucket step complete regardless of eventing outcome.
            catalogs = _get_catalog_protocol()
            await catalogs.mark_provisioning_step(catalog_id, "gcp_bucket", "complete")
            logger.info(
                "GcpProvisionCatalogTask: Catalog '%s' gcp_bucket step COMPLETE.",
                catalog_id,
            )

            # 4. Provision eventing — atomic (NOT best-effort). Eventing is part
            # of "the catalog is provisioned": if it cannot be set up, the
            # catalog must not be reported 'ready'. Failures are classified here:
            #
            #   • disabled (managed_eventing.enabled=False) → "skipped" → ready
            #     (a deliberate config choice — bucket-only catalog).
            #   • permission / structural (OrphanSubscriptionClash) → permanent:
            #     retrying re-issues the same denied/clashing call, so fail fast,
            #     mark gcp_eventing 'failed' (→ catalog 'failed') and tell the
            #     operator exactly what to grant before reprovisioning.
            #   • anything else → transient: re-raise so the task queue retries.
            #     gcp_eventing is left 'pending' (catalog stays 'provisioning');
            #     on retry exhaustion the terminal drain marks it 'failed'.
            #
            # The gcp_eventing step MUST reach a terminal state on every NON-retry
            # path, or evaluate_checklist (which only flips the catalog when no
            # step is still 'pending') would wedge it in 'provisioning'.
            try:
                eventing_status = await _provision_eventing(storage, catalog_id)
            except OrphanSubscriptionClash:
                await self._mark_step("failed", catalog_id, step_key="gcp_eventing")
                raise
            except Exception as eventing_err:  # noqa: BLE001 — classified below
                is_permission = bool(
                    _EVENTING_PERMISSION_ERRORS
                    and isinstance(eventing_err, _EVENTING_PERMISSION_ERRORS)
                )
                if is_permission:
                    # Permanent: same IAM denial on every retry. Fail fast.
                    await self._mark_step(
                        "failed", catalog_id, step_key="gcp_eventing"
                    )
                    logger.error(
                        "GcpProvisionCatalogTask: eventing setup permission-denied "
                        "for catalog '%s' (%s: %s); marking catalog FAILED. Grant "
                        "'pubsub.topics.attachSubscription' on the Pub/Sub project "
                        "and POST /catalog/catalogs/%s/reprovision to recover.",
                        catalog_id, type(eventing_err).__name__, eventing_err,
                        catalog_id,
                    )
                    try:
                        await log_error(
                            catalog_id,
                            "gcp.provision.eventing_failed",
                            (
                                f"Eventing setup failed — permission denied "
                                f"({type(eventing_err).__name__}): {eventing_err}. "
                                f"Grant pubsub.topics.attachSubscription on the "
                                f"Pub/Sub project, then POST "
                                f"/catalog/catalogs/{catalog_id}/reprovision."
                            ),
                        )
                    except Exception as log_err:  # pragma: no cover — best-effort
                        logger.error(
                            "GcpProvisionCatalogTask: failed to write tenant log "
                            "for catalog '%s' eventing failure: %s",
                            catalog_id, log_err,
                        )
                    raise PermanentTaskFailure(
                        f"Eventing IAM permission denied for catalog "
                        f"'{catalog_id}'; grant pubsub.topics.attachSubscription "
                        f"and reprovision."
                    ) from eventing_err
                # Transient: re-raise as a retryable RuntimeError (no
                # credential/storage-client indicator) so the RuntimeError
                # handler retries WITHOUT marking the healthy bucket failed.
                # gcp_eventing stays 'pending' across retries.
                logger.warning(
                    "GcpProvisionCatalogTask: transient eventing failure for "
                    "catalog '%s' (%s: %s); leaving gcp_eventing pending for "
                    "retry.",
                    catalog_id, type(eventing_err).__name__, eventing_err,
                )
                raise RuntimeError(
                    f"Transient eventing failure for catalog '{catalog_id}': "
                    f"{eventing_err}"
                ) from eventing_err
            await catalogs.mark_provisioning_step(
                catalog_id, "gcp_eventing", eventing_status
            )
            logger.info(
                "GcpProvisionCatalogTask: Catalog '%s' gcp_eventing step %s.",
                catalog_id, eventing_status.upper(),
            )

            return {
                "catalog_id": catalog_id,
                "bucket_name": bucket_name,
                "eventing_status": eventing_status,
                "status": "ready",
            }
        except PermanentTaskFailure:
            await self._mark_failed(catalog_id)
            raise
        except OrphanSubscriptionClash as e:
            # A push subscription with the deterministic name exists but is
            # bound to a different (or tombstoned) topic. Pub/Sub binding is
            # immutable, so retrying cannot recover — surface the recovery
            # message on the tenant log so the maintainer can act, and mark
            # the step failed terminally.
            logger.error(
                "GcpProvisionCatalogTask: subscription clash for catalog "
                "'%s': %s", catalog_id, e,
            )
            try:
                await log_error(
                    catalog_id or "",
                    "gcp.provision.subscription_clash",
                    str(e),
                )
            except Exception as log_err:  # pragma: no cover — diagnostic best-effort
                logger.error(
                    "GcpProvisionCatalogTask: failed to write tenant log for "
                    "catalog '%s' subscription clash: %s", catalog_id, log_err,
                )
            await self._mark_failed(catalog_id)
            raise PermanentTaskFailure(
                f"Pub/Sub subscription clash for catalog '{catalog_id}' — "
                f"manual recovery required. See tenant log "
                f"'gcp.provision.subscription_clash' for the gcloud command."
            ) from e
        except BucketConflictError as e:
            # The deterministic bucket name is owned by another GCP project or
            # already linked to another catalog. Retrying regenerates the SAME
            # name, so it can never succeed — mark the catalog 'conflict' (not
            # 'failed') and stop retrying. The bucket is NOT deleted: it is not
            # ours, and force-deleting it would destroy another owner's data.
            logger.error(
                "GcpProvisionCatalogTask: bucket-name CONFLICT for catalog "
                "'%s': %s", catalog_id, e,
            )
            await self._mark_conflict(catalog_id)
            raise PermanentTaskFailure(
                f"Bucket-name conflict for catalog '{catalog_id}': {e}"
            ) from e
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
                # On-prem / unauthorized: GCP cannot authenticate or init a
                # client. This is not a provisioning *failure* — the deployment
                # simply has no usable GCP. Mark BOTH steps 'skipped' so the
                # catalog still becomes ready instead of being wedged in
                # 'failed' (#1175). Marking the eventing step too is required by
                # the atomic contract: the terminal drain now resolves leftover
                # pending steps to 'failed', so a still-'pending' gcp_eventing
                # here would otherwise flip this (legitimately ready) catalog to
                # 'failed'. The task itself still dead-letters below so the
                # misconfiguration is surfaced to operators.
                await self._mark_step("skipped", catalog_id, step_key="gcp_bucket")
                await self._mark_step("skipped", catalog_id, step_key="gcp_eventing")
                raise PermanentTaskFailure(
                    f"GCP unavailable, cannot provision '{catalog_id}': {e}"
                ) from e
            logger.error(
                "GcpProvisionCatalogTask FAILED for %r (retryable): %s",
                catalog_id, e, exc_info=True,
            )
            raise
        except Exception as e:
            logger.error(
                "CRITICAL: GcpProvisionCatalogTask FAILED for %s: %s",
                catalog_id, e, exc_info=True,
            )
            await self._mark_failed(catalog_id)
            raise

    async def _mark_step(
        self,
        step_status: str,
        catalog_id: Optional[str],
        step_key: str = "gcp_bucket",
    ) -> None:
        """Mark a provisioning-checklist step terminal (#1175).

        Defaults to the ``gcp_bucket`` step (the bucket-phase error handlers),
        but accepts ``step_key`` so the eventing handler can mark
        ``gcp_eventing`` terminal too — no provisioner path may leave a step
        ``pending`` or the catalog is wedged in ``provisioning`` forever.

        ``failed`` flips the catalog to ``failed`` (the fail-fast guard then
        rejects writes with 409); ``skipped`` lets the catalog become ready
        anyway (on-prem / unauthorized, where GCP simply does not apply).

        Best-effort: if the call raises (DB down, protocol unavailable) we
        swallow — the task's own terminal status still lands via the runner,
        and a later ``gcp.init.*`` log line surfaces the problem. Silently
        proceeding beats masking the original exception about to be re-raised.
        """
        if not catalog_id:
            return
        try:
            catalogs = _get_catalog_protocol()
            await catalogs.mark_provisioning_step(catalog_id, step_key, step_status)
            logger.info(
                "GcpProvisionCatalogTask: catalog '%s' %s step → %s.",
                catalog_id, step_key, step_status,
            )
        except Exception as mark_err:  # pragma: no cover — diagnostic best-effort
            logger.error(
                "GcpProvisionCatalogTask: failed to mark catalog '%s' %s "
                "step '%s': %s", catalog_id, step_key, step_status, mark_err,
            )

    async def _mark_failed(self, catalog_id: Optional[str]) -> None:
        """Mark the ``gcp_bucket`` step ``failed`` (→ catalog ``failed``)."""
        await self._mark_step("failed", catalog_id)

    async def _mark_conflict(self, catalog_id: Optional[str]) -> None:
        """Flip ``catalog.catalogs.provisioning_status`` → ``'conflict'``.

        Distinct from ``'failed'``: a conflict is a terminal, non-retryable
        state caused by a bucket-name collision the platform cannot resolve on
        its own (the name belongs to another project, or another catalog).
        Like :meth:`_mark_failed` this is best-effort — the task's own
        PermanentTaskFailure still lands via the runner.
        """
        if not catalog_id:
            return
        try:
            catalogs = _get_catalog_protocol()
            await catalogs.update_provisioning_status(catalog_id, "conflict")
            logger.info(
                "GcpProvisionCatalogTask: catalog '%s' marked CONFLICT "
                "(bucket name unavailable; not retried, no resources deleted).",
                catalog_id,
            )
        except Exception as mark_err:  # pragma: no cover — diagnostic best-effort
            logger.error(
                "GcpProvisionCatalogTask: failed to mark catalog '%s' "
                "provisioning_status='conflict': %s", catalog_id, mark_err,
            )

