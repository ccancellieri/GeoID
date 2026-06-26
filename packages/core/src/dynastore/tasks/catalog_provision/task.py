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

"""Durable executor task for the provisioning-checklist registry (#2329).

Runs the registered provisioners for a catalog or collection in
priority order.  Provisioners at the same priority level run concurrently
under a bounded semaphore (``provisioning_group_concurrency`` from config,
default 4) so the dispatcher pool is never saturated by a large set of
parallel provisioners.

Registered here; nothing enqueues it yet.  The enqueue site is added in a
subsequent PR when ``create_catalog`` is migrated off the inline
provisioning call.

The task is fully idempotent: ``mark_provisioning_step`` is an upsert and
individual provisioner hooks are expected to use IF-NOT-EXISTS semantics.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from dynastore.tasks.protocols import TaskProtocol
from dynastore.modules.tasks.models import (
    TaskPayload,
    PermanentTaskFailure,
)
from dynastore.models.protocols import CatalogsProtocol
from dynastore.modules.catalog.catalog_service import get_catalog_engine
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.modules.catalog.provisioning_registry import provisioning_registry
from dynastore.tasks._helpers import _get_catalog_protocol, call_hook, get_tasks_config

logger = logging.getLogger(__name__)

# Default concurrency bound for provisioners within a single priority group.
# Overridable via TasksPluginConfig.provisioning_group_concurrency.
_DEFAULT_GROUP_CONCURRENCY = 4


async def _get_group_concurrency() -> int:
    """Read provisioning_group_concurrency from TasksPluginConfig; default 4.

    Fail-open: any config read failure returns the default so provisioning
    always proceeds.
    """
    cfg = await get_tasks_config()
    if cfg is not None:
        return cfg.provisioning_group_concurrency
    return _DEFAULT_GROUP_CONCURRENCY


class CatalogProvisionInputs(BaseModel):
    catalog_id: str
    scope: str = "catalog"
    operation: str = "provision"
    collection_id: Optional[str] = None
    # Reprovision control (#2395). When False (default), a ``provision`` run
    # skips provisioners whose checklist step is already satisfied
    # (``complete`` / ``skipped``) and re-runs only the unsatisfied ones
    # (``failed`` / ``degraded`` / ``pending`` / missing) — "reprovision only
    # what failed". When True, every provisioner runs regardless of its
    # current step (full replay). Ignored for deprovision operations, which
    # always run every teardown hook.
    force: bool = False
    # Deprovision context (#2340): config snapshot captured before any
    # deprovision hook runs. Required for deprovision operations so
    # external-resource cleanup (GCP bucket, eventing) can act on the
    # pre-deletion state even though the catalog_core hook drops the schema.
    config_snapshot: Optional[Dict[str, Any]] = None


class CatalogProvisionTask(TaskProtocol):
    """Durable executor for catalog and collection provisioning checklists.

    Reads the active provisioners from the registry, groups them by priority
    (ascending), and executes each group sequentially.  Within a group,
    provisioners run concurrently under a bounded semaphore to avoid
    saturating the dispatcher pool.

    Each provisioner's hook is marked ``complete`` on success or ``failed``
    on exception.  On any failure the task aborts immediately (the exception
    propagates so the task row reaches a terminal failed state) and the
    existing checklist drain fills remaining pending steps with ``failed``.

    The task is idempotent: ``mark_provisioning_step`` is an upsert and
    provisioner hooks are expected to use IF-NOT-EXISTS semantics.
    """

    task_type = "catalog_provision"
    priority: int = 50

    async def run(self, payload: TaskPayload[CatalogProvisionInputs]) -> Dict[str, Any]:
        inputs = payload.inputs
        catalog_id = inputs.catalog_id
        scope = inputs.scope
        operation = inputs.operation
        collection_id = inputs.collection_id
        force = inputs.force
        config_snapshot = inputs.config_snapshot

        if not catalog_id:
            raise PermanentTaskFailure("Missing 'catalog_id' in task inputs")
        if operation not in ("provision", "deprovision_soft", "deprovision_hard"):
            raise PermanentTaskFailure(
                f"Invalid operation '{operation}': must be one of "
                "'provision', 'deprovision_soft', 'deprovision_hard'"
            )

        logger.info(
            "CatalogProvisionTask: starting operation='%s' scope='%s' "
            "for catalog '%s'",
            operation, scope, catalog_id,
        )

        catalogs = _get_catalog_protocol()

        catalog_model = await catalogs.get_catalog_model(catalog_id)

        external_id: Optional[str] = getattr(catalog_model, "external_id", None)

        if catalog_model is None:
            if operation in ("deprovision_soft", "deprovision_hard"):
                # The hard-delete dispatch tombstones the catalog row (sets
                # ``deleted_at``) BEFORE enqueuing this task, and
                # ``get_catalog_model`` filters out tombstoned rows — so a
                # deprovision ALWAYS sees None here on its first run. The
                # teardown provisioners (catalog_core et al.) are written to
                # operate on tombstoned rows by id, so we must not bail out:
                # look the row up tombstone-inclusive and, if it still exists,
                # fall through and run the teardown. Only a row that is fully
                # gone (a retry after catalog_core already hard-deleted it)
                # means the teardown end-state is already satisfied — treat
                # that as an idempotent success instead of wedging the delete
                # in a permanent-failed loop that retries can never clear.
                row = await self._lookup_tombstoned_catalog(catalog_id)
                if row is None:
                    logger.warning(
                        "CatalogProvisionTask: catalog '%s' already absent for "
                        "operation='%s' — nothing to deprovision; treating as "
                        "idempotent success",
                        catalog_id, operation,
                    )
                    return {
                        "catalog_id": catalog_id,
                        "scope": scope,
                        "operation": operation,
                        "groups_run": 0,
                        "steps_completed": 0,
                        "steps_failed": 0,
                        "already_absent": True,
                    }
                external_id = row.get("external_id")
                logger.info(
                    "CatalogProvisionTask: catalog '%s' is tombstoned "
                    "(deleted_at set) — running deprovision teardown",
                    catalog_id,
                )
            else:
                raise PermanentTaskFailure(
                    f"Catalog '{catalog_id}' not found — cannot run provisioning"
                )

        # Fetch the active provisioners grouped by ascending priority.
        async with managed_transaction(get_catalog_engine()) as conn:
            groups: List[List[Any]] = await provisioning_registry.active_provisioners(
                catalog_id, conn, scope=scope
            )

        # For deprovision operations, reverse the group order so higher-priority
        # provisioners (GCP at priority 1) run BEFORE lower-priority ones
        # (catalog_core at priority 0). This ensures external resources (bucket,
        # eventing) are cleaned up before the schema is dropped.
        if operation in ("deprovision_soft", "deprovision_hard"):
            groups = list(reversed(groups))
            logger.info(
                "CatalogProvisionTask: deprovision for catalog '%s' — "
                "running %d groups in reverse priority order",
                catalog_id, len(groups),
            )

        # On every provision run, reset non-satisfied checklist steps to
        # 'pending' and flip the catalog status to 'provisioning' so the
        # status transitions monotonically (failed → provisioning → ready)
        # rather than staying on 'failed' while new steps complete.
        # With force=True every step is reset unconditionally (full replay).
        # When the checklist is empty or absent (no active provisioners, fresh
        # create without a checklist row) this is a no-op — reset returns {}.
        if operation == "provision":
            await catalogs.reset_checklist_for_reprovision(catalog_id, force=force)

        # Reprovision only what failed (#2395). For a ``provision`` run that is
        # not a forced full replay, drop provisioners whose checklist step is
        # already satisfied (``complete`` / ``skipped``) so a reprovision
        # re-runs only the unsatisfied steps (``failed`` / ``degraded`` /
        # ``pending`` / missing). On the first create the checklist is freshly
        # all-``pending``, so nothing is skipped and behaviour is unchanged;
        # on a max_retries replay the completed steps are skipped instead of
        # relying on every hook honouring IF-NOT-EXISTS. Deprovision always
        # runs every teardown hook, so the filter is provision-only.
        if operation == "provision" and not force:
            from dynastore.modules.catalog.provisioning_registry import (
                STEP_COMPLETE,
                STEP_SKIPPED,
            )

            checklist = await catalogs.get_provisioning_checklist(catalog_id)
            satisfied = {
                key
                for key, state in checklist.items()
                if state in (STEP_COMPLETE, STEP_SKIPPED)
            }
            if satisfied:
                groups = [
                    [p for p in group if p.key not in satisfied] for group in groups
                ]
                groups = [group for group in groups if group]
                logger.info(
                    "CatalogProvisionTask: reprovision for catalog '%s' — "
                    "skipping satisfied steps %s; running remaining provisioners",
                    catalog_id, sorted(satisfied),
                )

        concurrency = await _get_group_concurrency()
        semaphore = asyncio.Semaphore(concurrency)

        groups_run = 0
        steps_completed = 0
        steps_failed = 0

        ctx = {
            "catalog_id": catalog_id,
            "external_id": external_id,
            "scope": scope,
            "operation": operation,
            "collection_id": collection_id,
            "config_snapshot": config_snapshot,
        }

        for group in groups:
            groups_run += 1
            # Run all provisioners in this priority group concurrently, bounded
            # by the semaphore.  asyncio.gather collects results and exceptions;
            # we inspect them individually so one failure can abort the run.
            tasks = [
                self._run_provisioner(
                    provisioner, operation, catalogs, semaphore, ctx
                )
                for provisioner in group
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            first_exc: Optional[Exception] = None
            for provisioner, result in zip(group, results):
                if isinstance(result, BaseException) and not isinstance(result, Exception):
                    # Hard-cancellation (KeyboardInterrupt, CancelledError, …):
                    # re-raise immediately without marking the step or tallying.
                    raise result  # type: ignore[misc]
                if isinstance(result, Exception):
                    steps_failed += 1
                    logger.error(
                        "CatalogProvisionTask: provisioner '%s' failed for "
                        "catalog '%s': %s",
                        provisioner.key, catalog_id, result, exc_info=False,
                    )
                    if first_exc is None:
                        first_exc = result
                elif result is True:
                    steps_completed += 1
                # result is False → hook was None/skipped; no step marked

            if first_exc is not None:
                # All group results have been inspected and logged; abort with
                # the first exception so the task row reaches terminal failed.
                raise first_exc

        logger.info(
            "CatalogProvisionTask: completed operation='%s' scope='%s' "
            "catalog='%s' groups=%d completed=%d failed=%d",
            operation, scope, catalog_id,
            groups_run, steps_completed, steps_failed,
        )

        if operation == "provision" and groups_run > 0:
            # Emit lifecycle events after all provisioning steps complete so
            # non-provisioning subscribers (webhook outbox, audit log) fire
            # after the tenant schema is ready. Gated on ``groups_run > 0`` so a
            # no-op reprovision (every step already satisfied, nothing ran)
            # stays silent and does not re-emit creation events (#2395).
            await self._emit_catalog_created_events(catalog_id)

        return {
            "catalog_id": catalog_id,
            "scope": scope,
            "operation": operation,
            "groups_run": groups_run,
            "steps_completed": steps_completed,
            "steps_failed": steps_failed,
        }

    async def _lookup_tombstoned_catalog(
        self, catalog_id: str
    ) -> Optional[Dict[str, Any]]:
        """Read a catalog row by internal id, INCLUDING tombstoned rows.

        ``get_catalog_model`` filters ``deleted_at IS NULL``, but a deprovision
        runs against a row the dispatch already tombstoned. Mirror the
        tombstone-inclusive lookup used by the catalog_core deprovision hook so
        the task can tell "tombstoned, still needs teardown" (row present) from
        "fully gone, idempotent success" (row absent). Returns the row dict or
        None when no row exists for the id.
        """
        from dynastore.modules.db_config.query_executor import (
            DQLQuery,
            ResultHandler,
        )

        query = DQLQuery(
            "SELECT id, external_id FROM catalog.catalogs WHERE id = :id;",
            result_handler=ResultHandler.ONE_DICT,
        )
        async with managed_transaction(get_catalog_engine()) as conn:
            return await query.execute(conn, id=catalog_id)

    async def _emit_catalog_created_events(self, catalog_id: str) -> None:
        """Emit CATALOG_CREATION and AFTER_CATALOG_CREATION lifecycle events.

        Fires after all provisioning checklist steps complete so listeners
        (webhook outbox, audit log) observe a fully-ready catalog.  Each emit
        is best-effort — a failure is logged but does not fail the task.
        """
        from dynastore.modules.catalog.event_service import (
            CatalogEventType,
            emit_event,
        )
        from dynastore.tools.async_utils import signal_bus

        try:
            await emit_event(
                CatalogEventType.CATALOG_CREATION,
                catalog_id=catalog_id,
            )
        except Exception:
            logger.warning(
                "CatalogProvisionTask: CATALOG_CREATION emit failed for '%s'",
                catalog_id,
                exc_info=True,
            )

        try:
            await emit_event(
                CatalogEventType.AFTER_CATALOG_CREATION,
                catalog_id=catalog_id,
            )
        except Exception:
            logger.warning(
                "CatalogProvisionTask: AFTER_CATALOG_CREATION emit failed for '%s'",
                catalog_id,
                exc_info=True,
            )

        try:
            await signal_bus.emit("AFTER_CATALOG_CREATION", identifier=catalog_id)
        except Exception:
            logger.warning(
                "CatalogProvisionTask: signal_bus AFTER_CATALOG_CREATION emit "
                "failed for '%s'",
                catalog_id,
                exc_info=True,
            )

    async def _run_provisioner(
        self,
        provisioner: Any,
        operation: str,
        catalogs: CatalogsProtocol,
        semaphore: asyncio.Semaphore,
        ctx: Dict[str, Any],
    ) -> bool:
        """Execute one provisioner under the semaphore.

        Returns:
            True  — hook ran and step was marked complete.
            False — hook is None; step skipped (no checklist mark).

        Raises on hook failure after marking the step 'failed'.
        """
        hook = provisioner.provision if operation == "provision" else provisioner.deprovision
        if hook is None:
            logger.debug(
                "CatalogProvisionTask: provisioner '%s' has no hook for "
                "operation '%s' — skipping",
                provisioner.key, operation,
            )
            return False

        async with semaphore:
            try:
                await call_hook(hook, **ctx)
            except Exception as exc:
                # Mark the step failed before re-raising so the caller can
                # tally it and abort.
                try:
                    await catalogs.mark_provisioning_step(
                        ctx["catalog_id"], provisioner.key, "failed"
                    )
                except Exception as mark_err:  # pragma: no cover — best-effort
                    logger.error(
                        "CatalogProvisionTask: failed to mark step '%s' "
                        "as failed for catalog '%s': %s",
                        provisioner.key, ctx["catalog_id"], mark_err,
                    )
                raise exc

            await catalogs.mark_provisioning_step(
                ctx["catalog_id"], provisioner.key, "complete"
            )
            logger.debug(
                "CatalogProvisionTask: provisioner '%s' → complete",
                provisioner.key,
            )
            return True
