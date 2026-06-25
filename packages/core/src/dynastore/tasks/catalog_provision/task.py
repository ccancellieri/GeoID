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
import inspect
import logging
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from dynastore.tasks.protocols import TaskProtocol
from dynastore.modules.tasks.models import (
    TaskPayload,
    PermanentTaskFailure,
)
from dynastore.modules import get_protocol
from dynastore.models.protocols import CatalogsProtocol
from dynastore.modules.catalog.catalog_service import get_catalog_engine
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.modules.catalog.provisioning_registry import provisioning_registry

logger = logging.getLogger(__name__)

# Default concurrency bound for provisioners within a single priority group.
# Overridable via TasksPluginConfig.provisioning_group_concurrency.
_DEFAULT_GROUP_CONCURRENCY = 4


def _get_catalog_protocol() -> CatalogsProtocol:
    protocol = get_protocol(CatalogsProtocol)
    if not protocol:
        raise RuntimeError("CatalogsProtocol not available")
    return protocol


async def _get_group_concurrency() -> int:
    """Read provisioning_group_concurrency from TasksPluginConfig; default 4.

    Fail-open: any config read failure returns the default so provisioning
    always proceeds.
    """
    try:
        from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
        from dynastore.modules.tasks.tasks_config import TasksPluginConfig

        mgr = get_protocol(PlatformConfigsProtocol)
        if mgr is not None:
            cfg = await mgr.get_config(TasksPluginConfig)
            if isinstance(cfg, TasksPluginConfig):
                return cfg.provisioning_group_concurrency
    except Exception:  # noqa: BLE001 — best-effort; use default on any failure
        pass
    return _DEFAULT_GROUP_CONCURRENCY


class CatalogProvisionInputs(BaseModel):
    catalog_id: str
    scope: str = "catalog"
    operation: str = "provision"
    collection_id: Optional[str] = None


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
        if catalog_model is None:
            raise PermanentTaskFailure(
                f"Catalog '{catalog_id}' not found — cannot run provisioning"
            )

        external_id: Optional[str] = getattr(catalog_model, "external_id", None)

        # Fetch the active provisioners grouped by ascending priority.
        async with managed_transaction(get_catalog_engine()) as conn:
            groups: List[List[Any]] = await provisioning_registry.active_provisioners(
                catalog_id, conn, scope=scope
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

            for provisioner, result in zip(group, results):
                if isinstance(result, BaseException):
                    steps_failed += 1
                    logger.error(
                        "CatalogProvisionTask: provisioner '%s' failed for "
                        "catalog '%s': %s",
                        provisioner.key, catalog_id, result, exc_info=False,
                    )
                    # Abort: re-raise so the task reaches terminal failed state.
                    # The checklist drain in apply_terminal_action will fill
                    # remaining pending steps with 'failed'.
                    raise result  # type: ignore[misc]
                elif result is True:
                    steps_completed += 1
                # result is False → hook was None/skipped; no step marked

        logger.info(
            "CatalogProvisionTask: completed operation='%s' scope='%s' "
            "catalog='%s' groups=%d completed=%d failed=%d",
            operation, scope, catalog_id,
            groups_run, steps_completed, steps_failed,
        )
        return {
            "catalog_id": catalog_id,
            "scope": scope,
            "operation": operation,
            "groups_run": groups_run,
            "steps_completed": steps_completed,
            "steps_failed": steps_failed,
        }

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
                if inspect.iscoroutinefunction(hook):
                    await hook(**ctx)
                else:
                    hook(**ctx)
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
