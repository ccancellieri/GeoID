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

"""Durable task for running core tenant-provisioning steps (#2329).

This task re-executes the same steps as ``CatalogService._run_core_init``
against an already-committed ``catalog.catalogs`` row.  Because every DDL
step is ``IF NOT EXISTS``, the task is fully idempotent: a second run is a
no-op at the SQL level.

Registered but not yet wired into ``create_catalog`` (PR1).  PR2 will
enqueue this task from ``create_catalog`` and remove the inline call.
"""

import logging
from typing import Any, Dict, Optional

from pydantic import BaseModel

from dynastore.tasks.protocols import TaskProtocol
from dynastore.modules.tasks.models import (
    TaskPayload,
    PermanentTaskFailure,
)
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.modules.catalog.catalog_service import get_catalog_engine
from dynastore.tasks._helpers import _get_catalog_protocol

logger = logging.getLogger(__name__)


class CatalogCoreInitInputs(BaseModel):
    catalog_id: str
    external_id: str


class CatalogCoreInitTask(TaskProtocol):
    """Durable task that runs core tenant-init steps for a committed catalog row.

    Idempotent: all DDL uses ``IF NOT EXISTS``; duplicate runs are safe.
    Marks the ``catalog_core`` provisioning-checklist step ``complete`` on
    success and ``failed`` on permanent error.
    """

    task_type = "catalog_core_init"
    priority: int = 50

    async def run(self, payload: TaskPayload[CatalogCoreInitInputs]) -> Dict[str, Any]:
        catalog_id: Optional[str] = None
        try:
            inputs = payload.inputs
            catalog_id = inputs.catalog_id
            external_id = inputs.external_id

            if not catalog_id:
                raise PermanentTaskFailure("Missing 'catalog_id' in task inputs")
            if not external_id:
                raise PermanentTaskFailure("Missing 'external_id' in task inputs")

            logger.info(
                "CatalogCoreInitTask: running core init for catalog '%s' (external='%s')",
                catalog_id, external_id,
            )

            catalogs = _get_catalog_protocol()

            # Fetch the committed catalog row so _run_core_init has the model.
            catalog_model = await catalogs.get_catalog_model(catalog_id)
            if catalog_model is None:
                raise PermanentTaskFailure(
                    f"Catalog '{catalog_id}' not found — cannot run core init"
                )

            run_core_init = getattr(catalogs, "_run_core_init", None)
            if run_core_init is None:
                raise RuntimeError(
                    f"CatalogsProtocol implementation {type(catalogs).__name__} "
                    f"does not expose _run_core_init; cannot run core init task"
                )

            # physical_schema == internal id (column was dropped; id IS the schema)
            physical_schema = catalog_id

            async with managed_transaction(get_catalog_engine()) as conn:
                await run_core_init(conn, catalog_model, external_id, physical_schema)

            await catalogs.mark_provisioning_step(catalog_id, "catalog_core", "complete")
            logger.info(
                "CatalogCoreInitTask: catalog '%s' catalog_core step COMPLETE.",
                catalog_id,
            )

            return {
                "catalog_id": catalog_id,
                "external_id": external_id,
                "status": "complete",
            }

        except PermanentTaskFailure:
            await self._mark_step("failed", catalog_id)
            raise
        except Exception as exc:
            logger.error(
                "CatalogCoreInitTask FAILED for catalog '%s': %s",
                catalog_id, exc, exc_info=True,
            )
            await self._mark_step("failed", catalog_id)
            raise

    async def _mark_step(
        self,
        step_status: str,
        catalog_id: Optional[str],
        step_key: str = "catalog_core",
    ) -> None:
        """Mark a provisioning-checklist step terminal.

        Best-effort: if the call raises, we swallow so the original exception
        is not masked.
        """
        if not catalog_id:
            return
        try:
            catalogs = _get_catalog_protocol()
            await catalogs.mark_provisioning_step(catalog_id, step_key, step_status)
            logger.info(
                "CatalogCoreInitTask: catalog '%s' %s step → %s.",
                catalog_id, step_key, step_status,
            )
        except Exception as mark_err:  # pragma: no cover — diagnostic best-effort
            logger.error(
                "CatalogCoreInitTask: failed to mark catalog '%s' %s "
                "step '%s': %s", catalog_id, step_key, step_status, mark_err,
            )
