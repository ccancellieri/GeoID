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

import logging
from typing import Any, Dict, Optional

from dynastore.models.protocols import CatalogsProtocol
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.modules.processes.models import Process, StatusInfo
from dynastore.modules.processes.protocols import ProcessTaskProtocol
from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.models import TaskPayload, TaskStatusEnum, TaskUpdate
from dynastore.tools.discovery import get_protocol
from dynastore.tools.protocol_helpers import get_engine

from .definition import DIMENSIONS_MATERIALIZE_PROCESS_DEFINITION
from .models import DimensionsMaterializeRequest

logger = logging.getLogger(__name__)


class DimensionsMaterializeTask(
    ProcessTaskProtocol[
        Process, TaskPayload[DimensionsMaterializeRequest], Optional[StatusInfo]
    ]
):
    """OGC Process: materialise registered OGC Dimensions as RECORDS
    collections under the ``_dimensions_`` catalog.

    Replaces the in-lifespan materialisation that used to run on every
    pod boot. The task is idempotent per-dimension via a
    ``cube:dimensions`` equality check; unchanged dimensions are
    skipped without touching the DB.
    """

    @staticmethod
    def get_definition() -> Process:
        return DIMENSIONS_MATERIALIZE_PROCESS_DEFINITION

    def __init__(self, app_state: Any = None):
        self.app_state = app_state
        self.engine = get_engine()

    async def run(
        self, payload: TaskPayload[DimensionsMaterializeRequest]
    ) -> Optional[StatusInfo]:
        # Late import: going through the extension avoids pulling
        # ``ogc_dimensions`` into scopes that don't include the dimensions
        # extra (task module-import time would fail otherwise).
        from dynastore.extensions.dimensions.dimensions_extension import (
            get_registered_dimensions,
            materialize_all_dimensions,
        )

        request = payload.inputs
        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None or self.engine is None:
            raise RuntimeError(
                "Required protocols/engine unavailable for dimensions_materialize task."
            )
        engine = self.engine  # narrowed to non-None above

        # PLATFORM-scoped tasks persist status rows to the ``public`` schema
        # (see ProcessScope docstring). We do NOT resolve against
        # ``_dimensions_`` because the catalog may not exist yet on a fresh
        # deploy — materialization itself creates it via ensure_catalog_exists.
        schema = "public"

        # Ensure task_storage exists in the target schema (cellular safety).
        try:
            async with managed_transaction(engine) as conn:
                await tasks_module.ensure_task_storage_exists(conn, schema)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "dimensions_materialize: could not ensure task storage in "
                "%r: %s. Task state updates may be skipped.",
                schema, exc,
            )

        async def _report(update: TaskUpdate) -> None:
            if not payload.task_id:
                return
            try:
                await tasks_module.update_task(
                    engine, payload.task_id, update, schema=schema,
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    "dimensions_materialize: update_task failed: %s", exc,
                )

        await _report(TaskUpdate(status=TaskStatusEnum.RUNNING, progress=0))

        try:
            dimensions = get_registered_dimensions()
            if not dimensions:
                msg = (
                    "No dimensions registered — the dimensions extension is "
                    "not loaded in this process. Rebuild the service image "
                    "with SCOPE including 'extension_dimensions'."
                )
                logger.error(msg)
                await _report(
                    TaskUpdate(status=TaskStatusEnum.FAILED, error_message=msg)
                )
                raise RuntimeError(msg)

            results: Dict[str, Dict[str, Any]] = await materialize_all_dimensions(
                dimensions,
                dim_names=request.dim_names,
                force=request.force,
            )

            summary = _summarise(results)
            logger.info(
                "dimensions_materialize: processed=%d materialized=%d "
                "skipped=%d failed=%d total_records=%d",
                summary["processed"], summary["materialized"],
                summary["skipped"], summary["failed"], summary["total_records"],
            )

            outputs = {"summary": summary, "results": results}
            final_status = (
                TaskStatusEnum.FAILED
                if summary["failed"]
                else TaskStatusEnum.COMPLETED
            )
            await _report(
                TaskUpdate(
                    status=final_status,
                    progress=100,
                    outputs=outputs,
                    error_message=(
                        f"{summary['failed']} dimension(s) failed — see "
                        f"outputs.results for details"
                        if summary["failed"]
                        else None
                    ),
                )
            )
            return None  # state persisted via tasks_module
        except Exception as exc:
            logger.error(
                "dimensions_materialize task failed: %s", exc, exc_info=True,
            )
            await _report(
                TaskUpdate(
                    status=TaskStatusEnum.FAILED, error_message=str(exc),
                )
            )
            raise


def _summarise(results: Dict[str, Dict[str, Any]]) -> Dict[str, int]:
    materialized = sum(1 for r in results.values() if r.get("reason") == "materialized")
    skipped = sum(1 for r in results.values() if r.get("skipped"))
    failed = sum(1 for r in results.values() if r.get("reason") in ("error", "not_registered"))
    total_records = sum(int(r.get("materialized", 0) or 0) for r in results.values())
    return {
        "processed": len(results),
        "materialized": materialized,
        "skipped": skipped,
        "failed": failed,
        "total_records": total_records,
    }
