#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

import logging
from typing import Any, Dict, Optional

from dynastore.modules.processes.models import Process, StatusInfo
from dynastore.modules.processes.protocols import ProcessTaskProtocol
from dynastore.modules.tasks.maintenance import requeue_dead_letter_tasks_by_type
from dynastore.modules.tasks.models import TaskPayload
from dynastore.tools.protocol_helpers import get_engine

from .definition import REQUEUE_DEAD_LETTER_PROCESS_DEFINITION
from .models import TASK_TYPE_INPUTS_KEYS, RequeueDeadLetterTasksRequest

logger = logging.getLogger(__name__)


class RequeueDeadLetterTasksTask(
    ProcessTaskProtocol[
        Process, TaskPayload[RequeueDeadLetterTasksRequest], Optional[StatusInfo]
    ]
):
    """OGC Process: bulk-requeue DEAD_LETTER rows of a task_type.

    Delegates to :func:`requeue_dead_letter_tasks_by_type`; the
    catalog/collection scope from the URL path is translated to the
    task-type-specific JSONB inputs filter via
    :data:`TASK_TYPE_INPUTS_KEYS`.
    """

    @staticmethod
    def get_definition() -> Process:
        return REQUEUE_DEAD_LETTER_PROCESS_DEFINITION

    def __init__(self, app_state: Any = None):
        self.app_state = app_state
        self.engine = get_engine()

    async def run(
        self, payload: TaskPayload[RequeueDeadLetterTasksRequest]
    ) -> Optional[StatusInfo]:
        request = payload.inputs
        if isinstance(request, dict):
            request = RequeueDeadLetterTasksRequest(**request)

        if self.engine is None:
            raise RuntimeError(
                "requeue_dead_letter_tasks: no database engine available.",
            )

        inputs_match = _build_inputs_match(
            task_type=request.task_type,
            catalog_id=request.catalog_id,
            collection_id=request.collection_id,
        )

        count = await requeue_dead_letter_tasks_by_type(
            engine=self.engine,
            task_type=request.task_type,
            since=request.since,
            limit=request.limit,
            reset_retries=request.reset_retries,
            inputs_match=inputs_match or None,
        )

        job_id = payload.task_id
        message = (
            f"Requeued {count} DEAD_LETTER row(s) of type "
            f"{request.task_type!r}"
            + (
                f" matching {inputs_match!r}" if inputs_match else ""
            )
        )
        logger.info(message)
        return StatusInfo(
            jobID=job_id,
            status="successful",
            message=message,
            progress=100,
            links=[],
        )


def _build_inputs_match(
    *,
    task_type: str,
    catalog_id: Optional[str],
    collection_id: Optional[str],
) -> Dict[str, str]:
    """Translate URL-path scope to JSONB inputs filter.

    Raises if the caller hits a scoped endpoint with a task_type that
    has no declared catalog/collection JSONB keys — there is no safe
    filter to apply and a silent fall-through to platform-wide replay
    would violate the URL contract.
    """
    if catalog_id is None and collection_id is None:
        return {}
    keys = TASK_TYPE_INPUTS_KEYS.get(task_type)
    if not keys:
        raise ValueError(
            f"requeue_dead_letter_tasks: task_type {task_type!r} cannot be "
            f"requeued on a catalog- or collection-scoped endpoint — its "
            f"JSONB inputs do not declare catalog/collection keys. Invoke "
            f"on the platform-scoped endpoint instead.",
        )
    out: Dict[str, str] = {}
    if catalog_id is not None:
        cat_key = keys.get("catalog_id")
        if cat_key is None:
            raise ValueError(
                f"requeue_dead_letter_tasks: task_type {task_type!r} has "
                f"no catalog JSONB key.",
            )
        out[cat_key] = catalog_id
    if collection_id is not None:
        col_key = keys.get("collection_id")
        if col_key is None:
            raise ValueError(
                f"requeue_dead_letter_tasks: task_type {task_type!r} has "
                f"no collection JSONB key.",
            )
        out[col_key] = collection_id
    return out
