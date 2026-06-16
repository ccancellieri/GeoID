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
from typing import Any, Optional

from sqlalchemy.ext.asyncio import AsyncEngine

from dynastore.modules.processes.models import Process, StatusInfo
from dynastore.modules.processes.protocols import ProcessTaskProtocol
from dynastore.modules.tasks.maintenance import requeue_dead_letter_events_by_type
from dynastore.modules.tasks.models import TaskPayload
from dynastore.tools.protocol_helpers import get_engine

from .definition import REQUEUE_DEAD_LETTER_EVENTS_PROCESS_DEFINITION
from .models import RequeueDeadLetterEventsRequest

logger = logging.getLogger(__name__)


class RequeueDeadLetterEventsTask(
    ProcessTaskProtocol[
        Process, TaskPayload[RequeueDeadLetterEventsRequest], Optional[StatusInfo]
    ]
):
    """OGC Process: bulk-requeue DEAD_LETTER rows of an event_type.

    Delegates to :func:`requeue_dead_letter_events_by_type`.  Events are
    platform-tenanted (the ``schema_name`` column carries the tenant tag),
    so no URL-path scope translation is required — the process is PLATFORM-
    scoped only and replays every matching DEAD_LETTER row.
    """

    @staticmethod
    def get_definition() -> Process:
        return REQUEUE_DEAD_LETTER_EVENTS_PROCESS_DEFINITION

    def __init__(self, app_state: Any = None):
        self.app_state = app_state
        self.engine = get_engine()

    async def run(
        self, payload: TaskPayload[RequeueDeadLetterEventsRequest]
    ) -> Optional[StatusInfo]:
        request = payload.inputs
        if isinstance(request, dict):
            request = RequeueDeadLetterEventsRequest(**request)

        if not isinstance(self.engine, AsyncEngine):
            raise RuntimeError(
                "requeue_dead_letter_events: requires an AsyncEngine "
                f"(got {type(self.engine).__name__}).",
            )

        count = await requeue_dead_letter_events_by_type(
            engine=self.engine,
            event_type=request.event_type,
            since=request.since,
            limit=request.limit,
            reset_retries=request.reset_retries,
        )

        job_id = payload.task_id
        message = (
            f"Requeued {count} DEAD_LETTER event(s) of type "
            f"{request.event_type!r}"
        )
        logger.info(message)
        return StatusInfo(
            jobID=job_id,
            status="successful",
            message=message,
            progress=100,
            links=[],
        )
