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

from dynastore.modules.processes.models import (
    JobControlOptions,
    ProcessScope,
    TransmissionMode,
)
from dynastore.tools.process_factory import create_process_definition

from .models import RequeueDeadLetterEventsRequest

REQUEUE_DEAD_LETTER_EVENTS_PROCESS_DEFINITION = create_process_definition(
    id="requeue-dead-letter-events",
    title="Requeue DEAD_LETTER events",
    description=(
        "Bulk-requeue DEAD_LETTER event rows in tasks.events back to PENDING "
        "after an operator fixes the underlying handler failure that caused "
        "EventDrainTask to exhaust retries. Events are platform-tenanted, so "
        "this process is available on the platform-scoped endpoint only. "
        "Wakes the EventDrainTask drain co-transactionally after requeue. "
        "Returns the count of rows transitioned back to PENDING."
    ),
    version="1.0.0",
    input_model=RequeueDeadLetterEventsRequest,
    scopes=[
        ProcessScope.PLATFORM,
    ],
    job_control_options=[
        JobControlOptions.SYNC_EXECUTE,
        JobControlOptions.ASYNC_EXECUTE,
    ],
    output_transmission=[TransmissionMode.VALUE],
)
