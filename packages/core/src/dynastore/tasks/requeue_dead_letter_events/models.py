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

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class RequeueDeadLetterEventsRequest(BaseModel):
    """Input payload for the ``requeue_dead_letter_events`` OGC Process.

    Operator-driven bulk replay of DEAD_LETTER event rows in
    ``tasks.events``.  Events are platform-tenanted (the ``schema_name``
    column carries the tenant tag, not the URL path), so this process
    declares PLATFORM scope only — there is no catalog- or collection-scoped
    variant.

    Invoke after fixing the underlying handler failure that caused
    ``EventDrainTask`` to exhaust retries and send rows to DEAD_LETTER.
    """

    event_type: str = Field(
        description=(
            "event_type column value to replay (e.g. 'catalog_creation'). "
            "Required — matches the value stored in tasks.events."
        ),
    )
    since: Optional[datetime] = Field(
        default=None,
        description=(
            "If set, only replay rows whose created_at >= this timestamp. "
            "Use to scope a replay to a specific incident window."
        ),
    )
    limit: int = Field(
        default=1000,
        ge=1,
        le=100_000,
        description="Maximum number of rows to requeue in one call.",
    )
    reset_retries: bool = Field(
        default=True,
        description=(
            "If true (default), reset retry_count to 0 so the requeued row "
            "gets a fresh attempt budget. Set false to preserve the prior "
            "count when you expect the failure to recur."
        ),
    )
