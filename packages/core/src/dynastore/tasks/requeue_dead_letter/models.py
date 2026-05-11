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

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class RequeueDeadLetterTasksRequest(BaseModel):
    """Input payload for the ``requeue_dead_letter_tasks`` OGC Process.

    Operator-driven bulk replay of DEAD_LETTER task rows, intended as the
    companion action after a SCOPE drift is fixed that previously caused
    the reactive reaper (#502) to DLQ unclaimable rows.

    ``catalog_id`` / ``collection_id`` are injected from the URL path when
    the process is invoked on the catalog- or collection-scoped endpoint
    (the Processes service is the source of truth; clients should leave
    them empty in the body). When invoked on the platform-scoped endpoint
    both are ``None`` and the replay is unfiltered by entity scope.
    """

    task_type: str = Field(
        description=(
            "task_type to replay (e.g. 'index_propagation'). Required — "
            "matches the value the dispatcher uses to claim rows."
        ),
    )
    since: Optional[datetime] = Field(
        default=None,
        description=(
            "If set, only replay rows whose finished_at >= this timestamp. "
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
    catalog_id: Optional[str] = Field(
        default=None,
        description=(
            "Injected from URL path on catalog- and collection-scoped "
            "endpoints. Do not set in the body."
        ),
    )
    collection_id: Optional[str] = Field(
        default=None,
        description=(
            "Injected from URL path on the collection-scoped endpoint. "
            "Do not set in the body."
        ),
    )


# Mapping from URL-path identifiers to the JSONB key used by each
# task_type for the same logical scope. Only task_types that actually
# carry catalog/collection in their inputs payload appear here; for any
# other task_type the entity-scoped endpoints reject the request (no
# safe filter to apply).
TASK_TYPE_INPUTS_KEYS: dict[str, dict[str, str]] = {
    "index_propagation": {"catalog_id": "catalog", "collection_id": "collection"},
}
