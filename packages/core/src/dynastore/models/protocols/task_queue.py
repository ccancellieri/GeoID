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

"""
Protocol for durable task queue operations.

Extends TasksProtocol (CRUD) with queue semantics: enqueue with dedup,
atomic claiming, completion, failure with retry, heartbeat, and maintenance.

Implementations:
- PostgreSQL: SELECT FOR UPDATE SKIP LOCKED (default, on-premise + Cloud Run)
- Cloud Tasks: Google Cloud Tasks push model (future)
"""

from typing import (
    Any,
    Dict,
    List,
    Optional,
    Protocol,
    runtime_checkable,
)
from datetime import timedelta
import uuid

from dynastore.models.protocols.tasks import TasksProtocol


@runtime_checkable
class TaskQueueProtocol(TasksProtocol, Protocol):
    """
    Protocol for durable task queue operations.

    Provides queue semantics on top of TasksProtocol CRUD:
    - Enqueue with deduplication (dedup_key)
    - Atomic claim with execution mode + task type filtering
    - Completion and failure with retry/dead-letter
    - Heartbeat extension for long-running tasks
    - Stale task recovery (janitor)
    - Orphan cleanup for deleted catalogs

    The `schema` parameter in inherited TasksProtocol methods now refers to the
    `schema_name` column value in the global `tasks.tasks` table, not a
    PostgreSQL schema qualifier.
    """

    async def enqueue(
        self,
        engine: Any,
        task_data: Any,
        schema_name: str,
        dedup_key: Optional[str] = None,
        execution_mode: str = "ASYNCHRONOUS",
        scope: str = "CATALOG",
    ) -> Optional[Any]:
        """
        Enqueue a task into the global task queue.

        Returns the created Task, or None if dedup_key already exists
        (ON CONFLICT DO NOTHING).
        """
        ...

    async def claim_next(
        self,
        engine: Any,
        async_task_types: List[str],
        sync_task_types: List[str],
        visibility_timeout: timedelta,
        owner_id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Atomically claim the next available task matching the given types
        and execution modes.

        Uses FOR UPDATE SKIP LOCKED to prevent contention across instances.
        Returns a dict with task fields including schema_name, or None if
        no matching task is available.
        """
        ...

    async def complete(
        self,
        engine: Any,
        task_id: uuid.UUID,
        timestamp: Any,
        outputs: Optional[Any] = None,
    ) -> None:
        """Mark a claimed task as COMPLETED with optional outputs."""
        ...

    async def fail(
        self,
        engine: Any,
        task_id: uuid.UUID,
        timestamp: Any,
        error_message: str,
        retry: bool = True,
    ) -> None:
        """
        Mark a claimed task as failed.

        If retry=True and retry_count < max_retries, requeue with
        exponential backoff. Otherwise move to DEAD_LETTER.
        """
        ...

    async def heartbeat(
        self,
        engine: Any,
        task_ids: List[uuid.UUID],
        visibility_timeout: timedelta,
    ) -> None:
        """Extend locked_until for active tasks (batched heartbeat)."""
        ...

    async def find_stale(
        self,
        engine: Any,
        stale_threshold: timedelta,
        schema_name: Optional[str] = None,
    ) -> List[Any]:
        """
        Find active tasks with expired locks (janitor use).

        If schema_name is None, scans globally.
        """
        ...

    async def cleanup_orphans(self, engine: Any, grace_period: timedelta) -> int:
        """
        Move tasks for deleted catalogs to DEAD_LETTER.

        Async tasks are kept for grace_period after catalog deletion before
        being dead-lettered, to allow in-flight tasks to complete.

        Returns count of tasks moved to DEAD_LETTER.
        """
        ...

    async def get_capable_task_types(self) -> Dict[str, List[str]]:
        """
        Returns task types this instance can execute, grouped by execution mode.

        Returns dict like:
            {"ASYNCHRONOUS": ["ingestion", "gcp_provision"], "SYNCHRONOUS": ["gdal"]}

        Built from runner capability map (runner.can_handle()).
        """
        ...
