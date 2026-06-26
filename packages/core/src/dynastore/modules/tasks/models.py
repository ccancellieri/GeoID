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

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, Dict, Any
from datetime import timedelta
from dataclasses import dataclass, field

# Re-exports kept for backward compatibility — these are the canonical
# task models defined in ``dynastore.models.tasks``; this module also
# adds runtime types (``RunnerContext``, ``RunnerCapabilities``, etc.).
from dynastore.models.tasks import (  # noqa: F401 — re-export
    TaskStatusEnum,
    TaskExecutionMode,
    TaskExecutionScope,
    TaskPayload,
    TaskBase,
    TaskCreate,
    TaskUpdate,
    Task,
    SpawnTaskRequest,
    TaskRef,
    TaskPage,
    RequeueResult,
)

from dynastore.modules.db_config.query_executor import DbResource

# Public surface. The canonical task models are defined in
# ``dynastore.models.tasks`` and re-exported here for backward compatibility;
# nothing in this module references them, so a per-file F401 sweep would strip
# the re-export and break every ``dynastore.modules.tasks.models`` importer.
# Listing them in ``__all__`` makes the re-export machine-checkable and immune
# to ``ruff --fix`` (ruff never reports a name that appears in ``__all__``) —
# the same guard applied across the cross-package re-export sites in #1747
# (refs #1547).
__all__ = [
    # cross-package re-exports from dynastore.models.tasks (load-bearing)
    "TaskStatusEnum",
    "TaskExecutionMode",
    "TaskExecutionScope",
    "TaskPayload",
    "TaskBase",
    "TaskCreate",
    "TaskUpdate",
    "Task",
    # Tasks API request/response DTOs (re-exported from dynastore.models.tasks)
    "SpawnTaskRequest",
    "TaskRef",
    "TaskPage",
    "RequeueResult",
    # runtime types defined in this module
    "PermanentTaskFailure",
    "DEFERRED_COMPLETION",
    "RunnerContext",
    "RunnerCapabilities",
]


class PermanentTaskFailure(Exception):
    """
    Raised when a task encounters a condition that will never succeed regardless
    of retries (e.g., required infrastructure is unavailable).
    The dispatcher will dead-letter the task immediately without retrying.
    """


class _DeferredCompletionSentinel:
    """Marker returned by a runner to signal that it scheduled background
    execution on the same claimed row and will update ``complete_task`` /
    ``fail_task`` itself.

    Contract with the dispatcher:

    - Dispatcher MUST NOT call ``complete_task`` after seeing this sentinel
      (the background coroutine will do it when work terminates).
    - Dispatcher MUST NOT ``unregister`` the heartbeat for the task —
      ownership has been transferred to the background coroutine which
      will unregister in its ``finally`` block.
    - The background coroutine is responsible for every terminal state
      (COMPLETED / FAILED / CancelledError handling).

    Used by :class:`BackgroundRunner` in the dispatcher-claimed path to
    avoid the pre-2026-04 duplicate-insert bug (a second task row being
    written with ``status='RUNNING'`` while the dispatcher marked the
    real claimed row COMPLETED prematurely in 124ms).
    """

    _instance: "Optional[_DeferredCompletionSentinel]" = None

    def __new__(cls) -> "_DeferredCompletionSentinel":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:  # pragma: no cover — trivial
        return "DEFERRED_COMPLETION"

    def __bool__(self) -> bool:  # pragma: no cover — trivial
        # Truthy so the dispatcher's ``if result is not None`` / ``if result:``
        # continues to treat the runner as having handled the task.
        return True


DEFERRED_COMPLETION = _DeferredCompletionSentinel()
"""Module-level singleton; compare with ``is DEFERRED_COMPLETION``."""


class RunnerContext(BaseModel):
    engine: DbResource
    task_type: str
    caller_id: str = Field(..., min_length=1, description="Identifier of the user or system that initiated the task.")
    inputs: Dict[str, Any]
    asset: Optional[Any] = None
    db_schema: str = "tasks"
    """Catalog internal id (e.g. ``s_2ka8fbc3``) used as the
    ``catalog_id`` column on the task row. ``"platform"`` for PLATFORM-scoped
    work, ``"system"`` for cross-tenant platform tasks. NOT the host PG
    schema of the tasks table — that lives globally in ``get_task_schema()``."""
    collection_id: Optional[str] = Field(
        default=None,
        description="STAC Collection id when the task targets a specific "
                    "collection (ingestion, tile preseed/invalidate, exports). "
                    "NULL for catalog-wide / platform work. Persisted on the "
                    "task row so ``list_tasks`` can filter by collection.",
    )
    extra_context: Dict[str, Any]
    dedup_key: Optional[str] = None
    """Idempotency token. When set, runners pass this to ``TaskCreate`` so the
    DB partial unique index on ``(catalog_id, dedup_key)`` for non-terminal
    tasks collapses redelivered events into a single task. ``create_task``
    returns ``None`` on a dedup hit; the runner then returns ``None`` and
    ``ExecutionEngine.execute`` short-circuits without trying other runners."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

# --- Enumerations ---

# Moved to dynastore.models.tasks

@dataclass
class RunnerCapabilities:
    """Declares what a runner needs from the dispatcher (visibility window, etc.)."""
    visibility_timeout: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    """How long the dispatcher should hold the lock before the Janitor can reclaim the task."""
    requires_request_context: bool = False
    """If True, this runner requires an active HTTP request context (e.g. BackgroundTasks).
    The standalone worker dispatcher will automatically skip it and fall through to the next runner."""
    max_concurrency: int = 1
    """Maximum number of concurrent tasks this runner can handle."""
    requires_task_discovery: bool = True
    """If True, this runner requires a registered task instance to be available."""

# --- Generic Payload Model ---

# Moved to dynastore.models.tasks

# --- Database & API Models ---

# Moved to dynastore.models.tasks
