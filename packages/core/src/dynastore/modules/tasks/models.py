# /Users/ccancellieri/work/code/dynastore/src/dynastore/modules/tasks/models.py
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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, Dict, Any, TypeVar, Generic, List
from uuid import UUID, uuid4
from datetime import datetime, timezone, UTC, timedelta
from enum import Enum
from dataclasses import dataclass, field

import uuid

from dynastore.models.tasks import (
    TaskStatusEnum,
    TaskExecutionMode,
    TaskExecutionScope,
    TaskPayload,
    TaskBase,
    TaskCreate,
    TaskUpdate,
    Task,
)

from dynastore.modules.db_config.query_executor import DbResource


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
    extra_context: Dict[str, Any]

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
