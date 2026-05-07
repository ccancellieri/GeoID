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

"""
OGC-aligned job lifecycle engine.

Consolidates task lifecycle logic previously duplicated across runners
(SyncRunner, BackgroundRunner, FastAPIBackgroundRunner) and callers
(processes_module, dispatcher).

Execution paths:
  - Immediate (OGC Part 1):  execute()  → accepted → running → successful/failed
  - Deferred  (OGC Part 4):  create_job() → created; start_job() → accepted → ...
  - Dispatcher (queue):      dispatch()  → running → successful/failed
  - Dismiss   (OGC Part 1):  dismiss_job() → dismissed

OGC Job State Machine::

    POST .../jobs            → CREATED (unlocked)
      ├── PATCH .../jobs/{id}  → CREATED (updated)
      └── POST .../jobs/{id}/results → PENDING → ACTIVE → COMPLETED/FAILED

    POST .../execution       → PENDING → ACTIVE → COMPLETED/FAILED

    DELETE .../jobs/{id}     → DISMISSED (from any non-terminal state)
"""

import json
import logging
from datetime import timedelta
from typing import Any, Dict, Optional, List
from uuid import UUID

from dynastore.models.tasks import (
    TaskCreate,
    TaskUpdate,
    TaskStatusEnum,
    TaskExecutionMode,
    Task,
)
from dynastore.modules.db_config.query_executor import DbResource
from dynastore.models.auth_models import SYSTEM_USER_ID

logger = logging.getLogger(__name__)

# Terminal statuses — dismiss and update operations check against these
_TERMINAL_STATUSES = frozenset({
    TaskStatusEnum.COMPLETED,
    TaskStatusEnum.FAILED,
    TaskStatusEnum.DISMISSED,
    TaskStatusEnum.DEAD_LETTER,
})


class ExecutionEngine:
    """
    OGC-aligned job lifecycle engine.

    Single entry point for all execution paths: immediate (Part 1),
    deferred (Part 4), dispatcher (queue claim), and dismiss.

    Runner selection is consolidated here, replacing the duplicate
    loops in dispatcher.py and processes_module.py.
    """

    # ------------------------------------------------------------------
    # Runner resolution (replaces duplicate selection in dispatcher +
    # processes_module)
    # ------------------------------------------------------------------

    @staticmethod
    def get_runners_for(
        task_type: str,
        mode: TaskExecutionMode,
        *,
        has_request_context: bool = False,
    ) -> list:
        """
        Returns capable runners for *task_type* in *mode*, filtered by
        context requirements (e.g. ``requires_request_context``).

        Runners are returned in priority order (highest first, as provided
        by ``get_runners``).
        """
        from dynastore.modules.tasks.runners import get_runners

        result = []
        for runner in get_runners(mode):
            if not runner.can_handle(task_type):
                continue
            caps = getattr(runner, "capabilities", None)
            if caps and getattr(caps, "requires_request_context", False):
                if not has_request_context:
                    continue
            result.append(runner)
        return result

    # ------------------------------------------------------------------
    # Part 1 — Immediate execution
    # ------------------------------------------------------------------

    async def execute(
        self,
        task_type: str,
        inputs: dict,
        *,
        engine: DbResource,
        mode: TaskExecutionMode = TaskExecutionMode.ASYNCHRONOUS,
        caller_id: str = SYSTEM_USER_ID,
        db_schema: str = "tasks",
        background_tasks: Any = None,
        **extras: Any,
    ) -> Any:
        """
        Immediate execution (OGC Part 1).

        Builds a ``RunnerContext``, selects the highest-priority capable
        runner, and delegates execution.  Falls through to the next runner
        on failure until all are exhausted.

        Returns whatever the runner returns (result for sync, StatusInfo /
        Task for async).
        """
        from dynastore.modules.tasks.models import RunnerContext

        has_request_context = background_tasks is not None
        runners = self.get_runners_for(
            task_type, mode, has_request_context=has_request_context
        )

        if not runners:
            raise NotImplementedError(
                f"No available runner for task '{task_type}' "
                f"with mode '{mode.value}'."
            )

        context = RunnerContext(
            engine=engine,
            task_type=task_type,
            caller_id=caller_id,
            inputs=inputs,
            db_schema=db_schema,
            extra_context={
                "background_tasks": background_tasks,
                **{k: v for k, v in extras.items() if v is not None},
            },
        )

        result = None
        last_error: Optional[Exception] = None

        for runner in runners:
            try:
                logger.debug(
                    "ExecutionEngine: trying runner '%s' for '%s'.",
                    runner.__class__.__name__,
                    task_type,
                )
                result = await runner.run(context)
                if result is not None:
                    logger.info(
                        "ExecutionEngine: task '%s' handled by '%s'.",
                        task_type,
                        runner.__class__.__name__,
                    )
                    break
            except Exception as e:
                last_error = e
                logger.warning(
                    "Runner '%s' failed for '%s': %s",
                    runner.__class__.__name__,
                    task_type,
                    e,
                )
                continue

        if result is None:
            raise RuntimeError(
                f"All runners for mode '{mode.value}' failed for "
                f"task '{task_type}'. Last error: {last_error}"
            )

        return result

    # ------------------------------------------------------------------
    # Part 4 — Deferred execution
    # ------------------------------------------------------------------

    async def create_job(
        self,
        task_type: str,
        inputs: Optional[dict] = None,
        *,
        engine: DbResource,
        caller_id: str = SYSTEM_USER_ID,
        db_schema: str = "tasks",
        collection_id: Optional[str] = None,
        **extras: Any,
    ) -> Task:
        """
        Create a job in ``CREATED`` status (OGC Part 4).

        The job is not yet started — inputs can be refined via
        ``update_job()`` before triggering execution with
        ``start_job()``.
        """
        from dynastore.tools.protocol_helpers import resolve
        from dynastore.models.protocols.tasks import TasksProtocol

        tasks_mgr = resolve(TasksProtocol)

        task_create = TaskCreate(
            caller_id=caller_id,
            task_type=task_type,
            inputs=inputs or {},
            collection_id=collection_id,
        )
        job = await tasks_mgr.create_task(
            engine,
            task_create,
            schema=db_schema,
            initial_status=TaskStatusEnum.CREATED.value,
        )
        logger.info(
            "ExecutionEngine: created deferred job '%s' (%s) in schema '%s'.",
            job.task_id,
            task_type,
            db_schema,
        )
        return job

    async def update_job(
        self,
        job_id: UUID,
        inputs: dict,
        *,
        engine: DbResource,
        db_schema: str = "tasks",
    ) -> Task:
        """
        Update job inputs (OGC Part 4 PATCH).

        Only allowed while the job is in ``CREATED`` status (unlocked).
        Returns 423 Locked if the job has already been started.
        """
        from dynastore.tools.protocol_helpers import resolve
        from dynastore.models.protocols.tasks import TasksProtocol

        tasks_mgr = resolve(TasksProtocol)
        job = await tasks_mgr.get_task(engine, job_id, schema=db_schema)
        if not job:
            raise ValueError(f"Job '{job_id}' not found.")

        if job.status != TaskStatusEnum.CREATED:
            from fastapi import HTTPException

            raise HTTPException(
                status_code=423,
                detail=(
                    f"Job '{job_id}' is locked "
                    f"(status={job.status.value}). "
                    f"Only CREATED jobs can be updated."
                ),
            )

        # Update the task's inputs via a status-preserving update that
        # carries the new inputs payload.
        update = TaskUpdate(outputs={"_deferred_inputs": inputs})
        await tasks_mgr.update_task(engine, job_id, update, schema=db_schema)

        # Re-fetch to return the updated state
        updated = await tasks_mgr.get_task(engine, job_id, schema=db_schema)
        if updated is None:
            raise ValueError(f"Job '{job_id}' disappeared after update.")
        return updated

    async def start_job(
        self,
        job_id: UUID,
        *,
        engine: DbResource,
        mode: TaskExecutionMode = TaskExecutionMode.ASYNCHRONOUS,
        db_schema: str = "tasks",
        background_tasks: Any = None,
        **extras: Any,
    ) -> Any:
        """
        Trigger execution of a ``CREATED`` job (OGC Part 4
        ``POST /jobs/{id}/results``).

        Transitions the job from ``CREATED`` → ``PENDING``, then
        delegates to ``execute()`` for runner selection.
        """
        from dynastore.tools.protocol_helpers import resolve
        from dynastore.models.protocols.tasks import TasksProtocol

        tasks_mgr = resolve(TasksProtocol)
        job = await tasks_mgr.get_task(engine, job_id, schema=db_schema)
        if not job:
            raise ValueError(f"Job '{job_id}' not found.")

        if job.status != TaskStatusEnum.CREATED:
            from fastapi import HTTPException

            raise HTTPException(
                status_code=409,
                detail=(
                    f"Job '{job_id}' cannot be started "
                    f"(status={job.status.value}). "
                    f"Only CREATED jobs can be started."
                ),
            )

        # Transition to PENDING (OGC "accepted")
        await tasks_mgr.update_task(
            engine,
            job_id,
            TaskUpdate(status=TaskStatusEnum.PENDING),
            schema=db_schema,
        )

        # Execute using the standard immediate path
        return await self.execute(
            task_type=job.task_type,
            inputs=job.inputs or {},
            engine=engine,
            mode=mode,
            caller_id=job.caller_id or SYSTEM_USER_ID,
            db_schema=db_schema,
            background_tasks=background_tasks,
            task_id=str(job_id),
            **extras,
        )

    # ------------------------------------------------------------------
    # Dismiss (OGC Part 1 — DELETE /jobs/{id})
    # ------------------------------------------------------------------

    async def dismiss_job(
        self,
        job_id: UUID,
        *,
        engine: DbResource,
        db_schema: str = "tasks",
    ) -> Task:
        """
        Cancel a job (OGC dismiss).

        Sets status to ``DISMISSED`` from any non-terminal state.
        Returns 404 if not found, 409 if already terminal.
        """
        from dynastore.tools.protocol_helpers import resolve
        from dynastore.models.protocols.tasks import TasksProtocol

        tasks_mgr = resolve(TasksProtocol)
        job = await tasks_mgr.get_task(engine, job_id, schema=db_schema)
        if not job:
            raise ValueError(f"Job '{job_id}' not found.")

        if job.status in _TERMINAL_STATUSES:
            from fastapi import HTTPException

            raise HTTPException(
                status_code=409,
                detail=(
                    f"Job '{job_id}' is already terminal "
                    f"(status={job.status.value})."
                ),
            )

        await tasks_mgr.update_task(
            engine,
            job_id,
            TaskUpdate(status=TaskStatusEnum.DISMISSED),
            schema=db_schema,
        )
        logger.info("ExecutionEngine: dismissed job '%s'.", job_id)

        dismissed = await tasks_mgr.get_task(engine, job_id, schema=db_schema)
        if dismissed is None:
            raise ValueError(f"Job '{job_id}' disappeared after dismiss.")
        return dismissed

    # ------------------------------------------------------------------
    # Dispatcher path — claimed tasks from the queue
    # ------------------------------------------------------------------

    async def dispatch(
        self,
        task_row: dict,
        *,
        engine: DbResource,
        heartbeat: Optional[Any] = None,
    ) -> Any:
        """
        Execute a claimed task from the queue.

        The task is already ACTIVE (claimed by the dispatcher with
        ``claim_next``).  No new job record is created — runners that
        receive this context should execute the task directly.

        Falls back to direct ``TaskProtocol`` execution if no runner
        handles the task type.

        ``heartbeat`` is the dispatcher's :class:`BatchedHeartbeat` — when
        passed, it is placed in ``RunnerContext.extra_context`` under
        ``"heartbeat"`` so async runners (``BackgroundRunner``) can keep
        the claimed row's ``locked_until`` alive while they execute in
        the background **on the same claimed row** (see
        ``DEFERRED_COMPLETION`` contract in
        :mod:`dynastore.modules.tasks.models`).
        """
        from dynastore.modules.tasks.models import RunnerContext
        from dynastore.tasks import get_task_instance, hydrate_task_payload

        task_id = task_row["task_id"]
        task_type = task_row["task_type"]
        execution_mode = task_row.get(
            "execution_mode", TaskExecutionMode.ASYNCHRONOUS
        )

        runner_mode = (
            TaskExecutionMode.SYNCHRONOUS
            if execution_mode == "SYNCHRONOUS"
            else TaskExecutionMode.ASYNCHRONOUS
        )

        raw_payload = {
            "task_id": task_id,
            "caller_id": task_row.get("caller_id") or "",
            "inputs": (
                task_row["inputs"]
                if isinstance(task_row.get("inputs"), dict)
                else (
                    json.loads(task_row["inputs"])
                    if task_row.get("inputs")
                    else {}
                )
            ),
        }

        from dynastore.tools.correlation import _INTERNAL_KEY, set_correlation_id
        task_inputs = raw_payload["inputs"]
        cid = task_inputs.pop(_INTERNAL_KEY, None)
        token = set_correlation_id(cid) if cid else None

        # Dispatcher-path handoff: the row is already ACTIVE with the
        # dispatcher's heartbeat extending ``locked_until``.  Runners that
        # schedule async work (``BackgroundRunner``) pick up both the
        # claimed ``task_id + task_timestamp`` AND the heartbeat handle so
        # they can re-register under their own ownership before the
        # dispatcher unregisters (see DEFERRED_COMPLETION).
        _extra_context: Dict[str, Any] = {
            "task_id": str(task_id),
            "task_timestamp": task_row.get("timestamp"),
            # Pass the dispatcher's claim through so runners that take over
            # ownership (GcpJobRunner.claim_for_dispatch) can recognise the
            # in-process predecessor instead of treating it as a competing
            # peer and bailing with "race detected" (closes #217).
            "prior_owner_id": task_row.get("owner_id"),
        }
        if heartbeat is not None:
            _extra_context["heartbeat"] = heartbeat

        context = RunnerContext(
            engine=engine,
            task_type=task_type,
            caller_id=raw_payload["caller_id"],
            inputs=task_inputs,
            db_schema=task_row.get("schema_name", "tasks"),
            extra_context=_extra_context,
        )

        try:
            # 1. Try runners (skip those needing request context)
            runners = self.get_runners_for(
                task_type, runner_mode, has_request_context=False
            )

            result = None
            for runner in runners:
                logger.debug(
                    "ExecutionEngine.dispatch: trying runner '%s' for task %s.",
                    runner.__class__.__name__,
                    task_id,
                )
                result = await runner.run(context)
                if result is not None:
                    logger.info(
                        "ExecutionEngine.dispatch: task %s handled by '%s'.",
                        task_id,
                        runner.__class__.__name__,
                    )
                    break

            # 2. Fallback: direct TaskProtocol singleton execution
            if result is None:
                task_instance = get_task_instance(task_type)
                if task_instance:
                    logger.info(
                        "ExecutionEngine.dispatch: executing task '%s' "
                        "via TaskProtocol singleton.",
                        task_type,
                    )
                    hydrated_payload = hydrate_task_payload(
                        task_instance, raw_payload
                    )
                    result = await task_instance.run(hydrated_payload)
                else:
                    raise RuntimeError(
                        f"No runner or task implementation found "
                        f"for '{task_type}'."
                    )
        finally:
            if token is not None:
                from dynastore.tools.correlation import _correlation_id_var
                _correlation_id_var.reset(token)

        return result

    # ------------------------------------------------------------------
    # Job query helpers (used by OGC endpoints)
    # ------------------------------------------------------------------

    async def run_ephemeral(
        self,
        task_data: TaskCreate,
        schema: str,
        *,
        engine: DbResource,
        visibility_timeout: timedelta = timedelta(minutes=5),
    ) -> Any:
        """Enqueue a task as PENDING then immediately claim and run it in this process.

        Unlike BackgroundRunner (which creates tasks as RUNNING), the task starts
        as PENDING so it remains visible to the dispatcher queue. A heartbeat loop
        extends the visibility window while the task runs. On CancelledError (SIGTERM)
        the task is reset to PENDING so another process can pick it up.

        Use for long-running operations that must survive process restarts.
        """
        import asyncio
        from datetime import datetime, timezone
        from dynastore.modules.tasks.models import PermanentTaskFailure
        from dynastore.modules.tasks.tasks_module import (
            claim_by_id,
            complete_task,
            create_task,
            fail_task,
            heartbeat_tasks,
            reset_task_to_pending,
        )

        task = await create_task(engine, task_data, schema)
        if task is None:
            raise RuntimeError("run_ephemeral: create_task returned None (dedup hit on a non-dedup task).")
        owner_id = f"ephemeral-{task.task_id}"
        row = await claim_by_id(engine, task.task_id, visibility_timeout, owner_id)
        if row is None:
            raise RuntimeError(
                f"run_ephemeral: failed to claim task {task.task_id}"
            )

        async def _heartbeat() -> None:
            interval = visibility_timeout.total_seconds() / 3
            while True:
                await asyncio.sleep(interval)
                try:
                    await heartbeat_tasks(engine, [task.task_id], visibility_timeout)
                except Exception:
                    pass

        hb = asyncio.create_task(_heartbeat())
        try:
            result = await self.dispatch(row, engine=engine)
            await complete_task(engine, task.task_id, row["timestamp"], outputs=result)
            return result
        except asyncio.CancelledError:
            await reset_task_to_pending(engine, task.task_id)
            raise
        except PermanentTaskFailure as exc:
            await fail_task(
                engine, task.task_id, datetime.now(timezone.utc), str(exc), retry=False,
            )
            raise
        except Exception as exc:
            await fail_task(
                engine, task.task_id, datetime.now(timezone.utc), str(exc), retry=True,
            )
            raise
        finally:
            hb.cancel()

    async def get_job(
        self,
        job_id: UUID,
        *,
        engine: DbResource,
        db_schema: str = "tasks",
    ) -> Optional[Task]:
        """Retrieve a single job by ID."""
        from dynastore.tools.protocol_helpers import resolve
        from dynastore.models.protocols.tasks import TasksProtocol

        tasks_mgr = resolve(TasksProtocol)
        return await tasks_mgr.get_task(engine, job_id, schema=db_schema)

    async def list_jobs(
        self,
        *,
        engine: DbResource,
        db_schema: str = "tasks",
        limit: int = 20,
        offset: int = 0,
    ) -> List[Task]:
        """List jobs within a schema (OGC GET /jobs)."""
        from dynastore.tools.protocol_helpers import resolve
        from dynastore.models.protocols.tasks import TasksProtocol

        tasks_mgr = resolve(TasksProtocol)
        return await tasks_mgr.list_tasks(
            engine, schema=db_schema, limit=limit, offset=offset
        )


# -- Module-level singleton --
execution_engine = ExecutionEngine()
