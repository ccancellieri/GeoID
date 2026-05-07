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

import logging
import os
from typing import Any, Callable, Coroutine, List, Optional, Union, Dict, Tuple, Type, Protocol, runtime_checkable, AsyncGenerator
from contextlib import asynccontextmanager

from dynastore.modules.tasks.models import (PermanentTaskFailure, Task, TaskCreate,
                                            TaskExecutionMode, TaskPayload,
                                            TaskStatusEnum, TaskUpdate, RunnerContext)
from dynastore.tasks import get_task_instance
import asyncio
from dynastore.tools.plugin import ProtocolPlugin
from dynastore.tools.discovery import register_plugin
from dynastore.modules.concurrency import get_background_executor

logger = logging.getLogger(__name__)


async def _emit_task_failure(
    context: "RunnerContext",
    job: Any,
    error_message: str,
    exc: Exception,
) -> None:
    """Shared helper: emits a generic platform 'task.failed' event after a task fails.

    ``job`` may be either a ``Task`` (OGC Part 1 direct-invocation path) or
    ``None`` (dispatcher-claimed path, where the task_id comes from
    ``context.extra_context['task_id']``).

    Severity is derived from the exception type:
    - 'recoverable'   — transient errors (timeout, OSError, etc.)
    - 'unrecoverable' — all other exceptions

    The event carries the full input context so listeners can perform rollback
    without any knowledge of the specific module that dispatched the task.
    Also logs via tenant log_manager if catalog_id is resolvable.
    """
    # Determine severity from exception type
    severity = "unrecoverable"
    if isinstance(exc, (TimeoutError, OSError, asyncio.TimeoutError)):
        severity = "recoverable"

    # Resolve task_id from either the passed Task object or the claimed context
    task_id_str = (
        str(job.task_id) if job is not None and getattr(job, "task_id", None) is not None
        else str((context.extra_context or {}).get("task_id") or "unknown")
    )

    catalog_id = (context.inputs or {}).get("catalog_id")
    originating_event = (context.extra_context or {}).get("originating_event")

    # Tenant-scoped structured log (best-effort, never blocks failure path)
    if catalog_id:
        try:
            from dynastore.modules.catalog.log_manager import log_error
            await log_error(
                catalog_id,
                event_type="task.failed",
                message=f"Task '{context.task_type}' ({task_id_str}) failed [{severity}]: {error_message}",
                details={"task_type": context.task_type, "severity": severity},
            )
        except Exception as log_exc:
            logger.debug(f"log_manager unavailable for task failure logging: {log_exc}")

    # Emit generic platform event — all rollback logic lives in module listeners
    try:
        from dynastore.modules.catalog.event_service import emit_event
        await emit_event(
            "task.failed",
            task_id=task_id_str,
            task_type=context.task_type,
            error_message=error_message,
            severity=severity,
            inputs=context.inputs,
            originating_event=originating_event,
            catalog_id=catalog_id,
        )
    except Exception as emit_exc:
        logger.error(f"Failed to emit task.failed event: {emit_exc}")

@runtime_checkable
class RunnerProtocol(Protocol):
    """Defines the contract for a class-based task runner."""

    priority: int
    mode: TaskExecutionMode
    runner_type: str = "unknown"
    """Stable identifier surfaced in `ProcessSummary.typologies[].runner_type`
    so clients can tell how a process will execute (e.g. 'fastapi_background',
    'gcp_cloud_run', 'sync'). Defaults to 'unknown' — concrete implementors
    must override."""

    async def setup(self, app_state: Any) -> None:
        """
        An optional, async method called at application startup to allow the
        runner to initialize resources or perform discovery.
        """
        pass  # Default implementation does nothing.

    @property
    def capabilities(self) -> Any:
        """Returns the capabilities of this runner (e.g. max_concurrency, tags)."""
        ...

    def can_handle(self, task_type: str) -> bool:
        """
        Returns True if this runner can execute tasks of the given type.

        Used by the CapabilityMap at startup (and on refresh) to build the
        set of task types this instance can claim from the global queue.

        Default: returns True (runner accepts all task types).
        """
        return True

    async def run(self, context: RunnerContext) -> Union[Task, Any]:
        """
        The core execution logic for the runner. This method is required.
        """
        ...

def get_runners(mode: TaskExecutionMode) -> List[RunnerProtocol]:
    """
    Retrieves a prioritized list of registered runner instances for a given mode.
    """
    # Ensure default runners are registered
    register_default_runners()
    
    from dynastore.tools.discovery import get_protocols
    return [r for r in get_protocols(RunnerProtocol) if r.mode == mode]

def get_all_runners_with_setup() -> List[Tuple[int, RunnerProtocol]]:
    """
    Returns a prioritized list of all runners that have implemented
    a custom setup method.
    """
    from dynastore.tools.discovery import get_protocols
    all_runners = []
    # get_protocols already sorts by priority desc
    for runner in get_protocols(RunnerProtocol):
        # Check if the instance's setup method is not the default one from the base class
        if getattr(runner.setup, "__func__", runner.setup) is not RunnerProtocol.setup:
            all_runners.append((runner.priority, runner))
    return all_runners

# --- Default Runner Implementations ---

class SyncRunner(RunnerProtocol, ProtocolPlugin[Any]):
    mode = TaskExecutionMode.SYNCHRONOUS
    priority = 100
    runner_type = "sync"
    """
    An in-process, synchronous runner that executes the job immediately.
    """

    async def setup(self, app_state: Any) -> None:
        pass

    @property
    def capabilities(self) -> Any:
        from dynastore.modules.tasks.models import RunnerCapabilities
        return RunnerCapabilities(max_concurrency=100)

    def can_handle(self, task_type: str) -> bool:
        return get_task_instance(task_type) is not None

    async def run(self, context: RunnerContext) -> Any:
        from dynastore.tools.protocol_helpers import resolve
        from dynastore.models.protocols.tasks import TasksProtocol
        
        # Resolve the task management protocol
        tasks_mgr = resolve(TasksProtocol)
        
        # This is an in-process runner, so it's responsible for getting the task instance.
        task_instance = get_task_instance(context.task_type)
        logger.debug(f"SyncRunner: lookup for '{context.task_type}' -> {task_instance}")
        if not task_instance:
            return None

        # Create a task record to track this synchronous execution for auditing.
        task_create_request = TaskCreate(
            caller_id=context.caller_id,
            task_type=str(context.task_type),
            inputs=context.inputs,
        )
        
        job = await tasks_mgr.create_task(context.engine, task_create_request, schema=context.db_schema)
        logger.info(f"Created audit task '{job.task_id}' for synchronous process '{context.task_type}'.")
        
        # ... (rest of the logic remains the same, but using tasks_mgr) ...
        from dynastore.tasks import hydrate_task_payload

        raw_payload = {
            "task_id": job.task_id, 
            "caller_id": context.caller_id, 
            "inputs": context.inputs,
            "asset": context.asset
        }

        try:
            logger.info(f"Executing sync task '{job.task_id}'...")
            await tasks_mgr.update_task(context.engine, job.task_id, TaskUpdate(status=TaskStatusEnum.RUNNING), schema=context.db_schema)

            # Hydrate and execute
            hydrated_payload = hydrate_task_payload(task_instance, raw_payload)
            result = await task_instance.run(hydrated_payload)

            # OGC API - Processes Part 1 requires ``GET /jobs/{id}/results`` to work
            # for both async AND sync executions when status=successful. Persist
            # outputs on the audit task so sync-executed jobs can be retrieved later.
            update_data = TaskUpdate(status=TaskStatusEnum.COMPLETED, progress=100, outputs=result)
            await tasks_mgr.update_task(context.engine, job.task_id, update_data, schema=context.db_schema)
            logger.info(f"Sync task '{job.task_id}' completed successfully.")
            return result

        except Exception as e:
            logger.error(f"Sync task '{job.task_id}' failed: {e}", exc_info=True)
            error_message = f"Synchronous execution failed: {str(e)}"
            update_data = TaskUpdate(status=TaskStatusEnum.FAILED, error_message=error_message)
            await tasks_mgr.update_task(context.engine, job.task_id, update_data, schema=context.db_schema)
            await _emit_task_failure(context, job, error_message, e)
            # Re-raise to allow the service layer to return a 500 error.
            raise

class BackgroundRunner(RunnerProtocol, ProtocolPlugin[Any]):
    mode = TaskExecutionMode.ASYNCHRONOUS
    priority = 100
    """
    Runs a task asynchronously in the background.
    Uses Starlette/FastAPI BackgroundTasks if available in context, otherwise asyncio.create_task.
    Returns a StatusInfo object immediately (201 Created pattern).
    """
    def __init__(self):
        self._running_tasks = set()
        self._max_concurrency = int(os.getenv("BACKGROUND_RUNNER_CONCURRENCY", "100"))
        self._semaphore = asyncio.Semaphore(self._max_concurrency)

    def can_handle(self, task_type: str) -> bool:
        return get_task_instance(task_type) is not None

    @asynccontextmanager
    async def lifespan(self, app_state: object) -> AsyncGenerator[None, None]:
        try:
            yield
        finally:
            # Shutdown: wait for running tasks to finish or timeout
            if self._running_tasks:
                pending_count = len(self._running_tasks)
                logger.info(f"BackgroundRunner: Waiting for {pending_count} background tasks to complete...")
                # We give them a decent timeout as some might be provisioning resources
                _, pending = await asyncio.wait(list(self._running_tasks), timeout=10.0)
                if pending:
                    logger.warning(f"BackgroundRunner: {len(pending)} tasks did not finish in time and will be cancelled.")
                    for p in pending:
                        p.cancel()
                    await asyncio.gather(*list(pending), return_exceptions=True)
                logger.info("BackgroundRunner: Cleaned up background tasks.")

    @property
    def capabilities(self) -> Any:
        from dynastore.modules.tasks.models import RunnerCapabilities
        return RunnerCapabilities(max_concurrency=self._max_concurrency)

    async def run(self, context: RunnerContext) -> Any:
        from dynastore.tools.protocol_helpers import resolve
        from dynastore.models.protocols.tasks import TasksProtocol

        tasks_mgr = resolve(TasksProtocol)

        logger.debug(f"BackgroundRunner.run called for task_type: {context.task_type}, mode: {self.mode}")
        task_instance = get_task_instance(context.task_type)
        if not task_instance:
            logger.error(f"Failed to find task instance for type: {context.task_type}")
            return None
        logger.debug(f"Found task instance: {task_instance.__class__.__name__} for task_type: {context.task_type}")

        # ------------------------------------------------------------------
        # Dispatcher-claimed path: the row is already ACTIVE with owner_id,
        # started_at, locked_until and last_heartbeat_at set by claim_batch.
        # DO NOT create a second row.  Schedule background execution against
        # the claimed (task_id, timestamp) and return DEFERRED_COMPLETION so
        # the dispatcher skips both complete_task and heartbeat.unregister.
        # ------------------------------------------------------------------
        claimed_task_id_str = context.extra_context.get("task_id")
        claimed_timestamp = context.extra_context.get("task_timestamp")
        if claimed_task_id_str and claimed_timestamp is not None:
            return await self._run_claimed(
                context=context,
                tasks_mgr=tasks_mgr,
                task_instance=task_instance,
                claimed_task_id_str=claimed_task_id_str,
                claimed_timestamp=claimed_timestamp,
            )

        # ------------------------------------------------------------------
        # Direct-invocation path (OGC Part 1 / request-context usage):
        # no pre-existing claim, so create a fresh row and schedule work
        # against it.  Unchanged behaviour.
        # ------------------------------------------------------------------
        task_create_request = TaskCreate(
            caller_id=context.caller_id,
            task_type=str(context.task_type),
            inputs=context.inputs,
        )
        job = await tasks_mgr.create_task(
            context.engine, task_create_request, schema=context.db_schema,
            initial_status="RUNNING",
        )
        logger.info(f"Created audit task '{job.task_id}' for async process '{context.task_type}'.")

        from dynastore.tasks import hydrate_task_payload

        raw_payload = {
            "task_id": job.task_id,
            "caller_id": context.caller_id,
            "inputs": context.inputs,
            "asset": context.asset
        }

        background_tasks = context.extra_context.get("background_tasks")

        async def _execute_background():
            async with self._semaphore:
                try:
                    logger.info(f"Executing async task '{job.task_id}' in background...")
                    hydrated_payload = hydrate_task_payload(task_instance, raw_payload)
                    result = await task_instance.run(hydrated_payload)

                    update_data = TaskUpdate(status=TaskStatusEnum.COMPLETED, progress=100, outputs=result)
                    await tasks_mgr.update_task(context.engine, job.task_id, update_data, schema=context.db_schema)
                    logger.info(f"Async task '{job.task_id}' completed successfully.")

                except asyncio.CancelledError:
                    logger.warning(f"Async task '{job.task_id}' was cancelled (SIGTERM?). Resetting to PENDING.")
                    try:
                        await tasks_mgr.update_task(context.engine, job.task_id, TaskUpdate(status=TaskStatusEnum.PENDING), schema=context.db_schema)
                    except Exception as e:
                        logger.error(f"Failed to reset cancelled task '{job.task_id}': {e}")
                    raise
                except Exception as e:
                    logger.error(f"Async task '{job.task_id}' failed: {e}", exc_info=True)
                    error_message = f"Asynchronous execution failed: {str(e)}"
                    try:
                        update_data = TaskUpdate(status=TaskStatusEnum.FAILED, error_message=error_message)
                        await tasks_mgr.update_task(context.engine, job.task_id, update_data, schema=context.db_schema)
                    except Exception as update_error:
                        logger.critical(f"Failed to update task '{job.task_id}' status to FAILED: {update_error}")
                    await _emit_task_failure(context, job, error_message, e)

        if background_tasks:
            background_tasks.add_task(_execute_background)
            logger.info(f"Task '{job.task_id}' submitted to Starlette BackgroundTasks.")
        else:
            executor = get_background_executor()
            t = executor.submit(_execute_background(), task_name=f"task:{job.task_id}")
            self._running_tasks.add(t)
            t.add_done_callback(self._running_tasks.discard)
            logger.info(f"Task '{job.task_id}' submitted via BackgroundExecutor.")

        from dynastore.modules.processes.models import StatusInfo
        return StatusInfo(
            jobID=job.task_id,
            status="accepted",
            message="Task accepted for asynchronous execution.",
            progress=0,
            links=[]
        )

    async def _run_claimed(
        self,
        *,
        context: RunnerContext,
        tasks_mgr: Any,
        task_instance: Any,
        claimed_task_id_str: str,
        claimed_timestamp: Any,
    ) -> Any:
        """Dispatcher-path branch — schedule execution on the already-claimed row.

        Returns :data:`DEFERRED_COMPLETION` so the dispatcher does NOT call
        ``complete_task`` (the background coroutine does that) and does NOT
        ``unregister`` its heartbeat entry (the coroutine takes ownership).

        The background coroutine:

        1. Re-registers the claimed task on the dispatcher's
           ``BatchedHeartbeat`` (when passed via ``extra_context``), keeping
           ``locked_until`` alive while the task runs.
        2. Runs ``task_instance.run(payload)``.
        3. Updates the SAME claimed row to COMPLETED (success) or FAILED
           (exception) — operating on the claimed ``(task_id, timestamp)``.
        4. Unregisters its heartbeat entry in a ``finally`` block.

        On ``asyncio.CancelledError`` (SIGTERM / shutdown), the row is reset
        to PENDING so the pg_cron reaper or another dispatcher can retry it.
        """
        import uuid as _uuid
        from datetime import datetime as _dt
        from dynastore.tasks import hydrate_task_payload

        try:
            claimed_task_id = _uuid.UUID(claimed_task_id_str)
        except (ValueError, TypeError, AttributeError):
            logger.error(
                "BackgroundRunner._run_claimed: invalid task_id %r in "
                "extra_context — falling back to no-op return (dispatcher "
                "will mark the row COMPLETED normally).",
                claimed_task_id_str,
            )
            return None

        # Normalize timestamp — DB may hand it back as datetime already.
        if isinstance(claimed_timestamp, _dt):
            claimed_ts = claimed_timestamp
        else:
            try:
                claimed_ts = _dt.fromisoformat(str(claimed_timestamp))
            except (ValueError, TypeError):
                logger.error(
                    "BackgroundRunner._run_claimed: invalid timestamp %r — "
                    "falling back to no-op return.",
                    claimed_timestamp,
                )
                return None

        heartbeat = context.extra_context.get("heartbeat")
        raw_payload = {
            "task_id": str(claimed_task_id),
            "caller_id": context.caller_id,
            "inputs": context.inputs,
            "asset": context.asset,
        }

        # Terminal-state primitives: delegate to the module's
        # ``complete_task`` / ``fail_task`` so ``finished_at`` +
        # ``locked_until`` + ``owner_id`` are cleared correctly, the retry
        # policy is applied consistently, and the commit lands (both
        # helpers use ``managed_transaction`` internally — which
        # explicitly commits via ``conn.begin()``, sidestepping
        # ``_execute_async_workflow``'s pool-return rollback).
        from dynastore.modules.tasks.tasks_module import (
            complete_task as _complete_task,
            fail_task as _fail_task,
        )
        from datetime import datetime as _dt, timezone as _tz

        async def _execute_background_claimed() -> None:
            async with self._semaphore:
                # Re-register on the heartbeat so the row keeps extending
                # ``locked_until`` while the actual work runs.  The dispatcher
                # will skip its own ``unregister`` when it sees
                # ``DEFERRED_COMPLETION``, so this re-registration is what
                # keeps the entry alive across the ownership handoff.
                if heartbeat is not None:
                    try:
                        await heartbeat.register(str(claimed_task_id), claimed_ts)
                    except Exception as e:
                        logger.warning(
                            "BackgroundRunner: heartbeat re-register failed "
                            "for %s: %s — reaper will cover if locked_until expires.",
                            claimed_task_id, e,
                        )

                try:
                    logger.info(
                        f"BackgroundRunner: executing claimed task '{claimed_task_id}' "
                        f"({context.task_type}) in background."
                    )
                    hydrated_payload = hydrate_task_payload(task_instance, raw_payload)
                    result = await task_instance.run(hydrated_payload)

                    await _complete_task(
                        context.engine, claimed_task_id,
                        _dt.now(_tz.utc), outputs=result,
                    )
                    logger.info(f"BackgroundRunner: claimed task '{claimed_task_id}' completed.")

                except asyncio.CancelledError:
                    # SIGTERM / shutdown.  Reset to PENDING via fail_task
                    # with retry=True; the pg_cron reaper would catch it
                    # anyway via expired locked_until, but writing
                    # explicitly here produces a cleaner audit trail
                    # (retry_count incremented, error_message attributed).
                    logger.warning(
                        f"BackgroundRunner: claimed task '{claimed_task_id}' "
                        f"cancelled (SIGTERM?) — resetting to PENDING for retry."
                    )
                    try:
                        await _fail_task(
                            context.engine, claimed_task_id,
                            _dt.now(_tz.utc),
                            "Runner interrupted (SIGTERM / pod shutdown)",
                            retry=True,
                        )
                    except Exception as e:
                        logger.error(
                            f"BackgroundRunner: failed to reset cancelled task "
                            f"'{claimed_task_id}' to PENDING: {e} — pg_cron reaper will catch it."
                        )
                    raise

                except PermanentTaskFailure as e:
                    logger.error(
                        f"BackgroundRunner: claimed task '{claimed_task_id}' "
                        f"PermanentTaskFailure (no retry): {e}",
                    )
                    error_message = f"Asynchronous execution failed: {str(e)}"
                    try:
                        await _fail_task(
                            context.engine, claimed_task_id,
                            _dt.now(_tz.utc),
                            error_message,
                            retry=False,
                        )
                    except Exception as update_error:
                        logger.critical(
                            f"BackgroundRunner: failed to mark claimed task "
                            f"'{claimed_task_id}' FAILED: {update_error} — "
                            f"pg_cron reaper will catch it via expired locked_until.",
                            exc_info=True,
                        )
                    await _emit_task_failure(context, None, error_message, e)

                except Exception as e:
                    logger.error(
                        f"BackgroundRunner: claimed task '{claimed_task_id}' failed: {e}",
                        exc_info=True,
                    )
                    error_message = f"Asynchronous execution failed: {str(e)}"
                    try:
                        await _fail_task(
                            context.engine, claimed_task_id,
                            _dt.now(_tz.utc),
                            error_message,
                            retry=True,
                        )
                    except Exception as update_error:
                        logger.critical(
                            f"BackgroundRunner: failed to mark claimed task "
                            f"'{claimed_task_id}' FAILED: {update_error} — "
                            f"pg_cron reaper will catch it via expired locked_until.",
                            exc_info=True,
                        )
                    await _emit_task_failure(context, None, error_message, e)

                finally:
                    if heartbeat is not None:
                        try:
                            await heartbeat.unregister(str(claimed_task_id))
                        except Exception:  # pragma: no cover — best effort
                            pass

        background_tasks = context.extra_context.get("background_tasks")
        if background_tasks:
            background_tasks.add_task(_execute_background_claimed)
            logger.info(
                f"BackgroundRunner: claimed task '{claimed_task_id}' submitted to Starlette BackgroundTasks."
            )
        else:
            executor = get_background_executor()
            t = executor.submit(
                _execute_background_claimed(),
                task_name=f"task:{claimed_task_id}",
            )
            self._running_tasks.add(t)
            t.add_done_callback(self._running_tasks.discard)
            logger.info(
                f"BackgroundRunner: claimed task '{claimed_task_id}' submitted via BackgroundExecutor."
            )

        from dynastore.modules.tasks.models import DEFERRED_COMPLETION
        return DEFERRED_COMPLETION

class CapabilityMap:
    """
    In-memory map of task_type to capable runners, grouped by execution mode.

    Built at dispatcher startup by querying each runner's can_handle() method.
    Refreshable at runtime when Cloud Run Jobs are added/removed.
    """

    def __init__(self):
        self._async_types: set = set()
        self._sync_types: set = set()
        self._lock = asyncio.Lock()

    async def refresh(self) -> None:
        """Rebuild capability map from current runners, loaded task types,
        and the cached ``TaskRoutingConfig`` (service-affinity narrowing).

        Filtering precedence:

        1. ``get_loaded_task_types()`` — the task class actually imported
           on this process. Hard top-level imports of runtime deps mean
           a service without the dep won't even register the task.
        2. ``runner.can_handle(task_type)`` — at least one runner of the
           required execution mode admits the type.
        3. ``TaskRoutingConfig.routing[task_type]`` — if non-empty, this
           process's ``service_name`` must appear in the list.

        Step 3 is the operator-controlled intent gate. Missing/empty
        routing entry preserves legacy "any capable service may claim"
        behaviour.
        """
        from dynastore.tasks import get_loaded_task_types
        from dynastore.modules.tasks.dispatcher import _SERVICE_NAME

        # Resolve cached TaskRoutingConfig — best-effort. When the config
        # registry isn't ready (early startup, tests) we fall back to "no
        # routing", which preserves legacy behaviour.
        routing: Dict[str, List[str]] = {}
        routing_disabled: bool = False
        try:
            from dynastore.tools.discovery import get_protocol
            from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
            from dynastore.modules.tasks.tasks_config import TaskRoutingConfig

            config_mgr = get_protocol(PlatformConfigsProtocol)
            if config_mgr is not None:
                cfg = await config_mgr.get_config(TaskRoutingConfig)
                # ``routing_disabled`` is the operator kill-switch.  No
                # separate ``enabled`` toggle on this class — the cargo
                # ``enabled`` field that used to be inherited from
                # ``PluginConfig`` was removed in the Phase 0 cleanup.
                if isinstance(cfg, TaskRoutingConfig):
                    routing = cfg.routing or {}
                    routing_disabled = cfg.routing_disabled
        except Exception as exc:  # noqa: BLE001 — non-fatal, log and continue
            logger.debug(
                "CapabilityMap: TaskRoutingConfig not yet available (%s) — "
                "service-affinity routing skipped this refresh.", exc,
            )

        def _routed_to_me(task_type: str) -> bool:
            if routing_disabled or _SERVICE_NAME is None:
                return True
            targets = routing.get(task_type)
            return not targets or _SERVICE_NAME in targets

        async with self._lock:
            self._async_types.clear()
            self._sync_types.clear()
            for task_type in get_loaded_task_types():
                if not _routed_to_me(task_type):
                    continue
                for runner in get_runners(TaskExecutionMode.ASYNCHRONOUS):
                    if runner.can_handle(task_type):
                        self._async_types.add(task_type)
                        break
                for runner in get_runners(TaskExecutionMode.SYNCHRONOUS):
                    if runner.can_handle(task_type):
                        self._sync_types.add(task_type)
                        break
            logger.info(
                "CapabilityMap refreshed (service=%r, routing_disabled=%s, "
                "routing_keys=%d): async=%s, sync=%s",
                _SERVICE_NAME, routing_disabled, len(routing),
                sorted(self._async_types), sorted(self._sync_types),
            )

    @property
    def async_types(self) -> List[str]:
        return list(self._async_types)

    @property
    def sync_types(self) -> List[str]:
        return list(self._sync_types)

    @property
    def all_types(self) -> List[str]:
        return list(self._async_types | self._sync_types)

    def can_claim(self, task_type: str, execution_mode: str) -> bool:
        """Check if this instance can claim a task of the given type and mode."""
        if execution_mode == TaskExecutionMode.ASYNCHRONOUS:
            return task_type in self._async_types
        elif execution_mode == TaskExecutionMode.SYNCHRONOUS:
            return task_type in self._sync_types
        return False


# Singleton capability map instance
capability_map = CapabilityMap()


def register_default_runners() -> None:
    """Ensures that default runners are registered in the global plugin registry.
    Safe to call multiple times.
    """
    register_plugin(SyncRunner())
    register_plugin(BackgroundRunner())

# Register default runners on module load
register_default_runners()
