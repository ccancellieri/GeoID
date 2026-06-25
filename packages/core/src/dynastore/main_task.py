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

import asyncio
import logging
import argparse
import json
import traceback
import typing
from datetime import timedelta
from types import SimpleNamespace
import sys
import os
import uuid

# --- Basic Configuration ---
log_level_name = os.getenv('LOG_LEVEL', 'INFO').upper()
log_level = getattr(logging, log_level_name, logging.INFO)
logging.basicConfig(
    level=log_level,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


# Heartbeat refreshes locked_until every visibility_timeout/3 seconds.
# Default visibility_timeout is 5 min, matching the dispatcher contract.
_VISIBILITY_TIMEOUT = timedelta(
    seconds=int(os.getenv("TASK_VISIBILITY_TIMEOUT_SECONDS", "300"))
)


async def _heartbeat_loop(engine, task_id: uuid.UUID, interval_seconds: float) -> None:
    """Extend locked_until every interval_seconds while the task runs.

    On Cloud Run job preempt (SIGTERM) the asyncio task tree is cancelled,
    this coroutine stops, locked_until lapses, and the maintenance reaper resets
    the row to PENDING (retry_count + 1) for another worker to pick up.
    """
    from dynastore.modules.tasks.tasks_module import heartbeat_tasks
    while True:
        await asyncio.sleep(interval_seconds)
        try:
            await heartbeat_tasks(engine, [task_id], _VISIBILITY_TIMEOUT)
        except Exception as e:
            logger.warning(f"Heartbeat failed for task {task_id}: {e}")


def _resolve_payload_model(target_task: object, task_name: str) -> type:
    """Return the pydantic model annotated on ``target_task.run``'s ``payload``.

    Uses ``typing.get_type_hints`` rather than the raw ``__annotations__`` dict
    so that PEP 563 / ``from __future__ import annotations`` string annotations
    are resolved to the live class.  Without this, a task module that stringizes
    its annotations yields the *string* ``"TaskPayload[...]"`` instead of the
    class, and both ``.model_validate`` and ``.__name__`` raise AttributeError —
    aborting every Cloud Run Job execution for that task.

    Raises ``TypeError`` if ``run`` is missing, its hints can't be resolved, or
    it has no ``payload`` annotation.
    """
    run_method = getattr(target_task, "run", None)
    if run_method is None:
        raise TypeError(f"Task '{task_name}' has no `run` method.")
    try:
        hints = typing.get_type_hints(run_method)
    except Exception as exc:
        raise TypeError(
            f"Task '{task_name}': failed to resolve type hints for its "
            f"`run` method: {exc}"
        ) from exc
    payload_model = hints.get("payload")
    if payload_model is None:
        raise TypeError(
            f"Task '{task_name}' has a `run` method without a 'payload' "
            "type annotation."
        )
    return payload_model


async def report_failure(task_id: str, schema: str, error_message: str):
    """
    Attempts to report a fatal failure to the database.
    This helper initializes just enough of the module system to update the task status.
    """
    if not task_id or not schema:
        logger.error("Cannot report failure to DB: task_id or schema missing.")
        return

    try:
        from dynastore import modules
        from dynastore.modules.tasks.models import TaskUpdate, TaskStatusEnum

        app_state = SimpleNamespace()
        # Initialize foundational modules for reporting (failsafe)
        from dynastore.tasks.bootstrap import bootstrap_task_env
        bootstrap_task_env(app_state)

        async with modules.lifespan(app_state):
            logger.info(f"Reporting failure for task '{task_id}' in schema '{schema}'...")
            from dynastore.modules import get_protocol
            from dynastore.models.protocols import DatabaseProtocol, TasksProtocol

            db = get_protocol(DatabaseProtocol)
            if not db:
                logger.warning("DatabaseProtocol implementation not found. Cannot report failure to DB.")
                return
            engine = db.engine

            tasks_mgr = get_protocol(TasksProtocol)
            if not tasks_mgr:
                logger.warning("TasksProtocol implementation not found. Cannot report failure to DB.")
                return

            update_data = TaskUpdate(
                status=TaskStatusEnum.FAILED,
                error_message=f"Fatal Runner Error: {error_message}"
            )
            await tasks_mgr.update_task(engine, uuid.UUID(task_id), update_data, schema=schema)
            logger.info(f"Successfully reported failure for task '{task_id}'.")
    except Exception as e:
        logger.critical(f"Failed to report failure to database: {e}", exc_info=True)

async def main(task_name: str, payload: dict, schema: str):
    """
    Generic framework to initialize foundational modules and run a single background task.

    Lifecycle (when ``payload['task_id']`` is set — the normal Cloud Run Job path):

      1. Bootstrap foundational + task modules.
      2. Take ownership of the existing task row (set owner_id + extend locked_until).
      3. Start a heartbeat coroutine refreshing ``locked_until`` every
         ``visibility_timeout / 3`` seconds.
      4. Validate the payload against the task's declared model and run it.
      5. On success: ``complete_task(status=COMPLETED, outputs=res)``.
         On expected failure (PermanentTaskFailure): ``fail_task(retry=False)``.
         On unexpected exception: ``fail_task(retry=True)``.
         On SIGTERM (asyncio.CancelledError): ``reset_task_to_pending`` so the
         reaper / dispatcher can re-claim it.
      6. ``finally``: cancel the heartbeat coroutine.

    Without these writes the row stays ACTIVE; the maintenance reaper would flip it
    back to PENDING after locked_until lapses, causing an infinite re-enqueue
    loop (one Cloud Run Job execution per reap cycle).
    """
    from dynastore.modules import lifespan as modules_lifespan
    from dynastore import tasks
    from dynastore.tasks.bootstrap import bootstrap_task_env
    from dynastore.modules.tasks.models import PermanentTaskFailure
    from datetime import datetime, timezone

    # Create a simple app_state object. The lifespan managers will populate it.
    app_state = SimpleNamespace()

    # Ephemeral Cloud Run Job pods run a single task then exit.  At production
    # scale hundreds of these pods may run concurrently.  Long-lived
    # dispatcher-side background loops (starting with the task-capability
    # registry heartbeat, #2271) must NOT start inside job pods: each would
    # open its own DB connection and hammer configs.task_capability_registry
    # with writes and lock contention.  The flag is read by TasksModule.lifespan
    # to gate those loops.
    app_state.ephemeral_job = True

    task_id_str = payload.get("task_id")
    task_id_uuid = uuid.UUID(task_id_str) if task_id_str else None

    # 1. Bootstrap the environment
    bootstrap_task_env(app_state)

    # 2. Execute the module lifecycles to initialize them and attach to the app_state.
    async with modules_lifespan(app_state):
        try:
            logger.info("--- [main_task.py] Foundational Modules are active. ---")

            # 3. Execute the task lifecycles, which instantiates tasks with the now-populated app_state.
            async with tasks.manage_tasks(app_state):
                logger.info("--- [main_task.py] Background Tasks are active. ---")

                task_config = tasks.get_all_task_configs().get(task_name)
                if not task_config or not task_config.instance:
                    raise ValueError(f"Task '{task_name}' not found or failed to initialize.")
                target_task = task_config.instance

                logging.info(f"--- [main_task.py] Loaded task '{task_name}' successfully. ---")

                # Resolve the expected payload model from the `run` method's
                # type hints (handles PEP 563 string annotations — see
                # _resolve_payload_model).
                payload_model = _resolve_payload_model(target_task, task_name)

                logging.info(
                    f"--- [main_task.py] Task '{task_name}' expects payload of "
                    f"type '{getattr(payload_model, '__name__', payload_model)}'. ---"
                )
                logging.debug(f"--- [main_task.py] Payload: {payload} ---")
                # Validate the incoming dictionary against the task's expected payload model.
                validate_payload = payload_model.model_validate(payload)

                # Resolve DB engine + tasks manager once for ownership / heartbeat / completion.
                from dynastore.modules import get_protocol
                from dynastore.models.protocols import DatabaseProtocol, TasksProtocol
                db = get_protocol(DatabaseProtocol)
                tasks_mgr = get_protocol(TasksProtocol)
                engine = db.engine if db else None

                hb_task = None
                if task_id_uuid is not None and engine is not None and tasks_mgr is not None:
                    # Take ownership via an atomic, status-guarded claim. The
                    # owner id MUST share the spawner's ``gcp_cloud_run_``
                    # family (GcpJobRunner stamps it and passes the same id
                    # through ``DYNASTORE_EXECUTION_ID``) so the happy-path
                    # "ACTIVE & mine" branch of claim_for_execution matches
                    # instead of reading as a foreign owner.
                    from dynastore.modules.tasks.tasks_module import claim_for_execution
                    execution_id = os.getenv("DYNASTORE_EXECUTION_ID") or task_id_str
                    owner_id = f"gcp_cloud_run_{execution_id}"
                    claimed_row = await claim_for_execution(
                        engine,
                        task_id_uuid,
                        schema,
                        owner_id,
                        _VISIBILITY_TIMEOUT,
                    )
                    if claimed_row is None:
                        # Lost claim: the row is already terminal, or ACTIVE
                        # with a live lease held by another execution. This is
                        # the #726 guard — a Cloud Run Job spawned by a reaper
                        # re-enqueue (lease lapsed mid-cold-start) must NOT
                        # re-run a task another execution already finished or
                        # is finishing. Exit cleanly; the lifespans unwind.
                        logger.warning(
                            f"--- [main_task.py] Task {task_id_uuid} not claimable "
                            f"(already terminal or owned by a live execution) — "
                            f"skipping execution, no double-run. ---"
                        )
                        return
                    interval = _VISIBILITY_TIMEOUT.total_seconds() / 3
                    hb_task = asyncio.create_task(
                        _heartbeat_loop(engine, task_id_uuid, interval)
                    )
                    logger.info(
                        f"--- [main_task.py] Took ownership of task {task_id_uuid} "
                        f"(heartbeat every {interval:.0f}s, lease {_VISIBILITY_TIMEOUT.total_seconds():.0f}s). ---"
                    )
                else:
                    logger.warning(
                        "--- [main_task.py] Running without DB ownership / heartbeat "
                        "(task_id missing or DB protocol unavailable). ---"
                    )

                try:
                    logger.info(f"--- [main_task.py] Executing task: '{task_name}' ---")
                    res = await target_task.run(payload=validate_payload)
                    logger.info(f"--- [main_task.py] Task '{task_name}' returned: {res} ---")

                    # Success: mark COMPLETED in the same row that GcpJobRunner created.
                    if task_id_uuid is not None and engine is not None:
                        from dynastore.modules.tasks.tasks_module import (
                            complete_task,
                            persist_outputs,
                        )
                        finished_at = datetime.now(timezone.utc)
                        try:
                            jsonable_res = (
                                res.model_dump() if hasattr(res, "model_dump") else res
                            )
                        except Exception:
                            jsonable_res = str(res)
                        # Persist outputs as a distinct, bounded-retry write
                        # BEFORE the terminal status flip. Cloud Run reports an
                        # execution SUCCEEDED only once this container exits 0 —
                        # i.e. only after both writes — so the liveness
                        # reconciler (#735), finding a SUCCEEDED execution on a
                        # still-ACTIVE row, can complete it from the outputs
                        # already on the row instead of recovering an empty
                        # result. A lost outputs write would defeat that, so it
                        # is retried; the status flip stays with complete_task.
                        for _attempt in range(1, 4):
                            try:
                                await persist_outputs(
                                    engine, task_id_uuid, jsonable_res
                                )
                                break
                            except Exception as _persist_err:
                                if _attempt == 3:
                                    logger.error(
                                        f"--- [main_task.py] Failed to persist "
                                        f"outputs for task {task_id_uuid} after "
                                        f"3 attempts: {_persist_err} — proceeding "
                                        f"to complete_task. ---"
                                    )
                                else:
                                    await asyncio.sleep(0.5 * _attempt)
                        await complete_task(
                            engine, task_id_uuid, finished_at, outputs=jsonable_res
                        )
                        logger.info(
                            f"--- [main_task.py] Task {task_id_uuid} marked COMPLETED. ---"
                        )
                except asyncio.CancelledError:
                    # SIGTERM during execution — release the row so reaper / another worker
                    # can pick it up. Honour max_retries as a circuit breaker (per row).
                    if task_id_uuid is not None and engine is not None:
                        from dynastore.modules.tasks.tasks_module import reset_task_to_pending
                        try:
                            await reset_task_to_pending(engine, task_id_uuid)
                            logger.warning(
                                f"--- [main_task.py] Task {task_id_uuid} cancelled (SIGTERM); "
                                f"reset to PENDING for retry. ---"
                            )
                        except Exception as reset_err:
                            logger.error(
                                f"Failed to reset task {task_id_uuid} on cancel: {reset_err}"
                            )
                    raise
                except PermanentTaskFailure as exc:
                    if task_id_uuid is not None and engine is not None:
                        from dynastore.modules.tasks.tasks_module import fail_task
                        await fail_task(
                            engine, task_id_uuid, datetime.now(timezone.utc),
                            str(exc), retry=False,
                        )
                        logger.error(
                            f"--- [main_task.py] Task {task_id_uuid} permanently failed: {exc} ---"
                        )
                    raise
                finally:
                    if hb_task is not None:
                        hb_task.cancel()
                        try:
                            await hb_task
                        except asyncio.CancelledError:
                            pass  # expected: we just cancelled it
                        except Exception:
                            logger.warning(
                                "Heartbeat task errored during shutdown for task %s",
                                task_id_uuid,
                                exc_info=True,
                            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if task_id_str:
                logger.info(f"Attempting to report task failure using active lifecycle for task '{task_id_str}'...")
                from dynastore.modules import get_protocol
                from dynastore.models.protocols import DatabaseProtocol, TasksProtocol
                try:
                    db = get_protocol(DatabaseProtocol)
                    tasks_mgr = get_protocol(TasksProtocol)

                    if not db or not tasks_mgr:
                        logger.warning("DatabaseProtocol or TasksProtocol not found. Cannot report failure to DB.")
                    else:
                        engine = db.engine
                        # Use fail_task so the per-row max_retries circuit breaker fires
                        # (DEAD_LETTER once retry_count + 1 >= max_retries) instead of
                        # silently masking the row as FAILED with no retry accounting.
                        from dynastore.modules.tasks.tasks_module import fail_task
                        from datetime import datetime, timezone
                        await fail_task(
                            engine, uuid.UUID(task_id_str),
                            datetime.now(timezone.utc), f"Runtime Error: {str(e)}",
                            retry=True,
                        )
                        logger.info("Successfully reported failure to DB via fail_task.")
                except Exception as report_error:
                    logger.error(f"Failed to report failure within lifecycle: {report_error}. Falling back to external reporter.")
            raise

    logger.info(f"--- [main_task.py] Task '{task_name}' execution complete. All lifecycles shut down. ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DynaStore Generic Task Runner.")
    parser.add_argument("task_name", type=str, help="The registered name of the task to run.")
    parser.add_argument("payload", type=str, help="The JSON payload for the task as a string.")
    parser.add_argument("--schema", type=str, help="The database schema where the task is registered.", default="tasks")
    args = parser.parse_args()

    # Cloud Run does NOT shell-expand a Job's declared ``args:``. When a job is
    # executed without per-execution --args overrides (e.g. a manual
    # ``gcloud run jobs execute <job>`` for debugging/recovery, rather than the
    # in-app GcpJobRunner which always sends ContainerOverrides), the literal
    # default arguments from iac-job.yml reach this entrypoint verbatim. Two
    # guards keep that path from dying with a cryptic JSONDecodeError:
    _USAGE = (
        "Run a real task with explicit arguments, e.g.:\n"
        "  gcloud run jobs execute <job> --args '<task_name>,<json_payload>'\n"
        "or dispatch it through the platform task runner (GcpJobRunner), which "
        "always sends per-execution overrides."
    )

    # 1. ``noop`` sentinel — the documented default task in iac-job.yml. Lets an
    #    operator prove a job is wired up correctly without any side effect.
    if args.task_name == "noop":
        logger.info(
            "Task runner invoked with the 'noop' sentinel (no per-execution "
            "overrides); nothing to do.\n%s",
            _USAGE,
        )
        sys.exit(0)

    # 2. Unexpanded ``${...}`` literal — an older/misconfigured job still
    #    carrying pseudo-shell defaults like ``${TASK_NAME:-default-task}``.
    #    Fail loud with actionable guidance instead of a JSON parse error.
    if "${" in args.task_name or "${" in args.payload:
        logger.critical(
            "Task runner received unexpanded placeholder arguments "
            "(task_name=%r, payload=%r). Cloud Run does not shell-expand a "
            "Job's default args; this job was executed without the required "
            "--args overrides.\n%s",
            args.task_name,
            args.payload,
            _USAGE,
        )
        sys.exit(1)

    task_id = None
    try:
        payload_dict = json.loads(args.payload)
        task_id = payload_dict.get("task_id")
        asyncio.run(main(args.task_name, payload_dict, args.schema))
    except (json.JSONDecodeError) as e:
        msg = f"Fatal: Invalid JSON payload for task '{args.task_name}'.\n{e}\n{_USAGE}"
        logger.critical(msg, exc_info=True)
        # We probably don't have a task_id if JSON is invalid, so just exit
        sys.exit(1)
    except Exception as e:
        full_error = f"{e}\n{traceback.format_exc()}"
        logger.critical(f"An unexpected fatal error occurred: {full_error}")
        if task_id:
            asyncio.run(report_failure(task_id, args.schema, str(e)))
        sys.exit(1)
