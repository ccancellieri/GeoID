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

import asyncio
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

from dynastore.modules.processes.models import Process, ExecuteRequest, as_process_task_payload
from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.runners import RunnerProtocol
from dynastore.modules.tasks.models import (
    Task,
    TaskCreate,
    TaskExecutionMode,
    RunnerContext,
    DEFERRED_COMPLETION,
)
from dynastore.tools.identifiers import generate_id_hex
from dynastore.tools.plugin import ProtocolPlugin

logger = logging.getLogger(__name__)


# Default lease for a freshly-launched Cloud Run Job. Picked to outlast typical
# job startup + run; the in-job heartbeat (main_task.py) extends it while the
# job runs, and the pg_cron reaper resets the row if the lease lapses.
_DEFAULT_TASK_TIMEOUT_SECONDS = int(os.getenv("TASK_TIMEOUT", "3600"))

# Lease used for the REST-path INSERT-as-claimed flow.  Must outlast the whole
# gap from RunJob trigger until main_task.py reaches its claim step and its
# heartbeat starts extending the lease — Cloud Run RunJob API call + container
# cold start + module init.  Observed cold-start on the review env is ~1m45s;
# the old 60s default lapsed mid-boot, so the pg_cron reaper reclaimed the row
# and a second Cloud Run execution was spawned (#726).  300s clears observed
# cold-start with margin while still releasing a genuinely dead spawner well
# inside the reaper cycle.  Env-overridable for per-environment tuning.
_SPAWN_LEASE_SECONDS = int(os.getenv("GCP_RUNNER_SPAWN_LEASE", "300"))

# Bounded retry around RunJob — handles transient Cloud Run control-plane
# blips without surfacing them as task failures.  Exhausted retries fall back
# to fail_task(retry=True) so the platform-wide hard_retry_cap remains the
# circuit breaker.
_RUNJOB_MAX_ATTEMPTS = 3
_RUNJOB_BACKOFF_BASE_SECONDS = 1.0


def _is_transient_runjob_error(exc: BaseException) -> bool:
    """True for Cloud Run control-plane errors worth retrying with backoff.

    Conservative: anything that smells like 4xx (auth, bad args, not found)
    is treated as permanent.  google.api_core.exceptions defines a
    *Transient* hierarchy but importing google here on every error widens
    the worker import surface — match by class name instead so the runner
    stays import-light.
    """
    name = type(exc).__name__
    if name in {
        "ServiceUnavailable",       # 503
        "InternalServerError",      # 500
        "DeadlineExceeded",         # 504
        "GatewayTimeout",
        "TooManyRequests",          # 429
        "RetryError",
        "ConnectionError",
        "TimeoutError",
    }:
        return True
    if isinstance(exc, (ConnectionError, TimeoutError, asyncio.TimeoutError)):
        return True
    return False


class GcpJobRunner(RunnerProtocol, ProtocolPlugin[Any]):
    """Runner for async processes that triggers a Google Cloud Run job.

    Two invocation paths:

    1. **REST path** (``ExecutionEngine.execute``, no ``task_id`` in
       ``extra_context``): create a fresh PENDING row, mark ACTIVE, launch a
       Cloud Run Job with the new ``task_id`` in its payload.

    2. **Dispatcher path** (``ExecutionEngine.dispatch``, ``task_id`` provided
       in ``extra_context``): the row was already claimed by the dispatcher.
       Do **not** create a new row — extend the lease and launch a Cloud Run
       Job carrying the existing ``task_id``. Return :data:`DEFERRED_COMPLETION`
       so the dispatcher does not write COMPLETED ahead of the job container.

    Path 2 used to call ``create_task`` unconditionally, producing one new row
    + one Cloud Run Job execution per dispatcher claim. Combined with the
    pg_cron reaper resetting ACTIVE rows back to PENDING when ``locked_until``
    lapsed, this caused an unbounded re-enqueue loop. See plan
    ``geoid-when-a-job-giggly-diffie`` for the full diagnosis.
    """

    mode = TaskExecutionMode.ASYNCHRONOUS
    priority = 10
    runner_type = "gcp_cloud_run"

    @property
    def capabilities(self) -> Any:
        return {"backend": "gcp_cloud_run"}

    async def setup(self, app_state: Any) -> None:
        """Warm the sync job map cache at startup."""
        from dynastore.modules.gcp.tools.jobs import load_job_config
        try:
            await load_job_config()
        except Exception as e:
            logger.warning(f"GcpJobRunner: failed to warm job cache: {e}")

    def can_handle(self, task_type: str) -> bool:
        """Returns True if a Cloud Run Job is configured for this task type."""
        from dynastore.modules.gcp.tools.jobs import get_job_map_sync
        return task_type in get_job_map_sync()

    async def run(self, context: RunnerContext) -> Optional[Any]:
        """Dispatch the task to a Cloud Run Job.

        Returns either the freshly-created :class:`Task` (REST path) or
        :data:`DEFERRED_COMPLETION` (dispatcher path).

        REST path INSERTs the row directly as ACTIVE with this runner as
        owner_id and a short spawn lease, so the on_task_insert trigger
        (``WHEN NEW.status = 'PENDING'``) does not fire and no dispatcher
        pod can claim the row before the Cloud Run Job is launched.  This
        closes the REST↔dispatcher race that previously spawned two Cloud
        Run executions per task.

        Dispatcher path uses ``claim_for_dispatch`` to take ownership only
        if the row is unowned or owned by another GcpJobRunner — defends
        against any future regression that re-opens the producer-side race.

        On Cloud Run trigger failure: bounded exp-backoff retry on
        transient errors, then fail_task(retry=True) — the platform-wide
        hard_retry_cap remains the circuit breaker.
        """
        from dynastore.modules.gcp.tools.jobs import (
            load_job_config,
            run_cloud_run_job_async,
            try_load_process_definition,
            get_job_max_retries,
        )

        job_map = await load_job_config()
        job_name = job_map.get(context.task_type)
        if not job_name:
            return None
        # Coerce inputs to a plain dict — caller may have passed an
        # ExecuteRequest or a JSON string in some legacy paths.
        raw_inputs: Any = context.inputs
        if isinstance(raw_inputs, str):
            import json as _json
            inputs_dict = _json.loads(raw_inputs)
        elif isinstance(raw_inputs, dict):
            inputs_dict = dict(raw_inputs)
        elif hasattr(raw_inputs, "model_dump"):
            inputs_dict = raw_inputs.model_dump()
        else:
            inputs_dict = {}

        # Discriminator: a non-empty task_id in extra_context means the
        # dispatcher already claimed a PENDING row and is delegating
        # execution to us. Reuse that row instead of creating another.
        claimed_task_id = context.extra_context.get("task_id") if context.extra_context else None

        execution_id = generate_id_hex()
        owner_id = f"gcp_cloud_run_{execution_id}"
        task_lease = timedelta(seconds=_DEFAULT_TASK_TIMEOUT_SECONDS)
        new_locked_until = datetime.now(timezone.utc) + task_lease

        existing_task: Optional[Task] = None

        if claimed_task_id is not None:
            # Dispatcher path: take ownership conditionally.  Accept the
            # in-process dispatcher's predecessor claim (``prior_owner_id``)
            # as a legitimate hand-off; only bail with DEFERRED_COMPLETION
            # when the row was raced by an unrelated worker.
            import uuid
            task_id_uuid = uuid.UUID(str(claimed_task_id))
            prior_owner_id = (
                context.extra_context.get("prior_owner_id")
                if context.extra_context
                else None
            )
            claimed = await tasks_module.claim_for_dispatch(
                context.engine,
                task_id_uuid,
                owner_id=owner_id,
                locked_until=new_locked_until,
                expected_owner_prefix="gcp_cloud_run_",
                prior_owner_id=prior_owner_id,
            )
            if not claimed:
                logger.warning(
                    "GcpJobRunner: dispatcher-path race detected — task '%s' "
                    "already owned by a non-GcpJobRunner worker. Skipping Cloud "
                    "Run dispatch (no double-spawn).",
                    task_id_uuid,
                )
                return DEFERRED_COMPLETION
            task_id_for_payload = task_id_uuid
            logger.info(
                f"GcpJobRunner: dispatcher-path reuse of task '{task_id_uuid}' for "
                f"job '{job_name}' (execution_id={execution_id}, lease={task_lease.total_seconds():.0f}s)."
            )
        else:
            # REST path: born claimed.  INSERT with status='ACTIVE',
            # owner_id, locked_until set in one statement so the
            # on_task_insert trigger does not fire and no dispatcher pod
            # can claim the row out from under us before Cloud Run starts.
            # Honour the Cloud Run job's MAX_RETRIES env (capped at job-level
            # rather than the column default of 3) so a single misbehaving job
            # cannot loop more than once by default.
            job_max_retries = get_job_max_retries(context.task_type)
            # Optional: caller may pre-supply a dedup_key in extra_context to
            # collapse at-least-once redeliveries (Pub/Sub push, retry storms).
            dedup_key = (
                context.extra_context.get("dedup_key") if context.extra_context else None
            )

            task_create_request = TaskCreate(
                caller_id=context.caller_id,
                task_type=context.task_type,
                inputs=inputs_dict,
                max_retries=job_max_retries if job_max_retries is not None else 3,
                dedup_key=dedup_key,
            )
            spawn_lease_until = (
                datetime.now(timezone.utc)
                + timedelta(seconds=_SPAWN_LEASE_SECONDS)
            )
            new_task = await tasks_module.create_task(
                context.engine,
                task_create_request,
                schema=context.db_schema,
                initial_status="ACTIVE",
                owner_id=owner_id,
                locked_until=spawn_lease_until,
            )
            if new_task is None:
                logger.info(
                    f"GcpJobRunner: dedup hit on task_type='{context.task_type}' "
                    f"dedup_key='{dedup_key}' — skipping Cloud Run Job dispatch."
                )
                return None

            task_id_for_payload = new_task.task_id
            existing_task = new_task
            logger.info(
                f"GcpJobRunner: REST-path born-claimed task '{new_task.task_id}' for "
                f"job '{job_name}' (execution_id={execution_id}, "
                f"spawn_lease={_SPAWN_LEASE_SECONDS}s, "
                f"max_retries={job_max_retries if job_max_retries is not None else 'default'})."
            )

        process_defn = try_load_process_definition(context.task_type)

        if process_defn is not None and isinstance(process_defn, Process):
            if isinstance(context.inputs, dict) and "inputs" in context.inputs:
                exec_req = ExecuteRequest(**context.inputs)
            elif isinstance(context.inputs, ExecuteRequest):
                exec_req = context.inputs
            else:
                exec_req = ExecuteRequest(inputs=context.inputs)
            payload = as_process_task_payload(
                task_id=task_id_for_payload,
                caller_id=context.caller_id,
                execution_request=exec_req,
            )
        else:
            from dynastore.modules.tasks.models import TaskPayload
            payload = TaskPayload(
                task_id=task_id_for_payload,
                caller_id=context.caller_id,
                inputs=context.inputs,
            )

        args = [context.task_type, payload.model_dump_json(), "--schema", context.db_schema]
        env_vars = {"DYNASTORE_EXECUTION_ID": execution_id}

        last_exc: Optional[BaseException] = None
        for attempt in range(1, _RUNJOB_MAX_ATTEMPTS + 1):
            try:
                await run_cloud_run_job_async(
                    job_name=job_name, args=args, env_vars=env_vars,
                )
                last_exc = None
                break
            except Exception as e:
                last_exc = e
                if not _is_transient_runjob_error(e):
                    logger.error(
                        "GcpJobRunner: permanent RunJob error for '%s' task '%s' "
                        "on attempt %d/%d: %s",
                        job_name, task_id_for_payload, attempt,
                        _RUNJOB_MAX_ATTEMPTS, e,
                    )
                    break
                if attempt < _RUNJOB_MAX_ATTEMPTS:
                    backoff = _RUNJOB_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
                    logger.warning(
                        "GcpJobRunner: transient RunJob error for '%s' task '%s' "
                        "on attempt %d/%d: %s — retrying in %.1fs",
                        job_name, task_id_for_payload, attempt,
                        _RUNJOB_MAX_ATTEMPTS, e, backoff,
                    )
                    await asyncio.sleep(backoff)
                else:
                    logger.error(
                        "GcpJobRunner: transient RunJob error for '%s' task '%s' "
                        "exhausted %d attempts: %s",
                        job_name, task_id_for_payload, _RUNJOB_MAX_ATTEMPTS, e,
                    )

        if last_exc is not None:
            # Spawn failed — release the row to PENDING with retry_count++ so
            # the dispatcher (or a different runner) picks it up.  fail_task
            # handles the hard_retry_cap circuit breaker centrally; we never
            # write a transient FAILED on the spawner side.
            from dynastore.modules.tasks.tasks_module import fail_task
            try:
                await fail_task(
                    context.engine,
                    task_id_for_payload,
                    datetime.now(timezone.utc),
                    f"GcpJobRunner: failed to trigger Cloud Run job '{job_name}': {last_exc}",
                    retry=_is_transient_runjob_error(last_exc),
                )
            except Exception as release_err:  # noqa: BLE001 — diagnostic
                logger.error(
                    "GcpJobRunner: failed to release task '%s' after RunJob "
                    "failure: %s (original error: %s)",
                    task_id_for_payload, release_err, last_exc,
                )
            raise last_exc

        logger.info(f"Dispatched Cloud Run job '{job_name}'.")

        # Dispatcher path: tell the dispatcher the row is being handled
        # asynchronously by the Cloud Run Job container — it will write
        # COMPLETED / FAILED itself via main_task.py.
        if claimed_task_id is not None:
            return DEFERRED_COMPLETION

        return existing_task
