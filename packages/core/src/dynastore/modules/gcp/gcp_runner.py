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
import os
from datetime import datetime, timezone, timedelta
from typing import TYPE_CHECKING, Any, Optional

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

if TYPE_CHECKING:
    from dynastore.modules.tasks.liveness import LivenessVerdict

logger = logging.getLogger(__name__)


# Lease used for the REST-path INSERT-as-claimed flow.  No longer a cold-start
# *guess* (#726's 60→300s bump was a fragile fixed timer) — it is now just a
# modest floor covering the window before the GcpLivenessReconciler's first
# pass.  Once the reconciler runs it extends the lease of any execution it
# confirms ALIVE, so the floor never has to outlast cold start (#735).
#
# The live value is ``GcpModuleConfig.spawn_lease_seconds`` — a runtime-tunable
# PluginConfig field.  This module-level constant is only the fallback default
# used when the config registry is not yet available (early startup, tests).
_SPAWN_LEASE_SECONDS = int(os.getenv("GCP_RUNNER_SPAWN_LEASE", "300"))


async def _resolve_spawn_lease_seconds() -> int:
    """Read the spawn-lease floor from ``GcpModuleConfig`` (runtime-mutable).

    Falls back to :data:`_SPAWN_LEASE_SECONDS` when the config registry is not
    available — so the runner still works during early startup and in tests.
    """
    try:
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols import ConfigsProtocol
        from dynastore.modules.gcp.gcp_config import GcpModuleConfig

        cfg_svc = get_protocol(ConfigsProtocol)
        if cfg_svc is not None:
            cfg = await cfg_svc.get_config(GcpModuleConfig)
            if isinstance(cfg, GcpModuleConfig):
                return int(cfg.spawn_lease_seconds)
    except Exception as e:  # noqa: BLE001 — config is best-effort, fall back
        logger.debug(
            "GcpJobRunner: GcpModuleConfig unavailable (%s); "
            "using fallback spawn lease %ds.", e, _SPAWN_LEASE_SECONDS,
        )
    return _SPAWN_LEASE_SECONDS

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
    MaintenanceSupervisor ``task_reaper`` job resetting ACTIVE rows back to
    PENDING when ``locked_until`` lapsed, this caused an unbounded re-enqueue
    loop. See #726 for the full diagnosis and #735 for the
    liveness-reconciler followup.
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

    def declared_tasks(self) -> list:
        """Return one entry per discovered Cloud Run Job mapping.

        Each entry has ``task_key``, ``runner_type``, and ``job`` so Unit 4
        can build a routing matrix from the live GCP job configuration.
        """
        try:
            from dynastore.modules.gcp.tools.jobs import get_job_map_sync
            job_map = get_job_map_sync()
            return [
                {"task_key": task_key, "runner_type": self.runner_type, "job": job_name}
                for task_key, job_name in job_map.items()
            ]
        except Exception as e:  # noqa: BLE001 — declared_tasks must never raise
            logger.debug("GcpJobRunner.declared_tasks: job map unavailable (%s)", e)
            return []

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

        # options.job pin from routing config takes precedence over discovery.
        # When a RunnerTarget carries options={"job": "my-job-name"} the routing
        # engine injects it via extra_context["runner_options"]. If absent or
        # unset, fall back to the standard Cloud Run job-map discovery path so
        # existing deployments that carry no routing config are unaffected.
        _runner_options: dict = (context.extra_context or {}).get("runner_options") or {}
        _pinned_job: Optional[str] = _runner_options.get("job") or None

        if _pinned_job:
            job_name = _pinned_job
        else:
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
        # Lease for a freshly-launched Cloud Run Job. Picked to outlast typical
        # job startup + run; the in-job heartbeat (main_task.py) extends it while
        # the job runs, and the MaintenanceSupervisor task_reaper resets the row
        # if the lease lapses.
        _timeout_seconds = 3600
        try:
            from dynastore.tools.discovery import get_protocol
            from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
            from dynastore.modules.tasks.tasks_config import TasksPluginConfig
            _cm = get_protocol(PlatformConfigsProtocol)
            if _cm is not None:
                _cfg = await _cm.get_config(TasksPluginConfig)
                if isinstance(_cfg, TasksPluginConfig):
                    _timeout_seconds = _cfg.task_timeout_seconds
        except Exception:  # noqa: BLE001 — default lease on any read failure
            pass
        task_lease = timedelta(seconds=_timeout_seconds)
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
                collection_id=context.collection_id,
                max_retries=job_max_retries if job_max_retries is not None else 3,
                dedup_key=dedup_key,
            )
            spawn_lease_seconds = await _resolve_spawn_lease_seconds()
            spawn_lease_until = (
                datetime.now(timezone.utc)
                + timedelta(seconds=spawn_lease_seconds)
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
                f"spawn_lease={spawn_lease_seconds}s, "
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
        runner_ref: Optional[str] = None
        for attempt in range(1, _RUNJOB_MAX_ATTEMPTS + 1):
            try:
                runner_ref = await run_cloud_run_job_async(
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

        # Persist the Cloud Run execution handle on the row so the liveness
        # probe can later ask the Executions API "is this execution alive?".
        # Done on BOTH paths (REST born-claimed + dispatcher reuse) — the row
        # exists either way.  Best-effort: a missed write just leaves
        # runner_ref NULL and the probe degrades to UNKNOWN (MaintenanceSupervisor
        # task_reaper backstops) — never worth failing a successfully-dispatched
        # job over.
        if runner_ref:
            try:
                await tasks_module.set_runner_ref(
                    context.engine, task_id_for_payload, runner_ref
                )
            except Exception as ref_err:  # noqa: BLE001 — diagnostic only
                logger.warning(
                    "GcpJobRunner: failed to persist runner_ref for task '%s' "
                    "(%s) — liveness probe will degrade to UNKNOWN for this row.",
                    task_id_for_payload, ref_err,
                )
        else:
            logger.debug(
                "GcpJobRunner: no execution handle returned for task '%s' — "
                "liveness probe will degrade to UNKNOWN for this row.",
                task_id_for_payload,
            )

        # Dispatcher path: tell the dispatcher the row is being handled
        # asynchronously by the Cloud Run Job container — it will write
        # COMPLETED / FAILED itself via main_task.py.
        if claimed_task_id is not None:
            return DEFERRED_COMPLETION

        return existing_task

    # ------------------------------------------------------------------
    # LivenessProbeProtocol — #735
    #
    # GcpJobRunner spawns Cloud Run Job *executions* that outlive this
    # process. The MaintenanceSupervisor task_reaper resetting a lapsed-lease
    # row to PENDING is therefore wrong for these rows when the execution is
    # still alive (cold start) — it spawns a duplicate. The liveness reconciler
    # asks this runner whether the execution is actually alive before the
    # reaper acts; this is how it knows.
    # ------------------------------------------------------------------

    def owns(self, owner_id: str) -> bool:
        """True if ``owner_id`` was stamped by this runner family.

        GcpJobRunner stamps ``gcp_cloud_run_{execution_id}`` on every row it
        claims (both REST and dispatcher paths), so the prefix is a reliable,
        durable discriminator.
        """
        return bool(owner_id) and owner_id.startswith("gcp_cloud_run_")

    @staticmethod
    def _completion_time_set(execution: Any) -> bool:
        """True when the execution has a real ``completion_time``.

        ``run_v2.Execution`` has no single state enum; ``completion_time`` is a
        timestamp that is unset (epoch / None) until the execution terminates.
        proto-plus maps it to a ``datetime``; an unset field reads as epoch
        (year 1970) or ``None`` depending on the client version — both are
        treated as "not completed".
        """
        ct = getattr(execution, "completion_time", None)
        if ct is None:
            return False
        try:
            return getattr(ct, "year", 1970) > 1970
        except Exception:  # noqa: BLE001 — unknown shape, be conservative
            return bool(ct)

    @staticmethod
    def _map_execution_state(execution: Any) -> "LivenessVerdict":
        """Derive a :class:`LivenessVerdict` from a ``run_v2.Execution``.

        Pure function — no I/O — so it is trivially unit-testable against fake
        execution objects. State is derived from the per-attempt counts plus
        ``completion_time`` because ``Execution`` exposes no single status enum.
        """
        from dynastore.modules.tasks.liveness import LivenessVerdict

        succeeded = getattr(execution, "succeeded_count", 0) or 0
        cancelled = getattr(execution, "cancelled_count", 0) or 0
        running = getattr(execution, "running_count", 0) or 0

        if GcpJobRunner._completion_time_set(execution):
            # Terminated. Succeeded wins; an explicit cancellation reads as
            # DEAD (gone, retry it); anything else completed is a real failure.
            if succeeded > 0:
                return LivenessVerdict.TERMINAL_SUCCEEDED
            if cancelled > 0:
                return LivenessVerdict.DEAD
            return LivenessVerdict.TERMINAL_FAILED

        if running > 0:
            return LivenessVerdict.ALIVE
        # No completion, no running task yet: pending / scheduling / cold
        # start. The execution exists, so it counts as alive — this is exactly
        # the window the fixed spawn-lease used to mis-handle.
        return LivenessVerdict.ALIVE

    async def probe_liveness(self, task: Any) -> "LivenessVerdict":
        """Query the Cloud Run Executions API for the execution backing ``task``.

        Never raises — every failure mode degrades to ``UNKNOWN`` so the
        reconciler loop survives and the MaintenanceSupervisor task_reaper
        remains the backstop:

        * ``runner_ref`` not captured yet (sub-second spawn→capture gap) → UNKNOWN
        * execution ``NotFound`` (deleted / never materialized)         → DEAD
        * any other error (transient API blip, missing client, …)       → UNKNOWN
        """
        from dynastore.modules.tasks.liveness import LivenessVerdict

        runner_ref = getattr(task, "runner_ref", None)
        if not runner_ref:
            return LivenessVerdict.UNKNOWN

        try:
            from dynastore.modules import get_protocol
            from dynastore.models.protocols import JobExecutionProtocol

            gcp_module = get_protocol(JobExecutionProtocol)
            if gcp_module is None or not hasattr(gcp_module, "get_executions_client"):
                logger.debug(
                    "GcpJobRunner.probe_liveness: no executions client available "
                    "for runner_ref '%s' — UNKNOWN.", runner_ref,
                )
                return LivenessVerdict.UNKNOWN

            # ``get_executions_client`` is GCPModule-specific, not part of the
            # JobExecutionProtocol contract — guarded by the hasattr check above.
            client = gcp_module.get_executions_client()  # type: ignore[attr-defined]
            execution = await client.get_execution(name=runner_ref)
            return self._map_execution_state(execution)
        except Exception as exc:  # noqa: BLE001 — MUST NOT raise; map to a verdict
            if type(exc).__name__ == "NotFound" or "NotFound" in type(exc).__name__:
                logger.info(
                    "GcpJobRunner.probe_liveness: execution '%s' NotFound — DEAD.",
                    runner_ref,
                )
                return LivenessVerdict.DEAD
            logger.warning(
                "GcpJobRunner.probe_liveness: inconclusive probe for '%s' (%s) "
                "— UNKNOWN, MaintenanceSupervisor task_reaper backstops.", runner_ref, exc,
            )
            return LivenessVerdict.UNKNOWN

    # ------------------------------------------------------------------
    # StopSignalProtocol — confirmed-dismiss
    #
    # When dismiss_job() sets status=DISMISSED on an ACTIVE/RUNNING row it
    # leaves dismiss_confirmed_at NULL because the Cloud Run execution is
    # still live. The liveness reconciler probes the execution on each
    # cycle and calls signal_stop / force_stop here to drive the backing
    # execution to a halt, then stamps dismiss_confirmed_at once stopped.
    #
    # IAM perms needed by the runner service account:
    #   run.executions.cancel  — for signal_stop (graceful SIGTERM)
    #   run.executions.delete  — for force_stop  (hard teardown)
    # ------------------------------------------------------------------

    @staticmethod
    def _get_executions_client_safe() -> Optional[Any]:
        """Resolve the Cloud Run ExecutionsAsyncClient without raising.

        Reuses the GCPModule-managed client that ``probe_liveness`` already
        acquires — same guard pattern (hasattr check + None-safe path) so the
        stop-signal path and the liveness-probe path share one client lifecycle.
        Returns ``None`` when the client is unavailable (GCP module not loaded,
        early startup, tests without mocking).
        """
        try:
            from dynastore.modules import get_protocol
            from dynastore.models.protocols import JobExecutionProtocol

            gcp_module = get_protocol(JobExecutionProtocol)
            if gcp_module is None or not hasattr(gcp_module, "get_executions_client"):
                return None
            return gcp_module.get_executions_client()  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001 — never raise from a stop-signal helper
            logger.debug(
                "GcpJobRunner._get_executions_client_safe: client unavailable (%s).", exc
            )
            return None

    async def signal_stop(self, task: Any) -> bool:
        """Initiate a graceful stop (SIGTERM) of the Cloud Run execution backing ``task``.

        Calls ``cancel_execution`` — a Cloud Run LRO that sends SIGTERM to the
        job container.  The LRO is *initiated* only; ``.result()`` is NOT
        awaited because confirmation comes from a later ``get_execution`` probe
        (the reconciler's next cycle via ``probe_liveness``).

        Returns ``True`` when the cancel request was sent (or the execution was
        already gone / cancelled).  Returns ``False`` when no ``runner_ref`` is
        available.  Never raises — any API error is logged at WARNING and
        returns ``False`` so the reconciler degrades gracefully.

        Idempotent: cancelling an already-cancelled execution is a no-op on
        the Cloud Run side (returns quickly without error).
        """
        runner_ref = getattr(task, "runner_ref", None)
        if not runner_ref:
            logger.debug(
                "GcpJobRunner.signal_stop: task %s has no runner_ref — cannot cancel.",
                getattr(task, "task_id", "?"),
            )
            return False

        client = self._get_executions_client_safe()
        if client is None:
            logger.warning(
                "GcpJobRunner.signal_stop: no executions client available "
                "for runner_ref '%s' — skipping cancel.", runner_ref,
            )
            return False

        try:
            # ``cancel_execution`` accepts ``name`` as a keyword argument;
            # no ``run_v2.*`` request-object import is needed — same import-light
            # approach used by ``probe_liveness`` (``client.get_execution(name=…)``).
            await client.cancel_execution(name=runner_ref)
            logger.info(
                "GcpJobRunner.signal_stop: cancel_execution initiated for '%s' "
                "(task %s).", runner_ref, getattr(task, "task_id", "?"),
            )
            return True
        except Exception as exc:  # noqa: BLE001 — MUST NOT raise
            logger.warning(
                "GcpJobRunner.signal_stop: cancel_execution failed for '%s' (%s) "
                "— returning False; force_stop will escalate if deadline passes.",
                runner_ref, exc,
            )
            return False

    async def force_stop(self, task: Any) -> bool:
        """Force-teardown (hard delete) the Cloud Run execution backing ``task``.

        Calls ``delete_execution`` — a Cloud Run LRO that tears down the
        execution unconditionally.  The LRO is *initiated* only; confirmation
        is not awaited (mirrors ``signal_stop``).

        Returns ``True`` when the delete request was sent.  Returns ``False``
        when no ``runner_ref`` is available or the request failed.  Never
        raises.
        """
        runner_ref = getattr(task, "runner_ref", None)
        if not runner_ref:
            logger.debug(
                "GcpJobRunner.force_stop: task %s has no runner_ref — cannot delete.",
                getattr(task, "task_id", "?"),
            )
            return False

        client = self._get_executions_client_safe()
        if client is None:
            logger.warning(
                "GcpJobRunner.force_stop: no executions client available "
                "for runner_ref '%s' — skipping delete.", runner_ref,
            )
            return False

        try:
            # ``delete_execution`` accepts ``name`` as a keyword argument;
            # no ``run_v2.*`` request-object import is needed.
            await client.delete_execution(name=runner_ref)
            logger.info(
                "GcpJobRunner.force_stop: delete_execution initiated for '%s' "
                "(task %s).", runner_ref, getattr(task, "task_id", "?"),
            )
            return True
        except Exception as exc:  # noqa: BLE001 — MUST NOT raise
            logger.warning(
                "GcpJobRunner.force_stop: delete_execution failed for '%s' (%s) "
                "— returning False.", runner_ref, exc,
            )
            return False
