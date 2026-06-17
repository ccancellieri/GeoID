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

"""
OGC-aligned job lifecycle engine.

Consolidates task lifecycle logic previously duplicated across runners
(SyncRunner, BackgroundRunner, FastAPIBackgroundRunner) and callers
(processes_module, dispatcher).  Runner selection is governed by the
platform-tier TaskRoutingConfig.

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
from dataclasses import dataclass
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


# ----------------------------------------------------------------------
# Routing-aware runner selection (module-level seam)
#
# ``select_runner_for`` resolves the ordered RunnerTarget list from the
# platform-tier TaskRoutingConfig and picks the best matching runner
# instance registered in this process.
#
# Fail-open contract: when routing has no opinion (empty targets, no
# matching target, or resolver error) the function falls back to the
# highest-priority async runner whose ``can_handle`` returns True.
# This preserves the queue's forward-progress guarantee — a degraded
# config or missing routing entry must never brick the dispatcher.
# ----------------------------------------------------------------------



async def select_runner_for(task_key: str):
    """Pick a runner using the routing config; fail-open to priority order.

    Resolution order:
    1. Load the ordered RunnerTarget list from TaskRoutingConfig via routing.resolver.
    2. Call routing.select_target with this service's identity and a can_handle
       predicate over the registered async runners.
    3. If a target is matched, return the runner instance whose runner_type matches.
       Attach the target's options to the runner context via RunnerContext.extra_context
       (see ExecutionEngine.execute — the caller injects runner_options from here).
    4. Fail-open: if no target matches (no config, no viable runner), return the
       highest-priority async runner whose can_handle is True.  The queue must
       never brick because of a missing routing entry.

    Payload-level hints are wired in a later unit; for now request_hints=frozenset().
    """
    from dynastore.modules.tasks.routing import resolver as routing_resolver
    from dynastore.modules.tasks.routing.model import RunnerTarget
    from dynastore.modules.tasks.dispatcher import _SERVICE_NAME
    from dynastore.modules.tasks.models import TaskExecutionMode
    from dynastore.modules.tasks.runners import get_runners

    try:
        targets = await routing_resolver.resolved_targets(task_key)
    except Exception:  # noqa: BLE001 — resolver is best-effort
        targets = []

    async_runners = get_runners(TaskExecutionMode.ASYNCHRONOUS)

    def _can_handle(runner_type: str) -> bool:
        return any(
            getattr(r, "runner_type", None) == runner_type and r.can_handle(task_key)
            for r in async_runners
        )

    # payload hints wired in a later unit
    target: "Optional[RunnerTarget]" = None
    if targets:
        try:
            target = routing_resolver.select_target(
                targets,
                frozenset(),
                _SERVICE_NAME or "",
                _can_handle,
            )
        except Exception:  # noqa: BLE001 — selection is best-effort
            target = None

    if target is not None:
        for r in async_runners:
            if getattr(r, "runner_type", None) == target.runner and r.can_handle(task_key):
                # Carry routing options so GcpJobRunner can read target.options.job.
                # The caller (ExecutionEngine.execute) stores this on the RunnerContext
                # extra_context under "runner_options" before calling runner.run().
                r._routing_options = target.options  # type: ignore[attr-defined]
                return r

    # Fail-open: fall back to the highest-priority capable async runner.
    candidates = sorted(
        [r for r in async_runners if r.can_handle(task_key)],
        key=lambda r: r.priority,
        reverse=True,
    )
    return candidates[0] if candidates else None


# ----------------------------------------------------------------------
# Terminal-outcome routing (on_success / on_failure / on_timeout)
#
# When a task reaches a terminal state the dispatcher (sync / non-deferred)
# and the BackgroundRunner coroutine (deferred) consult the routed
# RunnerTarget's terminal Action for that outcome and apply it.  The only
# Action with a side effect is ROUTE — it enqueues a follow-on task,
# realising pipelines (ingestion -> dwh_join) and compensation/fallback
# (on_failure / on_timeout -> a recovery process).  REPORT / DEAD_LETTER /
# FAIL are descriptors of the terminal status the caller already wrote and
# trigger no follow-on.
#
# Offloaded Cloud Run Jobs finalize in the GcpLivenessReconciler, a separate
# loop with its own owner_id race guards; terminal Actions and the
# background-runtime timeout are both wired there (landed in PR #1720).
# ----------------------------------------------------------------------

# Reserved inputs key carrying the ROUTE continuation hop count.  Persisted in
# the task's ``inputs`` (the only free-form persisted column — no schema
# change), stripped from the typed payload in ``hydrate_task_payload`` so task
# input models never see it.
_ROUTE_DEPTH_KEY = "_route_depth"

# Hard ceiling on ROUTE continuation hops.  Guards against on_success /
# on_failure ROUTE cycles (A->B->A) and runaway fan-out: once a chain reaches
# this depth the continuation is refused and logged rather than enqueued.
_MAX_ROUTE_DEPTH = 16


@dataclass(frozen=True)
class RoutingTerminal:
    """Resolved terminal policy for one task_key in this process.

    ``on_success`` / ``on_failure`` / ``on_timeout`` are the selected target's
    terminal Actions (model defaults when routing has no opinion).
    ``timeout_seconds`` is the sync-runner execution ceiling (per-target
    ``options['timeout_seconds']`` falling back to
    ``TasksPluginConfig.task_timeout_seconds``), or ``None`` when unknown.
    """

    on_success: "Any"
    on_failure: "Any"
    on_timeout: "Any"
    timeout_seconds: Optional[float]


async def resolve_routing_terminal(task_key: str) -> RoutingTerminal:
    """Resolve the terminal Actions + sync timeout for ``task_key``.

    Re-resolves the SAME RunnerTarget that :func:`select_runner_for` picked
    (empty request hints, this service) and reads its terminal Action triple.
    Fail-open to the model defaults (REPORT / DEAD_LETTER / DEAD_LETTER) when
    routing has no opinion — identical contract to ``select_runner_for`` so a
    degraded config can never brick terminal handling.
    """
    from dynastore.modules.tasks.routing import resolver as routing_resolver
    from dynastore.modules.tasks.routing.model import Action, ActionVerb
    from dynastore.modules.tasks.dispatcher import _SERVICE_NAME
    from dynastore.modules.tasks.models import TaskExecutionMode
    from dynastore.modules.tasks.runners import get_runners

    default = RoutingTerminal(
        on_success=Action(action=ActionVerb.REPORT),
        on_failure=Action(action=ActionVerb.DEAD_LETTER),
        on_timeout=Action(action=ActionVerb.DEAD_LETTER),
        timeout_seconds=await _default_task_timeout(),
    )

    try:
        targets = await routing_resolver.resolved_targets(task_key)
    except Exception:  # noqa: BLE001 — resolver is best-effort
        return default
    if not targets:
        return default

    async_runners = get_runners(TaskExecutionMode.ASYNCHRONOUS)

    def _can_handle(runner_type: str) -> bool:
        return any(
            getattr(r, "runner_type", None) == runner_type and r.can_handle(task_key)
            for r in async_runners
        )

    target = None
    try:
        target = routing_resolver.select_target(
            targets, frozenset(), _SERVICE_NAME or "", _can_handle,
        )
    except Exception:  # noqa: BLE001 — selection is best-effort
        target = None
    # No runner in THIS process matched, but the first configured target still
    # describes the policy (e.g. when the row ran on a fail-open fallback
    # runner). Terminal Actions are policy keyed by task, not by which concrete
    # runner served it.
    if target is None:
        target = targets[0]

    per_target_timeout = target.options.get("timeout_seconds")
    timeout_seconds = (
        float(per_target_timeout)
        if isinstance(per_target_timeout, (int, float))
        else await _default_task_timeout()
    )
    return RoutingTerminal(
        on_success=target.on_success,
        on_failure=target.on_failure,
        on_timeout=target.on_timeout,
        timeout_seconds=timeout_seconds,
    )


async def _default_task_timeout() -> Optional[float]:
    """Platform-default task timeout from TasksPluginConfig, or None."""
    try:
        from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
        from dynastore.modules.tasks.tasks_config import TasksPluginConfig
        from dynastore.tools.discovery import get_protocol

        mgr = get_protocol(PlatformConfigsProtocol)
        if mgr is None:
            return None
        cfg = await mgr.get_config(TasksPluginConfig)
        if isinstance(cfg, TasksPluginConfig):
            return float(cfg.task_timeout_seconds)
    except Exception:  # noqa: BLE001 — best-effort; timeout is a safety net
        return None
    return None


async def _read_task_status(engine: DbResource, task_id: Any) -> Optional[str]:
    """Read a task's current status from the global tasks table; None on error."""
    try:
        from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler
        from dynastore.modules.tasks.tasks_module import (
            get_task_schema,
            managed_transaction,
        )

        task_schema = get_task_schema()
        sql = f"SELECT status FROM {task_schema}.tasks WHERE task_id = :task_id;"
        async with managed_transaction(engine) as conn:
            row = await DQLQuery(
                sql, result_handler=ResultHandler.ONE_DICT
            ).execute(conn, task_id=task_id)
        return row.get("status") if row else None
    except Exception:  # noqa: BLE001 — read is best-effort
        logger.warning(
            "terminal routing: status read failed for task %s", task_id,
            exc_info=True,
        )
        return None


# Task types that carry a ``catalog_id`` in their inputs and require the
# provisioning checklist to be drained on terminal exit.
_PROVISIONING_TASK_TYPES: frozenset = frozenset({"gcp_provision_catalog"})


async def _drain_provisioning_checklist(
    engine: DbResource,
    *,
    task_id: Any,
    task_type: str,
    inputs: Optional[Dict[str, Any]],
    outcome: str,
) -> None:
    """Drain still-pending checklist steps for a provisioning task that has
    reached a terminal state (#1909).

    Fires immediately when the task completes or dead-letters so the catalog
    exits ``provisioning`` without waiting for the next sweep cycle.  The
    periodic sweep (``sweep_wedged_provisioning_catalogs``) remains as a
    backstop for tasks that die without passing through a terminal action.

    For ``failure`` / ``timeout`` outcomes the drain fires only when the row
    has actually reached a terminal failed status (DEAD_LETTER or FAILED) —
    a transient retry that resets the row to PENDING must not drain the
    checklist, because the task may still mark its own steps on a later attempt.

    Best-effort: any exception is logged at WARNING and swallowed so a drain
    failure never poisons the task completion path.
    """
    if task_type not in _PROVISIONING_TASK_TYPES:
        return

    catalog_id = (inputs or {}).get("catalog_id") if isinstance(inputs, dict) else None
    if not catalog_id:
        logger.debug(
            "checklist drain: task %s (%s) has no catalog_id in inputs — skip",
            task_id, task_type,
        )
        return

    if outcome in ("failure", "timeout"):
        status = await _read_task_status(engine, task_id)
        if status not in ("DEAD_LETTER", "FAILED"):
            # Transient retry — task may still succeed and mark its own steps.
            logger.debug(
                "checklist drain: task %s (%s) outcome=%s status=%s — "
                "not yet terminal, skipping drain",
                task_id, task_type, outcome, status,
            )
            return

    try:
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.catalogs import CatalogsProtocol

        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            logger.warning(
                "checklist drain: CatalogsProtocol not available for task %s "
                "(%s, outcome=%s) — drain skipped; sweep will recover",
                task_id, task_type, outcome,
            )
            return
        updated = await catalogs.drain_pending_checklist_steps(
            catalog_id, terminal_status="degraded",
        )
        if updated:
            logger.warning(
                "checklist drain: task %s (%s, outcome=%s) — "
                "drained pending steps for catalog '%s'",
                task_id, task_type, outcome, catalog_id,
            )
        else:
            logger.debug(
                "checklist drain: task %s (%s, outcome=%s) — "
                "catalog '%s' had no pending steps to drain",
                task_id, task_type, outcome, catalog_id,
            )
    except Exception:  # noqa: BLE001 — drain must not fail the completion path
        logger.warning(
            "checklist drain: drain failed for task %s (%s, outcome=%s, "
            "catalog=%r) — sweep will recover",
            task_id, task_type, outcome, catalog_id, exc_info=True,
        )


async def apply_terminal_action(
    engine: DbResource,
    *,
    task_id: Any,
    task_type: str,
    inputs: Optional[Dict[str, Any]],
    caller_id: Optional[str],
    collection_id: Optional[str],
    schema: str,
    scope: Optional[str],
    outcome: str,
    action: "Any",
) -> Optional[Any]:
    """Apply one terminal-outcome Action for a finished task.

    Returns the routed follow-on ``Task`` when a ROUTE action enqueued one
    (so an inline-sync caller can offload-on-timeout and surface the new
    async job to the client), otherwise ``None``.

    The caller has already written the base terminal status (COMPLETED for
    ``success``; FAILED / DEAD_LETTER for ``failure``; DEAD_LETTER for
    ``timeout``).  This performs only the follow-on side effect:

    * ROUTE  — enqueue ``action.process`` with the original inputs merged with
      ``action.payload``, carrying a ``_route_depth`` cycle guard; refuses to
      chain past ``_MAX_ROUTE_DEPTH``.
    * REPORT / DEAD_LETTER / FAIL — no follow-on (the base status already
      encodes the terminal outcome).

    For ``failure`` / ``timeout`` outcomes the ROUTE continuation fires ONLY
    when the row actually reached a terminal failed state (DEAD_LETTER /
    FAILED).  A transient retry (status reset to PENDING) must not fire it,
    otherwise compensation would run on every attempt; terminality is read from
    ground truth rather than re-deriving the dual-gate retry arithmetic.

    Fail-soft: a broken continuation is logged at WARNING and swallowed — it
    must never re-fail the original row or brick the loop.
    """
    # Zero-latency checklist drain for provisioning tasks (#1909): fires on
    # every terminal action call so the catalog exits ``provisioning`` in the
    # same moment the task completes or dead-letters, rather than waiting for
    # the next sweep cycle.  Double-guarded fail-soft: _drain_provisioning_checklist
    # already swallows its own exceptions; the outer guard ensures that even an
    # unexpected raise never bricks the routing continuation below.
    try:
        await _drain_provisioning_checklist(
            engine,
            task_id=task_id,
            task_type=task_type,
            inputs=inputs,
            outcome=outcome,
        )
    except Exception:  # noqa: BLE001 — drain must not block routing
        logger.warning(
            "checklist drain: unexpected error for task %s (%s, outcome=%s); "
            "routing continues",
            task_id, task_type, outcome, exc_info=True,
        )

    from dynastore.modules.tasks.routing.model import ActionVerb

    if action is None or action.action != ActionVerb.ROUTE:
        return None
    process = action.process
    if not process:
        return None

    if outcome in ("failure", "timeout"):
        status = await _read_task_status(engine, task_id)
        if status not in ("DEAD_LETTER", "FAILED"):
            # Not terminal yet (transient retry pending) — do not chain.
            return None

    base_inputs: Dict[str, Any] = dict(inputs) if isinstance(inputs, dict) else {}
    depth = base_inputs.get(_ROUTE_DEPTH_KEY, 0)
    try:
        depth = int(depth)
    except (TypeError, ValueError):
        depth = 0
    if depth >= _MAX_ROUTE_DEPTH:
        logger.warning(
            "terminal routing: refusing ROUTE %s -> %s (outcome=%s): "
            "_route_depth=%d reached _MAX_ROUTE_DEPTH=%d (cycle guard)",
            task_type, process, outcome, depth, _MAX_ROUTE_DEPTH,
        )
        return None

    forwarded = {
        **base_inputs,
        **(action.payload or {}),
        _ROUTE_DEPTH_KEY: depth + 1,
    }

    try:
        from dynastore.models.tasks import TaskCreate, TaskScope
        from dynastore.modules.tasks.tasks_module import create_task

        ttype = _route_target_kind(process)
        task_data = TaskCreate(
            task_type=process,
            type=ttype,
            caller_id=caller_id or SYSTEM_USER_ID,
            inputs=forwarded,
            collection_id=collection_id,
            scope=scope or TaskScope.CATALOG,
        )
        created = await create_task(engine, task_data, schema=schema)
        if created is None:
            logger.info(
                "terminal routing: ROUTE %s -> %s (outcome=%s) deduplicated "
                "(non-terminal twin already queued)",
                task_type, process, outcome,
            )
            return None
        logger.info(
            "terminal routing: %s of %s ROUTED -> %s (depth=%d)",
            outcome, task_type, process, depth + 1,
        )
        return created
    except Exception:  # noqa: BLE001 — continuation must not brick the loop
        logger.warning(
            "terminal routing: ROUTE continuation %s -> %s failed; original "
            "row remains terminal (outcome=%s)",
            task_type, process, outcome, exc_info=True,
        )
        return None


def _route_target_kind(process_key: str) -> str:
    """Best-effort 'task' | 'process' label for a ROUTE target; defaults 'task'.

    Informational only — the dispatcher claims by task_type + execution_mode,
    not by this column — so an unknown key safely defaults to 'task'.
    """
    try:
        from dynastore.tasks import resolve_task_type_kind

        return resolve_task_type_kind(process_key, "task")
    except Exception:  # noqa: BLE001
        return "task"


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
        collection_id: Optional[str] = None,
        background_tasks: Any = None,
        dedup_key: Optional[str] = None,
        **extras: Any,
    ) -> Any:
        """
        Immediate execution (OGC Part 1).

        Builds a ``RunnerContext``, selects the highest-priority capable
        runner, and delegates execution.  Falls through to the next runner
        on failure until all are exhausted.

        ``collection_id`` (when known) is forwarded into ``RunnerContext`` so
        runner-side ``TaskCreate(...)`` writes it onto the persisted row.
        Without this, the column on ``tasks.tasks`` stayed NULL for OGC
        Process-routed work even when the route resolved
        ``/collections/{collection_id}`` — losing per-collection filterability.

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

        # Use routing config to pick the preferred runner and carry its options
        # into the RunnerContext so runners like GcpJobRunner can access them.
        # Fail-open: any miss (no config, no matching target, or selection error)
        # leaves the existing priority order intact.
        runner_options: Optional[dict] = None
        try:
            preferred = await select_runner_for(task_type)
            if preferred is not None:
                # select_runner_for stores routing options as a transient attr
                runner_options = getattr(preferred, "_routing_options", None)
                if hasattr(preferred, "_routing_options"):
                    del preferred._routing_options  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001 — selection is best-effort
            preferred = None
        if preferred is not None and any(preferred is r for r in runners):
            runners = [preferred] + [r for r in runners if r is not preferred]

        context = RunnerContext(
            engine=engine,
            task_type=task_type,
            caller_id=caller_id,
            inputs=inputs,
            db_schema=db_schema,
            collection_id=collection_id,
            dedup_key=dedup_key,
            extra_context={
                "background_tasks": background_tasks,
                **({"runner_options": runner_options} if runner_options else {}),
                **{k: v for k, v in extras.items() if v is not None},
            },
        )

        result = None
        last_error: Optional[Exception] = None
        dedup_hit = False

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
                # `result is None` with a `dedup_key` set is the agreed
                # signal from runners that a non-terminal task with the
                # same key already exists. Short-circuit: do not try
                # other runners (which would insert their own row).
                if dedup_key is not None:
                    dedup_hit = True
                    logger.info(
                        "ExecutionEngine: dedup hit on '%s' for task '%s'; "
                        "no new task scheduled.",
                        dedup_key,
                        task_type,
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

        if result is None and not dedup_hit:
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

        from dynastore.models.tasks import DEFAULT_PROCESS_TITLE

        # Deferred jobs are process executions. A missing title means the job did
        # not pass through execute_process (which already resolves title +
        # description), so look the process up ONCE and stamp both its definition
        # title and description. When a title is already present the upstream path
        # has resolved everything, so we skip the registry scan entirely — this
        # keeps high-frequency enqueue paths off the process registry.
        job_inputs = dict(inputs or {})
        if not job_inputs.get("title"):
            # Lazy import avoids a circular dependency at module load time.
            from dynastore.modules.processes.processes_module import (  # noqa: PLC0415
                resolve_stored_description,
                resolve_stored_title,
            )
            from dynastore.modules.processes.protocols import ProcessRegistryProtocol
            from dynastore.tools.discovery import get_protocols as _get_protocols

            _proc = None
            for _reg in _get_protocols(ProcessRegistryProtocol):
                _proc = await _reg.get_process(task_type)
                if _proc:
                    break

            job_inputs["title"] = resolve_stored_title(
                None, getattr(_proc, "title", None), DEFAULT_PROCESS_TITLE
            )
            if not job_inputs.get("description"):
                _d = resolve_stored_description(None, getattr(_proc, "description", None))
                if _d is not None:
                    job_inputs["description"] = _d

        task_create = TaskCreate(
            caller_id=caller_id,
            task_type=task_type,
            inputs=job_inputs,
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
            from dynastore.modules.tasks.exceptions import JobLockedError

            raise JobLockedError(
                f"Job '{job_id}' is locked "
                f"(status={job.status.value}). "
                f"Only CREATED jobs can be updated."
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
            from dynastore.modules.tasks.exceptions import JobStateConflictError

            raise JobStateConflictError(
                f"Job '{job_id}' cannot be started "
                f"(status={job.status.value}). "
                f"Only CREATED jobs can be started."
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
            from dynastore.modules.tasks.exceptions import JobStateConflictError

            raise JobStateConflictError(
                f"Job '{job_id}' is already terminal "
                f"(status={job.status.value})."
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
            # ``caller_id`` is NOT NULL-able on RunnerContext (min_length=1).
            # A row may carry no caller (system-internal enqueues such as the
            # ``event_drain`` trigger): default to the system id rather than the
            # empty string, matching every other call site in this module.
            "caller_id": task_row.get("caller_id") or SYSTEM_USER_ID,
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
                except Exception as exc:
                    logger.warning(
                        "task heartbeat failed task_id=%s: %s",
                        task.task_id, exc,
                    )

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
