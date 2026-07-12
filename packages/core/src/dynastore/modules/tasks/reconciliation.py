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

"""Executor-agnostic liveness-reconcile policy shared by the periodic
reconciler and the on-demand read-path trigger.

A Cloud Run Job container writes its own terminal status from inside
``main_task.py``. If Cloud Run SIGTERM-kills the container on
``taskTimeout``, no terminal write happens: the row stays ``ACTIVE``/
``RUNNING`` with a lapsing ``locked_until`` and OGC Processes ``GET
/jobs/{id}`` keeps reporting ``running`` forever. ``GcpLivenessReconciler``
(``modules/gcp/liveness_reconciler.py``) fixes this on a timer, but a client
poll never triggers a probe between reconciler passes.

:func:`reconcile_task_liveness` closes that gap: called from a single-job GET
handler, it probes the row's owning runner (via the same
:class:`~dynastore.modules.tasks.liveness.LivenessProbeProtocol` the periodic
reconciler uses) and — best-effort, budget-capped — applies the verdict
before the row is serialized back to the caller.

:data:`VerdictAction` / :func:`decide_verdict_action` are the single source
of truth for "which write does this verdict authorize", shared by both the
periodic reconciler and this on-demand helper so the two paths cannot drift
apart on the verdict→action mapping. Each caller still owns its own
logging, retry-message wording and race-loss bookkeeping.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any

from dynastore.modules.tasks.liveness import LivenessVerdict, resolve_probe

if TYPE_CHECKING:
    from dynastore.models.tasks import Task

logger = logging.getLogger(__name__)


class VerdictAction(str, Enum):
    """The write action a :class:`LivenessVerdict` authorizes.

    Pure mapping — no I/O. Both ``GcpLivenessReconciler._reconcile_row`` and
    :func:`reconcile_task_liveness` route through :func:`decide_verdict_action`
    so a future verdict can only ever mean one thing in one place.
    """

    EXTEND_LEASE = "extend_lease"
    FAIL_RETRY = "fail_retry"
    COMPLETE = "complete"
    NOOP = "noop"


def decide_verdict_action(verdict: LivenessVerdict) -> VerdictAction:
    """Map a probe verdict to the action it authorizes. Never raises."""
    if verdict == LivenessVerdict.ALIVE:
        return VerdictAction.EXTEND_LEASE
    if verdict in (LivenessVerdict.DEAD, LivenessVerdict.TERMINAL_FAILED):
        return VerdictAction.FAIL_RETRY
    if verdict == LivenessVerdict.TERMINAL_SUCCEEDED:
        return VerdictAction.COMPLETE
    return VerdictAction.NOOP  # LivenessVerdict.UNKNOWN


# Statuses a lapsed lease is worth probing for. DISMISSED/COMPLETED/FAILED/
# DEAD_LETTER/CREATED/PENDING are either terminal already or not yet claimed
# by a runner, so there is nothing remote to reconcile.
_RECONCILABLE_STATUSES = frozenset({"ACTIVE", "RUNNING"})


async def reconcile_task_liveness(
    engine: Any,
    task: "Task",
    *,
    schema: str,
    budget_seconds: float = 0.8,
) -> "Task":
    """Best-effort, on-demand liveness reconcile for a single job/task GET.

    Called from the single-job OGC Processes ``GET /jobs/{id}`` route and the
    Tasks API single-task GET route, right before the fetched row is
    serialized to the caller — so a client poll can observe a Cloud Run
    Job's SIGTERM-killed execution as terminal instead of waiting for the
    next periodic reconciler pass.

    Guard: only acts when ``task.status`` is still live (``ACTIVE`` or
    ``RUNNING``) AND the lease has lapsed (``locked_until`` in the past).
    Anything else — already terminal, or a lease that hasn't lapsed yet — is
    returned unchanged with no probe call.

    Resolves the owning runner's probe via ``owner_id``
    (:func:`~dynastore.modules.tasks.liveness.resolve_probe`); an unmapped
    owner (in-process / ephemeral runner) means there is nothing remote to
    reconcile and ``task`` is returned unchanged.

    The probe call is budget-capped (default 800ms) so a slow/unreachable
    Executions API cannot stall the GET response; a timeout or any probe
    exception is logged and swallowed — this function MUST NOT raise into
    the caller. The periodic reconciler remains the backstop for anything
    this best-effort call misses.

    Verdict handling reuses :func:`decide_verdict_action` (the same mapping
    ``GcpLivenessReconciler`` uses) and the owner-guarded writers so a
    concurrent periodic-reconciler pass or ``MaintenanceSupervisor``
    task-reaper sweep can never be clobbered — a lost race (writer matches
    0 rows) returns ``task`` unchanged, same as a probe failure.

    Returns the freshly re-fetched :class:`Task` when a write landed, so the
    caller serializes the up-to-date status; otherwise returns ``task``
    unchanged.
    """
    from dynastore.modules.db_config.connection_health_config import (
        resolve_leadership_config,
    )
    from dynastore.modules.tasks import tasks_module

    now = datetime.now(timezone.utc)

    if task.status not in _RECONCILABLE_STATUSES:
        return task
    if task.locked_until is None or task.locked_until >= now:
        return task

    probe = resolve_probe(task.owner_id)
    if probe is None:
        # In-process / ephemeral / unrecognized owner — nothing remote to
        # reconcile. The maintenance reaper handles this row, as today.
        return task

    try:
        verdict = await asyncio.wait_for(
            probe.probe_liveness(task), timeout=budget_seconds
        )
    except asyncio.TimeoutError:
        logger.info(
            "reconcile_task_liveness: probe timed out after %.1fs for task %s "
            "(owner=%s) — leaving for the periodic reconciler.",
            budget_seconds, task.task_id, task.owner_id,
        )
        return task
    except Exception as exc:  # noqa: BLE001 — best-effort; must not raise into the caller
        logger.warning(
            "reconcile_task_liveness: probe failed for task %s (owner=%s): %s "
            "— leaving for the periodic reconciler.",
            task.task_id, task.owner_id, exc,
        )
        return task

    action = decide_verdict_action(verdict)
    acted = False

    if action is VerdictAction.EXTEND_LEASE:
        _, _, _, extend_seconds, _ = resolve_leadership_config()
        acted = await tasks_module.heartbeat_task_if_active(
            engine, task.task_id, timedelta(seconds=extend_seconds),
            created_at=task.timestamp,
        )
    elif action is VerdictAction.FAIL_RETRY:
        acted = await tasks_module.fail_task(
            engine, task.task_id, now,
            "Reconciled: remote execution terminated without reporting "
            f"status (probe verdict={verdict.value}).",
            retry=True, owner_id=task.owner_id, created_at=task.timestamp,
        )
    elif action is VerdictAction.COMPLETE:
        acted = await tasks_module.complete_task(
            engine, task.task_id, now, outputs=task.outputs, owner_id=task.owner_id,
            created_at=task.timestamp,
        )
    else:  # VerdictAction.NOOP (LivenessVerdict.UNKNOWN)
        return task

    if not acted:
        # Lost the race to the periodic reconciler / MaintenanceSupervisor
        # task-reaper between this probe and the write — the row moved out
        # from under us. Truthful no-op, same as a probe failure: return
        # the row unchanged.
        logger.info(
            "reconcile_task_liveness: %s action for task %s matched 0 rows — "
            "lost the race to the periodic reconciler/reaper.",
            action.value, task.task_id,
        )
        return task

    refreshed = await tasks_module.get_task(engine, task.task_id, schema)
    return refreshed if refreshed is not None else task


# ---------------------------------------------------------------------------
# #2897 — secondary-indexing convergence flip on read
# ---------------------------------------------------------------------------

_SECONDARY_INDEXING_KEY = "secondary_indexing"


async def reconcile_secondary_indexing(
    engine: Any,
    task: "Task",
    *,
    budget_seconds: float = 0.8,
) -> "Task":
    """Best-effort read-side convergence check for a pending secondary index.

    An ingestion COMPLETED with a non-empty ``tasks.storage`` backlog persists
    ``outputs["secondary_indexing"] = {"state": "pending", ...}``. This
    re-counts that backlog on read (mirroring :func:`reconcile_task_liveness`'s
    on-demand pattern) so a client polling after ``storage_drain`` has caught
    up sees ``"converged"`` without waiting for a fresh ingestion.

    No-op unless the task is COMPLETED with a ``pending`` secondary-indexing
    state on its outputs — anything else (including a task type that never set
    the field) is returned unchanged with no query.

    Bounding the read never wraps ``asyncio.wait_for`` around an in-flight
    query: cancelling a live ``await conn.execute(...)`` mid-flight is a
    ``BaseException`` that escapes the executor's cleanup, leaking the
    checked-out connection out of the pool for good (every timeout would
    permanently shrink it by one). Instead, ``budget_seconds`` bounds two
    things that ARE safely cancellable: the pool-FIFO wait via
    ``managed_transaction(engine, acquire_timeout=budget_seconds)`` (mirrors
    the ``acquire_engine_connection_bounded`` plumbing behind #2933 — raises
    ``PoolSaturationError`` on a saturated pool without ever starting a
    query), and the query itself via a server-side ``SET LOCAL
    statement_timeout`` (Postgres cancels the COUNT itself past the budget,
    raising ``query_canceled``/57014, rather than the client abandoning a
    connection it can't get back). A saturated pool, a statement-timeout
    cancel, or any other exception all serve ``task`` unchanged rather than
    stalling or failing the GET.

    A drained-to-zero backlog is best-effort persisted back onto the row
    (via ``persist_outputs``, which does not touch ``status``) so later reads
    skip this check entirely; the persist failing still serves the converged
    view for this one response.
    """
    if task.status != "COMPLETED":
        return task
    outputs = task.outputs
    if not isinstance(outputs, dict):
        return task
    secondary = outputs.get(_SECONDARY_INDEXING_KEY)
    if not isinstance(secondary, dict) or secondary.get("state") != "pending":
        return task
    if not task.catalog_id or not task.collection_id:
        return task

    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

    from dynastore.modules.db_config.exceptions import PoolSaturationError
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.tasks import tasks_module
    from dynastore.modules.tasks.async_writer_backlog import count_pending_item_ops

    try:
        async with managed_transaction(
            engine, acquire_timeout=budget_seconds,
        ) as conn:
            # Async-only assertion (mirrors query_executor.py's DDL executor):
            # ``engine`` is always an async resource on this read path (the
            # request-scoped AsyncConnection or the shared async engine),
            # narrows the type for pyright's benefit too.
            assert isinstance(conn, (AsyncConnection, AsyncSession))
            # Bound the query server-side rather than the client awaiting it:
            # a Postgres-side cancel raises a normal (safely-handled) DB
            # exception instead of orphaning the connection. Reset back to
            # DEFAULT on the way out so a nested SAVEPOINT (the real caller
            # here reuses the request's own open connection) never leaves this
            # budget in effect for whatever that connection runs afterwards —
            # a failure path skips the reset, but then the whole block's
            # ROLLBACK reverts the GUC anyway.
            timeout_ms = max(1, int(budget_seconds * 1000))
            await conn.execute(
                text(f"SET LOCAL statement_timeout = '{timeout_ms}ms'")
            )
            queued = await count_pending_item_ops(
                conn,
                task_schema=tasks_module.get_task_schema(),
                catalog_id=task.catalog_id,
                collection_id=task.collection_id,
            )
            await conn.execute(text("SET LOCAL statement_timeout = DEFAULT"))
    except PoolSaturationError as exc:
        logger.info(
            "reconcile_secondary_indexing: pool saturated acquiring a "
            "connection for task %s: %s — serving stored pending state.",
            task.task_id, exc,
        )
        return task
    except Exception as exc:  # noqa: BLE001 — best-effort; must not raise into the caller
        logger.warning(
            "reconcile_secondary_indexing: count failed for task %s: %s — "
            "serving stored pending state.",
            task.task_id, exc,
        )
        return task

    if queued > 0:
        # Still outstanding: refresh the served count without a write — a
        # DB write on every poll while the backlog works itself down would
        # just add load for a value that's about to change again.
        merged_outputs = dict(outputs)
        merged_outputs[_SECONDARY_INDEXING_KEY] = {**secondary, "queued": queued}
        return task.model_copy(update={"outputs": merged_outputs})

    merged_outputs = dict(outputs)
    merged_outputs[_SECONDARY_INDEXING_KEY] = {"state": "converged", "queued": 0}
    try:
        await tasks_module.persist_outputs(engine, task.task_id, merged_outputs)
    except Exception as exc:  # noqa: BLE001 — persisting the flip is best-effort
        logger.warning(
            "reconcile_secondary_indexing: failed to persist converged flip "
            "for task %s: %s — serving converged view this request only.",
            task.task_id, exc,
        )
    return task.model_copy(update={"outputs": merged_outputs})
