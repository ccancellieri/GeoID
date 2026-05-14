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

"""GCP execution-liveness reconciler — the real fix superseding #726's band-aid.

#726 bumped the Cloud Run spawn lease 60→300s — a fixed timer guessing
cold-start duration, fragile the moment the image grows or a region throttles.
This reconciler replaces the guess with a real signal.

Every ``interval_seconds`` (default 20s — faster than the 60s pg_cron reaper,
so it gets first look) it scans lapsed-lease ``gcp_cloud_run_*`` task rows and,
for each, asks the owning runner — via :class:`LivenessProbeProtocol` — whether
the Cloud Run execution backing the row is actually alive. It then acts on the
verdict:

* ``ALIVE``              → extend the lease; the reaper's next pass skips the row.
* ``DEAD`` / ``TERMINAL_FAILED`` → ``fail_task(retry=True)`` immediately.
* ``TERMINAL_SUCCEEDED`` → reconcile the row to COMPLETED from the ``outputs``
  the container persisted before exiting 0.
* ``UNKNOWN``            → a young row whose handle isn't captured yet gets one
  short grace extension; otherwise no-op and the pg_cron reaper backstops.

The pg_cron ``reap_stuck_tasks`` function is intentionally **unchanged** — it
stays the ultimate backstop and is correct for in-process runners (whose owner
ids no probe maps, so the reconciler no-ops on them).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.liveness import LivenessVerdict, resolve_probe

logger = logging.getLogger(__name__)


class GcpLivenessReconciler:
    """Background loop that reconciles lapsed-lease Cloud Run task rows.

    Lifecycle is explicit (``start`` / ``stop``) so the host — ``GCPModule``'s
    lifespan — owns it cleanly. One bad row never kills the loop; one failed
    pass never kills the loop.
    """

    def __init__(
        self,
        engine: Any,
        *,
        interval_seconds: float = 20.0,
        extend_visibility_seconds: int = 300,
        unknown_grace_seconds: int = 180,
    ) -> None:
        self._engine = engine
        self._interval_seconds = float(interval_seconds)
        self._extend_visibility_seconds = int(extend_visibility_seconds)
        self._unknown_grace_seconds = int(unknown_grace_seconds)
        self._task: Optional[asyncio.Task] = None
        self._stopped = asyncio.Event()

    # --- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        """Spawn the reconcile loop. Idempotent — a running loop is left alone."""
        if self._task is not None and not self._task.done():
            return
        self._stopped.clear()
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        """Signal the loop to stop, cancel it, and await its teardown.

        Safe to call when the reconciler was never started.
        """
        self._stopped.set()
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001 — teardown
            pass
        self._task = None

    # --- loop --------------------------------------------------------------

    async def _run_loop(self) -> None:
        """Run ``_reconcile_once`` every ``interval_seconds`` until stopped.

        A failed pass is logged and the loop continues — the reconciler is a
        safety net, it must not be the thing that breaks.
        """
        while not self._stopped.is_set():
            try:
                await self._reconcile_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 — one bad pass must not kill the loop
                logger.error(
                    "GcpLivenessReconciler: reconcile pass failed: %s", e,
                    exc_info=True,
                )
            try:
                await asyncio.wait_for(
                    self._stopped.wait(), timeout=self._interval_seconds
                )
            except asyncio.TimeoutError:
                pass

    async def _reconcile_once(self) -> None:
        """Scan lapsed-lease Cloud Run rows and reconcile each one."""
        rows = await tasks_module.select_lapsed_gcp_tasks(self._engine)
        for row in rows:
            try:
                await self._reconcile_row(row)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 — one bad row must not stop the rest
                logger.warning(
                    "GcpLivenessReconciler: failed to reconcile task %s: %s",
                    row.get("task_id"), e,
                )

    async def _reconcile_row(self, row: Dict[str, Any]) -> None:
        """Probe the owning runner for ``row`` and act on the verdict."""
        from dynastore.models.tasks import Task

        owner_id = row.get("owner_id")
        task_id = row.get("task_id")
        if task_id is None:
            return  # malformed row — nothing to reconcile

        probe = resolve_probe(owner_id)
        if probe is None:
            # In-process / ephemeral / unrecognized owner — no probe maps it.
            # The pg_cron reaper handles this row exactly as today.
            return

        task = Task.model_validate(row)
        verdict = await probe.probe_liveness(task)
        now = datetime.now(timezone.utc)
        runner_ref = row.get("runner_ref")

        if verdict == LivenessVerdict.ALIVE:
            # The execution is genuinely running (or cold-starting) — extend the
            # lease so the reaper's next pass skips the row. This IS the
            # liveness signal that replaces the fixed spawn-lease timer.
            await tasks_module.heartbeat_tasks(
                self._engine, [task_id],
                timedelta(seconds=self._extend_visibility_seconds),
            )
            logger.info(
                "GcpLivenessReconciler: task %s ALIVE (execution=%s) — lease extended %ds.",
                task_id, runner_ref, self._extend_visibility_seconds,
            )
        elif verdict in (LivenessVerdict.DEAD, LivenessVerdict.TERMINAL_FAILED):
            reason = (
                "Cloud Run execution failed"
                if verdict == LivenessVerdict.TERMINAL_FAILED
                else "Cloud Run execution gone/cancelled"
            )
            await tasks_module.fail_task(
                self._engine, task_id, now,
                f"GcpLivenessReconciler: {reason} ({runner_ref})",
                retry=True,
            )
            logger.warning(
                "GcpLivenessReconciler: task %s %s (execution=%s) — failed (retry).",
                task_id, verdict.value, runner_ref,
            )
        elif verdict == LivenessVerdict.TERMINAL_SUCCEEDED:
            # The execution exited 0 but the row is still ACTIVE — reconcile it
            # to COMPLETED from the outputs the container persisted before exit
            # (main_task.py writes outputs before the terminal status flip, so
            # they are already on the row by the time the execution SUCCEEDED).
            outputs = row.get("outputs")
            await tasks_module.complete_task(
                self._engine, task_id, now, outputs=outputs,
            )
            logger.info(
                "GcpLivenessReconciler: task %s TERMINAL_SUCCEEDED (execution=%s) "
                "— reconciled to COMPLETED%s.",
                task_id, runner_ref,
                "" if outputs is not None else " (no outputs on row)",
            )
        else:  # LivenessVerdict.UNKNOWN
            started_at = row.get("started_at")
            young = (
                started_at is not None
                and started_at > now - timedelta(seconds=self._unknown_grace_seconds)
            )
            if not runner_ref and young:
                # The spawn→runner_ref-capture gap: give the row one short
                # grace extension so the reaper doesn't reclaim it before the
                # handle lands and a real probe becomes possible.
                await tasks_module.heartbeat_tasks(
                    self._engine, [task_id],
                    timedelta(seconds=self._unknown_grace_seconds),
                )
                logger.info(
                    "GcpLivenessReconciler: task %s UNKNOWN, young & no handle "
                    "— short grace extension %ds.",
                    task_id, self._unknown_grace_seconds,
                )
            else:
                # Inconclusive and not in the capture-gap window — leave it for
                # the pg_cron reaper. Fail-safe by design.
                logger.debug(
                    "GcpLivenessReconciler: task %s UNKNOWN — leaving for pg_cron reaper.",
                    task_id,
                )
