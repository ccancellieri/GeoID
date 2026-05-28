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
import os
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, NamedTuple, Optional

from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.liveness import LivenessVerdict, resolve_probe

logger = logging.getLogger(__name__)


def _resolve_service_name() -> str:
    """The service identity stamped on the structured metric log line.

    Same source-of-truth (``instance.json`` → ``SERVICE_NAME`` env → literal
    ``"unknown"``) used by ``query_executor``'s ``db_pool_acquire`` metric, so
    a GCP log-based metric can partition reconciler passes by service exactly
    as it partitions pool-acquire latency.
    """
    try:
        from dynastore.modules.db_config.instance import get_service_name
        name = get_service_name()
        if name:
            return name
    except Exception:  # noqa: BLE001 — never let metrics setup crash imports
        pass
    return os.getenv("SERVICE_NAME") or "unknown"


_SERVICE_NAME_FOR_METRICS = _resolve_service_name()


class ReconcileOutcome(NamedTuple):
    """The result of reconciling one lapsed-lease row.

    ``verdict`` is the probe's verdict verbatim — truthful even when the
    follow-up action lost a race. ``race_lost`` is ``True`` only on the
    ALIVE path when the conditional heartbeat matched 0 rows (the pg_cron
    reaper won the SELECT→probe→act race); it lets ``_reconcile_once`` tally
    race losses distinctly without re-deriving them.
    """

    verdict: LivenessVerdict
    race_lost: bool = False


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
        except asyncio.CancelledError:
            pass  # expected — we just cancelled it
        except Exception:
            logger.warning(
                "GCPLivenessReconciler: loop task errored during teardown",
                exc_info=True,
            )
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
        """Scan lapsed-lease Cloud Run rows and reconcile each one.

        Accumulates a verdict-distribution :class:`~collections.Counter` over
        the pass plus a distinct race-loss tally, and emits one structured
        INFO summary line at the end (#741 item 3 / #745 items 1-2).

        The summary line follows the house ``<token> service=… key=value``
        shape used by ``db_pool_acquire`` so a GCP log-based metric can
        extract fields without a prometheus_client dependency. ``service=``,
        ``scanned=`` and ``RACE_LOST=`` are always present — even on an idle
        pass — so the metric filter and the race-loss extractor never lose
        their data point; the per-verdict counts are emitted only when
        non-zero to keep the line compact.
        """
        rows = await tasks_module.select_lapsed_gcp_tasks(self._engine)
        verdicts: Counter[str] = Counter()
        unmapped = 0
        errors = 0
        race_lost = 0
        for row in rows:
            try:
                outcome = await self._reconcile_row(row)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 — one bad row must not stop the rest
                errors += 1
                logger.warning(
                    "GcpLivenessReconciler: failed to reconcile task %s: %s",
                    row.get("task_id"), e,
                )
                continue
            if outcome is None:
                unmapped += 1
            else:
                verdicts[outcome.verdict.name] += 1
                if outcome.race_lost:
                    race_lost += 1

        # One structured summary line per pass. ``RACE_LOST`` is the headline
        # signal of #745 — extended in #750 to span every verdict path. A row
        # whose follow-up write matched 0 rows still counts under its probed
        # verdict in the distribution (the probe was truthful):
        #
        # * ALIVE  + ``heartbeat_task_if_active`` matched 0 → ALIVE + RACE_LOST
        # * DEAD / TERMINAL_FAILED + ``fail_task`` matched 0 (owner_id guard)
        #   → DEAD / TERMINAL_FAILED + RACE_LOST
        # * TERMINAL_SUCCEEDED + ``complete_task`` matched 0 (owner_id guard)
        #   → TERMINAL_SUCCEEDED + RACE_LOST
        #
        # In every case the same operator signal applies: the reconciler is
        # losing the SELECT→probe→act race to the pg_cron reaper and
        # ``liveness_reconciler_interval_seconds`` needs tuning down.
        parts = [f"{name}={count}" for name, count in sorted(verdicts.items())]
        if unmapped:
            parts.append(f"UNMAPPED={unmapped}")
        if errors:
            parts.append(f"ERROR={errors}")
        verdict_suffix = (" " + " ".join(parts)) if parts else ""
        logger.info(
            "liveness_reconcile_pass service=%s scanned=%d RACE_LOST=%d%s",
            _SERVICE_NAME_FOR_METRICS, len(rows), race_lost, verdict_suffix,
        )

    async def _reconcile_row(self, row: Dict[str, Any]) -> Optional[ReconcileOutcome]:
        """Probe the owning runner for ``row`` and act on the verdict.

        Returns a :class:`ReconcileOutcome` (verdict + race-loss flag) so
        :meth:`_reconcile_once` can build a per-pass distribution and tally
        race losses. Returns ``None`` when no probe owns the row (in-process /
        ephemeral / unrecognized) — those rows are left for the pg_cron reaper.
        """
        from dynastore.models.tasks import Task

        owner_id = row.get("owner_id")
        task_id = row.get("task_id")
        if task_id is None:
            return None  # malformed row — nothing to reconcile

        probe = resolve_probe(owner_id)
        if probe is None:
            # In-process / ephemeral / unrecognized owner — no probe maps it.
            # The pg_cron reaper handles this row exactly as today.
            return None

        task = Task.model_validate(row)
        verdict = await probe.probe_liveness(task)
        now = datetime.now(timezone.utc)
        runner_ref = row.get("runner_ref")

        if verdict == LivenessVerdict.ALIVE:
            # The execution is genuinely running (or cold-starting) — extend
            # the lease so the reaper's next pass skips the row. This IS the
            # liveness signal that replaces the fixed spawn-lease timer.
            #
            # Conditional heartbeat: the helper updates only when the row is
            # still ``ACTIVE`` and returns whether it matched. A ``False``
            # return means the row was reclaimed by the pg_cron reaper between
            # this reconciler's SELECT-commit and its UPDATE — the accepted
            # race window. Surface it so operators can see how often it fires
            # in practice and tune the reconciler interval down (#741 item 3).
            extended = await tasks_module.heartbeat_task_if_active(
                self._engine, task_id,
                timedelta(seconds=self._extend_visibility_seconds),
            )
            if extended:
                logger.info(
                    "GcpLivenessReconciler: task %s ALIVE (execution=%s) — lease extended %ds.",
                    task_id, runner_ref, self._extend_visibility_seconds,
                )
            else:
                logger.warning(
                    "GcpLivenessReconciler: task %s ALIVE (execution=%s) but heartbeat "
                    "matched 0 rows — the pg_cron reaper won the SELECT→probe→act race. "
                    "Consider tuning liveness_reconciler_interval_seconds down.",
                    task_id, runner_ref,
                )
            # verdict stays ALIVE (the probe was truthful); race_lost carries
            # the "reaper got there first" signal for the pass summary.
            return ReconcileOutcome(verdict, race_lost=not extended)
        elif verdict in (LivenessVerdict.DEAD, LivenessVerdict.TERMINAL_FAILED):
            reason = (
                "Cloud Run execution failed"
                if verdict == LivenessVerdict.TERMINAL_FAILED
                else "Cloud Run execution gone/cancelled"
            )
            # Race-guarded by ``owner_id``: only fail the exact execution
            # attempt the probe observed. If the pg_cron reaper reclaimed the
            # row and the dispatcher re-claimed it as a fresh attempt between
            # this reconciler's SELECT and now, ``fail_task`` matches 0 rows —
            # don't fail a task that is legitimately running again (#750).
            acted = await tasks_module.fail_task(
                self._engine, task_id, now,
                f"GcpLivenessReconciler: {reason} ({runner_ref})",
                retry=True, owner_id=owner_id,
            )
            if acted:
                logger.warning(
                    "GcpLivenessReconciler: task %s %s (execution=%s) — failed (retry).",
                    task_id, verdict.value, runner_ref,
                )
            else:
                logger.warning(
                    "GcpLivenessReconciler: task %s %s (execution=%s) but fail_task "
                    "matched 0 rows — the pg_cron reaper won the SELECT→probe→act race. "
                    "Consider tuning liveness_reconciler_interval_seconds down.",
                    task_id, verdict.value, runner_ref,
                )
            return ReconcileOutcome(verdict, race_lost=not acted)
        elif verdict == LivenessVerdict.TERMINAL_SUCCEEDED:
            # The execution exited 0 but the row is still ACTIVE — reconcile it
            # to COMPLETED from the outputs the container persisted before exit
            # (main_task.py writes outputs before the terminal status flip, so
            # they are already on the row by the time the execution SUCCEEDED).
            #
            # Race-guarded by ``owner_id`` exactly like the fail path: if the
            # row was reclaimed and re-dispatched, ``complete_task`` matches 0
            # rows and we report a lost race instead of completing a fresh
            # attempt out from under Cloud Run (#750).
            outputs = row.get("outputs")
            acted = await tasks_module.complete_task(
                self._engine, task_id, now, outputs=outputs, owner_id=owner_id,
            )
            if acted:
                logger.info(
                    "GcpLivenessReconciler: task %s TERMINAL_SUCCEEDED (execution=%s) "
                    "— reconciled to COMPLETED%s.",
                    task_id, runner_ref,
                    "" if outputs is not None else " (no outputs on row)",
                )
            else:
                logger.warning(
                    "GcpLivenessReconciler: task %s TERMINAL_SUCCEEDED (execution=%s) but "
                    "complete_task matched 0 rows — the pg_cron reaper won the "
                    "SELECT→probe→act race. Consider tuning "
                    "liveness_reconciler_interval_seconds down.",
                    task_id, runner_ref,
                )
            return ReconcileOutcome(verdict, race_lost=not acted)
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
            return ReconcileOutcome(verdict)
