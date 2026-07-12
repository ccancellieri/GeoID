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

"""GCP execution-liveness reconciler — the real fix superseding #726's band-aid.

#726 bumped the Cloud Run spawn lease 60→300s — a fixed timer guessing
cold-start duration, fragile the moment the image grows or a region throttles.
This reconciler replaces the guess with a real signal.

Every ``interval_seconds`` (default from ``LeadershipConfig.leadership_interval_seconds``) it scans lapsed-lease
``gcp_cloud_run_*`` task rows and, for each, asks the owning runner — via
:class:`LivenessProbeProtocol` — whether the Cloud Run execution backing the row
is actually alive. It then acts on the verdict:

* ``ALIVE``              → extend the lease; the reaper's next pass skips the row.
* ``DEAD`` / ``TERMINAL_FAILED`` → ``fail_task(retry=True)`` immediately.
* ``TERMINAL_SUCCEEDED`` → reconcile the row to COMPLETED from the ``outputs``
  the container persisted before exiting 0.
* ``UNKNOWN``            → a young row whose handle isn't captured yet gets one
  short grace extension; otherwise no-op and the MaintenanceSupervisor
  ``task_reaper`` job backstops.

The ``reap_stuck_tasks`` PL/pgSQL function is intentionally **unchanged** — it
stays the ultimate backstop and is correct for in-process runners (whose owner
ids no probe maps, so the reconciler no-ops on them).
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, NamedTuple, Optional, Union

from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.liveness import LivenessVerdict, resolve_probe, resolve_stop_signal
from dynastore.modules.tasks.reconciliation import VerdictAction, decide_verdict_action
from dynastore.modules.db_config.connection_health_config import resolve_leadership_config
from dynastore.tools.background_service import (
    Leadership,
    LeaseRenewalMode,
    PeriodicService,
    PodPolicy,
    ServiceContext,
)

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
    ALIVE path when the conditional heartbeat matched 0 rows (the
    MaintenanceSupervisor ``task_reaper`` job won the SELECT→probe→act race);
    it lets ``_reconcile_once`` tally race losses distinctly without
    re-deriving them.
    """

    verdict: LivenessVerdict
    race_lost: bool = False


def _get_dismiss_force_delete_after() -> timedelta:
    """Get the grace period before force-deleting dismissed liveness records.

    Configuration: Resolved from
    ``LeadershipConfig.dismiss_force_delete_after_seconds`` (default 600s).
    """
    _, dismiss_seconds, _, _, _ = resolve_leadership_config()
    return timedelta(seconds=dismiss_seconds)


class GcpLivenessReconciler(PeriodicService):
    """Periodic service that reconciles lapsed-lease Cloud Run task rows.

    ``BackgroundSupervisor`` owns the loop lifecycle. One bad row never kills
    the loop; one failed pass is logged and the supervisor continues on the
    next cadence tick.

    Leadership policy: ``LEADER_ONLY`` — exactly one pod per service drives
    the reconciler writes. Followers skip until the advisory lock is free.

    Pod policy: ``SKIP_EPHEMERAL`` — Cloud Run Job containers run one task
    and exit; they must not open reconciler connections or contend on task rows.
    This is defense-in-depth alongside the existing
    ``_should_register_gcp_job_runner()`` gate in ``GCPModule.lifespan``.
    """

    name = "gcp_liveness_reconciler"
    leadership = Leadership.LEADER_ONLY
    pod_policy = PodPolicy.SKIP_EPHEMERAL
    # Default cadence (20s) sits close to the lease TTL (30s); per-tick
    # acquire/release would re-elect almost every cycle. Heartbeat mode
    # holds tenure across ticks and renews on its own cadence instead (#2900).
    lease_renewal_mode = LeaseRenewalMode.HEARTBEAT

    def __init__(
        self,
        engine: Any = None,
        *,
        interval_seconds: float | None = None,
        extend_visibility_seconds: int | None = None,
        unknown_grace_seconds: int | None = None,
        staleness_grace_seconds: int | None = None,
        staleness_max_passes: int | None = None,
    ) -> None:
        from dynastore.modules.db_config.instance import get_service_name as _get_service_name
        service = _get_service_name() or "unknown"

        # Resolve configurable leadership settings
        _, _, cfg_interval, cfg_visibility, cfg_unknown_grace = resolve_leadership_config()

        self.cadence_seconds: float = float(interval_seconds if interval_seconds is not None else cfg_interval)
        self.lock_key: Optional[Union[int, str]] = f"gcp-liveness-reconciler:{service}"
        self._engine: Any = engine
        self._extend_visibility_seconds: int = int(extend_visibility_seconds if extend_visibility_seconds is not None else cfg_visibility)
        self._unknown_grace_seconds: int = int(unknown_grace_seconds if unknown_grace_seconds is not None else cfg_unknown_grace)
        # geoid#2819: non-locking staleness detection tunables. Defaults here
        # (mirroring the backstop's hardcoded-default pattern above) are
        # overridden by GcpModuleConfig.liveness_staleness_grace_seconds /
        # liveness_staleness_max_passes when wired from gcp_module.py.
        self._staleness_grace_seconds: int = int(
            staleness_grace_seconds if staleness_grace_seconds is not None else 60
        )
        self._staleness_max_passes: int = int(
            staleness_max_passes if staleness_max_passes is not None else 2
        )
        # In-process streak tracker: task_id -> consecutive passes seen by the
        # non-locking staleness scan but NOT reached by this pass's FOR UPDATE
        # SKIP LOCKED scan. Leader-gated (this instance only ticks on the
        # elected leader pod), so no cross-pod synchronization is needed —
        # a leadership handoff simply restarts the streak, which is safe:
        # a genuinely stuck row re-accumulates the streak within
        # ``staleness_max_passes`` more ticks.
        self._stale_streak: Dict[Any, int] = {}

    # --- PeriodicService tick ----------------------------------------------

    async def tick(self, ctx: ServiceContext) -> None:
        """One reconcile pass, driven by ``BackgroundSupervisor`` on cadence.
        
        Uses ``ctx.lock_connection`` when available (LEADER_ONLY mode) to reuse
        the advisory-lock connection for DB work, avoiding a second pool checkout.
        Falls back to ``ctx.engine`` for RUN_EVERYWHERE mode or non-leader calls.
        """
        # Prefer lock_connection (AUTOCOMMIT advisory-lock connection) to avoid
        # acquiring a second connection from the pool during the tick.
        self._engine = ctx.lock_connection if ctx.lock_connection is not None else ctx.engine
        try:
            await self._reconcile_once()
        except Exception as e:  # noqa: BLE001 — one bad pass must not kill the loop
            logger.error(
                "GcpLivenessReconciler: reconcile pass failed: %s", e,
                exc_info=True,
            )

    async def _reconcile_once(self) -> None:
        """Scan lapsed-lease Cloud Run rows and reconcile each one.

        Also scans DISMISSED-but-unconfirmed GCP rows and drives them toward a
        confirmed stop via :class:`StopSignalProtocol`.

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
        claimed_ids = {row.get("task_id") for row in rows if row.get("task_id") is not None}
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
        # losing the SELECT→probe→act race to the MaintenanceSupervisor
        # ``task_reaper`` job and ``liveness_reconciler_interval_seconds``
        # needs tuning down.
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

        # geoid#2819: non-locking staleness pass — catches a row the FOR
        # UPDATE SKIP LOCKED scan above keeps silently skipping because a
        # zombie PG session still holds its row lock.
        await self._check_staleness(claimed_ids)

        # Dismissed-unconfirmed scan: drive DISMISSED rows whose backing
        # Cloud Run execution is still alive toward a confirmed stop.
        dismissed_rows = await tasks_module.select_dismissed_unconfirmed_gcp_tasks(
            self._engine
        )
        dismiss_unconfirmed_total = 0
        for drow in dismissed_rows:
            try:
                confirmed = await self._reconcile_dismissed_row(drow)
                if confirmed:
                    dismiss_unconfirmed_total += 1
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 — one bad row must not stop the rest
                logger.warning(
                    "GcpLivenessReconciler: failed to reconcile dismissed task %s: %s",
                    drow.get("task_id"), e,
                )
        if dismissed_rows:
            logger.info(
                "liveness_dismiss_pass service=%s scanned=%d dismiss_unconfirmed_total=%d",
                _SERVICE_NAME_FOR_METRICS,
                len(dismissed_rows),
                dismiss_unconfirmed_total,
            )

    async def _check_staleness(self, claimed_ids: set[Any]) -> None:
        """geoid#2819: surface a row the locking scan keeps silently skipping.

        Diffs a plain, non-locking SELECT of lapsed rows
        (:func:`tasks_module.select_stale_gcp_tasks`) against ``claimed_ids``
        — the ``task_id``s this pass's ``FOR UPDATE SKIP LOCKED`` scan
        actually reached. A row present in the former but absent from the
        latter, past ``_staleness_grace_seconds``, was silently skipped by
        the locking scan — consistent with a zombie PG session still holding
        its row lock (the row lock itself is invisible from application code
        without a ``pg_locks`` join; this is a symptom-based proxy).

        Only after ``_staleness_max_passes`` consecutive occurrences is the
        row logged loudly and a lock-free heal attempted, via the same
        probe + owner-guarded-write path ``_reconcile_row`` already uses for
        the primary scan and the RUN_EVERYWHERE backstop. Those writes are
        plain ``UPDATE ... WHERE task_id = ...`` statements bounded by
        ``DBConfig.lock_timeout`` (#2837 delivered this) — a still-live
        zombie lock makes the attempt fail fast instead of hanging, so
        retrying it every pass is safe. If the zombie session has since been
        reaped (by ``idle_in_transaction_session_timeout``, also #2837) the
        write lands immediately.

        The streak tracker is in-process state on this leader-gated instance
        — acceptable per geoid#2819: a leadership handoff simply restarts a
        stuck row's streak, and a genuinely stuck row re-accumulates it
        within a few more ticks.
        """
        try:
            stale_rows = await tasks_module.select_stale_gcp_tasks(
                self._engine, self._staleness_grace_seconds
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 — detection must not break the tick
            logger.warning(
                "GcpLivenessReconciler: staleness scan failed: %s", e,
            )
            return

        now = datetime.now(timezone.utc)
        seen_ids: set = set()
        for row in stale_rows:
            task_id = row.get("task_id")
            if task_id is None:
                continue
            seen_ids.add(task_id)

            if task_id in claimed_ids:
                # The locking scan reached this row on this very pass — it
                # is merely lapsed, not stuck behind a lock. Reset any prior
                # streak; a future occurrence starts counting from zero.
                self._stale_streak.pop(task_id, None)
                continue

            streak = self._stale_streak.get(task_id, 0) + 1
            self._stale_streak[task_id] = streak
            if streak < self._staleness_max_passes:
                continue

            locked_until = row.get("locked_until")
            age_s = (now - locked_until).total_seconds() if locked_until is not None else -1.0
            logger.warning(
                "liveness_staleness_visible_unclaimable service=%s task_id=%s "
                "owner_id=%s runner_ref=%s age_s=%.0f passes=%d — row is "
                "visible to a plain SELECT but has been skipped by FOR "
                "UPDATE SKIP LOCKED for %d consecutive reconciler pass(es); "
                "likely a zombie PG session still holding the row lock. "
                "Attempting a lock-free heal.",
                _SERVICE_NAME_FOR_METRICS, task_id, row.get("owner_id"),
                row.get("runner_ref"), age_s, streak, streak,
            )
            try:
                await self._reconcile_row(row)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 — one bad row must not stop the rest
                logger.warning(
                    "GcpLivenessReconciler: lock-free heal attempt failed "
                    "for task %s: %s", task_id, e,
                )
                # Leave the streak in place — the row is still stuck and the
                # next pass will retry the heal without re-logging until the
                # streak next crosses the threshold on a fresh occurrence.

        # Rows that dropped out of the stale scan this pass (healed,
        # reclaimed by the locking scan, or no longer lapsed) reset — a
        # fresh occurrence starts the streak over rather than accumulating
        # across unrelated incidents.
        for task_id in list(self._stale_streak):
            if task_id not in seen_ids:
                self._stale_streak.pop(task_id, None)

    async def _reconcile_dismissed_row(self, row: Dict[str, Any]) -> bool:
        """Drive a single DISMISSED-but-unconfirmed GCP row toward confirmed stop.

        Three outcomes (in order):

        a) Execution already stopped (probe returns terminal/DEAD/UNKNOWN-with-no-ref):
           stamp ``dismiss_confirmed_at = NOW()`` and return ``True``.
        b) Execution still ALIVE within the force-delete deadline:
           call ``runner.signal_stop(task)`` (cancel / graceful SIGTERM) and
           return ``False`` — the next reconciler cycle re-probes.
        c) Execution still ALIVE past the deadline
           (``LeadershipConfig.dismiss_force_delete_after_seconds``, default 600s):
           call ``runner.force_stop(task)`` (hard delete), stamp
           ``dismiss_confirmed_at``, emit ``dismiss_unconfirmed_total``,
           return ``True``.

        The deadline is derived from ``last_heartbeat_at`` (most recent alive
        signal from the job container) falling back to ``timestamp`` (row
        creation time) — both columns are always present without any schema
        change.  Elapsed time is measured from the most recent alive signal
        rather than from the dismiss request because we have no dismiss-request
        timestamp on the row; using the most-recent-alive-signal gives a
        conservative upper bound on execution age.

        Returns ``True`` when ``dismiss_confirmed_at`` was stamped this cycle
        (used by the caller to tally ``dismiss_unconfirmed_total``).
        """
        from dynastore.models.tasks import Task

        task_id = row.get("task_id")
        if task_id is None:
            return False  # malformed row — skip

        owner_id = row.get("owner_id")
        runner = resolve_stop_signal(owner_id)

        task = Task.model_validate(row)
        now = datetime.now(timezone.utc)
        runner_ref = row.get("runner_ref")

        # Probe liveness via the existing probe path (no runner_ref → UNKNOWN).
        probe = resolve_probe(owner_id)
        if probe is not None:
            verdict = await probe.probe_liveness(task)
        else:
            verdict = LivenessVerdict.UNKNOWN

        execution_stopped = verdict in (
            LivenessVerdict.DEAD,
            LivenessVerdict.TERMINAL_SUCCEEDED,
            LivenessVerdict.TERMINAL_FAILED,
        ) or (verdict == LivenessVerdict.UNKNOWN and not runner_ref)

        if execution_stopped:
            # Execution is gone/terminal — confirm the dismiss immediately.
            stamped = await tasks_module.stamp_dismiss_confirmed(self._engine, task_id)
            if stamped:
                logger.info(
                    "GcpLivenessReconciler: dismissed task %s confirmed stopped "
                    "(verdict=%s, execution=%s).",
                    task_id, verdict.value, runner_ref,
                )
            else:
                logger.debug(
                    "GcpLivenessReconciler: dismissed task %s stamp_dismiss_confirmed "
                    "matched 0 rows (concurrent reconciler or already confirmed).",
                    task_id,
                )
            return stamped

        # Execution is still ALIVE (or UNKNOWN with a handle).
        # Determine elapsed time since the last alive signal to decide
        # whether to escalate to force_stop.
        ref_ts: Optional[datetime] = row.get("last_heartbeat_at") or row.get("timestamp")
        elapsed = (now - ref_ts) if ref_ts is not None else None
        past_deadline = (
            elapsed is not None and elapsed >= _get_dismiss_force_delete_after()
        )

        if past_deadline:
            # Escalate: force-teardown the execution.
            force_sent = False
            if runner is not None:
                force_sent = await runner.force_stop(task)
            stamped = await tasks_module.stamp_dismiss_confirmed(self._engine, task_id)
            logger.warning(
                "GcpLivenessReconciler: dismissed task %s past force-stop deadline "
                "(elapsed=%.0fs, ref_ts=%s) — force_stop sent=%s, confirmed=%s.",
                task_id,
                elapsed.total_seconds() if elapsed is not None else -1,
                ref_ts,
                force_sent,
                stamped,
            )
            return stamped

        # Still within the graceful window — send a cancel signal and wait.
        if runner is not None:
            await runner.signal_stop(task)
        else:
            logger.debug(
                "GcpLivenessReconciler: dismissed task %s has no stop-signal runner "
                "for owner_id '%s' — will re-probe next cycle.",
                task_id, owner_id,
            )
        return False

    async def _reconcile_row_public(self, row: Dict[str, Any]) -> Optional[ReconcileOutcome]:
        """Public alias of :meth:`_reconcile_row` for :class:`GcpLivenessBackstop`.

        Both classes live in this module and share the verdict→action policy;
        the alias avoids a private-member reach-across while keeping
        ``_reconcile_row`` itself unchanged for the primary tick path.
        """
        return await self._reconcile_row(row)

    async def _reconcile_row(self, row: Dict[str, Any]) -> Optional[ReconcileOutcome]:
        """Probe the owning runner for ``row`` and act on the verdict.

        Returns a :class:`ReconcileOutcome` (verdict + race-loss flag) so
        :meth:`_reconcile_once` can build a per-pass distribution and tally
        race losses. Returns ``None`` when no probe owns the row (in-process /
        ephemeral / unrecognized) — those rows are left for the MaintenanceSupervisor
        ``task_reaper`` job.
        """
        from dynastore.models.tasks import Task

        owner_id = row.get("owner_id")
        task_id = row.get("task_id")
        if task_id is None:
            return None  # malformed row — nothing to reconcile

        probe = resolve_probe(owner_id)
        if probe is None:
            # In-process / ephemeral / unrecognized owner — no probe maps it.
            # The MaintenanceSupervisor task_reaper job handles this row.
            return None

        task = Task.model_validate(row)
        verdict = await probe.probe_liveness(task)
        now = datetime.now(timezone.utc)
        runner_ref = row.get("runner_ref")

        # ``decide_verdict_action`` is the single source of truth for the
        # verdict→action mapping, shared with the on-demand
        # ``reconcile_task_liveness`` read-path trigger (#750-followup) — the
        # two paths cannot drift apart on which action a verdict authorizes.
        # Each branch below still owns its own logging, retry-message wording,
        # race-loss bookkeeping and terminal-action routing.
        action = decide_verdict_action(verdict)

        if action is VerdictAction.EXTEND_LEASE:
            # The execution is genuinely running (or cold-starting) — extend
            # the lease so the reaper's next pass skips the row. This IS the
            # liveness signal that replaces the fixed spawn-lease timer.
            #
            # Conditional heartbeat: the helper updates only when the row is
            # still ``ACTIVE`` and returns whether it matched. A ``False``
            # return means the row was reclaimed by the MaintenanceSupervisor
            # ``task_reaper`` job between this reconciler's SELECT-commit and
            # its UPDATE — the accepted race window. Surface it so operators
            # can see how often it fires in practice and tune the reconciler
            # interval down (#741 item 3).
            extended = await tasks_module.heartbeat_task_if_active(
                self._engine, task_id,
                timedelta(seconds=self._extend_visibility_seconds),
                created_at=task.timestamp,
            )
            if extended:
                logger.info(
                    "GcpLivenessReconciler: task %s ALIVE (execution=%s) — lease extended %ds.",
                    task_id, runner_ref, self._extend_visibility_seconds,
                )
            else:
                logger.warning(
                    "GcpLivenessReconciler: task %s ALIVE (execution=%s) but heartbeat "
                    "matched 0 rows — the MaintenanceSupervisor task_reaper won the "
                    "SELECT→probe→act race. "
                    "Consider tuning liveness_reconciler_interval_seconds down.",
                    task_id, runner_ref,
                )
            # verdict stays ALIVE (the probe was truthful); race_lost carries
            # the "reaper got there first" signal for the pass summary.
            return ReconcileOutcome(verdict, race_lost=not extended)
        elif action is VerdictAction.FAIL_RETRY:
            reason = (
                "Cloud Run execution failed"
                if verdict == LivenessVerdict.TERMINAL_FAILED
                else "Cloud Run execution gone/cancelled"
            )
            # Race-guarded by ``owner_id``: only fail the exact execution
            # attempt the probe observed. If the MaintenanceSupervisor
            # task_reaper job reclaimed the row and the dispatcher re-claimed
            # it as a fresh attempt between
            # this reconciler's SELECT and now, ``fail_task`` matches 0 rows —
            # don't fail a task that is legitimately running again (#750).
            acted = await tasks_module.fail_task(
                self._engine, task_id, now,
                f"GcpLivenessReconciler: {reason} ({runner_ref})",
                retry=True, owner_id=owner_id, created_at=task.timestamp,
            )
            if acted:
                logger.warning(
                    "GcpLivenessReconciler: task %s %s (execution=%s) — failed (retry).",
                    task_id, verdict.value, runner_ref,
                )
                # Exactly-once: fire the terminal Action only when the owner-guarded
                # write landed.  DEAD maps to on_timeout (Cloud Run taskTimeout
                # cancellation) while TERMINAL_FAILED maps to on_failure (logic
                # error / non-zero exit).
                _outcome = "timeout" if verdict == LivenessVerdict.DEAD else "failure"
                try:
                    from dynastore.modules.tasks.execution import (
                        apply_terminal_action as _apply_terminal_action,
                        resolve_routing_terminal as _resolve_routing_terminal,
                    )
                    _terminal = await _resolve_routing_terminal(task.task_type)
                    _action = _terminal.on_timeout if verdict == LivenessVerdict.DEAD else _terminal.on_failure
                    await _apply_terminal_action(
                        self._engine,
                        outcome=_outcome,
                        action=_action,
                        task_id=task_id,
                        task_type=task.task_type,
                        inputs=row.get("inputs"),
                        caller_id=row.get("caller_id"),
                        collection_id=row.get("collection_id"),
                        schema=row.get("catalog_id", "tasks"),
                        scope=row.get("scope"),
                    )
                except Exception as _ta_exc:
                    logger.warning(
                        "GcpLivenessReconciler: apply_terminal_action(%s) failed "
                        "for task %s: %s — continuing.",
                        _outcome, task_id, _ta_exc,
                    )
            else:
                logger.warning(
                    "GcpLivenessReconciler: task %s %s (execution=%s) but fail_task "
                    "matched 0 rows — the MaintenanceSupervisor task_reaper won the "
                    "SELECT→probe→act race. "
                    "Consider tuning liveness_reconciler_interval_seconds down.",
                    task_id, verdict.value, runner_ref,
                )
            return ReconcileOutcome(verdict, race_lost=not acted)
        elif action is VerdictAction.COMPLETE:
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
                created_at=task.timestamp,
            )
            if acted:
                logger.info(
                    "GcpLivenessReconciler: task %s TERMINAL_SUCCEEDED (execution=%s) "
                    "— reconciled to COMPLETED%s.",
                    task_id, runner_ref,
                    "" if outputs is not None else " (no outputs on row)",
                )
                # Exactly-once: fire on_success only when the owner-guarded write
                # landed.  A lost race (else branch) must not double-enqueue.
                try:
                    from dynastore.modules.tasks.execution import (
                        apply_terminal_action as _apply_terminal_action,
                        resolve_routing_terminal as _resolve_routing_terminal,
                    )
                    _terminal = await _resolve_routing_terminal(task.task_type)
                    await _apply_terminal_action(
                        self._engine,
                        outcome="success",
                        action=_terminal.on_success,
                        task_id=task_id,
                        task_type=task.task_type,
                        inputs=row.get("inputs"),
                        caller_id=row.get("caller_id"),
                        collection_id=row.get("collection_id"),
                        schema=row.get("catalog_id", "tasks"),
                        scope=row.get("scope"),
                    )
                except Exception as _ta_exc:
                    logger.warning(
                        "GcpLivenessReconciler: apply_terminal_action(success) failed "
                        "for task %s: %s — continuing.",
                        task_id, _ta_exc,
                    )
            else:
                logger.warning(
                    "GcpLivenessReconciler: task %s TERMINAL_SUCCEEDED (execution=%s) but "
                    "complete_task matched 0 rows — the MaintenanceSupervisor task_reaper "
                    "won the SELECT→probe→act race. Consider tuning "
                    "liveness_reconciler_interval_seconds down.",
                    task_id, runner_ref,
                )
            return ReconcileOutcome(verdict, race_lost=not acted)
        else:  # VerdictAction.NOOP (LivenessVerdict.UNKNOWN)
            started_at = row.get("started_at")
            # #2893: started_at is NULL while a REMOTE task's container is
            # still cold-starting (no longer stamped at dispatch). NULL here
            # means "not started yet", not "unknown" — treat it as maximally
            # young rather than falling through to the else branch below,
            # which would deny this grace extension to every not-yet-booted
            # container.
            young = started_at is None or (
                started_at > now - timedelta(seconds=self._unknown_grace_seconds)
            )
            if not runner_ref and young:
                # The spawn→runner_ref-capture gap: give the row one short
                # grace extension so the reaper doesn't reclaim it before the
                # handle lands and a real probe becomes possible.
                await tasks_module.heartbeat_tasks(
                    self._engine, [(task_id, task.timestamp)],
                    timedelta(seconds=self._unknown_grace_seconds),
                )
                logger.info(
                    "GcpLivenessReconciler: task %s UNKNOWN, young & no handle "
                    "— short grace extension %ds.",
                    task_id, self._unknown_grace_seconds,
                )
            else:
                # Inconclusive and not in the capture-gap window — leave it for
                # the MaintenanceSupervisor task_reaper job. Fail-safe by design.
                logger.debug(
                    "GcpLivenessReconciler: task %s UNKNOWN — leaving for MaintenanceSupervisor task_reaper.",
                    task_id,
                )
            return ReconcileOutcome(verdict)


class GcpLivenessBackstop(PeriodicService):
    """RUN_EVERYWHERE watchdog that heals lapsed rows a starved leader misses (#2771).

    ``GcpLivenessReconciler`` is ``LEADER_ONLY``: exactly one pod drives its
    writes, and every other pod skips its tick until the advisory/lease lock
    is free. If the leader loop never elects — e.g. the DB-pool-churn
    scenario in #2333, where every ``acquire_leadership()`` attempt trips a
    transient connection error — the reconciler's pass silently never runs.
    Lapsed-lease Cloud Run task rows then sit un-probed with no operator
    signal until someone notices a job stuck ``RUNNING`` for days.

    This service ticks on every pod, independent of leader election, so a
    starved leader loop can no longer take the whole backstop down with it.

    Detection is symptom-based rather than tied to one election backend (the
    lease-table backend persists a row a follower could inspect; the
    advisory backend does not). A lapsed-lease row only trips the backstop
    once it has been expired for at least ``stale_multiplier *
    liveness_reconciler_interval_seconds`` — long enough that a healthy
    reconciler tick would certainly have reached it already, so ordinary
    cadence jitter between reconciler passes never fires this path. When the
    primary reconciler is healthy, this tick's scan finds nothing past the
    staleness bar and is a cheap no-op indexed SELECT.

    Once starvation is confirmed for the pass, every currently lapsed row is
    reconciled (not only the stale ones that tripped detection) via
    :meth:`GcpLivenessReconciler._reconcile_row_public` — the same probe +
    :func:`~dynastore.modules.tasks.reconciliation.decide_verdict_action` +
    owner-guarded write path the primary reconciler and the on-demand #2620
    read-path trigger both use, so this is wiring, not new verdict policy.

    Concurrent execution across pods during a real starvation event is
    tolerated by the same race-handling the reconciler already documents:
    every write is owner_id-guarded, so a pod that loses the race to another
    pod (or to the MaintenanceSupervisor task_reaper) gets a truthful no-op,
    not a double-write. No additional advisory lock is taken here on
    purpose — the failure mode this backstop exists for is exactly a DB
    under lock/connection pressure, and adding another lock acquisition to
    the recovery path would trade a correctness gap for a liveness gap.
    """

    name = "gcp_liveness_backstop"
    leadership = Leadership.RUN_EVERYWHERE
    pod_policy = PodPolicy.SKIP_EPHEMERAL

    def __init__(
        self,
        *,
        cadence_seconds: Optional[float] = None,
        stale_multiplier: Optional[float] = None,
        extend_visibility_seconds: Optional[int] = None,
        unknown_grace_seconds: Optional[int] = None,
    ) -> None:
        _, _, cfg_interval, cfg_visibility, cfg_unknown_grace = resolve_leadership_config()
        self._primary_interval_seconds: float = cfg_interval
        self.cadence_seconds = float(
            cadence_seconds if cadence_seconds is not None else max(cfg_interval * 3.0, 60.0)
        )
        self._stale_multiplier: float = float(
            stale_multiplier if stale_multiplier is not None else 3.0
        )
        self._extend_visibility_seconds: Optional[int] = extend_visibility_seconds
        self._unknown_grace_seconds: Optional[int] = unknown_grace_seconds

    async def tick(self, ctx: ServiceContext) -> None:
        try:
            await self._reconcile_if_starved(ctx.engine)
        except Exception as e:  # noqa: BLE001 — one bad pass must not kill the loop
            logger.error(
                "GcpLivenessBackstop: pass failed: %s", e, exc_info=True,
            )

    async def _reconcile_if_starved(self, engine: Any) -> None:
        rows = await tasks_module.select_lapsed_gcp_tasks(engine)
        if not rows:
            return

        now = datetime.now(timezone.utc)
        stale_cutoff = now - timedelta(
            seconds=self._primary_interval_seconds * self._stale_multiplier
        )
        starved = [
            row for row in rows
            if row.get("locked_until") is not None and row["locked_until"] <= stale_cutoff
        ]
        if not starved:
            # Rows exist but none are stale enough to prove the primary
            # reconciler missed them — ordinary inter-pass jitter.
            return

        oldest_lapsed_s = max(
            (now - row["locked_until"]).total_seconds() for row in starved
        )
        logger.warning(
            "liveness_backstop_starvation_detected service=%s scanned=%d "
            "starved=%d oldest_lapsed_s=%.0f — the LEADER_ONLY reconciler "
            "appears to have missed at least %.0f cycle(s); reconciling directly.",
            _SERVICE_NAME_FOR_METRICS, len(rows), len(starved),
            oldest_lapsed_s, self._stale_multiplier,
        )

        reconciler = GcpLivenessReconciler(
            engine,
            extend_visibility_seconds=self._extend_visibility_seconds,
            unknown_grace_seconds=self._unknown_grace_seconds,
        )
        healed = 0
        for row in rows:
            try:
                outcome = await reconciler._reconcile_row_public(row)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 — one bad row must not stop the rest
                logger.warning(
                    "GcpLivenessBackstop: failed to reconcile task %s: %s",
                    row.get("task_id"), e,
                )
                continue
            if outcome is not None:
                healed += 1
        logger.info(
            "liveness_backstop_pass service=%s scanned=%d healed=%d",
            _SERVICE_NAME_FOR_METRICS, len(rows), healed,
        )
