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

"""Unit tests for ``GcpLivenessReconciler`` — the per-verdict reconciler loop.

#735: this loop replaces the fixed spawn-lease guess. Every ~20s (faster than
the 60s pg_cron reaper, so it gets first look) it scans lapsed-lease
``gcp_cloud_run_*`` task rows, probes the owning runner, and acts on the
verdict — extending the lease of a live execution, failing a dead one, or
reconciling a row whose execution already terminated. The pg_cron reaper stays
unchanged as the ultimate backstop.
"""

from __future__ import annotations

import asyncio
import inspect
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


@pytest.fixture(autouse=True)
def disable_managed_eventing():
    """Neutralize the DB-bound autouse fixture from gcp/conftest.py."""
    return None


def _reconciler_mod():
    from dynastore.modules.gcp import liveness_reconciler
    return liveness_reconciler


def _make_reconciler(**kwargs):
    from dynastore.modules.gcp.liveness_reconciler import GcpLivenessReconciler

    defaults = dict(
        interval_seconds=0.01,
        extend_visibility_seconds=300,
        unknown_grace_seconds=180,
    )
    defaults.update(kwargs)
    return GcpLivenessReconciler(engine=SimpleNamespace(), **defaults)


def _row(*, owner_id="gcp_cloud_run_abc", runner_ref="projects/p/.../executions/e",
         started_at=None, outputs=None):
    return {
        "task_id": uuid.uuid4(),
        "schema_name": "tasks",
        "task_type": "ingest",
        "owner_id": owner_id,
        "runner_ref": runner_ref,
        "started_at": started_at or datetime.now(timezone.utc),
        "outputs": outputs,
        "retry_count": 0,
        "max_retries": 3,
        "locked_until": datetime.now(timezone.utc) - timedelta(seconds=5),
    }


class _Probe:
    runner_type = "gcp_cloud_run"

    def __init__(self, verdict):
        self._verdict = verdict

    def owns(self, owner_id):
        return True

    async def probe_liveness(self, task):
        return self._verdict


def _patch_actions(monkeypatch):
    """Replace the terminal/heartbeat helpers with AsyncMocks.

    * ``heartbeat_tasks``         — the batched unconditional helper, used by
                                    the UNKNOWN-young grace extension.
    * ``heartbeat_task_if_active`` — the single-row conditional helper (#741),
                                    used by the ALIVE path so the rowcount
                                    can surface a reaper-won-race signal.
    * ``fail_task`` / ``complete_task`` — the terminal-verdict actions; like
                                    ``heartbeat_task_if_active`` they return a
                                    bool (#750 owner-guarded race signal), so
                                    they default to ``True`` (acted on a row).
    * ``select_dismissed_unconfirmed_gcp_tasks`` / ``stamp_dismiss_confirmed``
      — the confirmed-dismiss helpers; patched here so tests that call
      ``_reconcile_once`` (which drives both the lapsed-lease and
      dismissed-unconfirmed scans) do not hit the real DB.
    * ``select_stale_gcp_tasks`` — the geoid#2819 non-locking staleness scan
      ``_reconcile_once`` also drives every pass; defaults to an empty result
      so tests that don't care about staleness detection see no extra rows.
    """
    from dynastore.modules.tasks import tasks_module

    hb = AsyncMock()
    hb_if_active = AsyncMock(return_value=True)
    fail = AsyncMock(return_value=True)
    complete = AsyncMock(return_value=True)
    select_dismissed = AsyncMock(return_value=[])
    stamp = AsyncMock(return_value=True)
    select_stale = AsyncMock(return_value=[])
    monkeypatch.setattr(tasks_module, "heartbeat_tasks", hb)
    monkeypatch.setattr(tasks_module, "heartbeat_task_if_active", hb_if_active)
    monkeypatch.setattr(tasks_module, "fail_task", fail)
    monkeypatch.setattr(tasks_module, "complete_task", complete)
    monkeypatch.setattr(tasks_module, "select_dismissed_unconfirmed_gcp_tasks", select_dismissed)
    monkeypatch.setattr(tasks_module, "stamp_dismiss_confirmed", stamp)
    monkeypatch.setattr(tasks_module, "select_stale_gcp_tasks", select_stale)
    return SimpleNamespace(
        heartbeat=hb,
        heartbeat_if_active=hb_if_active,
        fail=fail,
        complete=complete,
        select_dismissed=select_dismissed,
        stamp=stamp,
        select_stale=select_stale,
    )


def _patch_probe(monkeypatch, verdict):
    probe = _Probe(verdict)
    monkeypatch.setattr(_reconciler_mod(), "resolve_probe", lambda owner_id: probe)
    return probe


# --- per-verdict actions ---------------------------------------------------

@pytest.mark.asyncio
async def test_alive_extends_lease(monkeypatch):
    from dynastore.modules.tasks.liveness import LivenessVerdict

    actions = _patch_actions(monkeypatch)
    _patch_probe(monkeypatch, LivenessVerdict.ALIVE)
    rec = _make_reconciler()
    row = _row()

    await rec._reconcile_row(row)

    # ALIVE uses the *conditional* heartbeat so the rowcount surfaces the
    # reaper-race signal (#741 observability).
    actions.heartbeat_if_active.assert_awaited_once()
    assert actions.heartbeat_if_active.await_args.args[1] == row["task_id"]
    actions.heartbeat.assert_not_awaited()
    actions.fail.assert_not_awaited()
    actions.complete.assert_not_awaited()


@pytest.mark.asyncio
async def test_dead_fails_task_with_retry(monkeypatch):
    from dynastore.modules.tasks.liveness import LivenessVerdict

    actions = _patch_actions(monkeypatch)
    _patch_probe(monkeypatch, LivenessVerdict.DEAD)
    rec = _make_reconciler()
    row = _row()

    await rec._reconcile_row(row)

    actions.fail.assert_awaited_once()
    assert actions.fail.await_args.kwargs.get("retry") is True
    # #750: the terminal-verdict write is owner-guarded — the reconciler
    # passes the owner_id it probed so it can only fail that exact attempt.
    assert actions.fail.await_args.kwargs.get("owner_id") == row["owner_id"]
    actions.heartbeat.assert_not_awaited()
    actions.complete.assert_not_awaited()


@pytest.mark.asyncio
async def test_terminal_failed_fails_task_with_retry(monkeypatch):
    from dynastore.modules.tasks.liveness import LivenessVerdict

    actions = _patch_actions(monkeypatch)
    _patch_probe(monkeypatch, LivenessVerdict.TERMINAL_FAILED)
    rec = _make_reconciler()

    await rec._reconcile_row(_row())

    actions.fail.assert_awaited_once()
    assert actions.fail.await_args.kwargs.get("retry") is True


@pytest.mark.asyncio
async def test_terminal_succeeded_completes_task_with_outputs(monkeypatch):
    from dynastore.modules.tasks.liveness import LivenessVerdict

    actions = _patch_actions(monkeypatch)
    _patch_probe(monkeypatch, LivenessVerdict.TERMINAL_SUCCEEDED)
    rec = _make_reconciler()
    row = _row(outputs={"result": "ok"})

    await rec._reconcile_row(row)

    actions.complete.assert_awaited_once()
    assert actions.complete.await_args.kwargs.get("outputs") == {"result": "ok"}
    # #750: owner-guarded — only completes the exact attempt the probe saw.
    assert actions.complete.await_args.kwargs.get("owner_id") == row["owner_id"]
    actions.fail.assert_not_awaited()


@pytest.mark.asyncio
async def test_unknown_young_no_ref_extends_once(monkeypatch):
    """A row with no runner_ref yet but still inside the spawn→capture grace
    window gets one short lease extension — covering the capture gap."""
    from dynastore.modules.tasks.liveness import LivenessVerdict

    actions = _patch_actions(monkeypatch)
    _patch_probe(monkeypatch, LivenessVerdict.UNKNOWN)
    rec = _make_reconciler(unknown_grace_seconds=180)
    row = _row(runner_ref=None, started_at=datetime.now(timezone.utc) - timedelta(seconds=10))

    await rec._reconcile_row(row)

    actions.heartbeat.assert_awaited_once()


@pytest.mark.asyncio
async def test_unknown_old_is_noop(monkeypatch):
    """Past the grace window with no runner_ref — leave it for the pg_cron reaper."""
    from dynastore.modules.tasks.liveness import LivenessVerdict

    actions = _patch_actions(monkeypatch)
    _patch_probe(monkeypatch, LivenessVerdict.UNKNOWN)
    rec = _make_reconciler(unknown_grace_seconds=60)
    row = _row(runner_ref=None, started_at=datetime.now(timezone.utc) - timedelta(seconds=600))

    await rec._reconcile_row(row)

    actions.heartbeat.assert_not_awaited()
    actions.fail.assert_not_awaited()
    actions.complete.assert_not_awaited()


@pytest.mark.asyncio
async def test_unmapped_owner_is_noop(monkeypatch):
    """No probe owns the row (in-process / ephemeral runner) — reconciler
    no-ops, the pg_cron reaper handles it exactly as today."""
    actions = _patch_actions(monkeypatch)
    monkeypatch.setattr(_reconciler_mod(), "resolve_probe", lambda owner_id: None)
    rec = _make_reconciler()

    await rec._reconcile_row(_row(owner_id="dispatcher-pod-1"))

    actions.heartbeat.assert_not_awaited()
    actions.fail.assert_not_awaited()
    actions.complete.assert_not_awaited()


# --- loop resilience -------------------------------------------------------

@pytest.mark.asyncio
async def test_reconcile_once_one_bad_row_does_not_stop_the_rest(monkeypatch):
    from dynastore.modules.tasks import tasks_module
    from dynastore.modules.tasks.liveness import LivenessVerdict

    actions = _patch_actions(monkeypatch)
    rows = [_row(), _row(), _row()]
    monkeypatch.setattr(
        tasks_module, "select_lapsed_gcp_tasks", AsyncMock(return_value=rows)
    )

    # Second row's probe raises; first and third must still be reconciled.
    calls = {"n": 0}

    class _FlakyProbe:
        runner_type = "gcp_cloud_run"

        def owns(self, owner_id):
            return True

        async def probe_liveness(self, task):
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("probe blew up")
            return LivenessVerdict.ALIVE

    monkeypatch.setattr(_reconciler_mod(), "resolve_probe", lambda owner_id: _FlakyProbe())
    rec = _make_reconciler()

    await rec._reconcile_once()

    # Two good rows still extended despite the middle one raising.
    assert actions.heartbeat_if_active.await_count == 2


# --- lifecycle (PeriodicService loop) --------------------------------------

@pytest.mark.asyncio
async def test_periodic_run_loop_ticks_then_stops_on_shutdown(monkeypatch):
    """PeriodicService.run drives tick() repeatedly and exits on shutdown.

    The hand-wired start()/stop()/_run_loop scaffolding was removed when the
    reconciler migrated onto the BackgroundService primitive; the supervisor now
    owns lifecycle. This verifies the inherited run() loop calls tick() at least
    once and returns promptly once the shutdown event is set.
    """
    from dynastore.tools.background_service import ServiceContext

    rec = _make_reconciler(interval_seconds=0.01)
    ran = {"n": 0}

    async def _fake_once():
        ran["n"] += 1

    monkeypatch.setattr(rec, "_reconcile_once", _fake_once)

    ctx = ServiceContext(
        engine=rec._engine,
        shutdown=asyncio.Event(),
        is_ephemeral=False,
        name="test-host",
    )
    loop_task = asyncio.create_task(rec.run(ctx))
    await asyncio.sleep(0.05)
    ctx.shutdown.set()
    await asyncio.wait_for(loop_task, timeout=1.0)

    assert ran["n"] >= 1
    assert loop_task.done()


def test_lifespan_gates_reconciler_on_job_runner_host():
    """The reconciler is started in ``GCPModule.lifespan`` only on a process
    that hosts the job-runner — Cloud Run Job containers and opted-out
    services must not run it (they would compete needlessly)."""
    from dynastore.modules.gcp import gcp_module

    src = inspect.getsource(gcp_module.GCPModule.lifespan)
    assert "GcpLivenessReconciler" in src
    assert "_should_register_gcp_job_runner" in src


def test_lifespan_reconciler_start_uses_engine_property_not_raw_attr():
    """#778 regression — the reconciler-start gate added in #735 used raw
    ``self._engine`` which is *never assigned* in production: ``@engine.setter``
    has no callers (``grep -rn '.engine\\s*=' packages/core/src/`` is empty),
    so the raw attribute stays ``None`` forever. The DDL schema-init above
    the gate uses ``self.engine`` (the property with ``get_engine()`` fallback)
    and works; the reconciler gate used the raw attribute and silently
    skipped on every Cloud Run service since #735 shipped.

    Pin the gate to obtain the engine through the property so the
    ``get_engine()`` fallback applies — parity with the DDL section directly
    above it.
    """
    from dynastore.modules.gcp import gcp_module

    src = inspect.getsource(gcp_module.GCPModule.lifespan)
    start_marker = "Liveness reconciler"
    assert start_marker in src, (
        "Reconciler-start block lost its marker comment; rename only after "
        "deleting this regression test or updating the marker here."
    )
    start_idx = src.index(start_marker)
    # The reconciler-start block is short (~30 lines); 2500 chars covers it
    # plus a safety margin without pulling in the teardown branch.
    block = src[start_idx:start_idx + 2500]

    # Bug shape — raw attribute check — MUST NOT reappear.
    assert "self._engine is not None" not in block, (
        "The reconciler-start gate must NOT use raw ``self._engine``: the "
        "``@engine.setter`` is uncalled in production so the raw attribute "
        "stays ``None`` forever and the reconciler silently skips. "
        "Use ``self.engine`` (the property with ``get_engine()`` fallback) "
        "instead — same path the schema-init above already uses. See #778."
    )
    # Property path MUST be used to resolve the engine for the reconciler.
    assert "self.engine" in block, (
        "Reconciler-start must read the engine through ``self.engine`` "
        "(the property), not a raw attribute access. See #778."
    )


# --- #741 item 3: observability + reaper-race detection --------------------


def _summary_lines(caplog):
    """The per-pass structured summary lines emitted by ``_reconcile_once``."""
    return [
        r for r in caplog.records
        if r.getMessage().startswith("liveness_reconcile_pass")
    ]


@pytest.mark.asyncio
async def test_reconcile_once_logs_verdict_distribution_summary(monkeypatch, caplog):
    """Per-pass observability — one summary log line carries the verdict
    distribution. Operators need it to confirm the reconciler is actually
    winning the race against the 60s pg_cron reaper; without it the
    reconciler is a silent background loop."""
    from dynastore.modules.tasks import tasks_module
    from dynastore.modules.tasks.liveness import LivenessVerdict

    _patch_actions(monkeypatch)
    rows = [_row(), _row(), _row(), _row()]
    monkeypatch.setattr(
        tasks_module, "select_lapsed_gcp_tasks", AsyncMock(return_value=rows)
    )

    verdicts = iter([
        LivenessVerdict.ALIVE,
        LivenessVerdict.ALIVE,
        LivenessVerdict.DEAD,
        LivenessVerdict.UNKNOWN,
    ])

    class _RotatingProbe:
        runner_type = "gcp_cloud_run"

        def owns(self, owner_id):
            return True

        async def probe_liveness(self, task):
            return next(verdicts)

    monkeypatch.setattr(_reconciler_mod(), "resolve_probe", lambda owner_id: _RotatingProbe())
    rec = _make_reconciler(unknown_grace_seconds=0)  # UNKNOWN row takes the no-op branch

    with caplog.at_level("INFO", logger="dynastore.modules.gcp.liveness_reconciler"):
        await rec._reconcile_once()

    summary_lines = _summary_lines(caplog)
    assert summary_lines, "missing per-pass verdict-distribution summary log"
    msg = summary_lines[-1].getMessage()
    # Counts in the summary must reflect what was probed this pass.
    assert "ALIVE=2" in msg
    assert "DEAD=1" in msg
    assert "UNKNOWN=1" in msg


@pytest.mark.asyncio
async def test_reconcile_pass_summary_is_structured_keyvalue(monkeypatch, caplog):
    """#745 item 2 — the summary line follows the house ``<token> service=…
    key=value`` shape (same as ``db_pool_acquire``) so a GCP log-based metric
    can extract fields without a prometheus_client dep. The stable anchor
    fields ``service=``, ``scanned=`` and ``RACE_LOST=`` are ALWAYS present —
    even on an idle pass — so the metric filter and the race-loss extractor
    never silently lose their data point."""
    from dynastore.modules.tasks import tasks_module

    _patch_actions(monkeypatch)
    monkeypatch.setattr(
        tasks_module, "select_lapsed_gcp_tasks", AsyncMock(return_value=[])
    )
    rec = _make_reconciler()

    with caplog.at_level("INFO", logger="dynastore.modules.gcp.liveness_reconciler"):
        await rec._reconcile_once()

    summary_lines = _summary_lines(caplog)
    assert summary_lines, "idle pass must still emit a summary line"
    msg = summary_lines[-1].getMessage()
    assert msg.startswith("liveness_reconcile_pass ")
    assert "service=" in msg
    assert "scanned=0" in msg
    assert "RACE_LOST=0" in msg  # anchored even when zero


@pytest.mark.asyncio
async def test_reconcile_once_summary_counts_race_losses_separately(monkeypatch, caplog):
    """#745 item 1 — a row that probed ALIVE but whose conditional heartbeat
    matched 0 rows (the pg_cron reaper won the race) is still a truthful
    ``ALIVE`` verdict, but the summary must tally it under a DISTINCT
    ``RACE_LOST`` count. Otherwise ``ALIVE=3`` hides that 1 of those 3 never
    actually got its lease extended — and the race-loss rate is the signal
    for whether the reconciler interval needs tuning down."""
    from dynastore.modules.tasks import tasks_module
    from dynastore.modules.tasks.liveness import LivenessVerdict

    _patch_actions(monkeypatch)
    rows = [_row(), _row(), _row()]
    monkeypatch.setattr(
        tasks_module, "select_lapsed_gcp_tasks", AsyncMock(return_value=rows)
    )
    # All three probe ALIVE; the conditional heartbeat succeeds for the first
    # two and loses the race on the third.
    monkeypatch.setattr(
        tasks_module,
        "heartbeat_task_if_active",
        AsyncMock(side_effect=[True, True, False]),
    )
    _patch_probe(monkeypatch, LivenessVerdict.ALIVE)
    rec = _make_reconciler()

    with caplog.at_level("INFO", logger="dynastore.modules.gcp.liveness_reconciler"):
        await rec._reconcile_once()

    msg = _summary_lines(caplog)[-1].getMessage()
    assert "ALIVE=3" in msg, "the probe verdict count stays truthful"
    assert "RACE_LOST=1" in msg, "race losses must be tallied distinctly"


@pytest.mark.asyncio
async def test_reconcile_row_returns_outcome_with_race_lost_flag(monkeypatch):
    """``_reconcile_row`` returns a ``ReconcileOutcome(verdict, race_lost)`` so
    ``_reconcile_once`` can tally race losses without re-deriving them. An
    ALIVE row whose heartbeat matched carries ``race_lost=False``; one whose
    heartbeat lost the race carries ``race_lost=True`` — with the verdict
    still ALIVE either way."""
    from dynastore.modules.tasks import tasks_module
    from dynastore.modules.tasks.liveness import LivenessVerdict

    _patch_actions(monkeypatch)
    _patch_probe(monkeypatch, LivenessVerdict.ALIVE)
    rec = _make_reconciler()

    # Heartbeat succeeds → race not lost.
    monkeypatch.setattr(
        tasks_module, "heartbeat_task_if_active", AsyncMock(return_value=True)
    )
    won = await rec._reconcile_row(_row())
    assert won.verdict == LivenessVerdict.ALIVE
    assert won.race_lost is False

    # Heartbeat matched 0 rows → race lost, verdict still ALIVE.
    monkeypatch.setattr(
        tasks_module, "heartbeat_task_if_active", AsyncMock(return_value=False)
    )
    lost = await rec._reconcile_row(_row())
    assert lost.verdict == LivenessVerdict.ALIVE
    assert lost.race_lost is True


@pytest.mark.asyncio
async def test_reconcile_row_unmapped_owner_returns_none(monkeypatch):
    """An unmapped owner still returns ``None`` (not a ReconcileOutcome) so
    ``_reconcile_once`` tallies it as UNMAPPED, not as a verdict."""
    _patch_actions(monkeypatch)
    monkeypatch.setattr(_reconciler_mod(), "resolve_probe", lambda owner_id: None)
    rec = _make_reconciler()
    assert await rec._reconcile_row(_row(owner_id="dispatcher-pod-1")) is None


@pytest.mark.asyncio
async def test_alive_logs_warning_when_reaper_won_race(monkeypatch, caplog):
    """``heartbeat_task_if_active`` returns ``False`` when no row was updated —
    i.e. the row was no longer ACTIVE at UPDATE time. That is the accepted
    SELECT→probe→act race window from the #735 design: the pg_cron reaper
    flipped the row to PENDING between the reconciler's SELECT and its
    heartbeat. Surface it so operators can see when it happens and tune the
    reconciler interval down."""
    from dynastore.modules.tasks import tasks_module
    from dynastore.modules.tasks.liveness import LivenessVerdict

    _patch_actions(monkeypatch)
    monkeypatch.setattr(
        tasks_module,
        "heartbeat_task_if_active",
        AsyncMock(return_value=False),
    )
    _patch_probe(monkeypatch, LivenessVerdict.ALIVE)
    rec = _make_reconciler()

    with caplog.at_level("WARNING", logger="dynastore.modules.gcp.liveness_reconciler"):
        await rec._reconcile_row(_row())

    race_warnings = [
        r for r in caplog.records
        if r.levelname == "WARNING"
        and "reaper" in r.getMessage().lower()
        and "race" in r.getMessage().lower()
    ]
    assert race_warnings, (
        "reaper-race loss must log a WARNING — without it the race window "
        "is operationally invisible."
    )


# --- #750: terminal-verdict paths are race-guarded too ---------------------


@pytest.mark.asyncio
async def test_dead_race_lost_when_fail_task_skips(monkeypatch, caplog):
    """#750 — the DEAD path is owner-guarded. When ``fail_task`` matches 0
    rows (the row was reclaimed + re-dispatched between SELECT and write),
    ``_reconcile_row`` reports ``race_lost=True`` and logs a WARNING — it
    does NOT fail a task that is legitimately running again."""
    from dynastore.modules.tasks import tasks_module
    from dynastore.modules.tasks.liveness import LivenessVerdict

    _patch_actions(monkeypatch)
    monkeypatch.setattr(tasks_module, "fail_task", AsyncMock(return_value=False))
    _patch_probe(monkeypatch, LivenessVerdict.DEAD)
    rec = _make_reconciler()

    with caplog.at_level("WARNING", logger="dynastore.modules.gcp.liveness_reconciler"):
        outcome = await rec._reconcile_row(_row())

    assert outcome.verdict == LivenessVerdict.DEAD
    assert outcome.race_lost is True
    race_warnings = [
        r for r in caplog.records
        if r.levelname == "WARNING"
        and "reaper" in r.getMessage().lower()
        and "race" in r.getMessage().lower()
    ]
    assert race_warnings, "a lost DEAD-path race must log a WARNING"


@pytest.mark.asyncio
async def test_terminal_succeeded_race_lost_when_complete_task_skips(monkeypatch, caplog):
    """#750 — the TERMINAL_SUCCEEDED path is owner-guarded the same way. A
    0-row ``complete_task`` means the reaper won the race; report it instead
    of completing a fresh attempt out from under Cloud Run."""
    from dynastore.modules.tasks import tasks_module
    from dynastore.modules.tasks.liveness import LivenessVerdict

    _patch_actions(monkeypatch)
    monkeypatch.setattr(tasks_module, "complete_task", AsyncMock(return_value=False))
    _patch_probe(monkeypatch, LivenessVerdict.TERMINAL_SUCCEEDED)
    rec = _make_reconciler()

    with caplog.at_level("WARNING", logger="dynastore.modules.gcp.liveness_reconciler"):
        outcome = await rec._reconcile_row(_row(outputs={"result": "ok"}))

    assert outcome.verdict == LivenessVerdict.TERMINAL_SUCCEEDED
    assert outcome.race_lost is True
    race_warnings = [
        r for r in caplog.records
        if r.levelname == "WARNING"
        and "reaper" in r.getMessage().lower()
        and "race" in r.getMessage().lower()
    ]
    assert race_warnings, "a lost TERMINAL_SUCCEEDED-path race must log a WARNING"


@pytest.mark.asyncio
async def test_terminal_race_losses_counted_in_pass_summary(monkeypatch, caplog):
    """#750 — race losses on the terminal paths feed the same ``RACE_LOST``
    tally as the ALIVE path, so the per-pass summary line reflects every
    SELECT→probe→act race the reconciler lost, not just the ALIVE ones."""
    from dynastore.modules.tasks import tasks_module
    from dynastore.modules.tasks.liveness import LivenessVerdict

    _patch_actions(monkeypatch)
    monkeypatch.setattr(tasks_module, "fail_task", AsyncMock(return_value=False))
    monkeypatch.setattr(tasks_module, "complete_task", AsyncMock(return_value=False))
    rows = [_row(), _row()]
    monkeypatch.setattr(
        tasks_module, "select_lapsed_gcp_tasks", AsyncMock(return_value=rows)
    )
    verdicts = iter([LivenessVerdict.DEAD, LivenessVerdict.TERMINAL_SUCCEEDED])

    class _RotatingProbe:
        runner_type = "gcp_cloud_run"

        def owns(self, owner_id):
            return True

        async def probe_liveness(self, task):
            return next(verdicts)

    monkeypatch.setattr(_reconciler_mod(), "resolve_probe", lambda owner_id: _RotatingProbe())
    rec = _make_reconciler()

    with caplog.at_level("INFO", logger="dynastore.modules.gcp.liveness_reconciler"):
        await rec._reconcile_once()

    msg = _summary_lines(caplog)[-1].getMessage()
    assert "RACE_LOST=2" in msg, "both terminal-path race losses must be tallied"


# --- geoid#2819: non-locking staleness detection ----------------------------
#
# A row held by a zombie PG session (dead remote backend still holding its
# row lock) is silently skipped by the FOR UPDATE SKIP LOCKED scan on every
# pass. ``_check_staleness`` diffs a plain, non-locking SELECT against the
# task_ids the locking scan actually reached this pass, and after
# ``staleness_max_passes`` consecutive occurrences logs loudly and attempts a
# lock-free heal via the same probe + owner-guarded-write path as the primary
# scan / backstop.


def _staleness_warnings(caplog):
    return [
        r for r in caplog.records
        if "liveness_staleness_visible_unclaimable" in r.getMessage()
    ]


@pytest.mark.asyncio
async def test_staleness_streak_increments_but_no_warning_below_threshold(monkeypatch, caplog):
    from dynastore.modules.tasks import tasks_module

    actions = _patch_actions(monkeypatch)
    row = _row()
    monkeypatch.setattr(tasks_module, "select_stale_gcp_tasks", AsyncMock(return_value=[row]))
    rec = _make_reconciler(staleness_max_passes=3)

    with caplog.at_level("WARNING", logger="dynastore.modules.gcp.liveness_reconciler"):
        await rec._check_staleness(claimed_ids=set())

    assert rec._stale_streak[row["task_id"]] == 1
    assert not _staleness_warnings(caplog), "must not warn before crossing staleness_max_passes"
    actions.heartbeat_if_active.assert_not_awaited()
    actions.fail.assert_not_awaited()
    actions.complete.assert_not_awaited()


@pytest.mark.asyncio
async def test_staleness_warns_and_attempts_lock_free_heal_after_max_passes(monkeypatch, caplog):
    from dynastore.modules.tasks import tasks_module
    from dynastore.modules.tasks.liveness import LivenessVerdict

    actions = _patch_actions(monkeypatch)
    _patch_probe(monkeypatch, LivenessVerdict.ALIVE)
    row = _row()
    monkeypatch.setattr(tasks_module, "select_stale_gcp_tasks", AsyncMock(return_value=[row]))
    rec = _make_reconciler(staleness_max_passes=2)

    with caplog.at_level("WARNING", logger="dynastore.modules.gcp.liveness_reconciler"):
        await rec._check_staleness(claimed_ids=set())
        await rec._check_staleness(claimed_ids=set())

    warnings = _staleness_warnings(caplog)
    assert len(warnings) == 1, "loud warning fires exactly once on the pass that crosses the threshold"
    msg = warnings[0].getMessage()
    assert str(row["task_id"]) in msg
    assert "passes=2" in msg
    # Lock-free heal attempted via the shared _reconcile_row path (same
    # probe + owner-guarded-write path the primary scan / backstop use).
    actions.heartbeat_if_active.assert_awaited_once()


@pytest.mark.asyncio
async def test_staleness_streak_resets_when_row_claimed_by_locking_scan(monkeypatch):
    """A row the FOR UPDATE SKIP LOCKED scan reaches this pass is merely
    lapsed, not stuck behind a lock — its streak must reset, not accumulate
    toward a false-positive loud warning."""
    from dynastore.modules.tasks import tasks_module

    _patch_actions(monkeypatch)
    row = _row()
    monkeypatch.setattr(tasks_module, "select_stale_gcp_tasks", AsyncMock(return_value=[row]))
    rec = _make_reconciler(staleness_max_passes=3)

    await rec._check_staleness(claimed_ids=set())
    await rec._check_staleness(claimed_ids=set())
    assert rec._stale_streak[row["task_id"]] == 2

    await rec._check_staleness(claimed_ids={row["task_id"]})
    assert row["task_id"] not in rec._stale_streak


@pytest.mark.asyncio
async def test_staleness_streak_resets_when_row_disappears_from_scan(monkeypatch):
    """A row that stops appearing in the non-locking scan (healed, reclaimed,
    or no longer lapsed) must not keep an accumulated streak around — a
    fresh occurrence later starts counting from zero."""
    from dynastore.modules.tasks import tasks_module

    _patch_actions(monkeypatch)
    row = _row()
    select_stale = AsyncMock(return_value=[row])
    monkeypatch.setattr(tasks_module, "select_stale_gcp_tasks", select_stale)
    rec = _make_reconciler(staleness_max_passes=3)

    await rec._check_staleness(claimed_ids=set())
    assert rec._stale_streak[row["task_id"]] == 1

    select_stale.return_value = []
    await rec._check_staleness(claimed_ids=set())
    assert row["task_id"] not in rec._stale_streak


@pytest.mark.asyncio
async def test_staleness_heal_failure_does_not_raise_and_streak_survives(monkeypatch, caplog):
    """A probe/write exception during the lock-free heal attempt must not
    break the tick — the streak is left in place so the next pass retries."""
    from dynastore.modules.tasks import tasks_module

    _patch_actions(monkeypatch)
    row = _row()
    monkeypatch.setattr(tasks_module, "select_stale_gcp_tasks", AsyncMock(return_value=[row]))

    def _boom(owner_id):
        raise RuntimeError("probe resolution blew up")

    monkeypatch.setattr(_reconciler_mod(), "resolve_probe", _boom)
    rec = _make_reconciler(staleness_max_passes=1)

    with caplog.at_level("WARNING", logger="dynastore.modules.gcp.liveness_reconciler"):
        await rec._check_staleness(claimed_ids=set())  # must not raise

    fail_logs = [
        r for r in caplog.records if "lock-free heal attempt failed" in r.getMessage()
    ]
    assert fail_logs
    assert rec._stale_streak[row["task_id"]] == 1


@pytest.mark.asyncio
async def test_reconcile_once_threads_claimed_ids_into_staleness_check(monkeypatch):
    """``_reconcile_once`` must pass the primary scan's claimed task_ids into
    ``_check_staleness`` so a row genuinely reached this pass resets its
    streak instead of accumulating toward a false-positive loud warning."""
    from dynastore.modules.tasks import tasks_module
    from dynastore.modules.tasks.liveness import LivenessVerdict

    _patch_actions(monkeypatch)
    _patch_probe(monkeypatch, LivenessVerdict.ALIVE)
    row = _row()
    monkeypatch.setattr(tasks_module, "select_lapsed_gcp_tasks", AsyncMock(return_value=[row]))
    monkeypatch.setattr(tasks_module, "select_stale_gcp_tasks", AsyncMock(return_value=[row]))
    rec = _make_reconciler(staleness_max_passes=1)
    rec._stale_streak[row["task_id"]] = 5  # pretend a prior streak existed

    await rec._reconcile_once()

    assert row["task_id"] not in rec._stale_streak


@pytest.mark.asyncio
async def test_staleness_scan_failure_is_swallowed(monkeypatch, caplog):
    """A staleness-scan SELECT failure (e.g. transient DB error) must not
    break the reconciler tick — detection is best-effort."""
    from dynastore.modules.tasks import tasks_module

    _patch_actions(monkeypatch)
    monkeypatch.setattr(
        tasks_module, "select_stale_gcp_tasks",
        AsyncMock(side_effect=RuntimeError("db unavailable")),
    )
    rec = _make_reconciler()

    with caplog.at_level("WARNING", logger="dynastore.modules.gcp.liveness_reconciler"):
        await rec._check_staleness(claimed_ids=set())  # must not raise

    assert any("staleness scan failed" in r.getMessage() for r in caplog.records)
