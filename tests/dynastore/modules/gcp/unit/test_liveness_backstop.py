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

"""Unit tests for ``GcpLivenessBackstop`` (#2771).

Reproduces the leader-starvation condition — ``GcpLivenessReconciler`` is
``LEADER_ONLY`` and its pass silently never runs when the leader loop cannot
elect — and proves the RUN_EVERYWHERE backstop detects it and heals the
lapsed rows directly, without ever winning (or needing) leadership.

No real DB required: ``select_lapsed_gcp_tasks`` and the per-verdict action
helpers are monkeypatched, mirroring test_liveness_reconciler.py.
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


def _backstop_mod():
    from dynastore.modules.gcp import liveness_reconciler
    return liveness_reconciler


def _make_backstop(**kwargs):
    from dynastore.modules.gcp.liveness_reconciler import GcpLivenessBackstop

    defaults = dict(
        cadence_seconds=0.01,
        stale_multiplier=3.0,
        extend_visibility_seconds=300,
        unknown_grace_seconds=180,
    )
    defaults.update(kwargs)
    return GcpLivenessBackstop(**defaults)


def _row(*, owner_id="gcp_cloud_run_abc", runner_ref="projects/p/.../executions/e",
         lapsed_seconds=5.0):
    return {
        "task_id": uuid.uuid4(),
        "schema_name": "tasks",
        "task_type": "ingest",
        "owner_id": owner_id,
        "runner_ref": runner_ref,
        "started_at": datetime.now(timezone.utc) - timedelta(seconds=lapsed_seconds + 30),
        "outputs": None,
        "retry_count": 0,
        "max_retries": 3,
        "locked_until": datetime.now(timezone.utc) - timedelta(seconds=lapsed_seconds),
    }


class _AliveProbe:
    runner_type = "gcp_cloud_run"

    def owns(self, owner_id):
        return True

    async def probe_liveness(self, task):
        from dynastore.modules.tasks.liveness import LivenessVerdict
        return LivenessVerdict.ALIVE


def _patch_actions(monkeypatch):
    from dynastore.modules.tasks import tasks_module

    hb_if_active = AsyncMock(return_value=True)
    fail = AsyncMock(return_value=True)
    complete = AsyncMock(return_value=True)
    monkeypatch.setattr(tasks_module, "heartbeat_task_if_active", hb_if_active)
    monkeypatch.setattr(tasks_module, "fail_task", fail)
    monkeypatch.setattr(tasks_module, "complete_task", complete)
    return SimpleNamespace(heartbeat_if_active=hb_if_active, fail=fail, complete=complete)


def _patch_lapsed_rows(monkeypatch, rows):
    from dynastore.modules.tasks import tasks_module
    monkeypatch.setattr(
        tasks_module, "select_lapsed_gcp_tasks", AsyncMock(return_value=rows)
    )


def _patch_interval(monkeypatch, interval_seconds=20.0):
    """Pin the primary reconciler's configured interval used to derive the
    backstop's staleness bar, independent of any live global config state."""
    monkeypatch.setattr(
        _backstop_mod(),
        "resolve_leadership_config",
        lambda: (30, 600, interval_seconds, 300, 180),
    )


def _summary_lines(caplog, token):
    return [r for r in caplog.records if r.getMessage().startswith(token)]


# --- construction / policy fields -------------------------------------------


def test_backstop_is_run_everywhere():
    from dynastore.tools.background_service import Leadership, PodPolicy

    backstop = _make_backstop()
    assert backstop.name == "gcp_liveness_backstop"
    assert backstop.leadership is Leadership.RUN_EVERYWHERE
    assert backstop.pod_policy is PodPolicy.SKIP_EPHEMERAL


# --- detection: healthy leader (no starvation) → no-op ----------------------


@pytest.mark.asyncio
async def test_noop_when_nothing_lapsed(monkeypatch, caplog):
    _patch_interval(monkeypatch)
    actions = _patch_actions(monkeypatch)
    _patch_lapsed_rows(monkeypatch, [])
    backstop = _make_backstop()

    with caplog.at_level("WARNING", logger="dynastore.modules.gcp.liveness_reconciler"):
        await backstop.tick(SimpleNamespace(engine=object()))

    actions.heartbeat_if_active.assert_not_awaited()
    assert not _summary_lines(caplog, "liveness_backstop_starvation_detected")


@pytest.mark.asyncio
async def test_noop_when_lapsed_but_within_healthy_jitter(monkeypatch, caplog):
    """A row lapsed by less than stale_multiplier * interval is ordinary
    cadence jitter between two healthy reconciler passes — must NOT trigger
    the backstop (it would otherwise duplicate the leader's own work on
    every pod, every tick)."""
    _patch_interval(monkeypatch, interval_seconds=20.0)
    actions = _patch_actions(monkeypatch)
    monkeypatch.setattr(_backstop_mod(), "resolve_probe", lambda owner_id: _AliveProbe())
    # Lapsed by 10s — well under 3 * 20s = 60s staleness bar.
    _patch_lapsed_rows(monkeypatch, [_row(lapsed_seconds=10.0)])
    backstop = _make_backstop(stale_multiplier=3.0)

    with caplog.at_level("WARNING", logger="dynastore.modules.gcp.liveness_reconciler"):
        await backstop.tick(SimpleNamespace(engine=object()))

    actions.heartbeat_if_active.assert_not_awaited()
    assert not _summary_lines(caplog, "liveness_backstop_starvation_detected")


# --- recovery: leader starvation → backstop heals ---------------------------


@pytest.mark.asyncio
async def test_starved_leader_rows_are_healed_by_backstop(monkeypatch, caplog):
    """Reproduces #2771: rows lapsed far beyond the primary reconciler's
    cadence (simulating a leader loop that never elects, e.g. under #2333
    DB-pool churn) are detected as starved AND reconciled directly by the
    RUN_EVERYWHERE backstop — with no leadership acquired at all."""
    _patch_interval(monkeypatch, interval_seconds=20.0)
    actions = _patch_actions(monkeypatch)
    monkeypatch.setattr(_backstop_mod(), "resolve_probe", lambda owner_id: _AliveProbe())
    # Lapsed by 10 minutes — far past 3 * 20s = 60s staleness bar; a healthy
    # reconciler ticking every ~20s would never let a row age this far.
    rows = [_row(lapsed_seconds=600.0), _row(lapsed_seconds=650.0)]
    _patch_lapsed_rows(monkeypatch, rows)
    backstop = _make_backstop(stale_multiplier=3.0)

    with caplog.at_level("INFO", logger="dynastore.modules.gcp.liveness_reconciler"):
        await backstop.tick(SimpleNamespace(engine=object()))

    # Both rows healed (ALIVE → lease extended) despite no leader ever
    # having run GcpLivenessReconciler's own tick.
    assert actions.heartbeat_if_active.await_count == 2

    starvation_lines = _summary_lines(caplog, "liveness_backstop_starvation_detected")
    assert starvation_lines, "starvation must be surfaced, not silent (#2771)"
    assert "starved=2" in starvation_lines[-1].getMessage()

    pass_lines = _summary_lines(caplog, "liveness_backstop_pass")
    assert pass_lines
    assert "healed=2" in pass_lines[-1].getMessage()


@pytest.mark.asyncio
async def test_starvation_triggers_reconcile_of_all_lapsed_rows_not_only_stale_ones(
    monkeypatch, caplog,
):
    """Once starvation is confirmed for the pass, every currently lapsed row
    is reconciled — not just the ones that individually tripped detection —
    so a freshly-lapsed row doesn't wait for its own staleness bar once the
    backstop already knows the primary path is down."""
    _patch_interval(monkeypatch, interval_seconds=20.0)
    actions = _patch_actions(monkeypatch)
    monkeypatch.setattr(_backstop_mod(), "resolve_probe", lambda owner_id: _AliveProbe())
    rows = [_row(lapsed_seconds=600.0), _row(lapsed_seconds=5.0)]
    _patch_lapsed_rows(monkeypatch, rows)
    backstop = _make_backstop(stale_multiplier=3.0)

    await backstop.tick(SimpleNamespace(engine=object()))

    assert actions.heartbeat_if_active.await_count == 2


@pytest.mark.asyncio
async def test_one_bad_row_does_not_stop_the_rest(monkeypatch, caplog):
    _patch_interval(monkeypatch, interval_seconds=20.0)
    actions = _patch_actions(monkeypatch)

    calls = {"n": 0}

    class _FlakyProbe:
        runner_type = "gcp_cloud_run"

        def owns(self, owner_id):
            return True

        async def probe_liveness(self, task):
            from dynastore.modules.tasks.liveness import LivenessVerdict
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("probe blew up")
            return LivenessVerdict.ALIVE

    monkeypatch.setattr(_backstop_mod(), "resolve_probe", lambda owner_id: _FlakyProbe())
    rows = [_row(lapsed_seconds=600.0) for _ in range(3)]
    _patch_lapsed_rows(monkeypatch, rows)
    backstop = _make_backstop(stale_multiplier=3.0)

    await backstop.tick(SimpleNamespace(engine=object()))

    # Two good rows still healed despite the middle one raising.
    assert actions.heartbeat_if_active.await_count == 2


@pytest.mark.asyncio
async def test_tick_swallows_scan_failure(monkeypatch, caplog):
    """A SELECT failure (e.g. DB unavailable) must not raise into the
    BackgroundSupervisor — same fail-soft contract as PeriodicService._safe_tick
    provides for RUN_EVERYWHERE loops."""
    from dynastore.modules.tasks import tasks_module

    _patch_interval(monkeypatch)
    monkeypatch.setattr(
        tasks_module, "select_lapsed_gcp_tasks",
        AsyncMock(side_effect=RuntimeError("db unavailable")),
    )
    backstop = _make_backstop()

    with caplog.at_level("ERROR", logger="dynastore.modules.gcp.liveness_reconciler"):
        await backstop.tick(SimpleNamespace(engine=object()))  # must not raise

    assert any(r.levelname == "ERROR" for r in caplog.records)


# --- wiring: registered alongside the primary reconciler --------------------


def test_lifespan_registers_backstop_alongside_reconciler():
    """The backstop must be wired into the same gated registration block as
    the LEADER_ONLY reconciler it backs up — see GCPModule.lifespan."""
    from dynastore.modules.gcp import gcp_module

    src = inspect.getsource(gcp_module.GCPModule.lifespan)
    assert "GcpLivenessBackstop" in src
    assert "_should_register_gcp_job_runner" in src
