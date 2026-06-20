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

"""Unit tests for the 5 new BackgroundService wrappers (issue #2279).

Verifies:
1. Each service declares the expected name / leadership / pod_policy.
2. Periodic services' tick() delegates to the underlying work function.

No real DB — all underlying functions are monkeypatched.
"""
from __future__ import annotations

import asyncio

import pytest

from dynastore.tools.background_service import Leadership, PodPolicy, ServiceContext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ctx(*, is_ephemeral: bool = False) -> ServiceContext:
    return ServiceContext(
        engine=object(),
        shutdown=asyncio.Event(),
        is_ephemeral=is_ephemeral,
        name="test-svc",
    )


# ---------------------------------------------------------------------------
# QueueListenerService
# ---------------------------------------------------------------------------

class TestQueueListenerService:
    def test_policy_fields(self):
        from dynastore.modules.tasks.queue import QueueListenerService

        svc = QueueListenerService(poll_timeout=5.0)
        assert svc.name == "queue_listener"
        assert svc.leadership is Leadership.RUN_EVERYWHERE
        assert svc.pod_policy is PodPolicy.SKIP_EPHEMERAL
        assert svc.lock_key is None

    @pytest.mark.asyncio
    async def test_run_delegates_to_start_queue_listener(self, monkeypatch):
        import dynastore.modules.tasks.queue as q_mod
        from dynastore.modules.tasks.queue import QueueListenerService

        calls: list = []

        async def _fake_listener(engine, shutdown, poll_timeout):
            calls.append({"engine": engine, "poll_timeout": poll_timeout})

        monkeypatch.setattr(q_mod, "start_queue_listener", _fake_listener)

        svc = QueueListenerService(poll_timeout=7.5)
        ctx = _ctx()
        await svc.run(ctx)

        assert len(calls) == 1
        assert calls[0]["engine"] is ctx.engine
        assert calls[0]["poll_timeout"] == 7.5


# ---------------------------------------------------------------------------
# DispatcherService
# ---------------------------------------------------------------------------

class TestDispatcherService:
    def test_policy_fields(self):
        from dynastore.modules.tasks.dispatcher import DispatcherService

        svc = DispatcherService()
        assert svc.name == "dispatcher"
        assert svc.leadership is Leadership.RUN_EVERYWHERE
        assert svc.pod_policy is PodPolicy.SKIP_EPHEMERAL
        assert svc.lock_key is None

    @pytest.mark.asyncio
    async def test_run_delegates_to_run_dispatcher(self, monkeypatch):
        import dynastore.modules.tasks.dispatcher as d_mod
        from dynastore.modules.tasks.dispatcher import DispatcherService

        calls: list = []

        async def _fake_dispatcher(engine, schema, shutdown):
            calls.append({"engine": engine, "schema": schema})

        monkeypatch.setattr(d_mod, "run_dispatcher", _fake_dispatcher)

        svc = DispatcherService()
        ctx = _ctx()
        await svc.run(ctx)

        assert len(calls) == 1
        assert calls[0]["engine"] is ctx.engine
        assert calls[0]["schema"] is None


# ---------------------------------------------------------------------------
# StuckPendingWarnerService
# ---------------------------------------------------------------------------

class TestStuckPendingWarnerService:
    def test_policy_fields(self):
        from dynastore.modules.tasks.tasks_module import StuckPendingWarnerService

        svc = StuckPendingWarnerService(schema="tasks")
        assert svc.name == "stuck_pending_warner"
        assert svc.leadership is Leadership.RUN_EVERYWHERE
        assert svc.pod_policy is PodPolicy.SKIP_EPHEMERAL
        assert svc.lock_key is None

    def test_cadence_seconds_set_from_interval_s(self):
        from dynastore.modules.tasks.tasks_module import StuckPendingWarnerService

        svc = StuckPendingWarnerService(schema="tasks", interval_s=90.0)
        assert svc.cadence_seconds == 90.0

    @pytest.mark.asyncio
    async def test_tick_executes_scan_and_logs(self, monkeypatch):
        import contextlib
        import dynastore.modules.tasks.tasks_module as tm

        rows = [
            {"task_id": "abc", "task_type": "dummy", "schema_name": "tasks", "inputs": None, "age_s": 700.0}
        ]
        emit_calls: list = []
        redispatch_calls: list = []

        @contextlib.asynccontextmanager
        async def _fake_mt(_engine):
            yield object()

        async def _fake_execute(self_q, _conn, **kw):
            return rows

        async def _fake_emit(r):
            emit_calls.append(r)

        async def _fake_redispatch(engine, r):
            redispatch_calls.append(r)

        monkeypatch.setattr(tm, "managed_transaction", _fake_mt)
        monkeypatch.setattr(tm.DQLQuery, "execute", _fake_execute)
        monkeypatch.setattr(tm, "_emit_stuck_pending_logs", _fake_emit)
        monkeypatch.setattr(tm, "_redispatch_stuck_rows", _fake_redispatch)

        from dynastore.modules.tasks.tasks_module import StuckPendingWarnerService

        svc = StuckPendingWarnerService(schema="tasks")
        await svc.tick(_ctx())

        assert emit_calls == [rows]
        assert redispatch_calls == [rows]


# ---------------------------------------------------------------------------
# ProactiveSweepService
# ---------------------------------------------------------------------------

class TestProactiveSweepService:
    def test_policy_fields(self):
        from dynastore.modules.tasks.tasks_module import ProactiveSweepService

        svc = ProactiveSweepService(schema="tasks")
        assert svc.name == "proactive_capability_sweep"
        assert svc.leadership is Leadership.RUN_EVERYWHERE
        assert svc.pod_policy is PodPolicy.SKIP_EPHEMERAL
        assert svc.lock_key is None

    def test_cadence_seconds_set_from_interval_s(self):
        from dynastore.modules.tasks.tasks_module import ProactiveSweepService

        svc = ProactiveSweepService(schema="tasks", interval_s=120.0)
        assert svc.cadence_seconds == 120.0

    @pytest.mark.asyncio
    async def test_tick_calls_backstop_and_wedged_sweep(self, monkeypatch):
        import dynastore.modules.tasks.tasks_module as tm

        backstop_calls: list = []
        wedged_calls: list = []

        # Empty capability map so the inner for-loop is a no-op.
        monkeypatch.setattr(
            "dynastore.modules.tasks.capability_oracle.TASK_TYPE_CAPABILITY_INPUTS_KEY",
            {},
        )

        async def _fake_backstop(engine, schema, *, ttl_grace_seconds, min_age_s):
            backstop_calls.append({"schema": schema})

        async def _fake_wedged(engine, min_age_s):
            wedged_calls.append({"min_age_s": min_age_s})
            return 0

        monkeypatch.setattr(tm, "_run_mandatory_backstop_pass", _fake_backstop)
        monkeypatch.setattr(tm, "sweep_wedged_provisioning_catalogs", _fake_wedged)

        from dynastore.modules.tasks.tasks_module import ProactiveSweepService

        svc = ProactiveSweepService(schema="tasks")
        await svc.tick(_ctx())

        assert len(backstop_calls) == 1
        assert backstop_calls[0]["schema"] == "tasks"
        assert len(wedged_calls) == 1


# ---------------------------------------------------------------------------
# CapabilityPublisherService
# ---------------------------------------------------------------------------

class TestCapabilityPublisherService:
    def test_policy_fields(self):
        from dynastore.modules.tasks.capability_publisher import CapabilityPublisherService

        svc = CapabilityPublisherService(ttl_seconds=60.0, refresh_seconds=25.0)
        assert svc.name == "capability_publisher"
        assert svc.leadership is Leadership.RUN_EVERYWHERE
        assert svc.pod_policy is PodPolicy.SKIP_EPHEMERAL
        assert svc.lock_key is None
        assert svc.cadence_seconds == 25.0

    @pytest.mark.asyncio
    async def test_tick_collects_and_refreshes(self, monkeypatch):
        import dynastore.modules.tasks.capability_publisher as cp_mod

        collect_calls: list = []
        refresh_calls: list = []

        def _fake_collect():
            collect_calls.append(True)
            return ["cap_a", "cap_b"]

        async def _fake_refresh(caps, *, ttl_seconds):
            refresh_calls.append({"caps": caps, "ttl": ttl_seconds})
            return len(caps)

        monkeypatch.setattr(cp_mod, "_collect_local_capabilities", _fake_collect)
        monkeypatch.setattr(cp_mod, "_refresh_once", _fake_refresh)

        from dynastore.modules.tasks.capability_publisher import CapabilityPublisherService

        svc = CapabilityPublisherService(ttl_seconds=45.0, refresh_seconds=20.0)
        await svc.tick(_ctx())

        assert len(collect_calls) == 1
        assert len(refresh_calls) == 1
        assert refresh_calls[0]["caps"] == ["cap_a", "cap_b"]
        assert refresh_calls[0]["ttl"] == 45.0

    @pytest.mark.asyncio
    async def test_tick_swallows_exceptions(self, monkeypatch):
        """A failing _refresh_once must not propagate — tick() must be fail-soft."""
        import dynastore.modules.tasks.capability_publisher as cp_mod

        def _fake_collect():
            return ["cap_a"]

        async def _fail_refresh(caps, *, ttl_seconds):
            raise RuntimeError("cache down")

        monkeypatch.setattr(cp_mod, "_collect_local_capabilities", _fake_collect)
        monkeypatch.setattr(cp_mod, "_refresh_once", _fail_refresh)

        from dynastore.modules.tasks.capability_publisher import CapabilityPublisherService

        svc = CapabilityPublisherService()
        # Must not raise.
        await svc.tick(_ctx())
