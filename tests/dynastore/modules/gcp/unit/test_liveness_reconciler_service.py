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

"""Unit tests for ``GcpLivenessReconciler`` as a ``PeriodicService``.

Mirrors tests/dynastore/modules/tasks/unit/test_task_services.py. Verifies:

1. Policy fields: name, leadership, pod_policy, cadence_seconds, lock_key shape.
2. tick() delegates to _reconcile_once() exactly once per call.
3. tick() is fail-soft: an exception from _reconcile_once() is logged and
   swallowed rather than propagated to the supervisor.

No real DB required — _reconcile_once is monkeypatched throughout.
"""
from __future__ import annotations

import asyncio

import pytest

from dynastore.tools.background_service import Leadership, PodPolicy, ServiceContext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def disable_managed_eventing():
    """Neutralize the DB-bound autouse fixture from gcp/conftest.py."""
    return None


def _ctx(*, is_ephemeral: bool = False) -> ServiceContext:
    return ServiceContext(
        engine=object(),
        shutdown=asyncio.Event(),
        is_ephemeral=is_ephemeral,
        name="test-svc",
    )


def _make_service(**kwargs):
    from dynastore.modules.gcp.liveness_reconciler import GcpLivenessReconciler

    defaults: dict = dict(
        interval_seconds=20.0,
        extend_visibility_seconds=300,
        unknown_grace_seconds=180,
    )
    defaults.update(kwargs)
    return GcpLivenessReconciler(**defaults)


# ---------------------------------------------------------------------------
# Policy fields
# ---------------------------------------------------------------------------


class TestGcpLivenessReconcilerPolicyFields:
    def test_name(self):
        svc = _make_service()
        assert svc.name == "gcp_liveness_reconciler"

    def test_leadership_is_leader_only(self):
        svc = _make_service()
        assert svc.leadership is Leadership.LEADER_ONLY

    def test_pod_policy_is_skip_ephemeral(self):
        svc = _make_service()
        assert svc.pod_policy is PodPolicy.SKIP_EPHEMERAL

    def test_cadence_seconds_matches_interval_seconds(self):
        svc = _make_service(interval_seconds=45.0)
        assert svc.cadence_seconds == 45.0

    def test_cadence_seconds_default(self):
        svc = _make_service()
        assert svc.cadence_seconds == 20.0

    def test_lock_key_is_service_scoped_string(self, monkeypatch):
        """lock_key must be a string of the form 'gcp-liveness-reconciler:<service>'
        so a rolling deploy never elects two leaders for the same service."""
        import dynastore.modules.db_config.instance as _inst

        monkeypatch.setattr(_inst, "get_service_name", lambda: "catalog")
        from dynastore.modules.gcp.liveness_reconciler import GcpLivenessReconciler

        svc = GcpLivenessReconciler()
        assert isinstance(svc.lock_key, str)
        assert svc.lock_key == "gcp-liveness-reconciler:catalog"

    def test_lock_key_falls_back_to_unknown_when_no_service_name(self, monkeypatch):
        import dynastore.modules.db_config.instance as _inst

        monkeypatch.setattr(_inst, "get_service_name", lambda: None)
        from dynastore.modules.gcp.liveness_reconciler import GcpLivenessReconciler

        svc = GcpLivenessReconciler()
        assert svc.lock_key == "gcp-liveness-reconciler:unknown"


# ---------------------------------------------------------------------------
# tick() delegation
# ---------------------------------------------------------------------------


class TestGcpLivenessReconcilerTick:
    @pytest.mark.asyncio
    async def test_tick_delegates_to_reconcile_once(self, monkeypatch):
        """tick() must call _reconcile_once() exactly once per invocation."""
        from dynastore.modules.gcp.liveness_reconciler import GcpLivenessReconciler

        calls: list = []

        svc = GcpLivenessReconciler()

        async def _fake_once():
            calls.append(True)

        monkeypatch.setattr(svc, "_reconcile_once", _fake_once)

        ctx = _ctx()
        await svc.tick(ctx)

        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_tick_threads_engine_from_ctx(self, monkeypatch):
        """tick() must update self._engine from ctx.engine before calling
        _reconcile_once so the reconciler always uses the supervisor-provided
        engine, not the constructor-time one."""
        from dynastore.modules.gcp.liveness_reconciler import GcpLivenessReconciler

        sentinel = object()
        seen_engine: list = []

        svc = GcpLivenessReconciler(engine=None)

        async def _fake_once():
            seen_engine.append(svc._engine)

        monkeypatch.setattr(svc, "_reconcile_once", _fake_once)

        ctx = ServiceContext(
            engine=sentinel,
            shutdown=asyncio.Event(),
            is_ephemeral=False,
            name="test-svc",
        )
        await svc.tick(ctx)

        assert len(seen_engine) == 1
        assert seen_engine[0] is sentinel

    @pytest.mark.asyncio
    async def test_tick_is_fail_soft(self, monkeypatch):
        """An exception from _reconcile_once must not propagate out of tick() —
        the supervisor must remain alive for the next cadence tick."""
        from dynastore.modules.gcp.liveness_reconciler import GcpLivenessReconciler

        svc = GcpLivenessReconciler()

        async def _boom():
            raise RuntimeError("reconcile blew up")

        monkeypatch.setattr(svc, "_reconcile_once", _boom)

        # Must not raise.
        await svc.tick(_ctx())

    @pytest.mark.asyncio
    async def test_tick_fail_soft_logs_error(self, monkeypatch, caplog):
        """When _reconcile_once raises, tick() must log at ERROR level so the
        failure is operationally visible despite being swallowed."""
        import logging
        from dynastore.modules.gcp.liveness_reconciler import GcpLivenessReconciler

        svc = GcpLivenessReconciler()

        async def _boom():
            raise RuntimeError("injected failure")

        monkeypatch.setattr(svc, "_reconcile_once", _boom)

        with caplog.at_level(logging.ERROR, logger="dynastore.modules.gcp.liveness_reconciler"):
            await svc.tick(_ctx())

        error_records = [r for r in caplog.records if r.levelname == "ERROR"]
        assert error_records, "tick() must log at ERROR when _reconcile_once raises"
