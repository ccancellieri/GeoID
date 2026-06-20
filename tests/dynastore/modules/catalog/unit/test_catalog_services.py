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

"""Policy assertions for the 4 catalog/DB background services.

Verifies that each service declares the expected BackgroundService policy
(name, leadership, pod_policy) and preserves its exact advisory lock-key
constant so rolling deploys elect the same leader, not two independent ones.

These tests are pure-unit (no DB, no executor) — they only construct the
service and inspect its class/instance attributes.
"""
from __future__ import annotations

import pytest

from dynastore.tools.background_service import Leadership, PeriodicService, PodPolicy


# ---------------------------------------------------------------------------
# MaintenanceSupervisor
# ---------------------------------------------------------------------------


class TestMaintenanceSupervisorPolicy:
    def _make(self):
        from dynastore.modules.catalog.maintenance_supervisor import (
            MaintenanceSupervisor,
            _SUPERVISOR_ADVISORY_LOCK_KEY,
        )
        svc = MaintenanceSupervisor({"hard_cap": 5})
        return svc, _SUPERVISOR_ADVISORY_LOCK_KEY

    def test_is_periodic_service(self):
        svc, _ = self._make()
        assert isinstance(svc, PeriodicService)

    def test_name(self):
        svc, _ = self._make()
        assert svc.name == "maintenance_supervisor"

    def test_leadership(self):
        svc, _ = self._make()
        assert svc.leadership is Leadership.LEADER_ONLY

    def test_pod_policy(self):
        svc, _ = self._make()
        assert svc.pod_policy is PodPolicy.SKIP_EPHEMERAL

    def test_lock_key_preserved(self):
        """lock_key must be the verbatim constant 0x4D41494E_54454E41.

        Changing this would let an old pod and a new pod both win the advisory
        lock during a rolling deploy, double-running maintenance jobs.
        """
        svc, constant = self._make()
        assert svc.lock_key == constant
        assert svc.lock_key == 0x4D41494E_54454E41

    def test_cadence_is_60s(self):
        svc, _ = self._make()
        assert svc.cadence_seconds == 60.0

    @pytest.mark.asyncio
    async def test_tick_calls_run_once(self, monkeypatch):
        """tick() must delegate to run_once()."""
        from dynastore.modules.catalog.maintenance_supervisor import MaintenanceSupervisor
        from dynastore.tools.background_service import ServiceContext
        import asyncio

        svc = MaintenanceSupervisor({"hard_cap": 5})
        called = []

        async def _fake_run_once():
            called.append(True)

        monkeypatch.setattr(svc, "run_once", _fake_run_once)
        ctx = ServiceContext(
            engine=None,
            shutdown=asyncio.Event(),
            is_ephemeral=False,
            name="test",
        )
        await svc.tick(ctx)
        assert called == [True]


# ---------------------------------------------------------------------------
# SoftDeleteReaper
# ---------------------------------------------------------------------------


class TestSoftDeleteReaperPolicy:
    def _make(self, interval: float = 3600.0):
        from dynastore.modules.catalog.soft_delete_reaper import (
            SoftDeleteReaper,
            SoftDeleteReaperConfig,
            _REAPER_ADVISORY_LOCK_KEY,
        )
        cfg = SoftDeleteReaperConfig(reaper_interval_seconds=interval)
        svc = SoftDeleteReaper(cfg)
        return svc, _REAPER_ADVISORY_LOCK_KEY

    def test_is_periodic_service(self):
        svc, _ = self._make()
        assert isinstance(svc, PeriodicService)

    def test_name(self):
        svc, _ = self._make()
        assert svc.name == "soft_delete_reaper"

    def test_leadership(self):
        svc, _ = self._make()
        assert svc.leadership is Leadership.LEADER_ONLY

    def test_pod_policy(self):
        svc, _ = self._make()
        assert svc.pod_policy is PodPolicy.SKIP_EPHEMERAL

    def test_lock_key_preserved(self):
        """lock_key must be the verbatim constant 0x5D3A7E1F_C2B84961."""
        svc, constant = self._make()
        assert svc.lock_key == constant
        assert svc.lock_key == 0x5D3A7E1F_C2B84961

    def test_cadence_follows_config(self):
        svc, _ = self._make(interval=7200.0)
        assert svc.cadence_seconds == 7200.0

    @pytest.mark.asyncio
    async def test_tick_calls_run_once(self, monkeypatch):
        from dynastore.modules.catalog.soft_delete_reaper import (
            SoftDeleteReaper,
            SoftDeleteReaperConfig,
        )
        from dynastore.tools.background_service import ServiceContext
        import asyncio

        svc = SoftDeleteReaper(SoftDeleteReaperConfig())
        called = []

        async def _fake_run_once():
            called.append(True)

        monkeypatch.setattr(svc, "run_once", _fake_run_once)
        ctx = ServiceContext(
            engine=None,
            shutdown=asyncio.Event(),
            is_ephemeral=False,
            name="test",
        )
        await svc.tick(ctx)
        assert called == [True]


# ---------------------------------------------------------------------------
# LifecycleReaper
# ---------------------------------------------------------------------------


class TestLifecycleReaperPolicy:
    def _make(self, interval: float = 300.0):
        from dynastore.modules.catalog.lifecycle_reaper import (
            LifecycleReaper,
            LifecycleReaperConfig,
            _LIFECYCLE_REAPER_ADVISORY_LOCK_KEY,
        )
        cfg = LifecycleReaperConfig(reaper_interval_seconds=interval)
        svc = LifecycleReaper(cfg)
        return svc, _LIFECYCLE_REAPER_ADVISORY_LOCK_KEY

    def test_is_periodic_service(self):
        svc, _ = self._make()
        assert isinstance(svc, PeriodicService)

    def test_name(self):
        svc, _ = self._make()
        assert svc.name == "lifecycle_reaper"

    def test_leadership(self):
        svc, _ = self._make()
        assert svc.leadership is Leadership.LEADER_ONLY

    def test_pod_policy(self):
        svc, _ = self._make()
        assert svc.pod_policy is PodPolicy.SKIP_EPHEMERAL

    def test_lock_key_preserved(self):
        """lock_key must be the verbatim constant 0x4C494643_52454150."""
        svc, constant = self._make()
        assert svc.lock_key == constant
        assert svc.lock_key == 0x4C494643_52454150

    def test_cadence_follows_config(self):
        svc, _ = self._make(interval=600.0)
        assert svc.cadence_seconds == 600.0

    @pytest.mark.asyncio
    async def test_tick_calls_run_once(self, monkeypatch):
        from dynastore.modules.catalog.lifecycle_reaper import (
            LifecycleReaper,
            LifecycleReaperConfig,
        )
        from dynastore.tools.background_service import ServiceContext
        import asyncio

        svc = LifecycleReaper(LifecycleReaperConfig())
        called = []

        async def _fake_run_once():
            called.append(True)

        monkeypatch.setattr(svc, "run_once", _fake_run_once)
        ctx = ServiceContext(
            engine=None,
            shutdown=asyncio.Event(),
            is_ephemeral=False,
            name="test",
        )
        await svc.tick(ctx)
        assert called == [True]


# ---------------------------------------------------------------------------
# DbContentionMonitor
# ---------------------------------------------------------------------------


class TestDbContentionMonitorPolicy:
    def _make(self, interval: int = 30):
        from dynastore.modules.db.db_contention_monitor import (
            DbContentionMonitor,
            DbContentionMonitorConfig,
            _CONTENTION_MONITOR_LOCK_KEY,
        )
        cfg = DbContentionMonitorConfig(interval_seconds=interval)
        svc = DbContentionMonitor(cfg)
        return svc, _CONTENTION_MONITOR_LOCK_KEY

    def test_is_periodic_service(self):
        svc, _ = self._make()
        assert isinstance(svc, PeriodicService)

    def test_name(self):
        svc, _ = self._make()
        assert svc.name == "db_contention_monitor"

    def test_leadership(self):
        svc, _ = self._make()
        assert svc.leadership is Leadership.LEADER_ONLY

    def test_pod_policy(self):
        svc, _ = self._make()
        assert svc.pod_policy is PodPolicy.SKIP_EPHEMERAL

    def test_lock_key_preserved(self):
        """lock_key must be the verbatim constant 0x4C4F434B_4D4F4E49."""
        svc, constant = self._make()
        assert svc.lock_key == constant
        assert svc.lock_key == 0x4C4F434B_4D4F4E49

    def test_cadence_follows_config(self):
        svc, _ = self._make(interval=60)
        assert svc.cadence_seconds == 60.0

    @pytest.mark.asyncio
    async def test_tick_calls_run_once(self, monkeypatch):
        from dynastore.modules.db.db_contention_monitor import (
            DbContentionMonitor,
            DbContentionMonitorConfig,
        )
        from dynastore.tools.background_service import ServiceContext
        import asyncio

        svc = DbContentionMonitor(DbContentionMonitorConfig())
        called = []

        async def _fake_run_once():
            called.append(True)
            return None

        monkeypatch.setattr(svc, "run_once", _fake_run_once)
        ctx = ServiceContext(
            engine=None,
            shutdown=asyncio.Event(),
            is_ephemeral=False,
            name="test",
        )
        await svc.tick(ctx)
        assert called == [True]


# ---------------------------------------------------------------------------
# Cross-service: all lock keys are distinct
# ---------------------------------------------------------------------------


def test_all_advisory_lock_keys_are_unique():
    """All 4 services must use distinct advisory lock constants.

    A collision would let two different services race for the same pg advisory
    lock, causing one to be silently starved whenever the other holds the lock.
    """
    from dynastore.modules.catalog.maintenance_supervisor import (
        _SUPERVISOR_ADVISORY_LOCK_KEY,
    )
    from dynastore.modules.catalog.soft_delete_reaper import _REAPER_ADVISORY_LOCK_KEY
    from dynastore.modules.catalog.lifecycle_reaper import (
        _LIFECYCLE_REAPER_ADVISORY_LOCK_KEY,
    )
    from dynastore.modules.db.db_contention_monitor import _CONTENTION_MONITOR_LOCK_KEY

    keys = [
        _SUPERVISOR_ADVISORY_LOCK_KEY,
        _REAPER_ADVISORY_LOCK_KEY,
        _LIFECYCLE_REAPER_ADVISORY_LOCK_KEY,
        _CONTENTION_MONITOR_LOCK_KEY,
    ]
    assert len(keys) == len(set(keys)), (
        f"Advisory lock key collision detected among: {keys}"
    )
