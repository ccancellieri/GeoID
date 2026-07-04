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

"""Read-only DB contention monitor — observability behaviour (DB-free).

These tests pin the contract the operator depends on for "is it locking or
infra?": a calm snapshot logs ONE info heartbeat; lock-waits and slow queries
escalate to WARNING with distinct, attributable detail lines; and the steady
state runs exactly one (cheap) aggregate query — detail queries fire only when
the aggregate says there is something to look at.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import dynastore.modules.db.db_contention_monitor as mod
from dynastore.modules.db.db_contention_monitor import (
    DbContentionMonitor,
    DbContentionMonitorConfig,
)

# asyncio runs in AUTO mode (see pytest.ini) — async tests are detected without
# an explicit marker, and a module-level asyncio mark would mis-tag the one sync
# config test.

_CALM_AGG = {
    "max_connections": 100,
    "total": 8,
    "active": 2,
    "idle": 6,
    "idle_in_txn": 0,
    "waiting_on_lock": 0,
    "longest_active_secs": 1,
    "longest_idle_txn_secs": 0,
    "advisory_locks": 3,
}

_LOCKED_AGG = {
    **_CALM_AGG,
    "waiting_on_lock": 2,
    "longest_active_secs": 4,
}

_SLOW_AGG = {
    **_CALM_AGG,
    "waiting_on_lock": 0,
    "longest_active_secs": 45,
}

_BLOCKED_ROWS = [
    {
        "blocked_pid": 4242,
        "blocked_secs": 7,
        "wait_event_type": "Lock",
        "wait_event": "relation",
        "application_name": "dynastore-maps",
        "blocked_query": "SELECT ... FROM tiles",
        "blocker_pids": [99],
    }
]

_SLOW_ROWS = [
    {
        "pid": 7777,
        "active_secs": 45,
        "wait_event_type": None,
        "wait_event": None,
        "application_name": "dynastore-maps",
        "query": "SELECT ST_AsMVT(...) FROM gaul",
    }
]


def _install_stub(monkeypatch, *, agg, blocked=None, slow=None):
    """Patch DQLQuery + managed_transaction + get_engine; record SQL issued."""
    issued: list[str] = []

    class _DQLStub:
        def __init__(self, sql, result_handler=None, **_kw):
            self.sql = sql

        async def execute(self, _conn, **_params):
            issued.append(self.sql)
            if "pg_blocking_pids" in self.sql:
                return list(blocked or [])
            if "make_interval" in self.sql:
                return list(slow or [])
            # the aggregate snapshot
            return [dict(agg)]

    @asynccontextmanager
    async def _txn(_engine):
        yield object()

    monkeypatch.setattr(mod, "DQLQuery", _DQLStub)
    monkeypatch.setattr(mod, "managed_transaction", _txn)
    monkeypatch.setattr(mod, "get_engine", lambda: object())
    return issued


async def test_calm_tick_logs_single_info_heartbeat_and_one_query(monkeypatch, caplog):
    issued = _install_stub(monkeypatch, agg=_CALM_AGG)
    monitor = DbContentionMonitor(DbContentionMonitorConfig())
    with caplog.at_level(logging.INFO, logger=mod.logger.name):
        snap = await monitor.run_once()

    assert snap is not None
    # Steady state: ONLY the aggregate query runs (no blocked/slow detail).
    assert len(issued) == 1
    records = [r for r in caplog.records if r.name == mod.logger.name]
    assert len(records) == 1
    assert records[0].levelno == logging.INFO
    assert "db_contention:" in records[0].getMessage()
    assert "waiting_on_lock=0" in records[0].getMessage()


async def test_lock_wait_escalates_to_warning_with_blocker(monkeypatch, caplog):
    issued = _install_stub(monkeypatch, agg=_LOCKED_AGG, blocked=_BLOCKED_ROWS)
    monitor = DbContentionMonitor(DbContentionMonitorConfig())
    with caplog.at_level(logging.INFO, logger=mod.logger.name):
        await monitor.run_once()

    # waiting_on_lock>0 ⟹ the blocked-detail query fires.
    assert any("pg_blocking_pids" in s for s in issued)
    msgs = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("db_contention:" in m for m in msgs)  # summary escalated
    lockline = [m for m in msgs if "LOCK-WAIT" in m]
    assert lockline, "expected a LOCK-WAIT detail line"
    assert "blocked_by=[99]" in lockline[0]


async def test_slow_query_escalates_without_lock_detail(monkeypatch, caplog):
    issued = _install_stub(monkeypatch, agg=_SLOW_AGG, slow=_SLOW_ROWS)
    monitor = DbContentionMonitor(DbContentionMonitorConfig())
    with caplog.at_level(logging.INFO, logger=mod.logger.name):
        await monitor.run_once()

    # longest_active over threshold ⟹ slow-detail query fires; no lock query.
    assert any("make_interval" in s for s in issued)
    assert not any("pg_blocking_pids" in s for s in issued)
    msgs = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    slowline = [m for m in msgs if "SLOW-QUERY" in m]
    assert slowline, "expected a SLOW-QUERY detail line"
    assert "ST_AsMVT" in slowline[0]


async def test_disabled_monitor_is_a_noop(monkeypatch):
    issued = _install_stub(monkeypatch, agg=_CALM_AGG)
    monitor = DbContentionMonitor(DbContentionMonitorConfig(enabled=False))
    snap = await monitor.run_once()
    assert snap is None
    assert issued == []


async def test_snapshot_failure_is_swallowed(monkeypatch):
    @asynccontextmanager
    async def _boom(_engine):
        raise RuntimeError("pg_stat_activity unavailable")
        yield  # pragma: no cover

    monkeypatch.setattr(mod, "managed_transaction", _boom)
    monkeypatch.setattr(mod, "get_engine", lambda: object())
    monitor = DbContentionMonitor(DbContentionMonitorConfig())
    # Observability must never become its own incident.
    assert await monitor.run_once() is None


def test_config_from_env(monkeypatch):
    monkeypatch.setenv("DB_CONTENTION_MONITOR_ENABLED", "false")
    monkeypatch.setenv("DB_CONTENTION_MONITOR_INTERVAL_SECONDS", "15")
    monkeypatch.setenv("DB_CONTENTION_SLOW_QUERY_SECONDS", "20")
    monkeypatch.setenv("DB_CONTENTION_LOCK_WAIT_SECONDS", "3")
    cfg = DbContentionMonitorConfig.from_env()
    assert cfg.enabled is False
    assert cfg.interval_seconds == 15
    assert cfg.slow_query_seconds == 20
    assert cfg.lock_wait_seconds == 3


def test_lease_renewal_mode_is_heartbeat():
    """#2900: default cadence (30s) equals the lease TTL, so this monitor
    holds tenure across ticks instead of re-electing per tick -- it should
    stay stably leader-elected through the very contention episodes it
    exists to observe."""
    from dynastore.tools.background_service import LeaseRenewalMode

    assert DbContentionMonitor.lease_renewal_mode is LeaseRenewalMode.HEARTBEAT
