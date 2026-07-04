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

"""Zombie-session reaper — DB-free behaviour.

Pins the strict recognition contract: a session is only ever terminated when
it (1) carries our stamped application_name shape, (2) has been idle past the
threshold, (3) has no fresh configs.instance_liveness row scoped to its own
service, and (4) is still idle past the threshold at the moment of the
terminate itself (TOCTOU recheck). Any one of those failing must leave the
session untouched.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import pytest
from pydantic import ValidationError

import dynastore.modules.db.zombie_session_reaper as mod
from dynastore.modules.db.zombie_session_reaper import (
    ZombieSessionReaper,
    ZombieSessionReaperConfig,
)

_LIVE_INSTANCE = "11111111111111111111111111111111"
_DEAD_INSTANCE = "22222222222222222222222222222222"
_DEAD_INSTANCE_B = "33333333333333333333333333333333"

_DEAD_ROW = {
    "pid": 4242,
    "application_name": f"catalog-api:{_DEAD_INSTANCE}",
    "state": "idle",
    "idle_secs": 3600,
    "last_query": "SELECT pg_try_advisory_xact_lock($1)",
}

_LIVE_ROW = {
    "pid": 7777,
    "application_name": f"catalog-api:{_LIVE_INSTANCE}",
    "state": "idle",
    "idle_secs": 3600,
    "last_query": "SELECT 1",
}

_UNSTAMPED_ROW = {
    "pid": 9999,
    "application_name": "psql",
    "state": "idle",
    "idle_secs": 999999,
    "last_query": "SELECT 1",
}

_SERVICE_A_DEAD_ROW = {
    "pid": 111,
    "application_name": f"service-a:{_DEAD_INSTANCE}",
    "state": "idle",
    "idle_secs": 3600,
    "last_query": "SELECT 1",
}

_SERVICE_B_DEAD_ROW = {
    "pid": 222,
    "application_name": f"service-b:{_DEAD_INSTANCE_B}",
    "state": "idle",
    "idle_secs": 3600,
    "last_query": "SELECT 1",
}


def _install_stub(
    monkeypatch,
    *,
    config,
    candidates,
    fresh_instance_ids=None,
    any_fresh=1,
    fresh_instance_ids_by_service=None,
    any_fresh_by_service=None,
    lock_rows=None,
    recheck_ok=True,
):
    """Route each DQLQuery by a distinguishing substring in its SQL text."""
    issued: list[str] = []

    class _DQLStub:
        def __init__(self, sql, result_handler=None, **_kw):
            self.sql = sql

        async def execute(self, _conn, **params):
            issued.append(self.sql)
            if "application_name ~" in self.sql:
                return [dict(r) for r in candidates]
            if "SELECT 1" in self.sql and "pg_stat_activity" in self.sql:
                return 1 if recheck_ok else None
            if "SELECT count(*)" in self.sql and "instance_liveness" in self.sql:
                if any_fresh_by_service is not None:
                    return any_fresh_by_service.get(params.get("service"), 0)
                return any_fresh
            if "SELECT instance_id" in self.sql:
                if fresh_instance_ids_by_service is not None:
                    ids = fresh_instance_ids_by_service.get(params.get("service"), [])
                else:
                    ids = fresh_instance_ids or []
                return [{"instance_id": i} for i in ids]
            if "FROM pg_locks" in self.sql:
                return [dict(r) for r in (lock_rows or [])]
            if "pg_terminate_backend" in self.sql:
                return True
            raise AssertionError(f"unexpected SQL: {self.sql}")

    @asynccontextmanager
    async def _txn(_engine):
        yield object()

    async def _load_config():
        return config

    monkeypatch.setattr(mod, "DQLQuery", _DQLStub)
    monkeypatch.setattr(mod, "background_managed_transaction", _txn)
    monkeypatch.setattr(mod, "get_engine", lambda: object())
    monkeypatch.setattr(mod, "load_zombie_session_reaper_config", _load_config)
    return issued


async def test_disabled_reaper_is_a_noop(monkeypatch):
    issued = _install_stub(
        monkeypatch,
        config=ZombieSessionReaperConfig(enabled=False),
        candidates=[_DEAD_ROW],
        fresh_instance_ids=[],
    )
    reaper = ZombieSessionReaper(ZombieSessionReaperConfig(enabled=False))
    await reaper.run_once()
    assert issued == []


async def test_no_candidates_is_a_noop(monkeypatch):
    issued = _install_stub(
        monkeypatch, config=ZombieSessionReaperConfig(enabled=True),
        candidates=[], fresh_instance_ids=[],
    )
    reaper = ZombieSessionReaper(ZombieSessionReaperConfig(enabled=True))
    await reaper.run_once()
    assert any("pg_stat_activity" in s for s in issued)
    assert not any("pg_terminate_backend" in s for s in issued)


async def test_live_instance_session_is_never_reaped(monkeypatch, caplog):
    issued = _install_stub(
        monkeypatch,
        config=ZombieSessionReaperConfig(enabled=True),
        candidates=[_LIVE_ROW],
        fresh_instance_ids=[_LIVE_INSTANCE],
    )
    reaper = ZombieSessionReaper(ZombieSessionReaperConfig(enabled=True))
    with caplog.at_level(logging.WARNING, logger=mod.logger.name):
        await reaper.run_once()

    assert not any("pg_terminate_backend" in s for s in issued)
    assert not any("lock_reaped" in r.getMessage() for r in caplog.records)


async def test_dead_instance_session_is_reaped_with_lock_detail(monkeypatch, caplog):
    issued = _install_stub(
        monkeypatch,
        config=ZombieSessionReaperConfig(enabled=True),
        candidates=[_DEAD_ROW],
        fresh_instance_ids=[],  # dead instance carries no fresh row
        lock_rows=[{"lock_id": 123456789}],
    )
    reaper = ZombieSessionReaper(ZombieSessionReaperConfig(enabled=True))
    with caplog.at_level(logging.WARNING, logger=mod.logger.name):
        await reaper.run_once()

    assert any("pg_terminate_backend" in s for s in issued)
    msgs = [r.getMessage() for r in caplog.records]
    lock_line = [m for m in msgs if m.startswith("lock_reaped")]
    assert lock_line, "expected a lock_reaped structured warning before terminating"
    assert "123456789" in lock_line[0]
    assert any("terminated pid=4242" in m for m in msgs)


async def test_unstamped_session_is_never_reaped(monkeypatch):
    """A session whose application_name doesn't match our stamped shape at
    all must never be considered, even if pg_stat_activity somehow returned
    it (defense in depth beyond the SQL-level regex filter)."""
    issued = _install_stub(
        monkeypatch, config=ZombieSessionReaperConfig(enabled=True),
        candidates=[_UNSTAMPED_ROW], fresh_instance_ids=[],
    )
    reaper = ZombieSessionReaper(ZombieSessionReaperConfig(enabled=True))
    await reaper.run_once()
    assert not any("pg_terminate_backend" in s for s in issued)


async def test_safety_valve_skips_pass_when_no_fresh_liveness_rows_exist(monkeypatch, caplog):
    """If the liveness table has no fresh rows for this service at all,
    distrust it for that service rather than treat every candidate as dead."""
    issued = _install_stub(
        monkeypatch,
        config=ZombieSessionReaperConfig(enabled=True),
        candidates=[_DEAD_ROW], fresh_instance_ids=[], any_fresh=0,
    )
    reaper = ZombieSessionReaper(ZombieSessionReaperConfig(enabled=True))
    with caplog.at_level(logging.WARNING, logger=mod.logger.name):
        await reaper.run_once()

    assert not any("pg_terminate_backend" in s for s in issued)
    assert not any("SELECT instance_id" in s for s in issued)
    assert any("distrusting" in r.getMessage() for r in caplog.records)


async def test_safety_valve_is_scoped_per_service(monkeypatch, caplog):
    """A heartbeat outage affecting only one service must not let that
    service's candidates get reaped just because ANOTHER service still has
    fresh liveness rows — and must not block the healthy service either."""
    issued = _install_stub(
        monkeypatch,
        config=ZombieSessionReaperConfig(enabled=True),
        candidates=[_SERVICE_A_DEAD_ROW, _SERVICE_B_DEAD_ROW],
        any_fresh_by_service={"service-a": 0, "service-b": 1},
        fresh_instance_ids_by_service={"service-a": [], "service-b": []},
    )
    reaper = ZombieSessionReaper(ZombieSessionReaperConfig(enabled=True))
    with caplog.at_level(logging.WARNING, logger=mod.logger.name):
        await reaper.run_once()

    msgs = [r.getMessage() for r in caplog.records]
    assert any("distrusting" in m and "service-a" in m for m in msgs)
    assert any("terminated pid=222" in m for m in msgs)
    assert not any("terminated pid=111" in m for m in msgs)
    assert any("pg_terminate_backend" in s for s in issued)


async def test_toctou_recheck_skips_reap_when_session_no_longer_idle(monkeypatch, caplog):
    """A session that woke back up (or whose pid was recycled) between the
    scan and the reap must not be terminated."""
    issued = _install_stub(
        monkeypatch,
        config=ZombieSessionReaperConfig(enabled=True),
        candidates=[_DEAD_ROW],
        fresh_instance_ids=[],
        recheck_ok=False,
    )
    reaper = ZombieSessionReaper(ZombieSessionReaperConfig(enabled=True))
    with caplog.at_level(logging.INFO, logger=mod.logger.name):
        await reaper.run_once()

    assert not any("pg_terminate_backend" in s for s in issued)
    assert any("recheck failed" in r.getMessage() for r in caplog.records)


async def test_candidate_scan_failure_is_swallowed(monkeypatch):
    @asynccontextmanager
    async def _boom(_engine):
        raise RuntimeError("pg_stat_activity unavailable")
        yield  # pragma: no cover

    async def _load_config():
        return ZombieSessionReaperConfig(enabled=True)

    monkeypatch.setattr(mod, "background_managed_transaction", _boom)
    monkeypatch.setattr(mod, "get_engine", lambda: object())
    monkeypatch.setattr(mod, "load_zombie_session_reaper_config", _load_config)
    reaper = ZombieSessionReaper(ZombieSessionReaperConfig(enabled=True))
    await reaper.run_once()  # must not raise


def test_config_rejects_stale_after_shorter_than_idle_threshold():
    with pytest.raises(ValidationError):
        ZombieSessionReaperConfig(
            idle_threshold_seconds=1800, liveness_stale_after_seconds=300,
        )


def test_config_defaults_satisfy_the_validator():
    cfg = ZombieSessionReaperConfig()
    assert cfg.liveness_stale_after_seconds >= cfg.idle_threshold_seconds


def test_config_accepts_stale_after_equal_to_idle_threshold():
    cfg = ZombieSessionReaperConfig(
        idle_threshold_seconds=900, liveness_stale_after_seconds=900,
    )
    assert cfg.liveness_stale_after_seconds == 900
