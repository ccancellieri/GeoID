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

"""Leadership advisory-lock observability (DB-free).

The locks we create that live longest are the session advisory locks held for
a leadership tenure. Previously only acquisition was logged; release and
held-duration were silent, and there was no way to see "which locks does this
pod hold right now". These tests pin the new behaviour: the held-lock registry
tracks the tenure, and release logs the held duration.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

from sqlalchemy.ext.asyncio import AsyncEngine

import dynastore.modules.db_config.locking_tools as lt

# asyncio runs in AUTO mode (pytest.ini): async tests need no explicit marker,
# and a module-level mark would mis-tag the one sync test below.


class _FakeConn:
    async def execution_options(self, **_kw):
        return self


class _FakeConnCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *_a):
        return False


class _DQLStub:
    def __init__(self, sql, result_handler=None, **_kw):
        self.sql = sql

    async def execute(self, _conn, **_params):
        # pg_try_advisory_lock → acquired; pg_advisory_unlock → no-op.
        return "pg_try_advisory_lock" in self.sql


def _fake_engine(conn):
    engine = MagicMock(spec=AsyncEngine)
    engine.connect.return_value = _FakeConnCtx(conn)
    return engine


async def test_registry_tracks_tenure_and_release_logs_duration(monkeypatch, caplog):
    monkeypatch.setattr(lt, "DQLQuery", _DQLStub)
    engine = _fake_engine(_FakeConn())
    key = 0x1234_5678

    with caplog.at_level(logging.INFO, logger=lt.logger.name):
        async with lt.pg_advisory_leadership(engine, key, name="TestLeader") as (won, lock_conn):
            assert won is True
            assert lock_conn is not None
            # Inside the tenure the lock is in the process registry.
            held = lt.held_advisory_locks()
            assert key in held
            assert held[key][0] == "TestLeader"

        # On exit it is removed.
        assert key not in lt.held_advisory_locks()

    msgs = [r.getMessage() for r in caplog.records if r.name == lt.logger.name]
    assert any("leadership lock acquired" in m for m in msgs)
    released = [m for m in msgs if "leadership lock released" in m]
    assert released, "release must be logged"
    assert "held=" in released[0]


async def test_not_leader_does_not_register(monkeypatch):
    class _DenyDQL(_DQLStub):
        async def execute(self, _conn, **_params):
            return False  # lost the election

    monkeypatch.setattr(lt, "DQLQuery", _DenyDQL)
    engine = _fake_engine(_FakeConn())
    key = 0xDEAD_BEEF
    async with lt.pg_advisory_leadership(engine, key, name="Loser") as (won, lock_conn):
        assert won is False
        assert lock_conn is None
        assert key not in lt.held_advisory_locks()


async def test_held_snapshot_is_a_copy():
    snap = lt.held_advisory_locks()
    snap[123] = ("x", 0.0)
    # Mutating the snapshot must not corrupt the live registry.
    assert 123 not in lt.held_advisory_locks()
