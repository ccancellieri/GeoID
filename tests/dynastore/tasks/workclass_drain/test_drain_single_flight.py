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

"""Cross-pod drain single-flight + reaper reclaim grace (#3144).

Claim-version fencing keeps duplicate drains correct but not cheap: when
heartbeat writes lag a congested pooler, the reaper reclaims rows a live
run is still processing and a second pod pays the same hydration transient
concurrently. Two complementary bounds:

* ``DrainSingleFlightGate`` — a session-scoped ``pg_try_advisory_lock`` on
  a direct (non-pooled) connection held for the whole in-process run, so at
  most one in-process drain per workclass runs platform-wide (option B);
* a reclaim grace on drain workclasses in ``reap_stuck_tasks``, so the
  reaper only resets a drain task's row once ``locked_until`` has lapsed by
  more than two heartbeat visibility windows (option A) — this also covers
  the offloaded flavors the in-process gate cannot see.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

import dynastore.tasks.workclass_drain.single_flight as single_flight
from dynastore.tasks.workclass_drain.event_drain_task import EventDrainTask
from dynastore.tasks.workclass_drain.single_flight import (
    DrainSingleFlightGate,
    _direct_lane_dsn,
)
from dynastore.tasks.workclass_drain.storage_drain_task import (
    StorageDrainOffloadTask,
    StorageDrainTask,
)


class _FakeAsyncpgConn:
    """Minimal asyncpg-connection stand-in: records lock/unlock/close calls."""

    def __init__(self, try_lock_result: bool = True):
        self.try_lock_result = try_lock_result
        self.queries: list = []
        self.closed = False

    async def fetchval(self, sql: str, *args):
        self.queries.append((sql, args))
        if "pg_try_advisory_lock" in sql:
            return self.try_lock_result
        if "pg_advisory_unlock" in sql:
            return True
        raise AssertionError(f"unexpected gate query: {sql}")

    async def close(self):
        self.closed = True


def _patch_lane(monkeypatch, conn: _FakeAsyncpgConn | None, dsn="postgresql://x/y"):
    """Point the gate at a fake direct lane and a fake asyncpg.connect."""
    monkeypatch.setattr(single_flight, "_direct_lane_dsn", lambda: dsn)
    import asyncpg

    if conn is None:
        async def _connect(*a, **k):
            raise OSError("connect refused")
    else:
        async def _connect(*a, **k):
            return conn

    monkeypatch.setattr(asyncpg, "connect", _connect)


class TestDrainSingleFlightGate:
    @pytest.mark.asyncio
    async def test_acquired_then_released(self, monkeypatch):
        conn = _FakeAsyncpgConn(try_lock_result=True)
        _patch_lane(monkeypatch, conn)
        gate = DrainSingleFlightGate("storage")

        assert await gate.acquire() is True
        assert not conn.closed  # session must stay open while the run holds it
        assert any("pg_try_advisory_lock" in q for q, _ in conn.queries)

        await gate.release()
        assert any("pg_advisory_unlock" in q for q, _ in conn.queries)
        assert conn.closed

    @pytest.mark.asyncio
    async def test_held_elsewhere_skips_and_closes(self, monkeypatch):
        conn = _FakeAsyncpgConn(try_lock_result=False)
        _patch_lane(monkeypatch, conn)
        gate = DrainSingleFlightGate("storage")

        assert await gate.acquire() is False
        assert conn.closed  # a skipped run must not leak the probe session
        # release() after a lost acquire is a harmless no-op
        await gate.release()
        assert not any("pg_advisory_unlock" in q for q, _ in conn.queries)

    @pytest.mark.asyncio
    async def test_release_is_idempotent(self, monkeypatch):
        conn = _FakeAsyncpgConn(try_lock_result=True)
        _patch_lane(monkeypatch, conn)
        gate = DrainSingleFlightGate("storage")
        assert await gate.acquire() is True
        await gate.release()
        await gate.release()
        unlocks = [q for q, _ in conn.queries if "pg_advisory_unlock" in q]
        assert len(unlocks) == 1

    @pytest.mark.asyncio
    async def test_fails_open_without_direct_lane(self, monkeypatch):
        monkeypatch.setattr(single_flight, "_direct_lane_dsn", lambda: None)
        gate = DrainSingleFlightGate("storage")
        assert await gate.acquire() is True  # ungated, not skipped
        await gate.release()

    @pytest.mark.asyncio
    async def test_fails_open_on_connect_error(self, monkeypatch):
        _patch_lane(monkeypatch, conn=None)  # connect raises
        gate = DrainSingleFlightGate("storage")
        assert await gate.acquire() is True
        await gate.release()

    @pytest.mark.asyncio
    async def test_fails_open_on_lock_query_error(self, monkeypatch):
        class _BrokenConn(_FakeAsyncpgConn):
            async def fetchval(self, sql, *args):
                raise RuntimeError("backend dropped")

        conn = _BrokenConn()
        _patch_lane(monkeypatch, conn)
        gate = DrainSingleFlightGate("storage")
        assert await gate.acquire() is True
        assert conn.closed  # broken probe session must not leak

    def test_workclass_keys_are_distinct_and_namespaced(self):
        from dynastore.modules.tasks.durable.locks import stable_lock_id_blake2b

        storage = DrainSingleFlightGate("storage")._lock_key
        events = DrainSingleFlightGate("events")._lock_key
        assert storage != events
        # Same derivation, distinct first part → cannot collide with the
        # dispatcher's serialization guards over the shared PG lock space.
        assert storage == stable_lock_id_blake2b(
            "workclass_drain_single_flight", "storage"
        )
        assert storage != stable_lock_id_blake2b("storage")


class TestDirectLaneDsn:
    def test_listen_lane_wins(self, monkeypatch):
        from dynastore.modules.db_config.db_config import DBConfig

        monkeypatch.setattr(
            DBConfig, "listen_database_url", "postgresql+asyncpg://u:p@direct:5432/db"
        )
        assert _direct_lane_dsn() == "postgresql://u:p@direct:5432/db"

    def test_pooler_without_listen_lane_is_untrustworthy(self, monkeypatch):
        from dynastore.modules.db_config.db_config import DBConfig

        monkeypatch.setattr(DBConfig, "listen_database_url", "")
        monkeypatch.setattr(DBConfig, "db_pooling_mode", "transaction_pooler")
        assert _direct_lane_dsn() is None

    def test_direct_mode_falls_back_to_database_url(self, monkeypatch):
        from dynastore.modules.db_config.db_config import DBConfig

        monkeypatch.setattr(DBConfig, "listen_database_url", "")
        monkeypatch.setattr(DBConfig, "db_pooling_mode", "direct")
        monkeypatch.setattr(
            DBConfig, "database_url", "postgresql+asyncpg://u:p@db:5432/db"
        )
        assert _direct_lane_dsn() == "postgresql://u:p@db:5432/db"


class _RecordingGate:
    """DrainSingleFlightGate stand-in recording construction and calls."""

    instances: list = []

    def __init__(self, workclass: str):
        self.workclass = workclass
        self.acquire_result = True
        self.released = False
        type(self).instances.append(self)

    async def acquire(self) -> bool:
        return self.acquire_result

    async def release(self) -> None:
        self.released = True


@pytest.fixture(autouse=True)
def _reset_recording_gate():
    _RecordingGate.instances = []
    yield
    _RecordingGate.instances = []


def _patch_task_engine(monkeypatch):
    """Neither the skip path nor the gated path may need a real DB engine."""
    import dynastore.modules.db_config.db_timeout_config as db_timeout_config

    engine = MagicMock()
    engine.dispose = AsyncMock()
    monkeypatch.setattr(db_timeout_config, "create_task_engine", lambda cfg: engine)
    return engine


class TestStorageRunSingleFlight:
    @pytest.mark.asyncio
    async def test_skips_without_claiming_when_gate_held(self, monkeypatch):
        _patch_task_engine(monkeypatch)
        import dynastore.tasks.workclass_drain.storage_drain_task as mod

        monkeypatch.setattr(mod, "DrainSingleFlightGate", _RecordingGate)

        async def _must_not_run(self, **kwargs):
            raise AssertionError("gated run must never claim or probe")

        monkeypatch.setattr(StorageDrainTask, "drain_once", _must_not_run)
        monkeypatch.setattr(
            StorageDrainTask, "_offload_drain_is_active", _must_not_run
        )

        _RecordingGate.instances = []
        task = StorageDrainTask()
        # Pre-set the gate outcome for the instance run() will construct.
        orig_init = _RecordingGate.__init__

        def _init(self, workclass):
            orig_init(self, workclass)
            self.acquire_result = False

        monkeypatch.setattr(_RecordingGate, "__init__", _init)
        report = await task.run(MagicMock())

        assert "skipped" in (report.message or "")
        assert report.metrics is not None and report.metrics.get("drained") == 0
        (gate,) = _RecordingGate.instances
        assert gate.workclass == "storage"
        assert gate.released  # skip path still tears the gate object down

    @pytest.mark.asyncio
    async def test_gate_released_after_normal_run(self, monkeypatch):
        _patch_task_engine(monkeypatch)
        import dynastore.tasks.workclass_drain.storage_drain_task as mod

        monkeypatch.setattr(mod, "DrainSingleFlightGate", _RecordingGate)

        async def _empty(self, **kwargs):
            return 0

        async def _no_offload(self, engine):
            return False

        monkeypatch.setattr(StorageDrainTask, "drain_once", _empty)
        monkeypatch.setattr(StorageDrainTask, "_offload_drain_is_active", _no_offload)

        report = await StorageDrainTask().run(MagicMock())
        assert "completed" in (report.message or "")
        (gate,) = _RecordingGate.instances
        assert gate.released

    @pytest.mark.asyncio
    async def test_offload_subclass_never_gates(self, monkeypatch):
        _patch_task_engine(monkeypatch)
        import dynastore.tasks.workclass_drain.storage_drain_task as mod

        monkeypatch.setattr(mod, "DrainSingleFlightGate", _RecordingGate)

        async def _empty(self, **kwargs):
            return 0

        monkeypatch.setattr(StorageDrainOffloadTask, "drain_once", _empty)

        await StorageDrainOffloadTask().run(MagicMock())
        assert _RecordingGate.instances == []


class TestEventRunSingleFlight:
    @pytest.mark.asyncio
    async def test_skips_without_claiming_when_gate_held(self, monkeypatch):
        _patch_task_engine(monkeypatch)
        import dynastore.tasks.workclass_drain.event_drain_task as mod

        def _init(self, workclass):
            _RecordingGate.instances.append(self)
            self.workclass = workclass
            self.acquire_result = False
            self.released = False

        monkeypatch.setattr(_RecordingGate, "__init__", _init)
        monkeypatch.setattr(mod, "DrainSingleFlightGate", _RecordingGate)

        async def _must_not_run(self, **kwargs):
            raise AssertionError("gated run must never claim")

        monkeypatch.setattr(EventDrainTask, "drain_once", _must_not_run)

        report = await EventDrainTask().run(MagicMock())
        assert "skipped" in (report.message or "")
        (gate,) = _RecordingGate.instances
        assert gate.workclass == "events"
        assert gate.released

    @pytest.mark.asyncio
    async def test_ephemeral_job_never_gates(self, monkeypatch):
        _patch_task_engine(monkeypatch)
        import dynastore.tasks.workclass_drain.event_drain_task as mod

        monkeypatch.setattr(mod, "DrainSingleFlightGate", _RecordingGate)

        async def _empty(self, **kwargs):
            return 0

        monkeypatch.setattr(EventDrainTask, "drain_once", _empty)

        task = EventDrainTask(SimpleNamespace(ephemeral_job=True))
        await task.run(MagicMock())
        assert _RecordingGate.instances == []


class TestReaperDrainReclaimGrace:
    def test_grace_predicate_present(self):
        from dynastore.modules.tasks.tasks_module import (
            DRAIN_RECLAIM_GRACE_SECONDS,
            GLOBAL_TASKS_REAPER_DDL,
        )

        assert (
            f"make_interval(secs => {DRAIN_RECLAIM_GRACE_SECONDS})"
            in GLOBAL_TASKS_REAPER_DDL
        )
        # Non-drain tasks keep the immediate reclaim.
        assert "ELSE INTERVAL '0 seconds'" in GLOBAL_TASKS_REAPER_DDL

    def test_grace_is_two_visibility_windows(self):
        from dynastore.modules.tasks.tasks_module import DRAIN_RECLAIM_GRACE_SECONDS

        # The runner heartbeat visibility window defaults to 5 minutes
        # (execution.py) — the grace bounds reaper aggressiveness at two of
        # those, trading minutes of dead-worker recovery for no live-run
        # duplicate spawns.
        assert DRAIN_RECLAIM_GRACE_SECONDS == 600

    def test_drain_types_match_task_classvars(self):
        from dynastore.modules.tasks.tasks_module import DRAIN_WORKCLASS_TASK_TYPES

        assert set(DRAIN_WORKCLASS_TASK_TYPES) == {
            EventDrainTask.task_type,
            StorageDrainTask.task_type,
            StorageDrainOffloadTask.task_type,
        }

    def test_signature_stays_frozen_two_args(self):
        # CREATE OR REPLACE with an added parameter would create a second
        # function identity (an overload) — old-revision callers would keep
        # hitting the stale 2-arg body forever. Tunables go in the body.
        from dynastore.modules.tasks.tasks_module import GLOBAL_TASKS_REAPER_DDL

        header = GLOBAL_TASKS_REAPER_DDL.split("RETURNS", 1)[0]
        assert "p_max_retries INT DEFAULT 3" in header
        assert "p_hard_cap INT DEFAULT 5" in header
        assert header.count("DEFAULT") == 2
