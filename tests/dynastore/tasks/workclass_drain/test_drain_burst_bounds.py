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

"""Regression: storage-drain memory-burst bounds (#3121).

The dev catalog OOM was a BURST, not a leak: two storage_drain runs
dispatched concurrently on one worker each materialized an id-only read
chunk's raw JSONB/GeoJSON rows through asyncpg's ``json.loads`` codec at
once — a decode transient invisible to the hydration byte budget (which
only measures BUILT documents) — and the stacked spikes crossed the
container limit inside a single 60s metric sample. Three bounds:

* runs serialize per worker process (``_drain_run_gate``);
* id-only re-read chunks adapt their row count to the measured hydrated
  byte cost (``_next_id_only_chunk_rows``) instead of a fixed 50 rows;
* freed transients are handed back to the OS (``trim_malloc_arenas``)
  instead of staying pinned in glibc arenas at the burst peak.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from dynastore.tasks.workclass_drain.storage_drain_task import (
    _ID_ONLY_READ_CHUNK_ROWS,
    _ID_ONLY_READ_PROBE_ROWS,
    StorageDrainOffloadTask,
    StorageDrainTask,
    _next_id_only_chunk_rows,
)
from dynastore.tools.memory_trim import trim_malloc_arenas


class TestNextIdOnlyChunkRows:
    BUDGET = 16 * 1024 * 1024  # matches the default hydration byte budget

    def test_oversized_rows_shrink_to_one(self):
        # 5 rows measured at ~20MiB each: even one row exceeds the budget —
        # fetch it alone rather than split (a row is atomic).
        got = _next_id_only_chunk_rows(
            chunk_bytes=5 * 20 * 1024 * 1024, rows_read=5,
            byte_budget=self.BUDGET, current=5,
        )
        assert got == 1

    def test_mb_scale_rows_fit_budget(self):
        # ~4MiB rows: 16MiB budget fits 4 per SELECT.
        got = _next_id_only_chunk_rows(
            chunk_bytes=5 * 4 * 1024 * 1024, rows_read=5,
            byte_budget=self.BUDGET, current=5,
        )
        assert got == 4

    def test_small_rows_grow_to_ceiling(self):
        # 1KiB rows would fit thousands — the pre-#3121 fixed size caps it.
        got = _next_id_only_chunk_rows(
            chunk_bytes=5 * 1024, rows_read=5,
            byte_budget=self.BUDGET, current=5,
        )
        assert got == _ID_ONLY_READ_CHUNK_ROWS

    def test_unmeasured_chunk_keeps_current(self):
        # Every geoid absent → nothing hydrated → no evidence to resize on.
        assert _next_id_only_chunk_rows(
            chunk_bytes=0, rows_read=5, byte_budget=self.BUDGET, current=7,
        ) == 7
        assert _next_id_only_chunk_rows(
            chunk_bytes=1024, rows_read=0, byte_budget=self.BUDGET, current=7,
        ) == 7

    def test_probe_is_small_and_within_ceiling(self):
        assert 1 <= _ID_ONLY_READ_PROBE_ROWS < _ID_ONLY_READ_CHUNK_ROWS


class TestDrainRunGate:
    @pytest.mark.asyncio
    async def test_concurrent_runs_serialize(self, monkeypatch):
        active = 0
        max_active = 0

        async def _fake_run_drain(self, payload):
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.01)
            active -= 1
            return "done"

        monkeypatch.setattr(StorageDrainTask, "_run_drain", _fake_run_drain)
        results = await asyncio.gather(
            StorageDrainTask().run(MagicMock()),
            StorageDrainTask().run(MagicMock()),
            StorageDrainTask().run(MagicMock()),
        )
        assert results == ["done"] * 3
        assert max_active == 1

    @pytest.mark.asyncio
    async def test_offload_task_shares_the_gate(self, monkeypatch):
        # StorageDrainOffloadTask inherits run() unmodified, so a base run
        # and an offload run on the same worker must also serialize.
        active = 0
        max_active = 0

        async def _fake_run_drain(self, payload):
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.01)
            active -= 1
            return "done"

        monkeypatch.setattr(StorageDrainTask, "_run_drain", _fake_run_drain)
        await asyncio.gather(
            StorageDrainTask().run(MagicMock()),
            StorageDrainOffloadTask().run(MagicMock()),
        )
        assert max_active == 1

    @pytest.mark.asyncio
    async def test_gate_released_on_failure(self, monkeypatch):
        async def _boom(self, payload):
            raise RuntimeError("drain blew up")

        monkeypatch.setattr(StorageDrainTask, "_run_drain", _boom)
        with pytest.raises(RuntimeError):
            await StorageDrainTask().run(MagicMock())

        # The gate must be free again — a second run acquires immediately.
        async def _ok(self, payload):
            return "recovered"

        monkeypatch.setattr(StorageDrainTask, "_run_drain", _ok)
        assert await asyncio.wait_for(
            StorageDrainTask().run(MagicMock()), timeout=1.0,
        ) == "recovered"


def test_trim_malloc_arenas_is_safe_everywhere():
    # Linux/glibc releases pages; macOS and musl are documented no-ops.
    # Either way it must return a bool and never raise.
    assert trim_malloc_arenas() in (True, False)
