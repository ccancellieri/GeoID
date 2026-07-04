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

"""Unit tests for ``LogDrainer`` (#2833) — the leader-elected reader of the
Valkey-buffered log queue. Pure-mock style: a fake ``ListCacheBackend``
stands in for Valkey; no live DB, no live Valkey.
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.catalog.log_drainer import LogDrainer, _MAX_CHUNKS_PER_TICK
from dynastore.modules.catalog.log_manager import LogEntryCreate
from dynastore.modules.catalog.log_service_config import LogServiceConfig
from dynastore.tools import cache as cache_tools


class _FakeListBackend:
    name = "fake-list"
    priority = 50

    def __init__(self, items=None) -> None:
        self._items: list = list(items or [])
        self.lpop_calls: list = []

    async def get(self, key):
        return None

    async def set(self, key, value, *, ttl=None, exist=None):
        return True

    async def clear(self, *, key=None, namespace=None, tags=None):
        return False

    async def exists(self, key):
        return False

    async def close(self):
        return None

    async def rpush_trimmed(self, key, values, *, max_len):
        self._items.extend(values)
        return 0

    async def lpop_many(self, key, count):
        self.lpop_calls.append(count)
        chunk = self._items[:count]
        self._items = self._items[count:]
        return chunk


def _raw(entry: LogEntryCreate) -> bytes:
    return entry.model_dump_json().encode("utf-8")


def _entries(n: int) -> list:
    return [
        LogEntryCreate(catalog_id="cat1", event_type="e", message=f"m{i}")
        for i in range(n)
    ]


@pytest.fixture
def fresh_cache_manager(monkeypatch):
    manager = cache_tools.CacheManager()
    monkeypatch.setattr(cache_tools, "_cache_manager", manager)
    return manager


class TestNoValkeyIsANoop:
    @pytest.mark.asyncio
    async def test_noop_when_active_backend_has_no_list_ops(self, fresh_cache_manager):
        # Only the default local backend is registered.
        drainer = LogDrainer(LogServiceConfig())
        with patch(
            "dynastore.modules.catalog.log_drainer.write_batch_to_backends",
            new=AsyncMock(),
        ) as dispatch:
            await drainer.run_once()
        dispatch.assert_not_awaited()


class TestChunkDispatch:
    @pytest.mark.asyncio
    async def test_drains_one_chunk_and_dispatches(self, fresh_cache_manager):
        entries = _entries(3)
        backend = _FakeListBackend(items=[_raw(e) for e in entries])
        fresh_cache_manager.register_backend(backend)
        cfg = LogServiceConfig(valkey_drain_chunk_size=10)
        drainer = LogDrainer(cfg)

        with patch(
            "dynastore.modules.catalog.log_drainer.load_log_config",
            new=AsyncMock(return_value=cfg),
        ), patch(
            "dynastore.modules.catalog.log_drainer.write_batch_to_backends",
            new=AsyncMock(),
        ) as dispatch:
            await drainer.run_once()

        dispatch.assert_awaited_once()
        dispatched = dispatch.await_args.args[0]
        assert {e.message for e in dispatched} == {"m0", "m1", "m2"}

    @pytest.mark.asyncio
    async def test_drains_multiple_chunks_within_one_tick(self, fresh_cache_manager):
        entries = _entries(5)
        backend = _FakeListBackend(items=[_raw(e) for e in entries])
        fresh_cache_manager.register_backend(backend)
        cfg = LogServiceConfig(valkey_drain_chunk_size=2)
        drainer = LogDrainer(cfg)

        with patch(
            "dynastore.modules.catalog.log_drainer.load_log_config",
            new=AsyncMock(return_value=cfg),
        ), patch(
            "dynastore.modules.catalog.log_drainer.write_batch_to_backends",
            new=AsyncMock(),
        ) as dispatch:
            await drainer.run_once()

        # 5 entries / chunk_size=2 -> chunks of 2, 2, 1 (3 dispatches).
        assert dispatch.await_count == 3
        assert backend.lpop_calls == [2, 2, 2]

    @pytest.mark.asyncio
    async def test_malformed_entry_is_skipped_not_fatal(self, fresh_cache_manager, caplog):
        good = _entries(1)[0]
        backend = _FakeListBackend(items=[b"not-json", _raw(good)])
        fresh_cache_manager.register_backend(backend)
        cfg = LogServiceConfig(valkey_drain_chunk_size=10)
        drainer = LogDrainer(cfg)

        with patch(
            "dynastore.modules.catalog.log_drainer.load_log_config",
            new=AsyncMock(return_value=cfg),
        ), patch(
            "dynastore.modules.catalog.log_drainer.write_batch_to_backends",
            new=AsyncMock(),
        ) as dispatch, caplog.at_level(
            logging.WARNING, logger="dynastore.modules.catalog.log_drainer"
        ):
            await drainer.run_once()

        dispatched = dispatch.await_args.args[0]
        assert len(dispatched) == 1
        assert dispatched[0].message == "m0"
        assert any("malformed" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_bounded_to_max_chunks_per_tick(self, fresh_cache_manager):
        chunk_size = 2
        total = (_MAX_CHUNKS_PER_TICK + 5) * chunk_size
        backend = _FakeListBackend(items=[_raw(e) for e in _entries(total)])
        fresh_cache_manager.register_backend(backend)
        cfg = LogServiceConfig(valkey_drain_chunk_size=chunk_size)
        drainer = LogDrainer(cfg)

        with patch(
            "dynastore.modules.catalog.log_drainer.load_log_config",
            new=AsyncMock(return_value=cfg),
        ), patch(
            "dynastore.modules.catalog.log_drainer.write_batch_to_backends",
            new=AsyncMock(),
        ):
            await drainer.run_once()

        assert len(backend.lpop_calls) == _MAX_CHUNKS_PER_TICK

    @pytest.mark.asyncio
    async def test_empty_queue_dispatches_nothing(self, fresh_cache_manager):
        backend = _FakeListBackend(items=[])
        fresh_cache_manager.register_backend(backend)
        cfg = LogServiceConfig()
        drainer = LogDrainer(cfg)

        with patch(
            "dynastore.modules.catalog.log_drainer.load_log_config",
            new=AsyncMock(return_value=cfg),
        ), patch(
            "dynastore.modules.catalog.log_drainer.write_batch_to_backends",
            new=AsyncMock(),
        ) as dispatch:
            await drainer.run_once()

        dispatch.assert_not_awaited()
        assert backend.lpop_calls == [cfg.valkey_drain_chunk_size]


class TestConstruction:
    def test_cadence_seconds_from_config(self):
        cfg = LogServiceConfig(valkey_drain_interval_seconds=7.5)
        drainer = LogDrainer(cfg)
        assert drainer.cadence_seconds == 7.5

    def test_leader_only_and_skip_ephemeral(self):
        from dynastore.tools.background_service import Leadership, PodPolicy

        drainer = LogDrainer(LogServiceConfig())
        assert drainer.leadership is Leadership.LEADER_ONLY
        assert drainer.pod_policy is PodPolicy.SKIP_EPHEMERAL

    def test_lease_renewal_mode_is_heartbeat(self):
        """#2900: default cadence (2s) is far faster than the lease TTL, so
        this service holds tenure across ticks instead of re-electing per tick."""
        from dynastore.tools.background_service import LeaseRenewalMode

        drainer = LogDrainer(LogServiceConfig())
        assert drainer.lease_renewal_mode is LeaseRenewalMode.HEARTBEAT
