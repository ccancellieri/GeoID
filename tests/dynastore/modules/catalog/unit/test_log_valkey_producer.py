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

"""Unit tests for the Valkey-buffered log producer seam (#2833) —
``LogService._dispatch_to_backends`` / ``_push_to_valkey_queue``.

Pure-mock style: a fake ``ListCacheBackend`` stands in for
``ValkeyCacheBackend`` so these tests never touch a real Valkey server
(``test_cache_valkey_list_ops.py`` covers the real backend's RPUSH/LTRIM/
LPOP semantics).
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.catalog import log_manager
from dynastore.modules.catalog.log_manager import LogEntryCreate, LogService
from dynastore.modules.catalog.log_service_config import LogServiceConfig
from dynastore.tools import cache as cache_tools


# ---------------------------------------------------------------------------
# Fake ListCacheBackend — records pushed/popped entries without Valkey.
# ---------------------------------------------------------------------------


class _FakeListBackend:
    name = "fake-list"
    priority = 50

    def __init__(self) -> None:
        self.pushed: list = []
        self.rpush_calls: list = []
        self.raise_on_push: Exception | None = None

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
        self.rpush_calls.append((key, list(values), max_len))
        if self.raise_on_push is not None:
            raise self.raise_on_push
        self.pushed.extend(values)
        return max(0, len(self.pushed) - max_len)

    async def lpop_many(self, key, count):
        chunk = self.pushed[:count]
        self.pushed = self.pushed[count:]
        return chunk


def _entry(**overrides) -> LogEntryCreate:
    base = dict(catalog_id="cat1", event_type="test.event", level="INFO", message="hi")
    base.update(overrides)
    return LogEntryCreate(**base)


@pytest.fixture
def fresh_cache_manager(monkeypatch):
    """Isolated CacheManager per test — mirrors
    test_usage_counter_valkey.py's TestBackendResolution fixture."""
    manager = cache_tools.CacheManager()
    monkeypatch.setattr(cache_tools, "_cache_manager", manager)
    return manager


@pytest.fixture(autouse=True)
def _reset_drop_warning_state():
    """The drop-warning cooldown state is module-level (mirrors
    AsyncBufferAggregator's per-instance version) — reset it so tests
    don't leak rate-limit state into each other."""
    log_manager._queue_dropped_since_warning = 0
    log_manager._last_queue_drop_warning = 0.0
    yield
    log_manager._queue_dropped_since_warning = 0
    log_manager._last_queue_drop_warning = 0.0


# ---------------------------------------------------------------------------
# _push_to_valkey_queue
# ---------------------------------------------------------------------------


class TestPushToValkeyQueue:
    @pytest.mark.asyncio
    async def test_pushes_when_list_backend_registered(self, fresh_cache_manager):
        backend = _FakeListBackend()
        fresh_cache_manager.register_backend(backend)
        cfg = LogServiceConfig(valkey_queue_key="logs:q", valkey_queue_max_len=100)

        with patch(
            "dynastore.modules.catalog.log_service_config.load",
            new=AsyncMock(return_value=cfg),
        ):
            ok = await log_manager._push_to_valkey_queue([_entry()])

        assert ok is True
        assert len(backend.rpush_calls) == 1
        key, values, max_len = backend.rpush_calls[0]
        assert key == "logs:q"
        assert max_len == 100
        assert len(values) == 1

    @pytest.mark.asyncio
    async def test_round_trips_via_json(self, fresh_cache_manager):
        backend = _FakeListBackend()
        fresh_cache_manager.register_backend(backend)
        cfg = LogServiceConfig()
        entry = _entry(id="abc123", message="round trip me")

        with patch(
            "dynastore.modules.catalog.log_service_config.load",
            new=AsyncMock(return_value=cfg),
        ):
            await log_manager._push_to_valkey_queue([entry])

        raw = backend.pushed[0]
        restored = LogEntryCreate.model_validate_json(raw)
        assert restored.id == "abc123"
        assert restored.message == "round trip me"
        assert restored.timestamp == entry.timestamp

    @pytest.mark.asyncio
    async def test_falls_back_when_no_list_backend_registered(self, fresh_cache_manager):
        # Only the default local backend is registered — it has no list ops.
        ok = await log_manager._push_to_valkey_queue([_entry()])
        assert ok is False

    @pytest.mark.asyncio
    async def test_falls_back_on_valkey_error(self, fresh_cache_manager):
        backend = _FakeListBackend()
        backend.raise_on_push = ConnectionError("boom")
        fresh_cache_manager.register_backend(backend)
        cfg = LogServiceConfig()

        with patch(
            "dynastore.modules.catalog.log_service_config.load",
            new=AsyncMock(return_value=cfg),
        ):
            ok = await log_manager._push_to_valkey_queue([_entry()])

        assert ok is False

    @pytest.mark.asyncio
    async def test_warns_on_drop(self, fresh_cache_manager, caplog):
        backend = _FakeListBackend()
        backend.pushed = [b"x"] * 150  # pre-seed past the cap
        fresh_cache_manager.register_backend(backend)
        cfg = LogServiceConfig(valkey_queue_max_len=100)

        with patch(
            "dynastore.modules.catalog.log_service_config.load",
            new=AsyncMock(return_value=cfg),
        ), caplog.at_level(
            logging.WARNING, logger="dynastore.modules.catalog.log_manager"
        ):
            await log_manager._push_to_valkey_queue([_entry()])

        assert any("Valkey log queue at cap" in r.message for r in caplog.records)


class TestDropWarningRateLimit:
    def test_first_drop_warns(self, caplog):
        with caplog.at_level(
            logging.WARNING, logger="dynastore.modules.catalog.log_manager"
        ):
            log_manager._warn_queue_drop(5)
        assert any("dropped 5" in r.message for r in caplog.records)

    def test_second_drop_within_cooldown_is_suppressed(self, caplog):
        with caplog.at_level(
            logging.WARNING, logger="dynastore.modules.catalog.log_manager"
        ):
            log_manager._warn_queue_drop(3)
            log_manager._warn_queue_drop(2)
        warnings = [r for r in caplog.records if "Valkey log queue at cap" in r.message]
        assert len(warnings) == 1


# ---------------------------------------------------------------------------
# LogService._dispatch_to_backends — the producer seam itself
# ---------------------------------------------------------------------------


class TestDispatchToBackendsSeam:
    @pytest.mark.asyncio
    async def test_buffered_flush_prefers_valkey_when_available(
        self, fresh_cache_manager
    ):
        backend = _FakeListBackend()
        fresh_cache_manager.register_backend(backend)
        cfg = LogServiceConfig()
        service = LogService()

        with patch(
            "dynastore.modules.catalog.log_service_config.load",
            new=AsyncMock(return_value=cfg),
        ), patch(
            "dynastore.modules.catalog.log_manager.write_batch_to_backends",
            new=AsyncMock(),
        ) as direct:
            await service._dispatch_to_backends([_entry()])

        assert len(backend.pushed) == 1
        direct.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_falls_back_to_direct_dispatch_without_valkey(
        self, fresh_cache_manager
    ):
        service = LogService()

        with patch(
            "dynastore.modules.catalog.log_manager.write_batch_to_backends",
            new=AsyncMock(),
        ) as direct:
            entries = [_entry()]
            await service._dispatch_to_backends(entries)

        direct.assert_awaited_once_with(entries)

    @pytest.mark.asyncio
    async def test_immediate_bypasses_valkey_even_when_available(
        self, fresh_cache_manager
    ):
        """immediate=True writes exist for Cloud Run scale-to-zero
        reliability — they must never be parked in the queue."""
        backend = _FakeListBackend()
        fresh_cache_manager.register_backend(backend)
        service = LogService()

        with patch(
            "dynastore.modules.catalog.log_manager.write_batch_to_backends",
            new=AsyncMock(),
        ) as direct:
            entries = [_entry(id="immediate-1")]
            await service._dispatch_to_backends(entries, immediate=True)

        assert backend.pushed == []
        direct.assert_awaited_once_with(entries)

    @pytest.mark.asyncio
    async def test_empty_entries_is_a_noop(self, fresh_cache_manager):
        service = LogService()
        with patch(
            "dynastore.modules.catalog.log_manager.write_batch_to_backends",
            new=AsyncMock(),
        ) as direct:
            await service._dispatch_to_backends([])
        direct.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_log_event_immediate_true_sets_immediate_flag(
        self, fresh_cache_manager
    ):
        """End-to-end through log_event: a registered LogBackendProtocol
        (so the call doesn't degrade to stdlib logging) plus a registered
        Valkey ListCacheBackend — immediate must still bypass it."""
        backend = _FakeListBackend()
        fresh_cache_manager.register_backend(backend)
        service = LogService()

        fake_log_backend = AsyncMock()
        fake_log_backend.name = "fake-log-backend"
        fake_log_backend.write_batch = AsyncMock(return_value={"status": "success"})

        with patch(
            "dynastore.modules.catalog.log_manager.get_protocol",
            return_value=[fake_log_backend],
        ):
            entry_id = await service.log_event(
                "cat1", "test.event", message="immediate write", immediate=True
            )

        assert entry_id is not None
        fake_log_backend.write_batch.assert_awaited_once()
        assert backend.pushed == []  # never touched the queue
