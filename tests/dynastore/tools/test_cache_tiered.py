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

"""Unit tests for tiered cache backend (Part A).

Covers:
- TieredAsyncBackend: multi-tier read/write/clear/exists
- Read path: L2-authoritative with version envelopes
- Write path: all tiers written with tier-specific TTLs
- Version semantics: L2 wins on higher ver; L1 wins on own fresh write
- Tombstone invalidation: keyed clear is synchronous on all tiers
- Legacy raw entries: treated as ver=0, out-versioned by any new write
- Circuit breaker: consecutive failures, threshold breach, unregister
"""
import asyncio
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.tools.cache import (
    LocalAsyncCacheBackend,
    TieredAsyncBackend,
    _ev_parse,
    _notify_backend_change,
)


def _unpack(data: Any) -> Any:
    """Test helper: extract the payload from a version envelope, or return raw for legacy."""
    if isinstance(data, dict) and "__v" in data:
        return data.get("__d")
    return data


class FakeCacheBackend:
    """Fake backend for testing tiered logic without real I/O."""

    def __init__(self, name: str, priority: int):
        self._name = name
        self._priority = priority
        self._store: Dict[str, bytes] = {}
        self._ops: List[tuple] = []  # Track operations for assertions

    @property
    def name(self) -> str:
        return self._name

    @property
    def priority(self) -> int:
        return self._priority

    async def get(self, key: str) -> Optional[bytes]:
        self._ops.append(("get", key))
        return self._store.get(key)

    async def set(
        self,
        key: str,
        value: bytes,
        *,
        ttl: Optional[float] = None,
        exist: Optional[bool] = None,
    ) -> bool:
        self._ops.append(("set", key, ttl))
        if exist is True and key not in self._store:
            return False
        if exist is False and key in self._store:
            return False
        self._store[key] = value
        return True

    async def clear(
        self,
        *,
        key: Optional[str] = None,
        namespace: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> bool:
        self._ops.append(("clear", key, namespace))
        if key is not None:
            if key in self._store:
                del self._store[key]
                return True
            return False
        if namespace is not None:
            prefix = namespace + ":"
            to_delete = [k for k in self._store if k.startswith(prefix)]
            for k in to_delete:
                del self._store[k]
            return len(to_delete) > 0
        self._store.clear()
        return True

    async def exists(self, key: str) -> bool:
        self._ops.append(("exists", key))
        return key in self._store

    async def close(self) -> None:
        self._ops.append(("close",))


class TestTieredAsyncBackend:
    """TieredAsyncBackend chaining and coherence."""

    def test_exposes_stats_attribute(self):
        """Regression: cached() pokes _backend._stats.{hits,misses,size} on
        every call regardless of backend type.  TieredAsyncBackend used
        to omit ``_stats`` and AttributeError'd on the very first cache
        miss — observed in prod as a 500 from
        ``_get_catalog_model_cached`` after Valkey/L2 was wired in.
        """
        from dynastore.models.protocols.cache import CacheStats

        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])
        assert hasattr(tiered, "_stats")
        assert isinstance(tiered._stats, CacheStats)
        # cached() does ``_backend._stats.misses += 1`` — must not raise.
        tiered._stats.misses += 1
        tiered._stats.hits += 1
        assert tiered._stats.hits == 1 and tiered._stats.misses == 1

    @pytest.mark.asyncio
    async def test_get_l1_hit_returns_l1_value(self):
        """L1-only hit (legacy raw) returns value; L2 also checked (L2-authoritative path)."""
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        # Pre-populate L1 only with a legacy (non-envelope) value.
        # L2 is empty, so L1 wins by default.
        await l1.set("key1", b"value1")

        result = await tiered.get("key1")
        assert result == b"value1"
        # Both tiers are read (L2-authoritative path always checks L2).
        assert ("get", "key1") in l1._ops
        assert ("get", "key1") in l2._ops

    @pytest.mark.asyncio
    async def test_get_l2_hit_returns_l2_and_populates_l1(self):
        """L2-only hit returns value and populates L1 for future reads."""
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        # Pre-populate L2 only with a legacy raw value (ver=0).
        await l2.set("key2", b"value2")

        result = await tiered.get("key2")
        assert result == b"value2"
        # Both tiers are read.
        assert ("get", "key2") in l1._ops
        assert ("get", "key2") in l2._ops

        # L1 populated for future reads (envelope written with TTL cap).
        l1_set = [op for op in l1._ops if op[0] == "set"]
        assert len(l1_set) == 1
        assert l1_set[0][2] == TieredAsyncBackend.DEFAULT_L1_TTL_CAP

    @pytest.mark.asyncio
    async def test_get_l2_error_falls_back_to_l1(self):
        """L2 error falls back to L1 best-effort (degraded mode)."""
        class FailingBackend(FakeCacheBackend):
            async def get(self, key: str):
                raise RuntimeError("L2 down")

        l1 = FakeCacheBackend("l1", 1000)
        l2 = FailingBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        # Pre-populate L1 with an envelope (via tiered.set, so version is stamped).
        await tiered.set("key1", b"value1")

        result = await tiered.get("key1")
        assert result == b"value1"  # L1 fallback worked

    @pytest.mark.asyncio
    async def test_get_miss_returns_none(self):
        """Value not in any tier returns None."""
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        result = await tiered.get("missing")
        assert result is None
        assert ("get", "missing") in l1._ops
        assert ("get", "missing") in l2._ops

    @pytest.mark.asyncio
    async def test_set_writes_all_tiers(self):
        """set() writes version envelopes to both L1 (short TTL) and L2 (full TTL).

        L2 write is async background, so we must wait for the task.
        """
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        result = await tiered.set("key3", b"value3", ttl=300)
        assert result is True

        # L1 has the envelope immediately; unwrap to check payload.
        assert _unpack(await l1.get("key3")) == b"value3"

        # L2 write is background — wait for pending tasks.
        await tiered.close()
        assert _unpack(await l2.get("key3")) == b"value3"

        # Verify TTLs: L1 capped at DEFAULT_L1_TTL_CAP, L2 gets full ttl.
        l1_set = [op for op in l1._ops if op[0] == "set"]
        l2_set = [op for op in l2._ops if op[0] == "set"]
        assert l1_set[0][2] == TieredAsyncBackend.DEFAULT_L1_TTL_CAP
        assert l2_set[0][2] == 300  # L2 gets full 300s TTL

    @pytest.mark.asyncio
    async def test_clear_key_clears_all_tiers(self):
        """clear(key=...) writes a tombstone to L1 and L2 synchronously.

        The tombstone makes tiered.get() return None from both tiers
        immediately after clear() returns — no drain step needed for L2.
        """
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        await tiered.set("key4", b"value4")
        await tiered.close()  # drain background L2 set
        assert _unpack(await l1.get("key4")) == b"value4"
        assert _unpack(await l2.get("key4")) == b"value4"

        result = await tiered.clear(key="key4")
        assert result is True

        # Both tiers hold tombstones; tiered.get() resolves to None immediately.
        assert await tiered.get("key4") is None
        # Direct tier reads show tombstone envelopes (not None, not the old value).
        l1_raw = await l1.get("key4")
        l2_raw = await l2.get("key4")
        assert isinstance(l1_raw, dict) and "__t" in l1_raw, "L1 must hold a tombstone"
        assert isinstance(l2_raw, dict) and "__t" in l2_raw, "L2 must hold a tombstone (sync write)"

    @pytest.mark.asyncio
    async def test_clear_namespace_clears_all_tiers(self):
        """clear(namespace=...) deletes from L1 and L2 synchronously."""
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        await tiered.set("app:key1", b"v1")
        await tiered.set("app:key2", b"v2")
        await tiered.close()  # drain background L2 sets

        result = await tiered.clear(namespace="app")
        assert result is True

        # Both tiers cleared synchronously (no drain step needed for L2).
        assert await l1.get("app:key1") is None
        assert await l1.get("app:key2") is None
        assert await l2.get("app:key1") is None
        assert await l2.get("app:key2") is None

    @pytest.mark.asyncio
    async def test_exists_checks_tiers(self):
        """exists() returns True if key in any tier."""
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        await l2.set("key5", b"value5")  # L2 only

        result = await tiered.exists("key5")
        assert result is True

    @pytest.mark.asyncio
    async def test_close_closes_all_backends(self):
        """close() calls close on all tiers."""
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        await tiered.close()

        assert ("close",) in l1._ops
        assert ("close",) in l2._ops

    def test_name_reflects_tiers(self):
        """name property combines tier names."""
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        assert "tiered" in tiered.name
        assert "l1" in tiered.name
        assert "l2" in tiered.name

    def test_priority_is_min_of_tiers(self):
        """priority is lowest (best) of all tiers."""
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        assert tiered.priority == 100  # min(1000, 100)


class TestTieredAsyncBackendL1TtlCap:
    """L1 TTL cap configurability (#930).

    The default cap (CachePluginConfig.l1_default_ttl_seconds, 30s, read
    live) bounds the per-process staleness window after a cross-process
    cache invalidate (process A invalidates Valkey L2; process B's L1 only
    converges once its local entry expires). For correctness-critical
    caches (config tiers, storage router) the cap is lowered to 2s so
    post-PUT staleness converges quickly.
    """

    @pytest.mark.asyncio
    async def test_default_l1_cap_is_live_config_default(self):
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        assert tiered._effective_l1_ttl_cap() == 30.0
        assert tiered._effective_l1_ttl_cap() == TieredAsyncBackend.DEFAULT_L1_TTL_CAP

    @pytest.mark.asyncio
    async def test_default_l1_cap_follows_live_config(self, monkeypatch):
        """With no per-site override, the cap tracks the live config value;
        an explicit l1_ttl_cap pins the site regardless of the global."""
        from dynastore.tools import cache as cache_mod

        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])
        pinned = TieredAsyncBackend(
            [FakeCacheBackend("l1", 1000), FakeCacheBackend("l2", 100)],
            l1_ttl_cap=2.0,
        )

        monkeypatch.setattr(cache_mod, "_l1_default_ttl_value", 5.0)
        assert tiered._effective_l1_ttl_cap() == 5.0
        assert pinned._effective_l1_ttl_cap() == 2.0

        await tiered.set("k", b"v", ttl=300)
        await tiered.close()
        l1_set = [op for op in l1._ops if op[0] == "set"]
        assert l1_set[0][2] == 5.0

    @pytest.mark.asyncio
    async def test_custom_l1_cap_used_on_set(self):
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2], l1_ttl_cap=2.0)

        await tiered.set("k", b"v", ttl=300)
        await tiered.close()  # wait for background L2 write
        # set() calls l1.set once (the envelope write) and l2.set once (background).
        l1_set = [op for op in l1._ops if op[0] == "set"]
        l2_set = [op for op in l2._ops if op[0] == "set"]
        assert l1_set[0][2] == 2.0   # L1 capped
        assert l2_set[0][2] == 300   # L2 full TTL

    @pytest.mark.asyncio
    async def test_custom_l1_cap_used_on_l2_populate_back(self):
        """L2 hit populates L1 with the configured cap, not the legacy 60s."""
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2], l1_ttl_cap=2.0)

        # Seed L2 directly so L1 stays empty.
        await l2.set("k", b"v")
        result = await tiered.get("k")
        assert result == b"v"

        l1_set = [op for op in l1._ops if op[0] == "set"]
        assert l1_set, "expected L1 populate-back on L2 hit"
        assert l1_set[0][2] == 2.0

    @pytest.mark.asyncio
    async def test_l1_cap_does_not_inflate_smaller_caller_ttl(self):
        """If caller passes ttl < cap, L1 uses the smaller value (not the cap)."""
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2], l1_ttl_cap=10.0)

        await tiered.set("k", b"v", ttl=3)
        await tiered.close()  # wait for background L2 write
        l1_set = [op for op in l1._ops if op[0] == "set"]
        assert l1_set[0][2] == 3  # min(3, 10)


class TestTieredAsyncBackendL2Retry:
    """L2 background write with retry (#2328).

    L1 writes are synchronous (fast, always succeed). L2+ writes are
    scheduled as background tasks with exponential backoff retry.
    """

    @pytest.mark.asyncio
    async def test_set_returns_immediately_after_l1(self):
        """set() returns True immediately after L1 write (envelope stored)."""
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        result = await tiered.set("k", b"v")
        assert result is True
        # L1 written synchronously; stored as envelope.
        assert _unpack(await l1.get("k")) == b"v"

    @pytest.mark.asyncio
    async def test_l2_write_is_background(self):
        """L2 write happens in background after set() returns."""
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        await tiered.set("k", b"v")

        # Background task pending (or already completed — accept either).
        assert len(tiered._pending_bg_tasks) > 0 or _unpack(await l2.get("k")) == b"v"

        # Wait for background task; envelope written to L2.
        await tiered.close()
        assert _unpack(await l2.get("k")) == b"v"

    @pytest.mark.asyncio
    async def test_l2_retry_on_failure(self, caplog):
        """L2 write retries with exponential backoff on failure."""
        import logging

        class FailingBackend(FakeCacheBackend):
            def __init__(self, name, priority, fail_times):
                super().__init__(name, priority)
                self._fail_times = fail_times
                self._attempts = 0

            async def set(self, key, value, *, ttl=None, exist=None):
                self._attempts += 1
                if self._attempts <= self._fail_times:
                    raise RuntimeError(f"fail attempt {self._attempts}")
                return await super().set(key, value, ttl=ttl, exist=exist)

        l1 = FakeCacheBackend("l1", 1000)
        l2 = FailingBackend("l2", 100, fail_times=2)
        tiered = TieredAsyncBackend([l1, l2], l2_retry_attempts=3, l2_retry_backoff=0.01)

        with caplog.at_level(logging.WARNING, logger="dynastore.tools.cache"):
            await tiered.set("k", b"v")
            await tiered.close()

        # L2 eventually succeeded after 2 failures; envelope written.
        assert _unpack(await l2.get("k")) == b"v"
        assert l2._attempts == 3

    @pytest.mark.asyncio
    async def test_l2_retry_exhausted_logs_warning(self, caplog):
        """When L2 retry exhausted, warning logged (TTL cap will self-heal)."""
        import logging

        class AlwaysFailingBackend(FakeCacheBackend):
            async def set(self, key, value, *, ttl=None, exist=None):
                raise RuntimeError("always fails")

        l1 = FakeCacheBackend("l1", 1000)
        l2 = AlwaysFailingBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2], l2_retry_attempts=2, l2_retry_backoff=0.01)

        with caplog.at_level(logging.WARNING, logger="dynastore.tools.cache"):
            await tiered.set("k", b"v")
            await tiered.close()

        assert any("L2 cache set failed after 2 attempts" in r.getMessage() for r in caplog.records)
        assert any("TTL cap will self-heal" in r.getMessage() for r in caplog.records)

    @pytest.mark.asyncio
    async def test_clear_key_is_synchronous_on_all_tiers(self):
        """Keyed clear writes a tombstone synchronously to L1 and L2.

        Unlike a value set (where L2 is background), the tombstone must be
        visible cluster-wide immediately so that L2-authoritative reads in
        other processes return None right away.
        """
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        await tiered.set("k", b"v")
        await tiered.close()  # drain background L2 write

        result = await tiered.clear(key="k")
        assert result is True

        # Both tiers have tombstones immediately — no drain needed.
        l1_raw = await l1.get("k")
        l2_raw = await l2.get("k")
        assert isinstance(l1_raw, dict) and "__t" in l1_raw, "L1 must hold tombstone"
        assert isinstance(l2_raw, dict) and "__t" in l2_raw, "L2 must hold tombstone synchronously"

        # tiered.get() resolves the tombstone to a miss.
        assert await tiered.get("k") is None

    @pytest.mark.asyncio
    async def test_clear_l2_failure_logs_warning_l1_tombstone_guards(self, caplog):
        """If L2 tombstone write fails, a warning is logged; L1 tombstone still guards."""
        import logging

        class FailingSetBackend(FakeCacheBackend):
            async def set(self, key, value, *, ttl=None, exist=None):
                raise RuntimeError("L2 down")

        l1 = FakeCacheBackend("l1", 1000)
        l2 = FailingSetBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        with caplog.at_level(logging.WARNING, logger="dynastore.tools.cache"):
            result = await tiered.clear(key="k")

        assert result is True
        # Warning emitted for L2 failure.
        assert any("tombstone write failed" in r.getMessage() for r in caplog.records)
        # L1 tombstone written successfully; local reads return None.
        l1_raw = await l1.get("k")
        assert isinstance(l1_raw, dict) and "__t" in l1_raw

    @pytest.mark.asyncio
    async def test_close_waits_for_pending_tasks(self):
        """close() waits for all pending background tasks."""
        import asyncio

        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        await tiered.set("k1", b"v1")
        await tiered.set("k2", b"v2")

        # Pending tasks exist
        pending_before = len(tiered._pending_bg_tasks)

        await tiered.close()

        # All tasks completed; envelopes written to L2.
        assert len(tiered._pending_bg_tasks) == 0
        assert _unpack(await l2.get("k1")) == b"v1"
        assert _unpack(await l2.get("k2")) == b"v2"


class TestVersionedL2AuthoritativeRead:
    """Version-stamped L2-authoritative read semantics.

    Every stored value is an envelope ``{"__v": ver, "__d": payload}`` where
    ``ver = time.time_ns()`` at write time.  ``get()`` reads both L1 and L2
    and returns the value from the tier with the **higher version**.

    This gives two guarantees simultaneously:
    - Cross-instance accuracy: a newer write from another instance propagates
      to local L1 on the next read (L2 wins with higher ver).
    - Read-your-write: an immediate local read after ``set()`` returns the
      fresh L1 value because L1.ver > stale L2.ver (L1 wins).
    """

    @pytest.mark.asyncio
    async def test_set_then_immediate_get_returns_new_value(self):
        """Immediate get() after set() returns the freshly-written value.

        L2 still holds a legacy (unversioned, ver=0) value when L2's background
        write has not yet run.  L1 holds the fresh envelope (ver=T >> 0), so L1
        wins the version comparison and the new value is returned.
        """
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        # Seed L2 with a legacy (unversioned, ver=0) value.
        await l2.set("key", b"old")

        # Write new value — L1 gets envelope(ver=T); L2 background write NOT yet run.
        await tiered.set("key", b"new")

        # get() reads L1 (ver=T) and L2 (ver=0, legacy); L1 wins.
        result = await tiered.get("key")
        assert result == b"new", (
            "L1 envelope (fresh ver) must beat the stale legacy L2 value"
        )
        # L1 still holds the fresh envelope (not downgraded by the stale L2 read).
        assert _unpack(await l1.get("key")) == b"new"

    @pytest.mark.asyncio
    async def test_clear_then_get_does_not_re_serve_cleared_value(self):
        """After clear(), get() returns None immediately (tombstone in both tiers).

        The tombstone is written synchronously to L1 and L2, so no drain step
        is needed to make the invalidation visible.
        """
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        await tiered.set("key", b"value")
        await tiered.close()  # drain background L2 set
        assert _unpack(await l1.get("key")) == b"value"
        assert _unpack(await l2.get("key")) == b"value"

        result = await tiered.clear(key="key")
        assert result is True

        # Tombstone is in both tiers immediately — no drain needed.
        assert await tiered.get("key") is None

    @pytest.mark.asyncio
    async def test_stale_l2_lower_ver_does_not_overwrite_fresher_l1(self):
        """A stale L2 entry (lower ver) must not clobber a fresher L1 entry.

        Simulates another instance writing an older value to L2 while the local
        instance has a newer write in L1.
        """
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        import time

        # Write "old" to L2 with a low version (simulating an older write).
        old_ver = 1
        await l2.set("key", {"__v": old_ver, "__d": b"old"})

        # Write "new" to L1 with a higher version (our recent local write).
        new_ver = time.time_ns()
        await l1.set("key", {"__v": new_ver, "__d": b"new"})

        # get() must pick L1 (new_ver > old_ver).
        result = await tiered.get("key")
        assert result == b"new", "L1 higher version must win over stale L2 lower version"

        # L1 must retain the fresh value — not downgraded by L2 read-back.
        assert _unpack(await l1.get("key")) == b"new"

    @pytest.mark.asyncio
    async def test_newer_l2_higher_ver_wins_and_refreshes_l1(self):
        """A newer L2 entry (higher ver, from another instance) wins and updates L1.

        Simulates a write from another Cloud Run instance propagating via L2.
        Local L1 still holds the old value.  After get(), L1 is refreshed with
        the newer L2 value so subsequent reads hit L1 with the up-to-date data.
        """
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        import time

        # L1 has an older value.
        old_ver = 1
        await l1.set("key", {"__v": old_ver, "__d": b"old"})

        # L2 has a newer value (simulating another instance's write).
        new_ver = time.time_ns()
        await l2.set("key", {"__v": new_ver, "__d": b"new"})

        # get() reads both; L2 (new_ver) wins.
        result = await tiered.get("key")
        assert result == b"new", "L2 higher version must win and be returned"

        # L1 must now hold the refreshed value from L2.
        assert _unpack(await l1.get("key")) == b"new"

    @pytest.mark.asyncio
    async def test_legacy_raw_l2_entry_is_tolerated_and_out_versioned(self):
        """A legacy (non-envelope) L2 entry is treated as ver=0.

        During rolling deployment, older instances may have stored raw values
        without envelopes.  A new write via set() stamps ver=T >> 0, so L1
        always beats the legacy L2 value.  The test also verifies that parsing
        a legacy raw entry does not raise.
        """
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        # Legacy raw value in L2 (no envelope, no version).
        await l2.set("key", b"legacy")

        # _ev_parse must tolerate it: ver=0, value=raw, not a tombstone.
        ver, val, tomb = _ev_parse(b"legacy")
        assert ver == 0
        assert val == b"legacy"
        assert tomb is False

        # Write a new value — L1 gets ver=T >> 0.
        await tiered.set("key", b"new")

        # get() picks L1 (T > 0) over the legacy L2.
        result = await tiered.get("key")
        assert result == b"new", "New envelope write must out-version legacy L2 entry"


class TestConfigCachesUseTightL1Cap:
    """Static check: correctness-critical config caches pin a tight L1 TTL (#930).

    The caches share ``DEFAULT_CONFIG_CACHE_L1_TTL`` (value: 2 seconds) rather
    than repeating the literal — the invariant guarded here is that each
    declares *some* ``l1_ttl=`` bound wired to that shared constant, not a
    bespoke or missing one.
    """

    def test_catalog_and_collection_caches_pass_l1_ttl(self):
        import inspect
        from dynastore.modules.catalog import config_service
        from dynastore.tools.cache import DEFAULT_CONFIG_CACHE_L1_TTL

        assert DEFAULT_CONFIG_CACHE_L1_TTL == 2

        src = inspect.getsource(config_service)
        # Catalog-tier
        assert 'namespace="catalog_config"' in src
        catalog_decl = src.split("_catalog_config_cache")[0].rsplit("@cached", 1)[1]
        assert "l1_ttl=DEFAULT_CONFIG_CACHE_L1_TTL" in catalog_decl, (
            "_catalog_config_cache must declare l1_ttl=DEFAULT_CONFIG_CACHE_L1_TTL "
            "to bound the post-PUT staleness window across Cloud Run processes (#930)"
        )
        # Collection-tier
        assert 'namespace="collection_config"' in src
        collection_decl = (
            src.split("_collection_config_cache")[0].rsplit("@cached", 1)[1]
        )
        assert "l1_ttl=DEFAULT_CONFIG_CACHE_L1_TTL" in collection_decl

    def test_platform_config_cache_passes_l1_ttl(self):
        import inspect
        from dynastore.modules.db_config.platform_config_service import (
            PlatformConfigService,
        )
        from dynastore.tools.cache import DEFAULT_CONFIG_CACHE_L1_TTL

        assert DEFAULT_CONFIG_CACHE_L1_TTL == 2

        src = inspect.getsource(PlatformConfigService._setup_cache)
        assert "l1_ttl=DEFAULT_CONFIG_CACHE_L1_TTL" in src, (
            "platform_config cache must declare l1_ttl=DEFAULT_CONFIG_CACHE_L1_TTL (#930)"
        )

    def test_storage_router_cache_passes_l1_ttl(self):
        import inspect
        from dynastore.modules.storage import router
        from dynastore.tools.cache import DEFAULT_CONFIG_CACHE_L1_TTL

        assert DEFAULT_CONFIG_CACHE_L1_TTL == 2

        src = inspect.getsource(router)
        router_decl = src.split("_resolve_driver_ids_cached")[0].rsplit("@cached", 1)[1]
        assert "l1_ttl=DEFAULT_CONFIG_CACHE_L1_TTL" in router_decl


class TestCachedConditionOnRead:
    """Read-side ``condition=`` enforcement in ``cached()``.

    Guards against stale entries (written before the condition was added,
    or by a code path that bypassed it) being served forever via the
    fast path.  Without this, the only mitigation was the entry's TTL —
    which doesn't help when the original write had ``ttl=None``.
    """

    @pytest.mark.asyncio
    async def test_condition_failing_value_is_evicted_and_refetched(self):
        """Stale value already in the backend → condition fails → evicted.

        Pins ``cached()`` to a named backend so the test can inject a
        stale entry into the same backing store the wrapper reads from.
        """
        from dynastore.tools.cache import cached, get_cache_manager

        class _NamedBackend(LocalAsyncCacheBackend):
            @property
            def name(self) -> str:  # type: ignore[override]
                return "cond-read-test-backend"

        backend = _NamedBackend()
        get_cache_manager().register_backend(backend)

        try:
            calls = {"n": 0}

            @cached(
                maxsize=8,
                ttl=60,
                namespace="cond_read_test",
                backend="cond-read-test-backend",
                condition=lambda v: isinstance(v, dict) and v.get("status") == "ready",
            )
            async def fetch(key: str):
                calls["n"] += 1
                return {"status": "ready", "rev": calls["n"]}

            # Warm cache with a 'ready' result, then mutate the stored entry
            # to a stale 'provisioning' value — mirrors a pre-condition
            # write that survives in the backend after a deploy adds it.
            await fetch("k1")
            assert calls["n"] == 1

            stored_keys = list(backend._store.keys())
            assert stored_keys, "expected a cache entry under the namespace"
            key = stored_keys[0]
            backend._store[key].value = {"status": "provisioning"}

            # Fast path sees the stale value → condition fails → entry
            # cleared → wrapped function re-invoked → fresh ready cached.
            result = await fetch("k1")
            assert result == {"status": "ready", "rev": 2}
            assert calls["n"] == 2

            # Post-eviction read serves the cached ready value.
            result = await fetch("k1")
            assert result == {"status": "ready", "rev": 2}
            assert calls["n"] == 2
        finally:
            get_cache_manager().unregister_backend(backend)


@pytest.mark.skipif(
    __import__("importlib.util").util.find_spec("valkey") is None,
    reason="valkey not installed (optional dependency)",
)
class TestCircuitBreaker:
    """Circuit breaker logic on ValkeyCacheBackend.

    Skipped if valkey is not installed (it's an optional dependency for services).
    """

    def test_consecutive_failures_increment_counter(self):
        """Each failure increments _consecutive_failures."""
        import sys
        from unittest.mock import MagicMock

        # Mock the valkey module before importing ValkeyCacheBackend
        sys.modules["valkey.asyncio"] = MagicMock()
        sys.modules["valkey"] = MagicMock()

        try:
            from dynastore.tools.cache_valkey import ValkeyCacheBackend

            backend = ValkeyCacheBackend(client=MagicMock())
            assert backend._consecutive_failures == 0

            # Simulate a failure
            backend._record_failure()
            assert backend._consecutive_failures == 1

            backend._record_failure()
            assert backend._consecutive_failures == 2
        finally:
            sys.modules.pop("valkey.asyncio", None)
            sys.modules.pop("valkey", None)

    def test_success_resets_failure_counter(self):
        """Each success resets _consecutive_failures to 0."""
        import sys
        from unittest.mock import MagicMock

        sys.modules["valkey.asyncio"] = MagicMock()
        sys.modules["valkey"] = MagicMock()

        try:
            from dynastore.tools.cache_valkey import ValkeyCacheBackend

            backend = ValkeyCacheBackend(client=MagicMock())
            backend._consecutive_failures = 2

            backend._record_success()
            assert backend._consecutive_failures == 0
        finally:
            sys.modules.pop("valkey.asyncio", None)
            sys.modules.pop("valkey", None)

    def test_circuit_breaker_default_constant(self):
        """Sane default exposed for the circuit-breaker threshold.

        The env-var path (VALKEY_CIRCUIT_BREAKER_THRESHOLD) was removed —
        the runtime value flows from CachePluginConfig.circuit_breaker_threshold
        into ValkeyCacheBackend.__init__; this constant is the fallback when
        the PluginConfig knob is unset.
        """
        from dynastore.tools.cache_valkey import _VALKEY_CIRCUIT_BREAKER_DEFAULT

        assert _VALKEY_CIRCUIT_BREAKER_DEFAULT >= 1
        assert isinstance(_VALKEY_CIRCUIT_BREAKER_DEFAULT, int)

    async def test_set_failure_logs_at_warning_with_exc_info(self, caplog):
        """#590: per-op failures emit at WARNING with exc_info so the
        per-shard ConnectionError/TimeoutError actually lands in Cloud Run."""
        import logging
        import sys

        sys.modules["valkey.asyncio"] = MagicMock()
        sys.modules["valkey"] = MagicMock()

        try:
            from dynastore.tools.cache_valkey import ValkeyCacheBackend

            backend = ValkeyCacheBackend(client=MagicMock())
            backend._client = MagicMock()
            backend._client.set = AsyncMock(side_effect=RuntimeError("boom-from-shard"))

            with caplog.at_level(logging.WARNING, logger="dynastore.tools.cache_valkey"):
                result = await backend.set("k", b"v")

            assert result is False
            warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
            assert any("ValkeyCacheBackend.set failed" in r.getMessage() for r in warnings)
            # exc_info populated → traceback survives the INFO floor in Cloud Run.
            assert any(r.exc_info is not None and "boom-from-shard" in str(r.exc_info[1]) for r in warnings)
        finally:
            sys.modules.pop("valkey.asyncio", None)
            sys.modules.pop("valkey", None)


class TestMaxDistributedTTL:
    """Test max_distributed_ttl parameter on @cached decorator (#2328).

    When ttl=None and distributed=True, the decorator must cap the effective TTL
    to prevent unbounded staleness when L2 (Valkey) is unreliable and invalidations
    are dropped.
    """

    async def test_ttl_none_distributed_true_uses_default_cap(self):
        """ttl=None + distributed=True → uses DEFAULT_MAX_DISTRIBUTED_TTL on L2."""
        from dynastore.tools.cache import (
            cached,
            DEFAULT_MAX_DISTRIBUTED_TTL,
            get_cache_manager,
        )

        l2 = FakeCacheBackend("l2", 100)
        get_cache_manager().register_backend(l2)
        try:
            calls = {"n": 0}

            @cached(
                maxsize=8,
                namespace="max_ttl_test",
                distributed=True,
                stale_grace=0,  # isolate this test from the #2902 default grace window
            )
            async def fetch(key: str):
                calls["n"] += 1
                return {"rev": calls["n"]}

            await fetch("k1")
            assert calls["n"] == 1

            # L2 write is background - wait for completion
            await asyncio.sleep(0.1)

            # L2 (distributed tier) received set with DEFAULT_MAX_DISTRIBUTED_TTL
            set_ops = [op for op in l2._ops if op[0] == "set"]
            assert len(set_ops) >= 1, f"L2 ops: {l2._ops}"
            ttl_passed = set_ops[0][2]
            assert ttl_passed == DEFAULT_MAX_DISTRIBUTED_TTL
        finally:
            get_cache_manager().unregister_backend(l2)

    async def test_ttl_none_distributed_false_uses_none(self):
        """ttl=None + distributed=False → no cap (local-only caches can live forever)."""
        from dynastore.tools.cache import cached

        calls = {"n": 0}

        @cached(maxsize=8, namespace="local_uncapped", distributed=False)
        async def fetch(key: str):
            calls["n"] += 1
            return {"rev": calls["n"]}

        await fetch("k1")
        assert calls["n"] == 1

    async def test_explicit_max_distributed_ttl_overrides_default(self):
        """max_distributed_ttl=300 overrides DEFAULT_MAX_DISTRIBUTED_TTL."""
        from dynastore.tools.cache import cached, get_cache_manager

        l2 = FakeCacheBackend("l2", 100)
        get_cache_manager().register_backend(l2)
        try:
            calls = {"n": 0}

            @cached(
                maxsize=8,
                namespace="custom_cap",
                distributed=True,
                max_distributed_ttl=300,
                stale_grace=0,  # isolate this test from the #2902 default grace window
            )
            async def fetch(key: str):
                calls["n"] += 1
                return {"rev": calls["n"]}

            await fetch("k1")
            assert calls["n"] == 1

            # L2 write is background - wait for completion
            await asyncio.sleep(0.1)

            set_ops = [op for op in l2._ops if op[0] == "set"]
            assert len(set_ops) >= 1, f"L2 ops: {l2._ops}"
            ttl_passed = set_ops[0][2]
            assert ttl_passed == 300.0
        finally:
            get_cache_manager().unregister_backend(l2)

    async def test_explicit_ttl_unaffected_by_cap(self):
        """Explicit ttl=60 is used verbatim; max_distributed_ttl ignored."""
        from dynastore.tools.cache import cached, get_cache_manager

        l2 = FakeCacheBackend("l2", 100)
        get_cache_manager().register_backend(l2)
        try:
            calls = {"n": 0}

            @cached(
                maxsize=8,
                ttl=60,
                namespace="explicit_ttl",
                distributed=True,
                max_distributed_ttl=300,
                stale_grace=0,  # isolate this test from the #2902 default grace window
            )
            async def fetch(key: str):
                calls["n"] += 1
                return {"rev": calls["n"]}

            await fetch("k1")
            assert calls["n"] == 1

            # L2 write is background - wait for completion
            await asyncio.sleep(0.1)

            set_ops = [op for op in l2._ops if op[0] == "set"]
            assert len(set_ops) >= 1, f"L2 ops: {l2._ops}"
            ttl_passed = set_ops[0][2]
            assert ttl_passed == 60.0
        finally:
            get_cache_manager().unregister_backend(l2)

    async def test_infinity_disables_cap(self):
        """max_distributed_ttl=float('inf') disables the cap → ttl=None passed to L2."""
        from dynastore.tools.cache import cached, get_cache_manager

        l2 = FakeCacheBackend("l2", 100)
        get_cache_manager().register_backend(l2)
        try:
            calls = {"n": 0}

            @cached(
                maxsize=8,
                namespace="inf_cap",
                distributed=True,
                max_distributed_ttl=float("inf"),
            )
            async def fetch(key: str):
                calls["n"] += 1
                return {"rev": calls["n"]}

            await fetch("k1")
            assert calls["n"] == 1

            # L2 write is background - wait for completion
            await asyncio.sleep(0.1)

            set_ops = [op for op in l2._ops if op[0] == "set"]
            assert len(set_ops) >= 1, f"L2 ops: {l2._ops}"
            ttl_passed = set_ops[0][2]
            assert ttl_passed is None
        finally:
            get_cache_manager().unregister_backend(l2)


class TestLocalCacheGetOrSetStaleGraceFallback:
    """LocalCache.get_or_set(stale_grace=...) — bounded slow path + degraded
    serving on rebuild failure/timeout (#2902).

    ``LocalCache.get_or_set`` is the exact root-cause API from #2902: an
    unbounded per-key lock wait + factory() call with no stale fallback,
    which under DB pool starvation rides every queued waiter to the
    caller's own gateway timeout.
    """

    def _make_cache(self):
        from dynastore.models.protocols.cache import CacheConfig
        from dynastore.tools.cache import LocalAsyncCacheBackend, LocalCache

        backend = LocalAsyncCacheBackend()
        config = CacheConfig(namespace="lc_stale_test")
        return LocalCache(backend=backend, config=config)

    @pytest.mark.asyncio
    async def test_fresh_value_preferred_when_not_expired(self):
        """A hit within the logical TTL behaves exactly as before — no
        staleness bookkeeping observable, factory called only once."""
        cache = self._make_cache()
        calls = {"n": 0}

        async def factory():
            calls["n"] += 1
            return {"rev": calls["n"]}

        first = await cache.get_or_set("k1", factory, ttl=60)
        second = await cache.get_or_set("k1", factory, ttl=60)
        assert first == second == {"rev": 1}
        assert calls["n"] == 1

    @pytest.mark.asyncio
    async def test_stale_served_on_factory_error(self, caplog):
        """factory() raising after the logical TTL expires serves the
        still-in-grace stale value instead of propagating."""
        import logging

        cache = self._make_cache()
        calls = {"n": 0}

        async def factory():
            calls["n"] += 1
            if calls["n"] == 1:
                return {"rev": 1}
            raise RuntimeError("rebuild failed")

        first = await cache.get_or_set("k1", factory, ttl=0.05, stale_grace=60)
        assert first == {"rev": 1}
        await asyncio.sleep(0.15)  # logical ttl expires; grace keeps it alive

        with caplog.at_level(logging.WARNING, logger="dynastore.tools.cache"):
            second = await cache.get_or_set("k1", factory, ttl=0.05, stale_grace=60)

        assert second == {"rev": 1}
        assert any(
            "cache_stale_served" in r.getMessage() and "reason=error" in r.getMessage()
            for r in caplog.records
        )

    @pytest.mark.asyncio
    async def test_stale_served_on_slow_path_timeout(self, caplog, monkeypatch):
        """A factory() call that outlives slow_path_timeout_seconds is
        abandoned and the stale value is served instead of hanging."""
        import logging

        from dynastore.tools import cache as cache_mod

        monkeypatch.setattr(
            cache_mod, "_load_slow_path_timeout", AsyncMock(return_value=0.05)
        )

        cache = self._make_cache()
        calls = {"n": 0}

        async def factory():
            calls["n"] += 1
            if calls["n"] == 1:
                return {"rev": 1}
            await asyncio.sleep(1.0)  # exceeds the 0.05s slow-path timeout
            return {"rev": 2}

        first = await cache.get_or_set("k1", factory, ttl=0.05, stale_grace=60)
        assert first == {"rev": 1}
        await asyncio.sleep(0.15)

        with caplog.at_level(logging.WARNING, logger="dynastore.tools.cache"):
            second = await cache.get_or_set("k1", factory, ttl=0.05, stale_grace=60)

        assert second == {"rev": 1}
        assert any(
            "cache_stale_served" in r.getMessage() and "reason=timeout" in r.getMessage()
            for r in caplog.records
        )

    @pytest.mark.asyncio
    async def test_no_stale_failing_factory_propagates_promptly(self):
        """No prior value to fall back on — the factory's exception propagates."""
        cache = self._make_cache()

        async def factory():
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            await cache.get_or_set("k1", factory, ttl=60, stale_grace=60)

    @pytest.mark.asyncio
    async def test_grace_zero_disables_stale_serving(self):
        """stale_grace=0 keeps today's behavior: expired entries are gone,
        a failing rebuild propagates."""
        cache = self._make_cache()
        calls = {"n": 0}

        async def factory():
            calls["n"] += 1
            if calls["n"] == 1:
                return {"rev": 1}
            raise RuntimeError("rebuild failed")

        first = await cache.get_or_set("k1", factory, ttl=0.05, stale_grace=0)
        assert first == {"rev": 1}
        await asyncio.sleep(0.15)

        with pytest.raises(RuntimeError, match="rebuild failed"):
            await cache.get_or_set("k1", factory, ttl=0.05, stale_grace=0)

    @pytest.mark.asyncio
    async def test_legacy_unprefixed_entry_is_not_returned_when_grace_active(self):
        """Cross-revision safety (#2902 review): a pre-existing entry under
        the plain (unprefixed) key -- as an old revision without stale-wrap
        unwrap logic would have written it -- must not be misread as the
        cached value once stale-wrapping (and its ``sv1|`` key prefix) is
        active. The versioned key misses and the value is freshly rebuilt.
        """
        cache = self._make_cache()
        calls = {"n": 0}

        async def factory():
            calls["n"] += 1
            return {"rev": calls["n"]}

        legacy_key = cache._full_key("k1")
        await cache._backend.set(legacy_key, {"rev": "legacy-corrupt-if-read"}, ttl=60)

        result = await cache.get_or_set("k1", factory, ttl=60, stale_grace=60)

        assert result == {"rev": 1}
        assert calls["n"] == 1


class TestCachedStaleGraceFallback:
    """@cached(stale_grace=...) — bounded slow path + degraded serving on
    rebuild failure/timeout (#2902), the production call path used by
    every ``@cached``-decorated function in the codebase.
    """

    @pytest.mark.asyncio
    async def test_fresh_value_preferred_when_not_expired(self):
        from dynastore.tools.cache import cached

        calls = {"n": 0}

        @cached(maxsize=8, ttl=60, namespace="cached_stale_fresh_test", distributed=False)
        async def fetch(key: str):
            calls["n"] += 1
            return {"rev": calls["n"]}

        first = await fetch("k1")
        second = await fetch("k1")
        assert first == second == {"rev": 1}
        assert calls["n"] == 1

    @pytest.mark.asyncio
    async def test_stale_served_on_factory_error(self, caplog):
        import logging

        from dynastore.tools.cache import cached

        calls = {"n": 0}

        @cached(
            maxsize=8,
            ttl=0.05,
            stale_grace=60,
            namespace="cached_stale_error_test",
            distributed=False,
        )
        async def fetch(key: str):
            calls["n"] += 1
            if calls["n"] == 1:
                return {"rev": 1}
            raise RuntimeError("rebuild failed")

        first = await fetch("k1")
        assert first == {"rev": 1}
        await asyncio.sleep(0.15)

        with caplog.at_level(logging.WARNING, logger="dynastore.tools.cache"):
            second = await fetch("k1")

        assert second == {"rev": 1}
        assert any(
            "cache_stale_served" in r.getMessage() and "reason=error" in r.getMessage()
            for r in caplog.records
        )

    @pytest.mark.asyncio
    async def test_stale_served_on_slow_path_timeout(self, caplog, monkeypatch):
        import logging

        from dynastore.tools import cache as cache_mod
        from dynastore.tools.cache import cached

        monkeypatch.setattr(
            cache_mod, "_load_slow_path_timeout", AsyncMock(return_value=0.05)
        )

        calls = {"n": 0}

        @cached(
            maxsize=8,
            ttl=0.05,
            stale_grace=60,
            namespace="cached_stale_timeout_test",
            distributed=False,
        )
        async def fetch(key: str):
            calls["n"] += 1
            if calls["n"] == 1:
                return {"rev": 1}
            await asyncio.sleep(1.0)  # exceeds the 0.05s slow-path timeout
            return {"rev": 2}

        first = await fetch("k1")
        assert first == {"rev": 1}
        await asyncio.sleep(0.15)

        with caplog.at_level(logging.WARNING, logger="dynastore.tools.cache"):
            second = await fetch("k1")

        assert second == {"rev": 1}
        assert any(
            "cache_stale_served" in r.getMessage() and "reason=timeout" in r.getMessage()
            for r in caplog.records
        )

    @pytest.mark.asyncio
    async def test_no_stale_failing_factory_propagates_promptly(self):
        from dynastore.tools.cache import cached

        @cached(
            maxsize=8,
            ttl=60,
            stale_grace=60,
            namespace="cached_stale_no_prior_test",
            distributed=False,
        )
        async def fetch(key: str):
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            await fetch("k1")

    @pytest.mark.asyncio
    async def test_grace_zero_disables_stale_serving(self):
        from dynastore.tools.cache import cached

        calls = {"n": 0}

        @cached(
            maxsize=8,
            ttl=0.05,
            stale_grace=0,
            namespace="cached_stale_disabled_test",
            distributed=False,
        )
        async def fetch(key: str):
            calls["n"] += 1
            if calls["n"] == 1:
                return {"rev": 1}
            raise RuntimeError("rebuild failed")

        first = await fetch("k1")
        assert first == {"rev": 1}
        await asyncio.sleep(0.15)

        with pytest.raises(RuntimeError, match="rebuild failed"):
            await fetch("k1")


class TestCachedCrossRevisionKeyCompat:
    """@cached key version-prefixing (#2902 review).

    Valkey is shared across rolling-deploy revisions. An old-revision
    process has no unwrap logic for the stale-wrapped envelope, so if it
    read a wrapped entry its fast path would return the wrapper dict AS
    the cached value -- silently corrupting every cached endpoint until
    the entry expires. Storing stale-wrapped entries under a
    version-prefixed key (``sv1|``) means old code simply misses (one
    cold rebuild, absorbed by the bounded slow path) instead of
    misreading, and makes rollback safe (the old revision resumes
    reading its own unprefixed keys).
    """

    @pytest.mark.asyncio
    async def test_legacy_unprefixed_entry_is_not_returned_when_grace_active(self):
        import inspect

        from dynastore.tools.cache import (
            LocalAsyncCacheBackend,
            _make_cache_key,
            cached,
            get_cache_manager,
        )

        class _NamedBackend(LocalAsyncCacheBackend):
            @property
            def name(self) -> str:  # type: ignore[override]
                return "xrev-legacy-test-backend"

        backend = _NamedBackend()
        get_cache_manager().register_backend(backend)
        try:
            calls = {"n": 0}

            @cached(
                maxsize=8,
                ttl=60,
                namespace="xrev_legacy_test",
                backend="xrev-legacy-test-backend",
            )
            async def fetch(key: str):
                calls["n"] += 1
                return {"rev": calls["n"]}

            # Simulate an old-revision entry: a raw (unwrapped) value stored
            # under the plain key -- the format @cached used before #2902.
            sig = inspect.signature(fetch.__wrapped__)
            legacy_key = _make_cache_key(
                "xrev_legacy_test", ("k1",), {}, sig, set(), False
            )
            await backend.set(legacy_key, {"rev": "legacy-corrupt-if-read"}, ttl=60)

            result = await fetch("k1")

            # Must NOT return the legacy raw value -- a miss + fresh rebuild.
            assert result == {"rev": 1}
            assert calls["n"] == 1
        finally:
            get_cache_manager().unregister_backend(backend)

    @pytest.mark.asyncio
    async def test_cache_invalidate_clears_the_prefixed_entry(self):
        from dynastore.tools.cache import (
            LocalAsyncCacheBackend,
            cached,
            get_cache_manager,
        )

        class _NamedBackend(LocalAsyncCacheBackend):
            @property
            def name(self) -> str:  # type: ignore[override]
                return "xrev-invalidate-test-backend"

        backend = _NamedBackend()
        get_cache_manager().register_backend(backend)
        try:
            calls = {"n": 0}

            @cached(
                maxsize=8,
                ttl=60,
                namespace="xrev_invalidate_test",
                backend="xrev-invalidate-test-backend",
            )
            async def fetch(key: str):
                calls["n"] += 1
                return {"rev": calls["n"]}

            first = await fetch("k1")
            assert first == {"rev": 1}

            stored_keys = list(backend._store.keys())
            assert len(stored_keys) == 1
            assert stored_keys[0].startswith("sv1|"), (
                "a stale-wrapped entry must live under the versioned key"
            )

            fetch.cache_invalidate("k1")
            assert list(backend._store.keys()) == []

            second = await fetch("k1")
            assert second == {"rev": 2}
            assert calls["n"] == 2
        finally:
            get_cache_manager().unregister_backend(backend)

    @pytest.mark.asyncio
    async def test_grace_disabled_keeps_unprefixed_keys(self):
        """stale_grace=0 stores under today's plain key -- unaffected by
        the version-prefix mechanism."""
        from dynastore.tools.cache import (
            LocalAsyncCacheBackend,
            cached,
            get_cache_manager,
        )

        class _NamedBackend(LocalAsyncCacheBackend):
            @property
            def name(self) -> str:  # type: ignore[override]
                return "xrev-disabled-test-backend"

        backend = _NamedBackend()
        get_cache_manager().register_backend(backend)
        try:

            @cached(
                maxsize=8,
                ttl=60,
                stale_grace=0,
                namespace="xrev_disabled_test",
                backend="xrev-disabled-test-backend",
            )
            async def fetch(key: str):
                return {"rev": 1}

            await fetch("k1")

            stored_keys = list(backend._store.keys())
            assert len(stored_keys) == 1
            assert not stored_keys[0].startswith("sv1|")
        finally:
            get_cache_manager().unregister_backend(backend)


class TestSlowPathTimeoutRecursionGuard:
    """The slow-path-timeout loader reads CachePluginConfig through the
    config service, whose loads are themselves ``@cached`` — without a
    re-entrancy guard a miss on the config entry recursed back into the
    loader until the Python stack limit (post-#2921 boot storms)."""

    def _reset_memo(self, monkeypatch):
        import dynastore.tools.cache as cache_mod

        monkeypatch.setattr(cache_mod, "_slow_path_timeout_checked_at", 0.0)
        monkeypatch.setattr(
            cache_mod,
            "_slow_path_timeout_value",
            cache_mod._DEFAULT_SLOW_PATH_TIMEOUT_SECONDS,
        )

    @pytest.mark.asyncio
    async def test_reentrant_config_load_terminates_and_returns_value(
        self, monkeypatch
    ):
        import dynastore.tools.cache as cache_mod
        import dynastore.tools.discovery as discovery

        self._reset_memo(monkeypatch)
        calls = 0

        class _FakeConfigs:
            async def get_config(self, _cls):
                nonlocal calls
                calls += 1
                # What the @cached config path does on a miss: it re-enters
                # the loader. The guard must answer with the memo/default
                # instead of recursing.
                inner = await cache_mod._load_slow_path_timeout()
                assert inner == cache_mod._DEFAULT_SLOW_PATH_TIMEOUT_SECONDS

                class _Cfg:
                    slow_path_timeout_seconds = 42.0

                return _Cfg()

        monkeypatch.setattr(discovery, "get_protocol", lambda _p: _FakeConfigs())

        out = await cache_mod._load_slow_path_timeout()
        assert out == 42.0
        assert calls == 1  # the guard prevented a second config load

    @pytest.mark.asyncio
    async def test_memo_skips_config_reload_within_refresh_window(
        self, monkeypatch
    ):
        import dynastore.tools.cache as cache_mod
        import dynastore.tools.discovery as discovery

        self._reset_memo(monkeypatch)

        class _Cfg:
            slow_path_timeout_seconds = 42.0

        class _FakeConfigs:
            async def get_config(self, _cls):
                return _Cfg()

        monkeypatch.setattr(discovery, "get_protocol", lambda _p: _FakeConfigs())
        assert await cache_mod._load_slow_path_timeout() == 42.0

        def _boom(_p):
            raise AssertionError("config service must not be hit within the window")

        monkeypatch.setattr(discovery, "get_protocol", _boom)
        assert await cache_mod._load_slow_path_timeout() == 42.0

    @pytest.mark.asyncio
    async def test_failure_is_stamped_and_not_hammered(self, monkeypatch):
        import dynastore.tools.cache as cache_mod
        import dynastore.tools.discovery as discovery

        self._reset_memo(monkeypatch)
        attempts = 0

        def _failing(_p):
            nonlocal attempts
            attempts += 1
            raise RuntimeError("config service down")

        monkeypatch.setattr(discovery, "get_protocol", _failing)
        assert (
            await cache_mod._load_slow_path_timeout()
            == cache_mod._DEFAULT_SLOW_PATH_TIMEOUT_SECONDS
        )
        assert (
            await cache_mod._load_slow_path_timeout()
            == cache_mod._DEFAULT_SLOW_PATH_TIMEOUT_SECONDS
        )
        assert attempts == 1  # second call served by the failure-stamped memo


class TestSharedRebuildNotCancelledByCallers:
    """The slow-path rebuild must never be cancelled by a caller giving up:
    cancelling in-flight DB work mid-query poisons asyncpg connections
    (#2900) — the post-#2921 state-11 storm was seeded exactly this way by
    the old ``asyncio.timeout`` around ``factory()``."""

    def _make_cache(self, ns):
        from dynastore.models.protocols.cache import CacheConfig
        from dynastore.tools.cache import LocalAsyncCacheBackend, LocalCache

        return LocalCache(
            backend=LocalAsyncCacheBackend(), config=CacheConfig(namespace=ns)
        )

    @pytest.mark.asyncio
    async def test_timeout_leaves_rebuild_running_and_cache_written(
        self, monkeypatch
    ):
        import dynastore.tools.cache as cache_mod

        monkeypatch.setattr(
            cache_mod, "_load_slow_path_timeout", AsyncMock(return_value=0.05)
        )
        cache = self._make_cache("t_detach")

        release = asyncio.Event()
        cancelled = False
        calls = 0

        async def factory():
            nonlocal cancelled, calls
            calls += 1
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled = True
                raise
            return "fresh"

        # No stale value exists -> the caller's timeout must propagate...
        with pytest.raises(TimeoutError):
            await cache.get_or_set("k", factory, ttl=60)

        # ...but the rebuild keeps running: releasing it completes the write.
        release.set()
        await asyncio.sleep(0.05)
        assert cancelled is False
        assert calls == 1

        # The detached rebuild's write landed: a new get_or_set is a fast-path
        # hit and never invokes its (poisoned-pill) factory.
        async def must_not_run():
            raise AssertionError("factory must not run — value was written")

        assert await cache.get_or_set("k", must_not_run, ttl=60) == "fresh"

    @pytest.mark.asyncio
    async def test_caller_cancellation_does_not_cancel_rebuild(
        self, monkeypatch
    ):
        import dynastore.tools.cache as cache_mod

        monkeypatch.setattr(
            cache_mod, "_load_slow_path_timeout", AsyncMock(return_value=30.0)
        )
        cache = self._make_cache("t_detach_cancel")

        release = asyncio.Event()
        cancelled = False

        async def factory():
            nonlocal cancelled
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled = True
                raise
            return "fresh"

        caller = asyncio.create_task(cache.get_or_set("k", factory, ttl=60))
        await asyncio.sleep(0.02)  # let the rebuild start
        caller.cancel()  # simulates a client disconnect killing the request
        with pytest.raises(asyncio.CancelledError):
            await caller

        release.set()
        await asyncio.sleep(0.05)
        assert cancelled is False

        async def must_not_run():
            raise AssertionError("factory must not run — value was written")

        assert await cache.get_or_set("k", must_not_run, ttl=60) == "fresh"

    @pytest.mark.asyncio
    async def test_concurrent_misses_share_one_rebuild(self, monkeypatch):
        import dynastore.tools.cache as cache_mod

        monkeypatch.setattr(
            cache_mod, "_load_slow_path_timeout", AsyncMock(return_value=30.0)
        )
        cache = self._make_cache("t_detach_share")
        calls = 0

        async def factory():
            nonlocal calls
            calls += 1
            await asyncio.sleep(0.05)
            return "fresh"

        results = await asyncio.gather(
            *(cache.get_or_set("k", factory, ttl=60) for _ in range(5))
        )
        assert results == ["fresh"] * 5
        assert calls == 1


class TestRebuildExceptionHygiene:
    """A failing detached rebuild retrieves its own exception and logs one
    structured WARN line instead of surfacing asyncio's raw "exception was
    never retrieved" traceback (#2900/#2902); single-flight bookkeeping still
    cleans up so the next miss on the same key retries."""

    def _make_cache(self, ns):
        from dynastore.models.protocols.cache import CacheConfig
        from dynastore.tools.cache import LocalAsyncCacheBackend, LocalCache

        return LocalCache(
            backend=LocalAsyncCacheBackend(), config=CacheConfig(namespace=ns)
        )

    @pytest.mark.asyncio
    async def test_abandoned_rebuild_failure_logs_structured_warning(
        self, caplog, monkeypatch
    ):
        import logging

        import dynastore.tools.cache as cache_mod

        monkeypatch.setattr(
            cache_mod, "_load_slow_path_timeout", AsyncMock(return_value=0.05)
        )
        cache = self._make_cache("t_rebuild_fail")
        release = asyncio.Event()

        async def factory():
            await release.wait()
            raise RuntimeError("boom")

        with caplog.at_level(logging.WARNING, logger="dynastore.tools.cache"):
            # No stale value available -- the caller's timeout propagates
            # while the rebuild keeps running detached.
            with pytest.raises(TimeoutError):
                await cache.get_or_set("k", factory, ttl=60)

            release.set()
            await asyncio.sleep(0.05)  # let the detached rebuild fail + log

        assert any(
            "cache_rebuild_failed" in r.getMessage()
            and "key=" in r.getMessage()
            and "error_type=RuntimeError" in r.getMessage()
            and "boom" in r.getMessage()
            for r in caplog.records
        )

    @pytest.mark.asyncio
    async def test_next_miss_retries_after_abandoned_failure(self, monkeypatch):
        import dynastore.tools.cache as cache_mod

        monkeypatch.setattr(
            cache_mod, "_load_slow_path_timeout", AsyncMock(return_value=0.05)
        )
        cache = self._make_cache("t_rebuild_retry")
        calls = 0

        async def failing_factory():
            nonlocal calls
            calls += 1
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError):
            await cache.get_or_set("k", failing_factory, ttl=60)
        assert calls == 1

        async def ok_factory():
            nonlocal calls
            calls += 1
            return "fresh"

        assert await cache.get_or_set("k", ok_factory, ttl=60) == "fresh"
        assert calls == 2


class TestRebuildConcurrencySemaphore:
    """At most ``max_concurrent_detached_rebuilds`` detached rebuild tasks run
    at once across the process (#2902): under a CPU-throttling storm, one
    detached rebuild per cache-miss key can otherwise stampede the small DB
    pool. Excess candidates queue for a semaphore slot inside their own
    detached task, not on the caller's side."""

    def _make_cache(self, ns):
        from dynastore.models.protocols.cache import CacheConfig
        from dynastore.tools.cache import LocalAsyncCacheBackend, LocalCache

        return LocalCache(
            backend=LocalAsyncCacheBackend(), config=CacheConfig(namespace=ns)
        )

    def _reset_semaphore(self, monkeypatch):
        import dynastore.tools.cache as cache_mod

        monkeypatch.setattr(cache_mod, "_rebuild_semaphore", None)
        monkeypatch.setattr(cache_mod, "_rebuild_semaphore_limit", 0)

    @pytest.mark.asyncio
    async def test_concurrent_rebuilds_are_bounded(self, monkeypatch):
        import dynastore.tools.cache as cache_mod

        self._reset_semaphore(monkeypatch)
        monkeypatch.setattr(
            cache_mod, "_load_slow_path_timeout", AsyncMock(return_value=30.0)
        )
        monkeypatch.setattr(
            cache_mod, "_load_max_concurrent_rebuilds", AsyncMock(return_value=2)
        )

        cache = self._make_cache("t_rebuild_sem")
        in_flight = 0
        max_in_flight = 0
        release = asyncio.Event()

        async def factory():
            nonlocal in_flight, max_in_flight
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            await release.wait()
            in_flight -= 1
            return "fresh"

        # 5 distinct keys -> 5 distinct detached tasks, no single-flight sharing.
        tasks = [
            asyncio.create_task(cache.get_or_set(f"k{i}", factory, ttl=60))
            for i in range(5)
        ]
        await asyncio.sleep(0.05)  # let everything queue/start
        assert max_in_flight == 2  # bounded by the semaphore, not all 5 at once

        release.set()
        results = await asyncio.gather(*tasks)
        assert results == ["fresh"] * 5

    @pytest.mark.asyncio
    async def test_semaphore_rebuilds_on_config_change(self, monkeypatch):
        """A changed limit (hot-reload) replaces the semaphore; holders of
        the superseded semaphore still release normally."""
        import dynastore.tools.cache as cache_mod

        self._reset_semaphore(monkeypatch)
        monkeypatch.setattr(
            cache_mod, "_load_max_concurrent_rebuilds", AsyncMock(return_value=2)
        )
        sem1 = await cache_mod._get_rebuild_semaphore()
        assert sem1._value == 2  # type: ignore[attr-defined]

        monkeypatch.setattr(
            cache_mod, "_load_max_concurrent_rebuilds", AsyncMock(return_value=5)
        )
        sem2 = await cache_mod._get_rebuild_semaphore()
        assert sem2 is not sem1
        assert sem2._value == 5  # type: ignore[attr-defined]

    def test_default_limit_is_conservative(self):
        import dynastore.tools.cache as cache_mod

        assert cache_mod._DEFAULT_MAX_CONCURRENT_REBUILDS == 4

    def test_cache_plugin_config_field_default_matches(self):
        from dynastore.modules.cache.cache_config import CachePluginConfig

        assert CachePluginConfig().max_concurrent_detached_rebuilds == 4
