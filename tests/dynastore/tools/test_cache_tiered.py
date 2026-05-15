"""Unit tests for tiered cache backend (Part A).

Covers:
- TieredAsyncBackend: multi-tier read/write/clear/exists
- Read path: L1 hit, L2 hit (populate L1), L3 (populate L1+L2)
- Write path: all tiers written with tier-specific TTLs
- Circuit breaker: consecutive failures, threshold breach, unregister
- Backend re-resolution: after circuit breaker trip
"""
import asyncio
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.tools.cache import (
    LocalAsyncCacheBackend,
    TieredAsyncBackend,
    _notify_backend_change,
)


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
    async def test_get_l1_hit(self):
        """Value in L1 is returned immediately without querying L2."""
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        # Pre-populate L1 only
        await l1.set("key1", b"value1")

        result = await tiered.get("key1")
        assert result == b"value1"
        assert ("get", "key1") in l1._ops
        assert ("get", "key1") not in l2._ops  # L2 not queried

    @pytest.mark.asyncio
    async def test_get_l2_hit_populates_l1(self):
        """Value in L2 is returned and L1 is populated."""
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        # Pre-populate L2 only
        await l2.set("key2", b"value2")

        result = await tiered.get("key2")
        assert result == b"value2"
        assert ("get", "key2") in l1._ops  # L1 queried first
        assert ("get", "key2") in l2._ops  # L2 queried after L1 miss

        # L1 now has the value (populated with short TTL)
        l1_get = [op for op in l1._ops if op[0] == "get"]
        l1_set = [op for op in l1._ops if op[0] == "set"]
        assert len(l1_set) == 1
        assert l1_set[0][2] == 60  # Short L1 TTL

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
        """set() writes to both L1 (short TTL) and L2 (full TTL)."""
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        result = await tiered.set("key3", b"value3", ttl=300)
        assert result is True

        # Both tiers have the value
        assert await l1.get("key3") == b"value3"
        assert await l2.get("key3") == b"value3"

        # Verify TTLs: L1 gets min(ttl, 60), L2 gets full ttl
        l1_set = [op for op in l1._ops if op[0] == "set"]
        l2_set = [op for op in l2._ops if op[0] == "set"]
        assert l1_set[0][2] == 60  # L1 gets 60s TTL (min of 300, 60)
        assert l2_set[0][2] == 300  # L2 gets full 300s TTL

    @pytest.mark.asyncio
    async def test_clear_key_clears_all_tiers(self):
        """clear(key=...) deletes from all tiers."""
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        # Set in both tiers
        await tiered.set("key4", b"value4")
        assert await tiered.get("key4") == b"value4"

        # Clear the key
        result = await tiered.clear(key="key4")
        assert result is True

        # Both tiers cleared
        assert await l1.get("key4") is None
        assert await l2.get("key4") is None

    @pytest.mark.asyncio
    async def test_clear_namespace_clears_all_tiers(self):
        """clear(namespace=...) deletes from all tiers."""
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        # Set multiple keys in namespace
        await tiered.set("app:key1", b"v1")
        await tiered.set("app:key2", b"v2")

        # Clear namespace
        result = await tiered.clear(namespace="app")
        assert result is True

        # Both tiers cleared
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

            backend = ValkeyCacheBackend("redis://localhost:6379")
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

            backend = ValkeyCacheBackend("redis://localhost:6379")
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

            backend = ValkeyCacheBackend("redis://localhost:6379")
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
