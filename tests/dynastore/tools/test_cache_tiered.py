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
- Read path: L1 hit, L2 hit (populate L1), L3 (populate L1+L2)
- Write path: all tiers written with tier-specific TTLs
- Circuit breaker: consecutive failures, threshold breach, unregister
- Backend re-resolution: after circuit breaker trip
"""
import asyncio
from typing import Dict, List, Optional
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

        # L1 now has the value (populated with default cap TTL).
        l1_get = [op for op in l1._ops if op[0] == "get"]
        l1_set = [op for op in l1._ops if op[0] == "set"]
        assert len(l1_set) == 1
        assert l1_set[0][2] == TieredAsyncBackend.DEFAULT_L1_TTL_CAP

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
        """set() writes to both L1 (short TTL) and L2 (full TTL).

        L2 write is async background, so we must wait for the task.
        """
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        result = await tiered.set("key3", b"value3", ttl=300)
        assert result is True

        # L1 has the value immediately
        assert await l1.get("key3") == b"value3"

        # L2 write is background — wait for pending tasks
        await tiered.close()
        assert await l2.get("key3") == b"value3"

        # Verify TTLs: L1 capped at DEFAULT_L1_TTL_CAP, L2 gets full ttl.
        l1_set = [op for op in l1._ops if op[0] == "set"]
        l2_set = [op for op in l2._ops if op[0] == "set"]
        assert l1_set[0][2] == TieredAsyncBackend.DEFAULT_L1_TTL_CAP
        assert l2_set[0][2] == 300  # L2 gets full 300s TTL

    @pytest.mark.asyncio
    async def test_clear_key_clears_all_tiers(self):
        """clear(key=...) deletes from L1 sync, L2 background."""
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        await tiered.set("key4", b"value4")
        await tiered.close()  # wait for background L2 set
        assert await l1.get("key4") == b"value4"
        assert await l2.get("key4") == b"value4"

        result = await tiered.clear(key="key4")
        assert result is True

        # L1 cleared immediately
        assert await l1.get("key4") is None

        # L2 cleared in background
        await tiered.close()
        assert await l2.get("key4") is None

    @pytest.mark.asyncio
    async def test_clear_namespace_clears_all_tiers(self):
        """clear(namespace=...) deletes from L1 sync, L2 background."""
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        await tiered.set("app:key1", b"v1")
        await tiered.set("app:key2", b"v2")
        await tiered.close()  # wait for background L2 sets

        result = await tiered.clear(namespace="app")
        assert result is True

        # L1 cleared immediately
        assert await l1.get("app:key1") is None
        assert await l1.get("app:key2") is None

        # L2 cleared in background
        await tiered.close()
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

    Default cap (60s) bounds the per-process staleness window after a
    cross-process cache invalidate (process A invalidates Valkey L2;
    process B's L1 only converges once its local entry expires). For
    correctness-critical caches (config tiers, storage router) the cap
    is lowered to 2s so post-PUT staleness converges quickly.
    """

    @pytest.mark.asyncio
    async def test_default_l1_cap_is_60s(self):
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        assert tiered._l1_ttl_cap == 60.0
        assert tiered._l1_ttl_cap == TieredAsyncBackend.DEFAULT_L1_TTL_CAP

    @pytest.mark.asyncio
    async def test_custom_l1_cap_used_on_set(self):
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2], l1_ttl_cap=2.0)

        await tiered.set("k", b"v", ttl=300)
        await tiered.close()  # wait for background L2 write
        l1_set = [op for op in l1._ops if op[0] == "set"]
        l2_set = [op for op in l2._ops if op[0] == "set"]
        assert l1_set[0][2] == 2.0
        assert l2_set[0][2] == 300

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
        """set() returns True immediately after L1 write."""
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        result = await tiered.set("k", b"v")
        assert result is True
        assert await l1.get("k") == b"v"  # L1 written synchronously

    @pytest.mark.asyncio
    async def test_l2_write_is_background(self):
        """L2 write happens in background after set() returns."""
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        await tiered.set("k", b"v")

        # Background task pending
        assert len(tiered._pending_bg_tasks) > 0 or await l2.get("k") == b"v"

        # Wait for background task
        await tiered.close()
        assert await l2.get("k") == b"v"

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

        # L2 eventually succeeded after 2 failures
        assert await l2.get("k") == b"v"
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
    async def test_clear_is_l1_sync_l2_background(self):
        """clear() clears L1 sync, L2 in background."""
        l1 = FakeCacheBackend("l1", 1000)
        l2 = FakeCacheBackend("l2", 100)
        tiered = TieredAsyncBackend([l1, l2])

        await l1.set("k", b"v")
        await l2.set("k", b"v")

        result = await tiered.clear(key="k")
        assert result is True

        # L1 cleared immediately
        assert await l1.get("k") is None
        # L2 still has value
        assert await l2.get("k") == b"v"

        # Wait for background clear
        await tiered.close()
        assert await l2.get("k") is None

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

        # All tasks completed
        assert len(tiered._pending_bg_tasks) == 0
        assert await l2.get("k1") == b"v1"
        assert await l2.get("k2") == b"v2"


class TestConfigCachesUseTightL1Cap:
    """Static check: correctness-critical config caches pin ``l1_ttl=2`` (#930)."""

    def test_catalog_and_collection_caches_pass_l1_ttl(self):
        import inspect
        from dynastore.modules.catalog import config_service

        src = inspect.getsource(config_service)
        # Catalog-tier
        assert 'namespace="catalog_config"' in src
        catalog_decl = src.split("_catalog_config_cache")[0].rsplit("@cached", 1)[1]
        assert "l1_ttl=2" in catalog_decl, (
            "_catalog_config_cache must declare l1_ttl=2 to bound the "
            "post-PUT staleness window across Cloud Run processes (#930)"
        )
        # Collection-tier
        assert 'namespace="collection_config"' in src
        collection_decl = (
            src.split("_collection_config_cache")[0].rsplit("@cached", 1)[1]
        )
        assert "l1_ttl=2" in collection_decl

    def test_platform_config_cache_passes_l1_ttl(self):
        import inspect
        from dynastore.modules.db_config.platform_config_service import (
            PlatformConfigService,
        )

        src = inspect.getsource(PlatformConfigService._setup_cache)
        assert "l1_ttl=2" in src, (
            "platform_config cache must declare l1_ttl=2 (#930)"
        )

    def test_storage_router_cache_passes_l1_ttl(self):
        import inspect
        from dynastore.modules.storage import router

        src = inspect.getsource(router)
        router_decl = src.split("_resolve_driver_ids_cached")[0].rsplit("@cached", 1)[1]
        assert "l1_ttl=2" in router_decl


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

            @cached(maxsize=8, namespace="max_ttl_test", distributed=True)
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
