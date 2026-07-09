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

"""Process-wide L1 byte budget: sizing estimator, GDSF-lite eviction,
live-config plumbing.

Every local backend counts its entries' approximate deep size against one
shared per-process budget (``CachePluginConfig.l1_memory_percent`` of the
worker's memory share). When the total exceeds the budget, the backend
holding the most bytes evicts its lowest value-per-byte entry
(``(hits + 1) / size_bytes`` over its LRU-oldest sample) — large,
rarely-hit entries go before small hot ones.
"""
from __future__ import annotations

import sys

import pytest

from dynastore.tools import cache as cache_mod
from dynastore.tools.cache import (
    LocalAsyncCacheBackend,
    LocalSyncCacheBackend,
    _approx_deep_size,
    _L1MemoryBudget,
)
from dynastore.models.protocols.cache import CacheItemPriority


# ---------------------------------------------------------------------------
#  Sizing estimator
# ---------------------------------------------------------------------------


class TestApproxDeepSize:
    def test_scalars_match_getsizeof(self):
        for obj in ("abc", b"abc", 42, 3.14, True, None):
            assert _approx_deep_size(obj) == sys.getsizeof(obj)

    def test_containers_grow_with_content(self):
        small = _approx_deep_size({"a": "x"})
        big = _approx_deep_size({"a": "x" * 10_000})
        assert big > small + 9_000

    def test_nested_object_counts_dict(self):
        class Payload:
            def __init__(self):
                self.blob = "y" * 5_000

        assert _approx_deep_size(Payload()) > 5_000

    def test_large_container_is_scaled_not_walked(self):
        # 10k equal-size items, sampled at _L1_SIZING_MAX_ITEMS and scaled:
        # the estimate must be in the right order of magnitude.
        data = ["z" * 100 for _ in range(10_000)]
        est = _approx_deep_size(data)
        assert est > 100 * 10_000  # at least the raw character payload

    def test_cyclic_structure_terminates(self):
        a: dict = {}
        a["self"] = a
        assert _approx_deep_size(a) > 0

    def test_slots_object(self):
        class Slotted:
            __slots__ = ("blob",)

            def __init__(self):
                self.blob = "s" * 2_000

        assert _approx_deep_size(Slotted()) > 2_000


# ---------------------------------------------------------------------------
#  Budget accounting + eviction
# ---------------------------------------------------------------------------


@pytest.fixture
def tiny_budget(monkeypatch):
    """A fresh budget manager with a fixed 64KiB base and 100% percent,
    swapped in for the module singleton so test backends register there."""
    budget = _L1MemoryBudget()
    budget._base_bytes = 64 * 1024
    budget._base_resolved = True
    monkeypatch.setattr(cache_mod, "_l1_budget", budget)
    monkeypatch.setattr(cache_mod, "_l1_memory_percent_value", 100.0)
    return budget


class TestByteAccounting:
    @pytest.mark.asyncio
    async def test_set_get_clear_keeps_bytes_consistent(self, tiny_budget):
        b = LocalAsyncCacheBackend(max_size=100)
        assert b._bytes == 0
        await b.set("k1", b"v" * 1000)
        assert b._bytes > 1000
        after_one = b._bytes
        await b.set("k1", b"v")  # replace shrinks
        assert 0 < b._bytes < after_one
        await b.clear(key="k1")
        assert b._bytes == 0

    @pytest.mark.asyncio
    async def test_expiry_releases_bytes(self, tiny_budget):
        b = LocalAsyncCacheBackend(max_size=100)
        await b.set("k", b"v" * 1000, ttl=-1)  # already expired
        assert b._bytes > 0
        assert await b.get("k") is None
        assert b._bytes == 0

    @pytest.mark.asyncio
    async def test_count_cap_eviction_releases_bytes(self, tiny_budget):
        b = LocalAsyncCacheBackend(max_size=3)
        for i in range(6):
            await b.set(f"k{i}", b"v" * 100)
        assert len(b._store) < 3 + 1
        assert b._bytes == sum(e.size_bytes for e in b._store.values())


class TestBudgetEviction:
    @pytest.mark.asyncio
    async def test_total_stays_under_budget(self, tiny_budget):
        b = LocalAsyncCacheBackend(max_size=10_000)
        for i in range(200):
            await b.set(f"k{i}", b"x" * 1024)
        assert tiny_budget.total_bytes() <= tiny_budget.budget_bytes()
        assert b._stats.evictions > 0

    @pytest.mark.asyncio
    async def test_largest_backend_is_trimmed_first(self, tiny_budget):
        small = LocalAsyncCacheBackend(max_size=10_000)
        big = LocalAsyncCacheBackend(max_size=10_000)
        await small.set("s", b"x" * 100)
        for i in range(200):
            await big.set(f"k{i}", b"x" * 1024)
        assert big._stats.evictions > 0
        assert small._stats.evictions == 0
        assert await small.get("s") is not None

    @pytest.mark.asyncio
    async def test_frequently_hit_entry_survives(self, tiny_budget):
        """GDSF-lite: within the LRU-oldest sample, a hot small entry
        outlives a cold large one."""
        b = LocalAsyncCacheBackend(max_size=10_000)
        await b.set("hot", b"h" * 512)
        await b.set("cold", b"c" * 8192)
        for _ in range(50):
            assert await b.get("hot") is not None
        # "hot" was re-read, so it is also no longer LRU-oldest; refill the
        # LRU head with both candidates by touching nothing else and forcing
        # pressure.
        for i in range(200):
            await b.set(f"filler{i}", b"f" * 1024)
        # cold (large, 0 hits) must be gone before hot (small, 50 hits).
        assert await b.get("cold") is None
        assert await b.get("hot") is not None

    @pytest.mark.asyncio
    async def test_never_remove_entries_survive(self, tiny_budget):
        from dynastore.tools.cache import _CacheEntry

        b = LocalAsyncCacheBackend(max_size=10_000)
        pinned = _CacheEntry(
            value=b"p" * 1024,
            expires_at=None,
            priority=CacheItemPriority.NEVER_REMOVE,
            size_bytes=1024,
        )
        b._store["pinned"] = pinned
        b._bytes += 1024
        for i in range(200):
            await b.set(f"k{i}", b"x" * 1024)
        assert "pinned" in b._store

    def test_sync_backend_participates(self, tiny_budget):
        b = LocalSyncCacheBackend(max_size=10_000)
        for i in range(200):
            b.set(f"k{i}", b"x" * 1024)
        assert tiny_budget.total_bytes() <= tiny_budget.budget_bytes()
        assert b._stats.evictions > 0

    @pytest.mark.asyncio
    async def test_disabled_when_percent_zero(self, tiny_budget, monkeypatch):
        monkeypatch.setattr(cache_mod, "_l1_memory_percent_value", 0.0)
        b = LocalAsyncCacheBackend(max_size=10_000)
        for i in range(200):
            await b.set(f"k{i}", b"x" * 1024)
        assert b._stats.evictions == 0  # count cap not hit, no byte eviction

    @pytest.mark.asyncio
    async def test_disabled_when_no_memory_base(self, monkeypatch):
        budget = _L1MemoryBudget()
        budget._base_bytes = None
        budget._base_resolved = True
        monkeypatch.setattr(cache_mod, "_l1_budget", budget)
        monkeypatch.setattr(cache_mod, "_l1_memory_percent_value", 100.0)
        b = LocalAsyncCacheBackend(max_size=10_000)
        for i in range(50):
            await b.set(f"k{i}", b"x" * 1024)
        assert budget.budget_bytes() is None
        assert b._stats.evictions == 0

    @pytest.mark.asyncio
    async def test_disabled_budget_skips_sizing(self, monkeypatch):
        """With the budget off, set() must not pay the deep-size walk:
        entries are charged 0 bytes."""
        budget = _L1MemoryBudget()
        budget._base_bytes = None
        budget._base_resolved = True
        monkeypatch.setattr(cache_mod, "_l1_budget", budget)
        b = LocalAsyncCacheBackend(max_size=100)
        await b.set("k", b"x" * 4096)
        assert b._store["k"].size_bytes == 0
        assert b._bytes == 0

    @pytest.mark.asyncio
    async def test_oversized_value_is_not_admitted(self, tiny_budget):
        """A single value above _L1_ADMISSION_MAX_FRACTION of the budget
        stays out of L1 instead of evicting the working set (set() still
        reports success so tiered callers proceed to the distributed tier)."""
        b = LocalAsyncCacheBackend(max_size=10_000)
        for i in range(10):
            await b.set(f"small{i}", b"s" * 512)
        bytes_before = b._bytes
        # 64KiB budget * 0.25 = 16KiB cap; 32KiB payload must be rejected.
        assert await b.set("giant", b"g" * 32 * 1024) is True
        assert await b.get("giant") is None
        assert b._bytes == bytes_before
        assert b._stats.evictions == 0  # nothing was sacrificed for it
        for i in range(10):
            assert await b.get(f"small{i}") is not None

    @pytest.mark.asyncio
    async def test_oversized_overwrite_drops_stale_entry(self, tiny_budget):
        """Rejecting an oversized replacement must not leave the previous
        (now stale) value serving reads."""
        b = LocalAsyncCacheBackend(max_size=10_000)
        await b.set("k", b"old" * 100)
        assert await b.set("k", b"g" * 32 * 1024) is True
        assert await b.get("k") is None
        assert b._bytes == 0

    @pytest.mark.asyncio
    async def test_hits_carry_over_on_overwrite(self, tiny_budget):
        """Periodically-refreshed hot keys keep their GDSF hit history."""
        b = LocalAsyncCacheBackend(max_size=100)
        await b.set("k", b"v1")
        for _ in range(5):
            await b.get("k")
        await b.set("k", b"v2")
        assert b._store["k"].hits == 5

    @pytest.mark.asyncio
    async def test_sync_caller_never_evicts_async_backend(self, tiny_budget):
        """enforce(sync_caller=True) runs on arbitrary worker threads; async
        stores have no thread lock, so only sync backends are eligible
        victims even when an async backend holds the most bytes."""
        a = LocalAsyncCacheBackend(max_size=10_000)
        s = LocalSyncCacheBackend(max_size=10_000)
        # Fill the async backend right up to the budget without tripping it.
        for i in range(60):
            await a.set(f"a{i}", b"x" * 1000)
        async_entries = len(a._store)
        assert a._stats.evictions == 0
        # Sync writes push the process over budget; the sync-called
        # enforcement must trim only the sync backend.
        for i in range(20):
            s.set(f"s{i}", b"y" * 1000)
        assert a._stats.evictions == 0
        assert len(a._store) == async_entries
        assert s._stats.evictions > 0

    @pytest.mark.asyncio
    async def test_approx_total_tracks_exact_total(self, tiny_budget):
        """The lock-free fast-path counter follows the exact per-backend
        sums through set/replace/clear."""
        b = LocalAsyncCacheBackend(max_size=100)
        await b.set("k1", b"v" * 500)
        await b.set("k2", b"v" * 700)
        await b.set("k1", b"v" * 100)  # replace
        assert tiny_budget._approx_total == tiny_budget.total_bytes()
        await b.clear()
        assert tiny_budget._approx_total == 0


# ---------------------------------------------------------------------------
#  Live-config plumbing
# ---------------------------------------------------------------------------


class TestL1RuntimeConfig:
    def test_setter_updates_globals(self, monkeypatch):
        monkeypatch.setattr(cache_mod, "_l1_default_ttl_value", 30.0)
        monkeypatch.setattr(cache_mod, "_l1_memory_percent_value", 10.0)
        cache_mod.set_l1_runtime_config(
            l1_default_ttl_seconds=7.5, l1_memory_percent=25.0
        )
        assert cache_mod._l1_default_ttl_value == 7.5
        assert cache_mod._l1_memory_percent_value == 25.0

    def test_setter_ignores_none(self, monkeypatch):
        monkeypatch.setattr(cache_mod, "_l1_default_ttl_value", 30.0)
        cache_mod.set_l1_runtime_config(
            l1_default_ttl_seconds=None, l1_memory_percent=None
        )
        assert cache_mod._l1_default_ttl_value == 30.0

    def test_budget_percent_read_live(self, tiny_budget, monkeypatch):
        assert tiny_budget.budget_bytes() == 64 * 1024
        monkeypatch.setattr(cache_mod, "_l1_memory_percent_value", 50.0)
        assert tiny_budget.budget_bytes() == 32 * 1024
