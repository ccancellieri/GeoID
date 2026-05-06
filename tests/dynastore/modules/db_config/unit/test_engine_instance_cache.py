"""Cycle F.5 — pin EngineInstanceCache lifecycle mechanics.

Tests use mock engine classes implementing EngineInstanceProtocol so
the cache mechanics (lazy init, eviction, double-checked locking,
``enabled=False`` handling) are exercised without touching real
runtime resources (PG pool, ES client, etc.).
"""
from __future__ import annotations

import asyncio
from typing import Any, Optional
from unittest.mock import AsyncMock

import pytest

from dynastore.modules.db_config.engine_config import (
    EngineConfig,
    EngineLifecycleConfig,
)
from dynastore.modules.db_config.engine_instance_cache import (
    EngineInstanceCache,
    EngineInstanceProtocol,
)


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


class _FakeEngine:
    """Minimal EngineInstanceProtocol impl backed by AsyncMock counters."""

    def __init__(
        self,
        *,
        enabled: bool = True,
        policy: str = "global",
        ttl_seconds: Optional[int] = None,
    ) -> None:
        self.enabled = enabled
        self.lifecycle = EngineLifecycleConfig(
            policy=policy, ttl_seconds=ttl_seconds,
        )
        self.engine_init = AsyncMock(side_effect=self._make_instance)
        self.engine_release = AsyncMock()
        self._counter = 0

    def _make_instance(self) -> str:
        self._counter += 1
        return f"instance-{self._counter}"


class _Clock:
    """Manual monotonic clock — tests advance it deterministically."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _make_resolver(refs: dict):
    def resolver(engine_ref: str):
        return refs.get(engine_ref)
    return resolver


# ---------------------------------------------------------------------------
# Lazy init + caching
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_lazy_inits_on_first_call():
    eng = _FakeEngine()
    cache = EngineInstanceCache(engine_resolver=_make_resolver({"pg": eng}))
    instance = await cache.get("pg")
    assert instance == "instance-1"
    eng.engine_init.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_returns_cached_on_second_call():
    eng = _FakeEngine()
    cache = EngineInstanceCache(engine_resolver=_make_resolver({"pg": eng}))
    a = await cache.get("pg")
    b = await cache.get("pg")
    assert a is b
    assert eng.engine_init.await_count == 1


@pytest.mark.asyncio
async def test_get_concurrent_first_calls_share_one_init():
    """Per-ref lock prevents double-instantiation under concurrent
    callers — the engine_init coroutine fires exactly once."""
    eng = _FakeEngine()
    cache = EngineInstanceCache(engine_resolver=_make_resolver({"pg": eng}))
    results = await asyncio.gather(
        cache.get("pg"), cache.get("pg"), cache.get("pg"),
    )
    assert results == ["instance-1", "instance-1", "instance-1"]
    assert eng.engine_init.await_count == 1


@pytest.mark.asyncio
async def test_get_distinct_refs_yield_independent_instances():
    """Each engine_ref gets its own engine_init call (no cross-ref
    sharing).  ``_FakeEngine`` numbers instances per-engine, so both
    engines produce ``"instance-1"`` independently — what we pin is
    that each engine's ``engine_init`` was awaited exactly once."""
    eng_pg = _FakeEngine()
    eng_es = _FakeEngine()
    cache = EngineInstanceCache(
        engine_resolver=_make_resolver({"pg": eng_pg, "es": eng_es}),
    )
    await cache.get("pg")
    await cache.get("es")
    assert eng_pg.engine_init.await_count == 1
    assert eng_es.engine_init.await_count == 1


# ---------------------------------------------------------------------------
# Resolution failures
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_unknown_ref_raises_keyerror():
    cache = EngineInstanceCache(engine_resolver=_make_resolver({}))
    with pytest.raises(KeyError, match=r"not registered"):
        await cache.get("nonexistent")


@pytest.mark.asyncio
async def test_get_disabled_engine_raises_runtimeerror():
    """``enabled=False`` (operator maintenance window) → 503 semantics
    via RuntimeError; caller surfaces appropriately."""
    eng = _FakeEngine(enabled=False)
    cache = EngineInstanceCache(engine_resolver=_make_resolver({"pg": eng}))
    with pytest.raises(RuntimeError, match=r"disabled"):
        await cache.get("pg")
    eng.engine_init.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_engine_without_protocol_methods_raises_typeerror():
    """An engine config that doesn't implement EngineInstanceProtocol
    cannot participate in the cache."""

    class _Bare:
        enabled = True
        lifecycle = EngineLifecycleConfig()

    cache = EngineInstanceCache(engine_resolver=_make_resolver({"bare": _Bare()}))
    with pytest.raises(TypeError, match=r"EngineInstanceProtocol"):
        await cache.get("bare")


# ---------------------------------------------------------------------------
# Eviction (manual + sweep)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_evict_calls_engine_release():
    eng = _FakeEngine()
    cache = EngineInstanceCache(engine_resolver=_make_resolver({"pg": eng}))
    instance = await cache.get("pg")
    assert await cache.evict("pg") is True
    eng.engine_release.assert_awaited_once_with(instance)


@pytest.mark.asyncio
async def test_evict_unknown_ref_returns_false():
    cache = EngineInstanceCache(engine_resolver=_make_resolver({}))
    assert await cache.evict("never-cached") is False


@pytest.mark.asyncio
async def test_evict_swallows_engine_release_exceptions():
    """``engine_release`` failures are best-effort — logged but the
    entry is still removed from the cache."""
    eng = _FakeEngine()
    eng.engine_release = AsyncMock(side_effect=RuntimeError("teardown boom"))
    cache = EngineInstanceCache(engine_resolver=_make_resolver({"pg": eng}))
    await cache.get("pg")
    assert await cache.evict("pg") is True
    # Re-getting after eviction triggers a new init (entry was removed).
    await cache.get("pg")
    assert eng.engine_init.await_count == 2


@pytest.mark.asyncio
async def test_sweep_evicts_expired_ttl_lru_entries():
    clock = _Clock()
    eng = _FakeEngine(policy="ttl_lru", ttl_seconds=300)
    cache = EngineInstanceCache(
        engine_resolver=_make_resolver({"pg": eng}),
        clock=clock,
    )
    await cache.get("pg")
    assert eng.engine_init.await_count == 1
    # Within TTL — sweep does not evict.
    clock.advance(100)
    assert await cache.sweep() == 0
    # Beyond TTL — sweep evicts.
    clock.advance(250)  # 350s since last access
    assert await cache.sweep() == 1
    eng.engine_release.assert_awaited_once()


@pytest.mark.asyncio
async def test_sweep_does_not_evict_global_policy():
    """``policy="global"`` entries never evict regardless of age."""
    clock = _Clock()
    eng = _FakeEngine(policy="global")
    cache = EngineInstanceCache(
        engine_resolver=_make_resolver({"pg": eng}),
        clock=clock,
    )
    await cache.get("pg")
    clock.advance(100_000)  # arbitrary far future
    assert await cache.sweep() == 0
    eng.engine_release.assert_not_awaited()


@pytest.mark.asyncio
async def test_sweep_drops_orphan_entry_when_engine_deregistered():
    """If the engine is gone from the resolver between get() and sweep,
    the entry is dropped without calling release (engine class is gone)."""
    eng = _FakeEngine()
    refs = {"pg": eng}
    cache = EngineInstanceCache(engine_resolver=_make_resolver(refs))
    await cache.get("pg")
    refs.pop("pg")
    assert await cache.sweep() == 1


@pytest.mark.asyncio
async def test_get_refreshes_last_accessed_to_prevent_eviction():
    """Re-accessing an entry within TTL refreshes last_accessed so
    the next sweep does not evict (LRU semantics)."""
    clock = _Clock()
    eng = _FakeEngine(policy="ttl_lru", ttl_seconds=300)
    cache = EngineInstanceCache(
        engine_resolver=_make_resolver({"pg": eng}),
        clock=clock,
    )
    await cache.get("pg")
    clock.advance(200)
    await cache.get("pg")  # refresh
    clock.advance(200)  # 400s since first init, but only 200s since refresh
    assert await cache.sweep() == 0


# ---------------------------------------------------------------------------
# close()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_close_releases_all_cached_instances():
    eng_pg = _FakeEngine()
    eng_es = _FakeEngine()
    cache = EngineInstanceCache(
        engine_resolver=_make_resolver({"pg": eng_pg, "es": eng_es}),
    )
    await cache.get("pg")
    await cache.get("es")
    await cache.close()
    eng_pg.engine_release.assert_awaited_once()
    eng_es.engine_release.assert_awaited_once()


@pytest.mark.asyncio
async def test_close_is_idempotent():
    eng = _FakeEngine()
    cache = EngineInstanceCache(engine_resolver=_make_resolver({"pg": eng}))
    await cache.get("pg")
    await cache.close()
    await cache.close()  # No exception.
    eng.engine_release.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_after_close_raises():
    cache = EngineInstanceCache(engine_resolver=_make_resolver({}))
    await cache.close()
    with pytest.raises(RuntimeError, match=r"closed"):
        await cache.get("pg")


# ---------------------------------------------------------------------------
# Protocol structural compliance
# ---------------------------------------------------------------------------


def test_fake_engine_satisfies_protocol():
    """``_FakeEngine`` is structural-compatible with the runtime-checkable
    Protocol so isinstance-narrowing in the cache works."""
    eng = _FakeEngine()
    assert isinstance(eng, EngineInstanceProtocol)
