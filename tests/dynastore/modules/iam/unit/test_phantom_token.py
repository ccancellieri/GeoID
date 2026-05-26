"""Unit tests for the phantom-token resolution cache (#1343).

Everything is driven through a fake counting backend. Version-counter and
denylist tests monkeypatch ``phantom_token._distributed_backend`` (the raw L2
handle); resolution tests monkeypatch ``phantom_token._resolution_backend`` so
the fake stands in for the whole L1+L2 tier and failure injection hits it
directly. No real Valkey, no cache-manager registration internals.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest

from dynastore.modules.iam import phantom_token


@pytest.fixture(autouse=True)
def _reset_phantom_caches() -> Any:
    """Clear the in-process tier + version memo around every test."""
    phantom_token._reset_caches()
    yield
    phantom_token._reset_caches()


class FakeCountingBackend:
    """Minimal in-memory stand-in for a distributed CountingCacheBackend."""

    def __init__(self, *, priority: int = 100) -> None:
        self._priority = priority
        self.store: Dict[str, bytes] = {}
        self.counters: Dict[str, int] = {}
        self.get_calls = 0
        self.set_calls = 0
        self.get_count_calls = 0
        self.set_ttls: List[Optional[float]] = []
        # Optional injected failures.
        self.raise_on_get = False
        self.raise_on_set = False
        self.raise_on_exists = False
        self.raise_on_get_count = False
        self.raise_on_incr = False

    @property
    def priority(self) -> int:
        return self._priority

    @property
    def name(self) -> str:
        return "fake-l2"

    async def get(self, key: str) -> Optional[bytes]:
        self.get_calls += 1
        if self.raise_on_get:
            raise RuntimeError("boom-get")
        return self.store.get(key)

    async def set(
        self,
        key: str,
        value: bytes,
        *,
        ttl: Optional[float] = None,
        exist: Optional[bool] = None,
    ) -> bool:
        self.set_calls += 1
        self.set_ttls.append(ttl)
        if self.raise_on_set:
            raise RuntimeError("boom-set")
        self.store[key] = value
        return True

    async def exists(self, key: str) -> bool:
        if self.raise_on_exists:
            raise RuntimeError("boom-exists")
        return key in self.store

    async def get_count(self, key: str) -> Optional[int]:
        self.get_count_calls += 1
        if self.raise_on_get_count:
            raise RuntimeError("boom-count")
        return self.counters.get(key)

    async def incr(self, key: str, amount: int = 1, *, ttl: Optional[float] = None) -> int:
        if self.raise_on_incr:
            raise RuntimeError("boom-incr")
        self.counters[key] = self.counters.get(key, 0) + amount
        return self.counters[key]


def _cfg(**overrides: Any) -> Any:
    base = dict(
        phantom_token_resolution_enabled=True,
        binding_resolution_ttl_seconds=300,
        denylist_enabled=True,
        denylist_ttl_seconds=300,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _install(monkeypatch: pytest.MonkeyPatch, backend: Optional[FakeCountingBackend]) -> None:
    """Patch the raw L2 handle (version counter + denylist)."""
    monkeypatch.setattr(phantom_token, "_distributed_backend", lambda: backend)


def _install_res(monkeypatch: pytest.MonkeyPatch, backend: Optional[FakeCountingBackend]) -> None:
    """Patch the resolution tier AND the raw L2 handle to the same fake.

    The resolution path reads/writes via ``_resolution_backend`` and reads the
    version via ``_distributed_backend``; pointing both at one fake keeps the
    set/get-call assertions and failure injection on a single object.
    """
    monkeypatch.setattr(phantom_token, "_resolution_backend", lambda: backend)
    monkeypatch.setattr(phantom_token, "_distributed_backend", lambda: backend)


def _counting_resolver(value: Any):
    calls = {"n": 0}

    async def resolver() -> Any:
        calls["n"] += 1
        return value

    return resolver, calls


# --------------------------------------------------------------------------- #
# 1. phantom_token_active
# --------------------------------------------------------------------------- #
def test_active_false_when_flag_off(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, FakeCountingBackend())
    assert phantom_token.phantom_token_active(_cfg(phantom_token_resolution_enabled=False)) is False


def test_active_false_when_no_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, None)
    assert phantom_token.phantom_token_active(_cfg()) is False


def test_active_true_when_flag_on_and_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, FakeCountingBackend())
    assert phantom_token.phantom_token_active(_cfg()) is True


# --------------------------------------------------------------------------- #
# 2. get_binding_version
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_get_binding_version_no_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, None)
    assert await phantom_token.get_binding_version("iam") == 0


@pytest.mark.asyncio
async def test_get_binding_version_absent_counter(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, FakeCountingBackend())
    assert await phantom_token.get_binding_version("iam") == 0


@pytest.mark.asyncio
async def test_get_binding_version_after_bumps(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, FakeCountingBackend())
    await phantom_token.bump_binding_version("iam")
    await phantom_token.bump_binding_version("iam")
    await phantom_token.bump_binding_version("iam")
    assert await phantom_token.get_binding_version("iam") == 3


@pytest.mark.asyncio
async def test_get_binding_version_swallows_error(monkeypatch: pytest.MonkeyPatch) -> None:
    backend = FakeCountingBackend()
    backend.raise_on_get_count = True
    _install(monkeypatch, backend)
    assert await phantom_token.get_binding_version("iam") == 0  # no raise


@pytest.mark.asyncio
async def test_get_binding_version_memoized(monkeypatch: pytest.MonkeyPatch) -> None:
    backend = FakeCountingBackend()
    _install(monkeypatch, backend)

    # Two reads within the memo window -> one L2 round-trip.
    assert await phantom_token.get_binding_version("s") == 0
    assert await phantom_token.get_binding_version("s") == 0
    assert backend.get_count_calls == 1

    # A local bump evicts the memo and is reflected on the next read.
    await phantom_token.bump_binding_version("s")
    assert await phantom_token.get_binding_version("s") == 1
    assert backend.get_count_calls == 2


# --------------------------------------------------------------------------- #
# 3. bump_binding_version
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_bump_no_backend_is_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, None)
    await phantom_token.bump_binding_version("iam")  # must not raise


@pytest.mark.asyncio
async def test_bump_increments_counter(monkeypatch: pytest.MonkeyPatch) -> None:
    backend = FakeCountingBackend()
    _install(monkeypatch, backend)
    await phantom_token.bump_binding_version("cat_x")
    assert backend.counters[phantom_token._VERSION_PREFIX + "cat_x"] == 1


@pytest.mark.asyncio
async def test_bump_swallows_error(monkeypatch: pytest.MonkeyPatch) -> None:
    backend = FakeCountingBackend()
    backend.raise_on_incr = True
    _install(monkeypatch, backend)
    await phantom_token.bump_binding_version("iam")  # must not raise


# --------------------------------------------------------------------------- #
# 4. resolve_bindings_cached MISS then HIT
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_resolve_miss_then_hit(monkeypatch: pytest.MonkeyPatch) -> None:
    backend = FakeCountingBackend()
    _install_res(monkeypatch, backend)
    value = {"roles": ["editor"], "principal": {"subject_id": "u1"}}
    resolver, calls = _counting_resolver(value)

    r1 = await phantom_token.resolve_bindings_cached(
        provider="oidc", subject_id="u1", schema="cat_x", resolver=resolver, cfg=_cfg()
    )
    r2 = await phantom_token.resolve_bindings_cached(
        provider="oidc", subject_id="u1", schema="cat_x", resolver=resolver, cfg=_cfg()
    )

    assert r1 == value
    assert r2 == value
    assert calls["n"] == 1  # second call served from cache
    assert backend.set_calls == 1


# --------------------------------------------------------------------------- #
# 5. version bump invalidates
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_version_bump_invalidates(monkeypatch: pytest.MonkeyPatch) -> None:
    backend = FakeCountingBackend()
    _install_res(monkeypatch, backend)
    resolver, calls = _counting_resolver({"roles": ["viewer"]})

    await phantom_token.resolve_bindings_cached(
        provider="oidc", subject_id="u1", schema="cat_x", resolver=resolver, cfg=_cfg()
    )
    await phantom_token.bump_binding_version("cat_x")
    await phantom_token.resolve_bindings_cached(
        provider="oidc", subject_id="u1", schema="cat_x", resolver=resolver, cfg=_cfg()
    )

    assert calls["n"] == 2  # new key after bump


# --------------------------------------------------------------------------- #
# 6. cached negative
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_cached_negative(monkeypatch: pytest.MonkeyPatch) -> None:
    backend = FakeCountingBackend()
    _install_res(monkeypatch, backend)
    resolver, calls = _counting_resolver(None)

    r1 = await phantom_token.resolve_bindings_cached(
        provider="oidc", subject_id="u1", schema="cat_x", resolver=resolver, cfg=_cfg()
    )
    r2 = await phantom_token.resolve_bindings_cached(
        provider="oidc", subject_id="u1", schema="cat_x", resolver=resolver, cfg=_cfg()
    )

    assert r1 is None
    assert r2 is None
    assert calls["n"] == 1  # negative was cached


# --------------------------------------------------------------------------- #
# 7. backend None -> resolver every time, no caching
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_no_backend_resolver_every_time(monkeypatch: pytest.MonkeyPatch) -> None:
    # No L2 -> _resolution_backend() returns None -> passthrough.
    _install(monkeypatch, None)
    resolver, calls = _counting_resolver({"roles": ["editor"]})

    await phantom_token.resolve_bindings_cached(
        provider="oidc", subject_id="u1", schema="cat_x", resolver=resolver, cfg=_cfg()
    )
    await phantom_token.resolve_bindings_cached(
        provider="oidc", subject_id="u1", schema="cat_x", resolver=resolver, cfg=_cfg()
    )

    assert calls["n"] == 2


# --------------------------------------------------------------------------- #
# 8. cache read error -> falls through to resolver
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_cache_read_error_falls_through(monkeypatch: pytest.MonkeyPatch) -> None:
    backend = FakeCountingBackend()
    backend.raise_on_get = True
    _install_res(monkeypatch, backend)
    value = {"roles": ["editor"]}
    resolver, calls = _counting_resolver(value)

    result = await phantom_token.resolve_bindings_cached(
        provider="oidc", subject_id="u1", schema="cat_x", resolver=resolver, cfg=_cfg()
    )

    assert result == value
    assert calls["n"] == 1


# --------------------------------------------------------------------------- #
# 9. cache write error / non-serializable result -> returns value, no raise
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_cache_write_error_returns_value(monkeypatch: pytest.MonkeyPatch) -> None:
    backend = FakeCountingBackend()
    backend.raise_on_set = True
    _install_res(monkeypatch, backend)
    value = {"roles": ["editor"]}
    resolver, calls = _counting_resolver(value)

    result = await phantom_token.resolve_bindings_cached(
        provider="oidc", subject_id="u1", schema="cat_x", resolver=resolver, cfg=_cfg()
    )

    assert result == value
    assert calls["n"] == 1


@pytest.mark.asyncio
async def test_non_serializable_result_returns_value(monkeypatch: pytest.MonkeyPatch) -> None:
    backend = FakeCountingBackend()
    _install_res(monkeypatch, backend)
    # An object json.dumps cannot serialize; the write-through must swallow it.
    sentinel = object()
    resolver, calls = _counting_resolver(sentinel)

    result = await phantom_token.resolve_bindings_cached(
        provider="oidc", subject_id="u1", schema="cat_x", resolver=resolver, cfg=_cfg()
    )

    assert result is sentinel
    assert calls["n"] == 1


# --------------------------------------------------------------------------- #
# 10. resolver exception propagates
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_resolver_exception_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    backend = FakeCountingBackend()
    _install_res(monkeypatch, backend)

    async def resolver() -> Dict[str, Any]:
        raise RuntimeError("db down")

    with pytest.raises(RuntimeError, match="db down"):
        await phantom_token.resolve_bindings_cached(
            provider="oidc", subject_id="u1", schema="cat_x", resolver=resolver, cfg=_cfg()
        )


# --------------------------------------------------------------------------- #
# 11. (provider, subject_id) keying — S2S namespaced separately from human
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_provider_namespacing(monkeypatch: pytest.MonkeyPatch) -> None:
    backend = FakeCountingBackend()
    _install_res(monkeypatch, backend)
    human_resolver, human_calls = _counting_resolver({"roles": ["viewer"]})
    sa_resolver, sa_calls = _counting_resolver({"roles": ["admin"]})

    # Same subject_id, different provider -> distinct cache entries.
    await phantom_token.resolve_bindings_cached(
        provider="oidc", subject_id="same", schema="cat_x", resolver=human_resolver, cfg=_cfg()
    )
    await phantom_token.resolve_bindings_cached(
        provider="oidc:service_account",
        subject_id="same",
        schema="cat_x",
        resolver=sa_resolver,
        cfg=_cfg(),
    )
    # Repeat both -> served from their own cache entries.
    h2 = await phantom_token.resolve_bindings_cached(
        provider="oidc", subject_id="same", schema="cat_x", resolver=human_resolver, cfg=_cfg()
    )
    s2 = await phantom_token.resolve_bindings_cached(
        provider="oidc:service_account",
        subject_id="same",
        schema="cat_x",
        resolver=sa_resolver,
        cfg=_cfg(),
    )

    assert human_calls["n"] == 1
    assert sa_calls["n"] == 1
    assert h2 == {"roles": ["viewer"]}
    assert s2 == {"roles": ["admin"]}


# --------------------------------------------------------------------------- #
# 12. denylist
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_is_denied_false_when_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, FakeCountingBackend())
    assert await phantom_token.is_denied("tok1") is False


@pytest.mark.asyncio
async def test_is_denied_false_when_no_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, None)
    assert await phantom_token.is_denied("tok1") is False


@pytest.mark.asyncio
async def test_is_denied_true_after_deny(monkeypatch: pytest.MonkeyPatch) -> None:
    backend = FakeCountingBackend()
    _install(monkeypatch, backend)
    await phantom_token.deny("tok1", ttl_seconds=300)
    assert await phantom_token.is_denied("tok1") is True


@pytest.mark.asyncio
async def test_is_denied_fails_open(monkeypatch: pytest.MonkeyPatch) -> None:
    backend = FakeCountingBackend()
    backend.raise_on_exists = True
    _install(monkeypatch, backend)
    assert await phantom_token.is_denied("tok1") is False  # fail open


@pytest.mark.asyncio
async def test_deny_no_backend_is_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, None)
    await phantom_token.deny("tok1", ttl_seconds=300)  # must not raise


# --------------------------------------------------------------------------- #
# 13. _resolution_backend wiring — builds a memoized tiered backend over L2
# --------------------------------------------------------------------------- #
def test_resolution_backend_is_tiered_and_memoized(monkeypatch: pytest.MonkeyPatch) -> None:
    backend = FakeCountingBackend(priority=100)
    monkeypatch.setattr(phantom_token, "_distributed_backend", lambda: backend)

    tb = phantom_token._resolution_backend()
    assert tb is not None
    assert type(tb).__name__ == "TieredAsyncBackend"
    # Memoized: same instance on a second call over the same L2.
    assert phantom_token._resolution_backend() is tb


def test_resolution_backend_none_without_l2(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(phantom_token, "_distributed_backend", lambda: None)
    assert phantom_token._resolution_backend() is None
