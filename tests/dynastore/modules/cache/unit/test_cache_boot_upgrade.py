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

"""Boot-order LOCAL -> Valkey self-upgrade for CacheModule.

CacheModule (priority 9) initialises before DBService (priority 10) creates
the DB pool and before TasksModule (priority 15) seeds
``valkey_engine_config``, so ``engine_cache.get('valkey_engine')`` cannot
resolve while the module is entering its lifespan — the engine snapshot is
only populated a moment later, after the module yields.

Before this fix the module made a one-shot LOCAL-vs-Valkey decision at
priority 9 and never re-checked, so every instance ran a per-instance
in-memory cache for its whole lifetime (no shared L2), and in the LOCAL
path it returned before registering the config apply handler — so not even
a ``PATCH /configs/plugins/valkey_engine_config`` could rebind it.

These tests pin the two guarantees:
  1. A boot-order degrade to LOCAL upgrades itself to the shared Valkey
     backend once the engine snapshot becomes resolvable, bumping the
     ``@cached`` backend generation so consumers re-resolve lazily.
  2. The ValkeyEngineConfig apply handler is registered even in the LOCAL
     path (so a later config change still drives a live reconnect), and
     unregistered on shutdown.
"""

from __future__ import annotations

import asyncio
import types
from typing import Any


class _FakeBackend:
    """Stand-in for ValkeyCacheBackend with a healthy probe."""

    priority = 0

    def __init__(self, *_a: Any, **kw: Any) -> None:
        self.client = kw.get("client")
        self._closed = False

    async def info(self) -> dict:
        return {
            "server": {"redis_version": "7.2", "redis_mode": "standalone"},
            "memory": {"used_memory_human": "1M"},
        }

    async def topology(self) -> dict:
        return {"is_cluster": False, "primaries": 1, "replicas": 0, "slots": []}

    async def close(self) -> None:
        self._closed = True


class _FakeManager:
    """Isolates the test from the process-wide cache manager."""

    def __init__(self) -> None:
        self.registered: list[Any] = []

    def register_backend(self, backend: Any) -> None:
        self.registered.append(backend)

    def unregister_backend(self, backend: Any) -> None:
        if backend in self.registered:
            self.registered.remove(backend)


def _reset_module_state() -> None:
    import dynastore.modules.cache.cache_module as cm

    cm._current_backend = None


async def test_boot_order_degrade_upgrades_to_valkey_when_snapshot_ready(
    monkeypatch,
):
    """LOCAL at boot -> Valkey once the engine snapshot resolves."""
    import dynastore.modules.cache.cache_module as cm
    import dynastore.tools.cache as dcache
    import dynastore.tools.cache_valkey as cv
    from dynastore.modules.cache.cache_module import CacheModule

    _reset_module_state()

    # engine_cache.get: KeyError until the snapshot is "ready", then a client.
    ready = asyncio.Event()
    sentinel_client = object()

    class _EngineCacheStub:
        async def get(self, ref: str) -> object:
            if not ready.is_set():
                raise KeyError(ref)
            return sentinel_client

    engine_cache = _EngineCacheStub()

    monkeypatch.setattr(cv, "_CACHE_DEPS_OK", True)
    monkeypatch.setattr(cv, "ValkeyCacheBackend", _FakeBackend)

    # Isolate from the real process-wide cache manager; keep the real
    # generation counter so we can assert consumers will re-resolve.
    fake_mgr = _FakeManager()
    monkeypatch.setattr(dcache, "get_cache_manager", lambda: fake_mgr)

    async def _no_cfg(*_a: Any, **_kw: Any) -> Any:
        from dynastore.modules.cache.cache_config import CachePluginConfig

        return CachePluginConfig()

    monkeypatch.setattr(cm, "_load_cache_config", _no_cfg)

    # Tighten the backoff so the test resolves quickly.
    monkeypatch.setattr(cm, "_BOOT_UPGRADE_INITIAL_DELAY", 0.01)
    monkeypatch.setattr(cm, "_BOOT_UPGRADE_MAX_DELAY", 0.02)

    gen_before = dcache._backend_generation
    app_state = types.SimpleNamespace(engine_cache=engine_cache)
    module = CacheModule(app_state=app_state)

    async with module.lifespan(app_state):
        # At entry the snapshot is not ready → LOCAL, nothing registered.
        assert cm._current_backend is None

        # Snapshot becomes resolvable; the background upgrade must pick it up.
        ready.set()
        for _ in range(300):
            if cm._current_backend is not None:
                break
            await asyncio.sleep(0.01)

        assert isinstance(cm._current_backend, _FakeBackend), (
            "boot upgrade must register a Valkey backend once the engine "
            "snapshot resolves — a latched LOCAL fallback would leave "
            "_current_backend None"
        )
        assert fake_mgr.registered and isinstance(
            fake_mgr.registered[-1], _FakeBackend
        )
        assert dcache._backend_generation > gen_before, (
            "backend generation must bump so @cached consumers re-resolve"
        )

    # Shutdown closes and clears the upgraded backend.
    assert cm._current_backend is None


async def test_boot_upgrade_retries_transient_valkey_probe_failure(monkeypatch):
    """A transient Valkey probe failure must not latch LOCAL permanently.

    The engine snapshot resolves immediately, but the Valkey *server* is
    momentarily unreachable — the first reconnect builds a backend whose
    probe raises, leaving ``_current_backend`` None.  The boot-upgrade loop
    must keep retrying (not burn its remaining budget on one blip) and
    upgrade once the probe recovers.
    """
    import dynastore.modules.cache.cache_module as cm
    import dynastore.tools.cache as dcache
    import dynastore.tools.cache_valkey as cv
    from dynastore.modules.cache.cache_module import CacheModule

    _reset_module_state()

    sentinel_client = object()

    class _EngineCacheStub:
        async def get(self, ref: str) -> object:
            return sentinel_client

    # Shared across the fresh backend instance built on each reconnect
    # attempt: probe raises on the first build, succeeds thereafter.
    attempts = {"n": 0}

    class _FlakyBackend(_FakeBackend):
        async def info(self) -> dict:
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise ConnectionError("valkey momentarily unreachable")
            return await super().info()

    monkeypatch.setattr(cv, "_CACHE_DEPS_OK", True)
    monkeypatch.setattr(cv, "ValkeyCacheBackend", _FlakyBackend)

    fake_mgr = _FakeManager()
    monkeypatch.setattr(dcache, "get_cache_manager", lambda: fake_mgr)

    async def _no_cfg(*_a: Any, **_kw: Any) -> Any:
        from dynastore.modules.cache.cache_config import CachePluginConfig

        return CachePluginConfig()

    monkeypatch.setattr(cm, "_load_cache_config", _no_cfg)
    monkeypatch.setattr(cm, "_BOOT_UPGRADE_INITIAL_DELAY", 0.01)
    monkeypatch.setattr(cm, "_BOOT_UPGRADE_MAX_DELAY", 0.02)

    app_state = types.SimpleNamespace(engine_cache=_EngineCacheStub())
    module = CacheModule(app_state=app_state)

    async with module.lifespan(app_state):
        for _ in range(300):
            if cm._current_backend is not None:
                break
            await asyncio.sleep(0.01)

        assert isinstance(cm._current_backend, _FlakyBackend), (
            "boot upgrade must survive a transient probe failure and retry "
            "until the Valkey server is reachable"
        )
        assert attempts["n"] >= 2, (
            "the first probe must have failed and a later attempt succeeded"
        )

    assert cm._current_backend is None


async def test_boot_upgrade_rebuilds_snapshot_once_db_pool_becomes_ready(
    monkeypatch,
):
    """#2857: the boot-upgrade loop must re-run build_engine_snapshot itself,
    not just re-probe a snapshot dict nothing else will ever populate.

    Models the exact production race: ``DBConfigModule``'s own one-shot
    ``refresh_snapshot_until_ready`` background task already exhausted its
    retry budget with the resolver snapshot still empty (the DB pool wasn't
    up yet during that window), the SAME way it always does in production
    because ``CacheModule`` (priority 9) is sequenced strictly before
    ``DBService`` (priority 10) installs the pool. The pool then genuinely
    comes up a couple of attempts into the boot-upgrade loop.

    Uses a *real* ``EngineInstanceCache`` wired with ``make_resolver`` /
    ``make_writer`` over a snapshot dict that starts empty and — crucially —
    stays permanently empty unless something calls the on-demand
    ``engine_snapshot_refresh`` callable published on ``app_state``. Before
    the fix, ``_boot_upgrade_to_valkey`` never called it, so
    ``engine_cache.get`` would KeyError for the whole retry budget even
    once the pool was ready.
    """
    import dynastore.modules.cache.cache_module as cm
    import dynastore.tools.cache as dcache
    import dynastore.tools.cache_valkey as cv
    from dynastore.modules.cache.cache_module import CacheModule
    from dynastore.modules.db_config.engine_config import ValkeyEngineConfig
    from dynastore.modules.db_config.engine_instance_cache import EngineInstanceCache
    from dynastore.modules.db_config.engine_resolver import make_resolver, make_writer

    _reset_module_state()

    snapshot: dict = {}
    engine_cache = EngineInstanceCache(
        engine_resolver=make_resolver(snapshot),
        engine_writer=make_writer(snapshot),
    )

    sentinel_client = object()

    async def _fake_engine_init(self: ValkeyEngineConfig) -> object:
        return sentinel_client

    monkeypatch.setattr(ValkeyEngineConfig, "engine_init", _fake_engine_init)

    # The DB pool "comes up" on the 2nd on-demand refresh attempt — the
    # first still observes ``db_resource is None`` and adds nothing.
    pool_ready_after = 2
    calls = {"n": 0}

    async def _snapshot_refresh() -> bool:
        calls["n"] += 1
        if calls["n"] < pool_ready_after:
            return False
        cfg = ValkeyEngineConfig()
        snapshot["valkey_engine_config"] = cfg
        snapshot["valkey_engine"] = cfg
        return True

    monkeypatch.setattr(cv, "_CACHE_DEPS_OK", True)
    monkeypatch.setattr(cv, "ValkeyCacheBackend", _FakeBackend)

    fake_mgr = _FakeManager()
    monkeypatch.setattr(dcache, "get_cache_manager", lambda: fake_mgr)

    async def _no_cfg(*_a: Any, **_kw: Any) -> Any:
        from dynastore.modules.cache.cache_config import CachePluginConfig

        return CachePluginConfig()

    monkeypatch.setattr(cm, "_load_cache_config", _no_cfg)
    monkeypatch.setattr(cm, "_BOOT_UPGRADE_INITIAL_DELAY", 0.01)
    monkeypatch.setattr(cm, "_BOOT_UPGRADE_MAX_DELAY", 0.02)

    app_state = types.SimpleNamespace(
        engine_cache=engine_cache,
        engine_snapshot_refresh=_snapshot_refresh,
    )
    module = CacheModule(app_state=app_state)

    async with module.lifespan(app_state):
        # Boot race still lost at module entry — same as production.
        assert cm._current_backend is None

        for _ in range(300):
            if cm._current_backend is not None:
                break
            await asyncio.sleep(0.01)

        assert isinstance(cm._current_backend, _FakeBackend), (
            "boot upgrade must rebuild the snapshot via "
            "engine_snapshot_refresh and register a Valkey backend once "
            "the DB pool becomes ready — without rebuilding, "
            "engine_cache.get would KeyError forever against a "
            "permanently empty snapshot"
        )
        assert calls["n"] >= pool_ready_after, (
            "the boot-upgrade loop must call engine_snapshot_refresh on "
            "more than one attempt"
        )
        assert fake_mgr.registered and isinstance(
            fake_mgr.registered[-1], _FakeBackend
        )

    assert cm._current_backend is None


async def test_boot_upgrade_stays_local_when_snapshot_never_rebuilds(
    monkeypatch,
):
    """Regression guard: without a working snapshot_refresh, an empty
    resolver snapshot must never spontaneously resolve — pins the pre-fix
    failure mode so a future change can't silently reintroduce it.
    """
    import dynastore.modules.cache.cache_module as cm
    import dynastore.tools.cache_valkey as cv
    from dynastore.modules.cache.cache_module import CacheModule
    from dynastore.modules.db_config.engine_instance_cache import EngineInstanceCache
    from dynastore.modules.db_config.engine_resolver import make_resolver, make_writer

    _reset_module_state()

    snapshot: dict = {}
    engine_cache = EngineInstanceCache(
        engine_resolver=make_resolver(snapshot),
        engine_writer=make_writer(snapshot),
    )

    monkeypatch.setattr(cv, "_CACHE_DEPS_OK", True)
    monkeypatch.setattr(cv, "ValkeyCacheBackend", _FakeBackend)

    async def _no_cfg(*_a: Any, **_kw: Any) -> Any:
        from dynastore.modules.cache.cache_config import CachePluginConfig

        return CachePluginConfig()

    monkeypatch.setattr(cm, "_load_cache_config", _no_cfg)
    monkeypatch.setattr(cm, "_BOOT_UPGRADE_MAX_ATTEMPTS", 3)
    monkeypatch.setattr(cm, "_BOOT_UPGRADE_INITIAL_DELAY", 0.001)
    monkeypatch.setattr(cm, "_BOOT_UPGRADE_MAX_DELAY", 0.001)

    # No engine_snapshot_refresh on app_state at all (back-compat / never
    # wired) — the snapshot has no way to ever gain an entry.
    app_state = types.SimpleNamespace(engine_cache=engine_cache)
    module = CacheModule(app_state=app_state)

    async with module.lifespan(app_state):
        await asyncio.sleep(0.05)
        assert cm._current_backend is None, (
            "an empty resolver snapshot with no refresher wired must stay "
            "LOCAL — it has no path to ever resolve 'valkey_engine'"
        )

    assert cm._current_backend is None


async def test_degraded_boot_registers_apply_handler(monkeypatch):
    """The apply handler is live even when boot degrades to LOCAL.

    Guards the second defect: previously the LOCAL path returned before
    registering the handler, so a later config change could never rebind
    the cache.  The engine snapshot never becomes ready in this test, so
    the module stays LOCAL for the whole lifespan — yet the handler must be
    registered while entered and removed on exit.
    """
    import dynastore.modules.cache.cache_module as cm
    import dynastore.tools.cache_valkey as cv
    from dynastore.modules.cache.cache_module import (
        CacheModule,
        _on_valkey_engine_config_change,
    )
    from dynastore.modules.db_config.engine_config import ValkeyEngineConfig

    _reset_module_state()

    class _NeverReadyEngineCache:
        async def get(self, ref: str) -> object:
            raise KeyError(ref)

    monkeypatch.setattr(cv, "_CACHE_DEPS_OK", True)
    monkeypatch.setattr(cv, "ValkeyCacheBackend", _FakeBackend)

    async def _no_cfg(*_a: Any, **_kw: Any) -> Any:
        from dynastore.modules.cache.cache_config import CachePluginConfig

        return CachePluginConfig()

    monkeypatch.setattr(cm, "_load_cache_config", _no_cfg)
    # Keep the background upgrade cheap; it will just spin and give up.
    monkeypatch.setattr(cm, "_BOOT_UPGRADE_MAX_ATTEMPTS", 1)
    monkeypatch.setattr(cm, "_BOOT_UPGRADE_INITIAL_DELAY", 0.001)
    monkeypatch.setattr(cm, "_BOOT_UPGRADE_MAX_DELAY", 0.001)

    assert _on_valkey_engine_config_change not in ValkeyEngineConfig.get_apply_handlers()

    app_state = types.SimpleNamespace(engine_cache=_NeverReadyEngineCache())
    module = CacheModule(app_state=app_state)

    async with module.lifespan(app_state):
        assert _on_valkey_engine_config_change in (
            ValkeyEngineConfig.get_apply_handlers()
        ), "apply handler must be registered even in the LOCAL degrade path"

    assert _on_valkey_engine_config_change not in (
        ValkeyEngineConfig.get_apply_handlers()
    ), "apply handler must be unregistered on shutdown"


async def test_deps_missing_stays_local_no_upgrade_task(monkeypatch):
    """When the module_cache extra is absent Valkey is impossible — no
    upgrade task, no handler churn, just a clean LOCAL yield."""
    import dynastore.modules.cache.cache_module as cm
    import dynastore.tools.cache_valkey as cv
    from dynastore.modules.cache.cache_module import CacheModule

    _reset_module_state()

    monkeypatch.setattr(cv, "_CACHE_DEPS_OK", False)

    async def _no_cfg(*_a: Any, **_kw: Any) -> Any:
        from dynastore.modules.cache.cache_config import CachePluginConfig

        return CachePluginConfig()

    monkeypatch.setattr(cm, "_load_cache_config", _no_cfg)

    class _EngineCacheStub:
        async def get(self, ref: str) -> object:
            raise AssertionError("must not touch engine_cache when deps missing")

    app_state = types.SimpleNamespace(engine_cache=_EngineCacheStub())
    module = CacheModule(app_state=app_state)

    entered = False
    async with module.lifespan(app_state):
        entered = True
        assert cm._current_backend is None
    assert entered
