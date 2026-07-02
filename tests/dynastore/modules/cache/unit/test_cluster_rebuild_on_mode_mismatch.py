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

"""Engine-path cluster auto-detect / rebuild on stored-config mismatch.

When the engine builds a *standalone* client (``ValkeyEngineConfig.cluster_mode
=False``) but the connected server reports ``redis_mode=cluster``, the stored
config is wrong.  The module keeps the cache correct for the life of the
process by rebuilding a dedicated cluster-mode client from a
``cluster_mode=True`` copy of the live engine config and registering THAT
backend instead of the standalone wrap.

This is the only cluster auto-detect path in the codebase after the
engine-driven unification (the old env-driven fallback that used to host it
was removed).  These tests pin the rebuild wiring so a refactor can't
silently drop it.
"""

from __future__ import annotations

import asyncio
import types
from typing import Any

import pytest


def _make_app_state(**kwargs: Any) -> types.SimpleNamespace:
    return types.SimpleNamespace(**kwargs)


class _FakeBackend:
    """Records construction kwargs; INFO/topology are class-level fixtures.

    The first instance is the engine standalone wrap (info reports cluster,
    topology reports NOT-a-cluster → triggers the rebuild).  The rebuilt
    instance is the cluster-mode client the module must end up registering.
    """

    instances: list["_FakeBackend"] = []

    def __init__(self, *a: Any, **kw: Any) -> None:
        self.client = kw.get("client")
        self.owns_client = kw.get("owns_client")
        self.closed = False
        _FakeBackend.instances.append(self)

    async def info(self) -> dict[str, Any]:
        return {
            "server": {"redis_version": "9.0", "redis_mode": "cluster"},
            "memory": {"used_memory_human": "1.0M"},
        }

    async def topology(self) -> dict[str, Any]:
        # Standalone client's discovered topology — NOT a cluster, which is
        # exactly the mismatch the rebuild reacts to.
        return {"is_cluster": False, "primaries": 1, "replicas": 0, "slots": []}

    async def close(self) -> None:
        self.closed = True


async def test_engine_path_rebuilds_cluster_client_on_mode_mismatch(
    monkeypatch,
):
    """Server says cluster, engine built standalone → rebuild + register it."""
    from dynastore.modules.cache.cache_module import CacheModule

    _FakeBackend.instances = []

    standalone_client = object()  # what engine_cache hands back
    cluster_client = object()  # what the cluster_mode=True rebuild yields

    import dynastore.tools.cache_valkey as cv
    monkeypatch.setattr(cv, "_CACHE_DEPS_OK", True)
    monkeypatch.setattr(cv, "ValkeyCacheBackend", _FakeBackend)

    class _EngineCacheStub:
        async def get(self, ref: str) -> object:
            assert ref == "valkey_engine"
            return standalone_client

    # Fake engine config whose cluster_mode=True copy builds the cluster client.
    class _FakeCfg:
        def model_copy(self, update: dict[str, Any]) -> "_FakeCfg":
            assert update == {"cluster_mode": True}
            return self

        async def engine_init(self) -> object:
            return cluster_client

    async def _fake_load_cfg() -> Any:
        return _FakeCfg()

    monkeypatch.setattr(
        "dynastore.modules.cache.cache_module._load_valkey_engine_config",
        _fake_load_cfg,
    )

    async def _no_cfg(*_a: Any, **_kw: Any) -> Any:
        from dynastore.modules.cache.cache_config import CachePluginConfig
        return CachePluginConfig()

    monkeypatch.setattr(
        "dynastore.modules.cache.cache_module._load_cache_config", _no_cfg,
    )

    # Capture which backend ends up registered as the live cache.
    registered: dict[str, Any] = {}

    class _FakeManager:
        def register_backend(self, backend: Any) -> None:
            registered["backend"] = backend

    import dynastore.tools.cache as cache_mod
    monkeypatch.setattr(cache_mod, "get_cache_manager", lambda: _FakeManager())
    monkeypatch.setattr(cache_mod, "_notify_backend_upgrade", lambda: None)

    app_state = _make_app_state(engine_cache=_EngineCacheStub())

    module = CacheModule(app_state=app_state)
    async with module.lifespan(app_state):
        # Two backends built: [0] standalone wrap, [1] rebuilt cluster client.
        assert len(_FakeBackend.instances) == 2, (
            "expected the standalone wrap plus a rebuilt cluster client"
        )
        standalone_wrap, rebuilt = _FakeBackend.instances
        assert standalone_wrap.client is standalone_client
        assert standalone_wrap.owns_client is False
        # The rebuilt client owns its connection and is the one registered.
        assert rebuilt.client is cluster_client
        assert rebuilt.owns_client is True
        assert registered["backend"] is rebuilt, (
            "the cluster-mode rebuild must be the registered live backend"
        )


async def test_engine_path_evicts_superseded_standalone_client_on_rebuild_success(
    monkeypatch,
):
    """#2743 — once the corrected cluster-mode client is built and live, the
    superseded standalone client must be evicted from ``engine_cache``
    instead of idling in it (``lifecycle.policy`` defaults to "global",
    i.e. never TTL-evicted) for the rest of the process."""
    from dynastore.modules.cache.cache_module import CacheModule

    _FakeBackend.instances = []

    standalone_client = object()
    cluster_client = object()

    import dynastore.tools.cache_valkey as cv
    monkeypatch.setattr(cv, "_CACHE_DEPS_OK", True)
    monkeypatch.setattr(cv, "ValkeyCacheBackend", _FakeBackend)

    evict_calls: list[str] = []

    class _EngineCacheStub:
        async def get(self, ref: str) -> object:
            assert ref == "valkey_engine"
            return standalone_client

        async def evict(self, ref: str) -> bool:
            evict_calls.append(ref)
            return True

    class _FakeCfg:
        def model_copy(self, update: dict[str, Any]) -> "_FakeCfg":
            return self

        async def engine_init(self) -> object:
            return cluster_client

    async def _fake_load_cfg() -> Any:
        return _FakeCfg()

    monkeypatch.setattr(
        "dynastore.modules.cache.cache_module._load_valkey_engine_config",
        _fake_load_cfg,
    )

    async def _no_cfg(*_a: Any, **_kw: Any) -> Any:
        from dynastore.modules.cache.cache_config import CachePluginConfig
        return CachePluginConfig()

    monkeypatch.setattr(
        "dynastore.modules.cache.cache_module._load_cache_config", _no_cfg,
    )

    class _FakeManager:
        def register_backend(self, backend: Any) -> None:
            pass

    import dynastore.tools.cache as cache_mod
    monkeypatch.setattr(cache_mod, "get_cache_manager", lambda: _FakeManager())
    monkeypatch.setattr(cache_mod, "_notify_backend_upgrade", lambda: None)

    app_state = _make_app_state(engine_cache=_EngineCacheStub())

    module = CacheModule(app_state=app_state)
    async with module.lifespan(app_state):
        assert evict_calls == ["valkey_engine"], (
            "the superseded standalone client must be evicted exactly once, "
            "right after the cluster-mode rebuild succeeds"
        )


async def test_engine_path_rebuild_eviction_failure_does_not_break_rebuild(
    monkeypatch,
):
    """An eviction failure is best-effort — the successfully-rebuilt
    cluster-mode backend must still be the one registered as live."""
    from dynastore.modules.cache.cache_module import CacheModule

    _FakeBackend.instances = []

    standalone_client = object()
    cluster_client = object()

    import dynastore.tools.cache_valkey as cv
    monkeypatch.setattr(cv, "_CACHE_DEPS_OK", True)
    monkeypatch.setattr(cv, "ValkeyCacheBackend", _FakeBackend)

    class _EngineCacheStub:
        async def get(self, ref: str) -> object:
            return standalone_client

        async def evict(self, ref: str) -> bool:
            raise RuntimeError("engine cache unavailable")

    class _FakeCfg:
        def model_copy(self, update: dict[str, Any]) -> "_FakeCfg":
            return self

        async def engine_init(self) -> object:
            return cluster_client

    async def _fake_load_cfg() -> Any:
        return _FakeCfg()

    monkeypatch.setattr(
        "dynastore.modules.cache.cache_module._load_valkey_engine_config",
        _fake_load_cfg,
    )

    async def _no_cfg(*_a: Any, **_kw: Any) -> Any:
        from dynastore.modules.cache.cache_config import CachePluginConfig
        return CachePluginConfig()

    monkeypatch.setattr(
        "dynastore.modules.cache.cache_module._load_cache_config", _no_cfg,
    )

    registered: dict[str, Any] = {}

    class _FakeManager:
        def register_backend(self, backend: Any) -> None:
            registered["backend"] = backend

    import dynastore.tools.cache as cache_mod
    monkeypatch.setattr(cache_mod, "get_cache_manager", lambda: _FakeManager())
    monkeypatch.setattr(cache_mod, "_notify_backend_upgrade", lambda: None)

    app_state = _make_app_state(engine_cache=_EngineCacheStub())

    module = CacheModule(app_state=app_state)
    async with module.lifespan(app_state):
        rebuilt = _FakeBackend.instances[-1]
        assert rebuilt.client is cluster_client
        assert registered["backend"] is rebuilt


async def test_engine_path_rebuild_failure_keeps_standalone(monkeypatch):
    """If the rebuild raises, the still-functional standalone wrap stays live."""
    from dynastore.modules.cache.cache_module import CacheModule

    _FakeBackend.instances = []
    standalone_client = object()

    import dynastore.tools.cache_valkey as cv
    monkeypatch.setattr(cv, "_CACHE_DEPS_OK", True)
    monkeypatch.setattr(cv, "ValkeyCacheBackend", _FakeBackend)

    class _EngineCacheStub:
        async def get(self, ref: str) -> object:
            return standalone_client

    async def _boom_load_cfg() -> Any:
        raise RuntimeError("configs service unreachable")

    monkeypatch.setattr(
        "dynastore.modules.cache.cache_module._load_valkey_engine_config",
        _boom_load_cfg,
    )

    async def _no_cfg(*_a: Any, **_kw: Any) -> Any:
        from dynastore.modules.cache.cache_config import CachePluginConfig
        return CachePluginConfig()

    monkeypatch.setattr(
        "dynastore.modules.cache.cache_module._load_cache_config", _no_cfg,
    )

    registered: dict[str, Any] = {}

    class _FakeManager:
        def register_backend(self, backend: Any) -> None:
            registered["backend"] = backend

    import dynastore.tools.cache as cache_mod
    monkeypatch.setattr(cache_mod, "get_cache_manager", lambda: _FakeManager())
    monkeypatch.setattr(cache_mod, "_notify_backend_upgrade", lambda: None)

    app_state = _make_app_state(engine_cache=_EngineCacheStub())

    module = CacheModule(app_state=app_state)
    async with module.lifespan(app_state):
        # Only the standalone wrap was built; the rebuild failed and was
        # swallowed, leaving the original backend registered.
        assert len(_FakeBackend.instances) == 1
        assert registered["backend"] is _FakeBackend.instances[0]
        assert registered["backend"].client is standalone_client


async def test_recovery_after_trip_reapplies_cluster_correction_from_stale_snapshot(
    monkeypatch,
):
    """#2741 finding 1 — a boot-time cluster-mode correction must survive a
    later circuit-breaker recovery.

    The correction is deliberately never persisted into the stored
    ``ValkeyEngineConfig`` (config stays the SSOT — the operator still has
    to PATCH it), so ``engine_cache.get("valkey_engine")`` keeps handing
    back a *standalone* client even after a pod already corrected once.
    When the corrected cluster backend later trips (a transient blip) and
    the circuit-breaker recovery loop reconnects, it must re-detect the
    same mismatch against that stale snapshot and land a cluster client
    again — not silently regress to routing a real cluster's traffic
    through a standalone client.
    """
    import dynastore.modules.cache.cache_module as cm
    import dynastore.tools.cache_valkey as cv

    cm._current_backend = None
    cm._recovery_task = None
    _FakeBackend.instances = []

    standalone_client = object()  # what the STALE snapshot still returns
    cluster_client = object()  # what the cluster_mode=True correction builds

    monkeypatch.setattr(cv, "_CACHE_DEPS_OK", True)
    monkeypatch.setattr(cv, "ValkeyCacheBackend", _FakeBackend)

    class _EngineCacheStub:
        def __init__(self) -> None:
            self.evict_calls: list[str] = []

        async def get(self, ref: str) -> object:
            assert ref == "valkey_engine"
            return standalone_client

        async def evict(self, ref: str) -> bool:
            self.evict_calls.append(ref)
            return True

    engine_cache = _EngineCacheStub()
    cm._app_state = _make_app_state(engine_cache=engine_cache)

    class _FakeCfg:
        def model_copy(self, update: dict[str, Any]) -> "_FakeCfg":
            assert update == {"cluster_mode": True}
            return self

        async def engine_init(self) -> object:
            return cluster_client

    async def _fake_load_valkey_cfg() -> Any:
        return _FakeCfg()

    monkeypatch.setattr(cm, "_load_valkey_engine_config", _fake_load_valkey_cfg)

    async def _no_cfg(*_a: Any, **_kw: Any) -> Any:
        from dynastore.modules.cache.cache_config import CachePluginConfig
        return CachePluginConfig()

    monkeypatch.setattr(cm, "_load_cache_config", _no_cfg)

    import dynastore.tools.cache as cache_mod

    manager = types.SimpleNamespace(
        unregister_backend=lambda backend: None,
        register_backend=lambda backend: None,
    )
    monkeypatch.setattr(cache_mod, "get_cache_manager", lambda: manager)
    monkeypatch.setattr(cache_mod, "_notify_backend_upgrade", lambda: None)

    monkeypatch.setattr(cm, "_CB_RECOVERY_INITIAL_DELAY", 0.01)
    monkeypatch.setattr(cm, "_CB_RECOVERY_MAX_DELAY", 0.02)

    # A pod that already boot-corrected to a cluster backend; that backend
    # is what trips now.
    tripped_backend = _FakeBackend(client=cluster_client, owns_client=True)
    cm._current_backend = tripped_backend

    cm._on_backend_trip(tripped_backend)
    task = cm._recovery_task
    assert task is not None
    for _ in range(500):
        if task.done():
            break
        await asyncio.sleep(0.01)

    assert cm._current_backend is not None, "recovery must land a backend"
    assert cm._current_backend is not tripped_backend
    assert cm._current_backend.client is cluster_client, (
        "recovery rebuilt from the stale standalone-mode snapshot, detected "
        "the mismatch again, and must re-correct to a cluster client — not "
        "leave the standalone client from the stale snapshot in place"
    )
    assert cm._current_backend.owns_client is True
    assert "valkey_engine" in engine_cache.evict_calls

    cm._current_backend = None
    cm._recovery_task = None
