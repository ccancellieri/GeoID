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

"""Unit tests for ``ScalingSignalPublisher.tick``.

Isolated from the real discovery registry and cache manager via monkeypatch
so the test never touches process-global state or a real backend.
"""

from __future__ import annotations

from typing import List

import pytest

from dynastore.models.scaling import ScalingSignal
from dynastore.modules.scaling import publisher as publisher_mod
from dynastore.modules.scaling.aggregator import SIGNALS_CACHE_KEY
from dynastore.modules.scaling.config import ScalingPolicyConfig
from dynastore.modules.scaling.publisher import ScalingSignalPublisher
from dynastore.tools.background_service import ServiceContext


class _FakeAsyncCacheBackend:
    """Minimal in-memory stand-in for the ``CacheBackend`` protocol."""

    def __init__(self):
        self._store: dict = {}

    async def get(self, key: str):
        return self._store.get(key)

    async def set(self, key: str, value, *, ttl=None):
        self._store[key] = value


class _FailingCacheManager:
    def get_async_backend(self):
        raise RuntimeError("no backend registered")


class _StubProvider:
    def __init__(self, signals: List[ScalingSignal]):
        self._signals = signals

    def scaling_signals(self) -> List[ScalingSignal]:
        return self._signals


class _StubConfigs:
    def __init__(self, policy: ScalingPolicyConfig):
        self._policy = policy

    async def get_config(self, config_cls, catalog_id=None, collection_id=None, ctx=None):
        assert config_cls is ScalingPolicyConfig
        return self._policy


def _ctx() -> ServiceContext:
    import asyncio

    return ServiceContext(
        engine=None, shutdown=asyncio.Event(), is_ephemeral=False, name="test-host",
    )


@pytest.mark.asyncio
async def test_tick_writes_instance_and_global_signals(monkeypatch):
    policy = ScalingPolicyConfig(enabled=True, publish_interval_seconds=15)
    configs = _StubConfigs(policy)
    instance_signal = ScalingSignal(
        source="duckdb_pool", metric="pool_saturation", value=0.5, scope="instance", ts=0.0
    )
    global_signal = ScalingSignal(
        source="db_contention_monitor", metric="conn_pressure", value=0.3, scope="global", ts=0.0
    )
    provider = _StubProvider([instance_signal, global_signal])

    backend = _FakeAsyncCacheBackend()

    class _CacheManager:
        def get_async_backend(self):
            return backend

    monkeypatch.setattr(publisher_mod, "get_protocols", lambda proto: [provider])
    monkeypatch.setattr(publisher_mod, "get_cache_manager", lambda: _CacheManager())

    pub = ScalingSignalPublisher(configs)
    await pub.tick(_ctx())

    doc = backend._store[SIGNALS_CACHE_KEY]
    assert doc["instances"], "instance-scope signal was not published"
    published_instance = next(iter(doc["instances"].values()))
    assert published_instance["signals"][0]["metric"] == "pool_saturation"

    assert doc["global"], "global-scope signal was not published"
    published_global = next(iter(doc["global"].values()))
    assert published_global["signal"]["metric"] == "conn_pressure"


@pytest.mark.asyncio
async def test_tick_disabled_policy_skips_publish(monkeypatch):
    policy = ScalingPolicyConfig(enabled=False)
    configs = _StubConfigs(policy)
    provider = _StubProvider(
        [ScalingSignal(source="x", metric="y", value=0.1, scope="instance", ts=0.0)]
    )

    calls = {"n": 0}

    def _get_protocols(proto):
        calls["n"] += 1
        return [provider]

    monkeypatch.setattr(publisher_mod, "get_protocols", _get_protocols)

    pub = ScalingSignalPublisher(configs)
    await pub.tick(_ctx())

    assert calls["n"] == 0, "disabled policy must short-circuit before discovering providers"


@pytest.mark.asyncio
async def test_tick_cache_backend_failure_does_not_raise(monkeypatch):
    policy = ScalingPolicyConfig(enabled=True)
    configs = _StubConfigs(policy)
    provider = _StubProvider(
        [ScalingSignal(source="x", metric="y", value=0.1, scope="instance", ts=0.0)]
    )

    monkeypatch.setattr(publisher_mod, "get_protocols", lambda proto: [provider])
    monkeypatch.setattr(publisher_mod, "get_cache_manager", lambda: _FailingCacheManager())

    pub = ScalingSignalPublisher(configs)

    # Must not raise despite the cache manager blowing up.
    await pub.tick(_ctx())


@pytest.mark.asyncio
async def test_tick_provider_raising_does_not_abort_publish(monkeypatch):
    """One bad provider must not stop the rest from publishing (mirrors the
    ``ScalingSignalProtocol`` contract's "never raise" note being enforced
    defensively by the publisher too)."""
    policy = ScalingPolicyConfig(enabled=True)
    configs = _StubConfigs(policy)

    class _RaisingProvider:
        def scaling_signals(self):
            raise RuntimeError("boom")

    good_signal = ScalingSignal(
        source="duckdb_pool", metric="pool_saturation", value=0.4, scope="instance", ts=0.0
    )
    good_provider = _StubProvider([good_signal])

    backend = _FakeAsyncCacheBackend()

    class _CacheManager:
        def get_async_backend(self):
            return backend

    monkeypatch.setattr(
        publisher_mod, "get_protocols", lambda proto: [_RaisingProvider(), good_provider]
    )
    monkeypatch.setattr(publisher_mod, "get_cache_manager", lambda: _CacheManager())

    pub = ScalingSignalPublisher(configs)
    await pub.tick(_ctx())

    doc = backend._store[SIGNALS_CACHE_KEY]
    assert doc["instances"], "the good provider's signal must still be published"
