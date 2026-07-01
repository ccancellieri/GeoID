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

import logging
from types import SimpleNamespace
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


def _fake_cgroup(cpu=0.7, mem=0.4):
    return SimpleNamespace(
        read_cpu_utilization=lambda: cpu, read_memory_utilization=lambda: mem,
    )


def _diag(usable: bool) -> dict:
    """A ``probe_cgroup()``-shaped dict with every key ``format_cgroup_probe``
    reads, for tests that want to control the boot-probe verdict
    deterministically instead of depending on the real host filesystem."""
    return {
        "cgroup_version": "v2" if usable else "unknown (no cpu.stat found)",
        "cpu_stat_path": "/sys/fs/cgroup/cpu.stat", "cpu_stat_raw": None, "cpu_stat_error": None,
        "cpu_max_path": "/sys/fs/cgroup/cpu.max", "cpu_max_raw": None, "cpu_max_error": None,
        "memory_current_path": "/sys/fs/cgroup/memory.current", "memory_current_raw": None,
        "memory_current_error": None,
        "memory_max_path": "/sys/fs/cgroup/memory.max", "memory_max_raw": None, "memory_max_error": None,
        "cpu_usage_usec": None, "allotted_cores": 1.0, "memory_utilization": None,
        "usable": usable,
    }


@pytest.mark.asyncio
async def test_cgroup_probe_logged_once_regardless_of_disabled_policy(caplog):
    """The one-shot startup probe fires on the very first tick even with the
    loop disabled — dev validation needs no config flip — and never again
    after, regardless of the verdict."""
    policy = ScalingPolicyConfig(enabled=False)
    configs = _StubConfigs(policy)
    pub = ScalingSignalPublisher(configs)
    pub._cgroup = _fake_cgroup()

    with caplog.at_level(logging.INFO, logger=publisher_mod.logger.name):
        await pub.tick(_ctx())
        await pub.tick(_ctx())

    probe_lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith("cgroup_probe")]
    assert len(probe_lines) == 1, "the startup probe must log exactly once, not every tick"


@pytest.mark.asyncio
async def test_no_per_tick_compare_log(monkeypatch, caplog):
    """The per-tick comparison log was removed as noise once cgroup proved
    non-viable on Cloud Run — the boot probe is the only cgroup-related log
    line a normal tick can produce."""
    monkeypatch.setattr(publisher_mod, "probe_cgroup", lambda: _diag(usable=True))
    policy = ScalingPolicyConfig(enabled=True)
    configs = _StubConfigs(policy)
    monkeypatch.setattr(publisher_mod, "get_protocols", lambda proto: [])
    monkeypatch.setattr(publisher_mod, "get_cache_manager", lambda: _FailingCacheManager())

    pub = ScalingSignalPublisher(configs)
    pub._cgroup = _fake_cgroup(cpu=0.42, mem=0.33)

    with caplog.at_level(logging.INFO, logger=publisher_mod.logger.name):
        await pub.tick(_ctx())
        await pub.tick(_ctx())

    assert not any(r.getMessage().startswith("scaling_metric_compare") for r in caplog.records)


@pytest.mark.asyncio
async def test_cgroup_read_skipped_entirely_when_probe_reports_unusable(monkeypatch):
    """Cloud Run's confirmed shape: the boot probe finds no cgroup
    filesystem, so every later tick must skip the read outright — no
    per-tick filesystem cost on a platform where it will never succeed."""
    monkeypatch.setattr(publisher_mod, "probe_cgroup", lambda: _diag(usable=False))
    policy = ScalingPolicyConfig(enabled=True)
    configs = _StubConfigs(policy)
    monkeypatch.setattr(publisher_mod, "get_protocols", lambda proto: [])

    backend = _FakeAsyncCacheBackend()

    class _CacheManager:
        def get_async_backend(self):
            return backend

    monkeypatch.setattr(publisher_mod, "get_cache_manager", lambda: _CacheManager())

    calls = {"cpu": 0, "mem": 0}

    def _cpu():
        calls["cpu"] += 1
        return 0.9

    def _mem():
        calls["mem"] += 1
        return 0.9

    pub = ScalingSignalPublisher(configs)
    pub._cgroup = SimpleNamespace(read_cpu_utilization=_cpu, read_memory_utilization=_mem)

    await pub.tick(_ctx())
    await pub.tick(_ctx())
    await pub.tick(_ctx())

    assert calls == {"cpu": 0, "mem": 0}, "unusable cgroup must never be read, not even once"
    doc = backend._store.get(SIGNALS_CACHE_KEY, {"instances": {}})
    published = next(iter(doc["instances"].values()), {}).get("signals", [])
    assert not any(s["source"] == "cgroup" for s in published)


@pytest.mark.asyncio
async def test_cgroup_readings_published_as_instance_signals(monkeypatch):
    """When usable, the cgroup self-report rides in the SAME per-instance
    payload, via the SAME backend handle the publisher already uses — no
    second Valkey client, purely additive to the existing instance-signal
    list."""
    monkeypatch.setattr(publisher_mod, "probe_cgroup", lambda: _diag(usable=True))
    policy = ScalingPolicyConfig(enabled=True)
    configs = _StubConfigs(policy)
    monkeypatch.setattr(publisher_mod, "get_protocols", lambda proto: [])

    backend = _FakeAsyncCacheBackend()

    class _CacheManager:
        def get_async_backend(self):
            return backend

    monkeypatch.setattr(publisher_mod, "get_cache_manager", lambda: _CacheManager())

    pub = ScalingSignalPublisher(configs)
    pub._cgroup = _fake_cgroup(cpu=0.6, mem=0.2)

    await pub.tick(_ctx())

    doc = backend._store[SIGNALS_CACHE_KEY]
    published = next(iter(doc["instances"].values()))["signals"]
    by_metric = {s["metric"]: s for s in published}
    assert by_metric["cpu_utilization"]["value"] == 0.6
    assert by_metric["cpu_utilization"]["source"] == "cgroup"
    assert by_metric["cpu_utilization"]["scope"] == "instance"
    assert by_metric["memory_utilization"]["value"] == 0.2


@pytest.mark.asyncio
async def test_cgroup_none_reading_contributes_no_signal(monkeypatch):
    """A fail-soft ``None`` (e.g. the first CPU sample after a usable
    probe) must not publish a fabricated signal."""
    monkeypatch.setattr(publisher_mod, "probe_cgroup", lambda: _diag(usable=True))
    policy = ScalingPolicyConfig(enabled=True)
    configs = _StubConfigs(policy)
    monkeypatch.setattr(publisher_mod, "get_protocols", lambda proto: [])

    backend = _FakeAsyncCacheBackend()

    class _CacheManager:
        def get_async_backend(self):
            return backend

    monkeypatch.setattr(publisher_mod, "get_cache_manager", lambda: _CacheManager())

    pub = ScalingSignalPublisher(configs)
    pub._cgroup = _fake_cgroup(cpu=None, mem=None)

    await pub.tick(_ctx())

    doc = backend._store[SIGNALS_CACHE_KEY]
    # No provider and no cgroup reading this tick -> nothing published for
    # this instance, but the tick must still complete without raising.
    assert doc["instances"] == {} or not next(iter(doc["instances"].values()))["signals"]
