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

"""Unit tests for ``MonitoringSignalProvider`` — the slow, corroborating
CPU/memory-utilization tier feeding the #2333 autoscaling loop.

The backend is a plain fake conforming to ``MetricsBackendProtocol``; these
tests never touch a real cloud API. They pin: (1) it conforms to
``ScalingSignalProtocol`` the same way ``PgPoolSignalProvider`` does, (2) a
failed/None backend read is fail-soft and keeps the prior cached value
instead of clearing it, and (3) the disabled-by-default config short-circuits
before the backend is ever called.
"""

from __future__ import annotations

import asyncio
from typing import Dict, Optional

import pytest

from dynastore.models.protocols.metrics_backend import MetricKind
from dynastore.modules.scaling.config import MonitoringSignalConfig
from dynastore.modules.scaling.monitoring_signal_provider import MonitoringSignalProvider
from dynastore.tools.background_service import ServiceContext


class _FakeBackend:
    """Conforms to ``MetricsBackendProtocol`` — one canned value/exception
    per metric, swappable between ticks."""

    def __init__(self, values: Optional[Dict[MetricKind, Optional[float]]] = None):
        self._values = values or {}
        self._raise: Dict[MetricKind, Exception] = {}
        self.calls = 0

    def set_value(self, metric: MetricKind, value: Optional[float]) -> None:
        self._values[metric] = value
        self._raise.pop(metric, None)

    def set_raises(self, metric: MetricKind, exc: Exception) -> None:
        self._raise[metric] = exc

    async def read_utilization(self, metric: MetricKind, *, window_seconds: float) -> Optional[float]:
        self.calls += 1
        if metric in self._raise:
            raise self._raise[metric]
        return self._values.get(metric)


class _StubConfigs:
    def __init__(self, cfg: MonitoringSignalConfig):
        self._cfg = cfg

    async def get_config(self, config_cls, catalog_id=None, collection_id=None, ctx=None):
        assert config_cls is MonitoringSignalConfig
        return self._cfg


def _ctx() -> ServiceContext:
    return ServiceContext(engine=None, shutdown=asyncio.Event(), is_ephemeral=False, name="test-host")


def test_initial_poll_is_delayed_one_default_cadence():
    backend = _FakeBackend({MetricKind.CPU_UTILIZATION: 0.7})
    configs = _StubConfigs(MonitoringSignalConfig(enabled=True))
    provider = MonitoringSignalProvider(backend, configs)

    assert provider.initial_delay_seconds == 60.0


@pytest.mark.asyncio
async def test_disabled_by_default_never_calls_backend():
    backend = _FakeBackend({MetricKind.CPU_UTILIZATION: 0.7})
    configs = _StubConfigs(MonitoringSignalConfig())  # enabled=False by default
    provider = MonitoringSignalProvider(backend, configs)

    await provider.tick(_ctx())

    assert backend.calls == 0
    assert provider.scaling_signals() == []


@pytest.mark.asyncio
async def test_enabled_tick_publishes_cpu_and_memory_as_global_signals():
    backend = _FakeBackend({
        MetricKind.CPU_UTILIZATION: 0.55,
        MetricKind.MEMORY_UTILIZATION: 0.40,
    })
    configs = _StubConfigs(MonitoringSignalConfig(enabled=True))
    provider = MonitoringSignalProvider(backend, configs)

    await provider.tick(_ctx())
    signals = provider.scaling_signals()

    by_metric = {s.metric: s for s in signals}
    assert set(by_metric) == {"cpu_utilization", "memory_utilization"}
    assert by_metric["cpu_utilization"].value == 0.55
    assert by_metric["cpu_utilization"].scope == "global"
    assert by_metric["memory_utilization"].value == 0.40


@pytest.mark.asyncio
async def test_value_clamped_to_unit_interval():
    backend = _FakeBackend({MetricKind.CPU_UTILIZATION: 1.4, MetricKind.MEMORY_UTILIZATION: -0.2})
    configs = _StubConfigs(MonitoringSignalConfig(enabled=True))
    provider = MonitoringSignalProvider(backend, configs)

    await provider.tick(_ctx())
    by_metric = {s.metric: s.value for s in provider.scaling_signals()}

    assert by_metric["cpu_utilization"] == 1.0
    assert by_metric["memory_utilization"] == 0.0


@pytest.mark.asyncio
async def test_backend_exception_is_fail_soft_and_keeps_prior_value():
    """A failed read must not break the tick (the loop keeps ticking) and
    must not clear an already-cached signal — it simply doesn't refresh it
    this pass, ageing out naturally via the aggregator's staleness window."""
    backend = _FakeBackend({MetricKind.CPU_UTILIZATION: 0.6})
    configs = _StubConfigs(MonitoringSignalConfig(enabled=True))
    provider = MonitoringSignalProvider(backend, configs)

    await provider.tick(_ctx())
    assert any(s.metric == "cpu_utilization" and s.value == 0.6 for s in provider.scaling_signals())

    backend.set_raises(MetricKind.CPU_UTILIZATION, RuntimeError("Monitoring API 503"))
    await provider.tick(_ctx())  # must not raise

    cpu_signal = next(s for s in provider.scaling_signals() if s.metric == "cpu_utilization")
    assert cpu_signal.value == 0.6  # prior value retained


@pytest.mark.asyncio
async def test_none_reading_keeps_prior_value_without_clearing():
    backend = _FakeBackend({MetricKind.CPU_UTILIZATION: 0.6})
    configs = _StubConfigs(MonitoringSignalConfig(enabled=True))
    provider = MonitoringSignalProvider(backend, configs)

    await provider.tick(_ctx())
    backend.set_value(MetricKind.CPU_UTILIZATION, None)
    await provider.tick(_ctx())

    cpu_signal = next(s for s in provider.scaling_signals() if s.metric == "cpu_utilization")
    assert cpu_signal.value == 0.6


@pytest.mark.asyncio
async def test_never_polled_reports_no_signals():
    """A pod that never won leadership (so ``tick`` never ran on it) must
    contribute nothing — mirrors ``DbContentionMonitor`` before its first
    leader tick."""
    backend = _FakeBackend({MetricKind.CPU_UTILIZATION: 0.9})
    provider = MonitoringSignalProvider(backend, _StubConfigs(MonitoringSignalConfig(enabled=True)))

    assert provider.scaling_signals() == []


@pytest.mark.asyncio
async def test_tick_pass_that_raises_entirely_does_not_propagate(monkeypatch):
    """Even if ``_poll_once`` itself blows up (not just one metric read),
    ``tick`` must swallow it — a poisoned pass costs one cadence, not the
    leadership lock (mirrors ``GcpScalingReconciler.tick``)."""
    backend = _FakeBackend({MetricKind.CPU_UTILIZATION: 0.5})
    configs = _StubConfigs(MonitoringSignalConfig(enabled=True))
    provider = MonitoringSignalProvider(backend, configs)

    async def _boom(cfg):
        raise RuntimeError("unexpected failure")

    monkeypatch.setattr(provider, "_poll_once", _boom)

    await provider.tick(_ctx())  # must not raise
    assert provider.scaling_signals() == []
