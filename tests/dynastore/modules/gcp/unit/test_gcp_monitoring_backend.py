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

"""Unit tests for ``GCPMonitoringBackend`` — the one place a Cloud Monitoring
metric type / REST shape appears. ``httpx.AsyncClient`` is stubbed so these
never touch the network; each test pins one fail-soft path (missing
identity, bad credentials, HTTP error, malformed payload) plus the happy
path's value extraction and Bearer-token auth.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.models.protocols.metrics_backend import MetricKind
from dynastore.modules.gcp.gcp_monitoring_backend import GCPMonitoringBackend


def _payload(value: float, *, kind: str = "doubleValue") -> dict:
    return {"timeSeries": [{"points": [{"value": {kind: value}}]}]}


class _FakeResponse:
    def __init__(self, payload: dict, *, status_error: Exception | None = None):
        self._payload = payload
        self._status_error = status_error

    def raise_for_status(self) -> None:
        if self._status_error:
            raise self._status_error

    def json(self) -> dict:
        return self._payload


class _FakeAsyncClient:
    """Minimal async-context-manager stand-in for ``httpx.AsyncClient``."""

    def __init__(self, response: _FakeResponse, *, captured: dict, timeout=None):
        self._response = response
        self._captured = captured

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, *, params=None, headers=None):
        self._captured["url"] = url
        self._captured["params"] = params
        self._captured["headers"] = headers
        return self._response


def _fake_platform(*, project_id="proj-1", service_name="catalog-dev", token="tok-123"):
    return SimpleNamespace(
        get_project_id=lambda: project_id,
        get_service_name=lambda: service_name,
        get_fresh_token=AsyncMock(return_value=token),
    )


@pytest.mark.asyncio
async def test_reads_latest_point_and_sends_bearer_token():
    captured: dict = {}
    response = _FakeResponse(_payload(0.42))
    platform = _fake_platform(token="tok-abc")

    with patch(
        "dynastore.modules.gcp.gcp_monitoring_backend.httpx.AsyncClient",
        lambda timeout=None: _FakeAsyncClient(response, captured=captured, timeout=timeout),
    ):
        backend = GCPMonitoringBackend(platform)
        value = await backend.read_utilization(MetricKind.CPU_UTILIZATION, window_seconds=120.0)

    assert value == 0.42
    assert captured["headers"]["Authorization"] == "Bearer tok-abc"
    assert "cpu/utilizations" in dict(captured["params"])["filter"]
    assert "catalog-dev" in dict(captured["params"])["filter"]


@pytest.mark.asyncio
async def test_int64_value_is_accepted_too():
    captured: dict = {}
    response = _FakeResponse(_payload(3, kind="int64Value"))
    platform = _fake_platform()

    with patch(
        "dynastore.modules.gcp.gcp_monitoring_backend.httpx.AsyncClient",
        lambda timeout=None: _FakeAsyncClient(response, captured=captured),
    ):
        backend = GCPMonitoringBackend(platform)
        value = await backend.read_utilization(MetricKind.MEMORY_UTILIZATION, window_seconds=120.0)

    assert value == 3.0


@pytest.mark.asyncio
async def test_missing_service_name_returns_none_without_network_call():
    platform = _fake_platform(service_name=None)

    with patch(
        "dynastore.modules.gcp.gcp_monitoring_backend.httpx.AsyncClient",
    ) as client_cls:
        backend = GCPMonitoringBackend(platform)
        value = await backend.read_utilization(MetricKind.CPU_UTILIZATION, window_seconds=120.0)

    assert value is None
    client_cls.assert_not_called()


@pytest.mark.asyncio
async def test_token_fetch_failure_is_fail_soft():
    platform = _fake_platform()
    platform.get_fresh_token = AsyncMock(side_effect=RuntimeError("ADC unavailable"))

    backend = GCPMonitoringBackend(platform)
    value = await backend.read_utilization(MetricKind.CPU_UTILIZATION, window_seconds=120.0)

    assert value is None


@pytest.mark.asyncio
async def test_http_error_is_fail_soft():
    response = _FakeResponse({}, status_error=RuntimeError("503 Service Unavailable"))
    platform = _fake_platform()

    with patch(
        "dynastore.modules.gcp.gcp_monitoring_backend.httpx.AsyncClient",
        lambda timeout=None: _FakeAsyncClient(response, captured={}),
    ):
        backend = GCPMonitoringBackend(platform)
        value = await backend.read_utilization(MetricKind.CPU_UTILIZATION, window_seconds=120.0)

    assert value is None


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"timeSeries": []},
        {"timeSeries": [{"points": []}]},
        {"timeSeries": [{"points": [{"value": {}}]}]},
        {"timeSeries": [{"points": [{}]}]},
    ],
)
@pytest.mark.asyncio
async def test_malformed_payload_shapes_return_none(payload):
    response = _FakeResponse(payload)
    platform = _fake_platform()

    with patch(
        "dynastore.modules.gcp.gcp_monitoring_backend.httpx.AsyncClient",
        lambda timeout=None: _FakeAsyncClient(response, captured={}),
    ):
        backend = GCPMonitoringBackend(platform)
        value = await backend.read_utilization(MetricKind.CPU_UTILIZATION, window_seconds=120.0)

    assert value is None


@pytest.mark.asyncio
async def test_httpx_not_installed_returns_none(monkeypatch):
    from dynastore.modules.gcp import gcp_monitoring_backend as mod

    monkeypatch.setattr(mod, "httpx", None)
    backend = GCPMonitoringBackend(_fake_platform())

    value = await backend.read_utilization(MetricKind.CPU_UTILIZATION, window_seconds=120.0)

    assert value is None
