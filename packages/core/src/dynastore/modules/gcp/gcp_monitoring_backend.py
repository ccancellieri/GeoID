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

"""GCP Cloud Monitoring implementation of ``MetricsBackendProtocol``.

The ONLY place in this codebase that knows a GCP metric type string or the
Cloud Monitoring REST shape. ``MonitoringSignalProvider`` (cloud-neutral,
in ``dynastore.modules.scaling``) and the #2333 aggregator never see any of
it — they consume ``MetricsBackendProtocol.read_utilization()`` uniformly,
so a Prometheus/OpenTelemetry, CloudWatch, or Azure Monitor backend can
replace this one without touching either.

Uses the Cloud Monitoring API v3 ``timeSeries.list`` REST endpoint directly
via ``httpx`` rather than the ``google-cloud-monitoring`` client library,
which is not a dependency of this codebase today — adding it would pull in
another generated gRPC client for two read-only calls. ``httpx`` and
``google-auth`` are already ``module_gcp`` dependencies (the latter via
``google-cloud-storage``), so this needs nothing new.

Authenticates as the pod's own service account: *platform* supplies a fresh
OAuth2 access token via Application Default Credentials (the same credential
object ``GCPModule`` builds at startup), never a user token.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Protocol, runtime_checkable

try:
    import httpx
except ImportError:  # pragma: no cover — module_gcp always installs httpx
    httpx = None  # type: ignore[assignment]

from dynastore.models.protocols.metrics_backend import MetricKind

logger = logging.getLogger(__name__)

_MONITORING_BASE_URL = "https://monitoring.googleapis.com/v3"

# Neutral MetricKind -> GCP Cloud Run container metric type. The only place
# a GCP metric-type string appears in this codebase.
_GCP_METRIC_TYPES: Dict[MetricKind, str] = {
    MetricKind.CPU_UTILIZATION: "run.googleapis.com/container/cpu/utilizations",
    MetricKind.MEMORY_UTILIZATION: "run.googleapis.com/container/memory/utilizations",
}


@runtime_checkable
class _GcpIdentitySource(Protocol):
    """Structural shape this backend needs from the GCP platform module.

    Satisfied by ``GCPModule`` without importing it here — avoids a circular
    import (``gcp_module.py`` constructs this backend at lifespan time).
    """

    def get_project_id(self) -> Optional[str]: ...
    def get_service_name(self) -> Optional[str]: ...
    async def get_fresh_token(self) -> str: ...


class GCPMonitoringBackend:
    """``MetricsBackendProtocol`` via Cloud Monitoring ``timeSeries.list``.

    Aligns each series to 60s buckets with the P50 aligner, then reduces
    across every container instance reporting for the service with the P50
    cross-series reducer — one fleet-representative figure per poll, not a
    per-instance breakdown (the aggregator only ever wants ``scope="global"``
    from this backend; per-instance detail is the fast pool signals' job).
    """

    def __init__(
        self,
        platform: _GcpIdentitySource,
        *,
        timeout_seconds: float = 10.0,
    ) -> None:
        self._platform = platform
        self._timeout_seconds = timeout_seconds

    async def read_utilization(
        self, metric: MetricKind, *, window_seconds: float
    ) -> Optional[float]:
        """Fail-soft: any missing credential, HTTP error, or malformed
        response returns ``None`` — never raises. The caller
        (``MonitoringSignalProvider``) is responsible for holding its prior
        cached value on ``None``.
        """
        if httpx is None:
            logger.debug("GCPMonitoringBackend: httpx not installed — skipping.")
            return None
        metric_type = _GCP_METRIC_TYPES.get(metric)
        if metric_type is None:
            return None

        try:
            project_id = self._platform.get_project_id()
            service_name = self._platform.get_service_name()
        except Exception:
            logger.debug("GCPMonitoringBackend: identity lookup failed.", exc_info=True)
            return None
        if not project_id or not service_name:
            logger.debug(
                "GCPMonitoringBackend: not a named Cloud Run service "
                "(project_id=%s service_name=%s) — skipping.",
                project_id, service_name,
            )
            return None

        try:
            token = await self._platform.get_fresh_token()
        except Exception:
            logger.debug("GCPMonitoringBackend: no fresh ADC token available.", exc_info=True)
            return None

        now = datetime.now(timezone.utc)
        start = now - timedelta(seconds=window_seconds)
        # A plain dict is sufficient here (no repeated query-param keys are
        # needed — a single ``groupByFields`` value covers the one label we
        # reduce across) and sidesteps httpx's invariant List[Tuple[...]]
        # typing for QueryParamTypes.
        params: Dict[str, str] = {
            "filter": (
                f'metric.type="{metric_type}" AND '
                f'resource.labels.service_name="{service_name}"'
            ),
            "interval.startTime": _rfc3339(start),
            "interval.endTime": _rfc3339(now),
            "aggregation.alignmentPeriod": "60s",
            "aggregation.perSeriesAligner": "ALIGN_PERCENTILE_50",
            "aggregation.crossSeriesReducer": "REDUCE_PERCENTILE_50",
            "aggregation.groupByFields": "resource.label.service_name",
        }
        url = f"{_MONITORING_BASE_URL}/projects/{project_id}/timeSeries"
        try:
            async with httpx.AsyncClient(timeout=self._timeout_seconds) as client:
                response = await client.get(
                    url, params=params, headers={"Authorization": f"Bearer {token}"}
                )
                response.raise_for_status()
                payload = response.json()
        except Exception:
            logger.debug(
                "GCPMonitoringBackend: timeSeries.list failed for %s.",
                metric.value, exc_info=True,
            )
            return None

        return _latest_point_value(payload)


def _rfc3339(ts: datetime) -> str:
    return ts.isoformat(timespec="seconds").replace("+00:00", "Z")


def _latest_point_value(payload: Dict[str, Any]) -> Optional[float]:
    """Extract the most recent point's value from a ``timeSeries.list``
    response. Cloud Monitoring returns points newest-first, so ``points[0]``
    is the latest aligned bucket. Tolerates any malformed/empty shape by
    returning ``None`` rather than raising — this is untrusted network input.
    """
    try:
        series = payload.get("timeSeries") or []
        if not series:
            return None
        points = series[0].get("points") or []
        if not points:
            return None
        value = points[0].get("value") or {}
        raw = value.get("doubleValue", value.get("int64Value"))
        if raw is None:
            return None
        return float(raw)
    except (AttributeError, TypeError, ValueError, IndexError):
        return None
