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

"""Cloud-neutral pull interface for platform utilization telemetry.

The seam between ``MonitoringSignalProvider`` (which turns a raw utilization
reading into a normalized ``ScalingSignal`` the #2333 control loop already
understands) and whatever telemetry system reports it. Mirrors
``PlatformScalingProtocol``: kept minimal, one read method, because only one
implementation (GCP Cloud Monitoring) exists today — a Prometheus/OpenTelemetry,
CloudWatch, or Azure Monitor backend implements this same Protocol only when
it actually needs to, not speculatively. No GCP (or any other provider) type
appears in this module.
"""

from __future__ import annotations

from enum import Enum
from typing import Optional, Protocol, runtime_checkable


class MetricKind(str, Enum):
    """Neutral metric identity — never a provider-specific metric type string
    (e.g. GCP's ``run.googleapis.com/container/cpu/utilizations``).

    Values are always normalized to ``[0, 1]`` utilization ratios, matching
    ``ScalingSignal.value``.
    """

    CPU_UTILIZATION = "cpu_utilization"
    MEMORY_UTILIZATION = "memory_utilization"


@runtime_checkable
class MetricsBackendProtocol(Protocol):
    """Read-only access to a platform's utilization telemetry."""

    async def read_utilization(
        self, metric: MetricKind, *, window_seconds: float
    ) -> Optional[float]:
        """Return the most recent aligned reading for *metric* over the
        trailing *window_seconds*, normalized to ``[0, 1]``.

        Returns ``None`` when the reading is unavailable (no credentials,
        the metric isn't reporting yet, a transient API error). Implementations
        must not raise — a failed read degrades to ``None`` so the caller can
        hold its previously cached value rather than losing the signal for a
        whole tick.
        """
        ...
