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

"""Slow, corroborating platform-metrics tier for the #2333 autoscaling loop.

Before this provider, every signal feeding the control loop was internal
(DuckDB / PG connection-pool saturation) — cheap and instant, but blind to
*why* a pod is busy. A live dev load test proved the actual SLA wall was PG
pool starvation while CPU stayed under 44%: the pool-only signal correctly
said "hot", but the aggregator had no way to tell "CPU-bound, add instances"
apart from "pool-starved with CPU to spare, deepen the pool instead" — it
would always reach for the one lever it knew about (more instances), which
does not relieve DB-side contention.

This provider closes that gap by reading CPU / memory utilization from a
:class:`~dynastore.models.protocols.metrics_backend.MetricsBackendProtocol`
and republishing them as ``scope="global"`` :class:`ScalingSignal` — the same
shape ``PgPoolSignalProvider`` (instance-scope) and ``DbContentionMonitor``
(global-scope) already produce, so ``ScalingSignalPublisher`` and
``compute_desired_min`` consume it with no special-casing.

Cloud-neutral by design: this module never imports a cloud SDK. The actual
metrics read (Cloud Monitoring, Prometheus, CloudWatch, ...) is entirely the
concern of whatever :class:`MetricsBackendProtocol` implementation is
injected — see ``dynastore.modules.gcp.gcp_monitoring_backend`` for the one
that exists today.

Deliberately mirrors ``DbContentionMonitor``'s two-speed shape: a
leader-elected, cost-bounded poll (``tick``/``_poll_once``) caches the latest
reading, and the cheap, synchronous ``scaling_signals()`` — called every
publish cadence by ``ScalingSignalPublisher`` — only ever reads that cache,
never triggers a network call itself.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Union

if TYPE_CHECKING:
    from dynastore.models.scaling import ScalingSignal

from dynastore.models.protocols.configs import ConfigsProtocol
from dynastore.models.protocols.metrics_backend import MetricKind, MetricsBackendProtocol
from dynastore.modules.scaling.config import MonitoringSignalConfig
from dynastore.tools.background_service import (
    Leadership,
    PeriodicService,
    PodPolicy,
    ServiceContext,
)
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

# Advisory lock key for leader election. A distinct string key, same
# convention as the GCP reconcilers which key by plain string rather than a
# bigint constant. Listed alongside every other static lock/lease key in
# modules/tasks/durable/lock_registry.py.
_MONITORING_SIGNAL_LOCK_KEY = "monitoring-signal-provider"


class MonitoringSignalProvider(PeriodicService):
    """``ScalingSignalProtocol`` producer for platform CPU/memory utilization.

    LEADER_ONLY: the metrics this provider reads (Cloud Run service CPU/
    memory utilization, etc.) are fleet-wide aggregates, identical for every
    pod — exactly one poll per fleet is correct, mirroring
    ``DbContentionMonitor``. SKIP_EPHEMERAL: one-shot job containers are not
    part of the scaled service's instance pool.
    """

    name = "monitoring_signal_provider"
    leadership = Leadership.LEADER_ONLY
    pod_policy = PodPolicy.SKIP_EPHEMERAL

    def __init__(
        self,
        backend: MetricsBackendProtocol,
        configs: Optional[ConfigsProtocol] = None,
    ) -> None:
        self._backend = backend
        self._configs = configs
        self.lock_key: Optional[Union[int, str]] = _MONITORING_SIGNAL_LOCK_KEY
        # Overwritten from live config on the first tick; this is only the
        # value used before that (registration time / tests constructing the
        # provider directly), matching MonitoringSignalConfig's own default.
        self.cadence_seconds = 60.0
        # metric -> (last normalized value, unix ts it was read). Empty until
        # this pod has run a leader tick and the backend answered at least
        # once — mirrors DbContentionMonitor's "nothing to report yet" state.
        self._last: Dict[MetricKind, Tuple[float, float]] = {}

    async def _load_config(self) -> MonitoringSignalConfig:
        configs = self._configs or get_protocol(ConfigsProtocol)
        if configs is None:
            return MonitoringSignalConfig()
        cfg = await configs.get_config(MonitoringSignalConfig)
        return cfg if isinstance(cfg, MonitoringSignalConfig) else MonitoringSignalConfig()

    async def tick(self, ctx: ServiceContext) -> None:
        """One poll pass. Fail-soft — never raises, mirroring
        ``DbContentionMonitor``/``GcpScalingReconciler`` so a transient
        metrics-API error costs one cadence period, not the leadership lock."""
        cfg = await self._load_config()
        self.cadence_seconds = float(cfg.poll_interval_seconds)
        if not cfg.enabled:
            return
        try:
            await self._poll_once(cfg)
        except Exception:  # noqa: BLE001 — one bad pass must not kill the loop
            logger.warning(
                "monitoring_signal_provider: poll pass failed (best-effort).",
                exc_info=True,
            )

    async def _poll_once(self, cfg: MonitoringSignalConfig) -> None:
        """Read every tracked metric from the backend, caching each success.

        A metric whose read fails or returns ``None`` simply keeps its
        previous cached entry (if any) rather than being cleared — the
        signal ages out naturally via the aggregator's own staleness window
        instead of vanishing the instant one poll hiccups.
        """
        for metric in (MetricKind.CPU_UTILIZATION, MetricKind.MEMORY_UTILIZATION):
            try:
                value = await self._backend.read_utilization(
                    metric, window_seconds=cfg.window_seconds
                )
            except Exception:
                logger.debug(
                    "monitoring_signal_provider: backend raised reading %s "
                    "(best-effort, keeping prior cached value).",
                    metric.value, exc_info=True,
                )
                continue
            if value is None:
                continue
            self._last[metric] = (max(0.0, min(1.0, value)), time.time())

    def scaling_signals(self) -> List["ScalingSignal"]:
        """``ScalingSignalProtocol``: fleet-wide CPU/memory utilization.

        ``scope="global"`` — a Cloud Run service's utilization metrics are
        the same figure regardless of which pod asks, exactly like
        ``DbContentionMonitor``'s ``conn_pressure``. Returns one entry per
        metric that has ever been successfully polled by this pod's leader
        tick; an empty list on a pod that has never won leadership or before
        the first successful poll.
        """
        if not self._last:
            return []
        from dynastore.models.scaling import ScalingSignal

        return [
            ScalingSignal(
                source="monitoring_signal_provider",
                metric=metric.value,
                value=value,
                scope="global",
                ts=ts,
            )
            for metric, (value, ts) in self._last.items()
        ]
