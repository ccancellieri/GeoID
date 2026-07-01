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

"""Per-instance autoscaling signal publisher.

Runs on EVERY pod (``Leadership.RUN_EVERYWHERE``, not leader-elected — each
pod can only observe its own instance-scope state). Each tick collects
``scaling_signals()`` from every registered ``ScalingSignalProtocol``
provider and publishes them to the shared Valkey-backed signals document
so the leader-elected platform reconciler can aggregate fleet-wide.

Also carries the #2333 cgroup-v2 self-report (``dynastore.modules.scaling.
cgroup_metrics``) on this same cadence and the same single Valkey backend
handle (``get_cache_manager().get_async_backend()`` below) — no second
connection pool. Cgroup self-report is a PORTABILITY FALLBACK for platforms
that expose the cgroup filesystem (GKE, local, bare-metal), not the Cloud
Run signal source: a live probe on ``dev-dynastore-catalog`` (Cloud Run
gen2) confirmed ``/sys/fs/cgroup/*`` is not exposed inside Cloud Run's
sandbox at all, so ``MonitoringSignalProvider`` (Cloud Monitoring) remains
the only working Cloud Run backend and the one ``compute_desired_min``
reads. Usability is probed once per pod at first tick and cached — when
unusable (Cloud Run today), every later tick skips the cgroup read and
publishes no cgroup signal entirely; when usable, the reading rides in this
pod's existing per-instance payload, published but not yet consumed by
``compute_desired_min`` (dormant, pending a portability target that can
actually exercise it).
"""

from __future__ import annotations

import logging
import os
import socket
import time
from typing import List, Optional

from dynastore.models.protocols.configs import ConfigsProtocol
from dynastore.models.protocols.scaling_signal import ScalingSignalProtocol
from dynastore.models.scaling import ScalingSignal
from dynastore.modules.scaling.aggregator import write_global_signals, write_instance_signals
from dynastore.modules.scaling.cgroup_metrics import (
    CgroupMetricsReader,
    format_cgroup_probe,
    probe_cgroup,
)
from dynastore.modules.scaling.config import ScalingPolicyConfig
from dynastore.tools.background_service import PeriodicService, PodPolicy, ServiceContext
from dynastore.tools.cache import get_cache_manager
from dynastore.tools.discovery import get_protocol, get_protocols

logger = logging.getLogger(__name__)


def _instance_id() -> str:
    """Stable identity for this process within the fleet (hostname:pid).

    Mirrors the runner-identity pattern used for task-claiming
    (``dynastore.modules.tasks.dispatcher._runner_id``): on Cloud Run,
    ``HOSTNAME`` is the unique per-instance revision id, so this is stable
    for the process lifetime and distinct across instances.
    """
    return f"{socket.gethostname()}:{os.getpid()}"


_INSTANCE_ID = _instance_id()


class ScalingSignalPublisher(PeriodicService):
    """Collects and publishes this pod's autoscaling signals every tick."""

    name = "scaling_signal_publisher"
    pod_policy = PodPolicy.SKIP_EPHEMERAL
    cadence_seconds: float = 15.0

    def __init__(self, configs: Optional[ConfigsProtocol] = None) -> None:
        self._configs = configs
        self._cgroup = CgroupMetricsReader()
        # ``None`` = not probed yet. Probed exactly once, on this pod's very
        # first tick (which PeriodicService fires immediately on start — see
        # its docstring), regardless of ``policy.enabled`` — the boot
        # diagnostic is useful even when the loop is off. Cached for the
        # process's lifetime: once ``False`` (e.g. Cloud Run, whose sandbox
        # does not expose ``/sys/fs/cgroup`` at all), every later tick skips
        # the cgroup read and publishes no cgroup signal — a dead filesystem
        # doesn't start working mid-process, so there is nothing to re-probe.
        self._cgroup_usable: Optional[bool] = None

    async def _load_policy(self) -> ScalingPolicyConfig:
        configs = self._configs or get_protocol(ConfigsProtocol)
        if configs is None:
            return ScalingPolicyConfig()
        cfg = await configs.get_config(ScalingPolicyConfig)
        return cfg if isinstance(cfg, ScalingPolicyConfig) else ScalingPolicyConfig()

    async def tick(self, ctx: ServiceContext) -> None:
        policy = await self._load_policy()
        self.cadence_seconds = float(policy.publish_interval_seconds)

        if self._cgroup_usable is None:
            try:
                diag = probe_cgroup()
                logger.info(format_cgroup_probe(diag))
                self._cgroup_usable = bool(diag["usable"])
            except Exception:
                logger.debug("scaling: cgroup_probe failed (best-effort)", exc_info=True)
                self._cgroup_usable = False

        # Skip the read entirely when the boot probe found no cgroup
        # filesystem — no per-tick cost, no per-tick log, on a platform
        # (Cloud Run) where it will never succeed.
        cgroup_cpu: Optional[float] = None
        cgroup_mem: Optional[float] = None
        if self._cgroup_usable:
            try:
                cgroup_cpu = self._cgroup.read_cpu_utilization()
                cgroup_mem = self._cgroup.read_memory_utilization()
            except Exception:
                logger.debug("scaling: cgroup read failed (best-effort)", exc_info=True)

        if not policy.enabled:
            return

        providers = get_protocols(ScalingSignalProtocol)
        instance_signals: List[ScalingSignal] = []
        global_signals: List[ScalingSignal] = []
        for provider in providers:
            try:
                for signal in provider.scaling_signals():
                    if signal.scope == "instance":
                        instance_signals.append(signal)
                    else:
                        global_signals.append(signal)
            except Exception:
                logger.debug(
                    "scaling: provider %r raised collecting signals", provider, exc_info=True
                )

        # Dormant fallback: rides in this pod's existing per-instance
        # payload, published through the SAME backend handle below — no
        # second Valkey client, no new connection pool. Not consumed by
        # ``compute_desired_min`` (that stays on the Monitoring-derived
        # global signal) — publish-only until a portability target proves
        # it out.
        now = time.time()
        if cgroup_cpu is not None:
            instance_signals.append(
                ScalingSignal(
                    source="cgroup", metric="cpu_utilization",
                    value=cgroup_cpu, scope="instance", ts=now,
                )
            )
        if cgroup_mem is not None:
            instance_signals.append(
                ScalingSignal(
                    source="cgroup", metric="memory_utilization",
                    value=cgroup_mem, scope="instance", ts=now,
                )
            )

        try:
            backend = get_cache_manager().get_async_backend()
        except Exception:
            logger.debug("scaling: no cache backend available — skipping publish", exc_info=True)
            return

        max_age = policy.publish_interval_seconds * 3
        await write_instance_signals(
            backend, _INSTANCE_ID, instance_signals, max_age_seconds=max_age
        )
        await write_global_signals(backend, global_signals, max_age_seconds=max_age)
