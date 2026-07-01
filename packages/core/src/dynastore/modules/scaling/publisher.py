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
"""

from __future__ import annotations

import logging
import os
import socket
from typing import Optional

from dynastore.models.protocols.configs import ConfigsProtocol
from dynastore.models.protocols.scaling_signal import ScalingSignalProtocol
from dynastore.modules.scaling.aggregator import write_global_signals, write_instance_signals
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

    async def _load_policy(self) -> ScalingPolicyConfig:
        configs = self._configs or get_protocol(ConfigsProtocol)
        if configs is None:
            return ScalingPolicyConfig()
        cfg = await configs.get_config(ScalingPolicyConfig)
        return cfg if isinstance(cfg, ScalingPolicyConfig) else ScalingPolicyConfig()

    async def tick(self, ctx: ServiceContext) -> None:
        policy = await self._load_policy()
        self.cadence_seconds = float(policy.publish_interval_seconds)
        if not policy.enabled:
            return

        providers = get_protocols(ScalingSignalProtocol)
        instance_signals = []
        global_signals = []
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
