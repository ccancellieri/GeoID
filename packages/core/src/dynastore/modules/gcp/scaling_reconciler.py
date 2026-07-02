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

"""Leader-elected reconciler for the protocol-driven autoscaling control loop.

Each tick: read the shared signals document, compute the desired
``min_instances`` via the pure ``compute_desired_min`` function, and — if it
changed and the change is outside the deadband/cooldown — actuate it via
``PlatformScalingProtocol``.

``LEADER_ONLY``: exactly one pod per service drives the actuation, mirroring
``GcpLivenessReconciler``. ``SKIP_EPHEMERAL``: Cloud Run Job containers run
one task and exit; they are not part of the service's scaled instance pool.
"""

from __future__ import annotations

import logging
import time
from typing import Optional, Sequence, Union

from dynastore.models.protocols.configs import ConfigsProtocol
from dynastore.models.scaling import ScalingSignal
from dynastore.models.protocols.platform_scaling import PlatformScalingProtocol
from dynastore.modules.scaling.aggregator import (
    collect_live_signals,
    compute_desired_min,
    compute_duckdb_pool_bump,
    extract_global_metric,
    read_signals_document,
)
from dynastore.modules.scaling.config import ScalingPolicyConfig
from dynastore.tools.background_service import Leadership, PeriodicService, PodPolicy, ServiceContext
from dynastore.tools.cache import get_cache_manager
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


class GcpScalingReconciler(PeriodicService):
    """Periodic service that actuates the autoscaling control loop's decision."""

    name = "gcp_scaling_reconciler"
    leadership = Leadership.LEADER_ONLY
    pod_policy = PodPolicy.SKIP_EPHEMERAL
    cadence_seconds: float = 30.0

    def __init__(
        self,
        platform: Optional[PlatformScalingProtocol] = None,
        configs: Optional[ConfigsProtocol] = None,
    ) -> None:
        self.lock_key: Optional[Union[int, str]] = "gcp-scaling-reconciler"
        self._platform = platform
        self._configs = configs
        self._last_change_ts: float = 0.0
        self._last_pool_change_ts: float = 0.0

    async def _load_policy(self) -> ScalingPolicyConfig:
        configs = self._configs or get_protocol(ConfigsProtocol)
        if configs is None:
            return ScalingPolicyConfig()
        cfg = await configs.get_config(ScalingPolicyConfig)
        return cfg if isinstance(cfg, ScalingPolicyConfig) else ScalingPolicyConfig()

    async def tick(self, ctx: ServiceContext) -> None:
        """One reconcile pass. Fail-soft — never raises, mirroring
        ``GcpLivenessReconciler.tick`` so a transient API error costs one
        cadence period, not the leadership lock."""
        try:
            await self._reconcile_once()
        except Exception as exc:  # noqa: BLE001 — one bad pass must not kill the loop
            logger.error("GcpScalingReconciler: reconcile pass failed: %s", exc, exc_info=True)

    async def _reconcile_once(self) -> None:
        policy = await self._load_policy()
        if not policy.enabled:
            return

        platform = self._platform or get_protocol(PlatformScalingProtocol)
        if platform is None:
            logger.debug("GcpScalingReconciler: no PlatformScalingProtocol registered — skipping.")
            return

        try:
            backend = get_cache_manager().get_async_backend()
        except Exception:
            logger.debug("GcpScalingReconciler: no cache backend available — skipping.", exc_info=True)
            return

        doc = await read_signals_document(backend)
        max_age = policy.publish_interval_seconds * 3
        now = time.time()
        signals = collect_live_signals(doc, max_age_seconds=max_age, now=now)

        current = await platform.get_min_instances()
        if current is None:
            # The live floor is unknown — a transient platform read error, or no
            # credentials. Hold this tick rather than treating "unknown" as
            # min_replicas, which could force a healthy fleet down on one flaky
            # read. The next tick re-reads and acts once the floor is known.
            logger.debug(
                "GcpScalingReconciler: current min_instances unknown — holding this tick."
            )
            return

        desired = compute_desired_min(
            signals,
            policy,
            current_min=current,
            last_change_ts=self._last_change_ts,
            now=now,
        )

        # Memory is a SLOW revision-roll actuator (a limit bump cold-starts
        # every instance) — never actuated per-tick. Surface a recommendation
        # only, so an operator (or a future, separately-gated actuator) can
        # act on it deliberately.
        memory_utilization = extract_global_metric(signals, "memory_utilization")
        if (
            memory_utilization is not None
            and memory_utilization >= policy.memory_recommendation_ceiling
        ):
            logger.warning(
                "GcpScalingReconciler: memory_utilization=%.2f >= recommendation "
                "ceiling %.2f — consider raising the Cloud Run memory limit "
                "(revision-roll change, not actuated automatically).",
                memory_utilization, policy.memory_recommendation_ceiling,
            )

        if desired != current:
            await platform.set_min_instances(desired)
            self._last_change_ts = now
            logger.info(
                "GcpScalingReconciler: min_instances %d -> %d (signals=%d).",
                current, desired, len(signals),
            )

        await self._maybe_bump_duckdb_pool(policy, signals, now)

    async def _maybe_bump_duckdb_pool(
        self,
        policy: ScalingPolicyConfig,
        signals: Sequence[ScalingSignal],
        now: float,
    ) -> None:
        """Actuate the DuckDB pool-autosize decision, if one is due this tick.

        Off by default (``policy.duckdb_pool_autosize``). The actuation
        itself is a single config write: the leader reads the current
        platform-scope ``DuckdbEngineConfig``, bumps ``pool_size`` per
        ``compute_duckdb_pool_bump``, and writes it back. Every instance
        picks the change up via the existing hot-reload path — that write
        IS the fan-out, no extra plumbing needed. Fail-soft: a read/write
        error costs one tick, mirroring the rest of this reconciler.

        Read-modify-write note: ``ConfigsProtocol`` exposes no
        compare-and-set primitive today, so a concurrent operator edit
        (e.g. to ``threads``, a field this actuator never touches) landing
        between our read and our write would otherwise be silently
        reverted by ``model_copy(update=...)``. Mitigated by re-reading and
        recomputing the bump immediately before the write (see below) — a
        residual race window remains between that re-read and the
        ``set_config`` call, which only a real CAS primitive would close
        (tracked as a follow-up, not built here).
        """
        if not policy.duckdb_pool_autosize:
            return

        configs = self._configs or get_protocol(ConfigsProtocol)
        if configs is None:
            return

        from dynastore.modules.db_config.engine_config import DuckdbEngineConfig

        try:
            current_cfg = await configs.get_config(DuckdbEngineConfig)
        except Exception:
            logger.debug(
                "GcpScalingReconciler: DuckdbEngineConfig read failed — "
                "skipping pool-autosize this tick.",
                exc_info=True,
            )
            return

        # First pass: is a bump due at all? Cheap early-exit so a quiet tick
        # (flag off already returned above; here: not saturated, CPU not
        # idle, at the cap, or cooling down) never pays for the re-read below.
        if compute_duckdb_pool_bump(
            signals, policy,
            current_pool_size=current_cfg.pool_size,
            last_pool_change_ts=self._last_pool_change_ts,
            now=now,
        ) is None:
            return

        # Re-read immediately before the write and recompute the bump from
        # THIS fresh copy — narrows the read-modify-write window to just the
        # gap between this read and the write call below, and ensures the
        # update is layered on whatever pool_size (and every other field)
        # actually holds right now, not the possibly-stale first read.
        try:
            fresh_cfg = await configs.get_config(DuckdbEngineConfig)
        except Exception:
            logger.debug(
                "GcpScalingReconciler: DuckdbEngineConfig re-read before "
                "pool-autosize write failed — skipping this tick.",
                exc_info=True,
            )
            return

        new_size = compute_duckdb_pool_bump(
            signals, policy,
            current_pool_size=fresh_cfg.pool_size,
            last_pool_change_ts=self._last_pool_change_ts,
            now=now,
        )
        if new_size is None:
            return

        updated_cfg = fresh_cfg.model_copy(update={"pool_size": new_size})
        try:
            await configs.set_config(DuckdbEngineConfig, updated_cfg)
        except Exception:
            logger.error(
                "GcpScalingReconciler: failed to write DuckdbEngineConfig.pool_size bump",
                exc_info=True,
            )
            return

        self._last_pool_change_ts = now
        logger.info(
            "GcpScalingReconciler: duckdb pool_size %d -> %d "
            "(pool saturated, CPU idle — deepening pool instead of instance count).",
            fresh_cfg.pool_size, new_size,
        )
