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

"""Leader-elected drainer for the Valkey-buffered log queue (#2833).

Producers (``LogService._dispatch_to_backends``) push serialized batches
onto a bounded Valkey list instead of every pod dispatching to the log
backend(s) itself. This service is the single reader: one leader-elected
pod pops chunks off that list on a fixed cadence and fans them out to
every registered ``LogBackendProtocol`` via
``log_manager.write_batch_to_backends`` — giving Elasticsearch one bulk
writer instead of one per pod under a write burst.

Architecture contract
----------------------
- Leadership: ``Leadership.LEADER_ONLY`` via the same lease-table election
  ``BackgroundSupervisor`` uses for ``MaintenanceSupervisor`` /
  ``SoftDeleteReaper`` — one pod fleet-wide drains the queue, not one per
  instance.
- No Valkey, no work: ``run_once()`` no-ops when the active cache backend
  does not implement ``ListCacheBackend`` (Valkey unreachable or the
  ``module_cache`` extra not installed) — the producer's direct-dispatch
  fallback already covers that case, so there is nothing queued to drain.
- Bounded per-tick drain: pops at most ``_MAX_CHUNKS_PER_TICK`` chunks of
  ``valkey_drain_chunk_size`` entries each tick, so a large backlog drains
  over several ticks instead of one tick running unbounded.
- Malformed entries (a corrupted or foreign payload on the list) are
  logged and skipped rather than aborting the whole chunk.
"""

from __future__ import annotations

import logging
from typing import List, Optional, Union

from dynastore.modules.catalog.log_manager import (
    LogEntryCreate,
    write_batch_to_backends,
)
from dynastore.modules.catalog.log_service_config import (
    LogServiceConfig,
    load as load_log_config,
)
from dynastore.tools.background_service import (
    Leadership,
    LeaseRenewalMode,
    PeriodicService,
    PodPolicy,
    ServiceContext,
)

logger = logging.getLogger(__name__)

# Advisory lock key for leader election — must not collide with any other
# leader-elected loop (maintenance_supervisor / soft_delete_reaper / ...).
_LOG_DRAINER_ADVISORY_LOCK_KEY = 0x4C4F4744_52414E31  # "LOGDRAN1" in ASCII hex

# Bounds one tick's total work regardless of backlog depth — a burst that
# filled the queue drains over several ticks rather than one very long tick.
_MAX_CHUNKS_PER_TICK = 20


class LogDrainer(PeriodicService):
    """Pops chunks off the shared Valkey log queue and writes them to backends."""

    name = "log_drainer"
    leadership = Leadership.LEADER_ONLY
    pod_policy = PodPolicy.SKIP_EPHEMERAL
    # Default cadence is 2s, far faster than the lease TTL — per-tick
    # acquire/release would hammer configs.leader_lease every couple of
    # seconds. Heartbeat mode holds tenure across ticks and renews on its
    # own ~10s cadence instead (#2900).
    lease_renewal_mode = LeaseRenewalMode.HEARTBEAT

    def __init__(self, config: LogServiceConfig) -> None:
        self.cadence_seconds = config.valkey_drain_interval_seconds
        self.lock_key: Optional[Union[int, str]] = _LOG_DRAINER_ADVISORY_LOCK_KEY

    async def tick(self, ctx: ServiceContext) -> None:
        await self.run_once()

    async def run_once(self) -> None:
        """Drain up to ``_MAX_CHUNKS_PER_TICK`` chunks from the Valkey queue.

        Safe to call directly in tests; never raises — Valkey trouble is
        logged and the tick simply ends (the entries stay queued, or were
        already dispatched directly by the producer fallback).
        """
        from dynastore.tools.cache import get_cache_manager
        from dynastore.models.protocols.cache import ListCacheBackend

        try:
            backend = get_cache_manager().get_async_backend()
        except RuntimeError:
            return
        if not isinstance(backend, ListCacheBackend):
            logger.debug(
                "log_drainer: active cache backend has no list ops — "
                "nothing to drain this tick."
            )
            return

        cfg = await load_log_config()

        for _ in range(_MAX_CHUNKS_PER_TICK):
            try:
                raw_entries = await backend.lpop_many(
                    cfg.valkey_queue_key, cfg.valkey_drain_chunk_size
                )
            except Exception as exc:
                logger.warning(
                    "log_drainer: lpop_many failed (%s); will retry next tick.", exc
                )
                return

            if not raw_entries:
                return

            entries: List[LogEntryCreate] = []
            for raw in raw_entries:
                try:
                    entries.append(LogEntryCreate.model_validate_json(raw))
                except Exception as exc:
                    logger.warning(
                        "log_drainer: dropping malformed queue entry (%s).", exc
                    )

            await write_batch_to_backends(entries)

            if len(raw_entries) < cfg.valkey_drain_chunk_size:
                # Popped fewer than a full chunk — queue is drained for now.
                return
