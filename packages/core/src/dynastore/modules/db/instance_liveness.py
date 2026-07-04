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

"""Per-instance liveness heartbeat (geoid#2924).

``configs.leader_lease`` only carries a row for whichever pod currently holds
a given lease — a pod that never wins any election has no row there, so it
cannot answer "is this specific instance alive". ``configs.task_capability_registry``
has the same gap one level up (keyed by ``service``, not by instance).

This module publishes one row per running process, keyed by the stable
per-process ``instance_id`` minted at import time
(:func:`dynastore.modules.db_config.instance.get_instance_id`), renewed on a
cheap cadence by every pod (``RUN_EVERYWHERE`` — every replica must prove its
own liveness, not just the elected leader of some other role). The zombie-
session reaper (``modules/db/zombie_session_reaper.py``) is the sole consumer:
"no row for this instance_id, or a row stale past a generous grace window" is
its proof that an instance is gone.

Gated on the reaper, not just registered
-----------------------------------------
The reaper is disabled by default (opt-in). This heartbeat is a fleet-wide,
every-pod, every-60s background writer, so it must not run at all while the
reaper is off — running it unconditionally would add exactly the kind of
ungated background DB load class geoid#2900 already fights. ``tick()``
re-reads ``ZombieSessionReaperConfig.enabled`` live on every call (cheap: the
central platform-config getter caches it) rather than being gated once at
registration, so flipping the reaper on via a live config PATCH starts the
heartbeat immediately — no redeploy needed to populate the liveness table
before the reaper can trust it.
"""

from __future__ import annotations

import logging

from dynastore.modules.db_config.instance import get_instance_id, get_service_name
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    background_managed_transaction,
)
from dynastore.tools.background_service import PeriodicService, PodPolicy, ServiceContext
from dynastore.tools.protocol_helpers import get_engine

logger = logging.getLogger(__name__)

_UPSERT_SQL = """
INSERT INTO configs.instance_liveness (instance_id, service, renewed_at)
VALUES (:instance_id, :service, now())
ON CONFLICT (instance_id) DO UPDATE SET
    renewed_at = now(),
    service    = EXCLUDED.service
"""


async def heartbeat(engine, *, instance_id: str, service: str) -> None:
    """UPSERT this process's liveness row. Fail-soft — never raises.

    Routed through ``background_managed_transaction`` (not plain
    ``managed_transaction``) so this fleet-wide, every-pod writer competes for
    a DB connection through the same bounded background-concurrency semaphore
    as ``MaintenanceSupervisor`` / the drain spawner, instead of a raw pool
    checkout that could pile up alongside foreground traffic.
    """
    async with background_managed_transaction(engine) as conn:
        await DQLQuery(_UPSERT_SQL, result_handler=ResultHandler.NONE).execute(
            conn, instance_id=instance_id, service=service,
        )


async def _reaper_enabled() -> bool:
    """Live-read ``ZombieSessionReaperConfig.enabled``.

    Imported lazily to avoid a module-load-time dependency between the two
    sibling modules. Any failure to reach the config service (DB down at
    startup, protocol not yet registered) is treated as "disabled" — fail-safe:
    the heartbeat simply skips a tick rather than risk writing when the
    reaper's own state is unknown.
    """
    try:
        from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
        from dynastore.modules.db.zombie_session_reaper import ZombieSessionReaperConfig
        from dynastore.tools.discovery import get_protocol

        svc = get_protocol(PlatformConfigsProtocol)
        if svc is not None:
            cfg = await svc.get_config(ZombieSessionReaperConfig)
            if isinstance(cfg, ZombieSessionReaperConfig):
                return cfg.enabled
    except Exception:  # noqa: BLE001 — fail-safe: treat as disabled
        logger.debug("instance_liveness_heartbeat: reaper-enabled check failed.", exc_info=True)
    return False


class InstanceLivenessHeartbeat(PeriodicService):
    """Renews this process's ``configs.instance_liveness`` row on a cheap cadence.

    ``RUN_EVERYWHERE`` — deliberately NOT leader-elected: every replica must
    publish its own liveness, not just the one pod currently holding some
    other role's lease.  ``PodPolicy.ALL`` — even ephemeral job pods hold DB
    connections while they run and can leave zombie sessions behind, so they
    heartbeat too.

    Always registered (see module docstring), but ``tick()`` does zero DB work
    — not even a config-service round trip beyond the cached read — when
    ``ZombieSessionReaperConfig.enabled`` is False, which is the default.

    Failures are logged and swallowed so a transient DB hiccup never crashes
    the loop; a missed heartbeat just makes this instance briefly look "stale"
    to the reaper, which is the fail-safe direction (worst case, a very slow
    reconnect delays reaping of this instance's own future zombie sessions —
    it never causes another instance's healthy session to be killed).
    """

    name = "instance_liveness_heartbeat"
    pod_policy = PodPolicy.ALL
    cadence_seconds = 60.0

    def __init__(self) -> None:
        self._instance_id = get_instance_id()
        self._service = get_service_name() or "unknown"

    async def tick(self, ctx: ServiceContext) -> None:
        if not await _reaper_enabled():
            return
        engine = ctx.engine
        if engine is None:
            return
        try:
            await heartbeat(engine, instance_id=self._instance_id, service=self._service)
        except Exception:  # noqa: BLE001 — liveness must never crash the loop
            logger.warning(
                "instance_liveness_heartbeat: renew failed for instance %s "
                "(non-fatal; will retry next tick).",
                self._instance_id,
                exc_info=True,
            )
