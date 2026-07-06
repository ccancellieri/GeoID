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

"""ConfigReloadService — Layer A platform-config hot-reload watcher.

A platform-scope config write (``PlatformConfigService.set_config`` /
``set_config_by_ref``) runs its apply handlers immediately, but only on the
instance that received the write. Every other long-lived instance keeps
whatever in-process state those handlers built (e.g. a cached connection)
until it happens to restart. This service closes that gap without a
redeploy, reusing infrastructure that already exists rather than adding a
new DB connection or a dedicated poll loop:

- The write path issues ``pg_notify('platform_config_changed', class_key)``
  on the SAME connection/transaction as the config row (see
  ``platform_config_service.set_config`` / ``set_config_by_ref``), so the
  notification can never be observed before the row it describes has
  committed.
- That channel rides the ONE LISTEN connection ``modules/tasks/queue.py``
  already opens per instance (``PgListenBridge``), including its periodic
  health-beat — no second LISTEN connection is opened for this.
- This service wakes on ``SignalBus.wait_for(PLATFORM_CONFIG_CHANGED, ...)``:
  a real NOTIFY (sub-second), the bridge's health-beat, or its own timeout
  floor if the bridge is ever absent. On every wake it lists the current
  platform_configs rows (one query, platform scope only — never enumerates
  tenant schemas), diffs each class's ``updated_at`` against what this
  instance last saw, and re-runs apply handlers for the classes that moved.

Valkey pub/sub delivery is an explicit future "Layer B" and is out of scope
here.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Union

from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.modules.db_config.stored_config_read import _validate_stored_config
from dynastore.modules.db_config.platform_config_service import (
    PlatformConfigService,
    run_apply_handlers,
)
from dynastore.models.plugin_config import resolve_config_class
from dynastore.tools.async_utils import signal_bus, PLATFORM_CONFIG_CHANGED
from dynastore.tools.background_service import (
    Leadership,
    LeaseRenewalMode,
    PodPolicy,
    ServiceContext,
)

logger = logging.getLogger(__name__)


class ConfigReloadService:
    """BackgroundService wrapper for the platform-config reload watcher.

    Runs on every long-lived pod (RUN_EVERYWHERE) and skips ephemeral Cloud
    Run Job pods (SKIP_EPHEMERAL) — a job pod exits after its one task and
    never needs to converge on a later config change. No leadership election:
    every instance independently reconciles its own in-process state.
    """

    name = "config_reload"
    leadership = Leadership.RUN_EVERYWHERE
    pod_policy = PodPolicy.SKIP_EPHEMERAL
    lock_key: Optional[Union[int, str]] = None
    # Required by the BackgroundService protocol; only consulted for a
    # LEADER_ONLY service under the lease backend, so it is inert for this
    # RUN_EVERYWHERE watcher. Declared to keep the type contract satisfied.
    lease_renewal_mode: LeaseRenewalMode = LeaseRenewalMode.PER_TICK

    def __init__(
        self,
        pcfg: PlatformConfigService,
        *,
        enabled: bool = True,
        reload_interval_seconds: float = 30.0,
    ) -> None:
        self._pcfg = pcfg
        self._enabled = enabled
        self._reload_interval_seconds = reload_interval_seconds
        # {class_key: last_seen updated_at} — seeded at startup, advanced
        # only for classes that successfully reconcile.
        self._last_seen: Dict[str, Any] = {}

    async def run(self, ctx: ServiceContext) -> None:
        if not self._enabled:
            logger.info(
                "ConfigReloadService: disabled via ConfigReloadConfig.enabled=False; not starting."
            )
            return

        await self._seed()

        while not ctx.shutdown.is_set():
            await signal_bus.wait_for(
                PLATFORM_CONFIG_CHANGED, timeout=self._reload_interval_seconds
            )
            if ctx.shutdown.is_set():
                break
            await self._reconcile(ctx)

    async def _seed(self) -> None:
        """Populate the last-seen ``updated_at`` baseline at startup.

        Deliberately does NOT run apply handlers here — boot already applied
        whatever configuration each class loaded from a cold start; this only
        anchors the diff baseline so the first post-boot reconcile does not
        re-fire every handler for configs that haven't actually changed.
        """
        try:
            rows = await self._pcfg.list_configs_versioned()
        except Exception:
            logger.warning(
                "ConfigReloadService: startup seed failed; starting from an "
                "empty baseline (every class present at the first wake will "
                "be treated as changed)",
                exc_info=True,
            )
            return
        for _ref_key, class_key, _config_data, updated_at in rows:
            self._last_seen[class_key] = updated_at

    async def _reconcile(self, ctx: ServiceContext) -> None:
        """One reconcile pass triggered by a wake (NOTIFY, health-beat, or timeout).

        Best-effort per class: a class that fails to resolve/validate/apply
        is logged and skipped without touching its last-seen token (so the
        next wake retries it), and does not prevent the remaining classes in
        the same batch from reconciling.
        """
        try:
            rows = await self._pcfg.list_configs_versioned()
        except Exception:
            logger.warning(
                "ConfigReloadService: reconcile listing failed; will retry on next wake",
                exc_info=True,
            )
            return

        for _ref_key, class_key, config_data, updated_at in rows:
            prior = self._last_seen.get(class_key)
            if prior is not None and updated_at <= prior:
                continue

            cls = resolve_config_class(class_key)
            if cls is None:
                logger.warning(
                    "ConfigReloadService: class_key %r not registered; skipping reconcile",
                    class_key,
                )
                continue

            try:
                config = _validate_stored_config(cls, config_data)
                async with managed_transaction(ctx.engine) as conn:
                    await run_apply_handlers(cls, config, None, None, conn)
            except Exception:
                logger.error(
                    "ConfigReloadService: reconcile failed for class=%r; "
                    "will retry on next wake",
                    class_key,
                    exc_info=True,
                )
                continue

            self._last_seen[class_key] = updated_at
            logger.info(
                "ConfigReloadService: reconciled %s (updated_at=%s)",
                class_key, updated_at,
            )
