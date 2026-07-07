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

"""Shared cross-pod notification hub — one LISTEN bridge for the whole process.

A single :class:`~dynastore.tools.async_utils.PgListenBridge` multiplexes every
registered NOTIFY channel over one connection and fans them into the in-process
``signal_bus``. Features contribute their channels + transforms via
``register_listen_channel`` (see ``tools/async_utils.py``) from their own
modules; this service simply owns the one bridge.

This replaces the previous arrangement where the task queue owned the only
bridge and unrelated features (config hot-reload, and soon collection L1
invalidation for #2143) had to bolt their channels onto ``modules/tasks``.

Runs on every long-lived pod (``RUN_EVERYWHERE``) and is skipped on ephemeral
Cloud Run Job pods (``SKIP_EPHEMERAL``) — job pods claim one task and exit; they
never need to listen. No leadership election: every pod opens its own LISTEN
connection; downstream consumers (dispatcher SKIP LOCKED, token-gated config
reconcile) are safe under broadcast wakes.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional, Union

from dynastore.tools.async_utils import (
    PgListenBridge,
    signal_bus,
    build_registry_transform,
    registered_listen_channels,
)
from dynastore.tools.background_service import (
    Leadership,
    PodPolicy,
    ServiceContext,
)

logger = logging.getLogger(__name__)


async def run_notification_hub(
    engine,
    shutdown_event: asyncio.Event,
    poll_timeout: float = 30.0,
) -> None:
    """Run the single shared LISTEN bridge for all registered channels.

    Mirrors the task-queue listener's lifecycle: a sync engine (no asyncpg
    ``add_listener``) degrades to periodic ``signal_bus`` emission so
    health-beat-driven consumers still fire; an async engine gets a real
    LISTEN bridge with auto-reconnect + health beat.

    Channel registration is import-time, but modules register at different
    lifespan priorities — this hub runs from DBConfigModule (priority 0), well
    before TasksModule (priority 15) imports its queue channels. So the hub
    watches ``registered_listen_channels()`` and rebuilds the bridge whenever
    the set grows, rather than snapshotting once and missing late registrants.
    Rebuilds are rare (only as new modules boot) and cheap (one reconnect).
    """
    from dynastore.modules.db_config.query_executor import is_async_resource

    if not is_async_resource(engine):
        # Sync engine (e.g. tests / sync-only jobs): no asyncpg add_listener.
        # Emit periodic wakes for whatever is registered so health-beat-driven
        # consumers still converge.
        logger.info("NotificationHub: sync engine — periodic signal mode.")
        while not shutdown_event.is_set():
            await asyncio.sleep(poll_timeout)
            for ch in registered_listen_channels():
                await signal_bus.emit(ch)
        logger.info("NotificationHub: stopped.")
        return

    active_channels: list[str] = []
    bridge: Optional[PgListenBridge] = None
    bridge_task: Optional[asyncio.Task] = None

    async def _stop_bridge() -> None:
        nonlocal bridge, bridge_task
        if bridge is not None:
            await bridge.stop()
        if bridge_task is not None:
            bridge_task.cancel()
            try:
                await bridge_task
            except asyncio.CancelledError:
                pass
        bridge, bridge_task = None, None

    try:
        while not shutdown_event.is_set():
            channels = registered_listen_channels()

            if channels and channels != active_channels:
                await _stop_bridge()
                active_channels = channels
                bridge = PgListenBridge(
                    channels=active_channels,
                    signal_bus=signal_bus,
                    health_timeout=poll_timeout,
                    transform=build_registry_transform(),
                )
                bridge_task = asyncio.create_task(
                    bridge.run(engine), name="pg_listen_bridge"
                )
                logger.info(
                    "NotificationHub: LISTEN bridge (re)started for %s.",
                    active_channels,
                )

            # Poll for newly-registered channels on the health-beat cadence;
            # exit promptly on shutdown.
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=poll_timeout)
            except asyncio.TimeoutError:
                continue
    except asyncio.CancelledError:
        logger.info("NotificationHub: cancelled.")
    finally:
        await _stop_bridge()

    logger.info("NotificationHub: stopped.")


class NotificationHubService:
    """BackgroundService wrapper owning the one shared LISTEN bridge per pod."""

    name = "notification_hub"
    leadership = Leadership.RUN_EVERYWHERE
    pod_policy = PodPolicy.SKIP_EPHEMERAL
    lock_key: Optional[Union[int, str]] = None

    def __init__(self, *, poll_timeout: float = 30.0) -> None:
        self._poll_timeout = poll_timeout

    async def run(self, ctx: ServiceContext) -> None:
        await run_notification_hub(
            ctx.engine, ctx.shutdown, poll_timeout=self._poll_timeout,
        )
