#    Copyright 2025 FAO
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

"""
tasks/queue.py — Zero-polling queue signal system for DynaStore task dispatch.

Every instance opens its own lightweight asyncpg LISTEN connection via
``PgListenBridge``.  No leader election is needed for notification delivery
— all instances receive pg_notify events simultaneously.  The Dispatcher's
``claim_next()`` with ``SKIP LOCKED`` handles contention safely.

A health-timeout fires periodic signals so the Dispatcher can run its
Janitor sweep even when no notifications arrive.
"""

import asyncio
import logging
from typing import Optional, Tuple

from sqlalchemy.ext.asyncio import AsyncEngine

from dynastore.tools.async_utils import signal_bus, PgListenBridge
from dynastore.modules.db_config.query_executor import DbResource

logger = logging.getLogger(__name__)

# Public channel names — used by dispatcher and other modules.
NEW_TASK_QUEUED = "new_task_queued"
TASK_STATUS_CHANGED = "task_status_changed"
EVENTS_CHANNEL = "dynastore_events_channel"


def _notification_transform(
    channel: str, payload: Optional[str]
) -> Optional[Tuple[str, Optional[str]]]:
    """Transform pg_notify into (signal_name, identifier) for SignalBus.

    - ``new_task_queued``: payload is the task_type (used for capability
      filtering). Emits with ``identifier=None`` (dispatcher wakes for any
      matching task, claim_next handles specifics).
    - Other channels: forward payload as the identifier so consumers can
      wait for a specific event (e.g. task status change for a specific job).

    Returns ``None`` to suppress the notification (e.g. unhandled task types).
    """
    if channel == NEW_TASK_QUEUED:
        if payload:
            from dynastore.modules.tasks.runners import capability_map
            if payload not in capability_map.all_types:
                return None  # Skip: we can't handle this task type
        return (NEW_TASK_QUEUED, None)

    # TASK_STATUS_CHANGED, EVENTS_CHANNEL — forward with payload as identifier
    return (channel, payload)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def start_queue_listener(
    engine: DbResource,
    shutdown_event: asyncio.Event,
    channel: str = NEW_TASK_QUEUED,
    poll_timeout: float = 30.0,
) -> None:
    """
    Starts the queue listener using PgListenBridge for zero-polling
    notification delivery.

    Every instance opens its own lightweight LISTEN connection.
    PgListenBridge handles auto-reconnect and periodic health signals.
    """
    from dynastore.modules.db_config.query_executor import is_async_resource

    if not is_async_resource(engine):
        logger.info("QueueListener: Sync engine — periodic signal mode.")
        while not shutdown_event.is_set():
            await asyncio.sleep(poll_timeout)
            await signal_bus.emit(NEW_TASK_QUEUED)
        logger.info("QueueListener: Stopped.")
        return

    bridge = PgListenBridge(
        channels=[NEW_TASK_QUEUED, TASK_STATUS_CHANGED, EVENTS_CHANNEL],
        signal_bus=signal_bus,
        health_timeout=poll_timeout,
        transform=_notification_transform,
    )

    bridge_task = asyncio.create_task(bridge.run(engine), name="pg_listen_bridge")

    try:
        await shutdown_event.wait()
    except asyncio.CancelledError:
        logger.info("QueueListener: Cancelled.")
    finally:
        await bridge.stop()
        bridge_task.cancel()
        try:
            await bridge_task
        except asyncio.CancelledError:
            pass

    logger.info("QueueListener: Stopped.")
