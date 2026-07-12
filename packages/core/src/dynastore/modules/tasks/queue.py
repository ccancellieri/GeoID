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

"""
tasks/queue.py — Zero-polling queue signal system for DynaStore task dispatch.

Every instance opens its own lightweight asyncpg LISTEN connection via
``PgListenBridge``.  No leader election is needed for notification delivery
— all instances receive pg_notify events simultaneously.  The Dispatcher's
``claim_next()`` with ``SKIP LOCKED`` handles contention safely.

A health-timeout fires periodic signals so the Dispatcher re-polls the
queue even when no notifications arrive.
"""

import asyncio
import logging
from typing import Optional, Tuple

from dynastore.tools.async_utils import register_listen_channel

logger = logging.getLogger(__name__)

# Public channel names — used by dispatcher and other modules.
NEW_TASK_QUEUED = "new_task_queued"
TASK_STATUS_CHANGED = "task_status_changed"
EVENTS_CHANNEL = "dynastore_events_channel"
# Cross-pod in-process cancel requests.  Payload is the task_id string.
# Emitted by BackgroundRunner.signal_stop when the target task lives on
# another pod; every pod receives it via pg_notify and the owning pod
# cancels its local asyncio.Task.
CANCEL_REQUESTED = "cancel_requested"

# Per-loop inbox populated by _notification_transform so BackgroundRunner's
# cancel listener can drain task_ids without blocking on signal_bus wait.
_cancel_inbox: "asyncio.Queue[str] | None" = None


def _get_cancel_inbox() -> "asyncio.Queue[str]":
    """Return (or lazily create) the module-level cancel inbox for the current loop."""
    global _cancel_inbox
    if _cancel_inbox is None:
        _cancel_inbox = asyncio.Queue()
    return _cancel_inbox


def _notification_transform(
    channel: str, payload: Optional[str]
) -> Optional[Tuple[str, Optional[str]]]:
    """Transform pg_notify into (signal_name, identifier) for SignalBus.

    - ``new_task_queued``: payload is the task_type (used for capability
      filtering). Emits with ``identifier=None`` (dispatcher wakes for any
      matching task, claim_next handles specifics).
    - ``cancel_requested``: payload is the task_id string.  Pushes to the
      module-level cancel inbox so BackgroundRunner's listener can drain it,
      then emits the broadcast wake signal ``(CANCEL_REQUESTED, None)``.
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

    if channel == CANCEL_REQUESTED:
        if payload:
            # Unbounded inbox drained continuously by every pod's cancel
            # listener — put_nowait cannot block or raise here.
            _get_cancel_inbox().put_nowait(payload)
        # Broadcast wake — identifier=None so every pod's cancel listener wakes.
        return (CANCEL_REQUESTED, None)

    # TASK_STATUS_CHANGED, EVENTS_CHANNEL — forward with payload as identifier
    return (channel, payload)


# Register the task-queue channels with the shared notification hub. Each maps
# through ``_notification_transform`` above. The single bridge is owned by
# ``modules/db_config/notification_hub.py``; this module no longer opens its own
# LISTEN connection. PLATFORM_CONFIG_CHANGED is owned by db_config, not here.
for _task_channel in (
    NEW_TASK_QUEUED,
    TASK_STATUS_CHANGED,
    EVENTS_CHANNEL,
    CANCEL_REQUESTED,
):
    register_listen_channel(_task_channel, _notification_transform)
