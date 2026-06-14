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
Protocol for durable event bus operations.

Extends EventsProtocol (in-process pub/sub) with the durable-outbox publish
side and the in-process listener-dispatch surface that the control-plane
EventDrainTask drives. The claim/ack/nack consume lifecycle now lives in the
tasks module (EventDrainTask draining ``tasks.events``), not here.

Implementations:
- PostgreSQL outbox + LISTEN/NOTIFY (default, on-premise + Cloud Run)
- GCP EventArc (future)
"""

from typing import (
    Any,
    Dict,
    Optional,
    Protocol,
    runtime_checkable,
)

from dynastore.models.protocols.events import EventsProtocol


@runtime_checkable
class EventBusProtocol(EventsProtocol, Protocol):
    """
    Protocol for durable event bus operations.

    Provides, on top of EventsProtocol:
    - Publish events to the durable outbox (transactional-outbox pattern)
    - Report whether any async listeners are registered (``has_listeners``)
    - Dispatch an event to the in-process async listeners
      (``dispatch_to_listeners``)

    The claim/ack/nack consume lifecycle is owned by ``EventDrainTask`` (the
    control-plane drain of ``tasks.events``), which resolves an implementation
    of this protocol and calls ``dispatch_to_listeners`` per claimed row.
    """

    async def publish(
        self,
        event_type: str,
        payload: Dict[str, Any],
        scope: str = "PLATFORM",
        schema_name: Optional[str] = None,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        identity_id: Optional[str] = None,
        db_resource: Optional[Any] = None,
    ) -> str:
        """
        Publish an event to the durable outbox.

        Returns event_id on success.
        If db_resource is provided, the INSERT participates in the caller's
        transaction (transactional outbox pattern).
        """
        ...

    def has_listeners(self) -> bool:
        """
        Returns True if any async event listeners are registered.

        Modules opt in by registering listeners via @async_event_listener;
        the EventDrainTask dispatches each claimed event to them.
        """
        ...

    async def dispatch_to_listeners(
        self, event_type: str, payload: Dict[str, Any]
    ) -> None:
        """
        Await every registered async listener for *event_type*.

        The process-independent handler surface: the control-plane
        EventDrainTask resolves the event bus and dispatches each claimed
        event through this method. Awaits each handler with the payload's
        positional/keyword args; the first handler exception propagates so the
        drain can retry the row. An event_type with no registered listeners is
        a successful no-op.
        """
        ...

