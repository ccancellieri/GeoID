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

"""
Protocol for durable event bus operations.

Extends EventsProtocol (in-process pub/sub) with outbox semantics:
publish to durable storage, consume with SKIP LOCKED, ack/nack lifecycle.

Implementations:
- PostgreSQL outbox + LISTEN/NOTIFY (default, on-premise + Cloud Run)
- GCP EventArc (future)
"""

from typing import (
    Any,
    Dict,
    List,
    Optional,
    Protocol,
    runtime_checkable,
)

from dynastore.models.protocols.events import EventsProtocol


@runtime_checkable
class EventBusProtocol(EventsProtocol, Protocol):
    """
    Protocol for durable event bus operations.

    Provides outbox semantics on top of EventsProtocol:
    - Publish events to durable storage
    - Consume events in batches (SKIP LOCKED)
    - Acknowledge (delete) or negative-acknowledge (dead-letter)
    - Automatic consumer lifecycle based on registered listeners

    Consumed events are logged to the appropriate scope (catalog logs or
    system logs) via LogsProtocol before deletion.
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

    async def consume_batch(
        self,
        engine: Any,
        batch_size: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Claim and return a batch of pending events using FOR UPDATE SKIP LOCKED.

        Each returned event dict contains at minimum:
        event_id, event_type, scope, schema_name, collection_id, payload.

        Claimed events are marked as PROCESSING to prevent re-consumption.
        """
        ...

    async def ack(
        self,
        engine: Any,
        event_ids: List[str],
    ) -> None:
        """
        Acknowledge consumed events.

        Logs to the appropriate scope via LogsProtocol, then DELETEs the
        events from the outbox. Consumed events are not retained.
        """
        ...

    async def nack(
        self,
        engine: Any,
        event_id: str,
        error: str,
    ) -> None:
        """
        Negative-acknowledge a consumed event.

        Increments retry_count. If max retries exceeded, moves to DEAD_LETTER
        status. Otherwise resets to PENDING for re-delivery.
        """
        ...

    def has_listeners(self) -> bool:
        """
        Returns True if any async event listeners are registered.

        Used to determine whether to automatically start the event consumer.
        Modules opt in by registering listeners via @async_event_listener.
        """
        ...

    async def start_consumer(self, shutdown_event: Any) -> None:
        """
        Start the background event consumer loop.

        The consumer claims events via consume_batch(), dispatches to
        registered listeners, and acks/nacks based on outcome.
        Uses leader election (advisory lock) to minimize DB connections.
        """
        ...

    async def stop_consumer(self) -> None:
        """Stop the background event consumer loop gracefully."""
        ...

