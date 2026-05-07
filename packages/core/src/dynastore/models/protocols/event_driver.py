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

"""
EventDriverProtocol — abstract driver for event persistence, consumption, and notification.

Replaces EventStorageProtocol with a richer capability/delivery-mode contract that
accommodates multiple backend types (PostgreSQL outbox, GCP Pub/Sub, JMS, EventArc).

Implementations:
  - PostgreSQL (default): ``EventsModule`` in dynastore.modules.events
    Capabilities: PERSISTENCE, LOCKING, NOTIFICATION, SUBSCRIBE, DEAD_LETTER
    DeliveryMode: AT_LEAST_ONCE
  - Other backends: implement a subset of capabilities and declare them accordingly.

Backend-agnostic consumer loop:

    while not shutdown.is_set():
        async with storage.acquire_consumer_lock(leader_key) as is_leader:
            if not is_leader:
                await asyncio.sleep(retry_s)
                continue
            while not shutdown.is_set():
                events = await storage.consume_batch(scope, batch_size, shard=shard_id)
                if not events:
                    await storage.wait_for_events(timeout=poll_interval)
                    continue
                # dispatch…
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Dict,
    FrozenSet,
    List,
    Optional,
    Protocol,
    runtime_checkable,
)

if TYPE_CHECKING:
    from dynastore.modules.events.models import EventSubscription, EventSubscriptionCreate
    from dynastore.modules.db_config.query_executor import DbResource


# ---------------------------------------------------------------------------
# Capability constants
# ---------------------------------------------------------------------------


class EventDriverCapability:
    """Feature flags for EventDriverProtocol backends."""

    PERSISTENCE = "persistence"
    """Durable write that survives restarts (JMS PERSISTENT / PG / Pub/Sub)."""

    LOCKING = "locking"
    """Distributed leader election (e.g. PG advisory lock)."""

    NOTIFICATION = "notification"
    """Push-style readiness signal (PG LISTEN/NOTIFY or Pub/Sub push).
    When present, ``wait_for_events`` returns on signal; otherwise it sleeps."""

    SUBSCRIBE = "subscribe"
    """Webhook/push subscription management."""

    REPLAY = "replay"
    """Consumed events can be replayed (Kafka, Pub/Sub seek)."""

    DEAD_LETTER = "dead_letter"
    """Exhausted events are moved to a dead-letter queue."""


# ---------------------------------------------------------------------------
# Delivery mode constants
# ---------------------------------------------------------------------------


class DeliveryMode:
    """Delivery guarantee exposed by the driver."""

    AT_MOST_ONCE = "at_most_once"
    """Fire-and-forget; no retry on failure."""

    AT_LEAST_ONCE = "at_least_once"
    """Retry on failure — default for PG transactional outbox."""


# ---------------------------------------------------------------------------
# Accumulation policy
# ---------------------------------------------------------------------------


@dataclass
class AccumulationPolicy:
    """How long events accumulate before expiry or archival.

    The concrete backend interprets these values when creating retention jobs.
    """

    retention_days: int = 7
    """CONSUMED events are deleted after this many days."""

    dead_letter_days: int = 30
    """PENDING events exceeding max_retries are moved to DEAD_LETTER and
    kept for this many days before deletion."""

    max_retries: int = 3
    """Number of delivery attempts before an event is moved to DEAD_LETTER."""


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class EventDriverProtocol(Protocol):
    """
    Abstract driver for event persistence, consumption, and notification.

    Implementations: PostgreSQL outbox (EventsModule), Google Pub/Sub,
    JMS broker, GCP EventArc, etc.

    Consumers depend only on this protocol, never on the concrete implementation.
    """

    # ------------------------------------------------------------------
    # Identity & capabilities
    # ------------------------------------------------------------------

    @property
    def capabilities(self) -> FrozenSet[str]:
        """Frozenset of ``EventDriverCapability`` constants this backend supports."""
        ...

    def has_capability(self, cap: str) -> bool:
        """Return True if *cap* is in this backend's capability set."""
        ...

    @property
    def delivery_mode(self) -> str:
        """One of the ``DeliveryMode`` constants."""
        ...

    @property
    def accumulation_policy(self) -> AccumulationPolicy:
        """Retention / retry policy for this backend."""
        ...

    # ------------------------------------------------------------------
    # Schema / DDL lifecycle
    # ------------------------------------------------------------------

    async def initialize(self, conn: Any) -> None:
        """
        Create or verify the global event table (e.g. ``events.events``).

        Called once by EventsModule during its lifespan startup, under an
        advisory lock so the operation is safe across multiple Cloud Run
        instances.
        """
        ...

    async def init_catalog_scope(self, conn: Any, catalog_schema: str) -> None:
        """
        Create or verify per-catalog event tables inside ``catalog_schema``.

        No-op for backends that use the global outbox exclusively (default PG
        implementation after Phase 2).  Future drivers may use this to provision
        per-tenant Pub/Sub topics or JMS destinations.
        """
        ...

    async def init_collection_scope(
        self, conn: Any, catalog_schema: str, collection_id: str
    ) -> None:
        """
        Create a dedicated partition/destination for a collection.

        No-op for backends that use the global outbox exclusively.
        """
        ...

    async def drop_collection_scope(
        self, conn: Any, catalog_schema: str, collection_id: str
    ) -> None:
        """
        Remove the dedicated partition/destination for a collection.

        No-op for backends that use the global outbox exclusively.
        """
        ...

    # ------------------------------------------------------------------
    # Distributed lock
    # ------------------------------------------------------------------

    def acquire_consumer_lock(self, key: str) -> Any:
        """
        Try to acquire a distributed consumer lock as an async context manager.

        Non-blocking: yields ``True`` if this instance became the leader,
        ``False`` otherwise (either the lock is already held by a peer, or the
        backend lacks the ``LOCKING`` capability). Callers must sleep & retry
        on ``False`` to poll for leadership.

        When ``True`` is yielded, the lock is held for the lifetime of the
        context manager and released automatically on connection drop (PG
        advisory lock on a dedicated AUTOCOMMIT connection) — no heartbeat
        required.

        Usage::

            while not shutdown_event.is_set():
                async with storage.acquire_consumer_lock(leader_key) as is_leader:
                    if not is_leader:
                        await asyncio.sleep(5.0)
                        continue
                    await run_consume_loop(shutdown_event)
        """
        ...

    # ------------------------------------------------------------------
    # Produce
    # ------------------------------------------------------------------

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
        Persist an event to the durable outbox.

        Returns the ``event_id`` on success.

        If ``db_resource`` is supplied the INSERT participates in the caller's
        existing transaction (transactional outbox pattern), otherwise the
        implementation opens its own transaction.
        """
        ...

    # ------------------------------------------------------------------
    # Consume
    # ------------------------------------------------------------------

    async def consume_batch(
        self,
        scope: str = "PLATFORM",
        batch_size: int = 100,
        shard: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Claim and return a batch of PENDING events for the given scope.

        Uses ``FOR UPDATE SKIP LOCKED`` (or an equivalent backend mechanism)
        to avoid double-processing.  Claimed events are marked PROCESSING.

        When *shard* is provided, only events for that shard are returned
        (enables the 16-task sharded consumer pattern).

        Each returned dict contains at minimum:
            event_id, event_type, scope, schema_name, catalog_id, collection_id,
            identity_id, payload
        """
        ...

    async def ack(self, event_ids: List[str]) -> None:
        """
        Acknowledge successfully processed events.

        Removes them permanently from the outbox.
        """
        ...

    async def nack(self, event_id: str, error: str) -> None:
        """
        Negative-acknowledge a failed event.

        Increments ``retry_count``.  If max retries are exceeded the event is
        moved to ``DEAD_LETTER`` status; otherwise it is reset to ``PENDING``
        for re-delivery.
        """
        ...

    async def search_events(
        self,
        engine: Any,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        identity_id: Optional[str] = None,
        event_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        Search for events matching the given criteria.
        """
        ...

    # ------------------------------------------------------------------
    # Consumer synchronisation
    # ------------------------------------------------------------------

    async def wait_for_events(self, timeout: float = 10.0) -> None:
        """
        Wait until new events may be available, or until *timeout* seconds
        have elapsed.

        PostgreSQL implementation:
            Listens on channel ``dynastore_events_channel`` and returns as
            soon as a ``pg_notify`` is received (or the timeout expires).

        Non-notify implementations:
            Simply ``await asyncio.sleep(timeout)`` — gives the same polling
            semantics without any special-casing in the consumer.
        """
        ...

    # ------------------------------------------------------------------
    # Webhook subscription management
    # ------------------------------------------------------------------

    async def subscribe(
        self,
        subscription_data: "EventSubscriptionCreate",
        engine: Optional["DbResource"] = None,
    ) -> "EventSubscription":
        """Create or update a webhook subscription."""
        ...

    async def unsubscribe(
        self,
        subscriber_name: str,
        event_type: str,
        engine: Optional["DbResource"] = None,
    ) -> Optional["EventSubscription"]:
        """Delete a webhook subscription."""
        ...

    async def get_subscriptions_for_event_type(
        self,
        event_type: str,
        engine: Optional["DbResource"] = None,
    ) -> List["EventSubscription"]:
        """Return all webhook subscribers for an event type."""
        ...
