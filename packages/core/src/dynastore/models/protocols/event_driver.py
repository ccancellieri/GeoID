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
EventDriverProtocol — abstract driver for event persistence and notification.

Replaces EventStorageProtocol with a richer capability/delivery-mode contract
that accommodates multiple backend types (PostgreSQL ``tasks.events``, GCP
Pub/Sub, JMS, EventArc).

Implementations:
  - PostgreSQL (default): ``TaskEventDriver`` in dynastore.modules.tasks.event_driver
    Capabilities: PERSISTENCE, NOTIFICATION, SUBSCRIBE, DEAD_LETTER
    DeliveryMode: AT_LEAST_ONCE
  - Other backends: implement a subset of capabilities and declare them accordingly.

The durable consume lifecycle (claim -> dispatch -> ack/retry) is owned by
``EventDrainTask`` — the control-plane drain of ``tasks.events``.  Drivers
provide the publish side, the NOTIFY/wait signal, subscriptions, and event
search.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    FrozenSet,
    List,
    Optional,
    Protocol,
    runtime_checkable,
)

if TYPE_CHECKING:
    from dynastore.modules.tasks.events.models import EventSubscription, EventSubscriptionCreate
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
    Abstract driver for event persistence and notification.

    Implementations: PostgreSQL ``tasks.events`` (EventsModule), Google
    Pub/Sub, JMS broker, GCP EventArc, etc.

    The sole reader of ``tasks.events`` is ``EventDrainTask``, which claims
    rows, delivers them to in-process async listeners, and applies fenced
    terminal writes (done / retry / dead-letter).

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
        Create or verify the global event table.

        Called once by EventsModule during its lifespan startup.  The default
        PG implementation is a no-op: ``tasks.events`` DDL is owned by the
        tasks module.
        """
        ...

    async def init_catalog_scope(self, conn: Any, catalog_schema: str) -> None:
        """
        Create or verify per-catalog event tables inside ``catalog_schema``.

        No-op for backends that use the global outbox exclusively (default PG
        implementation).  Future drivers may use this to provision per-tenant
        Pub/Sub topics or JMS destinations.
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
        Persist an event to the durable outbox (``tasks.events``).

        Returns the ``event_id`` on success.

        If ``db_resource`` is supplied the INSERT participates in the caller's
        existing transaction (transactional outbox pattern), otherwise the
        implementation opens its own transaction.
        """
        ...

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

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
        scope: str = "PLATFORM",
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
    ) -> Optional["EventSubscription"]:
        """Delete a webhook subscription.

        ``scope``, ``catalog_id``, and ``collection_id`` narrow the deletion
        to one specific scope variant when the same subscriber/event pair has
        subscriptions at multiple scopes.
        """
        ...

    async def get_subscriptions_for_event_type(
        self,
        event_type: str,
        engine: Optional["DbResource"] = None,
    ) -> List["EventSubscription"]:
        """Return all webhook subscribers for an event type."""
        ...
