from typing import (
    Any,
    Dict,
    List,
    Optional,
    Union,
    Protocol,
    runtime_checkable,
    Callable,
    Coroutine,
    Awaitable,
)

Listener = Callable[..., Coroutine[Any, Any, None]]

@runtime_checkable
class EventsProtocol(Protocol):
    """
    Generic protocol for application domain events.
    Allows components to publish and subscribe to events without direct dependencies.
    """
    
    def register(self, event_type: str, listener: Listener):
        """Register an async listener for an event type."""
        ...

    def sync_event_listener(self, event_type: str):
        """Decorator to register a synchronous event listener."""
        ...

    def async_event_listener(self, event_type: str):
        """Decorator to register an asynchronous (background) event listener."""
        ...

    async def emit(
        self,
        event_type: str,
        *args,
        db_resource: Optional[Any] = None,
        raise_on_error: bool = False,
        **kwargs,
    ):
        """
        Emit an event.
        Persists to Outbox if db_resource is provided.
        Fires sync listeners immediately.
        Schedules async listeners.
        """
        ...
