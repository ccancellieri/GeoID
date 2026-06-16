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

import logging
from typing import (
    ClassVar,
    List,
    Optional,
    Any,
    Callable,
    Coroutine,
    Dict,
    Tuple,
    Union,
)
from collections import defaultdict
from contextlib import asynccontextmanager

from dynastore.modules.tasks.events.primitives import (
    EventScope,
    EventRegistry,
    define_event,
)
from dynastore.models.shared_models import EventType
from dynastore.models.protocols import EventDriverProtocol
from dynastore.models.protocols.event_bus import EventBusProtocol
from dynastore.modules.db_config.query_executor import (
    DbResource,
)
from dynastore.tools.discovery import get_protocol
from dynastore.modules.concurrency import run_in_background
from dynastore.models.driver_context import DriverContext

logger = logging.getLogger(__name__)


class CatalogEventType(EventType):
    # Catalog Lifecycle
    BEFORE_CATALOG_CREATION = define_event(
        "before_catalog_creation", EventScope.PLATFORM
    )
    CATALOG_CREATION = define_event("catalog_creation", EventScope.PLATFORM)
    AFTER_CATALOG_CREATION = define_event("after_catalog_creation", EventScope.PLATFORM)

    BEFORE_CATALOG_UPDATE = define_event("before_catalog_update", EventScope.PLATFORM)
    CATALOG_UPDATE = define_event("catalog_update", EventScope.PLATFORM)
    AFTER_CATALOG_UPDATE = define_event("after_catalog_update", EventScope.PLATFORM)

    BEFORE_CATALOG_DELETION = define_event(
        "before_catalog_deletion", EventScope.PLATFORM
    )
    CATALOG_DELETION = define_event("catalog_deletion", EventScope.PLATFORM)
    AFTER_CATALOG_DELETION = define_event("after_catalog_deletion", EventScope.PLATFORM)

    BEFORE_CATALOG_HARD_DELETION = define_event(
        "before_catalog_hard_deletion", EventScope.PLATFORM
    )
    CATALOG_HARD_DELETION = define_event("catalog_hard_deletion", EventScope.PLATFORM)
    AFTER_CATALOG_HARD_DELETION = define_event(
        "after_catalog_hard_deletion", EventScope.PLATFORM
    )
    CATALOG_HARD_DELETION_FAILURE = define_event(
        "catalog_hard_deletion_failure", EventScope.PLATFORM
    )

    # Collection Lifecycle
    BEFORE_COLLECTION_CREATION = define_event(
        "before_collection_creation", EventScope.CATALOG
    )
    COLLECTION_CREATION = define_event("collection_creation", EventScope.CATALOG)
    AFTER_COLLECTION_CREATION = define_event(
        "after_collection_creation", EventScope.CATALOG
    )

    BEFORE_COLLECTION_UPDATE = define_event(
        "before_collection_update", EventScope.CATALOG
    )
    COLLECTION_UPDATE = define_event("collection_update", EventScope.CATALOG)
    AFTER_COLLECTION_UPDATE = define_event(
        "after_collection_update", EventScope.CATALOG
    )

    BEFORE_COLLECTION_DELETION = define_event(
        "before_collection_deletion", EventScope.CATALOG
    )
    COLLECTION_DELETION = define_event("collection_deletion", EventScope.CATALOG)
    AFTER_COLLECTION_DELETION = define_event(
        "after_collection_deletion", EventScope.CATALOG
    )

    BEFORE_COLLECTION_HARD_DELETION = define_event(
        "before_collection_hard_deletion", EventScope.CATALOG
    )
    COLLECTION_HARD_DELETION = define_event(
        "collection_hard_deletion", EventScope.CATALOG
    )
    AFTER_COLLECTION_HARD_DELETION = define_event(
        "after_collection_hard_deletion", EventScope.CATALOG
    )

    # Asset Lifecycle
    BEFORE_ASSET_CREATION = define_event("before_asset_creation", EventScope.COLLECTION)
    ASSET_CREATION = define_event("asset_creation", EventScope.COLLECTION)
    AFTER_ASSET_CREATION = define_event("after_asset_creation", EventScope.COLLECTION)

    BEFORE_ASSET_UPDATE = define_event("before_asset_update", EventScope.COLLECTION)
    ASSET_UPDATE = define_event("asset_update", EventScope.COLLECTION)
    AFTER_ASSET_UPDATE = define_event("after_asset_update", EventScope.COLLECTION)

    BEFORE_ASSET_DELETION = define_event("before_asset_deletion", EventScope.COLLECTION)
    ASSET_DELETION = define_event("asset_deletion", EventScope.COLLECTION)
    AFTER_ASSET_DELETION = define_event("after_asset_deletion", EventScope.COLLECTION)

    BEFORE_ASSET_HARD_DELETION = define_event(
        "before_asset_hard_deletion", EventScope.COLLECTION
    )
    ASSET_HARD_DELETION = define_event("asset_hard_deletion", EventScope.COLLECTION)
    AFTER_ASSET_HARD_DELETION = define_event(
        "after_asset_hard_deletion", EventScope.COLLECTION
    )

    # Item Lifecycle
    BEFORE_ITEM_CREATION = define_event("before_item_creation", EventScope.COLLECTION)
    ITEM_CREATION = define_event("item_creation", EventScope.COLLECTION)
    AFTER_ITEM_CREATION = define_event("after_item_creation", EventScope.COLLECTION)

    BEFORE_ITEM_UPDATE = define_event("before_item_update", EventScope.COLLECTION)
    ITEM_UPDATE = define_event("item_update", EventScope.COLLECTION)
    AFTER_ITEM_UPDATE = define_event("after_item_update", EventScope.COLLECTION)

    BEFORE_ITEM_DELETION = define_event("before_item_deletion", EventScope.COLLECTION)
    ITEM_DELETION = define_event("item_deletion", EventScope.COLLECTION)
    AFTER_ITEM_DELETION = define_event("after_item_deletion", EventScope.COLLECTION)

    BEFORE_ITEM_HARD_DELETION = define_event(
        "before_item_hard_deletion", EventScope.COLLECTION
    )
    ITEM_HARD_DELETION = define_event("item_hard_deletion", EventScope.COLLECTION)
    AFTER_ITEM_HARD_DELETION = define_event(
        "after_item_hard_deletion", EventScope.COLLECTION
    )

    # M3.0 — metadata-changed events (role-based driver plan §Events).
    #
    # Emitted by ``catalog_router.upsert_catalog_metadata`` and
    # ``delete_catalog_metadata`` on every Primary WRITE/DELETE so INDEX
    # (ReindexWorker) and BACKUP (export-endpoint) consumers see a
    # single signal per change.  Payload shape:
    #
    #   {"catalog_id": "<id>", "domain": "CORE" | "STAC",
    #    "updated_at": "<ISO-8601 UTC>",
    #    "operation": "upsert" | "delete"}
    #
    # Scope = PLATFORM because catalog-tier metadata lives in the global
    # ``catalog.catalog_metadata_*`` tables (not per-tenant schemas).
    # COLLECTION_METADATA_CHANGED scope = CATALOG because it references
    # a per-tenant ``{schema}.collection_metadata_*`` row.
    CATALOG_METADATA_CHANGED = define_event(
        "catalog_metadata_changed", EventScope.PLATFORM
    )
    # ⚠ Forward declaration — NO emitter on this branch.
    # COLLECTION_METADATA_CHANGED is registered here so
    # ``CatalogRoutingConfig`` + ``ReindexWorker`` can reference it
    # without a circular-import workaround, but no code path currently
    # emits it.  The collection-tier equivalent of the catalog-tier
    # M3.0 work (event emission from the split-table router) was NOT
    # delivered on this branch — see the scope-cut tracker for the
    # follow-up.  After PR 1e step 3b the relevant emitter would live
    # behind ``CollectionPostgresqlDriver`` (the composition wrapper
    # that owns the collection_core + collection_stac sidecar fan-out).
    # Until an emitter exists, listeners registered against this event
    # will never fire — they are not an accidental production coupling.
    COLLECTION_METADATA_CHANGED = define_event(
        "collection_metadata_changed", EventScope.CATALOG
    )

    # Bulk Event Lifecycle
    BEFORE_BULK_ITEM_CREATION = define_event(
        "before_bulk_item_creation", EventScope.COLLECTION
    )
    BULK_ITEM_CREATION = define_event("bulk_item_creation", EventScope.COLLECTION)
    AFTER_BULK_ITEM_CREATION = define_event(
        "after_bulk_item_creation", EventScope.COLLECTION
    )

    # Task Lifecycle Events (platform-scoped, fired by runners - no module coupling)
    TASK_FAILED = define_event("task.failed", EventScope.PLATFORM)

Listener = Callable[..., Coroutine[Any, Any, None]]


class EventService(EventBusProtocol):
    """
    Manages event registration, emission, and persistence.
    Implements EventBusProtocol (which extends EventsProtocol) for durable
    event delivery via the global events outbox table.
    """

    # Shared state for listeners across all instances (Singleton behavior for registration)
    _sync_listeners: ClassVar[Dict[str, List[Listener]]] = defaultdict(list)
    _async_listeners: ClassVar[Dict[str, List[Listener]]] = defaultdict(list)

    def register(self, event_type: Union[EventType, str], listener: Listener):
        """Register an async listener for an event type (legacy, defaults to sync)."""
        e_val = event_type.value if isinstance(event_type, EventType) else event_type
        self._sync_listeners[e_val].append(listener)
        logger.debug(f"Registered listener {listener} for event {e_val}")

    def sync_event_listener(self, event_type: Union[EventType, str]):
        """Decorator to register a synchronous (in-transaction) event listener."""
        e_val = event_type.value if isinstance(event_type, EventType) else event_type

        def decorator(func: Listener) -> Listener:
            self._sync_listeners[e_val].append(func)
            logger.info(
                f"Registered sync event listener {func.__module__}.{func.__name__} for {e_val}"
            )
            return func

        return decorator

    def async_event_listener(self, event_type: Union[EventType, str]):
        """Decorator to register an asynchronous (background) event listener."""
        e_val = event_type.value if isinstance(event_type, EventType) else event_type

        def decorator(func: Listener) -> Listener:
            self._async_listeners[e_val].append(func)
            logger.info(
                f"Registered async event listener {func.__module__}.{func.__name__} for {e_val}"
            )
            return func

        return decorator

    def unregister(self, event_type: Union[EventType, str], listener: Listener) -> bool:
        """Remove a previously registered listener. Returns True if found and removed."""
        e_val = event_type.value if isinstance(event_type, EventType) else event_type
        for bucket in (self._async_listeners, self._sync_listeners):
            listeners = bucket.get(e_val)
            if listeners and listener in listeners:
                listeners.remove(listener)
                logger.debug(
                    "Unregistered listener %s.%s for %s",
                    listener.__module__, listener.__name__, e_val,
                )
                return True
        return False

    async def emit(
        self,
        event_type: Union[EventType, str],
        *args,
        db_resource: Optional[DbResource] = None,
        raise_on_error: bool = False,
        **kwargs,
    ):
        """
        Emit an event.
        Persists to Outbox if db_resource is provided.
        Fires sync listeners immediately.
        Schedules async listeners.
        """
        e_val = event_type.value if isinstance(event_type, EventType) else event_type

        if not EventRegistry.is_valid(e_val):
            logger.debug(f"Emitting unregistered event type: {e_val}")

        # 1. Persistence (Outbox Pattern)
        scope = EventRegistry._events.get(e_val, EventScope.PLATFORM)
        is_global_event = scope == EventScope.PLATFORM

        if db_resource:
            catalog_id = kwargs.get("catalog_id")
            collection_id = kwargs.get("collection_id")
            identity_id = kwargs.get("identity_id")
            payload = {"args": args, "kwargs": kwargs}
            schema_name = None

            from dynastore.tools.discovery import get_protocol

            if not is_global_event and catalog_id:
                from dynastore.models.protocols import CatalogsProtocol

                catalogs = get_protocol(CatalogsProtocol)
                if catalogs:
                    schema_name = await catalogs.resolve_physical_schema(
                        catalog_id, ctx=DriverContext(db_resource=db_resource)
                    )

            event_driver = get_protocol(EventDriverProtocol)
            if event_driver:
                await event_driver.publish(
                    event_type=e_val,
                    payload=payload,
                    scope=str(scope),
                    schema_name=schema_name,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    identity_id=identity_id,
                    db_resource=db_resource,
                )

        # 2. Sync Listeners (In-Transaction, Blocking)
        logger.debug(f"Emitting sync event {e_val}")
        for listener in self._sync_listeners.get(e_val, []):
            try:
                await listener(*args, **kwargs)
            except Exception as e:
                logger.error(
                    f"Error in sync event listener for '{e_val}': {e}", exc_info=True
                )
                if raise_on_error:
                    raise e

        # 3. Async Listeners (Background, Non-Blocking)
        async_listeners = self._async_listeners.get(e_val, [])
        if async_listeners:
            logger.debug(
                f"Scheduling {len(async_listeners)} async listeners for {e_val}"
            )
            for listener in async_listeners:
                try:
                    # Hold a strong ref via ``run_in_background`` so the loop
                    # cannot GC the task mid-execution and silently drop the
                    # event delivery (Python asyncio docs §"Important").
                    run_in_background(
                        listener(*args, **kwargs),
                        name=f"event_listener:{e_val}",
                    )
                except Exception as e:
                    logger.error(
                        f"Error scheduling async listener for '{e_val}': {e}",
                        exc_info=True,
                    )

    def emit_side_effect(self, event_type: Union[EventType, str], *args, **kwargs):
        self.emit_detached(event_type, *args, **kwargs)

    def emit_detached(self, event_type: Union[EventType, str], *args, **kwargs):
        e_val = event_type.value if isinstance(event_type, EventType) else event_type
        listeners = self._async_listeners.get(e_val, [])
        if not listeners:
            return

        logger.debug(
            f"Scheduling detached event {e_val} for {len(listeners)} async listeners"
        )

        for listener in listeners:
            try:
                run_in_background(
                    listener(*args, **kwargs),
                    name=f"event_listener_detached:{e_val}",
                )
            except Exception as e:
                logger.error(
                    f"Error scheduling detached listener for '{e_val}': {e}",
                    exc_info=True,
                )

    async def search_events(
        self,
        engine: DbResource,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        identity_id: Optional[str] = None,
        event_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Search events in the global outbox. Delegates to EventDriverProtocol."""
        driver = get_protocol(EventDriverProtocol)
        if driver:
            return await driver.search_events(
                engine=engine,
                catalog_id=catalog_id,
                collection_id=collection_id,
                identity_id=identity_id,
                event_type=event_type,
                limit=limit,
                offset=offset,
            )
        return []

    # --- EventBusProtocol durable outbox methods ---

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
    ) -> Optional[str]:
        """Delegates to EventDriverProtocol."""
        driver = get_protocol(EventDriverProtocol)
        if not driver:
            logger.warning("EventDriverProtocol not available for publish")
            return None

        return await driver.publish(
            event_type=event_type,
            payload=payload,
            scope=scope,
            schema_name=schema_name,
            catalog_id=catalog_id,
            collection_id=collection_id,
            identity_id=identity_id,
            db_resource=db_resource,
        )

    def has_listeners(self) -> bool:
        """Returns True if any async event listeners are registered."""
        return bool(self._async_listeners)

    async def dispatch_to_listeners(
        self, event_type: str, payload: Dict[str, Any]
    ) -> None:
        """Await every registered async listener for *event_type*.

        The in-process async-listener registry (``_async_listeners``) is the
        process-independent handler surface the durable consumer dispatches
        through.  Each handler is awaited with the event payload's positional
        and keyword args (``payload["args"]`` / ``payload["kwargs"]``); the
        first handler exception propagates so the caller can retry the event.

        The sole consumer is ``EventDrainTask`` — the control-plane drain of
        ``tasks.events`` — which resolves this ``EventService`` via
        ``get_protocol(EventBusProtocol)`` and invokes this method per claimed
        row, retrying the row on any handler exception.

        An ``event_type`` with no registered listeners is a successful no-op
        (there is nothing to deliver): the drain treats it as a completed row.
        """
        async_listeners = self._async_listeners.get(event_type, [])
        if not async_listeners:
            return
        args = payload.get("args", []) if isinstance(payload, dict) else []
        kwargs = payload.get("kwargs", {}) if isinstance(payload, dict) else {}
        for listener in async_listeners:
            await listener(*args, **kwargs)

    @asynccontextmanager
    async def transaction(
        self, detached: bool = False, db_resource: Optional[DbResource] = None
    ):
        buffer: List[Tuple[str, tuple, dict]] = []

        async def emitter(event_type: Union[EventType, str], *args, **kwargs):
            e_val = (
                event_type.value if isinstance(event_type, EventType) else event_type
            )
            if db_resource and "db_resource" not in kwargs:
                kwargs["db_resource"] = db_resource

            await self.emit(event_type, *args, **kwargs)
            buffer.append((e_val, args, kwargs))

        yield emitter


event_service = EventService()


def register_event_listener(event_type: Union[EventType, str], listener: Listener):
    event_service.register(event_type, listener)


sync_event_listener = event_service.sync_event_listener
async_event_listener = event_service.async_event_listener


async def emit_event(
    event_type: Union[EventType, str],
    *args,
    db_resource: Optional[DbResource] = None,
    **kwargs,
):
    await event_service.emit(event_type, *args, db_resource=db_resource, **kwargs)
