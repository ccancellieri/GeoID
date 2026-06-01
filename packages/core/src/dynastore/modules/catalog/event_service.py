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

import logging
import asyncio
import json
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

from dynastore.modules.events.primitives import (
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
from dynastore.modules.db_config.exceptions import TableNotFoundError
from dynastore.tools.discovery import get_protocol
from dynastore.modules.concurrency import get_background_executor, run_in_background
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

    def __init__(self):
        self._consumer_running = False
        self._consumer_task = None

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

    async def consume_batch(
        self,
        batch_size: int = 100,
    ) -> List[Dict[str, Any]]:
        """Claim and return a batch of pending events.

        Delegates to :class:`EventDriverProtocol`.  The driver owns its
        own connection pool — no ``engine``/``db_resource`` parameter
        here because consume-side operations do NOT participate in the
        caller's transaction: events have already committed at publish
        time, and the claim → ACK cycle is a separate transaction the
        driver manages internally.  Transaction-scoped ACK (e.g. "ACK
        only if my downstream write committed") belongs in a future
        ``consume_scoped`` / ``ack_scoped`` pair with an explicit
        ``db_resource`` contract; adding an unused parameter here would
        mislead callers into assuming one existed.
        """
        driver = get_protocol(EventDriverProtocol)
        if not driver:
            logger.warning("EventDriverProtocol not available for consume_batch")
            return []
        return await driver.consume_batch(scope="PLATFORM", batch_size=batch_size)

    async def ack(
        self,
        event_ids: List[str],
    ) -> None:
        """Acknowledge consumed events.

        See :meth:`consume_batch` for the no-``engine`` rationale — ACK
        runs on the driver's own connection, separately from any
        caller transaction.
        """
        if not event_ids:
            return
        driver = get_protocol(EventDriverProtocol)
        if driver:
            await driver.ack(event_ids=event_ids)

    async def nack(
        self,
        event_id: str,
        error: str,
    ) -> None:
        """Negative-acknowledge a consumed event.

        See :meth:`consume_batch` for the no-``engine`` rationale.
        """
        driver = get_protocol(EventDriverProtocol)
        if driver:
            await driver.nack(event_id=event_id, error=error)

    def has_listeners(self) -> bool:
        """Returns True if any async event listeners are registered."""
        return bool(self._async_listeners)

    async def _consume_shard(
        self,
        shard_id: int,
        shutdown_event: Any,
        *,
        scope: str,
    ) -> None:
        """One consumer task per shard — own connection, own SKIP LOCKED scan.

        On ``asyncio.TimeoutError`` (pool checkout timed out — meaning all
        connections are in use), back off exponentially up to 60 s instead
        of retrying every 5 s. Pile-on under sustained pool pressure was
        the dominant amplifier of the production storm; backing off lets
        the pool drain. The backoff resets to 5 s after any successful
        ``consume_batch``.
        """
        if shard_id > 0:
            await asyncio.sleep(shard_id * 0.25)
        logger.info(
            "EventService: shard consumer started (shard=%d, scope=%s).", shard_id, scope
        )
        timeout_backoff_s = 5.0
        while not getattr(shutdown_event, "is_set", lambda: False)():
            try:
                driver = get_protocol(EventDriverProtocol)
                if not driver:
                    await asyncio.sleep(5.0)
                    continue

                events = await driver.consume_batch(
                    scope=scope, batch_size=100, shard=shard_id
                )
                timeout_backoff_s = 5.0  # successful checkout → reset
                if not events:
                    await driver.wait_for_events(timeout=10.0)
                    continue

                for event in events:
                    event_type_str = event.get("event_type", "")
                    payload = event.get("payload", {})
                    if isinstance(payload, str):
                        payload = json.loads(payload)

                    event_id = str(event.get("event_id") or event.get("id"))
                    try:
                        async_listeners = self._async_listeners.get(event_type_str, [])
                        for listener in async_listeners:
                            args = payload.get("args", [])
                            kwargs = payload.get("kwargs", {})
                            await listener(*args, **kwargs)

                        await driver.ack(event_ids=[event_id])
                    except Exception as e:
                        logger.error(
                            f"Failed to process event {event_id}: {e}", exc_info=True
                        )
                        await driver.nack(event_id=event_id, error=str(e))

            except asyncio.CancelledError:
                raise
            except TableNotFoundError as e:
                logger.warning(
                    "EventService shard %d: table missing (%s); backing off 60s.",
                    shard_id, e,
                )
                await asyncio.sleep(60.0)
            except (asyncio.TimeoutError, TimeoutError) as e:
                logger.warning(
                    "EventService shard %d: pool checkout timed out (%s); "
                    "backing off %.0fs (next: %.0fs).",
                    shard_id, e or "<no message>",
                    timeout_backoff_s, min(timeout_backoff_s * 2, 60.0),
                )
                await asyncio.sleep(timeout_backoff_s)
                timeout_backoff_s = min(timeout_backoff_s * 2, 60.0)
            except Exception as e:
                logger.error(
                    "EventService shard %d consumer error: %s: %s",
                    shard_id, type(e).__name__, e or "<no message>",
                    exc_info=True,
                )
                await asyncio.sleep(5.0)

        logger.info(
            "EventService: shard consumer stopped (shard=%d, scope=%s).", shard_id, scope
        )

    async def _run_consume_loop(
        self,
        shutdown_event: Any,
        *,
        scope: str = "PLATFORM",
        channels: Optional[List[str]] = None,
    ) -> None:
        """Spawn 16 shard consumer tasks and wait for all to complete."""
        logger.info("EventService: starting 16-shard consume loop (scope=%s).", scope)
        tasks = [
            asyncio.create_task(
                self._consume_shard(shard_id, shutdown_event, scope=scope),
                name=f"EventShard:{shard_id}",
            )
            for shard_id in range(16)
        ]
        await asyncio.gather(*tasks)
        logger.info("EventService: 16-shard consume loop stopped (scope=%s).", scope)

    async def start_consumer(
        self,
        shutdown_event: Any,
        *,
        scope: str = "PLATFORM",
        leader_key: str = "dynastore.events.consumer.v1",
        channels: Optional[List[str]] = None,
    ) -> None:
        """
        Start the background event consumer under a distributed advisory lock.
        Every worker fleet-wide calls this; exactly one wins and runs the
        consume loop. On pod/worker death the lock is released automatically
        and a waiter takes over.
        """
        if getattr(self, "_consumer_running", False):
            logger.warning("EventService: Consumer already running.")
            return

        # Review #5 regression-fuse: the flag flip lives next to the
        # ``executor.submit`` call (not at the top of ``_leader_loop``)
        # so that a synchronous failure inside ``submit`` — e.g. the
        # background executor has been shut down — rolls the flag back
        # to ``False``.  Without the try/except below, the flag would
        # stay ``True`` forever and every subsequent ``start_consumer``
        # call would short-circuit at the guard above, bricking the
        # consumer across the pod's lifetime.
        self._consumer_running = True
        self._consumer_task = None

        from contextlib import asynccontextmanager
        from dynastore.tools.async_utils import run_leader_loop

        retry_s = 5.0

        @asynccontextmanager
        async def _acquire_leadership():
            driver = get_protocol(EventDriverProtocol)
            if not driver:
                logger.warning(
                    "EventService: EventDriverProtocol not available; retrying in 5s."
                )
                yield False
                return
            async with driver.acquire_consumer_lock(leader_key) as is_leader:
                yield is_leader

        async def _on_leader():
            await self._run_consume_loop(
                shutdown_event,
                scope=scope,
                channels=channels or ["dynastore_events_channel"],
            )

        async def _leader_loop():
            try:
                await run_leader_loop(
                    acquire_leadership=_acquire_leadership,
                    on_leader=_on_leader,
                    name=f"EventService[{leader_key}]",
                    cadence_seconds=retry_s,
                    is_shutdown=lambda: getattr(
                        shutdown_event, "is_set", lambda: False
                    )(),
                )
            finally:
                self._consumer_running = False
                logger.info(
                    "EventService: leader loop stopped (key=%s).", leader_key
                )

        executor = get_background_executor()
        try:
            self._consumer_task = executor.submit(
                _leader_loop(), task_name=f"EventServiceLeader:{leader_key}"
            )
        except BaseException:
            # ``submit`` failed synchronously — the coroutine never ran,
            # so the ``_consumer_running = False`` reset inside
            # ``_leader_loop`` will never execute.  Roll the flag back
            # ourselves so a later ``start_consumer`` retry isn't
            # blocked by the already-running guard.
            self._consumer_running = False
            self._consumer_task = None
            raise
        logger.info(
            "EventService: leader-elected consumer started (key=%s, scope=%s).",
            leader_key,
            scope,
        )

    async def stop_consumer(self) -> None:
        """Stop the background event consumer loop gracefully.

        **Await-after-cancel is load-bearing.**  ``task.cancel()`` only
        *requests* cancellation — the actual coroutine still has to run
        to its next await, catch the ``CancelledError``, and exit.  If
        the caller (fixture teardown, module shutdown) doesn't await
        the task, the asyncio event loop keeps the task pending and
        pytest's runner hangs forever waiting for it to finish.  This
        was the root cause of the 30–50 min CI stalls: every test that
        used ``app_lifespan`` + registered any async event listener
        wired up the consumer via ``start_consumer``, and teardown
        discarded the reference without draining the cancellation.
        """
        self._consumer_running = False
        task = getattr(self, "_consumer_task", None)
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass  # expected — we just cancelled it
            except Exception:
                # A partially-executed consume loop can raise on reconnect-retry
                # paths.  Teardown MUST continue (an unhandled shutdown error
                # masking a test's real failure is worse), but the error is
                # logged rather than silently swallowed.
                logger.warning(
                    "EventService: durable consumer task errored during shutdown",
                    exc_info=True,
                )
            self._consumer_task = None
            logger.info("EventService: Durable event consumer task cancelled.")

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
