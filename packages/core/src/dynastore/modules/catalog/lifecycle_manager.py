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
Lifecycle Management Framework

Provides a centralized registry for modular resource management at Catalog and Collection levels.
Plugins can register initialization and destruction hooks to create/destroy resources (tables, etc.)
in a safe, granular manner that prevents locking issues.

Supports both:
- Synchronous transactional operations (fast, in-transaction)
- Asynchronous external component operations (slow, with config snapshots)
"""

import logging
import asyncio
import inspect
from typing import Callable, Awaitable, List, Dict, Any, Union, Tuple, Optional
from pydantic import BaseModel, ConfigDict
from dynastore.modules.db_config.query_executor import DbResource
from dynastore.tools.async_utils import signal_bus
from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols import DatabaseProtocol


logger = logging.getLogger(__name__)

# Type aliases for synchronous lifecycle hooks (transactional table operations)
# Note: These can now be async OR sync functions, but they run within the synchronous transaction context.
# Hook type with priority
HookWithPriority = Tuple[int, Any]

SyncCatalogInitializer = Union[
    Callable[[DbResource, str, str], Awaitable[None]],
    Callable[[DbResource, str, str], None],
]
SyncCatalogDestroyer = Union[
    Callable[[DbResource, str, str], Awaitable[None]],
    Callable[[DbResource, str, str], None],
]
SyncCollectionInitializer = Union[
    Callable[[DbResource, str, str, str], Awaitable[None]],
    Callable[[DbResource, str, str, str], None],
]
SyncCollectionDestroyer = Union[
    Callable[[DbResource, str, str, str], Awaitable[None]],
    Callable[[DbResource, str, str, str], None],
]

# --- Context Models ---

class LifecycleContext(BaseModel):
    """
    Formal context for background lifecycle operations.
    Bridges the 'visibility gap' by providing metadata that might not 
    yet be committed to the database.
    """
    model_config = ConfigDict(extra="allow")

    physical_schema: str
    physical_table: Optional[str] = None
    config: Dict[str, Any] = {}


# Type aliases for async external component hooks (receive config snapshots, run in background)
AsyncCatalogInitializer = Callable[
    [str, LifecycleContext], Awaitable[None]
]  # (catalog_id, context)
AsyncCatalogDestroyer = Callable[
    [str, LifecycleContext], Awaitable[None]
]  # (catalog_id, context)
AsyncCollectionInitializer = Callable[
    [str, str, LifecycleContext], Awaitable[None]
]  # (catalog_id, collection_id, context)
AsyncCollectionDestroyer = Callable[
    [str, str, LifecycleContext], Awaitable[None]
]  # (catalog_id, collection_id, context)

# Type aliases for asset lifecycle hooks
SyncAssetInitializer = Union[
    Callable[[DbResource, str, str, str, str], Awaitable[None]],
    Callable[[DbResource, str, str, str, str], None],
]
SyncAssetDestroyer = Union[
    Callable[[DbResource, str, str, str, str], Awaitable[None]],
    Callable[[DbResource, str, str, str, str], None],
]
AsyncAssetInitializer = Callable[
    [str, str, str, LifecycleContext], Awaitable[None]
]  # (catalog_id, collection_id, asset_code, context)
AsyncAssetDestroyer = Callable[
    [str, str, str, LifecycleContext], Awaitable[None]
]  # (catalog_id, collection_id, asset_code, context)


class LifecycleRegistry:
    """
    Centralized registry for catalog and collection lifecycle hooks.

    Features:
    - Safe execution: Errors in one plugin don't block others
    - Granular control: Separate hooks for catalog vs collection
    - Extensible: Plugins register via decorators
    - Dual-mode: Sync transactional + Async external components
    """

    def __init__(self):
        # Synchronous transactional hooks (tables)
        self._sync_catalog_initializers: List[HookWithPriority] = []
        # Post-INSERT sync phase: runs in the creation transaction AFTER the
        # ``catalog.catalogs`` row exists (see ``post_create_catalog``). For
        # work that must reference the registry row — e.g. an FK into
        # ``catalog.catalogs`` or an ``UPDATE`` that must match it (#1131).
        self._sync_catalog_post_create: List[HookWithPriority] = []
        self._sync_catalog_destroyers: List[HookWithPriority] = []
        self._sync_collection_initializers: List[HookWithPriority] = []
        self._sync_collection_destroyers: List[HookWithPriority] = []
        # Hard-delete-only hooks: fired only when a collection is permanently removed
        # (partition drop, etc.).  NOT called on soft / logical deletion.
        self._sync_collection_hard_destroyers: List[HookWithPriority] = []

        logger.info(f"INSTANCE: LifecycleRegistry initialized. ID: {id(self)}")

        # Track background tasks to prevent loop errors in tests and allow clean shutdown
        self._active_tasks: List[asyncio.Task] = []

        # Async external component hooks (GCP buckets, etc.)
        self._async_catalog_initializers: List[HookWithPriority] = []
        self._async_catalog_destroyers: List[HookWithPriority] = []
        self._async_collection_initializers: List[HookWithPriority] = []
        self._async_collection_destroyers: List[HookWithPriority] = []

        # Asset lifecycle hooks
        self._sync_asset_initializers: List[HookWithPriority] = []
        self._sync_asset_destroyers: List[HookWithPriority] = []
        self._async_asset_initializers: List[HookWithPriority] = []
        self._async_asset_destroyers: List[HookWithPriority] = []

    def clear(self):
        """Reset ALL registered hooks and cancel+clear active background tasks.
        
        This is a hard reset. Use soft_clear() for test isolation if you want
        to preserve module-level static hooks (decorators).
        """
        self._sync_catalog_initializers.clear()
        self._sync_catalog_post_create.clear()
        self._sync_catalog_destroyers.clear()
        self._sync_collection_initializers.clear()
        self._sync_collection_destroyers.clear()
        self._sync_collection_hard_destroyers.clear()
        self._async_catalog_initializers.clear()
        self._async_catalog_destroyers.clear()
        self._async_collection_initializers.clear()
        self._async_collection_destroyers.clear()
        self._sync_asset_initializers.clear()
        self._sync_asset_destroyers.clear()
        self._async_asset_initializers.clear()
        self._async_asset_destroyers.clear()

        for task in list(self._active_tasks):
            if not task.done():
                task.cancel()
        self._active_tasks.clear()
        logger.info("LifecycleRegistry fully cleared.")

    def soft_clear(self):
        """Standard reset for test isolation.
        
        Preserves 'static' hooks (plain functions, usually from decorators) 
        and only clears 'dynamic' hooks (bound methods, usually instance-specific).
        Also cancels all active background tasks to release DB connections.
        """
        lists_to_process = [
            self._sync_catalog_initializers,
            self._sync_catalog_post_create,
            self._sync_catalog_destroyers,
            self._sync_collection_initializers,
            self._sync_collection_destroyers,
            self._sync_collection_hard_destroyers,
            self._async_catalog_initializers,
            self._async_catalog_destroyers,
            self._async_collection_initializers,
            self._async_collection_destroyers,
            self._sync_asset_initializers,
            self._sync_asset_destroyers,
            self._async_asset_initializers,
            self._async_asset_destroyers,
        ]

        for hook_list in lists_to_process:
            # Keep only hooks that are NOT bound methods (static hooks)
            remaining = [h for h in hook_list if not hasattr(h[1], "__self__")]
            deleted_count = len(hook_list) - len(remaining)
            hook_list.clear()
            hook_list.extend(remaining)
            if deleted_count > 0:
                logger.debug(f"SoftClear: Removed {deleted_count} dynamic hooks from a list.")

        for task in list(self._active_tasks):
            if not task.done():
                task.cancel()
        self._active_tasks.clear()
        logger.info("LifecycleRegistry soft-cleared (Static hooks preserved).")

    def _on_task_done(self, task):
        """Safe callback to remove task from active list."""
        try:
            if task in self._active_tasks:
                self._active_tasks.remove(task)
        except (ValueError, RuntimeError):
            pass

    def _sort_hooks(self, hooks: List[HookWithPriority], reverse=False) -> List[Any]:
        """Returns sorted list of hook functions."""
        # Sort by priority (index 0) descending by default (higher priority first)
        # For destruction, we might want reverse initialization order.
        return [h[1] for h in sorted(hooks, key=lambda x: x[0], reverse=not reverse)]

    def _register_hook(
        self, hook_list: List[HookWithPriority], priority: int, func: Any
    ) -> Any:
        """Internal helper to register a hook with priority and deduplication.
        
        If the handler is a bound method, it replaces any existing hook for the same 
        method name on the same class type (to handle module re-initialization in tests).
        """
        is_bound_method = hasattr(func, "__self__") and func.__self__ is not None
        
        new_hook_list = []
        replaced = False
        
        for p, h in hook_list:
            # Check for exact identity first
            if h == func:
                new_hook_list.append((priority, func))
                replaced = True
                continue
            
            # Check for bound method replacement (same class, same method name, but DIFFERENT instance)
            if is_bound_method and hasattr(h, "__self__") and h.__self__ is not None:
                if (type(h.__self__) is type(func.__self__) and
                    h.__self__ is not func.__self__ and
                    getattr(h, "__name__", None) == getattr(func, "__name__", None)):
                    # Replace old instance's hook with the new one
                    new_hook_list.append((priority, func))
                    replaced = True
                    continue
            
            # Name/Module check for plain functions
            if not is_bound_method and not hasattr(h, "__self__"):
                if (getattr(h, "__name__", None) == getattr(func, "__name__", None) and 
                    getattr(h, "__module__", None) == getattr(func, "__module__", None)):
                    new_hook_list.append((priority, func))
                    replaced = True
                    continue

            new_hook_list.append((p, h))

        if not replaced:
            new_hook_list.append((priority, func))
            
        # Re-sort by priority
        new_hook_list.sort(key=lambda x: x[0])
        
        # Update in place
        hook_list.clear()
        hook_list.extend(new_hook_list)
        
        logger.info(f"Registered/Updated hook: {getattr(func, '__name__', str(func))} (priority: {priority})")
        return func
    def _unregister_hook(self, hook_list: List[HookWithPriority], func: Any) -> None:
        """Internal helper to remove a hook by its function identity."""
        # We search for the tuple(priority, func) and remove it
        hooks_to_remove = [h for h in hook_list if h[1] == func]
        for h in hooks_to_remove:
            hook_list.remove(h)
            logger.info(
                f"Unregistered lifecycle hook: {func.__module__}.{func.__name__}"
            )

    def unregister_async_catalog_initializer(self, func: Any) -> None:
        self._unregister_hook(self._async_catalog_initializers, func)

    def unregister_async_catalog_destroyer(self, func: Any) -> None:
        self._unregister_hook(self._async_catalog_destroyers, func)

    # Synchronous transactional hook registration
    def sync_catalog_initializer(
        self, priority: int = 0
    ) -> Callable[[SyncCatalogInitializer], SyncCatalogInitializer]:
        """Decorator to register a catalog initialization hook."""

        def decorator(func: SyncCatalogInitializer) -> SyncCatalogInitializer:
            return self._register_hook(self._sync_catalog_initializers, priority, func)

        # Handle case where it's used without parentheses: @registry.sync_catalog_initializer
        if callable(priority):
            func = priority
            priority = 0
            return decorator(func)  # type: ignore[return-value]

        return decorator

    def sync_catalog_post_create(
        self, priority: int = 0
    ) -> Callable[[SyncCatalogInitializer], SyncCatalogInitializer]:
        """Decorator to register a post-INSERT catalog hook.

        Unlike ``sync_catalog_initializer`` (which runs *before* the
        ``catalog.catalogs`` row is inserted, for physical-schema/table
        setup), these hooks run in the same creation transaction *after* the
        row exists — for work that must reference it (FK inserts, status
        UPDATEs). Same signature as a sync initializer: ``(conn, schema,
        catalog_id)``.
        """

        def decorator(func: SyncCatalogInitializer) -> SyncCatalogInitializer:
            return self._register_hook(self._sync_catalog_post_create, priority, func)

        if callable(priority):
            func = priority
            priority = 0
            return decorator(func)  # type: ignore[return-value]

        return decorator

    def sync_catalog_destroyer(
        self, priority: int = 0
    ) -> Callable[[SyncCatalogDestroyer], SyncCatalogDestroyer]:
        def decorator(func: SyncCatalogDestroyer) -> SyncCatalogDestroyer:
            return self._register_hook(self._sync_catalog_destroyers, priority, func)

        if callable(priority):
            func = priority
            priority = 0
            return decorator(func)  # type: ignore[return-value]
        return decorator

    def sync_collection_initializer(
        self, priority: int = 0
    ) -> Callable[[SyncCollectionInitializer], SyncCollectionInitializer]:
        def decorator(func: SyncCollectionInitializer) -> SyncCollectionInitializer:
            return self._register_hook(self._sync_collection_initializers, priority, func)

        if callable(priority):
            func = priority
            priority = 0
            return decorator(func)  # type: ignore[return-value]
        return decorator

    def sync_collection_destroyer(
        self, priority: int = 0
    ) -> Callable[[SyncCollectionDestroyer], SyncCollectionDestroyer]:
        def decorator(func: SyncCollectionDestroyer) -> SyncCollectionDestroyer:
            return self._register_hook(self._sync_collection_destroyers, priority, func)

        if callable(priority):
            func = priority
            priority = 0
            return decorator(func)  # type: ignore[return-value]
        return decorator

    def sync_collection_hard_destroyer(
        self, priority: int = 0
    ) -> Callable[[SyncCollectionDestroyer], SyncCollectionDestroyer]:
        def decorator(func: SyncCollectionDestroyer) -> SyncCollectionDestroyer:
            return self._register_hook(
                self._sync_collection_hard_destroyers, priority, func
            )

        if callable(priority):
            func = priority
            priority = 0
            return decorator(func)  # type: ignore[return-value]
        return decorator

    # Async external component hook registration
    def async_catalog_initializer(
        self, priority: int = 0
    ) -> Callable[[AsyncCatalogInitializer], AsyncCatalogInitializer]:
        def decorator(func: AsyncCatalogInitializer) -> AsyncCatalogInitializer:
            return self._register_hook(self._async_catalog_initializers, priority, func)

        if callable(priority):
            func = priority
            priority = 0
            return decorator(func)  # type: ignore[return-value]
        return decorator

    def async_catalog_destroyer(
        self, priority: int = 0
    ) -> Callable[[AsyncCatalogDestroyer], AsyncCatalogDestroyer]:
        def decorator(func: AsyncCatalogDestroyer) -> AsyncCatalogDestroyer:
            return self._register_hook(self._async_catalog_destroyers, priority, func)

        if callable(priority):
            func = priority
            priority = 0
            return decorator(func)  # type: ignore[return-value]
        return decorator

    def async_collection_initializer(
        self, priority: int = 0
    ) -> Callable[[AsyncCollectionInitializer], AsyncCollectionInitializer]:
        def decorator(func: AsyncCollectionInitializer) -> AsyncCollectionInitializer:
            return self._register_hook(self._async_collection_initializers, priority, func)

        if callable(priority):
            func = priority
            priority = 0
            return decorator(func)  # type: ignore[return-value]
        return decorator

    def has_async_collection_initializers(self) -> bool:
        """True if any external async collection initializer is registered.

        ``create_collection`` consults this to decide whether a new collection
        needs a ``PROVISIONING`` window (#2066): with no async initializers the
        collection is ready the moment its registry row commits, so marking it
        provisioning would only add a pointless 409 window.
        """
        return bool(self._async_collection_initializers)

    def async_collection_destroyer(
        self, priority: int = 0
    ) -> Callable[[AsyncCollectionDestroyer], AsyncCollectionDestroyer]:
        def decorator(func: AsyncCollectionDestroyer) -> AsyncCollectionDestroyer:
            return self._register_hook(self._async_collection_destroyers, priority, func)

        if callable(priority):
            func = priority
            priority = 0
            return decorator(func)  # type: ignore[return-value]
        return decorator

    # Asset lifecycle hook registration
    def sync_asset_initializer(
        self, priority: int = 0
    ) -> Callable[[SyncAssetInitializer], SyncAssetInitializer]:
        def decorator(func: SyncAssetInitializer) -> SyncAssetInitializer:
            return self._register_hook(self._sync_asset_initializers, priority, func)

        if callable(priority):
            func = priority
            priority = 0
            return decorator(func)  # type: ignore[return-value]
        return decorator

    def sync_asset_destroyer(
        self, priority: int = 0
    ) -> Callable[[SyncAssetDestroyer], SyncAssetDestroyer]:
        def decorator(func: SyncAssetDestroyer) -> SyncAssetDestroyer:
            return self._register_hook(self._sync_asset_destroyers, priority, func)

        if callable(priority):
            func = priority
            priority = 0
            return decorator(func)  # type: ignore[return-value]
        return decorator

    def async_asset_initializer(
        self, priority: int = 0
    ) -> Callable[[AsyncAssetInitializer], AsyncAssetInitializer]:
        def decorator(func: AsyncAssetInitializer) -> AsyncAssetInitializer:
            return self._register_hook(self._async_asset_initializers, priority, func)

        if callable(priority):
            func = priority
            priority = 0
            return decorator(func)  # type: ignore[return-value]
        return decorator

    def async_asset_destroyer(
        self, priority: int = 0
    ) -> Callable[[AsyncAssetDestroyer], AsyncAssetDestroyer]:
        def decorator(func: AsyncAssetDestroyer) -> AsyncAssetDestroyer:
            return self._register_hook(self._async_asset_destroyers, priority, func)

        if callable(priority):
            func = priority
            priority = 0
            return decorator(func)  # type: ignore[return-value]
        return decorator

    # Synchronous transactional execution
    async def _run_initializer_isolated(
        self, conn, label: str, func, *args, **kwargs
    ) -> bool:
        """
        Run an initializer inside its own SAVEPOINT so that if it fails,
        only the SAVEPOINT is rolled back and the outer transaction
        remains healthy. ``func`` is the un-called initializer; we invoke
        it INSIDE the savepoint (and ``await`` it iff it returned a
        coroutine) so sync initializers are isolated too — pre-calling
        them at the call site would run their side effects outside the
        savepoint, defeating the purpose.

        Returns True if successful, False if the initializer failed
        (non-fatal). Raises if the outer transaction itself is already
        aborted (fatal).
        """
        from sqlalchemy.ext.asyncio import AsyncConnection

        # Only use begin_nested on async connections that support it
        if not isinstance(conn, AsyncConnection):
            # Sync connection or engine — call directly with try/except
            try:
                result = func(*args, **kwargs)
                if inspect.isawaitable(result):
                    await result
                return True
            except Exception as e:
                logger.error(f"{label} failed: {e}", exc_info=True)
                return False

        try:
            async with conn.begin_nested():
                result = func(*args, **kwargs)
                if inspect.isawaitable(result):
                    await result
            return True
        except Exception as e:
            # If begin_nested itself fails, the outer transaction is already aborted
            # (InFailedSQLTransactionError on SAVEPOINT creation). This is fatal.
            error_str = str(e)
            
            # Check for fatal connection errors - we cannot continue on a dead connection
            from dynastore.modules.db_config.exceptions import DatabaseConnectionError
            if isinstance(e, DatabaseConnectionError) or "ConnectionDoesNotExistError" in error_str:
                logger.error(f"{label}: fatal connection error. Aborting lifecycle hooks. Error: {e}")
                raise

            if "InFailedSQLTransaction" in error_str or "current transaction is aborted" in error_str:
                logger.error(
                    f"{label}: outer transaction already aborted before SAVEPOINT. "
                    f"Cannot continue lifecycle initialization. Error: {e}"
                )
                raise  # fatal — caller must roll back the outer transaction
            # Otherwise the SAVEPOINT was rolled back cleanly; log and continue
            logger.error(f"{label} failed (SAVEPOINT rolled back, outer tx healthy): {e}", exc_info=True)
            return False

    async def init_catalog(
        self, conn: DbResource, schema: str, catalog_id: str
    ) -> None:
        """Execute all registered sync catalog initializers (transactional).
        
        Each initializer runs in its own SAVEPOINT so a failing hook cannot
        poison the outer transaction.
        """
        logger.info(
            f"Initializing catalog resources for '{catalog_id}' (schema: {schema})"
        )

        for initializer in self._sort_hooks(self._sync_catalog_initializers):
            logger.info(f"Calling sync catalog initializer: {initializer.__module__}.{initializer.__name__}")
            label = (
                f"Sync catalog initializer {initializer.__module__}.{initializer.__name__} "
                f"for '{catalog_id}'"
            )
            try:
                await self._run_initializer_isolated(
                    conn, label, initializer, conn, schema, catalog_id,
                )
            except Exception:
                # Outer transaction is aborted — stop trying further initializers
                logger.error(
                    f"Outer transaction aborted during catalog initialization for '{catalog_id}'. "
                    "Aborting remaining initializers."
                )
                raise

    async def post_create_catalog(
        self, conn: DbResource, schema: str, catalog_id: str
    ) -> None:
        """Execute all registered post-INSERT catalog hooks (transactional).

        Runs in the creation transaction after ``INSERT INTO catalog.catalogs``,
        so hooks may reference the registry row. Each hook runs in its own
        SAVEPOINT so a failing hook cannot poison the outer transaction.
        """
        if not self._sync_catalog_post_create:
            return
        logger.info(
            f"Running post-create catalog hooks for '{catalog_id}' (schema: {schema})"
        )

        for hook in self._sort_hooks(self._sync_catalog_post_create):
            label = (
                f"Post-create catalog hook {hook.__module__}.{hook.__name__} "
                f"for '{catalog_id}'"
            )
            try:
                await self._run_initializer_isolated(
                    conn, label, hook, conn, schema, catalog_id,
                )
            except Exception:
                # Outer transaction is aborted — stop trying further hooks
                logger.error(
                    f"Outer transaction aborted during post-create hooks for '{catalog_id}'. "
                    "Aborting remaining hooks."
                )
                raise

    async def destroy_catalog(
        self, conn: DbResource, schema: str, catalog_id: str
    ) -> None:
        """Execute all registered sync catalog destroyers (transactional)."""
        logger.info(
            f"Destroying catalog resources for '{catalog_id}' (schema: {schema})"
        )

        # Sort destroyers in reverse priority (last initialized, first destroyed)
        for destroyer in self._sort_hooks(self._sync_catalog_destroyers, reverse=True):
            try:
                if inspect.iscoroutinefunction(destroyer):
                    await destroyer(conn, schema, catalog_id)
                else:
                    destroyer(conn, schema, catalog_id)
            except Exception as e:
                logger.error(
                    f"Sync catalog destroyer {destroyer.__module__}.{destroyer.__name__} "
                    f"failed for '{catalog_id}': {e}",
                    exc_info=True,
                )

    async def init_collection(
        self,
        conn: DbResource,
        schema: str,
        catalog_id: str,
        collection_id: str,
        **kwargs,
    ) -> None:
        """Execute all registered sync collection initializers (transactional).
        
        Each initializer runs in its own SAVEPOINT so a failing hook cannot
        poison the outer transaction.
        """
        logger.info(
            f"Initializing collection resources for '{catalog_id}:{collection_id}' "
            f"(schema: {schema})"
        )

        for initializer in self._sort_hooks(self._sync_collection_initializers):
            label = (
                f"Sync collection initializer {initializer.__module__}.{initializer.__name__} "
                f"for '{catalog_id}:{collection_id}'"
            )
            try:
                await self._run_initializer_isolated(
                    conn, label, initializer, conn, schema, catalog_id, collection_id,
                    **kwargs,
                )
            except Exception:
                # Outer transaction is aborted — stop trying further initializers
                logger.error(
                    f"Outer transaction aborted during collection initialization for "
                    f"'{catalog_id}:{collection_id}'. Aborting remaining initializers."
                )
                raise

    async def destroy_collection(
        self, conn: DbResource, schema: str, catalog_id: str, collection_id: str
    ) -> None:
        """Execute all registered sync collection destroyers (transactional)."""
        logger.info(
            f"Destroying collection resources for '{catalog_id}:{collection_id}' "
            f"(schema: {schema})"
        )

        for destroyer in self._sort_hooks(self._sync_collection_destroyers, reverse=True):
            try:
                if inspect.iscoroutinefunction(destroyer):
                    await destroyer(conn, schema, catalog_id, collection_id)
                else:
                    destroyer(conn, schema, catalog_id, collection_id)
            except Exception as e:
                logger.error(
                    f"Sync collection destroyer {destroyer.__module__}.{destroyer.__name__} "
                    f"failed for '{catalog_id}:{collection_id}': {e}",
                    exc_info=True,
                )

    async def hard_destroy_collection(
        self, conn: DbResource, schema: str, catalog_id: str, collection_id: str
    ) -> None:
        """Execute all registered hard-delete collection hooks."""
        logger.info(
            f"Hard destroying collection resources for '{catalog_id}:{collection_id}' "
            f"(schema: {schema})"
        )

        for destroyer in self._sort_hooks(
            self._sync_collection_hard_destroyers, reverse=True
        ):
            try:
                if inspect.iscoroutinefunction(destroyer):
                    await destroyer(conn, schema, catalog_id, collection_id)
                else:
                    destroyer(conn, schema, catalog_id, collection_id)
            except Exception as e:
                logger.error(
                    f"Sync collection hard destroyer {destroyer.__module__}.{destroyer.__name__} "
                    f"failed for '{catalog_id}:{collection_id}': {e}",
                    exc_info=True,
                )

    def init_async_catalog(
        self, catalog_id: str, context: LifecycleContext
    ) -> asyncio.Task | None:
        """Execute all registered async catalog initializers (external)."""
        if not self._async_catalog_initializers:
            return None

        logger.info(f"Scheduling async catalog initialization for '{catalog_id}'")

        from dynastore.modules.concurrency import run_in_background

        async def _run_all():
            # Wait for the catalog to be fully persisted (AFTER_CATALOG_CREATION signal)
            # This bridges the visibility gap for background tasks.
            try:
                # 3 second timeout — signal should arrive within milliseconds in normal use
                await signal_bus.wait_for(
                    "AFTER_CATALOG_CREATION", identifier=catalog_id, timeout=3.0
                )
            except asyncio.TimeoutError:
                logger.warning(
                    f"Background task for catalog '{catalog_id}' timed out waiting for AFTER_CATALOG_CREATION signal. Proceeding anyway..."
                )

            for initializer in self._sort_hooks(self._async_catalog_initializers):
                try:
                    await initializer(catalog_id, context)
                except Exception as e:
                    logger.error(
                        f"Async catalog initializer {initializer.__module__}.{initializer.__name__} "
                        f"failed for '{catalog_id}': {e}",
                        exc_info=True,
                    )
            
            # Cleanup signal
            await signal_bus.clear("AFTER_CATALOG_CREATION", identifier=catalog_id)

        task = run_in_background(_run_all(), name=f"init_catalog_async_{catalog_id}")
        self._active_tasks.append(task)
        task.add_done_callback(self._on_task_done)
        return task

    def destroy_async_catalog(
        self, catalog_id: str, context: LifecycleContext
    ) -> asyncio.Task | None:
        """
        Execute async external component destroyers for catalog (e.g., GCP bucket deletion).
        Runs in background with config snapshot BEFORE schema drop.
        """
        if not self._async_catalog_destroyers:
            return None

        logger.info(f"Scheduling async catalog destruction for '{catalog_id}'")

        from dynastore.modules.concurrency import run_in_background

        async def _run_all():
            for destroyer in self._sort_hooks(self._async_catalog_destroyers, reverse=True):
                try:
                    await destroyer(catalog_id, context)
                except Exception as e:
                    logger.error(
                        f"Async catalog destroyer {destroyer.__module__}.{destroyer.__name__} "
                        f"failed for '{catalog_id}': {e}",
                        exc_info=True,
                    )
                    # Emit failure event
                    try:
                        from dynastore.modules.catalog.event_service import (
                            emit_event,
                            CatalogEventType,
                        )

                        db_proto = get_protocol(DatabaseProtocol)
                        if db_proto:
                            await emit_event(
                                CatalogEventType.CATALOG_HARD_DELETION_FAILURE,
                                catalog_id=catalog_id,
                                payload={
                                    "error": str(e),
                                    "schema": context.physical_schema,
                                    "component": f"{destroyer.__module__}.{destroyer.__name__}",
                                },
                                db_resource=db_proto.engine,
                            )
                    except Exception as emit_err:
                        logger.error(
                            f"Failed to emit CATALOG_HARD_DELETION_FAILURE: {emit_err}"
                        )

        task = run_in_background(_run_all(), name=f"destroy_catalog_async_{catalog_id}")
        self._active_tasks.append(task)
        task.add_done_callback(self._on_task_done)
        return task

    def init_async_collection(
        self,
        catalog_id: str,
        collection_id: str,
        context: LifecycleContext,
        on_complete: Optional[Callable[[], Awaitable[None]]] = None,
    ) -> asyncio.Task | None:
        """Execute all registered async collection initializers (external).

        ``on_complete`` is an optional finalizer awaited once every initializer
        has run (in a ``finally``, so it fires even if an initializer raised).
        ``create_collection`` passes it to flip ``lifecycle_status`` from
        ``provisioning`` to ``active`` (#2066) once the async window closes.
        When ``on_complete`` is supplied the task is scheduled even if no
        initializers are registered, so the flip is never skipped.
        """
        if not self._async_collection_initializers and on_complete is None:
            return None

        logger.info(
            f"Scheduling async collection initialization for '{catalog_id}:{collection_id}'"
        )

        from dynastore.modules.concurrency import run_in_background

        async def _run_all():
            try:
                # Wait for the collection to be fully persisted (AFTER_COLLECTION_CREATION signal)
                try:
                    # 3 second timeout — signal should arrive within milliseconds in normal use
                    await signal_bus.wait_for(
                        "AFTER_COLLECTION_CREATION", identifier=collection_id, timeout=3.0
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        f"Background task for collection '{collection_id}' timed out waiting for AFTER_COLLECTION_CREATION signal. Proceeding anyway..."
                    )

                for initializer in self._sort_hooks(self._async_collection_initializers):
                    try:
                        await initializer(catalog_id, collection_id, context)
                    except Exception as e:
                        logger.error(
                            f"Async collection initializer {initializer.__module__}.{initializer.__name__} "
                            f"failed for '{catalog_id}/{collection_id}': {e}",
                            exc_info=True,
                        )

                # Cleanup signal
                await signal_bus.clear("AFTER_COLLECTION_CREATION", identifier=collection_id)
            finally:
                # Close the provisioning window regardless of initializer
                # outcome — a best-effort external init failure must not strand
                # the collection in PROVISIONING (writes 409 forever). #2066.
                if on_complete is not None:
                    try:
                        await on_complete()
                    except Exception as e:
                        logger.error(
                            f"Provisioning finalizer failed for "
                            f"'{catalog_id}:{collection_id}': {e}",
                            exc_info=True,
                        )

        task = run_in_background(
            _run_all(), name=f"init_collection_async_{catalog_id}_{collection_id}"
        )
        self._active_tasks.append(task)
        task.add_done_callback(self._on_task_done)
        return task

    def destroy_async_collection(
        self,
        catalog_id: str,
        collection_id: str,
        context: LifecycleContext,
    ) -> asyncio.Task | None:
        """Execute all registered async collection destroyers (external)."""
        if not self._async_collection_destroyers:
            return None

        logger.info(
            f"Scheduling async collection destruction for '{catalog_id}:{collection_id}'"
        )

        from dynastore.modules.concurrency import run_in_background

        async def _run_all():
            for destroyer in self._sort_hooks(self._async_collection_destroyers, reverse=True):
                try:
                    await destroyer(catalog_id, collection_id, context)
                except Exception as e:
                    logger.error(
                        f"Async collection destroyer {destroyer.__module__}.{destroyer.__name__} "
                        f"failed for '{catalog_id}:{collection_id}': {e}",
                        exc_info=True,
                    )

        task = run_in_background(
            _run_all(), name=f"destroy_collection_async_{catalog_id}_{collection_id}"
        )
        self._active_tasks.append(task)
        task.add_done_callback(self._on_task_done)
        return task

    # Asset lifecycle execution
    async def init_asset_sync(
        self,
        conn: DbResource,
        schema: str,
        catalog_id: str,
        collection_id: str,
        asset_code: str,
    ) -> None:
        """Execute all registered sync asset initializers (transactional)."""
        logger.info(
            f"Initializing sync asset resources for '{catalog_id}:{collection_id}:{asset_code}'"
        )

        for initializer in self._sort_hooks(self._sync_asset_initializers):
            try:
                if inspect.iscoroutinefunction(initializer):
                    await initializer(
                        conn, schema, catalog_id, collection_id, asset_code
                    )
                else:
                    initializer(conn, schema, catalog_id, collection_id, asset_code)
            except Exception as e:
                logger.error(
                    f"Sync asset initializer {initializer.__module__}.{initializer.__name__} "
                    f"failed for '{catalog_id}:{collection_id}:{asset_code}': {e}",
                    exc_info=True,
                )

    async def destroy_asset_sync(
        self,
        conn: DbResource,
        schema: str,
        catalog_id: str,
        collection_id: str,
        asset_code: str,
    ) -> None:
        """Execute all registered sync asset destroyers (transactional)."""
        logger.info(
            f"Destroying sync asset resources for '{catalog_id}:{collection_id}:{asset_code}'"
        )

        for destroyer in self._sort_hooks(self._sync_asset_destroyers, reverse=True):
            try:
                if inspect.iscoroutinefunction(destroyer):
                    await destroyer(conn, schema, catalog_id, collection_id, asset_code)
                else:
                    destroyer(conn, schema, catalog_id, collection_id, asset_code)
            except Exception as e:
                logger.error(
                    f"Sync asset destroyer {destroyer.__module__}.{destroyer.__name__} "
                    f"failed for '{catalog_id}:{collection_id}:{asset_code}': {e}",
                    exc_info=True,
                )

    def init_async_asset(
        self,
        catalog_id: str,
        collection_id: str,
        asset_code: str,
        context: LifecycleContext,
    ) -> asyncio.Task | None:
        """Execute all registered async asset initializers (external)."""
        if not self._async_asset_initializers:
            return None

        logger.info(
            f"Scheduling async asset initialization for "
            f"'{catalog_id}:{collection_id}:{asset_code}'"
        )

        from dynastore.modules.concurrency import run_in_background

        async def _run_all():
            for initializer in self._sort_hooks(self._async_asset_initializers):
                try:
                    await initializer(
                        catalog_id, collection_id, asset_code, context
                    )
                except Exception as e:
                    logger.error(
                        f"Async asset initializer {initializer.__module__}.{initializer.__name__} "
                        f"failed for '{catalog_id}:{collection_id}:{asset_code}': {e}",
                        exc_info=True,
                    )

        task = run_in_background(
            _run_all(),
            name=f"init_asset_async_{catalog_id}_{collection_id}_{asset_code}",
        )
        self._active_tasks.append(task)
        task.add_done_callback(self._active_tasks.remove)
        return task

    def destroy_async_asset(
        self,
        catalog_id: str,
        collection_id: str,
        asset_code: str,
        context: LifecycleContext,
    ) -> asyncio.Task | None:
        """Execute all registered async asset destroyers (external)."""
        if not self._async_asset_destroyers:
            return None

        logger.info(
            f"Scheduling async asset destruction for "
            f"'{catalog_id}:{collection_id}:{asset_code}'"
        )

        from dynastore.modules.concurrency import run_in_background

        async def _run_all():
            for destroyer in self._sort_hooks(self._async_asset_destroyers, reverse=True):
                try:
                    await destroyer(
                        catalog_id, collection_id, asset_code, context
                    )
                except Exception as e:
                    logger.error(
                        f"Async asset destroyer {destroyer.__module__}.{destroyer.__name__} "
                        f"failed for '{catalog_id}:{collection_id}:{asset_code}': {e}",
                        exc_info=True,
                    )

        task = run_in_background(
            _run_all(),
            name=f"destroy_asset_async_{catalog_id}_{collection_id}_{asset_code}",
        )
        self._active_tasks.append(task)
        task.add_done_callback(self._active_tasks.remove)
        return task


    async def wait_for_all_tasks(self, timeout: float = 30.0):
        """Waits for all scheduled catalog lifecycle tasks (both internal and DB-tracked).

        The DB task queue poll is skipped when no TaskQueueProtocol provider is
        registered.  Without an active dispatcher, PENDING rows in the tasks table
        will never advance to a terminal state, so polling would burn the full
        ``timeout`` for no benefit.  This also covers the common test scenario
        where ``TasksModule`` is excluded from the SCOPE — the poll is bypassed and
        teardown returns as soon as the asyncio ``_active_tasks`` settle.
        """
        import asyncio

        # --- 1. Internal Task Registry Wait ---
        if self._active_tasks:
            try:
                # Wait for internal asyncio tasks first.
                await asyncio.wait(list(self._active_tasks), timeout=min(timeout, 5.0))
            except Exception as e:
                logger.debug(f"wait_for_all_tasks: Internal tasks error: {e}")

        # --- 2. Database Task Queue Wait ---
        # Only poll the DB when a live task dispatcher is registered.  Without a
        # dispatcher, PENDING tasks will never reach a terminal state in this
        # process, so the poll loop would run until timeout with zero progress.
        from dynastore.models.protocols import DatabaseProtocol, TaskQueueProtocol
        from dynastore.modules import get_protocol
        dispatcher = get_protocol(TaskQueueProtocol)
        if dispatcher is None:
            logger.debug(
                "wait_for_all_tasks: no TaskQueueProtocol registered — "
                "skipping DB task-queue poll (no dispatcher is running)"
            )
        else:
            db_proto = get_protocol(DatabaseProtocol)
            if db_proto:
                engine = db_proto.get_any_engine()
                if engine:
                    from dynastore.modules.tasks.tasks_module import get_task_schema
                    from dynastore.modules.db_config.query_executor import (
                        managed_transaction,
                        DDLQuery,
                        DQLQuery,
                        ResultHandler,
                    )
                    from dynastore.tasks import get_loaded_task_types

                    schema = get_task_schema()
                    loaded_types = list(get_loaded_task_types())
                    logger.debug(f"wait_for_all_tasks: loaded_types={loaded_types}, default_schema={schema}")

                    if loaded_types:
                        # Single COUNT on the global tasks table — no per-schema discovery needed.
                        placeholders = ", ".join(f":t_{i}" for i in range(len(loaded_types)))
                        type_params = {f"t_{i}": t for i, t in enumerate(loaded_types)}
                        count_sql = (
                            f"SELECT COUNT(*) FROM {schema}.tasks "
                            f"WHERE status IN ('PENDING', 'ACTIVE', 'RUNNING') "
                            f"AND task_type IN ({placeholders})"
                        )

                        poll_timeout = min(timeout, 30.0)
                        start_time = asyncio.get_event_loop().time()

                        while (asyncio.get_event_loop().time() - start_time) < poll_timeout:
                            self._active_tasks = [t for t in self._active_tasks if not t.done()]

                            try:
                                async with managed_transaction(engine) as conn:
                                    await DDLQuery("SET LOCAL lock_timeout = '50ms'").execute(conn)
                                    total_pending = await DQLQuery(
                                        count_sql, result_handler=ResultHandler.SCALAR_ONE
                                    ).execute(conn, **type_params) or 0
                            except Exception as e:
                                logger.debug(f"wait_for_all_tasks: poll error: {e}")
                                if not self._active_tasks:
                                    break
                                await asyncio.sleep(0.1)
                                continue

                            if total_pending == 0 and not self._active_tasks:
                                break

                            from dynastore.modules.tasks.queue import TASK_STATUS_CHANGED
                            from dynastore.tools.async_utils import signal_bus
                            cycle_timeout = 0.1 if not self._active_tasks else 0.5
                            try:
                                await signal_bus.wait_for(TASK_STATUS_CHANGED, timeout=cycle_timeout)
                            except asyncio.TimeoutError:
                                pass

        # Final settlement check for internal tasks
        settlement_timeout = min(timeout, 5.0)
        start_time = asyncio.get_event_loop().time()
        while self._active_tasks and (asyncio.get_event_loop().time() - start_time) < settlement_timeout:
            await asyncio.sleep(0.1)
            self._active_tasks = [t for t in self._active_tasks if not t.done()]

    def clear_registry(self) -> None:
        """Alias for `clear()`. Kept for backward compatibility."""
        self.clear()


# Global instance
lifecycle_registry = LifecycleRegistry()

# Convenience decorator exports (using registry methods as decorators)
sync_catalog_initializer = lifecycle_registry.sync_catalog_initializer
sync_catalog_post_create = lifecycle_registry.sync_catalog_post_create
sync_catalog_destroyer = lifecycle_registry.sync_catalog_destroyer
sync_collection_initializer = lifecycle_registry.sync_collection_initializer
sync_collection_destroyer = lifecycle_registry.sync_collection_destroyer
sync_collection_hard_destroyer = lifecycle_registry.sync_collection_hard_destroyer

async_catalog_initializer = lifecycle_registry.async_catalog_initializer
async_catalog_destroyer = lifecycle_registry.async_catalog_destroyer
async_collection_initializer = lifecycle_registry.async_collection_initializer
async_collection_destroyer = lifecycle_registry.async_collection_destroyer

sync_asset_initializer = lifecycle_registry.sync_asset_initializer
sync_asset_destroyer = lifecycle_registry.sync_asset_destroyer
async_asset_initializer = lifecycle_registry.async_asset_initializer
async_asset_destroyer = lifecycle_registry.async_asset_destroyer
