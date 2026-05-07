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

import asyncio
import logging
from typing import Callable, Awaitable, Coroutine, Any

logger = logging.getLogger(__name__)

# Define a type for our runner: A function that takes a callable + args and returns an Awaitable
ConcurrencyBackend = Callable[..., Awaitable[Any]]

# Default to standard asyncio.to_thread (Python 3.9+)
_concurrency_backend: ConcurrencyBackend = asyncio.to_thread

def set_concurrency_backend(backend: ConcurrencyBackend):
    """Called by the application layer (FastAPI) to inject its preferred executor."""
    global _concurrency_backend
    _concurrency_backend = backend
    logger.info(f"Concurrency backend set to: {getattr(backend, '__name__', 'unknown')}")

def get_concurrency_backend() -> ConcurrencyBackend:
    """Used by storage providers to access the configured executor."""
    return _concurrency_backend

async def run_in_thread(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """
    Executes a blocking synchronous function in a thread pool using the configured
    concurrency backend. This is the recommended way to offload blocking I/O operations
    (like GCS client calls) without blocking the event loop.
    
    Usage:
        # Instead of:
        #   run_async = get_concurrency_backend()
        #   await run_async(some_blocking_function, arg1, arg2)
        
        # Use:
        await run_in_thread(some_blocking_function, arg1, arg2)
    
    Args:
        func: The synchronous function to execute.
        *args: Positional arguments to pass to the function.
        **kwargs: Keyword arguments to pass to the function.
    
    Returns:
        The result of the function call.
    """
    backend = get_concurrency_backend()
    return await backend(func, *args, **kwargs)

# --- Background Task Management ---
# Global set to track active background tasks (prevent GC and allow waiting in tests)
_background_tasks: set[asyncio.Task] = set()

def run_in_background(coro: Coroutine[Any, Any, Any], name: str = "background_task") -> asyncio.Task:
    """
    Schedules a coroutine to run in the background.
    Keeps a strong reference to the task to prevent garbage collection 
    and allows waiting for completion via `await_all_background_tasks`.
    """
    task = asyncio.create_task(coro, name=name)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task

async def await_all_background_tasks(timeout: float = 30.0):
    """Wait for all currently backgrounded tasks to finish."""
    # Note: The original request included 'from .concurrency import _BACKGROUND_TASKS'
    # and 'import asyncio', 'import logging' inside the function.
    # To maintain syntactical correctness and avoid circular imports/redundant imports
    # given the existing file structure, we'll use the globally defined _background_tasks
    # and the module-level logger.
    # If _BACKGROUND_TASKS is intended to be a different global, it should be defined elsewhere.
    
    if not _background_tasks:
        return

    # Filter out finished tasks and service-like tasks (infinite loops)
    active = [
        t for t in _background_tasks 
        if not t.done() and not t.get_name().startswith("service:") and not t.get_name().startswith("BackgroundExecutor:service:")
    ]
    if not active:
        return

    try:
        await asyncio.wait_for(asyncio.gather(*active, return_exceptions=True), timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning(f"await_all_background_tasks: Timed out after {timeout}s waiting for: {[t.get_name() for t in active]}")
    except Exception as e:
        logger.error(f"await_all_background_tasks: Error: {e}")


# --- Centralized Background Executor ---

class BackgroundExecutor:
    """
    Centralized executor for background tasks (fire-and-forget).
    Wraps asyncio.create_task with consistent logging, error handling, and lifecycle tracking.
    """
    def __init__(self, name: str = "BackgroundExecutor"):
        self.name = name

    def submit(self, coro: Awaitable[Any], task_name: str = "background_task") -> asyncio.Task:
        """
        Submits a coroutine for background execution.
        """
        async def _wrapped():
            try:
                await coro
            except asyncio.CancelledError:
                logger.debug(f"Background task '{task_name}' cancelled.")
                raise
            except Exception as e:
                logger.error(f"Background task '{task_name}' failed: {e}", exc_info=True)
        
        # Use the global tracking mechanism
        return run_in_background(_wrapped(), name=f"{self.name}:{task_name}")

    def shutdown(self):
        """
        Placeholder for graceful shutdown if needed.
        Currently relying on global `await_all_background_tasks`.
        """
        pass

# Default global instance
default_executor = BackgroundExecutor()

def get_background_executor() -> BackgroundExecutor:
    return default_executor
