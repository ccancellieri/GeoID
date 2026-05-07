import logging
import asyncio
from typing import Optional, Any, Iterator, Callable, List, Awaitable, Dict, Tuple
from dynastore.modules.db_config.query_executor import run_in_event_loop as _real_run

logger = logging.getLogger(__name__)

def run_in_event_loop(awaitable: Awaitable[Any]) -> Any:
    """
    Deprecated bridge: Runs an awaitable from a synchronous context.
    Use `dynastore.modules.db_config.query_executor.run_in_event_loop` directly.
    """
    return _real_run(awaitable)

class SyncQueueIterator:
    """
    Bridge between async queue and sync iterator.
    
    Allows sync code (running in a thread) to consume items from an async queue.
    Used by tasks to bridge async data producers (like DB streaming) and sync consumers (like file writers).
    """
    
    def __init__(self, queue: 'asyncio.Queue', loop: 'asyncio.AbstractEventLoop'):
        self.queue = queue
        self.loop = loop
    
    def __iter__(self) -> Iterator[Any]:
        return self
    
    def __next__(self) -> Any:
        try:
            # We use run_coroutine_threadsafe to get the next item from the queue
            # since this is called from a synchronous context (a thread).
            future = asyncio.run_coroutine_threadsafe(self.queue.get(), self.loop)
            item = future.result()
            if item is None:
                raise StopIteration
            return item
        except StopIteration:
            raise
        except Exception as e:
            logger.error(f"Error retrieving item from queue: {e}")
            raise StopIteration

class AsyncBufferAggregator:
    """
    Generic aggregator that buffers items and flushes them in batches.
    Useful for high-throughput accounting/stats where individual DB writes are too expensive.
    
    Supports:
    - Time-based flushing (e.g. every 5 seconds)
    - Threshold-based flushing (e.g. every 100 items)
    - Key-based aggregation (e.g. summing increments for the same API key)
    """
    def __init__(
        self, 
        flush_callback: Callable[[List[Any]], Awaitable[None]],
        threshold: int = 100,
        interval: float = 5.0,
        name: str = "aggregator"
    ):
        self._callback = flush_callback
        self._threshold = threshold
        self._interval = interval
        self._name = name
        self._buffer: List[Any] = []
        self._lock = asyncio.Lock()
        self._flush_task: Optional[asyncio.Task] = None
        self._flush_event = asyncio.Event()
        self._last_flush = 0.0  # Lazy initialization on first flush
        self._flush_exec_lock: Optional[asyncio.Lock] = None

    async def add(self, item: Any):
        """Adds an item to the buffer and triggers flush if threshold reached."""
        async with self._lock:
            self._buffer.append(item)
            if len(self._buffer) >= self._threshold:
                self._flush_event.set()

    async def _trigger_flush(self, wait: bool = False) -> Optional[asyncio.Task]:
        """Internal: Drains buffer and executes callback."""
        if not self._buffer:
            return None

        to_flush = self._buffer[:]
        self._buffer.clear()
        self._last_flush = asyncio.get_running_loop().time()
        
        # We wrap the callback in another lock check if we want to serialize flushes.
        # But wait, run_in_background is decoupled.
        # Let's ensure ONE flush runs at a time for THIS aggregator.
        
        from dynastore.modules.concurrency import run_in_background
        
        async def _flush_locked():
            # This second internal lock ensures that callback executions for this
            # specific aggregator are serialized.
            if self._flush_exec_lock is None:
                self._flush_exec_lock = asyncio.Lock()

            async with self._flush_exec_lock:
                await self._callback(to_flush)
                
        task = run_in_background(_flush_locked(), name=f"flush_{self._name}")
        if wait:
             await task
        return task

    async def start(self):
        """Starts the periodic flush loop."""
        if self._flush_task:
            return
        self._flush_task = asyncio.create_task(self._loop())

    async def stop(self):
        """Stops the loop and performs a final flush."""
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
            self._flush_task = None
        await self._trigger_flush(wait=True)

    async def _loop(self):
        """Background loop for time-based and event-based flushing."""
        while True:
            try:
                # Wait for either the event (threshold reached) or the timeout (interval reached)
                try:
                    await asyncio.wait_for(self._flush_event.wait(), timeout=self._interval)
                except asyncio.TimeoutError:
                    pass # Interval reached

                async with self._lock:
                    self._flush_event.clear()
                    await self._trigger_flush(wait=True)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in {self._name} loop: {e}")

class KeyValueAggregator(AsyncBufferAggregator):
    """
    Specialized aggregator that sums values for the same key.
    Useful for usage counters.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._kv_buffer: Dict[Any, int] = {}

    async def add_increment(self, key: Any, amount: int = 1):
        async with self._lock:
            self._kv_buffer[key] = self._kv_buffer.get(key, 0) + amount
            if len(self._kv_buffer) >= self._threshold:
                 self._flush_event.set()

    async def _trigger_flush(self, wait: bool = False) -> Optional[asyncio.Task]:
        if not self._kv_buffer:
            return None

        to_flush = list(self._kv_buffer.items())
        self._kv_buffer.clear()
        self._last_flush = asyncio.get_event_loop().time()

        from dynastore.modules.concurrency import run_in_background

        async def _flush_locked():
            if self._flush_exec_lock is None:
                self._flush_exec_lock = asyncio.Lock()
            async with self._flush_exec_lock:
                await self._callback(to_flush)
                
        task = run_in_background(_flush_locked(), name=f"flush_{self._name}")
        if wait:
             await task
        return task


class WaitableSignal:
    """
    A simple wrapper around asyncio.Event that allows multiple waiters
    to wait for a specific signal identified by a name and an optional identifier.
    
    Robustness: recreate the internal Event if the event loop changes.
    """

    def __init__(self):
        self._event = None
        self._loop = None

    def _ensure_event(self) -> asyncio.Event:
        """Lazily creates or recreates the event for the current loop."""
        loop = asyncio.get_running_loop()
        if self._event is None or self._loop != loop:
            self._event = asyncio.Event()
            self._loop = loop
        return self._event

    async def wait(self, timeout: Optional[float] = None) -> bool:
        """
        Waits for the signal to be emitted.
        Returns True if the signal was received, False if it timed out.
        Clears the event upon successful wakeup to allow subsequent waits.
        """
        event = self._ensure_event()
        try:
            if timeout is not None:
                await asyncio.wait_for(event.wait(), timeout=timeout)
            else:
                await event.wait()
            # Clear the event so future wait() calls will actually block
            event.clear()
            return True
        except asyncio.TimeoutError:
            event.clear() # Ensure clean state on timeout
            return False

    def emit(self):
        """Emits the signal, waking up all waiters."""
        # Note: emit might be called from a context where we don't want to 
        # initialize a loop-bound event if one doesn't exist.
        # But usually emit and wait happen in the same process/loop context.
        if self._event:
            self._event.set()


class SignalBus:
    """
    Registry for WaitableSignals, allowing tasks to synchronize across different
    parts of the application without direct references.
    
    Robustness: handles multiple event loops (useful for tests).
    """

    def __init__(self):
        # We store signals per loop to avoid interaction between tests
        self._signals_per_loop: Dict[asyncio.AbstractEventLoop, Dict[Tuple[str, Optional[str]], WaitableSignal]] = {}
        self._locks_per_loop: Dict[asyncio.AbstractEventLoop, asyncio.Lock] = {}

    def _get_context(self) -> Tuple[asyncio.Lock, Dict[Tuple[str, Optional[str]], WaitableSignal]]:
        loop = asyncio.get_running_loop()
        if loop not in self._locks_per_loop:
            self._locks_per_loop[loop] = asyncio.Lock()
            self._signals_per_loop[loop] = {}
        return self._locks_per_loop[loop], self._signals_per_loop[loop]

    async def get_signal(
        self, name: str, identifier: Optional[str] = None
    ) -> WaitableSignal:
        """Retrieves or creates a signal for the given name and identifier."""
        lock, signals = self._get_context()
        key = (name, identifier)
        async with lock:
            if key not in signals:
                signals[key] = WaitableSignal()
            return signals[key]

    async def emit(self, name: str, identifier: Optional[str] = None):
        """Emits a signal, creating it if it doesn't exist."""
        signal = await self.get_signal(name, identifier)
        signal.emit()

    async def wait_for(
        self,
        name: str,
        identifier: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> bool:
        """Waits for a signal to be emitted. Returns True if received, False if timed out."""
        signal = await self.get_signal(name, identifier)
        return await signal.wait(timeout=timeout)

    async def clear(self, name: Optional[str] = None, identifier: Optional[str] = None):
        """Removes a signal from the registry. If name is None, clears ALL signals for the current loop."""
        lock, signals = self._get_context()
        async with lock:
            if name is None:
                signals.clear()
                logger.debug("Cleared all signals from SignalBus for current loop.")
            else:
                key = (name, identifier)
                signals.pop(key, None)
                logger.debug(f"Cleared signal '{name}' (id={identifier}) from SignalBus.")


# Global SignalBus instance
signal_bus = SignalBus()


class PgListenBridge:
    """Bridges PostgreSQL LISTEN/NOTIFY channels to SignalBus with auto-reconnect.

    Opens ONE lightweight asyncpg LISTEN connection per instance (no query
    execution). On each notification, optionally transforms it via a callback,
    then emits to SignalBus.  Auto-reconnects on connection failure.

    A periodic health timeout emits signals even when no notifications arrive,
    ensuring janitor / claim sweeps still fire.

    Usage::

        bridge = PgListenBridge(
            channels=["new_task_queued", "dynastore_events_channel"],
            signal_bus=signal_bus,
            health_timeout=30.0,
            transform=lambda ch, payload: (ch, None) if ch == "new_task_queued" else (ch, payload),
        )
        task = asyncio.create_task(bridge.run(engine))
        ...
        await bridge.stop()
        task.cancel()
    """

    def __init__(
        self,
        channels: List[str],
        signal_bus: SignalBus,
        health_timeout: float = 120.0,
        transform: Optional[Callable[[str, Optional[str]], Optional[Tuple[str, Optional[str]]]]] = None,
    ):
        self._channels = channels
        self._signal_bus = signal_bus
        self._health_timeout = health_timeout
        self._transform = transform
        self._running = False

    async def run(self, engine) -> None:
        """Long-running LISTEN loop.  Call via ``asyncio.create_task()``."""
        self._running = True

        while self._running:
            try:
                async with engine.connect() as conn:
                    raw = await conn.get_raw_connection()
                    driver_conn = getattr(raw, "driver_connection", None)

                    if not (driver_conn and hasattr(driver_conn, "add_listener")):
                        logger.warning(
                            "PgListenBridge: asyncpg not available "
                            f"(driver={type(driver_conn).__name__}) — periodic fallback."
                        )
                        await self._periodic_fallback()
                        return

                    queue: asyncio.Queue = asyncio.Queue()

                    def _on_notify(connection, pid, channel, payload):
                        queue.put_nowait((channel, payload))

                    for ch in self._channels:
                        await driver_conn.add_listener(ch, _on_notify)

                    logger.info(f"PgListenBridge: LISTEN active on {self._channels}.")

                    try:
                        while self._running:
                            try:
                                channel, payload = await asyncio.wait_for(
                                    queue.get(), timeout=self._health_timeout
                                )
                            except asyncio.TimeoutError:
                                # Liveness: emit periodic signal for janitor / claim sweep
                                for ch in self._channels:
                                    await self._signal_bus.emit(ch)
                                continue

                            # Optional transform / filter
                            if self._transform:
                                result = self._transform(channel, payload)
                                if result is None:
                                    continue
                                channel, identifier = result
                            else:
                                identifier = payload

                            await self._signal_bus.emit(channel, identifier=identifier)
                    finally:
                        for ch in self._channels:
                            try:
                                await driver_conn.remove_listener(ch, _on_notify)
                            except Exception:
                                pass

            except asyncio.CancelledError:
                break
            except Exception as e:
                if not self._running:
                    break
                logger.warning(
                    f"PgListenBridge: connection error — reconnecting in 5s: {e}"
                )
                await asyncio.sleep(5.0)

        logger.info("PgListenBridge: Stopped.")

    async def _periodic_fallback(self) -> None:
        """Fallback for non-asyncpg engines: periodic signal emission."""
        while self._running:
            await asyncio.sleep(self._health_timeout)
            for ch in self._channels:
                await self._signal_bus.emit(ch)

    async def stop(self) -> None:
        """Signal the bridge to stop."""
        self._running = False
