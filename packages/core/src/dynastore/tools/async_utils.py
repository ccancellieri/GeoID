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
import asyncio
import random
from contextlib import AbstractAsyncContextManager
from typing import Optional, Any, Iterator, Callable, List, Awaitable, Dict, Tuple

logger = logging.getLogger(__name__)

# pg_notify channel carrying platform-scope config-write commits (Layer A
# hot-reload watcher). Emitted co-transactionally by PlatformConfigService's
# platform-tier write paths (set_config / set_config_by_ref) and consumed by
# ConfigReloadService via SignalBus, riding the same LISTEN connection the
# task-queue bridge already holds — see modules/tasks/queue.py and
# modules/db_config/config_reload_service.py.
PLATFORM_CONFIG_CHANGED = "platform_config_changed"


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
            raise StopIteration from e

class LoopLocalLock:
    """An ``asyncio.Lock`` proxy that is safe to instantiate at module-level or
    class-body scope.

    A raw ``asyncio.Lock()`` created at import time binds to whichever event
    loop first awaits it and then raises ``RuntimeError`` if reused from a
    different loop. That breaks under multiple loops (tests spin up a fresh
    loop per case; multi-worker processes likewise) and silently shares one
    lock object across every instance/loop — false cross-instance safety in a
    distributed-stateless runtime where only PG/ES/Valkey are shared.

    This proxy keeps one real lock *per running loop*, created lazily on first
    use, so a single module- or class-level instance is correct across loops
    while still serialising work within each loop. Usage is drop-in::

        _GUARD = LoopLocalLock()        # safe at module/class-body scope
        async def f():
            async with _GUARD:
                ...
    """

    __slots__ = ("_locks",)

    def __init__(self) -> None:
        self._locks: Dict[asyncio.AbstractEventLoop, asyncio.Lock] = {}

    def _for_running_loop(self) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        lock = self._locks.get(loop)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[loop] = lock
        return lock

    async def __aenter__(self) -> "LoopLocalLock":
        await self._for_running_loop().acquire()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        # Runs in the same task and loop as ``__aenter__``, so this resolves to
        # the very lock that was acquired.
        self._for_running_loop().release()

    def locked(self) -> bool:
        """True if the current running loop's lock is held. False if no loop
        is running or the lock for this loop has not been created yet."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return False
        lock = self._locks.get(loop)
        return lock.locked() if lock is not None else False


class LoopLocalSemaphore:
    """An ``asyncio.Semaphore`` proxy that is safe to instantiate at module-level,
    class-body, or ``__init__`` scope of an import-time singleton.

    Same hazard and fix as :class:`LoopLocalLock`: a raw ``asyncio.Semaphore()``
    created before any loop is running binds to whichever loop first awaits it
    (on the first blocking ``acquire``) and then breaks if the same object is
    reused from another loop (function-scoped test loops; a re-created runtime
    loop). This proxy keeps one real semaphore *per running loop*, created
    lazily on first use, each with the same ``value`` capacity. Drop-in for
    ``async with``::

        self._semaphore = LoopLocalSemaphore(max_concurrency)  # safe in __init__
        ...
        async with self._semaphore:
            ...
    """

    __slots__ = ("_value", "_semaphores")

    def __init__(self, value: int) -> None:
        self._value = value
        self._semaphores: Dict[asyncio.AbstractEventLoop, asyncio.Semaphore] = {}

    def _for_running_loop(self) -> asyncio.Semaphore:
        loop = asyncio.get_running_loop()
        sem = self._semaphores.get(loop)
        if sem is None:
            sem = asyncio.Semaphore(self._value)
            self._semaphores[loop] = sem
        return sem

    async def __aenter__(self) -> "LoopLocalSemaphore":
        await self._for_running_loop().acquire()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        # Same task/loop as ``__aenter__`` (``async with``), so this resolves to
        # the very semaphore that was acquired.
        self._for_running_loop().release()


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
        name: str = "aggregator",
        max_size: Optional[int] = None,
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
        # Hard cap on the buffer (None = unbounded, the historical default —
        # existing callers of this generic utility are unaffected). When set,
        # add() drops the OLDEST buffered item once at capacity rather than
        # growing memory without bound while the backend is slow; drops are
        # rate-limited to one summary warning per _DROP_WARNING_COOLDOWN.
        self._max_size = max_size
        self._dropped_since_warning = 0
        self._last_drop_warning = 0.0

    _DROP_WARNING_COOLDOWN = 30.0  # seconds

    async def add(self, item: Any):
        """Adds an item to the buffer and triggers flush if threshold reached.

        When ``max_size`` is set and the buffer is already at capacity, the
        oldest buffered item is dropped to make room — bounded memory over
        completeness, since a backlog this deep already implies old items
        are stale by the time they would flush.
        """
        async with self._lock:
            if self._max_size is not None and len(self._buffer) >= self._max_size:
                self._buffer.pop(0)
                self._dropped_since_warning += 1
                now = asyncio.get_event_loop().time()
                if now - self._last_drop_warning >= self._DROP_WARNING_COOLDOWN:
                    logger.warning(
                        "%s: buffer at cap (%d) — dropped %d oldest entr%s "
                        "in the last %.0fs (backend too slow to keep up).",
                        self._name,
                        self._max_size,
                        self._dropped_since_warning,
                        "y" if self._dropped_since_warning == 1 else "ies",
                        self._DROP_WARNING_COOLDOWN,
                    )
                    self._last_drop_warning = now
                    self._dropped_since_warning = 0
            self._buffer.append(item)
            if len(self._buffer) >= self._threshold:
                self._flush_event.set()

    async def _trigger_flush(self, wait: bool = False) -> Optional[asyncio.Task]:
        """Internal: swaps the buffer out under the lock, then dispatches the
        callback OUTSIDE the lock.

        The swap (copy + clear) is the only step that needs ``self._lock`` —
        it must be atomic with respect to concurrent ``add()`` calls. The
        callback itself (a network call to the log backend) must NOT run
        while the lock is held: every producer's ``add()`` blocks on that
        same lock, so a slow (not dead) backend would otherwise stall every
        in-flight ``log_event()`` call for the callback's full retry budget —
        reintroducing, at the aggregator layer, the exact "logging blocks the
        request path" failure class #2749 exists to eliminate.
        """
        async with self._lock:
            if not self._buffer:
                return None
            to_flush = self._buffer[:]
            self._buffer.clear()
            self._last_flush = asyncio.get_running_loop().time()

        from dynastore.modules.concurrency import run_in_background

        async def _flush_locked():
            # This second internal lock ensures that callback executions for this
            # specific aggregator are serialized. It is independent of
            # self._lock (which only ever guards the buffer itself).
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

                # _trigger_flush takes self._lock itself, only around the
                # buffer swap — do NOT wrap it in self._lock here, or the
                # callback dispatch below runs while every add() is blocked.
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
        """Swaps ``_kv_buffer`` out under the lock, dispatches outside it —
        see ``AsyncBufferAggregator._trigger_flush`` for why."""
        async with self._lock:
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


# --- Leader-elected periodic loops ---

async def run_leader_loop(
    *,
    acquire_leadership: Callable[[], AbstractAsyncContextManager[Tuple[bool, Any]]],
    on_leader: Callable[[Any], Awaitable[None]],
    name: str,
    cadence_seconds: float = 5.0,
    is_shutdown: Optional[Callable[[], bool]] = None,
    shutdown_event: Optional[asyncio.Event] = None,
    tick_timeout: Optional[float] = None,
    pre_tick_probe: Optional[Callable[[Any], Awaitable[None]]] = None,
) -> None:
    """Run a leader-elected loop that resigns on any exception.

    Runner-agnostic: ``run_leader_loop`` is a plain async coroutine. The caller
    chooses the runner — ``asyncio.create_task``, ``BackgroundExecutor.submit``,
    a FastAPI background task, etc. The helper itself imports nothing
    runner-specific.

    Each outer iteration:
      1. Calls ``acquire_leadership()`` and enters its context manager
         (typically a non-blocking lease-table CAS acquire — see
         ``dynastore.modules.db_config.locking_tools.lease_leadership``).
      2. The context yields ``(is_leader, lock_connection)``. ``lock_connection``
         is backend-defined — ``lease_leadership`` always yields ``None`` since
         it does not pin a connection between yield and release, but the
         signature stays generic so a future backend could hand one back for
         the leader to reuse. If ``is_leader`` is ``True``,
         ``on_leader(lock_connection)`` is called.
      3. Exits the context (releasing leadership) and sleeps ``cadence_seconds``.
      4. On any exception inside the leadership context, the context is exited
         (releasing leadership) before sleeping — preventing leader-held
         resources from staying associated with a poisoned pool slot across
         retries.

    ``on_leader`` receives the lock connection and may run its own inner
    periodic loop, but MUST let exceptions propagate. Swallowing exceptions
    inside ``on_leader`` keeps leadership held and is the anti-pattern this
    helper exists to prevent.

    INVARIANT — no inner per-tick retry loop. ``on_leader`` must fail fast on a
    transient error and let it propagate so this loop resigns (exits the
    leadership context, releasing the lease) and leadership hands off to
    another pod. Do NOT wrap the tick body in a retry/backoff loop: an inner
    retry withholds leadership from other pods through the entire backoff,
    worsening pool pressure exactly when the DB is already struggling.
    Self-healing belongs to the OUTER loop here (resign → sleep one cadence →
    re-elect), never the inner tick.

    ``acquire_leadership`` MUST yield exactly once on every code path. Do not
    hand-roll it — use
    ``dynastore.modules.db_config.locking_tools.lease_leadership``, which is
    safe under transaction-mode connection pooling (no connection pinned
    across the tenure, no lease leaking into the pool) and never yields from
    an ``except`` handler (the historical double-yield bug surfaced as
    ``RuntimeError: generator didn't stop`` and resigned every cycle).

    ``tick_timeout`` bounds the maximum time leadership is held per tick. If
    the tick exceeds this timeout, it is cancelled and leadership is
    released, preventing a slow tick from blocking leadership election under
    pool contention or external API latency. Defaults to ``cadence_seconds``
    to ensure the tick completes within one cadence window.
    """
    is_shutdown = is_shutdown or (lambda: False)
    # When cadence_seconds is zero (test/fast-loop mode) the effective tick
    # timeout defaults to None (unbounded) rather than zero (instant cancel).
    # A zero-second wait_for would cancel the body before it runs.
    effective_tick_timeout: Optional[float] = (
        tick_timeout if tick_timeout is not None
        else (cadence_seconds if cadence_seconds > 0 else None)
    )

    async def _sleep_cadence() -> None:
        # Jitter the wait by +/-15% so a fleet of replicas on the same
        # cadence doesn't herd on a shared hot row every period. The
        # zero-cadence test/fast-loop case is left unjittered.
        jittered = (
            cadence_seconds * random.uniform(0.85, 1.15)
            if cadence_seconds > 0
            else cadence_seconds
        )
        if shutdown_event is not None:
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=jittered)
            except asyncio.TimeoutError:
                pass
        else:
            await asyncio.sleep(jittered)

    while not is_shutdown():
        try:
            async with acquire_leadership() as (is_leader, lock_conn):
                if not is_leader:
                    await _sleep_cadence()
                    continue
                logger.info("%s: leadership acquired", name)
                if pre_tick_probe is not None:
                    await pre_tick_probe(lock_conn)
                try:
                    await asyncio.wait_for(on_leader(lock_conn), timeout=effective_tick_timeout)
                except asyncio.TimeoutError:
                    logger.warning(
                        "%s: tick timed out after %.1fs (leadership released); "
                        "consider reducing tick workload or increasing tick_timeout.",
                        name, effective_tick_timeout,
                    )
            await _sleep_cadence()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "%s: leader loop error; resigning and retrying in %ss",
                name, cadence_seconds,
            )
            await _sleep_cadence()
