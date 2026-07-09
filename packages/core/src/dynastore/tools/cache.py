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
Centralized caching framework -- decorator, backends, serializers, manager.

Two-layer architecture:
- ``CacheBackend`` / ``SyncCacheBackend``: low-level (bytes), for implementors
- ``Cache``: high-level (typed), for application code
- ``@cached``: decorator built on top, not part of the protocol

Usage::

    from dynastore.tools.cache import cached, CacheIgnore

    @cached(maxsize=1024, ttl=300, namespace="catalog_config", ignore=["conn"])
    async def get_config(catalog_id: str, conn: DbResource) -> dict:
        ...

    # or using type annotation:
    @cached(maxsize=1024, ttl=300, namespace="catalog_config")
    async def get_config(catalog_id: str, conn: CacheIgnore[DbResource] = None) -> dict:
        ...
"""

from __future__ import annotations

import asyncio
import collections
import contextvars
import functools
import hashlib
import inspect
import itertools
import json
import logging
import random
import sys
import threading
import time
import weakref
from datetime import timedelta
from typing import (
    Annotated,
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Dict,
    List,
    Optional,
    Set,
    TypeVar,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)
from typing import Protocol, runtime_checkable

from dynastore.models.protocols.cache import (
    Cache,
    CacheBackend,
    CacheConfig,
    CacheEvent,
    CacheEventData,
    CacheEventListener,
    CacheItemPriority,
    CacheSerializer,
    CacheStats,
    SyncCacheBackend,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Strong refs for fire-and-forget cache-invalidation tasks scheduled from
# sync contexts. ``loop.create_task`` returns a weak-ref'd Task; without a
# strong reference somewhere, a GC pass can collect the task before the
# invalidation runs — stale data continues to be served from the
# distributed backend until the TTL expires.
_pending_invalidations: Set[asyncio.Task] = set()


def _track(task: asyncio.Task) -> asyncio.Task:
    """Hold a strong reference to ``task`` until completion."""
    _pending_invalidations.add(task)
    task.add_done_callback(_pending_invalidations.discard)
    return task


# ---------------------------------------------------------------------------
#  CacheIgnore type annotation
# ---------------------------------------------------------------------------


class _CacheIgnoreMarker:
    """Sentinel marker for CacheIgnore[T] annotation."""


CacheIgnore = Annotated[T, _CacheIgnoreMarker()]
"""Type annotation to exclude a parameter from cache key generation.

Usage::

    async def get_config(
        catalog_id: str,
        conn: CacheIgnore[DbResource] = None,
    ) -> dict:
        ...
"""


def _has_cache_ignore(annotation: Any) -> bool:
    """Check if an annotation is CacheIgnore[T]."""
    if get_origin(annotation) is Annotated:
        for arg in get_args(annotation):
            if isinstance(arg, _CacheIgnoreMarker):
                return True
    return False


# ---------------------------------------------------------------------------
#  LockableCacheBackend — optional protocol for backends with stampede protection
# ---------------------------------------------------------------------------


@runtime_checkable
class LockableCacheBackend(Protocol):
    """Optional protocol for cache backends that provide per-key async locks.

    When a backend registered via ``CacheManager`` implements this protocol,
    the ``@cached`` decorator uses its ``get_lock()`` for stampede protection.
    Backends that do not implement it fall back to decorator-local asyncio locks.

    ``LocalAsyncCacheBackend`` implements this protocol.  Redis or other
    external backends should implement it if they want native lock semantics
    (e.g. Redis SETNX-based distributed locks).
    """

    async def get_lock(self, key: str) -> asyncio.Lock:
        """Return an asyncio.Lock for the given cache key."""
        ...


# ---------------------------------------------------------------------------
#  Serializers
# ---------------------------------------------------------------------------


class NullSerializer:
    """Passthrough for in-memory backends -- stores objects directly."""

    def dumps(self, value: Any) -> bytes:
        return value

    def loads(self, data: bytes) -> Any:
        return data


class JsonSerializer:
    """JSON serialization -- safe, inspectable, default for distributed."""

    def dumps(self, value: Any) -> bytes:
        return json.dumps(value, default=str, separators=(",", ":")).encode("utf-8")

    def loads(self, data: bytes) -> Any:
        return json.loads(data)


class PydanticSerializer:
    """Auto-detects Pydantic models and uses model_dump_json/model_validate_json."""

    def dumps(self, value: Any) -> bytes:
        if hasattr(value, "model_dump_json"):
            return value.model_dump_json().encode("utf-8")
        return json.dumps(value, default=str, separators=(",", ":")).encode("utf-8")

    def loads(self, data: bytes) -> Any:
        return json.loads(data)


class MsgPackSerializer:
    """Compact binary serialization using msgpack (optional dependency)."""

    def __init__(self) -> None:
        try:
            import msgpack  # noqa: F401
            self._msgpack = msgpack
        except ImportError as e:
            raise ImportError(
                "msgpack is required for MsgPackSerializer. "
                "Install it with: pip install msgpack"
            ) from e

    def dumps(self, value: Any) -> bytes:
        result = self._msgpack.packb(value, use_bin_type=True)
        if result is None:
            raise RuntimeError("msgpack.packb returned None")
        return result

    def loads(self, data: bytes) -> Any:
        return self._msgpack.unpackb(data, raw=False)


# ---------------------------------------------------------------------------
#  Cache key builder
# ---------------------------------------------------------------------------


def _make_cache_key(
    func_qualname: str,
    args: tuple,
    kwargs: dict,
    sig: inspect.Signature,
    ignored_params: Set[str],
    typed: bool,
) -> str:
    """Build a deterministic cache key from function call arguments."""
    bound = sig.bind(*args, **kwargs)
    bound.apply_defaults()

    key_parts: list = [func_qualname]

    for param_name, param_value in bound.arguments.items():
        if param_name in ignored_params:
            continue
        if param_name == "self":
            continue
        try:
            key_parts.append(repr(param_value))
            if typed:
                key_parts.append(type(param_value).__name__)
        except Exception:
            key_parts.append(str(id(param_value)))

    raw = "|".join(key_parts)
    if len(raw) > 200:
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return raw


# ---------------------------------------------------------------------------
#  _CacheEntry - internal storage record
# ---------------------------------------------------------------------------


class _CacheEntry:
    """Internal record stored in the local cache backends."""
    __slots__ = ("value", "expires_at", "priority", "size_bytes", "hits")

    def __init__(
        self,
        value: Any,
        expires_at: Optional[float],
        priority: int = CacheItemPriority.NORMAL,
        size_bytes: int = 0,
    ):
        self.value = value
        self.expires_at = expires_at
        self.priority = priority
        self.size_bytes = size_bytes
        self.hits = 0

    def is_expired(self) -> bool:
        return self.expires_at is not None and time.monotonic() > self.expires_at


# ---------------------------------------------------------------------------
#  L1 memory budget — process-wide byte accounting for local backends
# ---------------------------------------------------------------------------

# Built-in defaults for the CachePluginConfig L1 knobs, active until the
# config is loaded (pull path: _load_cache_plugin_config_values; push path:
# set_l1_runtime_config called by CacheModule's apply handler).
_DEFAULT_L1_TTL_SECONDS: float = 30.0
_DEFAULT_L1_MEMORY_PERCENT: float = 10.0
_l1_default_ttl_value: float = _DEFAULT_L1_TTL_SECONDS
_l1_memory_percent_value: float = _DEFAULT_L1_MEMORY_PERCENT

_L1_SIZING_MAX_DEPTH = 6
_L1_SIZING_MAX_ITEMS = 64
# Entries whose deep size cannot be measured at all are charged this nominal
# figure so they still participate in budget accounting.
_L1_OPAQUE_SIZE_BYTES = 64
# How many LRU-oldest candidates the byte-budget eviction scores per pass
# (see _L1MemoryBudget.enforce).
_L1_EVICTION_SAMPLE = 8
# A single value larger than this fraction of the byte budget is not admitted
# to L1 at all: evicting most of the working set to hold one entry is never a
# win, and the budget scan considers the newest (MRU) entry last, so an
# oversized insert would otherwise strip every older entry before touching
# the offender. Tiered callers still write the value to the distributed tier.
_L1_ADMISSION_MAX_FRACTION = 0.25


def _approx_deep_size(obj: Any, _depth: int = 0, _seen: Optional[Set[int]] = None) -> int:
    """Approximate deep size of ``obj`` in bytes, bounded to stay O(small).

    Recursion is capped at ``_L1_SIZING_MAX_DEPTH`` levels and
    ``_L1_SIZING_MAX_ITEMS`` sampled items per container (the sampled sum is
    scaled up by the container's true length), so sizing a large cached value
    costs a bounded number of ``sys.getsizeof`` calls rather than a full
    graph walk. The result is an estimate used only for relative budget
    accounting — never for exact RSS math.
    """
    try:
        size = sys.getsizeof(obj)
    except Exception:
        return _L1_OPAQUE_SIZE_BYTES
    if obj is None or isinstance(obj, (str, bytes, bytearray, int, float, bool)):
        return size
    if _depth >= _L1_SIZING_MAX_DEPTH:
        return size
    if _seen is None:
        _seen = set()
    oid = id(obj)
    if oid in _seen:
        return 0
    _seen.add(oid)
    try:
        if isinstance(obj, dict):
            sampled_n = 0
            sampled = 0
            for k, v in itertools.islice(obj.items(), _L1_SIZING_MAX_ITEMS):
                sampled += _approx_deep_size(k, _depth + 1, _seen)
                sampled += _approx_deep_size(v, _depth + 1, _seen)
                sampled_n += 1
            if sampled_n and len(obj) > sampled_n:
                sampled = int(sampled * len(obj) / sampled_n)
            return size + sampled
        if isinstance(obj, (list, tuple, set, frozenset)):
            sampled_n = 0
            sampled = 0
            for v in itertools.islice(obj, _L1_SIZING_MAX_ITEMS):
                sampled += _approx_deep_size(v, _depth + 1, _seen)
                sampled_n += 1
            if sampled_n and len(obj) > sampled_n:
                sampled = int(sampled * len(obj) / sampled_n)
            return size + sampled
        attrs = getattr(obj, "__dict__", None)
        if attrs is not None:
            return size + _approx_deep_size(attrs, _depth + 1, _seen)
        slots = getattr(obj, "__slots__", None)
        if slots:
            return size + sum(
                _approx_deep_size(getattr(obj, s), _depth + 1, _seen)
                for s in slots
                if isinstance(s, str) and hasattr(obj, s)
            )
        return size
    except Exception:
        return size


class _L1MemoryBudget:
    """Process-wide byte budget shared by every local (L1) cache backend.

    Every ``LocalAsyncCacheBackend`` / ``LocalSyncCacheBackend`` registers
    itself here at construction (weakly — a dropped backend leaves the
    registry on GC) and keeps a running ``_bytes`` total of its entries'
    approximate deep sizes. After each insert the writing backend calls
    :meth:`enforce`; while the process-wide total exceeds the budget, the
    backend currently holding the most bytes evicts its lowest
    **value-per-byte** entry (GDSF-lite: among its LRU-oldest
    ``_L1_EVICTION_SAMPLE`` evictable entries, the one with the smallest
    ``(hits + 1) / size_bytes`` score goes first — large, rarely-hit, least
    recently used entries before small hot ones). ``NEVER_REMOVE`` entries
    are exempt.

    Budget = per-worker memory share (container memory / GUNICORN_WORKERS,
    via ``memory_watchdog.resolve_watchdog_budget_mb`` — each gunicorn worker
    is its own process with its own L1s) × ``l1_memory_percent`` / 100. The
    percent is read live so a CachePluginConfig PATCH applies immediately;
    when no memory base is resolvable (e.g. local macOS dev) or the percent
    is 0, byte-budget eviction is disabled and only per-site entry-count
    caps apply.

    Thread-safety: a single ``threading.Lock`` serializes enforcement. Async
    backends mutate only from the event-loop thread; the sync backend calls
    ``enforce(sync_caller=True)`` after releasing its own lock and
    ``_evict_one_for_budget`` re-takes it, so lock order is always
    manager → backend, never the reverse. Sync-thread enforcement never
    evicts from async backends (their stores have no thread lock); see
    :meth:`enforce`.
    """

    def __init__(self) -> None:
        self._backends: "weakref.WeakSet[Any]" = weakref.WeakSet()
        self._lock = threading.Lock()
        self._base_bytes: Optional[int] = None
        self._base_resolved = False
        # Lock-free approximate process-wide total, maintained by
        # backend._charge() via notify(). Races may skew it slightly, but
        # enforce() resyncs it from the exact per-backend sums whenever it
        # actually runs, so drift is bounded to one enforcement window. Its
        # job is to let the under-budget fast path skip the lock entirely.
        self._approx_total = 0

    def register(self, backend: Any) -> None:
        with self._lock:
            self._backends.add(backend)

    def notify(self, delta: int) -> None:
        """Record a byte-count change from a backend (see ``_approx_total``)."""
        self._approx_total += delta

    def budget_bytes(self) -> Optional[int]:
        """Current byte budget, or ``None`` when byte eviction is disabled."""
        pct = _l1_memory_percent_value
        if pct <= 0:
            return None
        if not self._base_resolved:
            # Deferred import: memory_watchdog imports tools modules that may
            # themselves import this module at startup.
            try:
                from dynastore.tools.memory_watchdog import resolve_watchdog_budget_mb

                base_mb = resolve_watchdog_budget_mb()
                self._base_bytes = (
                    base_mb * 1024 * 1024 if base_mb is not None else None
                )
                if self._base_bytes is None:
                    logger.info(
                        "L1 byte budget: no memory base resolvable — "
                        "byte-budget eviction disabled for this process"
                    )
                else:
                    logger.info(
                        "L1 byte budget: base %s MB per worker", base_mb
                    )
            except Exception as e:
                self._base_bytes = None
                logger.warning(
                    "L1 byte budget: base resolution failed (%s) — "
                    "byte-budget eviction disabled for this process", e
                )
            # Env-derived base is static per process — resolve once.
            self._base_resolved = True
        if self._base_bytes is None:
            return None
        return int(self._base_bytes * (pct / 100.0))

    def total_bytes(self) -> int:
        return sum(b._bytes for b in list(self._backends))

    def enforce(self, *, sync_caller: bool = False) -> None:
        budget = self.budget_bytes()
        if budget is None:
            return
        # Under-budget fast path: no lock, no backend scan. _approx_total is
        # resynced to the exact total below whenever enforcement runs.
        if self._approx_total <= budget:
            return
        with self._lock:
            # sync_caller: enforcement is running on an arbitrary worker
            # thread (a sync backend's set()). Async backends' stores have no
            # thread lock — they are only ever mutated from the event-loop
            # thread — so only thread-safe (sync) backends are eligible
            # victims here; async-side pressure is trimmed by the next
            # event-loop write.
            candidates = [
                b
                for b in self._backends
                if b._bytes > 0 and (not sync_caller or b._evict_thread_safe)
            ]
            total = sum(b._bytes for b in self._backends)
            while total > budget and candidates:
                victim = max(candidates, key=lambda b: b._bytes)
                freed = victim._evict_one_for_budget()
                if freed <= 0:
                    # Nothing evictable in this backend (all NEVER_REMOVE);
                    # leave it alone and keep trimming the others.
                    candidates.remove(victim)
                    continue
                total -= freed
                if victim._bytes <= 0:
                    candidates.remove(victim)
            self._approx_total = total


_l1_budget = _L1MemoryBudget()


def set_l1_runtime_config(
    *,
    l1_default_ttl_seconds: Optional[float] = None,
    l1_memory_percent: Optional[float] = None,
) -> None:
    """Push live ``CachePluginConfig`` L1 knobs into this layer.

    Called by ``CacheModule._on_cache_plugin_config_change`` so a config
    PATCH applies immediately (mirrors how ``circuit_breaker_threshold`` is
    pushed onto the live Valkey backend); the pull path in
    :func:`_load_cache_plugin_config_values` picks up seeded values on the
    normal refresh window.
    """
    global _l1_default_ttl_value, _l1_memory_percent_value
    if l1_default_ttl_seconds is not None:
        _l1_default_ttl_value = float(l1_default_ttl_seconds)
    if l1_memory_percent is not None:
        _l1_memory_percent_value = float(l1_memory_percent)


# ---------------------------------------------------------------------------
#  Shared local-store helpers
# ---------------------------------------------------------------------------
# LocalAsyncCacheBackend and LocalSyncCacheBackend have identical storage
# semantics (OrderedDict of _CacheEntry + byte accounting via _charge). These
# helpers hold the single copy of that logic so eviction scoring and byte
# accounting cannot drift between the two. Callers own the locking: the sync
# backend invokes them under self._lock, the async backend only from the
# event-loop thread.


def _local_drop_nolock(backend: Any, key: str) -> bool:
    """Remove one entry, keeping byte accounting and stats consistent."""
    entry = backend._store.pop(key, None)
    if entry is None:
        return False
    backend._charge(-entry.size_bytes)
    backend._stats.size = len(backend._store)
    return True


def _local_evict_one_for_budget_nolock(backend: Any) -> int:
    """Evict the backend's lowest value-per-byte entry; return freed bytes.

    Called under memory pressure by ``_l1_budget.enforce()``. Scores the
    LRU-oldest ``_L1_EVICTION_SAMPLE`` evictable entries by
    ``(hits + 1) / size_bytes`` and evicts the smallest score, so large
    entries that are rarely re-read go before small hot ones (GDSF-lite).
    ``NEVER_REMOVE`` entries are skipped; returns 0 when nothing is
    evictable.
    """
    evict_key = None
    best_score = None
    sampled = 0
    for k, entry in backend._store.items():
        if entry.priority >= CacheItemPriority.NEVER_REMOVE:
            continue
        score = (entry.hits + 1) / max(1, entry.size_bytes)
        if best_score is None or score < best_score:
            best_score = score
            evict_key = k
        sampled += 1
        if sampled >= _L1_EVICTION_SAMPLE:
            break
    if evict_key is None:
        return 0
    entry = backend._store.pop(evict_key)
    backend._charge(-entry.size_bytes)
    backend._stats.size = len(backend._store)
    backend._stats.evictions += 1
    return entry.size_bytes


def _local_evict_if_needed_nolock(backend: Any) -> None:
    """Entry-count (max_size) LRU eviction, independent of the byte budget."""
    while len(backend._store) >= backend._max_size:
        evict_key = None
        for k, entry in backend._store.items():
            if entry.priority < CacheItemPriority.NEVER_REMOVE:
                evict_key = k
                break
        if evict_key is None:
            # All entries are NEVER_REMOVE, evict oldest anyway
            evict_key = next(iter(backend._store))
        entry = backend._store.pop(evict_key)
        backend._charge(-entry.size_bytes)
        backend._stats.evictions += 1


def _local_set_nolock(
    backend: Any,
    key: str,
    value: Any,
    ttl: Optional[float],
    exist: Optional[bool],
) -> bool:
    """Insert/replace one entry: sizing, admission cap, byte accounting."""
    store = backend._store
    has_key = key in store
    if exist is True and not has_key:
        return False
    if exist is False and has_key:
        return False

    budget = _l1_budget.budget_bytes()
    # Sizing is only paid when the byte budget is active. Disabled-budget
    # entries are charged 0; if the budget is enabled later they stay
    # uncharged until rewritten, which their TTL bounds.
    size = _approx_deep_size(value) if budget is not None else 0

    if (
        exist is None
        and budget is not None
        and size > budget * _L1_ADMISSION_MAX_FRACTION
    ):
        # Admission cap (see _L1_ADMISSION_MAX_FRACTION): report success but
        # keep the value out of L1 — tiered callers still write it to the
        # distributed tier, direct callers recompute. Conditional writes
        # (exist=True/False) bypass the cap: their callers rely on the
        # stored-or-not outcome, not on cache economics.
        if has_key:
            _local_drop_nolock(backend, key)
        return True

    expires_at = (time.monotonic() + ttl) if ttl is not None else None
    entry = _CacheEntry(value=value, expires_at=expires_at, size_bytes=size)

    if has_key:
        old = store[key]
        # Carry hit history over so periodically-refreshed hot keys don't
        # score as cold in the GDSF eviction.
        entry.hits = old.hits
        backend._charge(size - old.size_bytes)
        store[key] = entry
        store.move_to_end(key)
    else:
        _local_evict_if_needed_nolock(backend)
        store[key] = entry
        backend._charge(size)

    backend._stats.size = len(store)
    return True


def _local_clear_all_nolock(backend: Any) -> bool:
    """Drop every entry (and per-key locks), zeroing byte accounting."""
    had_items = len(backend._store) > 0
    backend._store.clear()
    locks = getattr(backend, "_locks", None)
    if locks is not None:
        locks.clear()
    backend._charge(-backend._bytes)
    backend._stats.size = 0
    return had_items


# ---------------------------------------------------------------------------
#  LocalAsyncCacheBackend
# ---------------------------------------------------------------------------


class LocalAsyncCacheBackend:
    """In-memory async cache backend using OrderedDict with LRU eviction.

    - TTL checked on ``get()``, lazy expiration
    - Thundering-herd protection via per-key ``asyncio.Lock``
    - ``NullSerializer`` (stores objects directly)
    - priority = 1000
    """

    # Async stores have no thread lock — budget eviction from a foreign
    # (non-event-loop) thread is unsafe. See _L1MemoryBudget.enforce.
    _evict_thread_safe = False

    def __init__(self, max_size: int = 4096) -> None:
        self._store: collections.OrderedDict[str, _CacheEntry] = collections.OrderedDict()
        self._max_size = max_size
        self._locks: Dict[str, asyncio.Lock] = {}
        self._stats = CacheStats(maxsize=max_size)
        self._bytes = 0
        _l1_budget.register(self)

    @property
    def name(self) -> str:
        return "local-async"

    @property
    def priority(self) -> int:
        return 1000

    def _charge(self, delta: int) -> None:
        """Single mutation point for byte accounting (backend + budget)."""
        self._bytes += delta
        _l1_budget.notify(delta)

    def _drop_entry(self, key: str) -> bool:
        return _local_drop_nolock(self, key)

    def _evict_one_for_budget(self) -> int:
        return _local_evict_one_for_budget_nolock(self)

    async def get(self, key: str) -> Optional[bytes]:
        entry = self._store.get(key)
        if entry is None:
            return None
        if entry.is_expired():
            self._drop_entry(key)
            return None
        entry.hits += 1
        self._store.move_to_end(key)
        return entry.value

    async def set(
        self,
        key: str,
        value: bytes,
        *,
        ttl: Optional[float] = None,
        exist: Optional[bool] = None,
    ) -> bool:
        ok = _local_set_nolock(self, key, value, ttl, exist)
        if ok:
            _l1_budget.enforce()
        return ok

    async def clear(
        self,
        *,
        key: Optional[str] = None,
        namespace: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> bool:
        if key is not None:
            return self._drop_entry(key)

        if namespace is not None:
            prefix = namespace + ":"
            to_delete = [k for k in self._store if k.startswith(prefix)]
            for k in to_delete:
                self._drop_entry(k)
            return len(to_delete) > 0

        # tags: not supported in local backend (no tag index)
        if tags is not None:
            return False

        # Clear everything
        return _local_clear_all_nolock(self)

    async def exists(self, key: str) -> bool:
        entry = self._store.get(key)
        if entry is None:
            return False
        if entry.is_expired():
            self._drop_entry(key)
            return False
        return True

    async def close(self) -> None:
        _local_clear_all_nolock(self)

    def _evict_if_needed(self) -> None:
        _local_evict_if_needed_nolock(self)

    async def get_lock(self, key: str) -> asyncio.Lock:
        # asyncio is single-threaded and cooperative — no concurrent access to
        # _locks between suspension points, so no global lock is needed here.
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]


# ---------------------------------------------------------------------------
#  Cache version envelope
# ---------------------------------------------------------------------------

# Field names chosen to be short and collision-resistant.  Real cached
# values are Python dicts/models from config services; having both "__v"
# AND "__d" at the top level of a user value is astronomically unlikely.
_EV_VER = "__v"   # monotonic version key  (int, time.time_ns())
_EV_DATA = "__d"  # payload key            (any serialisable value)
_EV_TOMB = "__t"  # tombstone flag key     (bool True; no __d present)


def _ev_wrap(value: Any, ver: int) -> Dict[str, Any]:
    """Wrap ``value`` in a version envelope for tiered storage."""
    return {_EV_VER: ver, _EV_DATA: value}


def _ev_tombstone(ver: int) -> Dict[str, Any]:
    """Create a tombstone envelope (marks a deleted key)."""
    return {_EV_VER: ver, _EV_TOMB: True}


# CacheBackend.set() is typed `value: bytes` ("Backends store raw bytes;
# the Cache wrapper handles serialization" — see models/protocols/cache.py).
# TieredAsyncBackend stores version envelopes (plain dicts) directly and
# relies on its backends being object-passthrough (NullSerializer) tiers —
# same runtime contract LocalCache uses via `self._serializer.dumps()`.
# Routing envelopes through NullSerializer.dumps() here is a no-op at
# runtime (it returns the value unchanged) but gives pyright a `bytes`-typed
# value at the `CacheBackend.set()` call sites, matching that convention
# without changing the CacheBackend interface or backend behavior.
_ENVELOPE_SERIALIZER = NullSerializer()


def _ev_parse(data: Any) -> "tuple[int, Any, bool]":
    """Parse a stored value.

    Returns ``(ver, value, is_tombstone)``:

    - Normal envelope  → ``(ver, value, False)``
    - Tombstone        → ``(ver, None, True)``
    - Legacy raw value → ``(0, data, False)`` — treated as ver 0 so any
      new envelope write out-versions it during rolling deployment.
    """
    if isinstance(data, dict) and _EV_VER in data:
        ver = int(data[_EV_VER])
        if _EV_TOMB in data:
            return ver, None, True
        return ver, data.get(_EV_DATA), False
    # Legacy (pre-envelope) entry — ver=0 is out-versioned by any new write.
    return 0, data, False


# ---------------------------------------------------------------------------
#  Stale-serving envelope (#2902)
# ---------------------------------------------------------------------------

# A cache entry is kept physically alive past its logical TTL by a grace
# window, so a slow or failing rebuild (DB pool starvation, downstream
# outage) can serve the last-known-good value instead of every queued
# waiter riding an unbounded factory() call to the caller's own gateway
# timeout. This wrapper is independent of the L1/L2 version envelope above
# (``_ev_wrap``/``_ev_parse`` — tiered-backend concurrency): it operates one
# layer up, on the value ``LocalCache.get_or_set`` and the ``@cached``
# decorator hand to whichever backend is resolved (local or tiered), so it
# behaves the same regardless of backend type.

DEFAULT_STALE_GRACE_SECONDS: float = 300.0
_DEFAULT_SLOW_PATH_TIMEOUT_SECONDS: float = 30.0

_STALE_AT = "__sat"    # wall-clock write time (time.time())
_STALE_TTL = "__sttl"  # logical ttl (seconds) in effect at write time
_STALE_VAL = "__sval"  # payload

_NO_STALE: Any = object()  # sentinel: "no stale value available for fallback"


def _stale_wrap(value: Any, ttl: float) -> Dict[str, Any]:
    """Wrap ``value`` with its write time + logical ttl for stale-grace tracking."""
    return {_STALE_AT: time.time(), _STALE_TTL: ttl, _STALE_VAL: value}


def _stale_unwrap(raw: Any) -> "tuple[Any, bool]":
    """Unwrap a stale-tracked entry -> ``(value, is_stale)``.

    Entries not produced by ``_stale_wrap`` (grace disabled at write time)
    are returned as-is and are never considered stale.
    """
    if not (
        isinstance(raw, dict)
        and _STALE_AT in raw
        and _STALE_TTL in raw
        and _STALE_VAL in raw
    ):
        return raw, False
    age = time.time() - float(raw[_STALE_AT])
    return raw[_STALE_VAL], age > float(raw[_STALE_TTL])


# The config read below goes through the config service, whose loads are
# themselves ``@cached`` — so a cache miss on the config entry would re-enter
# the slow path and call back into this loader, recursing until Python's
# stack limit ("maximum recursion depth exceeded" storms on boot). The
# ContextVar breaks that same-task re-entrancy; the memo keeps the value off
# the per-miss hot path (and off the config service entirely between
# refreshes).
_SLOW_PATH_TIMEOUT_REFRESH_SECONDS: float = 60.0
_DEFAULT_MAX_CONCURRENT_REBUILDS: int = 4
_slow_path_timeout_value: float = _DEFAULT_SLOW_PATH_TIMEOUT_SECONDS
_max_concurrent_rebuilds_value: int = _DEFAULT_MAX_CONCURRENT_REBUILDS
_slow_path_timeout_checked_at: float = 0.0  # time.monotonic(); 0 = never
_slow_path_timeout_loading: "contextvars.ContextVar[bool]" = contextvars.ContextVar(
    "cache_slow_path_timeout_loading", default=False
)


async def _load_cache_plugin_config_values() -> None:
    """Refresh the memoized ``CachePluginConfig`` values used on the cache
    slow path: ``slow_path_timeout_seconds`` and
    ``max_concurrent_detached_rebuilds``.

    Both fields live on the same config row, so they share one fetch, one
    re-entrancy guard, and one refresh interval — see
    :func:`_load_slow_path_timeout` for why the guard exists.
    """
    global _slow_path_timeout_value, _max_concurrent_rebuilds_value
    global _slow_path_timeout_checked_at

    if _slow_path_timeout_loading.get():
        # Re-entered from the @cached config load this function triggered:
        # answering with the memo/default is what terminates the recursion.
        return

    now = time.monotonic()
    if (
        _slow_path_timeout_checked_at
        and now - _slow_path_timeout_checked_at < _SLOW_PATH_TIMEOUT_REFRESH_SECONDS
    ):
        return

    token = _slow_path_timeout_loading.set(True)
    try:
        from dynastore.modules.cache.cache_config import CachePluginConfig
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        configs_proto = get_protocol(ConfigsProtocol)
        if configs_proto is not None:
            cfg = await configs_proto.get_config(CachePluginConfig)
            if cfg is not None:
                _slow_path_timeout_value = cfg.slow_path_timeout_seconds
                _max_concurrent_rebuilds_value = cfg.max_concurrent_detached_rebuilds
                # L1 knobs share the same config row — refresh them on the
                # same fetch so seeded (non-default) values are picked up
                # even if no config PATCH ever fires (see
                # set_l1_runtime_config for the push path).
                set_l1_runtime_config(
                    l1_default_ttl_seconds=getattr(
                        cfg, "l1_default_ttl_seconds", None
                    ),
                    l1_memory_percent=getattr(cfg, "l1_memory_percent", None),
                )
    except Exception as e:
        logger.debug(
            "cache: CachePluginConfig load failed (%s), using cached/default values", e
        )
    finally:
        _slow_path_timeout_loading.reset(token)
        # Stamp even on failure so a broken config path is retried once per
        # refresh window, not hammered on every cache miss.
        _slow_path_timeout_checked_at = now


async def _load_slow_path_timeout() -> float:
    """Load ``slow_path_timeout_seconds`` from ``CachePluginConfig``.

    Bounds lock-wait + factory time on the ``get_or_set``/``@cached`` slow
    path. Falls back to the last-known value (default 30s) when the memoized
    value is still fresh or ``ConfigsProtocol`` is unavailable / the config
    load fails — mirrors
    ``modules/tasks/dispatcher.py::_load_oracle_inner_timeout``.
    """
    await _load_cache_plugin_config_values()
    return _slow_path_timeout_value


async def _load_max_concurrent_rebuilds() -> int:
    """Load ``max_concurrent_detached_rebuilds`` from ``CachePluginConfig``.

    Bounds how many detached cache-rebuild tasks (see
    :func:`_await_shared_rebuild`) may run concurrently across the process.
    Falls back to the last-known value (default 4) on the same terms as
    :func:`_load_slow_path_timeout`.
    """
    await _load_cache_plugin_config_values()
    return _max_concurrent_rebuilds_value


# In-flight rebuild tasks, one per cache key. A rebuild runs as its OWN task,
# shared by every concurrent miss on that key, and always runs to completion:
# cancelling in-flight DB work mid-query leaves the asyncpg connection's
# protocol state machine stuck ("cannot switch to state N; another operation
# is in progress") and poisons the pool (#2900). The original slow-path
# ``asyncio.timeout`` did exactly that — it cancelled ``factory()`` at the
# budget, and every client disconnect did the same to the request's factory —
# so under post-deploy cold-cache load each 30s rebuild cancellation seeded a
# fresh poisoned connection (the 06:29Z state-11 storm). Callers now wait on
# the shared task via ``wait_for(shield(task))``: a caller that gives up
# (budget below, client disconnect) stops WAITING, while the rebuild finishes
# and writes the cache for whoever comes next.
_inflight_rebuilds: Dict[str, "asyncio.Task[Any]"] = {}

# Process-wide cap on concurrent detached rebuild tasks (#2902): a CPU-
# throttling storm can turn dozens of simultaneous cache misses (each its own
# key) into dozens of detached tasks all racing for the same small DB pool,
# each holding/queuing a slot for up to ``slow_path_timeout_seconds``. The
# semaphore is acquired INSIDE the detached task (see ``_gated_rebuild``
# below), not by the caller of ``_await_shared_rebuild`` — callers already
# fall back to a stale value or their own timeout while they wait, so having
# an excess rebuild candidate queue for a slot costs nothing but time.
_rebuild_semaphore: Optional[asyncio.Semaphore] = None
_rebuild_semaphore_limit: int = 0


async def _get_rebuild_semaphore() -> asyncio.Semaphore:
    """Return the process-wide detached-rebuild concurrency semaphore.

    Reads ``max_concurrent_detached_rebuilds`` live (mirrors the
    background-DB-concurrency semaphore in ``query_executor.py``) and rebuilds
    the semaphore when the configured limit changes; in-flight holders of a
    superseded semaphore are unaffected and release normally.
    """
    global _rebuild_semaphore, _rebuild_semaphore_limit
    limit = await _load_max_concurrent_rebuilds()
    if _rebuild_semaphore is None or _rebuild_semaphore_limit != limit:
        _rebuild_semaphore = asyncio.Semaphore(limit)
        _rebuild_semaphore_limit = limit
    return _rebuild_semaphore


def _is_orphaned_teardown_error(exc: BaseException) -> bool:
    """True for a torn-down-connection error from an orphaned rebuild (#3023).

    A detached ``@cached`` rebuild (:func:`_await_shared_rebuild`) always runs
    to completion even after every waiter has stopped waiting on it (#2900 --
    cancelling in-flight DB work mid-query poisons the connection pool). If
    the rebuild's own connection then gets torn down before it finishes, the
    resulting ``InterfaceError``/``DatabaseConnectionError`` ("connection is
    closed") is expected teardown noise rather than an actionable failure --
    no live caller depends on the outcome.
    """
    from sqlalchemy.exc import InterfaceError
    from dynastore.modules.db_config.exceptions import DatabaseConnectionError

    if isinstance(exc, (InterfaceError, DatabaseConnectionError)):
        return "closed" in str(exc).lower()
    return False


async def _await_shared_rebuild(
    key: str,
    rebuild: "Callable[[], Coroutine[Any, Any, Any]]",
    timeout: float,
) -> Any:
    """Await the single in-flight rebuild task for ``key``, bounded by ``timeout``.

    Creates the task if none is running. Raises ``TimeoutError`` when the wait
    budget elapses (the task keeps running detached) and re-raises whatever the
    rebuild itself raised; the caller decides whether a stale value absorbs it.
    """
    task = _inflight_rebuilds.get(key)
    if task is None or task.done():

        async def _gated_rebuild(_rebuild: "Callable[[], Coroutine[Any, Any, Any]]" = rebuild) -> Any:
            sem = await _get_rebuild_semaphore()
            async with sem:
                return await _rebuild()

        task = asyncio.get_running_loop().create_task(_gated_rebuild())
        _inflight_rebuilds[key] = task

        def _cleanup(t: "asyncio.Task[Any]", _key: str = key) -> None:
            if _inflight_rebuilds.get(_key) is t:
                _inflight_rebuilds.pop(_key, None)
            # Retrieve the exception so an abandoned detached rebuild (every
            # waiter already timed out) surfaces as one structured log line
            # instead of asyncio's raw "exception was never retrieved" ERROR
            # traceback.
            if not t.cancelled():
                exc = t.exception()
                if exc is not None:
                    if _is_orphaned_teardown_error(exc):
                        # The rebuild outlived every waiter (all timed out or
                        # disconnected) and its own connection was then torn
                        # down mid-flight — no live caller depends on this
                        # result, so a "connection is closed" rollback/
                        # transaction error here is expected teardown noise,
                        # not an actionable failure (#3023). Cancelling the
                        # rebuild instead is not an option: it always runs to
                        # completion by design (#2900) since cancelling
                        # in-flight DB work mid-query poisons the pool.
                        logger.debug(
                            "cache_rebuild_orphaned key=%s error_type=%s error=%s",
                            _key, type(exc).__name__, exc,
                        )
                    else:
                        logger.warning(
                            "cache_rebuild_failed key=%s error_type=%s error=%s",
                            _key, type(exc).__name__, exc,
                        )

        task.add_done_callback(_cleanup)
    return await asyncio.wait_for(asyncio.shield(task), timeout=timeout)


# ---------------------------------------------------------------------------
#  TieredAsyncBackend
# ---------------------------------------------------------------------------


class TieredAsyncBackend:
    """Chain multiple cache backends in priority order (L1, L2, L3...).

    Read path: L2-authoritative with version stamping — every value is
    stored as a ``{"__v": ver, "__d": payload}`` envelope where ``ver`` is
    ``time.time_ns()`` captured at write time.  On ``get()``, both L1
    (in-process) and L2 (distributed) are read; the envelope with the
    **higher version wins**.

    This gives two guarantees at once:
    - Cross-instance accuracy: L2 (Valkey) acts as the distributed source
      of truth; a newer write from another Cloud Run instance propagates to
      L1 automatically on the next local read.
    - Read-your-write: ``set()`` stamps L1 with a fresh ver before the
      asynchronous L2 write completes, so an immediate local read always
      returns the new value (L1 ver > stale L2 ver).

    Clock-skew caveat: ``time.time_ns()`` is wall-clock, so two instances
    with clock skew may produce non-monotonic versions across processes.
    This is acceptable because these caches hold infrequently-written
    configuration data and the version is only compared within a TTL window
    bounded by ``l1_ttl_cap``.  NTP keeps skew well under the write interval
    for these workloads.

    Write path: stamp envelope → L1 synchronous, L2+ background with retry.
    If a conditional write (``exist=``) is rejected by L1, L2 writes are
    skipped and ``False`` is returned.

    Clear/invalidation: keyed clear writes a **tombstone** envelope to L1
    **and** L2 **synchronously** (not background) so any L2-authoritative
    read in another process sees the tombstone immediately.  Tombstones
    carry a fresh ver so they out-version any concurrent stale async set.
    Namespace/tags clears delete from all backends synchronously.

    Background L2 writes (#2328):
        L2+ set operations are scheduled as background tasks with exponential
        backoff retry.  If L2 is unreliable the TTL cap self-heals staleness.
        Set ``l2_retry_attempts=0`` to skip background writes entirely (no
        task scheduled, no warning emitted).

    Implements ``get_lock`` by delegating to the first tier that supports it.
    """

    DEFAULT_L1_TTL_CAP: float = _DEFAULT_L1_TTL_SECONDS
    DEFAULT_L2_RETRY_ATTEMPTS: int = 3
    DEFAULT_L2_RETRY_BACKOFF: float = 0.1

    def __init__(
        self,
        backends: List[CacheBackend],
        l1_ttl_cap: Optional[float] = None,
        l2_retry_attempts: Optional[int] = None,
        l2_retry_backoff: Optional[float] = None,
    ) -> None:
        """Initialize with an ordered list of backends (best to worst).

        Args:
            backends: Ordered list of backends (L1, L2, ...).
            l1_ttl_cap: Bounds TTL for L1 tier. ``None`` (the default) means
                "use ``CachePluginConfig.l1_default_ttl_seconds``", read live
                on every L1 write (default 30s) so a config PATCH applies
                without rebuilding the backend.
            l2_retry_attempts: Max attempts for L2+ background writes.
                Defaults to 3. Set to 0 to skip background writes entirely
                (no task is scheduled, no warning is emitted).
            l2_retry_backoff: Initial backoff in seconds for L2+ retry.
                Defaults to 0.1s (100ms). Exponential backoff applied.
        """
        if not backends:
            raise ValueError("TieredAsyncBackend requires at least one backend")
        self._backends = backends
        self._name = "-".join(b.name for b in backends)
        self._priority = min(b.priority for b in backends)
        self._l1_ttl_cap_explicit: Optional[float] = (
            None if l1_ttl_cap is None else float(l1_ttl_cap)
        )
        self._l2_retry_attempts = (
            self.DEFAULT_L2_RETRY_ATTEMPTS
            if l2_retry_attempts is None
            else int(l2_retry_attempts)
        )
        self._l2_retry_backoff = (
            self.DEFAULT_L2_RETRY_BACKOFF
            if l2_retry_backoff is None
            else float(l2_retry_backoff)
        )
        self._required_l2 = any(
            getattr(backend, "required", False) for backend in backends[1:]
        )
        self._stats = CacheStats()
        self._pending_bg_tasks: Set[asyncio.Task] = set()

    @property
    def name(self) -> str:
        return f"tiered({self._name})"

    @property
    def priority(self) -> int:
        return self._priority

    def _effective_l1_ttl_cap(self) -> float:
        """L1 TTL cap: per-site override, else the live config default."""
        return (
            self._l1_ttl_cap_explicit
            if self._l1_ttl_cap_explicit is not None
            else _l1_default_ttl_value
        )

    async def get(self, key: str) -> Optional[Any]:
        """L2-authoritative versioned read.

        Reads both L1 (in-process) and L2 (distributed) and returns the
        value carried by the **higher-versioned** envelope.

        - L2 unavailable: falls back to L1 best-effort (degraded mode).
        - L2 wins (ver >= L1 ver): refreshes L1 from L2 and returns L2 value.
        - L1 wins (own fresh write not yet propagated): returns L1, leaves L2.
        - Tombstone in winning tier: returns ``None`` (caller treats as miss).
        - Legacy unversioned entry in either tier: treated as ver=0, so any
          envelope write out-versions it automatically during rolling deploys.
        """
        l1_raw = await self._backends[0].get(key)
        l1_ver, l1_val, l1_tomb = _ev_parse(l1_raw) if l1_raw is not None else (-1, None, False)
        l1_present = l1_raw is not None

        # Read L2 (distributed source of truth).
        l2_present = False
        l2_ver = -1
        l2_val: Any = None
        l2_tomb = False
        if len(self._backends) > 1:
            try:
                l2_raw = await self._backends[1].get(key)
                if l2_raw is not None:
                    l2_present = True
                    l2_ver, l2_val, l2_tomb = _ev_parse(l2_raw)
            except Exception as e:
                log = logger.error if self._required_l2 else logger.warning
                log("L2 cache get failed (key=%s): %s", key, e)
                if self._required_l2:
                    raise
                # L2 unavailable — fall back to L1 best-effort.
                if l1_present and not l1_tomb:
                    return l1_val
                return None

        if not l1_present and not l2_present:
            return None

        # L2 wins when it is present and its version is >= L1's version.
        if l2_present and (not l1_present or l2_ver >= l1_ver):
            if l2_tomb:
                # Propagate tombstone to L1 so subsequent reads don't serve stale data.
                if l1_present and not l1_tomb:
                    await self._backends[0].set(
                        key,
                        _ENVELOPE_SERIALIZER.dumps(_ev_tombstone(l2_ver)),
                        ttl=self._effective_l1_ttl_cap(),
                    )
                return None
            # Refresh L1 from L2 (version-guarded write).
            await self._backends[0].set(
                key,
                _ENVELOPE_SERIALIZER.dumps(_ev_wrap(l2_val, l2_ver)),
                ttl=self._effective_l1_ttl_cap(),
            )
            return l2_val

        # L1 wins — our own fresher write not yet propagated to L2.
        if l1_tomb:
            return None
        return l1_val

    async def set(
        self,
        key: str,
        value: Any,
        *,
        ttl: Optional[float] = None,
        exist: Optional[bool] = None,
    ) -> bool:
        """Stamp a version envelope and write to L1 synchronously, L2+ in background.

        The version (``time.time_ns()``) is captured once and embedded in the
        envelope stored in every tier.  Because ``get()`` picks the higher
        version, the synchronous L1 write wins over any stale L2 value still
        present during the background-write window (read-your-write guarantee).

        If a conditional write (``exist=True/False``) is rejected by L1, L2
        writes are skipped and ``False`` is returned.
        """
        ver = time.time_ns()
        envelope = _ENVELOPE_SERIALIZER.dumps(_ev_wrap(value, ver))

        l1_cap = self._effective_l1_ttl_cap()
        l1_ttl: Optional[float] = min(ttl, l1_cap) if ttl is not None else l1_cap
        l1_ok = await self._backends[0].set(key, envelope, ttl=l1_ttl, exist=exist)
        if not l1_ok:
            # Conditional write precondition rejected by L1; skip L2.
            return False

        for i, backend in enumerate(self._backends[1:], start=2):
            async def _set_op(b: CacheBackend = backend, e: Any = envelope) -> bool:
                return await b.set(key, e, ttl=ttl, exist=exist)
            task = asyncio.create_task(
                self._bg_op_with_retry(_set_op, i, "set", f"key={key}")
            )
            self._pending_bg_tasks.add(task)
            task.add_done_callback(self._pending_bg_tasks.discard)

        return True

    async def clear(
        self,
        *,
        key: Optional[str] = None,
        namespace: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> bool:
        """Invalidate cache entries with immediate cluster-wide visibility.

        Keyed clear (``key=``):
            Writes a versioned tombstone to L1 **and** all L2+ backends
            **synchronously** (not in background).  A tombstone carries a
            fresh ``time.time_ns()`` version so it out-versions any
            concurrent stale async set still in flight.  Callers that read
            via ``get()`` see the tombstone immediately regardless of which
            instance they are on.  On L2 write failure a warning is logged
            but the L1 tombstone still guards local reads for ``l1_ttl_cap``.

        Namespace / tags clear:
            Deletes matching keys from all backends synchronously.  Namespace
            clears cannot use per-key tombstones without a full key scan, so
            they rely on the same synchronous-delete approach as before.
        """
        if key is not None:
            ver = time.time_ns()
            tombstone = _ENVELOPE_SERIALIZER.dumps(_ev_tombstone(ver))
            l1_cap = self._effective_l1_ttl_cap()
            # L1 tombstone — synchronous, always fast.
            await self._backends[0].set(key, tombstone, ttl=l1_cap)
            # L2+ tombstone — synchronous so other instances see it immediately.
            for i, backend in enumerate(self._backends[1:], start=2):
                try:
                    await backend.set(key, tombstone, ttl=l1_cap)
                except Exception as e:
                    logger.warning(
                        "L%d cache clear tombstone write failed (key=%s): %s — "
                        "L1 tombstone guards local reads for %.0fs",
                        i, key, e, l1_cap,
                    )
            return True

        # Namespace / tags clear: synchronous delete on all backends.
        l1_result = await self._backends[0].clear(namespace=namespace, tags=tags)
        for i, backend in enumerate(self._backends[1:], start=2):
            try:
                await backend.clear(namespace=namespace, tags=tags)
            except Exception as e:
                logger.warning(
                    "L%d cache clear failed (ns=%s): %s", i, namespace, e
                )
        return l1_result

    async def _bg_op_with_retry(
        self,
        op_factory: Callable[[], Awaitable[bool]],
        tier: int,
        op_name: str,
        log_ctx: str,
    ) -> None:
        """Shared background retry loop for L2+ set operations.

        Skipped entirely (no attempt, no warning) when ``l2_retry_attempts`` is 0.
        ``op_factory`` is called once per attempt so each retry creates a fresh
        coroutine — never re-awaits a spent one.
        """
        if self._l2_retry_attempts <= 0:
            return
        for attempt in range(self._l2_retry_attempts):
            try:
                if await op_factory():
                    return
            except Exception as e:
                logger.debug(
                    "L%d cache %s exception (attempt %d/%d): %s",
                    tier, op_name, attempt + 1, self._l2_retry_attempts, e,
                )
            if attempt < self._l2_retry_attempts - 1:
                backoff = self._l2_retry_backoff * (2**attempt)
                await asyncio.sleep(backoff)
        logger.warning(
            "L%d cache %s failed after %d attempts (%s) — TTL cap will self-heal",
            tier, op_name, self._l2_retry_attempts, log_ctx,
        )

    async def exists(self, key: str) -> bool:
        """Check any tier."""
        for backend in self._backends:
            if await backend.exists(key):
                return True
        return False

    async def close(self) -> None:
        """Wait for pending background tasks, then close all backends."""
        if self._pending_bg_tasks:
            await asyncio.gather(*self._pending_bg_tasks, return_exceptions=True)
        for backend in self._backends:
            await backend.close()

    async def get_lock(self, key: str) -> asyncio.Lock:
        """Delegate to first tier that implements LockableCacheBackend."""
        for backend in self._backends:
            if isinstance(backend, LockableCacheBackend):
                return await backend.get_lock(key)
        return asyncio.Lock()


# ---------------------------------------------------------------------------
#  LocalSyncCacheBackend
# ---------------------------------------------------------------------------


class LocalSyncCacheBackend:
    """In-memory synchronous cache backend using OrderedDict with LRU eviction.

    - Same semantics as ``LocalAsyncCacheBackend`` but for sync contexts
    - priority = 1000
    """

    # _evict_one_for_budget takes self._lock, so budget eviction is safe
    # from any thread. See _L1MemoryBudget.enforce.
    _evict_thread_safe = True

    def __init__(self, max_size: int = 4096) -> None:
        self._store: collections.OrderedDict[str, _CacheEntry] = collections.OrderedDict()
        self._max_size = max_size
        self._lock = threading.Lock()
        self._stats = CacheStats(maxsize=max_size)
        self._bytes = 0
        _l1_budget.register(self)

    @property
    def name(self) -> str:
        return "local-sync"

    @property
    def priority(self) -> int:
        return 1000

    def _charge(self, delta: int) -> None:
        """Single mutation point for byte accounting (backend + budget)."""
        self._bytes += delta
        _l1_budget.notify(delta)

    def _drop_entry_nolock(self, key: str) -> bool:
        """Remove one entry (caller holds ``self._lock``)."""
        return _local_drop_nolock(self, key)

    def _evict_one_for_budget(self) -> int:
        """Byte-budget eviction — same GDSF-lite scoring as the async backend.

        Takes ``self._lock`` itself: only ever called by ``_l1_budget.enforce()``,
        which this backend invokes strictly AFTER releasing its own lock, so
        lock order stays manager → backend.
        """
        with self._lock:
            return _local_evict_one_for_budget_nolock(self)

    def get(self, key: str) -> Optional[bytes]:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            if entry.is_expired():
                self._drop_entry_nolock(key)
                return None
            entry.hits += 1
            self._store.move_to_end(key)
            return entry.value

    def set(
        self,
        key: str,
        value: bytes,
        *,
        ttl: Optional[float] = None,
        exist: Optional[bool] = None,
    ) -> bool:
        with self._lock:
            ok = _local_set_nolock(self, key, value, ttl, exist)
        # Outside self._lock: enforce() may call back into
        # _evict_one_for_budget, which re-takes it (threading.Lock is not
        # reentrant). sync_caller: this may run on any thread, so only
        # thread-safe backends are eligible eviction victims.
        if ok:
            _l1_budget.enforce(sync_caller=True)
        return ok

    def clear(
        self,
        *,
        key: Optional[str] = None,
        namespace: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> bool:
        with self._lock:
            if key is not None:
                return self._drop_entry_nolock(key)

            if namespace is not None:
                prefix = namespace + ":"
                to_delete = [k for k in self._store if k.startswith(prefix)]
                for k in to_delete:
                    self._drop_entry_nolock(k)
                return len(to_delete) > 0

            if tags is not None:
                return False

            return _local_clear_all_nolock(self)

    def exists(self, key: str) -> bool:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return False
            if entry.is_expired():
                self._drop_entry_nolock(key)
                return False
            return True

    def close(self) -> None:
        with self._lock:
            _local_clear_all_nolock(self)

    def _evict_if_needed(self) -> None:
        _local_evict_if_needed_nolock(self)


# ---------------------------------------------------------------------------
#  LocalCache -- high-level Cache wrapping local async backend
# ---------------------------------------------------------------------------


class LocalCache:
    """High-level ``Cache`` implementation wrapping a ``LocalAsyncCacheBackend``.

    Handles namespacing, serialization, events, and stampede protection
    via ``get_or_set()``.
    """

    def __init__(
        self,
        backend: LocalAsyncCacheBackend,
        config: CacheConfig,
        serializer: Optional[CacheSerializer] = None,
        event_listeners: Optional[List[CacheEventListener]] = None,
    ):
        self._backend = backend
        self._config = config
        self._serializer = serializer or NullSerializer()
        self._listeners = event_listeners or []
        self._stats = CacheStats(maxsize=config.max_size)

    def _full_key(self, key: str, namespace: Optional[str] = None) -> str:
        ns = namespace or self._config.namespace
        return f"{ns}:{key}" if ns else key

    def _resolve_ttl(self, ttl: Optional[Union[timedelta, float]]) -> Optional[float]:
        if ttl is not None:
            return ttl.total_seconds() if isinstance(ttl, timedelta) else float(ttl)
        if self._config.default_ttl is not None:
            return float(self._config.default_ttl)
        return None

    async def _emit(self, event: CacheEvent, key: Optional[str] = None, **kw: Any) -> None:
        if not self._listeners:
            return
        data = CacheEventData(
            event=event,
            key=key,
            namespace=self._config.namespace,
            backend_name=self._backend.name,
            **kw,
        )
        for listener in self._listeners:
            try:
                result = listener(data)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                logger.debug("Cache event listener error", exc_info=True)

    async def get(
        self,
        key: str,
        *,
        default: Any = None,
        namespace: Optional[str] = None,
    ) -> Any:
        full_key = self._full_key(key, namespace)
        t0 = time.monotonic()
        raw = await self._backend.get(full_key)
        elapsed = (time.monotonic() - t0) * 1000

        if raw is None:
            self._stats.misses += 1
            await self._emit(CacheEvent.GET_MISS, key=full_key, elapsed_ms=elapsed)
            return default

        self._stats.hits += 1
        await self._emit(CacheEvent.GET_HIT, key=full_key, elapsed_ms=elapsed)
        return self._serializer.loads(raw)

    async def set(
        self,
        key: str,
        value: Any,
        *,
        ttl: Optional[Union[timedelta, float]] = None,
        namespace: Optional[str] = None,
        exist: Optional[bool] = None,
        priority: CacheItemPriority = CacheItemPriority.NORMAL,
        tags: Optional[List[str]] = None,
    ) -> bool:
        if not self._config.enable:
            return False
        full_key = self._full_key(key, namespace)
        resolved_ttl = self._resolve_ttl(ttl)
        serialized = self._serializer.dumps(value)
        ok = await self._backend.set(
            full_key, serialized, ttl=resolved_ttl, exist=exist
        )
        if ok:
            await self._emit(CacheEvent.SET, key=full_key, ttl=resolved_ttl)
        return ok

    async def clear(
        self,
        *,
        key: Optional[str] = None,
        namespace: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> bool:
        full_key = self._full_key(key) if key else None
        ns = namespace or (None if key else self._config.namespace or None)
        result = await self._backend.clear(key=full_key, namespace=ns, tags=tags)
        if result:
            await self._emit(CacheEvent.CLEAR, key=full_key)
        self._stats.size = self._backend._stats.size
        return result

    async def exists(
        self,
        key: str,
        *,
        namespace: Optional[str] = None,
    ) -> bool:
        return await self._backend.exists(self._full_key(key, namespace))

    async def get_or_set(
        self,
        key: str,
        factory: Callable[[], Awaitable[Any]],
        *,
        ttl: Optional[Union[timedelta, float]] = None,
        namespace: Optional[str] = None,
        stale_grace: Optional[Union[timedelta, float]] = None,
    ) -> Any:
        """Stampede-safe get-or-create with bounded slow path + stale fallback.

        ``stale_grace`` (default ``DEFAULT_STALE_GRACE_SECONDS`` = 300s, ``0``
        disables it) keeps the previous value physically alive past its
        logical ``ttl`` so that, if the rebuild's lock-wait + ``factory()``
        call exceeds ``CachePluginConfig.slow_path_timeout_seconds`` (default
        30s) or ``factory()`` raises, the stale value is served instead of
        propagating (see #2902). With no stale value available, the original
        exception/timeout propagates promptly.
        """
        resolved_ttl = self._resolve_ttl(ttl)
        grace = (
            DEFAULT_STALE_GRACE_SECONDS
            if stale_grace is None
            else (
                stale_grace.total_seconds()
                if isinstance(stale_grace, timedelta)
                else float(stale_grace)
            )
        )
        stale_on = grace > 0 and resolved_ttl is not None
        # Cross-revision key compatibility (#2902): an old-revision process
        # reading a stale-wrapped entry has no unwrap logic and would return
        # the wrapper dict as the value. Version-prefixing the key when
        # stale-wrapping is active means old code simply misses (one cold
        # rebuild, absorbed by the bounded slow path) instead of misreading;
        # this also makes rollback safe since the old revision resumes
        # reading its own unprefixed keys.
        full_key = self._full_key(key, namespace)
        if stale_on:
            full_key = "sv1|" + full_key
        stale_candidate: Any = _NO_STALE

        async def _read() -> "tuple[Any, bool]":
            raw = await self._backend.get(full_key)
            if raw is None:
                return _NO_STALE, False
            loaded = self._serializer.loads(raw)
            return _stale_unwrap(loaded) if stale_on else (loaded, False)

        # Fast path: value in cache
        value, is_stale = await _read()
        if value is not _NO_STALE:
            if not is_stale:
                self._stats.hits += 1
                return value
            stale_candidate = value

        # Slow path: one shared rebuild task per key (see
        # _await_shared_rebuild) so a caller abandoning the wait — the budget
        # below, or a client disconnect cancelling the request — never
        # cancels the factory's in-flight DB work (#2900 poisoning). The
        # per-key lock is kept for cross-checking with waiters outside this
        # process-local single-flight.
        async def _rebuild() -> Any:
            lock = await self._backend.get_lock(full_key)
            async with lock:
                # Double-check after acquiring lock
                value, is_stale = await _read()
                if value is not _NO_STALE and not is_stale:
                    self._stats.hits += 1
                    return value

                self._stats.misses += 1
                result = await factory()
                if stale_on and resolved_ttl is not None:
                    to_store: Any = _stale_wrap(result, resolved_ttl)
                    physical_ttl = resolved_ttl + grace
                else:
                    to_store = result
                    physical_ttl = resolved_ttl
                serialized = self._serializer.dumps(to_store)
                await self._backend.set(full_key, serialized, ttl=physical_ttl)
                await self._emit(CacheEvent.SET, key=full_key, ttl=physical_ttl)
                return result

        timeout = await _load_slow_path_timeout()
        try:
            return await _await_shared_rebuild("gos|" + full_key, _rebuild, timeout)
        except Exception as exc:
            reason = "timeout" if isinstance(exc, TimeoutError) else "error"
            if stale_candidate is not _NO_STALE:
                logger.warning(
                    "cache_stale_served key=%s reason=%s error=%s",
                    full_key, reason, exc,
                )
                return stale_candidate
            raise

    async def close(self) -> None:
        pass  # Backend lifecycle managed by CacheManager


# ---------------------------------------------------------------------------
#  CacheManager -- central registry + factory
# ---------------------------------------------------------------------------


class CacheManager:
    """Central cache backend registry and factory.

    Pre-registers ``LocalAsyncCacheBackend`` (priority=1000) and
    ``LocalSyncCacheBackend`` (priority=1000).  When Redis/Memcache backends
    register with lower priority, they transparently take over.

    Discoverable via ``get_protocol(CacheManagerProtocol)``.
    """

    def __init__(self) -> None:
        self._async_backends: Dict[str, LocalAsyncCacheBackend] = {}
        self._sync_backends: Dict[str, LocalSyncCacheBackend] = {}
        self._event_listeners: List[CacheEventListener] = []

        # Pre-register local backends
        self._default_async = LocalAsyncCacheBackend()
        self._default_sync = LocalSyncCacheBackend()
        self._async_backends[self._default_async.name] = self._default_async
        self._sync_backends[self._default_sync.name] = self._default_sync

    def register_backend(
        self, backend: Union[CacheBackend, SyncCacheBackend]
    ) -> None:
        if hasattr(backend, "__await__") or inspect.iscoroutinefunction(getattr(backend, "get", None)):
            self._async_backends[backend.name] = backend  # type: ignore[assignment]
        else:
            # Check if it has async get method
            get_method = getattr(backend, "get", None)
            if get_method and asyncio.iscoroutinefunction(get_method):
                self._async_backends[backend.name] = backend  # type: ignore[assignment]
            else:
                self._sync_backends[backend.name] = backend  # type: ignore[assignment]
        logger.info("Registered cache backend: %s (priority=%d)", backend.name, backend.priority)
        _notify_backend_change()

    def unregister_backend(
        self, backend: Union[CacheBackend, SyncCacheBackend]
    ) -> None:
        """Unregister a backend (e.g. on circuit breaker trip).

        Removal is identity-checked: backend names are class-level
        constants (every ``ValkeyCacheBackend`` is named ``"valkey"``),
        so popping by name alone would let a stale instance's late
        circuit-breaker trip rip out a healthy replacement that a live
        reconnect just registered under the same name.
        """
        removed = False
        if self._async_backends.get(backend.name) is backend:
            del self._async_backends[backend.name]
            removed = True
        if self._sync_backends.get(backend.name) is backend:
            del self._sync_backends[backend.name]
            removed = True
        if not removed:
            logger.info(
                "Ignored unregister for superseded cache backend instance: %s",
                backend.name,
            )
            return
        logger.warning("Unregistered cache backend: %s", backend.name)
        _notify_backend_change()

    def get_async_backend(
        self, name: Optional[str] = None
    ) -> CacheBackend:
        if name is not None:
            backend = self._async_backends.get(name)
            if backend is None:
                raise KeyError(f"No async cache backend named '{name}'")
            return backend
        if not self._async_backends:
            raise RuntimeError("No async cache backends registered")
        return min(self._async_backends.values(), key=lambda b: b.priority)

    def get_sync_backend(
        self, name: Optional[str] = None
    ) -> SyncCacheBackend:
        if name is not None:
            backend = self._sync_backends.get(name)
            if backend is None:
                raise KeyError(f"No sync cache backend named '{name}'")
            return backend
        if not self._sync_backends:
            raise RuntimeError("No sync cache backends registered")
        return min(self._sync_backends.values(), key=lambda b: b.priority)

    def create_cache(self, config: CacheConfig) -> Cache:
        backend = self._default_async
        if config.max_size:
            backend = LocalAsyncCacheBackend(max_size=config.max_size)
            self._async_backends[f"local-async-{config.namespace or id(backend)}"] = backend
        return LocalCache(
            backend=backend,
            config=config,
            serializer=NullSerializer(),
            event_listeners=list(self._event_listeners),
        )

    def add_event_listener(self, listener: CacheEventListener) -> None:
        self._event_listeners.append(listener)


# ---------------------------------------------------------------------------
#  Module-level singleton
# ---------------------------------------------------------------------------

_cache_manager: Optional[CacheManager] = None


def get_cache_manager() -> CacheManager:
    """Get or create the global CacheManager singleton."""
    global _cache_manager
    if _cache_manager is None:
        _cache_manager = CacheManager()
    return _cache_manager


# ---------------------------------------------------------------------------
#  Backend upgrade tracking
# ---------------------------------------------------------------------------

_backend_generation: int = 0


def _notify_backend_change() -> None:
    """Called when a backend is registered or unregistered.

    Bumps the generation counter so that ``@cached`` functions lazily
    re-resolve their backend on the next call.
    """
    global _backend_generation
    _backend_generation += 1


# Backward compatibility alias
_notify_backend_upgrade = _notify_backend_change


# ---------------------------------------------------------------------------
#  @cached decorator
# ---------------------------------------------------------------------------


DEFAULT_MAX_DISTRIBUTED_TTL: float = 3600.0

# Shared TTL/L1-TTL pair for config-like caches (catalog/collection config,
# platform config, storage router, collection model) that must self-heal
# within a bounded cross-pod staleness window after invalidation is dropped
# by an unreliable distributed backend. ``DEFAULT_CONFIG_CACHE_TTL`` bounds
# the L2 window; ``DEFAULT_CONFIG_CACHE_L1_TTL`` tightens the L1 window so
# sibling pods converge quickly after a write.
DEFAULT_CONFIG_CACHE_TTL: float = 300.0
DEFAULT_CONFIG_CACHE_L1_TTL: float = 2.0


def cached(
    maxsize: int = 1024,
    ttl: Optional[Union[float, int]] = None,
    jitter: Optional[Union[float, int]] = None,
    backend: Optional[str] = None,
    namespace: Optional[str] = None,
    ignore: Optional[List[str]] = None,
    typed: bool = False,
    condition: Optional[Callable[[Any], bool]] = None,
    key_builder: Optional[Callable[..., str]] = None,
    distributed: bool = True,
    l1_ttl: Optional[Union[float, int]] = None,
    max_distributed_ttl: Optional[Union[float, int]] = None,
    stale_grace: Optional[Union[float, int]] = None,
) -> Callable:
    """Centralized caching decorator for sync and async functions.

    Replaces all ``@alru_cache`` / ``@lru_cache`` usage across the codebase.

    Args:
        maxsize: Maximum number of entries.
        ttl: Time-to-live in seconds. ``None`` = no expiration for local caches;
            for distributed caches, capped by ``max_distributed_ttl`` to prevent
            unbounded staleness when L2 (Valkey) is unreliable (#2328).
        jitter: Random TTL variance in seconds (prevents thundering herd on expiry).
        backend: Named backend or ``None`` for default local memory.
        namespace: Cache namespace prefix for key isolation.
        ignore: Parameter names to exclude from cache key.
        typed: Cache differently based on argument types.
        condition: Post-condition -- only cache if ``condition(result)`` is True.
        key_builder: Custom key builder ``(func, *args, **kwargs) -> str``.
        distributed: If ``False``, always use local in-memory backend regardless
            of registered distributed backends. Use for non-serializable return
            types (driver instances, singletons).
        l1_ttl: Override the L1 (in-process) TTL cap when a tiered backend is in
            play. Defaults to ``CachePluginConfig.l1_default_ttl_seconds``
            (30s), read live so a config PATCH applies without a restart.
            Set a small value (e.g. 2s) for correctness-critical caches where
            post-PUT staleness across sibling Cloud Run processes must converge
            quickly (#930). Ignored when ``distributed=False`` or when no
            distributed backend is registered.
        max_distributed_ttl: Maximum TTL for distributed (L2) tier when
            ``ttl=None``. Prevents unbounded staleness when Valkey is unreliable
            and invalidations are dropped (#2328). Defaults to 3600s (1 hour).
            Set higher for slowly-changing metadata (e.g., tiles config) or to
            ``float("inf")`` to disable the cap. Ignored for local-only caches.
        stale_grace: Grace window (seconds) a value is kept physically alive
            past its logical ``ttl`` so a rebuild that exceeds
            ``CachePluginConfig.slow_path_timeout_seconds`` (default 30s) or
            raises can serve the stale value instead of propagating (#2902).
            Defaults to ``DEFAULT_STALE_GRACE_SECONDS`` (300s). ``0`` disables
            stale serving. Ignored when ``ttl=None`` (no logical expiry, so
            nothing can go stale).

    The decorated function gets these methods:
        - ``.cache_invalidate(*args, **kwargs)`` -- invalidate specific entry
        - ``.cache_clear()`` -- clear all entries for this namespace
        - ``.cache_info()`` -> ``CacheStats``
    """

    def decorator(func: Callable) -> Callable:
        sig = inspect.signature(func)
        is_async = inspect.iscoroutinefunction(func)
        func_qualname = func.__qualname__

        # Determine ignored params: explicit + CacheIgnore[T] annotations
        ignored_params: Set[str] = set(ignore or [])
        try:
            hints = get_type_hints(func, include_extras=True)
            for param_name, annotation in hints.items():
                if param_name == "return":
                    continue
                if _has_cache_ignore(annotation):
                    ignored_params.add(param_name)
        except Exception:
            pass

        # Lazy backend resolution — deferred to first cache access so that
        # CacheModule has time to register Valkey during its lifespan.
        _is_named_backend = backend is not None
        _backend: Optional[Any] = None
        _backend_has_lock: bool = True
        _backend_gen: int = -1  # tracks _backend_generation at resolution time

        def _resolve_backend() -> None:
            nonlocal _backend, _backend_has_lock, _backend_gen
            if _is_named_backend:
                _backend = get_cache_manager().get_async_backend(backend)
            elif distributed:
                local = LocalAsyncCacheBackend(max_size=maxsize)
                try:
                    distributed_backend = get_cache_manager().get_async_backend()
                    if distributed_backend.priority < 1000:
                        # Tiered: L1 (local) + L2 (distributed)
                        _backend = TieredAsyncBackend(
                            [local, distributed_backend],
                            l1_ttl_cap=(
                                float(l1_ttl) if l1_ttl is not None else None
                            ),
                        )
                    else:
                        # No distributed backend registered, use local only
                        _backend = local
                except RuntimeError:
                    _backend = local
            else:
                _backend = LocalAsyncCacheBackend(max_size=maxsize)  # forced local
            _backend_has_lock = isinstance(_backend, LockableCacheBackend)
            _backend_gen = _backend_generation
            logger.debug(
                "cache backend resolved: fn=%s backend=%s distributed=%s",
                func_qualname, getattr(_backend, "name", type(_backend).__name__), distributed,
            )

        _sync_backend = LocalSyncCacheBackend(max_size=maxsize) if not is_async else None
        # Fallback per-key locks for external backends that don't implement get_lock
        _fallback_locks: Dict[str, asyncio.Lock] = {}

        ns = namespace or func_qualname

        _effective_max_distributed_ttl = (
            float(max_distributed_ttl)
            if max_distributed_ttl is not None
            else DEFAULT_MAX_DISTRIBUTED_TTL
        )
        if ttl is None and distributed and max_distributed_ttl != float("inf"):
            logger.debug(
                "@cached(%s): distributed=True with ttl=None — applying max_distributed_ttl=%.0fs (#2328)",
                func_qualname, _effective_max_distributed_ttl,
            )

        _grace = (
            DEFAULT_STALE_GRACE_SECONDS if stale_grace is None else float(stale_grace)
        )

        # Cross-revision key compatibility (#2902): Valkey is shared across
        # rolling-deploy revisions. An old-revision process has no unwrap
        # logic for the stale-wrapped envelope, so its fast path would
        # return the wrapper dict AS the cached value — silently corrupting
        # every cached endpoint until the entry expires. Storing
        # stale-wrapped entries under a version-prefixed key means old code
        # simply misses (cold-rebuilds once, absorbed by the bounded slow
        # path) instead of misreading. Grace-disabled functions keep
        # today's unprefixed keys (those entries are never wrapped, so they
        # stay cross-revision safe as-is).
        _key_prefix = "sv1|" if _grace > 0 else ""

        def _build_key(args: tuple, kwargs: dict) -> str:
            if key_builder is not None:
                return _key_prefix + key_builder(func, *args, **kwargs)
            return _key_prefix + _make_cache_key(
                ns, args, kwargs, sig, ignored_params, typed
            )

        def _resolve_ttl() -> Optional[float]:
            if ttl is None:
                if distributed and max_distributed_ttl != float("inf"):
                    return _effective_max_distributed_ttl
                return None
            base = float(ttl)
            if jitter:
                base += random.uniform(-float(jitter), float(jitter))
                base = max(0.1, base)
            return base

        if is_async:

            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                nonlocal _backend, _backend_gen
                # Lazy resolve on first call; re-resolve on backend upgrade
                if _backend is None or (
                    distributed
                    and not _is_named_backend
                    and _backend_gen != _backend_generation
                ):
                    _resolve_backend()
                assert _backend is not None

                cache_key = _build_key(args, kwargs)
                stale_candidate: Any = _NO_STALE

                # Fast path
                raw = await _backend.get(cache_key)
                if raw is not None:
                    value, is_stale = _stale_unwrap(raw) if _grace > 0 else (raw, False)
                    if not is_stale:
                        # Re-validate against ``condition`` so stale entries written
                        # before the condition was added (or by a code path that
                        # bypassed it) cannot keep being served forever.  Drop the
                        # entry across all tiers so the next read refetches.
                        if condition is None or condition(value):
                            _backend._stats.hits += 1
                            return value  # NullSerializer for local; msgpack for Valkey
                        await _backend.clear(key=cache_key)
                    else:
                        stale_candidate = value

                # Stampede protection: one shared rebuild task per key (see
                # _await_shared_rebuild), waited on for at most
                # slow_path_timeout_seconds. A caller abandoning the wait (the
                # budget, or a client disconnect cancelling the request) never
                # cancels the in-flight rebuild — cancelled mid-query DB work
                # poisons asyncpg connections (#2900). On timeout or a rebuild
                # exception, fall back to a still-grace-window stale value.
                async def _rebuild() -> Any:
                    assert _backend is not None
                    if _backend_has_lock:
                        lock = await _backend.get_lock(cache_key)
                    else:
                        if cache_key not in _fallback_locks:
                            _fallback_locks[cache_key] = asyncio.Lock()
                        lock = _fallback_locks[cache_key]
                    async with lock:
                        raw = await _backend.get(cache_key)
                        if raw is not None:
                            value, is_stale = (
                                _stale_unwrap(raw) if _grace > 0 else (raw, False)
                            )
                            if not is_stale:
                                if condition is None or condition(value):
                                    _backend._stats.hits += 1
                                    return value
                                await _backend.clear(key=cache_key)

                        _backend._stats.misses += 1
                        result = await func(*args, **kwargs)

                        if condition is not None and not condition(result):
                            return result

                        resolved = _resolve_ttl()
                        if _grace > 0 and resolved is not None:
                            to_store = _stale_wrap(result, resolved)
                            await _backend.set(
                                cache_key, to_store, ttl=resolved + _grace
                            )
                        else:
                            await _backend.set(cache_key, result, ttl=resolved)
                        return result

                timeout = await _load_slow_path_timeout()
                try:
                    return await _await_shared_rebuild(
                        "dec|" + cache_key, _rebuild, timeout
                    )
                except Exception as exc:
                    reason = "timeout" if isinstance(exc, TimeoutError) else "error"
                    if stale_candidate is not _NO_STALE:
                        logger.warning(
                            "cache_stale_served key=%s reason=%s error=%s",
                            cache_key, reason, exc,
                        )
                        return stale_candidate
                    raise

            def sync_cache_invalidate_impl(*args: Any, **kwargs: Any) -> None:
                """Sync invalidation -- works in both sync and async contexts."""
                nonlocal _backend
                if _backend is None:
                    # Backend not yet initialized (no GET has run yet on this
                    # function).  Resolve it now so a PUT that precedes the first
                    # GET still writes a tombstone to the distributed cache and
                    # prevents stale reads on other pods.
                    _resolve_backend()
                if _backend is None:
                    return
                cache_key = _build_key(args, kwargs)
                if isinstance(_backend, LocalAsyncCacheBackend):
                    _backend._drop_entry(cache_key)
                else:
                    # Distributed backend: schedule async clear (fire-and-forget)
                    try:
                        loop = asyncio.get_running_loop()
                        _track(loop.create_task(_backend.clear(key=cache_key)))
                    except RuntimeError:
                        pass

            def sync_cache_clear_impl() -> None:
                """Sync clear -- works in both sync and async contexts."""
                if _backend is None:
                    return
                if isinstance(_backend, LocalAsyncCacheBackend):
                    _local_clear_all_nolock(_backend)
                else:
                    # Distributed backend: schedule async namespace clear
                    try:
                        loop = asyncio.get_running_loop()
                        _track(loop.create_task(_backend.clear(namespace=_key_prefix + ns)))
                    except RuntimeError:
                        pass

            def sync_cache_clear_prefix_impl(sub_namespace: str) -> None:
                """Drop all entries whose key starts with ``sub_namespace + "|"``.

                ``@cached`` functions build keys as ``"{ns}|{arg1}|{arg2}|..."``
                (version-prefixed with ``sv1|`` when stale-grace is active,
                see ``_key_prefix`` above) where ``ns`` is the decorator's
                ``namespace`` parameter.  A *sub-namespace* is a prefix of
                that key that identifies a subset of entries — for example
                ``"collection_config|'mycat'|'mycoll'"`` matches every
                class-key entry for that (catalog, collection) pair.

                Local backend: scans the in-process store directly using the
                ``|`` separator so the match is exact.

                Tiered/distributed backend: clears L2 (Valkey) by scheduling
                ``clear(namespace=sub_namespace)``; Valkey scans with the
                pattern ``{prefix}{sub_namespace}|*`` which matches the same
                entries.  The L1 tier of a tiered backend is NOT cleared
                synchronously here — that tier's ``l1_ttl`` cap bounds the
                residual staleness window (≤2 s for correctness-critical
                caches such as ``_collection_config_cache``).
                """
                if _backend is None:
                    return
                versioned_sub_ns = _key_prefix + sub_namespace
                key_prefix = f"{versioned_sub_ns}|"
                if isinstance(_backend, LocalAsyncCacheBackend):
                    # Direct in-process scan — no separator ambiguity.
                    to_delete = [k for k in list(_backend._store.keys()) if k.startswith(key_prefix)]
                    for k in to_delete:
                        _backend._drop_entry(k)
                else:
                    # Distributed or tiered: Valkey's clear(namespace=X) scans
                    # "ds:{X}|*" which matches the @cached key format.
                    # For TieredAsyncBackend the local L1 tier is not reached
                    # by this clear; residual L1 entries expire within l1_ttl.
                    try:
                        loop = asyncio.get_running_loop()
                        _track(loop.create_task(_backend.clear(namespace=versioned_sub_ns)))
                    except RuntimeError:
                        pass

            def cache_info() -> CacheStats:
                if _backend is None:
                    return CacheStats()
                return CacheStats(
                    hits=_backend._stats.hits,
                    misses=_backend._stats.misses,
                    size=len(getattr(_backend, "_store", {})),
                    maxsize=maxsize,
                    evictions=_backend._stats.evictions,
                )

            setattr(async_wrapper, "cache_invalidate", sync_cache_invalidate_impl)
            setattr(async_wrapper, "cache_clear", sync_cache_clear_impl)
            setattr(async_wrapper, "cache_clear_prefix", sync_cache_clear_prefix_impl)
            setattr(async_wrapper, "cache_info", cache_info)
            setattr(async_wrapper, "cache_namespace", ns)
            return async_wrapper

        else:
            assert _sync_backend is not None

            @functools.wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                cache_key = _build_key(args, kwargs)

                raw = _sync_backend.get(cache_key)
                if raw is not None:
                    if condition is None or condition(raw):
                        _sync_backend._stats.hits += 1
                        return raw
                    _sync_backend.clear(key=cache_key)

                _sync_backend._stats.misses += 1
                result = func(*args, **kwargs)

                if condition is not None and not condition(result):
                    return result

                resolved = _resolve_ttl()
                _sync_backend.set(cache_key, result, ttl=resolved)
                return result

            def sync_cache_invalidate(*args: Any, **kwargs: Any) -> None:
                cache_key = _build_key(args, kwargs)
                _sync_backend.clear(key=cache_key)

            def sync_cache_clear() -> None:
                _sync_backend.clear(namespace=ns)

            def cache_info() -> CacheStats:
                return CacheStats(
                    hits=_sync_backend._stats.hits,
                    misses=_sync_backend._stats.misses,
                    size=len(_sync_backend._store),
                    maxsize=maxsize,
                    evictions=_sync_backend._stats.evictions,
                )

            setattr(sync_wrapper, "cache_invalidate", sync_cache_invalidate)
            setattr(sync_wrapper, "cache_clear", sync_cache_clear)
            setattr(sync_wrapper, "cache_info", cache_info)
            setattr(sync_wrapper, "cache_namespace", ns)
            return sync_wrapper

    return decorator


# ---------------------------------------------------------------------------
#  Module-level helpers for cached functions (typed accessors)
# ---------------------------------------------------------------------------

def cache_clear(fn: Callable[..., Any]) -> None:
    """Clear all cache entries for a @cached-decorated function."""
    clearer = getattr(fn, "cache_clear", None)
    if clearer is None:
        raise TypeError(f"{fn!r} is not a @cached-decorated function")
    clearer()


def cache_invalidate(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
    """Invalidate a specific cache entry for a @cached-decorated function."""
    inv = getattr(fn, "cache_invalidate", None)
    if inv is None:
        raise TypeError(f"{fn!r} is not a @cached-decorated function")
    inv(*args, **kwargs)
