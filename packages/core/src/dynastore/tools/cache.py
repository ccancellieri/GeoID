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
import functools
import hashlib
import inspect
import json
import logging
import random
import threading
import time
from datetime import timedelta
from typing import (
    Annotated,
    Any,
    Awaitable,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Set,
    Tuple,
    Type,
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
    CacheManagerProtocol,
    CacheSerializer,
    CacheStats,
    SyncCacheBackend,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


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
        except ImportError:
            raise ImportError(
                "msgpack is required for MsgPackSerializer. "
                "Install it with: pip install msgpack"
            )

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
    __slots__ = ("value", "expires_at", "priority")

    def __init__(
        self,
        value: Any,
        expires_at: Optional[float],
        priority: int = CacheItemPriority.NORMAL,
    ):
        self.value = value
        self.expires_at = expires_at
        self.priority = priority

    def is_expired(self) -> bool:
        return self.expires_at is not None and time.monotonic() > self.expires_at


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

    def __init__(self, max_size: int = 4096) -> None:
        self._store: collections.OrderedDict[str, _CacheEntry] = collections.OrderedDict()
        self._max_size = max_size
        self._locks: Dict[str, asyncio.Lock] = {}
        self._stats = CacheStats(maxsize=max_size)

    @property
    def name(self) -> str:
        return "local-async"

    @property
    def priority(self) -> int:
        return 1000

    async def get(self, key: str) -> Optional[bytes]:
        entry = self._store.get(key)
        if entry is None:
            return None
        if entry.is_expired():
            del self._store[key]
            return None
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
        has_key = key in self._store
        if exist is True and not has_key:
            return False
        if exist is False and has_key:
            return False

        expires_at = (time.monotonic() + ttl) if ttl is not None else None
        entry = _CacheEntry(value=value, expires_at=expires_at)

        if has_key:
            self._store[key] = entry
            self._store.move_to_end(key)
        else:
            self._evict_if_needed()
            self._store[key] = entry

        self._stats.size = len(self._store)
        return True

    async def clear(
        self,
        *,
        key: Optional[str] = None,
        namespace: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> bool:
        if key is not None:
            if key in self._store:
                del self._store[key]
                self._stats.size = len(self._store)
                return True
            return False

        if namespace is not None:
            prefix = namespace + ":"
            to_delete = [k for k in self._store if k.startswith(prefix)]
            for k in to_delete:
                del self._store[k]
            self._stats.size = len(self._store)
            return len(to_delete) > 0

        # tags: not supported in local backend (no tag index)
        if tags is not None:
            return False

        # Clear everything
        had_items = len(self._store) > 0
        self._store.clear()
        self._locks.clear()
        self._stats.size = 0
        return had_items

    async def exists(self, key: str) -> bool:
        entry = self._store.get(key)
        if entry is None:
            return False
        if entry.is_expired():
            del self._store[key]
            return False
        return True

    async def close(self) -> None:
        self._store.clear()
        self._locks.clear()

    def _evict_if_needed(self) -> None:
        while len(self._store) >= self._max_size:
            # Find lowest-priority entry to evict (skip NEVER_REMOVE)
            evict_key = None
            for k, entry in self._store.items():
                if entry.priority < CacheItemPriority.NEVER_REMOVE:
                    evict_key = k
                    break
            if evict_key is None:
                # All entries are NEVER_REMOVE, evict oldest anyway
                evict_key = next(iter(self._store))
            del self._store[evict_key]
            self._stats.evictions += 1

    async def get_lock(self, key: str) -> asyncio.Lock:
        # asyncio is single-threaded and cooperative — no concurrent access to
        # _locks between suspension points, so no global lock is needed here.
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]


# ---------------------------------------------------------------------------
#  TieredAsyncBackend
# ---------------------------------------------------------------------------


class TieredAsyncBackend:
    """Chain multiple cache backends in priority order (L1, L2, L3...).

    Read path: tries each tier in order, populating upstream tiers on miss.
    Write path: writes to all tiers.
    Delete/clear: deletes from all tiers.

    Implements get_lock by delegating to the first tier that supports it.
    """

    def __init__(self, backends: List[CacheBackend]) -> None:
        """Initialize with an ordered list of backends (best to worst)."""
        if not backends:
            raise ValueError("TieredAsyncBackend requires at least one backend")
        self._backends = backends
        self._name = "-".join(b.name for b in backends)
        self._priority = min(b.priority for b in backends)
        # ``cached()`` increments _stats.{hits,misses,size} unconditionally
        # on every call regardless of the backend type, so the wrapper
        # must expose the same shape as the leaf backends or it AttributeErrors
        # on the first call.  Stats here aggregate across tiers — leaves
        # keep their own per-tier counters for finer-grained inspection.
        self._stats = CacheStats()

    @property
    def name(self) -> str:
        return f"tiered({self._name})"

    @property
    def priority(self) -> int:
        return self._priority

    async def get(self, key: str) -> Optional[bytes]:
        """Try each tier in order, populating upstream on miss."""
        for i, backend in enumerate(self._backends):
            value = await backend.get(key)
            if value is not None:
                # Populate all upstream tiers (0..i-1) with miss TTL
                for j in range(i):
                    await self._backends[j].set(key, value, ttl=60)  # L1 short TTL
                return value
        return None

    async def set(
        self,
        key: str,
        value: bytes,
        *,
        ttl: Optional[float] = None,
        exist: Optional[bool] = None,
    ) -> bool:
        """Write to all tiers (with tier-specific TTLs)."""
        # L1 gets short TTL, L2+ get full TTL
        results = []
        for i, backend in enumerate(self._backends):
            tier_ttl = min(ttl or 300, 60) if i == 0 else ttl
            result = await backend.set(key, value, ttl=tier_ttl, exist=exist)
            results.append(result)
        return all(results)

    async def clear(
        self,
        *,
        key: Optional[str] = None,
        namespace: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> bool:
        """Clear from all tiers."""
        results = []
        for backend in self._backends:
            result = await backend.clear(key=key, namespace=namespace, tags=tags)
            results.append(result)
        return any(results)

    async def exists(self, key: str) -> bool:
        """Check any tier."""
        for backend in self._backends:
            if await backend.exists(key):
                return True
        return False

    async def close(self) -> None:
        """Close all backends."""
        for backend in self._backends:
            await backend.close()

    async def get_lock(self, key: str) -> asyncio.Lock:
        """Delegate to first tier that implements LockableCacheBackend."""
        for backend in self._backends:
            if isinstance(backend, LockableCacheBackend):
                return await backend.get_lock(key)
        # Fallback: create a new lock (caller's decorator will cache it)
        return asyncio.Lock()


# ---------------------------------------------------------------------------
#  LocalSyncCacheBackend
# ---------------------------------------------------------------------------


class LocalSyncCacheBackend:
    """In-memory synchronous cache backend using OrderedDict with LRU eviction.

    - Same semantics as ``LocalAsyncCacheBackend`` but for sync contexts
    - priority = 1000
    """

    def __init__(self, max_size: int = 4096) -> None:
        self._store: collections.OrderedDict[str, _CacheEntry] = collections.OrderedDict()
        self._max_size = max_size
        self._lock = threading.Lock()
        self._stats = CacheStats(maxsize=max_size)

    @property
    def name(self) -> str:
        return "local-sync"

    @property
    def priority(self) -> int:
        return 1000

    def get(self, key: str) -> Optional[bytes]:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            if entry.is_expired():
                del self._store[key]
                return None
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
            has_key = key in self._store
            if exist is True and not has_key:
                return False
            if exist is False and has_key:
                return False

            expires_at = (time.monotonic() + ttl) if ttl is not None else None
            entry = _CacheEntry(value=value, expires_at=expires_at)

            if has_key:
                self._store[key] = entry
                self._store.move_to_end(key)
            else:
                self._evict_if_needed()
                self._store[key] = entry

            self._stats.size = len(self._store)
            return True

    def clear(
        self,
        *,
        key: Optional[str] = None,
        namespace: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> bool:
        with self._lock:
            if key is not None:
                if key in self._store:
                    del self._store[key]
                    self._stats.size = len(self._store)
                    return True
                return False

            if namespace is not None:
                prefix = namespace + ":"
                to_delete = [k for k in self._store if k.startswith(prefix)]
                for k in to_delete:
                    del self._store[k]
                self._stats.size = len(self._store)
                return len(to_delete) > 0

            if tags is not None:
                return False

            had_items = len(self._store) > 0
            self._store.clear()
            self._stats.size = 0
            return had_items

    def exists(self, key: str) -> bool:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return False
            if entry.is_expired():
                del self._store[key]
                return False
            return True

    def close(self) -> None:
        with self._lock:
            self._store.clear()

    def _evict_if_needed(self) -> None:
        while len(self._store) >= self._max_size:
            evict_key = None
            for k, entry in self._store.items():
                if entry.priority < CacheItemPriority.NEVER_REMOVE:
                    evict_key = k
                    break
            if evict_key is None:
                evict_key = next(iter(self._store))
            del self._store[evict_key]
            self._stats.evictions += 1


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
    ) -> Any:
        full_key = self._full_key(key, namespace)

        # Fast path: value in cache
        raw = await self._backend.get(full_key)
        if raw is not None:
            self._stats.hits += 1
            return self._serializer.loads(raw)

        # Slow path: acquire per-key lock for stampede protection
        lock = await self._backend.get_lock(full_key)
        async with lock:
            # Double-check after acquiring lock
            raw = await self._backend.get(full_key)
            if raw is not None:
                self._stats.hits += 1
                return self._serializer.loads(raw)

            self._stats.misses += 1
            value = await factory()
            resolved_ttl = self._resolve_ttl(ttl)
            serialized = self._serializer.dumps(value)
            await self._backend.set(full_key, serialized, ttl=resolved_ttl)
            await self._emit(CacheEvent.SET, key=full_key, ttl=resolved_ttl)
            return value

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
        """Unregister a backend (e.g. on circuit breaker trip)."""
        self._async_backends.pop(backend.name, None)
        self._sync_backends.pop(backend.name, None)
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


def _register_cache_manager_as_plugin() -> None:
    """Register CacheManager with the plugin discovery system.

    Called lazily on first ``@cached`` usage to avoid circular imports.
    """
    try:
        from dynastore.tools.discovery import register_plugin
        manager = get_cache_manager()
        register_plugin(manager)
    except ImportError:
        pass


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
) -> Callable:
    """Centralized caching decorator for sync and async functions.

    Replaces all ``@alru_cache`` / ``@lru_cache`` usage across the codebase.

    Args:
        maxsize: Maximum number of entries.
        ttl: Time-to-live in seconds. ``None`` = no expiration.
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

        # Detect instance method (first param is 'self')
        params = list(sig.parameters.keys())
        is_method = len(params) > 0 and params[0] == "self"

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
                        _backend = TieredAsyncBackend([local, distributed_backend])
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

        def _build_key(args: tuple, kwargs: dict) -> str:
            if key_builder is not None:
                return key_builder(func, *args, **kwargs)
            return _make_cache_key(
                ns, args, kwargs, sig, ignored_params, typed
            )

        def _resolve_ttl() -> Optional[float]:
            if ttl is None:
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

                # Fast path
                raw = await _backend.get(cache_key)
                if raw is not None:
                    # Re-validate against ``condition`` so stale entries written
                    # before the condition was added (or by a code path that
                    # bypassed it) cannot keep being served forever.  Drop the
                    # entry across all tiers so the next read refetches.
                    if condition is None or condition(raw):
                        _backend._stats.hits += 1
                        return raw  # NullSerializer for local; msgpack for Valkey
                    await _backend.clear(key=cache_key)

                # Stampede protection
                if _backend_has_lock:
                    lock = await _backend.get_lock(cache_key)
                else:
                    if cache_key not in _fallback_locks:
                        _fallback_locks[cache_key] = asyncio.Lock()
                    lock = _fallback_locks[cache_key]
                async with lock:
                    raw = await _backend.get(cache_key)
                    if raw is not None:
                        if condition is None or condition(raw):
                            _backend._stats.hits += 1
                            return raw
                        await _backend.clear(key=cache_key)

                    _backend._stats.misses += 1
                    result = await func(*args, **kwargs)

                    if condition is not None and not condition(result):
                        return result

                    resolved = _resolve_ttl()
                    await _backend.set(cache_key, result, ttl=resolved)
                    return result

            def sync_cache_invalidate_impl(*args: Any, **kwargs: Any) -> None:
                """Sync invalidation -- works in both sync and async contexts."""
                if _backend is None:
                    return
                cache_key = _build_key(args, kwargs)
                if isinstance(_backend, LocalAsyncCacheBackend):
                    if cache_key in _backend._store:
                        del _backend._store[cache_key]
                        _backend._stats.size = len(_backend._store)
                else:
                    # Distributed backend: schedule async clear (fire-and-forget)
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(_backend.clear(key=cache_key))
                    except RuntimeError:
                        pass

            def sync_cache_clear_impl() -> None:
                """Sync clear -- works in both sync and async contexts."""
                if _backend is None:
                    return
                if isinstance(_backend, LocalAsyncCacheBackend):
                    _backend._store.clear()
                    _backend._locks.clear()
                    _backend._stats.size = 0
                else:
                    # Distributed backend: schedule async namespace clear
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(_backend.clear(namespace=ns))
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
