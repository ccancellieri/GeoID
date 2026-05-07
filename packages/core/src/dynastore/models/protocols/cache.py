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
Cache framework protocol definitions and Pydantic configuration models.

Two-layer architecture inspired by .NET IDistributedCache/IMemoryCache split:
- CacheBackend: low-level (bytes), for backend implementors (Redis, Memcache, Neo4j, etc.)
- Cache: high-level (typed values), for application code

Decorator (@cached) is built on top, not part of the protocol.
"""

from __future__ import annotations

import enum
import logging
from dataclasses import dataclass, field
from datetime import timedelta
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Protocol,
    Union,
    runtime_checkable,
)

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
#  Enums
# ---------------------------------------------------------------------------


class EvictionPolicy(str, enum.Enum):
    """Cache eviction strategies."""
    LRU = "lru"       # Least recently used (default, most common)
    LFU = "lfu"       # Least frequently used
    FIFO = "fifo"     # First in, first out
    RANDOM = "random"  # Random eviction


class CacheItemPriority(int, enum.Enum):
    """Eviction priority for cache entries. Lower priority items are evicted first."""
    LOW = 0            # Evicted first under memory pressure
    NORMAL = 50        # Default priority
    HIGH = 100         # Evicted last
    NEVER_REMOVE = 255  # Pinned entry (critical configs, etc.)


class CacheEvent(str, enum.Enum):
    """Cache operation event types for monitoring and hooks."""
    GET_HIT = "get_hit"
    GET_MISS = "get_miss"
    SET = "set"
    DELETE = "delete"
    CLEAR = "clear"
    EVICT = "evict"
    EXPIRE = "expire"
    ERROR = "error"


# ---------------------------------------------------------------------------
#  Event data
# ---------------------------------------------------------------------------


class CacheEventData(BaseModel):
    """Structured event emitted by the cache on each operation."""
    model_config = {"arbitrary_types_allowed": True}

    event: CacheEvent
    key: Optional[str] = None
    namespace: Optional[str] = None
    ttl: Optional[float] = None
    elapsed_ms: Optional[float] = None
    backend_name: Optional[str] = None


CacheEventListener = Callable[[CacheEventData], Union[Awaitable[None], None]]


# ---------------------------------------------------------------------------
#  Pydantic configuration models
# ---------------------------------------------------------------------------


class CacheConfig(BaseModel):
    """Constructor configuration for a cache instance.

    Passed to ``CacheManager.create_cache()`` or used internally by the
    ``@cached`` decorator to configure per-function caching.
    """
    namespace: str = ""
    default_ttl: Optional[Union[float, int]] = None
    max_size: Optional[int] = None
    eviction_policy: EvictionPolicy = EvictionPolicy.LRU
    enable: bool = True
    suppress_errors: bool = False


# ---------------------------------------------------------------------------
#  Cache statistics
# ---------------------------------------------------------------------------


@dataclass
class CacheStats:
    """Runtime cache statistics returned by ``cache_info()``."""
    hits: int = 0
    misses: int = 0
    size: int = 0
    maxsize: Optional[int] = None
    evictions: int = 0

    @property
    def currsize(self) -> int:
        """Alias for ``size`` — compatible with ``alru_cache.cache_info()``."""
        return self.size


# ---------------------------------------------------------------------------
#  Serialization protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class CacheSerializer(Protocol):
    """Serialization is orthogonal to storage.

    Backends store raw bytes; the ``Cache`` wrapper handles serialization.
    Implementations:
    - ``JsonSerializer``   -- safe, inspectable, default for distributed
    - ``MsgPackSerializer`` -- compact binary, fast
    - ``NullSerializer``   -- passthrough for in-memory (no bytes conversion)
    - ``PydanticSerializer`` -- Pydantic model_dump_json / model_validate_json
    """

    def dumps(self, value: Any) -> bytes: ...
    def loads(self, data: bytes) -> Any: ...


# ---------------------------------------------------------------------------
#  Low-level backend protocols (bytes, for implementors)
# ---------------------------------------------------------------------------


@runtime_checkable
class CacheBackend(Protocol):
    """Minimal async cache backend -- what implementors must provide.

    Operates on raw bytes.  Serialization is handled by the Cache wrapper.
    Inspired by .NET ``IDistributedCache`` and dogpile ``CacheBackend``.

    Unified ``clear()`` handles all granularities:
    - ``clear()``                           -- clear everything
    - ``clear(namespace="catalog_config")`` -- clear all keys in namespace
    - ``clear(key="specific:key")``         -- delete a single key
    - ``clear(tags=["user", "session"])``   -- tag-based invalidation
    """

    @property
    def name(self) -> str:
        """Unique backend identifier (e.g. ``"local"``, ``"redis"``)."""
        ...

    @property
    def priority(self) -> int:
        """Lower value = preferred.  Local memory = 1000, Redis = 100."""
        ...

    async def get(self, key: str) -> Optional[bytes]:
        """Retrieve a cached value. Returns ``None`` on miss."""
        ...

    async def set(
        self,
        key: str,
        value: bytes,
        *,
        ttl: Optional[float] = None,
        exist: Optional[bool] = None,
    ) -> bool:
        """Store a value.

        Args:
            key: Cache key.
            value: Serialized value.
            ttl: Time-to-live in seconds.  ``None`` = no expiration.
            exist: Conditional write semantics (Redis NX/XX):
                ``None``  = always set (default)
                ``False`` = set only if key does NOT exist (NX / add)
                ``True``  = set only if key DOES exist (XX / replace)

        Returns:
            ``True`` if the value was stored, ``False`` otherwise.
        """
        ...

    async def clear(
        self,
        *,
        key: Optional[str] = None,
        namespace: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> bool:
        """Unified clear/delete operation.

        With no arguments: clears the entire backend.
        With ``key``: deletes a single entry.
        With ``namespace``: deletes all entries whose key starts with the namespace prefix.
        With ``tags``: deletes all entries associated with the given tags.

        Returns:
            ``True`` if any entries were removed.
        """
        ...

    async def exists(self, key: str) -> bool:
        """Check whether a key exists without fetching the value."""
        ...

    async def close(self) -> None:
        """Release backend resources (connection pools, etc.)."""
        ...


@runtime_checkable
class SyncCacheBackend(Protocol):
    """Synchronous variant for non-async contexts (e.g. ``get_protocol()``)."""

    @property
    def name(self) -> str: ...

    @property
    def priority(self) -> int: ...

    def get(self, key: str) -> Optional[bytes]: ...

    def set(
        self,
        key: str,
        value: bytes,
        *,
        ttl: Optional[float] = None,
        exist: Optional[bool] = None,
    ) -> bool: ...

    def clear(
        self,
        *,
        key: Optional[str] = None,
        namespace: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> bool: ...

    def exists(self, key: str) -> bool: ...

    def close(self) -> None: ...


# ---------------------------------------------------------------------------
#  Optional extension protocols (for capable backends)
# ---------------------------------------------------------------------------


@runtime_checkable
class BatchCacheBackend(CacheBackend, Protocol):
    """For backends supporting MGET/MSET (Redis, Memcache)."""

    async def get_many(self, keys: List[str]) -> List[Optional[bytes]]: ...

    async def set_many(
        self, mapping: Dict[str, bytes], *, ttl: Optional[float] = None
    ) -> bool: ...


@runtime_checkable
class LockingCacheBackend(CacheBackend, Protocol):
    """For backends supporting distributed locks (Redis ``SET NX``)."""

    async def acquire_lock(self, key: str, token: str, ttl: float) -> bool: ...
    async def release_lock(self, key: str, token: str) -> bool: ...


@runtime_checkable
class TaggableCacheBackend(CacheBackend, Protocol):
    """For backends supporting tag-based group invalidation."""

    async def set_with_tags(
        self,
        key: str,
        value: bytes,
        tags: List[str],
        *,
        ttl: Optional[float] = None,
    ) -> bool: ...

    async def clear_tags(self, *tags: str) -> bool: ...


# ---------------------------------------------------------------------------
#  High-level Cache protocol (typed values, for application code)
# ---------------------------------------------------------------------------


@runtime_checkable
class Cache(Protocol):
    """Full-featured cache -- wraps backend with serialization, namespacing, events.

    This is what application code depends on.  Backends are implementation details.
    Inspired by aiocache ``BaseCache``, cashews ``Cache``, dogpile ``CacheRegion``,
    Spring ``Cache``, and .NET ``IDistributedCache``.
    """

    async def get(
        self,
        key: str,
        *,
        default: Any = None,
        namespace: Optional[str] = None,
    ) -> Any:
        """Retrieve a typed value.  Returns ``default`` on miss."""
        ...

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
        """Store a typed value with full options."""
        ...

    async def clear(
        self,
        *,
        key: Optional[str] = None,
        namespace: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> bool:
        """Unified clear/delete. See ``CacheBackend.clear()`` for semantics."""
        ...

    async def exists(
        self,
        key: str,
        *,
        namespace: Optional[str] = None,
    ) -> bool: ...

    async def get_or_set(
        self,
        key: str,
        factory: Callable[[], Awaitable[Any]],
        *,
        ttl: Optional[Union[timedelta, float]] = None,
        namespace: Optional[str] = None,
    ) -> Any:
        """Stampede-safe get-or-create.

        If the key exists, return cached value.  Otherwise, acquire a lock,
        call ``factory()`` exactly once, cache the result, and return it.
        Other concurrent callers wait for the result.

        Inspired by dogpile ``get_or_create``, .NET ``GetOrCreate``,
        and cashews ``get_or_set``.
        """
        ...

    async def close(self) -> None:
        """Release resources."""
        ...


# ---------------------------------------------------------------------------
#  CacheManager protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class CacheManagerProtocol(Protocol):
    """Central cache backend registry and factory.

    Discoverable via ``get_protocol(CacheManagerProtocol)``.
    """

    def register_backend(
        self, backend: Union[CacheBackend, SyncCacheBackend]
    ) -> None:
        """Register a cache backend (async or sync)."""
        ...

    def get_async_backend(
        self, name: Optional[str] = None
    ) -> CacheBackend:
        """Get an async backend by name.  ``None`` = highest-priority."""
        ...

    def get_sync_backend(
        self, name: Optional[str] = None
    ) -> SyncCacheBackend:
        """Get a sync backend by name.  ``None`` = highest-priority."""
        ...

    def create_cache(self, config: CacheConfig) -> Cache:
        """Factory: create a ``Cache`` wrapping the appropriate backend."""
        ...

    def add_event_listener(self, listener: CacheEventListener) -> None:
        """Register a global event listener (applied to all caches)."""
        ...
