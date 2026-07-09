# Caching Framework

## Overview

The platform uses a centralized caching framework built around the `@cached` decorator in `dynastore/tools/cache.py`. It provides a two-layer architecture:

- **`CacheBackend` / `SyncCacheBackend`**: Low-level protocols (bytes), for backend implementors
- **`Cache`**: High-level protocol (typed values), for application code
- **`@cached`**: Decorator built on top, the primary API for most use cases

## `@cached` Decorator

```python
from dynastore.tools.cache import cached, CacheIgnore

@cached(
    maxsize=1024,           # Max entries (LRU eviction)
    ttl=300,                # Time-to-live in seconds
    jitter=30,              # Random TTL variance (anti-thundering-herd)
    namespace="my_cache",   # Key isolation prefix
    ignore=["conn"],        # Param names excluded from cache key
    typed=False,            # Cache differently by argument type
    condition=lambda r: r is not None,  # Only cache if True
    key_builder=None,       # Custom key function
)
async def get_data(item_id: str, conn: DbResource) -> dict:
    ...
```

### Excluding Parameters from Cache Key

Two equivalent approaches:

```python
# 1. ignore parameter
@cached(maxsize=128, ignore=["conn", "engine"])
async def fetch(item_id: str, conn: DbResource): ...

# 2. CacheIgnore type annotation
@cached(maxsize=128)
async def fetch(item_id: str, conn: CacheIgnore[DbResource] = None): ...
```

### Cache Management Methods

All decorated functions get these sync methods:

```python
# Invalidate a specific entry (by matching args)
get_data.cache_invalidate("item_123", conn=some_conn)

# Clear all entries for this function's namespace
get_data.cache_clear()

# Get cache statistics
info = get_data.cache_info()  # CacheStats(hits, misses, size, maxsize, evictions)
info.currsize  # alias for size (backward compat)
```

**`cache_invalidate` and `cache_clear` are always synchronous** — they directly manipulate the backend's internal dict. This allows calling from both sync and async contexts without `await`.

### Instance-Bound Caches

For methods that need per-instance caching:

```python
class MyService:
    def __init__(self):
        self._setup_cache()

    def _setup_cache(self):
        self.get_data_cached = cached(maxsize=64, ttl=300, namespace="my_svc")(
            self._get_data_db
        )

    async def _get_data_db(self, item_id: str) -> Optional[dict]:
        ...  # actual DB fetch
```

### Sync vs Async Auto-Detection

The decorator auto-detects sync/async functions via `inspect.iscoroutinefunction()`:
- **Async functions** use `LocalAsyncCacheBackend` with `asyncio.Lock` stampede protection
- **Sync functions** use `LocalSyncCacheBackend` with `threading.Lock`

## Backends

### LocalAsyncCacheBackend

In-memory async backend using `collections.OrderedDict`:
- LRU eviction via `move_to_end()` on access
- TTL checked lazily on `get()`
- Per-key `asyncio.Lock` for stampede protection
- Priority-based eviction (`CacheItemPriority`)

### LocalSyncCacheBackend

Same semantics but with `threading.Lock` for sync contexts.

### L1 memory budget

All local (L1) backends in a process share one byte budget:
`CachePluginConfig.l1_memory_percent` (default 10%) of the worker's memory
share — the container memory divided by the gunicorn worker count, the same
per-worker base the memory watchdog uses. Each entry is charged an
approximate deep size at insert; when the process-wide total exceeds the
budget, the backend holding the most bytes evicts its lowest
**value-per-byte** entry: among its least-recently-used candidates, the one
with the smallest `(hits + 1) / size_bytes` score goes first, so large,
rarely-hit entries are dropped before small hot ones. `NEVER_REMOVE`
entries are exempt, and the per-site `maxsize` entry-count cap still
applies independently. A single value larger than a fixed fraction of the
budget is not admitted to L1 at all — the write still succeeds (and still
reaches the distributed tier in tiered setups), since evicting the working
set to hold one oversized entry is never a win. When no memory base can be resolved (e.g. local
dev without a container limit) or the percent is 0, byte-budget eviction
is disabled and only the entry-count caps bound the caches.

When a distributed backend is registered, tiered caches trust L1 for at
most `CachePluginConfig.l1_default_ttl_seconds` (default 30s) before the
next read reconciles with the distributed tier; a per-site
`@cached(l1_ttl=...)` still overrides this. Both settings are
hot-reloadable through the cache config plugin.

### CacheManager

Central registry of backends, discoverable via `get_protocol(CacheManagerProtocol)`:

```python
from dynastore.tools.cache import CacheManager

manager = CacheManager()
manager.register_backend(LocalAsyncCacheBackend(max_size=1024))
cache = manager.create_cache(CacheConfig(namespace="my_ns", default_ttl=300))
```

### Required Shared Backend

Deployments that require cross-instance cache consistency can set
`CachePluginConfig.shared_backend_required=true`. In that mode the Valkey
backend is treated as part of the service contract rather than an optional
accelerator:

- startup refuses to enter local-only cache mode when Valkey cannot be built or
  probed;
- live reconnects keep the last healthy Valkey backend until a replacement has
  been built and probed successfully;
- the Valkey circuit breaker does not unregister the shared backend and drift to
  in-process L1;
- tiered cache reads re-raise distributed-tier failures instead of serving a
  potentially stale local value.

Leave the flag `false` for local development, tests, or deployments where
best-effort in-process caching is acceptable.

## Protocols

Defined in `dynastore/models/protocols/cache.py`:

| Protocol | Purpose |
|----------|---------|
| `CacheBackend` | Async low-level (bytes) interface |
| `SyncCacheBackend` | Sync variant |
| `Cache` | High-level typed interface with `get_or_set()` |
| `CacheManagerProtocol` | Backend registry + cache factory |
| `CacheSerializer` | Serialization (NullSerializer for in-memory, JsonSerializer for distributed) |
| `BatchCacheBackend` | Extension for MGET/MSET |
| `LockingCacheBackend` | Extension for distributed locks |
| `TaggableCacheBackend` | Extension for tag-based invalidation |

## Important: `discovery.py` Exception

`get_protocol()` and `get_protocols()` in `dynastore/tools/discovery.py` use `functools.lru_cache`, **not** `@cached`. This is intentional — `discovery.py` is a foundational module that cannot depend on the cache framework without creating a circular import.
