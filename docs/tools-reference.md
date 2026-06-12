# Dynastore Tools & Utilities Reference

Developer guide for all shared utility modules. **Before writing new helpers, check this reference** — the utility you need likely exists.

---

## Quick Lookup Table

| Need | Module | Key Function/Class |
|------|--------|--------------------|
| Protocol resolution | `tools.discovery` | `get_protocol()`, `get_protocols()` |
| Fail-fast protocol | `tools.protocol_helpers` | `resolve(ProtocolType)` |
| DB engine access | `tools.protocol_helpers` | `get_engine()` |
| Async caching | `tools.cache` | `@cached(maxsize, ttl, namespace)` |
| Cache invalidation | `tools.cache` | `cache_invalidate(fn, *args)`, `cache_clear(fn)` |
| JSON serialization | `tools.json` | `CustomJSONEncoder`, `orjson_default()` |
| JSON response (FastAPI) | `extensions.tools.fast_api` | `AppJSONResponse`, `ORJSONResponse` |
| SQL injection prevention | `tools.db` | `validate_sql_identifier()` |
| SQL name sanitization | `tools.db` | `sanitize_for_sql_identifier()` |
| UUID generation | `tools.identifiers` | `generate_uuidv7()` |
| Geometry processing | `tools.geospatial` | `process_geometry()` |
| i18n field access | `tools.language_utils` | `resolve_localized_field()`, `inject_localized_field()` |
| Pub/sub signals | `tools.async_utils` | `SignalBus`, `WaitableSignal` |
| Batch aggregation | `tools.async_utils` | `AsyncBufferAggregator`, `KeyValueAggregator` |
| Sync-async bridge | `tools.async_utils` | `SyncQueueIterator` |
| Plugin base class | `tools.plugin` | `ProtocolPlugin[AppStateT]` |
| Component .env loading | `tools.env` | `load_component_dotenv(cls)` |
| File export | `tools.file_io` | `write_csv()`, `write_parquet()`, `write_geojson()`, etc. |
| String type parsing | `tools.json` | `parse_string_to_python_type()` |
| Pydantic dict parsing | `tools.pydantic` | `FlexibleDictParam`, `TemplateParam` |
| URL helpers | `extensions.tools.url` | `build_sibling_redirect()`, `get_base_url()` |
| Auth context | `extensions.iam.guards` | `security_context_from_request()`, `principal_from_request()`, `ensure_privileged_role_assignment()` |
| Permission check | `modules.iam.authorization` | `require_permission(ctx, Permission.X)` |
| Request state access | `extensions.tools.request_state` | `get_principal()`, `get_catalog_id()` |
| Conflict to HTTP 409 | `extensions.tools.conflict_handler` | `conflict_to_409()` |
| OGC response envelope | `extensions.tools.formatters` | `OGCResponseMetadata` |
| OGC subset parsing | `tools.ogc_common` | `parse_subset_parameter()` |
| SQL expression matching | `tools.expression` | `evaluate_sql_condition()` |
| Advisory locks | `db_config.locking_tools` | `acquire_startup_lock()`, `retry_on_lock_conflict()` |
| Advisory lock-id derivation | `durable.locks` | `stable_lock_id_sha256()`, `stable_lock_id_blake2b()` |
| Partition management | `db_config.partition_tools` | `ensure_hierarchical_partitions_exist()` |
| Schema/extension DDL | `db_config.maintenance_tools` | `ensure_schema_exists()`, `ensure_enum_type()` |
| Timer/profiling | `tools.timer` | `Timer` context manager |
| File path manipulation | `tools.path` | `insert_before_extension()` |
| Safe attribute access | `tools.utils` | `safe_get(obj, key, default)` |
| Class repr | `tools.class_tools` | `__repr__(self, sensitive_attrs, ...)` |
| Requirements parsing | `tools.dependencies` | `check_requirements()`, `parse_requirements_list()` |
| CQL filtering | `modules.tools.cql` | CQL parser for OGC queries |
| Feature models | `tools.features` | `Feature`, `FeatureProperties` |
| STAC metadata | `extensions.stac.metadata_helpers` | `merge_stac_metadata()`, `prune_managed_content()` |
| DuckDB helpers | `modules.storage.drivers._duckdb_helpers` | `normalize_to_dicts()`, `dicts_to_features()` |

---

## Core Tools (`dynastore.tools.*`)

### Plugin Discovery & Protocol Resolution

**Import**: `from dynastore.tools.discovery import get_protocol, get_protocols, register_plugin`

The protocol registry is the backbone of Dynastore's plugin architecture. All modules, extensions, and tasks register implementations of Protocol contracts. Consumer code resolves them at runtime.

```python
# Resolve a single protocol (first match, priority-ordered)
from dynastore.tools.discovery import get_protocol
config_registry = get_protocol(ConfigRegistryProtocol)

# Fail-fast resolution (raises RuntimeError if missing)
from dynastore.tools.protocol_helpers import resolve
engine = resolve(DatabaseProtocol).engine

# Convenience: get the async DB engine directly
from dynastore.tools.protocol_helpers import get_engine
engine = get_engine()
```

**Design rules**:
- `discovery.py` uses `functools.lru_cache` (NOT `@cached`) to avoid circular imports with `cache.py`
- `register_plugin()` / `unregister_plugin()` automatically clear the LRU cache
- `get_protocols()` returns results sorted by `.priority` (lower = higher priority)
- `is_available()` guard allows optional plugins to be skipped gracefully

### Async Caching (`@cached`)

**Import**: `from dynastore.tools.cache import cached, cache_invalidate, cache_clear, CacheIgnore`

Two-layer architecture: `CacheBackend` (low-level bytes) -> `Cache` (high-level typed) -> `@cached` (decorator).

```python
# Basic usage
@cached(maxsize=1024, ttl=300, namespace="catalog_config")
async def get_config(catalog_id: str) -> dict:
    ...

# Ignore parameters that shouldn't be part of the cache key
@cached(maxsize=1024, ttl=300, namespace="catalog_config")
async def get_config(catalog_id: str, conn: CacheIgnore[DbResource] = None) -> dict:
    ...

# Or using ignore= parameter
@cached(maxsize=64, ignore=["conn"])
async def get_config(catalog_id: str, conn=None) -> dict:
    ...

# Instance-bound pattern
self._get_thing = cached(maxsize=64)(self._db_get_thing)

# Invalidation (always synchronous)
cache_invalidate(get_config, "my_catalog")  # specific key
cache_clear(get_config)                      # all keys
```

### JSON Serialization

**Import**: `from dynastore.tools.json import CustomJSONEncoder, orjson_default, parse_string_to_python_type`

Handles: `datetime`, `date`, `UUID`, `Decimal`, `Enum`, `bytes`, Shapely geometries, Pydantic models.

```python
# Standard library encoder (for json.dumps)
import json
data = json.dumps(obj, cls=CustomJSONEncoder)

# High-performance orjson (for API responses)
import orjson
data = orjson.dumps(obj, default=orjson_default)

# Parse strings to typed Python objects
parse_string_to_python_type("42")        # -> int(42)
parse_string_to_python_type("true")      # -> True
parse_string_to_python_type("2024-01-01") # -> date(2024, 1, 1)

# Sanitize for API serialization
from dynastore.tools.json import sanitize_for_serialization
safe = sanitize_for_serialization(complex_obj)
```

### SQL Safety

**Import**: `from dynastore.tools.db import validate_sql_identifier, sanitize_for_sql_identifier`

**CRITICAL**: Always call `validate_sql_identifier()` before any f-string schema/table interpolation.

```python
# Validation (raises InvalidIdentifierError on bad input)
schema = validate_sql_identifier(user_input)  # -> lowercased, validated
sql = f'SELECT * FROM "{schema}".assets'

# Sanitization (for partition keys, replaces bad chars with _)
safe_name = sanitize_for_sql_identifier("my-value.1")  # -> "my_value_1"
```

Constraints: max 63 chars, no reserved keywords, must start with letter or `_`, allows `a-z0-9_.->`

### UUID Generation

**Import**: `from dynastore.tools.identifiers import generate_uuidv7`

```python
# RFC 9562 time-ordered UUID (sortable, ideal for entity IDs)
entity_id = generate_uuidv7()
```

### Async Utilities

**Import**: `from dynastore.tools.async_utils import ...`

```python
# Pub/sub (config changes, CORS rebuild, etc.)
from dynastore.tools.async_utils import signal_bus
signal_bus.emit("config_changed", {"key": "cors"})
signal_bus.on("config_changed", my_handler)

# Batch aggregation (high-throughput accounting)
aggregator = AsyncBufferAggregator(
    flush_callback=my_batch_write,
    threshold=100,
    interval=5.0
)
await aggregator.add(item)

# Sync-async bridge (for tasks that need sync iterators over async queues)
iterator = SyncQueueIterator(queue, loop)
for item in iterator:
    process(item)

# PostgreSQL LISTEN/NOTIFY -> SignalBus bridge
from dynastore.tools.async_utils import PgListenBridge
bridge = PgListenBridge(signal_bus, engine)
```

### Localization (i18n)

**Import**: `from dynastore.tools.language_utils import resolve_localized_field, inject_localized_field`

```python
# Extract a language value (fallback: en -> first available)
title = resolve_localized_field(item.title, lang="fr")

# Inject a single language (preserves others)
updated = inject_localized_field(item.title, "New Title", lang="en")

# All languages
all_titles = resolve_localized_field(item.title, lang="*")
```

### Geospatial

**Import**: `from dynastore.tools.geospatial import process_geometry`

Consolidates validation, fix attempts, CRS reprojection, and simplification.

```python
processed = process_geometry(geom_wkb_hex, storage_config)
```

Optional dependencies: `pyproj` (CRS), `h3`, `s2sphere`. Geometry exceptions in `tools.geospatial_exceptions`.

### Plugin Base Class

**Import**: `from dynastore.tools.plugin import ProtocolPlugin`

```python
class MyDriver(ProtocolPlugin["AppState"]):
    priority: int = 10

    @asynccontextmanager
    async def lifespan(self, app_state: "AppState"):
        await self._start()
        try:
            yield
        finally:
            await self._stop()

    def is_available(self) -> bool:
        return HAS_OPTIONAL_DEPENDENCY
```

### Component Environment

**Import**: `from dynastore.tools.env import load_component_dotenv`

```python
# Auto-loads .env co-located with the component source file
# override=False: production env vars always take precedence
load_component_dotenv(MyModule)
```

### File Export

**Import**: `from dynastore.tools.file_io import write_csv, write_parquet, write_geojson, write_geopackage, write_shapefile`

All writers accept feature dicts and produce formatted output. Internal `_sanitize()` normalizes UUID, Decimal, datetime, and bytes.

### Expression Evaluation

**Import**: `from dynastore.tools.expression import evaluate_sql_condition`

Safe in-memory SQL-like condition evaluation using Python's `ast` module (no arbitrary code execution).

```python
result = evaluate_sql_condition(
    condition="status = :status AND priority > 5",
    properties={"status": "active", "priority": 10},
    params={"status": "active"}
)  # -> True
```

---

## Extension Tools (`dynastore.extensions.tools.*`)

### FastAPI Responses

**Import**: `from dynastore.extensions.tools.fast_api import AppJSONResponse, ORJSONResponse`

```python
# Standard (CustomJSONEncoder-based)
return AppJSONResponse(content=data)

# High-performance (orjson-based)
return ORJSONResponse(content=data)
```

### URL Helpers

**Import**: `from dynastore.extensions.tools.url import build_sibling_redirect, get_base_url, get_root_url`

```python
# Relative redirect (avoids root_path duplication behind proxies)
url = build_sibling_redirect("login", params={"error": "Invalid credentials"})
# -> "login?error=Invalid+credentials"

# Request URL helpers
base = get_base_url(request)
root = get_root_url(request)
```

**Rule**: Always use relative URLs via `build_sibling_redirect()` — never absolute `root_path`.

### Auth & Request State

```python
# Request → SecurityContext + Permission enforcement
from dynastore.extensions.iam.guards import (
    security_context_from_request,
    principal_from_request,
)
from dynastore.models.protocols.authorization import Permission
from dynastore.modules.iam.authorization import require_permission

ctx = security_context_from_request(request)
await require_permission(ctx, Permission.SYSADMIN)  # raises PermissionError

# Typed request.state accessors (replaces scattered getattr)
from dynastore.extensions.tools.request_state import get_principal, get_catalog_id, get_principal_role
principal = get_principal(request)
catalog_id = get_catalog_id(request)
```

Endpoint-level authorization is enforced by `IamMiddleware` against the policy
registry — route handlers no longer declare `Depends(require_*)` guards.

### Conflict Handling

**Import**: `from dynastore.extensions.tools.conflict_handler import conflict_to_409`

```python
try:
    await create_entity(...)
except (UniqueViolationError, IntegrityError) as e:
    raise conflict_to_409(e, resource_name="Collection", resource_id=coll_id)
```

### OGC Response Formatting

**Import**: `from dynastore.extensions.tools.formatters import OGCResponseMetadata, OutputFormatEnum`

```python
meta = OGCResponseMetadata(
    numberMatched=total,
    numberReturned=len(items),
    links=[...]
)
```

Format dispatcher supports: CSV, GeoPackage, Parquet, Shapefile, GeoJSON.

---

## Database Config Tools (`dynastore.modules.db_config.*`)

### Advisory Locks

**Import**: `from dynastore.modules.db_config.locking_tools import acquire_startup_lock, retry_on_lock_conflict`

```python
# Startup DDL with advisory lock (re-entrant, auto-released)
async with managed_transaction(engine) as conn:
    await acquire_startup_lock(conn, "my_migration_key")
    await DDLQuery(ddl).execute(conn)

# Retry decorator for lock contention
@retry_on_lock_conflict(max_retries=5, base_delay=0.5)
async def my_db_operation():
    ...
```

Lock-id derivation lives in `dynastore.durable.locks` and is **frozen** (pin-tested):
`stable_lock_id_sha256(key)` folds a string to the signed 64-bit id used by
session-scoped leadership (`pg_advisory_leadership`, `acquire_startup_lock`);
`stable_lock_id_blake2b(*parts)` produces the non-negative 63-bit id used by
transaction-scoped guards (`pg_try_advisory_xact_lock`). The two are
intentionally distinct; changing either re-keys live advisory locks and breaks
single-leader guarantees during a rolling deploy. Leadership loops compose
`pg_advisory_leadership` with `run_leader_loop` (`dynastore.tools.async_utils`).

### Partition Management

**Import**: `from dynastore.modules.db_config.partition_tools import ensure_hierarchical_partitions_exist, ensure_partition_exists`

```python
await ensure_hierarchical_partitions_exist(conn, partition_def)
await ensure_partition_exists(conn, table, partition_name, range_start, range_end)
```

### Schema & Extension DDL

**Import**: `from dynastore.modules.db_config.maintenance_tools import ensure_schema_exists, ensure_enum_type, ensure_db_extension`

```python
await ensure_schema_exists(conn, schema_name)
await ensure_enum_type(conn, "my_status", ["active", "inactive"])
await ensure_db_extension(conn, "postgis")
```

---

## Domain-Specific Helpers

### STAC Metadata (`extensions.stac.metadata_helpers`)

```python
from dynastore.extensions.stac.metadata_helpers import merge_stac_metadata, prune_managed_content

# Split external STAC content from platform-managed fields on write
external = await prune_managed_content(stac_item)

# Merge external content back onto the base item on read (lang = active locale)
full_item = await merge_stac_metadata(base_item, external, lang="en")
```

### Multi-Catalog IAM Helpers (`modules.iam.multi_catalog_helpers`)

```python
from dynastore.modules.iam.multi_catalog_helpers import (
    create_global_principal,
    sync_principal_across_catalogs,
    get_effective_permissions_for_catalog,
)
```

### DuckDB Helpers (`modules.storage.drivers._duckdb_helpers`)

```python
from dynastore.modules.storage.drivers._duckdb_helpers import normalize_to_dicts, dicts_to_features
dicts = normalize_to_dicts(feature_collection)
features = dicts_to_features(dicts)
```

---

## Anti-Patterns to Avoid

| Anti-Pattern | Correct Approach |
|--------------|-----------------|
| `hasattr(obj, "method")` | `isinstance(obj, SomeProtocol)` with `@runtime_checkable` |
| `getattr(request.state, "x", None)` | `request_state.get_principal(request)` |
| Direct module import for DB engine | `protocol_helpers.get_engine()` or `resolve(DatabaseProtocol)` |
| Unsafe string condition matching | `evaluate_sql_condition()` (AST-based, safe) |
| `json.dumps()` without encoder | `json.dumps(obj, cls=CustomJSONEncoder)` |
| Absolute redirect URLs | `build_sibling_redirect()` (relative) |
| `functools.lru_cache` on async methods | `@cached()` from `tools.cache` |
| Raw f-string SQL identifiers | `validate_sql_identifier()` first |
| `@cached` in `discovery.py` | `functools.lru_cache` only (circular import) |
| Duplicate validation logic | Check this reference first |

---

## Architecture Diagram

```
dynastore/
├── tools/                          # CORE: zero domain deps, safe to import everywhere
│   ├── discovery.py                # Protocol registry (lru_cache, NOT @cached)
│   ├── cache.py                    # @cached decorator + CacheBackend
│   ├── plugin.py                   # ProtocolPlugin base class
│   ├── protocol_helpers.py         # resolve() + get_engine()
│   ├── json.py                     # CustomJSONEncoder + orjson_default
│   ├── db.py                       # SQL identifier validation
│   ├── identifiers.py              # UUIDv7 generation
│   ├── async_utils.py              # SignalBus, aggregators, sync-async bridges
│   ├── language_utils.py           # i18n field resolution
│   ├── geospatial.py               # Geometry processing pipeline
│   ├── expression.py               # Safe SQL condition evaluation
│   ├── file_io.py                  # CSV/Parquet/GeoJSON/Shapefile writers
│   ├── pydantic.py                 # FlexibleDictParam, TemplateParam
│   ├── env.py                      # Component .env loading
│   ├── features.py                 # Feature/FeatureProperties models
│   ├── timer.py                    # Timer context manager
│   ├── path.py                     # File path manipulation
│   ├── utils.py                    # safe_get()
│   ├── class_tools.py              # Flexible __repr__
│   ├── dependencies.py             # Requirements parsing
│   └── ogc_common.py               # OGC subset parameter parsing
│
├── extensions/tools/               # HTTP/API layer utilities (may import FastAPI)
│   ├── fast_api.py                 # AppJSONResponse, ORJSONResponse
│   ├── url.py                      # URL builders, build_sibling_redirect()
│   ├── security.py                 # get_principal() FastAPI dependency
│   ├── request_state.py            # Typed request.state accessors
│   ├── conflict_handler.py         # DB constraint -> HTTP 409
│   ├── formatters.py               # OGCResponseMetadata, format dispatchers
│   ├── conformance.py              # OGC conformance helpers
│   ├── query.py                    # Query utilities
│   ├── db.py                       # Extension-level DB utilities
│   ├── exception_handlers.py       # FastAPI exception mappers
│   └── language_utils.py           # i18n (re-export from core)
│
├── durable/                        # Durable-work primitives (shared SSOT)
│   └── locks.py                    # Frozen advisory lock-id derivations
│
├── modules/db_config/              # Database infrastructure
│   ├── locking_tools.py            # Advisory locks, retry_on_lock_conflict
│   ├── partition_tools.py          # Hierarchical partitioning
│   ├── maintenance_tools.py        # Schema/extension/partition DDL
│   ├── sql_tools.py                # Raw SQL script execution
│   └── tools.py                    # DB URL normalization
│
├── modules/tools/                  # Module-level shared
│   ├── cql.py                      # CQL parser
│   └── features.py                 # OGC feature utilities
│
└── models/shared_models.py         # Link, OutputFormatEnum, constants
```

**Dependency rule**: `tools/` has zero domain imports. `extensions/tools/` may import FastAPI/Starlette. `modules/*/tools/` may import their parent module's models.
