# Protocol-Based Discovery & Decoupling

## Overview
Agro-Informatics Platform (AIP) - Catalog Services uses a "Duck Typing" dependency injection mechanism based on Python's `runtime_checkable` protocols. This approach allows components (Modules, Extensions, and Tasks) to discover each other's capabilities without direct imports, significantly reducing circular dependencies and architectural coupling.

## The `get_protocol` Tool
The core of this mechanism is the `get_protocol(Protocol)` utility located in `dynastore.tools.discovery`.

### How it Works
1.  **Capability over Class**: A component asks for a **CAPABILITY** (defined by a Protocol) rather than a specific **IMPLEMENTATION** class.
2.  **Unified Search**: It searches across all instantiated:
    *   **Modules**: Foundational services (DB, Auth, etc.)
    *   **Extensions**: Web-facing features (STAC, Tiles, etc.)
    *   **Tasks**: Background processing units.
3.  **Prioritization & Fallback**:
    *   **Priority**: Each implementation can declare a `priority` attribute (integer). Higher values have higher precedence.
    *   **Availability**: Implementations can define an `is_available() -> bool` method. The discovery tool skips any implementation where this returns `False`.
    *   **Selection**: `get_protocol()` returns the highest priority implementation that is currently available.
4.  **Caching**: The results are cached using `@lru_cache` to ensure performance remains high during frequent lookups.

### Discovery APIs
- `get_protocol(protocol: Type[T]) -> Optional[T]`: Returns the single best (highest priority, available) implementation.
- `get_protocols(protocol: Type[T]) -> List[T]`: Returns all available implementations sorted by priority.

## Example Use Case: Decentralized Policies
Previously, extensions like `tiles` and `stac` had to import the `apikey` module directly to register their public access policies. This created a hard dependency and potential circular imports.

### The Decoupled Approach:
1.  **Define the Protocol**: The `AuthorizationProtocol` (in `dynastore.models.auth`) defines the `register_policy` method.
2.  **Implement the Protocol**: The `ApiKeyModule` implements `register_policy`.
3.  **Discover and Use**:
    ```python
    from dynastore.models.auth import AuthorizationProtocol
    from dynastore.tools.discovery import get_protocol

    def register_extension_policies():
        authz = get_protocol(AuthorizationProtocol)
        if authz:
            authz.register_policy(my_policy)
    ```

## Benefits
-   **No Direct Imports**: Extensions don't need to know which module provides authorization.
-   **Pluggability**: The `ApiKeyModule` could be replaced by a `KeycloakModule` as long as it implements `AuthorizationProtocol`.
-   **Resilience**: Extensions gracefully handle cases where no authorization provider is present (e.g., in a minimal environment) or fall back to lower-priority providers.

## Implementation Details
-   **Tool**: `dynastore/tools/discovery.py`
-   **Protocols**: `dynastore/models/auth.py`, `dynastore/modules/protocols.py`
-   **Refactored Example**: [tiles/presets/__init__.py](../../packages/extensions/tiles/src/dynastore/extensions/tiles/presets/__init__.py)

## Task & Event Protocols

The task and event system defines a layered protocol hierarchy that separates CRUD concerns from queue/bus semantics, enabling pluggable backends.

### Protocol Hierarchy

```
TasksProtocol (CRUD — task read/write/list)
    └── TaskQueueProtocol (queue operations — enqueue, claim, heartbeat, janitor)

EventsProtocol (in-process pub/sub)
    └── EventBusProtocol (durable delivery — publish, consume, ack/nack)
```

### `TasksProtocol`

Defined in `dynastore/models/protocols/tasks.py`. Provides basic CRUD operations for task records:
- `create_task`, `update_task`, `get_task`, `list_tasks`
- Catalog-aware helpers: `create_task_for_catalog`, `get_task_for_catalog`, `list_tasks_for_catalog`

The `schema` parameter refers to the `schema_name` column value in the global `tasks.tasks` table (e.g., `"s_2ka8fbc3"` or `"system"`), not a PostgreSQL schema qualifier.

### `TaskQueueProtocol`

Defined in `dynastore/models/protocols/task_queue.py`. Extends `TasksProtocol` with durable queue semantics:

| Method | Description |
|---|---|
| `enqueue()` | Insert task with optional `dedup_key` (ON CONFLICT DO NOTHING) |
| `claim_next()` | Atomically claim next PENDING task via FOR UPDATE SKIP LOCKED, filtered by runner capability map |
| `complete()` | Mark task COMPLETED with outputs |
| `fail()` | Mark FAILED with retry (exponential backoff) or DEAD_LETTER |
| `heartbeat()` | Batched extension of `locked_until` for active tasks |
| `find_stale()` | Find ACTIVE tasks with expired locks (janitor) |
| `cleanup_orphans()` | Dead-letter tasks for deleted catalogs |
| `get_capable_task_types()` | Returns task types this instance can execute |

**Implementations:**
- PostgreSQL: `SELECT FOR UPDATE SKIP LOCKED` (default, on-premise + Cloud Run)
- Cloud Tasks: Google Cloud Tasks push model (future)

### `EventBusProtocol`

Defined in `dynastore/models/protocols/event_bus.py`. Extends `EventsProtocol` with durable outbox semantics:

| Method | Description |
|---|---|
| `publish()` | Insert event into outbox with optional `dedup_key` and transactional outbox support |
| `consume_batch()` | Claim batch of PENDING events via SKIP LOCKED |
| `ack()` | Log + DELETE consumed events |
| `nack()` | Increment retry or move to DEAD_LETTER |
| `has_listeners()` | Check if any async event listeners are registered |
| `start_consumer()` | Start background consumer loop with leader election |
| `stop_consumer()` | Graceful shutdown |

The consumer starts **automatically** when any module registers async event listeners (`has_listeners()` returns True). No environment variable is needed — modules opt in declaratively.

**Implementations:**
- PostgreSQL outbox + LISTEN/NOTIFY (default)
- GCP EventArc (future)

### `RunnerProtocol` (Enhanced)

Task runners implement a priority-based dispatch chain with `can_handle()`:

| Runner | Priority | `can_handle()` | Mode |
|---|---|---|---|
| `GcpCloudRunRunner` | 10 | `task_type in self._job_map_cache` | ASYNC |
| `BackgroundRunner` | 100 | `get_task_instance(task_type) is not None` | ASYNC |
| `SyncRunner` | 100 | `get_task_instance(task_type) is not None` | SYNC |

On-premise environments have no `GcpCloudRunRunner` — tasks fall through to `BackgroundRunner` transparently. A `CapabilityMap` singleton aggregates all runner capabilities at startup and feeds the claim query with the exact task types each instance can handle.
