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

"""
### Task Interaction Overview

This module defines the data models for background tasks and asynchronous processes.
The task system uses a database-backed queue (PostgreSQL) for durability and visibility.

#### Interaction Diagram

```mermaid
sequenceDiagram
    participant App as Application Service
    participant DB as PostgreSQL (tasks table)
    participant Disp as Dispatcher / QueueListener
    participant Exec as BackgroundExecutor (Worker)
    participant HB as Heartbeat Manager

    App->>DB: INSERT Task (PENDING)
    Disp->>DB: SELECT PENDING Tasks (FOR UPDATE SKIP LOCKED)
    DB-->>Disp: Returns task row
    Disp->>DB: UPDATE Task (ACTIVE, locked_until)
    Disp->>Exec: Submit task execution
    Exec->>HB: Start heartbeats
    loop Task Execution
        HB->>DB: UPDATE Task (last_heartbeat_at, locked_until)
        Exec->>Exec: Run task logic
    end
    Exec->>HB: Stop heartbeats
    Exec->>DB: UPDATE Task (COMPLETED/FAILED, outputs/error)
```

#### Lifecycle & Interactions

1.  **Submission:** 
    A task is enqueued from the application using a `BackgroundRunner`. This inserts a row into the `tasks` table with status `PENDING`.
    
2.  **Discovery & Dispatch:**
    A background `Dispatcher` (for global tasks) or `QueueListener` (per tenant) periodically polls (or listens via pg_notify) for `PENDING` tasks. 
    It "claims" a task by setting its status to `ACTIVE`, assigning a unique `owner_id`, and setting a `locked_until` timestamp to prevent other dispatchers from picking it up.

3.  **Execution:**
    The dispatcher passes the task hydrated into a `TaskPayload` to a `BackgroundExecutor` worker. The executor runs the specific task's `run()` method.

4.  **Liveness & Heartbeats:**
    For long-running tasks, a `HeartbeatManager` runs concurrently with the task execution. It periodically updates the `last_heartbeat_at` and `locked_until` fields in the database.
    If a worker crashes, the `locked_until` expires, and a 'Janitor' process eventually resets the task to `PENDING` or `FAILED`.

5.  **Completion:**
    Upon finishing, the worker updates the task record to `COMPLETED` (or `FAILED`), stores the results in `outputs` (or `error_message`), and clears the lock.

6.  **Observation:**
    The `tasks` table serves as a Source of Truth. Utilities like `wait_for_all_tasks` can query this table across schemas to synchronize test state.

#### Data Partitioning & Table Structure

To ensure scalability and isolation, the task system is split across multiple levels:

*   **System Level (Global Tasks):**
    Defined in the `tasks.tasks` schema/table. This table stores tasks that affect the entire platform or common infrastructure (e.g., global provisioning, system-wide maintenance).
*   **Catalog Level (Tenant Tasks):**
    Each catalog (tenant) has its own isolated `tasks` table within its physical schema (e.g., `s_<catalog_id>.tasks`). This prevents cross-tenant noise and ensures that heavy task loads in one catalog do not impact others.
*   **Collection Level:**
    Tasks targeting a specific STAC Collection (e.g. data ingestion into a collection) live inside the tenant-level `tasks` table but are identified by the `collection_id` column. This allows for rapid filtering of tasks relevant to a specific dataset.

#### Maintenance & Retention

The task system is designed for high-volume, ephemeral data management:

*   **Partitioning:** All `tasks` tables are partitioned by **RANGE** on the `timestamp` column. By default, partitions are created monthly (`ensure_future_partitions`).
*   **Retention Policy:** A background process (`register_retention_policy`) manages the data lifecycle. COMPLETED and FAILED tasks are typically pruned after 1 month to prevent the database from growing indefinitely.
*   **Recovery:** A 'Janitor' process detects tasks in `ACTIVE` state that have missed their heartbeats (exceeding `locked_until`) and automatically resets them to `PENDING` for retry or moves them to `DEAD_LETTER`.

#### Multi-Application Distributed Execution

DynaStore supports distributed task execution across multiple application instances (e.g., Cloud Run services) sharing a single PostgreSQL database.

##### Parallelism & Synchronization
*   **Ownership:** When a runner claims a task, it sets a unique `owner_id` (instance identity) and a `locked_until` timestamp.
*   **Contention Management:** The system uses `FOR UPDATE SKIP LOCKED` during task acquisition to ensure multiple replicas can poll the same table without blocking each other or picking up the same task.
*   **Reactive Scaling:** Using PostgreSQL `LISTEN/NOTIFY`, replicas are alerted immediately when new tasks are queued, minimizing latency without heavy polling.

##### Hypothetical Configurations

1. **Unified Configuration (Monolithic Runner)**
   A single Cloud Run service handles both HTTP API requests and all background task modules.
   ```
   [ Cloud Run Service (API + Runner) ] x 10 Replicas
           |
           +--> [ DB: catalog.catalogs ]
           +--> [ DB: tasks.tasks     ] (All modules enabled)
   ```
   *   **Pros:** Simpler deployment and infra management.
   *   **Cons:** CPU-heavy tasks (like ingestion) can degrade API responsiveness.

2. **Split Configuration (Specialized Workers)**
   Distinct services are deployed for API vs. specific background workloads using the `SCOPE` filter.
   ```
   [ Cloud Run API ] x 5 Replicas (SCOPE="api_open")
   [ Cloud Run Provisioner ] x 2 Replicas (SCOPE="gcp_provision")
   [ Cloud Run Ingestor ] x 20 Replicas (SCOPE="stac_ingest")
           |
           v
   [ Database (Shared Task Infrastructure) ]
   ```
   *   **Pros:** Isolated scaling; ingestion spikes don't impact API latency.
   *   **Cons:** Higher management overhead.

##### Pitfalls & Improvements
*   **Deadlocks:** Can occur if tasks acquire multiple database locks in inconsistent orders (e.g., locking a catalog row, then a task row, while another process does the reverse). *Improvement:* Standardize lock ordering and keep transactions short.
*   **Resource Leaks:** Long-running tasks that fail to release database connections or file handles can exhaust service quotas. *Improvement:* Use `asynccontextmanager` for all resources and implement aggressive timeouts in `managed_transaction`.
*   **Closed Channel Errors (gRPC):** Occur if module clients (GCP) are closed while background threads are still active. *Improvement:* Improved runner lifecycle to await all active tasks before module destruction.

For a deeper dive into architecture and operational best practices, see the [DynaStore Distributed Task System README](./README.md).

"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, Dict, Any, TypeVar, Generic, List
from dynastore.models.shared_models import Link
from uuid import UUID, uuid4
from datetime import datetime, timezone, UTC, timedelta
from enum import Enum

# --- Enumerations ---

class TaskStatusEnum(str, Enum):
    """Internal task status enumeration.

    OGC API Processes mapping:
      CREATED     → "created"    (Part 4: deferred, unlocked — PATCH allowed)
      PENDING     → "accepted"   (Part 1: queued for execution, locked)
      ACTIVE      → "running"    (Part 1: claimed by runner, locked)
      RUNNING     → "running"    (legacy alias)
      COMPLETED   → "successful" (Part 1: terminal)
      FAILED      → "failed"     (Part 1: terminal)
      DISMISSED   → "dismissed"  (Part 1: cancelled, terminal)
      DEAD_LETTER → "failed"     (internal: max retries exhausted)
    """
    CREATED     = "CREATED"      # OGC Part 4: defined but not started (unlocked)
    PENDING     = "PENDING"
    ACTIVE      = "ACTIVE"       # Claimed by a runner; heartbeat expected
    RUNNING     = "RUNNING"      # Legacy alias kept for backwards compatibility
    COMPLETED   = "COMPLETED"
    FAILED      = "FAILED"
    DISMISSED   = "DISMISSED"
    DEAD_LETTER = "DEAD_LETTER"  # max_retries exhausted; requires manual intervention


class TaskExecutionMode(str, Enum):
    """Defines the execution strategy for a task runner."""
    SYNCHRONOUS = "SYNCHRONOUS"
    ASYNCHRONOUS = "ASYNCHRONOUS"


class TaskExecutionScope(str, Enum):
    """Where a task should execute. Caller-specified, not class-level.

    This mirrors the existing pattern where ``execution_mode``
    (SYNC/ASYNC) is caller-specified via ``TaskCreate.execution_mode``
    or the HTTP ``Prefer`` header.  The same task can run in-process
    for a multi-driver write OR be offloaded remotely for a bulk
    ingestion — the caller decides.

    Resolution order (same as execution_mode):
      1. Caller preference (header, explicit param)
      2. Process constraints (``supportedScopes``)
      3. Runner availability
    """
    IN_PROCESS = "IN_PROCESS"      # asyncio.create_task, no dispatcher claim
    LOCAL_QUEUE = "LOCAL_QUEUE"    # Dispatcher claim loop (current default)
    REMOTE = "REMOTE"              # Offload to remote execution (Cloud Run Job, etc.)


class TaskScope(str, Enum):
    """Scope of a task within the platform."""
    CATALOG = "CATALOG"      # Scoped to a catalog (schema_name = tenant schema)
    SYSTEM = "SYSTEM"        # Platform-level (schema_name = 'system')
    ASSET = "ASSET"          # Scoped to an asset operation

# --- Generic Payload Model ---

InputsType = TypeVar("InputsType")

class TaskPayload(BaseModel, Generic[InputsType]):
    """
    A standardized data-transfer object (DTO) for passing information
    to a background task's `run` method.

    Used for both tasks and process executions (process-as-task).
    """
    task_id: UUID = Field(..., description="The unique ID of the job record from the database.")
    caller_id: str = Field(..., description="Identifier of the user or system that initiated the task.")
    inputs: InputsType = Field(..., description="The specific, strongly-typed inputs for the task.")
    asset: Optional[Any] = None

    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)

# --- Database & API Models ---

class TaskBase(BaseModel):
    caller_id: Optional[str] = None
    task_type: str
    type: str = "task"  # 'task' or 'process'
    inputs: Optional[Dict[str, Any]] = None
    collection_id: Optional[str] = None

class TaskCreate(TaskBase):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "task_type": "ingest",
                    "caller_id": "user@example.com",
                    "inputs": {
                        "source": "https://example.com/data.csv",
                        "collection_id": "stations",
                    },
                    "execution_mode": "ASYNCHRONOUS",
                    "execution_scope": "LOCAL_QUEUE",
                    "scope": "CATALOG",
                },
            ]
        },
    )

    execution_mode: str = TaskExecutionMode.ASYNCHRONOUS
    execution_scope: str = TaskExecutionScope.LOCAL_QUEUE
    scope: str = TaskScope.CATALOG
    max_retries: Optional[int] = Field(
        default=None,
        ge=0,
        description="Per-row retry budget consumed by the reaper / fail_task. "
                    "Defaults to the column DEFAULT (3) when None. Cloud Run "
                    "Job runners read it from the job's MAX_RETRIES env to "
                    "cap expensive long-running jobs at 1 retry.",
    )
    dedup_key: Optional[str] = Field(
        default=None,
        description="Deduplication key. If set, INSERT uses ON CONFLICT DO NOTHING "
                    "to prevent duplicate tasks from the same event.",
    )
    extra_context: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional context passed by the dispatcher (e.g. originating_event for rollback). "
                    "Not persisted to the DB — forwarded through RunnerContext at dispatch time.",
    )

class TaskUpdate(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "status": "RUNNING",
                    "progress": 50,
                },
                {
                    "status": "COMPLETED",
                    "progress": 100,
                    "outputs": {"result_url": "https://example.com/results/output.csv"},
                },
            ]
        },
    )

    status: Optional[TaskStatusEnum] = None
    progress: Optional[int] = Field(default=None, ge=0, le=100)
    outputs: Optional[Any] = None
    error_message: Optional[str] = None
    # Queue control fields (set by dispatcher / heartbeat manager)
    locked_until: Optional[datetime] = None
    last_heartbeat_at: Optional[datetime] = None
    owner_id: Optional[str] = None
    retry_count: Optional[int] = None

class Task(TaskBase):
    """
    Task model aligned with standard API job status format.
    Includes durable queue fields for dispatcher / Janitor / heartbeat.
    """
    jobID: UUID = Field(default_factory=uuid4, alias="task_id")
    type: str = Field(default="task", description="Type of job: 'task' or 'process'")

    # Global table fields
    schema_name: Optional[str] = Field(default=None, description="Tenant schema name (e.g. s_2ka8fbc3) or 'system'")
    scope: str = Field(default=TaskScope.CATALOG, description="Task scope: CATALOG, SYSTEM, or ASSET")
    execution_mode: str = Field(default=TaskExecutionMode.ASYNCHRONOUS, description="SYNCHRONOUS or ASYNCHRONOUS")
    dedup_key: Optional[str] = Field(default=None, description="Deduplication key for event-driven task creation")

    status: TaskStatusEnum = TaskStatusEnum.PENDING
    progress: int = Field(default=0, ge=0, le=100)

    outputs: Optional[Any] = None
    error_message: Optional[str] = Field(None, alias="message")

    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), alias="created")
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = Field(None, alias="updated")

    # Queue control — populated by dispatcher / heartbeat manager
    locked_until: Optional[datetime] = None
    last_heartbeat_at: Optional[datetime] = None
    owner_id: Optional[str] = None        # CloudIdentity email or caller_id
    retry_count: int = Field(default=0)
    max_retries: int = Field(default=3)

    links: List[Link] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    @property
    def task_id(self) -> UUID:
        """Backward compatibility alias for jobID."""
        return self.jobID
