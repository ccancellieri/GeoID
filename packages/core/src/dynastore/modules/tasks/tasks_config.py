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

# dynastore/modules/tasks/tasks_config.py
import os
from typing import ClassVar, Tuple
from pydantic import Field, model_validator
from dynastore.extensions.tools.exposure_mixin import ExposableConfigMixin
from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig

class TasksPluginConfig(ExposableConfigMixin, PluginConfig):
    """Configuration for the Background Tasks module.

    Inherits ``enabled`` from ``ExposableConfigMixin`` so the tasks
    extension's HTTP surface participates in the Service Exposure matrix
    like every other togglable extension; the background tasks module
    itself keeps running regardless of the toggle.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "tasks")


    queue_poll_interval: Mutable[float] = Field(
        default_factory=lambda: float(os.environ.get("DYNASTORE_QUEUE_POLL_INTERVAL", "30.0")),
        description="Fallback polling interval (in seconds) for the task queue listener when real-time push notifications are unavailable.",
        ge=0.1
    )

    hard_retry_cap: Mutable[int] = Field(
        default=5,
        ge=1,
        description=(
            "Platform-wide circuit breaker on per-task retries. The dispatcher "
            "stops claiming, the reaper writes DEAD_LETTER, and fail_task "
            "refuses further retries once a row reaches retry_count >= "
            "hard_retry_cap, regardless of the row's individual max_retries. "
            "Defends against re-enqueue loops where a misbehaving runner "
            "creates new rows or fails to mark the row terminal. The "
            "reap_stuck_tasks PL/pgSQL function is rebuilt via CREATE OR "
            "REPLACE at startup; live changes only take effect on the next "
            "service restart."
        ),
    )

    capability_publisher_ttl_seconds: Mutable[float] = Field(
        default=60.0,
        ge=10.0,
        le=600.0,
        description=(
            "TTL (seconds) for capability liveness sentinel keys written to "
            "the shared cache by every pod that can service a capability "
            "(e.g. an Indexer registered in this process). Read by the "
            "reactive reaper (#502): when the last pod with a capability "
            "dies, no one refreshes the key, the TTL expires, and "
            "unclaimable task rows are DLQed on the next dispatcher pass. "
            "Pair with capability_publisher_refresh_seconds <= ttl/2 so "
            "one missed tick is absorbed."
        ),
    )

    capability_publisher_refresh_seconds: Mutable[float] = Field(
        default=30.0,
        ge=5.0,
        description=(
            "How often each pod refreshes its capability sentinel keys. "
            "Must be <= capability_publisher_ttl_seconds / 2 to tolerate a "
            "single missed tick without false-positive DLQs."
        ),
    )

    proactive_sweep_interval_seconds: Mutable[float] = Field(
        default=60.0,
        ge=5.0,
        le=3600.0,
        description=(
            "Interval (seconds) between proactive sweeps that DLQ "
            "PENDING/retry=0 rows whose required capability has no live "
            "worker (issue #524). Reactive path stays as a safety net. "
            "Lower → tighter latency before stuck-PENDING rows leave "
            "PENDING; higher → fewer wakeups when the deployment is "
            "healthy. Read at startup; live changes apply on next pod "
            "restart (same model as hard_retry_cap)."
        ),
    )

    proactive_sweep_min_age_seconds: Mutable[float] = Field(
        default=300.0,
        ge=30.0,
        description=(
            "Minimum age (seconds) a row must be PENDING before the "
            "proactive sweep is allowed to look at it. Guards against "
            "false-positive DLQs during a publisher cold-start window: "
            "must be >= 2 * capability_publisher_ttl_seconds so any "
            "newly-deployed pod has at least one full refresh cycle to "
            "advertise its capability before a sweep can DLQ rows targeting it."
        ),
    )

    background_runner_concurrency: Mutable[int] = Field(
        default=4,
        ge=1,
        description=(
            "Maximum number of in-process background tasks that the BackgroundRunner "
            "may execute concurrently per pod.  At runtime the BackgroundRunner "
            "clamps this value to at most pool_total - SERVING_RESERVE so it can "
            "never exhaust the DB connection pool regardless of the configured value "
            "(pool_total = DB_POOL_MAX_SIZE, SERVING_RESERVE = 2).  "
            "Default 4: conservative baseline that fits under the minimum pool "
            "floor (SAFE_POOL_TOTAL_FLOOR = 10) while leaving headroom for serving "
            "reads.  Raise via the configs hot-reload API for high-throughput "
            "environments; the clamp keeps the pool safe even if the value is set "
            "too high."
        ),
    )
    dispatcher_batch_size: Mutable[int] = Field(
        default=10, ge=1,
        description="Rows claimed per dispatcher tick.")
    dispatcher_claim_reject_backoff_seconds: Mutable[int] = Field(
        default=30, ge=0,
        description="Back-off before re-claiming a rejected row.")
    task_timeout_seconds: Mutable[int] = Field(
        default=3600, ge=1,
        description="Cloud Run Job lease duration for a task executing on a Cloud Run Job.")

    provisioning_group_concurrency: Mutable[int] = Field(
        default=4,
        ge=1,
        description=(
            "Maximum number of provisioners within a single priority group that "
            "CatalogProvisionTask executes concurrently.  Bounds the asyncio "
            "gather over each group so the dispatcher pool is not saturated when "
            "many provisioners share the same priority.  Reads live via the "
            "configs hot-reload path; no service restart required."
        ),
    )

    terminal_task_ttl_days: Mutable[int] = Field(
        default=30,
        ge=1,
        description=(
            "Days to retain COMPLETED and FAILED tasks before the retention "
            "sweep deletes them. Applies to all tenants. Increase to keep "
            "longer audit trails; decrease to bound partition growth. "
            "Changes are picked up on the next retention tick (no restart)."
        ),
    )

    dlq_max_age_days: Mutable[int] = Field(
        default=90,
        ge=1,
        description=(
            "Days to retain DEAD_LETTER tasks before the retention sweep "
            "hard-deletes them. Rows older than this cutoff have exhausted "
            "operator intervention time and are purged. "
            "Changes are picked up on the next retention tick (no restart)."
        ),
    )

    dlq_alert_threshold: Mutable[int] = Field(
        default=100,
        ge=0,
        description=(
            "Emit a health alert (tasks.health_alert / dead_letter_overflow) "
            "when the platform-wide DEAD_LETTER task count exceeds this value. "
            "Set to 0 to alert on any DLQ entry. "
            "Changes are picked up on the next retention tick (no restart)."
        ),
    )

    retention_sweep_interval_seconds: Mutable[float] = Field(
        default=86400.0,  # daily
        ge=60.0,
        description=(
            "Interval (seconds) between task-retention passes: terminal-task "
            "purge, DLQ age-cap deletion, and DLQ count health-alert. "
            "Read at startup; changing requires a pod restart to take effect "
            "(same model as proactive_sweep_interval_seconds)."
        ),
    )

    async_writer_backlog_threshold: Mutable[int] = Field(
        default=2000,
        ge=0,
        description=(
            "Aggregate ready-row count across the global tasks.storage "
            "('ready') + tasks.events ('PENDING') outbox tables above which "
            "the serving-path secondary-write drainers (storage_drain / "
            "event_drain) prefer the offloaded async_writer Cloud Run Job "
            "over the in-process BackgroundRunner (#2622). Below the "
            "threshold the drain stays in-process — light load must not pay "
            "for a job hop. Read on every dispatch decision via a short-TTL "
            "cached probe (dynastore.modules.tasks.async_writer_backlog), so "
            "changes take effect within a few seconds, no restart required. "
            "Has no effect when no async_writer Cloud Run Job is deployed — "
            "the offload guard fails open to the in-process path in that case."
        ),
    )

    @model_validator(mode="after")
    def _enforce_refresh_le_half_ttl(self) -> "TasksPluginConfig":
        if self.capability_publisher_refresh_seconds > self.capability_publisher_ttl_seconds / 2:
            raise ValueError(
                "capability_publisher_refresh_seconds "
                f"({self.capability_publisher_refresh_seconds}s) must be "
                "<= capability_publisher_ttl_seconds / 2 "
                f"({self.capability_publisher_ttl_seconds / 2}s). A refresh "
                "interval larger than half the TTL means one missed tick "
                "expires the sentinel and the reactive reaper false-DLQs "
                "live capabilities."
            )
        min_age_floor = 2.0 * self.capability_publisher_ttl_seconds
        if self.proactive_sweep_min_age_seconds < min_age_floor:
            raise ValueError(
                "proactive_sweep_min_age_seconds "
                f"({self.proactive_sweep_min_age_seconds}s) must be "
                f">= 2 * capability_publisher_ttl_seconds ({min_age_floor}s) "
                "so a freshly-deployed pod has at least one full publisher "
                "cycle to advertise its capability before the proactive "
                "sweeper can DLQ rows targeting it."
            )
        return self
