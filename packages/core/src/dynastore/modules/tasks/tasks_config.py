# dynastore/modules/tasks/tasks_config.py
import os
from typing import ClassVar, Tuple
from pydantic import Field, model_validator
from dynastore.models.mutability import Mutable
from dynastore.modules.db_config.plugin_config import PluginConfig

class TasksPluginConfig(PluginConfig):
    """Configuration for the Background Tasks module."""
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
            "creates new rows or fails to mark the row terminal. The pg_cron "
            "reaper SQL is rebuilt at startup; live changes only take effect "
            "on the next service restart."
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
        default=100, ge=1,
        description="Max concurrent in-process background tasks per pod.")
    dispatcher_batch_size: Mutable[int] = Field(
        default=10, ge=1,
        description="Rows claimed per dispatcher tick.")
    dispatcher_claim_reject_backoff_seconds: Mutable[int] = Field(
        default=30, ge=0,
        description="Back-off before re-claiming a rejected row.")
    task_timeout_seconds: Mutable[int] = Field(
        default=3600, ge=1,
        description="Cloud Run Job lease duration for an off_loaded task.")

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
