# dynastore/modules/tasks/tasks_config.py
import os
from typing import ClassVar, Dict, List, Optional, Tuple
from pydantic import Field
from dynastore.extensions.tools.exposure_mixin import ExposableConfigMixin
from dynastore.modules.db_config.platform_config_service import PluginConfig

class TasksPluginConfig(ExposableConfigMixin, PluginConfig):
    """Configuration for the Background Tasks module."""
    _address: ClassVar[Tuple[str, ...]] = ("platform", "tasks")


    queue_poll_interval: float = Field(
        default_factory=lambda: float(os.environ.get("DYNASTORE_QUEUE_POLL_INTERVAL", "30.0")),
        description="Fallback polling interval (in seconds) for the task queue listener when real-time push notifications are unavailable.",
        ge=0.1
    )

    hard_retry_cap: int = Field(
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


class TaskRoutingConfig(ExposableConfigMixin, PluginConfig):
    """Service-affinity routing for the global task queue.

    Sibling to ``ItemsRoutingConfig`` but for the tasks tier. Maps
    ``task_type`` → ordered list of service names allowed to claim it.
    Empty/missing entry → any service whose runner can handle the task may
    claim (legacy behaviour).

    Each process compares the configured service names against its own
    ``service_name`` resolved from ``${DYNASTORE_CONFIG_ROOT}/instance.json``
    (see ``modules/db_config/instance.py``).  Per-deployment defaults are
    seeded by dropping a JSON file into ``${DYNASTORE_CONFIG_ROOT}/defaults/``;
    runtime changes go through the standard ``PUT /configs/plugins/task_routing_config``
    admin route and trigger the apply-handler that re-narrows the dispatcher's
    CapabilityMap.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "tasks")


    routing: Dict[str, List[str]] = Field(
        default_factory=dict,
        description=(
            "task_type → list of logical service names. Missing key or empty "
            "list = any capable service may claim. Set per deployment via the "
            "JSON files mounted into ${DYNASTORE_CONFIG_ROOT}/defaults/."
        ),
    )

    routing_disabled: bool = Field(
        default=False,
        description=(
            "Operator kill-switch — when true, the routing filter is a no-op "
            "and any service claims anything its CapabilityMap accepts. Use "
            "for emergency triage."
        ),
    )

    event_consumer_services: List[str] = Field(
        default_factory=list,
        description=(
            "Logical service names that run the catalog event consumer "
            "(durable 16-shard outbox loop in CatalogModule). Empty list = "
            "no service consumes — events accumulate in the outbox until a "
            "service is added. Default-empty is intentional: an unconfigured "
            "deployment fails noisy (queue depth visible in monitoring) "
            "rather than fails silent (connection storm everywhere). Set per "
            "deployment via the JSON files mounted into "
            "${DYNASTORE_CONFIG_ROOT}/defaults/."
        ),
    )
