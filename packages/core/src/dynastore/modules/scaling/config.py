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

"""Hot-reloadable thresholds for the autoscaling control loop.

Every tunable lives here — none of them are read from the environment.
Operators adjust them live via ``PATCH /configs/plugins/scaling_policy``;
the publisher and the reconciler both read a fresh copy of this config on
every tick, so a change to ``enabled``, the thresholds, or the budget takes
effect within one cadence period.

The one exception is the reconciler's *loop cadence*
(``reconcile_interval_seconds``): the leader loop captures its sleep interval
once at process start, so a change there only takes effect on the next
deployment. ``publish_interval_seconds`` is genuinely hot-reloaded because the
RUN_EVERYWHERE publisher re-reads it each iteration.
"""

from __future__ import annotations

from typing import ClassVar, Tuple

from pydantic import Field

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig


class ScalingPolicyConfig(PluginConfig):
    """Thresholds and budget for the protocol-driven scaling control loop."""

    _address: ClassVar[Tuple[str, ...]] = ("platform", "modules", "scaling")

    enabled: Mutable[bool] = Field(
        default=False,
        description=(
            "Master switch. Off by default — the publisher skips publishing "
            "and the reconciler never actuates until explicitly enabled."
        ),
    )

    min_replicas: Mutable[int] = Field(
        default=0,
        ge=0,
        description="Floor for the platform's min-instance-count lever.",
    )
    max_replicas: Mutable[int] = Field(
        default=10,
        ge=1,
        description=(
            "Ceiling for the platform's min-instance-count lever before the "
            "DB connection budget is applied — see ``db_max_connections`` / "
            "``per_instance_pool`` / ``connection_headroom``."
        ),
    )

    scale_out_saturation: Mutable[float] = Field(
        default=0.80,
        ge=0.0,
        le=1.0,
        description=(
            "Per-instance connection-pool saturation (MAX across reporting "
            "instances) at or above which the control loop considers scaling "
            "out."
        ),
    )
    conn_pressure_ceiling: Mutable[float] = Field(
        default=0.80,
        ge=0.0,
        le=1.0,
        description=(
            "Fleet-wide DB connection pressure at or above which scale-out is "
            "suppressed regardless of per-instance saturation — more "
            "instances cannot relieve a DB that is already the bottleneck. "
            "Aligned with ``DbContentionMonitorConfig.conn_pressure_ratio`` "
            "by default."
        ),
    )

    per_instance_pool: Mutable[int] = Field(
        default=4,
        ge=1,
        description=(
            "DB connections a single instance can hold open (e.g. the DuckDB "
            "driver's connection-pool size). Used to derive the budget "
            "ceiling — keep aligned with the deployed pool size."
        ),
    )
    db_max_connections: Mutable[int] = Field(
        default=100,
        ge=1,
        description="The database's max_connections, mirrored here so the "
        "budget ceiling can be computed without a live DB round trip.",
    )
    connection_headroom: Mutable[int] = Field(
        default=20,
        ge=0,
        description=(
            "Connections reserved for non-scaled consumers (migrations, "
            "admin tooling, other services) and subtracted from "
            "``db_max_connections`` before deriving the budget ceiling."
        ),
    )

    deadband: Mutable[float] = Field(
        default=0.10,
        ge=0.0,
        le=1.0,
        description=(
            "Gap below ``scale_out_saturation`` that the per-instance p95 "
            "saturation must drop under before a scale-in is considered — "
            "prevents flapping right at the scale-out threshold."
        ),
    )
    cooldown_seconds: Mutable[int] = Field(
        default=300,
        ge=0,
        description="Minimum time between two actuated changes to min-instance-count.",
    )
    step: Mutable[int] = Field(
        default=1,
        ge=1,
        description="Instances added or removed per actuated change.",
    )

    publish_interval_seconds: Mutable[int] = Field(
        default=15,
        ge=1,
        description=(
            "Cadence of the per-instance signal publisher. The Valkey "
            "document TTL is 3x this value, so a dead instance's signals "
            "age out within roughly 3 missed publishes."
        ),
    )
    reconcile_interval_seconds: Mutable[int] = Field(
        default=30,
        ge=1,
        description="Cadence of the leader-elected platform reconciler tick.",
    )
