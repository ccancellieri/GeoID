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

``MonitoringSignalConfig`` (below) governs the separate, slower
platform-metrics provider — its own poll cadence and on/off switch — while
the CPU/memory decision thresholds it feeds live on ``ScalingPolicyConfig``
alongside every other threshold ``compute_desired_min`` reads.
"""

from __future__ import annotations

from typing import ClassVar, Tuple

from pydantic import Field

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig


class ScalingPolicyConfig(PluginConfig):
    """Thresholds and budget for the protocol-driven scaling control loop.

    Cost-at-idle is a first-class design constraint here, not an
    afterthought (#2333): scale-OUT is fast (short ``scale_out_cooldown_
    seconds``) because it protects the SLA, but scale-IN is also prompt —
    just damped enough (``scale_in_cooldown_seconds`` / ``scale_in_step``)
    to avoid flapping right at the deadband boundary — because every idle
    minute spent above the floor load actually needs is pure cost. A dev
    load test found Cloud Run held 8 instances for ~25 minutes after load
    stopped under the old symmetric cooldown; see the two ``scale_in_*``
    fields below for the fix. The companion actuator — per-instance
    PostgreSQL pool depth — ratchets down the same way for free: keep
    ``DBConfig.pool_min_size`` near its floor and let SQLAlchemy's own
    overflow mechanism grow connections under load and close them on
    check-in once idle (see the note beside ``SAFE_POOL_MIN_FLOOR`` in
    ``db_config.py``). Both actuators are bounded by the same
    ``db_max_connections - connection_headroom`` budget.
    """

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
        description=(
            "Floor for the platform's min-instance-count lever. Keep this "
            "at the cheapest value the deployment can tolerate (0 where "
            "cold-start latency is acceptable) — the control loop only "
            "raises it under genuine DB pressure and ratchets back down "
            "promptly via ``scale_in_cooldown_seconds`` / ``scale_in_step`` "
            "once that pressure clears."
        ),
    )
    min_instances_floor: Mutable[int] = Field(
        default=1, ge=1,
        description="Hard floor the reconciler will never actuate below, independent of min_replicas. Guarantees the service never scales to zero once the loop is enabled.",
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
        default=20,
        ge=1,
        description=(
            "Worst-case PostgreSQL connections a single Cloud Run instance "
            "can hold open: the deployed ``DB_POOL_MAX_SIZE`` (per Gunicorn "
            "worker process — each worker builds its own SQLAlchemy engine "
            "via ``db_service.create_async_engine``) times ``GUNICORN_"
            "WORKERS``. Default 20 = the catalog service's dev-tier deploy "
            "values (``DB_POOL_MAX_SIZE=10`` x ``GUNICORN_WORKERS=2``, see "
            "the dynastore repo's ``.github/config/apps.dev.yml``) — confirm "
            "against the actual deployed values for the service this policy "
            "governs, they differ per environment and per app. This is the "
            "figure the ``db_pool_acquire slow`` telemetry (``dynastore."
            "modules.db_config.query_executor``) ultimately measures "
            "contention against, NOT the DuckDB driver's in-process "
            "connection pool (which never opens a PostgreSQL connection and "
            "so does not consume this budget). Deliberately uses the worst-"
            "case ceiling (``pool_max_size``), not the steady-state base "
            "(``pool_min_size``), so the budget guard stays safe even when "
            "every worker is bursting at once. Update this whenever "
            "``DB_POOL_MAX_SIZE`` or ``GUNICORN_WORKERS`` changes — a stale "
            "value lets the budget ceiling under-count real usage and pin "
            "more instances than Postgres can actually serve."
        ),
    )
    db_max_connections: Mutable[int] = Field(
        default=1000,
        ge=1,
        description=(
            "The actual ceiling on simultaneous PostgreSQL connections — the "
            "server's own ``max_connections`` GUC, or the connection cap of "
            "whatever transaction-mode pooler (AlloyDB Managed Connection "
            "Pooling / PgBouncer) fronts it, whichever is lower. Mirrored "
            "here so the budget ceiling can be computed without a live DB "
            "round trip. NOT the same figure as ``DBConfig.pool_max_size`` "
            "(a single Gunicorn worker's own burst ceiling). Default 1000 "
            "is the MEASURED ceiling of the dev/review AlloyDB instance "
            "(``fao-maps-review-alloydb-primary``, project "
            "``fao-maps-review``, reached over PSC) — confirm against the "
            "actual instance backing whatever environment this policy "
            "governs before relying on this default; a different Postgres "
            "instance (e.g. production) will have its own ceiling."
        ),
    )
    connection_headroom: Mutable[int] = Field(
        default=800,
        ge=0,
        description=(
            "Connections reserved for non-scaled consumers (migrations, "
            "admin tooling, other services) and subtracted from "
            "``db_max_connections`` before deriving the budget ceiling. "
            "Default 800 reserves for the co-tenants measured sharing "
            "``fao-maps-review-alloydb-primary`` alongside geoid's own "
            "scaled service: fao-maps-review's own workload plus the "
            "dynastore dev/review + fao-aip-catalog-review services (maps "
            "~81, catalog ~51, auth ~25, tools ~18, dynastore ~11 "
            "connections observed at measurement time — ~186 total, well "
            "under this 800 reservation to leave generous burst margin for "
            "all of them, not just their measured snapshot). Leaves geoid "
            "a computed budget of ``1000 - 800 = 200`` connections — still "
            "~2.5x geoid's own transaction-pooler-capped backend maximum "
            "(``max_pool_size`` per (db, user) pair, raised 50 -> 80 "
            "alongside this change — see the dynastore repo's PR #438), so "
            "200 is a safe soft cap here, not the binding constraint; the "
            "80-connection pooler cap binds first. This cross-consumer "
            "arithmetic is a LOWER BOUND — fao-maps-review's own budget "
            "requirement was not separately measured this session."
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
    scale_out_cooldown_seconds: Mutable[int] = Field(
        default=60,
        ge=0,
        description=(
            "Minimum time between two consecutive scale-OUT actuations "
            "(raising min-instance-count). Kept short by default — this is "
            "the SLA-protecting direction, and the fleet-MAX trigger "
            "(``scale_out_saturation``) already only fires when at least "
            "one instance is genuinely hot, so a short cooldown here mostly "
            "guards against actuating on every single tick rather than "
            "against a false trigger."
        ),
    )
    scale_in_cooldown_seconds: Mutable[int] = Field(
        default=120,
        ge=0,
        description=(
            "Minimum time between two consecutive scale-IN actuations "
            "(lowering min-instance-count). Deliberately longer than "
            "``scale_out_cooldown_seconds`` — asymmetric on purpose — so a "
            "brief dip right at the ``scale_out_saturation - deadband`` "
            "boundary doesn't flap the floor, but far shorter than the old "
            "single symmetric cooldown (300s) this field replaced. That "
            "300s cooldown, combined with a 1-instance ``step``, meant "
            "ratcheting a floor of 8 back down to 0 took up to ~40 minutes "
            "of sequential cooldown windows — this is exactly what a dev "
            "load test observed (Cloud Run held 8 instances for ~25 "
            "minutes after load stopped, accruing idle cost the whole "
            "time). Cost accrues every idle minute spent above the floor "
            "load actually needs, so scale-in must ratchet down PROMPTLY "
            "once pressure clears — not merely eventually. Tune down "
            "further (even to 0) if the fleet's saturation signal is "
            "stable enough that flapping isn't a real risk in your "
            "deployment."
        ),
    )
    step: Mutable[int] = Field(
        default=1,
        ge=1,
        description=(
            "Instances added per scale-OUT actuation. Scale-IN uses "
            "``scale_in_step`` instead (a larger default) so the floor "
            "ratchets back down to the cheap baseline in a handful of "
            "ticks rather than one instance at a time."
        ),
    )
    scale_in_step: Mutable[int] = Field(
        default=2,
        ge=1,
        description=(
            "Instances removed per scale-IN actuation. Larger than "
            "``step`` by default (asymmetric) — combined with the shorter "
            "``scale_in_cooldown_seconds``, this is what turns a slow "
            "one-at-a-time drain into a prompt ratchet back to "
            "``min_replicas`` once load clears, so idle cost doesn't "
            "linger at a stale-high floor."
        ),
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

    cpu_scale_out_ceiling: Mutable[float] = Field(
        default=0.65,
        ge=0.0,
        le=1.0,
        description=(
            "Fleet-wide CPU utilization (``scope='global'``, e.g. from "
            "``MonitoringSignalProvider``) at or above which scale-out is "
            "warranted even when no per-instance pool is saturated yet — "
            "the compute-bound case a connection-pool-only signal misses."
        ),
    )
    cpu_idle_ceiling: Mutable[float] = Field(
        default=0.30,
        ge=0.0,
        le=1.0,
        description=(
            "Fleet-wide CPU utilization below which the fleet is considered "
            "compute-idle. Gates two decisions: (1) when the per-instance "
            "pool IS saturated but CPU is below this ceiling, scale-out is "
            "held — more instances would not relieve a bottleneck that isn't "
            "compute, so the correct lever is a deeper PG pool, not more "
            "replicas; (2) reinforces scale-in — a cool pool is trusted to "
            "mean genuinely low load only when CPU is also idle."
        ),
    )
    memory_recommendation_ceiling: Mutable[float] = Field(
        default=0.85,
        ge=0.0,
        le=1.0,
        description=(
            "Fleet-wide memory utilization at or above which the reconciler "
            "logs a recommendation to raise the Cloud Run memory limit. "
            "Memory is a revision-roll actuator (cold start on every "
            "instance) — never actuated automatically per-tick, only "
            "surfaced for an operator to act on."
        ),
    )

    duckdb_pool_autosize: Mutable[bool] = Field(
        default=False,
        description=(
            "Second actuator for the same 'pool saturated but CPU idle' "
            "hold branch above (#2333): instead of only holding min_instances, "
            "deepen the DuckDB driver's connection pool by writing a bounded "
            "bump to ``DuckdbEngineConfig.pool_size``. The config write "
            "propagates to every instance via the existing hot-reload path — "
            "no separate fan-out plumbing. Off by default: only fires when "
            "the saturated instance-scope pool signal is specifically "
            "``source='duckdb_pool'`` (see ``DuckDbPoolSignalProvider``), so "
            "a deployment with no DuckDB-routed reads never actuates even "
            "when enabled."
        ),
    )
    duckdb_pool_size_max: Mutable[int] = Field(
        default=32,
        ge=1,
        le=64,
        description=(
            "Ceiling the autosize actuator will not deepen "
            "``DuckdbEngineConfig.pool_size`` past. Bounded by the same "
            "``le=64`` the field itself enforces; keep at or below the "
            "instance's CPU/memory headroom (``threads`` / ``max_memory_gb`` "
            "x pool_size, see ``DuckdbEngineConfig``)."
        ),
    )
    duckdb_pool_step: Mutable[int] = Field(
        default=2,
        ge=1,
        description="Connections added per DuckDB pool-autosize actuation.",
    )
    duckdb_pool_cooldown_seconds: Mutable[int] = Field(
        default=120,
        ge=0,
        description=(
            "Minimum time between two consecutive DuckDB pool-autosize "
            "actuations — same cooldown idiom as ``scale_in_cooldown_"
            "seconds``, damping repeated bumps while the pool is draining "
            "into its new capacity."
        ),
    )


class MonitoringSignalConfig(PluginConfig):
    """Tunables for ``MonitoringSignalProvider`` — the slow, corroborating
    platform-metrics tier (CPU/memory utilization) feeding the #2333
    control loop alongside the fast in-process pool signals.

    Kept separate from ``ScalingPolicyConfig`` because these knobs govern
    the *provider's own polling* (whether it polls at all, how often, over
    what lookback window), not the control loop's decision thresholds
    (which live in ``ScalingPolicyConfig`` and apply regardless of which
    backend produced the ``cpu_utilization`` / ``memory_utilization``
    signals).
    """

    _address: ClassVar[Tuple[str, ...]] = ("platform", "modules", "scaling", "monitoring_signal")

    enabled: Mutable[bool] = Field(
        default=False,
        description=(
            "Master switch for the platform-metrics provider. Off by "
            "default: querying a metrics API costs API calls and requires "
            "a service account with monitoring-viewer access, so it stays "
            "opt-in even when ``ScalingPolicyConfig.enabled`` is on."
        ),
    )
    poll_interval_seconds: Mutable[int] = Field(
        default=60,
        ge=30,
        description=(
            "Cadence of the provider's own poll against the metrics "
            "backend. Floored at 30s (the reconciler's own cadence) — "
            "metrics lag 1-3 minutes on most platforms, so polling faster "
            "than that only adds API cost without fresher data."
        ),
    )
    window_seconds: Mutable[float] = Field(
        default=120.0,
        ge=60.0,
        description=(
            "Lookback window queried on each poll. Must comfortably cover "
            "the metrics backend's own reporting lag so a poll always finds "
            "at least one aligned point."
        ),
    )
