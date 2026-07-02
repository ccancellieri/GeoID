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

"""Unit tests for the pure autoscaling decision function ``compute_desired_min``.

No I/O, no fixtures beyond plain construction — these exercise the algorithm
described in ``dynastore.modules.scaling.aggregator.compute_desired_min``'s
docstring directly.
"""

from __future__ import annotations

from dynastore.models.scaling import ScalingSignal
from dynastore.modules.scaling.aggregator import compute_desired_min, compute_duckdb_pool_bump
from dynastore.modules.scaling.config import ScalingPolicyConfig


def _policy(**overrides) -> ScalingPolicyConfig:
    return ScalingPolicyConfig(**overrides)


def _instance_signal(value: float, source: str = "duckdb_pool") -> ScalingSignal:
    return ScalingSignal(
        source=source, metric="pool_saturation", value=value, scope="instance", ts=0.0
    )


def _global_signal(value: float, metric: str = "conn_pressure") -> ScalingSignal:
    return ScalingSignal(
        source="db_contention_monitor", metric=metric, value=value, scope="global", ts=0.0
    )


def test_scale_out_when_fleet_max_at_or_above_saturation():
    policy = _policy(min_replicas=0, max_replicas=10, scale_out_saturation=0.80, step=1)
    signals = [_instance_signal(0.9)]

    result = compute_desired_min(
        signals, policy, current_min=2, last_change_ts=0.0, now=1000.0
    )

    assert result == 3


def test_no_scale_out_when_global_conn_pressure_guard_tripped():
    """Guard (#3 in the docstring): fleet is hot but the DB is already the
    bottleneck fleet-wide — scale-out must be suppressed."""
    policy = _policy(
        min_replicas=0, max_replicas=10, scale_out_saturation=0.80,
        conn_pressure_ceiling=0.80, step=1,
    )
    signals = [_instance_signal(0.95), _global_signal(0.85)]

    result = compute_desired_min(
        signals, policy, current_min=2, last_change_ts=0.0, now=1000.0
    )

    assert result == 2


def test_budget_clamp_never_exceeds_connection_budget_even_with_huge_max_replicas():
    """Budget ceiling (#4): the result never exceeds
    ``(db_max_connections - connection_headroom) // per_instance_pool``,
    regardless of ``max_replicas``."""
    policy = _policy(
        min_replicas=0,
        max_replicas=1000,
        scale_out_saturation=0.80,
        per_instance_pool=4,
        db_max_connections=100,
        connection_headroom=20,
        step=5,
    )
    budget_ceiling = (100 - 20) // 4  # 20
    signals = [_instance_signal(0.95)]

    result = compute_desired_min(
        signals, policy, current_min=19, last_change_ts=0.0, now=1000.0
    )

    assert result == budget_ceiling


def test_deadband_is_a_no_op():
    """Inside the deadband gap (neither hot enough to scale out nor cool
    enough to scale in) — the current value is held."""
    policy = _policy(
        min_replicas=0, max_replicas=10, scale_out_saturation=0.80, deadband=0.10, step=1,
    )
    # 0.75 < 0.80 (no scale-out) and 0.75 >= 0.80 - 0.10 = 0.70 (no scale-in).
    signals = [_instance_signal(0.75)]

    result = compute_desired_min(
        signals, policy, current_min=4, last_change_ts=0.0, now=1000.0
    )

    assert result == 4


def test_scale_out_cooldown_blocks_change_within_window():
    policy = _policy(
        min_replicas=0, max_replicas=10, scale_out_saturation=0.80, step=1,
        scale_out_cooldown_seconds=300,
    )
    signals = [_instance_signal(0.9)]

    result = compute_desired_min(
        signals, policy, current_min=2, last_change_ts=900.0, now=1000.0,  # 100s ago
    )

    assert result == 2


def test_scale_out_change_allowed_once_cooldown_elapses():
    policy = _policy(
        min_replicas=0, max_replicas=10, scale_out_saturation=0.80, step=1,
        scale_out_cooldown_seconds=300,
    )
    signals = [_instance_signal(0.9)]

    result = compute_desired_min(
        signals, policy, current_min=2, last_change_ts=600.0, now=1000.0,  # 400s ago
    )

    assert result == 3


def test_scale_in_cooldown_blocks_change_within_window():
    policy = _policy(
        min_replicas=0, max_replicas=10, scale_out_saturation=0.80, deadband=0.10,
        scale_in_step=2, scale_in_cooldown_seconds=120,
        per_instance_pool=1, db_max_connections=1000, connection_headroom=0,
    )
    signals = [_instance_signal(0.1)]

    result = compute_desired_min(
        signals, policy, current_min=8, last_change_ts=950.0, now=1000.0,  # 50s ago
    )

    assert result == 8


def test_scale_in_change_allowed_once_its_shorter_cooldown_elapses():
    """Asymmetric cooldown: scale-in's own (shorter) window has elapsed even
    though it's well inside what a symmetric 300s cooldown would have
    blocked — this is the fix for the #2333 ~25-minute stale-high-floor
    finding. Budget kept generous (non-binding) so only the cooldown/step
    behaviour is under test."""
    policy = _policy(
        min_replicas=0, max_replicas=10, scale_out_saturation=0.80, deadband=0.10,
        scale_in_step=2, scale_in_cooldown_seconds=120,
        per_instance_pool=1, db_max_connections=1000, connection_headroom=0,
    )
    signals = [_instance_signal(0.1)]

    result = compute_desired_min(
        signals, policy, current_min=8, last_change_ts=850.0, now=1000.0,  # 150s ago
    )

    assert result == 6


def test_scale_in_step_defaults_larger_than_scale_out_step():
    """Asymmetric by default: draining the floor moves faster than raising it."""
    policy = ScalingPolicyConfig()
    assert policy.scale_in_step > policy.step


def test_scale_in_cooldown_defaults_shorter_than_old_symmetric_value():
    """The replaced single ``cooldown_seconds`` defaulted to 300s; scale-in's
    own cooldown must default well under that so the floor ratchets down
    promptly once load clears (not the ~25-minute hold observed in the dev
    load test)."""
    policy = ScalingPolicyConfig()
    assert policy.scale_in_cooldown_seconds < 300
    assert policy.scale_out_cooldown_seconds <= policy.scale_in_cooldown_seconds


def test_scale_in_when_fleet_p95_below_deadband_threshold():
    policy = _policy(
        min_replicas=0, max_replicas=10, scale_out_saturation=0.80, deadband=0.10,
        scale_in_step=1,
    )
    signals = [_instance_signal(0.5), _instance_signal(0.5)]

    result = compute_desired_min(
        signals, policy, current_min=4, last_change_ts=0.0, now=1000.0
    )

    assert result == 3


def test_scale_in_clamps_to_min_replicas():
    policy = _policy(
        min_replicas=1, max_replicas=10, scale_out_saturation=0.80, deadband=0.10,
        scale_in_step=5,
    )
    signals = [_instance_signal(0.1)]

    result = compute_desired_min(
        signals, policy, current_min=2, last_change_ts=0.0, now=1000.0
    )

    assert result == 1


def test_no_instance_signals_holds_current_not_drains_to_min():
    """Insufficient-data guard: with zero fresh instance-scope signals (cache
    blip, all-stale fleet, or no instance-scope provider registered) the
    controller must HOLD the current floor, not read the absence as 0.0 and
    scale in every tick down to min_replicas."""
    policy = _policy(
        min_replicas=0, max_replicas=10, scale_out_saturation=0.80, deadband=0.10, step=1,
    )

    # No signals at all.
    assert compute_desired_min(
        [], policy, current_min=5, last_change_ts=0.0, now=1000.0
    ) == 5

    # Only a global signal present (no instance-scope readings) — still hold.
    assert compute_desired_min(
        [_global_signal(0.10)], policy, current_min=5, last_change_ts=0.0, now=1000.0
    ) == 5


def test_pool_saturated_but_cpu_idle_holds_prefers_pool_depth():
    """Actuator selection (#2333 monitoring signal): the pool looks hot but
    the fleet is compute-idle — adding instances would not relieve a
    bottleneck that isn't CPU. Hold; a deeper PG pool is the right lever."""
    policy = _policy(
        min_replicas=0, max_replicas=10, scale_out_saturation=0.80,
        cpu_idle_ceiling=0.30, step=1,
    )
    signals = [_instance_signal(0.95, source="pg_pool"), _global_signal(0.15, metric="cpu_utilization")]

    result = compute_desired_min(
        signals, policy, current_min=2, last_change_ts=0.0, now=1000.0
    )

    assert result == 2


def test_cpu_high_without_pool_saturation_still_scales_out():
    """Actuator selection: compute-bound fleet the pool-only signal would
    miss (pool well under saturation) still triggers scale-out."""
    policy = _policy(
        min_replicas=0, max_replicas=10, scale_out_saturation=0.80,
        cpu_scale_out_ceiling=0.65, step=1,
    )
    signals = [_instance_signal(0.5), _global_signal(0.70, metric="cpu_utilization")]

    result = compute_desired_min(
        signals, policy, current_min=2, last_change_ts=0.0, now=1000.0
    )

    assert result == 3


def test_cpu_scale_out_suppressed_by_conn_pressure_guard_too():
    """The DB-pressure guard suppresses scale-out regardless of which
    signal (pool saturation or CPU) is driving it."""
    policy = _policy(
        min_replicas=0, max_replicas=10, scale_out_saturation=0.80,
        cpu_scale_out_ceiling=0.65, conn_pressure_ceiling=0.80, step=1,
    )
    signals = [
        _instance_signal(0.5),
        _global_signal(0.70, metric="cpu_utilization"),
        _global_signal(0.85, metric="conn_pressure"),
    ]

    result = compute_desired_min(
        signals, policy, current_min=2, last_change_ts=0.0, now=1000.0
    )

    assert result == 2


def test_cool_pool_but_confirmed_hot_cpu_does_not_scale_in():
    """A cool connection pool alone must not be read as "low load" when CPU
    is confirmed high — the pool being idle doesn't mean the fleet is. With no
    DB-pressure guard, a compute-bound fleet scales OUT on the CPU signal the
    pool alone would miss, and never scales IN."""
    policy = _policy(
        min_replicas=0, max_replicas=10, scale_out_saturation=0.80,
        deadband=0.10, cpu_scale_out_ceiling=0.65, step=1,
    )
    signals = [_instance_signal(0.1), _global_signal(0.90, metric="cpu_utilization")]

    result = compute_desired_min(
        signals, policy, current_min=4, last_change_ts=0.0, now=1000.0
    )

    assert result == 5


def test_no_cpu_signal_falls_back_to_pool_only_algorithm():
    """Absent a monitoring backend entirely, the controller runs the pool-only
    path — the CPU branches are skipped when no CPU signal is present, and a
    cool p95 pool scales in by the asymmetric ``scale_in_step`` (2)."""
    policy = _policy(min_replicas=0, max_replicas=10, scale_out_saturation=0.80, deadband=0.10, step=1)
    signals = [_instance_signal(0.5)]

    result = compute_desired_min(
        signals, policy, current_min=4, last_change_ts=0.0, now=1000.0
    )

    assert result == 2


def test_scale_out_clamps_to_effective_max_replicas():
    policy = _policy(
        min_replicas=0, max_replicas=5, scale_out_saturation=0.80,
        per_instance_pool=1, db_max_connections=1000, connection_headroom=0,
        step=10,
    )
    signals = [_instance_signal(0.99)]

    result = compute_desired_min(
        signals, policy, current_min=4, last_change_ts=0.0, now=1000.0
    )

    assert result == 5  # effective_max caps at max_replicas since budget (1000) >> 5


# ---------------------------------------------------------------------------
# compute_duckdb_pool_bump — the second actuator for the same hold branch
# ---------------------------------------------------------------------------


def test_duckdb_pool_bump_off_by_default():
    """``duckdb_pool_autosize`` defaults False — inert even when every other
    condition for a bump is met."""
    policy = _policy(scale_out_saturation=0.80, cpu_idle_ceiling=0.30)
    signals = [_instance_signal(0.95, source="duckdb_pool"), _global_signal(0.1, metric="cpu_utilization")]

    result = compute_duckdb_pool_bump(
        signals, policy, current_pool_size=8, last_pool_change_ts=0.0, now=1000.0
    )

    assert result is None


def test_duckdb_pool_bump_fires_when_saturated_and_cpu_idle():
    """Mirrors the same condition ``compute_desired_min`` uses to hold
    min_instances — but here it actuates the pool depth instead."""
    policy = _policy(
        scale_out_saturation=0.80, cpu_idle_ceiling=0.30,
        duckdb_pool_autosize=True, duckdb_pool_step=2, duckdb_pool_size_max=32,
    )
    signals = [_instance_signal(0.95, source="duckdb_pool"), _global_signal(0.1, metric="cpu_utilization")]

    result = compute_duckdb_pool_bump(
        signals, policy, current_pool_size=8, last_pool_change_ts=0.0, now=1000.0
    )

    assert result == 10


def test_duckdb_pool_bump_ignores_non_duckdb_pool_sources():
    """A hot PG pool must never deepen the DuckDB pool — only
    ``source='duckdb_pool'`` instance signals count."""
    policy = _policy(
        scale_out_saturation=0.80, cpu_idle_ceiling=0.30, duckdb_pool_autosize=True,
    )
    signals = [_instance_signal(0.95, source="pg_pool"), _global_signal(0.1, metric="cpu_utilization")]

    result = compute_duckdb_pool_bump(
        signals, policy, current_pool_size=8, last_pool_change_ts=0.0, now=1000.0
    )

    assert result is None


def test_duckdb_pool_bump_requires_confirmed_cpu_idle():
    """Absent (or hot) CPU is NOT the hold branch — the pool-only algorithm
    already scales instances out in that case, so the pool actuator must
    stay out of the way."""
    policy = _policy(
        scale_out_saturation=0.80, cpu_idle_ceiling=0.30, duckdb_pool_autosize=True,
    )
    saturated = _instance_signal(0.95, source="duckdb_pool")

    # No CPU signal at all.
    assert compute_duckdb_pool_bump(
        [saturated], policy, current_pool_size=8, last_pool_change_ts=0.0, now=1000.0
    ) is None

    # CPU confirmed hot, not idle.
    hot = _global_signal(0.90, metric="cpu_utilization")
    assert compute_duckdb_pool_bump(
        [saturated, hot], policy, current_pool_size=8, last_pool_change_ts=0.0, now=1000.0
    ) is None


def test_duckdb_pool_bump_not_saturated_holds():
    policy = _policy(scale_out_saturation=0.80, cpu_idle_ceiling=0.30, duckdb_pool_autosize=True)
    signals = [_instance_signal(0.5, source="duckdb_pool"), _global_signal(0.1, metric="cpu_utilization")]

    result = compute_duckdb_pool_bump(
        signals, policy, current_pool_size=8, last_pool_change_ts=0.0, now=1000.0
    )

    assert result is None


def test_duckdb_pool_bump_respects_max_cap():
    policy = _policy(
        scale_out_saturation=0.80, cpu_idle_ceiling=0.30,
        duckdb_pool_autosize=True, duckdb_pool_size_max=8,
    )
    signals = [_instance_signal(0.95, source="duckdb_pool"), _global_signal(0.1, metric="cpu_utilization")]

    result = compute_duckdb_pool_bump(
        signals, policy, current_pool_size=8, last_pool_change_ts=0.0, now=1000.0
    )

    assert result is None


def test_duckdb_pool_bump_clamps_step_to_max_cap():
    policy = _policy(
        scale_out_saturation=0.80, cpu_idle_ceiling=0.30,
        duckdb_pool_autosize=True, duckdb_pool_step=10, duckdb_pool_size_max=12,
    )
    signals = [_instance_signal(0.95, source="duckdb_pool"), _global_signal(0.1, metric="cpu_utilization")]

    result = compute_duckdb_pool_bump(
        signals, policy, current_pool_size=8, last_pool_change_ts=0.0, now=1000.0
    )

    assert result == 12


def test_duckdb_pool_bump_respects_cooldown():
    policy = _policy(
        scale_out_saturation=0.80, cpu_idle_ceiling=0.30,
        duckdb_pool_autosize=True, duckdb_pool_cooldown_seconds=120,
    )
    signals = [_instance_signal(0.95, source="duckdb_pool"), _global_signal(0.1, metric="cpu_utilization")]

    result = compute_duckdb_pool_bump(
        signals, policy, current_pool_size=8, last_pool_change_ts=950.0, now=1000.0
    )

    assert result is None


def test_duckdb_pool_bump_allowed_once_cooldown_elapses():
    policy = _policy(
        scale_out_saturation=0.80, cpu_idle_ceiling=0.30,
        duckdb_pool_autosize=True, duckdb_pool_step=2, duckdb_pool_cooldown_seconds=120,
    )
    signals = [_instance_signal(0.95, source="duckdb_pool"), _global_signal(0.1, metric="cpu_utilization")]

    result = compute_duckdb_pool_bump(
        signals, policy, current_pool_size=8, last_pool_change_ts=800.0, now=1000.0
    )

    assert result == 10
