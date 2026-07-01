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

"""Signal storage (Valkey-backed) and the scaling decision function.

Storage shape
-------------
A single cache document holds every live signal::

    {
      "instances": {"<instance_id>": {"ts": <float>, "signals": [<dict>, ...]}},
      "global":    {"<source>:<metric>": {"ts": <float>, "signal": <dict>}},
    }

One document (not one key per instance) because the generic
``CacheBackend`` protocol exposes only ``get``/``set``/``clear`` — no
key-pattern scan — so there is no way for the leader to discover an
unbounded set of per-instance keys. Every publish does a best-effort
read-merge-write; lost updates under concurrent writers self-heal on the
next publish cadence (the same instance republishes every tick), which is
acceptable for this soft, eventually-consistent telemetry.

``compute_desired_min`` is the pure decision function — no I/O, fully
unit-testable.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Sequence

from dynastore.models.scaling import ScalingSignal
from dynastore.modules.scaling.config import ScalingPolicyConfig

logger = logging.getLogger(__name__)

SIGNALS_CACHE_KEY = "scaling:signals"


# ---------------------------------------------------------------------------
# Cache document read/write
# ---------------------------------------------------------------------------


async def read_signals_document(backend: Any) -> Dict[str, Any]:
    """Best-effort read of the shared signals document. Never raises."""
    try:
        raw = await backend.get(SIGNALS_CACHE_KEY)
    except Exception:
        logger.debug("scaling: failed to read signals document", exc_info=True)
        return {"instances": {}, "global": {}}
    if not isinstance(raw, dict):
        return {"instances": {}, "global": {}}
    raw.setdefault("instances", {})
    raw.setdefault("global", {})
    return raw


async def write_instance_signals(
    backend: Any,
    instance_id: str,
    signals: Sequence[ScalingSignal],
    *,
    max_age_seconds: float,
) -> None:
    """Merge this instance's current signals into the shared document.

    Prunes instance entries older than ``max_age_seconds`` on write so the
    document does not grow unbounded as instances scale down. Best-effort:
    swallows cache errors.
    """
    now = time.time()
    try:
        doc = await read_signals_document(backend)
        instances = doc["instances"]
        instances = {
            iid: entry
            for iid, entry in instances.items()
            if isinstance(entry, dict) and now - float(entry.get("ts", 0)) <= max_age_seconds
        }
        instances[instance_id] = {
            "ts": now,
            "signals": [s.model_dump(mode="json") for s in signals],
        }
        doc["instances"] = instances
        await backend.set(SIGNALS_CACHE_KEY, doc, ttl=max_age_seconds)
    except Exception:
        logger.debug("scaling: failed to publish instance signals", exc_info=True)


async def write_global_signals(
    backend: Any,
    signals: Sequence[ScalingSignal],
    *,
    max_age_seconds: float,
) -> None:
    """Merge global-scope signals into the shared document, keyed by
    ``"<source>:<metric>"`` so distinct global metrics don't clobber each
    other. Only the pod currently producing a fresh value (e.g. the
    DbContentionMonitor leader) contributes a non-empty entry here.
    Best-effort: swallows cache errors.
    """
    if not signals:
        return
    now = time.time()
    try:
        doc = await read_signals_document(backend)
        existing = doc["global"]
        existing = {
            key: entry
            for key, entry in existing.items()
            if isinstance(entry, dict) and now - float(entry.get("ts", 0)) <= max_age_seconds
        }
        for s in signals:
            existing[f"{s.source}:{s.metric}"] = {
                "ts": now,
                "signal": s.model_dump(mode="json"),
            }
        doc["global"] = existing
        await backend.set(SIGNALS_CACHE_KEY, doc, ttl=max_age_seconds)
    except Exception:
        logger.debug("scaling: failed to publish global signals", exc_info=True)


def collect_live_signals(
    doc: Dict[str, Any], *, max_age_seconds: float, now: float
) -> List[ScalingSignal]:
    """Flatten the document into a list of fresh ``ScalingSignal``s.

    Drops entries older than ``max_age_seconds`` (dead/stalled instances)
    and tolerates malformed entries rather than raising.
    """
    out: List[ScalingSignal] = []
    for entry in doc.get("instances", {}).values():
        if not isinstance(entry, dict) or now - float(entry.get("ts", 0)) > max_age_seconds:
            continue
        for raw in entry.get("signals", []):
            try:
                out.append(ScalingSignal.model_validate(raw))
            except Exception:
                continue
    for entry in doc.get("global", {}).values():
        if not isinstance(entry, dict) or now - float(entry.get("ts", 0)) > max_age_seconds:
            continue
        raw = entry.get("signal")
        if raw is None:
            continue
        try:
            out.append(ScalingSignal.model_validate(raw))
        except Exception:
            continue
    return out


# ---------------------------------------------------------------------------
# Pure decision function
# ---------------------------------------------------------------------------


def _percentile(values: List[float], pct: float) -> float:
    """Nearest-rank percentile over ``values`` (already-sorted not required)."""
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = max(0, min(len(ordered) - 1, round(pct / 100.0 * (len(ordered) - 1))))
    return ordered[rank]


def effective_max_replicas(policy: ScalingPolicyConfig) -> int:
    """Budget-clamped ceiling: never lets the controller invent DB capacity."""
    budget_ceiling = (policy.db_max_connections - policy.connection_headroom) // max(
        1, policy.per_instance_pool
    )
    return max(policy.min_replicas, min(policy.max_replicas, max(0, budget_ceiling)))


def compute_desired_min(
    signals: Sequence[ScalingSignal],
    policy: ScalingPolicyConfig,
    *,
    current_min: int,
    last_change_ts: float,
    now: float,
) -> int:
    """Decide the desired ``min_instances`` value.

    Pure function — no I/O — so it is fully unit-testable.

    Algorithm
    ---------
    1. Per-instance reading: for each reporting instance, its own MAX
       signal value (its hottest metric).
    2. Fleet readings: MAX (OR-up — any one hot instance can trigger
       scale-out) and p95 (majority-down — most instances must be cool
       before scale-in) over the per-instance readings.
    3. Global guard: if the fleet-wide ``conn_pressure`` (scope="global")
       is at or above ``conn_pressure_ceiling``, scale-out is suppressed —
       the DB is already the bottleneck and more instances make it worse.
    4. Budget ceiling: the result never exceeds
       ``(db_max_connections - connection_headroom) // per_instance_pool``,
       regardless of ``max_replicas`` — the controller cannot invent DB
       capacity.
    5. Cooldown: a change is suppressed if the last actuated change was
       less than ``cooldown_seconds`` ago.
    """
    # ScalingSignal carries no instance id, so the fleet MAX/p95 are taken
    # directly over every instance-scope signal value collected this tick
    # (one figure per contribution, not strictly one per pod). OR-up only
    # needs "is any instance hot", which a flat MAX already answers
    # correctly; p95 over the same set is a reasonable proxy for "most
    # instances are cool" given a stable signal count across instances.
    instance_values = [s.value for s in signals if s.scope == "instance"]

    # Insufficient-data guard: with no fresh instance-scope readings this tick
    # (a cache blip, an all-stale fleet, or a deployment whose engines expose no
    # instance-scope signal provider) we cannot tell "every instance is cool"
    # from "we know nothing". Hold the current floor — otherwise the default-0.0
    # fleet reading below would take the scale-in branch every tick and silently
    # drain the service to min_replicas regardless of real load.
    if not instance_values:
        return current_min

    global_conn_pressure = max(
        (s.value for s in signals if s.scope == "global" and s.metric == "conn_pressure"),
        default=None,
    )

    fleet_max = max(instance_values, default=0.0)
    fleet_p95 = _percentile(instance_values, 95)

    effective_max = effective_max_replicas(policy)

    guard_tripped = (
        global_conn_pressure is not None and global_conn_pressure >= policy.conn_pressure_ceiling
    )

    target = current_min
    if fleet_max >= policy.scale_out_saturation:
        if not guard_tripped:
            target = current_min + policy.step
        # else: hold — guard suppresses scale-out.
    elif fleet_p95 < (policy.scale_out_saturation - policy.deadband):
        target = current_min - policy.step
    # else: inside the deadband — no change.

    target = max(policy.min_replicas, min(effective_max, target))

    if target != current_min and (now - last_change_ts) < policy.cooldown_seconds:
        target = current_min

    return target
