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

"""Pure builder for the default task routing matrix.

Shared by ``TaskRoutingConfig._materialize_if_empty`` and the deployment
presets.  Profile-independent per-entry data lives here; only the runner
type and hint set differ between cloud and onprem profiles.

Two flavours:

* ``cloud``  — processes run as GCP Cloud Run Jobs (``gcp_cloud_run``
  runner) unless they are lightweight; lightweight processes and all
  system tasks stay in-process (``background``).
* ``onprem`` — every task and process runs in-process (``background``
  runner, never Cloud Run), routed to its natural affinity tier
  (``CLOUD_PROCESS_CONSUMERS``: ``gdal`` -> catalog/maps, ``tiles_*`` ->
  maps, the rest -> catalog). Suits a worker-less deployment with no Cloud
  Run Jobs offload target.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from dynastore.modules.tasks.routing.exec_hints import ExecHint
from dynastore.modules.tasks.routing.model import RunnerTarget


@dataclass(frozen=True)
class InventoryItem:
    """One entry from the live task registry, reduced to routing-relevant fields."""

    task_key: str
    kind: str                     # "process" | "task"
    affinity_tier: Optional[str]  # e.g. "catalog"; None for tier-agnostic


# Cloud topology — consumer lists for processes that fan to non-catalog services.
# Ported from the FAO production seed and the retired routing JSON.
# The DEFAULT for any process not listed here is ``["catalog"]``.
CLOUD_PROCESS_CONSUMERS: Dict[str, List[str]] = {
    "gdal": ["catalog", "maps"],
    "tiles_preseed": ["maps"],
    "tiles_export": ["maps"],
}

# Per-process Cloud Run Job task-timeout CEILINGS (seconds), written to the
# gcp_cloud_run RunnerTarget.options so a dense/huge preseed is not killed at
# the platform-default 3600s ceiling. This is only a ceiling — the Job still
# exits as soon as its work completes, so a larger value never delays a normal
# run; it only prevents premature termination of a legitimately long one.
# 86400s = Cloud Run's maximum Job task timeout (24h). Operators can override
# per-process via the platform routing config; for extents that need longer
# than one Job can run, partition by bbox and fan out (each partition is an
# independent, dedup-keyed tiles_preseed execution).
CLOUD_PROCESS_TIMEOUT_SECONDS: Dict[str, int] = {
    "tiles_preseed": 86400,
    "tiles_export": 86400,
}

# Processes that must stay in-process even under the cloud profile
# (lightweight enough that spinning up a Cloud Run Job would be wasteful).
# ``tiles_invalidate`` is the light delete-only cache-invalidation path;
# it runs as an in-process background task on the catalog service rather
# than a Cloud Run Job.
LIGHTWEIGHT_PROCESSES: frozenset = frozenset(
    {"requeue_dead_letter_tasks", "tiles_invalidate"}
)

# System tasks (kind="task") that offload to a Cloud Run Job under the cloud
# preset.  Under ``cloud``, these emit a ``gcp_cloud_run`` target with
# ``{ExecHint.OFFLOAD}`` so ``offload_required`` returns True and the existing
# ``_restrict_to_offload_runners`` guard drops BackgroundRunner, routing the
# task to the Cloud Run Job.  Under ``onprem`` they keep the standard
# ``background`` target (no Job exists in that topology).  The fail-open in
# ``_restrict_to_offload_runners`` preserves in-process execution when no
# offload runner is registered (e.g. the Job is not yet deployed).
OFFLOADABLE_SYSTEM_TASKS: frozenset = frozenset({"catalog_provision"})


def build_routing_matrix(
    inventory: Iterable[InventoryItem],
    preset: str = "cloud",
) -> Tuple[Dict[str, List[RunnerTarget]], Dict[str, List[RunnerTarget]]]:
    """Build the (tasks_map, processes_map) routing matrices for ``preset``.

    Args:
        inventory: Iterable of ``InventoryItem`` from the task registry.
        preset:    ``"onprem"`` for the in-process profile; any other value
                   (including the default ``"cloud"``) selects the cloud
                   profile, so a typo can never silently mis-route to onprem.

    Returns:
        A 2-tuple ``(tasks_map, processes_map)`` where each map is
        ``{task_key: [RunnerTarget, ...]}`` in application order.

    Selection semantics (per preset)
    ---------------------------------

    **System tasks** (``kind == "task"``), cloud preset:
        Tasks in ``OFFLOADABLE_SYSTEM_TASKS`` emit a ``gcp_cloud_run`` target
        with ``hints={OFFLOAD}`` so the dispatcher's ``offload_required``
        check returns True and ``_restrict_to_offload_runners`` routes to the
        Cloud Run Job.  All other system tasks use a single ``background``
        target on their ``affinity_tier`` (falls back to ``"catalog"``).

    **System tasks** (``kind == "task"``), onprem preset:
        All system tasks use a single ``background`` target on their
        ``affinity_tier`` (no Cloud Run Jobs in this topology).

    **Processes** (``kind == "process"``), cloud preset:
        Lightweight processes (``key in LIGHTWEIGHT_PROCESSES``) stay in-
        process: ``background`` runner, ``consumers=["catalog"]``,
        ``hints={BACKGROUND}``.
        All other processes offload to GCP Cloud Run Jobs: ``gcp_cloud_run``
        runner, ``consumers`` from ``CLOUD_PROCESS_CONSUMERS`` (defaulting
        to ``["catalog"]``), ``hints={OFFLOAD, HEAVY}``.  ``options`` carries
        a per-process ``timeout_seconds`` ceiling for the processes listed in
        ``CLOUD_PROCESS_TIMEOUT_SECONDS`` (heavy tile preseed) and is otherwise
        empty — job-name discovery is the runner's responsibility.

    **Processes** (``kind == "process"``), onprem preset:
        Every process runs in-process: ``background`` runner,
        ``hints={BACKGROUND}``, consumers taken from
        ``CLOUD_PROCESS_CONSUMERS`` (``gdal`` -> ``["catalog", "maps"]``,
        ``tiles_preseed``/``tiles_export`` -> ``["maps"]``, everything else
        -> ``["catalog"]``).  No process is routed to ``gcp_cloud_run`` and
        none is pinned to a dedicated ``"worker"`` tier — suits a worker-less
        deployment (a single fat node plus maps) with no Cloud Run Jobs.

    The former ``review`` preset has been retired: its only delta from cloud
    (an in-process ``gdal`` special-case on the catalog tier) was removed when
    gdal sync execution moved to the maps service (which ships osgeo +
    ``worker_task_gdal`` and registers a ``SyncRunner`` for ``Prefer:
    respond-sync``). A deployment still setting ``DYNASTORE_TASK_ROUTING_PRESET
    =review`` falls through to the cloud profile below.
    """
    tasks_map: Dict[str, List[RunnerTarget]] = {}
    processes_map: Dict[str, List[RunnerTarget]] = {}

    for item in inventory:
        if item.kind == "task":
            tier = item.affinity_tier or "catalog"
            if preset != "onprem" and item.task_key in OFFLOADABLE_SYSTEM_TASKS:
                # Cloud profile: heavy provisioning tasks offload to Cloud Run
                # Job so they never compete with request-serving on the catalog
                # pod.  ``offload_required`` reads this OFFLOAD hint at dispatch
                # time and ``_restrict_to_offload_runners`` drops BackgroundRunner
                # when a GcpJobRunner is present (fail-open when absent).
                tasks_map[item.task_key] = [
                    RunnerTarget(
                        consumers=[tier],
                        runner="gcp_cloud_run",
                        hints={ExecHint.OFFLOAD},
                    )
                ]
            else:
                tasks_map[item.task_key] = [
                    RunnerTarget(
                        consumers=[tier],
                        runner="background",
                        hints={ExecHint.BACKGROUND},
                    )
                ]
        else:
            # kind == "process"
            if preset != "onprem":
                # cloud profile — the default for any non-onprem value.
                if item.task_key in LIGHTWEIGHT_PROCESSES:
                    processes_map[item.task_key] = [
                        RunnerTarget(
                            consumers=["catalog"],
                            runner="background",
                            hints={ExecHint.BACKGROUND},
                        )
                    ]
                else:
                    consumers = CLOUD_PROCESS_CONSUMERS.get(item.task_key, ["catalog"])
                    _options: Dict[str, Any] = {}
                    _timeout = CLOUD_PROCESS_TIMEOUT_SECONDS.get(item.task_key)
                    if _timeout:
                        _options["timeout_seconds"] = _timeout
                    processes_map[item.task_key] = [
                        RunnerTarget(
                            consumers=list(consumers),
                            runner="gcp_cloud_run",
                            hints={ExecHint.OFFLOAD, ExecHint.HEAVY},
                            options=_options,
                        )
                    ]
            else:
                # onprem — in-process background on affinity tiers, never Cloud
                # Run. Mirrors the cloud CONSUMER topology
                # (CLOUD_PROCESS_CONSUMERS: gdal -> [catalog, maps], tiles_* ->
                # [maps], everything else -> [catalog]) but runs EVERY process
                # through the in-process ``background`` runner. Suits a
                # worker-less deployment — a single fat node (plus maps) with no
                # Cloud Run Jobs offload target — so nothing is routed to a
                # dedicated "worker" tier that need not exist. The
                # lightweight/heavy split is irrelevant here (all in-process), so
                # every process carries the same BACKGROUND hint.
                consumers = CLOUD_PROCESS_CONSUMERS.get(item.task_key, ["catalog"])
                processes_map[item.task_key] = [
                    RunnerTarget(
                        consumers=list(consumers),
                        runner="background",
                        hints={ExecHint.BACKGROUND},
                    )
                ]

    return tasks_map, processes_map
