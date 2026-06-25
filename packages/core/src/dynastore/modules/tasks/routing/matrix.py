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
from typing import Dict, Iterable, List, Optional, Tuple

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

# Processes that must stay in-process even under the cloud profile
# (lightweight enough that spinning up a Cloud Run Job would be wasteful).
# ``tiles_invalidate`` is the light delete-only cache-invalidation path;
# it runs as an in-process background task on the catalog service rather
# than a Cloud Run Job.
LIGHTWEIGHT_PROCESSES: frozenset = frozenset(
    {"requeue_dead_letter_tasks", "tiles_invalidate"}
)

# System tasks (kind="task") that MAY offload to a Cloud Run Job when the
# in-process dispatcher is under load.  These route like any other system
# task — a single ``background`` target — because the offload decision is
# dynamic, not static: a dispatcher-local load probe (see execution.py
# ``_should_offload_provisioning``) flips the task to an offload runner only
# when the in-process pool is at/above threshold, and the restriction acts on
# the registered runners, not on the routing target.  Emitting a static
# gcp_cloud_run target with an OFFLOAD/HEAVY hint here would make
# ``offload_required`` true unconditionally and defeat the load-adaptive path.
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

    **System tasks** (``kind == "task"``), all presets:
        A single ``background`` entry whose ``consumers`` list contains the
        task's ``affinity_tier`` (falls back to ``"catalog"`` for
        tier-agnostic tasks).

    **Processes** (``kind == "process"``), cloud preset:
        Lightweight processes (``key in LIGHTWEIGHT_PROCESSES``) stay in-
        process: ``background`` runner, ``consumers=["catalog"]``,
        ``hints={BACKGROUND}``.
        All other processes offload to GCP Cloud Run Jobs: ``gcp_cloud_run``
        runner, ``consumers`` from ``CLOUD_PROCESS_CONSUMERS`` (defaulting
        to ``["catalog"]``), ``hints={OFFLOAD, HEAVY}``.  The ``options``
        dict is intentionally empty — job-name discovery is the runner's
        responsibility.

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
            # All system tasks route to a single in-process ``background``
            # target.  Offloadable ones (OFFLOADABLE_SYSTEM_TASKS) are flipped
            # to a Cloud Run Job dynamically at dispatch time by the load probe
            # in execution.py — never via a static OFFLOAD/HEAVY routing hint.
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
                    processes_map[item.task_key] = [
                        RunnerTarget(
                            consumers=list(consumers),
                            runner="gcp_cloud_run",
                            hints={ExecHint.OFFLOAD, ExecHint.HEAVY},
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
