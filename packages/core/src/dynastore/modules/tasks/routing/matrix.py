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


def build_routing_matrix(
    inventory: Iterable[InventoryItem],
    preset: str = "cloud",
) -> Tuple[Dict[str, List[RunnerTarget]], Dict[str, List[RunnerTarget]]]:
    """Build the (tasks_map, processes_map) routing matrices for ``preset``.

    Args:
        inventory: Iterable of ``InventoryItem`` from the task registry.
        preset:    ``"cloud"`` (default), ``"onprem"``, or ``"review"``.

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

    **Processes** (``kind == "process"``), review preset:
        Mirrors the cloud preset exactly. The former ``gdal`` in-process
        special-case (``background`` runner on ``["catalog"]`` with
        ``{BACKGROUND, INTERACTIVE}`` hints) has been removed: gdal sync
        execution now lives on the maps service, which ships osgeo +
        ``worker_task_gdal`` and registers a ``SyncRunner`` that can handle
        it in-process when ``Prefer: respond-sync`` is sent.
    """
    tasks_map: Dict[str, List[RunnerTarget]] = {}
    processes_map: Dict[str, List[RunnerTarget]] = {}

    for item in inventory:
        if item.kind == "task":
            tier = item.affinity_tier or "catalog"
            tasks_map[item.task_key] = [
                RunnerTarget(
                    consumers=[tier],
                    runner="background",
                    hints={ExecHint.BACKGROUND},
                )
            ]
        else:
            # kind == "process"
            if preset in ("cloud", "review"):
                # review mirrors cloud exactly; gdal sync now lives on the maps service.
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
