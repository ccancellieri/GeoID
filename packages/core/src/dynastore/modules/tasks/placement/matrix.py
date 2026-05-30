"""Pure resolver of the default placement matrix.

Shared by config default-materialization and the deployment presets. Profile-
independent: cloud vs onprem differ only in how off_load resolves to a concrete
runner, not in these matrix values. System tasks (kind="task") run in-process
(async) on their owning tier; processes (kind="process") off_load to worker.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

from dynastore.modules.tasks.placement.model import ASYNC, OFF_LOAD, PlacementEntry


@dataclass(frozen=True)
class InventoryItem:
    task_key: str
    kind: str                       # "process" | "task"
    affinity_tier: Optional[str]    # e.g. "catalog"; None for tier-agnostic


# Consumers for processes, ported verbatim from the retired routing JSON
# (all five full matrices were identical). Default consumer for any process not
# listed is "worker".
_PROCESS_CONSUMERS: Dict[str, List[str]] = {
    "elasticsearch_indexer": ["worker"],
    "ingestion": ["worker"],
    "gdal": ["worker"],
    "tiles_preseed": ["worker"],
    "dwh_join": ["worker"],
    "export_features": ["worker"],
    "dimensions_materialize": ["worker"],
}
_DEFAULT_PROCESS_CONSUMERS = ["worker"]


def build_placement_matrix(inventory: Iterable[InventoryItem]) -> Dict[str, PlacementEntry]:
    matrix: Dict[str, PlacementEntry] = {}
    for item in inventory:
        if item.kind == "task":
            tier = item.affinity_tier or "catalog"
            matrix[item.task_key] = PlacementEntry(consumers=[tier], mode=ASYNC)
        else:
            consumers = _PROCESS_CONSUMERS.get(item.task_key, list(_DEFAULT_PROCESS_CONSUMERS))
            matrix[item.task_key] = PlacementEntry(consumers=consumers, mode=OFF_LOAD)
    return matrix
