"""The resolved default matrix: system tasks -> async on their affinity tier;
processes -> off_load on worker. No empty/null entries."""
from __future__ import annotations

from dynastore.modules.tasks.placement.matrix import build_placement_matrix, InventoryItem
from dynastore.modules.tasks.placement.model import OFF_LOAD, ASYNC


def _inv():
    return [
        InventoryItem(task_key="cascade_cleanup", kind="task", affinity_tier="catalog"),
        InventoryItem(task_key="index_propagation", kind="task", affinity_tier="catalog"),
        InventoryItem(task_key="gdal", kind="process", affinity_tier=None),
        InventoryItem(task_key="ingestion", kind="process", affinity_tier=None),
    ]


def test_system_task_is_async_on_affinity_tier():
    m = build_placement_matrix(_inv())
    assert m["cascade_cleanup"].mode == ASYNC
    assert m["cascade_cleanup"].consumers == ["catalog"]


def test_process_is_off_load_on_worker():
    m = build_placement_matrix(_inv())
    assert m["gdal"].mode == OFF_LOAD
    assert m["gdal"].consumers == ["worker"]


def test_every_task_has_a_concrete_entry():
    m = build_placement_matrix(_inv())
    assert set(m) == {"cascade_cleanup", "index_propagation", "gdal", "ingestion"}
    assert all(e.consumers and e.mode for e in m.values())
