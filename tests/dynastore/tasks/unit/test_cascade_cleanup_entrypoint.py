"""Regression: cascade_cleanup must be a discoverable, pinned worker task.

``CascadeCleanupTask`` is enqueued (schema=``system``) by the catalog
cascade orchestrator after a hard-delete to drain each resource owner's
``cleanup_one``. It is a fully-formed ``TaskProtocol`` with a ``run()``,
but it was never declared in the ``[project.entry-points."dynastore.tasks"]``
table, so ``discover_tasks()`` never imported the module, ``__init_subclass__``
never registered the class, and no service's ``CapabilityMap`` ever included
``cascade_cleanup``. Result: the row could never be claimed by any pod and
starved PENDING forever (retry_count=0, no DLQ path).

These pure-static checks (no app boot, no editable reinstall needed) pin the
two knobs that make the task claimable by exactly one capable consumer:

1. the entry-point declaration (so the class is discovered + registered), and
2. the placement to the ``catalog`` tier — the SAME tier that enqueues the row,
   so the registered cleanup owners match the snapshotted refs. A wrong-tier
   claim raises ``KeyError`` per ref → marks them DEAD → permanent loss.
"""
from __future__ import annotations

import tomllib
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[4]
_CORE_PYPROJECT = _REPO_ROOT / "packages" / "core" / "pyproject.toml"

_EXPECTED_TARGET = "dynastore.tasks.cascade_cleanup.task:CascadeCleanupTask"
_EXPECTED_CONSUMER = "catalog"


def test_cascade_cleanup_entrypoint_declared() -> None:
    """cascade_cleanup is declared in the dynastore.tasks entry-point group.

    Without this line ``discover_tasks()`` never loads the class on any
    service and the task is unclaimable by construction.
    """
    assert _CORE_PYPROJECT.is_file(), f"missing {_CORE_PYPROJECT}"
    data = tomllib.loads(_CORE_PYPROJECT.read_text())
    entry_points = (
        data.get("project", {})
        .get("entry-points", {})
        .get("dynastore.tasks", {})
    )
    assert "cascade_cleanup" in entry_points, (
        "cascade_cleanup is missing from [project.entry-points.\"dynastore.tasks\"] "
        "in packages/core/pyproject.toml. discover_tasks() loads task classes only "
        "from this group; without it the task is never registered and starves "
        "PENDING forever."
    )
    assert entry_points["cascade_cleanup"] == _EXPECTED_TARGET, (
        f"cascade_cleanup entry-point points at {entry_points['cascade_cleanup']!r}; "
        f"expected {_EXPECTED_TARGET!r}."
    )


def test_cascade_cleanup_placed_on_catalog_async() -> None:
    """cascade_cleanup must resolve to the catalog tier in async mode — the same
    tier that enqueues it, so the cleanup-owner registry matches. Pure-static via
    the matrix builder (no DB, no app boot).

    cascade_cleanup is a system task (kind="task") with affinity_tier="catalog",
    so the default placement matrix pins its consumer to the catalog tier and
    runs it in-process (async). Routing it elsewhere would mark every cleanup
    ref DEAD and lose the cleanup.
    """
    from dynastore.modules.tasks.placement.matrix import (
        InventoryItem,
        build_placement_matrix,
    )
    from dynastore.modules.tasks.placement.model import ASYNC

    matrix = build_placement_matrix([
        InventoryItem(task_key="cascade_cleanup", kind="task", affinity_tier="catalog"),
    ])
    entry = matrix["cascade_cleanup"]
    assert entry.consumers == [_EXPECTED_CONSUMER]
    assert entry.mode == ASYNC
