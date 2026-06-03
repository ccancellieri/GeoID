"""Unit tests for modules/tasks/routing — hint-driven task routing model.

Pure-static tests: no DB, no app boot, no real task registry needed.
Run with: uv run pytest --noconftest tests/dynastore/tasks/unit/test_routing.py -q
"""
from __future__ import annotations

import pytest
from typing import FrozenSet, List

from dynastore.modules.tasks.routing.exec_hints import ExecHint
from dynastore.modules.tasks.routing.matrix import InventoryItem, build_routing_matrix
from dynastore.modules.tasks.routing.model import (
    Action,
    ActionVerb,
    RunnerTarget,
    TaskRoutingConfig,
)
from dynastore.modules.tasks.routing.resolver import resolved_targets, select_target


# ---------------------------------------------------------------------------
# Action validation
# ---------------------------------------------------------------------------


def test_action_route_requires_process() -> None:
    """action=ROUTE without process raises ValueError."""
    with pytest.raises(ValueError, match="non-empty 'process'"):
        Action(action=ActionVerb.ROUTE, process=None)


def test_action_route_with_empty_process_raises() -> None:
    """action=ROUTE with empty-string process raises ValueError."""
    with pytest.raises(ValueError, match="non-empty 'process'"):
        Action(action=ActionVerb.ROUTE, process="")


def test_action_non_route_forbids_process() -> None:
    """action!=ROUTE with a non-None process raises ValueError."""
    with pytest.raises(ValueError, match="must be None"):
        Action(action=ActionVerb.REPORT, process="some_task")


def test_action_route_valid() -> None:
    """action=ROUTE with non-empty process is valid."""
    a = Action(action=ActionVerb.ROUTE, process="my_task")
    assert a.process == "my_task"


def test_action_report_default() -> None:
    """Default Action has action=REPORT and process=None."""
    a = Action()
    assert a.action == ActionVerb.REPORT
    assert a.process is None


def test_action_dead_letter_valid() -> None:
    """action=DEAD_LETTER without process is valid."""
    a = Action(action=ActionVerb.DEAD_LETTER)
    assert a.process is None


# ---------------------------------------------------------------------------
# ExecHint enum
# ---------------------------------------------------------------------------


def test_exec_hint_string_values() -> None:
    """ExecHint members equal their string values (StrEnum contract)."""
    assert ExecHint.BACKGROUND == "background"
    assert ExecHint.OFFLOAD == "offload"
    assert ExecHint.HEAVY == "heavy"
    assert ExecHint.SYNC == "sync"
    assert ExecHint.INTERACTIVE == "interactive"


def test_exec_hint_unknown_string_rejected() -> None:
    """An unknown string is not a valid ExecHint member."""
    with pytest.raises(ValueError):
        ExecHint("not_a_valid_hint")


# ---------------------------------------------------------------------------
# build_routing_matrix
# ---------------------------------------------------------------------------


def _make_inventory(
    tasks: list[tuple[str, str | None]] = (),
    processes: list[tuple[str, str | None]] = (),
) -> list[InventoryItem]:
    items = []
    for key, tier in tasks:
        items.append(InventoryItem(task_key=key, kind="task", affinity_tier=tier))
    for key, tier in processes:
        items.append(InventoryItem(task_key=key, kind="process", affinity_tier=tier))
    return items


def test_matrix_cloud_tasks_get_background() -> None:
    """System tasks always get background runner regardless of preset."""
    items = _make_inventory(tasks=[("my_task", "catalog"), ("other_task", None)])
    tasks_map, _procs_map = build_routing_matrix(items, "cloud")

    assert "my_task" in tasks_map
    t = tasks_map["my_task"][0]
    assert t.service == "catalog"
    assert t.runner == "background"
    assert ExecHint.BACKGROUND in t.hints

    t2 = tasks_map["other_task"][0]
    assert t2.service == "catalog"   # affinity_tier=None defaults to "catalog"
    assert t2.runner == "background"


def test_matrix_cloud_processes_get_gcp_cloud_run() -> None:
    """Cloud preset: processes get gcp_cloud_run + {OFFLOAD, HEAVY}."""
    items = _make_inventory(processes=[("ingestion", None)])
    _tasks_map, procs_map = build_routing_matrix(items, "cloud")

    assert "ingestion" in procs_map
    t = procs_map["ingestion"][0]
    assert t.runner == "gcp_cloud_run"
    assert ExecHint.OFFLOAD in t.hints
    assert ExecHint.HEAVY in t.hints
    assert t.service == "worker"    # _PROCESS_SERVICE["ingestion"] = "worker"


def test_matrix_cloud_process_job_name() -> None:
    """Cloud preset: job name derives from task key (underscores -> dashes)."""
    items = _make_inventory(processes=[("dwh_join", None)])
    _tasks_map, procs_map = build_routing_matrix(items, "cloud")
    t = procs_map["dwh_join"][0]
    assert t.options.get("job") == "dynastore-dwh-join"


def test_matrix_onprem_processes_get_background() -> None:
    """On-prem preset: processes get background + {BACKGROUND}."""
    items = _make_inventory(processes=[("ingestion", None)])
    _tasks_map, procs_map = build_routing_matrix(items, "onprem")

    t = procs_map["ingestion"][0]
    assert t.runner == "background"
    assert ExecHint.BACKGROUND in t.hints


def test_matrix_unknown_process_defaults_to_worker_service() -> None:
    """Process not in _PROCESS_SERVICE defaults to 'worker' service."""
    items = _make_inventory(processes=[("my_exotic_process", None)])
    _tasks_map, procs_map = build_routing_matrix(items, "cloud")
    t = procs_map["my_exotic_process"][0]
    assert t.service == "worker"


# ---------------------------------------------------------------------------
# TaskRoutingConfig.class_key
# ---------------------------------------------------------------------------


def test_task_routing_config_class_key() -> None:
    """class_key() must equal 'task_routing_config'."""
    assert TaskRoutingConfig.class_key() == "task_routing_config"


# ---------------------------------------------------------------------------
# resolved_targets (bucket selection)
# ---------------------------------------------------------------------------


def test_resolved_targets_returns_task_bucket() -> None:
    """resolved_targets() pulls from the tasks map for kind='task'."""
    target = RunnerTarget(service="catalog", runner="background", hints={ExecHint.BACKGROUND})
    cfg = TaskRoutingConfig(
        tasks={"my_task": [target]},
        processes={},
    )
    # Bypass _materialize_if_empty — we provided non-empty maps.
    result = resolved_targets(cfg, "my_task")
    # resolved_targets uses _DYNASTORE_TASKS to determine kind; for unknown
    # tasks it falls back to checking both maps. "my_task" is not in the live
    # registry during a noconftest run, so it should still find it via fallback.
    assert result == [target]


def test_resolved_targets_returns_process_bucket() -> None:
    """resolved_targets() pulls from the processes map for known process keys."""
    target = RunnerTarget(service="worker", runner="gcp_cloud_run", hints={ExecHint.OFFLOAD})
    cfg = TaskRoutingConfig(
        tasks={},
        processes={"ingestion": [target]},
    )
    result = resolved_targets(cfg, "ingestion")
    assert result == [target]


def test_resolved_targets_unknown_key_returns_empty() -> None:
    """Unknown task key returns []."""
    cfg = TaskRoutingConfig(tasks={}, processes={})
    result = resolved_targets(cfg, "no_such_task_xyz")
    assert result == []


# ---------------------------------------------------------------------------
# select_target — hint matching
# ---------------------------------------------------------------------------


def _always_true(runner_type: str, _svc: str) -> bool:
    return True


def _never_true(runner_type: str, _svc: str) -> bool:
    return False


def test_select_target_superset_match() -> None:
    """Target with hints that are a superset of request_hints is chosen."""
    t1 = RunnerTarget(service="catalog", runner="background", hints={ExecHint.BACKGROUND})
    t2 = RunnerTarget(service="catalog", runner="gcp_cloud_run", hints={ExecHint.OFFLOAD, ExecHint.HEAVY})

    # Request OFFLOAD — only t2 qualifies.
    result = select_target([t1, t2], frozenset({ExecHint.OFFLOAD}), "catalog", _always_true)
    assert result is t2


def test_select_target_longest_hints_wins() -> None:
    """When multiple targets match, the one with the most hints wins."""
    t1 = RunnerTarget(service="catalog", runner="a", hints={ExecHint.BACKGROUND})
    t2 = RunnerTarget(service="catalog", runner="b", hints={ExecHint.BACKGROUND, ExecHint.SYNC})

    # Both serve BACKGROUND; t2 is more specific (longer hints set).
    result = select_target([t1, t2], frozenset({ExecHint.BACKGROUND}), "catalog", _always_true)
    assert result is t2


def test_select_target_empty_request_first_viable() -> None:
    """Empty request_hints: first viable target in list order wins."""
    t1 = RunnerTarget(service="catalog", runner="a", hints={ExecHint.BACKGROUND})
    t2 = RunnerTarget(service="catalog", runner="b", hints={ExecHint.OFFLOAD})

    result = select_target([t1, t2], frozenset(), "catalog", _always_true)
    # Both match (empty request — no filter), t1 has shorter hints (len=1 == len=1),
    # so original position decides: t1 first.
    assert result is t1


def test_select_target_no_match_returns_none() -> None:
    """No candidate after filtering returns None."""
    t = RunnerTarget(service="catalog", runner="background", hints={ExecHint.BACKGROUND})
    result = select_target([t], frozenset({ExecHint.OFFLOAD}), "catalog", _always_true)
    assert result is None


def test_select_target_wrong_service_filtered_out() -> None:
    """Target for a different service is not selected."""
    t = RunnerTarget(service="worker", runner="background", hints={ExecHint.BACKGROUND})
    result = select_target([t], frozenset(), "catalog", _always_true)
    assert result is None


def test_select_target_can_handle_false_filtered_out() -> None:
    """Target where can_handle returns False is not selected."""
    t = RunnerTarget(service="catalog", runner="background", hints={ExecHint.BACKGROUND})
    result = select_target([t], frozenset(), "catalog", _never_true)
    assert result is None


def test_select_target_empty_target_hints_matches_all() -> None:
    """A target with empty hints (no declared preference) matches any request."""
    t = RunnerTarget(service="catalog", runner="sync", hints=set())
    # Even when caller requests OFFLOAD, empty-hints target still matches.
    result = select_target([t], frozenset({ExecHint.OFFLOAD}), "catalog", _always_true)
    assert result is t


def test_select_target_empty_list_returns_none() -> None:
    """Empty target list returns None."""
    result = select_target([], frozenset({ExecHint.BACKGROUND}), "catalog", _always_true)
    assert result is None
