"""Tests for the review routing preset and the scope_catalog_review extra.

Covers four concerns:
(a) review preset routes gdal to background runner on catalog with BACKGROUND hint.
(b) cloud preset remains unchanged — gdal still gcp_cloud_run with {OFFLOAD, HEAVY}.
(c) ReviewTaskRoutingPreset is registered only when osgeo is present.
(d) Prod-bleed guard: scope_catalog and scope_geoid do NOT pull in scope_catalog_review
    or the gdal sub-extras (module_gdal / worker_task_gdal), directly or transitively.
"""
from __future__ import annotations

import re
import tomllib
from pathlib import Path
from unittest.mock import MagicMock

from dynastore.modules.tasks.routing.exec_hints import ExecHint
from dynastore.modules.tasks.routing.matrix import (
    CLOUD_PROCESS_CONSUMERS,
    InventoryItem,
    build_routing_matrix,
)


def _proc(key: str) -> InventoryItem:
    return InventoryItem(task_key=key, kind="process", affinity_tier=None)


def _task(key: str, affinity: str | None = None) -> InventoryItem:
    return InventoryItem(task_key=key, kind="task", affinity_tier=affinity)


# ---------------------------------------------------------------------------
# (a) review preset — gdal target
# ---------------------------------------------------------------------------


def test_review_gdal_runner_is_background():
    _, procs = build_routing_matrix([_proc("gdal")], preset="review")
    t = procs["gdal"][0]
    assert t.runner == "background"


def test_review_gdal_consumers_is_catalog_only():
    _, procs = build_routing_matrix([_proc("gdal")], preset="review")
    assert procs["gdal"][0].consumers == ["catalog"]


def test_review_gdal_hints_contain_background():
    _, procs = build_routing_matrix([_proc("gdal")], preset="review")
    assert ExecHint.BACKGROUND in procs["gdal"][0].hints


def test_review_gdal_hints_contain_interactive():
    _, procs = build_routing_matrix([_proc("gdal")], preset="review")
    assert ExecHint.INTERACTIVE in procs["gdal"][0].hints


def test_review_gdal_no_offload_hint():
    _, procs = build_routing_matrix([_proc("gdal")], preset="review")
    assert ExecHint.OFFLOAD not in procs["gdal"][0].hints


def test_review_non_gdal_process_still_gcp_cloud_run():
    """Non-gdal processes keep the cloud target under the review preset."""
    _, procs = build_routing_matrix([_proc("ingestion")], preset="review")
    t = procs["ingestion"][0]
    assert t.runner == "gcp_cloud_run"
    assert ExecHint.OFFLOAD in t.hints
    assert ExecHint.HEAVY in t.hints


def test_review_tiles_preseed_consumers_unchanged():
    _, procs = build_routing_matrix([_proc("tiles_preseed")], preset="review")
    assert procs["tiles_preseed"][0].consumers == ["maps"]


def test_review_system_task_gets_background():
    tasks, _ = build_routing_matrix([_task("heartbeat", affinity="catalog")], preset="review")
    t = tasks["heartbeat"][0]
    assert t.runner == "background"
    assert ExecHint.BACKGROUND in t.hints
    assert t.consumers == ["catalog"]


# ---------------------------------------------------------------------------
# (b) cloud preset unchanged — gdal still gcp_cloud_run
# ---------------------------------------------------------------------------


def test_cloud_gdal_runner_unchanged():
    _, procs = build_routing_matrix([_proc("gdal")], preset="cloud")
    t = procs["gdal"][0]
    assert t.runner == "gcp_cloud_run"


def test_cloud_gdal_consumers_unchanged():
    _, procs = build_routing_matrix([_proc("gdal")], preset="cloud")
    assert procs["gdal"][0].consumers == CLOUD_PROCESS_CONSUMERS["gdal"]
    assert "catalog" in procs["gdal"][0].consumers
    assert "maps" in procs["gdal"][0].consumers


def test_cloud_gdal_hints_offload_heavy():
    _, procs = build_routing_matrix([_proc("gdal")], preset="cloud")
    hints = procs["gdal"][0].hints
    assert ExecHint.OFFLOAD in hints
    assert ExecHint.HEAVY in hints


def test_cloud_gdal_no_background_hint():
    _, procs = build_routing_matrix([_proc("gdal")], preset="cloud")
    assert ExecHint.BACKGROUND not in procs["gdal"][0].hints


# ---------------------------------------------------------------------------
# (c) ReviewTaskRoutingPreset registered only when osgeo present
# ---------------------------------------------------------------------------


def test_osgeo_present_registers_review(monkeypatch):
    from dynastore.modules.tasks.routing import presets as routing_presets
    fake = MagicMock()
    monkeypatch.setattr("dynastore.modules.storage.presets.register_preset", fake)
    monkeypatch.setattr(routing_presets, "_osgeo_available", lambda: True)
    routing_presets._register()
    registered = [c.args[0].name for c in fake.call_args_list]
    assert "review" in registered
    assert "cloud" in registered and "onprem" in registered


def test_osgeo_absent_skips_review(monkeypatch):
    from dynastore.modules.tasks.routing import presets as routing_presets
    fake = MagicMock()
    monkeypatch.setattr("dynastore.modules.storage.presets.register_preset", fake)
    monkeypatch.setattr(routing_presets, "_osgeo_available", lambda: False)
    routing_presets._register()
    registered = [c.args[0].name for c in fake.call_args_list]
    assert "review" not in registered
    assert "cloud" in registered and "onprem" in registered


def test_review_preset_platform_tier():
    from dynastore.modules.storage.presets.protocol import PresetTier
    from dynastore.modules.tasks.routing.presets import ReviewTaskRoutingPreset
    assert ReviewTaskRoutingPreset.tier == PresetTier.PLATFORM


# ---------------------------------------------------------------------------
# (d) Prod-bleed guard — pyproject.toml dependency check
# ---------------------------------------------------------------------------

_PYPROJECT = Path(__file__).parents[6] / "pyproject.toml"
# Resolve to absolute (the worktree root is 6 levels up from this file):
# tests/dynastore/modules/tasks/routing/ -> tests/dynastore/modules/tasks/
# -> tests/dynastore/modules/ -> tests/dynastore/ -> tests/ -> <root>
# Fallback: walk upward until pyproject.toml is found.
def _find_pyproject() -> Path:
    candidate = Path(__file__).resolve()
    for _ in range(10):
        candidate = candidate.parent
        p = candidate / "pyproject.toml"
        if p.exists():
            return p
    raise FileNotFoundError("pyproject.toml not found above test file")


def _load_extras() -> dict:
    pyproject = _find_pyproject()
    with pyproject.open("rb") as f:
        data = tomllib.load(f)
    return data["project"]["optional-dependencies"]


_DYNASTORE_REF = re.compile(r"dynastore\[([^\]]+)\]")


def _transitive_extras(name: str, extras: dict, seen: set | None = None) -> set:
    """Recursively expand ``dynastore[a,b,...]`` references reachable from
    ``extras[name]`` and return the full set of extra names pulled in
    (including ``name`` itself)."""
    seen = seen if seen is not None else set()
    if name in seen or name not in extras:
        return seen
    seen.add(name)
    for dep in extras[name]:
        for m in _DYNASTORE_REF.finditer(dep):
            for part in (p.strip() for p in m.group(1).split(",")):
                if part:
                    _transitive_extras(part, extras, seen)
    return seen


_GDAL_FAMILY = {"module_gdal", "worker_task_gdal", "extension_gdal", "task_gdal_deps", "gdal", "scope_catalog_review"}


def test_scope_catalog_transitively_excludes_gdal():
    extras = _load_extras()
    closure = _transitive_extras("scope_catalog", extras)
    assert not (closure & _GDAL_FAMILY), (
        f"scope_catalog transitively pulls gdal-family extras: {sorted(closure & _GDAL_FAMILY)} (prod-bleed)"
    )


def test_scope_geoid_transitively_excludes_gdal():
    extras = _load_extras()
    closure = _transitive_extras("scope_geoid", extras)
    assert not (closure & _GDAL_FAMILY), (
        f"scope_geoid transitively pulls gdal-family extras: {sorted(closure & _GDAL_FAMILY)} (prod-bleed)"
    )


def test_scope_catalog_review_transitively_includes_gdal():
    extras = _load_extras()
    closure = _transitive_extras("scope_catalog_review", extras)
    assert {"module_gdal", "worker_task_gdal"} <= closure


def test_scope_catalog_does_not_include_gdal_extras():
    extras = _load_extras()
    scope_catalog_deps = " ".join(extras.get("scope_catalog", []))
    assert "module_gdal" not in scope_catalog_deps, (
        "scope_catalog must not pull in module_gdal (prod-bleed guard)"
    )
    assert "worker_task_gdal" not in scope_catalog_deps, (
        "scope_catalog must not pull in worker_task_gdal (prod-bleed guard)"
    )
    assert "scope_catalog_review" not in scope_catalog_deps, (
        "scope_catalog must not include scope_catalog_review (prod-bleed guard)"
    )


def test_scope_geoid_does_not_include_gdal_extras():
    extras = _load_extras()
    scope_geoid_deps = " ".join(extras.get("scope_geoid", []))
    # scope_geoid extends scope_catalog; verify it also doesn't directly add gdal
    assert "module_gdal" not in scope_geoid_deps, (
        "scope_geoid must not directly pull in module_gdal (prod-bleed guard)"
    )
    assert "worker_task_gdal" not in scope_geoid_deps, (
        "scope_geoid must not directly pull in worker_task_gdal (prod-bleed guard)"
    )
    assert "scope_catalog_review" not in scope_geoid_deps, (
        "scope_geoid must not include scope_catalog_review (prod-bleed guard)"
    )


def test_scope_catalog_review_contains_gdal_extras():
    """Positive check: scope_catalog_review does carry both gdal sub-extras."""
    extras = _load_extras()
    review_deps = " ".join(extras.get("scope_catalog_review", []))
    assert "module_gdal" in review_deps
    assert "worker_task_gdal" in review_deps


def test_scope_catalog_review_extends_scope_catalog():
    """scope_catalog_review must be a strict superset of scope_catalog."""
    extras = _load_extras()
    review_deps = " ".join(extras.get("scope_catalog_review", []))
    assert "scope_catalog" in review_deps
