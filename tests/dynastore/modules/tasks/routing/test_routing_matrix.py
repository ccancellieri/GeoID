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

"""Unit tests for routing/matrix.py.

Covers:
- cloud preset: processes get gcp_cloud_run + {OFFLOAD, HEAVY}.
- cloud preset: gdal consumers = [catalog, maps]; tiles_* = [maps].
- cloud preset: requeue_dead_letter_tasks stays background.
- cloud preset: system tasks get background on their affinity tier.
- onprem preset: every process in-process (background) on its affinity tier
  (gdal -> [catalog, maps], tiles_* -> [maps], rest -> [catalog]); never
  gcp_cloud_run, never the bare [worker] tier.
"""
from __future__ import annotations

from dynastore.modules.tasks.routing.exec_hints import ExecHint
from dynastore.modules.tasks.routing.matrix import (
    CLOUD_PROCESS_CONSUMERS,
    CLOUD_PROCESS_TIMEOUT_SECONDS,
    LIGHTWEIGHT_PROCESSES,
    OFFLOADABLE_SYSTEM_TASKS,
    InventoryItem,
    build_routing_matrix,
)


def _task(key: str, affinity: str | None = None) -> InventoryItem:
    return InventoryItem(task_key=key, kind="task", affinity_tier=affinity)


def _proc(key: str) -> InventoryItem:
    return InventoryItem(task_key=key, kind="process", affinity_tier=None)


# ---------------------------------------------------------------------------
# cloud preset
# ---------------------------------------------------------------------------


def test_cloud_system_task_gets_background():
    tasks, _ = build_routing_matrix([_task("heartbeat", affinity="catalog")], preset="cloud")
    assert "heartbeat" in tasks
    t = tasks["heartbeat"][0]
    assert t.runner == "background"
    assert ExecHint.BACKGROUND in t.hints
    assert t.consumers == ["catalog"]


def test_cloud_system_task_affinity_fallback():
    """Tier-agnostic task (affinity_tier=None) should default to catalog."""
    tasks, _ = build_routing_matrix([_task("generic_task")], preset="cloud")
    assert tasks["generic_task"][0].consumers == ["catalog"]


def test_cloud_process_gets_gcp_cloud_run():
    _, procs = build_routing_matrix([_proc("ingestion")], preset="cloud")
    t = procs["ingestion"][0]
    assert t.runner == "gcp_cloud_run"
    assert ExecHint.OFFLOAD in t.hints
    assert ExecHint.HEAVY in t.hints


def test_cloud_gdal_consumers():
    _, procs = build_routing_matrix([_proc("gdal")], preset="cloud")
    assert procs["gdal"][0].consumers == CLOUD_PROCESS_CONSUMERS["gdal"]
    assert "catalog" in procs["gdal"][0].consumers
    assert "maps" in procs["gdal"][0].consumers


def test_cloud_tiles_preseed_consumers():
    _, procs = build_routing_matrix([_proc("tiles_preseed")], preset="cloud")
    assert procs["tiles_preseed"][0].consumers == ["maps"]


def test_cloud_tiles_export_consumers():
    _, procs = build_routing_matrix([_proc("tiles_export")], preset="cloud")
    assert procs["tiles_export"][0].consumers == ["maps"]


def test_cloud_tiles_preseed_carries_timeout_ceiling():
    """Heavy tile preseed offloads with a per-process timeout ceiling in
    options, so its Cloud Run Job is not capped at the 3600s platform default
    (a dense layer can run far past one hour)."""
    _, procs = build_routing_matrix([_proc("tiles_preseed")], preset="cloud")
    t = procs["tiles_preseed"][0]
    assert t.runner == "gcp_cloud_run"
    assert t.options.get("timeout_seconds") == CLOUD_PROCESS_TIMEOUT_SECONDS["tiles_preseed"]


def test_cloud_tiles_export_carries_timeout_ceiling():
    _, procs = build_routing_matrix([_proc("tiles_export")], preset="cloud")
    assert (
        procs["tiles_export"][0].options.get("timeout_seconds")
        == CLOUD_PROCESS_TIMEOUT_SECONDS["tiles_export"]
    )


def test_cloud_unlisted_process_has_no_timeout_override():
    """A process not in CLOUD_PROCESS_TIMEOUT_SECONDS keeps an empty options
    dict — the ceiling is opt-in per process, not a blanket change."""
    _, procs = build_routing_matrix([_proc("ingestion")], preset="cloud")
    assert procs["ingestion"][0].options == {}


def test_cloud_unknown_process_defaults_to_catalog():
    _, procs = build_routing_matrix([_proc("unknown_heavy_job")], preset="cloud")
    assert procs["unknown_heavy_job"][0].consumers == ["catalog"]


def test_cloud_lightweight_process_stays_background():
    for key in LIGHTWEIGHT_PROCESSES:
        _, procs = build_routing_matrix([_proc(key)], preset="cloud")
        t = procs[key][0]
        assert t.runner == "background", f"{key} should be background under cloud"
        assert ExecHint.BACKGROUND in t.hints, f"{key} should carry BACKGROUND hint"
        assert ExecHint.OFFLOAD not in t.hints
        assert t.consumers == ["catalog"]


def test_cloud_no_options_job_field():
    """Runner options dict should be empty -- job name is supplied by the runner."""
    _, procs = build_routing_matrix([_proc("ingestion")], preset="cloud")
    assert procs["ingestion"][0].options == {}


# ---------------------------------------------------------------------------
# onprem preset
# ---------------------------------------------------------------------------


def test_onprem_system_task_gets_background():
    tasks, _ = build_routing_matrix([_task("heartbeat", affinity="catalog")], preset="onprem")
    t = tasks["heartbeat"][0]
    assert t.runner == "background"
    assert ExecHint.BACKGROUND in t.hints
    assert t.consumers == ["catalog"]


def test_onprem_process_runs_in_process_on_affinity_tier():
    # A default process (not in CLOUD_PROCESS_CONSUMERS) lands on catalog,
    # in-process — never gcp_cloud_run, never the bare "worker" tier.
    _, procs = build_routing_matrix([_proc("ingestion")], preset="onprem")
    t = procs["ingestion"][0]
    assert t.runner == "background"
    assert t.consumers == ["catalog"]
    assert ExecHint.BACKGROUND in t.hints
    assert ExecHint.HEAVY not in t.hints


def test_onprem_gdal_routes_to_catalog_and_maps_in_process():
    # gdal's affinity (CLOUD_PROCESS_CONSUMERS) is [catalog, maps]; on-prem keeps
    # that consumer topology but runs it in-process so maps can claim gdalinfo.
    _, procs = build_routing_matrix([_proc("gdal")], preset="onprem")
    t = procs["gdal"][0]
    assert t.runner == "background"
    assert t.consumers == ["catalog", "maps"]
    assert ExecHint.BACKGROUND in t.hints


def test_onprem_tiles_preseed_routes_to_maps_in_process():
    _, procs = build_routing_matrix([_proc("tiles_preseed")], preset="onprem")
    t = procs["tiles_preseed"][0]
    assert t.runner == "background"
    assert t.consumers == ["maps"]


def test_onprem_never_offloads_to_cloud_run():
    for key in ("ingestion", "gdal", "tiles_preseed", "dwh_join", *LIGHTWEIGHT_PROCESSES):
        _, procs = build_routing_matrix([_proc(key)], preset="onprem")
        t = procs[key][0]
        assert t.runner == "background"
        assert ExecHint.OFFLOAD not in t.hints
        assert t.consumers != ["worker"]


# ---------------------------------------------------------------------------
# OFFLOADABLE_SYSTEM_TASKS — preset-driven offload
# ---------------------------------------------------------------------------


def test_cloud_offloadable_task_routes_to_gcp_cloud_run():
    """Under cloud preset, catalog_provision must emit gcp_cloud_run + OFFLOAD
    so offload_required() returns True and _restrict_to_offload_runners drops
    BackgroundRunner, routing to the Cloud Run Job."""
    for key in OFFLOADABLE_SYSTEM_TASKS:
        tasks, _ = build_routing_matrix([_task(key, affinity="catalog")], preset="cloud")
        t = tasks[key][0]
        assert t.runner == "gcp_cloud_run", (
            f"{key} under cloud must use gcp_cloud_run, got {t.runner!r}"
        )
        assert ExecHint.OFFLOAD in t.hints, (
            f"{key} under cloud must carry OFFLOAD hint"
        )
        assert ExecHint.BACKGROUND not in t.hints
        assert t.consumers == ["catalog"]


def test_onprem_offloadable_task_stays_background():
    """Under onprem preset, catalog_provision stays in-process (no Job available)."""
    for key in OFFLOADABLE_SYSTEM_TASKS:
        tasks, _ = build_routing_matrix([_task(key, affinity="catalog")], preset="onprem")
        t = tasks[key][0]
        assert t.runner == "background", (
            f"{key} under onprem must use background, got {t.runner!r}"
        )
        assert ExecHint.BACKGROUND in t.hints
        assert ExecHint.OFFLOAD not in t.hints


def test_offloadable_task_consumers_match_affinity():
    """catalog_provision consumer must match its affinity_tier under both presets."""
    for preset in ("cloud", "onprem"):
        tasks, _ = build_routing_matrix(
            [_task("catalog_provision", affinity="catalog")], preset=preset
        )
        assert tasks["catalog_provision"][0].consumers == ["catalog"]


# ---------------------------------------------------------------------------
# Mixed inventory
# ---------------------------------------------------------------------------


def test_tasks_and_processes_land_in_separate_maps():
    inv = [_task("sys_task"), _proc("my_proc")]
    tasks, procs = build_routing_matrix(inv, preset="cloud")
    assert "sys_task" in tasks
    assert "my_proc" in procs
    assert "sys_task" not in procs
    assert "my_proc" not in tasks


def test_empty_inventory_returns_empty_maps():
    tasks, procs = build_routing_matrix([], preset="cloud")
    assert tasks == {}
    assert procs == {}


# ---------------------------------------------------------------------------
# #2129/#2732 — WorkClass hot-plane drains remain tier-agnostic so they never
# route to a nonexistent "worker" tier. ``storage_drain`` is in-process-first
# and hands off to ``storage_drain_offload`` when its budget is exhausted.
# The marker-carrying drain jobs (``event_drain`` and
# ``storage_drain_offload``) route to Cloud Run under the cloud profile while
# onprem keeps them background.
# ---------------------------------------------------------------------------


def test_workclass_drains_are_tier_agnostic():
    from dynastore.tasks.workclass_drain.event_drain_task import (
        EventDrainTask,
    )
    from dynastore.tasks.workclass_drain.storage_drain_task import (
        StorageDrainTask,
    )

    assert EventDrainTask.affinity_tier is None
    assert StorageDrainTask.affinity_tier is None


def test_storage_drain_starts_in_process_on_catalog():
    for preset in ("cloud", "review", "onprem"):
        tasks, _ = build_routing_matrix(
            [_task("storage_drain")],
            preset=preset,
        )
        target = tasks["storage_drain"][0]
        assert target.consumers == ["catalog"]
        assert target.consumers != ["worker"]
        assert target.runner == "background"


def test_cloud_async_writer_drains_route_to_cloud_run():
    tasks, _ = build_routing_matrix(
        [_task("event_drain"), _task("storage_drain_offload")],
        preset="cloud",
    )
    for key in ("event_drain", "storage_drain_offload"):
        target = tasks[key][0]
        assert target.runner == "gcp_cloud_run"
        assert ExecHint.OFFLOAD in target.hints
        assert ExecHint.BACKGROUND not in target.hints
        assert target.consumers == ["catalog"]


def test_onprem_async_writer_drains_stay_background():
    tasks, _ = build_routing_matrix(
        [_task("event_drain"), _task("storage_drain_offload")],
        preset="onprem",
    )
    for key in ("event_drain", "storage_drain_offload"):
        target = tasks[key][0]
        assert target.runner == "background"
        assert ExecHint.BACKGROUND in target.hints
        assert ExecHint.OFFLOAD not in target.hints
        assert target.consumers == ["catalog"]
