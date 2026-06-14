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

"""Routing assertions for tiles_invalidate vs tiles_preseed.

tiles_invalidate:
  - Cloud: background runner, consumers=["catalog"], hints={BACKGROUND}
  - Onprem: background runner, consumers=["worker"], hints={BACKGROUND}
  - Review: same as cloud (background/catalog)
  - Never gcp_cloud_run

tiles_preseed (unchanged):
  - Cloud: gcp_cloud_run, consumers=["maps"], hints={OFFLOAD, HEAVY}
"""
from __future__ import annotations

from dynastore.modules.tasks.routing.exec_hints import ExecHint
from dynastore.modules.tasks.routing.matrix import (
    LIGHTWEIGHT_PROCESSES,
    InventoryItem,
    build_routing_matrix,
)


def _proc(key: str) -> InventoryItem:
    return InventoryItem(task_key=key, kind="process", affinity_tier=None)


# ---------------------------------------------------------------------------
# tiles_invalidate is in LIGHTWEIGHT_PROCESSES
# ---------------------------------------------------------------------------


def test_tiles_invalidate_in_lightweight_set():
    """tiles_invalidate must be declared as a lightweight process."""
    assert "tiles_invalidate" in LIGHTWEIGHT_PROCESSES


def test_tiles_preseed_not_in_lightweight_set():
    """tiles_preseed remains a heavy Cloud Run process (not lightweight)."""
    assert "tiles_preseed" not in LIGHTWEIGHT_PROCESSES


# ---------------------------------------------------------------------------
# Cloud preset
# ---------------------------------------------------------------------------


def test_cloud_tiles_invalidate_gets_background():
    """Cloud: tiles_invalidate routes to background runner on catalog."""
    _, procs = build_routing_matrix([_proc("tiles_invalidate")], preset="cloud")
    t = procs["tiles_invalidate"][0]
    assert t.runner == "background"
    assert "catalog" in t.consumers
    assert ExecHint.BACKGROUND in t.hints


def test_cloud_tiles_invalidate_not_gcp_cloud_run():
    """Cloud: tiles_invalidate must NOT route to gcp_cloud_run."""
    _, procs = build_routing_matrix([_proc("tiles_invalidate")], preset="cloud")
    t = procs["tiles_invalidate"][0]
    assert t.runner != "gcp_cloud_run"


def test_cloud_tiles_invalidate_no_offload_heavy():
    """Cloud: tiles_invalidate must not carry OFFLOAD or HEAVY hints."""
    _, procs = build_routing_matrix([_proc("tiles_invalidate")], preset="cloud")
    t = procs["tiles_invalidate"][0]
    assert ExecHint.OFFLOAD not in t.hints
    assert ExecHint.HEAVY not in t.hints


def test_cloud_tiles_preseed_unchanged_cloud_run():
    """Cloud: tiles_preseed still routes to gcp_cloud_run on maps (unchanged)."""
    _, procs = build_routing_matrix([_proc("tiles_preseed")], preset="cloud")
    t = procs["tiles_preseed"][0]
    assert t.runner == "gcp_cloud_run"
    assert t.consumers == ["maps"]
    assert ExecHint.OFFLOAD in t.hints
    assert ExecHint.HEAVY in t.hints


# ---------------------------------------------------------------------------
# Onprem preset
# ---------------------------------------------------------------------------


def test_onprem_tiles_invalidate_gets_background():
    """Onprem (worker-less): tiles_invalidate routes background to the catalog tier."""
    _, procs = build_routing_matrix([_proc("tiles_invalidate")], preset="onprem")
    t = procs["tiles_invalidate"][0]
    assert t.runner == "background"
    assert t.consumers == ["catalog"]
    assert ExecHint.BACKGROUND in t.hints
    assert ExecHint.HEAVY not in t.hints


def test_onprem_tiles_preseed_runs_background():
    """Onprem (worker-less): no worker tier to offload to, so the otherwise-heavy
    tiles_preseed is downgraded to a background run on the maps tier (the tier that
    carries the tile-rendering capability). HEAVY/offload only applies on cloud."""
    _, procs = build_routing_matrix([_proc("tiles_preseed")], preset="onprem")
    t = procs["tiles_preseed"][0]
    assert t.runner == "background"
    assert t.consumers == ["maps"]
    assert ExecHint.BACKGROUND in t.hints
    assert ExecHint.HEAVY not in t.hints


# ---------------------------------------------------------------------------
# Review preset
# ---------------------------------------------------------------------------


def test_review_tiles_invalidate_gets_background():
    """Review: tiles_invalidate routes same as cloud (background/catalog)."""
    _, procs = build_routing_matrix([_proc("tiles_invalidate")], preset="review")
    t = procs["tiles_invalidate"][0]
    assert t.runner == "background"
    assert "catalog" in t.consumers
    assert ExecHint.BACKGROUND in t.hints


def test_review_tiles_preseed_unchanged_cloud_run():
    """Review: tiles_preseed still routes to gcp_cloud_run/maps (unchanged)."""
    _, procs = build_routing_matrix([_proc("tiles_preseed")], preset="review")
    t = procs["tiles_preseed"][0]
    assert t.runner == "gcp_cloud_run"
    assert t.consumers == ["maps"]


# ---------------------------------------------------------------------------
# Mixed inventory: both tasks together
# ---------------------------------------------------------------------------


def test_both_tasks_in_cloud_routing():
    """Cloud: both tasks route correctly when present in the same inventory."""
    inv = [_proc("tiles_invalidate"), _proc("tiles_preseed")]
    _, procs = build_routing_matrix(inv, preset="cloud")

    assert procs["tiles_invalidate"][0].runner == "background"
    assert procs["tiles_preseed"][0].runner == "gcp_cloud_run"

    # consumers are distinct
    assert procs["tiles_invalidate"][0].consumers == ["catalog"]
    assert procs["tiles_preseed"][0].consumers == ["maps"]
