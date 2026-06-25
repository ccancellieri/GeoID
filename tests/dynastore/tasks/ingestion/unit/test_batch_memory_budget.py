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

"""Regression coverage: the bulk-ingestion loop must bound a write batch by an
accumulated-geometry memory budget, not by row count alone.

A fixed row count (database_batch_size) ignores geometry weight, so a batch of a
few very large geometries (administrative multipolygons) can exhaust the
container's memory before the row cap is ever reached — the observed
Out-Of-Memory kill on a large vector dataset. ``max_batch_memory_mb`` is the
documented knob for this; the loop must actually consult it and flush on
whichever limit (row cap OR memory budget) is reached first.
"""

from __future__ import annotations

import inspect

from dynastore.tasks.ingestion.main_ingestion import (
    _count_coordinate_ordinates,
    _estimate_feature_bytes,
    run_ingestion_task,
)


# ---------------------------------------------------------------------------
# Coordinate counting / byte estimation
# ---------------------------------------------------------------------------


def test_count_ordinates_point() -> None:
    assert _count_coordinate_ordinates([10.0, 20.0]) == 2


def test_count_ordinates_polygon_ring() -> None:
    ring = [[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]]
    # one polygon = list of rings
    assert _count_coordinate_ordinates([ring]) == 8


def test_count_ordinates_empty_or_garbage() -> None:
    assert _count_coordinate_ordinates(None) == 0
    assert _count_coordinate_ordinates([]) == 0
    assert _count_coordinate_ordinates("nonsense") == 0


def test_large_geometry_estimates_more_bytes_than_small() -> None:
    small = {"geometry": {"type": "Point", "coordinates": [1.0, 2.0]}}
    big_ring = [[float(i), float(i)] for i in range(5000)]
    big = {"geometry": {"type": "Polygon", "coordinates": [big_ring]}}
    assert _estimate_feature_bytes(big) > _estimate_feature_bytes(small) * 100


def test_estimate_handles_geometry_collection() -> None:
    gc = {
        "geometry": {
            "type": "GeometryCollection",
            "geometries": [
                {"type": "Point", "coordinates": [0.0, 0.0]},
                {"type": "LineString", "coordinates": [[0.0, 0.0], [1.0, 1.0]]},
            ],
        }
    }
    # base + (2 + 4) ordinates accounted for — strictly above the flat floor.
    assert _estimate_feature_bytes(gc) > _estimate_feature_bytes({"geometry": None})


def test_estimate_missing_geometry_is_flat_floor() -> None:
    assert _estimate_feature_bytes({"properties": {"a": 1}}) == _estimate_feature_bytes(
        {"geometry": None}
    )


# ---------------------------------------------------------------------------
# Source-shape guard: the loop must flush on the memory budget, not count only
# ---------------------------------------------------------------------------


def test_ingestion_loop_consults_memory_budget() -> None:
    src = inspect.getsource(run_ingestion_task)
    assert "max_batch_memory_mb" in src and "mem_budget_bytes" in src, (
        "run_ingestion_task no longer derives a per-batch memory budget from "
        "max_batch_memory_mb — a batch of large geometries can OOM the container "
        "before the row cap is hit. Re-add the geometry-byte budget."
    )
    assert "current_batch_bytes" in src, (
        "run_ingestion_task no longer accumulates per-batch geometry bytes, so "
        "the memory budget is never enforced."
    )
    assert ">= mem_budget_bytes" in src, (
        "the batch flush condition no longer checks the memory budget — it must "
        "flush on whichever of (row cap, memory budget) is reached first."
    )
