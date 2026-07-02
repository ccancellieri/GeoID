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


# ---------------------------------------------------------------------------
# Conservative defaults: geometry-heavy sources must not OOM at default config
# ---------------------------------------------------------------------------


def test_max_batch_memory_default_is_32mb() -> None:
    """The model default must be 32 MB — conservative enough for dense admin
    polygons (e.g. GAUL: 3103 features, 331 MB source) without hand-tuning."""
    from dynastore.tasks.ingestion.ingestion_models import TaskIngestionRequest
    req = TaskIngestionRequest(
        asset={"uri": "file:///test.geojson"},
        column_mapping={},
    )
    assert req.max_batch_memory_mb == 32, (
        f"max_batch_memory_mb default changed from 32 to {req.max_batch_memory_mb}; "
        "this could OOM containers on geometry-heavy sources. Restore to 32."
    )


def test_row_cap_default_is_50() -> None:
    """run_ingestion_task must fall back to 50, not a higher row count, when
    database_batch_size is unset. Dense admin-boundary sources (e.g. GAUL: 3103
    features, avg ~100+ KB each) rely on the memory budget to shrink batches
    well below any row-count cap, but light-attribute sources have no large
    geometry to trip that budget — the row cap alone must already be
    conservative enough to keep a batch's write/read-back/report footprint
    bounded regardless of geometry density."""
    src = inspect.getsource(run_ingestion_task)
    assert "or 50" in src, (
        "run_ingestion_task row_cap fallback is no longer 50. "
        "A higher default risks a large batch footprint on geometry-heavy "
        "sources when the byte budget alone doesn't trigger soon enough; "
        "restore 'or 50'."
    )


def test_read_batch_size_forwarded_to_reader() -> None:
    """read_batch_size must be threaded from the task request into the reader's
    open() call so pyogrio (the fallback reader) can chunk rather than
    materialise the whole GeoDataFrame at once."""
    src = inspect.getsource(run_ingestion_task)
    assert "read_batch_size=task_request.read_batch_size" in src, (
        "run_ingestion_task no longer forwards read_batch_size to the reader. "
        "Without it, PyogrioReader falls back to its internal default and the "
        "task-level override has no effect on the reader's chunk size."
    )


def test_pyogrio_reader_uses_chunked_read() -> None:
    """PyogrioReader.open must not call gpd.read_file (which materialises the
    full GeoDataFrame) but must use pyogrio.read_dataframe with chunksize so
    large sources are streamed in bounded chunks.

    This test reads the source file directly so it runs without pyogrio
    installed (pyogrio is only available in the geospatial_io scope).
    """
    import pathlib
    import dynastore.tasks.ingestion  # importable without pyogrio
    reader_file = (
        pathlib.Path(dynastore.tasks.ingestion.__file__).parent
        / "readers" / "pyogrio_reader.py"
    )
    src = reader_file.read_text()
    assert "gpd.read_file" not in src, (
        "PyogrioReader.open still calls gpd.read_file, which loads the entire "
        "GeoDataFrame into memory. Replace with pyogrio.read_dataframe(chunksize=...)."
    )
    assert "pyogrio.read_dataframe" in src and "chunksize=chunk_size" in src, (
        "PyogrioReader.open must call pyogrio.read_dataframe with chunksize to "
        "stream large sources in bounded chunks."
    )
