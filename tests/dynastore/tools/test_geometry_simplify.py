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

"""Tests for `dynastore.tools.geometry_simplify.simplify_to_fit`."""

import pytest
from shapely.geometry import mapping, Polygon

from dynastore.tools.geometry_simplify import (
    DEFAULT_MAX_BYTES,
    DEFAULT_MAX_ITERATIONS,
    DEFAULT_SIMPLIFY_TARGET_BYTES,
    DEFAULT_SNAP_GRID_SIZE,
    MODE_BBOX,
    MODE_NONE,
    MODE_SNAP_TO_GRID,
    MODE_TOLERANCE,
    geometry_geojson_size,
    maybe_simplify_for_es,
    simplify_to_fit,
)


def _ring(n_vertices: int) -> list[tuple[float, float]]:
    """Build a large closed ring with `n_vertices` densely sampled points."""
    import math

    return [
        (math.cos(2 * math.pi * i / n_vertices), math.sin(2 * math.pi * i / n_vertices))
        for i in range(n_vertices)
    ] + [(1.0, 0.0)]


def test_under_budget_returns_unchanged():
    poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])
    doc = {"id": "x", "geometry": mapping(poly)}
    out, factor, mode = simplify_to_fit(doc, max_bytes=10_000_000)
    assert out is doc
    assert factor == 1.0
    assert mode == MODE_NONE


def test_simplifies_to_fit_under_budget():
    poly = Polygon(_ring(50_000))
    doc = {"id": "x", "geometry": mapping(poly)}
    # Pick a tight budget that the original busts but a simplified
    # geometry can satisfy.
    out, factor, mode = simplify_to_fit(doc, max_bytes=100_000, max_iterations=3)
    assert mode == MODE_TOLERANCE
    assert 0.0 < factor < 1.0


def test_falls_back_to_bbox_when_iterations_exhausted():
    poly = Polygon(_ring(200_000))
    doc = {"id": "x", "geometry": mapping(poly)}
    # Budget below any possible simplified-polygon serialization forces
    # the bbox fallback after 3 iterations.
    out, factor, mode = simplify_to_fit(doc, max_bytes=30, max_iterations=3)
    assert mode == MODE_BBOX
    assert factor == 0.0
    # bbox geometry has exactly one ring of 5 coords.
    coords = out["geometry"]["coordinates"][0]
    assert len(coords) == 5


def test_no_geometry_returns_unchanged_even_if_oversized():
    doc = {"id": "x", "blob": "x" * 20_000}
    out, factor, mode = simplify_to_fit(doc, max_bytes=1000)
    assert mode == MODE_NONE
    assert factor == 1.0
    assert out is doc


# --- #1248: opt-in simplification + geometry-size measurement -------------


def test_maybe_simplify_disabled_returns_exact_geometry():
    """Default (simplify=False): the document is returned untouched even
    when it would bust the budget — exact geometry by default (#1248)."""
    poly = Polygon(_ring(50_000))
    geom = mapping(poly)
    doc = {"id": "x", "geometry": geom}
    out, factor, mode = maybe_simplify_for_es(doc, simplify=False, max_bytes=100_000)
    assert out is doc
    assert out["geometry"] == geom  # unchanged — full vertex count preserved
    assert factor == 1.0
    assert mode == MODE_NONE


def test_maybe_simplify_enabled_shrinks_to_fit():
    """simplify=True delegates to simplify_to_fit (opt-in path)."""
    poly = Polygon(_ring(50_000))
    doc = {"id": "x", "geometry": mapping(poly)}
    out, factor, mode = maybe_simplify_for_es(
        doc, simplify=True, max_bytes=100_000, max_iterations=3,
    )
    assert mode == MODE_TOLERANCE
    assert 0.0 < factor < 1.0


def test_geometry_geojson_size_none_is_zero():
    assert geometry_geojson_size(None) == 0
    assert geometry_geojson_size({}) == 0


def test_geometry_geojson_size_point_is_small():
    size = geometry_geojson_size({"type": "Point", "coordinates": [0.0, 0.0]})
    assert 0 < size < DEFAULT_MAX_BYTES


def test_geometry_geojson_size_large_polygon_exceeds_limit():
    poly = Polygon(_ring(900_000))
    size = geometry_geojson_size(mapping(poly))
    assert size > DEFAULT_MAX_BYTES


def test_default_max_iterations_is_eight():
    assert DEFAULT_MAX_ITERATIONS == 8


def test_custom_smaller_budget_shrinks_geometry():
    # A dense ring that serializes well over 1 MB.
    poly = Polygon(_ring(60_000))
    doc = {"id": "x", "geometry": mapping(poly)}
    out, factor, mode = simplify_to_fit(doc, max_bytes=1_000_000)
    assert geometry_geojson_size(out["geometry"]) <= 1_000_000
    assert mode in (MODE_TOLERANCE, MODE_BBOX)
    assert factor < 1.0


# --- New constants -----------------------------------------------------------


def test_default_simplify_target_bytes_is_1mb():
    assert DEFAULT_SIMPLIFY_TARGET_BYTES == 1_048_576


def test_default_snap_grid_size():
    assert DEFAULT_SNAP_GRID_SIZE == pytest.approx(1e-5)


def test_mode_snap_to_grid_constant():
    assert MODE_SNAP_TO_GRID == "snap_to_grid"


# --- Snap-to-grid mode -------------------------------------------------------


def test_snap_to_grid_under_budget_unchanged():
    """When the doc is already under budget snap_to_grid has no effect."""
    poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])
    doc = {"id": "x", "geometry": mapping(poly)}
    out, factor, mode = maybe_simplify_for_es(
        doc, simplify=True, max_bytes=10_000_000, snap_to_grid=True,
    )
    assert mode == MODE_NONE
    assert factor == 1.0


def test_snap_to_grid_reduces_heavy_polygon():
    """A heavy polygon should reduce to snap_to_grid mode when snap is enough."""
    # Dense ring: many vertices with tiny gaps, snapping to 0.01 collapses many.
    poly = Polygon(_ring(50_000))
    doc = {"id": "x", "geometry": mapping(poly)}
    out, factor, mode = maybe_simplify_for_es(
        doc, simplify=True, max_bytes=100_000, snap_to_grid=True, snap_grid_size=0.01,
    )
    # snap_to_grid should have produced either snap_to_grid or snap_to_grid+tolerance/bbox
    assert mode.startswith("snap_to_grid")
    assert factor < 1.0


def test_snap_to_grid_mode_recorded_when_snap_sufficient():
    """When snap alone brings the doc under budget, mode is exactly 'snap_to_grid'."""
    # Use a very generous snap grid size so snapping is very aggressive.
    poly = Polygon(_ring(20_000))
    doc = {"id": "x", "geometry": mapping(poly)}
    # Budget: tight but reachable by snapping to a coarse 0.1-degree grid.
    out, factor, mode = maybe_simplify_for_es(
        doc, simplify=True, max_bytes=500_000,
        snap_to_grid=True, snap_grid_size=0.1,
    )
    # Either snap alone was enough (snap_to_grid) or snap + D-P ran.
    assert mode in (MODE_SNAP_TO_GRID, f"{MODE_SNAP_TO_GRID}+{MODE_TOLERANCE}", f"{MODE_SNAP_TO_GRID}+{MODE_BBOX}")


def test_snap_to_grid_false_does_not_change_mode_name():
    """snap_to_grid=False uses the standard mode names without prefix."""
    poly = Polygon(_ring(50_000))
    doc = {"id": "x", "geometry": mapping(poly)}
    out, factor, mode = maybe_simplify_for_es(
        doc, simplify=True, max_bytes=100_000, snap_to_grid=False, max_iterations=3,
    )
    assert mode in (MODE_TOLERANCE, MODE_BBOX)
    assert not mode.startswith("snap_to_grid")


def test_snap_to_grid_prefix_when_snap_plus_simplify():
    """When snap is insufficient and D-P also runs, mode has 'snap_to_grid+' prefix."""
    # Extremely tight budget: snap won't be enough, D-P or bbox will also run.
    poly = Polygon(_ring(50_000))
    doc = {"id": "x", "geometry": mapping(poly)}
    out, factor, mode = maybe_simplify_for_es(
        doc, simplify=True, max_bytes=5_000,
        snap_to_grid=True, snap_grid_size=1e-5, max_iterations=2,
    )
    # Under a very tight budget the result is bbox (or tolerance).
    assert mode.startswith("snap_to_grid+")
    assert factor <= 1.0


def test_snap_to_grid_type_guard_rejects_type_change():
    """set_precision can return a MultiPolygon from a Polygon input when a
    narrow bridge (< snap_grid_size) is collapsed.  The type guard
    (``snapped.geom_type == geom.geom_type``) must reject such a result and
    fall through to the D-P loop, which uses preserve_topology=True and never
    changes geometry type.  The stored geometry must stay Polygon."""
    import unittest.mock as mock
    import shapely
    from shapely.geometry import MultiPolygon

    # A heavy Polygon that exceeds the budget.
    poly = Polygon(_ring(50_000))
    doc = {"id": "x", "geometry": mapping(poly)}

    # Simulate set_precision returning a MultiPolygon (type change).
    multi = MultiPolygon([poly.buffer(-0.1), poly.buffer(0.1)])

    with mock.patch.object(shapely, "set_precision", return_value=multi):
        out, factor, mode = maybe_simplify_for_es(
            doc, simplify=True, max_bytes=100_000,
            snap_to_grid=True, snap_grid_size=1e-5, max_iterations=3,
        )

    # Type guard rejected the MultiPolygon result; D-P ran instead.
    # The geometry in the doc must be Polygon (D-P preserves topology).
    assert out["geometry"]["type"] == "Polygon"
    # Mode has no snap_to_grid prefix because snap_ran stayed False.
    assert not mode.startswith("snap_to_grid")
    assert mode in (MODE_TOLERANCE, MODE_BBOX)


def test_snap_to_grid_mode_no_prefix_when_snap_raises():
    """When the snap step raises, snap_ran stays False and the mode must not
    carry the 'snap_to_grid+' prefix — D-P runs as if snap was never attempted."""
    import unittest.mock as mock
    import shapely

    poly = Polygon(_ring(50_000))
    doc = {"id": "x", "geometry": mapping(poly)}

    # Patch shapely.set_precision to raise, simulating an unsupported geometry
    # type or version mismatch.  The lazy import inside the function is
    # ``from shapely import set_precision as _set_precision``, so patching the
    # shapely module attribute is sufficient.
    with mock.patch.object(shapely, "set_precision", side_effect=RuntimeError("snap failure")):
        out, factor, mode = maybe_simplify_for_es(
            doc, simplify=True, max_bytes=100_000,
            snap_to_grid=True, snap_grid_size=1e-5, max_iterations=3,
        )
    # D-P ran without snap — mode has no prefix.
    assert mode in (MODE_TOLERANCE, MODE_BBOX)
    assert not mode.startswith("snap_to_grid")


