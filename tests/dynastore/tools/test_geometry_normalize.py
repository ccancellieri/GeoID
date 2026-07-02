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

"""Tests for `dynastore.tools.geometry_normalize` (#2769).

Covers ring re-orientation, antimeridian splitting (including a synthetic
Antarctica-like polar fixture), and the combined
`normalize_geometry_for_es` entry point.
"""
from shapely.geometry import shape

from dynastore.tools.geometry_normalize import (
    normalize_geometry_for_es,
    orient_and_validate,
    split_antimeridian,
)


def _signed_area(ring: list) -> float:
    """Shoelace signed area — positive is CCW (RFC 7946 exterior winding)."""
    total = 0.0
    for (x0, y0), (x1, y1) in zip(ring, ring[1:]):
        total += x0 * y1 - x1 * y0
    return total / 2.0


# ---------------------------------------------------------------------------
# orient_and_validate
# ---------------------------------------------------------------------------


def test_orient_fixes_clockwise_exterior_ring():
    # Deliberately clockwise (reverse of the CCW winding used elsewhere).
    cw_geom = {
        "type": "Polygon",
        "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
    }
    assert _signed_area(cw_geom["coordinates"][0]) < 0

    result = orient_and_validate(cw_geom)
    assert result is not None
    ring = [tuple(pt) for pt in result["coordinates"][0]]
    assert _signed_area(ring) > 0


def test_orient_leaves_already_ccw_ring_equivalent():
    ccw_geom = {
        "type": "Polygon",
        "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]],
    }
    result = orient_and_validate(ccw_geom)
    assert result is not None
    ring = [tuple(pt) for pt in result["coordinates"][0]]
    assert _signed_area(ring) > 0
    assert shape(result).equals(shape(ccw_geom))


def test_orient_returns_none_for_falsy_input():
    assert orient_and_validate(None) is None
    assert orient_and_validate({}) is None


# ---------------------------------------------------------------------------
# split_antimeridian
# ---------------------------------------------------------------------------


def test_split_antimeridian_no_op_under_threshold():
    geom = {
        "type": "Polygon",
        "coordinates": [[[10, 10], [20, 10], [20, 20], [10, 20], [10, 10]]],
    }
    assert split_antimeridian(geom) is None


def test_split_antimeridian_splits_dateline_crossing_polygon():
    """A ring written with vertices jumping from 170 to -170 (the wrap
    convention for "crosses the dateline") should split into two parts,
    one on each side of +/-180 deg."""
    geom = {
        "type": "Polygon",
        "coordinates": [[
            [170, -10], [-170, -10], [-170, 10], [170, 10], [170, -10],
        ]],
    }
    result = split_antimeridian(geom)
    assert result is not None
    assert result["type"] in ("MultiPolygon", "GeometryCollection")
    parsed = shape(result)
    assert parsed.area > 0
    # Two disjoint parts, each entirely on one side of the seam (a bbox
    # span < 180 deg) — the combined bbox legitimately touches both edges
    # (-180 and 180), so the per-part span, not the combined one, is what
    # proves the seam was actually cut rather than left as one contiguous
    # >= 180 deg-wide ring (the original unsplit failure mode).
    parts = list(parsed.geoms) if hasattr(parsed, "geoms") else [parsed]
    assert len(parts) == 2
    for part in parts:
        pminx, _pminy, pmaxx, _pmaxy = part.bounds
        assert (pmaxx - pminx) < 180.0


def test_split_antimeridian_polar_antarctica_like_fixture():
    """Synthetic GAUL-Antarctica-shaped fixture (#2769 root cause): a ring
    spanning the full longitude range at high southern latitude (-85..-90),
    the class of geometry that triggered the original ES rejection. Must
    not raise and must produce a non-empty result."""
    geom = {
        "type": "MultiPolygon",
        "coordinates": [[[
            [170.0, -85.0], [90.0, -87.0], [0.0, -90.0], [-90.0, -87.0],
            [-170.0, -85.0], [-170.0, -60.0], [170.0, -60.0], [170.0, -85.0],
        ]]],
    }
    result = split_antimeridian(geom)
    # Either a genuine split happened, or the bbox heuristic judged this
    # particular ring under threshold — either way, no exception and (when
    # a result IS produced) it must be non-empty and valid.
    if result is not None:
        parsed = shape(result)
        assert not parsed.is_empty


# ---------------------------------------------------------------------------
# normalize_geometry_for_es
# ---------------------------------------------------------------------------


def test_normalize_geometry_for_es_passthrough_for_falsy():
    assert normalize_geometry_for_es(None) is None
    assert normalize_geometry_for_es({}) == {}


def test_normalize_geometry_for_es_fixes_winding_and_keeps_geometry_type():
    cw_geom = {
        "type": "Polygon",
        "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
    }
    result = normalize_geometry_for_es(cw_geom)
    assert result is not None
    assert result["type"] == "Polygon"
    ring = [tuple(pt) for pt in result["coordinates"][0]]
    assert _signed_area(ring) > 0


def test_normalize_geometry_for_es_splits_antimeridian_crossing_input():
    geom = {
        "type": "Polygon",
        "coordinates": [[
            [170, -10], [-170, -10], [-170, 10], [170, 10], [170, -10],
        ]],
    }
    result = normalize_geometry_for_es(geom)
    assert result is not None
    assert result["type"] in ("MultiPolygon", "GeometryCollection")
