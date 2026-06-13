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

"""Tests for modules/volumes/mesh_builder.py.

Vertices are emitted in a local ENU metric frame anchored at *origin*, so the
tests use a realistic Den Haag origin with small (metres-scale) footprints and
assert the metric z-*extent* (top − bottom) equals the extrusion height, rather
than absolute coordinates.
"""

from __future__ import annotations

import pytest

pytest.importorskip("shapely", reason="shapely required for mesh_builder tests")

from shapely.geometry import Polygon, MultiPolygon
import shapely.wkb

from dynastore.models.protocols.geometry_fetcher import FeatureGeometry
from dynastore.modules.volumes.mesh_builder import (
    _MeshAccumulator,
    _fan_triangulate,
    _extrude_ring,
    _extrude_polygon,
    build_mesh_from_geometries,
)

# Den Haag-ish origin (lon, lat, height) and a ~22 m footprint step in degrees.
ORIGIN = (4.3007, 52.0705, 0.0)
_D = 0.0002  # ~13-22 m at this latitude


def _square_lonlat(lon0=4.3007, lat0=52.0705, size=_D):
    return [
        (lon0, lat0), (lon0 + size, lat0),
        (lon0 + size, lat0 + size), (lon0, lat0 + size), (lon0, lat0),
    ]


def _square_wkb(lon0=4.3007, lat0=52.0705, size=_D) -> bytes:
    return shapely.wkb.dumps(Polygon(_square_lonlat(lon0, lat0, size)))


# ---------------------------------------------------------------------------
# MeshAccumulator
# ---------------------------------------------------------------------------


def test_accumulator_empty_produces_empty_buffers():
    acc = _MeshAccumulator()
    buf = acc.to_buffers()
    assert buf.vertex_count == 0
    assert buf.index_count == 0


def test_accumulator_single_triangle():
    acc = _MeshAccumulator()
    a = acc.add_vertex(0.0, 0.0, 0.0)
    b = acc.add_vertex(1.0, 0.0, 0.0)
    c = acc.add_vertex(0.0, 1.0, 0.0)
    acc.add_triangle(a, b, c)

    buf = acc.to_buffers()
    assert buf.vertex_count == 3
    assert buf.index_count == 3
    assert buf.triangle_count == 1
    assert len(buf.positions) == 3 * 3 * 4  # 3 verts * 3 floats * 4 bytes
    assert len(buf.indices) == 3 * 4        # 3 indices * 4 bytes


# ---------------------------------------------------------------------------
# _fan_triangulate
# ---------------------------------------------------------------------------


def test_fan_triangulate_square_produces_four_triangles():
    acc = _MeshAccumulator()
    _fan_triangulate(_square_lonlat(), z=0.0, acc=acc, origin=ORIGIN)
    # 4-vertex ring → 4 fan triangles (centroid + each edge)
    assert acc.to_buffers().triangle_count == 4


def test_fan_triangulate_degenerate_ring_skipped():
    acc = _MeshAccumulator()
    _fan_triangulate([(4.3, 52.07), (4.301, 52.07)], z=0.0, acc=acc, origin=ORIGIN)
    assert acc.to_buffers().triangle_count == 0


def test_fan_triangulate_flip_winding():
    acc_normal = _MeshAccumulator()
    acc_flipped = _MeshAccumulator()
    ring = _square_lonlat()
    _fan_triangulate(ring, z=0.0, acc=acc_normal, origin=ORIGIN, flip_winding=False)
    _fan_triangulate(ring, z=0.0, acc=acc_flipped, origin=ORIGIN, flip_winding=True)
    # Triangle vertex order must differ.
    assert acc_normal.tris[0] != acc_flipped.tris[0]


# ---------------------------------------------------------------------------
# _extrude_ring (side walls)
# ---------------------------------------------------------------------------


def test_extrude_ring_square_produces_eight_triangles():
    acc = _MeshAccumulator()
    _extrude_ring(_square_lonlat(), z_bottom=0.0, z_top=5.0, acc=acc, origin=ORIGIN)
    # 4 edges × 2 triangles each = 8
    assert acc.to_buffers().triangle_count == 8


# ---------------------------------------------------------------------------
# _extrude_polygon (full prism: top + bottom + sides)
# ---------------------------------------------------------------------------


def test_extrude_polygon_unit_square():
    acc = _MeshAccumulator()
    poly = Polygon(_square_lonlat())
    _extrude_polygon(poly, z_base=0.0, extrusion_height=3.0, acc=acc, origin=ORIGIN)
    buf = acc.to_buffers()
    # top cap: 4 tris, bottom cap: 4 tris, sides: 8 tris = 16
    assert buf.triangle_count == 16
    # Z-extent (up axis) equals the extrusion height; coords are metres.
    assert buf.max_pos[2] - buf.min_pos[2] == pytest.approx(3.0, abs=0.01)
    # Footprint is metric (~22 m), not raw degrees.
    assert 5.0 < (buf.max_pos[0] - buf.min_pos[0]) < 60.0


# ---------------------------------------------------------------------------
# build_mesh_from_geometries
# ---------------------------------------------------------------------------


def test_build_mesh_empty_input():
    buf = build_mesh_from_geometries([], origin=ORIGIN)
    assert buf.vertex_count == 0


def test_build_mesh_single_feature():
    fg = FeatureGeometry(feature_id="f1", geom_wkb=_square_wkb(), height=5.0)
    buf = build_mesh_from_geometries([fg], origin=ORIGIN, default_extrusion_height=10.0)
    assert buf.vertex_count > 0
    assert buf.triangle_count > 0
    # height=5 used as extrusion → z-extent ≈ 5 m
    assert buf.max_pos[2] - buf.min_pos[2] == pytest.approx(5.0, abs=0.01)


def test_build_mesh_uses_default_extrusion_when_height_zero():
    fg = FeatureGeometry(feature_id="f1", geom_wkb=_square_wkb(), height=0.0)
    buf = build_mesh_from_geometries([fg], origin=ORIGIN, default_extrusion_height=7.0)
    assert buf.max_pos[2] - buf.min_pos[2] == pytest.approx(7.0, abs=0.01)


def test_build_mesh_sidecar_z_base_offsets_bottom():
    # When z_base is provided (sidecar zmin), the prism bottom sits at z_base
    # and the top at z_base + height, so the up extent still equals height but
    # the absolute up coordinate is lifted.
    fg = FeatureGeometry("z", _square_wkb(), height=4.0, z_base=30.0)
    buf = build_mesh_from_geometries([fg], origin=ORIGIN)
    assert buf.max_pos[2] - buf.min_pos[2] == pytest.approx(4.0, abs=0.01)
    # Bottom lifted to ~30 m (curvature over a 22 m footprint is sub-mm).
    assert buf.min_pos[2] == pytest.approx(30.0, abs=0.05)


def test_build_mesh_multiple_features():
    feats = [
        FeatureGeometry("a", _square_wkb(4.3007, 52.0705), height=3.0),
        FeatureGeometry("b", _square_wkb(4.3017, 52.0705), height=3.0),
    ]
    buf = build_mesh_from_geometries(feats, origin=ORIGIN)
    assert buf.vertex_count > 0
    # Two footprints ~0.001 deg apart in lon → tens of metres of east extent.
    assert (buf.max_pos[0] - buf.min_pos[0]) > 50.0


def test_build_mesh_skips_bad_wkb():
    bad = FeatureGeometry("bad", b"\x00" * 5, height=1.0)
    good = FeatureGeometry("good", _square_wkb(), height=1.0)
    buf = build_mesh_from_geometries([bad, good], origin=ORIGIN)
    assert buf.vertex_count > 0


def test_build_mesh_multipolygon():
    mp = MultiPolygon([
        Polygon(_square_lonlat(4.3007, 52.0705)),
        Polygon(_square_lonlat(4.3017, 52.0705)),
    ])
    wkb = shapely.wkb.dumps(mp)
    fg = FeatureGeometry("mp", wkb, height=2.0)
    buf = build_mesh_from_geometries([fg], origin=ORIGIN)
    assert buf.triangle_count > 0
    assert (buf.max_pos[0] - buf.min_pos[0]) > 50.0


def test_build_mesh_default_origin_still_runs():
    # Legacy call without origin must not crash (degenerate equatorial frame).
    fg = FeatureGeometry("f", _square_wkb(0.0, 0.0), height=3.0)
    buf = build_mesh_from_geometries([fg])
    assert buf.vertex_count > 0


# ---------------------------------------------------------------------------
# Per-vertex normals (#2099) — without these the renderer cannot light the
# mesh and the whole tile collapses into one flat, unshaded silhouette.
# ---------------------------------------------------------------------------


import math
import struct


def _normals(buf):
    n = buf.vertex_count
    return [struct.unpack_from("<3f", buf.normals, i * 12) for i in range(n)]


def test_build_mesh_emits_one_normal_per_vertex():
    fg = FeatureGeometry("f1", _square_wkb(), height=5.0)
    buf = build_mesh_from_geometries([fg], origin=ORIGIN)
    assert buf.vertex_count > 0
    # 3 float32 per vertex.
    assert len(buf.normals) == buf.vertex_count * 12


def test_build_mesh_normals_are_unit_length():
    fg = FeatureGeometry("f1", _square_wkb(), height=5.0)
    buf = build_mesh_from_geometries([fg], origin=ORIGIN)
    for nx, ny, nz in _normals(buf):
        assert math.isclose(math.sqrt(nx * nx + ny * ny + nz * nz), 1.0, abs_tol=1e-5)


def test_build_mesh_roof_normal_points_up():
    # The top cap must have at least one vertex whose normal points up (+Z),
    # i.e. the roof is lit from above rather than rendered as a flat shadow.
    fg = FeatureGeometry("f1", _square_wkb(), height=5.0)
    buf = build_mesh_from_geometries([fg], origin=ORIGIN)
    up_normals = [n for n in _normals(buf) if n[2] > 0.9]
    assert up_normals, "expected at least one upward (roof) normal"


def test_empty_mesh_has_empty_normals():
    from dynastore.modules.volumes.mesh_builder import empty_mesh
    buf = empty_mesh()
    assert buf.normals == b""
