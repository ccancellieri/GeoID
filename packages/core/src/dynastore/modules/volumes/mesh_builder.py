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

"""WKB geometry → 3D triangle mesh for glTF tile content.

Pipeline:
  1. Parse WKB bytes with shapely (already a geopandas transitive dep).
  2. Flatten MultiPolygon → list of Polygons; skip unsupported geometry types.
  3. For each polygon: extrude exterior ring to a 3D prism
     (bottom face at z_base, top face at z_base + extrusion_height).
  4. Triangulate top/bottom caps with centroid-fan (works for star-shaped
     footprints, which covers ~99% of building polygons).
  5. Side walls: per-edge quads → 2 triangles each.
  6. Return a flat list of float32 vertex positions + normal vectors and
     a packed binary buffer ready for a glTF bufferView.

Coordinate convention: input footprints are EPSG:4326 (lon, lat in degrees)
with heights in metres. Vertices are emitted in a **local East-North-Up
metric frame** anchored at *origin* (the collection centre), matching the
tileset root ``transform`` produced by ``tileset_builder``. The conversion is
exact (see ``geo.lonlat_to_enu``); the tile content is therefore in metres and
extrusion heights add cleanly on the up axis.

Dependencies: shapely >= 2.0 (via geopandas).
"""

from __future__ import annotations

import logging
import math
import struct
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Optional, Sequence, Tuple

from dynastore.modules.volumes.color_ramp import Stop, interpolate_ramp
from dynastore.modules.volumes.geo import lonlat_to_enu

logger = logging.getLogger(__name__)

# Default ENU origin (lon, lat, height) — overridable per call. (0,0,0) keeps
# legacy callers working as a degenerate equatorial frame.
_DEFAULT_ORIGIN: Tuple[float, float, float] = (0.0, 0.0, 0.0)

# Static-typing-only imports: pyright follows this branch for type narrowing
# even when the runtime ``try/except`` below cannot resolve shapely
# (extension_volumes is optional). Without this, ``isinstance(geom, Polygon)``
# would not narrow ``geom`` from ``BaseGeometry`` and downstream ``.geoms``
# accesses fail with reportAttributeAccessIssue.
if TYPE_CHECKING:
    from shapely.geometry import (
        GeometryCollection,
        MultiPolygon,
        Polygon,
    )

try:
    import shapely
    import shapely.wkb
    from shapely.geometry import (
        MultiPolygon,
        Polygon,
        GeometryCollection,
    )
    _SHAPELY_AVAILABLE = True
except ImportError:  # pragma: no cover
    _SHAPELY_AVAILABLE = False

from dynastore.models.protocols.geometry_fetcher import FeatureGeometry


@dataclass
class MeshBuffers:
    """Packed binary buffers for a single glTF TRIANGLES primitive."""

    positions: bytes       # float32 * 3 per vertex
    indices: bytes         # uint32 per index
    vertex_count: int
    index_count: int
    min_pos: Tuple[float, float, float]
    max_pos: Tuple[float, float, float]
    normals: bytes = b""   # float32 * 3 per vertex (smooth per-vertex normals)
    colors: bytes = b""    # uint8 RGBA * 4 per vertex (COLOR_0, normalized)

    @property
    def triangle_count(self) -> int:
        return self.index_count // 3


@dataclass
class _MeshAccumulator:
    verts: List[Tuple[float, float, float]] = field(default_factory=list)
    tris: List[Tuple[int, int, int]] = field(default_factory=list)
    # Per-vertex RGB stamped with whatever ``current_color`` was set before the
    # vertex was added. Stays all-``None`` when no ramp is active, in which case
    # ``to_buffers`` emits no COLOR_0 attribute.
    cols: List[Optional[Tuple[int, int, int]]] = field(default_factory=list)
    current_color: Optional[Tuple[int, int, int]] = None

    def set_color(self, rgb: Optional[Tuple[int, int, int]]) -> None:
        """Set the colour stamped on every subsequently-added vertex."""
        self.current_color = rgb

    def add_vertex(self, x: float, y: float, z: float) -> int:
        idx = len(self.verts)
        self.verts.append((x, y, z))
        self.cols.append(self.current_color)
        return idx

    def add_triangle(self, a: int, b: int, c: int) -> None:
        self.tris.append((a, b, c))

    def to_buffers(self) -> MeshBuffers:
        if not self.verts:
            return empty_mesh()

        n = len(self.verts)
        pos_buf = bytearray(n * 12)  # 3 × float32 per vertex
        min_x = min_y = min_z = float("inf")
        max_x = max_y = max_z = -float("inf")
        for i, (x, y, z) in enumerate(self.verts):
            struct.pack_into("<3f", pos_buf, i * 12, x, y, z)
            if x < min_x: min_x = x
            if y < min_y: min_y = y
            if z < min_z: min_z = z
            if x > max_x: max_x = x
            if y > max_y: max_y = y
            if z > max_z: max_z = z

        idx_buf = bytearray(len(self.tris) * 12)  # 3 × uint32 per triangle
        for i, (a, b, c) in enumerate(self.tris):
            struct.pack_into("<3I", idx_buf, i * 12, a, b, c)

        nrm_buf = self._compute_normals(n)
        col_buf = self._pack_colors(n)

        return MeshBuffers(
            positions=bytes(pos_buf),
            indices=bytes(idx_buf),
            vertex_count=n,
            index_count=len(self.tris) * 3,
            min_pos=(min_x, min_y, min_z),
            max_pos=(max_x, max_y, max_z),
            normals=bytes(nrm_buf),
            colors=bytes(col_buf),
        )

    def _pack_colors(self, n: int) -> bytes:
        """Pack per-vertex RGBA (uint8) for COLOR_0, or empty if no ramp ran.

        Returns ``b""`` when no vertex carried a colour, so the GLB writer omits
        the attribute entirely. Any vertex left uncoloured while others were
        coloured (shouldn't happen — colours are set per feature before its
        vertices are added) defaults to opaque white, the PBR no-tint identity.
        """
        if not any(c is not None for c in self.cols):
            return b""
        col_buf = bytearray(n * 4)
        for i, c in enumerate(self.cols):
            r, g, b = c if c is not None else (255, 255, 255)
            struct.pack_into("<4B", col_buf, i * 4, r, g, b, 255)
        return bytes(col_buf)

    def _compute_normals(self, n: int) -> bytearray:
        """Smooth per-vertex normals: accumulate area-weighted face normals.

        Each triangle's geometric normal (the cross product of two edges, whose
        magnitude is twice the triangle area) is added to all three of its
        vertices, then every accumulated vector is normalised. Sharing a vertex
        across faces with the same orientation (e.g. a flat roof cap) keeps that
        normal axis-aligned; vertical wall edges blend their two faces, which
        reads as a softly shaded corner. Without normals the renderer cannot
        light the mesh and the whole tile collapses into one flat silhouette.
        """
        nx = [0.0] * n
        ny = [0.0] * n
        nz = [0.0] * n
        for a, b, c in self.tris:
            ax, ay, az = self.verts[a]
            bx, by, bz = self.verts[b]
            cx, cy, cz = self.verts[c]
            e1x, e1y, e1z = bx - ax, by - ay, bz - az
            e2x, e2y, e2z = cx - ax, cy - ay, cz - az
            fnx = e1y * e2z - e1z * e2y
            fny = e1z * e2x - e1x * e2z
            fnz = e1x * e2y - e1y * e2x
            for idx in (a, b, c):
                nx[idx] += fnx
                ny[idx] += fny
                nz[idx] += fnz

        nrm_buf = bytearray(n * 12)
        for i in range(n):
            x, y, z = nx[i], ny[i], nz[i]
            mag = math.sqrt(x * x + y * y + z * z)
            if mag > 1e-12:
                x, y, z = x / mag, y / mag, z / mag
            else:
                # Degenerate (zero-area) accumulation — default to up so the
                # vertex is still validly normalised.
                x, y, z = 0.0, 0.0, 1.0
            struct.pack_into("<3f", nrm_buf, i * 12, x, y, z)
        return nrm_buf


def empty_mesh() -> MeshBuffers:
    return MeshBuffers(
        positions=b"",
        indices=b"",
        vertex_count=0,
        index_count=0,
        min_pos=(0.0, 0.0, 0.0),
        max_pos=(0.0, 0.0, 0.0),
    )


def _enu(lon: float, lat: float, h: float,
         origin: Tuple[float, float, float]) -> Tuple[float, float, float]:
    """Convert a lon/lat/height vertex to local ENU metres about *origin*."""
    return lonlat_to_enu(lon, lat, h, origin[0], origin[1], origin[2])


def _fan_triangulate(ring_xy: Sequence[Tuple[float, float]],
                     z: float,
                     acc: _MeshAccumulator,
                     origin: Tuple[float, float, float],
                     flip_winding: bool = False) -> None:
    """Centroid-fan triangulation of a closed lon/lat ring at height *z* (m).

    Each (lon, lat, z) vertex is converted to local ENU metres before it is
    added. The last coordinate of a shapely ring equals the first (closed); we
    skip it to avoid degenerate triangles.
    """
    coords = list(ring_xy)
    if coords and coords[-1] == coords[0]:
        coords = coords[:-1]
    n = len(coords)
    if n < 3:
        return

    cx = sum(c[0] for c in coords) / n
    cy = sum(c[1] for c in coords) / n
    center_idx = acc.add_vertex(*_enu(cx, cy, z, origin))

    ring_indices = [acc.add_vertex(*_enu(c[0], c[1], z, origin)) for c in coords]
    for i in range(n):
        a = ring_indices[i]
        b = ring_indices[(i + 1) % n]
        if flip_winding:
            acc.add_triangle(center_idx, b, a)
        else:
            acc.add_triangle(center_idx, a, b)


def _extrude_ring(ring_xy: Sequence[Tuple[float, float]],
                  z_bottom: float,
                  z_top: float,
                  acc: _MeshAccumulator,
                  origin: Tuple[float, float, float]) -> None:
    """Extrude a lon/lat ring into side-wall quads (two triangles each)."""
    coords = list(ring_xy)
    if coords and coords[-1] == coords[0]:
        coords = coords[:-1]
    n = len(coords)
    if n < 2:
        return

    bot = [acc.add_vertex(*_enu(c[0], c[1], z_bottom, origin)) for c in coords]
    top = [acc.add_vertex(*_enu(c[0], c[1], z_top, origin)) for c in coords]

    for i in range(n):
        j = (i + 1) % n
        # Two CCW triangles forming the outward-facing quad.
        acc.add_triangle(bot[i], bot[j], top[i])
        acc.add_triangle(top[i], bot[j], top[j])


def _extrude_polygon(polygon: Polygon,
                     z_base: float,
                     extrusion_height: float,
                     acc: _MeshAccumulator,
                     origin: Tuple[float, float, float]) -> None:
    z_top = z_base + extrusion_height
    ring = list(polygon.exterior.coords)

    # Top cap (normal pointing up → CCW when viewed from above).
    _fan_triangulate([(c[0], c[1]) for c in ring], z_top, acc, origin, flip_winding=False)
    # Bottom cap (normal pointing down → CW when viewed from above).
    _fan_triangulate([(c[0], c[1]) for c in ring], z_base, acc, origin, flip_winding=True)
    # Side walls.
    _extrude_ring([(c[0], c[1]) for c in ring], z_base, z_top, acc, origin)


def build_mesh_from_geometries(
    features: Sequence[FeatureGeometry],
    *,
    origin: Tuple[float, float, float] = _DEFAULT_ORIGIN,
    default_extrusion_height: float = 10.0,
    color_ramp: Optional[Sequence[Stop]] = None,
) -> MeshBuffers:
    """Build a combined triangle mesh from a sequence of ``FeatureGeometry``.

    All features are merged into one mesh (single glTF primitive / draw
    call). Features whose WKB cannot be parsed are skipped with a warning.

    Vertices are emitted in the local ENU metric frame anchored at *origin*
    ``(lon0, lat0, h0)`` — the same origin the tileset root transform uses.

    *z_base* for each feature is taken from ``feature.z_base`` when provided
    (the sidecar ``zmin``), else the WKB Z coordinate if the geometry is 3-D,
    otherwise 0.0. The extrusion height is ``feature.height`` if > 0, else
    ``default_extrusion_height``.

    When *color_ramp* is supplied (sorted ``(stop, rgb)`` pairs), each feature's
    vertices are stamped with an RGB looked up from its extrusion height, so the
    resulting GLB carries a per-building COLOR_0 attribute. Without a ramp the
    mesh is untinted and the writer emits no colour attribute.
    """
    if not _SHAPELY_AVAILABLE:
        raise RuntimeError(
            "shapely is required for 3D tile content generation; "
            "install dynastore[extension_volumes]"
        )

    acc = _MeshAccumulator()

    for fg in features:
        try:
            geom = shapely.wkb.loads(fg.geom_wkb)
        except Exception as exc:
            logger.warning(
                "Skipping feature %r: WKB parse failed: %s", fg.feature_id, exc
            )
            continue

        polys: List[Polygon] = []
        if isinstance(geom, Polygon):
            polys = [geom]
        elif isinstance(geom, MultiPolygon):
            polys = list(geom.geoms)
        elif isinstance(geom, GeometryCollection):
            polys = [g for g in geom.geoms if isinstance(g, Polygon)]
        else:
            continue

        extrusion = fg.height if fg.height > 0 else default_extrusion_height
        # Stamp this feature's vertices with a height-derived colour when a ramp
        # is active; cleared to None otherwise so uncoloured runs emit no COLOR_0.
        acc.set_color(interpolate_ramp(extrusion, color_ramp) if color_ramp else None)
        # Prefer the sidecar zmin (z_base) when present (#2089); otherwise fall
        # back to the geometry's Z, then to ground (0.0).
        sidecar_z_base = getattr(fg, "z_base", None)

        for poly in polys:
            if poly.is_empty:
                continue
            if sidecar_z_base is not None:
                z_base = float(sidecar_z_base)
            else:
                # Use Z from first exterior coordinate if present.
                first = poly.exterior.coords[0]
                z_base = float(first[2]) if len(first) > 2 else 0.0
            _extrude_polygon(poly, z_base=z_base,
                             extrusion_height=extrusion, acc=acc, origin=origin)

    return acc.to_buffers()
