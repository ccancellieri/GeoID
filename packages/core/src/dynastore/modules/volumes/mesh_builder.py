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

Coordinate convention: XY = easting/northing (or lon/lat), Z = elevation.
Consumers are responsible for passing a tileset with a matching CRS; this
module does not reproject.

Dependencies: shapely >= 2.0 (via geopandas).
"""

from __future__ import annotations

import logging
import struct
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Sequence, Tuple

logger = logging.getLogger(__name__)

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

    @property
    def triangle_count(self) -> int:
        return self.index_count // 3


@dataclass
class _MeshAccumulator:
    verts: List[Tuple[float, float, float]] = field(default_factory=list)
    tris: List[Tuple[int, int, int]] = field(default_factory=list)

    def add_vertex(self, x: float, y: float, z: float) -> int:
        idx = len(self.verts)
        self.verts.append((x, y, z))
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

        return MeshBuffers(
            positions=bytes(pos_buf),
            indices=bytes(idx_buf),
            vertex_count=n,
            index_count=len(self.tris) * 3,
            min_pos=(min_x, min_y, min_z),
            max_pos=(max_x, max_y, max_z),
        )


def empty_mesh() -> MeshBuffers:
    return MeshBuffers(
        positions=b"",
        indices=b"",
        vertex_count=0,
        index_count=0,
        min_pos=(0.0, 0.0, 0.0),
        max_pos=(0.0, 0.0, 0.0),
    )


def _fan_triangulate(ring_xy: Sequence[Tuple[float, float]],
                     z: float,
                     acc: _MeshAccumulator,
                     flip_winding: bool = False) -> None:
    """Centroid-fan triangulation of a closed 2-D ring at elevation *z*.

    The last coordinate of a shapely ring equals the first (closed); we
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
    center_idx = acc.add_vertex(cx, cy, z)

    ring_indices = [acc.add_vertex(c[0], c[1], z) for c in coords]
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
                  acc: _MeshAccumulator) -> None:
    """Extrude a 2-D ring into side-wall quads (two triangles each)."""
    coords = list(ring_xy)
    if coords and coords[-1] == coords[0]:
        coords = coords[:-1]
    n = len(coords)
    if n < 2:
        return

    bot = [acc.add_vertex(c[0], c[1], z_bottom) for c in coords]
    top = [acc.add_vertex(c[0], c[1], z_top) for c in coords]

    for i in range(n):
        j = (i + 1) % n
        # Two CCW triangles forming the outward-facing quad.
        acc.add_triangle(bot[i], bot[j], top[i])
        acc.add_triangle(top[i], bot[j], top[j])


def _extrude_polygon(polygon: Polygon,
                     z_base: float,
                     extrusion_height: float,
                     acc: _MeshAccumulator) -> None:
    z_top = z_base + extrusion_height
    ring = list(polygon.exterior.coords)

    # Top cap (normal pointing up → CCW when viewed from above).
    _fan_triangulate([(c[0], c[1]) for c in ring], z_top, acc, flip_winding=False)
    # Bottom cap (normal pointing down → CW when viewed from above).
    _fan_triangulate([(c[0], c[1]) for c in ring], z_base, acc, flip_winding=True)
    # Side walls.
    _extrude_ring([(c[0], c[1]) for c in ring], z_base, z_top, acc)


def build_mesh_from_geometries(
    features: Sequence[FeatureGeometry],
    *,
    default_extrusion_height: float = 10.0,
) -> MeshBuffers:
    """Build a combined triangle mesh from a sequence of ``FeatureGeometry``.

    All features are merged into one mesh (single glTF primitive / draw
    call). Features whose WKB cannot be parsed are skipped with a warning.

    *z_base* for each feature is taken from the WKB Z coordinate if the
    geometry is 3-D, otherwise 0.0. The extrusion height is
    ``feature.height`` if > 0, else ``default_extrusion_height``.
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

        for poly in polys:
            if poly.is_empty:
                continue
            # Use Z from first exterior coordinate if present.
            first = poly.exterior.coords[0]
            z_base = float(first[2]) if len(first) > 2 else 0.0
            _extrude_polygon(poly, z_base=z_base,
                             extrusion_height=extrusion, acc=acc)

    return acc.to_buffers()
