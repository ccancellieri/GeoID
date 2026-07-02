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

"""Sphere-normalization for geometries indexed as Elasticsearch ``geo_shape``.

Elasticsearch's ``geo_shape`` field type interprets ring vertices as points on
the sphere, not the plane — a ring that Shapely (planar) considers valid can
still be rejected by Lucene with a ``document_parsing_exception`` for two
distinct reasons (#2769):

* **Winding** — RFC 7946 requires the exterior ring counter-clockwise and
  holes clockwise. A clockwise exterior ring reads on the sphere as "the
  other, much larger hemisphere", which for anything but a point-sized
  polygon triggers a self-intersection/ambiguous-orientation rejection.
* **Antimeridian crossing** — a ring whose vertices legitimately wrap through
  +/-180 deg longitude (the GAUL Antarctica boundary is the case that
  surfaced this) is spatially valid but Lucene has no notion of "wrap
  around"; RFC 7946 sec 3.1.9 prescribes splitting such a geometry into two
  parts, one on each side of the antimeridian.

:func:`normalize_geometry_for_es` applies both fixes unconditionally and is
called from the single canonical ES document builder
(:func:`dynastore.modules.elasticsearch.canonical_doc.build_canonical_index_doc`)
so no write path — inline, bulk reindex, or the storage drain — can skip it.
Both steps degrade to a no-op (return the input unchanged) rather than raise;
normalization must never turn a good write into a failed one.

Implemented with Shapely primitives only (already a project dependency) —
no dedicated antimeridian-handling library. The antimeridian split below
translates the geometry into a longitude-continuous frame, clips at the
seam, and translates the excess back; this is the standard technique used by
GIS tooling that lacks a native "unwrap" primitive, though a purpose-built
antimeridian library (e.g. the ``antimeridian`` PyPI package) would handle
degenerate cases such as a ring that wraps the globe more than once — a case
world administrative-boundary data does not produce.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

try:
    from shapely.geometry import box, mapping, shape
    from shapely.ops import transform, unary_union
    from shapely.ops import orient as _shapely_orient
    from shapely.validation import make_valid as _shapely_make_valid

    _SHAPELY_AVAILABLE = True
except ImportError:  # shapely not installed — every function below no-ops
    _SHAPELY_AVAILABLE = False


# RFC 7946 sec 3.1.9: a geometry whose raw bbox longitude span reaches this
# threshold is treated as antimeridian-crossing. A bbox-based heuristic
# (rather than per-edge crossing detection) is cheap and side-effect free —
# a false positive just means the "east" clip below comes back empty and
# the split is a no-op.
_ANTIMERIDIAN_SPAN_DEG = 180.0

_POLYGONAL_TYPES = frozenset({"Polygon", "MultiPolygon"})
_VALID_RESULT_TYPES = frozenset({"Polygon", "MultiPolygon", "GeometryCollection"})


def orient_and_validate(geometry: Optional[dict]) -> Optional[dict]:
    """Repair topology (``make_valid``) then fix ring winding to RFC 7946.

    Returns ``None`` when *geometry* is falsy, unparseable, shapely is
    unavailable, or the repaired geometry collapses to empty — callers treat
    ``None`` as "no change available".
    """
    if not geometry or not _SHAPELY_AVAILABLE:
        return None
    try:
        geom = shape(geometry)
        geom = _shapely_make_valid(geom)
        if geom.is_empty:
            return None
        if geom.geom_type in _POLYGONAL_TYPES:
            geom = _shapely_orient(geom, sign=1.0)
        return mapping(geom)
    except Exception:
        return None


def split_antimeridian(geometry: Optional[dict]) -> Optional[dict]:
    """Split a geometry whose bbox spans >= 180 deg longitude at +/-180 deg.

    Returns ``None`` when *geometry* is falsy/unparseable, shapely is
    unavailable, the bbox span is under the threshold (nothing to split), or
    the split collapses to empty.
    """
    if not geometry or not _SHAPELY_AVAILABLE:
        return None
    try:
        geom = shape(geometry)
        minx, _miny, maxx, _maxy = geom.bounds
        if (maxx - minx) < _ANTIMERIDIAN_SPAN_DEG:
            return None

        # Shift the western half into a longitude-continuous [0, 360) frame
        # so the seam at the antimeridian becomes an ordinary internal split
        # point (at shifted x=180) rather than a coordinate discontinuity.
        def _shift_east(x: float, y: float, z: Optional[float] = None) -> Any:
            sx = x + 360.0 if x < 0 else x
            return (sx, y) if z is None else (sx, y, z)

        def _shift_back(x: float, y: float, z: Optional[float] = None) -> Any:
            sx = x - 360.0
            return (sx, y) if z is None else (sx, y, z)

        shifted = transform(_shift_east, geom)
        west_part = shifted.intersection(box(-180.0, -90.0, 180.0, 90.0))
        east_part = shifted.intersection(box(180.0, -90.0, 540.0, 90.0))
        if not east_part.is_empty:
            east_part = transform(_shift_back, east_part)

        parts = [g for g in (west_part, east_part) if not g.is_empty]
        if not parts:
            return None
        result = unary_union(parts)
        if result.is_empty or result.geom_type not in _VALID_RESULT_TYPES:
            return None
        return mapping(result)
    except Exception:
        return None


def normalize_geometry_for_es(geometry: Optional[dict]) -> Optional[dict]:
    """Apply orientation-fix + antimeridian split, best-effort.

    The single entry point called unconditionally from
    :func:`~dynastore.modules.elasticsearch.canonical_doc.build_canonical_index_doc`.
    Each step falls back to its input on failure, so this function never
    raises and never returns something worse than what it was given.
    """
    if not geometry:
        return geometry
    oriented = orient_and_validate(geometry) or geometry
    split = split_antimeridian(oriented)
    return split if split is not None else oriented
