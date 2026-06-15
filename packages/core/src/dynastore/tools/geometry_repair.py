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

"""GeoJSON geometry repair for write-time Elasticsearch poison-shape detection.

Single source-of-truth contract
---------------------------------
The *kinds* of geometry that ES poison-rejects (``invalid_shape_exception``,
``mapper_parsing_exception``) are enumerated in
:data:`~dynastore.modules.elasticsearch.bulk_classify._POISON_ERROR_TYPES`.
This module applies the **write-time counterpart**: it attempts to repair a
geometry using Shapely's ``make_valid`` and polygonal coercion before the item
is committed to any driver, so the API can return a 400 for known-unfixable
shapes instead of accepting them silently.

The repair logic follows the same approach used in the CityJSON ingest path
(``extensions/volumes/cityjson_ingest._coerce_to_multipolygon``) and is kept
here in *core* so the ``ItemService`` (which must not depend on an extension)
can use it.

Classification contract
-----------------------
* ``make_valid`` turns self-intersecting rings, bow-tie polygons, and duplicate-
  consecutive-coordinate rings into valid geometry (or a ``GeometryCollection``
  mixing polygon area with degenerate line/point residuals).
* Polygonal coercion discards ``LineString`` and ``Point`` residuals that ES
  ``geo_shape`` indexing rejects; only ``Polygon`` and ``MultiPolygon``
  components survive.
* If **no polygonal area survives** (the repaired shape is empty) the geometry
  is *poison*: it would always trigger ``invalid_shape_exception`` regardless
  of retries.
* Geometries that repair successfully are mutated in place on the item before
  the write so the ES driver receives an immediately-indexable shape.

Non-goals
---------
* This module does NOT perform full OGC validity checking — that would be too
  strict and would reject geometries that ES accepts fine.
* This module does NOT simplify large geometries; that responsibility stays
  with :func:`~dynastore.tools.geometry_simplify.simplify_to_fit`.
"""

from __future__ import annotations

from typing import Any, Optional, Tuple


def coerce_to_polygonal(geom: Any) -> Any:
    """Return the polygonal components of a Shapely geometry.

    Applies ``make_valid`` first (no-op for already-valid geometry), then
    extracts only ``Polygon`` and ``MultiPolygon`` components from the result,
    discarding ``LineString``, ``Point``, and ``LinearRing`` residuals.
    The result is a ``MultiPolygon`` (possibly empty) or the original geometry
    when it is already a valid ``Polygon`` / ``MultiPolygon``.

    Returns an **empty** ``MultiPolygon`` when no polygonal area survives
    (callers must check ``is_empty``).
    """
    import shapely.geometry
    import shapely.ops
    from shapely.validation import make_valid

    if not geom.is_valid:
        geom = make_valid(geom)

    if isinstance(geom, shapely.geometry.Polygon):
        if geom.is_empty:
            return shapely.geometry.MultiPolygon()
        return shapely.geometry.MultiPolygon([geom])

    if isinstance(geom, shapely.geometry.MultiPolygon):
        return geom

    # GeometryCollection or other mixed type: extract polygon-area components.
    polys: list[shapely.geometry.Polygon] = []
    stack = list(getattr(geom, "geoms", [geom]))
    while stack:
        component = stack.pop()
        if isinstance(component, shapely.geometry.Polygon):
            if not component.is_empty:
                polys.append(component)
        elif isinstance(component, shapely.geometry.MultiPolygon):
            for p in component.geoms:
                if not p.is_empty:
                    polys.append(p)
        elif hasattr(component, "geoms"):
            stack.extend(component.geoms)
        # LineString, Point, LinearRing etc. are intentionally dropped.

    if not polys:
        return shapely.geometry.MultiPolygon()
    if len(polys) == 1:
        return shapely.geometry.MultiPolygon(polys)
    merged = shapely.ops.unary_union(polys)
    if isinstance(merged, shapely.geometry.Polygon):
        return shapely.geometry.MultiPolygon([merged])
    if isinstance(merged, shapely.geometry.MultiPolygon):
        return merged
    return shapely.geometry.MultiPolygon(polys)


def repair_geometry_for_es(
    geometry: Optional[Any],
) -> Tuple[Optional[Any], bool, bool]:
    """Try to repair a GeoJSON geometry dict so it is safe for ES geo_shape.

    Parameters
    ----------
    geometry:
        A GeoJSON geometry dict (e.g. ``{"type": "Polygon", "coordinates": [...]}``),
        or ``None`` / empty dict.  Non-polygon geometry types (``Point``,
        ``LineString``, ``MultiPoint``, …) are returned unchanged because ES
        ``geo_shape`` accepts them as-is — only polygonal area requires coercion
        from ``GeometryCollection`` residuals.

    Returns
    -------
    repaired_geojson:
        The repaired GeoJSON dict, or ``None`` when the geometry is empty /
        ``None`` on entry.  For non-polygon types this is the original dict
        unchanged.
    was_repaired:
        ``True`` when the geometry was modified (make_valid or coercion changed
        it).  ``False`` when the geometry was already valid or is a non-polygon
        type.
    is_poison:
        ``True`` when the geometry collapsed to empty after repair (no polygonal
        area survives) — this geometry would always trigger
        ``invalid_shape_exception`` or ``mapper_parsing_exception`` in ES and
        must be rejected before the item reaches any driver.

    Classification contract
    -----------------------
    The poison types enumerated in
    ``dynastore.modules.elasticsearch.bulk_classify._POISON_ERROR_TYPES``
    include ``invalid_shape_exception`` (duplicate/degenerate coordinates) and
    ``mapper_parsing_exception`` (wrong geometry type for the mapped field).
    Both collapse to an empty result after ``make_valid`` + polygonal coercion
    on the write path, so checking ``is_empty`` on the repaired geometry is
    the single sufficient condition.
    """
    if not geometry:
        return None, False, False

    geom_type = geometry.get("type") if isinstance(geometry, dict) else None
    if not geom_type:
        return geometry, False, False

    # Non-polygonal geometry types are accepted by ES geo_shape as-is and do
    # not require make_valid coercion.  Only Polygon / MultiPolygon /
    # GeometryCollection inputs can produce the known-fatal degenerate shapes.
    polygonal_types = {"Polygon", "MultiPolygon", "GeometryCollection"}
    if geom_type not in polygonal_types:
        return geometry, False, False

    try:
        import shapely.geometry
        from shapely.geometry import shape, mapping
    except ImportError:
        # shapely unavailable at runtime — skip the check; the ES indexer will
        # surface the poison error via the async classify path.
        return geometry, False, False

    try:
        original = shape(geometry)
    except Exception:
        # Unparseable geometry — treat as poison (ES would reject it too).
        return None, False, True

    # Fast path: already valid Polygon or MultiPolygon → ES accepts these as-is.
    # GeometryCollection is always coerced (ES rejects it even when valid).
    is_pg_compatible = isinstance(
        original, (shapely.geometry.Polygon, shapely.geometry.MultiPolygon)
    )
    if is_pg_compatible and original.is_valid:
        return geometry, False, False

    repaired = coerce_to_polygonal(original)

    if repaired.is_empty:
        return None, False, True

    repaired_geojson = mapping(repaired)
    # Mark as repaired when:
    # (a) the geometry TYPE changed (e.g. GeometryCollection → MultiPolygon), OR
    # (b) the geometry coordinates changed (make_valid removed degenerate rings).
    type_changed = type(original) is not type(repaired)
    coords_changed = not original.equals(repaired)
    was_repaired = type_changed or coords_changed
    return repaired_geojson, was_repaired, False
