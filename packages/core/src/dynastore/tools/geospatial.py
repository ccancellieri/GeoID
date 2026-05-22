#    Copyright 2025 FAO
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

from __future__ import annotations

import hashlib
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

# shapely is imported lazily inside the geometry-processing helpers below.
# Module-level `import shapely` would force every consumer of this module
# (e.g. tasks/tiles_preseed/task.py via `from dynastore.tools.geospatial
# import SimplificationAlgorithm`) to require shapely in their image, even
# when they only need the lightweight enum re-export. Same pattern as
# tools/file_io.py and tools/features.py — see PR #141 (`727c0c2`) for the
# original incident: dwh_join Cloud Run Job entry-point load failed with
# `Skipping plugin 'dwh_join': No module named 'shapely'` because file_io's
# eager shapely import propagated through its consumer chain.
if TYPE_CHECKING:
    from shapely.geometry.base import BaseGeometry
    from dynastore.modules.storage.driver_config import GeometriesWriteBehavior

# Try importing optional spatial libraries
try:
    from pyproj import CRS, Transformer
except ImportError:
    CRS = None
    Transformer = None

try:
    import h3
except ImportError:
    h3 = None

try:
    import s2sphere
except ImportError:
    s2sphere = None

from geoalchemy2.elements import WKBElement
from geoalchemy2.shape import to_shape
from .geospatial_exceptions import (
    GeometryProcessingError,
    InvalidWKBError,
    InvalidGeometryError,
    UnfixableGeometryError,
    DisallowedGeometryTypeError,
    SridMismatchError,
    UnsupportedComputedKind,
)

logger = logging.getLogger(__name__)

from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesSidecarConfig,
    TargetDimension,
    SridMismatchPolicy,
    InvalidGeometryPolicy,
    SimplificationAlgorithm,
)

# Issue #220: ``_calculate_geometry_hash`` was removed.  ``geometry_hash`` is
# now a STORED GENERATED column on the geometries sidecar (PG-maintained via
# ``encode(digest(ST_AsBinary(geom), 'sha256'), 'hex')``).  Application code
# never computes the hash — eliminating the skew window between an
# application-computed hash and the actual stored geometry.


def process_geometry(
    geom_wkb_hex: str,
    storage_config: GeometriesSidecarConfig,
    source_srid: Optional[int] = None,
    write_behavior: Optional["GeometriesWriteBehavior"] = None,
) -> Dict[str, Any]:
    """
    Processes a geometry according to the storage configuration, ensuring it's
    ready for database insertion and returning its properties including a 4326 bbox.

    ``storage_config`` carries DDL/storage-shape concerns (``target_srid``,
    ``target_dimension``, ``write_bbox``); ``write_behavior`` carries per-row
    runtime policy (``invalid_geom_policy``, ``srid_mismatch_policy``,
    simplification, allow-list). When ``write_behavior`` is None a fresh
    :class:`GeometriesWriteBehavior` with documented defaults is used —
    matches the historical sidecar defaults (TRANSFORM / ATTEMPT_FIX / no
    simplification / no allow-list).
    """
    from dynastore.modules.storage.driver_config import GeometriesWriteBehavior
    behavior = write_behavior or GeometriesWriteBehavior()
    import shapely
    from shapely import wkb
    from shapely.validation import make_valid
    from shapely.ops import transform

    try:
        # Attempt to parse the WKB, which might be EWKB containing an SRID.
        # geoalchemy2's WKBElement is excellent for this.
        wkb_element = WKBElement(geom_wkb_hex, srid=-1, extended=True)
        shapely_geom = to_shape(wkb_element)
    except Exception as e:
        raise InvalidWKBError(f"Invalid WKB geometry: {e}") from e

    # Determine the initial SRID. Priority order:
    # 2. SRID embedded in the EWKB data.
    # 3. Default to 4326 as a fallback.
    if source_srid is None:
        if wkb_element.srid and wkb_element.srid > 1:
            source_srid = wkb_element.srid
            logger.debug(f"Inferred source SRID {source_srid} from EWKB geometry.")
        else:
            source_srid = 4326  # Fallback to default
            logger.warning(
                f"Source SRID not specified and not found in geometry. Assuming EPSG:{source_srid}."
            )

    processed_geom = shapely_geom

    # 1. Handle SRID Mismatch and Transformation
    if source_srid != storage_config.target_srid:
        if behavior.srid_mismatch_policy == SridMismatchPolicy.REJECT:
            raise SridMismatchError(
                f"SRID mismatch: Incoming SRID {source_srid} != target SRID {storage_config.target_srid}. Policy is REJECT."
            )
        elif behavior.srid_mismatch_policy == SridMismatchPolicy.TRANSFORM:
            if Transformer is None or CRS is None:
                raise GeometryProcessingError(
                    "CRS transformation requested but pyproj is not installed. "
                    "Install the 'crs' extra: pip install dynastore[crs]"
                )
            try:
                transformer = Transformer.from_crs(
                    CRS(f"EPSG:{source_srid}"),
                    CRS(f"EPSG:{storage_config.target_srid}"),
                    always_xy=True,
                )
                processed_geom = transform(transformer.transform, processed_geom)
                logger.debug(
                    f"Geometry transformed from EPSG:{source_srid} to EPSG:{storage_config.target_srid}."
                )
            except Exception as e:
                raise GeometryProcessingError(
                    f"Failed to transform geometry from EPSG:{source_srid} to EPSG:{storage_config.target_srid}: {e}"
                ) from e

    # 2. Handle Invalid Geometries
    if not processed_geom.is_valid:
        if behavior.invalid_geom_policy == InvalidGeometryPolicy.REJECT:
            raise InvalidGeometryError("Geometry is invalid and policy is REJECT.")
        elif behavior.invalid_geom_policy == InvalidGeometryPolicy.ATTEMPT_FIX:
            # Shapely's make_valid is not as robust as PostGIS ST_MakeValid,
            # but it's the best we can do in Python without PostGIS.
            processed_geom = make_valid(processed_geom)
            if not processed_geom.is_valid:
                raise UnfixableGeometryError(
                    "Geometry remains invalid after attempt to fix."
                )

    # 3. Force Dimension
    if storage_config.target_dimension == TargetDimension.FORCE_2D:
        processed_geom = shapely.force_2d(processed_geom)
    elif storage_config.target_dimension == TargetDimension.FORCE_3D:
        processed_geom = shapely.force_3d(processed_geom)

    # 4. Simplification (if configured)
    if (
        behavior.simplification_algorithm
        and behavior.simplification_tolerance
    ):
        if (
            behavior.simplification_algorithm
            == SimplificationAlgorithm.DOUGLAS_PEUCKER
        ):
            processed_geom = processed_geom.simplify(
                behavior.simplification_tolerance, preserve_topology=False
            )
        elif (
            behavior.simplification_algorithm
            == SimplificationAlgorithm.TOPOLOGY_PRESERVING
        ):
            processed_geom = processed_geom.simplify(
                behavior.simplification_tolerance, preserve_topology=True
            )

    # 5. Remove redundant vertices (normalize also handles winding order)
    if behavior.remove_redundant_vertices:
        processed_geom = processed_geom.normalize()

    # 6. Validate allowed geometry types
    if (
        behavior.allowed_geometry_types
        and processed_geom.geom_type not in behavior.allowed_geometry_types
    ):
        raise DisallowedGeometryTypeError(
            f"Geometry type '{processed_geom.geom_type}' not allowed. Allowed types: {behavior.allowed_geometry_types}"
        )

    # geometry_hash is now PG-generated on the geometries sidecar (issue #220)
    # — no application-side computation here.

    # Calculate bbox_coords in EPSG:4326 for validation
    bbox_coords = None
    if storage_config.write_bbox:
        current_srid_for_bbox = storage_config.target_srid
        bbox_geom = processed_geom

        # If the processed_geom is not already in 4326, transform it temporarily for bbox calculation
        if current_srid_for_bbox != 4326:
            if Transformer is None or CRS is None:
                logger.warning(
                    "pyproj not installed — cannot transform bbox to EPSG:4326. "
                    "Bbox will be in the storage SRID."
                )
                bbox_coords = list(bbox_geom.bounds)
            else:
                try:
                    transformer_to_4326 = Transformer.from_crs(
                        CRS(f"EPSG:{current_srid_for_bbox}"),
                        CRS("EPSG:4326"),
                        always_xy=True,
                    )
                    bbox_geom = transform(transformer_to_4326.transform, processed_geom)
                    logger.debug(f"Calculated bbox in EPSG:4326 for validation.")
                except Exception as e:
                    logger.warning(
                        f"Could not transform geometry to EPSG:4326 for bbox calculation: {e}. Bbox might be in original SRID and cause validation errors."
                    )
                    bbox_coords = list(bbox_geom.bounds)

        if bbox_coords is None:  # If transformation was successful or already in 4326
            bbox_coords = list(bbox_geom.bounds)

    # Determine WKB output dimension
    # For shapely < 2.0 output_dimension might be ignored or handled differently, but for 2.0+ it enforces coordinate dimension
    if storage_config.target_dimension == TargetDimension.FORCE_3D:
        wkb_hex_processed = wkb.dumps(processed_geom, hex=True, output_dimension=3)
    elif storage_config.target_dimension == TargetDimension.FORCE_2D:
        wkb_hex_processed = wkb.dumps(processed_geom, hex=True, output_dimension=2)
    else:
        wkb_hex_processed = wkb.dumps(processed_geom, hex=True)

    return {
        "geom_type": processed_geom.geom_type,
        "wkb_hex_processed": wkb_hex_processed,
        "bbox_coords": bbox_coords,
        "centroid": (processed_geom.centroid.x, processed_geom.centroid.y),
        "shapely_geom": processed_geom,
    }


def calculate_spatial_indices_from_centroid(
    centroid_lon: float,
    centroid_lat: float,
    h3_resolutions: List[int],
    s2_resolutions: List[int],
) -> Dict[str, Any]:
    """
    Calculates H3 and S2 spatial indices from centroid coordinates.
    Assumes coordinates are in EPSG:4326 (lon, lat).

    This is a memory-efficient alternative to calculate_spatial_indices
    that doesn't require keeping the full Shapely geometry in memory.
    """
    indices = {}

    # H3 indices
    if h3_resolutions and h3:
        try:
            for res in h3_resolutions:
                h3_index = h3.latlng_to_cell(centroid_lat, centroid_lon, res)
                if isinstance(h3_index, str):
                    h3_index = int(h3_index, 16)
                indices[f"h3_res{res}"] = h3_index
        except AttributeError:
            # Fallback for H3 v3.x
            try:
                for res in h3_resolutions:
                    h3_index = h3.geo_to_h3(centroid_lat, centroid_lon, res)  # type: ignore[attr-defined]
                    if isinstance(h3_index, str):
                        h3_index = int(h3_index, 16)
                    indices[f"h3_res{res}"] = h3_index
            except Exception as e:
                logger.warning(f"H3 index calculation failed (v3 fallback): {e}")
        except Exception as e:
            logger.warning(f"H3 index calculation failed: {e}")
    elif h3_resolutions and not h3:
        logger.warning("H3 library not installed. Skipping H3 indices.")

    # S2 indices
    if s2_resolutions and s2sphere:
        try:
            for res in s2_resolutions:
                latlng = s2sphere.LatLng.from_degrees(centroid_lat, centroid_lon)
                cell_id = s2sphere.CellId.from_lat_lng(latlng).parent(res)
                if cell_id is not None:
                    indices[f"s2_res{res}"] = cell_id.id()
        except Exception as e:
            logger.warning(f"S2 index calculation failed: {e}")
    elif s2_resolutions and not s2sphere:
        logger.warning("s2sphere library not installed. Skipping S2 indices.")

    return indices


def calculate_spatial_indices(
    geom: "BaseGeometry", h3_resolutions: List[int], s2_resolutions: List[int]
) -> Dict[str, Any]:
    """
    Calculates H3 and S2 spatial indices for a given Shapely geometry.
    The geometry is expected to be in EPSG:4326 for accurate index calculation.
    """
    from shapely.ops import transform

    indices = {}
    if not geom.is_empty:
        # The SRID of the geometry is now managed by the caller, but for H3/S2, we must work in 4326.
        # We assume the caller provides a geometry whose CRS is known.
        geom_srid = 4326  # Assume 4326 for now, this should be passed in or handled by caller logic

        # Ensure geometry is in 4326 for H3/S2 if possible
        if geom_srid != 4326:
            if Transformer is None or CRS is None:
                logger.warning("pyproj not installed — cannot transform geometry to EPSG:4326 for spatial index calculation. Skipping.")
                return indices
            try:
                transformer_to_4326 = Transformer.from_crs(
                    CRS(f"EPSG:{geom_srid}"), CRS("EPSG:4326"), always_xy=True
                )
                geom_4326 = transform(transformer_to_4326.transform, geom)
            except Exception as e:
                logger.warning(
                    f"Could not transform geometry to EPSG:4326 for spatial index calculation: {e}. Skipping H3/S2 indices."
                )
                return indices
        else:
            geom_4326 = geom

        # Calculate centroid for point-based indices
        centroid = geom_4326.centroid
        if not centroid.is_empty:
            # H3 indices
            if h3_resolutions and h3:
                try:
                    for res in h3_resolutions:
                        h3_index = h3.latlng_to_cell(
                            centroid.y, centroid.x, res
                        )  # v4 returns int
                        if isinstance(
                            h3_index, str
                        ):  # Safety for unexpected string output
                            h3_index = int(h3_index, 16)
                        indices[f"h3_res{res}"] = h3_index
                except AttributeError:
                    # Fallback for H3 v3.x if v4 is not present but h3 is
                    try:
                        for res in h3_resolutions:
                            h3_index = h3.geo_to_h3(  # type: ignore[attr-defined]
                                centroid.y, centroid.x, res
                            )  # v3 returns int
                            if isinstance(
                                h3_index, str
                            ):  # Safety for unexpected string output
                                h3_index = int(h3_index, 16)
                            indices[f"h3_res{res}"] = h3_index
                    except Exception as e:
                        logger.warning(
                            f"H3 index calculation failed (v3 fallback): {e}"
                        )
                except Exception as e:
                    logger.warning(f"H3 index calculation failed: {e}")
            elif h3_resolutions and not h3:
                logger.warning("H3 library not installed. Skipping H3 indices.")

            # S2 indices
            if s2_resolutions and s2sphere:
                try:
                    for res in s2_resolutions:
                        latlng = s2sphere.LatLng.from_degrees(centroid.y, centroid.x)
                        cell_id = s2sphere.CellId.from_lat_lng(latlng).parent(res)
                        if cell_id is not None:
                            indices[f"s2_res{res}"] = cell_id.id()
                except Exception as e:
                    logger.warning(f"S2 index calculation failed: {e}")
            elif s2_resolutions and not s2sphere:
                logger.warning("s2sphere library not installed. Skipping S2 indices.")
    return indices


def get_spatial_indices_for_bbox(
    bbox_coords: Tuple[float, float, float, float], h3_res: List[int], s2_res: List[int]
) -> Dict[str, List[str]]:
    """
    Calculates the set of H3 and S2 cell IDs that cover the given bounding box.
    (BBOX is in Xmin, Ymin, Xmax, Ymax order)
    """
    xmin, ymin, xmax, ymax = bbox_coords

    indices = {"h3_cells": [], "s2_cells": []}

    # H3 Optimization
    if h3_res and h3:
        target_h3_res = max(h3_res)
        try:
            # Construct GeoJSON for the bbox (Polygon)
            # Must be a closed loop: start coordinate == end coordinate
            geojson_poly = {
                "type": "Polygon",
                "coordinates": [
                    [
                        [xmin, ymin],
                        [xmin, ymax],
                        [xmax, ymax],
                        [xmax, ymin],
                        [xmin, ymin],
                    ]
                ],
            }

            # Use 'polygon_to_cells' (v4) or fallback to 'polyfill' (v3)
            if hasattr(h3, "polygon_to_cells"):
                indices["h3_cells"] = list(
                    h3.polygon_to_cells(geojson_poly, res=target_h3_res)
                )
            elif hasattr(h3, "polyfill"):
                indices["h3_cells"] = list(
                    h3.polyfill(geojson_poly, target_h3_res, geo_json_conformant=True)  # type: ignore[attr-defined]
                )

        except Exception as e:
            logger.error(f"H3 optimization failed: {e}")

    # S2 Optimization
    if s2_res and s2sphere:
        target_s2_res = max(s2_res)
        try:
            # S2 S2LatLngRect.from_point_pair is the robust way to create a rect from corners
            p1 = s2sphere.LatLng.from_degrees(ymin, xmin)
            p2 = s2sphere.LatLng.from_degrees(ymax, xmax)
            s2_rect = s2sphere.LatLngRect.from_point_pair(p1, p2)

            rc = s2sphere.RegionCoverer()

            # Configure coverer to be safe
            rc.min_level = 0
            rc.max_level = target_s2_res
            rc.max_cells = 1000  # Prevent exploding cell counts on large bboxes

            # Get covering
            cell_union = rc.get_covering(s2_rect)

            indices["s2_cells"] = [cell_id.to_token() for cell_id in cell_union]

        except Exception as e:
            logger.error(f"S2 optimization failed (RegionCoverer): {e}")

    return indices

# ---------------------------------------------------------------------------
# compute_derived_fields — phase 1 of items-policy consolidation (#957/#950).
#
# Takes a Shapely geometry + the feature's properties dict + a list of
# ``ComputedField`` entries and returns a dict keyed by each entry's
# ``resolved_name``. Drivers consult this dict during ``write_entities`` to
# materialise the declared values however the underlying store prefers
# (PG column, ES numeric, DuckDB sort key, Iceberg partition).
#
# EXTERNAL_ID is intentionally NOT handled here — it is path-extracted from
# the feature via ``ItemsWritePolicy.external_id_field`` upstream.
# ---------------------------------------------------------------------------

import json as _json

# Base32 alphabet used by the standard geohash scheme.
_GEOHASH_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def _encode_geohash(lat: float, lon: float, precision: int) -> str:
    """Standard 32-character geohash encoder. Bisects lon/lat alternately."""
    lat_lo, lat_hi = -90.0, 90.0
    lon_lo, lon_hi = -180.0, 180.0
    geohash: List[str] = []
    bits = [16, 8, 4, 2, 1]
    bit = 0
    ch = 0
    even = True
    while len(geohash) < precision:
        if even:
            mid = (lon_lo + lon_hi) / 2
            if lon >= mid:
                ch |= bits[bit]
                lon_lo = mid
            else:
                lon_hi = mid
        else:
            mid = (lat_lo + lat_hi) / 2
            if lat >= mid:
                ch |= bits[bit]
                lat_lo = mid
            else:
                lat_hi = mid
        even = not even
        if bit < 4:
            bit += 1
        else:
            geohash.append(_GEOHASH_BASE32[ch])
            bit = 0
            ch = 0
    return "".join(geohash)


def _canonical_attributes_hash(properties: Dict[str, Any]) -> str:
    """sha256(canonical-json(properties)). Sort keys for determinism."""
    payload = _json.dumps(
        properties,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _geometry_hash(geom: "BaseGeometry") -> str:
    """sha256(WKB) — mirrors the PG-side STORED GENERATED column (#220)."""
    from shapely import wkb as _wkb

    return hashlib.sha256(_wkb.dumps(geom)).hexdigest()


def _extract_feature_property(root_props: Dict[str, Any], path: str) -> Any:
    """Resolve a dotted path against a feature's ``properties`` dict.

    ``"properties.adm2_pcode"`` walks the dict; the leading ``"properties."``
    segment is stripped because the caller already passes the properties dict.
    A bare attribute name (e.g. ``"adm2_pcode"``) is read directly. Returns
    ``None`` when any segment is missing.
    """
    segments = path.split(".")
    if segments and segments[0] == "properties":
        segments = segments[1:]
    current: Any = root_props
    for seg in segments:
        if isinstance(current, dict) and seg in current:
            current = current[seg]
        else:
            return None
    return current


def _crs_is_geographic(srid: Optional[int]) -> bool:
    """True when ``srid`` denotes a lon/lat (angular) CRS.

    ``None`` returns ``False`` so callers that do not declare a CRS keep the
    historical planar behaviour. EPSG:4326 short-circuits without pyproj.
    """
    if srid is None:
        return False
    try:
        srid_int = int(srid)
    except (TypeError, ValueError):
        return False
    if srid_int == 4326:
        return True
    try:
        from pyproj import CRS

        return bool(CRS.from_epsg(srid_int).is_geographic)
    except Exception:
        return False


def _geodesic_area_perimeter(
    geometry: "BaseGeometry",
) -> Optional[Tuple[float, float]]:
    """``(area_m2, perimeter_m)`` on the WGS84 ellipsoid, or ``None``.

    Returns ``None`` when pyproj is unavailable so the caller can fall back to
    the planar Shapely values.
    """
    try:
        from pyproj import Geod

        area, perim = Geod(ellps="WGS84").geometry_area_perimeter(geometry)
        return abs(float(area)), abs(float(perim))
    except Exception:
        return None


def _geodesic_length(geometry: "BaseGeometry") -> Optional[float]:
    """Geodesic length in metres on the WGS84 ellipsoid, or ``None``."""
    try:
        from pyproj import Geod

        return float(Geod(ellps="WGS84").geometry_length(geometry))
    except Exception:
        return None


def compute_derived_fields(
    geometry: "BaseGeometry",
    properties: Dict[str, Any],
    fields: "List[Any]",
    srid: Optional[int] = None,
) -> Dict[str, Any]:
    """Materialise the declared :class:`ComputedField` entries for one feature.

    Returns a dict keyed by each field's ``resolved_name``. Raises
    :class:`UnsupportedComputedKind` if a kind cannot be materialised
    (missing optional library, geometry type mismatch).

    ``EXTERNAL_ID`` is dropped silently — callers strip it upstream since
    it is path-extracted from the feature, not derived from geometry/
    properties.

    ``srid`` is the CRS the ``geometry`` coordinates are expressed in. When it
    identifies a geographic CRS (e.g. EPSG:4326) the absolute metrics
    (``area``/``perimeter``/``length``) are computed on the WGS84 ellipsoid and
    returned in metres / square metres rather than the meaningless square
    degrees that Shapely's planar ``.area`` would yield on lon/lat coordinates.
    Omitting ``srid`` preserves the planar behaviour.
    """
    # Local import to avoid a cycle: computed_fields imports from
    # driver_config, which is itself imported from many modules. Keeping
    # the import local lets `geospatial.py` stay importable from
    # ``computed_fields`` future direction without ordering pain.
    from dynastore.modules.storage.computed_fields import (
        ComputedKind,
    )

    out: Dict[str, Any] = {}
    centroid_lat: Optional[float] = None
    centroid_lon: Optional[float] = None

    def _centroid() -> Tuple[float, float]:
        nonlocal centroid_lat, centroid_lon
        if centroid_lat is None:
            c = geometry.centroid
            centroid_lat = float(c.y)
            centroid_lon = float(c.x)
        return centroid_lat, centroid_lon  # type: ignore[return-value]

    geographic = _crs_is_geographic(srid)
    _ap_memo: List[Optional[Tuple[float, float]]] = []

    def _area_perimeter() -> Optional[Tuple[float, float]]:
        """Geodesic ``(area_m2, perimeter_m)`` when geographic, else ``None``."""
        if not geographic:
            return None
        if not _ap_memo:
            _ap_memo.append(_geodesic_area_perimeter(geometry))
        return _ap_memo[0]

    for f in fields:
        kind = f.kind
        key = f.resolved_name

        if kind == ComputedKind.EXTERNAL_ID:
            # ``name`` is the dotted JSON path into the feature; no name
            # means "external_id sits at properties.external_id".
            path = f.name or "properties.external_id"
            value = _extract_feature_property(properties, path)
            # ``resolved_name`` for EXTERNAL_ID falls back to ``"external_id"``
            # so downstream code has a stable key regardless of path.
            out["external_id"] = value
            continue

        if kind == ComputedKind.GEOMETRY_HASH:
            out[key] = _geometry_hash(geometry)

        elif kind == ComputedKind.ATTRIBUTES_HASH:
            out[key] = _canonical_attributes_hash(properties)

        elif kind == ComputedKind.GEOHASH:
            lat, lon = _centroid()
            out[key] = _encode_geohash(lat, lon, f.resolution)

        elif kind == ComputedKind.H3:
            if h3 is None:
                raise UnsupportedComputedKind(
                    "ComputedKind.H3 requires the optional 'h3' library"
                )
            lat, lon = _centroid()
            try:
                cell = h3.latlng_to_cell(lat, lon, f.resolution)
            except AttributeError:
                # h3 v3.x fallback
                cell = h3.geo_to_h3(lat, lon, f.resolution)  # type: ignore[attr-defined]
            out[key] = cell

        elif kind == ComputedKind.S2:
            if s2sphere is None:
                raise UnsupportedComputedKind(
                    "ComputedKind.S2 requires the optional 's2sphere' library"
                )
            lat, lon = _centroid()
            latlng = s2sphere.LatLng.from_degrees(lat, lon)
            cell = s2sphere.CellId.from_lat_lng(latlng).parent(f.resolution)
            out[key] = cell.to_token()  # type: ignore[union-attr]

        elif kind == ComputedKind.AREA:
            ap = _area_perimeter()
            out[key] = ap[0] if ap is not None else float(geometry.area)

        elif kind == ComputedKind.VOLUME:
            # Shapely exposes .volume only on 3D closed types (Solid).
            volume = getattr(geometry, "volume", None)
            out[key] = float(volume) if volume is not None else 0.0

        elif kind == ComputedKind.CIRCULARITY:
            import math as _math
            # Use geodesic area & perimeter together when geographic so the
            # ratio stays dimensionless and consistent (mixing m² with degrees
            # would corrupt it).
            ap = _area_perimeter()
            if ap is not None:
                area_val, perim_val = ap
            else:
                area_val, perim_val = float(geometry.area), float(geometry.length)
            if perim_val > 0:
                out[key] = float((4 * _math.pi * area_val) / (perim_val ** 2))
            else:
                out[key] = 0.0

        elif kind == ComputedKind.CONVEXITY:
            ap = _area_perimeter()
            if ap is not None:
                area_val = ap[0]
                hull_ap = _geodesic_area_perimeter(geometry.convex_hull)
                hull_area = hull_ap[0] if hull_ap is not None else 0.0
            else:
                area_val = float(geometry.area)
                hull_area = float(geometry.convex_hull.area)
            if hull_area > 0:
                out[key] = float(area_val / hull_area)
            else:
                out[key] = 0.0

        elif kind == ComputedKind.ASPECT_RATIO:
            minx, miny, maxx, maxy = geometry.bounds
            width = maxx - minx
            height = maxy - miny
            out[key] = float(width / height) if height > 0 else 0.0

        elif kind == ComputedKind.PERIMETER:
            # Perimeter is well-defined for polygons; for line/point we
            # report 0 / 0 rather than raising — matches Shapely's
            # ``.length`` on a Polygon returning the perimeter.
            if geometry.geom_type in ("Polygon", "MultiPolygon"):
                ap = _area_perimeter()
                out[key] = ap[1] if ap is not None else float(geometry.length)
            else:
                out[key] = 0.0

        elif kind == ComputedKind.LENGTH:
            gl = _geodesic_length(geometry) if geographic else None
            out[key] = gl if gl is not None else float(geometry.length)

        elif kind == ComputedKind.CENTROID:
            c = geometry.centroid
            # When ``centroid_type`` is set, the field is destined for a
            # PostGIS ``GEOMETRY(POINT[Z], srid)`` column and the WKB hex
            # is the canonical materialisation. ``POINTZ`` forces 3D
            # output (Shapely centroid drops Z by default); ``POINT``
            # emits 2D. With ``centroid_type=None`` the field is either
            # not stored or stored as a JSONB array.
            centroid_type = getattr(f, "centroid_type", None)
            if centroid_type is not None:
                from shapely import wkb as _wkb_mod
                if centroid_type == "POINTZ":
                    if not c.has_z:
                        if geometry.has_z and geometry.geom_type == "Point":
                            c = geometry
                        else:
                            import shapely as _sh
                            c = _sh.force_3d(c)
                    out[key] = _wkb_mod.dumps(c, hex=True, output_dimension=3)
                else:
                    # POINT: ensure 2D output even if input is 3D.
                    if c.has_z:
                        import shapely as _sh
                        c = _sh.force_2d(c)
                    out[key] = _wkb_mod.dumps(c, hex=True)
            else:
                out[key] = [float(c.x), float(c.y)]

        elif kind == ComputedKind.BBOX:
            minx, miny, maxx, maxy = geometry.bounds
            out[key] = [float(minx), float(miny), float(maxx), float(maxy)]

        elif kind == ComputedKind.VERTEX_COUNT:
            exterior = getattr(geometry, "exterior", None)
            geoms = getattr(geometry, "geoms", None)
            if exterior is not None:
                out[key] = len(exterior.coords)
            elif hasattr(geometry, "coords"):
                out[key] = len(geometry.coords)
            elif geoms is not None:
                total = 0
                for g in geoms:
                    g_exterior = getattr(g, "exterior", None)
                    if g_exterior is not None:
                        total += len(g_exterior.coords)
                    elif hasattr(g, "coords"):
                        total += len(g.coords)
                out[key] = total
            else:
                out[key] = 0

        elif kind == ComputedKind.HOLE_COUNT:
            interiors = getattr(geometry, "interiors", None)
            geoms = getattr(geometry, "geoms", None)
            if interiors is not None:
                out[key] = len(list(interiors))
            elif geoms is not None:
                total = 0
                for g in geoms:
                    g_interiors = getattr(g, "interiors", None)
                    if g_interiors is not None:
                        total += len(list(g_interiors))
                out[key] = total
            else:
                out[key] = 0

        else:
            raise UnsupportedComputedKind(
                f"ComputedKind.{kind.name} is not implemented in compute_derived_fields(). "
                "3D place-statistics (SURFACE_AREA, Z_RANGE, CENTROID_3D, etc.) are "
                "computed from the JSON-FG 'place' member via compute_place_derived_fields()."
            )

    return out


def compute_attribute_derived_fields(
    properties: Dict[str, Any],
    fields: "List[Any]",
) -> Dict[str, Any]:
    """Materialise declared ``ATTRIBUTE_STAT`` :class:`ComputedField` entries.

    Each field promotes a single value read from the feature's ``properties``
    at the field's ``source`` dotted path (e.g. ``"properties.population"``)
    into the attributes sidecar. Returns a dict keyed by each field's
    ``resolved_name``; fields whose source path is absent are skipped (the
    read-side ``failure_mode`` decides whether a missing value is fatal).

    Mirrors :func:`compute_derived_fields` for the attribute axis. Cross-field
    expressions are out of scope for v1 — a field reads exactly one path.
    """
    from dynastore.modules.storage.computed_fields import ComputedKind

    out: Dict[str, Any] = {}
    props = properties or {}
    for f in fields:
        if f.kind != ComputedKind.ATTRIBUTE_STAT:
            continue
        value = _extract_feature_property(props, f.source or "")
        if value is not None:
            out[f.resolved_name] = value
    return out


# ---------------------------------------------------------------------------
# compute_place_derived_fields — JSON-FG 'place' member statistics.
#
# Handles the 3D place-stat ComputedKinds (all members of _PLACE_TABLE_KINDS).
# Input is the raw JSON-FG 'place' dict (Solid, Prism, 3D Curve, …) plus the
# declared ComputedField list and optional JSON-FG validity interval.
#
# Design note on SURFACE_AREA for arbitrary Solid meshes: the triangulated-
# divergence-theorem approach used here gives exact results for any closed
# polyhedral mesh. For curved surfaces (B-rep, NURBS) that PostGIS ST_3DArea
# handles, the Python result will be approximate — this is documented in the
# kind's handler below and is the honest behaviour.
# ---------------------------------------------------------------------------

def compute_place_derived_fields(
    place_dict: Dict[str, Any],
    fields: "List[Any]",
    validity: Any = None,
) -> Dict[str, Any]:
    """Materialise declared 3D place-stat :class:`ComputedField` entries.

    ``place_dict`` is the raw JSON-FG ``place`` geometry dict.
    ``fields`` is the subset of ``compute_fields_overlay`` whose kinds are
    in ``_PLACE_TABLE_KINDS``.  ``validity`` is an optional JSON-FG ``time``
    interval value (dict / str / tuple) for ``TEMPORAL_DURATION``.

    Returns a dict keyed by each field's ``resolved_name``. Fields that
    cannot be derived (wrong geometry type, missing Z) are silently skipped.
    """
    from dynastore.modules.storage.computed_fields import ComputedKind

    out: Dict[str, Any] = {}
    geom_type = place_dict.get("type", "") if place_dict else ""

    for f in fields:
        kind = f.kind
        key = f.resolved_name
        try:
            if kind == ComputedKind.Z_RANGE:
                zs = _place_collect_z(place_dict)
                if zs:
                    out[key] = float(max(zs) - min(zs))

            elif kind == ComputedKind.CENTROID_3D:
                result = _place_centroid_3d(place_dict, geom_type)
                if result is not None:
                    out[key] = result

            elif kind == ComputedKind.VERTICAL_GRADIENT:
                result = _place_vertical_gradient(place_dict, geom_type)
                if result is not None:
                    out[key] = result

            elif kind == ComputedKind.SURFACE_AREA:
                result = _place_surface_area(place_dict, geom_type)
                if result is not None:
                    out[key] = result

            elif kind == ComputedKind.SURFACE_TO_VOLUME_RATIO:
                vol = _place_volume(place_dict, geom_type)
                sa = out.get("surface_area") or _place_surface_area(place_dict, geom_type)
                if vol and sa and vol > 0:
                    out[key] = float(sa) / float(vol)

            elif kind == ComputedKind.NET_FLOOR_AREA:
                result = _place_net_floor_area(place_dict, geom_type)
                if result is not None:
                    out[key] = result

            elif kind == ComputedKind.TEMPORAL_DURATION:
                result = _place_temporal_duration(validity)
                if result is not None:
                    out[key] = result

        except Exception as exc:
            logger.warning(
                f"compute_place_derived_fields: kind={kind.value} geom_type={geom_type}: {exc}"
            )

    return out


# --- Internal geometry helpers for place-stat kinds -------------------------

def _place_collect_z(place: Dict[str, Any]) -> List[float]:
    geom_type = place.get("type", "")
    if geom_type == "Prism":
        lower = place.get("lower")
        upper = place.get("upper")
        return [float(lower), float(upper)] if lower is not None and upper is not None else []
    if geom_type in ("MultiPrism",):
        zs: List[float] = []
        for p in place.get("prisms", []):
            zs.extend(_place_collect_z(p))
        return zs
    if geom_type in ("Solid", "MultiSolid"):
        coords = _place_flat_coords(place)
        return [c[2] for c in coords if len(c) >= 3]
    # GeoJSON-compatible: coordinates
    raw = place.get("coordinates")
    if raw is None:
        return []
    flat = _flatten_coords(raw)
    return [c[2] for c in flat if len(c) >= 3]


def _flatten_coords(raw: Any) -> List[List[float]]:
    if not raw:
        return []
    if isinstance(raw[0], (int, float)):
        return [raw]
    result: List[List[float]] = []
    for item in raw:
        result.extend(_flatten_coords(item))
    return result


def _place_flat_coords(place: Dict[str, Any]) -> List[Any]:
    geom_type = place.get("type", "")
    if geom_type == "Solid":
        coords: List[Any] = []
        for shell in place.get("exterior", []):
            if isinstance(shell, list):
                for ring in shell:
                    if isinstance(ring, list):
                        coords.extend(ring)
        return coords
    if geom_type == "MultiSolid":
        coords = []
        for solid in place.get("solids", []):
            coords.extend(_place_flat_coords(solid))
        return coords
    return []


def _triangle_surface_volume(v0: Any, v1: Any, v2: Any) -> Tuple[float, float]:
    import math as _math
    ax, ay, az = float(v0[0]), float(v0[1]), float(v0[2])
    bx, by, bz = float(v1[0]), float(v1[1]), float(v1[2])
    cx, cy, cz = float(v2[0]), float(v2[1]), float(v2[2])
    ux, uy, uz = bx - ax, by - ay, bz - az
    vx, vy, vz = cx - ax, cy - ay, cz - az
    nx = uy * vz - uz * vy
    ny = uz * vx - ux * vz
    nz = ux * vy - uy * vx
    area = _math.sqrt(nx * nx + ny * ny + nz * nz) / 2.0
    signed_vol = ax * (by * cz - bz * cy) + bx * (cy * az - cz * ay) + cx * (ay * bz - az * by)
    return area, signed_vol


def _place_surface_area(place: Dict[str, Any], geom_type: str) -> Optional[float]:
    if geom_type == "Prism":
        from shapely.geometry import shape as _shape
        base = place.get("base")
        lower, upper = place.get("lower"), place.get("upper")
        if not base:
            return None
        try:
            poly = _shape(base)
            height = abs(float(upper) - float(lower)) if lower is not None and upper is not None else None
            if height is None:
                return None
            return float(2 * poly.area + poly.length * height)
        except Exception:
            return None
    if geom_type == "Solid":
        faces = []
        for shell in place.get("exterior", []):
            if isinstance(shell, list):
                for ring in shell:
                    if isinstance(ring, list) and len(ring) >= 3:
                        faces.append(ring)
        total = 0.0
        for face in faces:
            v0 = face[0]
            for i in range(1, len(face) - 2):
                if len(v0) >= 3 and len(face[i]) >= 3 and len(face[i + 1]) >= 3:
                    area, _ = _triangle_surface_volume(v0, face[i], face[i + 1])
                    total += area
        return total if faces else None
    if geom_type == "MultiSolid":
        totals = [_place_surface_area(s, "Solid") for s in place.get("solids", [])]
        vals = [t for t in totals if t is not None]
        return sum(vals) if vals else None
    if geom_type == "MultiPrism":
        totals = [_place_surface_area(p, "Prism") for p in place.get("prisms", [])]
        vals = [t for t in totals if t is not None]
        return sum(vals) if vals else None
    return None


def _place_volume(place: Dict[str, Any], geom_type: str) -> Optional[float]:
    if geom_type == "Prism":
        from shapely.geometry import shape as _shape
        base = place.get("base")
        lower, upper = place.get("lower"), place.get("upper")
        if not base or lower is None or upper is None:
            return None
        try:
            return float(_shape(base).area) * abs(float(upper) - float(lower))
        except Exception:
            return None
    if geom_type == "Solid":
        faces = []
        for shell in place.get("exterior", []):
            if isinstance(shell, list):
                for ring in shell:
                    if isinstance(ring, list) and len(ring) >= 3:
                        faces.append(ring)
        total = 0.0
        for face in faces:
            v0 = face[0]
            for i in range(1, len(face) - 2):
                if len(v0) >= 3 and len(face[i]) >= 3 and len(face[i + 1]) >= 3:
                    _, sv = _triangle_surface_volume(v0, face[i], face[i + 1])
                    total += sv
        return abs(total) / 6.0 if faces else None
    if geom_type == "MultiSolid":
        vals = [_place_volume(s, "Solid") for s in place.get("solids", [])]
        nums = [v for v in vals if v is not None]
        return sum(nums) if nums else None
    if geom_type == "MultiPrism":
        vals = [_place_volume(p, "Prism") for p in place.get("prisms", [])]
        nums = [v for v in vals if v is not None]
        return sum(nums) if nums else None
    return None


def _place_net_floor_area(place: Dict[str, Any], geom_type: str) -> Optional[float]:
    if geom_type == "Prism":
        from shapely.geometry import shape as _shape
        base = place.get("base")
        if not base:
            return None
        try:
            return float(_shape(base).area)
        except Exception:
            return None
    if geom_type == "MultiPrism":
        vals = [_place_net_floor_area(p, "Prism") for p in place.get("prisms", [])]
        nums = [v for v in vals if v is not None]
        return sum(nums) if nums else None
    return None


def _place_centroid_3d(place: Dict[str, Any], geom_type: str) -> Optional[List[float]]:
    if geom_type == "Prism":
        from shapely.geometry import shape as _shape
        base = place.get("base")
        lower, upper = place.get("lower"), place.get("upper")
        if not base:
            return None
        try:
            c = _shape(base).centroid
            mid_z = (float(lower) + float(upper)) / 2.0 if lower is not None and upper is not None else 0.0
            return [float(c.x), float(c.y), mid_z]
        except Exception:
            return None
    if geom_type == "Solid":
        coords = _place_flat_coords(place)
        z_coords = [c for c in coords if len(c) >= 3]
        if not z_coords:
            return None
        cx = sum(float(c[0]) for c in z_coords) / len(z_coords)
        cy = sum(float(c[1]) for c in z_coords) / len(z_coords)
        cz = sum(float(c[2]) for c in z_coords) / len(z_coords)
        return [cx, cy, cz]
    zs = _place_collect_z(place)
    flat = _flatten_coords(place.get("coordinates") or [])
    if flat and zs:
        cx = sum(float(c[0]) for c in flat) / len(flat)
        cy = sum(float(c[1]) for c in flat) / len(flat)
        return [cx, cy, (max(zs) + min(zs)) / 2.0]
    return None


def _place_vertical_gradient(place: Dict[str, Any], geom_type: str) -> Optional[float]:
    import math as _math
    if geom_type not in ("LineString", "MultiLineString"):
        return None
    coords = _flatten_coords(place.get("coordinates") or [])
    if len(coords) < 2:
        return None
    total_rise = sum(
        abs(float(coords[i][2]) - float(coords[i - 1][2]))
        for i in range(1, len(coords))
        if len(coords[i]) >= 3 and len(coords[i - 1]) >= 3
    )
    total_run = sum(
        _math.hypot(
            float(coords[i][0]) - float(coords[i - 1][0]),
            float(coords[i][1]) - float(coords[i - 1][1]),
        )
        for i in range(1, len(coords))
    )
    return float(total_rise / total_run) if total_run > 0 else 0.0


def _place_temporal_duration(validity: Any) -> Optional[float]:
    """Compute duration in seconds from a JSON-FG validity interval."""
    try:
        from datetime import datetime

        def _parse(v: Any) -> Optional[datetime]:
            if isinstance(v, datetime):
                return v
            if isinstance(v, str):
                return datetime.fromisoformat(v.strip("[()] ").replace("Z", "+00:00"))
            return None

        if isinstance(validity, dict):
            start = _parse(validity.get("lower") or validity.get("start"))
            end = _parse(validity.get("upper") or validity.get("end"))
        elif isinstance(validity, (list, tuple)) and len(validity) == 2:
            start, end = _parse(validity[0]), _parse(validity[1])
        elif isinstance(validity, str):
            parts = validity.strip("[]()").split(",")
            if len(parts) == 2:
                start, end = _parse(parts[0].strip()), _parse(parts[1].strip())
            else:
                return None
        else:
            return None

        if start and end:
            return (end - start).total_seconds()
    except Exception as exc:
        logger.debug(f"_place_temporal_duration: {exc}")
    return None
