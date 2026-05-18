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
