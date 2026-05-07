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

"""
Geometry Statistics Computation Tool.

Computes geospatial statistics from Shapely geometries based on configuration.
"""

import math
from typing import Dict, Any

from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesStatisticsConfig,
    MorphologicalIndex,
    StatisticStorageMode,
)


def compute_geometry_statistics(
    shapely_geom, config: GeometriesStatisticsConfig
) -> Dict[str, Any]:
    """
    Computes configured statistics from a Shapely geometry.

    Args:
        shapely_geom: A Shapely geometry object (Point, LineString, Polygon, etc.)
        config: Configuration specifying which statistics to compute

    Returns:
        Dictionary of computed statistics suitable for DB insertion.
        For JSONB mode: returns nested dict for single column.
        For Columnar mode: returns flat dict with individual column values.
    """
    stats = {}

    # Basic geometric metrics
    if config.area.enabled:
        stats["area"] = float(shapely_geom.area)

    if config.volume.enabled and hasattr(shapely_geom, "volume"):
        # Volume only valid for 3D closed meshes
        stats["volume"] = float(shapely_geom.volume)

    if config.length.enabled:
        # For Polygons: perimeter, for LineStrings: length
        stats["length"] = float(shapely_geom.length)

    # Centroid
    if config.centroid_type == "geometric":
        c = shapely_geom.centroid

        # Ensure dimensionality match (Shapely centroid often drops Z)
        if shapely_geom.has_z and not c.has_z:
            if shapely_geom.geom_type == "Point":
                c = shapely_geom
            else:
                import shapely

                c = shapely.force_3d(c)

        if config.storage_mode == StatisticStorageMode.COLUMNAR:
            # Explicitly force 3D output if Z present to match column type
            from shapely import wkb

            if c.has_z:
                stats["centroid"] = wkb.dumps(c, hex=True, output_dimension=3)
            else:
                stats["centroid"] = wkb.dumps(c, hex=True)
        else:
            # JSONB mode: store as array
            centroid = [float(c.x), float(c.y)]
            if c.has_z:
                centroid.append(float(c.z))
            stats["centroid"] = centroid

    # Morphological indices
    for idx, should_compute in config.morphological_indices.items():
        if not should_compute:
            continue

        if idx == MorphologicalIndex.CIRCULARITY:
            # Formula: (4 * pi * Area) / (Perimeter^2)
            # Perfect circle = 1.0, less circular < 1.0
            if shapely_geom.length > 0:
                circularity = (4 * math.pi * shapely_geom.area) / (
                    shapely_geom.length**2
                )
                stats["circularity"] = float(circularity)

        elif idx == MorphologicalIndex.CONVEXITY:
            # Ratio of area to convex hull area
            # Perfect convex shape = 1.0, concave < 1.0
            hull_area = shapely_geom.convex_hull.area
            if hull_area > 0:
                convexity = shapely_geom.area / hull_area
                stats["convexity"] = float(convexity)

        elif idx == MorphologicalIndex.ASPECT_RATIO:
            # Ratio of bounding box width to height
            bounds = shapely_geom.bounds  # (minx, miny, maxx, maxy)
            width = bounds[2] - bounds[0]
            height = bounds[3] - bounds[1]
            if height > 0:
                aspect_ratio = width / height
                stats["aspect_ratio"] = float(aspect_ratio)

        # TODO: Implement SPHERICITY and FLATNESS for 3D geometries
        # These require 3D mesh analysis (e.g., using trimesh library)

    # Topology
    if config.vertex_count.enabled:
        if hasattr(shapely_geom, "exterior"):
            # Polygon
            stats["vertex_count"] = len(shapely_geom.exterior.coords)
        elif hasattr(shapely_geom, "coords"):
            # Point or LineString
            stats["vertex_count"] = len(shapely_geom.coords)
        else:
            # MultiPolygon, MultiLineString, etc.
            total_vertices = 0
            for geom in shapely_geom.geoms:
                if hasattr(geom, "exterior"):
                    total_vertices += len(geom.exterior.coords)
                elif hasattr(geom, "coords"):
                    total_vertices += len(geom.coords)
            stats["vertex_count"] = total_vertices

    if config.hole_count.enabled:
        if hasattr(shapely_geom, "interiors"):
            # Polygon with holes
            stats["hole_count"] = len(shapely_geom.interiors)
        else:
            stats["hole_count"] = 0

    return stats
