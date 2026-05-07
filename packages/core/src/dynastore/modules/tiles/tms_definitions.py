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

# dynastore/modules/tiles/tms_definitions.py

from typing import Dict, Any, List
from pyproj import CRS
from .tiles_models import TileMatrixSet, TileMatrix, BoundingBox

def _generate_quad_matrices(
    levels: int, 
    extent: List[float], 
    zero_level_matrix_width: int = 1, 
    zero_level_matrix_height: int = 1,
    tile_size: int = 256
) -> List[Dict[str, Any]]:
    """
    Generates OGC-compliant TileMatrix definitions for a quad-tree based set.
    Calculates resolutions dynamically based on the extent and tile size.
    """
    matrices = []
    # Width of the world at level 0 in CRS units
    map_width_crs = extent[2] - extent[0]
    
    # Resolution at level 0 (units per pixel)
    # res = map_width / (tile_width * matrix_width)
    initial_res = map_width_crs / (tile_size * zero_level_matrix_width)
    
    # Standardized pixel size (0.28mm) for scale denominator calc
    # OGC assumes 0.28mm/pixel (approx 90.7 DPI)
    # Scale Denom = Resolution * (MetersPerUnit / 0.00028)
    # We approximate this ratio for Mercator/WGS84 for standard compliance keys
    # but strictly we rely on 'cellSize' (resolution) for calculation.
    
    # Base scale reference for WebMercator (approximate)
    base_scale_denom = 559082264.0287178

    for i in range(levels):
        matrix_width = zero_level_matrix_width * (2 ** i)
        matrix_height = zero_level_matrix_height * (2 ** i)
        
        resolution = initial_res / (2 ** i)
        
        # Heuristic for scale denom if not strictly calculating from CRS meters
        scale_denom = base_scale_denom / (2 ** i)

        matrices.append({
            "id": str(i),
            "scaleDenominator": scale_denom, 
            "cellSize": resolution,
            "pointOfOrigin": [extent[0], extent[3]], # TopLeft: MinX, MaxY
            "tileWidth": tile_size,
            "tileHeight": tile_size,
            "matrixWidth": matrix_width,
            "matrixHeight": matrix_height
        })
    return matrices

def _get_crs_urn(srid: int) -> str:
    """Helper to format OGC CRS URNs using pyproj."""
    try:
        crs = CRS.from_epsg(srid)
        return f"http://www.opengis.net/def/crs/EPSG/0/{srid}"
    except Exception:
        return f"urn:ogc:def:crs:EPSG::{srid}"

# --- TMS Definitions ---

def create_web_mercator_quad() -> TileMatrixSet:
    """Creates the WebMercatorQuad TMS as a Pydantic model."""
    tms_id = "WebMercatorQuad"
    srid = 3857
    extent = [-20037508.3427892, -20037508.3427892, 20037508.3427892, 20037508.3427892]
    crs_urn = _get_crs_urn(srid)
    
    return TileMatrixSet(
        id=tms_id,
        title="Google Maps Compatible for the World",
        uri=f"http://www.opengis.net/def/tilematrixset/OGC/1.0/{tms_id}",
        crs=crs_urn,
        orderedAxes=["X", "Y"],
        wellKnownScaleSet="http://www.opengis.net/def/wkss/OGC/1.0/GoogleMapsCompatible",
        boundingBox=BoundingBox(
            lowerLeft=[extent[0], extent[1]],
            upperRight=[extent[2], extent[3]],
            crs=crs_urn
        ),
        tileMatrices=[TileMatrix(**m) for m in _generate_quad_matrices(25, extent)]
    )

def create_world_crs84_quad() -> TileMatrixSet:
    """Creates the WorldCRS84Quad TMS as a Pydantic model."""
    tms_id = "WorldCRS84Quad"
    crs_urn = "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
    extent = [-180.0, -90.0, 180.0, 90.0]
    
    return TileMatrixSet(
        id=tms_id,
        title="CRS84 for the World",
        uri=f"http://www.opengis.net/def/tilematrixset/OGC/1.0/{tms_id}",
        crs=crs_urn,
        orderedAxes=["Lon", "Lat"],
        wellKnownScaleSet="http://www.opengis.net/def/wkss/OGC/1.0/GoogleCRS84Quad",
        boundingBox=BoundingBox(
            lowerLeft=[extent[0], extent[1]],
            upperRight=[extent[2], extent[3]],
            crs=crs_urn
        ),
        tileMatrices=[TileMatrix(**m) for m in _generate_quad_matrices(
            levels=23, 
            extent=extent, 
            zero_level_matrix_width=2, 
            zero_level_matrix_height=1
        )]
    )

# Registry of validated, model-based TMS definitions
BUILTIN_TILE_MATRIX_SETS: Dict[str, TileMatrixSet] = {
    "WebMercatorQuad": create_web_mercator_quad(),
    "WorldCRS84Quad": create_world_crs84_quad(),
}
