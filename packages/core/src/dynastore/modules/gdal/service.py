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

import logging
from enum import Enum
from typing import Optional, Tuple, Set

try:
    from osgeo import gdal, ogr
    gdal.UseExceptions()
    ogr.UseExceptions()
    GDAL_AVAILABLE = True
except ImportError:
    GDAL_AVAILABLE = False

logger = logging.getLogger(__name__)

class FileType(str, Enum):
    RASTER = "raster"
    VECTOR = "vector"
    UNKNOWN = "unknown"

# Statically defined MIME types for common raster formats as fallbacks
STATIC_RASTER_MIME_TYPES = {
    "image/geotiff",
    "application/x-pmtiles",
}

# Statically defined MIME types for common vector formats as fallbacks
STATIC_VECTOR_MIME_TYPES = {
    "application/geo+json",
    "application/json",
    "application/vnd.google-earth.kml+xml",
    "application/gml+xml",
    "application/geopackage+sqlite3",
    "application/x-geopackage",
    "application/zip",
    "application/vnd.apache.parquet",
}

# Note: GdalFormatManager logic has been moved to GdalModule in gdal_module.py

def get_raster_info(file_path: str) -> dict:
    """Synchronous function to get raster file info using gdal.Info."""
    if not GDAL_AVAILABLE:
        raise RuntimeError("GDAL is not available.")
    
    try:
        info_options = gdal.InfoOptions(format='json')
        ds = gdal.Open(file_path, gdal.GA_ReadOnly)
        if not ds:
             raise RuntimeError(f"Could not open '{file_path}' as a raster dataset.")
        
        info = gdal.Info(ds, options=info_options)
        del ds
        if not info:
             raise RuntimeError(f"Failed to get raster info for '{file_path}'.")
        return info
    except Exception as e:
        logger.error(f"GDAL error processing raster '{file_path}': {e}")
        raise

def get_vector_info(file_path: str) -> dict:
    """Synchronous function to get vector file info using ogr.Open."""
    if not GDAL_AVAILABLE:
        raise RuntimeError("GDAL is not available.")
    
    try:
        ds = ogr.Open(file_path, 0)
        if not ds:
            raise RuntimeError(f"Could not open '{file_path}' as a vector dataset.")

        result = {
            "driverShortName": ds.GetDriver().GetName(),
            "layers": []
        }
        for i in range(ds.GetLayerCount()):
            layer = ds.GetLayerByIndex(i)
            srs = layer.GetSpatialRef()
            layer_info = {
                "name": layer.GetName(),
                "geometryType": ogr.GeometryTypeToName(layer.GetGeomType()),
                "featureCount": layer.GetFeatureCount(),
                "extent": layer.GetExtent(),
                "coordinateSystem": {
                    "wkt": srs.ExportToWkt() if srs else "Unknown"
                },
                "fields": []
            }
            layer_def = layer.GetLayerDefn()
            for j in range(layer_def.GetFieldCount()):
                field_def = layer_def.GetFieldDefn(j)
                layer_info["fields"].append({
                    "name": field_def.GetName(),
                    "type": field_def.GetFieldTypeName(field_def.GetType())
                })
            result["layers"].append(layer_info)
        del ds
        return result
    except Exception as e:
        logger.error(f"OGR error processing vector '{file_path}': {e}")
        raise


def _href_to_vsi_path(href: str) -> str:
    """Translate a canonical URI to a GDAL VSI path."""
    if href.startswith("gs://"):
        return "/vsigs/" + href[len("gs://"):]
    if href.startswith("s3://"):
        return "/vsis3/" + href[len("s3://"):]
    if href.startswith("http://") or href.startswith("https://"):
        return "/vsicurl/" + href
    return href


def open_raster_vsi(href: str):
    """Open a raster at an arbitrary URI through GDAL's VSI layer.

    Lazy-imports rasterio so callers that never open a raster can run
    without the heavy GDAL/rasterio stack installed.
    """
    import rasterio

    vsi = _href_to_vsi_path(href)
    env = rasterio.Env(
        GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR",
        VSI_CACHE="TRUE",
        CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".tif,.tiff,.nc",
    )
    env.__enter__()
    try:
        return rasterio.open(vsi, "r")
    except Exception:
        env.__exit__(None, None, None)
        raise
