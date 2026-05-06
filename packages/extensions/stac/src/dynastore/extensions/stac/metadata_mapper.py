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
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pystac
from pystac.extensions.eo import Band as EOBand, EOExtension
from pystac.extensions.raster import DataType, RasterBand, RasterExtension

logger = logging.getLogger(__name__)

PROJECTION_EXT = "https://stac-extensions.github.io/projection/v1.1.0/schema.json"
RASTER_EXT = "https://stac-extensions.github.io/raster/v1.1.0/schema.json"
EO_EXT = "https://stac-extensions.github.io/eo/v1.1.0/schema.json"
TABLE_EXT = "https://stac-extensions.github.io/table/v1.2.0/schema.json"

GDAL_TO_STAC_DTYPE: dict[str, DataType] = {
    "Byte": DataType.UINT8,
    "UInt16": DataType.UINT16,
    "Int16": DataType.INT16,
    "UInt32": DataType.UINT32,
    "Int32": DataType.INT32,
    "Float32": DataType.FLOAT32,
    "Float64": DataType.FLOAT64,
    "CInt16": DataType.CINT16,
    "CInt32": DataType.CINT32,
    "CFloat32": DataType.CFLOAT32,
    "CFloat64": DataType.CFLOAT64,
}

OGR_TO_STAC_TABLE_TYPE = {
    "String": "string",
    "Integer": "integer",
    "Integer64": "integer",
    "Real": "number",
    "Date": "date",
    "Time": "time",
    "DateTime": "datetime",
    "Binary": "binary",
    "StringList": "array",
    "IntegerList": "array",
    "RealList": "array",
}


@dataclass
class StacFields:
    """STAC-shaped fields derived from gdalinfo/ogrinfo JSON output.

    Pure data carrier — no pystac side effects. Apply to a target via
    ``apply_to_collection`` / ``apply_to_item`` / ``apply_to_asset``.
    """

    extensions: List[str] = field(default_factory=list)
    extra_fields: Dict[str, Any] = field(default_factory=dict)
    bbox: Optional[List[float]] = None
    raster_bands: List[RasterBand] = field(default_factory=list)
    eo_bands: List[EOBand] = field(default_factory=list)
    table_columns: List[Dict[str, Any]] = field(default_factory=list)


def gdal_to_stac_fields(gdal_info: Dict[str, Any]) -> StacFields:
    """Pure transform: gdalinfo/ogrinfo dict → StacFields.

    Detects raster (``size`` / ``bands``) vs vector (``layers``) and dispatches.
    Returns an empty ``StacFields`` for unrecognized payloads.
    """
    if not gdal_info:
        return StacFields()
    if "layers" in gdal_info:
        return _vector_to_fields(gdal_info)
    if "size" in gdal_info or "bands" in gdal_info:
        return _raster_to_fields(gdal_info)
    return StacFields()


def _vector_to_fields(gdal_info: Dict[str, Any]) -> StacFields:
    out = StacFields()
    layers = gdal_info.get("layers") or []
    if not layers:
        return out

    layer = layers[0]

    out.extensions.append(PROJECTION_EXT)
    if "coordinateSystem" in layer and "wkt" in layer["coordinateSystem"]:
        out.extra_fields["proj:wkt2"] = layer["coordinateSystem"]["wkt"]

    if (gtype := layer.get("geometryType")) is not None:
        out.extra_fields["vector:geometry_type"] = gtype
    if (count := layer.get("featureCount")) is not None:
        out.extra_fields["vector:count"] = count

    if "fields" in layer:
        out.extensions.append(TABLE_EXT)
        out.table_columns = [
            {
                "name": f["name"],
                "type": OGR_TO_STAC_TABLE_TYPE.get(f.get("type", ""), "string"),
            }
            for f in layer["fields"]
        ]

    if "extent" in layer:
        out.bbox = list(layer["extent"])
    return out


def _raster_to_fields(gdal_info: Dict[str, Any]) -> StacFields:
    out = StacFields()

    out.extensions.append(PROJECTION_EXT)
    if "coordinateSystem" in gdal_info and "wkt" in gdal_info["coordinateSystem"]:
        out.extra_fields["proj:wkt2"] = gdal_info["coordinateSystem"]["wkt"]

    size = gdal_info.get("size")
    if isinstance(size, list) and len(size) == 2:
        out.extra_fields["proj:shape"] = [size[1], size[0]]

    if "geoTransform" in gdal_info:
        out.extra_fields["proj:transform"] = gdal_info["geoTransform"]

    bands = gdal_info.get("bands") or []
    if bands:
        out.extensions.append(RASTER_EXT)
        out.extensions.append(EO_EXT)
        for b in bands:
            stac_dt = GDAL_TO_STAC_DTYPE.get(b.get("type", ""))
            out.raster_bands.append(
                RasterBand.create(nodata=b.get("noDataValue"), data_type=stac_dt)
            )
            out.eo_bands.append(
                EOBand.create(
                    name=f"band_{b.get('band')}",
                    description=b.get("description"),
                )
            )

    bbox = _bbox_from_raster(gdal_info)
    if bbox is not None:
        out.bbox = bbox
    return out


def _bbox_from_raster(gdal_info: Dict[str, Any]) -> Optional[List[float]]:
    wgs84 = gdal_info.get("wgs84Extent") or {}
    coords = wgs84.get("coordinates") if wgs84 else None
    if coords:
        ring = coords[0]
        lons = [c[0] for c in ring]
        lats = [c[1] for c in ring]
        return [min(lons), min(lats), max(lons), max(lats)]

    corners = gdal_info.get("cornerCoordinates") or {}
    ul = corners.get("upperLeft")
    lr = corners.get("lowerRight")
    if ul and lr:
        return [
            min(ul[0], lr[0]),
            min(ul[1], lr[1]),
            max(ul[0], lr[0]),
            max(ul[1], lr[1]),
        ]
    return None


def _add_extension(target: Any, ext_url: str) -> None:
    if ext_url not in target.stac_extensions:
        target.stac_extensions.append(ext_url)


def apply_to_collection(
    collection: pystac.Collection, fields: StacFields
) -> pystac.Collection:
    """Apply StacFields to a Collection (uses ``summaries`` for bands)."""
    for ext in fields.extensions:
        _add_extension(collection, ext)

    for k, v in fields.extra_fields.items():
        collection.extra_fields[k] = v

    if fields.table_columns:
        collection.extra_fields["table:columns"] = fields.table_columns

    if fields.raster_bands:
        RasterExtension.summaries(collection, add_if_missing=True).bands = (
            fields.raster_bands
        )
    if fields.eo_bands:
        EOExtension.summaries(collection, add_if_missing=True).bands = fields.eo_bands

    if fields.bbox is not None:
        collection.extent.spatial = pystac.SpatialExtent([fields.bbox])
    return collection


def apply_to_item(item: pystac.Item, fields: StacFields) -> pystac.Item:
    """Apply StacFields to an Item — bbox + projection properties.

    Bands are *asset-level* in STAC; use ``apply_to_asset`` for raster/eo bands.
    """
    for ext in fields.extensions:
        if ext in (RASTER_EXT, EO_EXT):
            continue
        _add_extension(item, ext)

    for k, v in fields.extra_fields.items():
        item.properties[k] = v

    if fields.bbox is not None:
        item.bbox = fields.bbox
    return item


def apply_to_asset(asset: pystac.Asset, fields: StacFields) -> pystac.Asset:
    """Apply StacFields to an Asset — raster/eo band schemas live here."""
    if fields.raster_bands:
        RasterExtension.ext(asset, add_if_missing=True).bands = fields.raster_bands
    if fields.eo_bands:
        EOExtension.ext(asset, add_if_missing=True).bands = fields.eo_bands
    return asset


def enrich_collection_from_metadata(
    collection: pystac.Collection, metadata: Dict[str, Any]
) -> None:
    """Backwards-compatible wrapper used by ``stac_virtual.py``.

    Applies non-``gdalinfo`` / non-``stac*`` keys as ``asset:<key>`` extra fields,
    then runs the gdal/ogr transformer over ``metadata['gdalinfo']``.
    """
    try:
        if not metadata:
            return

        for key, value in metadata.items():
            if key == "gdalinfo" or key.startswith("stac"):
                continue
            collection.extra_fields[f"asset:{key}"] = value

        gdal_info = metadata.get("gdalinfo")
        if not gdal_info:
            return

        fields = gdal_to_stac_fields(gdal_info)
        apply_to_collection(collection, fields)

    except Exception as e:
        logger.exception(f"Error enriching STAC Collection from metadata: {e}")
        raise
