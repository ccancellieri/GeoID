"""Build an OGC API - Coverages RangeType dict from a STAC item."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

_DTYPE_URIS = {
    "uint8":  "http://www.opengis.net/def/dataType/OGC/0/unsignedByte",
    "uint16": "http://www.opengis.net/def/dataType/OGC/0/unsignedShort",
    "uint32": "http://www.opengis.net/def/dataType/OGC/0/unsignedInt",
    "int16":  "http://www.opengis.net/def/dataType/OGC/0/signedShort",
    "int32":  "http://www.opengis.net/def/dataType/OGC/0/signedInt",
    "float32": "http://www.opengis.net/def/dataType/OGC/0/float32",
    "float64": "http://www.opengis.net/def/dataType/OGC/0/float64",
}


def build_rangetype(item: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if item is None:
        return None
    bands = _select_bands(item)
    fields: List[Dict[str, Any]] = []
    for b in bands:
        fields.append(_band_to_field(b))
    return {"type": "DataRecord", "field": fields}


def _select_bands(item: Dict[str, Any]) -> List[Dict[str, Any]]:
    assets = item.get("assets") or {}
    for key in ("data", "coverage"):
        if key in assets and (assets[key].get("raster:bands")):
            return assets[key]["raster:bands"]
    for a in assets.values():
        if a.get("raster:bands"):
            return a["raster:bands"]
    return []


def _band_to_field(b: Dict[str, Any]) -> Dict[str, Any]:
    dtype = b.get("data_type", "float32")
    nodata = b.get("nodata")
    entry: Dict[str, Any] = {
        "name": b.get("name", "band"),
        "definition": _DTYPE_URIS.get(dtype, f"dtype:{dtype}"),
    }
    if b.get("unit"):
        entry["uom"] = {"code": b["unit"]}
    if nodata is not None:
        entry["nilValues"] = [{"value": str(nodata), "reason": "nodata"}]
    return entry
