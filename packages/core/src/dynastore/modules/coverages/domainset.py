"""Build an OGC API - Coverages DomainSet dict from a STAC item.

Pure transform: no I/O, no rasterio.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


def build_domainset(item: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if item is None:
        return None
    props = item.get("properties") or {}
    bbox = item.get("bbox") or [0.0, 0.0, 0.0, 0.0]
    crs = _crs_from_props(props)

    axes = [
        {
            "type": "RegularAxis",
            "axisLabel": "Lon",
            "lowerBound": bbox[0],
            "upperBound": bbox[2],
            "uomLabel": "degree",
        },
        {
            "type": "RegularAxis",
            "axisLabel": "Lat",
            "lowerBound": bbox[1],
            "upperBound": bbox[3],
            "uomLabel": "degree",
        },
    ]

    dt = props.get("datetime")
    if dt:
        axes.append({
            "type": "IrregularAxis",
            "axisLabel": "Time",
            "lowerBound": dt,
            "upperBound": dt,
        })

    return {
        "type": "DomainSet",
        "generalGrid": {
            "srsName": crs,
            "axisLabels": [a["axisLabel"] for a in axes],
            "axis": axes,
        },
    }


def _crs_from_props(props: Dict[str, Any]) -> str:
    epsg = props.get("proj:epsg")
    if isinstance(epsg, int):
        return f"http://www.opengis.net/def/crs/EPSG/0/{epsg}"
    return "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
