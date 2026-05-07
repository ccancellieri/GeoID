"""Build EDR parameter metadata from STAC item raster:bands."""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def _select_bands(item: Dict[str, Any]) -> List[Dict[str, Any]]:
    assets = item.get("assets") or {}
    for key in ("data", "coverage"):
        if key in assets and assets[key].get("raster:bands"):
            return assets[key]["raster:bands"]
    for a in assets.values():
        if a.get("raster:bands"):
            return a["raster:bands"]
    return []


def build_parameters(item: Dict[str, Any]) -> Dict[str, Any]:
    """Build EDR parameters dict from STAC item raster:bands."""
    bands = _select_bands(item)
    params: Dict[str, Any] = {}
    for band in bands:
        name = band.get("name", "band")
        unit_label = band.get("unit", "1")
        description = band.get("description", name)
        params[name] = {
            "type": "Parameter",
            "description": {"en": description},
            "unit": {"label": {"en": unit_label}},
            "observedProperty": {
                "id": band.get("name", ""),
                "label": {"en": description},
            },
        }
    return params


def filter_parameters(
    bands: List[Dict[str, Any]],
    parameter_names: Optional[List[str]],
) -> List[Dict[str, Any]]:
    """Filter bands list to only those matching requested parameter names."""
    if not parameter_names:
        return bands
    return [b for b in bands if b.get("name") in parameter_names]
