"""Streaming CoverageJSON writer — emits bytes in chunks."""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, Iterator, List


def write_coveragejson(
    domainset: Dict[str, Any],
    rangetype: Dict[str, Any],
    values_iter: Iterable[List[List[float]]],
) -> Iterator[bytes]:
    axes = {a["axisLabel"]: a for a in domainset["generalGrid"]["axis"]}
    lon = axes.get("Lon") or {"lowerBound": 0, "upperBound": 0}
    lat = axes.get("Lat") or {"lowerBound": 0, "upperBound": 0}

    flat_per_field: Dict[str, List[float]] = {f["name"]: [] for f in rangetype["field"]}
    field_names = list(flat_per_field.keys())

    for band_idx, band_2d in enumerate(values_iter):
        if band_idx >= len(field_names):
            break
        name = field_names[band_idx]
        flat_per_field[name] = [v for row in band_2d for v in row]

    doc = {
        "type": "Coverage",
        "domain": {
            "type": "Domain",
            "domainType": "Grid",
            "axes": {
                "x": {"start": lon["lowerBound"], "stop": lon["upperBound"], "num": 2},
                "y": {"start": lat["lowerBound"], "stop": lat["upperBound"], "num": 2},
            },
            "referencing": [{
                "coordinates": ["x", "y"],
                "system": {"type": "GeographicCRS", "id": domainset["generalGrid"]["srsName"]},
            }],
        },
        "parameters": {
            f["name"]: {"type": "Parameter", "observedProperty": {"label": {"en": f["name"]}}}
            for f in rangetype["field"]
        },
        "ranges": {
            name: {
                "type": "NdArray", "dataType": "float",
                "axisNames": ["y", "x"],
                "shape": [2, 2],
                "values": values,
            }
            for name, values in flat_per_field.items()
        },
    }
    yield json.dumps(doc).encode("utf-8")
