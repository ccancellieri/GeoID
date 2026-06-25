#    Copyright 2026 FAO
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

"""Streaming CoverageJSON writer — emits bytes in chunks.

CoverageJSON spec: https://covjson.org/spec/ (OGC 19-087 §7.5 /req/coveragejson).
The NdArray ``shape`` and axis ``num`` must reflect the actual pixel count of
the data, not hardcoded sentinels.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, Iterator, List


def write_coveragejson(
    domainset: Dict[str, Any],
    rangetype: Dict[str, Any],
    values_iter: Iterable[List[List[float]]],
) -> Iterator[bytes]:
    """Emit a CoverageJSON Coverage document as a single UTF-8 JSON chunk.

    ``values_iter`` yields one 2-D list-of-lists (rows x cols) per field in
    ``rangetype["field"]``.  When the iterator is empty, ranges are emitted
    with empty ``values`` and ``shape`` ``[0, 0]`` rather than incorrect
    ``[2, 2]`` sentinels.

    The ``num`` values on the domain axes are derived from the actual data
    dimensions so that ``shape`` and axis grid sizes remain consistent.
    """
    axes = {a["axisLabel"]: a for a in (domainset.get("generalGrid") or {}).get("axis", [])}
    lon = axes.get("Lon") or {"lowerBound": 0, "upperBound": 0}
    lat = axes.get("Lat") or {"lowerBound": 0, "upperBound": 0}

    flat_per_field: Dict[str, List[float]] = {f["name"]: [] for f in rangetype.get("field", [])}
    field_names = list(flat_per_field.keys())

    # Track actual pixel dimensions from the first band consumed.
    actual_rows: int = 0
    actual_cols: int = 0

    for band_idx, band_2d in enumerate(values_iter):
        if band_idx >= len(field_names):
            break
        name = field_names[band_idx]
        flat_values: List[float] = []
        rows = 0
        cols = 0
        for row in band_2d:
            flat_values.extend(row)
            cols = len(row)
            rows += 1
        flat_per_field[name] = flat_values
        if band_idx == 0:
            actual_rows = rows
            actual_cols = cols

    srs_name = (domainset.get("generalGrid") or {}).get("srsName", "OGC:CRS84")

    doc = {
        "type": "Coverage",
        "domain": {
            "type": "Domain",
            "domainType": "Grid",
            "axes": {
                "x": {
                    "start": lon["lowerBound"],
                    "stop": lon["upperBound"],
                    "num": actual_cols,
                },
                "y": {
                    "start": lat["lowerBound"],
                    "stop": lat["upperBound"],
                    "num": actual_rows,
                },
            },
            "referencing": [{
                "coordinates": ["x", "y"],
                "system": {"type": "GeographicCRS", "id": srs_name},
            }],
        },
        "parameters": {
            f["name"]: {
                "type": "Parameter",
                "observedProperty": {"label": {"en": f["name"]}},
            }
            for f in rangetype.get("field", [])
        },
        "ranges": {
            name: {
                "type": "NdArray",
                "dataType": "float",
                "axisNames": ["y", "x"],
                "shape": [actual_rows, actual_cols],
                "values": values,
            }
            for name, values in flat_per_field.items()
        },
    }
    yield json.dumps(doc).encode("utf-8")
