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

"""EDR CoverageJSON output writers for point and area queries."""

from __future__ import annotations

import json
from typing import Any, Dict, Iterator, List, Optional, Tuple


def write_position_coveragejson(
    lon: float,
    lat: float,
    datetime_val: Optional[str],
    parameters: Dict[str, Any],
    values: Dict[int, Optional[float]],
    band_names: List[str],
    output_crs: str = "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
    z_level: Optional[float] = None,
) -> Iterator[bytes]:
    """Emit a CoverageJSON Coverage for a point (position) query.

    Args:
        lon: X coordinate (transformed to output CRS)
        lat: Y coordinate (transformed to output CRS)
        datetime_val: ISO 8601 datetime string
        parameters: EDR parameter metadata
        values: Dict of band_index → value
        band_names: List of band names for the output
        output_crs: CRS URI for the output coordinates
        z_level: Optional vertical level value
    """
    axes: Dict[str, Any] = {
        "x": {"values": [lon]},
        "y": {"values": [lat]},
    }
    referencing = [
        {
            "coordinates": ["x", "y"],
            "system": {
                "type": "GeographicCRS",
                "id": output_crs,
            },
        }
    ]
    if datetime_val:
        axes["t"] = {"values": [datetime_val]}
        referencing.append(
            {
                "coordinates": ["t"],
                "system": {"type": "TemporalRS", "calendar": "Gregorian"},
            }
        )

    if z_level is not None:
        axes["z"] = {"values": [z_level]}
        referencing.append(
            {
                "coordinates": ["z"],
                "system": {"type": "VerticalRS", "orientation": "up"},
            }
        )

    ranges: Dict[str, Any] = {}
    for idx, name in enumerate(band_names, start=1):
        v = values.get(idx)
        axis_names = ["y", "x"]
        if z_level is not None:
            axis_names = ["z"] + axis_names
        ranges[name] = {
            "type": "NdArray",
            "dataType": "float",
            "axisNames": axis_names,
            "shape": [1] * len(axis_names),
            "values": [v],
        }

    doc = {
        "type": "Coverage",
        "domain": {
            "type": "Domain",
            "domainType": "Point",
            "axes": axes,
            "referencing": referencing,
        },
        "parameters": parameters,
        "ranges": ranges,
    }
    yield json.dumps(doc).encode("utf-8")


def write_area_coveragejson(
    bbox: Tuple[float, float, float, float],
    parameters: Dict[str, Any],
    band_names: List[str],
    band_arrays: Dict[int, List],
    crs: str = "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
) -> Iterator[bytes]:
    """Emit a CoverageJSON Coverage for an area or cube query."""
    min_lon, min_lat, max_lon, max_lat = bbox

    ranges: Dict[str, Any] = {}
    shape_y = 2
    shape_x = 2
    for idx, name in enumerate(band_names, start=1):
        flat = band_arrays.get(idx, [])
        if flat and isinstance(flat[0], list):
            shape_y = len(flat)
            shape_x = len(flat[0]) if flat[0] else 0
            flat = [v for row in flat for v in row]
        ranges[name] = {
            "type": "NdArray",
            "dataType": "float",
            "axisNames": ["y", "x"],
            "shape": [shape_y, shape_x],
            "values": flat,
        }

    doc = {
        "type": "Coverage",
        "domain": {
            "type": "Domain",
            "domainType": "Grid",
            "axes": {
                "x": {"start": min_lon, "stop": max_lon, "num": shape_x},
                "y": {"start": min_lat, "stop": max_lat, "num": shape_y},
            },
            "referencing": [
                {
                    "coordinates": ["x", "y"],
                    "system": {"type": "GeographicCRS", "id": crs},
                }
            ],
        },
        "parameters": parameters,
        "ranges": ranges,
    }
    yield json.dumps(doc).encode("utf-8")
