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

"""Attribute → RGB colour ramp for 3D tile vertex colouring.

A *ramp* is an ordered list of ``(stop_value, (r, g, b))`` breakpoints. Given a
scalar attribute (e.g. building height in metres) the ramp returns an RGB
triple by piecewise-linear interpolation between the two surrounding stops, and
clamps to the end colours outside the stop range.

The stop colours are authored as hex strings (``"#2c7bb6"``) — the same
vocabulary Mapbox GL ``interpolate``/``fill-extrusion-color`` and SLD colour
maps use — so the same ramp can later be sourced from an OGC API - Styles
resource without changing the interpolation maths here.

Pure module: no I/O, no glTF, no shapely.
"""

from __future__ import annotations

from typing import List, Sequence, Tuple

RGB = Tuple[int, int, int]
Stop = Tuple[float, RGB]

# Default sequential ramp keyed on height in metres (ColorBrewer RdYlBu,
# reversed): low buildings cool blue, mid pale, tall buildings warm red. Gives
# an immediately readable height legend without any per-collection config.
DEFAULT_HEIGHT_RAMP_HEX: List[Tuple[float, str]] = [
    (0.0, "#2c7bb6"),
    (10.0, "#abd9e9"),
    (20.0, "#ffffbf"),
    (40.0, "#fdae61"),
    (80.0, "#d7191c"),
]


def parse_hex(color: str) -> RGB:
    """Parse ``"#rrggbb"`` (or ``"rrggbb"``) into an ``(r, g, b)`` 0-255 triple."""
    s = color.lstrip("#").strip()
    if len(s) != 6:
        raise ValueError(f"Expected a 6-digit hex colour, got {color!r}")
    return (int(s[0:2], 16), int(s[2:4], 16), int(s[4:6], 16))


def parse_ramp(stops_hex: Sequence[Tuple[float, str]]) -> List[Stop]:
    """Convert ``[(stop, "#rrggbb"), ...]`` config into sorted ``(stop, rgb)``.

    Stops are sorted by value so callers may author them in any order. An empty
    input yields an empty ramp (callers treat that as "no vertex colours").
    """
    parsed: List[Stop] = [(float(v), parse_hex(c)) for v, c in stops_hex]
    parsed.sort(key=lambda s: s[0])
    return parsed


def interpolate_ramp(value: float, stops: Sequence[Stop]) -> RGB:
    """Piecewise-linear RGB lookup for *value* against sorted *stops*.

    Clamps to the first/last colour outside the stop range. Raises ``ValueError``
    if *stops* is empty — callers must guard for the no-ramp case before calling.
    """
    if not stops:
        raise ValueError("interpolate_ramp requires at least one stop")
    if value <= stops[0][0]:
        return stops[0][1]
    if value >= stops[-1][0]:
        return stops[-1][1]
    for (v0, c0), (v1, c1) in zip(stops, stops[1:]):
        if v0 <= value <= v1:
            span = v1 - v0
            t = 0.0 if span == 0 else (value - v0) / span
            return (
                round(c0[0] + (c1[0] - c0[0]) * t),
                round(c0[1] + (c1[1] - c0[1]) * t),
                round(c0[2] + (c1[2] - c0[2]) * t),
            )
    # Unreachable given the clamps above, but keep the type checker happy.
    return stops[-1][1]
