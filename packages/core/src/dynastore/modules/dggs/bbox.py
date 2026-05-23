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

"""Shared bounding-box parsing for the DGGS indexers."""

from typing import Optional, Tuple


def parse_bbox(bbox_str: Optional[str]) -> Optional[Tuple[float, float, float, float]]:
    """Parse a comma-separated bbox string into (xmin, ymin, xmax, ymax).

    Returns None if the string is empty or None.
    Raises ValueError on malformed input.
    """
    if not bbox_str:
        return None
    parts = [p.strip() for p in bbox_str.split(",")]
    if len(parts) != 4:
        raise ValueError(
            f"bbox must have exactly 4 comma-separated values (xmin,ymin,xmax,ymax), got {len(parts)}"
        )
    try:
        xmin, ymin, xmax, ymax = (float(p) for p in parts)
    except ValueError:
        raise ValueError(f"bbox values must be numeric, got: {bbox_str!r}")
    if xmin >= xmax or ymin >= ymax:
        raise ValueError(
            f"bbox is degenerate: xmin={xmin} >= xmax={xmax} or ymin={ymin} >= ymax={ymax}"
        )
    return xmin, ymin, xmax, ymax
