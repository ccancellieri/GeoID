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
#    Company: FAO, Vile delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""EDR vertical level (z) parameter parsing.

Handles OGC EDR z parameter formats per OGC API EDR 19-086r6 §8.2.7:
- single level:     "100"
- level range:      "100/200"  (slash interval notation)
- named level:      "surface" (requires collection metadata)
"""

from __future__ import annotations

from typing import List, Optional, Tuple


def parse_z_param(value: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    """Parse EDR z param → (z_low, z_high).

    Returns (None, None) when value is empty.
    For single level returns (value, value).
    For ranges returns (low, high).

    Raises ValueError if the string cannot be parsed as a numeric value.
    """
    if not value:
        return None, None
    if "/" in value:
        parts = value.split("/", 1)
        try:
            low = float(parts[0]) if parts[0] else None
            high = float(parts[1]) if parts[1] else None
        except ValueError as exc:
            raise ValueError(f"Invalid z value: {value!r}") from exc
        return low, high
    try:
        v = float(value)
    except ValueError as exc:
        raise ValueError(f"Invalid z value: {value!r}") from exc
    return v, v


def select_bands_by_z(
    bands: List[dict],
    z_low: Optional[float],
    z_high: Optional[float],
) -> List[int]:
    """Select band indices (1-based) matching the vertical level range.

    Bands are expected to have raster:bands metadata with 'vertical:value'
    indicating the vertical level of that band. If no vertical metadata exists,
    falls back to treating bands as sequential levels (band 1 = level 1, etc.).

    Args:
        bands: List of band metadata dicts (from raster:bands)
        z_low: Lower bound of vertical level (inclusive)
        z_high: Upper bound of vertical level (inclusive)

    Returns:
        List of 1-based band indices that match the level range.
        If z_low and z_high are both None, returns all bands.
    """
    if z_low is None and z_high is None:
        return list(range(1, len(bands) + 1)) if bands else [1]

    if not bands:
        if z_low is not None and z_high is not None and z_low == z_high:
            idx = int(z_low)
            return [idx]
        return [1]

    matched: List[int] = []
    for i, band in enumerate(bands, start=1):
        v = band.get("vertical", {})
        if isinstance(v, dict):
            level = v.get("value")
        else:
            level = None

        if level is None:
            continue

        try:
            level_val = float(level)
        except (TypeError, ValueError):
            continue

        if z_low is not None and level_val < z_low:
            continue
        if z_high is not None and level_val > z_high:
            continue
        matched.append(i)

    if not matched and z_low is not None and z_high is not None and z_low == z_high:
        idx = int(z_low)
        if 1 <= idx <= len(bands):
            return [idx]

    return matched if matched else list(range(1, len(bands) + 1))
