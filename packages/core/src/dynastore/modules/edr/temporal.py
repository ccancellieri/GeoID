"""EDR datetime parameter parsing.

Handles OGC EDR datetime instant and interval formats:
- instant:    "2024-01-01T00:00:00Z"
- interval:   "2024-01-01/2024-12-31"
- open start: "../2024-12-31"
- open end:   "2024-01-01/.."
"""

from __future__ import annotations

from typing import Optional, Tuple


def parse_datetime_param(value: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Parse EDR datetime param → (start, end).

    Returns (None, None) when value is empty.
    For instants returns (value, value).
    """
    if not value:
        return None, None
    if "/" in value:
        parts = value.split("/", 1)
        start = parts[0] if parts[0] != ".." else None
        end = parts[1] if parts[1] != ".." else None
        return start, end
    return value, value
