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

import re
from typing import Dict, Any, Optional, Tuple

def parse_subset_parameter(subset_str: Optional[str]) -> Dict[str, Any]:
    """
    Parses an OGC API 'subset' parameter string into a dictionary.
    Example: "asset_code(api-ogc-123),h3_lvl10(8a3d8d2d4d2dfff)"
    """
    if not subset_str:
        return {}
    
    params = {}
    # Regex to find key(value) pairs
    pattern = re.compile(r'(\w+)\(([^)]+)\)')
    matches = pattern.findall(subset_str)
    
    for key, value in matches:
        # Basic type inference (can be expanded)
        if value.isnumeric():
            params[key] = int(value)
        elif re.match(r'^-?\d+\.\d+$', value):
            params[key] = float(value)
        else:
            params[key] = value

    return params


def parse_rfc3339_interval(value: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Parse an RFC3339 instant/interval per the OGC API ``datetime`` convention.

    Accepts:
    - an instant, e.g. ``"2024-01-01T00:00:00Z"`` -> ``(value, value)``
    - a closed interval, e.g. ``"2024-01-01/2024-12-31"`` -> ``(start, end)``
    - an open start, e.g. ``"../2024-12-31"`` -> ``(None, end)``
    - an open end, e.g. ``"2024-01-01/.."`` -> ``(start, None)``

    Returns ``(None, None)`` when ``value`` is empty. Only the ``"/"`` split and
    ``".."`` open-bound handling is performed here — the strings are returned
    as-is (no timezone/format validation or coercion to ``datetime``), so
    callers that need parsed instants or interval-vs-instant-specific
    downstream behavior (e.g. an ``EQ`` filter for an instant vs ``GTE``/``LTE``
    for an interval) build that from the returned tuple themselves.
    """
    if not value:
        return None, None
    if "/" in value:
        start_str, end_str = value.split("/", 1)
        start = None if start_str == ".." else start_str
        end = None if end_str == ".." else end_str
        return start, end
    return value, value