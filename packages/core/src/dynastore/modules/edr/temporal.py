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

"""EDR datetime parameter parsing.

Handles OGC EDR datetime instant and interval formats:
- instant:    "2024-01-01T00:00:00Z"
- interval:   "2024-01-01/2024-12-31"
- open start: "../2024-12-31"
- open end:   "2024-01-01/.."
"""

from __future__ import annotations

from typing import Optional, Tuple

from dynastore.tools.ogc_common import parse_rfc3339_interval


def parse_datetime_param(value: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Parse EDR datetime param → (start, end).

    Returns (None, None) when value is empty.
    For instants returns (value, value).
    """
    return parse_rfc3339_interval(value)
