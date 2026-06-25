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

"""Shared bounding-box parsing for the DGGS indexers."""

from typing import Optional, Tuple

from dynastore.tools.geospatial import parse_bbox_string, BboxDimensionality


def parse_bbox(bbox_str: Optional[str]) -> Optional[Tuple[float, float, float, float]]:
    """Parse a comma-separated bbox string into (xmin, ymin, xmax, ymax).

    Returns None if the string is empty or None.
    Raises ValueError on malformed input.
    """
    result = parse_bbox_string(
        bbox_str,
        dimensionality=BboxDimensionality.STRICT_2D,
        allow_none=True,
        validate_geometry=True,
    )
    return result
