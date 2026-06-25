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

"""EDR cube query handler: parse bbox and delegate to area extraction."""

from __future__ import annotations

from typing import Tuple

from dynastore.tools.geospatial import parse_bbox_string, BboxDimensionality


def parse_cube_bbox(bbox_str: str) -> Tuple[float, float, float, float]:
    """Parse EDR cube bbox param → (min_lon, min_lat, max_lon, max_lat).

    Accepts comma-separated values; optional 3D/4D extra values are ignored.
    """
    result = parse_bbox_string(
        bbox_str,
        dimensionality=BboxDimensionality.ALLOW_EXTRA_DIMS,
        allow_none=False,
        validate_geometry=True,
    )
    return result
