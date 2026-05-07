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
# 
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

from enum import Enum
from typing import Optional, Any, TYPE_CHECKING
try:
    from fastapi.responses import JSONResponse
except ImportError:
    try:
        from starlette.responses import JSONResponse
    except ImportError:
        from typing import Any as JSONResponse

from dynastore.tools.json import CustomJSONEncoder, orjson_default
from typing import Any
import orjson
import json

import logging
logger = logging.getLogger(__name__)

class AppJSONResponse(JSONResponse):  # type: ignore[reportGeneralTypeIssues]
    """
    A custom JSONResponse class that utilizes the CustomJSONEncoder to ensure
    consistent and correct JSON serialization across all endpoints.
    """
    def render(self, content: Any) -> bytes:
        """
        Renders the response content into JSON bytes using the custom encoder.
        """
        return json.dumps(
            content,
            ensure_ascii=False,
            allow_nan=False,
            indent=None,
            separators=(",", ":"),
            cls=CustomJSONEncoder,
        ).encode("utf-8")

class ORJSONResponse(JSONResponse):  # type: ignore[reportGeneralTypeIssues]
    """
    A high-performance JSON response class using orjson.
    It's configured with a custom `default` serializer to handle special
    types like Shapely geometries, combining orjson's speed with the
    application's specific data serialization needs.
    """
    media_type = "application/json"

    def render(self, content: Any) -> bytes:
        return orjson.dumps(
            content,
            default=orjson_default,
            option=orjson.OPT_NON_STR_KEYS | orjson.OPT_SERIALIZE_NUMPY,
        )


# --- RESPONSE FORMATTING ---


def _parse_srid_from_srs_name(srs_name: str) -> Optional[int]:
    """Robustly parses an SRID from OGC-compliant or simple EPSG strings."""
    if not srs_name:
        return None

    # Handle the common URN for WGS84, which doesn't end in a numeric SRID.
    if 'CRS84' in srs_name:
        return 4326

    try:
        # Handles both "EPSG:4326" and "urn:ogc:def:crs:EPSG::4326" by taking the last part.
        return int(srs_name.split(':')[-1])
    except (ValueError, IndexError):
        logger.error(f"Could not parse SRID from srs_name: {srs_name}")
        return None

