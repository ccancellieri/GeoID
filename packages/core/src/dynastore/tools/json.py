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
import json
import orjson
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel

def _wkb_loads(data: bytes):
    """Lazy import of shapely.wkb.loads to avoid hard dependency."""
    from shapely import wkb
    return wkb.loads(data)


def orjson_default(obj: Any) -> Any:
    """
    Default serializer for orjson, handling types it doesn't natively support.
    This function is used as a fallback for orjson.dumps().

    - Handles Shapely geometries (from WKB bytes or `__geo_interface__`).
    - Falls back to a string representation for any other unhandled types.
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, Decimal):
        # Preserve numeric type in JSON output (not string).
        # Use int when the value has no fractional part for cleaner output.
        if obj == obj.to_integral_value():
            return int(obj)
        return float(obj)
    if isinstance(obj, BaseModel):
        return obj.model_dump(exclude_none=True)
    if hasattr(obj, "__geo_interface__"):
        return obj.__geo_interface__
    if isinstance(obj, bytes):
        try:
            return _wkb_loads(obj).wkt
        except Exception:
            try:
                return obj.decode("utf-8")
            except UnicodeDecodeError:
                return obj.hex()

    # For any other type that orjson can't handle, it will raise a TypeError.
    # We catch this and fall back to a simple string representation.
    raise TypeError


# =============================================================================
# == GENERIC ENCODING (Python objects -> JSON string)
# =============================================================================


class CustomJSONEncoder(json.JSONEncoder):
    """
    A production-ready, extensible, and generic JSON encoder for FastAPI.

    This encoder handles common problematic types:
    - datetime and date objects (converts to ISO 8601 format).
    - UUID objects (converts to string), Decimal objects (converts to int/float).
    - Geospatial objects (like Shapely) with a `__geo_interface__` are
      converted to their string representation (e.g., WKT for Shapely).
    - `bytes` objects are decoded as UTF-8, falling back to hex for binary data.
    - Any other non-serializable object is converted to its string representation
      as a final fallback to prevent crashes.
    """

    def default(self, o: Any) -> Any:
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        if isinstance(o, uuid.UUID):
            return str(o)
        if isinstance(o, Decimal):
            if o == o.to_integral_value():
                return int(o)
            return float(o)
        # Pydantic models MUST be checked before __geo_interface__ because
        # geojson_pydantic models (Feature, Point, …) expose both
        # model_dump() and __geo_interface__; str(o) on those produces the
        # repr, not valid JSON.
        if isinstance(o, BaseModel):
            return o.model_dump(exclude_none=True)
        # For geospatial objects (e.g., Shapely) return the GeoJSON dict
        if hasattr(o, "__geo_interface__"):
            return o.__geo_interface__
        if isinstance(o, (set, frozenset)):
            return sorted(o)
        if isinstance(o, (bytes, bytearray, memoryview)):
            if isinstance(o, memoryview):
                o = o.tobytes()
            # First, attempt to parse the bytes as a WKB geometry, a common
            # format for binary geometry from databases like PostGIS.
            try:
                # If successful, return the WKT representation.
                return _wkb_loads(bytes(o)).wkt
            except Exception:
                # If it's not a valid WKB, fall back to the original text/hex logic.
                try:
                    return o.decode("utf-8")
                except UnicodeDecodeError:
                    # It's truly binary data, so represent as hex.
                    return o.hex()

        # Let the base class default method raise the TypeError for other
        # unserializable types, with a fallback to string representation.
        try:
            return super().default(o)
        except TypeError:
            return str(o)


# =============================================================================
# == DECODING & PARSING (String -> Python objects)
# =============================================================================
# This section contains generic utilities for converting strings into rich
# Python objects, useful for processing incoming data.


def parse_string_to_python_type(value: str) -> Any:
    """
    Intelligently parses a string value into a more specific Python type.

    Handles booleans, integers, floats, dates (YYYY-MM-DD), datetimes (ISO),
    JSON objects/arrays, and returns the original string if no conversion applies.
    """
    if not isinstance(value, str):
        return value
    stripped_value = value.strip()
    if stripped_value.lower() == "true":
        return True
    if stripped_value.lower() == "false":
        return False
    if stripped_value.isdigit():
        return int(stripped_value)
    try:
        return float(stripped_value)
    except (ValueError, TypeError):
        pass
    if stripped_value.startswith(("[", "{")):
        try:
            return json.loads(stripped_value)
        except json.JSONDecodeError:
            pass
    try:
        # Handle ISO format datetimes, including those with 'Z' for UTC
        return datetime.fromisoformat(stripped_value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        pass
    try:
        return date.fromisoformat(stripped_value)
    except (ValueError, TypeError):
        pass
    return value


class CustomJSONDecoder(json.JSONDecoder):
    """
    Custom JSON decoder that automatically attempts to parse string values
    into richer Python types using an object hook.
    """

    def __init__(self, *args, **kwargs):
        # Pass the static method as the object_hook callable.
        super().__init__(object_hook=CustomJSONDecoder._object_hook, *args, **kwargs)

    @staticmethod
    def _object_hook(obj: dict) -> dict:
        """
        Processes a dictionary from JSON, parsing string values.
        """
        for key, value in obj.items():
            if isinstance(value, str):
                obj[key] = parse_string_to_python_type(value)
        return obj


def sanitize_for_serialization(obj: Any) -> Any:
    """
    Recursively converts an object into a JSON-serializable format
    (dicts, lists, strings, numbers, booleans, None) using the custom encoder logic.
    This avoids the overhead of encoding to string and decoding back if possible,
    or uses the encoder for consistency.
    """
    # Optimized approach: use json dump/load with custom encoder to ensure strict compliance
    # This guarantees that what we return is fully serializable by standard JSON parsers
    # and matches the API response serialization.
    return json.loads(json.dumps(obj, cls=CustomJSONEncoder))
