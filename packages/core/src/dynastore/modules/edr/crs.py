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

"""EDR CRS (coordinate reference system) handling.

Supports CRS transformation for EDR output using pyproj/rasterio.
"""

from __future__ import annotations

from typing import Optional, Tuple

try:
    from pyproj import CRS
    PYPROJ_AVAILABLE = True
except ImportError:
    CRS = None
    PYPROJ_AVAILABLE = False


DEFAULT_OUTPUT_CRS = "http://www.opengis.net/def/crs/OGC/1.3/CRS84"


def parse_crs_param(value: Optional[str]) -> Optional[str]:
    """Parse CRS parameter and normalize to URI format.

    Accepts:
    - EPSG:XXXX → http://www.opengis.net/def/crs/EPSG/0/XXXX
    - Full CRS URI (returned as-is)
    - OGC CRS URIs (CRS84, CRS27, etc.)

    Returns None if value is empty (use default).
    Raises ValueError if CRS is invalid.
    """
    if not value:
        return None

    normalized = value.strip()
    if normalized.upper().startswith("EPSG:"):
        epsg_code = normalized.split(":", 1)[1]
        return f"http://www.opengis.net/def/crs/EPSG/0/{epsg_code}"

    if normalized.upper() in ("CRS84", "CRS27", "CRS83"):
        return f"http://www.opengis.net/def/crs/OGC/1.3/{normalized.upper()}"

    if normalized.startswith("http://") or normalized.startswith("https://"):
        return normalized

    if ":" in normalized:
        auth, code = normalized.split(":", 1)
        return f"http://www.opengis.net/def/crs/{auth}/0/{code}"

    raise ValueError(f"Invalid CRS identifier: {value!r}")


def validate_crs(crs_uri: str) -> bool:
    """Validate that a CRS URI is supported.

    Returns True if valid, raises ValueError if invalid.
    """
    if not PYPROJ_AVAILABLE:
        return True

    if crs_uri == DEFAULT_OUTPUT_CRS or crs_uri.endswith("/CRS84"):
        return True

    if crs_uri.startswith("http://www.opengis.net/def/crs/EPSG/0/"):
        try:
            code = int(crs_uri.rsplit("/", 1)[-1])
            from pyproj import CRS as PyprojCRS
            PyprojCRS.from_epsg(code)
            return True
        except Exception as exc:
            raise ValueError(f"Invalid EPSG code in CRS URI: {crs_uri}") from exc

    return True


def transform_point(
    lon: float,
    lat: float,
    src_crs: str,
    dst_crs: str,
) -> Tuple[float, float]:
    """Transform a point from src_crs to dst_crs.

    Args:
        lon: X coordinate (longitude)
        lat: Y coordinate (latitude)
        src_crs: Source CRS (URI or EPSG:XXXX)
        dst_crs: Destination CRS (URI or EPSG:XXXX)

    Returns:
        Tuple of (transformed_lon, transformed_lat)

    Raises:
        ImportError: if pyproj is not available
        ValueError: if transformation fails
    """
    if not PYPROJ_AVAILABLE:
        raise ImportError("pyproj is required for CRS transformation")

    src = _uri_to_crs(src_crs)
    dst = _uri_to_crs(dst_crs)

    if src.equals(dst):
        return lon, lat

    try:
        from pyproj import Transformer
        transformer = Transformer.from_crs(src, dst, always_xy=True)
        x, y = transformer.transform(lon, lat)
        return x, y
    except Exception as exc:
        raise ValueError(f"CRS transformation failed: {exc}") from exc


def _uri_to_crs(crs_uri: str):
    """Convert CRS URI to pyproj CRS object."""
    from pyproj import CRS as PyprojCRS

    if crs_uri.endswith("/CRS84"):
        return PyprojCRS.from_epsg(4326)

    if crs_uri.startswith("http://www.opengis.net/def/crs/EPSG/0/"):
        code = int(crs_uri.rsplit("/", 1)[-1])
        return PyprojCRS.from_epsg(code)

    if crs_uri.startswith("EPSG:"):
        code = int(crs_uri.split(":")[1])
        return PyprojCRS.from_epsg(code)

    return PyprojCRS.from_user_input(crs_uri)
