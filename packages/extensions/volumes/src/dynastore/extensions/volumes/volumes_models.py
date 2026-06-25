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

"""OGC API - 3D GeoVolumes response models (OGC 22-029).

All wire shapes for the GeoVolumes protocol live here. Models are
pydantic-v2 BaseModel with field aliases matching the 22-029 spec.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field, field_validator

from dynastore.tools.geospatial import parse_bbox_string, BboxDimensionality

# ---------------------------------------------------------------------------
# Canonical CRS for 3D WGS84 (OGC 22-029 §7.1)
# ---------------------------------------------------------------------------

_CRS84H = "http://www.opengis.net/def/crs/OGC/0/CRS84h"


# ---------------------------------------------------------------------------
# Bbox helpers (module-level — imported by tests and routes)
# ---------------------------------------------------------------------------


def _parse_bbox(raw: str) -> Tuple[float, float, Optional[float], float, float, Optional[float]]:
    """Parse a bbox query-string value into a 6-tuple (minx, miny, zmin, maxx, maxy, zmax).

    Accepts 4 numbers (2D) or 6 numbers (3D). All other arities raise ValueError.
    Non-numeric values also raise ValueError.
    """
    result = parse_bbox_string(
        raw,
        dimensionality=BboxDimensionality.OPTIONAL_3D,
        allow_none=False,
        validate_geometry=True,
    )
    return result


def _bbox_intersects(
    container_bbox: List[float],
    filter_bbox: Tuple[float, float, Optional[float], float, float, Optional[float]],
) -> bool:
    """Return True if the container's 3D bbox intersects the filter bbox.

    ``container_bbox`` is a 6-element list [minx, miny, zmin, maxx, maxy, zmax].
    ``filter_bbox`` is the parsed tuple from ``_parse_bbox`` (zmin/zmax may be None for 2D).

    Two intervals [a, b] and [c, d] intersect iff a <= d and c <= b.
    A None z component means the z dimension is unconstrained on that side.
    """
    if len(container_bbox) < 6:
        return False

    c_minx, c_miny, c_zmin, c_maxx, c_maxy, c_zmax = container_bbox
    f_minx, f_miny, f_zmin, f_maxx, f_maxy, f_zmax = filter_bbox

    # 2D intersection check
    if not (c_minx <= f_maxx and f_minx <= c_maxx):
        return False
    if not (c_miny <= f_maxy and f_miny <= c_maxy):
        return False

    # 3D z-range check (only when filter carries z values)
    if f_zmin is not None and f_zmax is not None:
        if not (c_zmin <= f_zmax and f_zmin <= c_zmax):
            return False

    return True


# ---------------------------------------------------------------------------
# Wire models
# ---------------------------------------------------------------------------


class ContentExtent(BaseModel):
    """3D bounding box extent for a 3D container (OGC 22-029 §7.2).

    ``bbox`` must have exactly 6 floats: [minLon, minLat, minH, maxLon, maxLat, maxH].
    The ``crs`` defaults to CRS84h (WGS84 + ellipsoidal height).
    """

    bbox: List[float] = Field(..., description="6-element 3D bbox [minX,minY,minZ,maxX,maxY,maxZ]")
    crs: str = Field(default=_CRS84H, description="CRS URI for the extent")

    @field_validator("bbox")
    @classmethod
    def _validate_bbox_arity(cls, v: List[float]) -> List[float]:
        if len(v) != 6:
            raise ValueError(
                "bbox must have exactly 6 elements "
                f"[minX, minY, minZ, maxX, maxY, maxZ], got {len(v)}."
            )
        return v


class ContentLink(BaseModel):
    """Link to a 3D content resource (e.g. a 3D Tiles tileset or CityJSONSeq stream)."""

    rel: str
    href: str
    type: Optional[str] = None
    title: Optional[str] = None


class ChildRef(BaseModel):
    """Reference to a child 3D container."""

    id: str
    title: Optional[str] = None
    collectionType: str = "3dcontainer"


class ThreeDContainer(BaseModel):
    """OGC API 3D GeoVolumes collection resource (22-029 §7.3).

    Represents a single 3D container with its spatial extent, content links,
    and optional child references.
    """

    id: str
    title: Optional[str] = None
    description: Optional[str] = None
    collectionType: str = "3dcontainer"
    contentExtent: ContentExtent
    content: Optional[List[ContentLink]] = None
    links: Optional[List[Dict[str, Any]]] = None
    children: Optional[List[ChildRef]] = None
    attribution: Optional[str] = None


class ThreeDContainerList(BaseModel):
    """Response envelope for the GeoVolumes collections listing."""

    collections: List[ThreeDContainer]
    links: List[Dict[str, Any]] = Field(default_factory=list)
