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

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class InterpolationEnum(str, Enum):
    LINEAR = "Linear"
    STEP = "Step"
    QUADRATIC = "Quadratic"
    CUBIC = "Cubic"


# ---------------------------------------------------------------------------
# TemporalGeometrySequence
# ---------------------------------------------------------------------------

class TemporalGeometryCreate(BaseModel):
    """Client-supplied payload to create a temporal geometry sequence."""
    datetimes: List[datetime] = Field(
        ..., min_length=1, description="Ordered list of instants (ISO 8601)."
    )
    coordinates: List[List[float]] = Field(
        ..., min_length=1,
        description="Coordinate array matching datetimes length. Each entry is [lon, lat] or [lon, lat, elev].",
    )
    crs: str = Field(
        default="http://www.opengis.net/def/crs/OGC/1.3/CRS84",
        description="Coordinate reference system URI.",
    )
    trs: str = Field(
        default="http://www.opengis.net/def/uom/ISO-8601/0/Gregorian",
        description="Temporal reference system URI.",
    )
    interpolation: InterpolationEnum = Field(
        default=InterpolationEnum.LINEAR,
        description="Interpolation method between positions.",
    )
    properties: Optional[Dict[str, Any]] = Field(
        default=None, description="Temporal scalar properties (e.g., speed, heading)."
    )


class TemporalGeometry(TemporalGeometryCreate):
    """Full temporal geometry sequence record from the database."""
    id: uuid.UUID
    mf_id: uuid.UUID
    catalog_id: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = ConfigDict(from_attributes=True)


# ---------------------------------------------------------------------------
# MovingFeature
# ---------------------------------------------------------------------------

class MovingFeatureCreate(BaseModel):
    """Client-supplied payload to create a moving feature."""
    feature_type: str = Field(
        default="Feature",
        description="MF-JSON feature type identifier.",
    )
    properties: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Static (non-temporal) properties of the moving feature.",
    )


class MovingFeature(MovingFeatureCreate):
    """Full moving feature record from the database."""
    id: uuid.UUID
    catalog_id: str
    collection_id: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = ConfigDict(from_attributes=True)


