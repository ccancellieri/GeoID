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

"""Pydantic models for OGC API - Connected Systems resources."""

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from dynastore.models.shared_models import Link


# ---------------------------------------------------------------------------
# System
# ---------------------------------------------------------------------------

class SystemBase(BaseModel):
    system_id: str = Field(..., description="User-defined system identifier, unique within a catalog.")
    name: str = Field(..., description="Human-readable name for this system.")
    description: Optional[str] = Field(None, description="Longer description of the system.")
    type: str = Field("Sensor", description="System type, e.g. Sensor, Platform, Actuator.")
    geometry: Optional[Dict[str, Any]] = Field(
        None,
        description="GeoJSON geometry representing the system position (usually a Point).",
    )
    properties: Optional[Dict[str, Any]] = Field(None, description="Arbitrary system properties.")
    stac_collection_id: Optional[str] = Field(
        None, description="STAC collection identifier linked to this system."
    )


class SystemCreate(SystemBase):
    pass


class SystemUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = None
    geometry: Optional[Dict[str, Any]] = None
    properties: Optional[Dict[str, Any]] = None
    stac_collection_id: Optional[str] = None


class System(SystemBase):
    id: uuid.UUID
    catalog_id: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    links: List[Link] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


# ---------------------------------------------------------------------------
# Deployment
# ---------------------------------------------------------------------------

class DeploymentBase(BaseModel):
    name: str = Field(..., description="Human-readable name for this deployment.")
    description: Optional[str] = None
    time_start: Optional[datetime] = Field(
        None, description="When the deployment started."
    )
    time_end: Optional[datetime] = Field(None, description="When the deployment ended.")
    geometry: Optional[Dict[str, Any]] = Field(
        None, description="GeoJSON geometry of the deployment site."
    )
    properties: Optional[Dict[str, Any]] = None


class DeploymentCreate(DeploymentBase):
    pass


class Deployment(DeploymentBase):
    id: uuid.UUID
    catalog_id: str
    system_id: uuid.UUID
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    links: List[Link] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


# ---------------------------------------------------------------------------
# DataStream
# ---------------------------------------------------------------------------

class DataStreamBase(BaseModel):
    datastream_id: str = Field(..., description="User-defined datastream identifier, unique within a catalog.")
    name: str = Field(..., description="Human-readable name for this datastream.")
    description: Optional[str] = None
    observed_property: str = Field(
        ..., description="The phenomenon being observed, e.g. 'temperature'."
    )
    unit_of_measurement: str = Field(
        ..., description="Unit symbol, e.g. 'degC', 'mm', '%'."
    )
    properties: Optional[Dict[str, Any]] = None


class DataStreamCreate(DataStreamBase):
    system_id: uuid.UUID = Field(..., description="Internal UUID of the parent system.")


class DataStream(DataStreamBase):
    id: uuid.UUID
    catalog_id: str
    system_id: uuid.UUID
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    links: List[Link] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


# ---------------------------------------------------------------------------
# Observation
# ---------------------------------------------------------------------------

class ObservationCreate(BaseModel):
    phenomenon_time: datetime = Field(..., description="Time instant of the observation.")
    result_value: float = Field(..., description="Measured value.")
    result_quality: Optional[float] = Field(
        None, ge=0.0, le=1.0, description="Quality score in [0, 1]."
    )
    parameters: Optional[Dict[str, Any]] = Field(None, description="Optional extra parameters.")


class Observation(ObservationCreate):
    id: uuid.UUID
    catalog_id: str
    datastream_id: uuid.UUID
    result_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    links: List[Link] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)
