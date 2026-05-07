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

"""
Open Table Format (OTF) models.

Data models for OTF-inspired capabilities: snapshots, schema evolution,
and time-travel. These support Iceberg, Delta Lake, and Hudi patterns
at the collection level via ``CollectionsProtocol`` extension methods.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class SchemaField(BaseModel):
    """A field in a schema version."""

    name: str
    type: str  # "string", "int64", "float64", "geometry", "timestamp", etc.
    required: bool = False
    doc: Optional[str] = None


class SchemaVersion(BaseModel):
    """A versioned schema definition."""

    schema_id: str
    fields: List[SchemaField] = Field(default_factory=list)
    timestamp: datetime
    parent_schema_id: Optional[str] = None


class SchemaEvolution(BaseModel):
    """Declarative schema change request. Does not rewrite data."""

    add_columns: List[SchemaField] = Field(default_factory=list)
    rename_columns: Dict[str, str] = Field(
        default_factory=dict, description="old_name -> new_name"
    )
    drop_columns: List[str] = Field(default_factory=list)
    type_promotions: Dict[str, str] = Field(
        default_factory=dict,
        description="field -> new_type (safe widenings: int32->int64, float32->float64)",
    )


class SnapshotInfo(BaseModel):
    """Immutable snapshot of a collection's state at a point in time."""

    snapshot_id: str
    parent_snapshot_id: Optional[str] = None
    timestamp: datetime
    label: Optional[str] = None
    operation: str = "append"  # append, overwrite, delete, compact
    summary: Dict[str, Any] = Field(default_factory=dict)
