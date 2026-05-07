#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
"""Shared request model for the feature-exporter service."""

from typing import List, Optional

from pydantic import BaseModel, Field

from dynastore.models.shared_models import OutputFormatEnum


class ExportFeaturesRequest(BaseModel):
    """Request model for exporting features from a collection.

    Consumed by every execution tier:
      - sync REST (small payloads, immediate response)
      - async background task (medium; < ~60s)
      - Cloud Run Job (long-running; production only)
    """

    catalog: str = Field(..., description="Catalog ID")
    collection: str = Field(..., description="Collection ID")
    output_format: OutputFormatEnum = Field(..., description="Output file format")
    destination_uri: str = Field(..., description="GCS URI for output (gs://...)")
    encoding: str = Field(default="utf-8", description="Character encoding for output")
    cql_filter: Optional[str] = Field(None, description="CQL2/ECQL filter expression")
    property_names: Optional[List[str]] = Field(
        None, description="List of properties to include (None = all)"
    )
    limit: Optional[int] = Field(
        None, description="Maximum number of features to export"
    )
    offset: Optional[int] = Field(None, description="Number of features to skip")
    target_srid: Optional[int] = Field(
        None, description="Target SRID for geometry transformation"
    )
    reporting: Optional[dict] = Field(None, description="Reporter configuration")
