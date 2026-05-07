#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
"""Lightweight input model for the DWH join export task."""

from typing import Optional

from pydantic import Field

from dynastore.extensions.dwh.models import DWHJoinRequest


class DwhJoinExportRequest(DWHJoinRequest):
    destination_uri: str = Field(..., description="GCS URI for output (gs://...)")
    reporting: Optional[dict] = Field(None, description="Reporter configuration")
