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

from typing import Optional, Tuple
from pydantic import BaseModel, Field


class TilesExportRequest(BaseModel):
    """Input payload for the Tiles PMTiles Export Process."""

    catalog_id: str = Field(..., description="Catalog to export tiles from.")
    collection_id: str = Field(..., description="Collection to export.")
    tms_id: str = Field(
        default="WebMercatorQuad",
        description="TileMatrixSet ID. Must be a TMS supported by the collection.",
    )
    min_zoom: int = Field(default=0, ge=0, le=30, description="Minimum zoom level to include.")
    max_zoom: int = Field(default=8, ge=0, le=30, description="Maximum zoom level to include.")
    bbox: Optional[Tuple[float, float, float, float]] = Field(
        default=None,
        description=(
            "Bounding box (min_lon, min_lat, max_lon, max_lat) in WGS84. "
            "Defaults to full world extent."
        ),
    )
