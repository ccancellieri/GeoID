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

from pydantic import BaseModel, Field
from typing import Literal, Optional, List, Tuple

class TilePreseedRequest(BaseModel):
    """
    Input payload for the Tiles Pre-seeding Process.
    """
    update_bbox: Optional[List[Tuple[float, float, float, float]]] = Field(
        None, 
        description="Optional list of bounding boxes to limit the pre-seeding process. If provided, the effective seeding area is the intersection of these boxes and the configured global bbox. Coordinates should be in WGS84 (EPSG:4326)."
    )
    catalog_id: str = Field(..., description="The catalog identifier to process.")
    collection_id: Optional[str] = Field(None, description="Optional collection identifier. If omitted, applies to all pre-seed enabled collections in the catalog.")
    tms_ids: Optional[List[str]] = Field(None, description="List of TMS IDs to process. Overrides configuration if provided.")
    formats: Optional[List[str]] = Field(None, description="List of formats to generate. Overrides configuration if provided.")
    output_format: Literal["mvt", "pmtiles"] = Field(
        "mvt",
        description="Output format: 'mvt' stores individual tiles; 'pmtiles' builds a PMTiles v3 archive.",
    )
