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

from pydantic import BaseModel, Field, model_validator
from typing import Literal, Optional, List, Tuple

# WebMercatorQuad (the only TMS the tiles extension currently supports) is
# defined for zoom 0-24 — a request-level zoom bound outside that range can
# never be satisfied by any TMS this process serves (#2953).
MAX_TMS_ZOOM = 24

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
    operation: Literal["seed", "renew"] = Field(
        "seed",
        description=(
            "Preseed operation: 'seed' / 'renew' render MVT/PMTiles and save the "
            "tiles (renew == re-seed today; the distinction is intent labelling). "
            "The light cache-invalidation (delete-only) path has been split into "
            "its own task type 'tiles_invalidate'."
        ),
    )
    min_zoom: Optional[int] = Field(
        None,
        ge=0,
        le=MAX_TMS_ZOOM,
        description=(
            "Minimum zoom level to pre-seed. If omitted, defaults to the "
            "collection's configured ``TilesConfig.min_zoom``. Combined with "
            "``max_zoom`` this lets a caller split a large collection's "
            "preseed into several bounded jobs (#2953) instead of always "
            "attempting the full configured zoom range in one run."
        ),
    )
    max_zoom: Optional[int] = Field(
        None,
        ge=0,
        le=MAX_TMS_ZOOM,
        description=(
            "Maximum zoom level to pre-seed. If omitted, defaults to the "
            "collection's configured ``TilesConfig.max_zoom`` (or the "
            "bounded default cap when no bbox is configured anywhere — see "
            "``PRESEED_DEFAULT_MAX_ZOOM``). Zoom levels grow the tile count "
            "combinatorially over a bbox, so an unbounded range is the "
            "primary cause of preseed jobs exceeding the Cloud Run job "
            "timeout (#2953)."
        ),
    )

    @model_validator(mode="after")
    def _validate_zoom_range(self) -> "TilePreseedRequest":
        if self.min_zoom is not None and self.max_zoom is not None and self.min_zoom > self.max_zoom:
            raise ValueError("min_zoom must be <= max_zoom")
        return self
