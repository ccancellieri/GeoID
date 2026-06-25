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

"""Payload model for the ``render_preseed`` durable task."""

from typing import List, Literal, Optional

from pydantic import BaseModel, Field


class RenderPreseedInputs(BaseModel):
    """Inputs serialised into the ``tasks.tasks.inputs`` JSONB column.

    Enqueued by ``modules/renders/preseed_sync.enqueue_render_preseed_task``
    on ``AFTER_ASSET_CREATION``.  The worker validates this model at drain
    time, so the field contract is stable between enqueueing and execution.
    """

    catalog_id: str = Field(description="Internal catalog identifier (immutable).")
    collection_id: str = Field(description="Internal collection identifier (immutable).")
    asset_id: str = Field(description="Asset that triggered this obligation.")

    producer_kind: Literal["raster", "vector"] = Field(
        description=(
            "Which render path to use.  ``'raster'`` → rio-tiler COG render "
            "saved under ``build_render_cache_key``.  ``'vector'`` → PostGIS "
            "MVT generation saved via ``TileStorageProtocol``."
        ),
    )

    min_zoom: int = Field(
        ge=0,
        description="Lowest zoom level to fill (inclusive).  Enforced before any render.",
    )
    max_zoom: int = Field(
        ge=0,
        description=(
            "Highest zoom level to fill (inclusive).  The task logs what was "
            "seeded and what was skipped — never silently caps."
        ),
    )

    tms_ids: List[str] = Field(
        default_factory=lambda: ["WebMercatorQuad"],
        description="TileMatrixSet IDs to pre-seed.",
    )

    style_id: Optional[str] = Field(
        default="default",
        description=(
            "Style identifier included in the raster cache key.  Included so "
            "that a style edit causes a cache-key shift on the next pre-seed "
            "run, leaving stale renders under the old key to expire naturally."
        ),
    )
