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

from typing import ClassVar, Dict, List, Optional, Tuple
from pydantic import Field, BaseModel
from dynastore.modules.db_config.platform_config_service import Mutable, PluginConfig
from dynastore.extensions.tools.exposure_mixin import ExposableConfigMixin
from dynastore.tools.geospatial import SimplificationAlgorithm

class TilesConfig(ExposableConfigMixin, PluginConfig):
    """
    Runtime configuration for the Tiles extension.
    Controls visibility, bounds, and on-the-fly generation settings.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "modules", "tiles")


    # Global mask/bounds
    bbox: Mutable[Optional[List[Tuple[float, float, float, float]]]] = Field(default=None, 
        description="Global bounding boxes for the collection/catalog. Requests outside these bounds return 404/Empty. If None, assumes world/max extent."
    )
    
    # Zoom limits
    min_zoom: Mutable[int] = Field(default=0, description="Minimum zoom level served.")
    max_zoom: Mutable[int] = Field(default=12, description="Maximum zoom level served.")
    
    # TMS Support
    supported_tms_ids: Mutable[List[str]] = Field(
        default=["WebMercatorQuad"], 
        description="List of supported TileMatrixSet IDs."
    )
    
    # Runtime Generation Settings
    simplification_by_zoom: Mutable[Optional[Dict[int, float]]] = Field(default=None, 
        description="Map of zoom level to simplification tolerance (in degrees/units). Applied during dynamic generation."
    )
    simplification_algorithm: Mutable[Optional[SimplificationAlgorithm]] = Field(
        default=SimplificationAlgorithm.TOPOLOGY_PRESERVING,
        description="Algorithm used for dynamic simplification."
    )

    # Caching
    cache_on_demand: Mutable[bool] = Field(
        default=True,
        description="If True, dynamically generated tiles are saved to the preseed storage for future reuse."
    )


class TilesCachingConfig(PluginConfig):
    """Operator-tunable knobs for the bucket-backed tile cache.

    Bucket selection is intentionally NOT exposed here — buckets are
    provisioned per-catalog by ``StorageProtocol.ensure_storage_for_catalog``
    and surfacing an override would break the per-catalog isolation
    invariants enforced by the catalog lifecycle. The bucket in use can
    be discovered at runtime via ``GET /admin/catalogs/{cat}`` (the
    storage block surfaces the resolved bucket name).

    Live edits via ``PUT /configs/plugins/tiles_caching_config`` apply on
    the next tile save / fetch — no rewrite of already-cached objects.
    Changing ``key_prefix`` orphans existing cached tiles (they remain
    under the old prefix until the bucket TTL evicts them).

    ``cache_enabled`` (default ``True``) gates the *bucket-backed L2
    cache only*.  When ``False``:

    - ``get_tile`` / ``get_tile_url`` / ``check_tile_exists`` return as a
      miss without touching the bucket (every request falls through to
      PostGIS generation).
    - ``save_tile`` is a no-op (already-generated tiles are not persisted
      to the bucket).
    - Deletes still execute (cleanup paths must work even after disabling
      the cache so operators can drop stale blobs).

    Disabling the L2 cache does NOT disable tile generation — for that
    use ``TilesConfig.enabled`` (the canonical ``ExposableConfigMixin``
    user in the tiles cluster; master switch on the tiles extension).
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "modules", "tiles")

    cache_enabled: Mutable[bool] = Field(
        default=True,
        description=(
            "Bucket-backed L2 cache toggle. NOT an extension exposure "
            "toggle — see ``TilesConfig.enabled`` for that."
        ),
    )

    key_prefix: Mutable[str] = Field(
        default="tiles/collections",
        min_length=1,
        max_length=128,
        pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_\-/]*[a-zA-Z0-9]$",
        description=(
            "Object-key prefix under the catalog bucket. The full key is "
            "``{key_prefix}/{collection_id}/{tms_id}/{z}/{x}/{y}.{format}``. "
            "Changing this orphans existing cached tiles."
        ),
    )

    ttl_seconds: Mutable[int] = Field(
        default=31536000,
        ge=0,
        le=31536000,
        description=(
            "``Cache-Control: public, max-age=<ttl_seconds>`` set on every "
            "tile object written to the bucket. 0 disables browser/CDN "
            "caching (objects still persist server-side). Default is one "
            "year (the GCS max-age ceiling)."
        ),
    )


class TilesPreseedConfig(PluginConfig):
    """
    Configuration for the Tiles Pre-seeding Process.
    This configures the background task that generates and stores tiles.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "modules", "tiles")

    preseed_enabled: Mutable[bool] = Field(
        default=True,
        description=(
            "When False, the tile pre-seed task short-circuits and skips "
            "generating/storing tiles. Per-task knob, NOT an extension "
            "exposure toggle (see ``TilesConfig.enabled``)."
        ),
    )

    # What to seed
    target_tms_ids: Mutable[List[str]] = Field(
        default=["WebMercatorQuad"], 
        description="List of TMS IDs to pre-seed. Must be a subset of TilesConfig.supported_tms_ids."
    )
    formats: Mutable[List[str]] = Field(
        default=["mvt"], 
        description="List of output formats to generate (e.g. 'mvt', 'geojson')."
    )
    
    # Where to seed (Spatial subset)
    bboxes: Mutable[Optional[List[Tuple[float, float, float, float]]]] = Field(default=None, 
        description="Specific areas to pre-seed. Intersected with TilesConfig.bbox."
    )
    
    # Storage Configuration
    storage_priority: Mutable[List[str]] = Field(
        default=["bucket", "pg"], 
        description="Priority list of storage providers to use for saving tiles."
    )
    
    # Generation Overrides
    simplification_by_zoom_override: Mutable[Optional[Dict[int, float]]] = Field(default=None, 
        description="Override runtime simplification settings for pre-seeded tiles."
    )
    
    # Catalog Level specific
    collections_to_preseed: Mutable[Optional[List[str]]] = Field(default=None, 
        description="For Catalog-level config: list of collections to include. If None, applies to all (or logic defined by task)."
    )
