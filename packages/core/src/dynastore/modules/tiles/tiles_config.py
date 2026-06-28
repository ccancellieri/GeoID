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

from typing import ClassVar, Dict, List, Optional, Tuple
from pydantic import Field
from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig
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

    By default, bucket selection is determined per-catalog by
    ``StorageProtocol.ensure_storage_for_catalog``.  Catalogs that have no
    provisioned bucket (bucket-free / deferred-provisioned) silently skip
    every tile-cache write and return a miss on every read, forcing PostGIS
    to re-render each tile on every request.

    ``cache_bucket_override`` opts the entire config scope into a shared,
    operator-managed bucket so bucket-free catalogs can participate in the L2
    tile cache.  Per-catalog isolation is preserved: blob keys are namespaced
    by the catalog's external (logical) identifier.

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

    # --- Shared / external bucket override (opt-in) ---
    #
    # When set, ALL tile cache I/O for this config scope is redirected to the
    # named GCS bucket instead of the catalog's provisioned bucket.  This
    # unblocks bucket-free (deferred-provisioned) catalogs: without it,
    # save_tile silently no-ops and every tile re-renders from PostGIS.
    #
    # Blob keys are namespaced by the catalog's external (logical) id so
    # per-catalog isolation is preserved within the shared bucket:
    #   {effective_prefix}/{catalog_id}/{collection_id}/{tms_id}/{z}/{x}/{y}.{fmt}
    # where effective_prefix = cache_bucket_prefix or key_prefix.
    #
    # The catalog_id written into the path is the external identifier from
    # the request URL, never the internal c_... physical schema name.
    cache_bucket_override: Mutable[Optional[str]] = Field(
        default=None,
        min_length=3,
        max_length=222,
        description=(
            "Opt-in: GCS bucket name to use for ALL tile cache I/O regardless "
            "of whether the catalog has a provisioned bucket. Enables bucket-free "
            "catalogs to cache and preseed tiles. Blob keys are namespaced by "
            "catalog_id (external logical id) to preserve isolation: "
            "{prefix}/{catalog_id}/{collection_id}/{tms_id}/{z}/{x}/{y}.{fmt}. "
            "None (default) = use the catalog's own provisioned bucket as before."
        ),
    )

    cache_bucket_prefix: Mutable[Optional[str]] = Field(
        default=None,
        min_length=1,
        max_length=128,
        pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_\-/]*[a-zA-Z0-9]$",
        description=(
            "Object-key prefix used inside ``cache_bucket_override``. "
            "Defaults to the value of ``key_prefix`` when None. "
            "Ignored when ``cache_bucket_override`` is not set."
        ),
    )

    # --- Write-reactive invalidation (#1292) ---
    #
    # Write-reactive invalidation itself has NO on/off knob: it is enabled by
    # default and capability-gated on the presence of a tile reader + a usable
    # cache store (see ``tile_cache_sync.is_tile_cache_active``). The only knob
    # genuinely worth exposing (issue #1292) is an OPT-IN to eagerly re-seed a
    # hot zoom range after invalidation, for collections that need instant
    # freshness instead of lazy repopulate-on-read.
    eager_reseed_max_zoom: Mutable[Optional[int]] = Field(
        default=None,
        ge=0,
        description=(
            "Phase 3 opt-in (#1292): when set, the write-reactive tile-cache "
            "sync eagerly re-renders invalidated tiles up to this zoom level "
            "after marking them stale, instead of waiting for the next "
            "read-miss. ``None`` (default) = pure invalidate-on-write + lazy "
            "repopulate. Currently honored as a no-op stub; eager reseed is "
            "wired in a follow-up."
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
