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

from typing import ClassVar, Dict, List, Literal, Optional, Tuple
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
    #
    # Default per-zoom simplification tolerances (EPSG:4326 source, WebMercatorQuad TMS,
    # extent=4096). Each key is the minimum zoom level for the bracket; the lookup in
    # _build_collection_subquery finds the highest key ≤ the requested zoom.
    # Tolerance = half a MVT pixel width in source-CRS degrees at the bracket's lower bound.
    # At z10+ the value is 0.0 (feature detail exceeds pixel resolution; simplification
    # produces no benefit and risks collapsing thin polygon slivers at interactive zoom).
    #
    # Zoom bracket | °/pixel  | Default tolerance
    # -------------|----------|------------------
    # z 0–1        | 0.088    | 0.044
    # z 2–3        | 0.022    | 0.011
    # z 4–5        | 0.0055   | 0.00275
    # z 6–7        | 0.00137  | 0.000687
    # z 8–9        | 0.000343 | 0.000172
    # z10+         | ≤0.000086| 0.0  (disabled)
    #
    # For meter-source collections (EPSG:3857) multiply by ~111 319 (m/° at equator).
    # Override per-collection via PUT /configs/platform/modules/tiles on the relevant scope.
    # Set to {} or null to disable all simplification.
    simplification_by_zoom: Mutable[Optional[Dict[int, float]]] = Field(
        default_factory=lambda: {
            0: 0.044,
            2: 0.011,
            4: 0.00275,
            6: 0.000687,
            8: 0.000172,
            10: 0.0,
        },
        description=(
            "Per-zoom simplification tolerance in source-CRS units (degrees for EPSG:4326). "
            "Each key is the minimum zoom level for its bracket; the lookup finds the "
            "highest key ≤ the requested zoom. 0.0 disables simplification for that bracket. "
            "Defaults are approximately half a MVT-pixel width at each zoom level for a "
            "4326-source WebMercatorQuad collection (extent=4096). "
            "Set to {} or null to disable simplification entirely."
        ),
    )
    simplification_algorithm: Mutable[Optional[SimplificationAlgorithm]] = Field(
        default=SimplificationAlgorithm.TOPOLOGY_PRESERVING,
        description=(
            "Algorithm used for dynamic simplification before ST_AsMVTGeom. "
            "topology_preserving (default) uses ST_SimplifyPreserveTopology — "
            "accurate, avoids self-intersections, but O(n log n). "
            "snap_to_grid uses ST_SnapToGrid — O(n) coordinate-snap, opt-in for "
            "speed at low/mid zoom where topology fidelity matters less. "
            "douglas_peucker uses ST_Simplify — faster than topology-preserving "
            "with minor sliver risk acceptable for display tiles."
        ),
    )

    # Zoom-aware feature density filter (opt-in, default disabled).
    #
    # At low zoom many polygon features project to sub-pixel area — they inflate
    # tile byte size and driver render time without being visible. When configured,
    # the MVT query adds a WHERE clause that discards these features before the
    # ST_AsMVT aggregate.
    #
    # Safety rules:
    #   - Points and LineStrings (projected area = 0) ALWAYS pass through.
    #     The predicate is  NOT (ST_Area(geom) > 0 AND ST_Area(geom) < :threshold),
    #     so only polygon geometries can be dropped.
    #   - The default is None (disabled) to prevent silently dropping valid features
    #     from mixed-geometry or sparsely-populated collections.
    #   - Activate only for polygon-dominant collections (e.g. administrative units).
    #   - Conservative example: {0: 4.0, 2: 4.0, 4: 2.0, 6: 1.0, 8: 0.0}
    #     (4 MVT pixel² is reliably invisible to any renderer; 1 px² is a single pixel).
    #   - 0.0 in any bracket disables filtering for that zoom and above.
    min_feature_pixel_area_by_zoom: Mutable[Optional[Dict[int, float]]] = Field(
        default=None,
        description=(
            "Opt-in zoom-aware feature density filter. "
            "Each key is the minimum zoom level for the bracket; value is the minimum "
            "projected area in MVT pixel² that a polygon feature must have to be included "
            "in the tile (ST_Area(ST_AsMVTGeom(...)) ≥ threshold). "
            "Points and LineStrings (area = 0) always pass. "
            "0.0 in any bracket disables filtering for that zoom and above. "
            "Default None = no density filtering (safe for all collection types). "
            "Override per-collection via PUT /configs/platform/modules/tiles."
        ),
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

    # --- Serve mode ---
    #
    # Controls how cache hits are delivered to the client.
    #
    # ``redirect`` (default): the service resolves a short-lived V4 signed URL
    # and issues a 307 so the client fetches bytes directly from GCS — zero
    # proxy bandwidth through the API process.  Requires the Cloud Run SA to
    # have ``iam.serviceAccounts.signBlob`` (IAMCredentials API); signing uses
    # ``identity_provider.get_account_email()`` + ``get_fresh_token()`` so no
    # static key file is needed.  If signing raises (e.g. permission denied),
    # the request falls back to ``proxy`` automatically and a WARNING is logged.
    #
    # ``proxy``: bytes are streamed through the API process (old behaviour).
    # Use when signing credentials are unavailable or when a reverse proxy
    # (CDN / Nginx) handles client-facing GCS redirects itself.
    cache_serve_mode: Mutable[Literal["proxy", "redirect"]] = Field(
        default="redirect",
        description=(
            "How cache hits reach the client. ``redirect`` (default) issues a "
            "307 to a V4 signed GCS URL — offloads byte transfer to GCS, no "
            "proxy bandwidth through the API. Requires ``iam.serviceAccounts"
            ".signBlob`` on the Cloud Run SA; falls back to ``proxy`` "
            "automatically if signing fails. ``proxy`` streams bytes through "
            "the API process (lower concurrency ceiling)."
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
