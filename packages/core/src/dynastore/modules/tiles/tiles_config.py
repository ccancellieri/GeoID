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

import logging
from typing import Any, ClassVar, Dict, List, Literal, Optional, Tuple
from pydantic import Field, field_serializer, field_validator
from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig
from dynastore.models.protocols.configs import ConfigsProtocol
from dynastore.extensions.tools.exposure_mixin import ExposableConfigMixin
from dynastore.tools.discovery import get_protocol
from dynastore.tools.geospatial import SimplificationAlgorithm
from dynastore.modules.tiles.tiles_writers import TileWriterConfig, resolve_writer_config_entry

logger = logging.getLogger(__name__)

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

    # Zoom-aware LINE density filter (opt-in, default disabled) — the length
    # analogue of ``min_feature_pixel_area_by_zoom``.
    #
    # The area filter above can never thin line features: a line's projected
    # area in tile space is 0, so it always passes. Line-dominant collections
    # (road/river/network graphs) therefore aggregate their FULL feature set
    # into every low-zoom tile, which is the primary cause of unrenderable
    # world-scale tiles for such collections. This filter discards lines whose
    # projected LENGTH in MVT pixels is below the per-zoom threshold — the
    # sub-pixel segments that inflate tile size and render time without being
    # visible — before the ST_AsMVT aggregate.
    #
    # Safety rules (mirror the area filter):
    #   - Points and Polygons (projected length = 0) ALWAYS pass through.
    #     The predicate is NOT (ST_Length(geom) > 0 AND ST_Length(geom) < :threshold),
    #     so only line geometries can be dropped.
    #   - The default is None (disabled) to prevent silently dropping valid
    #     features from mixed-geometry or sparse collections.
    #   - Activate for line-dominant collections (e.g. transport networks).
    #   - Conservative example: {0: 2.0, 2: 2.0, 4: 1.0, 6: 1.0, 8: 0.0}
    #     (a sub-2-pixel line is invisible; 1 px is a single pixel span).
    #   - 0.0 in any bracket disables filtering for that zoom and above.
    min_feature_pixel_length_by_zoom: Mutable[Optional[Dict[int, float]]] = Field(
        default=None,
        description=(
            "Opt-in zoom-aware line density filter. "
            "Each key is the minimum zoom level for the bracket; value is the minimum "
            "projected length in MVT pixels that a line feature must have to be included "
            "in the tile (ST_Length(ST_AsMVTGeom(...)) ≥ threshold). "
            "Points and Polygons (length = 0) always pass. "
            "0.0 in any bracket disables filtering for that zoom and above. "
            "Default None = no line density filtering (safe for all collection types). "
            "Override per-collection via PUT /configs/platform/modules/tiles."
        ),
    )

    # Zoom-aware per-tile FEATURE CAP (bounded by default, opt-out).
    #
    # The density filters above run AFTER ST_AsMVTGeom transforms every feature
    # intersecting the tile bbox, so at low zoom the transform cost is
    # O(features in bbox) = O(whole dataset) for a world-scale tile — the reason
    # low-zoom tiles for million/billion-feature collections are unrenderable.
    # This cap pushes a LIMIT into the per-collection subquery BEFORE the
    # transform, so a tile only ever transforms and aggregates at most N
    # features. Render time, tile size and ST_AsMVT memory become bounded by N,
    # independent of the collection's total feature count — the primitive that
    # lets preseed scale to very large collections.
    #
    #   - Each key is the minimum zoom for the bracket (highest key ≤ current
    #     zoom wins), mirroring the simplification/density resolution.
    #   - Value is the maximum number of features aggregated into one tile;
    #     0 in a bracket = uncapped for that zoom and above (the opt-out).
    #   - Default {0: 20000, 4: 50000, 8: 200000} — tight at world scale,
    #     looser as each tile covers less ground. Ordinary collections stay
    #     far below these per-tile counts; only pathological low-zoom tiles
    #     of very large collections are clipped. Disable with {0: 0}.
    #   - Without ``feature_rank_column`` the N kept are storage-order
    #     (arbitrary); set it to keep the most important features instead.
    max_features_per_tile_by_zoom: Mutable[Optional[Dict[int, int]]] = Field(
        default_factory=lambda: {0: 20000, 4: 50000, 8: 200000},
        description=(
            "Zoom-aware per-tile feature cap. Each key is the minimum zoom "
            "for the bracket; value is the maximum number of features aggregated "
            "into a tile (a LIMIT applied before ST_AsMVTGeom, bounding render "
            "cost and tile size regardless of dataset size). 0 in a bracket = "
            "uncapped for that zoom and above; set {0: 0} to disable. "
            "Default {0: 20000, 4: 50000, 8: 200000}."
        ),
    )

    # Self-tuning per-tile BYTE budget (#3155).
    #
    # The feature cap above bounds render cost, but bytes-per-feature varies
    # by orders of magnitude across layers (a 2-vertex segment vs a dense
    # MultiLineString), so no single count holds tile *bytes* to a target.
    # The budget adapts the effective per-tile LIMIT from the measured
    # bytes-per-feature of previous successful renders of the same collection
    # at the same zoom:
    #
    #   effective LIMIT = min(bracket cap, byte_budget // measured bytes/feature)
    #
    # The first render of a (collection, zoom) pair has no measurement yet and
    # uses the feature-cap ladder alone; every successful render refines the
    # estimate, so oversized tiles decay toward the budget on re-render instead
    # of being re-served at full weight forever. It is a target, not a hard
    # cap — a rendered tile is never discarded for exceeding it, and the
    # estimator is per-process (each worker converges after one render).
    tile_byte_budget: Mutable[int] = Field(
        default=1_048_576,
        ge=0,
        description=(
            "Target upper bound in bytes for a rendered MVT tile. The effective "
            "per-tile feature LIMIT adapts toward it using the measured "
            "bytes-per-feature of previous renders of the same collection and "
            "zoom (never above the max_features_per_tile_by_zoom bracket). "
            "A target, not a hard cap: an already-rendered tile is served "
            "whole. 0 disables the byte budget. Default 1 MiB."
        ),
    )

    # Optional stored column used to rank features when the per-tile cap or the
    # min-rank filter is active — e.g. a precomputed, indexed length-in-metres or
    # area-in-metres column on the geometry sidecar. When set, low-zoom tiles
    # keep the highest-ranked (largest / most important) features instead of an
    # arbitrary storage-order subset; when the column is indexed the min-rank
    # filter becomes an index-assisted pre-transform predicate that scales to
    # billions of features. Default None = no ranking.
    feature_rank_column: Mutable[Optional[str]] = Field(
        default=None,
        description=(
            "Name of a stored (ideally indexed) numeric column used to rank "
            "features for the min-rank filter (e.g. length_m / area_m2). "
            "Default None = no ranking."
        ),
    )

    # Zoom-aware minimum value of ``feature_rank_column`` a feature must have to
    # be included, evaluated BEFORE ST_AsMVTGeom. Importance-preserving
    # decimation: keeps the long/large features at low zoom and drops the rest
    # with an index-assisted predicate. Requires ``feature_rank_column``.
    #   - Highest key ≤ current zoom wins (bracket resolution).
    #   - Default None = disabled.
    min_feature_rank_by_zoom: Mutable[Optional[Dict[int, float]]] = Field(
        default=None,
        description=(
            "Opt-in zoom-aware minimum for feature_rank_column (highest key ≤ "
            "current zoom wins). Applied as a pre-transform WHERE, index-assisted "
            "when the column is indexed. Requires feature_rank_column. "
            "Default None = disabled."
        ),
    )

    # Optional stored column used as a per-feature DENSITY CEILING — the
    # inverse of feature_rank_column above. feature_rank_column/
    # min_feature_rank_by_zoom keep features with rank >= floor ("higher is
    # better"), which cannot express "drop overly dense/heavy geometry":
    # e.g. the computed vertex_count geometry stat is an inverse signal,
    # where MORE vertices means WORSE low-zoom render cost, not better. This
    # column feeds max_feature_density_by_zoom instead, an upper bound rather
    # than a lower one. Default None = no density ceiling.
    feature_density_column: Mutable[Optional[str]] = Field(
        default=None,
        description=(
            "Name of a stored (ideally indexed) numeric column measuring "
            "per-feature geometry density/heaviness, used by "
            "max_feature_density_by_zoom as a pre-transform ceiling. Unset "
            "(default) auto-resolves from the collection's stored "
            "statistics via the driver — the columnar vertex_count stat "
            "when the collection materialises one. Set explicitly only to "
            "point the ceiling at a different stored stat column. The "
            "ceiling itself stays disabled until max_feature_density_by_zoom "
            "is also configured."
        ),
    )

    # Zoom-aware maximum value of ``feature_density_column`` a feature may
    # have to be included, evaluated BEFORE ST_AsMVTGeom. The symmetric
    # counterpart of min_feature_rank_by_zoom: excludes features ABOVE the
    # ceiling instead of keeping features above a floor. Requires
    # ``feature_density_column``.
    #   - Highest key ≤ current zoom wins (bracket resolution).
    #   - Value is the inclusive ceiling: features with a density column
    #     value strictly above it are excluded at that zoom.
    #   - 0 in a bracket = no ceiling for that zoom and above, mirroring
    #     max_features_per_tile_by_zoom's opt-out.
    #   - Default None = disabled.
    max_feature_density_by_zoom: Mutable[Optional[Dict[int, float]]] = Field(
        default=None,
        description=(
            "Opt-in zoom-aware ceiling for feature_density_column (highest "
            "key ≤ current zoom wins). Features whose value exceeds the "
            "bracket's ceiling are excluded via a pre-transform WHERE, "
            "index-assisted when the column is indexed. 0 in a bracket = no "
            "ceiling for that zoom and above. Requires feature_density_column. "
            "Default None = disabled."
        ),
    )

    # Caching
    cache_on_demand: Mutable[bool] = Field(
        default=True,
        description="If True, dynamically generated tiles are saved to the preseed storage for future reuse."
    )

    # --- Live-render hardening (#2813) ---
    live_tile_timeout_seconds: Mutable[int] = Field(
        default=60,
        ge=1,
        description=(
            "Per-request PostgreSQL statement timeout (``SET LOCAL "
            "statement_timeout``) applied while rendering an on-demand MVT "
            "tile, aligned with the 60s load-balancer ceiling. On the live "
            "path ``render_budget_seconds`` (55s) is the graceful wall-clock "
            "cutoff and normally fires first (503 with ``Retry-After``); "
            "this statement timeout is the server-side reclaim backstop "
            "that frees the DB worker if a statement outlives that cutoff. "
            "A query that exceeds this is canceled server-side (pgcode "
            "57014); ``get_vector_tile`` then serves a stale cached tile "
            "when one exists, or fails fast with 503 + ``Retry-After``. "
            "Mirrors "
            "``TilesPreseedConfig.preseed_tile_timeout_seconds``."
        ),
    )

    # --- Render wall-clock budget (#2898) ---
    render_budget_seconds: Mutable[int] = Field(
        default=55,
        ge=1,
        description=(
            "Wall-clock budget for the whole ``get_vector_tile`` render "
            "phase (context resolution through rendered bytes), kept below "
            "the 60s load-balancer timeout so an abandoned render never "
            "outlives the client's request. Exceeding it aborts the render "
            "and returns 503 with ``Retry-After``, mirroring the pool-"
            "saturation fail-fast. Distinct from "
            "``live_tile_timeout_seconds``, which bounds only the final "
            "PostGIS statement — a slow multi-collection routing "
            "resolution ahead of that statement is not covered by it."
        ),
    )


async def cache_on_demand_enabled(
    catalog_id: str,
    collection_id: Optional[str] = None,
    *,
    catalog_config: Optional["TilesConfig"] = None,
) -> bool:
    """Resolve the operator's per-catalog/collection ``cache_on_demand`` intent.

    The catalog setting gates first: if disabled there, caching is off
    regardless of the collection. When ``collection_id`` is given, the
    collection's own ``TilesConfig`` can further opt out, defaulting to the
    catalog's value if unset. Pass an already-loaded ``catalog_config`` to
    skip the catalog config fetch (the hot vector-tile path already has one).

    Any config-load failure fails closed (returns ``False``) rather than
    raising, since callers use this to gate a best-effort accelerator.
    """
    mgr = get_protocol(ConfigsProtocol)
    if mgr is None:
        return False
    try:
        cat = catalog_config
        if cat is None:
            cat = await mgr.get_config(TilesConfig, catalog_id)
        catalog_cache = getattr(cat, "cache_on_demand", True)
        if not catalog_cache:
            return False
        if collection_id is None:
            return bool(catalog_cache)
        coll = await mgr.get_config(TilesConfig, catalog_id, collection_id)
        return bool(getattr(coll, "cache_on_demand", catalog_cache))
    except Exception as exc:
        logger.debug("cache_on_demand_enabled: config check failed: %s", exc)
        return False


class TilesCachingConfig(PluginConfig):
    """Operator-tunable knobs for the bucket-backed tile cache.

    By default, bucket selection is determined per-catalog by
    ``StorageProtocol.ensure_storage_for_catalog``.  Catalogs that have no
    provisioned bucket (bucket-free / deferred-provisioned) silently skip
    every tile-cache write and return a miss on every read, forcing PostGIS
    to re-render each tile on every request.

    Cache STORE selection is intentionally NOT here — it is backend-specific.
    The GCS backend caches in each catalog's own provisioned bucket by default,
    or in an operator-configured external bucket (``GcpTileCacheConfig.
    cache_bucket`` / ``cache_prefix``, GCP-specific and classified in the proxy
    tree by the protocol it backs) so bucket-free catalogs can still cache and
    preseed tiles; an on-prem/local-disk backend would key off a filesystem
    root instead. Keeping store selection out of this class lets non-GCP
    deployments reuse the same caching knobs without a GCP dependency.

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

    # --- Bounded background cache writer (Tier A) ---
    cache_writer_buffer_max_bytes: Mutable[int] = Field(
        default=134217728,  # 128 MiB
        ge=0,
        le=2147483648,
        description=(
            "Max total bytes of tile-cache writes buffered + in-flight in "
            "the process-wide background writer. Bounds the RAM the "
            "cache-writer can hold regardless of tile size; writes that "
            "would exceed the budget are dropped and re-rendered on the "
            "next request. 0 disables buffering (every write overflows and "
            "is dropped). Read once at process startup."
        ),
    )
    cache_writer_workers: Mutable[int] = Field(
        default=6,
        ge=1,
        le=64,
        description=(
            "Number of concurrent background workers draining tile-cache "
            "writes to the bucket. Bounds concurrent PUTs / connections. "
            "Read once at process startup; changing it requires a restart."
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

    # --- Writers list (backend-agnostic, extensible, typed) ---
    #
    # WHERE tiles are cached, as an ORDERED list of typed writer configs
    # rather than an enum + a scatter of backend-specific fields. Each
    # implementation provides its own ``TileWriterConfig`` subclass
    # co-located with the implementation (``PgTileWriterConfig`` here,
    # ``GcsTileWriterConfig`` in the ``gcp`` module, a local-disk equivalent
    # in ``modules/local``) and registers a ``(config class, factory)`` pair
    # via ``tiles_writers.register_tile_writer_factory`` — a new writer needs
    # no change to this config's schema. List order = selection priority: at
    # most ONE writer is ever active — ``tiles_writers.select_tile_writer``
    # picks the first AVAILABLE one (registered factory + enabled + target
    # resolves), which is what replaces ``storage_priority``. Resolution
    # (defaulting, back-compat mapping) lives in
    # ``tiles_writers.resolve_effective_writers``.
    writers: Mutable[Optional[List[TileWriterConfig]]] = Field(
        default=None,
        description=(
            "Ordered tile-cache writer list. ``None``/empty (default) = auto: "
            "``[GcsTileWriterConfig()]`` (the catalog's own managed bucket) "
            "when the ``gcp`` module registered a ``StorageProtocol`` "
            "provider, followed by ``PgTileWriterConfig()`` as a fail-safe "
            "fallback; else just ``[PgTileWriterConfig()]`` — today's "
            "behavior, zero migration. The first AVAILABLE writer in list "
            "order is selected and serves both reads and writes; an "
            "unavailable earlier candidate (e.g. no matching StorageProtocol "
            "registered) is skipped and logged, never silently drops writes. "
            "Each list entry must include its ``writer_key`` (its concrete "
            "class's snake_case identity, e.g. ``'gcs_tile_writer_config'``, "
            "``'pg_tile_writer_config'``) so it round-trips to the right typed class."
        ),
    )

    @field_validator("writers", mode="before")
    @classmethod
    def _resolve_writers(cls, value: Any) -> Any:
        if value is None:
            return value
        return [resolve_writer_config_entry(item) for item in value]

    @field_serializer("writers")
    def _serialize_writers(self, value: Optional[List[TileWriterConfig]]) -> Optional[List[Dict[str, Any]]]:
        # The field is typed List[TileWriterConfig] (the base class), so
        # pydantic's default nested-field serialization would use only the
        # base class's schema and silently drop each entry's own subclass
        # fields (e.g. GcsTileWriterConfig.bucket). Dumping each instance
        # directly uses its actual runtime type instead, preserving them —
        # and preserving the writer_key discriminator round_trip needs.
        if value is None:
            return None
        return [item.model_dump() for item in value]


async def _load_caching_config() -> TilesCachingConfig:
    """Fetch live ``TilesCachingConfig``; fall back to defaults if unavailable.

    Mirrors the ``ElasticsearchIndexConfig`` pattern (issue #489): a missing
    platform-configs layer (cold boot, unit test, manager not registered)
    yields safe defaults rather than crashing tile I/O.

    Lives here (not in the GCP module) so PG- and local-disk-only tile
    storage providers never need to import anything GCP-specific to resolve
    their caching config.
    """
    from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
    from dynastore.tools.discovery import get_protocol as _get_protocol

    mgr = _get_protocol(PlatformConfigsProtocol)
    if mgr is None:
        return TilesCachingConfig()
    try:
        cfg = await mgr.get_config(TilesCachingConfig)
    except Exception as exc:
        logger.debug("TilesCachingConfig: get_config failed (%s); using defaults", exc)
        return TilesCachingConfig()
    return cfg if isinstance(cfg, TilesCachingConfig) else TilesCachingConfig()


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
        description=(
            "DEPRECATED, no-op: storage selection is now "
            "``TilesCachingConfig.writers`` (first AVAILABLE writer in list "
            "order wins). Kept only so existing configs keep validating. "
            "When ``writers`` is unset and this list's first entry is "
            "``'pg'``, it is soft-mapped to ``[PgTileWriterConfig()]`` for "
            "back-compat, reproducing the original intent exactly; a leading "
            "``'bucket'`` (or any other value) leaves ``writers`` at its "
            "default auto-injection."
        ),
    )

    # Generation Overrides
    simplification_by_zoom_override: Mutable[Optional[Dict[int, float]]] = Field(default=None,
        description="Override runtime simplification settings for pre-seeded tiles."
    )

    # Catalog Level specific
    collections_to_preseed: Mutable[Optional[List[str]]] = Field(default=None,
        description="For Catalog-level config: list of collections to include. If None, applies to all (or logic defined by task)."
    )

    # --- Preseed hardening (#2813) ---
    preseed_max_zoom: Mutable[Optional[int]] = Field(
        default=None,
        ge=0,
        description=(
            "Caps the max zoom preseeded when no explicit request/runtime-"
            "config zoom applies, so an unbounded default bbox can't fan out "
            "to a huge tile count. ``None`` (default) falls back to a fixed "
            "internal cap (see ``tasks.tiles_preseed.task.PRESEED_DEFAULT_MAX_ZOOM``)."
        ),
    )

    preseed_tile_timeout_seconds: Mutable[int] = Field(
        default=60,
        ge=1,
        description=(
            "Per-tile PostgreSQL statement timeout (``SET LOCAL "
            "statement_timeout``) applied for the duration of each per-zoom "
            "preseed transaction. A tile that exceeds this is counted in "
            "``results['skipped']`` and the run continues, instead of one "
            "pathological tile stalling the whole job. Preseed runs offline "
            "with no load-balancer deadline, but renders the same heavy "
            "statements as the live path, so it defaults to the same "
            "ceiling as ``TilesConfig.live_tile_timeout_seconds``."
        ),
    )
