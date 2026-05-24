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

"""Write-reactive tile-cache invalidation (issue #1292, Phase 1).

When features in a collection are created or updated, any cached map tile
that overlaps the feature's *new* bbox is now stale. This module is the
coordinator that, on the write path, works out which tiles a batch of
features touches across the served zoom range and enqueues a durable
mark-stale obligation on the OUTBOX — the same atomic, retry-guaranteed
machinery the secondary index uses. A background drain then deletes those
tiles so the next read repopulates lazily from fresh data.

Design (see #1292):

* **OUTBOX seam, not events.** The enqueue rides the write transaction
  (atomic with the data write); a failed enqueue rolls the write back.
  Delivery is retry-guaranteed by the drain.
* **Capability-gated, enabled by default.** No on/off flag. The
  participant is active for a collection when a tile reader
  (``TileStorageProtocol``) is registered AND its backing store is
  usable. Presence of the components is the signal.
* **Verify + degrade.** If the store is not usable we log a WARNING and
  skip — the cache is simply not accelerated. Invalidation must NEVER
  block a read or a write.
* **Invalidate, don't eager-render.** We mark tiles stale (delete them);
  the existing lazy read-miss path re-renders via ``ST_AsMVT``.
* **Scale.** We never load geometry — we read the canonical bbox envelope
  off the Feature and compute tile coverage from it via ``morecantile``.
  Coverage is COALESCED and deduped across the whole batch, and the
  fan-out is CAPPED (a continent-scale bbox cannot enqueue an unbounded
  number of tiles — the served zoom range is clamped downward until the
  affected-tile count fits under a ceiling).
* **Mark-stale beats overwrite (order-tolerant).** Deleting a tile is
  idempotent; a later edit's delete cannot be clobbered by an in-flight
  regeneration of an earlier one, because regeneration is a *read*-miss
  repopulate that always reflects current data.

Phase 2 (prior-bbox capture for DELETE / geometry-MOVE) is NOT in this
module — see :data:`PHASE2_PRIOR_BBOX_EXTENSION_POINT` and
``affected_tiles_for_batch`` for the slot where a prior bbox would be
unioned in once core carries it on the write/event payload.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

logger = logging.getLogger(__name__)

# OUTBOX driver_id for the tile-cache invalidation participant. It is NOT a
# CollectionItemsStore driver — tiles aren't entities — so it never appears
# in the items routing config. It rides the OUTBOX as a free-form driver_id
# whose drain (``TileCacheBulkIndexer``) marks tiles stale instead of
# indexing documents.
TILE_CACHE_DRIVER_ID = "tile_cache_invalidator"

# Tile formats the read/render path serves and caches. ``tiles_service`` caches
# under each of these (format is part of the tile primary key / object key), so
# invalidation MUST drop every one — dropping only ``mvt`` would leave a stale
# ``pbf`` of the same coordinate cached. Keep in sync with the format whitelist
# in ``extensions/tiles/tiles_service.py`` (``["mvt", "pbf"]``).
SERVED_TILE_FORMATS: Tuple[str, ...] = ("mvt", "pbf")

# Hard ceiling on tiles enqueued per coalesced batch. A continent-scale or
# scattered-multipolygon bbox at high zoom can cover tens of thousands of
# tiles; clamping the served zoom range downward keeps a single edit (or a
# GLOSIS batch of large features) from enqueuing an unbounded fan-out. Coarse
# tiles still get invalidated, so correctness is preserved at the cost of
# finer-zoom freshness for very large features (those repopulate on the next
# read anyway).
DEFAULT_MAX_TILES_PER_BATCH = 4096

# WGS84 world bbox — used to clamp degenerate / world-spanning bboxes. The
# latitude bound is the Web Mercator validity limit (±85.0511287798°), beyond
# which morecantile rejects/garbles coordinates.
_WORLD_BBOX: "TileBBox" = (-180.0, -85.0511287798, 180.0, 85.0511287798)


# A WGS84 bbox: (west, south, east, north).
TileBBox = Tuple[float, float, float, float]

# A single tile coordinate within a TMS: (tms_id, z, x, y).
TileCoord = Tuple[str, int, int, int]


# ---------------------------------------------------------------------------
# Phase 2 extension point (documented; not implemented here).
#
# To invalidate the tiles a feature *used to* occupy (DELETE, or a geometry
# MOVE where the old footprint no longer overlaps the new one) the listener
# needs the feature's PRIOR bbox. Today the write/event payload doesn't carry
# it:
#   * ITEM_DELETION carries only item_id (no geometry/bbox);
#   * ITEM_UPDATE / the secondary-write fan-out carries only the NEW feature.
#
# Core change required (Phase 2): capture the prior bbox on the write txn and
# surface it to this participant. Concretely, item_service must read the
# existing row's bbox BEFORE the upsert/delete (inside the same wrapping TX,
# from the canonical bbox envelope column — never the raw geometry) and pass a
# {item_id: prior_bbox} map through to ``enqueue_tile_invalidations`` /
# ``affected_tiles_for_batch`` (and the ITEM_DELETION event payload must carry
# the prior bbox too). The union point is already present below: every place
# that consumes a feature's new bbox would also consume its prior bbox and add
# those tiles to the same coalesced set. No change to the coverage/coalesce/cap
# math or the drain is needed — only the prior-bbox source.
# ---------------------------------------------------------------------------
PHASE2_PRIOR_BBOX_EXTENSION_POINT = (
    "affected_tiles_for_batch / enqueue_tile_invalidations accept a "
    "prior_bboxes map once item_service captures the pre-write bbox on the "
    "write txn (and ITEM_DELETION carries it)."
)


# ===========================================================================
# Pure coverage / coalesce / cap math (no DB, no geometry, no I/O)
# ===========================================================================


def feature_bbox(feature: Any) -> Optional[TileBBox]:
    """Read the canonical bbox envelope off a Feature without any I/O.

    Prefers the explicit ``bbox`` field (``[w, s, e, n]``). When that is
    absent, falls back to a cheap bounds scan of the geometry coordinates
    ALREADY on the object — it never re-reads geometry from storage. Large
    geometries are expected to carry a materialized ``bbox`` from the write
    policy, so the common path is the field read; the coordinate scan only
    fires for inline geometries with no bbox, and operates purely on
    in-memory coords (no DB, no network). Returns ``None`` when no bbox is
    derivable.
    """
    bbox = getattr(feature, "bbox", None)
    if bbox is None and isinstance(feature, dict):
        bbox = feature.get("bbox")
    if bbox and len(bbox) >= 4:
        try:
            w, s, e, n = float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3])
        except (TypeError, ValueError):
            return None
        return _normalize_bbox(w, s, e, n)

    geom = getattr(feature, "geometry", None)
    if geom is None and isinstance(feature, dict):
        geom = feature.get("geometry")
    if geom is None:
        return None
    coords = geom.get("coordinates") if isinstance(geom, dict) else getattr(geom, "coordinates", None)
    if coords is None:
        return None
    bounds = _coords_bounds(coords)
    if bounds is None:
        return None
    return _normalize_bbox(*bounds)


def _normalize_bbox(w: float, s: float, e: float, n: float) -> TileBBox:
    """Order corners and clamp to the renderable WGS84 world extent.

    morecantile rejects/garbles latitudes outside the Web Mercator valid
    band; clamping here keeps a sloppy or world-spanning bbox from blowing
    up coverage computation.
    """
    if w > e:
        w, e = e, w
    if s > n:
        s, n = n, s
    w = max(w, _WORLD_BBOX[0])
    s = max(s, _WORLD_BBOX[1])
    e = min(e, _WORLD_BBOX[2])
    n = min(n, _WORLD_BBOX[3])
    return (w, s, e, n)


def _coords_bounds(coords: Any) -> Optional[Tuple[float, float, float, float]]:
    """Bounds of nested GeoJSON coordinate arrays. Pure, no geometry libs."""
    xs: List[float] = []
    ys: List[float] = []

    def _walk(node: Any) -> None:
        if isinstance(node, (list, tuple)):
            if (
                len(node) >= 2
                and isinstance(node[0], (int, float))
                and isinstance(node[1], (int, float))
            ):
                xs.append(float(node[0]))
                ys.append(float(node[1]))
            else:
                for child in node:
                    _walk(child)

    _walk(coords)
    if not xs or not ys:
        return None
    return (min(xs), min(ys), max(xs), max(ys))


def _tiles_for_bbox(
    bbox: TileBBox, tms_id: str, z: int,
) -> Set[TileCoord]:
    """All tiles of ``tms_id`` at zoom ``z`` overlapping ``bbox``.

    Uses ``morecantile`` (the TMS library already used by the preseed task)
    so coverage matches what the rest of the tile pipeline computes. A bbox
    that cannot be resolved (unknown TMS) yields an empty set rather than
    raising — invalidation must never break the write.
    """
    try:
        import morecantile
    except Exception:  # pragma: no cover - morecantile is a core dep
        logger.warning("tile_cache: morecantile unavailable; cannot compute coverage")
        return set()
    try:
        tms = morecantile.tms.get(tms_id)
    except Exception:
        logger.warning("tile_cache: unknown TMS '%s'; skipping coverage", tms_id)
        return set()
    w, s, e, n = bbox
    out: Set[TileCoord] = set()
    try:
        for t in tms.tiles(w, s, e, n, zooms=[z]):
            out.add((tms_id, z, t.x, t.y))
    except Exception as exc:
        logger.warning(
            "tile_cache: coverage failed for bbox=%s tms=%s z=%s: %s",
            bbox, tms_id, z, exc,
        )
    return out


def _count_tiles_for_bboxes(
    bboxes: Sequence[TileBBox], tms_ids: Sequence[str], z: int,
) -> int:
    """Deduped tile count for a set of bboxes at one zoom (for cap probing)."""
    acc: Set[TileCoord] = set()
    for tms_id in tms_ids:
        for bbox in bboxes:
            acc |= _tiles_for_bbox(bbox, tms_id, z)
    return len(acc)


def clamp_zoom_range_to_cap(
    bboxes: Sequence[TileBBox],
    tms_ids: Sequence[str],
    min_zoom: int,
    max_zoom: int,
    *,
    max_tiles: int = DEFAULT_MAX_TILES_PER_BATCH,
) -> int:
    """Largest ``z_max <= max_zoom`` whose cumulative coverage fits the cap.

    Counts tiles from ``min_zoom`` up, stopping before the running total
    would exceed ``max_tiles``. Coarse zooms (which someone is more likely
    to be viewing) are always kept; only the finest zooms of a very large
    bbox are dropped. Returns at least ``min_zoom`` so the coarsest tiles
    are always invalidated even if a single zoom already blows the cap.
    """
    if max_zoom < min_zoom:
        min_zoom, max_zoom = max_zoom, min_zoom
    running = 0
    chosen = min_zoom
    for z in range(min_zoom, max_zoom + 1):
        running += _count_tiles_for_bboxes(bboxes, tms_ids, z)
        if running > max_tiles and z > min_zoom:
            return z - 1
        chosen = z
    return chosen


def affected_tiles_for_batch(
    bboxes: Sequence[TileBBox],
    tms_ids: Sequence[str],
    min_zoom: int,
    max_zoom: int,
    *,
    max_tiles: int = DEFAULT_MAX_TILES_PER_BATCH,
    prior_bboxes: Optional[Sequence[TileBBox]] = None,
) -> Set[TileCoord]:
    """Coalesced, deduped, capped set of tiles a whole batch invalidates.

    ``bboxes`` are the NEW feature bboxes of the batch. ``prior_bboxes``
    (Phase 2) would be the pre-write bboxes; they're unioned into the same
    coverage so a DELETE / geometry MOVE also invalidates the old footprint.
    Phase 1 callers pass only ``bboxes``.

    The cap clamps the served zoom range downward so the result is bounded
    regardless of how large the features are.
    """
    all_bboxes: List[TileBBox] = list(bboxes)
    if prior_bboxes:
        all_bboxes.extend(prior_bboxes)
    if not all_bboxes or not tms_ids:
        return set()

    effective_max_zoom = clamp_zoom_range_to_cap(
        all_bboxes, tms_ids, min_zoom, max_zoom, max_tiles=max_tiles,
    )
    if effective_max_zoom < max_zoom:
        logger.info(
            "tile_cache: clamped invalidation zoom range %s..%s -> %s..%s "
            "(cap=%s) for %d bbox(es)",
            min_zoom, max_zoom, min_zoom, effective_max_zoom,
            max_tiles, len(all_bboxes),
        )

    tiles: Set[TileCoord] = set()
    for z in range(min_zoom, effective_max_zoom + 1):
        for tms_id in tms_ids:
            for bbox in all_bboxes:
                tiles |= _tiles_for_bbox(bbox, tms_id, z)
    return tiles


@dataclass(frozen=True)
class TileCacheCoverage:
    """Result of computing a batch's invalidation footprint."""

    tiles: Set[TileCoord]
    bbox_count: int
    clamped: bool


def coverage_for_features(
    features: Iterable[Any],
    tms_ids: Sequence[str],
    min_zoom: int,
    max_zoom: int,
    *,
    max_tiles: int = DEFAULT_MAX_TILES_PER_BATCH,
) -> TileCacheCoverage:
    """End-to-end: features -> bboxes -> coalesced, capped tile set."""
    bboxes: List[TileBBox] = []
    for f in features:
        bb = feature_bbox(f)
        if bb is not None:
            bboxes.append(bb)
    tiles = affected_tiles_for_batch(
        bboxes, tms_ids, min_zoom, max_zoom, max_tiles=max_tiles,
    )
    # ``clamped`` is informational; recompute the uncapped max-zoom decision
    # cheaply only when we actually had bboxes.
    clamped = bool(bboxes) and (
        clamp_zoom_range_to_cap(
            bboxes, tms_ids, min_zoom, max_zoom, max_tiles=max_tiles,
        )
        < max_zoom
    )
    return TileCacheCoverage(tiles=tiles, bbox_count=len(bboxes), clamped=clamped)


# ===========================================================================
# Capability gate + degrade
# ===========================================================================


async def is_tile_cache_active(catalog_id: str, collection_id: str) -> bool:
    """True when write-reactive invalidation should run for this collection.

    Capability-gated (issue #1292): ON when ALL of
      (0) the L2 cache is enabled — ``TilesCachingConfig.cache_enabled`` is
          ``True`` (when ``False`` nothing is cached, so there is nothing to
          invalidate and no obligation should be enqueued), and
      (1) a tile reader is configured — a ``TileStorageProtocol`` provider is
          registered (PG cache table or bucket), and
      (2) the backing store is usable — verified via ``verify_cache_store``.

    Verify + degrade: if the store is not usable we log a clear WARNING and
    return ``False`` (run with the cache disabled, degrading to live
    ``ST_AsMVT``). Never raises — a misconfigured cache must not block writes.
    """
    from dynastore.modules import get_protocol
    from dynastore.modules.tiles.tiles_module import TileStorageProtocol

    # (0) L2 cache off → nothing is being cached, so no invalidation
    # obligation is warranted. Resolve tolerantly (defaults to enabled).
    try:
        from dynastore.modules.gcp.tiles_storage import _load_caching_config

        cfg = await _load_caching_config()
        if not getattr(cfg, "cache_enabled", True):
            logger.debug(
                "tile_cache: L2 cache disabled for %s/%s — "
                "write-reactive invalidation off",
                catalog_id, collection_id,
            )
            return False
    except Exception:  # noqa: BLE001 — never let config lookup break a write
        pass

    provider = get_protocol(TileStorageProtocol)
    if provider is None:
        logger.debug(
            "tile_cache: no TileStorageProtocol registered for %s/%s — "
            "write-reactive invalidation off",
            catalog_id, collection_id,
        )
        return False

    usable = await verify_cache_store(provider, catalog_id, collection_id)
    if not usable:
        logger.warning(
            "tile_cache: cache store for %s/%s is not usable — running with "
            "the tile cache DISABLED (reads degrade to live ST_AsMVT). "
            "Write-reactive invalidation skipped.",
            catalog_id, collection_id,
        )
        return False
    return True


async def verify_cache_store(
    provider: Any, catalog_id: str, collection_id: str,
) -> bool:
    """Best-effort check that ``provider``'s backing store is reachable.

    The check is intentionally cheap and tolerant: a probe ``check_tile_exists``
    on a sentinel tile. A bucket/PG-table that answers (even "no") is usable;
    a backend that errors (missing bucket, dropped table, auth failure) is
    treated as not usable so the caller degrades. Never raises.
    """
    probe = getattr(provider, "check_tile_exists", None)
    if probe is None:
        # A provider with no existence probe still has save/get/delete — assume
        # usable and let the drain surface real failures via OUTBOX retry.
        return True
    try:
        await probe(
            catalog_id, collection_id, "WebMercatorQuad", 0, 0, 0, "mvt",
        )
        return True
    except Exception as exc:
        logger.warning(
            "tile_cache: store probe failed for %s/%s: %s",
            catalog_id, collection_id, exc,
        )
        return False


# ===========================================================================
# Enqueue side (write path) — atomic OUTBOX enqueue on the write txn
# ===========================================================================


def _encode_tile_payload(tiles: Set[TileCoord]) -> Dict[str, Any]:
    """Serialize a coalesced tile set into a single OUTBOX payload.

    One outbox row carries the whole coalesced batch (not one row per tile),
    so a GLOSIS batch that touches thousands of tiles enqueues a single
    durable obligation. The drain expands it back to coordinates.
    """
    return {
        "tiles": [
            {"tms_id": tms_id, "z": z, "x": x, "y": y}
            for (tms_id, z, x, y) in sorted(tiles)
        ],
    }


def decode_tile_payload(payload: Dict[str, Any]) -> List[TileCoord]:
    """Inverse of :func:`_encode_tile_payload` (used by the drain)."""
    out: List[TileCoord] = []
    for t in payload.get("tiles", []) or []:
        try:
            out.append((str(t["tms_id"]), int(t["z"]), int(t["x"]), int(t["y"])))
        except (KeyError, TypeError, ValueError):
            continue
    return out


async def _resolve_tile_extent(
    catalog_id: str, collection_id: str,
) -> Tuple[List[str], int, int]:
    """Resolve served TMS ids + zoom range from the live ``TilesConfig``.

    Falls back to safe defaults if config is unavailable. Pure read; never
    raises.
    """
    from dynastore.modules import get_protocol
    from dynastore.models.protocols import ConfigsProtocol
    from dynastore.modules.tiles.tiles_config import TilesConfig

    tms_ids: List[str] = ["WebMercatorQuad"]
    min_zoom, max_zoom = 0, 12
    configs = get_protocol(ConfigsProtocol)
    if configs is None:
        return tms_ids, min_zoom, max_zoom
    try:
        cfg = await configs.get_config(
            TilesConfig, catalog_id=catalog_id, collection_id=collection_id,
        )
    except Exception:
        return tms_ids, min_zoom, max_zoom
    if isinstance(cfg, TilesConfig):
        tms_ids = list(cfg.supported_tms_ids or tms_ids)
        min_zoom = int(cfg.min_zoom)
        max_zoom = int(cfg.max_zoom)
    return tms_ids, min_zoom, max_zoom


async def enqueue_tile_invalidations(
    conn: Any,
    catalog_id: str,
    collection_id: str,
    features: Sequence[Any],
    *,
    outbox: Optional[Any] = None,
    max_tiles: int = DEFAULT_MAX_TILES_PER_BATCH,
) -> int:
    """Coalesce a batch's affected tiles and enqueue ONE OUTBOX obligation.

    ``conn`` is forwarded to ``outbox.enqueue_bulk``. Phase 1 callers pass
    ``None``: the enqueue runs AFTER the data write is already committed, so
    there is nothing to be atomic with, and ``PgOutboxStore`` then acquires
    its OWN raw asyncpg connection and sets ``search_path`` to the tenant
    schema (mirroring ``IndexDispatcher._enqueue_outbox_record``). A non-None
    ``conn`` MUST be a raw asyncpg connection with ``search_path`` already
    pinned — a SQLAlchemy connection has no ``copy_records_to_table`` and would
    fail; the worst case of the self-managed path is one stale tile read.

    Returns the number of tiles marked for invalidation (0 when the
    participant is inactive, no bbox is derivable, or coverage is empty).
    Never raises out — invalidation must not break a write.
    """
    if not features:
        return 0
    try:
        if not await is_tile_cache_active(catalog_id, collection_id):
            return 0

        tms_ids, min_zoom, max_zoom = await _resolve_tile_extent(
            catalog_id, collection_id,
        )
        coverage = coverage_for_features(
            features, tms_ids, min_zoom, max_zoom, max_tiles=max_tiles,
        )
        if not coverage.tiles:
            return 0

        from dynastore.models.protocols.indexing import OutboxRecord
        from dynastore.modules.storage.driver_instance_id import (
            compute_driver_instance_id,
        )
        from dynastore.tools.identifiers import generate_uuidv7

        if outbox is None:
            from dynastore.modules.storage.index_dispatcher import (
                get_index_dispatcher,
            )
            outbox = get_index_dispatcher()._outbox
        if outbox is None:
            # No OutboxStore wired (cold boot / minimal SCOPE) — skip the
            # enqueue rather than crash; the outer try also degrades safely.
            logger.debug(
                "tile_cache: no OutboxStore available; skipping invalidation "
                "enqueue for %s/%s", catalog_id, collection_id,
            )
            return 0

        inst = compute_driver_instance_id(
            TILE_CACHE_DRIVER_ID, catalog_id, collection_id,
        )
        op_id = generate_uuidv7()
        record = OutboxRecord(
            op_id=op_id,
            driver_id=TILE_CACHE_DRIVER_ID,
            driver_instance_id=inst,
            collection_id=collection_id,
            # Semantically a mark-stale / delete of the cached tiles, not an
            # upsert of an entity. ``delete`` is the accurate op for the drain.
            op="delete",
            item_id=None,
            payload=_encode_tile_payload(coverage.tiles),
            idempotency_key=str(op_id),
        )
        await outbox.enqueue_bulk(conn, catalog_id=catalog_id, rows=[record])
        logger.info(
            "tile_cache: enqueued invalidation of %d tile(s) for %s/%s "
            "(bboxes=%d, clamped=%s)",
            len(coverage.tiles), catalog_id, collection_id,
            coverage.bbox_count, coverage.clamped,
        )
        return len(coverage.tiles)
    except Exception as exc:  # noqa: BLE001 — never break the write
        logger.warning(
            "tile_cache: failed to enqueue invalidation for %s/%s: %s",
            catalog_id, collection_id, exc,
        )
        return 0


# ===========================================================================
# Config reactivity — reconcile storage to the current cache config
# ===========================================================================


async def reconcile_tile_cache(
    catalog_id: str,
    collection_id: str,
) -> Dict[str, Any]:
    """Reconcile the actual tile storage to the live cache config (#1292).

    The cache config is operational (``Mutable``), not a frozen data
    contract, so a config change should take effect immediately and reconcile
    the stored tiles rather than waiting for the next feature write. This is
    that reconcile — the *same* invalidation machinery, triggered by a
    settings change instead of a feature edit:

    * **off → purge**: when the L2 cache is disabled
      (``TilesCachingConfig.cache_enabled == False``), drop every stored tile
      for the collection (it must stop serving stale objects).
    * **zoom-range narrowed**: when the served range shrank, tiles outside the
      new range are orphaned; we purge the whole collection so the next reads
      repopulate only the in-range tiles (a precise per-zoom drop is a
      Phase-3 optimization — purge-and-relazy is correct and cheap to reason
      about).
    * **backend switch (PG ↔ bucket)**: the new backend starts cold and the
      old one is whatever the now-deregistered provider held; the active
      provider is purged so it can't serve stale objects from a prior shape.

    Returns a small report dict. Never raises — a reconcile failure must not
    break a config write or a startup.
    """
    report: Dict[str, Any] = {
        "catalog_id": catalog_id,
        "collection_id": collection_id,
        "action": "noop",
        "purged": 0,
    }
    try:
        from dynastore.modules import get_protocol
        from dynastore.modules.tiles.tiles_module import (
            TileStorageProtocol,
            invalidate_collection_tiles,
        )
        from dynastore.modules.gcp.tiles_storage import _load_caching_config

        provider = get_protocol(TileStorageProtocol)
        if provider is None:
            report["action"] = "no_provider"
            return report

        cfg = await _load_caching_config()
        if not getattr(cfg, "cache_enabled", True):
            # off → purge everything for the collection and stop serving.
            await invalidate_collection_tiles(catalog_id, collection_id)
            report["action"] = "purged_cache_disabled"
            return report

        # Enabled: purge-and-relazy so any prior-shape (wider zoom range,
        # different backend) tiles can't survive. Lazy read-miss repopulates
        # only the tiles that are both in-range and actually requested.
        await invalidate_collection_tiles(catalog_id, collection_id)
        report["action"] = "reconciled"
        return report
    except Exception as exc:  # noqa: BLE001 — reconcile never breaks config write
        logger.warning(
            "tile_cache: reconcile failed for %s/%s: %s",
            catalog_id, collection_id, exc,
        )
        report["action"] = "error"
        report["error"] = str(exc)
        return report
