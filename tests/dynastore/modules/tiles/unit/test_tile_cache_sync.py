"""Unit tests for write-reactive tile-cache invalidation (#1292, Phase 1).

DB-free: exercises the pure coverage/coalesce/cap math, the capability
gate + degrade, the OUTBOX enqueue (with a fake OutboxStore), the drain-side
:class:`TileCacheBulkIndexer` (with a fake provider), and config-reactivity
reconcile (with fakes), without touching Postgres or a bucket.
"""
from __future__ import annotations

from typing import Any, List, Tuple

import pytest

from dynastore.modules.tiles import tile_cache_sync as tcs
from dynastore.modules.tiles.tile_cache_sync import (
    DEFAULT_MAX_TILES_PER_BATCH,
    TILE_CACHE_DRIVER_ID,
    affected_tiles_for_batch,
    clamp_zoom_range_to_cap,
    coverage_for_features,
    decode_tile_payload,
    feature_bbox,
)


# ---------------------------------------------------------------------------
# bbox extraction (no geometry loading)
# ---------------------------------------------------------------------------


def test_feature_bbox_prefers_explicit_bbox():
    f = {"bbox": [12.0, 41.0, 13.0, 42.0], "geometry": None}
    assert feature_bbox(f) == (12.0, 41.0, 13.0, 42.0)


def test_feature_bbox_orders_and_clamps_corners():
    # Inverted corners + out-of-range latitude get normalized/clamped.
    f = {"bbox": [13.0, 90.0, 12.0, -90.0]}
    w, s, e, n = feature_bbox(f)  # type: ignore[misc]
    assert w == 12.0 and e == 13.0
    assert s >= -85.06 and n <= 85.06


def test_feature_bbox_falls_back_to_geometry_bounds():
    f = {
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[0, 0], [2, 0], [2, 3], [0, 3], [0, 0]]],
        },
    }
    assert feature_bbox(f) == (0.0, 0.0, 2.0, 3.0)


def test_feature_bbox_none_when_no_geometry_or_bbox():
    assert feature_bbox({"properties": {}}) is None


def test_feature_bbox_reads_object_attributes():
    class _F:
        bbox = [1.0, 2.0, 3.0, 4.0]
        geometry = None

    assert feature_bbox(_F()) == (1.0, 2.0, 3.0, 4.0)


# ---------------------------------------------------------------------------
# coverage math across a zoom range
# ---------------------------------------------------------------------------


def test_coverage_grows_with_zoom():
    bbox = (12.0, 41.0, 13.0, 42.0)
    z2 = affected_tiles_for_batch([bbox], ["WebMercatorQuad"], 2, 2)
    z6 = affected_tiles_for_batch([bbox], ["WebMercatorQuad"], 6, 6)
    assert len(z6) >= len(z2)
    # Tile coords carry the tms id and zoom.
    for (tms_id, z, _x, _y) in z6:
        assert tms_id == "WebMercatorQuad"
        assert z == 6


def test_coverage_spans_full_zoom_range():
    bbox = (12.0, 41.0, 13.0, 42.0)
    tiles = affected_tiles_for_batch([bbox], ["WebMercatorQuad"], 0, 4)
    zooms = {z for (_t, z, _x, _y) in tiles}
    assert zooms == {0, 1, 2, 3, 4}


def test_empty_inputs_yield_empty_coverage():
    assert affected_tiles_for_batch([], ["WebMercatorQuad"], 0, 6) == set()
    assert affected_tiles_for_batch([(1, 2, 3, 4)], [], 0, 6) == set()


# ---------------------------------------------------------------------------
# per-batch coalescing + dedup
# ---------------------------------------------------------------------------


def test_overlapping_bboxes_coalesce_and_dedup():
    # Two nearly-identical bboxes at low zoom land on the same tile(s).
    a = (12.0, 41.0, 12.1, 41.1)
    b = (12.05, 41.05, 12.15, 41.15)
    one = affected_tiles_for_batch([a], ["WebMercatorQuad"], 3, 3)
    both = affected_tiles_for_batch([a, b], ["WebMercatorQuad"], 3, 3)
    # Coalesced set is a SET — no duplicate tiles even though two bboxes
    # contributed; at z3 these collapse to the same tile.
    assert both == one


def test_disjoint_bboxes_union():
    a = (12.0, 41.0, 13.0, 42.0)
    b = (-100.0, 30.0, -99.0, 31.0)
    union = affected_tiles_for_batch([a, b], ["WebMercatorQuad"], 5, 5)
    only_a = affected_tiles_for_batch([a], ["WebMercatorQuad"], 5, 5)
    only_b = affected_tiles_for_batch([b], ["WebMercatorQuad"], 5, 5)
    assert union == (only_a | only_b)
    assert only_a.isdisjoint(only_b)


def test_coverage_for_features_skips_features_without_bbox():
    feats = [
        {"bbox": [12.0, 41.0, 13.0, 42.0]},
        {"properties": {}},  # no bbox/geometry — skipped, not an error
    ]
    cov = coverage_for_features(feats, ["WebMercatorQuad"], 4, 4)
    assert cov.bbox_count == 1
    assert len(cov.tiles) >= 1


# ---------------------------------------------------------------------------
# fan-out cap for large bboxes
# ---------------------------------------------------------------------------


def test_cap_clamps_world_bbox():
    world = {"bbox": [-180.0, -85.0, 180.0, 85.0]}
    cov = coverage_for_features(
        [world], ["WebMercatorQuad"], 0, 12, max_tiles=100,
    )
    assert cov.clamped is True
    assert len(cov.tiles) <= 100


def test_cap_keeps_small_bbox_full_range():
    small = {"bbox": [12.0, 41.0, 12.01, 41.01]}
    cov = coverage_for_features(
        [small], ["WebMercatorQuad"], 0, 8, max_tiles=DEFAULT_MAX_TILES_PER_BATCH,
    )
    assert cov.clamped is False
    zooms = {z for (_t, z, _x, _y) in cov.tiles}
    assert max(zooms) == 8


def test_clamp_returns_at_least_min_zoom():
    world = (-180.0, -85.0, 180.0, 85.0)
    # Even an absurdly small cap keeps the coarsest zoom.
    z = clamp_zoom_range_to_cap([world], ["WebMercatorQuad"], 0, 10, max_tiles=1)
    assert z == 0


def test_clamp_monotonic_in_cap():
    world = (-180.0, -85.0, 180.0, 85.0)
    low = clamp_zoom_range_to_cap([world], ["WebMercatorQuad"], 0, 10, max_tiles=50)
    high = clamp_zoom_range_to_cap([world], ["WebMercatorQuad"], 0, 10, max_tiles=5000)
    assert high >= low


# ---------------------------------------------------------------------------
# payload encode/decode round-trip
# ---------------------------------------------------------------------------


def test_payload_round_trip():
    tiles = {("WebMercatorQuad", 3, 4, 5), ("WebMercatorQuad", 6, 7, 8)}
    payload = tcs._encode_tile_payload(tiles)
    decoded = set(decode_tile_payload(payload))
    assert decoded == tiles


def test_decode_skips_malformed_entries():
    payload = {"tiles": [{"tms_id": "X", "z": 1, "x": 2, "y": 3}, {"bad": True}]}
    assert decode_tile_payload(payload) == [("X", 1, 2, 3)]


# ===========================================================================
# Capability gate + degrade
# ===========================================================================


class _FakeProvider:
    """Fake reader/store implementing the coordinate-variant primitive (#1292).

    Records each coordinate-variant delete (all served formats in one call).
    """

    def __init__(self, *, exists_raises: bool = False) -> None:
        self.exists_raises = exists_raises
        self.deleted: List[Tuple] = []          # per-format delete_tile calls
        self.variant_calls: List[Tuple] = []    # delete_tile_variants calls

    async def check_tile_exists(self, *args, **kwargs) -> bool:
        if self.exists_raises:
            raise RuntimeError("bucket unreachable")
        return False

    async def delete_tile(self, catalog_id, collection_id, tms_id, z, x, y, fmt) -> bool:
        self.deleted.append((catalog_id, collection_id, tms_id, z, x, y, fmt))
        return True

    async def delete_tile_variants(
        self, catalog_id, collection_id, tms_id, z, x, y, formats,
    ) -> bool:
        self.variant_calls.append(
            (catalog_id, collection_id, tms_id, z, x, y, tuple(formats))
        )
        return True

    async def delete_tiles_for_collection(self, catalog_id, collection_id) -> int:
        return 0


class _LegacyProvider:
    """Provider WITHOUT the variant primitive — exercises the drain fallback."""

    def __init__(self) -> None:
        self.deleted: List[Tuple] = []

    async def delete_tile(self, catalog_id, collection_id, tms_id, z, x, y, fmt) -> bool:
        self.deleted.append((catalog_id, collection_id, tms_id, z, x, y, fmt))
        return True


@pytest.mark.asyncio
async def test_gate_off_when_no_provider(monkeypatch):
    monkeypatch.setattr(tcs, "get_protocol", lambda *_a, **_k: None, raising=False)
    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: None)
    assert await tcs.is_tile_cache_active("cat", "col") is False


@pytest.mark.asyncio
async def test_gate_on_when_provider_usable(monkeypatch):
    provider = _FakeProvider()
    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: provider)
    assert await tcs.is_tile_cache_active("cat", "col") is True


@pytest.mark.asyncio
async def test_gate_degrades_when_store_unusable(monkeypatch):
    provider = _FakeProvider(exists_raises=True)
    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: provider)
    # Store probe raises → degrade to disabled (warn + False), never raise.
    assert await tcs.is_tile_cache_active("cat", "col") is False


# ===========================================================================
# Enqueue side (OUTBOX, atomic) — with a fake OutboxStore
# ===========================================================================


class _FakeOutbox:
    def __init__(self) -> None:
        self.rows: List[Any] = []
        self.conns: List[Any] = []

    async def enqueue_bulk(self, conn=None, *, catalog_id: str, rows) -> None:
        self.conns.append(conn)
        self.rows.extend(rows)


@pytest.mark.asyncio
async def test_enqueue_builds_single_coalesced_row(monkeypatch):
    provider = _FakeProvider()
    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: provider)

    async def _extent(_cat, _col):
        return ["WebMercatorQuad"], 0, 4

    monkeypatch.setattr(tcs, "_resolve_tile_extent", _extent)

    outbox = _FakeOutbox()
    feats = [
        {"id": "a", "bbox": [12.0, 41.0, 13.0, 42.0]},
        {"id": "b", "bbox": [12.5, 41.5, 13.5, 42.5]},
    ]
    # Phase 1: the write is already committed, so the hook passes conn=None —
    # PgOutboxStore acquires its own raw asyncpg conn and sets search_path.
    n = await tcs.enqueue_tile_invalidations(
        None, "cat", "col", feats, outbox=outbox,
    )
    assert n > 0
    # ONE coalesced outbox row for the whole batch — not one per feature/tile.
    assert len(outbox.rows) == 1
    rec = outbox.rows[0]
    assert rec.driver_id == TILE_CACHE_DRIVER_ID
    assert rec.collection_id == "col"
    # Semantically a mark-stale / delete, not an entity upsert.
    assert rec.op == "delete"
    # Self-managed enqueue: forwarded conn is None (PgOutboxStore owns it).
    assert outbox.conns == [None]
    # Payload carries the coalesced tile set.
    decoded = decode_tile_payload(rec.payload)
    assert len(decoded) == n


@pytest.mark.asyncio
async def test_enqueue_noop_when_inactive(monkeypatch):
    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: None)
    outbox = _FakeOutbox()
    n = await tcs.enqueue_tile_invalidations(
        object(), "cat", "col", [{"id": "a", "bbox": [0, 0, 1, 1]}], outbox=outbox,
    )
    assert n == 0
    assert outbox.rows == []


@pytest.mark.asyncio
async def test_enqueue_never_raises_on_outbox_error(monkeypatch):
    provider = _FakeProvider()
    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: provider)

    async def _extent(_cat, _col):
        return ["WebMercatorQuad"], 0, 2

    monkeypatch.setattr(tcs, "_resolve_tile_extent", _extent)

    class _BoomOutbox:
        async def enqueue_bulk(self, *a, **k):
            raise RuntimeError("outbox down")

    # The enqueue swallows the error and returns 0 (write must not break).
    n = await tcs.enqueue_tile_invalidations(
        object(), "cat", "col", [{"id": "a", "bbox": [12, 41, 13, 42]}],
        outbox=_BoomOutbox(),
    )
    assert n == 0


# ===========================================================================
# Contract test against the REAL PgOutboxStore (no MagicMock outbox)
#
# The blocker passed unit tests precisely because the outbox was a fake. These
# drive the genuine ``PgOutboxStore.enqueue_bulk`` method/shape so a regression
# (passing a non-None SQLAlchemy conn, or a wrong column/record shape) fails
# here instead of silently in prod.
# ===========================================================================


class _FakeRawConn:
    """Stand-in for a raw asyncpg connection.

    Implements ``copy_records_to_table`` (the asyncpg bulk-COPY method
    ``PgOutboxStore.enqueue_bulk`` requires) and ``execute`` (used by
    ``_ensure_search_path`` on pool conns). A SQLAlchemy ``AsyncConnection`` has
    NEITHER — which is exactly why the blocker silently failed when handed one.
    """

    def __init__(self) -> None:
        self.copied: List[Any] = []
        self.executed: List[str] = []

    async def copy_records_to_table(self, table, *, records, columns):
        self.copied.append((table, list(records), list(columns)))

    async def execute(self, sql, *args):
        self.executed.append(sql)


class _SqlAlchemyLikeConn:
    """A conn WITHOUT ``copy_records_to_table`` — like a SQLAlchemy conn.

    Used to prove that handing such a conn to ``PgOutboxStore.enqueue_bulk``
    blows up (AttributeError) — i.e. the original blocker. The hook must never
    do this; it passes conn=None.
    """

    async def execute(self, sql, *args):
        return None


@pytest.mark.asyncio
async def test_real_pg_outbox_enqueue_with_conn_none_uses_own_conn():
    """conn=None → store acquires its OWN raw conn and COPYs the single row."""
    from dynastore.modules.storage.pg_outbox import PgOutboxStore

    raw = _FakeRawConn()
    store = PgOutboxStore(single_conn=raw)

    provider = _FakeProvider()
    import dynastore.modules as _mods
    import importlib

    tile_cache_sync = importlib.import_module(
        "dynastore.modules.tiles.tile_cache_sync"
    )
    # Resolve the provider + a fixed extent without DB.
    feats = [{"id": "a", "bbox": [12.0, 41.0, 13.0, 42.0]}]
    import pytest as _pytest  # noqa: F401  (kept for symmetry)

    async def _extent(_cat, _col):
        return ["WebMercatorQuad"], 0, 3

    # monkeypatch via setattr on the module objects directly (no fixture here).
    _orig_get = _mods.get_protocol
    _orig_extent = tile_cache_sync._resolve_tile_extent
    _mods.get_protocol = lambda *_a, **_k: provider  # type: ignore[assignment]
    tile_cache_sync._resolve_tile_extent = _extent  # type: ignore[assignment]
    try:
        n = await tile_cache_sync.enqueue_tile_invalidations(
            None, "cat", "col", feats, outbox=store,
        )
    finally:
        _mods.get_protocol = _orig_get  # type: ignore[assignment]
        tile_cache_sync._resolve_tile_extent = _orig_extent  # type: ignore[assignment]

    assert n > 0
    # Exactly one COPY into storage_outbox with the canonical column list.
    assert len(raw.copied) == 1
    table, records, columns = raw.copied[0]
    assert table == "storage_outbox"
    assert columns == [
        "op_id", "driver_id", "driver_instance_id", "collection_id",
        "op", "item_id", "payload", "idempotency_key",
    ]
    # ONE coalesced row; op is the delete/mark-stale op; driver pinned.
    assert len(records) == 1
    row = records[0]
    # Column order: (op_id, driver_id, driver_instance_id, collection_id,
    #                op, item_id, payload, idempotency_key)
    assert row[1] == "tile_cache_invalidator"
    assert row[3] == "col"
    assert row[4] == "delete"
    assert row[5] is None  # item_id


@pytest.mark.asyncio
async def test_real_pg_outbox_enqueue_rejects_sqlalchemy_like_conn():
    """A non-None SQLAlchemy-style conn has no copy_records_to_table → raises.

    This is the blocker's failure mode. The hook must pass conn=None; if a
    future change reintroduces a wrapping TX and hands a SQLAlchemy conn, the
    real store will raise here. (``enqueue_tile_invalidations`` swallows it and
    returns 0 — never breaking the write — but a coverage of 0 is the symptom.)
    """
    from dynastore.modules.storage.pg_outbox import PgOutboxStore
    from dynastore.models.protocols.indexing import OutboxRecord
    from dynastore.tools.identifiers import generate_uuidv7

    bad_conn = _SqlAlchemyLikeConn()
    store = PgOutboxStore(single_conn=object())  # store's own conn unused here
    op_id = generate_uuidv7()
    rec = OutboxRecord(
        op_id=op_id,
        driver_id="tile_cache_invalidator",
        driver_instance_id="inst",
        collection_id="col",
        op="delete",
        item_id=None,
        payload={"tiles": []},
        idempotency_key=str(op_id),
    )
    # Passing a non-None conn that lacks copy_records_to_table must raise —
    # exactly the silent blocker if it reaches the swallowing caller.
    with pytest.raises(AttributeError):
        await store.enqueue_bulk(bad_conn, catalog_id="cat", rows=[rec])


# ===========================================================================
# Drain-side TileCacheBulkIndexer — with a fake provider
# ===========================================================================


def _make_op(tiles, *, op_id=None, catalog="cat", collection="col"):
    from uuid import uuid4

    from dynastore.models.protocols.indexing import IndexableOp

    return IndexableOp(
        op_id=op_id or uuid4(),
        op="upsert",
        catalog_id=catalog,
        collection_id=collection,
        driver_instance_id="inst",
        item_id=None,
        payload=tcs._encode_tile_payload(set(tiles)),
        idempotency_key="k",
    )


@pytest.mark.asyncio
async def test_indexer_deletes_tiles_and_passes():
    from dynastore.tasks.outbox_drain.tile_cache_indexer_adapter import (
        TileCacheBulkIndexer,
    )

    provider = _FakeProvider()
    indexer = TileCacheBulkIndexer(provider=provider)
    tiles = {("WebMercatorQuad", 3, 4, 5), ("WebMercatorQuad", 4, 8, 10)}
    op = _make_op(tiles)
    result = await indexer.index_bulk([op])
    assert result.passed == [op.op_id]
    assert result.transient == [] and result.poison == []
    # One coordinate-variant delete per tile (each drops all served formats +
    # all cache-id variants in a single call).
    assert len(provider.variant_calls) == 2


@pytest.mark.asyncio
async def test_indexer_invalidates_all_served_formats():
    """Drain must drop BOTH mvt and pbf for each coordinate (#1292 SHOULD-FIX 2).

    The read/render path caches both formats; invalidating only mvt would leave
    a stale pbf. The variant primitive receives the full served-format set.
    """
    from dynastore.tasks.outbox_drain.tile_cache_indexer_adapter import (
        TileCacheBulkIndexer,
    )
    from dynastore.modules.tiles.tile_cache_sync import SERVED_TILE_FORMATS

    assert set(SERVED_TILE_FORMATS) == {"mvt", "pbf"}

    provider = _FakeProvider()
    indexer = TileCacheBulkIndexer(provider=provider)
    op = _make_op({("WebMercatorQuad", 5, 1, 2)})
    result = await indexer.index_bulk([op])
    assert result.passed == [op.op_id]
    assert len(provider.variant_calls) == 1
    (_cat, _col, _tms, _z, _x, _y, formats) = provider.variant_calls[0]
    # Every served format is invalidated for the coordinate.
    assert set(formats) == set(SERVED_TILE_FORMATS)


@pytest.mark.asyncio
async def test_indexer_legacy_fallback_deletes_each_format():
    """A provider without the variant primitive still drops every served format.

    The legacy fallback can only match the bare collection id (not @hash /
    multi-collection variants), but it MUST at least iterate all served formats.
    """
    from dynastore.tasks.outbox_drain.tile_cache_indexer_adapter import (
        TileCacheBulkIndexer,
    )
    from dynastore.modules.tiles.tile_cache_sync import SERVED_TILE_FORMATS

    provider = _LegacyProvider()
    indexer = TileCacheBulkIndexer(provider=provider)
    op = _make_op({("WebMercatorQuad", 5, 1, 2)})
    result = await indexer.index_bulk([op])
    assert result.passed == [op.op_id]
    # One delete_tile per served format for the single coordinate.
    deleted_formats = {d[6] for d in provider.deleted}
    assert deleted_formats == set(SERVED_TILE_FORMATS)
    assert len(provider.deleted) == len(SERVED_TILE_FORMATS)


@pytest.mark.asyncio
async def test_indexer_idempotent_on_absent_tiles():
    # delete_tile returning True for an absent tile → still passes (mark-stale
    # is idempotent / order-tolerant).
    from dynastore.tasks.outbox_drain.tile_cache_indexer_adapter import (
        TileCacheBulkIndexer,
    )

    class _AbsentProvider(_FakeProvider):
        async def delete_tile_variants(self, *a, **k) -> bool:
            return True  # absent tile delete = no-op success

    indexer = TileCacheBulkIndexer(provider=_AbsentProvider())
    op = _make_op({("WebMercatorQuad", 3, 4, 5)})
    result = await indexer.index_bulk([op])
    assert result.passed == [op.op_id]


@pytest.mark.asyncio
async def test_indexer_retries_when_no_provider(monkeypatch):
    from dynastore.tasks.outbox_drain.tile_cache_indexer_adapter import (
        TileCacheBulkIndexer,
    )

    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: None)
    indexer = TileCacheBulkIndexer(provider=None)
    op = _make_op({("WebMercatorQuad", 3, 4, 5)})
    result = await indexer.index_bulk([op])
    # Whole op → transient (retry); the store may come back.
    assert [oid for oid, _r in result.transient] == [op.op_id]
    assert result.passed == []


@pytest.mark.asyncio
async def test_indexer_retries_on_store_error():
    from dynastore.tasks.outbox_drain.tile_cache_indexer_adapter import (
        TileCacheBulkIndexer,
    )

    class _BoomProvider(_FakeProvider):
        async def delete_tile_variants(self, *a, **k) -> bool:
            raise RuntimeError("store hiccup")

    indexer = TileCacheBulkIndexer(provider=_BoomProvider())
    op = _make_op({("WebMercatorQuad", 3, 4, 5)})
    result = await indexer.index_bulk([op])
    assert [oid for oid, _r in result.transient] == [op.op_id]


@pytest.mark.asyncio
async def test_indexer_passes_empty_payload():
    from dynastore.tasks.outbox_drain.tile_cache_indexer_adapter import (
        TileCacheBulkIndexer,
    )

    from dynastore.models.protocols.indexing import IndexableOp
    from uuid import uuid4

    provider = _FakeProvider()
    indexer = TileCacheBulkIndexer(provider=provider)
    op = IndexableOp(
        op_id=uuid4(), op="upsert", catalog_id="cat", collection_id="col",
        driver_instance_id="inst", item_id=None, payload={"tiles": []},
        idempotency_key="k",
    )
    result = await indexer.index_bulk([op])
    assert result.passed == [op.op_id]
    assert provider.deleted == []


# ===========================================================================
# Config reactivity — reconcile
# ===========================================================================


@pytest.mark.asyncio
async def test_reconcile_purges_when_cache_disabled(monkeypatch):
    provider = _FakeProvider()
    purged: List[Tuple[str, str]] = []

    async def _invalidate(cat, col):
        purged.append((cat, col))

    async def _load_cfg():
        class _Cfg:
            cache_enabled = False
        return _Cfg()

    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: provider)
    import dynastore.modules.tiles.tiles_module as _tm
    monkeypatch.setattr(_tm, "invalidate_collection_tiles", _invalidate)
    import dynastore.modules.gcp.tiles_storage as _gcs
    monkeypatch.setattr(_gcs, "_load_caching_config", _load_cfg)

    report = await tcs.reconcile_tile_cache("cat", "col")
    assert report["action"] == "purged_cache_disabled"
    assert purged == [("cat", "col")]


@pytest.mark.asyncio
async def test_reconcile_reconciles_when_enabled(monkeypatch):
    provider = _FakeProvider()
    purged: List[Tuple[str, str]] = []

    async def _invalidate(cat, col):
        purged.append((cat, col))

    async def _load_cfg():
        class _Cfg:
            cache_enabled = True
        return _Cfg()

    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: provider)
    import dynastore.modules.tiles.tiles_module as _tm
    monkeypatch.setattr(_tm, "invalidate_collection_tiles", _invalidate)
    import dynastore.modules.gcp.tiles_storage as _gcs
    monkeypatch.setattr(_gcs, "_load_caching_config", _load_cfg)

    report = await tcs.reconcile_tile_cache("cat", "col")
    assert report["action"] == "reconciled"
    assert purged == [("cat", "col")]


@pytest.mark.asyncio
async def test_reconcile_noop_without_provider(monkeypatch):
    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: None)
    report = await tcs.reconcile_tile_cache("cat", "col")
    assert report["action"] == "no_provider"
