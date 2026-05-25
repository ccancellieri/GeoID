"""Unit tests for write-reactive tile-cache invalidation (#1292; #1298 enqueue).

DB-free: exercises the pure coverage/coalesce/cap math, the capability
gate + degrade, the ``tiles_preseed`` invalidate-task enqueue (driving the
REAL ``TaskCreate``/``create_task`` shape so an envelope bug fails here rather
than silently in prod), and config-reactivity reconcile (with fakes), without
touching Postgres or a bucket.
"""
from __future__ import annotations

import json
from typing import Any, List, Tuple

import pytest

from dynastore.modules.tiles import tile_cache_sync as tcs
from dynastore.modules.tiles.tile_cache_sync import (
    DEFAULT_MAX_TILES_PER_BATCH,
    affected_tiles_for_batch,
    clamp_zoom_range_to_cap,
    coverage_for_features,
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


# ===========================================================================
# Capability gate + degrade
# ===========================================================================


class _FakeProvider:
    """Fake reader/store implementing the coordinate-variant primitive (#1292).

    Method signatures match the real ``TileStorageProtocol`` so a green run
    here is genuine verification, not a shape-mismatch false positive.
    """

    def __init__(self, *, exists_raises: bool = False) -> None:
        self.exists_raises = exists_raises
        self.deleted: List[Tuple] = []          # per-format delete_tile calls
        self.variant_calls: List[Tuple] = []    # delete_tile_variants calls

    async def check_tile_exists(self, catalog_id, collection_id, tms_id, z, x, y, format) -> bool:
        if self.exists_raises:
            raise RuntimeError("bucket unreachable")
        return False

    async def delete_tile(self, catalog_id, collection_id, tms_id, z, x, y, format) -> bool:
        self.deleted.append((catalog_id, collection_id, tms_id, z, x, y, format))
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
# Enqueue side — enqueue a tiles_preseed `operation=invalidate` task (#1298).
#
# These drive the REAL TaskCreate / create_task input shaping. A fake DB layer
# (managed_transaction + DQLQuery) lets the genuine create_task build its INSERT
# so we can assert the stored `inputs` is the OGC ExecuteRequest envelope the
# preseed task consumes (payload.inputs.inputs). Mocking create_task into a
# no-op would hide an envelope bug — exactly the Phase-1 lesson.
# ===========================================================================


class _FakeRowResult:
    """A minimal row dict that Task.model_validate can accept for create_task."""


def _install_fake_create_task_db(
    monkeypatch, *, captured: dict, dedup_existing: bool = False, store: dict | None = None,
):
    """Patch the DB primitives create_task uses so it runs without Postgres.

    Captures the LAST INSERT's bind kwargs (including the JSON-serialized
    `inputs`) into ``captured`` so a test can assert the real envelope shape.

    Dedup is modeled faithfully against the REAL dedup_key the production code
    builds: the pre-check SELECT consults a per-(schema_name, dedup_key) ``store``
    of already-inserted non-terminal tasks and returns a row when one exists, so
    ``create_task`` short-circuits to None exactly as it would in Postgres. Pass
    a shared ``store`` dict across two enqueues to exercise the real coalesce
    (identical key) vs. distinct-key (no coalesce) behaviour. ``dedup_existing``
    pre-seeds the store as a wildcard so any first enqueue is treated as a hit.
    """
    import dynastore.modules.tasks.tasks_module as tm

    if store is None:
        store = {}
    # When dedup_existing is requested, every dedup pre-check should hit.
    seeded_wildcard = {"value": dedup_existing}

    class _FakeConn:
        pass

    class _FakeTxCtx:
        async def __aenter__(self):
            return _FakeConn()

        async def __aexit__(self, *exc):
            return False

    def _fake_managed_transaction(_engine):
        return _FakeTxCtx()

    from dynastore.modules.db_config.query_executor import ResultHandler

    class _FakeQuery:
        def __init__(self, sql, *, result_handler=None):
            self.sql = sql
            self.result_handler = result_handler

        async def execute(self, conn, **kwargs):
            sql_l = self.sql.strip().upper()
            if sql_l.startswith("SELECT"):
                # dedup pre-check, against the REAL (schema_name, dedup_key).
                if seeded_wildcard["value"]:
                    return {"task_id": "existing"}
                key = (kwargs.get("schema_name"), kwargs.get("dedup_key"))
                if key in store:
                    return {"task_id": store[key]}
                return None
            # INSERT ... RETURNING * — capture binds, register the dedup key so a
            # later identical-key SELECT coalesces, and return a full task row.
            captured.clear()
            captured.update(kwargs)
            from datetime import datetime, timezone

            key = (kwargs.get("schema_name"), kwargs.get("dedup_key"))
            store[key] = kwargs["task_id"]

            # The real PG JSONB column hands back a dict on read (not the
            # serialized string the INSERT bind carried). Mirror that so
            # Task.model_validate accepts the row, as it would in prod.
            stored_inputs = kwargs["inputs"]
            if isinstance(stored_inputs, str):
                stored_inputs = json.loads(stored_inputs)
            return {
                "task_id": kwargs["task_id"],
                "schema_name": kwargs["schema_name"],
                "scope": kwargs["scope"],
                "caller_id": kwargs["caller_id"],
                "task_type": kwargs["task_type"],
                "type": kwargs["type"],
                "execution_mode": kwargs["execution_mode"],
                "inputs": stored_inputs,
                "timestamp": kwargs["timestamp"],
                "collection_id": kwargs["collection_id"],
                "dedup_key": kwargs["dedup_key"],
                "status": kwargs["status"],
                "created_at": datetime.now(timezone.utc),
            }

    monkeypatch.setattr(tm, "managed_transaction", _fake_managed_transaction)
    monkeypatch.setattr(tm, "DQLQuery", _FakeQuery)
    monkeypatch.setattr(tm, "ResultHandler", ResultHandler, raising=False)
    # get_task cache invalidate is a no-op-friendly call on a fake conn.
    monkeypatch.setattr(tm.get_task, "cache_invalidate", lambda *a, **k: None, raising=False)
    return store


@pytest.mark.asyncio
async def test_enqueue_creates_invalidate_task_with_execute_request_envelope(monkeypatch):
    provider = _FakeProvider()
    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: provider)

    captured: dict = {}
    _install_fake_create_task_db(monkeypatch, captured=captured)

    feats = [
        {"id": "a", "bbox": [12.0, 41.0, 13.0, 42.0]},
        {"id": "b", "bbox": [12.5, 41.5, 13.5, 42.5]},
    ]
    n = await tcs.enqueue_tile_invalidation_task(
        "cat", "col", feats, engine=object(), schema="s_cat",
    )
    # Two distinct bboxes enqueued.
    assert n == 2

    # The task row was created as a `tiles_preseed` PENDING task for the
    # collection, with the per-collection dedup key.
    assert captured["task_type"] == "tiles_preseed"
    assert captured["status"] == "PENDING"
    assert captured["schema_name"] == "s_cat"
    assert captured["collection_id"] == "col"
    # dedup_key is per-collection + a coverage signature so only identical-
    # coverage edits coalesce; distinct bboxes get distinct keys.
    assert captured["dedup_key"].startswith("tile-invalidate:cat:col:")

    # The stored `inputs` MUST be the OGC ExecuteRequest envelope the preseed
    # task unwraps via payload.inputs.inputs. Round-trip it back through the
    # REAL models the dispatcher uses (ExecuteRequest, then TilePreseedRequest).
    stored = json.loads(captured["inputs"])
    from dynastore.modules.processes.models import ExecuteRequest
    from dynastore.tasks.tiles_preseed.models import TilePreseedRequest

    exec_req = ExecuteRequest(**stored)
    inner = exec_req.inputs  # this is what payload.inputs.inputs yields
    assert inner["operation"] == "invalidate"
    assert inner["catalog_id"] == "cat"
    assert inner["collection_id"] == "col"
    assert len(inner["update_bbox"]) == 2

    # And it validates as the request the task actually consumes.
    req = TilePreseedRequest.model_validate(inner)
    assert req.operation == "invalidate"
    assert req.catalog_id == "cat"
    assert req.collection_id == "col"
    assert len(req.update_bbox) == 2


@pytest.mark.asyncio
async def test_enqueue_collapses_duplicate_bboxes_within_batch(monkeypatch):
    provider = _FakeProvider()
    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: provider)

    captured: dict = {}
    _install_fake_create_task_db(monkeypatch, captured=captured)

    # Two identical bboxes in one batch collapse to one update_bbox entry.
    feats = [
        {"id": "a", "bbox": [12.0, 41.0, 13.0, 42.0]},
        {"id": "b", "bbox": [12.0, 41.0, 13.0, 42.0]},
    ]
    n = await tcs.enqueue_tile_invalidation_task(
        "cat", "col", feats, engine=object(), schema="s_cat",
    )
    assert n == 1
    stored = json.loads(captured["inputs"])
    assert len(stored["inputs"]["update_bbox"]) == 1


@pytest.mark.asyncio
async def test_enqueue_coalesces_identical_coverage(monkeypatch):
    """Two enqueues with the SAME bboxes coalesce: the second is a dedup hit.

    Identical coverage is the SAFE coalescing case — the first task already
    invalidates that exact area, so the second enqueue correctly returns 0 once
    its coverage-signature dedup_key matches the still-pending first task.
    """
    provider = _FakeProvider()
    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: provider)

    captured: dict = {}
    store = _install_fake_create_task_db(monkeypatch, captured=captured)

    feats = [{"id": "a", "bbox": [12.0, 41.0, 13.0, 42.0]}]

    first = await tcs.enqueue_tile_invalidation_task(
        "cat", "col", feats, engine=object(), schema="s_cat",
    )
    first_key = captured["dedup_key"]
    assert first == 1

    # Same coverage again, first task still non-terminal (registered in store).
    second = await tcs.enqueue_tile_invalidation_task(
        "cat", "col", feats, engine=object(), schema="s_cat",
    )
    # Coalesced — identical signature → dedup hit → 0.
    assert second == 0
    # Only one task got created (store has a single entry for this key).
    assert (("s_cat", first_key)) in store
    assert len(store) == 1


@pytest.mark.asyncio
async def test_enqueue_distinct_bboxes_both_enqueue_no_coverage_lost(monkeypatch):
    """C1 regression: two non-terminal enqueues at DISJOINT bboxes BOTH enqueue.

    The original bug used a static per-collection dedup_key, so batch B at a
    disjoint bbox Y was deduped against batch A's still-pending task (carrying
    X) and DROPPED — Y's tiles were never invalidated. The coverage-signature
    dedup_key fixes this: B's signature differs from A's, so create_task is NOT
    deduped and B's own invalidate task is created carrying Y's bbox.
    """
    provider = _FakeProvider()
    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: provider)

    captured: dict = {}
    store = _install_fake_create_task_db(monkeypatch, captured=captured)

    bbox_x = [12.0, 41.0, 13.0, 42.0]
    bbox_y = [-100.0, 30.0, -99.0, 31.0]  # disjoint from X

    # Batch A enqueues at X (task stays non-terminal in the store).
    n_a = await tcs.enqueue_tile_invalidation_task(
        "cat", "col", [{"id": "a", "bbox": bbox_x}], engine=object(), schema="s_cat",
    )
    key_a = captured["dedup_key"]
    assert n_a == 1

    # Batch B enqueues at the DISJOINT bbox Y while A is still pending.
    n_b = await tcs.enqueue_tile_invalidation_task(
        "cat", "col", [{"id": "b", "bbox": bbox_y}], engine=object(), schema="s_cat",
    )
    key_b = captured["dedup_key"]

    # B must NOT be deduped — its tiles must be invalidated.
    assert n_b > 0
    # Distinct signatures → two separate tasks queued.
    assert key_a != key_b
    assert len(store) == 2

    # The SECOND task carries Y's bbox in its invalidate envelope, so Y's tiles
    # will be invalidated (the whole point of the fix).
    stored = json.loads(captured["inputs"])
    from dynastore.modules.processes.models import ExecuteRequest
    from dynastore.tasks.tiles_preseed.models import TilePreseedRequest

    inner = ExecuteRequest(**stored).inputs
    assert inner["operation"] == "invalidate"
    req = TilePreseedRequest.model_validate(inner)
    assert req.operation == "invalidate"
    assert [list(b) for b in req.update_bbox] == [bbox_y]


@pytest.mark.asyncio
async def test_enqueue_prior_bbox_only_invalidates_old_footprint(monkeypatch):
    """Phase 2 (#1297) DELETE case: no new feature, only a prior bbox.

    A delete passes ``features=[]`` and the deleted item's extent as
    ``prior_bboxes``; the invalidate task must carry that extent so the tiles
    it used to occupy are dropped.
    """
    provider = _FakeProvider()
    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: provider)

    captured: dict = {}
    _install_fake_create_task_db(monkeypatch, captured=captured)

    prior = (12.0, 41.0, 13.0, 42.0)
    n = await tcs.enqueue_tile_invalidation_task(
        "cat", "col", [], engine=object(), schema="s_cat",
        prior_bboxes=[prior],
    )
    assert n == 1
    stored = json.loads(captured["inputs"])
    from dynastore.modules.processes.models import ExecuteRequest

    inner = ExecuteRequest(**stored).inputs
    assert inner["operation"] == "invalidate"
    assert [list(b) for b in inner["update_bbox"]] == [list(prior)]


@pytest.mark.asyncio
async def test_enqueue_unions_new_and_prior_bboxes(monkeypatch):
    """Phase 2 geometry-MOVE shape: new and prior extents both invalidate."""
    provider = _FakeProvider()
    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: provider)

    captured: dict = {}
    _install_fake_create_task_db(monkeypatch, captured=captured)

    new_bbox = [20.0, 50.0, 21.0, 51.0]
    prior = (12.0, 41.0, 13.0, 42.0)
    n = await tcs.enqueue_tile_invalidation_task(
        "cat", "col", [{"id": "a", "bbox": new_bbox}], engine=object(),
        schema="s_cat", prior_bboxes=[prior],
    )
    assert n == 2
    stored = json.loads(captured["inputs"])
    from dynastore.modules.processes.models import ExecuteRequest

    got = {tuple(b) for b in ExecuteRequest(**stored).inputs["update_bbox"]}
    assert got == {tuple(new_bbox), prior}


@pytest.mark.asyncio
async def test_enqueue_dedups_prior_bbox_equal_to_new(monkeypatch):
    """A prior bbox identical to the new bbox collapses to one entry."""
    provider = _FakeProvider()
    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: provider)

    captured: dict = {}
    _install_fake_create_task_db(monkeypatch, captured=captured)

    same = [12.0, 41.0, 13.0, 42.0]
    n = await tcs.enqueue_tile_invalidation_task(
        "cat", "col", [{"id": "a", "bbox": same}], engine=object(),
        schema="s_cat", prior_bboxes=[tuple(same)],
    )
    assert n == 1
    stored = json.loads(captured["inputs"])
    from dynastore.modules.processes.models import ExecuteRequest

    assert len(ExecuteRequest(**stored).inputs["update_bbox"]) == 1


@pytest.mark.asyncio
async def test_enqueue_noop_when_features_and_prior_empty(monkeypatch):
    """No new features and no prior bboxes → nothing enqueued (no gate call)."""
    captured: dict = {}
    store = _install_fake_create_task_db(monkeypatch, captured=captured)

    n = await tcs.enqueue_tile_invalidation_task(
        "cat", "col", [], engine=object(), schema="s_cat", prior_bboxes=[],
    )
    assert n == 0
    assert store == {}


def test_coverage_signature_matches_for_identical_differs_for_distinct():
    """The dedup signature is stable for identical coverage, distinct otherwise."""
    x = [[12.0, 41.0, 13.0, 42.0]]
    y = [[-100.0, 30.0, -99.0, 31.0]]

    # Identical coverage → identical signature (order- and noise-stable).
    assert tcs._coverage_signature(x) == tcs._coverage_signature(list(x))
    # Float-repr noise within precision collapses to the same signature.
    x_noisy = [[12.0000000001, 41.0, 13.0, 42.0]]
    assert tcs._coverage_signature(x) == tcs._coverage_signature(x_noisy)
    # Order independence: same set of bboxes in different order → same signature.
    multi_ab = [[12.0, 41.0, 13.0, 42.0], [-100.0, 30.0, -99.0, 31.0]]
    multi_ba = [[-100.0, 30.0, -99.0, 31.0], [12.0, 41.0, 13.0, 42.0]]
    assert tcs._coverage_signature(multi_ab) == tcs._coverage_signature(multi_ba)
    # Distinct coverage → distinct signature.
    assert tcs._coverage_signature(x) != tcs._coverage_signature(y)


@pytest.mark.asyncio
async def test_enqueue_noop_when_inactive(monkeypatch):
    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: None)

    created: List[Any] = []

    async def _boom_create(*a, **k):
        created.append((a, k))
        raise AssertionError("create_task must not be called when inactive")

    import dynastore.modules.tasks.tasks_module as tm
    monkeypatch.setattr(tm, "create_task", _boom_create)

    n = await tcs.enqueue_tile_invalidation_task(
        "cat", "col", [{"id": "a", "bbox": [0, 0, 1, 1]}],
        engine=object(), schema="s_cat",
    )
    assert n == 0
    assert created == []


@pytest.mark.asyncio
async def test_enqueue_noop_when_no_bbox(monkeypatch):
    provider = _FakeProvider()
    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: provider)

    async def _boom_create(*a, **k):
        raise AssertionError("create_task must not be called with no bbox")

    import dynastore.modules.tasks.tasks_module as tm
    monkeypatch.setattr(tm, "create_task", _boom_create)

    n = await tcs.enqueue_tile_invalidation_task(
        "cat", "col", [{"id": "a", "properties": {}}],  # no bbox/geometry
        engine=object(), schema="s_cat",
    )
    assert n == 0


@pytest.mark.asyncio
async def test_enqueue_never_raises_on_create_error(monkeypatch):
    provider = _FakeProvider()
    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: provider)

    async def _boom_create(*a, **k):
        raise RuntimeError("tasks table down")

    import dynastore.modules.tasks.tasks_module as tm
    monkeypatch.setattr(tm, "create_task", _boom_create)

    # Swallows the error and returns 0 (write must not break).
    n = await tcs.enqueue_tile_invalidation_task(
        "cat", "col", [{"id": "a", "bbox": [12, 41, 13, 42]}],
        engine=object(), schema="s_cat",
    )
    assert n == 0


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
