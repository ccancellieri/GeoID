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

"""Unit coverage for ``ItemQueryMixin._fetch_prior_bboxes_bulk`` (#1845, #2045).

Issue #1845 replaces the N+1 per-item ``get_item`` loop in tile-cache
prior-bbox capture with a single ``WHERE feature_id = ANY(:ids)`` bulk read.

Issue #2045 fixes an eligibility gap: the bulk reader was casting all item ids
to ``uuid[]``, which failed with ``invalid UUID`` for collections whose items
carry external/producer ids (e.g. CityJSON GUIDs like ``GUID_FFF9E566-...``).
The fix detects whether the collection has a sidecar with a
``feature_id_field_name`` (the external-id text column) and uses a text-column
match when available, falling back to the UUID cast only for UUID-id collections
(and silently filtering out any non-UUID values there rather than erroring).

These tests verify:
  (a) One bulk query replaces N reads — ``_apply_query_transformations`` is
      called exactly once regardless of batch size.
  (b) Degrade-safe: a query failure returns ``[]`` and never blocks the write.
  (c) Results are keyed correctly per id — only rows with complete (non-NULL)
      bbox scalar columns contribute a TileBBox.
  (d) Only the bbox column is queried — ``raw_selects`` contains ST_XMin/
      ST_YMin/ST_XMax/ST_YMax on the materialized column, never raw ``geom``.
  (e) Empty ``item_ids`` short-circuits immediately (no DB call).
  (f) Non-UUID ids succeed when the collection has a feature_id sidecar
      (text-column match via ``raw_where``, not ``uuid[]`` cast).
  (g) Non-UUID ids with no feature_id sidecar degrade to empty without error.
  (h) The id-matching WHERE is always injected via ``raw_where``/``raw_params``,
      never via ``item_ids`` (which triggers the optimizer's uuid-casting path).

All tests run without a real database by patching
``_apply_query_transformations`` and ``managed_transaction``.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, List, Optional
from unittest.mock import patch

import pytest

from dynastore.modules.catalog.item_service import ItemService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Stable UUID strings for tests that exercise the UUID-id path.
_UUID_A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
_UUID_B = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
_UUID_C = "cccccccc-cccc-cccc-cccc-cccccccccccc"

# Non-UUID strings that mimic CityJSON GUIDs (the #2045 scenario).
_GUID_A = "GUID_FFF9E566-1A2B-3C4D-5E6F-7A8B9C0D1E2F"
_GUID_B = "GUID_AABBCCDD-1122-3344-5566-778899AABBCC"


def _make_col_config(*, feature_id_field_name: Optional[str] = None) -> Any:
    """Return a minimal col_config stub that ``driver_sidecars()`` can inspect.

    When ``feature_id_field_name`` is provided the config carries a fake
    attributes sidecar whose ``feature_id_field_name`` matches, causing
    ``_fetch_prior_bboxes_bulk`` to use the text-column match path.
    """
    if feature_id_field_name is None:
        # No sidecars — driver_sidecars() returns [] for any object without
        # a ``sidecars`` attribute, which is the UUID-only path.
        return object()

    class _FakeSidecarConfig:
        sidecar_id = "attributes"
        sidecar_type = "attributes"

        @property
        def feature_id_field_name(self):  # noqa: D102
            return feature_id_field_name

    class _FakeColConfig:
        sidecars = [_FakeSidecarConfig()]

    return _FakeColConfig()


def _make_svc(
    *,
    col_config: Any = None,
    phys_schema: Optional[str] = "public",
    phys_table: Optional[str] = "items_cat_col",
    query_rows: Optional[List[Dict[str, Any]]] = None,
) -> "ItemService":
    """Build a minimal ItemService stub for _fetch_prior_bboxes_bulk tests.

    Patches:
      - ``_get_collection_config`` → returns ``col_config`` (default: no sidecars)
      - ``_resolve_physical_schema`` → returns ``phys_schema``
      - ``_resolve_physical_table`` → returns ``phys_table``
      - ``_apply_query_transformations`` → returns ("SELECT 1", {})
      - ``managed_transaction`` context manager → DQLQuery yields ``query_rows``
    """
    svc = ItemService.__new__(ItemService)
    svc.engine = None  # not used in these tests

    _cfg = col_config if col_config is not None else object()

    async def _get_col_cfg(*_a, **_k):
        return _cfg

    async def _resolve_schema(*_a, **_k):
        return phys_schema

    async def _resolve_table(*_a, **_k):
        return phys_table

    _transform_calls: List[Any] = []

    async def _apply_transforms(request, query_ctx, *args, **kwargs):
        _transform_calls.append(request)
        return "SELECT 1", {}

    svc._get_collection_config = _get_col_cfg  # type: ignore[attr-defined]
    svc._resolve_physical_schema = _resolve_schema  # type: ignore[attr-defined]
    svc._resolve_physical_table = _resolve_table  # type: ignore[attr-defined]
    svc._apply_query_transformations = _apply_transforms  # type: ignore[attr-defined]
    svc._transform_calls = _transform_calls  # type: ignore[attr-defined]
    svc._query_rows = query_rows if query_rows is not None else []
    return svc


# ---------------------------------------------------------------------------
# (e) Empty ids → short-circuit
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_empty_item_ids_returns_empty_without_db(monkeypatch):
    """An empty ``item_ids`` list must short-circuit immediately — no DB call."""
    svc = _make_svc(query_rows=[{"_xmin": 1.0, "_ymin": 2.0, "_xmax": 3.0, "_ymax": 4.0}])

    result = await svc._fetch_prior_bboxes_bulk("cat", "col", [])
    assert result == []
    # No _apply_query_transformations calls because we short-circuited
    assert svc._transform_calls == []  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# (a) One bulk query for N UUID items
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_single_bulk_query_for_multiple_uuid_items(monkeypatch):
    """``_apply_query_transformations`` is called exactly once for any batch size
    of UUID ids (the UUID-geoid path)."""
    rows = [
        {"_xmin": 10.0, "_ymin": 20.0, "_xmax": 11.0, "_ymax": 21.0},
        {"_xmin": 30.0, "_ymin": 40.0, "_xmax": 31.0, "_ymax": 41.0},
    ]

    svc = _make_svc(query_rows=rows)

    with _patch_db(svc._query_rows):  # type: ignore[attr-defined]
        result = await svc._fetch_prior_bboxes_bulk(
            "cat", "col", [_UUID_A, _UUID_B, _UUID_C],
        )

    assert len(svc._transform_calls) == 1, "exactly one bulk query call"  # type: ignore[attr-defined]
    assert result == [(10.0, 20.0, 11.0, 21.0), (30.0, 40.0, 31.0, 41.0)]


# ---------------------------------------------------------------------------
# (c) Results keyed correctly — NULL bbox rows are dropped
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_null_bbox_rows_are_dropped(monkeypatch):
    """Rows with any NULL ST_XMin/etc value do not contribute a TileBBox."""
    rows = [
        # Complete row
        {"_xmin": 5.0, "_ymin": 6.0, "_xmax": 7.0, "_ymax": 8.0},
        # Row with NULL bbox (item with no geometry / no bbox_geom)
        {"_xmin": None, "_ymin": None, "_xmax": None, "_ymax": None},
        # Partially-NULL row
        {"_xmin": 1.0, "_ymin": None, "_xmax": 3.0, "_ymax": 4.0},
    ]

    svc = _make_svc(query_rows=rows)

    with _patch_db(rows):
        result = await svc._fetch_prior_bboxes_bulk(
            "cat", "col", [_UUID_A, _UUID_B, _UUID_C],
        )

    assert result == [(5.0, 6.0, 7.0, 8.0)]


# ---------------------------------------------------------------------------
# (b) Degrade-safe on failure
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_degrades_to_empty_on_db_error(monkeypatch):
    """A database error returns ``[]`` and never raises."""
    svc = _make_svc()

    async def _failing_transforms(*_a, **_k):
        raise RuntimeError("connection lost")

    svc._apply_query_transformations = _failing_transforms  # type: ignore[attr-defined]

    # Valid UUID so we reach _apply_query_transformations (which then fails)
    result = await svc._fetch_prior_bboxes_bulk("cat", "col", [_UUID_A])
    assert result == []


@pytest.mark.asyncio
async def test_degrades_to_empty_when_phys_table_missing(monkeypatch):
    """When physical table cannot be resolved, returns ``[]`` without error."""
    svc = _make_svc(phys_table=None)

    with _patch_db([]):
        result = await svc._fetch_prior_bboxes_bulk("cat", "col", [_UUID_A])

    assert result == []


# ---------------------------------------------------------------------------
# (d) Only bbox column selected — no raw geometry in raw_selects
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_raw_selects_contain_only_bbox_scalars(monkeypatch):
    """The ``raw_selects`` must reference ST_XMin/YMin/XMax/YMax on the
    materialized bbox column and must NOT select ``geom`` (raw geometry)."""
    svc = _make_svc(query_rows=[])

    with _patch_db([]):
        await svc._fetch_prior_bboxes_bulk("cat", "col", [_UUID_A])

    assert len(svc._transform_calls) == 1  # type: ignore[attr-defined]
    req = svc._transform_calls[0]  # type: ignore[attr-defined]

    raw_selects_joined = " ".join(req.raw_selects)

    # Must contain all four ST_* bbox scalar functions
    for fn in ("ST_XMin", "ST_YMin", "ST_XMax", "ST_YMax"):
        assert fn in raw_selects_joined, f"{fn} missing from raw_selects"

    # Must NOT select the raw geometry column ``geom`` directly
    assert "sc_geometries.geom" not in raw_selects_joined, \
        "raw geometry column must not be selected"
    # The raw ``geom`` word appearing alone (e.g. ``sc_geometries.geom``) is
    # the signal; bbox_geom as part of ST_XMin(sc_geometries.bbox_geom) is fine.
    import re
    assert not re.search(r"\bsc_geometries\.geom\b(?!_)", raw_selects_joined), \
        "raw geometry must not appear in select"


# ---------------------------------------------------------------------------
# (h) id-matching WHERE always in raw_where/raw_params, never in item_ids
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_uuid_ids_forwarded_via_raw_where_not_item_ids(monkeypatch):
    """UUID ids must reach the query via ``raw_where``/``raw_params``, and
    ``item_ids`` on the QueryRequest must be ``None`` so the optimizer's
    uuid-casting path is bypassed."""
    svc = _make_svc(query_rows=[])

    with _patch_db([]):
        await svc._fetch_prior_bboxes_bulk(
            "cat", "col", [_UUID_A, _UUID_B, _UUID_C],
        )

    req = svc._transform_calls[0]  # type: ignore[attr-defined]
    # item_ids must be None — the optimizer must not add its own uuid[] CAST
    assert req.item_ids is None, "item_ids must be None; matching goes via raw_where"
    # All ids must appear in raw_params under the dedicated key
    assert req.raw_params is not None
    forwarded = req.raw_params.get("_tile_prior_ids", [])
    assert sorted(forwarded) == sorted([_UUID_A, _UUID_B, _UUID_C])
    # raw_where must reference the geoid uuid-cast clause
    assert req.raw_where is not None
    assert "uuid" in req.raw_where.lower() or "geoid" in req.raw_where.lower()


# ---------------------------------------------------------------------------
# (f) Non-UUID ids succeed via text-column match when feature_id sidecar exists
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_non_uuid_ids_use_text_column_when_sidecar_present(monkeypatch):
    """CityJSON GUID ids (non-UUID) must trigger the text-column match path
    when the collection has a sidecar with ``feature_id_field_name``.

    The query must NOT cast to ``uuid[]`` (which would raise
    ``invalid UUID 'GUID_...'``); instead the ids are matched via
    ``sc_attributes.external_id = ANY(:_tile_prior_ids)`` (text comparison).
    """
    rows = [
        {"_xmin": 1.0, "_ymin": 2.0, "_xmax": 3.0, "_ymax": 4.0},
    ]
    cfg = _make_col_config(feature_id_field_name="external_id")
    svc = _make_svc(col_config=cfg, query_rows=rows)

    with _patch_db(rows):
        result = await svc._fetch_prior_bboxes_bulk(
            "cat", "col", [_GUID_A, _GUID_B],
        )

    # The query must have been issued (not short-circuited)
    assert len(svc._transform_calls) == 1  # type: ignore[attr-defined]
    req = svc._transform_calls[0]  # type: ignore[attr-defined]

    # item_ids must be None — avoid the optimizer's uuid[] CAST
    assert req.item_ids is None

    # raw_where must reference the sidecar alias (sc_attributes) and field
    assert req.raw_where is not None
    assert "sc_attributes" in req.raw_where
    assert "external_id" in req.raw_where
    # Must NOT contain a UUID cast
    assert "uuid" not in req.raw_where.lower()

    # Both GUIDs must be in raw_params
    forwarded = req.raw_params.get("_tile_prior_ids", [])
    assert _GUID_A in forwarded
    assert _GUID_B in forwarded

    # Bboxes from the DB rows are returned
    assert result == [(1.0, 2.0, 3.0, 4.0)]


@pytest.mark.asyncio
async def test_non_uuid_ids_forwarded_in_single_bulk_call(monkeypatch):
    """All non-UUID ids are forwarded to ``_apply_query_transformations`` in
    exactly one call when a feature_id sidecar is present."""
    cfg = _make_col_config(feature_id_field_name="external_id")
    svc = _make_svc(col_config=cfg, query_rows=[])

    with _patch_db([]):
        await svc._fetch_prior_bboxes_bulk(
            "cat", "col", [_GUID_A, _GUID_B],
        )

    assert len(svc._transform_calls) == 1  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# (g) Non-UUID ids with no feature_id sidecar degrade to empty without error
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_non_uuid_ids_without_sidecar_return_empty(monkeypatch):
    """When the collection has no feature_id sidecar and all ids are non-UUID,
    the helper must return ``[]`` immediately without hitting the DB and without
    raising (including the PostgreSQL ``invalid UUID`` error).

    This is the prior failure mode: the optimizer cast non-UUID ids to uuid[]
    and PostgreSQL raised ``invalid UUID 'GUID_...'``, logged at DEBUG and
    surfaced as a silent empty result. After the fix, we short-circuit before
    the query is even built."""
    svc = _make_svc()  # no sidecars → UUID-only path

    result = await svc._fetch_prior_bboxes_bulk(
        "cat", "col", [_GUID_A, _GUID_B],
    )

    # No DB call — non-UUID ids + no sidecar → early return
    assert result == []
    assert svc._transform_calls == []  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_mixed_ids_without_sidecar_queries_only_uuids(monkeypatch):
    """When the collection has no feature_id sidecar and ids are a mix of UUID
    and non-UUID values, only the valid UUIDs are passed to the query."""
    rows = [{"_xmin": 5.0, "_ymin": 6.0, "_xmax": 7.0, "_ymax": 8.0}]
    svc = _make_svc(query_rows=rows)

    with _patch_db(rows):
        result = await svc._fetch_prior_bboxes_bulk(
            "cat", "col", [_GUID_A, _UUID_A, _GUID_B],
        )

    assert len(svc._transform_calls) == 1  # type: ignore[attr-defined]
    req = svc._transform_calls[0]  # type: ignore[attr-defined]
    forwarded = req.raw_params.get("_tile_prior_ids", [])
    # Only the UUID survives filtering
    assert forwarded == [_UUID_A]
    assert result == [(5.0, 6.0, 7.0, 8.0)]


# ---------------------------------------------------------------------------
# Context manager patch helper
# ---------------------------------------------------------------------------


@contextmanager
def _patch_db(rows: List[Dict[str, Any]]):
    """Patch ``managed_transaction`` and ``DQLQuery.execute`` in the
    ``item_query`` module namespace (where they were already imported) so that
    calls from ``_fetch_prior_bboxes_bulk`` return ``rows`` without a real DB.

    We must patch the NAME in the MODULE that uses it, not the origin module —
    because ``item_query.py`` does ``from ... import managed_transaction`` which
    binds the name locally; patching the source module's attribute has no effect
    on the already-bound local reference.
    """
    import dynastore.modules.catalog.item_query as iq
    import dynastore.modules.db_config.query_executor as qe

    async def _fake_execute(self_inner, conn, **kwargs):
        return rows

    class _FakeTx:
        async def __aenter__(self):
            return object()  # fake connection — never used in test

        async def __aexit__(self, *_):
            pass

    def _fake_managed_tx(*_a, **_k):
        return _FakeTx()

    with patch.object(iq, "managed_transaction", _fake_managed_tx), \
         patch.object(qe.DQLQuery, "execute", _fake_execute):
        yield
