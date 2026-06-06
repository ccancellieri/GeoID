"""Unit coverage for ``ItemQueryMixin._fetch_prior_bboxes_bulk`` (#1845).

Issue #1845 replaces the N+1 per-item ``get_item`` loop in tile-cache
prior-bbox capture with a single ``WHERE feature_id = ANY(:ids)`` bulk read.

These tests verify:
  (a) One bulk query replaces N reads — ``_apply_query_transformations`` is
      called exactly once regardless of batch size.
  (b) Degrade-safe: a query failure returns ``[]`` and never blocks the write.
  (c) Results are keyed correctly per id — only rows with complete (non-NULL)
      bbox scalar columns contribute a TileBBox.
  (d) Only the bbox column is queried — ``raw_selects`` contains ST_XMin/
      ST_YMin/ST_XMax/ST_YMax on the materialized column, never raw ``geom``.
  (e) Empty ``item_ids`` short-circuits immediately (no DB call).

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

def _make_svc(
    *,
    col_config: Any = None,
    phys_schema: Optional[str] = "public",
    phys_table: Optional[str] = "items_cat_col",
    query_rows: Optional[List[Dict[str, Any]]] = None,
) -> "ItemService":
    """Build a minimal ItemService stub for _fetch_prior_bboxes_bulk tests.

    Patches:
      - ``_get_collection_config`` → returns ``col_config`` (default: object())
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
# (a) One bulk query for N items
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_single_bulk_query_for_multiple_items(monkeypatch):
    """``_apply_query_transformations`` is called exactly once for any batch size."""
    rows = [
        {"_xmin": 10.0, "_ymin": 20.0, "_xmax": 11.0, "_ymax": 21.0},
        {"_xmin": 30.0, "_ymin": 40.0, "_xmax": 31.0, "_ymax": 41.0},
    ]

    svc = _make_svc(query_rows=rows)

    # Patch managed_transaction and DQLQuery to return our rows without a real DB
    with _patch_db(svc._query_rows):  # type: ignore[attr-defined]
        result = await svc._fetch_prior_bboxes_bulk("cat", "col", ["a", "b", "c"])

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
        result = await svc._fetch_prior_bboxes_bulk("cat", "col", ["a", "b", "c"])

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

    result = await svc._fetch_prior_bboxes_bulk("cat", "col", ["x"])
    assert result == []


@pytest.mark.asyncio
async def test_degrades_to_empty_when_phys_table_missing(monkeypatch):
    """When physical table cannot be resolved, returns ``[]`` without error."""
    svc = _make_svc(phys_table=None)

    with _patch_db([]):
        result = await svc._fetch_prior_bboxes_bulk("cat", "col", ["x"])

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
        await svc._fetch_prior_bboxes_bulk("cat", "col", ["item-1"])

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


@pytest.mark.asyncio
async def test_item_ids_forwarded_via_item_ids_field(monkeypatch):
    """All caller-supplied IDs reach ``QueryRequest.item_ids`` in the request."""
    svc = _make_svc(query_rows=[])

    with _patch_db([]):
        await svc._fetch_prior_bboxes_bulk("cat", "col", ["id-1", "id-2", "id-3"])

    req = svc._transform_calls[0]  # type: ignore[attr-defined]
    assert sorted(req.item_ids) == ["id-1", "id-2", "id-3"]


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
