"""Unit-level coverage for ``PgAssetDriver.search_assets`` SQL shape.

Verifies the metadata-path branch folds dotted keys into a single JSONB
``@>`` containment predicate (engages the ``idx_assets_metadata_gin_*``
GIN index) instead of emitting per-key text-extract equalities.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List

import pytest

from dynastore.modules.catalog.drivers import pg_asset_driver as mod


class _CapturingConn:
    pass


class _FakeTxCM:
    def __init__(self, conn: _CapturingConn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.fixture
def captured(monkeypatch):
    """Capture (sql, params) handed to ``DQLQuery.execute`` and return [].

    Patches the symbols ``managed_transaction`` and ``DQLQuery`` inside
    ``pg_asset_driver`` so the search_assets call is fully synchronous and
    requires no real DB.  Also stubs ``_resolve_schema`` to a fixed value.
    """
    state: dict = {"calls": []}

    def fake_managed(engine):
        return _FakeTxCM(_CapturingConn())

    class _FakeDQLQuery:
        def __init__(self, sql: str, **_):
            self.sql = sql

        async def execute(self, conn, **kwargs) -> List[Dict[str, Any]]:
            state["calls"].append({"sql": self.sql, "params": kwargs})
            return []

    monkeypatch.setattr(mod, "managed_transaction", fake_managed, raising=True)
    monkeypatch.setattr(mod, "DQLQuery", _FakeDQLQuery, raising=True)
    return state


def _make_driver(monkeypatch):
    """Build an AssetPostgresqlDriver with a fake engine + stubbed
    ``_resolve_schema`` so ``search_assets`` doesn't touch real wiring."""
    drv = mod.AssetPostgresqlDriver(engine=object())

    async def fake_resolve(catalog_id: str, db_resource=None) -> str:
        return f"s_{catalog_id}"

    drv._resolve_schema = fake_resolve  # type: ignore[method-assign]
    return drv


@pytest.mark.asyncio
async def test_no_query_emits_no_metadata_predicate(captured):
    drv = _make_driver(None)
    await drv.search_assets("cat-a", collection_id="coll-1")
    assert len(captured["calls"]) == 1
    sql = captured["calls"][0]["sql"]
    assert "metadata @>" not in sql
    assert "metadata_container" not in captured["calls"][0]["params"]


@pytest.mark.asyncio
async def test_top_level_metadata_filter_emits_containment(captured):
    drv = _make_driver(None)
    await drv.search_assets("cat-a", query={"metadata.provider": "ESA"})
    sql = captured["calls"][0]["sql"]
    params = captured["calls"][0]["params"]
    assert "metadata @> CAST(:metadata_container AS jsonb)" in sql
    # No legacy text-extract operator must remain
    assert "->>" not in sql
    assert "metadata_container" in params
    assert json.loads(params["metadata_container"]) == {"provider": "ESA"}


@pytest.mark.asyncio
async def test_nested_metadata_path_builds_nested_object(captured):
    drv = _make_driver(None)
    await drv.search_assets("cat-a", query={"metadata.sensor.name": "MSI"})
    params = captured["calls"][0]["params"]
    assert json.loads(params["metadata_container"]) == {
        "sensor": {"name": "MSI"}
    }


@pytest.mark.asyncio
async def test_multiple_metadata_filters_fold_into_one_container(captured):
    drv = _make_driver(None)
    await drv.search_assets(
        "cat-a",
        query={
            "metadata.provider": "ESA",
            "metadata.sensor.name": "MSI",
            "metadata.sensor.bands": 13,
        },
    )
    sql = captured["calls"][0]["sql"]
    params = captured["calls"][0]["params"]
    assert sql.count("metadata @>") == 1, "should fold into ONE @> predicate"
    assert json.loads(params["metadata_container"]) == {
        "provider": "ESA",
        "sensor": {"name": "MSI", "bands": 13},
    }


@pytest.mark.asyncio
async def test_mixed_metadata_and_scalar_filters(captured):
    drv = _make_driver(None)
    await drv.search_assets(
        "cat-a",
        query={
            "metadata.provider": "ESA",
            "asset_type": "RASTER",
        },
    )
    sql = captured["calls"][0]["sql"]
    params = captured["calls"][0]["params"]
    assert "metadata @> CAST(:metadata_container AS jsonb)" in sql
    assert '"asset_type" = :qval_1' in sql or '"asset_type" = :qval_0' in sql
    assert json.loads(params["metadata_container"]) == {"provider": "ESA"}
    assert "RASTER" in params.values()


@pytest.mark.asyncio
async def test_scalar_only_filter_skips_metadata_clause(captured):
    drv = _make_driver(None)
    await drv.search_assets("cat-a", query={"asset_type": "RASTER"})
    sql = captured["calls"][0]["sql"]
    params = captured["calls"][0]["params"]
    assert "metadata @>" not in sql
    assert "metadata_container" not in params


@pytest.mark.asyncio
async def test_conflicting_scalar_then_nested_path_raises(captured):
    drv = _make_driver(None)
    with pytest.raises(ValueError, match="conflicting filter"):
        await drv.search_assets(
            "cat-a",
            query={
                "metadata.a": "x",  # scalar at "a"
                "metadata.a.b": "y",  # tries to traverse INTO "a"
            },
        )


@pytest.mark.asyncio
async def test_conflicting_nested_then_scalar_raises(captured):
    drv = _make_driver(None)
    with pytest.raises(ValueError, match="conflicting filter"):
        await drv.search_assets(
            "cat-a",
            query={
                "metadata.a.b": "y",  # builds {"a": {"b": "y"}}
                "metadata.a": "x",  # tries to overwrite the dict with scalar
            },
        )


@pytest.mark.asyncio
async def test_empty_metadata_path_raises(captured):
    drv = _make_driver(None)
    with pytest.raises(ValueError, match="empty segment"):
        await drv.search_assets("cat-a", query={"metadata.": "x"})


@pytest.mark.asyncio
async def test_typed_value_preserved_in_container(captured):
    """Containment is type-strict — numeric values land as JSON numbers,
    not string-cast (the old ``->>`` behavior)."""
    drv = _make_driver(None)
    await drv.search_assets("cat-a", query={"metadata.size": 100})
    params = captured["calls"][0]["params"]
    assert json.loads(params["metadata_container"]) == {"size": 100}
    assert isinstance(json.loads(params["metadata_container"])["size"], int)


# ---------------------------------------------------------------------------
# Collection scope (tri-state) — #1095
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_default_scopes_to_catalog_tier(captured):
    """Default (all_collections=False, collection_id=None) keeps the
    ``IS NOT DISTINCT FROM`` predicate with a NULL parameter → catalog tier."""
    drv = _make_driver(None)
    await drv.search_assets("cat-a")
    sql = captured["calls"][0]["sql"]
    params = captured["calls"][0]["params"]
    assert "collection_id IS NOT DISTINCT FROM :collection_id" in sql
    assert params["collection_id"] is None


@pytest.mark.asyncio
async def test_single_collection_binds_predicate(captured):
    drv = _make_driver(None)
    await drv.search_assets("cat-a", collection_id="coll-1")
    sql = captured["calls"][0]["sql"]
    params = captured["calls"][0]["params"]
    assert "collection_id IS NOT DISTINCT FROM :collection_id" in sql
    assert params["collection_id"] == "coll-1"


@pytest.mark.asyncio
async def test_all_collections_drops_collection_predicate(captured):
    """all_collections=True spans the whole catalog: no collection predicate,
    and no collection_id bind param leaks into the query."""
    drv = _make_driver(None)
    await drv.search_assets("cat-a", collection_id="ignored", all_collections=True)
    sql = captured["calls"][0]["sql"]
    params = captured["calls"][0]["params"]
    assert "collection_id IS NOT DISTINCT FROM" not in sql
    assert "collection_id" not in params
    # catalog scoping still applies
    assert "catalog_id = :catalog_id" in sql
    assert "status <> 'deleted'" in sql
