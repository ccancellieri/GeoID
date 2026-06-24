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

"""Unit-level coverage for ``AssetPostgresqlDriver.search_assets`` SQL shape.

Verifies that asset filters translate into the expected WHERE clauses:

- ``eq`` filters on ``metadata.*`` paths fold into one JSONB ``@>``
  containment predicate (engages the ``idx_assets_metadata_gin_*`` GIN index).
- Comparison / text / list / null operators (#1096) become their own
  parameterized predicates, on top-level columns and on ``metadata.*`` paths
  (via the ``#>>`` accessor, with a numeric cast for numeric comparisons).
"""

from __future__ import annotations

import json
from typing import Any, Dict, List

import pytest

from dynastore.models.query_builder import AssetFilter, FilterOperator
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
    """Capture (sql, params) handed to ``DQLQuery.execute`` and return []."""
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
    drv = mod.AssetPostgresqlDriver(engine=object())

    async def fake_resolve(catalog_id: str, db_resource=None) -> str:
        return f"s_{catalog_id}"

    drv._resolve_schema = fake_resolve  # type: ignore[method-assign]
    return drv


def _eq(field: str, value: Any) -> AssetFilter:
    return AssetFilter(field=field, op=FilterOperator.EQ, value=value)


# ---------------------------------------------------------------------------
# Metadata EQ containment folding (preserved from the EQ-only era)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_filters_emits_no_predicate(captured):
    drv = _make_driver(None)
    await drv.search_assets("cat-a", collection_id="coll-1")
    assert len(captured["calls"]) == 1
    sql = captured["calls"][0]["sql"]
    assert "metadata @>" not in sql
    assert "metadata_container" not in captured["calls"][0]["params"]


@pytest.mark.asyncio
async def test_metadata_eq_emits_containment(captured):
    drv = _make_driver(None)
    await drv.search_assets("cat-a", filters=[_eq("metadata.provider", "ESA")])
    sql = captured["calls"][0]["sql"]
    params = captured["calls"][0]["params"]
    assert "metadata @> CAST(:metadata_container AS jsonb)" in sql
    assert "->>" not in sql
    assert json.loads(params["metadata_container"]) == {"provider": "ESA"}


@pytest.mark.asyncio
async def test_nested_metadata_path_builds_nested_object(captured):
    drv = _make_driver(None)
    await drv.search_assets("cat-a", filters=[_eq("metadata.sensor.name", "MSI")])
    params = captured["calls"][0]["params"]
    assert json.loads(params["metadata_container"]) == {"sensor": {"name": "MSI"}}


@pytest.mark.asyncio
async def test_multiple_metadata_eq_fold_into_one_container(captured):
    drv = _make_driver(None)
    await drv.search_assets(
        "cat-a",
        filters=[
            _eq("metadata.provider", "ESA"),
            _eq("metadata.sensor.name", "MSI"),
            _eq("metadata.sensor.bands", 13),
        ],
    )
    sql = captured["calls"][0]["sql"]
    params = captured["calls"][0]["params"]
    assert sql.count("metadata @>") == 1, "should fold into ONE @> predicate"
    assert json.loads(params["metadata_container"]) == {
        "provider": "ESA",
        "sensor": {"name": "MSI", "bands": 13},
    }


@pytest.mark.asyncio
async def test_mixed_metadata_eq_and_scalar(captured):
    drv = _make_driver(None)
    await drv.search_assets(
        "cat-a",
        filters=[_eq("metadata.provider", "ESA"), _eq("asset_type", "RASTER")],
    )
    sql = captured["calls"][0]["sql"]
    params = captured["calls"][0]["params"]
    assert "metadata @> CAST(:metadata_container AS jsonb)" in sql
    assert '"asset_type" = :af1' in sql
    assert json.loads(params["metadata_container"]) == {"provider": "ESA"}
    assert params["af1"] == "RASTER"


@pytest.mark.asyncio
async def test_scalar_eq_only_skips_metadata_clause(captured):
    drv = _make_driver(None)
    await drv.search_assets("cat-a", filters=[_eq("asset_type", "RASTER")])
    sql = captured["calls"][0]["sql"]
    params = captured["calls"][0]["params"]
    assert "metadata @>" not in sql
    assert "metadata_container" not in params
    assert '"asset_type" = :af0' in sql


@pytest.mark.asyncio
async def test_conflicting_scalar_then_nested_path_raises(captured):
    drv = _make_driver(None)
    with pytest.raises(ValueError, match="conflicting filter"):
        await drv.search_assets(
            "cat-a",
            filters=[_eq("metadata.a", "x"), _eq("metadata.a.b", "y")],
        )


@pytest.mark.asyncio
async def test_conflicting_nested_then_scalar_raises(captured):
    drv = _make_driver(None)
    with pytest.raises(ValueError, match="conflicting filter"):
        await drv.search_assets(
            "cat-a",
            filters=[_eq("metadata.a.b", "y"), _eq("metadata.a", "x")],
        )


@pytest.mark.asyncio
async def test_empty_metadata_path_raises(captured):
    drv = _make_driver(None)
    with pytest.raises(ValueError, match="empty segment"):
        await drv.search_assets("cat-a", filters=[_eq("metadata.", "x")])


@pytest.mark.asyncio
async def test_typed_value_preserved_in_container(captured):
    drv = _make_driver(None)
    await drv.search_assets("cat-a", filters=[_eq("metadata.size", 100)])
    params = captured["calls"][0]["params"]
    assert json.loads(params["metadata_container"]) == {"size": 100}
    assert isinstance(json.loads(params["metadata_container"])["size"], int)


# ---------------------------------------------------------------------------
# Operator coverage beyond EQ — #1096
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "op,sql_token",
    [
        (FilterOperator.GT, ">"),
        (FilterOperator.GTE, ">="),
        (FilterOperator.LT, "<"),
        (FilterOperator.LTE, "<="),
        (FilterOperator.NE, "!="),
    ],
)
async def test_scalar_comparison_operators_on_column(captured, op, sql_token):
    drv = _make_driver(None)
    await drv.search_assets(
        "cat-a", filters=[AssetFilter(field="size_bytes", op=op, value=100)]
    )
    sql = captured["calls"][0]["sql"]
    params = captured["calls"][0]["params"]
    assert f'"size_bytes" {sql_token} :af0' in sql
    assert params["af0"] == 100


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "op,sql_token", [(FilterOperator.LIKE, "LIKE"), (FilterOperator.ILIKE, "ILIKE")]
)
async def test_like_ilike_on_column(captured, op, sql_token):
    drv = _make_driver(None)
    await drv.search_assets(
        "cat-a", filters=[AssetFilter(field="filename", op=op, value="%.tif")]
    )
    sql = captured["calls"][0]["sql"]
    assert f'"filename" {sql_token} :af0' in sql
    assert captured["calls"][0]["params"]["af0"] == "%.tif"


@pytest.mark.asyncio
async def test_in_expands_to_placeholders(captured):
    drv = _make_driver(None)
    await drv.search_assets(
        "cat-a",
        filters=[AssetFilter(field="asset_id", op=FilterOperator.IN, value=["a", "b", "c"])],
    )
    sql = captured["calls"][0]["sql"]
    params = captured["calls"][0]["params"]
    assert '"asset_id" IN (:af0_0, :af0_1, :af0_2)' in sql
    assert params["af0_0"] == "a"
    assert params["af0_2"] == "c"


@pytest.mark.asyncio
async def test_nin_expands_to_not_in(captured):
    drv = _make_driver(None)
    await drv.search_assets(
        "cat-a",
        filters=[AssetFilter(field="asset_type", op=FilterOperator.NIN, value=["X", "Y"])],
    )
    sql = captured["calls"][0]["sql"]
    assert '"asset_type" NOT IN (:af0_0, :af0_1)' in sql


@pytest.mark.asyncio
async def test_in_requires_non_empty_list(captured):
    drv = _make_driver(None)
    with pytest.raises(ValueError, match="non-empty list"):
        await drv.search_assets(
            "cat-a",
            filters=[AssetFilter(field="asset_id", op=FilterOperator.IN, value=[])],
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "op,sql_token",
    [(FilterOperator.IS_NULL, "IS NULL"), (FilterOperator.IS_NOT_NULL, "IS NOT NULL")],
)
async def test_null_operators_emit_no_param(captured, op, sql_token):
    drv = _make_driver(None)
    await drv.search_assets("cat-a", filters=[AssetFilter(field="href", op=op, value=None)])
    sql = captured["calls"][0]["sql"]
    params = captured["calls"][0]["params"]
    assert f'"href" {sql_token}' in sql
    assert "af0" not in params


@pytest.mark.asyncio
async def test_metadata_numeric_comparison_uses_cast_accessor(captured):
    drv = _make_driver(None)
    await drv.search_assets(
        "cat-a",
        filters=[AssetFilter(field="metadata.size", op=FilterOperator.GTE, value=5)],
    )
    sql = captured["calls"][0]["sql"]
    params = captured["calls"][0]["params"]
    assert "(metadata #>> '{size}')::numeric >= :af0" in sql
    assert params["af0"] == 5
    assert "metadata @>" not in sql  # non-eq does NOT use containment


@pytest.mark.asyncio
async def test_metadata_text_like_uses_accessor(captured):
    drv = _make_driver(None)
    await drv.search_assets(
        "cat-a",
        filters=[AssetFilter(field="metadata.name", op=FilterOperator.LIKE, value="MSI%")],
    )
    sql = captured["calls"][0]["sql"]
    assert "metadata #>> '{name}' LIKE :af0" in sql


@pytest.mark.asyncio
async def test_unsafe_metadata_segment_for_non_eq_raises(captured):
    drv = _make_driver(None)
    with pytest.raises(ValueError, match="unsafe metadata path segment"):
        await drv.search_assets(
            "cat-a",
            filters=[AssetFilter(field="metadata.a'b", op=FilterOperator.GT, value=1)],
        )


@pytest.mark.asyncio
async def test_unsupported_operator_raises(captured):
    drv = _make_driver(None)
    with pytest.raises(ValueError, match="unsupported operator"):
        await drv.search_assets(
            "cat-a",
            filters=[AssetFilter(field="geom", op=FilterOperator.INTERSECTS, value="x")],
        )


# ---------------------------------------------------------------------------
# Collection scope (tri-state) — #1095
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_default_scopes_to_catalog_tier(captured):
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
    drv = _make_driver(None)
    await drv.search_assets("cat-a", collection_id="ignored", all_collections=True)
    sql = captured["calls"][0]["sql"]
    params = captured["calls"][0]["params"]
    assert "collection_id IS NOT DISTINCT FROM" not in sql
    assert "collection_id" not in params
    assert "catalog_id = :catalog_id" in sql
    assert "status <> 'deleted'" in sql
