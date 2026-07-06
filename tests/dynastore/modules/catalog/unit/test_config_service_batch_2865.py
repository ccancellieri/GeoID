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

"""Regression tests for #2865's remaining PG-fallback branch.

``search_items``'s PG fallback (``dynastore.extensions.stac.search``) used to
resolve a fresh ``ItemsPostgresqlDriverConfig`` per scoped collection via
``ConfigsProtocol.get_config(..., ctx=DriverContext(db_resource=<live conn>))``
— a live connection bypasses the config cache by design, so every collection
re-did the catalog-tier physical-schema resolution, table-existence checks,
and delta fetch even though none of that varies by collection. On an
auto-expanded (bare) ``/search`` over a large catalog that is one full round
trip per collection before the query ever ran.

``ConfigService.get_configs_batch`` replaces that with the catalog-tier work
done ONCE and every collection's tier-local delta fetched in a single
``collection_id = ANY(...)`` query. These tests pin the O(1) query-count
contract and prove the batched merge produces the same per-collection result
the old per-collection ``get_config`` loop would have.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Dict, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig


@asynccontextmanager
async def _fake_txn(_engine):
    yield MagicMock()


def _make_service():
    from dynastore.modules.catalog.config_service import ConfigService

    return ConfigService(engine=MagicMock(), catalog_manager=MagicMock())


def _wire(
    monkeypatch,
    svc,
    *,
    phys_schema: str = "phys_test",
    internal_ids: Optional[Dict[str, str]] = None,
):
    import dynastore.modules.catalog.config_service as svc_mod

    monkeypatch.setattr(svc_mod, "managed_transaction", _fake_txn)
    monkeypatch.setattr(svc_mod, "DriverContext", MagicMock)
    monkeypatch.setattr(svc_mod, "check_table_exists", AsyncMock(return_value=True))

    internal_ids = internal_ids or {}
    mock_mgr = MagicMock()
    mock_mgr.resolve_physical_schema = AsyncMock(return_value=phys_schema)

    async def _resolve_collection_ids(_catalog_id, cid, allow_missing=True):
        return MagicMock(id=internal_ids.get(cid, cid))

    mock_mgr.collections.resolve_collection_ids = AsyncMock(
        side_effect=_resolve_collection_ids
    )
    svc._get_catalog_manager = MagicMock(return_value=mock_mgr)
    svc.get_catalog_defaults_snapshot = AsyncMock(return_value=None)

    platform_svc = MagicMock()
    platform_svc.get_config = AsyncMock(return_value=ItemsPostgresqlDriverConfig())
    svc._get_platform_config_service = MagicMock(return_value=platform_svc)

    return svc_mod, mock_mgr


@pytest.mark.asyncio
async def test_get_configs_batch_issues_bounded_queries_for_many_collections(monkeypatch):
    """500 scoped collections must not cost 500 collection-tier queries."""
    svc = _make_service()
    internal_ids = {f"col-{i}": f"internal-col-{i}" for i in range(500)}
    svc_mod, _mgr = _wire(monkeypatch, svc, internal_ids=internal_ids)

    catalog_query = MagicMock()
    catalog_query.return_value.execute = AsyncMock(
        return_value={"physical_table": "tbl_catalog_default"}
    )
    monkeypatch.setattr(svc_mod._cq, "select_catalog_config", catalog_query)

    batch_rows = [
        {"collection_id": "internal-col-0", "config_data": {"physical_table": "tbl_col_0"}},
        {"collection_id": "internal-col-7", "config_data": {"physical_table": "tbl_col_7"}},
    ]
    batch_query = MagicMock()
    batch_query.return_value.execute = AsyncMock(return_value=batch_rows)
    monkeypatch.setattr(svc_mod._cq, "select_collection_configs_batch", batch_query)

    cids = list(internal_ids.keys())
    ctx = MagicMock(db_resource=MagicMock())
    result = await svc.get_configs_batch(
        ItemsPostgresqlDriverConfig, "cat1", cids, ctx=ctx,
    )

    # O(1) round trips regardless of the 500-collection scope.
    assert catalog_query.return_value.execute.await_count == 1
    assert batch_query.return_value.execute.await_count == 1
    assert svc_mod.check_table_exists.await_count == 2  # catalog + collection tables

    batch_call_kwargs = batch_query.return_value.execute.await_args.kwargs
    assert sorted(batch_call_kwargs["collection_ids"]) == sorted(internal_ids.values())

    assert len(result) == 500
    assert result["col-0"].physical_table == "tbl_col_0"
    assert result["col-7"].physical_table == "tbl_col_7"
    # Every other collection falls back to the catalog-tier delta.
    assert result["col-1"].physical_table == "tbl_catalog_default"
    assert result["col-499"].physical_table == "tbl_catalog_default"


@pytest.mark.asyncio
async def test_get_configs_batch_matches_per_collection_get_config_loop(monkeypatch):
    """The batched merge must equal what the old per-collection ``get_config``
    loop would have produced for each collection."""
    svc = _make_service()
    internal_ids = {"col-a": "internal-a", "col-b": "internal-b", "col-c": "internal-c"}
    svc_mod, _mgr = _wire(monkeypatch, svc, internal_ids=internal_ids)

    catalog_query = MagicMock()
    catalog_query.return_value.execute = AsyncMock(
        return_value={"physical_table": "tbl_catalog_default"}
    )
    monkeypatch.setattr(svc_mod._cq, "select_catalog_config", catalog_query)

    # Only "col-a" carries a collection-tier override; "col-b"/"col-c" fall
    # through to the catalog delta — the same fake row store backs both the
    # batched query and the old single-row query so the two paths are
    # compared against identical data.
    fake_db = {"internal-a": {"physical_table": "tbl_a"}}

    batch_query = MagicMock()
    batch_query.return_value.execute = AsyncMock(
        return_value=[
            {"collection_id": iid, "config_data": data}
            for iid, data in fake_db.items()
        ]
    )
    monkeypatch.setattr(svc_mod._cq, "select_collection_configs_batch", batch_query)

    async def _select_single(_conn, *, collection_id, ref_key):
        return fake_db.get(collection_id)

    single_query = MagicMock()
    single_query.return_value.execute = AsyncMock(side_effect=_select_single)
    monkeypatch.setattr(svc_mod._cq, "select_collection_config", single_query)

    cids = ["col-a", "col-b", "col-c"]

    batched = await svc.get_configs_batch(
        ItemsPostgresqlDriverConfig, "cat1", cids,
        ctx=MagicMock(db_resource=MagicMock()),
    )

    per_cid = {
        cid: await svc.get_config(
            ItemsPostgresqlDriverConfig, catalog_id="cat1", collection_id=cid,
            ctx=MagicMock(db_resource=MagicMock()),
        )
        for cid in cids
    }

    for cid in cids:
        assert batched[cid].physical_table == per_cid[cid].physical_table

    # The batched path served all 3 collections with ONE collection-tier
    # query; the old loop needed one per collection.
    assert batch_query.return_value.execute.await_count == 1
    assert single_query.return_value.execute.await_count == 3
