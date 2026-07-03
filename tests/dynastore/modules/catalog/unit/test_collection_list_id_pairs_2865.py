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

"""``list_collection_id_pairs`` (#2865) reads (id, external_id) straight off
the thin PG registry instead of hydrating every collection via the routed
metadata store — the O(N) hydration loop that hung ``/search`` on a
1,971-collection catalog.

Mirrors the sibling coverage for ``_make_collection_list_ids_query`` in
``test_collection_registry_list_ids_hides_transient.py``: pure SQL
inspection for the query builder (no DB connection required), plus a
mocked-service test proving ``CollectionService.list_collection_id_pairs``
runs a single query over the resolved physical schema and shapes the
returned rows into ``(id, external_id)`` tuples.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from dynastore.modules.catalog.collection_service import (
    CollectionService,
    _make_collection_list_id_pairs_query,
)

CATALOG_ID = "demo_catalog"


def test_id_pairs_query_excludes_non_active() -> None:
    """Same visibility predicate as the ids-only query: provisioning/deleting
    collections must not leak into the search-expansion id set."""
    query = _make_collection_list_id_pairs_query("phys_sch_test")
    sql = query.template
    assert sql, "expected a non-empty SQL template"
    assert "deleted_at IS NULL" in sql
    assert "lifecycle_status IS NULL" in sql


def test_id_pairs_query_selects_id_and_external_id() -> None:
    query = _make_collection_list_id_pairs_query("phys_sch_test")
    sql = query.template
    assert "SELECT id, external_id" in sql
    assert "ORDER BY id" in sql, "stable ordering is required"


def test_id_pairs_query_uses_provided_schema() -> None:
    query = _make_collection_list_id_pairs_query("my_custom_schema")
    sql = query.template
    assert '"my_custom_schema"' in sql


def test_id_pairs_query_is_unpaginated() -> None:
    """Unlike the ids-only query, this one has no LIMIT/OFFSET — the old
    ``list_collections(limit=1000)`` truncation is what silently dropped
    ~971 collections from search scope on the fao catalog."""
    query = _make_collection_list_id_pairs_query("phys_sch_test")
    sql = query.template
    assert ":limit" not in sql
    assert ":offset" not in sql


def _make_service() -> CollectionService:
    """Bare CollectionService without running __init__/setup."""
    return CollectionService.__new__(CollectionService)


class _NullCM:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, *a):
        return False


@pytest.mark.asyncio
async def test_list_collection_id_pairs_runs_single_query_over_resolved_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Returns exactly the rows the query executes to, shaped as
    ``(id, external_id)`` tuples, via one round-trip inside a managed
    transaction reusing the caller's db_resource."""
    import dynastore.modules.catalog.collection_service as mod

    svc = _make_service()
    svc.engine = None
    svc._resolve_physical_schema = AsyncMock(return_value="phys_sch_demo")  # type: ignore[method-assign]

    rows = [("col_0001", "ext-0001"), ("col_0002", None)]
    executed_queries = []

    class _FakeQuery:
        def __init__(self, template):
            self.template = template

        async def execute(self, conn):
            executed_queries.append(self.template)
            return rows

    monkeypatch.setattr(
        mod, "_make_collection_list_id_pairs_query",
        lambda phys_schema: _FakeQuery(f"SELECT ... {phys_schema}"),
    )
    monkeypatch.setattr(mod, "managed_transaction", lambda *a, **k: _NullCM())
    monkeypatch.setattr(mod, "get_protocol", lambda *a, **k: None)

    result = await svc.list_collection_id_pairs(CATALOG_ID)

    assert result == [("col_0001", "ext-0001"), ("col_0002", None)]
    assert len(executed_queries) == 1


@pytest.mark.asyncio
async def test_list_collection_id_pairs_returns_empty_when_no_physical_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dynastore.modules.catalog.collection_service as mod

    svc = _make_service()
    svc.engine = None
    svc._resolve_physical_schema = AsyncMock(return_value=None)  # type: ignore[method-assign]

    monkeypatch.setattr(mod, "managed_transaction", lambda *a, **k: _NullCM())
    monkeypatch.setattr(mod, "get_protocol", lambda *a, **k: None)

    result = await svc.list_collection_id_pairs(CATALOG_ID)

    assert result == []
