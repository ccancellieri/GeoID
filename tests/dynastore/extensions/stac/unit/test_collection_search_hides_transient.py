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

"""STAC collection search hides transient (non-ACTIVE) collections (#2194).

``search_collections`` builds its own UNION SQL (it does not route through the
collection drivers), so it carries the lifecycle predicate itself: a non-null
``lifecycle_status`` overlay (#2066) marks a collection mid-provisioning or
mid-hard-delete and it must not surface in search. The predicate lives in the
per-catalog subquery WHERE that backs both the CTE COUNT and the paged data
query. Verified DB-free by capturing the SQL.
"""

from __future__ import annotations

import pytest

import dynastore.extensions.stac.search as stac_search
from dynastore.extensions.stac.search import CollectionSearchRequest, ResultHandler


class _FakeCatalogs:
    async def resolve_physical_schema(self, _cid, ctx=None):
        return "phys_sch_test"


@pytest.mark.asyncio
async def test_search_collections_excludes_non_active(monkeypatch):
    captured: list[str] = []

    class _DQLStub:
        def __init__(self, sql, result_handler=None, **_kw):
            self.sql = sql
            self.result_handler = result_handler
            captured.append(sql)

        async def execute(self, *_a, **_kw):
            # Count first (truthy so the data query also runs); then the rows.
            if self.result_handler == ResultHandler.SCALAR_ONE_OR_NONE:
                return 1
            return []

    async def _none(*_a, **_kw):
        return None

    import dynastore.models.protocols.visibility as visibility

    monkeypatch.setattr(stac_search, "DQLQuery", _DQLStub)
    monkeypatch.setattr(stac_search, "get_protocol", lambda _proto: _FakeCatalogs())
    monkeypatch.setattr(visibility, "resolve_catalog_listing_ids", _none)
    monkeypatch.setattr(visibility, "resolve_collection_listing_ids", _none)

    req = CollectionSearchRequest(catalog_id="cat", limit=10, offset=0)
    # db_resource=None: the stubbed DQLQuery ignores the connection, and
    # DriverContext only accepts None or a real SQLAlchemy resource.
    collections, total = await stac_search.search_collections(None, req)

    assert collections == [] and total == 1
    # Both the COUNT CTE and the paged data query embed the per-catalog
    # subquery, so the lifecycle predicate must appear in each.
    assert len(captured) == 2, captured
    for sql in captured:
        assert "deleted_at IS NULL" in sql
        assert "c.lifecycle_status IS NULL" in sql
