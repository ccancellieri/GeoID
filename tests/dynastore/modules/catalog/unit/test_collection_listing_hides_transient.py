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

"""Collection listing/search hides transient (non-ACTIVE) collections (#2194).

A non-null ``lifecycle_status`` overlay (#2066) means the collection is
mid-provisioning or mid-hard-delete — write-gated and not a live catalog member.
The CORE PostgreSQL search driver backs ``list_collections`` (the OGC Features
and STAC plain listings funnel through it, for both the SEARCH and READ routing
ops), so its query must exclude those rows in BOTH the COUNT and the paged data
query — otherwise the page total disagrees with the rows. Verified DB-free by
capturing the SQL the driver builds.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

import dynastore.modules.storage.drivers.core_postgresql as core_pg
from dynastore.modules.storage.drivers.core_postgresql import (
    CollectionCorePostgresqlDriver,
    ResultHandler,
)


@asynccontextmanager
async def _stub_txn(_engine):
    yield object()


def _patch_driver_io(monkeypatch, captured):
    """Stub the driver's DB plumbing and record every SQL string it builds."""

    async def _phys(_catalog_id, db_resource=None):
        return "phys_sch_test"

    async def _no_visibility_constraint(_catalog_id):
        return None  # no authorization layer ⟹ unfiltered listing

    class _DQLStub:
        def __init__(self, sql, result_handler=None, **_kw):
            self.sql = sql
            self.result_handler = result_handler
            captured.append(sql)

        async def execute(self, *_a, **_kw):
            if self.result_handler == ResultHandler.SCALAR:
                return 0
            return []

    import dynastore.models.protocols.visibility as visibility

    monkeypatch.setattr(core_pg, "_resolve_physical_schema", _phys)
    monkeypatch.setattr(core_pg, "managed_transaction", _stub_txn)
    monkeypatch.setattr(core_pg, "DQLQuery", _DQLStub)
    monkeypatch.setattr(
        visibility, "resolve_collection_listing_ids", _no_visibility_constraint
    )


@pytest.mark.asyncio
async def test_search_metadata_excludes_non_active_in_count_and_data(monkeypatch):
    captured: list[str] = []
    _patch_driver_io(monkeypatch, captured)

    drv = CollectionCorePostgresqlDriver.__new__(CollectionCorePostgresqlDriver)
    rows, total = await drv.search_metadata(
        "cat", q=None, limit=10, offset=0, db_resource=object()
    )

    assert rows == [] and total == 0
    # Both the COUNT and the data SELECT must carry the predicate so the page
    # total never counts a collection the data query then hides.
    assert len(captured) == 2, captured
    for sql in captured:
        assert "c.deleted_at IS NULL" in sql
        assert "c.lifecycle_status IS NULL" in sql


@pytest.mark.asyncio
async def test_search_metadata_predicate_present_with_query_term(monkeypatch):
    """The lifecycle filter survives alongside a free-text ``q`` predicate."""
    captured: list[str] = []
    _patch_driver_io(monkeypatch, captured)

    drv = CollectionCorePostgresqlDriver.__new__(CollectionCorePostgresqlDriver)
    await drv.search_metadata("cat", q="rain", limit=5, offset=0, db_resource=object())

    assert captured, "expected the driver to build SQL"
    for sql in captured:
        assert "c.lifecycle_status IS NULL" in sql
        assert "ILIKE" in sql  # the q predicate is still there
