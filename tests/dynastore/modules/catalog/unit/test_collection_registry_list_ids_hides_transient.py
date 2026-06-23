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

"""PG registry id-list query hides transient collections (#2194).

The unfiltered listing path — used by OGC Features GET /collections,
STAC plain-listing, and every other caller that passes ``q=None`` to
``list_collections`` — enumerates collection ids from the thin PG
registry via ``_make_collection_list_ids_query`` and then hydrates each
one.  If the WHERE clause only filters ``deleted_at IS NULL`` without
also filtering ``lifecycle_status IS NULL``, collections mid-provisioning
or mid-hard-delete (#2066 overlay) surface in the listing as if they were
live.

This test verifies that the query built by the function carries both
predicates, keeping it consistent with ``CollectionCorePostgresqlDriver``
``search_metadata`` and the STAC ``search_collections`` query (both
already covered separately).  Pure SQL-inspection — no DB connection
required.
"""

from __future__ import annotations

from dynastore.modules.catalog.collection_service import (
    _make_collection_list_ids_query,
)


def test_list_ids_query_excludes_non_active() -> None:
    """The id-list SELECT carries ``lifecycle_status IS NULL`` (#2194)."""
    query = _make_collection_list_ids_query("phys_sch_test")
    sql = query.template
    assert sql, "expected a non-empty SQL template"
    assert "deleted_at IS NULL" in sql, (
        "basic soft-delete filter must be present"
    )
    assert "lifecycle_status IS NULL" in sql, (
        "lifecycle-overlay filter must be present so provisioning/deleting "
        "collections do not appear in the unfiltered listing path"
    )


def test_list_ids_query_uses_provided_schema() -> None:
    """The schema name is interpolated safely via the f-string."""
    query = _make_collection_list_ids_query("my_custom_schema")
    sql = query.template
    assert '"my_custom_schema"' in sql


def test_list_ids_query_has_pagination() -> None:
    """LIMIT and OFFSET placeholders are present for deterministic pagination."""
    query = _make_collection_list_ids_query("phys_sch_test")
    sql = query.template
    assert ":limit" in sql
    assert ":offset" in sql
    assert "ORDER BY id" in sql, "stable ordering is required for consistent pagination"
