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

"""Cycle F.4c.2 — pin the ref-keyed read API on ``ConfigsProtocol``.

Static-shape pin tests:

1. ``ConfigsProtocol`` carries the new methods ``list_refs_at_scope`` and
   ``get_config_by_ref``.
2. New query factories produce SQL filtered on ``ref_key`` (the F.4c.1 PK)
   and surface ``class_key`` alongside ``config_data`` so callers can
   resolve the dispatch class from the row.
3. ``list_*_refs`` queries return a lightweight ``ref_key, class_key``
   projection (no payload column → cheap enumeration).

End-to-end behaviour against a live DB is exercised by post-DB-reset
integration runs; this slice is read-API plumbing on top of F.4c.1
storage.
"""

from __future__ import annotations

from dynastore.models.protocols.configs import ConfigsProtocol
from dynastore.modules.db_config.typed_store import config_queries as _cq


# ---------------------------------------------------------------------------
# Protocol surface
# ---------------------------------------------------------------------------


def test_protocol_carries_list_refs_at_scope():
    assert hasattr(ConfigsProtocol, "list_refs_at_scope")


def test_protocol_carries_get_config_by_ref():
    assert hasattr(ConfigsProtocol, "get_config_by_ref")


# ---------------------------------------------------------------------------
# Platform-scope queries
# ---------------------------------------------------------------------------


def test_get_platform_config_by_ref_query_shape():
    """Surface class_key + config_data so callers can resolve the dispatch
    class from the row, and filter on ref_key (the F.4c.1 PK)."""
    sql = _cq.get_platform_config_by_ref.template.lower()
    assert "select class_key, config_data" in sql
    assert "where ref_key = :ref_key" in sql
    assert "configs.platform_configs" in sql


def test_list_platform_refs_query_shape():
    """``list_*_refs`` is a lightweight ``{ref_key, class_key}`` projection
    (no config_data column → operators can enumerate refs without paying
    JSON deserialise costs)."""
    sql = _cq.list_platform_refs.template.lower()
    assert "select ref_key, class_key" in sql
    assert "config_data" not in sql
    assert "configs.platform_configs" in sql


# ---------------------------------------------------------------------------
# Tenant-scope queries (catalog + collection)
# ---------------------------------------------------------------------------


def test_select_catalog_config_by_ref_query_shape():
    sql = _cq.select_catalog_config_by_ref("demo_schema").template.lower()
    assert "select class_key, config_data" in sql
    assert "where ref_key = :ref_key" in sql
    assert '"demo_schema".catalog_configs' in sql


def test_select_collection_config_by_ref_query_shape():
    sql = _cq.select_collection_config_by_ref("demo_schema").template.lower()
    assert "select class_key, config_data" in sql
    assert "where collection_id = :collection_id and ref_key = :ref_key" in sql
    assert '"demo_schema".collection_configs' in sql


def test_list_catalog_refs_query_shape():
    sql = _cq.list_catalog_refs("demo_schema").template.lower()
    assert "select ref_key, class_key" in sql
    assert "config_data" not in sql
    assert '"demo_schema".catalog_configs' in sql


def test_list_collection_refs_query_shape():
    """Collection refs are scoped per ``collection_id`` — pin that filter."""
    sql = _cq.list_collection_refs("demo_schema").template.lower()
    assert "select ref_key, class_key" in sql
    assert "where collection_id = :collection_id" in sql
    assert '"demo_schema".collection_configs' in sql


def test_factory_validates_phys_schema():
    """The tenant query factories must reject unsafe identifiers (uses the
    same validate_sql_identifier guard as ``tenant_configs_ddl``)."""
    import pytest
    from dynastore.tools.db import InvalidIdentifierError

    for factory in (
        _cq.select_catalog_config_by_ref,
        _cq.list_catalog_refs,
        _cq.select_collection_config_by_ref,
        _cq.list_collection_refs,
    ):
        with pytest.raises(InvalidIdentifierError):
            factory("bad name; DROP TABLE x;")
