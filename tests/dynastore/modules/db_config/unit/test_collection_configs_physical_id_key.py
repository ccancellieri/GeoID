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

"""Pin tests for the collection_configs physical_id re-key.

collection_configs.collection_id renamed to physical_id with a matching PK change.
The table now keys on (physical_id, ref_key) where physical_id is the immutable
c_... token from {schema}.collections.physical_id.

These are pure-unit tests: no live DB required.
"""

from __future__ import annotations

from dynastore.modules.db_config.typed_store import config_queries as _cq
from dynastore.modules.db_config.typed_store.ddl import tenant_configs_ddl


_SCHEMA = "test_tenant"


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------


def test_ddl_uses_physical_id_column():
    """tenant_configs_ddl must declare physical_id, not collection_id."""
    ddl = tenant_configs_ddl(_SCHEMA)
    assert "physical_id" in ddl
    assert "collection_id" not in ddl


def test_ddl_primary_key_on_physical_id_and_ref_key():
    """The PK must be (physical_id, ref_key)."""
    ddl = tenant_configs_ddl(_SCHEMA)
    assert "PRIMARY KEY (physical_id, ref_key)" in ddl


# ---------------------------------------------------------------------------
# Query factories — all 7 collection-scoped factories
# ---------------------------------------------------------------------------


def test_select_collection_config_uses_physical_id():
    sql = _cq.select_collection_config(_SCHEMA).template.lower()
    assert "where physical_id = :physical_id and ref_key = :ref_key" in sql
    assert "collection_id" not in sql


def test_select_collection_config_by_ref_uses_physical_id():
    sql = _cq.select_collection_config_by_ref(_SCHEMA).template.lower()
    assert "where physical_id = :physical_id and ref_key = :ref_key" in sql
    assert "collection_id" not in sql


def test_list_collection_refs_uses_physical_id():
    sql = _cq.list_collection_refs(_SCHEMA).template.lower()
    assert "where physical_id = :physical_id" in sql
    assert "collection_id" not in sql


def test_select_collection_config_for_update_uses_physical_id():
    sql = _cq.select_collection_config_for_update(_SCHEMA).template.lower()
    assert "where physical_id = :physical_id and ref_key = :ref_key" in sql
    assert "collection_id" not in sql


def test_upsert_collection_config_uses_physical_id():
    sql = _cq.upsert_collection_config(_SCHEMA).template.lower()
    assert "physical_id" in sql
    assert "on conflict (physical_id, ref_key)" in sql
    assert "collection_id" not in sql


def test_delete_collection_config_uses_physical_id():
    sql = _cq.delete_collection_config(_SCHEMA).template.lower()
    assert "where physical_id = :physical_id and ref_key = :ref_key" in sql
    assert "collection_id" not in sql


def test_list_collection_configs_paginated_uses_physical_id():
    sql = _cq.list_collection_configs_paginated(_SCHEMA).template.lower()
    assert "where physical_id = :physical_id" in sql
    assert "collection_id" not in sql
