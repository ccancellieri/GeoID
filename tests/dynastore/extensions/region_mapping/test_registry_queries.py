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

"""DB-free unit tests for ``registry_queries`` (dynastore#2821):

* DDL text is ``IF NOT EXISTS``-only, never ``ALTER``/``DROP``.
* ``ensure_mappings_table`` wires ``ensure_schema_exists`` + a sentinel+steps
  ``DDLBatch`` -- and is a no-op when the engine is unavailable.
* the CQL2 field mapping is restricted to :data:`ALLOWED_COLUMNS`, and
  ``parse_cql_filter`` against it accepts a known field, rejects an unknown
  one, and never string-interpolates a value (injection-shaped input stays
  a bind parameter).
* ``list_claims`` builds the expected SQL text + bind params for a given
  combination of filters.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# DDL text
# ---------------------------------------------------------------------------


def test_table_and_index_ddl_are_if_not_exists_only() -> None:
    from dynastore.extensions.region_mapping import registry_queries as rq

    for ddl_text in (rq._TABLE_DDL, rq._INDEX_DDL):
        upper = ddl_text.upper()
        assert "IF NOT EXISTS" in upper
        assert "ALTER" not in upper
        assert "DROP" not in upper

    assert "region_mapping.mappings" in rq._TABLE_DDL
    assert "region_mapping.mappings" in rq._INDEX_DDL


def test_table_ddl_declares_expected_columns() -> None:
    from dynastore.extensions.region_mapping import registry_queries as rq

    for column in rq.ALLOWED_COLUMNS:
        assert column in rq._TABLE_DDL, f"{column!r} missing from table DDL"

    assert "PRIMARY KEY" in rq._TABLE_DDL.upper()


# ---------------------------------------------------------------------------
# ensure_mappings_table wiring
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ensure_mappings_table_noop_when_engine_none() -> None:
    from dynastore.extensions.region_mapping.registry_queries import ensure_mappings_table

    await ensure_mappings_table(None)  # must not raise


@pytest.mark.asyncio
async def test_ensure_mappings_table_ensures_schema_then_runs_sentinel_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dynastore.extensions.region_mapping import registry_queries as rq

    conn = MagicMock(name="conn")

    @asynccontextmanager
    async def _fake_managed_transaction(engine: Any):
        assert engine is sentinel_engine
        yield conn

    schema_calls: List[str] = []

    async def _fake_ensure_schema_exists(passed_conn: Any, schema_name: str) -> None:
        assert passed_conn is conn
        schema_calls.append(schema_name)

    batch_calls: List[Dict[str, Any]] = []

    class _FakeBatch:
        def __init__(self, sentinel: Any, steps: List[Any]) -> None:
            self.sentinel = sentinel
            self.steps = steps

        async def execute(self, passed_conn: Any, **kwargs: Any) -> None:
            assert passed_conn is conn
            batch_calls.append({"sentinel": self.sentinel, "steps": self.steps, "kwargs": kwargs})

    monkeypatch.setattr(rq, "managed_transaction", _fake_managed_transaction)
    monkeypatch.setattr(rq, "ensure_schema_exists", _fake_ensure_schema_exists)
    monkeypatch.setattr(rq, "DDLBatch", _FakeBatch)

    sentinel_engine = object()
    await rq.ensure_mappings_table(sentinel_engine)

    assert schema_calls == [rq.SCHEMA]
    assert len(batch_calls) == 1
    assert len(batch_calls[0]["steps"]) == 2  # table DDL, then index DDL (sentinel)


# ---------------------------------------------------------------------------
# CQL2 field mapping + filter parsing
# ---------------------------------------------------------------------------


def test_build_cql_field_mapping_matches_allowed_columns() -> None:
    from dynastore.extensions.region_mapping.registry_queries import ALLOWED_COLUMNS, build_cql_field_mapping

    mapping = build_cql_field_mapping()
    assert set(mapping.keys()) == set(ALLOWED_COLUMNS)


def test_parse_cql_filter_accepts_known_field() -> None:
    from dynastore.modules.tools.cql import parse_cql_filter

    from dynastore.extensions.region_mapping.registry_queries import build_cql_field_mapping

    where, params = parse_cql_filter(
        "src_catalog = 'fao'", field_mapping=build_cql_field_mapping(), parser_type="cql2",
    )
    assert "src_catalog" in where
    assert "fao" in params.values()


def test_parse_cql_filter_rejects_unknown_field() -> None:
    from dynastore.modules.tools.cql import parse_cql_filter

    from dynastore.extensions.region_mapping.registry_queries import build_cql_field_mapping

    with pytest.raises(ValueError, match="Unknown propert"):
        parse_cql_filter(
            "not_a_real_column = 'x'", field_mapping=build_cql_field_mapping(), parser_type="cql2",
        )


def test_parse_cql_filter_binds_injection_shaped_value_as_parameter() -> None:
    """A value shaped like a SQL-injection payload must land in the bind
    param dict, never spliced into the WHERE text.

    Embedded single quotes are CQL2-Text-escaped (doubled) the way a real
    client would encode them; the parser must accept the literal and
    produce a single bound parameter carrying the raw, un-escaped value.
    """
    from dynastore.modules.tools.cql import parse_cql_filter

    from dynastore.extensions.region_mapping.registry_queries import build_cql_field_mapping

    payload = "'; DROP TABLE region_mapping.mappings; --"
    escaped = payload.replace("'", "''")
    where, params = parse_cql_filter(
        f"claim = '{escaped}'", field_mapping=build_cql_field_mapping(), parser_type="cql2",
    )
    assert "DROP TABLE" not in where
    assert payload in params.values()


# ---------------------------------------------------------------------------
# list_claims -- dynamic SQL/params builder
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_claims_builds_where_from_equality_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import registry_queries as rq

    captured: Dict[str, Any] = {}

    class _FakeDQLQuery:
        def __init__(self, sql: str, *, result_handler: Any) -> None:
            captured["sql"] = sql

        async def execute(self, db_resource: Any, **params: Any) -> List[Dict[str, Any]]:
            captured["params"] = params
            return []

    monkeypatch.setattr(rq, "DQLQuery", _FakeDQLQuery)

    await rq.list_claims(
        object(), mapping_id="fao_countries", role="primary", limit=50, offset=10,
    )

    sql = captured["sql"]
    assert "mapping_id = :mapping_id" in sql
    assert "role = :role" in sql
    assert "LIMIT :limit OFFSET :offset" in sql
    assert captured["params"] == {
        "mapping_id": "fao_countries", "role": "primary", "limit": 50, "offset": 10,
    }


@pytest.mark.asyncio
async def test_list_claims_with_no_filters_omits_where_clause(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import registry_queries as rq

    captured: Dict[str, Any] = {}

    class _FakeDQLQuery:
        def __init__(self, sql: str, *, result_handler: Any) -> None:
            captured["sql"] = sql

        async def execute(self, db_resource: Any, **params: Any) -> List[Dict[str, Any]]:
            return []

    monkeypatch.setattr(rq, "DQLQuery", _FakeDQLQuery)

    await rq.list_claims(object())

    assert "WHERE" not in captured["sql"]


@pytest.mark.asyncio
async def test_list_claims_embeds_cql_where_and_merges_params(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import registry_queries as rq

    captured: Dict[str, Any] = {}

    class _FakeDQLQuery:
        def __init__(self, sql: str, *, result_handler: Any) -> None:
            captured["sql"] = sql

        async def execute(self, db_resource: Any, **params: Any) -> List[Dict[str, Any]]:
            captured["params"] = params
            return []

    monkeypatch.setattr(rq, "DQLQuery", _FakeDQLQuery)

    await rq.list_claims(
        object(),
        src_catalog="fao",
        cql_where="claim_ci = :cqlp_0",
        cql_params={"cqlp_0": "country"},
    )

    sql = captured["sql"]
    assert "src_catalog = :src_catalog" in sql
    assert "(claim_ci = :cqlp_0)" in sql
    assert captured["params"]["cqlp_0"] == "country"
    assert captured["params"]["src_catalog"] == "fao"
