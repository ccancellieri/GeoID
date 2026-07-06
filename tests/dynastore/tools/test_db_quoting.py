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

"""Identifier-quoting toolkit for safe f-string SQL interpolation (issue #2700).

``quote_ident``/``qualify_table``/``qualify_column`` are the shared primitives
that replace the ad-hoc ``f'"{schema}"'``-style interpolation scattered across
the codebase — quoting with proper ``"`` doubling, plus (for the ``qualify_*``
helpers) validation via the existing ``validate_sql_identifier``/
``validate_column_identifier`` guards before interpolation.
"""

from __future__ import annotations

import pytest

from dynastore.tools.db import (
    InvalidIdentifierError,
    build_upsert,
    qualify_column,
    qualify_table,
    quote_ident,
)


# ---------------------------------------------------------------------------
# quote_ident
# ---------------------------------------------------------------------------


def test_quote_ident_wraps_plain_identifier():
    assert quote_ident("my_schema") == '"my_schema"'


def test_quote_ident_escapes_embedded_double_quote():
    # The one thing every unquoted/naively-quoted f-string site was missing.
    assert quote_ident('a"b') == '"a""b"'


def test_quote_ident_escapes_multiple_embedded_quotes():
    assert quote_ident('a"b"c') == '"a""b""c"'


def test_quote_ident_rejects_non_string():
    with pytest.raises(TypeError):
        quote_ident(123)  # type: ignore[arg-type]


def test_quote_ident_is_idempotent_on_already_quoted_input():
    assert quote_ident('"my_schema"') == '"my_schema"'


def test_quote_ident_wraps_star_by_default():
    # Without allow_star, "*" is just another identifier and gets quoted.
    assert quote_ident("*") == '"*"'


def test_quote_ident_passes_through_star_when_allowed():
    assert quote_ident("*", allow_star=True) == "*"


def test_quote_ident_escapes_non_star_even_when_allow_star_set():
    assert quote_ident('a"b', allow_star=True) == '"a""b"'


# ---------------------------------------------------------------------------
# qualify_table
# ---------------------------------------------------------------------------


def test_qualify_table_quotes_both_parts():
    assert qualify_table("my_schema", "notebooks") == '"my_schema"."notebooks"'


def test_qualify_table_rejects_invalid_schema():
    with pytest.raises(InvalidIdentifierError):
        qualify_table("bad schema", "notebooks")


def test_qualify_table_rejects_invalid_table():
    with pytest.raises(InvalidIdentifierError):
        qualify_table("my_schema", "bad.table")


def test_qualify_table_rejects_reserved_keyword_table():
    with pytest.raises(InvalidIdentifierError):
        qualify_table("my_schema", "select")


# ---------------------------------------------------------------------------
# qualify_column
# ---------------------------------------------------------------------------


def test_qualify_column_quotes_identifier():
    assert qualify_column("geoid") == '"geoid"'


def test_qualify_column_preserves_case():
    assert qualify_column("catalogId") == '"catalogId"'


def test_qualify_column_rejects_invalid_name():
    with pytest.raises(InvalidIdentifierError):
        qualify_column("bad name")


# ---------------------------------------------------------------------------
# build_upsert
# ---------------------------------------------------------------------------


def test_build_upsert_simple_single_pk():
    sql = build_upsert(
        table='"my_schema"."widgets"',
        columns=["id", "name", "price"],
        conflict_cols=["id"],
    )
    assert sql == (
        'INSERT INTO "my_schema"."widgets" ("id", "name", "price") '
        "VALUES (:id, :name, :price) "
        'ON CONFLICT ("id") DO UPDATE SET '
        '"name" = EXCLUDED."name", "price" = EXCLUDED."price";'
    )


def test_build_upsert_escapes_identifier_needing_quoting():
    # A column name carrying an embedded double quote must come out escaped
    # the same way quote_ident escapes it standalone.
    sql = build_upsert(
        table='"s"."t"',
        columns=['weird"col', "id"],
        conflict_cols=["id"],
    )
    assert '"weird""col"' in sql
    assert 'SET "weird""col" = EXCLUDED."weird""col"' in sql


def test_build_upsert_explicit_update_cols_is_a_strict_subset():
    # A caller can refresh only some of the non-conflict columns on update —
    # e.g. a freshness column stays untouched by an explicit list.
    sql = build_upsert(
        table='"s"."t"',
        columns=["notebook_id", "title", "owner_id"],
        conflict_cols=["notebook_id"],
        update_cols=["title"],
    )
    assert 'ON CONFLICT ("notebook_id") DO UPDATE SET "title" = EXCLUDED."title";' in sql
    assert "owner_id" not in sql.split("DO UPDATE SET")[1]


def test_build_upsert_empty_update_cols_renders_do_nothing():
    sql = build_upsert(
        table='"s"."t"',
        columns=["notebook_id", "title"],
        conflict_cols=["notebook_id"],
        update_cols=[],
    )
    assert "ON CONFLICT (\"notebook_id\") DO NOTHING;" in sql


def test_build_upsert_literal_values_used_in_values_clause_only():
    # literal_values overrides the VALUES-clause entry for a column but the
    # UPDATE SET clause still uses the ordinary EXCLUDED form.
    sql = build_upsert(
        table='"s"."t"',
        columns=["ref_key", "config_data", "updated_at"],
        conflict_cols=["ref_key"],
        literal_values={
            "config_data": "CAST(:config_data AS jsonb)",
            "updated_at": "NOW()",
        },
    )
    assert "VALUES (:ref_key, CAST(:config_data AS jsonb), NOW())" in sql
    assert '"updated_at" = EXCLUDED."updated_at"' in sql
    assert '"config_data" = EXCLUDED."config_data"' in sql


def test_build_upsert_returning_clause():
    sql = build_upsert(
        table='"s"."t"',
        columns=["id", "name"],
        conflict_cols=["id"],
        returning=["id", "name"],
    )
    assert sql.endswith('RETURNING "id", "name";')


def test_build_upsert_rejects_empty_columns():
    with pytest.raises(ValueError):
        build_upsert(table='"s"."t"', columns=[], conflict_cols=["id"])


def test_build_upsert_rejects_empty_conflict_cols():
    with pytest.raises(ValueError):
        build_upsert(table='"s"."t"', columns=["id"], conflict_cols=[])
