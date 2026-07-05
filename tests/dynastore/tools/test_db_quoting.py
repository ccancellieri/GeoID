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
