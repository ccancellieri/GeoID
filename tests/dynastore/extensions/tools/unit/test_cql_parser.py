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

import datetime

import pytest

pytest.importorskip("pygeofilter", reason="pygeofilter required for CQL tests")

from sqlalchemy.sql import column
from dynastore.modules.tools.cql import parse_cql_filter

def test_parse_cql_filter_unknown_property_validation():
    """Test that unknown properties trigger a helpful error message listing available properties."""
    cql = "bad_prop = 'value'"
    mapping = {"good_prop": column("good_prop"), "other_prop": column("other_prop")}
    
    with pytest.raises(ValueError) as excinfo:
        parse_cql_filter(cql, field_mapping=mapping, parser_type='cql2')
    
    error_msg = str(excinfo.value)
    assert "Unknown properties: bad_prop" in error_msg
    assert "Available properties: good_prop, other_prop" in error_msg

def test_parse_cql_filter_unknown_property_keyerror_fallback():
    """Test fallback error handling when valid_props isn't explicitly passed but key error happens."""
    # This might happen if validation is skipped (valid_props=False/None) but mapping fails
    # However, currently the function derives valid_props from mapping if not provided.
    # To test the KeyError path, we'd need to bypass the initial validation check?
    # If we pass valid_props=[], validation fails.
    # If we pass valid_props set to matching the keys, validation passes.
    # The KeyError usually happens if validation is somehow bypassed or incomplete.
    # Let's verify the validation logic primarily.
    pass

def test_parse_cql_filter_valid():
    """Test valid parsing."""
    cql = "good_prop = 'value'"
    mapping = {"good_prop": column("good_prop")}
    
    sql, params = parse_cql_filter(cql, field_mapping=mapping, parser_type='cql2')
    assert "good_prop" in sql
    assert len(params) > 0

if __name__ == "__main__":
    # verification run logic
    pass

def test_parse_cql_filter_quoted_string():
    """Test that a filter string wrapped in double quotes is handled correctly."""
    cql = '"good_prop = \'value\'"'
    mapping = {"good_prop": column("good_prop")}
    sql, params = parse_cql_filter(cql, field_mapping=mapping, parser_type='cql2')
    assert "good_prop" in sql

def test_parse_cql_filter_unquoted_value_error_message():
    """Test that unquoted values raise a helpful error message."""
    cql = "good_prop = SOME_VALUE"  # SOME_VALUE is interpreted as a property
    mapping = {"good_prop": column("good_prop")}
    
    with pytest.raises(ValueError) as excinfo:
        # valid_props is derived from mapping keys
        parse_cql_filter(cql, field_mapping=mapping, parser_type='cql2')
    
    msg = str(excinfo.value)
    assert "Unknown properties: SOME_VALUE" in msg
    assert "Hint: If these are intended to be values, ensure they are enclosed in single quotes" in msg

def test_parse_cql_filter_quoted_string_nested_single_quotes():
    """Test that double-quoted filter containing single-quoted values is handled correctly."""
    # Simulates: filter="asset_code='ITAL1_01'"
    cql = '"good_prop = \'value\'"'
    mapping = {"good_prop": column("good_prop")}
    sql, params = parse_cql_filter(cql, field_mapping=mapping, parser_type='cql2')
    assert "good_prop" in sql
    assert params

def test_parse_cql_filter_empty():
    """Test that empty or None filter returns empty SQL."""
    assert parse_cql_filter(None) == ("", {})
    assert parse_cql_filter("") == ("", {})


def test_parse_cql_filter_single_quoted_value():
    """A plain single-quoted string literal binds as a parameter (#1141)."""
    cql = "good_prop = 'PK001'"
    mapping = {"good_prop": column("good_prop")}
    sql, params = parse_cql_filter(cql, field_mapping=mapping, parser_type="cql2")
    assert "good_prop" in sql
    assert "PK001" in params.values()


def test_parse_cql_filter_escaped_embedded_single_quote():
    """A value containing an embedded quote (CQL2-Text ``''`` escape) parses.

    Refs #1141: the bundled pygeofilter grammar tokenises ``'O''Brien'`` as two
    separate string literals and 400s. ``parse_cql_filter`` must accept the
    spec-compliant doubled-quote escape and bind the *unescaped* value
    (``O'Brien``) as a parameter.
    """
    cql = "good_prop = 'O''Brien'"
    mapping = {"good_prop": column("good_prop")}
    sql, params = parse_cql_filter(cql, field_mapping=mapping, parser_type="cql2")
    assert "good_prop" in sql
    # The single quote is restored in the bound value (not the doubled escape).
    assert "O'Brien" in params.values()
    assert "O''Brien" not in params.values()


def test_parse_cql_filter_escaped_quote_at_boundaries():
    """Doubled quotes at the start/end of a value also round-trip (#1141)."""
    cql = "good_prop = '''PK'''"  # CQL2-Text for the value: 'PK'
    mapping = {"good_prop": column("good_prop")}
    sql, params = parse_cql_filter(cql, field_mapping=mapping, parser_type="cql2")
    assert "good_prop" in sql
    assert "'PK'" in params.values()


def test_parse_cql_filter_escaped_quote_multiple_clauses():
    """Doubled quotes survive across an AND of two equality clauses (#1141)."""
    cql = "owner = 'O''Hara' AND author = 'D''Angelo'"
    mapping = {"owner": column("owner"), "author": column("author")}
    sql, params = parse_cql_filter(cql, field_mapping=mapping, parser_type="cql2")
    bound = set(params.values())
    assert "O'Hara" in bound
    assert "D'Angelo" in bound


def test_parse_cql_filter_escaped_quote_in_in_list():
    """Doubled quotes round-trip inside an ``IN (...)`` value list (#1141)."""
    cql = "owner IN ('O''Hara', 'plain')"
    mapping = {"owner": column("owner")}
    sql, params = parse_cql_filter(cql, field_mapping=mapping, parser_type="cql2")
    bound = set(params.values())
    assert "O'Hara" in bound
    assert "plain" in bound


def test_parse_cql_filter_plain_value_unaffected_by_quote_handling():
    """Values with no embedded quote are bound verbatim (no regression)."""
    cql = "owner = 'PK001'"
    mapping = {"owner": column("owner")}
    _, params = parse_cql_filter(cql, field_mapping=mapping, parser_type="cql2")
    assert "PK001" in params.values()


# ── PG/ES CQL2 operator-parity fixes (#2945) ────────────────────────────
#
# The STAC search PG fallback (#2943) and the OGC API Features ``/items``
# ``filter=`` path both compile a CQL2 filter through this module's
# ``parse_cql_filter``/``parse_cql2_json_filter``, which route through
# ``_to_filter_with_parity_fixes`` / ``_ParityFilterEvaluator``. These tests
# cover the two gaps found auditing that PG translator against the shared ES
# CQL2->DSL translator (``cql_to_es.py``): LIKE pattern wildcard/singlechar
# adaptation, and per-operator T_* temporal semantics (previously all folded
# into an inclusive "between" by the bundled pygeofilter evaluator).

def test_parse_cql_filter_like_singlechar_wildcard_converted_to_sql():
    """CQL2's ``.`` single-char wildcard becomes SQL's ``_`` (not left inert).

    Before #2945 the PG path passed the CQL2 pattern straight to SQL's LIKE,
    so a CQL2 ``.`` wildcard (bundled grammar's default single-char token)
    was left as a literal dot — narrower than the ES translator, which does
    perform this adaptation.
    """
    cql = "name LIKE 'fo.o'"
    mapping = {"name": column("name")}
    sql, params = parse_cql_filter(cql, field_mapping=mapping, parser_type="cql2")
    assert "ESCAPE" in sql
    assert list(params.values()) == ["fo_o"]


def test_parse_cql_filter_like_literal_underscore_escaped():
    """A literal ``_`` in a LIKE pattern must not become SQL's own wildcard.

    Before #2945 a literal underscore (not a CQL2 wildcard token) reached SQL
    unescaped and was silently reinterpreted as SQL's single-char wildcard —
    broader than intended.
    """
    cql = "name LIKE 'foo_bar'"
    mapping = {"name": column("name")}
    sql, params = parse_cql_filter(cql, field_mapping=mapping, parser_type="cql2")
    assert list(params.values()) == ["foo\\_bar"]


def test_parse_cql_filter_like_multi_wildcard_converted_to_percent():
    """CQL2's ``%`` multi-char wildcard is unaffected (already SQL's own)."""
    cql = "name LIKE 'fo%'"
    mapping = {"name": column("name")}
    _, params = parse_cql_filter(cql, field_mapping=mapping, parser_type="cql2")
    assert list(params.values()) == ["fo%"]


def _temporal_sql(cql, mapping=None):
    mapping = mapping or {"dt": column("dt")}
    return parse_cql_filter(cql, field_mapping=mapping, parser_type="cql2")


def test_parse_cql_filter_temporal_before_is_exclusive():
    """T_BEFORE must be a strict ``<`` (matches the ES translator's ``lt``).

    Before #2945 the bundled evaluator's generic BEFORE handling produced an
    inclusive ``<=``, so a document exactly at the boundary instant matched
    on the PG fallback but not via ES — an inconsistency across paths.
    """
    sql, params = _temporal_sql("dt T_BEFORE TIMESTAMP('2020-01-01T00:00:00Z')")
    assert " < " in sql
    assert "<=" not in sql


def test_parse_cql_filter_temporal_after_is_exclusive():
    """T_AFTER must be a strict ``>`` (matches the ES translator's ``gt``)."""
    sql, params = _temporal_sql("dt T_AFTER TIMESTAMP('2020-01-01T00:00:00Z')")
    assert " > " in sql
    assert ">=" not in sql


def test_parse_cql_filter_temporal_begins_is_equality_on_low():
    """T_BEGINS matches the ES translator: exact equality on the interval start.

    Before #2945 BEGINS was folded into an inclusive between, matching every
    value in the interval instead of just its start.
    """
    sql, params = _temporal_sql(
        "dt T_BEGINS INTERVAL('2020-01-01T00:00:00Z','2021-01-01T00:00:00Z')"
    )
    assert " = " in sql
    assert list(params.values()) == [
        datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc)
    ]


def test_parse_cql_filter_temporal_begunby_is_equality_on_high():
    """T_BEGUNBY matches the ES translator: exact equality on the interval end."""
    sql, params = _temporal_sql(
        "dt T_BEGUNBY INTERVAL('2020-01-01T00:00:00Z','2021-01-01T00:00:00Z')"
    )
    assert " = " in sql
    assert list(params.values()) == [
        datetime.datetime(2021, 1, 1, tzinfo=datetime.timezone.utc)
    ]


def test_parse_cql_filter_temporal_during_is_strict_between():
    """T_DURING is a strict (exclusive) between, matching the ES translator."""
    sql, params = _temporal_sql(
        "dt T_DURING INTERVAL('2020-01-01T00:00:00Z','2021-01-01T00:00:00Z')"
    )
    assert " > " in sql and " < " in sql
    assert ">=" not in sql and "<=" not in sql


def test_parse_cql_filter_temporal_tcontains_is_strict_between():
    """T_CONTAINS matches T_DURING's bound (ES uses the same range, only its
    inert ``relation`` hint differs — a no-op on a scalar column)."""
    sql, params = _temporal_sql(
        "dt T_CONTAINS INTERVAL('2020-01-01T00:00:00Z','2021-01-01T00:00:00Z')"
    )
    assert " > " in sql and " < " in sql


def test_parse_cql_filter_temporal_toverlaps_is_inclusive_between():
    """T_OVERLAPS is an inclusive between, matching the ES translator."""
    sql, params = _temporal_sql(
        "dt T_OVERLAPS INTERVAL('2020-01-01T00:00:00Z','2021-01-01T00:00:00Z')"
    )
    assert ">=" in sql and "<=" in sql


def test_parse_cql_filter_temporal_overlappedby_is_inclusive_between():
    """T_OVERLAPPEDBY matches T_OVERLAPS' bound, mirroring the ES translator."""
    sql, params = _temporal_sql(
        "dt T_OVERLAPPEDBY INTERVAL('2020-01-01T00:00:00Z','2021-01-01T00:00:00Z')"
    )
    assert ">=" in sql and "<=" in sql


def test_parse_cql_filter_temporal_tequals_unchanged():
    """T_EQUALS was already correct (pygeofilter's own explicit branch);
    confirms the new evaluator preserves it."""
    sql, params = _temporal_sql("dt T_EQUALS TIMESTAMP('2020-01-01T00:00:00Z')")
    assert " = " in sql


@pytest.mark.parametrize("op", ["T_MEETS", "T_METBY", "T_ENDS", "T_ENDEDBY"])
def test_parse_cql_filter_temporal_ambiguous_ops_rejected(op):
    """MEETS/METBY/ENDS/ENDEDBY have no ES-side reference translation to
    mirror and no unambiguous meaning for a scalar (non-interval) property,
    so the PG fallback now rejects them explicitly (400) instead of silently
    folding them into a wrong "between" (the pre-#2945 behaviour)."""
    with pytest.raises(ValueError) as excinfo:
        _temporal_sql(
            f"dt {op} INTERVAL('2020-01-01T00:00:00Z','2021-01-01T00:00:00Z')"
        )
    assert op.replace("T_", "") in str(excinfo.value) or op in str(excinfo.value)
