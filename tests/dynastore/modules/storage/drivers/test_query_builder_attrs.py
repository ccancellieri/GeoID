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

"""Unit tests for the PG AccessFilter translator with ``_attrs.*`` (#1441).

Covers:
- _attrs.dept predicate → JSONB path SQL fragment
- First-class column predicates (catalog_id, collection_id) → skipped
- deny_all → FALSE clause
- allow_all with no predicates → None (no restriction)
- Multiple allow clauses → OR of fragments
"""
from __future__ import annotations

from dynastore.models.protocols.access_filter import (
    AccessClause,
    AccessFilter,
    FieldPredicate,
)
from dynastore.modules.storage.access_filter_pg import access_filter_to_pg_clause


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _filter(*clauses: AccessClause) -> AccessFilter:
    return AccessFilter(allow=tuple(clauses))


def _clause(*preds: FieldPredicate) -> AccessClause:
    return AccessClause(predicates=preds)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_attrs_dept_predicate_generates_jsonb_lookup():
    """``_attrs.dept IN ('finance', 'global')`` → JSONB path SQL."""
    af = _filter(
        _clause(FieldPredicate("_attrs.dept", ("finance", "global")))
    )
    sql, params = access_filter_to_pg_clause(af, envelope_col="access_envelope")

    assert sql is not None
    assert "access_envelope" in sql
    assert "->'attrs'" in sql
    assert "dept" in sql
    assert "ANY(" in sql
    # Param dict must have the values.
    assert any("finance" in str(v) for v in params.values())


def test_first_class_column_predicate_skipped():
    """``catalog_id`` is a first-class column — not translated to JSONB lookup."""
    af = _filter(
        _clause(FieldPredicate("catalog_id", ("cat1",)))
    )
    sql, params = access_filter_to_pg_clause(af)
    # catalog_id is handled by the caller's column predicates, not here.
    assert sql is None
    assert params == {}


def test_deny_all_returns_false():
    """deny_all → SQL FALSE clause."""
    af = AccessFilter(deny_all=True)
    sql, params = access_filter_to_pg_clause(af)
    assert sql == "FALSE"
    assert params == {}


def test_allow_all_no_restriction():
    """allow_all with no allow clauses → None (no additional WHERE)."""
    af = AccessFilter(allow_all=True)
    sql, params = access_filter_to_pg_clause(af)
    assert sql is None
    assert params == {}


def test_empty_allow_without_allow_all_is_deny_everything():
    """``AccessFilter(allow_all=False, allow=())`` means deny everything.

    Mirrors the ES translator's match-nothing behaviour; must emit ``FALSE``
    rather than ``None`` (no-restriction). Regression guard for the security
    bug where the implicit-deny case fell through to the no-clause branch.
    """
    af = AccessFilter(allow_all=False, allow=())
    sql, params = access_filter_to_pg_clause(af)
    assert sql == "FALSE"
    assert params == {}


def test_none_access_filter_no_restriction():
    """None → no restriction."""
    sql, params = access_filter_to_pg_clause(None)
    assert sql is None
    assert params == {}


def test_multiple_allow_clauses_or_ed():
    """Multiple clauses → OR of fragments."""
    af = _filter(
        _clause(FieldPredicate("_attrs.dept", ("finance",))),
        _clause(FieldPredicate("_attrs.dept", ("legal",))),
    )
    sql, params = access_filter_to_pg_clause(af)

    assert sql is not None
    assert " OR " in sql
    # Both param entries must exist.
    assert len(params) == 2


def test_mixed_scope_and_attr_in_clause():
    """scope pin + attr predicate in one clause: only attr is translated."""
    af = _filter(
        _clause(
            FieldPredicate("catalog_id", ("cat1",)),
            FieldPredicate("_attrs.dept", ("finance",)),
        )
    )
    sql, params = access_filter_to_pg_clause(af)

    # The attr predicate IS translated; catalog_id is skipped (first-class col).
    assert sql is not None
    assert "dept" in sql
    # catalog_id must NOT appear in the JSONB path SQL.
    assert "catalog_id" not in sql
