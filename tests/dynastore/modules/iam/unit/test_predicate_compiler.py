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

"""Unit tests for the attribute-predicate compiler registry (#1441).

Covers:
- InCompiler happy path (multiple values)
- InCompiler empty values → uncompilable
- EqCompiler happy path (one value)
- EqCompiler wrong number of values → uncompilable
- Unknown op → uncompilable (fail-closed)
- compile_attribute_predicates AND-accumulation
- compile_attribute_predicates short-circuits on uncompilable op
"""
from __future__ import annotations

import pytest

from dynastore.models.protocols.access_filter import FieldPredicate, RangePredicate
from dynastore.modules.iam.attribute_predicates import (
    AttributePredicate,
    BetweenCompiler,
    EqCompiler,
    GteCompiler,
    InCompiler,
    LteCompiler,
    PREDICATE_REGISTRY,
    compile_attribute_predicates,
)


# ---------------------------------------------------------------------------
# InCompiler
# ---------------------------------------------------------------------------

def test_in_compiler_happy_path():
    fp, unc = InCompiler().compile("dept", ["finance", "global"])
    assert unc is False
    assert fp == FieldPredicate("_attrs.dept", ("finance", "global"))


def test_in_compiler_single_value():
    fp, unc = InCompiler().compile("dept", ["finance"])
    assert unc is False
    assert fp == FieldPredicate("_attrs.dept", ("finance",))


def test_in_compiler_empty_values_uncompilable():
    fp, unc = InCompiler().compile("dept", [])
    assert unc is True
    assert fp is None


def test_in_compiler_field_name_prefixed():
    fp, unc = InCompiler().compile("sensitivity", ["low", "medium"])
    assert not unc
    assert fp is not None
    assert fp.field == "_attrs.sensitivity"


# ---------------------------------------------------------------------------
# EqCompiler
# ---------------------------------------------------------------------------

def test_eq_compiler_happy_path():
    fp, unc = EqCompiler().compile("dept", ["finance"])
    assert unc is False
    assert fp == FieldPredicate("_attrs.dept", ("finance",))


def test_eq_compiler_multiple_values_uncompilable():
    fp, unc = EqCompiler().compile("dept", ["finance", "global"])
    assert unc is True
    assert fp is None


def test_eq_compiler_empty_values_uncompilable():
    fp, unc = EqCompiler().compile("dept", [])
    assert unc is True
    assert fp is None


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

def test_registry_contains_in_and_eq():
    assert "in" in PREDICATE_REGISTRY
    assert "eq" in PREDICATE_REGISTRY


def test_unknown_op_not_in_registry():
    # "range" is not a supported op; "lte", "gte", "between" are now supported.
    assert "range" not in PREDICATE_REGISTRY


# ---------------------------------------------------------------------------
# compile_attribute_predicates
# ---------------------------------------------------------------------------

def test_compile_empty_list_no_restriction():
    fps, unc = compile_attribute_predicates([])
    assert fps == []
    assert unc is False


def test_compile_in_predicate():
    preds = [AttributePredicate(key="dept", op="in", values=["finance", "global"])]
    fps, unc = compile_attribute_predicates(preds)
    assert unc is False
    assert len(fps) == 1
    assert fps[0] == FieldPredicate("_attrs.dept", ("finance", "global"))


def test_compile_eq_predicate():
    preds = [AttributePredicate(key="region", op="eq", values=["EU"])]
    fps, unc = compile_attribute_predicates(preds)
    assert unc is False
    assert fps == [FieldPredicate("_attrs.region", ("EU",))]


def test_compile_multiple_predicates_and():
    preds = [
        AttributePredicate(key="dept", op="in", values=["finance"]),
        AttributePredicate(key="region", op="eq", values=["EU"]),
    ]
    fps, unc = compile_attribute_predicates(preds)
    assert unc is False
    assert len(fps) == 2


def test_compile_unknown_op_uncompilable_and_excluded():
    """An unknown op must set uncompilable=True; the grant is excluded."""
    preds = [AttributePredicate(key="sensitivity", op="range", values=["3"])]
    fps, unc = compile_attribute_predicates(preds)
    assert unc is True
    assert fps == []


def test_compile_mixed_known_and_unknown_op_uncompilable():
    """Even one unknown op poisons the whole grant (fail-closed)."""
    preds = [
        AttributePredicate(key="dept", op="in", values=["finance"]),
        AttributePredicate(key="sensitivity", op="range", values=["3"]),
    ]
    fps, unc = compile_attribute_predicates(preds)
    assert unc is True
    # The known predicate IS returned even though the grant is uncompilable;
    # the caller MUST drop the grant from the ALLOW set when unc is True.
    assert any(fp.field == "_attrs.dept" for fp in fps)


def test_compile_value_stringified():
    """Values are coerced to str to match the keyword mapping contract."""
    preds = [AttributePredicate(key="level", op="in", values=["1", "2", "3"])]
    fps, unc = compile_attribute_predicates(preds)
    assert not unc
    assert all(isinstance(v, str) for v in fps[0].values)


# ---------------------------------------------------------------------------
# Key-format validation (PG translator defense-in-depth)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "bad_key",
    [
        "x'--",          # SQL quote escape
        'x"; DROP',      # SQL injection vector
        "with space",
        "dot.path",      # dots reserved for the _attrs.<key> separator
        "1leading_digit",
        "",
        "-dash",
    ],
)
def test_attribute_predicate_key_rejects_unsafe_chars(bad_key):
    """``AttributePredicate.key`` must reject anything outside ``[A-Za-z_][A-Za-z0-9_]*``.

    The PG translator interpolates the key unquoted into a JSONB path; the
    pydantic validator is the first line of defense.
    """
    import pydantic
    with pytest.raises(pydantic.ValidationError):
        AttributePredicate(key=bad_key, op="in", values=["x"])


@pytest.mark.parametrize("good_key", ["dept", "_internal", "level_3", "Sensitivity"])
def test_attribute_predicate_key_accepts_safe_chars(good_key):
    """Valid identifier keys pass."""
    p = AttributePredicate(key=good_key, op="eq", values=["v"])
    assert p.key == good_key


# ---------------------------------------------------------------------------
# LteCompiler
# ---------------------------------------------------------------------------

def test_lte_compiler_happy_path():
    fp, unc = LteCompiler().compile("score", ["100"])
    assert unc is False
    assert fp == RangePredicate("_attrs.score", "lte", ("100",))


def test_lte_compiler_empty_values_uncompilable():
    fp, unc = LteCompiler().compile("score", [])
    assert unc is True
    assert fp is None


def test_lte_compiler_multiple_values_uncompilable():
    fp, unc = LteCompiler().compile("score", ["10", "20"])
    assert unc is True
    assert fp is None


def test_lte_compiler_field_name_prefixed():
    fp, unc = LteCompiler().compile("priority", ["5"])
    assert not unc
    assert fp is not None
    assert fp.field == "_attrs.priority"
    assert fp.op == "lte"


def test_lte_compiler_default_kind_is_numeric():
    fp, unc = LteCompiler().compile("val", ["42"])
    assert not unc
    assert fp is not None
    assert fp.kind == "numeric"


# ---------------------------------------------------------------------------
# GteCompiler
# ---------------------------------------------------------------------------

def test_gte_compiler_happy_path():
    fp, unc = GteCompiler().compile("score", ["50"])
    assert unc is False
    assert fp == RangePredicate("_attrs.score", "gte", ("50",))


def test_gte_compiler_empty_values_uncompilable():
    fp, unc = GteCompiler().compile("score", [])
    assert unc is True
    assert fp is None


def test_gte_compiler_multiple_values_uncompilable():
    fp, unc = GteCompiler().compile("score", ["1", "2", "3"])
    assert unc is True
    assert fp is None


def test_gte_compiler_default_kind_is_numeric():
    fp, unc = GteCompiler().compile("val", ["10"])
    assert not unc
    assert fp is not None
    assert fp.kind == "numeric"


# ---------------------------------------------------------------------------
# BetweenCompiler
# ---------------------------------------------------------------------------

def test_between_compiler_happy_path():
    fp, unc = BetweenCompiler().compile("score", ["10", "90"])
    assert unc is False
    assert fp == RangePredicate("_attrs.score", "between", ("10", "90"))


def test_between_compiler_one_value_uncompilable():
    fp, unc = BetweenCompiler().compile("score", ["10"])
    assert unc is True
    assert fp is None


def test_between_compiler_empty_values_uncompilable():
    fp, unc = BetweenCompiler().compile("score", [])
    assert unc is True
    assert fp is None


def test_between_compiler_three_values_uncompilable():
    fp, unc = BetweenCompiler().compile("score", ["1", "2", "3"])
    assert unc is True
    assert fp is None


def test_between_compiler_default_kind_is_numeric():
    fp, unc = BetweenCompiler().compile("val", ["0", "100"])
    assert not unc
    assert fp is not None
    assert fp.kind == "numeric"


# ---------------------------------------------------------------------------
# Timestamp variants via registry
# ---------------------------------------------------------------------------

def test_lte_timestamp_compiler_in_registry():
    compiler = PREDICATE_REGISTRY.get("lte:timestamp")
    assert compiler is not None
    fp, unc = compiler.compile("ts", ["2026-01-01T00:00:00Z"])
    assert not unc
    assert fp is not None
    assert fp.kind == "timestamp"
    assert fp.op == "lte"


def test_gte_timestamp_compiler_in_registry():
    compiler = PREDICATE_REGISTRY.get("gte:timestamp")
    assert compiler is not None
    fp, unc = compiler.compile("ts", ["2025-01-01T00:00:00Z"])
    assert not unc
    assert fp is not None
    assert fp.kind == "timestamp"
    assert fp.op == "gte"


def test_between_timestamp_compiler_in_registry():
    compiler = PREDICATE_REGISTRY.get("between:timestamp")
    assert compiler is not None
    fp, unc = compiler.compile("ts", ["2025-01-01", "2026-01-01"])
    assert not unc
    assert fp is not None
    assert fp.kind == "timestamp"
    assert fp.op == "between"


# ---------------------------------------------------------------------------
# Registry: range ops now present
# ---------------------------------------------------------------------------

def test_registry_contains_range_ops():
    for op in ("lte", "gte", "between"):
        assert op in PREDICATE_REGISTRY, f"op '{op}' should be in registry"


# ---------------------------------------------------------------------------
# compile_attribute_predicates with range ops
# ---------------------------------------------------------------------------

def test_compile_lte_predicate():
    preds = [AttributePredicate(key="score", op="lte", values=["100"])]
    fps, unc = compile_attribute_predicates(preds)
    assert not unc
    assert len(fps) == 1
    assert isinstance(fps[0], RangePredicate)
    assert fps[0].op == "lte"
    assert fps[0].bounds == ("100",)


def test_compile_gte_predicate():
    preds = [AttributePredicate(key="score", op="gte", values=["10"])]
    fps, unc = compile_attribute_predicates(preds)
    assert not unc
    assert len(fps) == 1
    assert isinstance(fps[0], RangePredicate)
    assert fps[0].op == "gte"


def test_compile_between_predicate():
    preds = [AttributePredicate(key="score", op="between", values=["10", "90"])]
    fps, unc = compile_attribute_predicates(preds)
    assert not unc
    assert len(fps) == 1
    assert isinstance(fps[0], RangePredicate)
    assert fps[0].op == "between"
    assert fps[0].bounds == ("10", "90")


def test_compile_mixed_eq_and_lte():
    preds = [
        AttributePredicate(key="dept", op="eq", values=["finance"]),
        AttributePredicate(key="score", op="lte", values=["100"]),
    ]
    fps, unc = compile_attribute_predicates(preds)
    assert not unc
    assert len(fps) == 2
    field_preds = [p for p in fps if isinstance(p, FieldPredicate)]
    range_preds = [p for p in fps if isinstance(p, RangePredicate)]
    assert len(field_preds) == 1
    assert len(range_preds) == 1


def test_compile_lte_wrong_arity_uncompilable():
    preds = [AttributePredicate(key="score", op="lte", values=["10", "20"])]
    fps, unc = compile_attribute_predicates(preds)
    assert unc is True
    assert fps == []


def test_compile_between_wrong_arity_uncompilable():
    preds = [AttributePredicate(key="score", op="between", values=["50"])]
    fps, unc = compile_attribute_predicates(preds)
    assert unc is True
    assert fps == []


# ---------------------------------------------------------------------------
# RangePredicate.matches — pure-Python drift-guard semantics
# ---------------------------------------------------------------------------

def test_range_predicate_lte_matches():
    p = RangePredicate("_attrs.score", "lte", ("100",))
    assert p.matches({"_attrs.score": "50"}) is True
    assert p.matches({"_attrs.score": "100"}) is True
    assert p.matches({"_attrs.score": "101"}) is False


def test_range_predicate_gte_matches():
    p = RangePredicate("_attrs.score", "gte", ("50",))
    assert p.matches({"_attrs.score": "100"}) is True
    assert p.matches({"_attrs.score": "50"}) is True
    assert p.matches({"_attrs.score": "49"}) is False


def test_range_predicate_between_matches():
    p = RangePredicate("_attrs.score", "between", ("10", "90"))
    assert p.matches({"_attrs.score": "10"}) is True
    assert p.matches({"_attrs.score": "50"}) is True
    assert p.matches({"_attrs.score": "90"}) is True
    assert p.matches({"_attrs.score": "9"}) is False
    assert p.matches({"_attrs.score": "91"}) is False


def test_range_predicate_missing_field_fails_closed():
    p = RangePredicate("_attrs.score", "lte", ("100",))
    assert p.matches({}) is False
    assert p.matches({"_attrs.other": "50"}) is False
