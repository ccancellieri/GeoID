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

from dynastore.models.protocols.access_filter import FieldPredicate
from dynastore.modules.iam.attribute_predicates import (
    AttributePredicate,
    EqCompiler,
    InCompiler,
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
    assert "lte" not in PREDICATE_REGISTRY
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
    preds = [AttributePredicate(key="sensitivity", op="lte", values=["3"])]
    fps, unc = compile_attribute_predicates(preds)
    assert unc is True
    assert fps == []


def test_compile_mixed_known_and_unknown_op_uncompilable():
    """Even one unknown op poisons the whole grant (fail-closed)."""
    preds = [
        AttributePredicate(key="dept", op="in", values=["finance"]),
        AttributePredicate(key="sensitivity", op="lte", values=["3"]),
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
