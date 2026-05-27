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

"""Grant-side attribute predicates and compiler registry.

Grant rows carry an ``attribute_predicates`` JSONB array. Each element is
an :class:`AttributePredicate` (key / op / values). The
:func:`compile_attribute_predicates` function turns a list of those into
zero or more :class:`~dynastore.models.protocols.access_filter.FieldPredicate`
or :class:`~dynastore.models.protocols.access_filter.RangePredicate`
objects, which are then ANDed into the grant's :class:`AccessClause` inside
``compile_read_filter``.

Equal-or-stricter invariant:
  - An unrecognised or uncompilable ``op`` drops the grant from the ALLOW set
    and sets ``uncompilable=True``.  Under-returning is the safe direction:
    documents are still reachable by a direct GET that re-runs the full engine.
  - Predicates within one grant: AND (one :class:`AccessClause`).
  - Multiple grants for the same scope: OR (one clause per grant).

Currently supported ops:
  - ``"in"``: membership, compiled to ``FieldPredicate("_attrs.<key>", values)``
  - ``"eq"``: equality, compiled to ``FieldPredicate("_attrs.<key>", (value,))``
  - ``"lte"``: ≤ comparison, compiled to ``RangePredicate("_attrs.<key>", "lte", (bound,))``
  - ``"gte"``: ≥ comparison, compiled to ``RangePredicate("_attrs.<key>", "gte", (bound,))``
  - ``"between"``: inclusive range, compiled to ``RangePredicate("_attrs.<key>", "between", (low, high))``

Unsupported ops set ``uncompilable=True`` and exclude the enclosing grant from
the ALLOW set.

Range ops accept an optional ``kind`` suffix (separated by ``:``) to tag the
bound type for PG casting.  Supported suffixes: ``numeric`` (default),
``timestamp``.  Examples: ``"lte:numeric"``, ``"gte:timestamp"``.

Note on rate_limit / max_count:
    Those are grant-level request conditions enforced at REQUEST time by
    :class:`~dynastore.modules.iam.conditions.RateLimitHandler` via the
    ``UsageCounterProtocol`` middleware (step-5).  They live in
    ``iam.grants.quota`` and are compiled into :class:`~dynastore.models.auth.Condition`
    objects by ``quota_to_conditions`` in ``scale_config.py``.  They are
    NOT document-level row predicates and must NOT be added to
    :data:`PREDICATE_REGISTRY`.  A grant row may carry BOTH ``quota`` (evaluated
    at request time) AND ``attribute_predicates`` (evaluated at query time) without
    any interference between the two mechanisms.
"""
from __future__ import annotations

import re
from typing import ClassVar, Dict, List, Optional, Protocol, Sequence, Tuple, Union

from pydantic import BaseModel, Field, field_validator

from dynastore.models.protocols.access_filter import FieldPredicate, RangePredicate

_KEY_PATTERN = re.compile(r"\A[A-Za-z_][A-Za-z0-9_]*\Z")

__all__ = [
    "AttributePredicate",
    "PredicateCompiler",
    "InCompiler",
    "EqCompiler",
    "LteCompiler",
    "LteTimestampCompiler",
    "GteCompiler",
    "GteTimestampCompiler",
    "BetweenCompiler",
    "BetweenTimestampCompiler",
    "compile_attribute_predicates",
    "PREDICATE_REGISTRY",
]


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

class AttributePredicate(BaseModel):
    """One attribute predicate from a grant's ``attribute_predicates`` list.

    Stored in ``iam.grants.attribute_predicates`` as a JSONB array element::

        {"key": "dept", "op": "in", "values": ["finance", "global"]}
    """

    key: str = Field(
        description=(
            "Document attribute key under ``_attrs``. Must match "
            "``[A-Za-z_][A-Za-z0-9_]*`` — the same shape used unquoted "
            "as a JSONB key in the PG translator."
        ),
    )
    op: str = Field(description="Comparison operator: 'in', 'eq', …")
    values: List[str] = Field(
        default_factory=list,
        description="Comparison values. Non-empty for 'in'/'eq'.",
    )

    @field_validator("key")
    @classmethod
    def _validate_key(cls, value: str) -> str:
        if not _KEY_PATTERN.fullmatch(value):
            raise ValueError(
                "AttributePredicate.key must match [A-Za-z_][A-Za-z0-9_]* "
                "(no quotes, dots, or whitespace)."
            )
        return value


# ---------------------------------------------------------------------------
# Compiler protocol + first-party implementations
# ---------------------------------------------------------------------------

class PredicateCompiler(Protocol):
    """Compile one :class:`AttributePredicate` into a field or range predicate.

    Implementations are stateless singletons registered in
    :data:`PREDICATE_REGISTRY` by ``op`` name.

    Returns ``(predicate, uncompilable)``:
      - ``(FieldPredicate(...), False)`` or ``(RangePredicate(...), False)``
        — successfully compiled.
      - ``(None, True)``                — cannot be compiled; the enclosing
        grant MUST be excluded from the ALLOW set (fail-closed).
    """

    op: ClassVar[str]

    def compile(
        self, key: str, values: Sequence[str]
    ) -> Tuple[Optional[Union[FieldPredicate, RangePredicate]], bool]: ...


class InCompiler:
    """``"in"`` — membership, one or more values."""

    op: ClassVar[str] = "in"

    def compile(
        self, key: str, values: Sequence[str]
    ) -> Tuple[Optional[FieldPredicate], bool]:
        if not values:
            # Empty value list means nothing is in the set → uncompilable
            # (semantics are unclear; fail closed).
            return None, True
        return FieldPredicate(f"_attrs.{key}", tuple(str(v) for v in values)), False


class EqCompiler:
    """``"eq"`` — equality, exactly one value."""

    op: ClassVar[str] = "eq"

    def compile(
        self, key: str, values: Sequence[str]
    ) -> Tuple[Optional[FieldPredicate], bool]:
        if len(values) != 1:
            return None, True
        return FieldPredicate(f"_attrs.{key}", (str(values[0]),)), False


class LteCompiler:
    """``"lte"`` / ``"lte:timestamp"`` — less-than-or-equal, exactly one bound."""

    op: ClassVar[str] = "lte"

    def compile(
        self, key: str, values: Sequence[str]
    ) -> Tuple[Optional[RangePredicate], bool]:
        if len(values) != 1:
            return None, True
        return RangePredicate(f"_attrs.{key}", "lte", (str(values[0]),)), False


class LteTimestampCompiler:
    """``"lte:timestamp"`` — less-than-or-equal with TIMESTAMPTZ cast."""

    op: ClassVar[str] = "lte:timestamp"

    def compile(
        self, key: str, values: Sequence[str]
    ) -> Tuple[Optional[RangePredicate], bool]:
        if len(values) != 1:
            return None, True
        return RangePredicate(f"_attrs.{key}", "lte", (str(values[0]),), kind="timestamp"), False


class GteCompiler:
    """``"gte"`` / ``"gte:numeric"`` — greater-than-or-equal, exactly one bound."""

    op: ClassVar[str] = "gte"

    def compile(
        self, key: str, values: Sequence[str]
    ) -> Tuple[Optional[RangePredicate], bool]:
        if len(values) != 1:
            return None, True
        return RangePredicate(f"_attrs.{key}", "gte", (str(values[0]),)), False


class GteTimestampCompiler:
    """``"gte:timestamp"`` — greater-than-or-equal with TIMESTAMPTZ cast."""

    op: ClassVar[str] = "gte:timestamp"

    def compile(
        self, key: str, values: Sequence[str]
    ) -> Tuple[Optional[RangePredicate], bool]:
        if len(values) != 1:
            return None, True
        return RangePredicate(f"_attrs.{key}", "gte", (str(values[0]),), kind="timestamp"), False


class BetweenCompiler:
    """``"between"`` — inclusive range, exactly two bounds: [low, high]."""

    op: ClassVar[str] = "between"

    def compile(
        self, key: str, values: Sequence[str]
    ) -> Tuple[Optional[RangePredicate], bool]:
        if len(values) != 2:
            return None, True
        return RangePredicate(f"_attrs.{key}", "between", (str(values[0]), str(values[1]))), False


class BetweenTimestampCompiler:
    """``"between:timestamp"`` — inclusive timestamp range, exactly two bounds."""

    op: ClassVar[str] = "between:timestamp"

    def compile(
        self, key: str, values: Sequence[str]
    ) -> Tuple[Optional[RangePredicate], bool]:
        if len(values) != 2:
            return None, True
        return RangePredicate(
            f"_attrs.{key}", "between", (str(values[0]), str(values[1])), kind="timestamp"
        ), False


# ---------------------------------------------------------------------------
# Registry + compilation entrypoint
# ---------------------------------------------------------------------------

#: Map of ``op`` → compiler instance.  Look up here before compiling.
PREDICATE_REGISTRY: Dict[str, PredicateCompiler] = {
    InCompiler.op: InCompiler(),
    EqCompiler.op: EqCompiler(),
    LteCompiler.op: LteCompiler(),
    LteTimestampCompiler.op: LteTimestampCompiler(),
    GteCompiler.op: GteCompiler(),
    GteTimestampCompiler.op: GteTimestampCompiler(),
    BetweenCompiler.op: BetweenCompiler(),
    BetweenTimestampCompiler.op: BetweenTimestampCompiler(),
}


def register_compiler(compiler: PredicateCompiler) -> None:
    """Register a custom compiler (e.g. in extension code).

    Thread-safety: registrations should happen at module import time, not
    inside request handlers.
    """
    PREDICATE_REGISTRY[compiler.op] = compiler  # type: ignore[index]


def compile_attribute_predicates(
    preds: Sequence[AttributePredicate],
) -> Tuple[List[Union[FieldPredicate, RangePredicate]], bool]:
    """Compile a list of attribute predicates into field or range predicates.

    Returns ``(predicates, uncompilable)``.  Each successfully compiled
    predicate is ANDed into the caller's :class:`AccessClause`.  An empty
    ``preds`` list returns ``([], False)`` (no restriction — grant behaves as
    today, full RBAC semantics).

    ``uncompilable=True`` means at least one predicate could not be compiled;
    the caller MUST exclude the enclosing grant from the ALLOW set.
    """
    result: List[Union[FieldPredicate, RangePredicate]] = []
    uncompilable = False
    for pred in preds:
        compiler = PREDICATE_REGISTRY.get(pred.op)
        if compiler is None:
            uncompilable = True
            continue
        fp, unc = compiler.compile(pred.key, pred.values)
        if unc or fp is None:
            uncompilable = True
            continue
        result.append(fp)
    return result, uncompilable
