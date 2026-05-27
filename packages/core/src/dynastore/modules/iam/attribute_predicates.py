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

Unsupported ops (``"lte"``, ``"range"``, time windows, …) set
``uncompilable=True`` and exclude the enclosing grant from the ALLOW set.
"""
from __future__ import annotations

from typing import ClassVar, Dict, List, Optional, Protocol, Sequence, Tuple

from pydantic import BaseModel, Field, field_validator

from dynastore.models.protocols.access_filter import FieldPredicate

import re

_KEY_PATTERN = re.compile(r"\A[A-Za-z_][A-Za-z0-9_]*\Z")

__all__ = [
    "AttributePredicate",
    "PredicateCompiler",
    "InCompiler",
    "EqCompiler",
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
    """Compile one :class:`AttributePredicate` into a :class:`FieldPredicate`.

    Implementations are stateless singletons registered in
    :data:`PREDICATE_REGISTRY` by ``op`` name.

    Returns ``(predicate, uncompilable)``:
      - ``(FieldPredicate(...), False)`` — successfully compiled.
      - ``(None, True)``                — cannot be compiled; the enclosing
        grant MUST be excluded from the ALLOW set (fail-closed).
    """

    op: ClassVar[str]

    def compile(
        self, key: str, values: Sequence[str]
    ) -> Tuple[Optional[FieldPredicate], bool]: ...


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


# ---------------------------------------------------------------------------
# Registry + compilation entrypoint
# ---------------------------------------------------------------------------

#: Map of ``op`` → compiler instance.  Look up here before compiling.
PREDICATE_REGISTRY: Dict[str, PredicateCompiler] = {
    InCompiler.op: InCompiler(),
    EqCompiler.op: EqCompiler(),
}


def register_compiler(compiler: PredicateCompiler) -> None:
    """Register a custom compiler (e.g. in extension code).

    Thread-safety: registrations should happen at module import time, not
    inside request handlers.
    """
    PREDICATE_REGISTRY[compiler.op] = compiler  # type: ignore[index]


def compile_attribute_predicates(
    preds: Sequence[AttributePredicate],
) -> Tuple[List[FieldPredicate], bool]:
    """Compile a list of attribute predicates into field predicates.

    Returns ``(field_predicates, uncompilable)``.  Each successfully compiled
    predicate is ANDed into the caller's :class:`AccessClause`.  An empty
    ``preds`` list returns ``([], False)`` (no restriction — grant behaves as
    today, full RBAC semantics).

    ``uncompilable=True`` means at least one predicate could not be compiled;
    the caller MUST exclude the enclosing grant from the ALLOW set.
    """
    result: List[FieldPredicate] = []
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
