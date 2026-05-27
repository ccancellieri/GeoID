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
"""Neutral, backend-agnostic projection of a read-authorization decision.

This is the contract that lets document-level security (row-level ABAC) be
shared between the authorization engine and any storage driver **without
coupling them**. The authorization engine compiles an :class:`AccessFilter`
from its policy / role / condition graph for a given principal; a storage
driver translates that filter into its own native predicate language (an
Elasticsearch ``bool`` query, a SQL ``WHERE`` clause, a Parquet row-group
predicate, ...) at query time.

Why a neutral type:
    The driver must never import the IAM module, and the IAM module must never
    import a driver. Both depend only on this module (which lives in
    ``models/protocols`` and imports nothing from either side). The driver asks
    ``get_protocol(PermissionProtocol).compile_read_filter(...)`` and receives
    one of these; it never sees a ``Policy``, a ``Condition`` or any IAM
    internal.

Semantics (a faithful, *fail-closed* projection of ``evaluate_access``):
    A document ``d`` is visible to the principal iff::

        not deny_all
        and (allow_all or any(clause.matches(d) for clause in allow))
        and not any(clause.matches(d) for clause in deny)

    where an :class:`AccessClause` matches ``d`` when *every* one of its
    :class:`FieldPredicate` s matches. Read as: ALLOW policies contribute OR
    branches (``should``); DENY policies contribute negated branches
    (``must_not``) and therefore win over ALLOW, preserving the deny-precedence
    invariant of ``evaluate_access`` *structurally* rather than by re-ranking.

    For a query that spans several collections with DIFFERENT per-collection
    grants, the scopes combine as an *exclusion-union* via the ``union`` field
    (see :meth:`union_of`): ``d`` is visible iff at least one collection's
    *complete* sub-filter admits it. The sub-filters are NOT flattened into one
    allow/deny set — that would let one collection's DENY suppress another
    collection's documents — they are evaluated independently and OR-ed.

The cardinal safety rule (proved by the drift guard property test):
    The filter must be **equal-or-stricter** than ``evaluate_access``. A filter
    that admits a document the engine would deny is a security leak; a filter
    that hides a document the engine would allow is a safe annoyance
    (under-return — the document is still reachable by a direct GET that runs
    the full engine). Therefore anything the compiler cannot express as an
    index predicate is dropped *from the ALLOW side* (the grant simply does not
    contribute), and ``uncompilable`` is set so callers/telemetry know the
    filter is stricter than the engine. When nothing can be allowed, the
    compiler returns :meth:`AccessFilter.deny_everything`.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, Sequence, Tuple, Union

__all__ = [
    "FieldPredicate",
    "RangePredicate",
    "AccessClause",
    "AccessFilter",
]


@dataclass(frozen=True)
class FieldPredicate:
    """A single ``field IN values`` constraint over one envelope field.

    ``values`` is a set of acceptable values for ``field``. Matching is
    membership: a scalar document field matches when its value is in
    ``values``; an *array* document field matches when it intersects
    ``values``. This maps directly onto an Elasticsearch
    ``terms`` query and onto a SQL ``field = ANY(values)`` / array-overlap.
    """

    field: str
    values: Tuple[str, ...]

    def matches(self, document: Mapping[str, Any]) -> bool:
        """Pure-Python evaluation — used by the drift guard property test.

        Mirrors the ``terms`` semantics a driver must implement: scalar field
        → membership; list/tuple/set field → non-empty intersection.
        """
        actual = document.get(self.field)
        allowed = set(self.values)
        if isinstance(actual, (list, tuple, set)):
            return any(v in allowed for v in actual)
        return actual in allowed


@dataclass(frozen=True)
class RangePredicate:
    """A single range constraint over one ``_attrs.<key>`` field.

    Complements :class:`FieldPredicate` for numeric and ISO-8601 timestamp
    comparisons that cannot be expressed as a set-membership test.

    ``op`` is one of:

    * ``"lte"`` — field ≤ bounds[0]
    * ``"gte"`` — field ≥ bounds[0]
    * ``"between"`` — bounds[0] ≤ field ≤ bounds[1]

    ``kind`` distinguishes how the bound string is interpreted by PG:

    * ``"numeric"`` (default) — cast to NUMERIC for the comparison.
    * ``"timestamp"`` — cast to TIMESTAMPTZ for the comparison.

    ES translates both identically (``range`` query on a keyword/date field);
    the ``kind`` tag is only used by the PG SQL fragment builder.

    ``bounds`` must contain exactly one element for ``"lte"``/``"gte"`` and
    exactly two elements for ``"between"``. Violating this contract at
    construction time is a programming error; the compilers enforce the arity
    constraint before emitting a ``RangePredicate``.

    Note on rate_limit / max_count:
        Those are grant-level *request* conditions, not document-level row
        predicates. They live in ``iam.grants.quota`` and are enforced by the
        :class:`~dynastore.modules.iam.conditions.RateLimitHandler` middleware
        (step-5) via ``quota_to_conditions``. They must NOT be added to
        ``PREDICATE_REGISTRY`` and must NOT be expressed as ``RangePredicate``
        entries in ``attribute_predicates``. The two mechanisms coexist on the
        same grant row without interference: ``grants.quota`` is evaluated at
        request time; ``grants.attribute_predicates`` produces row predicates
        at query time. Both enforce orthogonal constraints on the same grant.
    """

    field: str
    op: Literal["lte", "gte", "between"]
    bounds: Tuple[str, ...]
    kind: Literal["numeric", "timestamp"] = "numeric"

    def matches(self, document: Mapping[str, Any]) -> bool:
        """Pure-Python evaluation — used by the drift guard property test.

        Attempts numeric conversion first so that ``"50" >= "5"`` is
        correctly True for numeric-kind predicates. Falls back to
        lexicographic string comparison (sufficient for timestamp ISO-8601
        strings which sort correctly as text, and for the drift guard's
        representative value sets).
        """
        actual = document.get(self.field)
        if actual is None:
            return False

        def _cmp(a: str, b: str) -> int:
            """-1 if a<b, 0 if a==b, 1 if a>b. Numeric-first."""
            try:
                fa, fb = float(a), float(b)
                return (fa > fb) - (fa < fb)
            except (ValueError, TypeError):
                return (a > b) - (a < b)

        actual_s = str(actual)
        if self.op == "lte":
            return _cmp(actual_s, self.bounds[0]) <= 0
        if self.op == "gte":
            return _cmp(actual_s, self.bounds[0]) >= 0
        if self.op == "between":
            return _cmp(actual_s, self.bounds[0]) >= 0 and _cmp(actual_s, self.bounds[1]) <= 0
        return False  # unknown op — fail closed


@dataclass(frozen=True)
class AccessClause:
    """A conjunction (AND) of :class:`FieldPredicate` / :class:`RangePredicate` s.

    A document satisfies the clause iff it satisfies *every* predicate. An
    empty clause matches every document (an unconditional grant within its
    resource scope).
    """

    predicates: Tuple[Union[FieldPredicate, RangePredicate], ...] = ()

    def matches(self, document: Mapping[str, Any]) -> bool:
        return all(p.matches(document) for p in self.predicates)


@dataclass(frozen=True)
class AccessFilter:
    """Compiled, backend-neutral read scope for one principal.

    Drivers translate this to native predicates. The pure-Python
    :meth:`admits` is the reference semantics every driver translation must
    match (the drift guard checks ``admits`` against ``evaluate_access``).
    """

    #: Nothing is visible — deny-by-default / suspended principal / no ALLOW
    #: could be compiled. A driver MUST return zero results (e.g. a
    #: ``match_none`` query), never fall back to an unfiltered scan.
    deny_all: bool = False

    #: No row-level restriction — an unconditional ALLOW (e.g. a platform
    #: super-admin). A driver applies no access clause.
    allow_all: bool = False

    #: OR of allow clauses. A document must match at least one (unless
    #: ``allow_all``). Empty + not ``allow_all`` ⟹ same as ``deny_all``.
    allow: Tuple[AccessClause, ...] = ()

    #: OR of deny clauses (``must_not``). A document matching any is excluded
    #: regardless of ``allow`` — this is how deny-precedence is preserved.
    deny: Tuple[AccessClause, ...] = ()

    #: True when at least one grant relevant to this read scope carried a
    #: condition the compiler could not express as an index predicate. Such a
    #: grant is omitted from ``allow`` (fail-closed under-return). Purely
    #: informational: lets callers log/alert that search may under-return.
    uncompilable: bool = False

    #: OR of *complete* sub-filters (exclusion-union). When non-empty a document
    #: is admitted iff at least one sub-filter admits it in full — that is, the
    #: sub-filter's own ``allow``/``deny``/``deny_all``/``allow_all`` logic is
    #: evaluated independently and the results OR-ed. This is how a
    #: multi-collection read combines DIFFERENT per-collection grants WITHOUT
    #: cross-contamination: each sub-filter is already pinned to its own
    #: ``collection_id`` (every allow AND deny clause carries that pin), so its
    #: DENY clauses can only suppress its own collection's documents and its
    #: ALLOW clauses can only admit them. Flattening clauses across collections
    #: into a single ``allow``/``deny`` set would let a collection-A DENY (or an
    #: ``allow_all`` from A) wrongly affect collection-B documents — the union
    #: node exists precisely to prevent that. When ``union`` is set the top-level
    #: ``allow``/``deny``/``allow_all`` are ignored (``deny_all`` still wins as a
    #: fail-closed short-circuit); a sub-filter that is itself ``deny_everything``
    #: simply contributes nothing (its collection yields zero rows).
    union: Tuple["AccessFilter", ...] = ()

    #: Free-form driver hints the generic fields cannot carry. Drivers that do
    #: not understand a key MUST ignore it (never fail-open on it).
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def deny_everything(cls, *, uncompilable: bool = False) -> "AccessFilter":
        """Fail-closed sentinel: the principal may see nothing via search."""
        return cls(deny_all=True, uncompilable=uncompilable)

    @classmethod
    def allow_everything(cls) -> "AccessFilter":
        """No row-level restriction (caller already passed coarse authz)."""
        return cls(allow_all=True)

    @classmethod
    def from_clauses(
        cls,
        allow: Sequence[AccessClause],
        deny: Sequence[AccessClause] = (),
        *,
        uncompilable: bool = False,
    ) -> "AccessFilter":
        """Build from allow/deny clauses, failing closed when allow is empty."""
        allow_t = tuple(allow)
        if not allow_t:
            return cls.deny_everything(uncompilable=uncompilable)
        return cls(allow=allow_t, deny=tuple(deny), uncompilable=uncompilable)

    @classmethod
    def union_of(cls, sub_filters: Sequence["AccessFilter"]) -> "AccessFilter":
        """Combine complete sub-filters as an exclusion-union (OR of sub-filters).

        Used to merge per-collection read scopes for a multi-collection query so
        each collection returns exactly what its OWN grants allow. Each sub-filter
        must already be pinned to its collection (its allow AND deny clauses carry
        the ``collection_id`` predicate) so a sub-filter's DENY can never suppress
        another collection's documents.

        Fail-closed reductions (never widen):
          * no sub-filters ⟹ :meth:`deny_everything` (nothing in scope);
          * sub-filters that are themselves ``deny_everything`` are dropped —
            their collection simply yields zero rows, but they must NOT collapse
            the whole union to deny;
          * if every sub-filter is ``deny_everything`` ⟹ :meth:`deny_everything`;
          * a single surviving sub-filter is returned as-is (no wrapper node);
          * ``uncompilable`` is OR-ed across sub-filters so telemetry still sees
            that some branch under-returns.
        """
        subs = tuple(sub_filters)
        any_uncompilable = any(s.uncompilable for s in subs)
        # A ``deny_everything`` sub-filter admits nothing; in an OR it is inert,
        # so drop it rather than letting it short-circuit the whole union.
        live = tuple(s for s in subs if not s.deny_all)
        if not live:
            return cls.deny_everything(uncompilable=any_uncompilable)
        if len(live) == 1:
            sub = live[0]
            if any_uncompilable and not sub.uncompilable:
                # Preserve the union-wide under-return signal on the lone branch.
                from dataclasses import replace

                return replace(sub, uncompilable=True)
            return sub
        return cls(union=live, uncompilable=any_uncompilable)

    @property
    def is_unconditional(self) -> bool:
        """True when no per-document clause needs to be applied at all."""
        return self.allow_all and not self.deny and not self.union

    def admits(self, document: Mapping[str, Any]) -> bool:
        """Reference semantics — the contract every driver translation matches.

        This is intentionally pure and side-effect free so the drift guard can
        assert ``admits(doc) ⟹ evaluate_access(...) == ALLOW`` over generated
        policy/document combinations.
        """
        if self.deny_all:
            return False
        if self.union:
            # Exclusion-union: each sub-filter is evaluated IN FULL (its own
            # allow+deny+allow_all logic) and the results OR-ed. Because every
            # sub-filter is collection-pinned, a sub-filter's DENY cannot reach
            # another collection's documents.
            return any(sub.admits(document) for sub in self.union)
        if any(clause.matches(document) for clause in self.deny):
            return False
        if self.allow_all:
            return True
        return any(clause.matches(document) for clause in self.allow)
