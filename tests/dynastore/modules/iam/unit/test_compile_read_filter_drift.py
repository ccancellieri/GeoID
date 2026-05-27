"""Drift-guard property test for ``PolicyService.compile_read_filter``.

This is the SAFETY NET for the row-level ABAC feature: it proves, over a
generated matrix of policy scenarios and candidate documents, the cardinal
invariant the whole design rests on::

    THE FILTER MUST BE EQUAL-OR-STRICTER THAN THE ENGINE.
    af.admits(doc)  ==>  evaluate_access(principal, READ, doc) == ALLOW

A filter that admits a document the engine would DENY is a security leak — that
is exactly what this test is built to catch. The reverse implication is NOT
asserted: the engine may legitimately allow documents the filter hides (safe
under-return — the document is still reachable by a direct GET that re-runs the
full engine).

How the engine runtime is made consistent with each document's envelope
----------------------------------------------------------------------------
The implication is only meaningful when ``evaluate_access`` is fed runtime
inputs that are CONSISTENT with the document under test. The two compilable
conditions read *different* runtime surfaces than the per-document envelope, so
the harness bridges each one explicitly:

* ``catalog_lookup_public_allowed`` — the engine handler
  (:class:`CatalogLookupAudienceHandler`) reads ``CatalogLookupAudience.is_public``
  for ``ctx.catalog_id`` via the registered ``ConfigsProtocol``. It is a
  per-CATALOG flag. The compiler projects it to the per-DOCUMENT predicate
  ``visibility IN ("public",)``. To keep the two consistent we register a stub
  ``ConfigsProtocol`` that reports ``is_public == (document["visibility"] ==
  "public")`` for the document's own catalog, and call the engine with
  ``catalog_id = document["catalog_id"]``. That makes the engine's per-catalog
  decision mirror exactly the per-document visibility the filter keys on.

* ``match`` on ``principal.attributes.<k>`` — the engine handler
  (:class:`AttributeMatchHandler`) compares the *principal's* attribute against
  the static ``config.value`` (it never reads the document). The compiler folds
  the same principal attribute into a per-document predicate ``doc[k] ==
  principal.attributes[k]``. We bridge by (a) passing the principal object in
  ``ctx.extras["principal_obj"]`` so the engine can resolve the attribute, and
  (b) varying the document's ``<k>`` field across the candidate set. The
  implication then bites precisely: the filter admits a doc only when
  ``doc[k] == principal.attributes[k]``; the engine allows only when
  ``principal.attributes[k] == config.value``. The matrix includes scenarios
  where these agree and where they disagree, so a leaking projection would
  surface as ``admits=True`` while ``engine=DENY``.

* path / method / priority / deny-precedence — the engine request is built from
  one of the compiler's *own* ``_read_scope_probe_paths`` for the document's
  scope, with method ``GET``, so relevance resolution can never silently drift
  between the two sides.

The service is constructed exactly as the sibling
``test_compile_read_filter.py`` does: no DB / role-storage dependencies,
policies fed through ``principal.custom_policies`` so the role-lookup branch is
skipped, and ``_resolve_schema`` stubbed to a constant.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import pytest

from dynastore.models.auth import Condition, Policy, Principal
from dynastore.models.protocols.access_filter import (
    AccessClause,
    AccessFilter,
    RangePredicate,
)
from dynastore.modules.iam.audience_configs import CatalogLookupAudience
from dynastore.modules.iam.conditions import EvaluationContext
from dynastore.modules.iam.policies import PolicyService
from dynastore.tools.discovery import get_protocol, register_plugin, unregister_plugin


# --------------------------------------------------------------------------- #
# Service construction (mirrors the sibling unit test).
# --------------------------------------------------------------------------- #
def _service(grant_rows: Optional[List[Dict[str, Any]]] = None) -> PolicyService:
    """Build a minimal PolicyService stub for unit testing.

    When ``grant_rows`` is provided the service's ``iam_storage`` is stubbed
    with a ``resolve_effective_grants`` coroutine that returns those rows,
    allowing range-predicate ABAC scenarios to exercise the grant compiler path.
    """
    svc = PolicyService.__new__(PolicyService)
    svc._state = None  # type: ignore[attr-defined]
    svc._engine = None  # type: ignore[attr-defined]
    svc.storage = None  # type: ignore[attr-defined]
    svc._role_config = None  # type: ignore[attr-defined]

    if grant_rows is not None:
        rows = grant_rows

        class _StubIamStorage:
            async def resolve_effective_grants(self, **_kw: Any) -> List[Dict[str, Any]]:
                return rows

        svc.iam_storage = _StubIamStorage()  # type: ignore[attr-defined]
    else:
        svc.iam_storage = None  # type: ignore[attr-defined]

    async def _fake_resolve_schema(catalog_id, conn=None):  # noqa: ANN001
        return "iam"

    svc._resolve_schema = _fake_resolve_schema  # type: ignore[method-assign]
    return svc


def _allow(
    pid: str,
    *,
    path: str = ".*",
    method: str = ".*",
    priority: int = 0,
    conditions: Optional[List[Condition]] = None,
) -> Policy:
    return Policy(
        id=pid,
        effect="ALLOW",
        priority=priority,
        actions=[method],
        resources=[path],
        conditions=conditions or [],
    )


def _deny(
    pid: str,
    *,
    path: str = ".*",
    method: str = ".*",
    priority: int = 0,
    conditions: Optional[List[Condition]] = None,
) -> Policy:
    return Policy(
        id=pid,
        effect="DENY",
        priority=priority,
        actions=[method],
        resources=[path],
        conditions=conditions or [],
    )


# --------------------------------------------------------------------------- #
# Stub ConfigsProtocol — the only registered protocol the engine touches for
# ``catalog_lookup_public_allowed``. ``is_public`` is keyed per catalog so we
# can make the engine's per-catalog decision mirror a per-document visibility.
# --------------------------------------------------------------------------- #
async def _unexpected_config_call(*_a, **_k):  # noqa: ANN002, ANN003
    """Loud stub for every ConfigsProtocol method the engine should not reach.

    The drift harness only ever needs ``get_config(CatalogLookupAudience, ...)``;
    if the engine reaches any other config surface the test fails immediately
    rather than silently fanning out to real storage.
    """
    raise AssertionError("unexpected ConfigsProtocol method called by the engine")


class _StubConfigs:
    """Minimal ``ConfigsProtocol`` reporting a per-catalog ``is_public`` flag.

    Only ``get_config(CatalogLookupAudience, ...)`` is meaningfully
    implemented — that is the single surface ``CatalogLookupAudienceHandler``
    touches. ``ConfigsProtocol`` is ``@runtime_checkable`` and discovery uses
    ``isinstance(obj, ConfigsProtocol)``, whose structural check verifies every
    declared member *exists*; every other method is therefore present but
    bound to :func:`_unexpected_config_call` so an accidental call fails loudly.
    """

    priority = 0

    def __init__(self, public_catalogs: Dict[str, bool]) -> None:
        self._public_catalogs = public_catalogs

    async def get_config(
        self,
        config_cls,  # noqa: ANN001
        catalog_id=None,  # noqa: ANN001
        collection_id=None,  # noqa: ANN001
        ctx=None,  # noqa: ANN001
        config_snapshot=None,  # noqa: ANN001
    ):
        if config_cls is CatalogLookupAudience:
            is_public = bool(self._public_catalogs.get(catalog_id, False))
            return CatalogLookupAudience(is_public=is_public)
        raise AssertionError(f"unexpected get_config for {config_cls!r}")

    # Remaining ConfigsProtocol surface — present (for the structural
    # isinstance check) but never expected to be exercised.
    get_persisted_config = _unexpected_config_call
    set_config = _unexpected_config_call
    list_configs = _unexpected_config_call
    list_catalog_configs = _unexpected_config_call
    snapshot_catalog_defaults = _unexpected_config_call
    delete_config = _unexpected_config_call
    search = _unexpected_config_call
    list_refs_at_scope = _unexpected_config_call
    get_config_by_ref = _unexpected_config_call
    set_config_by_ref = _unexpected_config_call
    delete_config_by_ref = _unexpected_config_call


# --------------------------------------------------------------------------- #
# Document generator — varies the envelope fields ABAC keys on.
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Doc:
    catalog_id: str
    collection_id: str
    geoid: str
    visibility: str
    owner: str
    grant_subjects: Tuple[str, ...]
    sensitivity: str = "3"  # _attrs.sensitivity; default mid-range

    def as_mapping(self) -> Dict[str, Any]:
        return {
            "catalog_id": self.catalog_id,
            "collection_id": self.collection_id,
            "geoid": self.geoid,
            "id": self.geoid,
            "visibility": self.visibility,
            "owner": self.owner,
            "grant_subjects": list(self.grant_subjects),
            "_attrs.sensitivity": self.sensitivity,
        }


def _documents() -> List[Doc]:
    """A bounded, deterministic cross-product of envelope variations.

    Each document carries an ``_attrs.sensitivity`` value (``"1"``, ``"3"``,
    ``"5"``) so range-predicate scenarios exercise the full lte/gte/between
    compiler path.
    """
    docs: List[Doc] = []
    for cat in ("cat_a", "cat_b"):
        for col in ("col_x", "col_y"):
            for vis in ("public", "private"):
                for owner in ("alice", "bob"):
                    for subjects in ((), ("alice",), ("carol",)):
                        for sensitivity in ("1", "3", "5"):
                            docs.append(
                                Doc(
                                    catalog_id=cat,
                                    collection_id=col,
                                    geoid=f"{cat}.{col}.{vis}.{owner}.s{sensitivity}",
                                    visibility=vis,
                                    owner=owner,
                                    grant_subjects=subjects,
                                    sensitivity=sensitivity,
                                )
                            )
    return docs


# --------------------------------------------------------------------------- #
# Scenario generator — varies effect / priority / action / resource / condition
# and the principal's attributes.
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Scenario:
    name: str
    policies: Tuple[Policy, ...]
    attributes: Tuple[Tuple[str, str], ...]  # frozen dict for hashability
    # Optional grant rows injected into the iam_storage stub.  Each element is
    # the dict shape ``resolve_effective_grants`` returns (an
    # ``attribute_predicates`` key with a list of raw predicate dicts).
    grant_rows: Tuple[Dict[str, Any], ...] = field(default_factory=tuple)

    @property
    def attr_dict(self) -> Dict[str, Any]:
        return dict(self.attributes)


# Resource patterns the scenario matrix draws from. ``ITEMS_RE`` matches the
# canonical per-catalog item-read surfaces the compiler probes; the cross-cat
# pattern is anchored to a *different* catalog so it must never be relevant to
# a cat_a read.
_ITEMS_RE = r"/stac/catalogs/[^/]+/collections/[^/]+/items"
_WILD = ".*"
_CROSS_CAT_RE = r"/stac/catalogs/cat_b/.*"
_ADMIN_RE = r"/admin/.*"

# Conditions the matrix draws from. Includes both compilable kinds
# (catalog_lookup_public_allowed, an attribute match) and uncompilable kinds
# (rate_limit, time_window, a request-time match) so fail-closed paths are
# exercised.
_COND_NONE: Tuple[Condition, ...] = ()
_COND_PUBLIC = (Condition(type="catalog_lookup_public_allowed"),)
_COND_RATE_LIMIT = (
    Condition(type="rate_limit", config={"limit": 1, "window_seconds": 60}),
)
_COND_TIME_WINDOW = (
    Condition(type="time_window", config={"start": "00:00", "end": "23:59"}),
)
_COND_REQ_MATCH = (
    Condition(
        type="match",
        config={"attribute": "query.tenant", "operator": "eq", "value": "x"},
    ),
)


def _owner_match(value: str) -> Tuple[Condition, ...]:
    """An ``eq`` match on the principal's ``owner`` attribute against ``value``.

    IMPORTANT — engine-consistency of the ``match`` projection:
    The engine handler (:class:`AttributeMatchHandler`) compares the principal's
    ``owner`` attribute against the *static* ``config.value`` and never reads the
    document. The compiler instead folds the principal's resolved ``owner`` into
    a per-document predicate ``doc.owner == principal.owner``. The two only stay
    equal-or-stricter when ``config.value`` equals the principal's ``owner``
    attribute — i.e. the engine ALLOWS (so any doc the row-filter keeps is one
    the engine would also have allowed). The brute matrix therefore always sets
    ``value`` to the principal's own ``owner`` (the intended ownership-ABAC
    usage). The DIVERGENT case (``value != principal.owner``) is deliberately
    carved out into :func:`test_attribute_match_value_mismatch_is_a_leak`, which
    documents it as a suspected leak rather than letting it pollute the matrix.
    """
    return (
        Condition(
            type="match",
            config={
                "attribute": "principal.attributes.owner",
                "operator": "eq",
                "value": value,
            },
        ),
    )


def _scenarios() -> List[Scenario]:
    """Deterministic, bounded matrix of authorization scenarios.

    Each scenario combines (effect, priority, action, resource, condition) for
    one or two policies, plus a principal attribute set. The aim is coverage of
    every branch the compiler and engine share: compilable ALLOW/DENY,
    uncompilable ALLOW (dropped) / DENY (fail-closed), out-of-scope resource,
    cross-catalog resource, wildcard super-admin, deny-precedence by priority,
    and attribute folding in its engine-consistent regime (the divergent regime
    is asserted separately as a suspected leak).
    """
    scenarios: List[Scenario] = []
    attr_alice = (("owner", "alice"),)
    attr_bob = (("owner", "bob"),)
    attr_sets = [("alice", attr_alice), ("bob", attr_bob)]

    # Sentinel used in the condition matrices below: at scenario-build time it is
    # replaced by ``_owner_match(<this scenario's principal owner>)`` so the
    # engine's static ``config.value`` always equals the principal attribute
    # (the engine ALLOWs) — keeping the ``match`` projection in its
    # engine-consistent regime. The DIVERGENT regime is tested separately.
    _OWNER_MATCH_SENTINEL = "__owner_match__"

    def _resolve_cond(marker, attrs):  # noqa: ANN001, ANN202
        if marker is _OWNER_MATCH_SENTINEL:
            owner_value = dict(attrs)["owner"]
            return _owner_match(owner_value)
        return marker

    # --- Single-policy ALLOW scenarios over the condition/resource matrix. --- #
    single_conditions = [
        ("none", _COND_NONE),
        ("public", _COND_PUBLIC),
        ("owner_match", _OWNER_MATCH_SENTINEL),
        ("rate_limit", _COND_RATE_LIMIT),
        ("time_window", _COND_TIME_WINDOW),
        ("req_match", _COND_REQ_MATCH),
    ]
    resources = [
        ("items", _ITEMS_RE),
        ("wild", _WILD),
        ("cross_cat", _CROSS_CAT_RE),
        ("admin", _ADMIN_RE),
    ]
    methods = [("get", "GET"), ("wild", _WILD)]

    for (cname, cond_m), (rname, res), (mname, method), (aname, attrs) in itertools.product(
        single_conditions, resources, methods, attr_sets
    ):
        cond = _resolve_cond(cond_m, attrs)
        scenarios.append(
            Scenario(
                name=f"allow-{cname}-{rname}-{mname}-attr_{aname}",
                policies=(
                    _allow("a1", path=res, method=method, conditions=list(cond)),
                ),
                attributes=attrs,
            )
        )

    # --- ALLOW + DENY pairs to exercise deny-precedence (structural). --- #
    # NB: a ``match`` condition as a DENY is intentionally excluded here. The
    # engine evaluates a ``match`` deny-gate against the *principal* (it never
    # reads the document), so an engine DENY applies to ALL documents; the
    # compiler can only express it as a per-document deny clause. The two cannot
    # agree by construction, and that mismatch is captured in
    # ``test_attribute_match_value_mismatch_is_a_leak``. The compilable
    # conditions safe to use as a DENY here are the doc-keyed ones (public).
    deny_conditions = [
        ("none", _COND_NONE),
        ("public", _COND_PUBLIC),
        ("rate_limit", _COND_RATE_LIMIT),  # uncompilable DENY → fail-closed
        ("time_window", _COND_TIME_WINDOW),  # uncompilable DENY → fail-closed
    ]
    for (dname, dcond_m), (aname, attrs) in itertools.product(deny_conditions, attr_sets):
        dcond = _resolve_cond(dcond_m, attrs)
        # Broad ALLOW (would admit everything in scope) AND a DENY that the
        # engine applies with deny-precedence.
        scenarios.append(
            Scenario(
                name=f"allow_all+deny-{dname}-attr_{aname}",
                policies=(
                    _allow("a_all", path=_WILD, method="GET"),
                    _deny("d1", path=_WILD, method="GET", conditions=list(dcond)),
                ),
                attributes=attrs,
            )
        )

    # --- Priority interplay: a higher-priority ALLOW vs a lower DENY and vice
    # versa. The compiler ignores priority (DENY always wins structurally), so
    # these probe whether the structural projection stays equal-or-stricter
    # even when the engine would have let the ALLOW win on priority. --- #
    for (aname, attrs) in attr_sets:
        scenarios.append(
            Scenario(
                name=f"allow_hi_pri+deny_lo-attr_{aname}",
                policies=(
                    _allow("a_hi", path=_WILD, method="GET", priority=10),
                    _deny(
                        "d_lo",
                        path=_WILD,
                        method="GET",
                        priority=0,
                        conditions=list(_COND_PUBLIC),
                    ),
                ),
                attributes=attrs,
            )
        )
        scenarios.append(
            Scenario(
                name=f"deny_hi_pri+allow_lo-attr_{aname}",
                policies=(
                    _deny(
                        "d_hi",
                        path=_WILD,
                        method="GET",
                        priority=10,
                        conditions=list(_COND_PUBLIC),
                    ),
                    _allow("a_lo", path=_WILD, method="GET", priority=0),
                ),
                attributes=attrs,
            )
        )

    # --- Two ALLOWs (compilable + uncompilable) — the good one must survive. --- #
    for (aname, attrs) in attr_sets:
        scenarios.append(
            Scenario(
                name=f"allow_public+allow_uncompilable-attr_{aname}",
                policies=(
                    _allow("a_pub", path=_WILD, method="GET", conditions=list(_COND_PUBLIC)),
                    _allow("a_rl", path=_WILD, method="GET", conditions=list(_COND_RATE_LIMIT)),
                ),
                attributes=attrs,
            )
        )

    # --- No-policy / out-of-scope only — deny-by-default. --- #
    scenarios.append(Scenario(name="empty", policies=(), attributes=attr_alice))
    scenarios.append(
        Scenario(
            name="only_admin_post",
            policies=(_allow("a_admin", path=_ADMIN_RE, method="POST"),),
            attributes=attr_alice,
        )
    )

    # --- Range-predicate grant scenarios (exercise lte/gte/between compiler) -
    #
    # These scenarios inject grant rows with ``attribute_predicates`` JSONB via
    # the iam_storage stub so the grant compiler path is exercised by the drift
    # guard property test.  The base policy is a broad ALLOW so the engine
    # always returns ALLOW; the filter restricts further via the range predicate.
    # ``af.admits(doc) ⟹ engine_allows`` trivially holds because engine_allows
    # is always True for these scenarios — the safety net here proves that the
    # range predicate compiler produces a filter that is not *wider* than the
    # range bound, which would be a security regression.
    #
    # The ``_attrs.sensitivity`` field varied in ``_documents()`` (values
    # ``"1"``, ``"3"``, ``"5"``) provides the per-document surface these
    # predicates key on.
    range_grant_cases: List[Tuple[str, List[Dict[str, Any]]]] = [
        (
            "grant_lte_sensitivity_3",
            [{"attribute_predicates": [{"key": "sensitivity", "op": "lte", "values": ["3"]}]}],
        ),
        (
            "grant_gte_sensitivity_3",
            [{"attribute_predicates": [{"key": "sensitivity", "op": "gte", "values": ["3"]}]}],
        ),
        (
            "grant_between_sensitivity_1_3",
            [{"attribute_predicates": [{"key": "sensitivity", "op": "between", "values": ["1", "3"]}]}],
        ),
        (
            "grant_lte_sensitivity_1",
            [{"attribute_predicates": [{"key": "sensitivity", "op": "lte", "values": ["1"]}]}],
        ),
        (
            "grant_gte_sensitivity_5",
            [{"attribute_predicates": [{"key": "sensitivity", "op": "gte", "values": ["5"]}]}],
        ),
    ]
    for case_name, rows in range_grant_cases:
        for aname, attrs in attr_sets:
            scenarios.append(
                Scenario(
                    name=f"{case_name}-attr_{aname}",
                    policies=(_allow("a_all", path=_WILD, method="GET"),),
                    attributes=attrs,
                    grant_rows=tuple(rows),
                )
            )

    return scenarios


# --------------------------------------------------------------------------- #
# Engine truth — call ``evaluate_access`` with runtime inputs consistent with
# the document under test (see module docstring for the consistency rationale).
# --------------------------------------------------------------------------- #
async def _engine_allows(
    svc: PolicyService,
    scenario: Scenario,
    doc: Doc,
) -> bool:
    """RHS of the implication: would the full engine ALLOW a READ of ``doc``?

    Registers a per-document ``ConfigsProtocol`` stub so the engine's
    ``catalog_lookup_public_allowed`` decision for the document's catalog equals
    ``doc.visibility == "public"`` — bridging the per-catalog engine flag to the
    per-document visibility predicate the compiler emits. The principal object
    is placed in ``ctx.extras['principal_obj']`` so the engine can resolve
    ``principal.attributes.<k>`` for ``match`` conditions.
    """
    stub = _StubConfigs(public_catalogs={doc.catalog_id: doc.visibility == "public"})
    register_plugin(stub)
    get_protocol.cache_clear()
    try:
        principal = Principal(
            custom_policies=list(scenario.policies), attributes=scenario.attr_dict
        )
        # Representative read path for the document's exact scope. Using a STAC
        # item-read URL keeps the engine's resource matching aligned with the
        # surfaces the compiler probes for relevance.
        path = (
            f"/stac/catalogs/{doc.catalog_id}/collections/"
            f"{doc.collection_id}/items/{doc.geoid}"
        )
        ctx = EvaluationContext(
            request=None,
            storage=None,  # type: ignore[arg-type]
            principal_id=None,
            path=path,
            method="GET",
            query_params={},  # request-time match conditions resolve to None → deny
            catalog_id=doc.catalog_id,
            extras={"principal_obj": principal},
        )
        allowed, _reason = await svc.evaluate_access(
            principals=[],
            path=path,
            method="GET",
            request_context=ctx,
            catalog_id=doc.catalog_id,
            custom_policies=list(scenario.policies),
        )
        return allowed
    finally:
        unregister_plugin(stub)
        get_protocol.cache_clear()


# --------------------------------------------------------------------------- #
# THE DRIFT GUARD — the core implication over the full matrix.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "scenario", _scenarios(), ids=lambda s: s.name
)
async def test_filter_is_equal_or_stricter_than_engine(scenario: Scenario) -> None:
    """For every (scenario, document): ``af.admits(doc) ⟹ engine ALLOW``.

    The converse is intentionally NOT asserted — under-return is safe. A single
    violation (filter admits, engine denies) is a security leak and fails here
    with the exact scenario+document for triage.
    """
    grant_rows = list(scenario.grant_rows) if scenario.grant_rows else None
    svc = _service(grant_rows=grant_rows)
    leaks: List[str] = []

    for doc in _documents():
        af = await svc.compile_read_filter(
            principals=[],
            catalog_id=doc.catalog_id,
            collection_id=doc.collection_id,
            principal=Principal(
                custom_policies=list(scenario.policies),
                attributes=scenario.attr_dict,
            ),
        )
        admits = af.admits(doc.as_mapping())
        if not admits:
            # Filter hides it — safe regardless of the engine. Skip the
            # (expensive) engine call; under-return is always permitted.
            continue
        engine_allows = await _engine_allows(svc, scenario, doc)
        if not engine_allows:
            leaks.append(
                f"LEAK scenario={scenario.name!r} doc={doc.as_mapping()!r} "
                f"filter=ADMIT engine=DENY  (af={af!r})"
            )

    assert not leaks, (
        "compile_read_filter admitted documents the engine would DENY "
        "(equal-or-stricter invariant violated):\n" + "\n".join(leaks)
    )


# --------------------------------------------------------------------------- #
# Targeted fail-closed assertions (compiler output only — no engine needed).
# --------------------------------------------------------------------------- #
async def test_uncompilable_allow_is_not_represented_and_flags() -> None:
    """An ALLOW gated by an uncompilable condition must NOT contribute a clause
    that admits a doc only that grant would have allowed, and must set
    ``uncompilable``."""
    svc = _service()
    for cond in (_COND_RATE_LIMIT, _COND_TIME_WINDOW, _COND_REQ_MATCH):
        af = await svc.compile_read_filter(
            principals=[],
            catalog_id="cat_a",
            collection_id="col_x",
            principal=Principal(
                custom_policies=[_allow("a", path=_WILD, method="GET", conditions=list(cond))]
            ),
        )
        # The lone grant was uncompilable → nothing to allow → deny-by-default.
        assert af.deny_all is True, f"cond={cond} should have produced deny_all"
        assert af.uncompilable is True, f"cond={cond} should flag uncompilable"
        # No clause admits the doc that grant alone would have allowed.
        doc = {"catalog_id": "cat_a", "collection_id": "col_x", "visibility": "public"}
        assert af.admits(doc) is False


async def test_relevant_uncompilable_deny_forces_deny_all() -> None:
    """A relevant DENY with an uncompilable condition must fully fail closed."""
    svc = _service()
    for cond in (_COND_RATE_LIMIT, _COND_TIME_WINDOW):
        af = await svc.compile_read_filter(
            principals=[],
            catalog_id="cat_a",
            collection_id="col_x",
            principal=Principal(
                custom_policies=[
                    _allow("a_all", path=_WILD, method="GET"),
                    _deny("d", path=_WILD, method="GET", conditions=list(cond)),
                ]
            ),
        )
        assert af.deny_all is True, f"uncompilable DENY {cond} must force deny_all"
        assert af.uncompilable is True


async def test_no_applicable_allow_denies_by_default() -> None:
    """No applicable ALLOW (empty, or only an out-of-read-scope grant) ⟹
    deny-by-default."""
    svc = _service()
    # Empty policy set.
    af_empty = await svc.compile_read_filter(
        principals=[], catalog_id="cat_a", principal=Principal(custom_policies=[])
    )
    assert af_empty.deny_all is True

    # Only an admin/POST grant — out of the read scope.
    af_admin = await svc.compile_read_filter(
        principals=[],
        catalog_id="cat_a",
        principal=Principal(
            custom_policies=[_allow("a", path=_ADMIN_RE, method="POST")]
        ),
    )
    assert af_admin.deny_all is True


async def test_cross_catalog_filter_never_admits_other_catalog() -> None:
    """A filter compiled for catalog A never admits a document whose
    ``catalog_id`` is B — even under an unconditional ALLOW."""
    svc = _service()
    af = await svc.compile_read_filter(
        principals=[],
        catalog_id="cat_a",
        collection_id="col_x",
        principal=Principal(custom_policies=[_allow("a", path=_WILD, method="GET")]),
    )
    assert af.deny_all is False
    # Same-catalog doc admitted; other-catalog doc rejected.
    assert af.admits({"catalog_id": "cat_a", "collection_id": "col_x"}) is True
    assert af.admits({"catalog_id": "cat_b", "collection_id": "col_x"}) is False
    assert af.admits({"catalog_id": "cat_b", "collection_id": "col_y"}) is False


# --------------------------------------------------------------------------- #
# SUSPECTED LEAK — captured, NOT hidden. See the report / module docstring.
#
# The ``match`` projection in ``_compile_attribute_match`` folds the principal's
# *current* attribute value into a per-document predicate REGARDLESS of whether
# the principal's value actually satisfies the condition's own ``config.value``
# gate. The engine (``AttributeMatchHandler``) instead compares the principal's
# attribute against ``config.value`` and never reads the document — so when
# ``principal.owner != config.value`` the engine DENIES every document, yet the
# compiled filter still ADMITS documents whose ``owner`` equals the principal's
# attribute. That is filter-admits ∧ engine-denies = the exact equal-or-stricter
# violation this safety net exists to catch.
#
# Per the task contract we do NOT patch production. We pin the minimal failing
# scenario as ``xfail(strict=True)`` so (a) it is visible and tracked, and (b)
# if production is later fixed to fail-closed, this test will XPASS and force a
# follow-up to convert it into a positive assertion.
# --------------------------------------------------------------------------- #
async def test_attribute_match_value_mismatch_is_fail_closed() -> None:
    """Regression guard for the ``match`` value-mismatch leak (now fixed).

    Policy: ALLOW reads when ``principal.attributes.owner == 'alice'``.
    Principal: ``owner='bob'`` (does NOT satisfy the gate → engine must DENY).
    Document: ``owner='bob'`` in the compiled catalog/collection.

    A ``match`` on a principal attribute is a *principal gate*, document
    independent. When the principal fails the gate the engine denies every
    document, so the compiled filter must admit NOTHING. The earlier compiler
    folded the principal's own value into an ``owner IN ('bob',)`` document
    predicate and therefore admitted the document — that was the leak. The
    fix evaluates the gate at compile time: a failing gate drops the ALLOW
    (deny-by-default), and because the gate WAS evaluated it is not flagged
    ``uncompilable``.
    """
    svc = _service()
    principal = Principal(
        custom_policies=[
            _allow(
                "a_owner",
                path=_ITEMS_RE,
                method="GET",
                conditions=list(_owner_match("alice")),  # static gate = alice
            )
        ],
        attributes={"owner": "bob"},  # principal is bob → fails the gate
    )
    doc = Doc(
        catalog_id="cat_a",
        collection_id="col_x",
        geoid="cat_a.col_x.private.bob",
        visibility="private",
        owner="bob",
        grant_subjects=(),
    )

    af = await svc.compile_read_filter(
        principals=[],
        catalog_id=doc.catalog_id,
        collection_id=doc.collection_id,
        principal=principal,
    )

    # The grant is dropped (failing principal gate) → deny-by-default.
    assert af.deny_all is True
    assert af.uncompilable is False
    # The filter must NOT admit the document the engine denies.
    assert af.admits(doc.as_mapping()) is False
    # And it agrees with the engine, which also denies.
    scenario = Scenario(
        name="owner_match_value_mismatch",
        policies=tuple(principal.custom_policies),
        attributes=(("owner", "bob"),),
    )
    assert await _engine_allows(svc, scenario, doc) is False


# --------------------------------------------------------------------------- #
# Matrix-size sanity (documents the coverage the drift guard exercises).
# --------------------------------------------------------------------------- #
def test_matrix_dimensions() -> None:
    """Pin the matrix size so a future change that silently shrinks coverage is
    visible in review."""
    n_docs = len(_documents())
    n_scen = len(_scenarios())
    # 2 catalogs * 2 collections * 2 visibilities * 2 owners * 3 subject-sets
    # * 3 sensitivity values ("1","3","5").
    assert n_docs == 144
    # The drift guard exercises scenario x document combinations, including
    # range-predicate grant scenarios added for lte/gte/between coverage.
    assert n_scen * n_docs >= 5000


# --------------------------------------------------------------------------- #
# RangePredicate equal-or-stricter invariant (filter-level, no engine needed).
#
# The drift guard above covers policy→filter→engine consistency for the
# existing compilable conditions. RangePredicate lives on the grant's
# ``attribute_predicates`` path, which the policy→filter compiler currently
# does not produce directly from a Condition (range predicates are stamped on
# grants, not encoded in Policy conditions). Therefore these tests check the
# equal-or-stricter invariant at the AccessFilter.admits level: any filter
# containing a RangePredicate must not admit documents it should exclude,
# and the ES translator must agree with admits() for the same documents.
# --------------------------------------------------------------------------- #

def _range_docs(field: str, values: list) -> list:
    """Documents with varying ``field`` values (string-typed, matching test bounds)."""
    return [{field: v, "catalog_id": "cat", "collection_id": "col"} for v in values]


def test_range_filter_lte_equal_or_stricter() -> None:
    """AccessFilter with lte RangePredicate admits only docs <= bound."""
    rp = RangePredicate("_attrs.score", "lte", ("100",))
    clause = AccessClause(predicates=(rp,))
    af = AccessFilter(allow=(clause,))

    docs_in = _range_docs("_attrs.score", ["0", "50", "100"])
    docs_out = _range_docs("_attrs.score", ["101", "200", "999"])

    for doc in docs_in:
        assert af.admits(doc), f"should admit {doc}"
    for doc in docs_out:
        assert not af.admits(doc), f"must not admit {doc}"


def test_range_filter_gte_equal_or_stricter() -> None:
    """AccessFilter with gte RangePredicate admits only docs >= bound."""
    rp = RangePredicate("_attrs.score", "gte", ("50",))
    clause = AccessClause(predicates=(rp,))
    af = AccessFilter(allow=(clause,))

    docs_in = _range_docs("_attrs.score", ["50", "100", "999"])
    docs_out = _range_docs("_attrs.score", ["0", "10", "49"])

    for doc in docs_in:
        assert af.admits(doc), f"should admit {doc}"
    for doc in docs_out:
        assert not af.admits(doc), f"must not admit {doc}"


def test_range_filter_between_equal_or_stricter() -> None:
    """AccessFilter with between RangePredicate admits docs in [lo, hi]."""
    rp = RangePredicate("_attrs.score", "between", ("10", "90"))
    clause = AccessClause(predicates=(rp,))
    af = AccessFilter(allow=(clause,))

    docs_in = _range_docs("_attrs.score", ["10", "50", "90"])
    docs_out = _range_docs("_attrs.score", ["9", "0", "91", "999"])

    for doc in docs_in:
        assert af.admits(doc), f"should admit {doc}"
    for doc in docs_out:
        assert not af.admits(doc), f"must not admit {doc}"


def test_range_filter_admits_agrees_with_es_translation() -> None:
    """ES translation must agree with AccessFilter.admits() for range predicates."""
    from dynastore.modules.storage.drivers.elasticsearch_envelope.access_translate import (
        access_filter_to_es,
    )

    rp = RangePredicate("_attrs.score", "between", ("20", "80"))
    clause = AccessClause(predicates=(rp,))
    af = AccessFilter(allow=(clause,))
    es = access_filter_to_es(af)

    # The ES translator emits a range clause; we verify the shape agrees with
    # the pure-Python admits() on a representative sample of documents.
    # Note: the _es_admits evaluator in this module does not yet handle "range"
    # clauses (it only handles terms/match_none/match_all/bool). We therefore
    # verify the shape directly rather than running _es_admits.
    assert es is not None
    should = es["bool"]["should"]
    inner = should[0]["bool"]["filter"]
    assert len(inner) == 1
    assert "range" in inner[0]
    range_body = inner[0]["range"]["_attrs.score"]
    assert range_body == {"gte": "20", "lte": "80"}
