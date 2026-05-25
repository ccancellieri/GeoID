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
"""Unit tests for the shared read-scope compile helper used by every read
entry point that dispatches to an access-aware storage driver."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from dynastore.models.protocols.access_filter import AccessClause, AccessFilter, FieldPredicate
from dynastore.modules.storage import access_scope


class _FakePerms:
    """Records the compile_read_filter call and returns a canned filter."""

    def __init__(self, result: AccessFilter) -> None:
        self.result = result
        self.calls: list = []

    async def compile_read_filter(
        self, principals, catalog_id=None, collection_id=None, *, principal=None
    ):
        self.calls.append(
            {
                "principals": principals,
                "catalog_id": catalog_id,
                "collection_id": collection_id,
                "principal": principal,
            }
        )
        return self.result


def _patch_get_protocol(monkeypatch, value) -> None:
    # The helper imports get_protocol from dynastore.tools.discovery at call time.
    import dynastore.tools.discovery as discovery

    monkeypatch.setattr(discovery, "get_protocol", lambda _proto: value)


def _es_admits(clause, doc) -> bool:
    """Pure-Python evaluator of the ES clauses ``access_filter_to_es`` emits.

    Supports exactly the shapes that translator produces:
    ``match_none`` / ``match_all`` / ``bool`` with ``filter`` (AND of ``terms``),
    ``should`` (OR with ``minimum_should_match``) and ``must_not``. Used to prove
    the ES translation agrees with ``AccessFilter.admits`` for the same docs.
    """
    if clause is None:
        return True
    if "match_none" in clause:
        return False
    if "match_all" in clause:
        return True
    if "terms" in clause:
        (field, values), = clause["terms"].items()
        actual = doc.get(field)
        allowed = set(values)
        if isinstance(actual, (list, tuple, set)):
            return any(v in allowed for v in actual)
        return actual in allowed
    if "bool" in clause:
        body = clause["bool"]
        ok = True
        for sub in body.get("filter", []):
            ok = ok and _es_admits(sub, doc)
        for sub in body.get("must", []):
            ok = ok and _es_admits(sub, doc)
        for sub in body.get("must_not", []):
            ok = ok and not _es_admits(sub, doc)
        should = body.get("should")
        if should is not None:
            mim = body.get("minimum_should_match", 0)
            n = sum(1 for sub in should if _es_admits(sub, doc))
            ok = ok and (n >= mim)
        return ok
    raise AssertionError(f"unexpected clause shape: {clause!r}")


@pytest.mark.asyncio
async def test_no_permission_protocol_fails_closed(monkeypatch):
    _patch_get_protocol(monkeypatch, None)
    af = await access_scope.compile_read_access_filter(
        catalog_id="acme", collections=["c"], principals=["anon"], principal=None
    )
    assert isinstance(af, AccessFilter)
    assert af.deny_all is True


@pytest.mark.asyncio
async def test_single_collection_compiles_at_collection_scope(monkeypatch):
    perms = _FakePerms(
        AccessFilter(allow=(AccessClause((FieldPredicate("visibility", ("public",)),)),))
    )
    _patch_get_protocol(monkeypatch, perms)
    await access_scope.compile_read_access_filter(
        catalog_id="acme", collections=["only"], principals=["r"], principal=None
    )
    assert perms.calls[0]["catalog_id"] == "acme"
    assert perms.calls[0]["collection_id"] == "only"


class _PerCollectionPerms:
    """Returns a different compiled filter per ``collection_id``.

    Mirrors the real engine, which pins every clause to the requested
    ``collection_id`` via ``_scope_clause``: the canned filters below carry a
    ``collection_id`` predicate in each clause so the union node behaves exactly
    as production does (a sub-filter only ever admits its own collection's docs).
    """

    def __init__(self, by_collection: dict) -> None:
        # by_collection: collection_id -> AccessFilter
        self.by_collection = by_collection
        self.calls: list = []

    async def compile_read_filter(
        self, principals, catalog_id=None, collection_id=None, *, principal=None
    ):
        self.calls.append(
            {
                "principals": principals,
                "catalog_id": catalog_id,
                "collection_id": collection_id,
                "principal": principal,
            }
        )
        return self.by_collection.get(
            collection_id, AccessFilter.deny_everything()
        )


def _doc(collection_id, *, visibility="private", owner="bob"):
    return {
        "catalog_id": "acme",
        "collection_id": collection_id,
        "visibility": visibility,
        "owner": owner,
        "id": f"{collection_id}.{visibility}.{owner}",
    }


@pytest.mark.asyncio
async def test_multi_collection_compiles_once_per_collection(monkeypatch):
    perms = _PerCollectionPerms(
        {
            "a": AccessFilter(allow=(AccessClause((
                FieldPredicate("collection_id", ("a",)),
                FieldPredicate("visibility", ("public",)),
            )),)),
            "b": AccessFilter(allow=(AccessClause((
                FieldPredicate("collection_id", ("b",)),
                FieldPredicate("visibility", ("public",)),
            )),)),
        }
    )
    _patch_get_protocol(monkeypatch, perms)
    await access_scope.compile_read_access_filter(
        catalog_id="acme", collections=["a", "b"], principals=["r"], principal=None
    )
    # Compiled ONCE PER COLLECTION (per-collection differential grants), each
    # pinned to its own collection_id.
    pinned = sorted(c["collection_id"] for c in perms.calls)
    assert pinned == ["a", "b"]


@pytest.mark.asyncio
async def test_multi_collection_access_to_a_not_b_excludes_b(monkeypatch):
    # Principal can read collection A (any visibility) but NOTHING in B or C.
    perms = _PerCollectionPerms(
        {
            "a": AccessFilter(allow=(AccessClause((
                FieldPredicate("collection_id", ("a",)),
            )),)),
            # b, c absent → deny_everything (no access there).
        }
    )
    _patch_get_protocol(monkeypatch, perms)
    af = await access_scope.compile_read_access_filter(
        catalog_id="acme", collections=["a", "b", "c"], principals=["r"], principal=None
    )
    # A's docs admitted; B's and C's excluded — and a deny in B/C never
    # suppresses A's docs (exclusion-union, no cross-contamination).
    assert af.admits(_doc("a")) is True
    assert af.admits(_doc("b")) is False
    assert af.admits(_doc("c")) is False
    # ES translation must agree with admits() for the same docs.
    from dynastore.modules.storage.drivers.elasticsearch_envelope.access_translate import (
        access_filter_to_es,
    )
    es = access_filter_to_es(af)
    assert _es_admits(es, _doc("a")) is True
    assert _es_admits(es, _doc("b")) is False
    assert _es_admits(es, _doc("c")) is False


@pytest.mark.asyncio
async def test_multi_collection_differential_grants_no_cross_contamination(monkeypatch):
    # A allows visibility=public; B allows owner=me (its own collection only).
    perms = _PerCollectionPerms(
        {
            "a": AccessFilter(allow=(AccessClause((
                FieldPredicate("collection_id", ("a",)),
                FieldPredicate("visibility", ("public",)),
            )),)),
            "b": AccessFilter(allow=(AccessClause((
                FieldPredicate("collection_id", ("b",)),
                FieldPredicate("owner", ("me",)),
            )),)),
        }
    )
    _patch_get_protocol(monkeypatch, perms)
    af = await access_scope.compile_read_access_filter(
        catalog_id="acme", collections=["a", "b"], principals=["r"], principal=None
    )
    from dynastore.modules.storage.drivers.elasticsearch_envelope.access_translate import (
        access_filter_to_es,
    )
    es = access_filter_to_es(af)

    public_a = _doc("a", visibility="public", owner="bob")
    public_b = _doc("b", visibility="public", owner="bob")
    owned_b = _doc("b", visibility="private", owner="me")
    # A allows public → a public doc in A admits.
    assert af.admits(public_a) is True
    # B only allows owner=me → a public doc in B does NOT admit (A's public
    # grant must NOT leak into B).
    assert af.admits(public_b) is False
    # An owned doc in B admits.
    assert af.admits(owned_b) is True
    # ES path agrees with Python path for all three.
    for doc in (public_a, public_b, owned_b):
        assert _es_admits(es, doc) == af.admits(doc), doc


@pytest.mark.asyncio
async def test_multi_collection_deny_in_one_does_not_suppress_other(monkeypatch):
    # A: public allowed, BUT owner=blocked denied (deny scoped to A).
    # B: public allowed, no deny.
    perms = _PerCollectionPerms(
        {
            "a": AccessFilter(
                allow=(AccessClause((
                    FieldPredicate("collection_id", ("a",)),
                    FieldPredicate("visibility", ("public",)),
                )),),
                deny=(AccessClause((
                    FieldPredicate("collection_id", ("a",)),
                    FieldPredicate("owner", ("blocked",)),
                )),),
            ),
            "b": AccessFilter(allow=(AccessClause((
                FieldPredicate("collection_id", ("b",)),
                FieldPredicate("visibility", ("public",)),
            )),)),
        }
    )
    _patch_get_protocol(monkeypatch, perms)
    af = await access_scope.compile_read_access_filter(
        catalog_id="acme", collections=["a", "b"], principals=["r"], principal=None
    )
    from dynastore.modules.storage.drivers.elasticsearch_envelope.access_translate import (
        access_filter_to_es,
    )
    es = access_filter_to_es(af)

    blocked_a = _doc("a", visibility="public", owner="blocked")
    blocked_b = _doc("b", visibility="public", owner="blocked")
    # A's deny excludes a blocked owner in A.
    assert af.admits(blocked_a) is False
    # CRITICAL: A's deny on owner=blocked must NOT suppress B's public doc with
    # the same owner — the deny is scoped to A's sub-filter only.
    assert af.admits(blocked_b) is True
    for doc in (blocked_a, blocked_b):
        assert _es_admits(es, doc) == af.admits(doc), doc


@pytest.mark.asyncio
async def test_multi_collection_all_denied_fails_closed(monkeypatch):
    # No grants for any requested collection → union of deny_everything → deny.
    perms = _PerCollectionPerms({})
    _patch_get_protocol(monkeypatch, perms)
    af = await access_scope.compile_read_access_filter(
        catalog_id="acme", collections=["a", "b"], principals=["r"], principal=None
    )
    assert af.deny_all is True
    assert af.admits(_doc("a", visibility="public")) is False
    assert af.admits(_doc("b", visibility="public")) is False


@pytest.mark.asyncio
async def test_none_principals_normalize_to_empty_list(monkeypatch):
    perms = _FakePerms(AccessFilter.deny_everything())
    _patch_get_protocol(monkeypatch, perms)
    await access_scope.compile_read_access_filter(
        catalog_id="acme", collections=None, principals=None, principal=None
    )
    assert perms.calls[0]["principals"] == []


def test_principals_from_request_state_flat_list():
    request = SimpleNamespace(
        state=SimpleNamespace(
            principal="P", principal_id="user:alice", principal_role=["admin", "reader"]
        )
    )
    principals, principal = access_scope.principals_from_request_state(request)
    assert principal == "P"
    assert principals == ["user:alice", "admin", "reader"]


def test_principals_from_request_state_anonymous_scalar_role():
    request = SimpleNamespace(
        state=SimpleNamespace(
            principal=None, principal_id=None, principal_role="unauthenticated"
        )
    )
    principals, principal = access_scope.principals_from_request_state(request)
    assert principal is None
    assert principals == ["unauthenticated"]
