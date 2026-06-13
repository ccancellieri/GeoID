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

"""Unit tests for ``dynastore.models.protocols.visibility``.

Covers: RequestVisibility + contextvar lifecycle, visibility_bypass,
extract_id_constraint projection rules, and the resolve_* resolution
helpers (no-context, missing provider, provider raising, provider returning).
"""
from __future__ import annotations

import pytest

from dynastore.models.protocols.access_filter import (
    AccessClause,
    AccessFilter,
    FieldPredicate,
    RangePredicate,
)
from dynastore.models.protocols.visibility import (
    RequestVisibility,
    extract_id_constraint,
    get_request_visibility,
    reset_request_visibility,
    resolve_catalog_listing_ids,
    resolve_collection_listing_ids,
    set_request_visibility,
    visibility_bypass,
)
from dynastore.tools.discovery import register_plugin, unregister_plugin


# ---------------------------------------------------------------------------
# Contextvar lifecycle
# ---------------------------------------------------------------------------


def test_get_visibility_default_is_none():
    # No middleware has published a snapshot; must return None.
    assert get_request_visibility() is None


def test_set_and_get_visibility():
    vis = RequestVisibility(principals=("user:alice", "role:editor"))
    token = set_request_visibility(vis)
    try:
        assert get_request_visibility() is vis
    finally:
        reset_request_visibility(token)


def test_reset_visibility_restores_previous():
    prior = get_request_visibility()
    vis = RequestVisibility(principals=("user:bob",))
    token = set_request_visibility(vis)
    reset_request_visibility(token)
    assert get_request_visibility() is prior


def test_visibility_bypass_blanks_then_restores():
    vis = RequestVisibility(principals=("user:alice",))
    token = set_request_visibility(vis)
    try:
        with visibility_bypass():
            # Inside bypass the contextvar is blanked.
            assert get_request_visibility() is None
        # After the context manager, the original snapshot is back.
        assert get_request_visibility() is vis
    finally:
        reset_request_visibility(token)


def test_visibility_bypass_restores_on_exception():
    vis = RequestVisibility(principals=("user:alice",))
    token = set_request_visibility(vis)
    try:
        with pytest.raises(ValueError):
            with visibility_bypass():
                raise ValueError("oops")
        assert get_request_visibility() is vis
    finally:
        reset_request_visibility(token)


# ---------------------------------------------------------------------------
# extract_id_constraint
# ---------------------------------------------------------------------------


def test_extract_deny_everything_returns_empty_frozenset():
    flt = AccessFilter.deny_everything()
    assert extract_id_constraint(flt) == frozenset()


def test_extract_allow_everything_returns_none():
    flt = AccessFilter.allow_everything()
    assert extract_id_constraint(flt) is None


def test_extract_single_id_clause_returns_values():
    flt = AccessFilter.from_clauses(
        [AccessClause((FieldPredicate("id", ("cat-a", "cat-b")),))]
    )
    result = extract_id_constraint(flt)
    assert result == frozenset({"cat-a", "cat-b"})


def test_extract_multiple_id_clauses_unions_values():
    flt = AccessFilter.from_clauses([
        AccessClause((FieldPredicate("id", ("cat-a",)),)),
        AccessClause((FieldPredicate("id", ("cat-b", "cat-c")),)),
    ])
    result = extract_id_constraint(flt)
    assert result == frozenset({"cat-a", "cat-b", "cat-c"})


def test_extract_clause_with_extra_predicates_is_dropped():
    # A clause with two predicates cannot be expressed as a plain id list;
    # it is conservatively dropped, so the result may under-return.
    extra = AccessClause((
        FieldPredicate("id", ("cat-a",)),
        RangePredicate("_attrs.size", "lte", ("100",)),
    ))
    pure_id = AccessClause((FieldPredicate("id", ("cat-b",)),))
    flt = AccessFilter.from_clauses([extra, pure_id])
    result = extract_id_constraint(flt)
    # cat-b survives; cat-a's clause is dropped (stricter / under-return)
    assert "cat-b" in result
    assert "cat-a" not in result


def test_extract_filter_with_deny_clauses_returns_empty():
    # Any deny clause on the listing AccessFilter signals the compiler
    # produced a shape this projection cannot express; fail closed.
    flt = AccessFilter(
        deny_all=False,
        allow_all=False,
        allow=(AccessClause((FieldPredicate("id", ("cat-a",)),)),),
        deny=(AccessClause((FieldPredicate("id", ("cat-b",)),)),),
    )
    result = extract_id_constraint(flt)
    assert result == frozenset()


def test_extract_filter_with_union_returns_empty():
    sub = AccessFilter.from_clauses(
        [AccessClause((FieldPredicate("id", ("cat-a",)),))]
    )
    flt = AccessFilter.union_of([sub, sub])
    result = extract_id_constraint(flt)
    assert result == frozenset()


def test_extract_allow_all_with_deny_returns_empty():
    # allow_all + deny is not unconditional; the projection cannot express it.
    flt = AccessFilter(
        allow_all=True,
        deny=(AccessClause((FieldPredicate("id", ("cat-x",)),)),),
    )
    result = extract_id_constraint(flt)
    assert result == frozenset()


# ---------------------------------------------------------------------------
# resolve_catalog_listing_ids / resolve_collection_listing_ids
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_catalog_no_context_returns_none():
    # No visibility published → listing is unfiltered.
    assert get_request_visibility() is None
    result = await resolve_catalog_listing_ids()
    assert result is None


@pytest.mark.asyncio
async def test_resolve_collection_no_context_returns_none():
    assert get_request_visibility() is None
    result = await resolve_collection_listing_ids("cat-x")
    assert result is None


@pytest.mark.asyncio
async def test_resolve_catalog_with_context_no_provider_returns_empty():
    vis = RequestVisibility(principals=("user:alice",))
    token = set_request_visibility(vis)
    try:
        result = await resolve_catalog_listing_ids()
        assert result == frozenset()
    finally:
        reset_request_visibility(token)


@pytest.mark.asyncio
async def test_resolve_catalog_provider_raises_returns_empty():
    """A registered provider that raises must fail closed to empty frozenset."""

    class _BrokenProvider:
        async def catalog_listing_filter(self, visibility):
            raise RuntimeError("simulated failure")

        async def collection_listing_filter(self, visibility, catalog_id):
            raise RuntimeError("simulated failure")

        async def asset_listing_filter(self, visibility, catalog_id, collection_id):
            raise RuntimeError("simulated failure")

    provider = _BrokenProvider()
    register_plugin(provider)
    vis = RequestVisibility(principals=("user:alice",))
    token = set_request_visibility(vis)
    try:
        result = await resolve_catalog_listing_ids()
        assert result == frozenset()
    finally:
        reset_request_visibility(token)
        unregister_plugin(provider)


@pytest.mark.asyncio
async def test_resolve_catalog_provider_returns_ids():
    """Provider returning an id-list filter → extract produces that frozenset."""

    class _AllowCatsProvider:
        async def catalog_listing_filter(self, visibility):
            return AccessFilter.from_clauses(
                [AccessClause((FieldPredicate("id", ("cat-a", "cat-b")),))]
            )

        async def collection_listing_filter(self, visibility, catalog_id):
            return AccessFilter.allow_everything()

        async def asset_listing_filter(self, visibility, catalog_id, collection_id):
            return AccessFilter.allow_everything()

    provider = _AllowCatsProvider()
    register_plugin(provider)
    vis = RequestVisibility(principals=("user:alice",))
    token = set_request_visibility(vis)
    try:
        result = await resolve_catalog_listing_ids()
        assert result == frozenset({"cat-a", "cat-b"})
    finally:
        reset_request_visibility(token)
        unregister_plugin(provider)


@pytest.mark.asyncio
async def test_resolve_catalog_provider_returns_allow_everything_gives_none():
    """Provider returning allow_everything → no filtering (None)."""

    class _AllowAllProvider:
        async def catalog_listing_filter(self, visibility):
            return AccessFilter.allow_everything()

        async def collection_listing_filter(self, visibility, catalog_id):
            return AccessFilter.allow_everything()

        async def asset_listing_filter(self, visibility, catalog_id, collection_id):
            return AccessFilter.allow_everything()

    provider = _AllowAllProvider()
    register_plugin(provider)
    vis = RequestVisibility(principals=("user:sysadmin",))
    token = set_request_visibility(vis)
    try:
        result = await resolve_catalog_listing_ids()
        assert result is None
    finally:
        reset_request_visibility(token)
        unregister_plugin(provider)
