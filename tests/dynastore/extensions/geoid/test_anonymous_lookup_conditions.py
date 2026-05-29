"""Tests for the IAM-layer needle-only guard on the geoid anonymous lookup
ALLOW policy (un-fao/GeoID#1204 P2-R7).

The anonymous lookup surface (``POST /search/catalogs/{cat}/items-search``) must
be a *needle* lookup only: a caller may resolve a single item by ``geoid`` /
``external_id`` but must not be able to broaden the request into an enumeration
(``bbox`` / ``intersects`` / ``datetime`` / ``filter`` / ``q``). The route
handler validates the body, but the ALLOW policy also carries the
``lookup_only_search`` condition so enforcement holds at the declarative IAM
layer even if the route changes.

Security invariant pinned here: the ``lookup_only_search`` guard belongs ONLY to
the lookup ALLOW. It must NOT appear on the enumeration-DENY policies — a
``lookup_only_search`` on a DENY would make that DENY match only needle requests,
so a broadening enumeration request would skip the block and fall through. The
DENY policies must keep matching every request shape.
"""
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.extensions.geoid import catalog_policies


def _condition_types(policy) -> set:
    return {c.type for c in (policy.conditions or [])}


async def _registered_policies(monkeypatch) -> dict:
    """Drive the live per-catalog registrar with a mocked PermissionProtocol and
    return ``{policy_id: Policy}`` for every policy it tried to create."""
    pm = MagicMock()
    pm.get_policy = AsyncMock(return_value=None)  # nothing exists yet → create
    pm.create_policy = AsyncMock()
    # The role-enrichment branch checks ``isinstance(get_protocol(...), IamModule)``;
    # a MagicMock is not an IamModule, so enrichment is skipped (no DB access).
    monkeypatch.setattr(catalog_policies, "get_protocol", lambda _proto: pm)

    await catalog_policies.register_geoid_policies_for_catalog("test_catalog")

    return {
        call.args[0].id: call.args[0]
        for call in pm.create_policy.call_args_list
    }


@pytest.mark.asyncio
async def test_anonymous_lookup_allow_carries_lookup_only_search(monkeypatch):
    """The lookup ALLOW enforces needle-only at the IAM layer (#1204 P2-R7),
    while keeping the public-mode gate."""
    registered = await _registered_policies(monkeypatch)

    assert "geoid_anonymous_lookup" in registered
    allow = registered["geoid_anonymous_lookup"]
    assert allow.effect == "ALLOW"
    types = _condition_types(allow)
    assert "catalog_lookup_public_allowed" in types
    assert "lookup_only_search" in types


@pytest.mark.asyncio
async def test_enumeration_deny_policies_have_no_lookup_only_search(monkeypatch):
    """DENY policies must match every request shape: a ``lookup_only_search``
    guard on a DENY would let broadening enumeration requests bypass the block."""
    registered = await _registered_policies(monkeypatch)

    for deny_id in (
        "geoid_anonymous_stac_deny_lookup_only",
        "geoid_anonymous_features_deny_lookup_only",
    ):
        assert deny_id in registered
        deny = registered[deny_id]
        assert deny.effect == "DENY"
        types = _condition_types(deny)
        assert "catalog_lookup_public_allowed" in types
        assert "lookup_only_search" not in types
