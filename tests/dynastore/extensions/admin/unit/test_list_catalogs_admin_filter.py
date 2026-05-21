#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License");

"""Unit tests for `_catalog_admin_filter_ids` + `GET /admin/catalogs` filtering (#723).

The handler must surface the full catalog list to sysadmin / platform-admin
callers (current behaviour) and a per-caller filtered list to catalog-tier
admins (the bug-fix surface). Both behaviours are exercised here in
isolation: ``CatalogsProtocol`` and the membership cache
(``MembershipCacheProtocol.get_membership``) are replaced with stubs so no
database or registry runs.
"""
from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.extensions.admin.admin_service import (
    AdminService,
    _catalog_admin_filter_ids,
)


_GET_PROTOCOL = "dynastore.extensions.admin.admin_service.get_protocol"


def _make_request(principal: Optional[Any]) -> Any:
    state = SimpleNamespace(principal=principal)
    return SimpleNamespace(state=state)


def _make_principal(
    *,
    provider: str = "local",
    subject_id: str = "alice",
    roles: Optional[list[str]] = None,
) -> Any:
    return SimpleNamespace(
        provider=provider, subject_id=subject_id, roles=roles or []
    )


def _make_membership_cache(membership: Optional[Dict[str, Any]]) -> MagicMock:
    """A stub ``MembershipCacheProtocol`` whose ``get_membership`` coroutine
    returns the given membership mapping (or an empty one).
    """
    cache = MagicMock()
    cache.get_membership = AsyncMock(return_value=membership or {})
    return cache


@contextmanager
def _patched(membership: Optional[Dict[str, Any]] = None):
    """Patch ``get_protocol`` so that ``MembershipCacheProtocol`` resolves to a
    stub returning ``membership``; every other protocol resolves to ``None``.

    Mirrors the production path in ``_catalog_admin_filter_ids``, which calls
    ``get_protocol(MembershipCacheProtocol).get_membership(...)``.
    """
    cache = _make_membership_cache(membership)

    def _get_proto(cls):
        from dynastore.models.protocols.membership_cache import (
            MembershipCacheProtocol,
        )
        if cls is MembershipCacheProtocol:
            return cache
        return None

    with patch(_GET_PROTOCOL, side_effect=_get_proto):
        yield cache


@pytest.mark.asyncio
async def test_no_principal_returns_none():
    """Anonymous callers that reach here have an authoritative ALLOW; the
    filter must be a no-op so they see the full picker.
    """
    req = _make_request(principal=None)
    assert await _catalog_admin_filter_ids(req) is None


@pytest.mark.asyncio
async def test_sysadmin_role_returns_none():
    req = _make_request(principal=_make_principal(roles=["sysadmin"]))
    assert await _catalog_admin_filter_ids(req) is None


@pytest.mark.asyncio
async def test_admin_role_returns_none():
    req = _make_request(principal=_make_principal(roles=["admin"]))
    assert await _catalog_admin_filter_ids(req) is None


@pytest.mark.asyncio
async def test_platform_grant_returns_none():
    """A principal without sysadmin/admin role but with a platform-scope
    grant (`membership.platform=True`) gets the unfiltered list.
    """
    req = _make_request(principal=_make_principal(roles=["user"]))
    with _patched(membership={"platform": True, "catalog_roles": {}}):
        assert await _catalog_admin_filter_ids(req) is None


@pytest.mark.asyncio
async def test_catalog_admin_returns_only_admin_catalogs():
    """A catalog-tier admin sees only the catalogs they actually admin —
    catalogs where they hold a non-admin role are filtered out.
    """
    req = _make_request(principal=_make_principal(roles=["catalog_admin"]))
    with _patched(membership={
        "platform": False,
        "catalog_roles": {
            "cat-a": ["admin"],
            "cat-b": ["viewer"],   # member but not admin — filtered
            "cat-c": ["admin", "viewer"],
        },
    }):
        result = await _catalog_admin_filter_ids(req)
    assert result == {"cat-a", "cat-c"}


@pytest.mark.asyncio
async def test_catalog_member_without_admin_role_returns_empty_set():
    """A principal with only catalog viewer/user grants — no catalog-tier
    admin role — sees no catalogs in the picker.
    """
    req = _make_request(principal=_make_principal(roles=["user"]))
    with _patched(membership={
        "platform": False,
        "catalog_roles": {"cat-a": ["viewer"], "cat-b": ["user"]},
    }):
        assert await _catalog_admin_filter_ids(req) == set()


@pytest.mark.asyncio
async def test_principal_without_provider_or_subject_returns_empty_set():
    """Service principals (no provider/subject) can't be looked up in the
    grants table; the safe answer is "show nothing".
    """
    req = _make_request(principal=SimpleNamespace(
        provider=None, subject_id=None, roles=["custom"],
    ))
    assert await _catalog_admin_filter_ids(req) == set()


@pytest.mark.asyncio
async def test_no_membership_cache_protocol_returns_empty_set():
    """Slim deployments without MembershipCacheProtocol can't resolve
    memberships; the catalog admin sees nothing rather than the full list.
    """
    req = _make_request(principal=_make_principal(roles=["catalog_admin"]))

    def _get_proto(_cls):
        return None  # MembershipCacheProtocol → None

    with patch(_GET_PROTOCOL, side_effect=_get_proto):
        assert await _catalog_admin_filter_ids(req) == set()


# ---------------------------------------------------------------------------
# Handler-level integration: GET /admin/catalogs response shape.
# ---------------------------------------------------------------------------

_handler = AdminService.list_catalogs_for_admin


def _make_catalog_obj(cat_id: str, title: str = "") -> Any:
    m = MagicMock()
    m.id = cat_id
    m.model_dump = MagicMock(return_value={"title": title or cat_id})
    return m


def _make_catalogs_svc(cats: list[Any]) -> MagicMock:
    svc = MagicMock()
    svc.list_catalogs = AsyncMock(return_value=cats)
    return svc


@pytest.mark.asyncio
async def test_handler_returns_full_list_for_sysadmin():
    """Sysadmin: ``_catalog_admin_filter_ids`` returns None → handler skips
    the post-filter and returns every catalog from storage.
    """
    cats = [_make_catalog_obj("a"), _make_catalog_obj("b"), _make_catalog_obj("c")]
    catalogs_svc = _make_catalogs_svc(cats)
    req = _make_request(principal=_make_principal(roles=["sysadmin"]))

    with patch(_GET_PROTOCOL, return_value=catalogs_svc):
        result = await _handler(req, limit=200, offset=0, lang="en", q=None)

    assert [c["id"] for c in result] == ["a", "b", "c"]


@pytest.mark.asyncio
async def test_handler_filters_list_for_catalog_admin():
    """Catalog-tier admin: handler post-filters storage's full list down to
    the catalogs returned by ``_catalog_admin_filter_ids``.
    """
    cats = [_make_catalog_obj("a"), _make_catalog_obj("b"), _make_catalog_obj("c")]
    catalogs_svc = _make_catalogs_svc(cats)
    req = _make_request(principal=_make_principal(roles=["catalog_admin"]))

    cache = _make_membership_cache({
        "platform": False,
        "catalog_roles": {"a": ["admin"], "c": ["viewer"]},
    })

    def _get_proto(cls):
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        from dynastore.models.protocols.membership_cache import (
            MembershipCacheProtocol,
        )
        if cls is CatalogsProtocol:
            return catalogs_svc
        if cls is MembershipCacheProtocol:
            return cache
        return None

    with patch(_GET_PROTOCOL, side_effect=_get_proto):
        result = await _handler(req, limit=200, offset=0, lang="en", q=None)

    assert [c["id"] for c in result] == ["a"]


@pytest.mark.asyncio
async def test_handler_returns_empty_when_catalog_admin_has_no_admin_grants():
    """A principal who reaches the route but admins zero catalogs gets an
    empty list, never an unfiltered one — protects against accidental
    over-disclosure when the policy layer allows a borderline caller through.
    """
    cats = [_make_catalog_obj("a"), _make_catalog_obj("b")]
    catalogs_svc = _make_catalogs_svc(cats)
    req = _make_request(principal=_make_principal(roles=["catalog_admin"]))

    cache = _make_membership_cache({
        "platform": False,
        "catalog_roles": {"a": ["viewer"], "b": ["user"]},
    })

    def _get_proto(cls):
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        from dynastore.models.protocols.membership_cache import (
            MembershipCacheProtocol,
        )
        if cls is CatalogsProtocol:
            return catalogs_svc
        if cls is MembershipCacheProtocol:
            return cache
        return None

    with patch(_GET_PROTOCOL, side_effect=_get_proto):
        result = await _handler(req, limit=200, offset=0, lang="en", q=None)

    assert result == []
