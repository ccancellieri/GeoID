#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

"""Unit tests for the collection-scoped binding admin routes (#1342).

These routes are the write path that activates the resource-scope (#1341)
and per-binding quota (#1344) features: an operator binds a role or policy
to a principal, scoped to one collection, optionally with effect / validity
/ quota. Pinned here without an app, mirroring
``test_catalog_grant_privileged_guard.py``:

  * create a role binding → storage.grant called with the collection scope.
  * create a policy binding → validated via PermissionProtocol, then granted.
  * a role binding carries the catalog-scope privilege-escalation guard
    (catalog-tier admitted, platform-tier sysadmin blocked).
  * unregistered role / missing policy → 422.
  * list-by-principal returns collection-scoped + catalog-wide bindings.
  * list reverse view → storage.list_grants_for_resource.
  * revoke by match → storage.revoke_by_match with the collection scope.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import HTTPException

from dynastore.extensions.admin.admin_service import AdminService
from dynastore.models.protocols.policies import CreateBindingRequest


_GET_PROTOCOL = "dynastore.extensions.admin.admin_service.get_protocol"
_create = AdminService.create_collection_binding
_list = AdminService.list_collection_bindings
_revoke = AdminService.revoke_collection_binding

_CATALOG = "cat-a"
_COLL = "coll-x"
_SCHEMA = "cat_a_schema"


def _req(roles):
    return SimpleNamespace(state=SimpleNamespace(
        principal=SimpleNamespace(
            id=uuid4(), provider="local", subject_id="alice", roles=roles,
        ),
        principal_role=list(roles),
        policy_allowed=True,
    ))


def _mgr():
    mgr = MagicMock()
    mgr.list_roles = AsyncMock(return_value=[SimpleNamespace(name="admin"),
                                             SimpleNamespace(name="editor")])
    mgr.get_principal = AsyncMock(return_value=SimpleNamespace(subject_id="bob"))
    mgr.resolve_schema = AsyncMock(return_value=_SCHEMA)
    mgr.storage = MagicMock()
    mgr.storage.grant = AsyncMock(return_value=uuid4())
    mgr.storage.revoke_by_match = AsyncMock(return_value=1)
    mgr.storage.list_grants_for_subject = AsyncMock(return_value=[])
    mgr.storage.list_grants_for_resource = AsyncMock(return_value=[])
    return mgr


def _patch_protocols(mgr, *, policy_exists=True):
    from dynastore.modules.iam.iam_service import IamService
    from dynastore.models.protocols.catalogs import CatalogsProtocol
    from dynastore.models.protocols.policies import PermissionProtocol

    catalogs = MagicMock()
    catalogs.get_catalog_model = AsyncMock(return_value=SimpleNamespace(id=_CATALOG))
    catalogs.collections = MagicMock()
    catalogs.collections.get_collection = AsyncMock(
        return_value=SimpleNamespace(id=_COLL)
    )

    perm = MagicMock()
    perm.get_policy = AsyncMock(
        return_value=(SimpleNamespace(id="exp42") if policy_exists else None)
    )

    def _get_proto(cls):
        if cls is IamService:
            return mgr
        if cls is CatalogsProtocol:
            return catalogs
        if cls is PermissionProtocol:
            return perm
        return None

    return patch(_GET_PROTOCOL, side_effect=_get_proto)


@pytest.mark.asyncio
async def test_create_role_binding_scopes_to_collection_with_quota():
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    pid = uuid4()
    body = CreateBindingRequest(
        principal_id=pid, object_kind="role", object_ref="editor",
        quota={"rate_limit": {"limit": 100, "window_seconds": 60}},
    )
    with _patch_protocols(mgr):
        out = await _create(req, catalog_id=_CATALOG, collection_id=_COLL, body=body)
    mgr.storage.grant.assert_awaited_once()
    kwargs = mgr.storage.grant.await_args.kwargs
    assert kwargs["resource_kind"] == "collection"
    assert kwargs["resource_ref"] == _COLL
    assert kwargs["object_kind"] == "role" and kwargs["object_ref"] == "editor"
    assert kwargs["quota"] == {"rate_limit": {"limit": 100, "window_seconds": 60}}
    assert kwargs["subject_ref"] == str(pid)
    assert out["resource_ref"] == _COLL


@pytest.mark.asyncio
async def test_create_policy_binding_validates_and_grants():
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    body = CreateBindingRequest(
        principal_id=uuid4(), object_kind="policy", object_ref="exp42",
    )
    with _patch_protocols(mgr, policy_exists=True):
        await _create(req, catalog_id=_CATALOG, collection_id=_COLL, body=body)
    assert mgr.storage.grant.await_args.kwargs["object_kind"] == "policy"


@pytest.mark.asyncio
async def test_create_policy_binding_missing_policy_422():
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    body = CreateBindingRequest(
        principal_id=uuid4(), object_kind="policy", object_ref="ghost",
    )
    with _patch_protocols(mgr, policy_exists=False):
        with pytest.raises(HTTPException) as exc:
            await _create(req, catalog_id=_CATALOG, collection_id=_COLL, body=body)
    assert exc.value.status_code == 422
    mgr.storage.grant.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_role_binding_unregistered_role_422():
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    body = CreateBindingRequest(
        principal_id=uuid4(), object_kind="role", object_ref="nope",
    )
    with _patch_protocols(mgr):
        with pytest.raises(HTTPException) as exc:
            await _create(req, catalog_id=_CATALOG, collection_id=_COLL, body=body)
    assert exc.value.status_code == 422


@pytest.mark.asyncio
async def test_catalog_admin_can_bind_catalog_tier_role():
    """A sentinel-only catalog admin may bind catalog-tier 'admin' — the
    guard only blocks platform-tier role names at this scope."""
    req = _req(roles=["catalog_admin"])
    mgr = _mgr()
    body = CreateBindingRequest(principal_id=uuid4(), object_kind="role", object_ref="admin")
    with _patch_protocols(mgr):
        await _create(req, catalog_id=_CATALOG, collection_id=_COLL, body=body)
    mgr.storage.grant.assert_awaited_once()


@pytest.mark.asyncio
async def test_catalog_admin_cannot_bind_platform_tier_role():
    req = _req(roles=["catalog_admin"])
    mgr = _mgr()
    mgr.list_roles = AsyncMock(return_value=[SimpleNamespace(name="sysadmin")])
    body = CreateBindingRequest(principal_id=uuid4(), object_kind="role", object_ref="sysadmin")
    with _patch_protocols(mgr):
        with pytest.raises(HTTPException) as exc:
            await _create(req, catalog_id=_CATALOG, collection_id=_COLL, body=body)
    assert exc.value.status_code == 403
    mgr.storage.grant.assert_not_awaited()


@pytest.mark.asyncio
async def test_list_by_principal_includes_scoped_and_catalog_wide():
    mgr = _mgr()
    pid = uuid4()
    mgr.storage.list_grants_for_subject = AsyncMock(return_value=[
        {"object_ref": "editor", "resource_kind": "collection", "resource_ref": _COLL},
        {"object_ref": "viewer", "resource_kind": None, "resource_ref": None},
        {"object_ref": "other", "resource_kind": "collection", "resource_ref": "coll-other"},
    ])
    with _patch_protocols(mgr):
        rows = await _list(catalog_id=_CATALOG, collection_id=_COLL, principal_id=pid)
    refs = {r["object_ref"] for r in rows}
    assert refs == {"editor", "viewer"}  # scoped-here + catalog-wide, not coll-other


@pytest.mark.asyncio
async def test_list_reverse_uses_resource_query():
    mgr = _mgr()
    mgr.storage.list_grants_for_resource = AsyncMock(return_value=[{"subject_ref": "p1"}])
    with _patch_protocols(mgr):
        rows = await _list(catalog_id=_CATALOG, collection_id=_COLL, principal_id=None)
    mgr.storage.list_grants_for_resource.assert_awaited_once()
    kwargs = mgr.storage.list_grants_for_resource.await_args.kwargs
    assert kwargs["resource_kind"] == "collection" and kwargs["resource_ref"] == _COLL
    assert rows == [{"subject_ref": "p1"}]


@pytest.mark.asyncio
async def test_revoke_by_match_scopes_to_collection():
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    pid = uuid4()
    with _patch_protocols(mgr):
        await _revoke(
            req, catalog_id=_CATALOG, collection_id=_COLL,
            principal_id=pid, object_kind="role", object_ref="editor", effect="allow",
        )
    kwargs = mgr.storage.revoke_by_match.await_args.kwargs
    assert kwargs["resource_kind"] == "collection" and kwargs["resource_ref"] == _COLL
    assert kwargs["subject_ref"] == str(pid) and kwargs["object_ref"] == "editor"


@pytest.mark.asyncio
async def test_revoke_platform_tier_role_blocked():
    req = _req(roles=["catalog_admin"])
    mgr = _mgr()
    with _patch_protocols(mgr):
        with pytest.raises(HTTPException) as exc:
            await _revoke(
                req, catalog_id=_CATALOG, collection_id=_COLL,
                principal_id=uuid4(), object_kind="role", object_ref="sysadmin",
                effect="allow",
            )
    assert exc.value.status_code == 403
    mgr.storage.revoke_by_match.assert_not_awaited()
