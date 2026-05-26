#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

"""Unit tests for the platform / catalog scope binding admin routes (#1346).

These mirror :mod:`test_collection_binding_routes` but at the two outer
scopes — the routes here all write whole-scope grants
(``resource_kind=NULL``) into the appropriate schema (``iam`` for platform,
``{catalog_schema}`` for catalog). Their reason for existing is the same
gap that motivated the collection-scope endpoint: the legacy
``/admin/.../roles`` endpoints can only express the allow-only role
binding, so `effect=deny`, `valid_from`/`valid_until`, direct policy
bindings and per-binding `quota` had no UI write path at platform or
catalog scope.

  * platform: create role binding with deny/validity/quota → storage.grant
    on the ``iam`` schema with resource_* both NULL.
  * platform: create policy binding → validated via PermissionProtocol
    against the platform scope, then granted.
  * platform: role binding carries the unrestricted privilege-escalation
    guard (platform-tier 'sysadmin' may only be granted by an existing
    sysadmin) — same gate the legacy /platform/.../roles endpoint applies.
  * platform: unregistered role / missing policy → 422.
  * platform: list-by-principal returns the storage rows for that subject.
  * platform: revoke by match → storage.revoke_by_match on ``iam``.

  * catalog: create role binding → storage.grant on the resolved catalog
    schema with resource_* both NULL (i.e. catalog-wide).
  * catalog: privilege-escalation guard narrowed to platform-tier names
    (catalog-tier admin remains grantable by a catalog admin — #723).
  * catalog: create policy binding for the catalog → validated.
  * catalog: revoke by match → storage.revoke_by_match with the catalog
    schema and resource_* both NULL.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import HTTPException

from dynastore.extensions.admin.admin_service import AdminService
from dynastore.models.protocols.policies import CreateBindingRequest


_GET_PROTOCOL = "dynastore.extensions.admin.admin_service.get_protocol"

_create_pf = AdminService.create_platform_binding
_list_pf = AdminService.list_platform_grants
_revoke_pf = AdminService.revoke_platform_binding

_create_cat = AdminService.create_catalog_binding
_list_cat = AdminService.list_catalog_grants
_revoke_cat = AdminService.revoke_catalog_binding

_CATALOG = "cat-a"
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
    return mgr


def _patch_protocols(mgr, *, policy_exists=True):
    from dynastore.modules.iam.iam_service import IamService
    from dynastore.models.protocols.catalogs import CatalogsProtocol
    from dynastore.models.protocols.policies import PermissionProtocol

    catalogs = MagicMock()
    catalogs.get_catalog_model = AsyncMock(return_value=SimpleNamespace(id=_CATALOG))

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


# ---- Platform scope -----------------------------------------------------


@pytest.mark.asyncio
async def test_platform_create_role_binding_with_deny_validity_quota():
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    pid = uuid4()
    valid_until = datetime.now(timezone.utc) + timedelta(days=7)
    body = CreateBindingRequest(
        principal_id=pid, object_kind="role", object_ref="editor",
        effect="deny",
        valid_until=valid_until,
        quota={"rate_limit": {"limit": 50, "window_seconds": 60}},
    )
    with _patch_protocols(mgr):
        out = await _create_pf(req, body=body)
    mgr.storage.grant.assert_awaited_once()
    kwargs = mgr.storage.grant.await_args.kwargs
    assert kwargs["scope_schema"] == "iam"
    assert "resource_kind" not in kwargs or kwargs["resource_kind"] is None
    assert "resource_ref" not in kwargs or kwargs["resource_ref"] is None
    assert kwargs["effect"] == "deny"
    assert kwargs["valid_until"] == valid_until
    assert kwargs["quota"] == {"rate_limit": {"limit": 50, "window_seconds": 60}}
    assert kwargs["subject_ref"] == str(pid)
    assert out["effect"] == "deny"
    assert out["resource_kind"] is None and out["resource_ref"] is None


@pytest.mark.asyncio
async def test_platform_create_policy_binding_validates_against_platform_scope():
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    body = CreateBindingRequest(
        principal_id=uuid4(), object_kind="policy", object_ref="exp42",
    )
    with _patch_protocols(mgr, policy_exists=True) as _:
        await _create_pf(req, body=body)
    assert mgr.storage.grant.await_args.kwargs["object_kind"] == "policy"
    assert mgr.storage.grant.await_args.kwargs["scope_schema"] == "iam"


@pytest.mark.asyncio
async def test_platform_create_policy_binding_missing_policy_422():
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    body = CreateBindingRequest(
        principal_id=uuid4(), object_kind="policy", object_ref="ghost",
    )
    with _patch_protocols(mgr, policy_exists=False):
        with pytest.raises(HTTPException) as exc:
            await _create_pf(req, body=body)
    assert exc.value.status_code == 422
    mgr.storage.grant.assert_not_awaited()


@pytest.mark.asyncio
async def test_platform_create_role_binding_unregistered_role_422():
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    body = CreateBindingRequest(
        principal_id=uuid4(), object_kind="role", object_ref="nope",
    )
    with _patch_protocols(mgr):
        with pytest.raises(HTTPException) as exc:
            await _create_pf(req, body=body)
    assert exc.value.status_code == 422


@pytest.mark.asyncio
async def test_platform_create_role_binding_sysadmin_target_blocked_for_non_sysadmin():
    """At platform scope, granting a privileged role (default protected set =
    sysadmin tier) requires sysadmin — same gate as the legacy
    `/platform/.../roles` endpoint applies via `ensure_privileged_role_assignment`."""
    req = _req(roles=["catalog_admin"])
    mgr = _mgr()
    mgr.list_roles = AsyncMock(return_value=[SimpleNamespace(name="sysadmin")])
    body = CreateBindingRequest(
        principal_id=uuid4(), object_kind="role", object_ref="sysadmin",
    )
    with _patch_protocols(mgr):
        with pytest.raises(HTTPException) as exc:
            await _create_pf(req, body=body)
    assert exc.value.status_code == 403
    mgr.storage.grant.assert_not_awaited()


@pytest.mark.asyncio
async def test_platform_list_by_principal_returns_storage_rows():
    mgr = _mgr()
    pid = uuid4()
    rows = [{"object_ref": "sysadmin", "effect": "allow",
             "resource_kind": None, "resource_ref": None}]
    mgr.storage.list_grants_for_subject = AsyncMock(return_value=rows)
    with _patch_protocols(mgr):
        out = await _list_pf(principal_id=pid)
    mgr.storage.list_grants_for_subject.assert_awaited_once()
    kwargs = mgr.storage.list_grants_for_subject.await_args.kwargs
    assert kwargs["scope_schema"] == "iam"
    assert kwargs["subject_ref"] == str(pid)
    assert out == rows


@pytest.mark.asyncio
async def test_platform_revoke_by_match_scopes_to_iam():
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    pid = uuid4()
    with _patch_protocols(mgr):
        await _revoke_pf(
            req,
            principal_id=pid, object_kind="role", object_ref="editor", effect="allow",
        )
    kwargs = mgr.storage.revoke_by_match.await_args.kwargs
    assert kwargs["scope_schema"] == "iam"
    assert kwargs["subject_ref"] == str(pid) and kwargs["object_ref"] == "editor"


@pytest.mark.asyncio
async def test_platform_revoke_privileged_role_blocked_for_non_sysadmin():
    """Same gate as the legacy `/platform/.../roles/{name}` DELETE: only a
    sysadmin may revoke a privileged-tier role binding at platform scope."""
    req = _req(roles=["catalog_admin"])
    mgr = _mgr()
    with _patch_protocols(mgr):
        with pytest.raises(HTTPException) as exc:
            await _revoke_pf(
                req,
                principal_id=uuid4(), object_kind="role", object_ref="sysadmin",
                effect="allow",
            )
    assert exc.value.status_code == 403
    mgr.storage.revoke_by_match.assert_not_awaited()


# ---- Catalog scope ------------------------------------------------------


@pytest.mark.asyncio
async def test_catalog_create_role_binding_writes_catalog_wide_grant():
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    pid = uuid4()
    body = CreateBindingRequest(
        principal_id=pid, object_kind="role", object_ref="editor",
        effect="deny",
    )
    with _patch_protocols(mgr):
        out = await _create_cat(req, catalog_id=_CATALOG, body=body)
    mgr.storage.grant.assert_awaited_once()
    kwargs = mgr.storage.grant.await_args.kwargs
    assert kwargs["scope_schema"] == _SCHEMA
    assert "resource_kind" not in kwargs or kwargs["resource_kind"] is None
    assert "resource_ref" not in kwargs or kwargs["resource_ref"] is None
    assert kwargs["effect"] == "deny"
    assert out["resource_kind"] is None and out["resource_ref"] is None


@pytest.mark.asyncio
async def test_catalog_create_policy_binding_validates_catalog_scoped():
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    body = CreateBindingRequest(
        principal_id=uuid4(), object_kind="policy", object_ref="exp42",
    )
    with _patch_protocols(mgr, policy_exists=True):
        await _create_cat(req, catalog_id=_CATALOG, body=body)
    assert mgr.storage.grant.await_args.kwargs["object_kind"] == "policy"
    assert mgr.storage.grant.await_args.kwargs["scope_schema"] == _SCHEMA


@pytest.mark.asyncio
async def test_catalog_admin_can_bind_catalog_tier_role():
    """Catalog admins can grant catalog-tier roles at catalog scope —
    the guard only narrows on platform-tier role names (the #723 case)."""
    req = _req(roles=["catalog_admin"])
    mgr = _mgr()
    body = CreateBindingRequest(principal_id=uuid4(), object_kind="role", object_ref="admin")
    with _patch_protocols(mgr):
        await _create_cat(req, catalog_id=_CATALOG, body=body)
    mgr.storage.grant.assert_awaited_once()


@pytest.mark.asyncio
async def test_catalog_admin_cannot_bind_platform_tier_role():
    req = _req(roles=["catalog_admin"])
    mgr = _mgr()
    mgr.list_roles = AsyncMock(return_value=[SimpleNamespace(name="sysadmin")])
    body = CreateBindingRequest(principal_id=uuid4(), object_kind="role", object_ref="sysadmin")
    with _patch_protocols(mgr):
        with pytest.raises(HTTPException) as exc:
            await _create_cat(req, catalog_id=_CATALOG, body=body)
    assert exc.value.status_code == 403
    mgr.storage.grant.assert_not_awaited()


@pytest.mark.asyncio
async def test_catalog_list_by_principal_returns_storage_rows():
    mgr = _mgr()
    pid = uuid4()
    rows = [{"object_ref": "editor", "effect": "allow",
             "resource_kind": None, "resource_ref": None}]
    mgr.storage.list_grants_for_subject = AsyncMock(return_value=rows)
    with _patch_protocols(mgr):
        out = await _list_cat(catalog_id=_CATALOG, principal_id=pid)
    kwargs = mgr.storage.list_grants_for_subject.await_args.kwargs
    assert kwargs["scope_schema"] == _SCHEMA
    assert kwargs["subject_ref"] == str(pid)
    assert out == rows


@pytest.mark.asyncio
async def test_catalog_revoke_by_match_scopes_to_catalog_schema():
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    pid = uuid4()
    with _patch_protocols(mgr):
        await _revoke_cat(
            req, catalog_id=_CATALOG,
            principal_id=pid, object_kind="role", object_ref="editor", effect="allow",
        )
    kwargs = mgr.storage.revoke_by_match.await_args.kwargs
    assert kwargs["scope_schema"] == _SCHEMA
    assert kwargs["subject_ref"] == str(pid) and kwargs["object_ref"] == "editor"


@pytest.mark.asyncio
async def test_catalog_revoke_platform_tier_role_blocked():
    req = _req(roles=["catalog_admin"])
    mgr = _mgr()
    with _patch_protocols(mgr):
        with pytest.raises(HTTPException) as exc:
            await _revoke_cat(
                req, catalog_id=_CATALOG,
                principal_id=uuid4(), object_kind="role", object_ref="sysadmin",
                effect="allow",
            )
    assert exc.value.status_code == 403
    mgr.storage.revoke_by_match.assert_not_awaited()
