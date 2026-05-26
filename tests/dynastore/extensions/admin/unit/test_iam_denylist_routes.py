#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

"""Unit tests for the phantom-token denylist admin routes (#1343).

These mirror :mod:`test_scope_binding_routes` but cover the
operator-facing immediate-revocation surface added on top of the phantom
-token hot path:

  * POST writes through ``IamService.deny_subject`` and reports the
    effective (clamped) TTL.
  * LIST reads through ``IamService.list_denylist`` and translates the
    storage-form ids (raw jti / ``sub:<id>``) back to the wire prefix
    form (``jti:<id>`` / ``principal:<id>``).
  * DELETE writes through ``IamService.undeny_subject`` and is
    idempotent (204 whether or not an entry existed).
  * All three are gated by ``_ensure_sysadmin``; the broader
    ``admin_access`` policy (admin + sysadmin) is NOT enough — a token
    kill is a security-sensitive action and the catalog-admin tier must
    not reach it.
  * All three return 503 on a Valkey-down backend
    (:class:`DenylistBackendUnavailable`): an operator must know whether
    the kill / un-deny landed, so the admin path deliberately fails
    closed on a backend outage (unlike the hot path, which fails open).
  * TTL clamp: a request of e.g. 1 hour against a 5-minute ceiling
    stores 5 minutes — the operator cannot keep a kill alive longer
    than the configured maximum.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import HTTPException

from dynastore.extensions.admin.admin_service import AdminService
from dynastore.models.protocols.policies import DenylistEntryRequest
from dynastore.modules.iam.phantom_token import DenylistBackendUnavailable


_GET_PROTOCOL = "dynastore.extensions.admin.admin_service.get_protocol"

_add = AdminService.add_denylist_entry
_list = AdminService.list_denylist_entries
_remove = AdminService.remove_denylist_entry


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
    # deny_subject returns the EFFECTIVE clamped TTL; default to the route's
    # request-equals-ceiling case so tests opting in to a small TTL override.
    mgr.deny_subject = AsyncMock(return_value=300)
    mgr.undeny_subject = AsyncMock(return_value=True)
    mgr.list_denylist = AsyncMock(return_value=[])
    return mgr


def _patch_iam(mgr):
    from dynastore.modules.iam.iam_service import IamService

    def _get_proto(cls):
        if cls is IamService:
            return mgr
        return None

    return patch(_GET_PROTOCOL, side_effect=_get_proto)


# ---- Happy paths --------------------------------------------------------


@pytest.mark.asyncio
async def test_post_jti_writes_through_deny_subject_and_returns_effective_ttl():
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    mgr.deny_subject = AsyncMock(return_value=300)
    body = DenylistEntryRequest(
        subject="jti:abc123", ttl_seconds=600, reason="leaked",
    )
    with _patch_iam(mgr):
        out = await _add(req, body=body)
    mgr.deny_subject.assert_awaited_once()
    kwargs = mgr.deny_subject.await_args.kwargs
    args = mgr.deny_subject.await_args.args
    # subject is passed positional; routed form ("jti:" stripped to raw jti).
    assert args == ("abc123",) or kwargs.get("subject") == "abc123"
    assert kwargs["ttl_seconds"] == 600
    assert kwargs["reason"] == "leaked"
    # The wire-form ``subject`` round-trips on the response.
    assert out.subject == "jti:abc123"
    assert out.reason == "leaked"
    assert out.expires_at is not None and out.expires_at > 0


@pytest.mark.asyncio
async def test_post_principal_kind_translates_to_sub_prefix():
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    body = DenylistEntryRequest(subject="principal:user-42")
    with _patch_iam(mgr):
        out = await _add(req, body=body)
    args = mgr.deny_subject.await_args.args
    kwargs = mgr.deny_subject.await_args.kwargs
    assert args == ("sub:user-42",) or kwargs.get("subject") == "sub:user-42"
    # Round-trips back to the wire-form ``principal:`` prefix on the response.
    assert out.subject == "principal:user-42"


@pytest.mark.asyncio
async def test_post_invalid_subject_prefix_422():
    """No ``jti:`` / ``principal:`` prefix → 422 with no service call."""
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    body = DenylistEntryRequest(subject="bare-id-no-prefix-12345")
    with _patch_iam(mgr):
        with pytest.raises(HTTPException) as exc:
            await _add(req, body=body)
    assert exc.value.status_code == 422
    mgr.deny_subject.assert_not_awaited()


@pytest.mark.asyncio
async def test_post_ttl_clamp_request_above_ceiling_uses_ceiling():
    """Request 1h, IamScaleConfig ceiling 5min → service returns 300, response
    expires_at reflects the clamped TTL (not the requested 3600)."""
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    # Simulate the service-layer clamp landing at 300s.
    mgr.deny_subject = AsyncMock(return_value=300)
    body = DenylistEntryRequest(subject="jti:t1", ttl_seconds=3600)
    from time import time as _t
    before = _t()
    with _patch_iam(mgr):
        out = await _add(req, body=body)
    after = _t()
    # Response expires_at = now + clamped(300) within a small wall-clock window.
    assert out.expires_at is not None
    assert before + 300 - 2 <= out.expires_at <= after + 300 + 2


@pytest.mark.asyncio
async def test_list_translates_storage_keys_to_wire_form():
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    mgr.list_denylist = AsyncMock(return_value=[
        {"token_id": "abc123", "reason": "leaked", "expires_at": 1_000_000.0},
        {"token_id": "sub:user-42", "reason": None, "expires_at": None},
    ])
    with _patch_iam(mgr):
        out = await _list(req, subject=None, limit=100)
    assert [e.subject for e in out] == ["jti:abc123", "principal:user-42"]
    assert out[0].reason == "leaked"
    assert out[0].expires_at == 1_000_000.0
    # The storage prefix passed through is None (no filter).
    kwargs = mgr.list_denylist.await_args.kwargs
    assert kwargs.get("prefix") is None
    assert kwargs.get("limit") == 100


@pytest.mark.asyncio
async def test_list_subject_filter_translates_wire_prefix_to_storage():
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    with _patch_iam(mgr):
        await _list(req, subject="principal:user-", limit=50)
    kwargs = mgr.list_denylist.await_args.kwargs
    # ``principal:`` → ``sub:`` on the storage side.
    assert kwargs.get("prefix") == "sub:user-"
    assert kwargs.get("limit") == 50


@pytest.mark.asyncio
async def test_delete_writes_through_undeny_subject_and_translates_kind():
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    with _patch_iam(mgr):
        await _remove(req, subject="principal:user-7")
    args = mgr.undeny_subject.await_args.args
    kwargs = mgr.undeny_subject.await_args.kwargs
    assert args == ("sub:user-7",) or kwargs.get("subject") == "sub:user-7"


@pytest.mark.asyncio
async def test_delete_idempotent_on_missing_entry_returns_none_204():
    """undeny_subject returns False (no entry existed) → handler still 204
    (the framework default status_code on a None return); idempotent contract
    locked in here."""
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    mgr.undeny_subject = AsyncMock(return_value=False)
    with _patch_iam(mgr):
        result = await _remove(req, subject="jti:never-existed")
    # Handler returns None — FastAPI emits 204 by route decorator.
    assert result is None
    mgr.undeny_subject.assert_awaited_once()


# ---- Sysadmin guard -----------------------------------------------------


@pytest.mark.asyncio
async def test_post_non_sysadmin_rejected_403():
    """The ``admin_access`` policy lets ``admin`` reach /admin/ paths, but
    denylist mutations require the strict sysadmin role."""
    req = _req(roles=["admin"])  # platform admin, NOT sysadmin
    mgr = _mgr()
    body = DenylistEntryRequest(subject="jti:t1")
    with _patch_iam(mgr):
        with pytest.raises(HTTPException) as exc:
            await _add(req, body=body)
    assert exc.value.status_code == 403
    mgr.deny_subject.assert_not_awaited()


@pytest.mark.asyncio
async def test_list_non_sysadmin_rejected_403():
    req = _req(roles=["admin"])
    mgr = _mgr()
    with _patch_iam(mgr):
        with pytest.raises(HTTPException) as exc:
            await _list(req, subject=None, limit=10)
    assert exc.value.status_code == 403
    mgr.list_denylist.assert_not_awaited()


@pytest.mark.asyncio
async def test_delete_non_sysadmin_rejected_403():
    req = _req(roles=["admin"])
    mgr = _mgr()
    with _patch_iam(mgr):
        with pytest.raises(HTTPException) as exc:
            await _remove(req, subject="jti:t1")
    assert exc.value.status_code == 403
    mgr.undeny_subject.assert_not_awaited()


# ---- Fail-closed on Valkey down ----------------------------------------


@pytest.mark.asyncio
async def test_post_returns_503_when_backend_unavailable():
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    mgr.deny_subject = AsyncMock(side_effect=DenylistBackendUnavailable("down"))
    body = DenylistEntryRequest(subject="jti:t1")
    with _patch_iam(mgr):
        with pytest.raises(HTTPException) as exc:
            await _add(req, body=body)
    assert exc.value.status_code == 503


@pytest.mark.asyncio
async def test_list_returns_503_when_backend_unavailable():
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    mgr.list_denylist = AsyncMock(side_effect=DenylistBackendUnavailable("down"))
    with _patch_iam(mgr):
        with pytest.raises(HTTPException) as exc:
            await _list(req, subject=None, limit=10)
    assert exc.value.status_code == 503


@pytest.mark.asyncio
async def test_delete_returns_503_when_backend_unavailable():
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    mgr.undeny_subject = AsyncMock(side_effect=DenylistBackendUnavailable("down"))
    with _patch_iam(mgr):
        with pytest.raises(HTTPException) as exc:
            await _remove(req, subject="jti:t1")
    assert exc.value.status_code == 503
