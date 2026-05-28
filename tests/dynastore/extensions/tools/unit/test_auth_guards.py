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

"""Unit tests for generic auth guards in extensions/tools/auth_guards.py."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi import HTTPException

from dynastore.extensions.tools.auth_guards import (
    ensure_privileged_role_assignment,
    security_context_from_request,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _req(roles=None, policy_allowed=True, principal_role=None):
    """Build a minimal Request-like object with middleware state."""
    principal = None
    if roles is not None:
        principal = SimpleNamespace(
            subject_id="test-user",
            display_name="Test User",
            roles=roles,
        )
    return SimpleNamespace(
        state=SimpleNamespace(
            principal=principal,
            principal_role=principal_role,
            policy_allowed=policy_allowed,
        )
    )


# ---------------------------------------------------------------------------
# security_context_from_request
# ---------------------------------------------------------------------------

def test_security_context_from_request_extracts_principal():
    """Principal subject_id and roles from middleware state reach SecurityContext."""
    req = _req(roles=["sysadmin", "user"])
    ctx = security_context_from_request(req)
    assert ctx.principal_id == "test-user"
    assert "sysadmin" in ctx.roles
    assert "user" in ctx.roles
    assert ctx.policy_allowed is True


def test_security_context_from_request_anonymous_request():
    """An anonymous request (no principal) yields an empty SecurityContext."""
    req = SimpleNamespace(
        state=SimpleNamespace(principal=None, principal_role=None, policy_allowed=False)
    )
    ctx = security_context_from_request(req)
    assert ctx.principal_id is None
    assert ctx.roles == frozenset()
    assert ctx.policy_allowed is False


def test_security_context_from_request_merges_principal_role():
    """State principal_role values are merged into the role set."""
    req = _req(roles=["user"], principal_role="catalog_admin")
    ctx = security_context_from_request(req)
    assert "user" in ctx.roles
    assert "catalog_admin" in ctx.roles


# ---------------------------------------------------------------------------
# ensure_privileged_role_assignment
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ensure_privileged_role_assignment_allows_matching_role():
    """A sysadmin assigning another sysadmin role must succeed (no 403)."""
    req = _req(roles=["sysadmin"])

    async def _allow(*_a, **_kw):
        return None

    with patch("dynastore.extensions.tools.auth_guards.require_permission", _allow):
        # Should not raise
        await ensure_privileged_role_assignment(req, "sysadmin")


@pytest.mark.asyncio
async def test_ensure_privileged_role_assignment_blocks_non_sysadmin():
    """A non-sysadmin attempting to assign a privileged role must receive 403."""
    req = _req(roles=["admin"])

    async def _deny(*_a, **_kw):
        raise PermissionError("insufficient")

    with patch("dynastore.extensions.tools.auth_guards.require_permission", _deny):
        with pytest.raises(HTTPException) as exc_info:
            await ensure_privileged_role_assignment(req, "sysadmin")
    assert exc_info.value.status_code == 403


@pytest.mark.asyncio
async def test_ensure_privileged_role_assignment_skips_non_privileged_role():
    """Assigning a non-privileged role never invokes the permission check."""
    req = _req(roles=["user"])
    called = []

    async def _should_not_be_called(*_a, **_kw):
        called.append(True)

    with patch("dynastore.extensions.tools.auth_guards.require_permission", _should_not_be_called):
        await ensure_privileged_role_assignment(req, "viewer")

    assert not called, "require_permission must not be called for non-privileged roles"


@pytest.mark.asyncio
async def test_ensure_privileged_role_assignment_custom_protected_roles():
    """Custom protected_roles overrides the default admin_role_set."""
    req = _req(roles=["user"])
    called = []

    async def _check(*_a, **_kw):
        called.append(True)
        raise PermissionError("denied")

    with patch("dynastore.extensions.tools.auth_guards.require_permission", _check):
        with pytest.raises(HTTPException) as exc_info:
            await ensure_privileged_role_assignment(
                req, "super_editor", protected_roles=frozenset({"super_editor"})
            )
    assert exc_info.value.status_code == 403
    assert called
