"""Regression test for #631.

`POST /admin/roles` previously routed straight to
``IamStorage.create_role`` whose SQL is ``INSERT ... ON CONFLICT (id) DO
UPDATE SET ...``. That meant a second POST with the same role name
silently overwrote ``policies`` + ``parent_roles`` of the existing role
— easy to mistake for "create" and lose grant data.

After #631, ``IamService.create_role`` checks for an existing role
first and raises ``ValueError`` if one is found, mirroring the
``PolicyService.create_policy`` pattern. The admin POST handler maps
that to HTTP 409 so the FE can show an explicit "already exists" error
and route the user to PUT instead.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.models.auth_models import Role
from dynastore.modules.iam.iam_service import IamService


def _make_service(existing_role: Role | None) -> IamService:
    storage = MagicMock()
    storage.get_role = AsyncMock(return_value=existing_role)
    storage.create_role = AsyncMock(side_effect=lambda role, schema="iam": role)

    svc = IamService.__new__(IamService)
    svc.storage = storage  # type: ignore[attr-defined]
    svc.policy_service = None  # type: ignore[attr-defined]
    svc._role_config = None  # type: ignore[attr-defined]

    async def _resolve_schema(_catalog_id: Any = None) -> str:
        return "iam"

    svc._resolve_schema = _resolve_schema  # type: ignore[attr-defined]
    return svc


@pytest.mark.asyncio
async def test_create_role_rejects_duplicate_name():
    existing = Role(name="editor", description="seed", policies=[], parent_roles=[])
    svc = _make_service(existing_role=existing)

    new = Role(name="editor", description="caller-supplied", policies=["p1"], parent_roles=["admin"])
    with pytest.raises(ValueError, match=r"Role 'editor' already exists"):
        await svc.create_role(new)

    svc.storage.create_role.assert_not_called()


@pytest.mark.asyncio
async def test_create_role_inserts_when_name_is_free():
    svc = _make_service(existing_role=None)

    new = Role(name="reviewer", description="d", policies=[], parent_roles=[])
    out = await svc.create_role(new)

    assert out.name == "reviewer"
    svc.storage.create_role.assert_awaited_once()
