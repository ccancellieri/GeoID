"""Regression: ``get_effective_permissions`` must return the persisted principal_id.

Bug discovered live in review env on 2026-05-09 (geoid `dea5a51`):

``Principal.id`` carries a ``model_validator`` that fills in ``uuid4()`` when
``id`` is unset. ``IamService.get_effective_permissions`` previously built
``Principal(...)`` without passing ``id=``, so every call produced a fresh
random UUID. Any caller that read back ``principal.id`` to write to the DB
(notably the OIDC role-sync reconciler calling
``grant_platform_role(principal_id=principal.id, ...)``) wrote against orphan
UUIDs that had no matching row in ``iam.principals`` or ``iam.identity_links``.
The grants then could not be reached by ``LIST_ROLE_NAMES_FOR_IDENTITY``'s
join, so the user appeared to have no roles even after the reconciler
"succeeded". Live evidence: 5 consecutive requests, 5 unique throwaway
principal_ids, 5 orphan ``granted=['sysadmin']`` audit rows, 0 effective
grants.

Fix: ``get_effective_permissions`` now calls ``storage.get_principal_by_identity``
and re-uses the persisted ``id`` when constructing the runtime ``Principal``.
If no platform principal row exists for this identity, returns ``None`` so
the caller can auto-register a fresh one with a properly persisted id.

These tests pin that behaviour. Two assertions guard the regression:

  1. The returned ``Principal.id`` equals the persisted id from storage
     (NOT a freshly generated uuid4).
  2. Calling twice for the same identity returns the same id (idempotence —
     no caller should observe a moving target).

A negative case pins the "no principal row → return None" branch so a
future refactor can't quietly re-introduce the orphan path by skipping
the lookup.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

import pytest


class _FakeStorage:
    """Minimal IamStorage stub — only the calls hit by
    ``get_effective_permissions`` are implemented. Each call records its
    invocation count so tests can assert idempotence."""

    def __init__(
        self,
        *,
        principal_id: Optional[UUID],
        roles: Optional[List[str]] = None,
        auth_metadata: Optional[Dict[str, Any]] = None,
        custom_policies: Optional[List[Any]] = None,
    ) -> None:
        self._principal_id = principal_id
        self._roles = list(roles or [])
        self._auth_metadata = auth_metadata
        self._custom_policies = list(custom_policies or [])
        self.get_principal_by_identity_calls = 0

    async def get_identity_roles(
        self,
        provider: str,
        subject_id: str,
        catalog_schema: Optional[str] = None,
        conn: Optional[Any] = None,
    ) -> List[str]:
        return list(self._roles)

    async def get_identity_authorization(
        self, provider: str, subject_id: str, conn: Optional[Any] = None
    ) -> Optional[Dict[str, Any]]:
        return self._auth_metadata

    async def get_identity_policies(
        self, provider: str, subject_id: str, conn: Optional[Any] = None
    ) -> List[Any]:
        return list(self._custom_policies)

    async def get_principal_by_identity(
        self,
        provider: str,
        subject_id: str,
        conn: Optional[Any] = None,
    ) -> Optional[Any]:
        self.get_principal_by_identity_calls += 1
        if self._principal_id is None:
            return None

        # Mimic the persisted-Principal shape — only `id` is read by the
        # service, but provide enough surface to resemble the real row.
        class _PersistedPrincipal:
            def __init__(self, pid: UUID) -> None:
                self.id = pid

        return _PersistedPrincipal(self._principal_id)


def _build_iam_service(storage: _FakeStorage) -> Any:
    """Build a minimal IamService instance bypassing __init__."""
    from dynastore.modules.iam.iam_service import IamService

    svc = object.__new__(IamService)
    svc.storage = storage

    async def _resolve_schema(_catalog_id: Any, _conn: Any = None) -> str:
        return "iam"

    svc._resolve_schema = _resolve_schema
    return svc


@pytest.mark.asyncio
async def test_returns_persisted_principal_id_not_random() -> None:
    """The returned Principal.id MUST be the persisted id from storage,
    not a fresh uuid4 from Principal's default_factory."""
    persisted_id = UUID("11111111-1111-4111-8111-111111111111")
    storage = _FakeStorage(
        principal_id=persisted_id,
        roles=["sysadmin"],
        auth_metadata={"display_name": "alice", "is_active": True},
    )
    svc = _build_iam_service(storage)

    principal = await svc.get_effective_permissions(
        identity={"provider": "oidc", "sub": "alice@example.com"},
        catalog_id=None,
    )

    assert principal is not None
    assert principal.id == persisted_id, (
        f"Expected persisted id {persisted_id}, got {principal.id} "
        f"(regression: Principal.default_factory=uuid4 produced a fresh UUID)"
    )
    assert principal.roles == ["sysadmin"]


@pytest.mark.asyncio
async def test_idempotent_calls_return_same_id() -> None:
    """Two calls for the same identity must return the same Principal.id.
    The original bug surfaced as a *moving* id across consecutive requests
    — pinning idempotence catches re-introduction even if the persisted-id
    plumbing is partially correct."""
    persisted_id = UUID("22222222-2222-4222-8222-222222222222")
    storage = _FakeStorage(principal_id=persisted_id, roles=["user"])
    svc = _build_iam_service(storage)

    p1 = await svc.get_effective_permissions(
        identity={"provider": "oidc", "sub": "bob@example.com"}, catalog_id=None
    )
    p2 = await svc.get_effective_permissions(
        identity={"provider": "oidc", "sub": "bob@example.com"}, catalog_id=None
    )

    assert p1 is not None and p2 is not None
    assert p1.id == p2.id == persisted_id


@pytest.mark.asyncio
async def test_returns_none_when_no_principal_row() -> None:
    """When ``get_principal_by_identity`` returns None, the service must
    return None — never fall back to constructing a Principal with a fresh
    uuid4. The caller (``authenticate_and_get_principal`` with
    ``auto_register=True``) is responsible for creating a properly
    persisted row."""
    storage = _FakeStorage(
        principal_id=None,
        roles=["user"],
        auth_metadata={"display_name": "carol", "is_active": True},
    )
    svc = _build_iam_service(storage)

    principal = await svc.get_effective_permissions(
        identity={"provider": "oidc", "sub": "carol@example.com"},
        catalog_id=None,
    )

    assert principal is None, (
        "When no platform principal row exists, get_effective_permissions "
        "MUST return None so the caller can auto-register — never construct "
        "a Principal with a random id."
    )
    assert storage.get_principal_by_identity_calls == 1


@pytest.mark.asyncio
async def test_returns_none_for_missing_provider_or_sub() -> None:
    """Defensive: invalid identity dict short-circuits before any storage
    lookup. Pins the existing guard so it can't drift."""
    storage = _FakeStorage(principal_id=uuid4(), roles=["user"])
    svc = _build_iam_service(storage)

    assert await svc.get_effective_permissions({"sub": "x"}, catalog_id=None) is None
    assert await svc.get_effective_permissions({"provider": "oidc"}, catalog_id=None) is None
    assert await svc.get_effective_permissions({}, catalog_id=None) is None
    assert storage.get_principal_by_identity_calls == 0


@pytest.mark.asyncio
async def test_returns_none_when_no_roles_and_no_metadata() -> None:
    """Existing behaviour: an identity with neither roles nor auth_metadata
    has no access. Pinned here so the persisted-id lookup doesn't accidentally
    promote a no-access identity to a returned Principal."""
    storage = _FakeStorage(
        principal_id=UUID("33333333-3333-4333-8333-333333333333"),
        roles=[],
        auth_metadata=None,
    )
    svc = _build_iam_service(storage)

    principal = await svc.get_effective_permissions(
        identity={"provider": "oidc", "sub": "dave@example.com"},
        catalog_id=None,
    )

    assert principal is None
    # The early-return must fire BEFORE the get_principal_by_identity lookup,
    # otherwise we waste a query on no-access identities.
    assert storage.get_principal_by_identity_calls == 0
