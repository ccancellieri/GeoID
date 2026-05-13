"""Regression: HS256 fallback path defaults empty roles to ``["user"]``.

PR #198 fixed an asymmetry between the OIDC and HS256 token verification
branches in ``IamService.authenticate_and_get_role``. The OIDC branch
already defaulted to ``DefaultRole.USER`` when the principal carried no
realm roles (line 716), but the HS256 fallback branch — used by test
fixtures and internal services — preserved an empty role list verbatim
when the JWT payload's ``roles`` field was either missing OR explicitly
set to ``[]``. That left an authenticated principal with zero effective
roles, surfacing as ANONYMOUS-equivalent at the policy layer and
returning 403 from every ``/iam/me/*`` self-service endpoint.

These tests pin the default-to-user behaviour so a future refactor of
the role-resolution path can't silently regress SelfServiceAPI access.
"""

from typing import Any, List, Tuple

import pytest

from dynastore.models.protocols.authorization import IamRolesConfig

_DEFAULTS = IamRolesConfig()


class _FakeStorage:
    """Minimal IamStorage stub — get_role_hierarchy returns the input set."""

    async def get_role_hierarchy(self, roles: List[str], schema: str) -> List[str]:
        return list(roles)


class _FakeIdentityProvider:
    """Identity provider stub that always rejects (no OIDC validation)
    so the authenticate_and_get_role path falls through to HS256."""

    def get_provider_id(self) -> str:
        return "noop"

    async def validate_token(self, token: str) -> None:
        return None


def _build_iam_service_for_hs256(secret: str) -> Any:
    """Construct a minimal IamService instance wired just enough to exercise
    ``authenticate_and_get_role`` along the HS256 fallback path."""
    from dynastore.modules.iam.iam_service import IamService
    from dynastore.models.protocols.authorization import IamRolesConfig

    svc = object.__new__(IamService)
    svc.storage = _FakeStorage()
    svc._identity_providers = [_FakeIdentityProvider()]
    svc._role_config = IamRolesConfig()

    async def _get_roles_config() -> IamRolesConfig:
        return svc._role_config

    svc._get_roles_config = _get_roles_config

    async def _get_jwt_secrets_for_verification() -> List[str]:
        return [secret]

    async def _resolve_schema(_catalog_id: Any) -> str:
        return "iam"

    def _extract_token_from_request(request: Any) -> str:
        return getattr(request, "token", "")

    def _get_identity_providers() -> List[Any]:
        return svc._identity_providers

    svc.get_jwt_secrets_for_verification = _get_jwt_secrets_for_verification
    svc._resolve_schema = _resolve_schema
    svc.extract_token_from_request = _extract_token_from_request
    svc.get_identity_providers = _get_identity_providers

    return svc


def _make_request(token: str) -> Any:
    class _Req:
        def __init__(self, t: str) -> None:
            self.token = t

            class _State:
                catalog_id = None

            self.state = _State()

    return _Req(token)


@pytest.mark.asyncio
async def test_hs256_jwt_with_empty_roles_defaults_to_user_role() -> None:
    """A JWT carrying ``"roles": []`` must surface as ``["user"]``, not
    ``[]`` (which would collapse to ANONYMOUS at policy time)."""
    import jwt as pyjwt
    from datetime import datetime, timezone, timedelta

    secret = "test-secret-empty-roles-padded-to-32-chars-xx"
    payload = {
        "sub": "test-user-empty",
        "roles": [],
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(hours=1),
        "iss": "regression-test",
    }
    token = pyjwt.encode(payload, secret, algorithm="HS256")

    svc = _build_iam_service_for_hs256(secret)
    effective_roles, principal = await svc.authenticate_and_get_role(_make_request(token))

    assert principal is not None, "Principal should be resolved from valid HS256 token"
    assert _DEFAULTS.default_user_role_name in effective_roles, (
        f"Expected default-user role for empty-roles HS256 JWT, got {effective_roles!r}"
    )
    assert _DEFAULTS.anonymous_role_name not in effective_roles, (
        f"Authenticated principal must NOT carry anonymous role, got {effective_roles!r}"
    )


@pytest.mark.asyncio
async def test_hs256_jwt_without_roles_field_defaults_to_user_role() -> None:
    """A JWT that omits the ``roles`` field entirely should also default
    to ``["user"]`` (matches the pre-existing payload.get default but
    pinned here so the .get('roles', ['user']) → ``or ["user"]`` shift
    can't silently lose the missing-key case)."""
    import jwt as pyjwt
    from datetime import datetime, timezone, timedelta

    secret = "test-secret-no-roles-padded-to-32-chars-xxxxx"
    payload = {
        "sub": "test-user-noroles",
        # no "roles" key
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(hours=1),
        "iss": "regression-test",
    }
    token = pyjwt.encode(payload, secret, algorithm="HS256")

    svc = _build_iam_service_for_hs256(secret)
    effective_roles, principal = await svc.authenticate_and_get_role(_make_request(token))

    assert principal is not None
    assert _DEFAULTS.default_user_role_name in effective_roles, (
        f"Expected default-user role for roles-omitted HS256 JWT, got {effective_roles!r}"
    )


def test_normalize_authenticated_roles_helper_defaults() -> None:
    """Direct test of the extracted ``_normalize_authenticated_roles`` helper.

    The HS256 + OIDC paths now share this method — testing it directly
    pins the "default to USER" rule independently of the bigger
    authentication-flow integration tests above. Future refactors can
    grep for this helper instead of having to reverse-engineer the rule
    from two separate auth-flow branches.
    """
    from dynastore.modules.iam.iam_service import IamService
    from dynastore.models.protocols.authorization import IamRolesConfig

    # _normalize_authenticated_roles is an instance method — build a minimal stub.
    svc = object.__new__(IamService)
    svc._role_config = IamRolesConfig()
    norm = svc._normalize_authenticated_roles
    # None / empty list → default-user role
    assert norm(None) == [_DEFAULTS.default_user_role_name]
    assert norm([]) == [_DEFAULTS.default_user_role_name]
    # Single-string role → wrapped, NOT defaulted
    assert norm("editor") == ["editor"]
    # Non-empty list → returned verbatim
    assert norm(["sysadmin", "admin"]) == ["sysadmin", "admin"]
    # Single role in a list → preserved (NOT defaulted)
    assert norm(["editor"]) == ["editor"]


@pytest.mark.asyncio
async def test_hs256_jwt_with_explicit_roles_preserves_them() -> None:
    """Negative case: an HS256 JWT that DOES carry roles must keep them
    verbatim — the default only fires for the empty/missing case."""
    import jwt as pyjwt
    from datetime import datetime, timezone, timedelta

    secret = "test-secret-explicit-roles-padded-to-32-chars"
    payload = {
        "sub": "test-user-explicit",
        "roles": ["sysadmin"],
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(hours=1),
        "iss": "regression-test",
    }
    token = pyjwt.encode(payload, secret, algorithm="HS256")

    svc = _build_iam_service_for_hs256(secret)
    effective_roles, principal = await svc.authenticate_and_get_role(_make_request(token))

    assert principal is not None
    assert "sysadmin" in effective_roles, (
        f"Expected sysadmin role to be preserved, got {effective_roles!r}"
    )
