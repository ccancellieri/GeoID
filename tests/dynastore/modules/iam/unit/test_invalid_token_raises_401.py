"""Regression: invalid bearer tokens must surface as 401, not anonymous.

Closes the verified root cause behind issues #415/#416/#417: a request
with ``Authorization: Bearer <invalid>`` previously returned
``([anonymous], None)`` from ``IamService.authenticate_and_get_role``,
allowing the request to proceed under anonymous role. For endpoints
gated only by ``role != anonymous`` (or by sysadmin checks performed
later in handler code rather than via PermissionProtocol policies) this
amounted to authentication bypass.

The fix raises ``InvalidAuthTokenError`` when a token is present but no
provider can validate it; ``IamMiddleware`` translates that to a 401.
The "no token at all" path still returns ``[anonymous], None``.
"""

from typing import Any, List

import pytest

from dynastore.modules.iam.exceptions import InvalidAuthTokenError


class _FakeStorage:
    async def get_role_hierarchy(self, roles: List[str], schema: str) -> List[str]:
        return list(roles)


class _RejectingProvider:
    def get_provider_id(self) -> str:
        return "rejecting"

    async def validate_token(self, token: str) -> None:
        return None


def _build_svc(secret: str = "x" * 32) -> Any:
    from dynastore.modules.iam.iam_service import IamService
    from dynastore.models.protocols.authorization import IamRolesConfig

    svc = object.__new__(IamService)
    svc.storage = _FakeStorage()
    svc._identity_providers = [_RejectingProvider()]
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
    class _State:
        catalog_id = None

    class _Req:
        def __init__(self, t: str) -> None:
            self.token = t
            self.state = _State()

    return _Req(token)


@pytest.mark.asyncio
async def test_no_token_still_returns_anonymous() -> None:
    """The "no Authorization header" path is legitimate anonymous traffic
    (health checks, public landing pages). It must NOT raise."""
    from dynastore.models.protocols.authorization import IamRolesConfig

    svc = _build_svc()
    roles, principal = await svc.authenticate_and_get_role(_make_request(""))
    assert principal is None
    assert IamRolesConfig().anonymous_role_name in roles


@pytest.mark.asyncio
async def test_invalid_token_raises_invalid_auth_token_error() -> None:
    """Token present but rejected by every provider AND HS256 fallback
    must raise InvalidAuthTokenError so the middleware returns 401."""
    svc = _build_svc()
    with pytest.raises(InvalidAuthTokenError):
        await svc.authenticate_and_get_role(_make_request("not-a-real-jwt"))


@pytest.mark.asyncio
async def test_invalid_hs256_signature_raises() -> None:
    """A syntactically-valid JWT signed with a different secret must also
    raise — the HS256 fallback iterates the configured secrets list and
    falls through here with ``payload`` still None."""
    import jwt as pyjwt
    from datetime import datetime, timezone, timedelta

    svc = _build_svc(secret="server-secret-padded-to-32-chars-xxxx")
    forged = pyjwt.encode(
        {
            "sub": "attacker",
            "roles": ["sysadmin"],
            "iat": datetime.now(timezone.utc),
            "exp": datetime.now(timezone.utc) + timedelta(hours=1),
        },
        "attacker-chosen-secret-padded-to-32",
        algorithm="HS256",
    )

    with pytest.raises(InvalidAuthTokenError):
        await svc.authenticate_and_get_role(_make_request(forged))


def test_invalid_auth_token_error_carries_401_status() -> None:
    """Sanity-check the exception advertises 401 for any handler-side
    error mapper that reads ``status_code`` off IamError subclasses."""
    err = InvalidAuthTokenError("bad token")
    assert err.status_code == 401
