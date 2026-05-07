from fastapi import Depends, HTTPException, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional

from dynastore.modules import get_protocol
from dynastore.tools.discovery import get_protocols
from dynastore.models.protocols.authentication import AuthenticatorProtocol
from dynastore.modules.iam.interfaces import IdentityProviderProtocol
from dynastore.models.auth import Principal


# Module-level scheme — the ``scheme_name`` must match the key registered
# under ``components.securitySchemes`` by ``build_iam_openapi_schema`` so
# Swagger UI links the dependency to the right security scheme entry.
bearer_scheme = HTTPBearer(auto_error=False, scheme_name="HTTPBearer")


async def get_authenticator() -> AuthenticatorProtocol:
    protocol = get_protocol(AuthenticatorProtocol)
    if not protocol:
        raise HTTPException(status_code=500, detail="Authenticator implementation not available")
    return protocol


async def get_current_active_user(
    request: Request,
    bearer: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
    manager: AuthenticatorProtocol = Depends(get_authenticator),
) -> Principal:
    """
    Validates the bearer token and returns the current active principal.

    Reads the credential from FastAPI's ``HTTPBearer`` dependency rather
    than the raw ``Authorization`` header. The previous OAuth2PasswordBearer
    -based extraction advertised a Resource-Owner-Password-Credentials flow
    that the IdP (Keycloak ``geoid-fe`` client) does not enable; using
    ``HTTPBearer`` keeps the OpenAPI surface aligned with the actual scheme
    declared in IAM's ``components.securitySchemes``.
    """
    if bearer is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid Authorization header",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = bearer.credentials

    # We use the manager's central authentication method which handles
    # Identity Provider tokens (JWTs via Keycloak).

    # We might need the catalog_id from the path to verify scoped permissions
    catalog_id = request.path_params.get("catalog_id")

    # Try V2 Identity (JWT via identity providers)
    for provider in get_protocols(IdentityProviderProtocol):
        try:
            identity = await provider.validate_token(token)
            if identity:
                principal = await manager.authenticate_and_get_principal(
                    identity=identity,
                    target_schema=catalog_id or "_system_"
                )
                if principal:
                    if not principal.is_active:
                        raise HTTPException(status_code=400, detail="Inactive user")
                    return principal
        except HTTPException:
            raise
        except Exception:
            pass  # Try next provider

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
