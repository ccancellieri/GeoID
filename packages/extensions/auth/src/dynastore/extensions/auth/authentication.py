import os
import hashlib
import secrets
from typing import List, Any, Dict, Optional
from fastapi import APIRouter, FastAPI, Request, Depends, HTTPException, Form
from fastapi.responses import RedirectResponse, HTMLResponse, FileResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.web.decorators import expose_static
from dynastore.extensions.tools.url import build_sibling_redirect, resolve_redirect_uri
from dynastore.models.protocols import (
    CatalogsProtocol,
    ConfigsProtocol,
    PropertiesProtocol,
)
from contextlib import asynccontextmanager
from starlette.middleware.sessions import SessionMiddleware
import logging

logger = logging.getLogger(__name__)

# Module-level scheme so FastAPI registers a single ``HTTPBearer`` security
# scheme in the generated OpenAPI document. ``scheme_name`` matches the key
# used by IAM's ``build_iam_openapi_schema`` so Swagger UI ties the
# dependency to the right entry in ``components.securitySchemes`` and
# renders a lock icon on each operation.
bearer_scheme = HTTPBearer(auto_error=False, scheme_name="HTTPBearer")

_SYSADMIN_PROP_KEY = "auth_sysadmin_password_enc"


class _LoginRateLimiter:
    """In-memory per-IP/username failed login tracker with lockout."""

    def __init__(self, max_attempts: int = 5, lockout_seconds: int = 300):
        self.max_attempts = max_attempts
        self.lockout_seconds = lockout_seconds
        self._attempts: dict = {}  # key -> (count, first_attempt_time)

    def _key(self, ip: str, username: str) -> str:
        return f"{ip}:{username}"

    def is_locked(self, ip: str, username: str) -> bool:
        import time as _time
        key = self._key(ip, username)
        entry = self._attempts.get(key)
        if not entry:
            return False
        count, first_ts = entry
        if _time.monotonic() - first_ts > self.lockout_seconds:
            del self._attempts[key]
            return False
        return count >= self.max_attempts

    def record_failure(self, ip: str, username: str) -> None:
        import time as _time
        key = self._key(ip, username)
        entry = self._attempts.get(key)
        now = _time.monotonic()
        if entry:
            count, first_ts = entry
            if now - first_ts > self.lockout_seconds:
                self._attempts[key] = (1, now)
            else:
                self._attempts[key] = (count + 1, first_ts)
        else:
            self._attempts[key] = (1, now)

    def record_success(self, ip: str, username: str) -> None:
        self._attempts.pop(self._key(ip, username), None)


_login_limiter = _LoginRateLimiter()


def _derive_fernet_key(jwt_secret: str) -> bytes:
    """Derive a 256-bit Fernet-compatible key from the JWT secret via SHA-256."""
    import base64
    raw = hashlib.sha256(jwt_secret.encode()).digest()  # 32 bytes
    return base64.urlsafe_b64encode(raw)


# Identity Provider configuration.
# IDP_* are the canonical names; KEYCLOAK_* are accepted as aliases for
# backward compatibility with existing deployments.
#
# Two-client OAuth/OIDC is the standard layout: a public SPA login client
# (``IDP_CLIENT_ID``, e.g. ``geoid-fe``) and a separate API audience
# (``IDP_AUDIENCE``, e.g. ``geoid-be``). Single-client legacy setups can leave
# ``IDP_AUDIENCE`` unset; it falls back to ``IDP_CLIENT_ID`` and a one-shot
# deprecation warning is emitted at startup.
IDP_ISSUER_URL       = os.getenv("IDP_ISSUER_URL")    or os.getenv("KEYCLOAK_ISSUER_URL")
IDP_CLIENT_ID        = os.getenv("IDP_CLIENT_ID")     or os.getenv("KEYCLOAK_CLIENT_ID")
IDP_CLIENT_SECRET    = os.getenv("IDP_CLIENT_SECRET") or os.getenv("KEYCLOAK_CLIENT_SECRET")
IDP_PUBLIC_URL       = os.getenv("IDP_PUBLIC_URL")    or os.getenv("KEYCLOAK_PUBLIC_URL")
IDP_AUDIENCE         = os.getenv("IDP_AUDIENCE")      or os.getenv("KEYCLOAK_AUDIENCE")
# Dotted JSON path used to locate roles in the JWT. ``${IDP_AUDIENCE}`` is
# substituted with the resolved audience. Defaults to
# ``resource_access.${IDP_AUDIENCE}.roles``. Common operator overrides:
# ``resource_access.account.roles`` (sysadmin role on Keycloak's built-in
# account client) or ``realm_access.roles``.
IDP_ROLES_CLAIM_PATH = os.getenv("IDP_ROLES_CLAIM_PATH")

if IDP_CLIENT_ID and not IDP_AUDIENCE:
    IDP_AUDIENCE = IDP_CLIENT_ID
    logger.warning(
        "IDP_AUDIENCE is unset, falling back to IDP_CLIENT_ID for token "
        "audience validation. This is deprecated and will be removed in the "
        "next minor; set IDP_AUDIENCE explicitly."
    )


class Authentication(ExtensionProtocol):
    always_on = True
    priority: int = 110  # Must run after IamModule (100) which owns users schema init
    """
    Authentication Extension - OAuth2 / OIDC Identity Validation

    Responsibilities:
    - Validate OAuth2 tokens (JWT)
    - Provide OAuth2 endpoints (/authorize, /token, /userinfo, /refresh)
    - Support any OIDC-compliant identity provider
    - Return validated Identity objects

    Does NOT handle:
    - Principal management (that's in iam module)
    - Permission checking (that's authorization, not authentication)
    """

    router: APIRouter = APIRouter(
        tags=["Authentication & Authorization"], prefix="/auth"
    )

    # Active identity provider instance (set during lifespan)
    identity_provider = None

    def get_static_assets(self):
        from dynastore.extensions.tools.web_collect import collect_static_assets
        return collect_static_assets(self)

    @expose_static("auth")
    def _provide_auth_static(self) -> List[str]:
        """Expose auth static files."""
        static_dir = os.path.join(os.path.dirname(__file__), "static", "auth")
        files = []
        for root, _, filenames in os.walk(static_dir):
            for filename in filenames:
                files.append(os.path.join(root, filename))
        return files

    def configure_app(self, app: FastAPI):
        """Early configuration - add session middleware."""
        session_secret = os.environ.get("SESSION_SECRET_KEY")
        if not session_secret:
            logger.warning("SESSION_SECRET_KEY not set. Using temporary key.")
            session_secret = secrets.token_hex(32)

        app.add_middleware(SessionMiddleware, secret_key=session_secret)

    def __init__(self, app: Optional[FastAPI] = None):
        self.app = app
        # Setup routes will be called after lifespan initialization

    def _setup_routes(self):
        """Setup routes with closures to access instance state."""

        @self.router.get("/authorize")
        async def authorize(
            request: Request,
            response_type: str,
            client_id: str,
            redirect_uri: str,
            state: str,
            scope: str = "openid email profile",
            provider: Optional[str] = None,
        ):
            """
            OAuth2 Authorization Endpoint.

            Redirects the browser to the configured identity provider's login page.
            """
            # Resolve relative redirect_uri to absolute URL so the IdP can
            # match it against its allowed redirect URIs; also coerce
            # http:// → https:// when FORCE_HTTPS is set (inner LB is plain HTTP).
            redirect_uri = resolve_redirect_uri(request, redirect_uri)

            if self.identity_provider:
                auth_url = await self.identity_provider.get_authorization_url(
                    redirect_uri=redirect_uri, state=state, scope=scope
                )
                return RedirectResponse(url=auth_url)

            raise HTTPException(
                501, "No identity provider configured. Set IDP_ISSUER_URL."
            )

        @self.router.get("/userinfo")
        async def userinfo(
            bearer: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
        ):
            """
            OAuth2 UserInfo Endpoint.
            Returns normalized user profile from the validated JWT token.
            Tries strict audience validation first; falls back to lax (no aud
            check) when the token was issued for a different audience (e.g.
            ``account``) but signature + expiry + issuer are still verified.
            """
            if bearer is None:
                raise HTTPException(401, "Missing or invalid Authorization header")

            token = bearer.credentials

            if self.identity_provider:
                identity = await self.identity_provider.validate_token(token)
                if identity is None:
                    # Audience mismatch (token aud ≠ client_id) — re-try
                    # without aud check; signature/expiry/issuer still verified.
                    identity = await self.identity_provider.validate_token(
                        token, verify_audience=False
                    )
                if identity:
                    return identity

            raise HTTPException(401, "Invalid access token")

        @self.router.post("/token")
        async def token_exchange(request: Request):
            """
            OAuth2 Token Exchange Endpoint.
            Exchanges an authorization code for an access token via Keycloak.
            """
            form = await request.form()
            code = form.get("code")
            redirect_uri = form.get("redirect_uri", "")
            client_id = form.get("client_id", "")

            if not code or not isinstance(code, str):
                raise HTTPException(400, "Missing authorization code")
            if not isinstance(redirect_uri, str):
                redirect_uri = ""

            # Must match the value used at /authorize exactly (IdP compares strictly).
            redirect_uri = resolve_redirect_uri(request, redirect_uri)

            if self.identity_provider:
                try:
                    token_data = await self.identity_provider.exchange_code_for_token(
                        code=code, redirect_uri=redirect_uri,
                    )
                    return token_data
                except Exception as e:
                    logger.error("Token exchange failed: %s", e)
                    raise HTTPException(400, f"Token exchange failed: {e}")

            raise HTTPException(501, "No identity provider configured.")

        @self.router.post("/refresh")
        async def refresh_token(request: Request):
            """
            OAuth2 Token Refresh Endpoint.
            Exchanges a refresh token for a new access token via Keycloak.
            """
            form = await request.form()
            refresh_token_str = form.get("refresh_token")

            if not refresh_token_str or not isinstance(refresh_token_str, str):
                raise HTTPException(400, "Missing refresh_token")

            if self.identity_provider:
                try:
                    token_data = await self.identity_provider.refresh_token(
                        refresh_token=refresh_token_str,
                    )
                    return token_data
                except Exception as e:
                    logger.error("Token refresh failed: %s", e)
                    raise HTTPException(400, f"Token refresh failed: {e}")

            raise HTTPException(501, "No identity provider configured.")

        @self.router.get("/logout")
        async def logout(request: Request, redirect_uri: Optional[str] = None):
            """Logout endpoint - clears session."""
            root_path = request.scope.get("root_path", "").rstrip("/")
            if not redirect_uri:
                redirect_uri = f"{root_path}/auth/login"
            elif redirect_uri.startswith("/"):
                # If absolute path on same domain, prepend root_path
                redirect_uri = f"{root_path}{redirect_uri}"

            request.session.clear()
            return RedirectResponse(url=redirect_uri)

        @self.router.get("/debug")
        async def debug_auth(
            request: Request,
            bearer: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
        ):
            """Debug endpoint to inspect authentication state. Requires a valid Bearer token."""
            if bearer is None:
                raise HTTPException(401, "Missing or invalid Authorization header")

            token = bearer.credentials
            user_info = None

            if self.identity_provider:
                try:
                    user_info = await self.identity_provider.get_user_info(token)
                except Exception:
                    pass

            if not user_info:
                raise HTTPException(403, "Valid authenticated token required")

            return {
                "identity_providers": {
                    "oidc": "enabled" if self.identity_provider else "disabled",
                },
            }

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        """Initialize the identity provider during lifespan."""
        logger.info("Authentication Extension: Initializing identity provider")
        self.app_state = app

        if IDP_ISSUER_URL and IDP_CLIENT_ID:
            from dynastore.modules.iam.identity_providers import OidcIdentityProvider
            # Pass ``audience`` explicitly so the constructor fallback
            # (``audience or client_id``) cannot silently mask a missing
            # IDP_AUDIENCE in two-client setups.
            self.identity_provider = OidcIdentityProvider(
                issuer_url=IDP_ISSUER_URL,
                client_id=IDP_CLIENT_ID,
                client_secret=IDP_CLIENT_SECRET,
                public_url=IDP_PUBLIC_URL,
                audience=IDP_AUDIENCE,
                roles_claim_path=IDP_ROLES_CLAIM_PATH,
            )
            logger.info("✓ OIDC identity provider initialized: %s", IDP_ISSUER_URL)
        else:
            logger.info("⊘ No identity provider configured (IDP_ISSUER_URL not set)")

        # Setup routes after providers are initialized
        self._setup_routes()
        logger.info("✓ Authentication routes configured")

        yield
