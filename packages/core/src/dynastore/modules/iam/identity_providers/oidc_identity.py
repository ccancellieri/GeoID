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
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""
Generic OIDC Identity Provider.

Uses RFC 8414 / OIDC Discovery (/.well-known/openid-configuration) to resolve
all endpoint URLs at runtime, so it works with any standards-compliant IdP:
Keycloak, Okta, Auth0, Azure AD, Google, etc.

See identity_providers/README.md for how to add new provider types.
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
from importlib.metadata import version, PackageNotFoundError

import jwt
from jwt.exceptions import ExpiredSignatureError, InvalidTokenError, PyJWTError

from ..interfaces import IdentityProviderProtocol

logger = logging.getLogger(__name__)


def _user_agent() -> str:
    """Identify outbound JWKS fetches.

    The default ``Python-urllib/<py>`` UA emitted by ``urllib.request`` (used
    internally by ``PyJWKClient``) is rejected by some WAFs, e.g. Cloudflare's
    bot rules in front of FAO's Keycloak, which returns 403 — silently breaking
    JWT signature validation. RFC 9110 ``<product>/<version>`` form, derived
    from the installed package version, restores access without changing
    behaviour for IdPs that don't filter on UA.
    """
    try:
        return f"dynastore/{version('dynastore')}"
    except PackageNotFoundError:
        return "dynastore/unknown"


class OidcIdentityProvider(IdentityProviderProtocol):
    """
    Generic OIDC identity provider.

    Discovers all endpoints via /.well-known/openid-configuration and caches
    the metadata for one hour. Token signatures are validated with PyJWKClient
    which resolves the correct signing key by JWT `kid` header.

    Args:
        issuer_url:    OIDC issuer URL (backend-internal). Used for discovery,
                       token validation, and userinfo calls.
                       Example: ``http://keycloak:8080/realms/myrealm``
        client_id:     OAuth2 client ID registered with the IdP. In two-client
                       deployments this is the SPA / login client (e.g.
                       ``geoid-fe``); it is **not** used for audience
                       validation — see ``audience``.
        client_secret: OAuth2 client secret (for confidential clients).
        audience:      Expected ``aud`` claim (e.g. ``geoid-be``). When the
                       login client and the API audience are the same client,
                       pass the same value as ``client_id``. Defaults to
                       ``client_id`` for legacy single-client setups.
        public_url:    Public-facing issuer URL for browser redirects.
                       Example: ``http://localhost:8180/realms/myrealm``
                       Defaults to ``issuer_url``.
        roles_claim_path: Dotted JSON path used to locate roles inside the
                       decoded JWT claims. ``${audience}`` is substituted with
                       the resolved audience. Defaults to
                       ``resource_access.${audience}.roles``. Common operator
                       overrides: ``resource_access.account.roles`` (when
                       sysadmin sits on Keycloak's built-in account client) or
                       ``realm_access.roles`` (realm roles).
    """

    def __init__(
        self,
        issuer_url: str,
        client_id: str,
        client_secret: Optional[str] = None,
        audience: Optional[str] = None,
        public_url: Optional[str] = None,
        roles_claim_path: Optional[str] = None,
    ):
        self.issuer_url = issuer_url.rstrip("/")
        self.public_url = (public_url or issuer_url).rstrip("/")
        self.client_id = client_id
        self.client_secret = client_secret
        self.audience = audience or client_id

        # Resolve the roles claim path with ${audience} template substitution.
        # Default points at the API audience client roles (standard OIDC
        # pattern); operators can override via IDP_ROLES_CLAIM_PATH to e.g.
        # ``resource_access.account.roles`` (FAO Keycloak setup) or
        # ``realm_access.roles``.
        raw_path = roles_claim_path or "resource_access.${audience}.roles"
        self.roles_claim_path = raw_path.replace("${audience}", self.audience)

        self._meta: Optional[Dict[str, Any]] = None
        self._jwks_client = None  # jwt.PyJWKClient, initialised after discovery
        self._meta_fetched_at: Optional[datetime] = None
        self._meta_ttl: int = 3600  # seconds

    # ------------------------------------------------------------------
    # IdentityProviderProtocol
    # ------------------------------------------------------------------

    def get_provider_id(self) -> str:
        return "oidc"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_http_client(self):
        """
        Return an async HTTP client.

        Prefers the project-centralised HttpxProtocol client so connection
        pooling and retry settings are applied consistently. Falls back to a
        plain ``httpx.AsyncClient`` if the protocol is not yet registered
        (e.g., during early startup).
        """
        try:
            from dynastore.tools.discovery import get_protocol
            from dynastore.models.protocols.httpx import HttpxProtocol
            p = get_protocol(HttpxProtocol)
            if p:
                return p.get_httpx_client()
        except Exception:
            pass
        import httpx
        return httpx.AsyncClient()

    async def _ensure_meta(self) -> Dict[str, Any]:
        """
        Fetch and cache OIDC discovery metadata.

        Refreshes every ``_meta_ttl`` seconds (default: 1 hour). On failure,
        keeps the previous metadata if available so transient network errors
        don't break active sessions.
        """
        now = datetime.now(timezone.utc)
        if self._meta and self._meta_fetched_at:
            age = (now - self._meta_fetched_at).total_seconds()
            if age < self._meta_ttl:
                return self._meta

        discovery_url = f"{self.issuer_url}/.well-known/openid-configuration"
        try:
            client = self._get_http_client()
            async with client as c:
                response = await c.get(discovery_url, timeout=10.0)
                response.raise_for_status()
                meta = response.json()

            from jwt import PyJWKClient
            self._meta = meta
            self._jwks_client = PyJWKClient(
                meta["jwks_uri"],
                cache_keys=True,
                headers={"User-Agent": _user_agent()},
            )
            self._meta_fetched_at = now
            logger.info("OIDC discovery complete: %s", discovery_url)
        except Exception as e:
            if self._meta:
                logger.warning(
                    "OIDC discovery refresh failed (using cached metadata): %s", e
                )
            else:
                logger.error("OIDC discovery failed: %s", e)
                raise

        if self._meta is None:
            raise RuntimeError("OIDC metadata unavailable after discovery")
        return self._meta

    def _public_endpoint(self, internal_url: str) -> str:
        """
        Replace the internal issuer host/port with the public-facing one so
        browser redirects go to the correct URL.

        Example:
            internal: ``http://keycloak:8080/realms/geoid/protocol/…``
            public:   ``http://localhost:8180/realms/geoid/protocol/…``
        """
        if self.public_url == self.issuer_url:
            return internal_url
        return internal_url.replace(self.issuer_url, self.public_url, 1)

    # ------------------------------------------------------------------
    # Token Validation (Resource Server role)
    # ------------------------------------------------------------------

    async def _decode_token(
        self, token: str, *, verify_audience: bool = True
    ) -> Dict[str, Any]:
        """
        Resolve the signing key and decode the JWT. Raises
        :class:`jwt.exceptions.InvalidTokenError` (or subclass — including
        :class:`InvalidAudienceError`) on any verification failure. Returns
        the decoded claims dict on success.

        Audience enforcement: PyJWT enforces ``aud == self.audience`` when
        ``audience=`` is passed to :func:`jwt.decode`; if the token's ``aud``
        is an array, PyJWT also accepts it as long as one element matches.
        """
        await self._ensure_meta()
        if self._jwks_client is None:
            raise RuntimeError("JWKS client not initialised")
        signing_key = self._jwks_client.get_signing_key_from_jwt(token)
        decode_options: Dict[str, Any] = {"verify_exp": True}
        if not verify_audience:
            decode_options["verify_aud"] = False
        # Keycloak sets iss to the public/frontend URL (self.public_url),
        # which may differ from the internal Docker issuer_url used for
        # JWKS discovery. Accept either to handle proxy deployments.
        accepted_issuers = (
            [self.public_url, self.issuer_url]
            if self.public_url != self.issuer_url
            else [self.issuer_url]
        )
        last_err: Exception = RuntimeError("No issuers to try")
        for iss in accepted_issuers:
            try:
                # PyJWT enforces aud == self.audience here (raises
                # InvalidAudienceError on mismatch); accepts aud arrays.
                return jwt.decode(
                    token,
                    key=signing_key.key,
                    algorithms=["RS256", "RS384", "RS512", "ES256", "ES384", "ES512"],
                    audience=self.audience if verify_audience else None,
                    issuer=iss,
                    options=decode_options,  # type: ignore[reportArgumentType]
                )
            except InvalidTokenError as _e:
                last_err = _e
        raise last_err

    def extract_roles(self, claims: Dict[str, Any]) -> List[str]:
        """
        Resolve ``self.roles_claim_path`` (a dotted JSON path) against the
        decoded JWT claims and return the role list found there.

        Walks the path segment-by-segment via ``dict.get(part, {})``; the
        terminal segment must yield a ``list``. Returns an empty list when
        any segment is missing or when the terminal value is not a list.

        Does **not** silently merge other paths. Operators choose exactly
        one location for roles via ``IDP_ROLES_CLAIM_PATH``.
        """
        # Late ${audience} substitution in case the path was set after
        # construction with template syntax still present.
        path = self.roles_claim_path.replace("${audience}", self.audience)
        node: Any = claims
        parts = path.split(".")
        for part in parts:
            if not isinstance(node, dict):
                return []
            node = node.get(part, {})
        if isinstance(node, list):
            return list(node)
        return []

    async def validate_token(
        self, token: str, *, verify_audience: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Validate a JWT access token.

        Steps:
        1. Ensure OIDC metadata is available (triggers discovery on first call).
        2. Resolve the signing key by JWT ``kid`` header via PyJWKClient.
        3. Decode and verify claims (signature, expiry, issuer, audience).
        4. Return a normalised identity dict or ``None`` on any failure.

        Set ``verify_audience=False`` to skip the ``aud`` claim check while
        still verifying signature, expiry, and issuer. Useful when the token
        was issued for a different audience (e.g. ``account``) but the IDP
        has already validated it via the userinfo endpoint.
        """
        if not token:
            return None

        try:
            claims = await self._decode_token(token, verify_audience=verify_audience)
        except ExpiredSignatureError:
            logger.debug("OIDC token expired")
            return None
        except InvalidTokenError as e:
            logger.warning("OIDC token invalid: %s", e)
            return None
        except PyJWTError as e:
            logger.warning("OIDC JWT error: %s", e)
            return None
        except Exception as e:
            logger.error("Unexpected error validating OIDC token: %s", e)
            return None

        # Detect service account (client_credentials grant)
        client_id_claim = claims.get("client_id")
        is_service_account = client_id_claim is not None and not claims.get("email")

        # Fallback chain for the principal subject id. RFC 9068 / OIDC Core
        # require `sub`, but Keycloak 26 realms exported without an explicit
        # oidc-sub-mapper omit it, and the strict lookup returns None — which
        # cascades to `identity_links.subject_id NOT NULL` violations during
        # JIT registration. Fall back to preferred_username / email / client_id
        # so the principal still has a stable, non-null identifier.
        subject_id = (
            claims.get("sub")
            or claims.get("preferred_username")
            or claims.get("email")
            or client_id_claim
        )
        identity: Dict[str, Any] = {
            "provider": "oidc:service_account" if is_service_account else "oidc",
            "sub": subject_id,
            "preferred_username": claims.get("preferred_username"),
            "email": claims.get("email"),
            "name": (
                claims.get("name")
                or claims.get("preferred_username")
                or client_id_claim
            ),
            "email_verified": claims.get("email_verified", False),
            "groups": claims.get("groups", []),
            # ``roles`` is the operator-shaped result driven by
            # ``self.roles_claim_path`` (set via IDP_ROLES_CLAIM_PATH); it is
            # what downstream consumers should migrate to. ``realm_roles`` and
            # ``client_roles`` remain for back-compat with existing callers.
            "roles": self.extract_roles(claims),
            "realm_roles": claims.get("realm_access", {}).get("roles", []),
            "client_roles": (
                claims.get("resource_access", {})
                .get(self.client_id, {})
                .get("roles", [])
            ),
            "azp": claims.get("azp"),
            "client_id": client_id_claim,
            "is_service_account": is_service_account,
            "account_url": self._public_endpoint(self.issuer_url) + "/account/",
            "raw_claims": claims,
        }
        logger.debug(
            "OIDC token validated for %s: %s",
            "service account" if is_service_account else "user",
            identity.get("email") or client_id_claim or identity.get("sub"),
        )
        return identity

    # ------------------------------------------------------------------
    # OAuth2 / OIDC Login Flow (Auth Server broker role)
    # ------------------------------------------------------------------

    async def get_authorization_url(
        self,
        redirect_uri: str,
        state: str,
        scope: str = "openid email profile",
    ) -> str:
        """Return the browser-facing authorization URL."""
        from urllib.parse import urlencode

        meta = await self._ensure_meta()
        # Use the public-facing endpoint for browser redirects
        auth_endpoint = self._public_endpoint(meta["authorization_endpoint"])
        params = {
            "client_id": self.client_id,
            "response_type": "code",
            "redirect_uri": redirect_uri,
            "state": state,
            "scope": scope,
        }
        return f"{auth_endpoint}?{urlencode(params)}"

    async def exchange_code_for_token(
        self,
        code: str,
        redirect_uri: str,
        client_secret: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Exchange an authorization code for tokens."""
        meta = await self._ensure_meta()
        token_endpoint = meta["token_endpoint"]
        data: Dict[str, str] = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": self.client_id,
        }
        secret = client_secret or self.client_secret
        if secret:
            data["client_secret"] = secret

        client = self._get_http_client()
        async with client as c:
            response = await c.post(token_endpoint, data=data, timeout=10.0)
            response.raise_for_status()
            return response.json()

    async def refresh_token(
        self,
        refresh_token: str,
        client_secret: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Exchange a refresh token for a new access token."""
        meta = await self._ensure_meta()
        token_endpoint = meta["token_endpoint"]
        data: Dict[str, str] = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": self.client_id,
        }
        secret = client_secret or self.client_secret
        if secret:
            data["client_secret"] = secret

        client = self._get_http_client()
        async with client as c:
            response = await c.post(token_endpoint, data=data, timeout=10.0)
            response.raise_for_status()
            return response.json()

    async def get_user_info(self, access_token: str) -> Dict[str, Any]:
        """Fetch the user profile from the OIDC UserInfo endpoint."""
        meta = await self._ensure_meta()
        userinfo_endpoint = meta["userinfo_endpoint"]
        client = self._get_http_client()
        async with client as c:
            response = await c.get(
                userinfo_endpoint,
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=10.0,
            )
            response.raise_for_status()
            return response.json()
