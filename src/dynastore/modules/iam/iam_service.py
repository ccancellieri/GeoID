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

# File: dynastore/modules/iam/iam_service.py

import logging
import secrets
import os
import jwt
from contextlib import asynccontextmanager
from typing import List, Optional, Tuple, Any, Dict, Union, AsyncGenerator
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from dynastore.models.protocols.authorization import DefaultRole

from .models import (
    Principal,
    TokenResponse,
    Role,
    RefreshToken,
    Policy,
)
from .iam_storage import AbstractIamStorage
from .postgres_iam_storage import PostgresIamStorage
from .interfaces import IdentityProviderProtocol, AuthorizationStorageProtocol
from .policies import PolicyService
from .exceptions import (
    PrincipalNotFoundError,
    RateLimitExceededError,
    QuotaExceededError,
    IamError,
)

from dynastore.modules.db_config.tools import managed_transaction
from dynastore.modules import get_protocol
from dynastore.models.protocols import (
    PropertiesProtocol,
    DatabaseProtocol,
    CatalogsProtocol,
)
from dynastore.models.protocols.policies import PermissionProtocol
from dynastore.models.driver_context import DriverContext


logger = logging.getLogger(__name__)


class IamService:
    """
    Business Logic Layer for IAG v2.0.
    Handles the Identity -> Context -> Principal resolution and API Key management.
    """

    priority: int = 100

    def __init__(
        self,
        storage: Optional[AbstractIamStorage] = None,
        policy_service: Optional[object] = None,
        app_state: Optional[object] = None,
    ):
        # In V2, we prefer PostgresIamStorage which implements both interfaces.
        # Typed as Any because PostgresIamStorage has methods not declared in either
        # AbstractIamStorage or AuthorizationStorageProtocol (e.g. get_identity_roles).
        self.storage: Any = storage or PostgresIamStorage(app_state=app_state)
        self.policy_service = policy_service
        self.app_state = app_state
        self._jwt_secret: Optional[str] = None

    async def get_jwt_secret(self) -> str:
        """Retrieves or generates the active JWT secret."""
        props = get_protocol(PropertiesProtocol)
        if props:
            secret = await props.get_property("iam_jwt_secret")
            if secret:
                self._jwt_secret = secret
                return secret

        secret = os.environ.get("JWT_SECRET", secrets.token_urlsafe(32))
        if props:
            await props.set_property("iam_jwt_secret", secret, "iam_extension")
        else:
            logger.warning("PropertiesProtocol not available. Cannot persist new JWT secret.")

        self._jwt_secret = secret
        return secret

    async def get_jwt_secrets_for_verification(self) -> list:
        """Returns [current, previous] secrets for token verification (rotation support)."""
        result = []
        props = get_protocol(PropertiesProtocol)
        current = await self.get_jwt_secret()
        result.append(current)
        if props:
            prev = await props.get_property("iam_jwt_secret_prev")
            if prev:
                result.append(prev)
        return result

    async def rotate_jwt_secret(self) -> str:
        """Rotate JWT secret: current becomes previous, new secret is generated."""
        props = get_protocol(PropertiesProtocol)
        if not props:
            raise RuntimeError("PropertiesProtocol required for secret rotation.")
        current = await self.get_jwt_secret()
        new_secret = secrets.token_urlsafe(32)
        await props.set_property("iam_jwt_secret_prev", current, "iam_extension")
        await props.set_property("iam_jwt_secret", new_secret, "iam_extension")
        self._jwt_secret = new_secret
        logger.info("JWT secret rotated successfully.")
        return new_secret

    async def get_jwks(self) -> Dict[str, Any]:
        """Returns a simplified JWKS (Standard path for token verification)."""
        # For HS256, we don't have public keys.
        # But we could return a placeholder or information about the issuer.
        return {
            "keys": [],
            "info": "DynaStore Uses HS256 for internal tokens. Use /iam/auth/validate for verification.",
        }

    async def resolve_schema(
        self, catalog_id: Optional[str] = None, conn: Optional[Any] = None
    ) -> str:
        """
        Determines the database schema.
        If catalog_id is provided, resolves the physical tenant schema.
        Otherwise, defaults to the global 'iam' schema.
        """
        if not catalog_id or catalog_id == "_system_":
            return "iam"

        catalogs = get_protocol(CatalogsProtocol)
        if not catalogs:
            logger.warning(
                f"CatalogsProtocol not available for schema resolution of '{catalog_id}'. Falling back to 'iam'."
            )
            return "iam"

        # We use the CatalogsProtocol to resolve the schema.
        # It handles caching and physical schema lookup.
        try:
            res = await catalogs.resolve_physical_schema(
                catalog_id,
                ctx=DriverContext(db_resource=conn or catalogs.engine),  # type: ignore[attr-defined]
            )
            if res:
                return res
        except Exception as e:
            logger.warning(
                f"Error resolving schema for '{catalog_id}': {e}. Falling back to 'iam'."
            )

        return "iam"

    # Internal alias used by extensions and multi_catalog_helpers
    _resolve_schema = resolve_schema

    def get_identity_providers(self) -> List[IdentityProviderProtocol]:
        """Returns all registered identity providers."""
        from dynastore.tools.discovery import get_protocols

        return get_protocols(IdentityProviderProtocol)

    def get_identity_provider(
        self, provider_id: str
    ) -> Optional[IdentityProviderProtocol]:
        """Returns a specific identity provider by ID."""
        for provider in self.get_identity_providers():
            if provider.get_provider_id() == provider_id:
                return provider
        return None

    def get_policy_service(self) -> PermissionProtocol:
        """Returns the policy service for checking permissions."""
        return self.policy_service  # type: ignore[return-value]

    async def get_effective_permissions(
        self, identity: Dict[str, Any], catalog_id: str, conn: Optional[Any] = None
    ) -> Optional[Principal]:
        """
        Get effective permissions for an identity in a catalog.

        Roles are resolved via the unified grants table (one query
        across platform ∪ catalog scopes). Authorization metadata and
        custom policies live on the platform `iam.principals` row, so
        they're fetched once and apply uniformly.

        Args:
            identity: Identity dict with provider and sub
            catalog_id: Catalog code to check permissions for
            conn: Optional database connection

        Returns:
            Principal object with merged permissions or None
        """
        provider = identity.get("provider")
        subject_id = identity.get("sub")

        if not provider or not subject_id:
            logger.warning("Invalid identity: missing provider or sub")
            return None

        catalog_schema = await self._resolve_schema(catalog_id, conn)

        # 1. Resolve roles in one shot — platform grants ∪ catalog grants
        #    for the resolved tenant schema (or platform-only when
        #    catalog_schema falls back to "iam").
        effective_roles = await self.storage.get_identity_roles(
            provider=provider,
            subject_id=subject_id,
            catalog_schema=catalog_schema if catalog_schema != "iam" else None,
            conn=conn,
        ) or []

        # 2. Authorization metadata + custom policies live on the
        #    platform principal row (D12 — no per-tenant principals).
        auth_metadata = await self.storage.get_identity_authorization(
            provider=provider, subject_id=subject_id, conn=conn
        )

        # 3. If no roles and no metadata, user has no access
        if not effective_roles and not auth_metadata:
            return None

        # 4. Custom policies — platform-only row (D12).
        custom_policies = await self.storage.get_identity_policies(
            provider=provider, subject_id=subject_id, conn=conn
        ) or []

        # 5. Build runtime Principal
        return Principal(
            provider=provider,
            subject_id=subject_id,
            display_name=auth_metadata.get("display_name")
            if auth_metadata
            else subject_id,
            is_active=auth_metadata.get("is_active", True) if auth_metadata else True,
            valid_until=auth_metadata.get("valid_until") if auth_metadata else None,
            roles=effective_roles,
            custom_policies=custom_policies,
            attributes=auth_metadata.get("attributes", {}) if auth_metadata else {},
        )

    @asynccontextmanager
    async def lifespan(self, app_state: Any) -> AsyncGenerator[None, None]:
        """Manages the lifecycle of the service."""
        yield

    # --- IAG V2: Core Identity Logic ---

    async def authenticate_and_get_principal(
        self,
        identity: Dict[str, Any],
        target_schema: str,
        auto_register: bool = False,
    ) -> Optional[Principal]:
        """
        Resolves an Identity (from OAuth2) to a Principal with effective permissions.

        Two-tier principal system:
        1. Global principals (catalog schema) - platform-wide permissions
        2. Catalog principals (tenant schema) - catalog-specific permissions
        Permissions are merged hierarchically (global + catalog).

        Args:
            identity: Identity dict with 'provider' and 'sub' keys
            target_schema: Target catalog code
            auto_register: If True, create principal if not found

        Returns:
            Principal with merged permissions, or None if no access
        """
        # Use new permission resolution algorithm
        logger.warning(
            f"DEBUG: authenticate_and_get_principal for {identity.get('sub')} in {target_schema}"
        )
        principal = await self.get_effective_permissions(
            identity=identity, catalog_id=target_schema
        )

        if principal:
            logger.warning(
                f"DEBUG: principal found: {principal.id} with roles: {principal.roles}"
            )
            # Check if principal is active
            if not principal.is_active:
                logger.warning(f"Principal {principal.id} is inactive")
                return None

            # Check expiration
            if principal.valid_until and principal.valid_until < datetime.now(
                timezone.utc
            ):
                logger.warning(f"Principal {principal.id} has expired")
                return None

            return principal

        # Auto-registration (if enabled)
        if auto_register:
            return await self._auto_register_principal(identity, target_schema)

        return None

    async def _auto_register_principal(
        self, identity: Dict[str, Any], catalog_id: str
    ) -> Optional[Principal]:
        """
        Auto-register a new principal with default VIEWER role.

        Args:
            identity: Identity dict with 'provider' and 'sub' keys
            catalog_id: Target catalog code

        Returns:
            Newly created principal
        """
        provider = identity.get("provider")
        subject_id = identity.get("sub")
        email = identity.get("email", f"{subject_id}@{provider}")

        logger.info(
            f"Auto-registering new principal: {provider}:{subject_id} in {catalog_id}"
        )

        # Build attributes — enrich for service accounts
        attributes: Dict[str, Any] = {}
        if identity.get("is_service_account"):
            attributes["service_account"] = True
            if identity.get("client_id"):
                attributes["client_id"] = identity["client_id"]
            if identity.get("azp"):
                attributes["azp"] = identity["azp"]

        # Use realm_roles from OIDC token if present, otherwise default to DefaultRole.USER
        known_roles = {r.value for r in DefaultRole}
        realm_roles = [
            r for r in identity.get("realm_roles", [])
            if r in known_roles
        ]
        assigned_roles = realm_roles if realm_roles else [DefaultRole.USER.value]

        # Allocate the principal UUID up-front so we can pass it to
        # downstream calls without re-narrowing the model's
        # ``Optional[Union[UUID, str]]`` field — the model validator
        # would synthesize one anyway.
        new_principal_id: UUID = uuid4()
        new_principal = Principal(
            id=new_principal_id,
            provider=provider,
            subject_id=subject_id,
            display_name=email,
            roles=assigned_roles,
            is_active=True,
            custom_policies=[],
            attributes=attributes,
        )

        # Principal + identity link are platform-global (D12); the
        # storage layer hardcodes `iam` internally, no `schema=`.
        await self.storage.create_principal(new_principal)
        await self.storage.create_identity_link(
            principal_id=new_principal_id,
            provider=provider,
            subject_id=subject_id,
        )

        # Grants land at the right scope: catalog-scoped when we have
        # a real catalog, platform-scoped for `_system_` / no catalog.
        catalog_schema = await self._resolve_schema(catalog_id)
        await self._apply_role_grants(
            principal_id=new_principal_id,
            roles=assigned_roles,
            catalog_schema=catalog_schema,
        )

        logger.info(
            f"Auto-registered principal {new_principal.id} for {provider}:{subject_id}"
        )

        return new_principal

    async def _apply_role_grants(
        self,
        principal_id: UUID,
        roles: List[str],
        catalog_schema: str,
        granted_by: Optional[UUID] = None,
    ) -> None:
        """Issue role grants at the right scope.

        Platform schema (`iam`) grants go to `iam.grants`; everything
        else lands in `{catalog_schema}.grants`. Caller is responsible
        for deciding which roles belong at which scope — this helper
        just routes the grant to the correct storage facade.
        """
        if not roles:
            return
        if catalog_schema == "iam":
            for role_name in roles:
                await self.storage.grant_platform_role(
                    principal_id=principal_id,
                    role_name=role_name,
                    granted_by=granted_by,
                )
        else:
            for role_name in roles:
                await self.storage.grant_catalog_role(
                    principal_id=principal_id,
                    role_name=role_name,
                    catalog_schema=catalog_schema,
                    granted_by=granted_by,
                )

    async def _register_new_principal(
        self, identity: Dict[str, Any], schema: str
    ) -> Principal:
        """Helper to onboard a valid identity into a new tenant context.

        `schema` is retained for caller compatibility (it's also where
        the default role grant should land); the principal + link are
        always written to the platform `iam` schema (D12).
        """
        attributes: Dict[str, Any] = {"source": "auto_registration"}
        if identity.get("is_service_account"):
            attributes["service_account"] = True
            if identity.get("client_id"):
                attributes["client_id"] = identity["client_id"]
            if identity.get("azp"):
                attributes["azp"] = identity["azp"]

        new_p = Principal(
            id=uuid4(),
            display_name=identity.get("email", "Unknown"),
            roles=["viewer"],  # Default role
            attributes=attributes,
        )
        created = await self.storage.create_principal_link(new_p, identity)
        # Default-role grant at the requested scope.
        await self._apply_role_grants(
            principal_id=created.id,
            roles=new_p.roles or [],
            catalog_schema=schema,
        )
        return created

    # --- Principal Management (CRUD) ---

    async def create_principal(
        self, principal: Principal, catalog_id: Optional[str] = None
    ) -> Principal:
        # Principal + identity link are platform-global (D12). The
        # `catalog_id` argument only influences where role grants land.
        created = await self.storage.create_principal(principal)

        if principal.provider and principal.subject_id:
            await self.storage.create_identity_link(
                provider=principal.provider,
                subject_id=principal.subject_id,
                principal_id=created.id,
            )

            if principal.roles:
                catalog_schema = await self._resolve_schema(catalog_id)
                await self._apply_role_grants(
                    principal_id=created.id,
                    roles=principal.roles,
                    catalog_schema=catalog_schema,
                )
                # Storage row never carries roles (D12 — grants live in
                # `*.grants`). Echo the just-applied roles on the returned
                # object so callers see the as-granted state.
                created.roles = list(principal.roles)
        return created

    async def get_principal(
        self, principal_id: UUID, catalog_id: Optional[str] = None
    ) -> Optional[Principal]:
        # Principals are platform-global (D12); `catalog_id` ignored
        # except for callers that still expect the kwarg.
        return await self.storage.get_principal(principal_id)

    async def update_principal(
        self, principal: Principal, catalog_id: Optional[str] = None
    ) -> Optional[Principal]:
        return await self.storage.update_principal(principal)

    async def delete_principal(
        self, principal_id: UUID, catalog_id: Optional[str] = None
    ) -> bool:
        return await self.storage.delete_principal(principal_id)

    async def list_principals(
        self, limit: int = 100, offset: int = 0, catalog_id: Optional[str] = None
    ) -> List[Principal]:
        return await self.storage.list_principals(limit=limit, offset=offset)

    async def search_principals(
        self,
        identifier: Optional[str] = None,
        role: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        catalog_id: Optional[str] = None,
    ) -> List[Principal]:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.search_principals(
            identifier=identifier, role=role, limit=limit, offset=offset, schema=schema
        )

    # --- Role & Hierarchy Management ---

    async def create_role(self, role: Role, catalog_id: Optional[str] = None) -> Role:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.create_role(role, schema=schema)

    async def list_roles(self, catalog_id: Optional[str] = None) -> List[Role]:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.list_roles(schema=schema)

    async def update_role(
        self, role: Role, catalog_id: Optional[str] = None
    ) -> Optional[Role]:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.update_role(role, schema=schema)

    async def delete_role(
        self, name: str, cascade: bool = False, catalog_id: Optional[str] = None
    ) -> bool:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.delete_role(name, cascade=cascade, schema=schema)

    async def add_role_hierarchy(
        self, parent_role: str, child_role: str, catalog_id: Optional[str] = None
    ):
        schema = await self._resolve_schema(catalog_id)
        await self.storage.add_role_hierarchy(parent_role, child_role, schema=schema)

    async def remove_role_hierarchy(
        self, parent_role: str, child_role: str, catalog_id: Optional[str] = None
    ) -> bool:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.remove_role_hierarchy(
            parent_role, child_role, schema=schema
        )

    async def get_role_hierarchy(
        self, role_name: str, catalog_id: Optional[str] = None
    ) -> List[str]:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.get_role_hierarchy([role_name], schema=schema)

    # --- IamQueryProtocol surface (read-side, for non-IAM consumers) ---
    #
    # Exposes the catalog-membership lookup behind a stable signature so
    # routes / extensions can resolve it via ``get_protocol(IamQueryProtocol)``
    # and never need to import ``modules.iam.*`` directly.

    async def list_catalog_memberships(
        self, provider: str, subject_id: str
    ) -> Dict[str, Any]:
        """Return ``{platform, catalogs, total}`` for the given identity."""
        from dynastore.modules.iam.multi_catalog_helpers import (
            list_catalogs_for_identity,
        )

        return await list_catalogs_for_identity(
            iam_manager=self, provider=provider, subject_id=subject_id
        )

    # --- Policy Management ---

    async def create_policy(self, policy, catalog_id: Optional[str] = None):
        """Creates a new policy."""
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.create_policy(policy, schema=schema)

    async def get_policy(self, policy_id: str, catalog_id: Optional[str] = None):
        """Retrieves a policy by ID."""
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.get_policy(policy_id, schema=schema)

    async def list_policies(
        self, limit: int = 100, offset: int = 0, catalog_id: Optional[str] = None
    ):
        """Lists all policies with pagination."""
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.list_policies(
            limit=limit, offset=offset, schema=schema
        )

    async def update_policy(self, policy, catalog_id: Optional[str] = None):
        """Updates an existing policy."""
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.update_policy(policy, schema=schema)

    async def delete_policy(
        self, policy_id: str, catalog_id: Optional[str] = None
    ) -> bool:
        """Deletes a policy."""
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.delete_policy(policy_id, schema=schema)

    # --- Identity Link Management ---

    async def create_identity_link(
        self,
        principal_id: UUID,
        provider: str,
        subject_id: str,
        email: Optional[str] = None,
        catalog_id: Optional[str] = None,
    ) -> bool:
        """Creates an identity link for a principal.

        Identity links are platform-global (D12). The `catalog_id`
        argument is honored only for routing grants of the principal's
        existing role list to the right scope (platform vs. catalog).
        """
        success = await self.storage.create_identity_link(
            principal_id, provider, subject_id, email
        )

        if success:
            principal = await self.storage.get_principal(principal_id)
            if principal and principal.roles:
                catalog_schema = await self._resolve_schema(catalog_id)
                await self._apply_role_grants(
                    principal_id=principal_id,
                    roles=principal.roles,
                    catalog_schema=catalog_schema,
                )
        return success

    async def list_identity_links(
        self, principal_id: UUID, catalog_id: Optional[str] = None
    ):
        """Lists all identity links for a principal (platform-global)."""
        return await self.storage.list_identity_links(principal_id)

    async def delete_identity_link(
        self, provider: str, subject_id: str, catalog_id: Optional[str] = None
    ) -> bool:
        """Deletes an identity link (platform-global)."""
        return await self.storage.delete_identity_link(provider, subject_id)

    async def run_maintenance(self, catalog_id: Optional[str] = None) -> dict:
        """Runs maintenance (pruning) for the specified catalog/schema."""
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.run_maintenance(schema=schema)

    # --- Middleware Helpers ---

    def extract_token_from_request(self, request: Any) -> Optional[str]:
        """
        Extracts Bearer Token from request headers (OAuth2 compliant).
        """
        auth_header = getattr(request, "headers", {}).get("Authorization")
        if auth_header and auth_header.lower().startswith("bearer "):
            token = auth_header[7:].strip()
            return token

        return None

    @staticmethod
    def _normalize_authenticated_roles(raw: Any) -> List[str]:
        """Normalise the raw ``roles`` field from a JWT payload or Principal
        into a non-empty ``List[str]`` with the USER default applied.

        Single source of truth for the "an authenticated principal with zero
        declared roles still gets ``DefaultRole.USER``" rule. Without this
        default, an HS256 token signed with ``roles=[]`` (or a Principal
        whose ``.roles`` is ``None``/empty) would surface as ANONYMOUS-
        equivalent at the policy layer — breaking ``/iam/me/*`` self-service
        endpoints that any authenticated user should reach.

        Accepts:
          - ``None``                  → ``["user"]``
          - missing / empty list      → ``["user"]``
          - a single role as ``str``  → ``[role]``
          - a list of role names      → returned verbatim

        Pinned by tests in
        ``tests/dynastore/modules/iam/unit/test_authenticate_default_user_role.py``.
        """
        if isinstance(raw, str):
            return [raw]
        roles = list(raw or [])
        if not roles:
            roles = [DefaultRole.USER.value]
        return roles

    async def _expand_role_hierarchy_dual_scope(
        self, roles: List[str], schema: Optional[str],
    ) -> List[str]:
        """Expand a role list against BOTH the platform (``iam``) and the
        tenant catalog schema's ``role_hierarchy`` tables, returning the
        union.

        Platform-only roles (e.g. ``sysadmin``) live in ``iam.role_hierarchy``;
        catalog-scope roles (admin/editor/...) live in
        ``{catalog_schema}.role_hierarchy``. A principal may carry both at
        once (D1: scope intrinsic to role, D6: one grants table per scope),
        so we MUST union the two expansions to avoid silently dropping a
        parent chain on a catalog request.
        """
        platform_expanded = await self.storage.get_role_hierarchy(
            roles, schema="iam"
        )
        if schema and schema != "iam":
            tenant_expanded = await self.storage.get_role_hierarchy(
                roles, schema=schema
            )
        else:
            tenant_expanded = []
        return list(set(platform_expanded) | set(tenant_expanded))

    async def authenticate_and_get_role(
        self, request: Any
    ) -> Tuple[List[str], Optional[Principal]]:
        """
        Centrally authenticates a request and resolves the effective roles.
        Adapts between Legacy API Keys and V2 Identity Links.
        """
        token = self.extract_token_from_request(request)
        if not token:
            return [DefaultRole.ANONYMOUS.value], None

        catalog_id = getattr(getattr(request, "state", {}), "catalog_id", None)
        schema = await self._resolve_schema(catalog_id)

        # Authenticate via registered Identity Providers (OAuth2 / OIDC)
        identity_providers = self.get_identity_providers()
        logger.debug("Checking %d identity providers", len(identity_providers))
        for provider in identity_providers:
            provider_id = provider.get_provider_id()
            try:
                logger.debug("Trying provider %s", provider_id)
                identity = await provider.validate_token(token)
                if identity:
                    logger.debug("Identity found via provider %s", provider_id)
                    # Resolve identity to Principal with contextual roles
                    principal = await self.authenticate_and_get_principal(
                        identity=identity,
                        target_schema=catalog_id or "_system_",
                        auto_register=True,
                    )
                    if principal:
                        roles = self._normalize_authenticated_roles(principal.roles)
                        effective_roles = await self._expand_role_hierarchy_dual_scope(
                            roles, schema,
                        )
                        return effective_roles, principal
                    else:
                        logger.debug(
                            "Principal not found for identity via provider %s", provider_id
                        )
            except Exception as e:
                logger.error(
                    f"Provider {provider_id} failed to validate token: {e}",
                    exc_info=True,
                )

        # Fallback: try internal HS256 token (test fixtures, internal services)
        try:
            secrets_list = await self.get_jwt_secrets_for_verification()
            payload: Optional[dict] = None
            for secret in secrets_list:
                try:
                    payload = jwt.decode(token, secret, algorithms=["HS256"])
                    break
                except jwt.InvalidTokenError:
                    continue
            if payload:
                from dynastore.models.auth import Principal
                roles = self._normalize_authenticated_roles(payload.get("roles"))
                effective_roles = await self._expand_role_hierarchy_dual_scope(
                    roles, schema,
                )
                principal = Principal(
                    subject_id=payload.get("sub"),
                    provider="internal",
                    roles=roles,
                )
                return effective_roles, principal
        except Exception as e:
            logger.debug("Internal HS256 fallback failed: %s", e)

        # No valid identity found
        return [DefaultRole.ANONYMOUS.value], None
