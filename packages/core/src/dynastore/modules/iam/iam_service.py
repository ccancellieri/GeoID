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
from typing import List, Optional, Tuple, Any, Dict, AsyncGenerator
from datetime import datetime, timezone
from uuid import UUID, uuid4

from dynastore.models.protocols.authorization import IamRolesConfig

from .models import (
    Principal,
    Role,
)
from .iam_storage import AbstractIamStorage
from .postgres_iam_storage import PostgresIamStorage
from .interfaces import IdentityProviderProtocol
from .exceptions import (
    InvalidAuthTokenError,
)
from . import oidc_role_sync
from .oidc_role_sync_config import OidcRoleSyncConfig
from .jwt_attr_config import JwtAttributeClaimsConfig, _resolve_claim_value
from dynastore.tools.ttl_gate import TTLGate

from dynastore.modules import get_protocol
from dynastore.models.protocols import (
    PropertiesProtocol,
    CatalogsProtocol,
)
from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
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
        role_config: Optional[IamRolesConfig] = None,
    ):
        # In V2, we prefer PostgresIamStorage which implements both interfaces.
        # Typed as Any because PostgresIamStorage has methods not declared in either
        # AbstractIamStorage or AuthorizationStorageProtocol (e.g. get_identity_roles).
        self.storage: Any = storage or PostgresIamStorage(app_state=app_state)
        self.policy_service = policy_service
        self.app_state = app_state
        self._jwt_secret: Optional[str] = None
        # Same role landscape configuration used by the seeding flow. When
        # ``IamModule`` is mounted, lifespan loads the PluginConfig from the
        # DB and passes it in here so a runtime PATCH of the role landscape
        # takes effect on the next request without redeploy.
        self._role_config = role_config or IamRolesConfig()
        # Bounded per-principal throttle + serialization lock for OIDC
        # reconciliation. TTL is updated lazily from PluginConfig on first
        # use (see ``_get_oidc_sync_gate``).
        self._oidc_sync_gate: Optional[TTLGate[UUID]] = None
        self._oidc_sync_gate_ttl: float = -1.0

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
        self,
        catalog_id: Optional[str] = None,
        conn: Optional[Any] = None,
        *,
        strict: bool = False,
    ) -> str:
        """
        Determines the database schema.
        If catalog_id is provided, resolves the physical tenant schema.
        Otherwise, defaults to the global 'iam' schema.

        ``strict`` (keyword-only, default ``False``) governs what happens
        when a *catalog* schema cannot be resolved. The default keeps the
        historical, deliberately-defensive fallback to the platform ``iam``
        schema — correct for read/middleware paths, where an unresolvable
        tenant simply yields no catalog-scoped policies. WRITE paths
        (granting/revoking a catalog role) MUST pass ``strict=True``:
        silently retargeting a tenant write to ``iam`` persists the grant
        where the catalog never reads it back, while the endpoint still
        returns a misleading 204 success (the #1698 failure mode). With
        ``strict=True`` the resolution failure is raised so the caller
        surfaces a real error instead of writing nowhere.
        """
        if not catalog_id or catalog_id == "_system_":
            return "iam"

        catalogs = get_protocol(CatalogsProtocol)
        if not catalogs:
            if strict:
                raise ValueError(
                    f"Cannot resolve tenant schema for catalog '{catalog_id}': "
                    "CatalogsProtocol is unavailable."
                )
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
        except Exception as e:
            if strict:
                # Surface the real cause to the write caller instead of
                # silently writing the grant into 'iam'.
                raise
            logger.warning(
                f"Error resolving schema for '{catalog_id}': {e}. Falling back to 'iam'."
            )
            return "iam"

        if res:
            # Validate the DB-sourced physical schema as a safe SQL identifier
            # before it reaches identifier-interpolating queries (e.g. the
            # f-string schema interpolation in build_search_principals_query).
            # Defense-in-depth chokepoint covering every consumer of the
            # resolved schema; mirrors PolicyService._resolve_schema.
            from dynastore.modules.iam.policies import _validate_schema_name

            return _validate_schema_name(res)

        if strict:
            raise ValueError(
                f"Cannot resolve tenant schema for catalog '{catalog_id}': "
                "no physical schema found (catalog missing or not provisioned)."
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

        # 5. Resolve the real platform principal_id from identity_links so
        #    the returned Principal carries a stable identifier — without
        #    this, Pydantic's default_factory=uuid4 produces a fresh
        #    random UUID on every call, breaking any caller that uses
        #    ``principal.id`` to write back to the DB (e.g. the OIDC role
        #    sync reconciler grants would land on orphan principal_ids).
        #    If no principal row exists for this identity, fall through
        #    to None so the caller can auto-register a fresh one with a
        #    properly persisted id.
        existing = await self.storage.get_principal_by_identity(
            provider=provider, subject_id=subject_id, conn=conn
        )
        if existing is None:
            return None

        # 6. Build runtime Principal — re-use the persisted id.
        return Principal(
            id=existing.id,
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
        principal = await self.get_effective_permissions(
            identity=identity, catalog_id=target_schema
        )

        if principal:
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

            # Keycloak-wins reconciliation. If anything changed, re-resolve
            # so the returned Principal reflects the new grants.
            if await self._reconcile_oidc_roles(principal, identity):
                refreshed = await self.get_effective_permissions(
                    identity=identity, catalog_id=target_schema
                )
                if refreshed:
                    principal = refreshed

            return principal

        # Auto-registration (if enabled)
        if auto_register:
            return await self._auto_register_principal(identity, target_schema)

        return None

    async def _get_roles_config(self) -> IamRolesConfig:
        """Fetch IamRolesConfig via PlatformConfigsProtocol; falls back to
        defaults when the protocol or row is unavailable so authentication
        never blocks on missing config. A runtime PATCH of the config takes
        effect on the next request without restart.
        """
        configs = get_protocol(PlatformConfigsProtocol)
        if configs is None:
            return self._role_config
        try:
            cfg = await configs.get_config(IamRolesConfig)
            if isinstance(cfg, IamRolesConfig):
                return cfg
            return IamRolesConfig.model_validate(cfg)
        except Exception:
            logger.debug("IamRolesConfig unavailable; using in-memory fallback", exc_info=True)
            return self._role_config

    async def _get_oidc_sync_config(self) -> OidcRoleSyncConfig:
        """Fetch OidcRoleSyncConfig via PlatformConfigsProtocol; falls
        back to defaults (``reconcile_enabled=False``) when the protocol
        or row is unavailable so a missing/unwritten config never blocks auth.
        """
        configs = get_protocol(PlatformConfigsProtocol)
        if configs is None:
            return OidcRoleSyncConfig()
        try:
            cfg = await configs.get_config(OidcRoleSyncConfig)
            if isinstance(cfg, OidcRoleSyncConfig):
                return cfg
            return OidcRoleSyncConfig.model_validate(cfg)
        except Exception:
            logger.debug("OidcRoleSyncConfig unavailable; using defaults", exc_info=True)
            return OidcRoleSyncConfig()

    async def _get_jwt_attr_config(self) -> JwtAttributeClaimsConfig:
        """Fetch JwtAttributeClaimsConfig via PlatformConfigsProtocol; falls
        back to an empty config (no-op enrichment) when the protocol or row
        is unavailable so a missing config never blocks auth.
        """
        configs = get_protocol(PlatformConfigsProtocol)
        if configs is None:
            return JwtAttributeClaimsConfig()
        try:
            cfg = await configs.get_config(JwtAttributeClaimsConfig)
            if isinstance(cfg, JwtAttributeClaimsConfig):
                return cfg
            return JwtAttributeClaimsConfig.model_validate(cfg)
        except Exception:
            logger.debug("JwtAttributeClaimsConfig unavailable; using defaults", exc_info=True)
            return JwtAttributeClaimsConfig()

    async def _merge_jwt_attributes(
        self,
        principal: "Principal",
        identity: Dict[str, Any],
    ) -> None:
        """Enrich ``principal.attributes`` with validated JWT claim values.

        Iterates ``JwtAttributeClaimsConfig.claim_map`` and, for each mapped
        key not already present in ``principal.attributes``, extracts the
        claim value and sets it.  DB-stored attributes win on collision
        (only absent keys are filled).  Claims that are missing, None, or
        empty are skipped silently.
        """
        jwt_cfg = await self._get_jwt_attr_config()
        if not jwt_cfg.claim_map:
            return
        raw_claims = identity.get("raw_claims") or {}
        if not isinstance(raw_claims, dict):
            return
        issuer = raw_claims.get("iss")
        if not oidc_role_sync.is_issuer_allowed(issuer, jwt_cfg.issuer_allowlist):
            return
        for attr_key, claim_path in jwt_cfg.claim_map.items():
            if attr_key in principal.attributes:
                # DB value wins — never overwrite.
                continue
            value = _resolve_claim_value(raw_claims, claim_path)
            if value is None:
                continue
            principal.attributes[attr_key] = value

    def _get_oidc_sync_gate(self, ttl_seconds: float) -> TTLGate[UUID]:
        """Lazily construct (or rebuild on TTL change) the OIDC reconciler
        throttle. Bounded LRU at 4096 keys — large enough for any realistic
        single-pod authenticated-principal set, small enough to bound RAM."""
        if (
            self._oidc_sync_gate is None
            or self._oidc_sync_gate_ttl != ttl_seconds
        ):
            self._oidc_sync_gate = TTLGate(maxsize=4096, ttl_seconds=ttl_seconds)
            self._oidc_sync_gate_ttl = ttl_seconds
        return self._oidc_sync_gate

    async def _reconcile_oidc_roles(
        self, principal: Principal, identity: Dict[str, Any]
    ) -> bool:
        """Reconcile mapped OIDC roles into platform-scope grants.

        Returns True iff any grant was added or revoked. The caller
        should re-fetch effective permissions in that case.
        """
        cfg = await self._get_oidc_sync_config()
        if not cfg.reconcile_enabled or not cfg.role_mapping:
            return False

        principal_id = principal.id
        if not isinstance(principal_id, UUID):
            try:
                principal_id = UUID(str(principal_id))
            except Exception:
                return False

        gate = self._get_oidc_sync_gate(cfg.ttl_seconds)
        async with gate.acquire(principal_id) as gate_handle:
            if not gate_handle.should_run:
                return False

            raw_claims = identity.get("raw_claims") or {}
            issuer = raw_claims.get("iss") if isinstance(raw_claims, dict) else None
            if not oidc_role_sync.is_issuer_allowed(issuer, cfg.issuer_whitelist):
                logger.warning(
                    "OIDC role sync skipped: issuer %r not in whitelist for principal %s",
                    issuer, principal_id,
                )
                gate_handle.mark()
                return False

            oidc_roles = identity.get("roles") or []
            mapped_internal = set(cfg.role_mapping.values())
            try:
                current_platform_roles = await self.storage.list_platform_roles(
                    principal_id
                )
            except Exception:
                logger.debug(
                    "list_platform_roles failed during OIDC sync; skipping",
                    exc_info=True,
                )
                # Don't mark — let the next request retry immediately.
                return False

            # Restrict the diff to roles the mapping owns; everything else
            # (catalog-scope, viewer, manually-granted unrelated roles) is
            # left untouched by design.
            scoped_current = [r for r in current_platform_roles if r in mapped_internal]
            actions = oidc_role_sync.diff(
                oidc_roles=oidc_roles,
                current_internal_roles=scoped_current,
                role_mapping=cfg.role_mapping,
            )

            if not actions:
                gate_handle.mark()
                return False

            granted: List[str] = []
            revoked: List[str] = []
            for act in actions:
                try:
                    if act.action == "grant":
                        await self.storage.grant_platform_role(
                            principal_id=principal_id, role_name=act.role_name
                        )
                        granted.append(act.role_name)
                    else:
                        await self.storage.revoke_platform_role(
                            principal_id=principal_id, role_name=act.role_name
                        )
                        revoked.append(act.role_name)
                except Exception:
                    logger.exception(
                        "OIDC role sync %s failed for principal=%s role=%s",
                        act.action, principal_id, act.role_name,
                    )

            if granted or revoked:
                try:
                    await self.storage.log_audit_event(
                        event_type="oidc_role_sync",
                        principal_id=str(principal_id),
                        detail={
                            "source": "oidc_sync",
                            "issuer": issuer,
                            "granted": granted,
                            "revoked": revoked,
                            "oidc_roles": list(oidc_roles),
                        },
                    )
                except Exception:
                    # Sysadmin add/remove without an audit row is the
                    # load-bearing failure mode — escalate so it surfaces
                    # in log alerts, not just a debug line lost in noise.
                    logger.warning(
                        "audit write for oidc_role_sync failed (principal=%s "
                        "granted=%s revoked=%s)",
                        principal_id, granted, revoked,
                        exc_info=True,
                    )
                logger.info(
                    "OIDC role sync principal=%s granted=%s revoked=%s",
                    principal_id, granted, revoked,
                )

            gate_handle.mark()
            return bool(granted or revoked)

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

        # Use realm_roles from OIDC token if present, otherwise default to
        # the configured user role name. Known-role filtering reads the
        # active ``IamRolesConfig`` so operator-renamed deployments AND
        # operator-added roles (e.g. a custom "reviewer" seeded via PATCH)
        # are recognised against incoming JWT claims. Plus any role row
        # present in the DB (created via /admin/roles POST) — this widens
        # ``known_roles`` past the static seed so dynamically-created roles
        # in JWT realm claims are not silently filtered.
        cfg = await self._get_roles_config()
        known_roles = set(cfg.role_names)
        try:
            db_roles = await self.storage.list_roles(schema="iam")
            known_roles.update(r.name for r in db_roles)
        except Exception:
            logger.debug("list_roles unavailable; using config-only known_roles", exc_info=True)
        realm_roles = [
            r for r in identity.get("realm_roles", [])
            if r in known_roles
        ]
        base_roles = realm_roles if realm_roles else [cfg.default_user_role_name]

        # Overlay mapped OIDC roles (e.g. geoid.sysadmin -> sysadmin) so a
        # first-time login lands with the correct grant on the way in.
        sync_cfg = await self._get_oidc_sync_config()
        raw_claims = identity.get("raw_claims") or {}
        issuer = raw_claims.get("iss") if isinstance(raw_claims, dict) else None
        assigned_roles = oidc_role_sync.initial_role_overlay(
            oidc_roles=identity.get("roles") or [],
            base_roles=base_roles,
            config=sync_cfg,
            issuer=issuer,
        )

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
        saved = await self.storage.create_principal(new_principal)
        actual_id = saved.id if saved else new_principal_id
        await self.storage.create_identity_link(
            principal_id=actual_id,
            provider=provider,
            subject_id=subject_id,
        )

        # Grants land at the right scope: catalog-scoped when we have
        # a real catalog, platform-scoped for `_system_` / no catalog.
        catalog_schema = await self._resolve_schema(catalog_id)
        await self._apply_role_grants(
            principal_id=actual_id,
            roles=assigned_roles,
            catalog_schema=catalog_schema,
        )

        logger.info(
            f"Auto-registered principal {actual_id} for {provider}:{subject_id}"
        )

        return Principal(
            id=actual_id,
            provider=provider,
            subject_id=subject_id,
            display_name=email,
            roles=assigned_roles,
            is_active=True,
            custom_policies=[],
            attributes=attributes,
        )

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
            roles=[self._role_config.default_user_role_name],
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
        existing = await self.storage.get_role(role.name, schema=schema)
        if existing is not None:
            raise ValueError(
                f"Role '{role.name}' already exists. Use PUT /admin/roles/{role.name} to update."
            )
        return await self.storage.create_role(role, schema=schema)

    async def list_roles(self, catalog_id: Optional[str] = None) -> List[Role]:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.list_roles(schema=schema)

    async def update_role(
        self, role: Role, catalog_id: Optional[str] = None
    ) -> Optional[Role]:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.update_role(role, schema=schema)

    async def bind_policy_to_role(
        self,
        role_name: str,
        policy_entry: Dict[str, Any],
        catalog_id: Optional[str] = None,
    ) -> None:
        """Atomically append *policy_entry* to the role, deduping by ``id``."""
        schema = await self._resolve_schema(catalog_id)
        await self.storage.bind_policy_to_role(role_name, policy_entry, schema=schema)

    async def unbind_policy_from_role(
        self,
        role_name: str,
        policy_id: str,
        catalog_id: Optional[str] = None,
    ) -> None:
        """Atomically remove the entry with ``id == policy_id`` from the role."""
        schema = await self._resolve_schema(catalog_id)
        await self.storage.unbind_policy_from_role(role_name, policy_id, schema=schema)

    async def delete_role(
        self, name: str, cascade: bool = False, catalog_id: Optional[str] = None
    ) -> bool:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.delete_role(name, cascade=cascade, schema=schema)

    async def add_role_hierarchy(
        self, parent_role: str, child_role: str, catalog_id: Optional[str] = None
    ) -> None:
        if parent_role == child_role:
            raise ValueError(
                f"Adding edge {parent_role!r}->{child_role!r} would create a self-loop "
                "in the role hierarchy."
            )
        descendants = await self.get_role_hierarchy(child_role, catalog_id=catalog_id)
        if parent_role in descendants:
            raise ValueError(
                f"Adding edge {parent_role!r}->{child_role!r} would create a cycle: "
                f"{parent_role!r} is already reachable from {child_role!r}."
            )
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

    async def list_role_hierarchy(
        self, catalog_id: Optional[str] = None
    ) -> List[tuple[str, str]]:
        """Return all (parent_role, child_role) edges at this scope."""
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.list_role_hierarchy_edges(schema=schema)

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

    # --- Token revocation (phantom-token denylist, #1343) ---

    async def revoke_token(self, jti: str) -> None:
        """Deny a single token by its ``jti`` until the denylist TTL elapses.

        Immediate revocation ahead of natural token expiry; enforced by the
        denylist check in :meth:`authenticate_and_get_role` when
        ``denylist_enabled``. No-op unless a distributed backend is present.
        """
        from dynastore.modules.iam import phantom_token
        from dynastore.modules.iam.scale_config import get_iam_scale_config

        cfg = await get_iam_scale_config()
        await phantom_token.deny(
            str(jti), ttl_seconds=int(getattr(cfg, "denylist_ttl_seconds", 300))
        )

    async def revoke_principal(self, subject_id: str) -> None:
        """Deny every currently-issued token for a subject until the TTL.

        Uses the ``sub:`` denylist namespace matched by the auth path. The
        subject re-authenticates fine once the denylist entry expires (or
        sooner via a new token whose ``jti`` is not denied).
        """
        from dynastore.modules.iam import phantom_token
        from dynastore.modules.iam.scale_config import get_iam_scale_config

        cfg = await get_iam_scale_config()
        await phantom_token.deny(
            "sub:" + str(subject_id),
            ttl_seconds=int(getattr(cfg, "denylist_ttl_seconds", 300)),
        )

    # --- Token revocation: admin-facing inspect / remove (#1343) ---

    async def deny_subject(
        self,
        subject: str,
        *,
        ttl_seconds: Optional[int] = None,
        reason: Optional[str] = None,
    ) -> int:
        """Add an admin-authored denylist entry. Returns the effective TTL.

        ``subject`` is the storage-form token id (a raw ``jti`` for token kill
        or ``sub:<subject-id>`` for principal-wide kill). The route layer
        translates the wire ``jti:<id>`` / ``principal:<id>`` form into one of
        these before calling. The requested TTL is clamped to the
        ``IamScaleConfig.denylist_ttl_seconds`` ceiling: an operator cannot
        keep an entry alive longer than the configured maximum (otherwise
        Valkey would accumulate dead rows for stale tokens).

        Raises :class:`phantom_token.DenylistBackendUnavailable` when L2 is
        absent so the REST surface returns 503 ("can't confirm the kill
        landed") rather than silently no-op'ing.
        """
        from dynastore.modules.iam import phantom_token
        from dynastore.modules.iam.scale_config import get_iam_scale_config

        cfg = await get_iam_scale_config()
        ceiling = int(getattr(cfg, "denylist_ttl_seconds", 300))
        if ttl_seconds is None or int(ttl_seconds) <= 0:
            effective = ceiling
        else:
            effective = min(int(ttl_seconds), ceiling)
        if not phantom_token.denylist_backend_available():
            raise phantom_token.DenylistBackendUnavailable(
                "denylist backend unavailable"
            )
        await phantom_token.deny(subject, ttl_seconds=effective, reason=reason)
        return effective

    async def undeny_subject(self, subject: str) -> bool:
        """Remove a denylist entry. Returns True iff an entry existed."""
        from dynastore.modules.iam import phantom_token

        return await phantom_token.undeny(subject)

    async def list_denylist(
        self, *, prefix: Optional[str] = None, limit: int = 100,
    ) -> list:
        """Return active denylist entries (capped, optionally prefix-filtered)."""
        from dynastore.modules.iam import phantom_token

        return await phantom_token.list_denied(prefix=prefix, limit=limit)

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

    def _normalize_authenticated_roles(self, raw: Any) -> List[str]:
        """Normalise the raw ``roles`` field from a JWT payload or Principal
        into the effective role list, always including the baseline role.

        Single source of truth for the "every authenticated principal carries
        the baseline self-service role" rule. The baseline
        (``self._role_config.default_user_role_name``, which by default
        collapses onto ``anonymous_role_name`` == ``"unauthenticated"``) is the
        role the platform's public + self-info policies bind to:
        ``self_service_authorization_api`` (``/iam/me``, ``/iam/me/*``,
        ``/auth/userinfo``), ``auth_extension_public`` (``/auth/*``),
        ``public_access`` (``/health`` …) and ``web_public_access``
        (``/web/*``).

        It MUST be appended to the principal's declared realm roles, not just
        substituted in when the declared list is empty. A role-carrying user
        (e.g. ``admin``/``user``/``viewer``) otherwise resolves to its realm
        roles ONLY — dropping the baseline the moment it authenticates — and is
        then locked out of its own profile endpoints with a deny-by-default
        403, which surfaces as a grayed/stuck UI right after login. Declared
        realm roles still grant their elevated access on top of the baseline;
        appending the baseline only restores the public/self-info floor and
        never elevates (deny-precedence still applies).

        Anonymous (token-less) requests get ``[anonymous_role_name]`` upstream
        in ``authenticate_and_get_role``; authenticated-vs-anonymous is decided
        by the presence of a ``Principal`` object, never by inspecting this
        role list — so carrying the baseline here cannot misclassify a
        logged-in user as anonymous.

        Accepts:
          - ``None`` / missing / empty list → ``[default_user_role_name]``
          - a single role as ``str``        → ``[role, default_user_role_name]``
          - a list of role names            → ``[*roles, default_user_role_name]``

        (the baseline is de-duplicated when the declared roles already include
        it). Pinned by tests in
        ``tests/dynastore/modules/iam/unit/test_authenticate_default_user_role.py``.
        """
        if isinstance(raw, str):
            roles = [raw]
        else:
            roles = list(raw or [])
        baseline = self._role_config.default_user_role_name
        if baseline not in roles:
            roles.append(baseline)
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

    async def _resolve_effective_identity(
        self, identity: Dict[str, Any], catalog_id: Optional[str], schema: str
    ) -> "Optional[Tuple[List[str], Principal]]":
        """Authoritative (DB) resolution of a validated identity.

        Returns ``(effective_roles, principal)`` or ``None`` when the identity
        maps to no principal (the caller then tries the next provider / falls
        through to 401). Extracted so the phantom-token cache (#1343) can wrap
        it without duplicating the resolution.
        """
        principal = await self.authenticate_and_get_principal(
            identity=identity,
            target_schema=catalog_id or "_system_",
            auto_register=True,
        )
        if not principal:
            return None
        roles = self._normalize_authenticated_roles(principal.roles)
        effective_roles = await self._expand_role_hierarchy_dual_scope(roles, schema)
        return effective_roles, principal

    async def _resolve_identity_maybe_cached(
        self,
        identity: Dict[str, Any],
        catalog_id: Optional[str],
        schema: str,
        scale_cfg: Any,
    ) -> "Optional[Tuple[List[str], Principal]]":
        """Resolve a validated identity, via the phantom-token tier when active.

        When ``phantom_token_active`` (the #1343 flag is on AND a distributed
        Valkey backend is present), the ``(roles, principal)`` resolution is
        served from the shared, version-keyed L1+L2 tier instead of re-querying
        Postgres on every request; otherwise the authoritative DB path runs.
        Returns ``None`` when the identity maps to no principal.
        """
        from dynastore.modules.iam import phantom_token

        subject_id = str(identity.get("sub") or "")
        provider_name = str(identity.get("provider") or "")
        if not (subject_id and phantom_token.phantom_token_active(scale_cfg)):
            return await self._resolve_effective_identity(identity, catalog_id, schema)

        async def _resolver() -> Optional[Dict[str, Any]]:
            res = await self._resolve_effective_identity(identity, catalog_id, schema)
            if res is None:
                return None
            eff, principal = res
            # Cache a JSON-safe projection; rehydrated below on a hit.
            return {"roles": eff, "principal": principal.model_dump(mode="json")}

        cached = await phantom_token.resolve_bindings_cached(
            provider=provider_name,
            subject_id=subject_id,
            schema=schema,
            resolver=_resolver,
            cfg=scale_cfg,
        )
        if cached is None:
            return None
        principal = Principal.model_validate(cached["principal"])
        # Re-apply the account-state gates on every cache hit. A positive entry
        # was cached while the principal was active and unexpired; ``valid_until``
        # expiry is caught here immediately, and an ``is_active`` flip is honoured
        # via the binding-version bump on ``update_principal`` (+ the TTL backstop)
        # — never serve a deactivated / expired principal from a warm cache.
        if not principal.is_active:
            logger.warning("Principal %s is inactive (cache hit)", principal.id)
            return None
        if principal.valid_until and principal.valid_until < datetime.now(timezone.utc):
            logger.warning("Principal %s has expired (cache hit)", principal.id)
            return None
        return list(cached.get("roles") or []), principal

    async def authenticate_and_get_role(
        self, request: Any
    ) -> Tuple[List[str], Optional[Principal]]:
        """
        Centrally authenticates a request and resolves the effective roles.
        Adapts between Legacy API Keys and V2 Identity Links.
        """
        token = self.extract_token_from_request(request)
        if not token:
            roles_cfg = await self._get_roles_config()
            return [roles_cfg.anonymous_role_name], None

        catalog_id = getattr(getattr(request, "state", {}), "catalog_id", None)
        schema = await self._resolve_schema(catalog_id)

         # Authenticate via registered Identity Providers (OAuth2 / OIDC)
         identity_providers = self.get_identity_providers()
         logger.info("authenticate_token: Checking %d identity providers (providers=%s)", len(identity_providers), [p.get_provider_id() if hasattr(p, 'get_provider_id') else str(p) for p in identity_providers])
         logger.debug("Checking %d identity providers", len(identity_providers))
        scale_cfg: Any = None
        for provider in identity_providers:
            provider_id = provider.get_provider_id()
            try:
                logger.debug("Trying provider %s", provider_id)
                identity = await provider.validate_token(token)
                if not identity:
                    continue
                logger.debug("Identity found via provider %s", provider_id)
                # Resolve the scale config once (drives the #1343 hot path).
                if scale_cfg is None:
                    from dynastore.modules.iam.scale_config import (
                        get_iam_scale_config,
                    )

                    scale_cfg = await get_iam_scale_config()
                # Immediate revocation: reject a validated token whose jti / sub
                # is on the Valkey denylist (opt-in, #1343).
                if getattr(scale_cfg, "denylist_enabled", False):
                    from dynastore.modules.iam import phantom_token

                    subj = str(identity.get("sub") or "")
                    jti = identity.get("jti")
                    if (jti is not None and await phantom_token.is_denied(str(jti))) or (
                        subj and await phantom_token.is_denied("sub:" + subj)
                    ):
                        raise InvalidAuthTokenError(
                            "Authorization token has been revoked"
                        )
                # Resolve identity to Principal with contextual roles — served
                # from the phantom-token L1+L2 tier when active, else from DB.
                resolved = await self._resolve_identity_maybe_cached(
                    identity, catalog_id, schema, scale_cfg
                )
                if resolved is not None:
                    eff_roles, principal = resolved
                    # JWT-claim merge runs OUTSIDE the cache (claims are
                    # request-scoped; the cached result carries DB attributes only).
                    await self._merge_jwt_attributes(principal, identity)
                    return eff_roles, principal
                logger.debug(
                    "Principal not found for identity via provider %s", provider_id
                )
            except InvalidAuthTokenError:
                # A revoked token (or other explicit auth failure) must surface,
                # not be swallowed as a provider error and degrade to anonymous.
                raise
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

        # A token was present but no provider could validate it and the
        # HS256 fallback yielded no payload. Fail closed with 401 rather
        # than degrading to anonymous — degrading would let invalid-token
        # callers reach handlers gated only by ``role != anonymous`` and
        # bypass sysadmin checks on endpoints like POST /stac/catalogs,
        # GET /admin/principals, GET /logs/system. (issues #415/#416/#417)
        raise InvalidAuthTokenError(
            "Authorization token present but could not be validated"
        )
