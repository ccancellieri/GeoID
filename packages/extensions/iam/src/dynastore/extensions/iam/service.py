#    Copyright 2025 FAO
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

# File: dynastore/extensions/iam/service.py

import logging
import uuid
import re
import secrets
from pydantic import BaseModel, Field, model_validator
from contextlib import asynccontextmanager
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
    Request,
    FastAPI,
    Query,
)
from fastapi.openapi.utils import get_openapi
from typing import List, Optional, Any, Dict, Literal
from datetime import datetime
from uuid import UUID

import os
from fastapi.responses import HTMLResponse
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.web import expose_web_page
from dynastore.models.protocols import WebModuleProtocol
from dynastore.modules.iam.iam_service import IamService
from dynastore.tools.discovery import get_protocol, get_protocols
from dynastore.modules.db_config.tools import normalize_db_url
from dynastore.modules.db_config.query_executor import DbResource

from dynastore.modules.iam.models import (
    Principal,
    Role,
    Policy,
    PolicyBundle,
    TokenExchangeRequest,
    TokenResponse,
    RefreshToken,
    Condition,
)
# TODO: move to StatsProtocol to eliminate cross-module layer violation
from dynastore.modules.stats.storage import (
    get_access_logs,
    get_stats_summary,
    AccessLogPage,
    StatsSummary,
)
from dynastore.modules import get_protocol
from dynastore.models.protocols.authorization import IamRoleConfig
from dynastore.models.protocols.policies import PermissionProtocol
from dynastore.modules.iam.exceptions import (
    ConflictingResourceError,
    PrincipalNotFoundError,
)
from dynastore.extensions.iam.middleware import IamMiddleware
from dynastore.extensions.iam.authorization_api import (
    router as admin_router,
    me_router,
)
logger = logging.getLogger(__name__)


def iam_service_policies():
    """Pure declaration of IAM service policies (admin + self-service auth API).

    Returned to IAM via ``IamExtension.get_policies``. Same pattern as
    every other plugin in the SCOPE matrix — no direct
    ``register_policy`` call. IAM consuming its own contribution is the
    canonical case for this Protocol; treating it the same way as
    other plugins keeps the registration site singular and the seeding
    logic uniform.
    """
    return [
        Policy(
            id="admin_authorization_api",
            description="Allows admin users to manage user roles and permissions",
            actions=["GET", "POST", "PUT", "DELETE", "PATCH"],
            resources=["/admin/users/.*", "/admin/roles/.*", "/admin/policies/.*"],
            effect="ALLOW",
            partition_key="global",
        ),
        Policy(
            id="self_service_authorization_api",
            description="Allows authenticated users to view their own roles and catalog access",
            actions=["GET"],
            resources=["/me", "/me/available-roles", "/me/roles/.*", "/me/catalogs.*"],
            effect="ALLOW",
            partition_key="global",
        ),
    ]


def iam_service_role_bindings(
    sysadmin_role_name=None,
    admin_role_name=None,
    user_role_name=None,
):
    """Pure declaration of role bindings for IAM service policies.

    Role names default from the active ``IamRoleConfig`` (env-driven via
    ``IAM_ROLE_*`` vars) so seed-time bindings track operator-renamed
    deployments without explicit args.
    """
    from dynastore.models.protocols.authorization import IamRoleConfig
    cfg = IamRoleConfig()
    return [
        Role(name=admin_role_name or cfg.admin, policies=["admin_authorization_api"]),
        Role(name=sysadmin_role_name or cfg.sysadmin, policies=["admin_authorization_api"]),
        Role(name=user_role_name or cfg.user, policies=["self_service_authorization_api"]),
    ]


from dynastore.extensions.iam.guards import ensure_privileged_role_assignment


def _build_oauth2_endpoints() -> tuple[str, str]:
    """Resolve absolute Keycloak ``authorize`` / ``token`` URLs from env.

    Resolution order:
    1. If ``IDP_ISSUER_URL`` is set, derive the realm path from it (e.g.
       ``/realms/fao-aip-auth-review``).
    2. If ``IDP_PUBLIC_URL`` is also set and points at a different scheme+
       host than the issuer URL, swap the host so the browser hits the
       public-reachable Keycloak instead of an internal hostname.
    3. If ``IDP_ISSUER_URL`` is unset, fall back to the relative paths
       ``/auth/authorize`` + ``/auth/token`` (development default — Swagger
       UI's Authorize button will be non-functional but the schema still
       renders) and emit a one-shot warning.
    """
    issuer = os.getenv("IDP_ISSUER_URL")
    if not issuer:
        logger.warning(
            "IDP_ISSUER_URL not set; Swagger Authorize button will use "
            "relative URLs and may not work end-to-end."
        )
        return "/auth/authorize", "/auth/token"

    from urllib.parse import urlparse, urlunparse

    issuer = issuer.rstrip("/")
    parsed = urlparse(issuer)
    realm_path = parsed.path  # e.g. ``/realms/fao-aip-auth-review``

    public = os.getenv("IDP_PUBLIC_URL")
    if public:
        public = public.rstrip("/")
        public_parsed = urlparse(public)
        # Only swap when the public URL actually differs in scheme+host;
        # otherwise the issuer URL is already browser-reachable.
        same_origin = (
            public_parsed.scheme == parsed.scheme
            and public_parsed.netloc == parsed.netloc
        )
        if not same_origin:
            base = urlunparse(
                (public_parsed.scheme, public_parsed.netloc, realm_path, "", "", "")
            )
        else:
            base = issuer
    else:
        base = issuer

    return (
        f"{base}/protocol/openid-connect/auth",
        f"{base}/protocol/openid-connect/token",
    )


def build_iam_openapi_schema(app: FastAPI) -> Dict[str, Any]:
    """Build the OpenAPI schema with IAM security schemes + global security.

    Extracted from ``IamExtension.configure_app.custom_openapi`` so unit
    tests can exercise it without booting the IAM middleware stack.
    """
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
        servers=app.servers,
    )

    if "components" not in openapi_schema:
        openapi_schema["components"] = {}
    if "securitySchemes" not in openapi_schema["components"]:
        openapi_schema["components"]["securitySchemes"] = {}

    auth_url, token_url = _build_oauth2_endpoints()

    openapi_schema["components"]["securitySchemes"]["HTTPBearer"] = {
        "type": "http",
        "scheme": "bearer",
        "bearerFormat": "JWT",
        "description": (
            "Paste a JWT access token obtained from your Keycloak login "
            "(web UI or `/auth/web/`)."
        ),
    }
    openapi_schema["components"]["securitySchemes"]["OAuth2AuthorizationCode"] = {
        "type": "oauth2",
        "description": "OIDC Authorization Code flow via Keycloak (use the Authorize button above).",
        "flows": {
            "authorizationCode": {
                "authorizationUrl": auth_url,
                "tokenUrl": token_url,
                "scopes": {
                    "openid": "OpenID Connect",
                    "email": "User email",
                    "profile": "User profile",
                },
            }
        },
    }

    # Top-level default security: every operation inherits a lock icon in
    # Swagger UI. Routes that are intentionally public (e.g. /health,
    # /auth/jwks.json) opt out by setting ``security=[]`` on their
    # decorator. Per OpenAPI 3.0, multiple entries in this list are an
    # OR — the request only needs one of them.
    openapi_schema["security"] = [
        {"HTTPBearer": []},
        {"OAuth2AuthorizationCode": ["openid", "email", "profile"]},
    ]

    # Group every authentication / authorization / admin-user route under a
    # single tag so Swagger UI lists them together instead of scattering them
    # across "auth", "iam", "admin", and untagged sections. Drop the legacy
    # tag-array entries that the rename leaves orphaned.
    authn_authz_tag = {
        "name": "Authentication & Authorization",
        "description": (
            "OIDC login, JWT validation, user/role/policy management. "
            "All routes under /auth, /iam, and /admin/users."
        ),
    }
    legacy_tag_names = {
        "auth",
        "iam",
        "admin",
        "Authentication",
        "Authorization Management",
        "Self-Service Authorization",
        "Identity & Access Governance",
        "IAM Governance",
        "Credentials",
        "Admin",
    }
    existing_tags = openapi_schema.get("tags") or []
    pruned_tags = [
        tag for tag in existing_tags if tag.get("name") not in legacy_tag_names
    ]
    if not any(
        tag.get("name") == authn_authz_tag["name"] for tag in pruned_tags
    ):
        pruned_tags.append(authn_authz_tag)
    openapi_schema["tags"] = pruned_tags

    return openapi_schema


# --- DTOs (Data Transfer Objects) ---


class TokenRefreshRequest(BaseModel):
    """Standardized refresh request."""

    refresh_token: str
    ttl_seconds: int = Field(default=3600, ge=60, le=2592000)


class PolicyCreateRequest(BaseModel):
    """
    Input model for Policy Creation.
    """

    id: str = Field(..., description="Unique slug for the policy.")
    description: Optional[str] = Field(
        None, description="Optional description of the policy."
    )

    actions: List[str] = Field(
        ..., description="List of allowed actions, e.g., ['READ', 'LIST', 'STAC:GET']"
    )
    resources: List[str] = Field(
        default=["*"],
        description="Regex list for resource targeting, e.g., ['catalogs/A/collections/*']",
    )
    effect: Literal["ALLOW", "DENY"] = "ALLOW"

    conditions: Optional[List[Condition]] = None
    partition_key: str = "global"


class PolicyUpdateRequest(BaseModel):
    """Input model for Policy Update."""

    description: Optional[str] = None
    actions: Optional[List[str]] = None
    resources: Optional[List[str]] = None
    effect: Optional[Literal["ALLOW", "DENY"]] = None
    conditions: Optional[List[Condition]] = None


class PrincipalCreateRequest(BaseModel):
    """Input model for Principal Creation."""

    provider: str = Field(
        "local", description="Identity provider (e.g. 'local', 'keycloak')."
    )
    subject_id: str = Field(..., description="Unique subject ID from the provider.")
    roles: List[str] = Field(
        default_factory=list, description="Roles assigned to this principal."
    )
    attributes: Dict[str, Any] = Field(default_factory=dict)
    policy: Optional[PolicyBundle] = None


class PrincipalUpdateRequest(BaseModel):
    """Input model for Principal Update."""

    roles: Optional[List[str]] = None
    attributes: Optional[Dict[str, Any]] = None
    policy: Optional[PolicyBundle] = None


# Guards live in `extensions/iam/guards.py`; see top-of-file imports.
class IamExtension(ExtensionProtocol):
    priority: int = 100
    # Base router for high-level categorization
    router: APIRouter = APIRouter(
        prefix="/iam", tags=["Authentication & Authorization"]
    )

    # Standardized Auth Endpoints (OIDC/OAuth2 compatible)
    auth_router: APIRouter = APIRouter(
        prefix="/auth", tags=["Authentication & Authorization"]
    )

    # Governance Endpoints (Principals, Roles, Policies)
    gov_router: APIRouter = APIRouter(
        prefix="/governance", tags=["Authentication & Authorization"]
    )

    # Stats Endpoints (kept from removed credentials router)
    stats_router: APIRouter = APIRouter(
        prefix="/credentials", tags=["Authentication & Authorization"]
    )

    def get_web_pages(self):
        from dynastore.extensions.tools.web_collect import collect_web_pages
        return collect_web_pages(self)

    # PolicyContributor: declare IAM's own service policies; the lifespan
    # contributor loop forwards them to PermissionProtocol — same path as
    # every other plugin in the SCOPE matrix.
    def get_policies(self):
        return iam_service_policies()

    def get_role_bindings(self):
        return iam_service_role_bindings()

    def __init__(self, app: Optional[FastAPI] = None):
        super().__init__()
        self.app = app
        self._iam_manager: Optional[IamService] = None
        self._policy_service: Optional[PermissionProtocol] = None
        self._engine: Optional[DbResource] = None

        self._register_routes()

        # Include divided routers into the main router
        self.router.include_router(self.auth_router)
        self.router.include_router(self.gov_router)
        self.router.include_router(self.stats_router)

        self.router.include_router(admin_router)
        self.router.include_router(me_router)

    def _register_routes(self):
        # Public / Auth
        self.auth_router.add_api_route(
            "/jwks.json", self.get_jwks, methods=["GET"]
        )

        # Governance
        self.gov_router.add_api_route(
            "/policies", self.create_access_policy, methods=["POST"], response_model=Policy,
        )
        self.gov_router.add_api_route(
            "/policies/{policy_id}", self.update_access_policy, methods=["PUT"], response_model=Policy,
        )
        self.gov_router.add_api_route(
            "/policies", self.search_access_policies, methods=["GET"], response_model=List[Policy],
        )
        self.gov_router.add_api_route(
            "/policies/{policy_id}", self.delete_access_policy, methods=["DELETE"], status_code=status.HTTP_204_NO_CONTENT,
        )
        self.gov_router.add_api_route(
            "/roles", self.list_roles, methods=["GET"], response_model=List[Role],
        )
        self.gov_router.add_api_route(
            "/roles", self.create_role, methods=["POST"], response_model=Role,
        )
        self.gov_router.add_api_route(
            "/roles/{name}", self.update_role, methods=["PUT"], response_model=Role,
        )
        self.gov_router.add_api_route(
            "/roles/{name}", self.delete_role, methods=["DELETE"], status_code=status.HTTP_204_NO_CONTENT,
        )
        self.gov_router.add_api_route(
            "/hierarchies", self.add_role_hierarchy, methods=["POST"], status_code=status.HTTP_204_NO_CONTENT,
        )
        self.gov_router.add_api_route(
            "/hierarchies", self.remove_role_hierarchy, methods=["DELETE"], status_code=status.HTTP_204_NO_CONTENT,
        )
        self.gov_router.add_api_route(
            "/hierarchies/{role_name}", self.get_role_hierarchy, methods=["GET"], response_model=List[str],
        )
        self.gov_router.add_api_route(
            "/principals", self.create_principal, methods=["POST"], response_model=Principal,
        )
        self.gov_router.add_api_route(
            "/principals/{principal_id}", self.update_principal, methods=["PUT"], response_model=Principal,
        )
        self.gov_router.add_api_route(
            "/principals", self.search_principals, methods=["GET"], response_model=List[Principal],
        )
        self.gov_router.add_api_route(
            "/principals/{principal_id}", self.delete_principal, methods=["DELETE"], status_code=status.HTTP_204_NO_CONTENT,
        )

        # Stats
        self.stats_router.add_api_route(
            "/stats/summary", self.get_system_stats_summary, methods=["GET"], response_model=StatsSummary,
        )
        self.stats_router.add_api_route(
            "/stats/logs", self.get_system_access_logs, methods=["GET"], response_model=AccessLogPage,
        )


    def configure_app(self, app: FastAPI):
        from dynastore.extensions.iam.tenant_scope_middleware import (
            TenantScopeMiddleware,
        )

        # Two-layer authorization (Starlette LIFO order):
        #
        # 1. ``IamMiddleware`` (outer / runs first) — populates
        #    ``request.state.principal`` + ``principal_role``, evaluates
        #    declarative ``Policy``/``Role`` rules registered via
        #    ``PermissionProtocol``. Catches the broad cases.
        # 2. ``TenantScopeMiddleware`` (inner / runs after IAM) — enforces
        #    catalog-membership tenant isolation for routes listed in
        #    ``tenant_scope_registry.TENANT_SCOPED_ROUTES``. Pure
        #    declarative-rule consumer (no hardcoded role checks); the rule
        #    table tells it which paths to gate. Acts as a backstop until
        #    a per-policy ``catalog_membership`` condition + DENY-first
        #    evaluator semantics make policy-only enforcement viable.
        app.add_middleware(TenantScopeMiddleware)
        app.add_middleware(IamMiddleware)

        def custom_openapi():
            if app.openapi_schema:
                return app.openapi_schema

            app.openapi_schema = build_iam_openapi_schema(app)
            return app.openapi_schema

        app.openapi = custom_openapi

        # Web pages are discovered by WebModule via the WebPageContributor
        # capability protocol (see get_web_pages below).

        # Sub-routers are mounted to self.router in __init__
        # The central extension loader will handle app.include_router(self.router)

    @expose_web_page(
        page_id="admin",
        title="Admin",
        icon="fa-shield-halved",
        description="Administration and platform management.",
        # Hub visible to anyone bound to web_admin_access (admin/user
        # by default; sysadmin via PageVisibilityFilter bypass). Sub-pages
        # within enforce their own audience policies.
        audience_policy_id="web_admin_access",
        priority=10,
    )
    async def provide_admin_hub(self, request: Request):
        """Admin landing hub — lists accessible admin sub-pages as cards."""
        return HTMLResponse(content="""
<div class="space-y-6">
  <div>
    <h2 class="text-2xl font-bold text-white mb-1">Administration</h2>
    <p class="text-slate-400 text-sm">Manage the platform resources available to you.</p>
  </div>
  <div id="admin-hub-cards" class="grid md:grid-cols-2 gap-5">
    <div class="text-slate-600 text-sm py-4"><i class="fa-solid fa-spinner fa-spin mr-2"></i>Loading...</div>
  </div>
</div>
<script>
(function() {
  const iconColors = {
    'fa-users-gear': 'text-blue-400 bg-blue-500/10 border-blue-500/20',
    'fa-sliders':    'text-purple-400 bg-purple-500/10 border-purple-500/20',
  };
  const grid = document.getElementById('admin-hub-cards');
  if (!grid) return;

  const tkey = (typeof TOKEN_KEY !== 'undefined') ? TOKEN_KEY : 'ds_token';
  const token = (typeof authToken !== 'undefined' && authToken) || localStorage.getItem(tkey) || sessionStorage.getItem(tkey);
  const headers = token ? { 'Authorization': 'Bearer ' + token } : {};

  fetch('/web/config/pages', { headers })
    .then(r => r.json())
    .then(pages => {
      const subPages = pages.filter(p => p.section === 'admin');
      grid.innerHTML = '';
      if (!subPages.length) {
        grid.innerHTML = '<p class="text-slate-500 text-sm">No admin tools available.</p>';
        return;
      }
      subPages.forEach(p => {
        const colorClass = iconColors[p.icon] || 'text-slate-400 bg-slate-500/10 border-slate-500/20';
        const btn = document.createElement('button');
        btn.onclick = () => switchTab(p.id);
        btn.className = 'glass-panel text-left p-6 rounded-2xl border border-white/5 hover:border-white/10 transition-all group';
        btn.innerHTML = `
          <div class="w-12 h-12 rounded-xl ${colorClass} border flex items-center justify-center mb-4 group-hover:scale-110 transition-transform">
            <i class="fa-solid ${p.icon} text-xl"></i>
          </div>
          <h3 class="font-semibold text-white mb-1">${p.title}</h3>
          <p class="text-slate-400 text-sm">${p.description || ''}</p>`;
        grid.appendChild(btn);
      });
    })
    .catch(() => { grid.innerHTML = '<p class="text-red-400 text-sm">Failed to load admin tools.</p>'; });
})();
</script>
""")

    @expose_web_page(
        page_id="admin_panel",
        title="Admin Panel",
        icon="fa-users-gear",
        description="Manage users, roles, policies and catalog permissions.",
        # Audience driven by the admin_access policy's role bindings —
        # operators rebind the policy via REST to extend access (e.g. to
        # custom roles like data_curator) without editing this decorator.
        audience_policy_id="admin_access",
        section="admin",
        priority=20,
    )
    async def provide_admin_panel(self, request: Request):
        from dynastore._version import VERSION
        file_path = os.path.join(os.path.dirname(__file__), "..", "admin", "static", "admin_panel.html")
        with open(os.path.normpath(file_path), "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read().replace("{{VERSION}}", VERSION))

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        logger.info("IamExtension: Initializing lifecycle.")
        
        # Initialize instance attributes
        self._iam_manager = await self._get_iam_manager()
        self._policy_service = get_protocol(PermissionProtocol)
        if self._policy_service is None:
            raise RuntimeError("PermissionProtocol implementation not found.")
        
        from dynastore.models.protocols import DatabaseProtocol
        db_protocol = get_protocol(DatabaseProtocol)
        if not db_protocol:
            raise RuntimeError("DatabaseProtocol implementation not found.")
        self._engine = db_protocol.engine

        # Seed default policies (idempotent)
        try:
            # Seed both global (iam) and system (catalog) schemas
            await self._policy_service.provision_default_policies(catalog_id=None)
            await self._policy_service.provision_default_policies(catalog_id="_system_")
        except Exception as e:
            logger.error(f"Failed to seed default policies: {e}")

        # IAM's own service policies are declared via PolicyContributor
        # (IamExtension.get_policies / get_role_bindings) and picked up by
        # the contributor loop below alongside every other plugin's
        # declarations — same single registration site.

        # Register the IAM-side PageVisibilityFilter implementation so
        # web routes can delegate nav-list filtering without naming any
        # role themselves.
        try:
            from dynastore.extensions.iam.page_filter import IamPageVisibilityFilter
            from dynastore.tools.discovery import register_plugin
            register_plugin(IamPageVisibilityFilter())
        except Exception as e:
            logger.error("IamExtension: failed to register PageVisibilityFilter: %s", e)

        # Discover plugin-declared policies via PolicyContributor. Plugins
        # never touch PermissionProtocol directly — they declare here and
        # IAM forwards centrally. Keeps IAM/auth concepts isolated from
        # the rest of the platform.
        try:
            from dynastore.models.protocols.policy_contributor import PolicyContributor
            contributors = get_protocols(PolicyContributor)
            for contributor in contributors:
                origin = type(contributor).__name__
                try:
                    for policy in contributor.get_policies() or []:
                        self._policy_service.register_policy(policy)
                    for role in contributor.get_role_bindings() or []:
                        self._policy_service.register_role(role)
                    logger.debug(
                        "IamExtension: registered policies/bindings from %s",
                        origin,
                    )
                except Exception as e:
                    logger.error(
                        "IamExtension: %s contributor failed: %s",
                        origin,
                        e,
                    )
        except Exception as e:
            logger.error("IamExtension: PolicyContributor discovery failed: %s", e)

        yield

    async def _get_iam_manager(self) -> IamService:
        if self._iam_manager is None:
            svc = get_protocol(IamService)
            if not svc:
                raise RuntimeError("IamService implementation not found")
            self._iam_manager = svc
        return self._iam_manager

    @property
    def iam_manager(self) -> IamService:
        if self._iam_manager is None:
            # Accessed before lifespan completed — try to resolve from the registry.
            svc = get_protocol(IamService)
            if svc is None:
                raise RuntimeError("IamService implementation not found")
            return svc
        return self._iam_manager

    @property
    def policy_service(self) -> PermissionProtocol:
        if self._policy_service is None:
            raise RuntimeError("PermissionProtocol not initialized; lifespan has not run.")
        return self._policy_service

    # ==========================================
    # 1. PUBLIC / SELF-SERVICE (Any Auth User)
    # ==========================================

    async def get_jwks(self):
        """Public endpoint for JWKS discovery."""
        return await self.iam_manager.get_jwks()

    # ==========================================
    # 2. ADMIN OPERATIONS (/iam/admin)
    # Accessible by: Sysadmin, Admin
    # ==========================================

    # --- Policies ---

    async def create_access_policy(self, request: Request, policy_req: PolicyCreateRequest):
        """Creates a global access policy."""
        catalog_id = getattr(request.state, "catalog_id", None)

        policy_model = Policy(
            id=policy_req.id,
            description=policy_req.description,
            effect=policy_req.effect,
            actions=policy_req.actions,
            resources=policy_req.resources,
            conditions=policy_req.conditions or [],
            partition_key=policy_req.partition_key,
        )
        try:
            return await self.policy_service.create_policy(
                policy_model, catalog_id=catalog_id
            )
        except Exception as e:
            logger.error(f"Policy creation error: {e}")
            if "already exists" in str(e).lower():
                raise HTTPException(
                    status_code=409, detail=f"Policy already exists: {str(e)}"
                )
            raise HTTPException(
                status_code=500, detail=f"Unable to create policy: {str(e)}"
            )

    async def update_access_policy(
        self,
        request: Request, policy_id: str, policy_req: PolicyUpdateRequest
    ):
        """Updates an existing access policy."""
        catalog_id = getattr(request.state, "catalog_id", None)

        # 1. Fetch existing
        existing = await self.policy_service.get_policy(policy_id, catalog_id=catalog_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Policy not found.")

        # 2. Update fields
        update_data = policy_req.model_dump(exclude_unset=True)
        updated_model = existing.model_copy(update=update_data)

        try:
            result = await self.policy_service.update_policy(
                updated_model, catalog_id=catalog_id
            )
            if not result:
                raise HTTPException(
                    status_code=404, detail="Policy not found during update."
                )
            return result
        except Exception as e:
            logger.error(f"Policy update error: {e}")
            raise HTTPException(status_code=500, detail="Unable to update policy.")

    async def search_access_policies(
        self,
        request: Request,
        resource: Optional[str] = Query(
            None, description="Filter by resource regex pattern"
        ),
        action: Optional[str] = Query(None, description="Filter by action pattern"),
        limit: int = 100,
        offset: int = 0,
    ):
        """Search and list policies."""
        catalog_id = getattr(request.state, "catalog_id", None)
        return await self.policy_service.search_policies(
            resource_pattern=resource or ".*",
            action_pattern=action or ".*",
            limit=limit,
            offset=offset,
            catalog_id=catalog_id,
        )

    async def delete_access_policy(self, request: Request, policy_id: UUID):
        catalog_id = getattr(request.state, "catalog_id", None)
        deleted = await self.policy_service.delete_policy(str(policy_id), catalog_id=catalog_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Policy not found")

    # --- Roles & Hierarchies ---

    async def list_roles(self, request: Request):
        """Lists all dynamic roles."""
        catalog_id = getattr(request.state, "catalog_id", None)
        return await self.iam_manager.list_roles(catalog_id=catalog_id)

    async def create_role(self, role_req: Role, request: Request):
        """Creates a new dynamic role."""
        catalog_id = getattr(request.state, "catalog_id", None)
        return await self.iam_manager.create_role(role_req, catalog_id=catalog_id)

    async def update_role(self, name: str, role_req: Role, request: Request):
        """Updates an existing role."""
        catalog_id = getattr(request.state, "catalog_id", None)
        role_req.name = name  # Ensure name matches path
        return await self.iam_manager.update_role(role_req, catalog_id=catalog_id)

    async def delete_role(self, name: str, request: Request, cascade: bool = False):
        """Deletes a role with optional cascading removal from principals."""
        catalog_id = getattr(request.state, "catalog_id", None)
        await self.iam_manager.delete_role(name, cascade=cascade, catalog_id=catalog_id)

    # --- Hierarchy ---

    async def add_role_hierarchy(self, parent: str, child: str, request: Request):
        """Links two roles in a parent-child inheritance relationship."""
        catalog_id = getattr(request.state, "catalog_id", None)
        await self.iam_manager.add_role_hierarchy(parent, child, catalog_id=catalog_id)

    async def remove_role_hierarchy(self, parent: str, child: str, request: Request):
        """Removes a parent-child inheritance relationship."""
        catalog_id = getattr(request.state, "catalog_id", None)
        await self.iam_manager.remove_role_hierarchy(
            parent, child, catalog_id=catalog_id
        )

    async def get_role_hierarchy(self, role_name: str, request: Request):
        """Gets all effective roles (descendants) for a given role."""
        catalog_id = getattr(request.state, "catalog_id", None)
        return await self.iam_manager.get_role_hierarchy(
            role_name, catalog_id=catalog_id
        )

    # --- Principals ---

    async def create_principal(self, principal_req: PrincipalCreateRequest, request: Request):
        """
        Creates a new Principal.
        SECURITY: Admin cannot create another Admin/Sysadmin.
        """
        catalog_id = getattr(request.state, "catalog_id", None)

        # Check roles for privilege escalation
        for role in principal_req.roles:
            await ensure_privileged_role_assignment(request, role)

        principal_model = Principal(
            provider=principal_req.provider,
            subject_id=principal_req.subject_id,
            display_name=principal_req.subject_id,
            roles=principal_req.roles,
            attributes=principal_req.attributes,
            custom_policies=principal_req.policy.statements
            if principal_req.policy
            else [],
        )
        try:
            return await self.iam_manager.create_principal(
                principal_model, catalog_id=catalog_id
            )
        except Exception as e:
            logger.error(f"Principal creation error: {e}")
            if "duplicate key" in str(e).lower():
                raise HTTPException(status_code=409, detail="Principal already exists.")
            raise HTTPException(
                status_code=500, detail=f"Internal server error: {str(e)}"
            )

    async def update_principal(
        self,
        request: Request, principal_id: str, principal_req: PrincipalUpdateRequest
    ):
        """Updates an existing Principal."""
        catalog_id = getattr(request.state, "catalog_id", None)
        schema = await self.iam_manager._resolve_schema(catalog_id)

        # 1. Fetch existing
        existing = await self.iam_manager.storage.get_principal(
            principal_id, schema=schema
        )
        if not existing:
            raise HTTPException(status_code=404, detail="Principal not found.")

        # 2. Security Check (Admin cannot elevate/manage Admin/Sysadmin)
        for role in existing.roles:
            await ensure_privileged_role_assignment(request, role)

        # 3. Update fields
        update_data = principal_req.model_dump(exclude_unset=True)

        # If new roles are provided, check they are safe
        if "roles" in update_data:
            for role in update_data["roles"]:
                await ensure_privileged_role_assignment(request, role)

        updated_model = existing.model_copy(update=update_data)

        try:
            result = await self.iam_manager.update_principal(
                updated_model, catalog_id=catalog_id
            )
            if not result:
                raise HTTPException(
                    status_code=404, detail="Principal not found during update."
                )
            return result
        except Exception as e:
            logger.error(f"Principal update error: {e}")
            raise HTTPException(status_code=500, detail="Internal server error.")

    async def search_principals(
        self, # Added self
        request: Request,
        identifier: Optional[str] = Query(
            None, description="Filter by identifier (partial match)"
        ),
        role: Optional[str] = Query(
            None, description="Filter by metadata 'role' field"
        ),
        limit: int = 100,
        offset: int = 0,
    ):
        catalog_id = getattr(request.state, "catalog_id", None)
        return await self.iam_manager.search_principals(
            identifier=identifier,
            role=role,
            limit=limit,
            offset=offset,
            catalog_id=catalog_id,
        )

    async def delete_principal(self, principal_id: UUID, request: Request): # Added self
        """
        Deletes a Principal.
        SECURITY: Admin cannot delete another Admin/Sysadmin.
        """
        catalog_id = getattr(request.state, "catalog_id", None)
        schema = await self.iam_manager._resolve_schema(catalog_id)
        principal = await self.iam_manager.storage.get_principal(
            principal_id, schema=schema
        )
        if principal:
            target_role = principal.attributes.get("role")
            if target_role:
                await ensure_privileged_role_assignment(request, target_role)

        deleted = await self.iam_manager.delete_principal( # Changed _iam_manager to self.iam_manager
            principal_id, catalog_id=catalog_id
        )
        if not deleted:
            raise HTTPException(status_code=404, detail="Principal not found")

    # --- Stats ---

    async def get_system_stats_summary(
        self, # Added self
        request: Request,
        principal_id: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ):
        catalog_id = getattr(request.state, "catalog_id", None)
        schema = await self.iam_manager._resolve_schema(catalog_id)
        return await get_stats_summary(
            self._engine, # Changed _engine to self._engine
            principal_id=principal_id,
            start_date=start_date,
            end_date=end_date,
            schema=schema,
        )

    async def get_system_access_logs(
        self, # Added self
        request: Request,
        principal_id: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        cursor: Optional[str] = None,
        page_size: int = 100,
    ):
        catalog_id = getattr(request.state, "catalog_id", None)
        schema = await self.iam_manager._resolve_schema(catalog_id)
        return await get_access_logs(
            self._engine, # Changed _engine to self._engine
            principal_id=principal_id,
            start_date=start_date,
            end_date=end_date,
            cursor=cursor,
            page_size=page_size,
            schema=schema,
        )

