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
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

import logging
from typing import Optional
from uuid import UUID

from fastapi import FastAPI, APIRouter, HTTPException, Query, Request
from contextlib import asynccontextmanager

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.modules import get_protocol
from dynastore.modules.iam.iam_service import IamService
from dynastore.models.protocols.catalogs import CatalogsProtocol
from dynastore.models.protocols.policies import Policy, Role, Principal

from dynastore.extensions.iam.guards import ensure_privileged_role_assignment

from .models import (
    UserCreate, UserUpdate,
    RoleCreate, RoleUpdate, RoleResponse,
    PolicyCreate, PolicyUpdate, PolicyResponse,
    PrincipalResponse, AssignRoleRequest,
)
from .policies import admin_policies, admin_role_bindings

logger = logging.getLogger(__name__)


def _iam() -> IamService:
    mgr = get_protocol(IamService)
    if mgr is None:
        raise HTTPException(status_code=503, detail="Auth service not available.")
    return mgr


async def _assert_catalog_exists(catalog_id: str) -> None:
    """Raise 404 if ``catalog_id`` does not resolve to a known catalog.

    Required because ``IamService.resolve_schema`` is intentionally lenient:
    on an unknown catalog it logs a warning and falls back to the global
    ``iam`` schema (so middleware-style auth checks still work in degraded
    states). Admin endpoints that *write* catalog-scoped roles, or list
    catalog users, must instead reject unknown catalogs explicitly —
    otherwise an operator typo silently mutates global IAM state or returns
    the global user list.
    """
    catalogs = get_protocol(CatalogsProtocol)
    if catalogs is None:
        # No catalogs service → can't validate; let resolve_schema's
        # fallback path run. This matches IamService's own posture.
        return
    model = await catalogs.get_catalog_model(catalog_id)
    if model is None:
        raise HTTPException(status_code=404, detail=f"Catalog '{catalog_id}' not found.")


class AdminService(ExtensionProtocol):
    priority: int = 200
    """Admin REST API — user, role, policy, and catalog assignment management.

    Endpoint-level authorization is delegated to `IamMiddleware`, which evaluates
    policies dynamically against `request.url.path` + `request.method` using
    `PermissionProtocol.evaluate_access`. When the IAM module is not loaded the
    fail-closed `DefaultAuthorizer` protects privileged paths.
    """

    router: APIRouter = APIRouter(
        tags=["Authentication & Authorization"], prefix="/admin"
    )

    # PolicyContributor: declare authz needs; IAM forwards centrally.
    # No direct call to PermissionProtocol — keeps the plugin agnostic
    # of the enforcement implementation.
    def get_policies(self):
        return admin_policies()

    def get_role_bindings(self):
        return admin_role_bindings()

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        # Policies declared via PolicyContributor (get_policies +
        # get_role_bindings); IAM picks them up centrally.
        yield

    # -------------------------------------------------------------------------
    # User Management (/admin/users)
    # -------------------------------------------------------------------------

    @router.get(
        "/users",
        summary="List or search principals (filterable by provider, identifier, role, catalog)",
    )
    async def list_users(
        limit: int = Query(50, ge=1, le=500),  # type: ignore[reportGeneralTypeIssues]
        offset: int = Query(0, ge=0),
        provider: Optional[str] = Query(
            None,
            description="Filter by identity provider (e.g. 'local', 'oidc', 'system'). "
            "Omit to list all principals.",
        ),
        q: Optional[str] = Query(
            None,
            description="Free-text partial match on principal identifier "
            "(OGC API - Records §7.7). When set, switches to search mode.",
        ),
        role: Optional[str] = Query(
            None, description="Filter to principals that hold this role."
        ),
        catalog_id: Optional[str] = Query(
            None, description="Resolve role membership within this catalog scope."
        ),
    ):
        mgr = _iam()
        if q is not None or role is not None or catalog_id is not None:
            principals = await mgr.search_principals(
                identifier=q,
                role=role,
                limit=limit,
                offset=offset,
                catalog_id=catalog_id,
            )
        else:
            principals = await mgr.list_principals(limit=limit, offset=offset)
        if provider is not None:
            principals = [p for p in principals if p.provider == provider]
        out = []
        for p in principals:
            granted = await mgr.storage.list_platform_roles(principal_id=p.id)
            out.append(PrincipalResponse(
                id=str(p.id),
                provider=p.provider,
                subject_id=p.subject_id,
                display_name=p.display_name,
                roles=list(granted),
                is_active=p.is_active,
            ))
        return out

    @router.post("/users", summary="Create a principal (local user or raw)", status_code=201)
    async def create_user(request: Request, body: UserCreate):  # type: ignore[reportGeneralTypeIssues]
        mgr = _iam()

        # Privilege-escalation guard: only sysadmins can mint a principal
        # with a privileged role (admin/sysadmin by default — actual set is
        # IamRolesConfig.admin_role_set, operator-tunable).
        for role in body.roles or []:
            await ensure_privileged_role_assignment(request, role)

        # Local-IdP user path: `provider="local"` + `password` set → go through
        # the local provider's `create_user` so the credential gets persisted
        # and the principal subject_id is the local user's UUID. This is the
        # path the admin UI exercises.
        #
        # Raw-principal path: anything else (non-local provider, OR local
        # provider without a password — used by test/notebook flows that bind
        # a role to a synthetic identity that never logs in via password).
        # Skip the local_provider hop and construct the Principal directly,
        # honoring whatever subject_id the caller supplied (defaults to
        # username when omitted).
        if body.provider == "local" and body.password:
            providers = mgr.get_identity_providers()
            local_provider = next(
                (p for p in providers if getattr(p, "get_provider_id", lambda: None)() == "local"),
                None,
            )
            if local_provider and hasattr(local_provider, "create_user"):
                user_uuid = await getattr(local_provider, "create_user")(
                    username=body.username,
                    password=body.password,
                    email=body.email,
                )
                subject_id = str(user_uuid)
            else:
                subject_id = body.subject_id or body.username
        else:
            subject_id = body.subject_id or body.username

        new_principal = Principal(
            provider=body.provider,
            subject_id=subject_id,
            display_name=body.username,
            roles=body.roles or ["user"],
            is_active=True,
        )
        created = await mgr.create_principal(new_principal)

        return PrincipalResponse(
            id=str(created.id),
            provider=created.provider,
            subject_id=created.subject_id,
            display_name=created.display_name,
            roles=created.roles,
            is_active=created.is_active,
        )

    @router.get("/users/{principal_id}", summary="Get user details")
    async def get_user(principal_id: UUID):  # type: ignore[reportGeneralTypeIssues]
        mgr = _iam()
        p = await mgr.get_principal(principal_id)
        if not p:
            raise HTTPException(status_code=404, detail="User not found.")
        granted = await mgr.storage.list_platform_roles(principal_id=p.id)
        return PrincipalResponse(
            id=str(p.id), provider=p.provider, subject_id=p.subject_id,
            display_name=p.display_name, roles=list(granted), is_active=p.is_active,
        )

    @router.put("/users/{principal_id}", summary="Update user")
    async def update_user(request: Request, principal_id: UUID, body: UserUpdate):  # type: ignore[reportGeneralTypeIssues]
        mgr = _iam()
        p = await mgr.get_principal(principal_id)
        if not p:
            raise HTTPException(status_code=404, detail="User not found.")
        # Privilege-escalation guard: only sysadmins may manage a principal
        # that already holds a privileged role, or assign one via the update.
        for role in p.roles:
            await ensure_privileged_role_assignment(request, role)
        if body.roles is not None:
            for role in body.roles:
                await ensure_privileged_role_assignment(request, role)
        if body.is_active is not None:
            p.is_active = body.is_active
        if body.roles is not None:
            p.roles = body.roles
        updated = await mgr.update_principal(p)
        if updated is None:
            raise HTTPException(status_code=404, detail="User not found after update.")
        return PrincipalResponse(
            id=str(updated.id), provider=updated.provider, subject_id=updated.subject_id,
            display_name=updated.display_name, roles=updated.roles, is_active=updated.is_active,
        )

    @router.delete("/users/{principal_id}", status_code=204, summary="Delete user")
    async def delete_user(request: Request, principal_id: UUID):  # type: ignore[reportGeneralTypeIssues]
        mgr = _iam()
        p = await mgr.get_principal(principal_id)
        if p:
            # Privilege-escalation guard: only sysadmins may delete a
            # principal that holds a privileged role.
            for role in p.roles:
                await ensure_privileged_role_assignment(request, role)
        deleted = await mgr.delete_principal(principal_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="User not found.")

    # ---- Platform-scope role grants (D6 — `iam.grants`) -----------------

    @router.post(
        "/platform/principals/{principal_id}/roles",
        status_code=204,
        summary="Grant a platform-scope role to a principal",
    )
    async def grant_platform_role(request: Request, principal_id: UUID, body: AssignRoleRequest):  # type: ignore[reportGeneralTypeIssues]
        mgr = _iam()
        # Privilege-escalation guard: only sysadmins may grant a privileged role.
        await ensure_privileged_role_assignment(request, body.role)
        p = await mgr.get_principal(principal_id)
        if not p:
            raise HTTPException(status_code=404, detail="Principal not found.")
        registered = await mgr.list_roles(catalog_id=None)
        if not any(r.name == body.role for r in registered):
            raise HTTPException(
                status_code=422,
                detail=f"Role '{body.role}' is not registered in the platform role registry.",
            )
        try:
            await mgr.storage.grant_platform_role(
                principal_id=principal_id,
                role_name=body.role,
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @router.delete(
        "/platform/principals/{principal_id}/roles/{role_name}",
        status_code=204,
        summary="Revoke a platform-scope role from a principal",
    )
    async def revoke_platform_role(request: Request, principal_id: UUID, role_name: str):  # type: ignore[reportGeneralTypeIssues]
        mgr = _iam()
        # Privilege-escalation guard: only sysadmins may revoke a privileged role.
        await ensure_privileged_role_assignment(request, role_name)
        p = await mgr.get_principal(principal_id)
        if not p:
            raise HTTPException(status_code=404, detail="Principal not found.")
        try:
            await mgr.storage.revoke_platform_role(
                principal_id=principal_id,
                role_name=role_name,
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @router.get(
        "/platform/principals/{principal_id}/roles",
        summary="List platform-scope roles for a principal",
    )
    async def list_platform_roles(principal_id: UUID):  # type: ignore[reportGeneralTypeIssues]
        mgr = _iam()
        p = await mgr.get_principal(principal_id)
        if not p:
            raise HTTPException(status_code=404, detail="Principal not found.")
        return await mgr.storage.list_platform_roles(principal_id=principal_id)

    # ---- Catalog-scope role grants (D6 — `{catalog_schema}.grants`) -----

    @router.get(
        "/catalogs",
        summary="List catalogs (admin picker for catalog-scope role grants)",
    )
    async def list_catalogs_for_admin(
        limit: int = Query(200, ge=1, le=1000),  # type: ignore[reportGeneralTypeIssues]
        offset: int = Query(0, ge=0),
        lang: str = Query("en"),
        q: Optional[str] = Query(None, description="Free-text partial match on id/title/description"),
    ):
        catalogs_svc = get_protocol(CatalogsProtocol)
        if catalogs_svc is None:
            raise HTTPException(status_code=503, detail="Catalogs service not available.")
        items = await catalogs_svc.list_catalogs(limit=limit, offset=offset, lang=lang, q=q)
        out = []
        for c in items:
            title_raw = c.model_dump(mode="json").get("title")
            if isinstance(title_raw, dict):
                title = title_raw.get(lang) or next(iter(title_raw.values()), None)
            else:
                title = title_raw
            out.append({"id": c.id, "title": title or c.id})
        return out

    @router.post(
        "/catalogs/{catalog_id}/principals/{principal_id}/roles",
        status_code=204,
        summary="Grant a catalog-scope role to a principal",
    )
    async def grant_catalog_role(
        request: Request,  # type: ignore[reportGeneralTypeIssues]
        catalog_id: str,
        principal_id: UUID,
        body: AssignRoleRequest,
    ):
        mgr = _iam()
        # Privilege-escalation guard: only sysadmins may grant a privileged role
        # at any scope. Catalog-scope grants of platform-privileged roles are
        # rejected upstream (see role-registry split), but we still gate here
        # in case an operator extends the privileged set.
        await ensure_privileged_role_assignment(request, body.role)
        await _assert_catalog_exists(catalog_id)
        p = await mgr.get_principal(principal_id)
        if not p:
            raise HTTPException(status_code=404, detail="Principal not found.")
        registered = await mgr.list_roles(catalog_id=catalog_id)
        if not any(r.name == body.role for r in registered):
            raise HTTPException(
                status_code=422,
                detail=f"Role '{body.role}' is not registered for catalog '{catalog_id}'.",
            )
        try:
            await mgr.storage.grant_catalog_role(
                principal_id=principal_id,
                role_name=body.role,
                catalog_schema=await mgr.resolve_schema(catalog_id),
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @router.delete(
        "/catalogs/{catalog_id}/principals/{principal_id}/roles/{role_name}",
        status_code=204,
        summary="Revoke a catalog-scope role from a principal",
    )
    async def revoke_catalog_role(
        request: Request,  # type: ignore[reportGeneralTypeIssues]
        catalog_id: str,
        principal_id: UUID,
        role_name: str,
    ):
        mgr = _iam()
        # Privilege-escalation guard: only sysadmins may revoke a privileged role.
        await ensure_privileged_role_assignment(request, role_name)
        await _assert_catalog_exists(catalog_id)
        p = await mgr.get_principal(principal_id)
        if not p:
            raise HTTPException(status_code=404, detail="Principal not found.")
        try:
            await mgr.storage.revoke_catalog_role(
                principal_id=principal_id,
                role_name=role_name,
                catalog_schema=await mgr.resolve_schema(catalog_id),
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @router.get(
        "/catalogs/{catalog_id}/principals/{principal_id}/roles",
        summary="List catalog-scope roles for a principal",
    )
    async def list_catalog_roles_for_principal(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        principal_id: UUID,
    ):
        mgr = _iam()
        await _assert_catalog_exists(catalog_id)
        p = await mgr.get_principal(principal_id)
        if not p:
            raise HTTPException(status_code=404, detail="Principal not found.")
        return await mgr.storage.list_catalog_roles(
            principal_id=principal_id,
            catalog_schema=await mgr.resolve_schema(catalog_id),
        )

    @router.get("/catalogs/{catalog_id}/users", summary="List users assigned to a catalog")
    async def list_catalog_users(catalog_id: str):  # type: ignore[reportGeneralTypeIssues]
        mgr = _iam()
        await _assert_catalog_exists(catalog_id)
        catalog_schema = await mgr.resolve_schema(catalog_id)

        # Storage primitive returns a row per principal that holds at
        # least one grant in the tenant's `grants` table.
        users = await mgr.storage.get_catalog_users(catalog_schema=catalog_schema)
        if not users:
            return []

        # Hydrate each user with their actual catalog-scope role list so the
        # admin UI can render the same shape it used to receive.
        result = []
        for u in users:
            principal_id = u.get("id")
            if not principal_id:
                continue
            try:
                roles = await mgr.storage.list_catalog_roles(
                    principal_id=principal_id, catalog_schema=catalog_schema
                )
            except Exception as e:
                logger.warning(
                    "Failed to fetch catalog roles for principal %s in schema %s: %s",
                    principal_id, catalog_schema, e,
                )
                continue
            result.append(
                PrincipalResponse(
                    id=str(principal_id),
                    provider=u.get("provider"),
                    subject_id=u.get("subject_id"),
                    display_name=u.get("display_name"),
                    roles=roles,
                    is_active=u.get("is_active", True),
                )
            )
        return result

    # -------------------------------------------------------------------------
    # Role Management (/admin/roles)
    # -------------------------------------------------------------------------

    @router.get("/roles", summary="List all roles")
    async def list_roles(catalog_id: Optional[str] = Query(None)):  # type: ignore[reportGeneralTypeIssues]
        mgr = _iam()
        roles = await mgr.list_roles(catalog_id=catalog_id)
        # Per geoid#643: when scoped to a catalog, hide platform-tier
        # roles (e.g. sysadmin) — they're not grantable per-catalog and
        # surfacing them in the admin grant UI is misleading.
        if catalog_id is not None:
            roles_cfg = await mgr._get_roles_config()
            platform_only = roles_cfg.platform_role_names
            roles = [r for r in roles if r.name not in platform_only]
        return [
            RoleResponse(
                name=r.name,
                description=r.description,
                policies=r.policies or [],
                parent_roles=r.parent_roles or [],
            )
            for r in roles
        ]

    @router.get("/roles/{role_name}", summary="Get role details")
    async def get_role(role_name: str, catalog_id: Optional[str] = Query(None)):  # type: ignore[reportGeneralTypeIssues]
        mgr = _iam()
        roles = await mgr.list_roles(catalog_id=catalog_id)
        role = next((r for r in roles if r.name == role_name), None)
        if not role:
            raise HTTPException(status_code=404, detail=f"Role '{role_name}' not found.")
        return RoleResponse(
            name=role.name,
            description=role.description,
            policies=role.policies or [],
            parent_roles=role.parent_roles or [],
        )

    @router.post("/roles", summary="Create a new role", status_code=201)
    async def create_role(body: RoleCreate, catalog_id: Optional[str] = Query(None)):  # type: ignore[reportGeneralTypeIssues]
        mgr = _iam()
        role = Role(
            name=body.name,
            description=body.description,
            policies=body.policies,
            parent_roles=body.parent_roles,
        )
        try:
            created = await mgr.create_role(role, catalog_id=catalog_id)
        except ValueError as e:
            raise HTTPException(status_code=409, detail=str(e))
        return RoleResponse(
            name=created.name, description=created.description,
            policies=created.policies or [], parent_roles=created.parent_roles or [],
        )

    @router.put("/roles/{role_name}", summary="Update a role")
    async def update_role(
        role_name: str,  # type: ignore[reportGeneralTypeIssues]
        body: RoleUpdate,
        catalog_id: Optional[str] = Query(None),
    ):
        mgr = _iam()
        roles = await mgr.list_roles(catalog_id=catalog_id)
        existing = next((r for r in roles if r.name == role_name), None)
        if not existing:
            raise HTTPException(status_code=404, detail=f"Role '{role_name}' not found.")
        if body.description is not None:
            existing.description = body.description
        if body.policies is not None:
            existing.policies = body.policies
        if body.parent_roles is not None:
            existing.parent_roles = body.parent_roles
        updated = await mgr.update_role(existing, catalog_id=catalog_id)
        if updated is None:
            raise HTTPException(status_code=404, detail="Role not found after update.")
        return RoleResponse(
            name=updated.name, description=updated.description,
            policies=updated.policies or [], parent_roles=updated.parent_roles or [],
        )

    @router.delete("/roles/{role_name}", status_code=204, summary="Delete a role")
    async def delete_role(role_name: str, catalog_id: Optional[str] = Query(None)):  # type: ignore[reportGeneralTypeIssues]
        mgr = _iam()
        roles = await mgr.list_roles(catalog_id=catalog_id)
        existing = next((r for r in roles if r.name == role_name), None)
        if not existing:
            raise HTTPException(status_code=404, detail=f"Role '{role_name}' not found.")
        await mgr.delete_role(role_name, catalog_id=catalog_id)

    # -------------------------------------------------------------------------
    # Role Hierarchies (/admin/hierarchies)
    # -------------------------------------------------------------------------

    @router.post("/hierarchies", status_code=204, summary="Add a parent→child role hierarchy edge")
    async def add_role_hierarchy(  # type: ignore[reportGeneralTypeIssues]
        parent: str = Query(..., description="Parent role name"),
        child: str = Query(..., description="Child role name"),
        catalog_id: Optional[str] = Query(None),
    ):
        mgr = _iam()
        await mgr.add_role_hierarchy(parent, child, catalog_id=catalog_id)

    @router.delete("/hierarchies", status_code=204, summary="Remove a parent→child role hierarchy edge")
    async def remove_role_hierarchy(  # type: ignore[reportGeneralTypeIssues]
        parent: str = Query(..., description="Parent role name"),
        child: str = Query(..., description="Child role name"),
        catalog_id: Optional[str] = Query(None),
    ):
        mgr = _iam()
        await mgr.remove_role_hierarchy(parent, child, catalog_id=catalog_id)

    @router.get("/hierarchies/{role_name}", summary="Get effective descendants for a role")
    async def get_role_hierarchy(  # type: ignore[reportGeneralTypeIssues]
        role_name: str,
        catalog_id: Optional[str] = Query(None),
    ) -> list[str]:
        mgr = _iam()
        return await mgr.get_role_hierarchy(role_name, catalog_id=catalog_id)

    # -------------------------------------------------------------------------
    # Policy Management (/admin/policies)
    # -------------------------------------------------------------------------

    @router.get("/policies", summary="List all policies")
    async def list_policies(catalog_id: Optional[str] = Query(None)):  # type: ignore[reportGeneralTypeIssues]
        mgr = _iam()
        pm = mgr.get_policy_service()
        if not pm:
            raise HTTPException(status_code=503, detail="Policy manager not available.")
        policies = await pm.list_policies(catalog_id=catalog_id)
        return [
            PolicyResponse(
                id=p.id, description=p.description, actions=p.actions,
                resources=p.resources, effect=p.effect, partition_key=p.partition_key,
            )
            for p in policies
        ]

    @router.post("/policies", summary="Create a new policy", status_code=201)
    async def create_policy(body: PolicyCreate, catalog_id: Optional[str] = Query(None)):  # type: ignore[reportGeneralTypeIssues]
        mgr = _iam()
        pm = mgr.get_policy_service()
        if not pm:
            raise HTTPException(status_code=503, detail="Policy manager not available.")
        policy = Policy(
            id=body.id,
            description=body.description,
            actions=body.actions,
            resources=body.resources,
            effect=body.effect,
        )
        try:
            created = await pm.create_policy(policy, catalog_id=catalog_id)
        except ValueError as e:
            raise HTTPException(status_code=409, detail=str(e))
        return PolicyResponse(
            id=created.id, description=created.description, actions=created.actions,
            resources=created.resources, effect=created.effect, partition_key=created.partition_key,
        )

    @router.put("/policies/{policy_id}", summary="Update a policy")
    async def update_policy(
        policy_id: str,  # type: ignore[reportGeneralTypeIssues]
        body: PolicyUpdate,
        catalog_id: Optional[str] = Query(None),
    ):
        mgr = _iam()
        pm = mgr.get_policy_service()
        if not pm:
            raise HTTPException(status_code=503, detail="Policy manager not available.")
        existing = await pm.get_policy(policy_id, catalog_id=catalog_id)
        if not existing:
            raise HTTPException(status_code=404, detail=f"Policy '{policy_id}' not found.")
        if body.description is not None:
            existing.description = body.description
        if body.actions is not None:
            existing.actions = body.actions
        if body.resources is not None:
            existing.resources = body.resources
        if body.effect is not None:
            existing.effect = body.effect
        updated = await pm.update_policy(existing, catalog_id=catalog_id)
        if updated is None:
            raise HTTPException(status_code=404, detail="Policy not found after update.")
        return PolicyResponse(
            id=updated.id, description=updated.description, actions=updated.actions,
            resources=updated.resources, effect=updated.effect, partition_key=updated.partition_key,
        )

    @router.delete("/policies/{policy_id}", status_code=204, summary="Delete a policy")
    async def delete_policy(policy_id: str, catalog_id: Optional[str] = Query(None)):  # type: ignore[reportGeneralTypeIssues]
        mgr = _iam()
        pm = mgr.get_policy_service()
        if not pm:
            raise HTTPException(status_code=503, detail="Policy manager not available.")
        deleted = await pm.delete_policy(policy_id, catalog_id=catalog_id)
        if not deleted:
            raise HTTPException(status_code=404, detail=f"Policy '{policy_id}' not found.")

    # -------------------------------------------------------------------------
    # System Defaults (/admin/reset-defaults)
    # -------------------------------------------------------------------------

    @router.post("/reset-defaults", summary="Reset default policies and roles")
    async def reset_defaults(
        request: Request,  # type: ignore[reportGeneralTypeIssues]
        catalog_id: Optional[str] = Query(None, description="Catalog ID for tenant-scoped reset, or None for global"),
    ):
        mgr = _iam()
        pm = mgr.get_policy_service()
        if not pm:
            raise HTTPException(status_code=503, detail="Policy manager not available.")
        await pm.provision_default_policies(catalog_id=catalog_id, force=True)
        return {"message": "Default policies and roles have been reset.", "catalog_id": catalog_id or "global"}

    @router.post("/rotate-jwt-secret", summary="Rotate JWT signing secret")
    async def rotate_jwt_secret(request: Request):  # type: ignore[reportGeneralTypeIssues]
        mgr = _iam()
        if not hasattr(mgr, "rotate_jwt_secret"):
            raise HTTPException(status_code=503, detail="JWT rotation not supported.")
        await mgr.rotate_jwt_secret()
        return {"message": "JWT secret rotated. Previous secret remains valid for existing tokens."}
