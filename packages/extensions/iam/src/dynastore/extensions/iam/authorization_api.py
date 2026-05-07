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
Simplified IAG REST API Endpoints (v2.1)

Email-based authorization management with self-service and admin endpoints.

All role-management calls go through the unified grants model: platform
grants live in `iam.grants`, catalog grants live in `{catalog_schema}.grants`.
The legacy `schema=` parameter has been replaced by explicit
``list_platform_roles`` / ``list_catalog_roles`` / ``grant_platform_role`` /
``grant_catalog_role`` / ``revoke_platform_role`` / ``revoke_catalog_role``
facades on the storage layer.
"""

import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, EmailStr
from typing import List, Optional, Dict, Any, cast
from uuid import UUID
from datetime import datetime

from dynastore.modules import get_protocol
from dynastore.models.protocols import CatalogsProtocol
from dynastore.models.protocols.authentication import AuthenticatorProtocol
from dynastore.extensions.tools.exception_handlers import http_errors
from dynastore.models.driver_context import DriverContext

logger = logging.getLogger(__name__)


router = APIRouter(prefix="/admin", tags=["Authentication & Authorization"])
me_router = APIRouter(prefix="/me", tags=["Authentication & Authorization"])

# --- DTOs ---


class RoleGrantRequest(BaseModel):
    """Request to grant roles to a user."""

    roles: List[str]
    granted_by: Optional[str] = None


class PolicyCreateRequest(BaseModel):
    """Request to create a custom policy."""

    policy_name: Optional[str] = None
    actions: List[str]
    resources: List[str] = ["*"]
    effect: str = "ALLOW"
    conditions: Optional[List[Dict[str, Any]]] = None


class EffectiveAuthorizationResponse(BaseModel):
    """Response showing effective authorization for a user in a catalog."""

    email: str
    provider: str
    subject_id: str
    global_roles: List[str]
    catalog_roles: List[str]
    effective_roles: List[str]
    is_active: bool
    valid_until: Optional[datetime] = None


class CatalogAccessResponse(BaseModel):
    """Response showing catalog access for a user."""

    catalog_id: str
    roles: List[str]


# --- Helper Functions ---


async def get_storage():
    """Get storage instance."""
    authenticator = get_protocol(AuthenticatorProtocol)
    if not authenticator:
        raise HTTPException(
            status_code=500, detail="Authenticator implementation not available"
        )
    return cast(Any, authenticator).storage


async def get_catalogs_protocol() -> CatalogsProtocol:
    """Get catalog protocol implementation."""
    catalogs_protocol = get_protocol(CatalogsProtocol)
    if not catalogs_protocol:
        raise HTTPException(
            status_code=500, detail="Catalogs protocol implementation not available"
        )
    return catalogs_protocol


async def get_current_identity(request: Request) -> Dict[str, Any]:
    """Extract current user identity from request state."""
    if hasattr(request.state, "identity") and request.state.identity:
        return request.state.identity

    if hasattr(request.state, "principal") and request.state.principal:
        principal = request.state.principal
        return {
            "provider": principal.provider,
            "sub": principal.subject_id,
            "email": principal.display_name,
        }

    raise HTTPException(status_code=401, detail="Not authenticated")


async def resolve_catalog_schema(catalog_id: str) -> str:
    """Resolve catalog code to schema name."""
    catalog_module = await get_catalogs_protocol()
    try:
        schema = await catalog_module.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=cast(Any, catalog_module).engine)
        )
        if not schema:
            raise HTTPException(
                status_code=404,
                detail=f"Catalog not found or schema not resolved: {catalog_id}",
            )
        return schema
    except (ValueError, AttributeError):
        raise HTTPException(status_code=404, detail=f"Catalog not found: {catalog_id}")


async def _resolve_principal_id(storage, provider: str, subject_id: str) -> UUID:
    """Resolve `(provider, subject_id)` to a principal_id, or 404."""
    principal = await storage.get_principal_by_identity(
        provider=provider, subject_id=subject_id
    )
    if principal is None:
        raise HTTPException(
            status_code=404,
            detail=f"No principal for identity {provider}:{subject_id}",
        )
    return principal.id


# --- Self-Service Endpoints ---


@me_router.get("")
async def get_me(request: Request):
    """Return the current principal along with their platform roles.

    A single call the admin UI uses to decide which pages to show — avoids
    round-tripping ``/me/available-roles`` and ``/me/roles/global`` on every
    navigation. Returns 401 when unauthenticated so the client can redirect
    to login.
    """
    identity = await get_current_identity(request)
    provider = identity.get("provider")
    subject_id = identity.get("sub")
    email = identity.get("email")

    roles: List[str] = []
    try:
        storage = await get_storage()
        # No catalog_schema → platform grants only.
        roles = await storage.get_identity_roles(provider, subject_id)
    except HTTPException:
        # get_storage() raises 500 if authenticator not wired — degrade to
        # empty roles rather than breaking the endpoint. The principal
        # stanza is still useful for display.
        roles = []
    except Exception:
        # Don't break the page on a transient storage error, but DO log so
        # operators see "user has no roles" caused by a storage-layer fault
        # instead of a real grant gap. Without this, a DB outage manifests
        # as silent permission stripping in the UI.
        logger.warning(
            "get_me: failed to fetch platform roles for %s:%s; returning empty",
            provider, subject_id,
            exc_info=True,
        )
        roles = []

    return {
        "principal": {
            "provider": provider,
            "sub": subject_id,
            "email": email,
        },
        "roles": list(roles or []),
    }


@me_router.get("/available-roles")
async def get_available_roles(
    request: Request,
    catalog_id: Optional[str] = None,
):
    """List role definitions visible to the caller.

    With no `catalog_id`, returns the platform role registry (`iam.roles`).
    With a `catalog_id`, returns that tenant's role registry.
    """
    storage = await get_storage()
    await get_current_identity(request)  # Require authentication

    schema = "iam"
    if catalog_id:
        schema = await resolve_catalog_schema(catalog_id)

    roles = await storage.list_roles(schema=schema)
    return [
        {
            "name": r.name,
            "description": r.description,
        }
        for r in roles
    ]


@me_router.get("/roles/global", response_model=List[str])
async def get_my_global_roles(request: Request):
    """Get my platform-scope roles."""
    storage = await get_storage()
    identity = await get_current_identity(request)
    provider = identity.get("provider")
    subject_id = identity.get("sub")

    # Platform-only view: no catalog_schema.
    return await storage.get_identity_roles(provider, subject_id)


@me_router.get("/roles/catalogs/{catalog_id}", response_model=List[str])
async def get_my_catalog_roles(request: Request, catalog_id: str):
    """Get my roles in a specific catalog (catalog-scope only).

    Reports the roles granted in `{catalog_schema}.grants` — platform
    grants are intentionally excluded so the caller sees the
    catalog-local picture.
    """
    storage = await get_storage()
    identity = await get_current_identity(request)
    provider = identity.get("provider")
    subject_id = identity.get("sub")
    catalog_schema = await resolve_catalog_schema(catalog_id)

    principal = await storage.get_principal_by_identity(
        provider=provider, subject_id=subject_id
    )
    if principal is None:
        return []
    return await storage.list_catalog_roles(
        principal_id=principal.id, catalog_schema=catalog_schema
    )


@me_router.get("/catalogs", response_model=List[CatalogAccessResponse])
async def get_my_catalogs(request: Request):
    """Get all catalogs I have access to."""
    storage = await get_storage()
    identity = await get_current_identity(request)
    provider = identity.get("provider")
    subject_id = identity.get("sub")

    result: List[CatalogAccessResponse] = []

    # Platform-scope grants surface as the "*" entry.
    platform_roles = await storage.get_identity_roles(provider, subject_id)
    if platform_roles:
        result.append(
            CatalogAccessResponse(catalog_id="*", roles=platform_roles)
        )

    principal = await storage.get_principal_by_identity(
        provider=provider, subject_id=subject_id
    )
    if principal is None:
        return result

    catalog_module = await get_catalogs_protocol()
    catalog_ids = await storage.get_catalogs_for_identity(provider, subject_id)

    for cat_id in catalog_ids:
        try:
            catalog_schema = await catalog_module.resolve_physical_schema(
                cat_id, ctx=DriverContext(db_resource=cast(Any, catalog_module).engine)
            )
            catalog_roles = await storage.list_catalog_roles(
                principal_id=principal.id, catalog_schema=catalog_schema
            )
            if catalog_roles:
                result.append(
                    CatalogAccessResponse(catalog_id=cat_id, roles=catalog_roles)
                )
        except Exception:
            # One bad catalog must not nuke "list my catalogs", but log so
            # a systemic problem (DB outage, missing schema) doesn't appear
            # as "user has access to nothing" with no breadcrumb.
            logger.warning(
                "get_my_catalogs: failed for catalog=%s identity=%s:%s; skipping",
                cat_id, provider, subject_id,
                exc_info=True,
            )
            continue

    return result


@me_router.get("/catalogs/{catalog_id}", response_model=EffectiveAuthorizationResponse)
async def get_my_effective_authorization(request: Request, catalog_id: str):
    """Get my effective authorization in a specific catalog."""
    storage = await get_storage()
    identity = await get_current_identity(request)
    provider = identity.get("provider")
    subject_id = identity.get("sub")
    email = identity.get("email", "unknown@example.com")

    if not provider or not subject_id:
        raise HTTPException(status_code=401, detail="Identity missing provider/subject")

    catalog_schema = await resolve_catalog_schema(catalog_id)

    principal = await storage.get_principal_by_identity(
        provider=provider, subject_id=subject_id
    )

    global_roles: List[str] = []
    catalog_roles: List[str] = []
    if principal is not None:
        global_roles = await storage.list_platform_roles(principal_id=principal.id)
        catalog_roles = await storage.list_catalog_roles(
            principal_id=principal.id, catalog_schema=catalog_schema
        )

    # Authorization metadata lives on the platform principal row (D12).
    auth = await storage.get_identity_authorization(provider, subject_id)

    return EffectiveAuthorizationResponse(
        email=email,
        provider=provider,
        subject_id=subject_id,
        global_roles=global_roles,
        catalog_roles=catalog_roles,
        effective_roles=list({*global_roles, *catalog_roles}),
        is_active=auth.get("is_active", True) if auth else True,
        valid_until=auth.get("valid_until") if auth else None,
    )


# --- Admin Endpoints ---


@router.post("/users/{email}/roles/global", status_code=201)
async def grant_global_roles(email: EmailStr, request: RoleGrantRequest):
    """Grant platform-scope roles to a user (admin only)."""
    storage = await get_storage()

    async with http_errors({ValueError: 404}):
        provider, subject_id = await storage.resolve_identity(email)

    principal_id = await _resolve_principal_id(storage, provider, subject_id)
    granted_by_uuid: Optional[UUID] = None
    if request.granted_by:
        try:
            granted_by_uuid = UUID(request.granted_by)
        except (TypeError, ValueError):
            granted_by_uuid = None

    for role_name in request.roles:
        await storage.grant_platform_role(
            principal_id=principal_id,
            role_name=role_name,
            granted_by=granted_by_uuid,
        )

    return {"message": f"Granted platform roles to {email}", "roles": request.roles}


@router.get("/users/{email}/roles/global", response_model=List[str])
async def get_user_global_roles(email: EmailStr):
    """Get platform-scope roles for a user (admin only)."""
    storage = await get_storage()

    async with http_errors({ValueError: 404}):
        provider, subject_id = await storage.resolve_identity(email)

    return await storage.get_identity_roles(provider, subject_id)


@router.delete("/users/{email}/roles/global/{role}", status_code=204)
async def revoke_global_role(email: EmailStr, role: str):
    """Revoke a platform-scope role from a user (admin only)."""
    storage = await get_storage()

    async with http_errors({ValueError: 404}):
        provider, subject_id = await storage.resolve_identity(email)

    principal_id = await _resolve_principal_id(storage, provider, subject_id)
    await storage.revoke_platform_role(principal_id=principal_id, role_name=role)
    return None


@router.post("/users/{email}/roles/catalogs/{catalog_id}", status_code=201)
async def grant_catalog_roles(
    email: EmailStr, catalog_id: str, request: RoleGrantRequest
):
    """Grant catalog-scope roles to a user (admin only)."""
    storage = await get_storage()
    catalog_schema = await resolve_catalog_schema(catalog_id)

    async with http_errors({ValueError: 404}):
        provider, subject_id = await storage.resolve_identity(email)

    principal_id = await _resolve_principal_id(storage, provider, subject_id)
    granted_by_uuid: Optional[UUID] = None
    if request.granted_by:
        try:
            granted_by_uuid = UUID(request.granted_by)
        except (TypeError, ValueError):
            granted_by_uuid = None

    for role_name in request.roles:
        await storage.grant_catalog_role(
            principal_id=principal_id,
            role_name=role_name,
            catalog_schema=catalog_schema,
            granted_by=granted_by_uuid,
        )

    return {
        "message": f"Granted catalog roles to {email}",
        "catalog": catalog_id,
        "roles": request.roles,
    }


@router.get("/users/{email}/roles/catalogs/{catalog_id}", response_model=List[str])
async def get_user_catalog_roles(email: EmailStr, catalog_id: str):
    """Get catalog-scope roles for a user (admin only)."""
    storage = await get_storage()
    catalog_schema = await resolve_catalog_schema(catalog_id)

    async with http_errors({ValueError: 404}):
        provider, subject_id = await storage.resolve_identity(email)

    principal = await storage.get_principal_by_identity(
        provider=provider, subject_id=subject_id
    )
    if principal is None:
        return []
    return await storage.list_catalog_roles(
        principal_id=principal.id, catalog_schema=catalog_schema
    )


@router.delete("/users/{email}/roles/catalogs/{catalog_id}/{role}", status_code=204)
async def revoke_catalog_role(email: EmailStr, catalog_id: str, role: str):
    """Revoke a catalog-scope role from a user (admin only)."""
    storage = await get_storage()
    catalog_schema = await resolve_catalog_schema(catalog_id)

    async with http_errors({ValueError: 404}):
        provider, subject_id = await storage.resolve_identity(email)

    principal_id = await _resolve_principal_id(storage, provider, subject_id)
    await storage.revoke_catalog_role(
        principal_id=principal_id, role_name=role, catalog_schema=catalog_schema
    )
    return None


@router.get("/users/{email}/catalogs", response_model=List[CatalogAccessResponse])
async def get_user_catalogs(email: EmailStr):
    """Get all catalogs a user has access to (admin only)."""
    storage = await get_storage()
    catalog_module = await get_catalogs_protocol()

    async with http_errors({ValueError: 404}):
        provider, subject_id = await storage.resolve_identity(email)

    result: List[CatalogAccessResponse] = []

    # Platform-scope grants surface as the "*" entry.
    global_roles = await storage.get_identity_roles(provider, subject_id)
    if global_roles:
        result.append(CatalogAccessResponse(catalog_id="*", roles=global_roles))

    principal = await storage.get_principal_by_identity(
        provider=provider, subject_id=subject_id
    )
    if principal is None:
        return result

    catalog_ids = await storage.get_catalogs_for_identity(provider, subject_id)

    for cat_id in catalog_ids:
        try:
            catalog_schema = await catalog_module.resolve_physical_schema(
                cat_id, ctx=DriverContext(db_resource=cast(Any, catalog_module).engine)
            )
            catalog_roles = await storage.list_catalog_roles(
                principal_id=principal.id, catalog_schema=catalog_schema
            )
            if catalog_roles:
                result.append(
                    CatalogAccessResponse(catalog_id=cat_id, roles=catalog_roles)
                )
        except Exception:
            # Admin "list user catalogs" — same logging stance as get_my_catalogs.
            logger.warning(
                "get_user_catalogs: failed for catalog=%s identity=%s:%s; skipping",
                cat_id, provider, subject_id,
                exc_info=True,
            )
            continue

    return result


@router.get(
    "/users/{email}/catalogs/{catalog_id}",
    response_model=EffectiveAuthorizationResponse,
)
async def get_user_effective_authorization(email: EmailStr, catalog_id: str):
    """Get effective authorization for a user in a specific catalog (admin only)."""
    storage = await get_storage()
    catalog_schema = await resolve_catalog_schema(catalog_id)

    async with http_errors({ValueError: 404}):
        provider, subject_id = await storage.resolve_identity(email)

    if not provider or not subject_id:
        raise HTTPException(status_code=401, detail="Identity missing provider/subject")

    principal = await storage.get_principal_by_identity(
        provider=provider, subject_id=subject_id
    )

    global_roles: List[str] = []
    catalog_roles: List[str] = []
    if principal is not None:
        global_roles = await storage.list_platform_roles(principal_id=principal.id)
        catalog_roles = await storage.list_catalog_roles(
            principal_id=principal.id, catalog_schema=catalog_schema
        )

    # Authorization metadata lives on the platform principal row (D12).
    auth = await storage.get_identity_authorization(provider, subject_id)

    return EffectiveAuthorizationResponse(
        email=email,
        provider=provider,
        subject_id=subject_id,
        global_roles=global_roles,
        catalog_roles=catalog_roles,
        effective_roles=list({*global_roles, *catalog_roles}),
        is_active=auth.get("is_active", True) if auth else True,
        valid_until=auth.get("valid_until") if auth else None,
    )


@router.get("/catalogs/{catalog_id}/users", response_model=List[Dict[str, Any]])
async def get_catalog_users(catalog_id: str):
    """Get all users with access to a catalog (admin only)."""
    storage = await get_storage()
    catalog_schema = await resolve_catalog_schema(catalog_id)

    return await storage.get_catalog_users(catalog_schema=catalog_schema)
