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
from typing import TYPE_CHECKING, Literal, Optional
from uuid import UUID

if TYPE_CHECKING:
    from dynastore.modules.storage.presets import PresetTier

from fastapi import FastAPI, APIRouter, HTTPException, Query, Request
from contextlib import asynccontextmanager

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.modules import get_protocol
from dynastore.modules.iam.iam_service import IamService
from dynastore.models.protocols.authorization import IamRolesConfig
from dynastore.models.protocols.catalogs import CatalogsProtocol
from dynastore.models.protocols.policies import (
    Policy, Role, Principal,
    RoleCreate, RoleUpdate, RoleResponse,
    PrincipalResponse, AssignRoleRequest, CreateBindingRequest,
    PermissionProtocol,
)

from dynastore.extensions.iam.guards import ensure_privileged_role_assignment

from dynastore.models.protocols.policies import (
    PolicyCreate, PolicyUpdate, PolicyResponse,
)

from .models import (
    PrincipalCreate, PrincipalUpdate,
    UsagePage, UsageResetResponse, UsageRow,
    CatalogProvisioningView, ProvisioningTaskView,
)
from .policies import admin_policies, admin_role_bindings

logger = logging.getLogger(__name__)


def _policy_to_response(p: Policy) -> PolicyResponse:
    """Project a domain :class:`Policy` onto the wire :class:`PolicyResponse`.

    Module-level because the three callers are FastAPI route handlers
    nested inside :class:`AdminService`; Python's name resolution does
    not let nested functions see class-body names, so a class-body
    helper would (and did) raise ``NameError`` at request time.
    """
    return PolicyResponse(
        id=p.id,
        description=p.description,
        actions=p.actions,
        resources=p.resources,
        effect=p.effect,
        priority=p.priority,
        conditions=getattr(p, "conditions", []) or [],
    )


def _iam() -> IamService:
    mgr = get_protocol(IamService)
    if mgr is None:
        raise HTTPException(status_code=503, detail="Auth service not available.")
    return mgr


async def _is_catalog_only_admin(request: Request) -> bool:
    """True iff the caller reaches an admin route via the ``catalog_admin``
    sentinel binding and not via any platform-scope ALLOW.

    Used by routes that must narrow their response shape (or refuse
    enumeration) when a catalog admin reaches a platform-scope endpoint —
    e.g. ``GET /admin/principals`` is opened to catalog admins for target
    lookup before a catalog-scope role grant (#723 follow-up), but must
    refuse to enumerate the directory.

    Returns ``False`` for anonymous callers, sysadmin/admin role-holders,
    principals with any platform-scope grant, and any caller the IAM
    layer cannot resolve to a stable identity. The membership lookup
    reuses the per-pod 60s cache, so this is a cheap secondary check on
    the same critical path that already evaluated the policy.

    Load-bearing invariant: the role-name check on
    ``principal.roles`` (line below) assumes the IAM layer publishes
    *platform-tier* roles only on the principal object — catalog-tier
    grants live on ``request.state.principal_role`` as sentinels (see
    ``IamMiddleware._augment_with_catalog_sentinels`` and the contract
    comment in ``packages/extensions/iam/.../middleware.py:287``). If a
    future change ever lets catalog-scope role names appear in this
    flat list, this helper silently regresses to ``False`` for catalog
    admins and the principal-lookup gate disappears.
    """
    from dynastore.models.protocols.authorization import IamRolesConfig
    from dynastore.models.protocols.membership_cache import MembershipCacheProtocol

    principal = getattr(request.state, "principal", None)
    if principal is None:
        return False

    cfg = IamRolesConfig()
    principal_roles = set(getattr(principal, "roles", None) or [])
    if cfg.sysadmin_role_name in principal_roles or cfg.admin_role_name in principal_roles:
        return False

    provider = getattr(principal, "provider", None)
    subject_id = getattr(principal, "subject_id", None)
    if not provider or not subject_id:
        return False

    cache = get_protocol(MembershipCacheProtocol)
    if cache is None:
        # MembershipCacheProtocol unregistered (slim deploy without the
        # IAM extension) — admin cannot make the platform-vs-catalog
        # distinction so we conservatively report False (treat the caller
        # as not a catalog-only admin). The caller's existing policy
        # gate has already authorized them.
        return False

    membership = await cache.get_membership(provider, subject_id)
    if membership.get("platform"):
        return False
    return True


async def _catalog_admin_filter_ids(request: Request) -> Optional[set]:
    """Return the set of catalog ids a non-platform-admin caller may see in
    the admin catalog picker (#723), or ``None`` if no filter applies.

    ``None`` means "return everything" — the caller is sysadmin, holds the
    platform admin role, or has an explicit platform-scope grant. Returning
    a set (possibly empty) means the response should be restricted to
    catalogs in which the caller holds the catalog-tier admin role.

    Anonymous calls (no principal) also return ``None``; the policy layer
    has already gated the route, so anyone who reaches this code without a
    principal carries an authoritative ALLOW (e.g. operator overrides).

    Load-bearing invariant: same as ``_is_catalog_only_admin`` — the
    role-name check on ``principal.roles`` assumes the IAM layer
    publishes *platform-tier* roles only on the principal object
    (catalog-tier admin is exposed as a sentinel on
    ``request.state.principal_role``; see ``IamMiddleware``'s
    ``_augment_with_catalog_sentinels`` and the contract comment in
    ``packages/extensions/iam/.../middleware.py:287``). If a future
    change lets catalog-scope role names appear in this flat list, this
    helper silently regresses to "no filter" and catalog admins see the
    full picker again.
    """
    from dynastore.models.protocols.authorization import IamRolesConfig
    from dynastore.models.protocols.membership_cache import MembershipCacheProtocol

    principal = getattr(request.state, "principal", None)
    if principal is None:
        return None

    cfg = IamRolesConfig()
    principal_roles = set(getattr(principal, "roles", None) or [])
    if cfg.sysadmin_role_name in principal_roles or cfg.admin_role_name in principal_roles:
        return None

    provider = getattr(principal, "provider", None)
    subject_id = getattr(principal, "subject_id", None)
    if not provider or not subject_id:
        return set()

    cache = get_protocol(MembershipCacheProtocol)
    if cache is None:
        # MembershipCacheProtocol unregistered (no IAM extension loaded) —
        # cannot derive a catalog filter; fail closed to an empty set so
        # the picker shows nothing rather than the full directory.
        return set()

    membership = await cache.get_membership(provider, subject_id)
    if membership.get("platform"):
        return None

    admin_role = cfg.admin_role_name
    catalog_roles = membership.get("catalog_roles") or {}
    return {
        cat_id
        for cat_id, roles in catalog_roles.items()
        if admin_role in (roles or [])
    }


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


async def _assert_collection_exists(catalog_id: str, collection_id: str) -> None:
    """Raise 404 if ``collection_id`` does not exist under ``catalog_id``.

    Collection-scope preset endpoints write collection-tier config rows;
    an operator typo for the collection segment must fail loudly rather
    than seed orphan config under a non-existent collection.
    """
    catalogs = get_protocol(CatalogsProtocol)
    if catalogs is None:
        return
    collection = await catalogs.collections.get_collection(catalog_id, collection_id)
    if collection is None:
        raise HTTPException(
            status_code=404,
            detail=f"Collection '{collection_id}' not found in catalog '{catalog_id}'.",
        )


def _preset_reachable_at(preset, url_tier: "PresetTier") -> bool:
    """Whether ``preset`` may be applied at the ``url_tier`` URL family.

    Single-family tiers (``PLATFORM`` / ``CATALOG`` / ``COLLECTION``) match
    only their own URL family. ``ITEMS`` / ``ASSETS`` presets bind to the
    collection family always and additionally to the catalog family when
    ``catalog_scopable`` is set. Mismatches are surfaced as HTTP 409 by
    the caller — the preset exists but is not valid at that scope.
    """
    from dynastore.modules.storage.presets import PresetTier

    preset_tier = getattr(preset, "tier", None)
    if preset_tier == url_tier:
        return True
    if preset_tier in (PresetTier.ITEMS, PresetTier.ASSETS):
        if url_tier == PresetTier.COLLECTION:
            return True
        if url_tier == PresetTier.CATALOG:
            return bool(getattr(preset, "catalog_scopable", False))
    return False


def _resolve_preset_for_scope(preset_name: str, url_tier: "PresetTier"):
    """Look the preset up and verify it is reachable at ``url_tier``.

    Returns the preset on success. Raises 404 when unknown, 409 when the
    URL scope does not match the preset's declared tier.
    """
    from dynastore.modules.storage.presets import PresetTier, get_preset

    try:
        preset = get_preset(preset_name)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    if not _preset_reachable_at(preset, url_tier):
        preset_tier = getattr(preset, "tier", None)
        tier_label = preset_tier.value if isinstance(preset_tier, PresetTier) else str(preset_tier)
        raise HTTPException(
            status_code=409,
            detail=(
                f"Preset '{preset_name}' is a {tier_label}-tier preset and "
                f"cannot be applied at the {url_tier.value} URL scope."
            ),
        )
    return preset


async def _apply_preset_bundle(preset, base_scope: dict) -> dict:
    """Walk a preset bundle through ``ConfigsProtocol.set_config``.

    ``base_scope`` is the URL-derived scope (``{}`` / ``{catalog_id}`` /
    ``{catalog_id, collection_id}``); the preset's own per-entry scope is
    layered on top so a bundle can mix tiers. Returns the response body.
    """
    from dynastore.models.protocols.configs import ConfigsProtocol

    configs = get_protocol(ConfigsProtocol)
    if configs is None:
        raise HTTPException(status_code=503, detail="Configs service unavailable.")

    bundle = preset.build(**base_scope)
    applied: list[str] = []
    for entry in bundle.iter_apply():
        scope = {**base_scope, **dict(entry.scope)}
        await configs.set_config(entry.config_cls, entry.instance, **scope)
        applied.append(entry.slot)
    return {"preset": preset.name, "applied": applied, **base_scope}


async def _unapply_preset_bundle(preset, base_scope: dict) -> dict:
    """Rollback a preset bundle, leaf-first, with 409 on divergence.

    Mirrors the catalog-tier rollback contract (#971): a persisted row
    that no longer matches the preset's emitted instance blocks the whole
    rollback with 409 and nothing is deleted; missing rows are no-ops.
    """
    from dynastore.models.protocols.configs import ConfigsProtocol

    configs = get_protocol(ConfigsProtocol)
    if configs is None:
        raise HTTPException(status_code=503, detail="Configs service unavailable.")

    bundle = preset.build(**base_scope)
    diverged: list[dict] = []
    to_delete: list[tuple[str, type, dict]] = []

    for entry in bundle.iter_rollback():
        scope = {**base_scope, **dict(entry.scope)}
        persisted = await configs.get_persisted_config(entry.config_cls, **scope)
        if persisted is None:
            continue
        try:
            persisted_norm = entry.config_cls.model_validate(persisted).model_dump(mode="json")
        except Exception:  # noqa: BLE001 — surface the raw payload on validation failure
            persisted_norm = persisted
        expected_norm = entry.instance.model_dump(mode="json")
        if persisted_norm == expected_norm:
            to_delete.append((entry.slot, entry.config_cls, scope))
        else:
            diverged.append({
                "slot": entry.slot,
                "class": entry.config_cls.__name__,
                "persisted": persisted_norm,
                "expected": expected_norm,
            })

    if diverged:
        raise HTTPException(
            status_code=409,
            detail={
                "message": (
                    f"Preset '{preset.name}' cannot be rolled back: "
                    f"{len(diverged)} slot(s) diverge from the preset bundle."
                ),
                "diverged": diverged,
            },
        )

    deleted: list[str] = []
    for slot_name, cfg_cls, scope in to_delete:
        await configs.delete_config(cfg_cls, **scope)
        deleted.append(slot_name)
    return {"preset": preset.name, "deleted": deleted, **base_scope}


class AdminService(ExtensionProtocol):
    always_on = True
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
    # Principal Management (/admin/principals)
    # -------------------------------------------------------------------------

    @router.get(
        "/principals",
        summary="List or search principals (filterable by provider, identifier, role, catalog)",
    )
    async def list_principals(
        request: Request,  # type: ignore[reportGeneralTypeIssues]
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
        # Catalog-only admins reach this route via the admin_principal_lookup
        # policy so they can resolve a target subject_id before granting a
        # catalog-scope role; they MUST NOT enumerate the platform principal
        # directory. Require a non-empty q so the response is always scoped
        # to a search the caller already had a value for (#723 follow-up).
        if await _is_catalog_only_admin(request) and not (q and q.strip()):
            raise HTTPException(
                status_code=400,
                detail=(
                    "Catalog-tier admins must provide a non-empty 'q' query "
                    "parameter to search principals; directory enumeration "
                    "is restricted to platform admins."
                ),
            )
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

    @router.post("/principals", summary="Create a principal (local user or raw)", status_code=201)
    async def create_principal(request: Request, body: PrincipalCreate):  # type: ignore[reportGeneralTypeIssues]
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

    @router.get("/principals/{principal_id}", summary="Get principal details")
    async def get_principal(principal_id: UUID):  # type: ignore[reportGeneralTypeIssues]
        mgr = _iam()
        p = await mgr.get_principal(principal_id)
        if not p:
            raise HTTPException(status_code=404, detail="User not found.")
        granted = await mgr.storage.list_platform_roles(principal_id=p.id)
        return PrincipalResponse(
            id=str(p.id), provider=p.provider, subject_id=p.subject_id,
            display_name=p.display_name, roles=list(granted), is_active=p.is_active,
        )

    @router.put("/principals/{principal_id}", summary="Update principal")
    async def update_principal(request: Request, principal_id: UUID, body: PrincipalUpdate):  # type: ignore[reportGeneralTypeIssues]
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

    @router.delete("/principals/{principal_id}", status_code=204, summary="Delete principal")
    async def delete_principal(request: Request, principal_id: UUID):  # type: ignore[reportGeneralTypeIssues]
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
        request: Request,  # type: ignore[reportGeneralTypeIssues]
        limit: int = Query(200, ge=1, le=1000),  # type: ignore[reportGeneralTypeIssues]
        offset: int = Query(0, ge=0),
        lang: str = Query("en"),
        q: Optional[str] = Query(None, description="Free-text partial match on id/title/description"),
    ):
        catalogs_svc = get_protocol(CatalogsProtocol)
        if catalogs_svc is None:
            raise HTTPException(status_code=503, detail="Catalogs service not available.")

        admin_only_ids = await _catalog_admin_filter_ids(request)
        items = await catalogs_svc.list_catalogs(limit=limit, offset=offset, lang=lang, q=q)
        if admin_only_ids is not None:
            items = [c for c in items if c.id in admin_only_ids]
        out = []
        for c in items:
            title_raw = c.model_dump(mode="json").get("title")
            if isinstance(title_raw, dict):
                title = title_raw.get(lang) or next(iter(title_raw.values()), None)
            else:
                title = title_raw
            out.append({"id": c.id, "title": title or c.id})
        return out

    @router.get(
        "/catalogs/{catalog_id}",
        response_model=CatalogProvisioningView,
        summary="Sysadmin view of catalog provisioning status and most-recent provision task",
    )
    async def get_catalog_provisioning_view(catalog_id: str):  # type: ignore[reportGeneralTypeIssues]
        from dynastore.modules.tasks import tasks_module

        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            raise HTTPException(status_code=503, detail="Catalogs service not available.")
        catalog = await catalogs.get_catalog_model(catalog_id)
        if catalog is None:
            raise HTTPException(status_code=404, detail=f"Catalog '{catalog_id}' not found.")

        provisioning_status = getattr(catalog, "provisioning_status", "ready") or "ready"

        try:
            physical_schema = await catalogs.resolve_physical_schema(catalog_id, allow_missing=True)
        except Exception:
            physical_schema = None

        task_view: Optional[ProvisioningTaskView] = None
        if physical_schema:
            from dynastore.models.protocols import DatabaseProtocol
            from dynastore.modules.db_config.query_executor import managed_transaction

            db = get_protocol(DatabaseProtocol)
            if db is not None:
                try:
                    async with managed_transaction(db.engine) as conn:
                        tasks = await tasks_module.list_tasks(
                            conn, schema=physical_schema, limit=20, offset=0,
                        )
                    provision_tasks = [t for t in tasks if t.task_type == "gcp_provision_catalog"]
                    if provision_tasks:
                        t = sorted(
                            provision_tasks,
                            key=lambda x: x.finished_at or x.timestamp,
                            reverse=True,
                        )[0]
                        task_view = ProvisioningTaskView(
                            task_id=t.jobID,
                            status=t.status.value if hasattr(t.status, "value") else str(t.status),
                            error_message=t.error_message,
                            retry_count=t.retry_count,
                            max_retries=t.max_retries,
                            created_at=t.timestamp,
                            updated_at=t.finished_at,
                        )
                except Exception:
                    logger.warning(
                        "Failed to query provision tasks for catalog %s schema %s",
                        catalog_id, physical_schema,
                        exc_info=True,
                    )

        return CatalogProvisioningView(
            catalog_id=catalog_id,
            physical_schema=physical_schema,
            provisioning_status=provisioning_status,
            task=task_view,
        )

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
        # Privilege-escalation guard at CATALOG scope: only sysadmins may
        # grant a platform-tier admin role. Catalog-tier admin grants stay
        # open to catalog admins (this is the #723 use case — a catalog
        # admin appointing a colleague as co-admin of the same catalog).
        # The role-registry split (line below) already rejects platform-tier
        # role names at the catalog endpoint; this guard is a paranoid
        # second layer for deployments that extend admin_tier_role_names
        # with a platform-tier name.
        await ensure_privileged_role_assignment(
            request, body.role,
            protected_roles=IamRolesConfig().platform_admin_tier_role_set,
        )
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
        # Privilege-escalation guard at CATALOG scope: same narrowing as
        # the grant path — only platform-tier admin role names are blocked
        # here, catalog-tier admin revokes stay open to catalog admins.
        await ensure_privileged_role_assignment(
            request, role_name,
            protected_roles=IamRolesConfig().platform_admin_tier_role_set,
        )
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

    @router.get(
        "/catalogs/{catalog_id}/principals",
        summary="List principals assigned to a catalog",
    )
    async def list_catalog_principals(catalog_id: str):  # type: ignore[reportGeneralTypeIssues]
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

    # ---- Platform / catalog scope bindings (#1346 — same generic binding
    #      shape as the collection-scope endpoint below, but the resource
    #      dimension is fixed to NULL — i.e. whole-platform / whole-catalog).
    #      Adding these unblocks the Admin UI from the "role-allow only"
    #      ceiling of the legacy `/admin/.../roles` endpoints: operators can
    #      now author `effect=deny` grants, `valid_from`/`valid_until` time
    #      windows, direct `object_kind=policy` bindings, and per-binding
    #      `quota` at platform / catalog scope without dropping to SQL.
    #
    #      The legacy `/admin/platform/principals/{pid}/roles` and
    #      `/admin/catalogs/{cid}/principals/{pid}/roles` endpoints stay as
    #      backcompat wrappers (allow-only role grant); both write the same
    #      `iam.grants` / `{cat}.grants` row a binding here would write with
    #      defaults, so the two surfaces are exchangeable for existing
    #      callers and identical at the storage layer.

    @router.post(
        "/platform/grants",
        status_code=201,
        summary="Create a platform-scope binding (role|policy) for a principal",
    )
    async def create_platform_binding(
        request: Request,  # type: ignore[reportGeneralTypeIssues]
        body: CreateBindingRequest,
    ):
        mgr = _iam()
        if body.object_kind == "role":
            # Same escalation gate the legacy `/platform/.../roles` POST applies.
            await ensure_privileged_role_assignment(request, body.object_ref)
        p = await mgr.get_principal(body.subject_id)
        if not p:
            raise HTTPException(status_code=404, detail="Principal not found.")
        if body.object_kind == "role":
            registered = await mgr.list_roles(catalog_id=None)
            if not any(r.name == body.object_ref for r in registered):
                raise HTTPException(
                    status_code=422,
                    detail=(
                        f"Role '{body.object_ref}' is not registered in the "
                        f"platform role registry."
                    ),
                )
        else:
            perm = get_protocol(PermissionProtocol)
            pol = (
                await perm.get_policy(body.object_ref, catalog_id=None)
                if perm is not None
                else None
            )
            if pol is None:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        f"Policy '{body.object_ref}' not found at platform scope."
                    ),
                )
        granted_by = getattr(getattr(request.state, "principal", None), "id", None)
        try:
            grant_id = await mgr.storage.grant(
                scope_schema="iam",
                subject_kind="principal",
                subject_ref=str(body.subject_id),
                object_kind=body.object_kind,
                object_ref=body.object_ref,
                effect=body.effect,
                valid_from=body.valid_from,
                valid_until=body.valid_until,
                quota=body.quota,
                granted_by=granted_by,
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return {
            "id": str(grant_id) if grant_id else None,
            "subject_id": str(body.subject_id),
            "object_kind": body.object_kind,
            "object_ref": body.object_ref,
            "effect": body.effect,
            "resource_kind": None,
            "resource_ref": None,
        }

    @router.get(
        "/platform/grants",
        summary="List a principal's platform-scope bindings",
    )
    async def list_platform_grants(
        principal_id: UUID = Query(  # type: ignore[reportGeneralTypeIssues]
            ..., description="Principal whose platform-scope bindings to list."
        ),
    ):
        mgr = _iam()
        p = await mgr.get_principal(principal_id)
        if not p:
            raise HTTPException(status_code=404, detail="Principal not found.")
        return await mgr.storage.list_grants_for_subject(
            scope_schema="iam",
            subject_kind="principal",
            subject_ref=str(principal_id),
        )

    @router.delete(
        "/platform/grants",
        status_code=204,
        summary="Revoke a platform-scope binding (by match)",
    )
    async def revoke_platform_binding(
        request: Request,  # type: ignore[reportGeneralTypeIssues]
        subject_id: UUID = Query(..., description="Principal whose binding is revoked."),
        object_kind: Literal["role", "policy"] = Query(...),
        object_ref: str = Query(...),
        effect: Literal["allow", "deny"] = Query("allow"),
    ):
        mgr = _iam()
        if object_kind == "role":
            await ensure_privileged_role_assignment(request, object_ref)
        try:
            await mgr.storage.revoke_by_match(
                scope_schema="iam",
                subject_kind="principal",
                subject_ref=str(subject_id),
                object_kind=object_kind,
                object_ref=object_ref,
                effect=effect,
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @router.post(
        "/catalogs/{catalog_id}/grants",
        status_code=201,
        summary="Create a catalog-scope binding (role|policy) for a principal",
    )
    async def create_catalog_binding(
        request: Request,  # type: ignore[reportGeneralTypeIssues]
        catalog_id: str,
        body: CreateBindingRequest,
    ):
        mgr = _iam()
        if body.object_kind == "role":
            # Match `grant_catalog_role`'s narrowed guard: platform-tier names
            # are blocked here, catalog-tier admin grants stay open to catalog
            # admins (the #723 "appoint a co-admin of the same catalog" path).
            await ensure_privileged_role_assignment(
                request, body.object_ref,
                protected_roles=IamRolesConfig().platform_admin_tier_role_set,
            )
        await _assert_catalog_exists(catalog_id)
        p = await mgr.get_principal(body.subject_id)
        if not p:
            raise HTTPException(status_code=404, detail="Principal not found.")
        if body.object_kind == "role":
            registered = await mgr.list_roles(catalog_id=catalog_id)
            if not any(r.name == body.object_ref for r in registered):
                raise HTTPException(
                    status_code=422,
                    detail=(
                        f"Role '{body.object_ref}' is not registered for "
                        f"catalog '{catalog_id}'."
                    ),
                )
        else:
            perm = get_protocol(PermissionProtocol)
            pol = (
                await perm.get_policy(body.object_ref, catalog_id=catalog_id)
                if perm is not None
                else None
            )
            if pol is None:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        f"Policy '{body.object_ref}' not found for "
                        f"catalog '{catalog_id}'."
                    ),
                )
        granted_by = getattr(getattr(request.state, "principal", None), "id", None)
        try:
            grant_id = await mgr.storage.grant(
                scope_schema=await mgr.resolve_schema(catalog_id),
                subject_kind="principal",
                subject_ref=str(body.subject_id),
                object_kind=body.object_kind,
                object_ref=body.object_ref,
                effect=body.effect,
                valid_from=body.valid_from,
                valid_until=body.valid_until,
                quota=body.quota,
                granted_by=granted_by,
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return {
            "id": str(grant_id) if grant_id else None,
            "subject_id": str(body.subject_id),
            "object_kind": body.object_kind,
            "object_ref": body.object_ref,
            "effect": body.effect,
            "resource_kind": None,
            "resource_ref": None,
        }

    @router.get(
        "/catalogs/{catalog_id}/grants",
        summary="List a principal's catalog-scope bindings",
    )
    async def list_catalog_grants(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        principal_id: UUID = Query(
            ..., description="Principal whose catalog-scope bindings to list."
        ),
    ):
        mgr = _iam()
        await _assert_catalog_exists(catalog_id)
        p = await mgr.get_principal(principal_id)
        if not p:
            raise HTTPException(status_code=404, detail="Principal not found.")
        return await mgr.storage.list_grants_for_subject(
            scope_schema=await mgr.resolve_schema(catalog_id),
            subject_kind="principal",
            subject_ref=str(principal_id),
        )

    @router.delete(
        "/catalogs/{catalog_id}/grants",
        status_code=204,
        summary="Revoke a catalog-scope binding (by match)",
    )
    async def revoke_catalog_binding(
        request: Request,  # type: ignore[reportGeneralTypeIssues]
        catalog_id: str,
        subject_id: UUID = Query(..., description="Principal whose binding is revoked."),
        object_kind: Literal["role", "policy"] = Query(...),
        object_ref: str = Query(...),
        effect: Literal["allow", "deny"] = Query("allow"),
    ):
        mgr = _iam()
        if object_kind == "role":
            await ensure_privileged_role_assignment(
                request, object_ref,
                protected_roles=IamRolesConfig().platform_admin_tier_role_set,
            )
        await _assert_catalog_exists(catalog_id)
        try:
            await mgr.storage.revoke_by_match(
                scope_schema=await mgr.resolve_schema(catalog_id),
                subject_kind="principal",
                subject_ref=str(subject_id),
                object_kind=object_kind,
                object_ref=object_ref,
                effect=effect,
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    # ---- Collection-scope bindings (#1342 — generic role|policy grant
    #      with effect / validity / per-binding quota, scoped to a
    #      collection via the unified `grants` table) -----------------------
    #
    # These are the write path that activates the dormant resource-scope
    # (#1341) and per-binding quota (#1344) features: until an operator
    # authors a collection-scoped binding here, the resolver has nothing
    # scoped to enforce. The resource scope (collection) comes from the URL,
    # never the body, so a request can only bind within the path it targets.

    @router.post(
        "/catalogs/{catalog_id}/collections/{collection_id}/grants",
        status_code=201,
        summary="Create a collection-scoped binding (role|policy) for a principal",
    )
    async def create_collection_binding(
        request: Request,  # type: ignore[reportGeneralTypeIssues]
        catalog_id: str,
        collection_id: str,
        body: CreateBindingRequest,
    ):
        mgr = _iam()
        # Role bindings carry privilege-escalation risk — gate exactly like
        # the catalog-scope role grant (platform-tier names blocked; catalog
        # admins may still bind catalog-tier roles within their own catalog).
        # Policy bindings are not role escalations and the route is already
        # admin-gated, so no extra guard there.
        if body.object_kind == "role":
            await ensure_privileged_role_assignment(
                request, body.object_ref,
                protected_roles=IamRolesConfig().platform_admin_tier_role_set,
            )
        await _assert_catalog_exists(catalog_id)
        await _assert_collection_exists(catalog_id, collection_id)
        p = await mgr.get_principal(body.subject_id)
        if not p:
            raise HTTPException(status_code=404, detail="Principal not found.")
        # The bound object must exist in this catalog scope.
        if body.object_kind == "role":
            registered = await mgr.list_roles(catalog_id=catalog_id)
            if not any(r.name == body.object_ref for r in registered):
                raise HTTPException(
                    status_code=422,
                    detail=(
                        f"Role '{body.object_ref}' is not registered for "
                        f"catalog '{catalog_id}'."
                    ),
                )
        else:
            perm = get_protocol(PermissionProtocol)
            pol = (
                await perm.get_policy(body.object_ref, catalog_id=catalog_id)
                if perm is not None
                else None
            )
            if pol is None:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        f"Policy '{body.object_ref}' not found for "
                        f"catalog '{catalog_id}'."
                    ),
                )
        granted_by = getattr(getattr(request.state, "principal", None), "id", None)
        try:
            grant_id = await mgr.storage.grant(
                scope_schema=await mgr.resolve_schema(catalog_id),
                subject_kind="principal",
                subject_ref=str(body.subject_id),
                object_kind=body.object_kind,
                object_ref=body.object_ref,
                effect=body.effect,
                valid_from=body.valid_from,
                valid_until=body.valid_until,
                quota=body.quota,
                granted_by=granted_by,
                resource_kind="collection",
                resource_ref=collection_id,
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return {
            "id": str(grant_id) if grant_id else None,
            "subject_id": str(body.subject_id),
            "object_kind": body.object_kind,
            "object_ref": body.object_ref,
            "effect": body.effect,
            "resource_kind": "collection",
            "resource_ref": collection_id,
        }

    @router.get(
        "/catalogs/{catalog_id}/collections/{collection_id}/grants",
        summary="List collection-scoped bindings (by principal, or reverse who-has-access)",
    )
    async def list_collection_bindings(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        collection_id: str,
        principal_id: Optional[UUID] = Query(
            None,
            description="Filter to one principal's bindings effective in this collection.",
        ),
    ):
        mgr = _iam()
        await _assert_catalog_exists(catalog_id)
        await _assert_collection_exists(catalog_id, collection_id)
        schema = await mgr.resolve_schema(catalog_id)
        if principal_id is not None:
            rows = await mgr.storage.list_grants_for_subject(
                scope_schema=schema,
                subject_kind="principal",
                subject_ref=str(principal_id),
            )
            # A binding applies to this collection if it is scoped here or is
            # catalog-wide (resource_kind NULL) — mirrors the resolver's
            # additive scope semantics.
            return [
                r for r in rows
                if r.get("resource_ref") == collection_id
                or r.get("resource_kind") is None
            ]
        # Reverse view: every principal bound on this specific collection.
        return await mgr.storage.list_grants_for_resource(
            scope_schema=schema,
            resource_kind="collection",
            resource_ref=collection_id,
        )

    @router.delete(
        "/catalogs/{catalog_id}/collections/{collection_id}/grants",
        status_code=204,
        summary="Revoke a collection-scoped binding (by match)",
    )
    async def revoke_collection_binding(
        request: Request,  # type: ignore[reportGeneralTypeIssues]
        catalog_id: str,
        collection_id: str,
        subject_id: UUID = Query(..., description="Principal whose binding is revoked."),
        object_kind: Literal["role", "policy"] = Query(...),
        object_ref: str = Query(...),
        effect: Literal["allow", "deny"] = Query("allow"),
    ):
        mgr = _iam()
        if object_kind == "role":
            await ensure_privileged_role_assignment(
                request, object_ref,
                protected_roles=IamRolesConfig().platform_admin_tier_role_set,
            )
        await _assert_catalog_exists(catalog_id)
        await _assert_collection_exists(catalog_id, collection_id)
        try:
            await mgr.storage.revoke_by_match(
                scope_schema=await mgr.resolve_schema(catalog_id),
                subject_kind="principal",
                subject_ref=str(subject_id),
                object_kind=object_kind,
                object_ref=object_ref,
                effect=effect,
                resource_kind="collection",
                resource_ref=collection_id,
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

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
        return [_policy_to_response(p) for p in policies]

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
            priority=body.priority,
            conditions=body.conditions,
        )
        try:
            created = await pm.create_policy(policy, catalog_id=catalog_id)
        except ValueError as e:
            raise HTTPException(status_code=409, detail=str(e))
        return _policy_to_response(created)

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
        if body.priority is not None:
            existing.priority = body.priority
        if body.conditions is not None:
            existing.conditions = body.conditions
        updated = await pm.update_policy(existing, catalog_id=catalog_id)
        if updated is None:
            raise HTTPException(status_code=404, detail="Policy not found after update.")
        return _policy_to_response(updated)

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
    # Usage Inspection (/admin/policies/{policy_id}/usage)
    # -------------------------------------------------------------------------

    @router.get(
        "/policies/{policy_id}/usage",
        summary="List rate-limit / quota counter rows for a policy",
        response_model=UsagePage,
    )
    async def list_policy_usage(  # type: ignore[reportGeneralTypeIssues]
        policy_id: str,
        catalog_id: Optional[str] = Query(None),
        limit: int = Query(100, ge=1, le=500),
        offset: int = Query(0, ge=0),
    ):
        from dynastore.modules.iam.usage_counter_pg import PostgresUsageCounter

        # Use the PG driver directly for inspection — the layered driver
        # delegates list_for_policy to PG anyway (Valkey doesn't support
        # efficient SCAN-by-prefix at scale).
        pg = PostgresUsageCounter()
        rows = await pg.list_for_policy(policy_id, limit=limit + 1, offset=offset)
        next_offset = None
        if len(rows) > limit:
            rows = rows[:limit]
            next_offset = offset + limit
        return UsagePage(
            policy_id=policy_id,
            rows=[
                UsageRow(
                    principal_key=str(r["principal_key"]),
                    count=int(r["count"]),
                    window_start=r["window_start"].isoformat()
                    if hasattr(r["window_start"], "isoformat")
                    else str(r["window_start"]),
                    expires_at=(
                        r["expires_at"].isoformat()
                        if r.get("expires_at") and hasattr(r["expires_at"], "isoformat")
                        else (str(r["expires_at"]) if r.get("expires_at") else None)
                    ),
                    last_seen_at=(
                        r["last_seen_at"].isoformat()
                        if r.get("last_seen_at") and hasattr(r["last_seen_at"], "isoformat")
                        else (str(r["last_seen_at"]) if r.get("last_seen_at") else None)
                    ),
                )
                for r in rows
            ],
            next_offset=next_offset,
        )

    @router.delete(
        "/policies/{policy_id}/usage/{principal_key}",
        summary="Reset (renew) the counter row for a (policy, principal) pair",
        response_model=UsageResetResponse,
    )
    async def reset_policy_usage(  # type: ignore[reportGeneralTypeIssues]
        policy_id: str,
        principal_key: str,
        request: Request,
        catalog_id: Optional[str] = Query(None),
        window_seconds: Optional[int] = Query(
            None,
            description=(
                "Window width that originally produced the bucket — "
                "omit for lifetime quotas (max_count). The handler "
                "floors ``now`` to derive the live bucket and resets "
                "exactly that row."
            ),
        ),
    ):
        # Authorization is enforced router-side by IamMiddleware on
        # every /admin/* path — no extra guard call is needed here.
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.usage_counter import UsageCounterProtocol
        from dynastore.modules.iam.usage_counter_pg import PostgresUsageCounter

        # Reset on the live counter (Valkey-backed layered driver clears
        # both tiers) so the next request hits a fresh bucket.
        counter = get_protocol(UsageCounterProtocol) or PostgresUsageCounter()

        before = await counter.get(
            policy_id, principal_key, window_seconds=window_seconds
        )
        await counter.reset(
            policy_id, principal_key, window_seconds=window_seconds
        )

        # Audit: quota reset is a high-trust operator action (clears an
        # exhausted lifetime quota or zeroes a windowed limit). Failure
        # to record must NOT block the reset, but must be visible — log
        # at WARNING so it surfaces, matching the oidc_role_sync pattern.
        actor_id = getattr(request.state, "principal_id", None)
        client_ip = request.client.host if request.client else None
        try:
            await _iam().storage.log_audit_event(
                event_type="usage_counter_reset",
                principal_id=str(actor_id) if actor_id else None,
                ip_address=client_ip,
                detail={
                    "policy_id": policy_id,
                    "subject_principal_key": principal_key,
                    "window_seconds": window_seconds,
                    "previous_count": int(before or 0),
                    "catalog_id": catalog_id,
                },
            )
        except Exception:
            logger.warning(
                "audit write for usage_counter_reset failed "
                "(actor=%s policy=%s subject_key=%s)",
                actor_id, policy_id, principal_key,
                exc_info=True,
            )

        return UsageResetResponse(
            policy_id=policy_id,
            principal_key=principal_key,
            reset_count=int(before or 0),
        )

    # -------------------------------------------------------------------------
    # Capability stats (/admin/tasks/capability_stats) — #524 Signal A
    # -------------------------------------------------------------------------

    @router.get(
        "/tasks/capability_stats",
        summary="Per-capability dispatcher counters (claim_rejected, dlq_reactive, dlq_proactive)",
    )
    async def get_capability_stats():  # type: ignore[reportGeneralTypeIssues]
        """Returns one row per ``(capability_id, task_type)`` pair that has
        appeared on a PENDING or DEAD_LETTER row of a capability-gated task
        type. Each row carries the three Valkey counters maintained by
        ``capability_stats``: ``claim_rejected``, ``dlq_reactive``,
        ``dlq_proactive``. Counter values are ``None`` when the cache
        backend is unreachable (distinct from 0 = "never incremented in
        the TTL window").

        Gated by the broad ``admin_access`` policy (sysadmin + admin).
        Catalog-tier admins do not reach this surface — capability state
        is platform-wide.
        """
        from dynastore.models.protocols import DatabaseProtocol
        from dynastore.modules.db_config.query_executor import (
            DQLQuery, ResultHandler, managed_transaction,
        )
        from dynastore.modules.tasks.capability_oracle import (
            TASK_TYPE_CAPABILITY_INPUTS_KEY,
        )
        from dynastore.modules.tasks.capability_stats import read_counters
        from dynastore.modules.tasks.tasks_module import get_task_schema

        db = get_protocol(DatabaseProtocol)
        if db is None:
            raise HTTPException(
                status_code=503, detail="Database protocol unavailable.",
            )

        schema = get_task_schema()
        rows_out: list[dict] = []
        async with managed_transaction(db.engine) as conn:
            for task_type, inputs_key in TASK_TYPE_CAPABILITY_INPUTS_KEY.items():
                # ``inputs_key`` is a validated SQL identifier (see comment
                # on TASK_TYPE_CAPABILITY_INPUTS_KEY in capability_oracle.py).
                sql = (
                    f'SELECT DISTINCT inputs->>\'{inputs_key}\' AS cap_id '  # nosec
                    f'FROM "{schema}".tasks '
                    f"WHERE task_type = :task_type "
                    f"  AND status IN ('PENDING', 'DEAD_LETTER') "
                    f"  AND inputs->>'{inputs_key}' IS NOT NULL "
                    f"ORDER BY 1 LIMIT 200;"
                )
                try:
                    cap_rows = await DQLQuery(
                        sql, result_handler=ResultHandler.ALL_DICTS,
                    ).execute(conn, task_type=task_type)
                except Exception as exc:  # noqa: BLE001 — diagnostic endpoint
                    logger.warning(
                        "capability_stats: distinct query failed (task_type=%s): %s",
                        task_type, exc,
                    )
                    continue
                for r in cap_rows or []:
                    cap_id = r.get("cap_id")
                    if not cap_id:
                        continue
                    counters = await read_counters(cap_id, task_type)
                    rows_out.append({
                        "capability_id": cap_id,
                        "task_type": task_type,
                        **counters,
                    })
        return {"rows": rows_out}

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

    # -------------------------------------------------------------------------
    # Routing Presets (#847 / #972) — named, cascade-consistent config
    # bundles operators apply in one call. The registry is a single flat
    # namespace; a preset declares its ``tier`` and the URL family encodes
    # the apply scope. Three URL families, each with POST (apply) / DELETE
    # (rollback, #971) symmetry, dispatching on the preset's tier:
    #
    #   /admin/presets/{name}                                  platform
    #   /admin/catalogs/{cat}/presets/{name}                   catalog
    #   /admin/catalogs/{cat}/collections/{col}/presets/{name} collection
    #
    # Items/assets-tier presets are reachable at the collection family
    # (and at the catalog family when ``catalog_scopable``). A preset
    # applied at a URL family that does not match its tier returns 409.
    # All three families share ``_apply_preset_bundle`` /
    # ``_unapply_preset_bundle`` so apply/rollback semantics stay identical
    # across scopes.
    # -------------------------------------------------------------------------

    @router.get("/presets", summary="List registered routing presets (#847, #972)")
    async def list_routing_presets(
        request: Request,  # type: ignore[reportGeneralTypeIssues]
        tier: Optional[str] = Query(
            None,
            description=(
                "Filter to presets of this tier "
                "(platform / catalog / collection / items / assets)."
            ),
        ),
    ):
        from dynastore.modules.storage.presets import (
            PresetTier,
            get_preset,
            list_presets,
        )

        tier_filter: Optional[PresetTier] = None
        if tier is not None:
            try:
                tier_filter = PresetTier(tier)
            except ValueError as exc:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Unknown tier {tier!r}. Known: "
                        f"{[t.value for t in PresetTier]}"
                    ),
                ) from exc

        out = []
        for name in list_presets(tier_filter):
            preset = get_preset(name)
            entry = {"name": name, "description": preset.description}
            preset_tier = getattr(preset, "tier", None)
            if preset_tier is not None:
                entry["tier"] = (
                    preset_tier.value
                    if hasattr(preset_tier, "value")
                    else str(preset_tier)
                )
            # ``catalog_scopable`` only carries meaning for items/assets
            # presets but is emitted for all so clients can render it
            # uniformly.
            entry["catalog_scopable"] = bool(getattr(preset, "catalog_scopable", False))
            out.append(entry)
        return {"presets": out}

    # ----- Platform tier: /admin/presets/{name} -----------------------------

    @router.post(
        "/presets/{preset_name}",
        summary="Apply a platform-tier routing preset (#972)",
    )
    async def apply_platform_preset(
        request: Request,  # type: ignore[reportGeneralTypeIssues]
        preset_name: str,
    ):
        """Apply a ``PLATFORM``-tier preset (no scope params). Returns 409
        if ``preset_name`` declares a non-platform tier."""
        from dynastore.modules.storage.presets import PresetTier

        preset = _resolve_preset_for_scope(preset_name, PresetTier.PLATFORM)
        return await _apply_preset_bundle(preset, {})

    @router.delete(
        "/presets/{preset_name}",
        summary="Rollback a platform-tier routing preset (#972)",
    )
    async def unapply_platform_preset(
        request: Request,  # type: ignore[reportGeneralTypeIssues]
        preset_name: str,
    ):
        from dynastore.modules.storage.presets import PresetTier

        preset = _resolve_preset_for_scope(preset_name, PresetTier.PLATFORM)
        return await _unapply_preset_bundle(preset, {})

    # ----- Catalog tier: /admin/catalogs/{cat}/presets/{name} ---------------
    # Existing #847/#971 contract. Also reachable by items/assets presets
    # that declare ``catalog_scopable``.

    @router.post(
        "/catalogs/{catalog_id}/presets/{preset_name}",
        summary="Apply a routing preset to a catalog (#847)",
    )
    async def apply_routing_preset(
        request: Request,  # type: ignore[reportGeneralTypeIssues]
        catalog_id: str,
        preset_name: str,
    ):
        """Apply ``preset_name`` to ``catalog_id`` by walking the bundle
        through the standard ``ConfigsProtocol.set_config`` lifecycle.

        Each slot is applied at the catalog tier; the cascade validators
        (#960 scope 4 / items + collection) catch mixed public/private
        combos and the per-config validators run via ``set_config``. The
        endpoint does not bypass any validation — a preset is just a named
        bundle of standard ``set_config`` calls. Returns 409 if the preset
        is not reachable at the catalog scope (e.g. a collection-only
        preset).
        """
        from dynastore.modules.storage.presets import PresetTier

        await _assert_catalog_exists(catalog_id)
        preset = _resolve_preset_for_scope(preset_name, PresetTier.CATALOG)
        return await _apply_preset_bundle(preset, {"catalog_id": catalog_id})

    @router.delete(
        "/catalogs/{catalog_id}/presets/{preset_name}",
        summary="Rollback a routing preset (#971)",
    )
    async def unapply_routing_preset(
        request: Request,  # type: ignore[reportGeneralTypeIssues]
        catalog_id: str,
        preset_name: str,
    ):
        """Rollback ``preset_name`` from ``catalog_id`` when the persisted
        rows still match the preset bundle byte-for-byte.

        Any slot whose persisted row diverges from the preset's emitted
        instance is reported via HTTP 409 with a ``diverged`` payload and
        the endpoint deletes nothing — the operator must reconcile or
        force-PUT before a rollback succeeds. Slots are walked leaf-first
        (items template → collection template → catalog routing →
        audiences). Missing rows are no-ops.
        """
        from dynastore.modules.storage.presets import PresetTier

        await _assert_catalog_exists(catalog_id)
        preset = _resolve_preset_for_scope(preset_name, PresetTier.CATALOG)
        return await _unapply_preset_bundle(preset, {"catalog_id": catalog_id})

    # ----- Collection tier: /admin/catalogs/{cat}/collections/{col}/... -----
    # Reachable by COLLECTION-tier presets and by items/assets presets at
    # collection scope (#972).

    @router.post(
        "/catalogs/{catalog_id}/collections/{collection_id}/presets/{preset_name}",
        summary="Apply a routing preset to a collection (#972)",
    )
    async def apply_collection_preset(
        request: Request,  # type: ignore[reportGeneralTypeIssues]
        catalog_id: str,
        collection_id: str,
        preset_name: str,
    ):
        """Apply ``preset_name`` at the collection scope. Returns 409 if the
        preset is not reachable at the collection scope (e.g. a catalog-tier
        preset)."""
        from dynastore.modules.storage.presets import PresetTier

        await _assert_catalog_exists(catalog_id)
        await _assert_collection_exists(catalog_id, collection_id)
        preset = _resolve_preset_for_scope(preset_name, PresetTier.COLLECTION)
        return await _apply_preset_bundle(
            preset, {"catalog_id": catalog_id, "collection_id": collection_id}
        )

    @router.delete(
        "/catalogs/{catalog_id}/collections/{collection_id}/presets/{preset_name}",
        summary="Rollback a collection routing preset (#972)",
    )
    async def unapply_collection_preset(
        request: Request,  # type: ignore[reportGeneralTypeIssues]
        catalog_id: str,
        collection_id: str,
        preset_name: str,
    ):
        from dynastore.modules.storage.presets import PresetTier

        await _assert_catalog_exists(catalog_id)
        await _assert_collection_exists(catalog_id, collection_id)
        preset = _resolve_preset_for_scope(preset_name, PresetTier.COLLECTION)
        return await _unapply_preset_bundle(
            preset, {"catalog_id": catalog_id, "collection_id": collection_id}
        )
