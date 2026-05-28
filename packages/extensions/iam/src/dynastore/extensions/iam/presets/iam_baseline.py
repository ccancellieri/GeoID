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

"""IAM baseline preset — platform policies + role bindings.

Converts the IAM extension's own ``PolicyContributor`` declarations
(``iam_service_policies`` / ``iam_service_role_bindings``) into a
reversible ``Preset``. Applying this preset seeds the IAM service's own
auth-API policies and wires delegation so catalog admins can apply safe
presets at their scope. Revoking removes exactly those rows without
touching shared roles (``sysadmin``, ``admin``, ``user``).

This preset's ``apply`` call also updates the ``admin_catalog_access``
policy's ``catalog_admin_required`` condition to populate
``required_roles`` from ``params.delegation_role_names`` — fixing the
empty-list gap in the default admin-extension declaration.

PR-2 of umbrella #1412.
"""
from __future__ import annotations

import logging
from typing import ClassVar, List, Tuple, Type

from pydantic import BaseModel, Field

from dynastore.models.auth import Condition, Policy
from dynastore.models.auth_models import Role
from dynastore.models.protocols.authorization import IamRolesConfig
from dynastore.modules.storage.presets.preset import (
    AppliedDescriptor,
    PresetContext,
    PresetPlan,
    PresetPlanEntry,
)
from dynastore.modules.storage.presets.protocol import PresetTier

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Params
# ---------------------------------------------------------------------------

class IamBaselineParams(BaseModel):
    """Parameters for the ``iam_baseline`` preset.

    ``delegation_role_names`` populates the ``admin_catalog_access``
    policy's ``catalog_admin_required`` condition so catalog admins can
    manage their own catalog presets. The default ``["admin"]`` matches
    the seeded ``admin`` role name from ``IamRolesConfig``.

    ``allowed_preset_names`` is the safe-subset allowlist for catalog-
    scoped preset delegation. It is injected into the
    ``catalog_preset_delegation`` policy's ``catalog_admin_required``
    condition. The default ``["public_catalog", "private_catalog"]``
    matches the two catalog-scopable shape presets; composites and
    security-sensitive routing presets (e.g. ``items_es_private``) are
    intentionally OUT — they fan out to children or pin global drivers
    that the catalog admin should not necessarily authorise. Sysadmin
    and platform-grant principals bypass the allowlist.
    """

    delegation_role_names: List[str] = Field(
        default_factory=lambda: ["admin"],
        description=(
            "Role names that gain catalog-scoped preset delegation. "
            "Injected into admin_catalog_access's catalog_admin_required "
            "condition. An empty list leaves catalog delegation disabled."
        ),
    )
    allowed_preset_names: List[str] = Field(
        default_factory=lambda: ["public_catalog", "private_catalog"],
        description=(
            "Safe-subset allowlist for catalog-scoped preset delegation. "
            "Empty list disables all catalog-scoped preset POST/DELETE for "
            "the role; sysadmin/platform bypass remains."
        ),
    )


# ---------------------------------------------------------------------------
# Pure-data helpers (extracted from IamExtension.get_policies /
# get_role_bindings so the preset can call them without the contributor loop)
# ---------------------------------------------------------------------------

def _iam_service_policies() -> List[Policy]:
    """Pure declaration of the IAM extension's own service policies."""
    return [
        Policy(
            id="sysadmin_full_access",
            description="Unrestricted access for system administrators.",
            actions=["*"],
            resources=[".*"],
            effect="ALLOW",
            partition_key="global",
        ),
        Policy(
            id="admin_authorization_api",
            description="Allows admin users to manage user roles and permissions",
            actions=["GET", "POST", "PUT", "DELETE", "PATCH"],
            resources=["/admin/principals/.*", "/admin/roles/.*", "/admin/policies/.*"],
            effect="ALLOW",
            partition_key="global",
        ),
        Policy(
            id="self_service_authorization_api",
            description=(
                "Allows authenticated users to view their own roles and catalog access"
            ),
            actions=["GET"],
            resources=["/iam/me", "/iam/me/.*", "/auth/userinfo"],
            effect="ALLOW",
            partition_key="global",
        ),
    ]


def _iam_service_role_bindings() -> List[Role]:
    """Pure declaration of IAM extension role bindings."""
    cfg = IamRolesConfig()
    return [
        Role(name=cfg.admin_role_name, policies=["admin_authorization_api"]),
        # sysadmin_full_access must be included here so that update_role
        # (which replaces the policies list entirely) retains it alongside
        # the admin_authorization_api binding that this preset introduces.
        Role(name=cfg.sysadmin_role_name, policies=["sysadmin_full_access", "admin_authorization_api"]),
        Role(name=cfg.default_user_role_name, policies=["self_service_authorization_api"]),
    ]


def _admin_catalog_access_policy(required_roles: List[str]) -> Policy:
    """Return the admin_catalog_access policy with delegation roles populated."""
    return Policy(
        id="admin_catalog_access",
        description=(
            "Per-catalog admin access; admits roles declared in this "
            "policy's catalog_admin_required condition. Sysadmin and "
            "platform-grant principals bypass."
        ),
        actions=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        resources=[r"^/admin/catalogs/[^/]+(/.*)?$"],
        effect="ALLOW",
        conditions=[
            Condition(
                type="catalog_admin_required",
                config={"required_roles": required_roles},
            )
        ],
    )


def _catalog_preset_delegation_policy(
    delegation_role_names: List[str],
    allowed_preset_names: List[str],
) -> Policy:
    """Allow ``delegation_role_names`` to POST/DELETE catalog-scoped presets
    whose ``{name}`` is in ``allowed_preset_names``.

    The resource regex matches every catalog-scoped preset path; the
    ``catalog_admin_required`` condition's ``allowed_preset_names`` config
    key enforces the safe-subset gate. Sysadmin / platform-grant principals
    bypass both the role check and the allowlist (handler-level).
    """
    return Policy(
        id="catalog_preset_delegation",
        description=(
            "Lets catalog-tier admins (roles in required_roles) apply or "
            "revoke a safe subset of presets at their catalog scope. The "
            "allowed_preset_names guard is enforced by the catalog_admin_required "
            "condition handler."
        ),
        actions=["POST", "DELETE"],
        resources=[r"^/admin/catalogs/[^/]+/presets/[^/]+$"],
        effect="ALLOW",
        conditions=[
            Condition(
                type="catalog_admin_required",
                config={
                    "required_roles": delegation_role_names,
                    "allowed_preset_names": allowed_preset_names,
                },
            )
        ],
    )


# Shared roles whose rows must never be deleted by revoke — they are
# owned by the platform seed and other contributors bind to them.
_SHARED_ROLE_NAMES: Tuple[str, ...] = ("sysadmin", "admin", "user")


# ---------------------------------------------------------------------------
# Preset implementation
# ---------------------------------------------------------------------------

class IamBaseline:
    """IAM platform baseline preset.

    Idempotent: ``apply`` upserts each policy and role binding; the
    database ON CONFLICT handles repeated calls safely. ``revoke`` deletes
    only the policy rows this preset introduced; shared roles are left
    intact.
    """

    name: ClassVar[str] = "iam_baseline"
    description: ClassVar[str] = (
        "IAM platform policies + role bindings + catalog-tier admin delegation"
    )
    keywords: ClassVar[Tuple[str, ...]] = ("iam", "platform", "foundational")
    tier: ClassVar[PresetTier] = PresetTier.PLATFORM
    catalog_scopable: ClassVar[bool] = False
    params_model: ClassVar[Type[BaseModel]] = IamBaselineParams
    is_async: ClassVar[bool] = False

    async def dry_run(
        self,
        params: BaseModel,
        scope: str,
        ctx: PresetContext,
    ) -> PresetPlan:
        p = params if isinstance(params, IamBaselineParams) else IamBaselineParams.model_validate(params.model_dump())
        entries = []
        for pol in _iam_service_policies():
            entries.append(PresetPlanEntry(
                kind="upsert_policy",
                target=pol.id,
                detail={"effect": pol.effect, "actions": pol.actions},
            ))
        for role in _iam_service_role_bindings():
            entries.append(PresetPlanEntry(
                kind="upsert_role_binding",
                target=role.name,
                detail={"policies": role.policies},
            ))
        entries.append(PresetPlanEntry(
            kind="upsert_policy",
            target="admin_catalog_access",
            detail={"required_roles": p.delegation_role_names},
        ))
        entries.append(PresetPlanEntry(
            kind="upsert_policy",
            target="catalog_preset_delegation",
            detail={
                "required_roles": p.delegation_role_names,
                "allowed_preset_names": p.allowed_preset_names,
            },
        ))
        return PresetPlan(
            preset_name=self.name,
            scope_key=scope,
            entries=tuple(entries),
        )

    async def apply(
        self,
        params: BaseModel,
        scope: str,
        ctx: PresetContext,
    ) -> AppliedDescriptor:
        p = params if isinstance(params, IamBaselineParams) else IamBaselineParams.model_validate(params.model_dump())

        policy_service = ctx.policy
        iam_service = ctx.iam

        applied_policy_ids: List[str] = []
        applied_role_names: List[str] = []

        # Upsert IAM service policies.
        for pol in _iam_service_policies():
            await policy_service.update_policy(pol)
            applied_policy_ids.append(pol.id)
            logger.debug("iam_baseline: upserted policy %s", pol.id)

        # Upsert the updated admin_catalog_access policy with delegation roles.
        # NOTE: the admin extension's PolicyContributor still declares
        # admin_catalog_access with required_roles=[] and the contributor loop
        # runs on every startup. Until PR-5 removes the contributor loop, this
        # update is reset to required_roles=[] on the next process restart.
        # Operators who need persistent catalog-admin delegation should apply
        # iam_baseline and then NOT restart until PR-5 is in place, or set
        # delegation via the admin extension's role-binding REST API.
        admin_cat_pol = _admin_catalog_access_policy(p.delegation_role_names)
        await policy_service.update_policy(admin_cat_pol)
        applied_policy_ids.append(admin_cat_pol.id)
        logger.debug(
            "iam_baseline: updated admin_catalog_access required_roles=%s",
            p.delegation_role_names,
        )

        # Upsert catalog-scoped preset delegation policy.
        delegation_pol = _catalog_preset_delegation_policy(
            p.delegation_role_names, p.allowed_preset_names
        )
        await policy_service.update_policy(delegation_pol)
        applied_policy_ids.append(delegation_pol.id)
        logger.debug(
            "iam_baseline: upserted policy %s (allowlist=%s)",
            delegation_pol.id,
            p.allowed_preset_names,
        )

        # Upsert role bindings (existing roles get the new policy merged in).
        for role in _iam_service_role_bindings():
            await iam_service.update_role(role)
            applied_role_names.append(role.name)
            logger.debug("iam_baseline: upserted role binding %s", role.name)

        return AppliedDescriptor(payload={
            "policy_ids": applied_policy_ids,
            "role_names": applied_role_names,
            "delegation_role_names": p.delegation_role_names,
            "allowed_preset_names": p.allowed_preset_names,
        })

    async def revoke(
        self,
        applied_descriptor: AppliedDescriptor,
        ctx: PresetContext,
    ) -> None:
        policy_service = ctx.policy
        iam_service = ctx.iam

        payload = applied_descriptor.payload
        policy_ids: List[str] = payload.get("policy_ids", [])
        role_names: List[str] = payload.get("role_names", [])

        # Remove policies introduced by this preset.
        for pid in policy_ids:
            # admin_catalog_access was contributed by the admin extension;
            # revoke resets it to empty required_roles rather than deleting it
            # so the policy row survives for admin extension to manage.
            if pid == "admin_catalog_access":
                reset_pol = _admin_catalog_access_policy([])
                await policy_service.update_policy(reset_pol)
                logger.debug("iam_baseline: reset admin_catalog_access required_roles=[]")
                continue
            deleted = await policy_service.delete_policy(pid)
            logger.debug("iam_baseline: deleted policy %s (found=%s)", pid, deleted)

        # Remove role bindings but leave the shared role rows.
        # For shared roles strip only the policies this preset added;
        # for non-shared roles delete the row entirely.
        iam_policy_ids = {pol.id for pol in _iam_service_policies()}
        existing_roles = {r.name: r for r in (await iam_service.list_roles())}

        for rname in role_names:
            if rname in _SHARED_ROLE_NAMES:
                existing = existing_roles.get(rname)
                if existing is not None:
                    remaining = [p for p in existing.policies if p not in iam_policy_ids]
                    updated = existing.model_copy(update={"policies": remaining})
                    await iam_service.update_role(updated)
                    logger.debug(
                        "iam_baseline: stripped policies from shared role %s", rname
                    )
            else:
                deleted = await iam_service.delete_role(rname)
                logger.debug("iam_baseline: deleted role %s (found=%s)", rname, deleted)
