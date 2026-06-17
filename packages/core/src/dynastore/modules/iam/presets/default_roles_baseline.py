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

"""``default_roles_baseline`` preset — platform predefined roles.

Canonical declaration of the platform-tier and catalog-tier predefined
roles.  The ``"iam"`` keyword triggers the self-lockout guard on DELETE.

Seeded roles:
  - ``sysadmin``  — platform tier, full access.
  - ``admin``     — catalog tier; operator-grantable to per-catalog
                    admins. Required for catalog-admin UI surfaces.
  - ``unauthenticated`` — anonymous floor; ships with no policy
                    bindings (``public_access_baseline`` unions
                    ``public_access`` in at lifespan).

No hierarchy edges are seeded by default: catalog-tier ``admin`` is a
peer of platform ``sysadmin``, and ``unauthenticated`` MUST NOT inherit
either. Operators can add edges via ``RoleAdminProtocol.add_role_hierarchy``.
"""
from __future__ import annotations

import logging
from typing import ClassVar, List, Tuple, Type

from pydantic import BaseModel

from dynastore.models.auth_models import Role
from dynastore.models.protocols.authorization import RoleSeed
from dynastore.modules.storage.presets.preset import (
    AppliedDescriptor,
    NoParams,
    PresetContext,
    PresetPlan,
    PresetPlanEntry,
)
from dynastore.modules.storage.presets.protocol import PresetTier

logger = logging.getLogger(__name__)


DEFAULT_PLATFORM_ROLES: List[RoleSeed] = [
    RoleSeed(
        name="sysadmin",
        description="System Administrator with full platform access.",
        policies=["sysadmin_full_access"],
        level=100,
        parent=None,
    ),
]


DEFAULT_CATALOG_ROLES: List[RoleSeed] = [
    RoleSeed(
        name="admin",
        description="Tenant administrator — manages roles, grants, and members.",
        policies=[],
        level=100,
        parent=None,
    ),
    RoleSeed(
        name="unauthenticated",
        description="Read-only floor for anonymous (unauthenticated) requests.",
        policies=[],
        level=0,
        parent=None,
    ),
]

# Hierarchy edges derived from RoleSeed.parent for both tiers.
_ALL_SEEDS: List[RoleSeed] = list(DEFAULT_PLATFORM_ROLES) + list(DEFAULT_CATALOG_ROLES)
_HIERARCHY_EDGES: List[Tuple[str, str]] = [
    (s.parent, s.name) for s in _ALL_SEEDS if s.parent
]


def _seeds_to_roles() -> List[Role]:
    return [
        Role(name=s.name, description=s.description, policies=list(s.policies))
        for s in _ALL_SEEDS
    ]


class DefaultRolesBaseline:
    """Platform predefined roles seeded into the global ``iam`` schema.

    Upserts ``sysadmin``, ``admin``, and ``unauthenticated``.  DELETE
    removes only the roles this apply wrote; operator-added roles are
    preserved.
    """

    name: ClassVar[str] = "default_roles_baseline"
    description: ClassVar[str] = (
        "Platform predefined roles: sysadmin, admin, unauthenticated. "
        "Scoped to the global iam schema."
    )
    keywords: ClassVar[Tuple[str, ...]] = ("iam", "platform", "foundational", "roles")
    tier: ClassVar[PresetTier] = PresetTier.PLATFORM
    catalog_scopable: ClassVar[bool] = False
    params_model: ClassVar[Type[BaseModel]] = NoParams

    async def dry_run(
        self,
        params: BaseModel,
        scope: str,
        ctx: PresetContext,
    ) -> PresetPlan:
        entries: List[PresetPlanEntry] = []
        for seed in _ALL_SEEDS:
            entries.append(
                PresetPlanEntry(
                    kind="upsert_role",
                    target=seed.name,
                    detail={
                        "description": seed.description,
                        "policies": list(seed.policies),
                    },
                )
            )
        for parent, child in _HIERARCHY_EDGES:
            entries.append(
                PresetPlanEntry(
                    kind="add_role_hierarchy",
                    target=f"{parent} → {child}",
                    detail={"parent": parent, "child": child},
                )
            )
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
        iam = ctx.iam
        written_roles: List[str] = []
        written_edges: List[Tuple[str, str]] = []

        for role in _seeds_to_roles():
            await _upsert_role(iam, role)
            written_roles.append(role.name)

        for parent, child in _HIERARCHY_EDGES:
            await iam.add_role_hierarchy(parent, child)
            written_edges.append([parent, child])  # type: ignore[arg-type]

        return AppliedDescriptor(
            payload={
                "role_names": written_roles,
                "hierarchy_edges": written_edges,
            }
        )

    async def revoke(
        self,
        applied_descriptor: AppliedDescriptor,
        ctx: PresetContext,
    ) -> None:
        iam = ctx.iam
        role_names: List[str] = applied_descriptor.payload.get("role_names", [])
        edges: List[List[str]] = applied_descriptor.payload.get("hierarchy_edges", [])

        for parent, child in edges:
            try:
                await iam.remove_role_hierarchy(parent, child)
            except Exception as exc:
                logger.warning(
                    "default_roles_baseline revoke: could not remove hierarchy "
                    "%r → %r: %s",
                    parent,
                    child,
                    exc,
                )

        for name in role_names:
            try:
                await iam.delete_role(name)
            except Exception as exc:
                logger.warning(
                    "default_roles_baseline revoke: could not delete role %r: %s",
                    name,
                    exc,
                )

        return None


async def _get_role_by_name(iam: object, role_name: str) -> "Role | None":
    """Fetch a role by name via the IamService list_roles API."""
    roles: List[Role] = await iam.list_roles()  # type: ignore[union-attr]
    return next((r for r in roles if r.name == role_name), None)


async def _upsert_role(iam: object, role: Role) -> None:
    """Ensure the role exists WITHOUT clobbering its policy bindings.

    This preset guarantees the *existence* of the platform role shells
    (sysadmin / admin / unauthenticated); their policy bindings are
    contributed by OTHER presets — e.g. ``public_access_baseline`` unions
    ``public_access`` into ``unauthenticated`` to keep the Cloud Run
    ``/health`` startup probe reachable anonymously, and ``iam_baseline``
    binds the self-service surface. The seed roles carry ``policies=[]``,
    so a blind ``update_role`` would REPLACE the column (full-replace
    semantics) and wipe those bindings — re-applying this preset directly
    or as a composite child would then deny-by-default every anonymous
    request (``/health`` 403, undeployable on Cloud Run). So on an existing
    role we preserve its current policies and metadata and only refresh the
    description.
    """
    existing = await _get_role_by_name(iam, role.name)
    if existing is None:
        await iam.create_role(role)  # type: ignore[union-attr]
        return
    if list(existing.policies) == list(role.policies) and existing.metadata == role.metadata:
        return
    merged = role.model_copy(
        update={"policies": list(existing.policies), "metadata": dict(existing.metadata)}
    )
    await iam.update_role(merged)  # type: ignore[union-attr]
