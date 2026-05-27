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

"""``default_roles_baseline`` preset — platform predefined roles + hierarchy.

Reads the canonical role definitions from
``dynastore.models.protocols.authorization`` and upserts them into the
global ``iam`` schema.  The ``"iam"`` keyword triggers the self-lockout
guard on DELETE.

PR-5 will remove the hardcoded defaults from ``authorization.py`` once
boot-time auto-seed is fully dropped; until then this preset reads from
them as its source of truth.
"""
from __future__ import annotations

import logging
from typing import ClassVar, List, Tuple, Type

from pydantic import BaseModel

from dynastore.models.auth_models import Role
from dynastore.models.protocols.authorization import (
    _DEFAULT_CATALOG_ROLES,
    _DEFAULT_PLATFORM_ROLES,
    RoleSeed,
)
from dynastore.modules.storage.presets.preset import (
    AppliedDescriptor,
    NoParams,
    PresetContext,
    PresetPlan,
    PresetPlanEntry,
)
from dynastore.modules.storage.presets.protocol import PresetTier

logger = logging.getLogger(__name__)

# Hierarchy edges derived from RoleSeed.parent for both tiers.
_ALL_SEEDS: List[RoleSeed] = list(_DEFAULT_PLATFORM_ROLES) + list(_DEFAULT_CATALOG_ROLES)
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

    Upserts ``sysadmin``, ``admin``, ``editor``, ``user``, and
    ``unauthenticated`` plus the role hierarchy chain
    ``admin → editor → user → unauthenticated``.  DELETE removes only
    the roles and edges this apply wrote; operator-added roles are
    preserved.
    """

    name: ClassVar[str] = "default_roles_baseline"
    description: ClassVar[str] = (
        "Platform predefined roles: admin, editor, user, unauthenticated, "
        "plus role hierarchy. Scoped to the global iam schema."
    )
    keywords: ClassVar[Tuple[str, ...]] = ("iam", "platform", "foundational", "roles")
    tier: ClassVar[PresetTier] = PresetTier.PLATFORM
    catalog_scopable: ClassVar[bool] = False
    params_model: ClassVar[Type[BaseModel]] = NoParams
    is_async: ClassVar[bool] = False

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


async def _upsert_role(iam: object, role: Role) -> None:
    """Update role if it exists; create it otherwise."""
    result = await iam.update_role(role)  # type: ignore[union-attr]
    if result is None:
        await iam.create_role(role)  # type: ignore[union-attr]
