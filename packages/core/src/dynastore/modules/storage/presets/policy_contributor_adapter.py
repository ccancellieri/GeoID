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

"""Generic adapter that turns any ``PolicyContributor`` into a ``Preset``.

Each extension that ships route-level policies implements the structural
``PolicyContributor`` protocol (``get_policies`` / ``get_role_bindings``).
This adapter wraps the contributor in the generalised ``Preset`` lifecycle
so operators can ``POST /admin/presets/{ext}_enable`` instead of relying
on boot-time automatic seeding.

The ``contributor_factory`` callable is invoked at ``apply`` / ``revoke``
/ ``dry_run`` time, not at registration time, so the contributor class is
constructed inside the preset call — avoiding module-load ordering
surprises and ensuring each call operates on a fresh instance.

PR-3 of umbrella #1412.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, ClassVar, List, Tuple, Type, cast

from pydantic import BaseModel

from dynastore.models.protocols.policy_contributor import PolicyContributor

from .preset import (
    AppliedDescriptor,
    NoParams,
    PresetContext,
    PresetPlan,
    PresetPlanEntry,
)
from .protocol import PresetTier

logger = logging.getLogger(__name__)

# Shared role names that belong to the platform seed and must never be
# deleted by a per-extension preset revoke — only the policy links are
# stripped.
_SHARED_ROLE_NAMES: Tuple[str, ...] = ("sysadmin", "admin", "user", "anonymous")


class PolicyContributorPreset:
    """Generic adapter: ``PolicyContributor`` instance → ``Preset``.

    Wraps any object that satisfies the structural ``PolicyContributor``
    protocol (``get_policies() -> Iterable[Policy]``,
    ``get_role_bindings() -> Iterable[Role]``) and exposes it as a
    reversible platform-tier preset.

    ``contributor_factory`` is called on every ``apply`` / ``revoke`` /
    ``dry_run`` invocation so the contributor is constructed inside the
    operation call — no state is captured at registration time.

    ``name``, ``description``, ``keywords`` are supplied at construction
    and stored as class-level attributes on the instance (the Preset
    protocol checks class-level attrs, so they are also set on the
    instance's ``__class__`` via ``__init_subclass__``; here they are set
    as instance attributes and the Preset protocol's structural check
    will find them either way because ``isinstance`` checks ``__dict__``
    on runtime_checkable protocols).

    Actually: to satisfy ``Preset`` as a *structural* protocol the attrs
    need to be accessible on the instance.  Python's
    ``runtime_checkable`` ``isinstance`` only checks method presence, not
    ClassVar values, so simple instance attributes work fine.
    """

    tier: ClassVar[PresetTier] = PresetTier.PLATFORM
    catalog_scopable: ClassVar[bool] = False
    params_model: ClassVar[Type[BaseModel]] = NoParams
    is_async: ClassVar[bool] = False

    def __init__(
        self,
        *,
        name: str,
        description: str,
        keywords: Tuple[str, ...],
        contributor_factory: "Callable[[], Any]",
    ) -> None:
        self.name = name
        self.description = description
        self.keywords = keywords
        self._contributor_factory = contributor_factory

    # ------------------------------------------------------------------
    # Public read-only property so the lifespan narrowing check can
    # inspect which contributor class this preset wraps.
    # ------------------------------------------------------------------

    @property
    def contributor_class(self) -> type:
        """The class produced by ``contributor_factory()``."""
        return type(self._contributor_factory())

    # ------------------------------------------------------------------
    # Preset methods
    # ------------------------------------------------------------------

    async def dry_run(
        self,
        params: BaseModel,
        scope: str,
        ctx: PresetContext,
    ) -> PresetPlan:
        contributor: PolicyContributor = cast(PolicyContributor, self._contributor_factory())
        entries: List[PresetPlanEntry] = []

        for pol in (contributor.get_policies() or []):
            entries.append(PresetPlanEntry(
                kind="upsert_policy",
                target=pol.id,
                detail={"effect": pol.effect, "actions": pol.actions},
            ))

        for role in (contributor.get_role_bindings() or []):
            entries.append(PresetPlanEntry(
                kind="upsert_role_binding",
                target=role.name,
                detail={"policies": role.policies},
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
        contributor: PolicyContributor = cast(PolicyContributor, self._contributor_factory())

        applied_policy_ids: List[str] = []
        applied_role_names: List[str] = []

        for pol in (contributor.get_policies() or []):
            await ctx.policy.update_policy(pol)
            applied_policy_ids.append(pol.id)
            logger.debug("%s: upserted policy %s", self.name, pol.id)

        for role in (contributor.get_role_bindings() or []):
            await ctx.iam.update_role(role)
            applied_role_names.append(role.name)
            logger.debug("%s: upserted role binding %s", self.name, role.name)

        return AppliedDescriptor(payload={
            "preset_name": self.name,
            "policy_ids": applied_policy_ids,
            "role_names": applied_role_names,
        })

    async def revoke(
        self,
        applied_descriptor: AppliedDescriptor,
        ctx: PresetContext,
    ) -> None:
        payload = applied_descriptor.payload
        policy_ids: List[str] = payload.get("policy_ids", [])
        role_names: List[str] = payload.get("role_names", [])

        # Collect the set of policy IDs this preset introduced so that
        # when stripping shared roles we only remove the relevant bindings.
        own_policy_ids = set(policy_ids)

        # Reverse role bindings first, then delete orphan policies.
        # Stripping shared roles before policy deletion ensures no role
        # references a policy that has already been removed.
        existing_roles = {r.name: r for r in (await ctx.iam.list_roles())}

        for rname in role_names:
            if rname in _SHARED_ROLE_NAMES:
                existing = existing_roles.get(rname)
                if existing is not None:
                    remaining = [p for p in existing.policies if p not in own_policy_ids]
                    updated = existing.model_copy(update={"policies": remaining})
                    await ctx.iam.update_role(updated)
                    logger.debug(
                        "%s: stripped policies from shared role %s", self.name, rname
                    )
            else:
                deleted = await ctx.iam.delete_role(rname)
                logger.debug(
                    "%s: deleted role %s (found=%s)", self.name, rname, deleted
                )

        # Delete policies introduced by this preset after role bindings are cleared.
        for pid in policy_ids:
            deleted = await ctx.policy.delete_policy(pid)
            logger.debug("%s: deleted policy %s (found=%s)", self.name, pid, deleted)
