#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""
Authorization protocol — narrow, framework-free permission façade.

Used by both extensions (via FastAPI `Depends` wrappers in
`dynastore.extensions.iam.guards`) and tasks (via
`dynastore.modules.iam.authorization.checks.require_permission`).

When `IamModule` is loaded, an `IamAuthorizer` is registered via
`register_plugin` and returned by `get_protocols(AuthorizerProtocol)`.
When it is not loaded, the `DefaultAuthorizer` sentinel takes over and
fails closed on privileged checks.
"""

from enum import Enum
from typing import ClassVar, List, Optional, Protocol, Tuple, runtime_checkable

from pydantic import BaseModel, Field, model_validator

from dynastore.extensions.tools.exposure_mixin import ExposableConfigMixin
from dynastore.models.protocols.authorization_context import SecurityContext
from dynastore.modules.db_config.platform_config_service import Mutable, PluginConfig


class Permission(str, Enum):
    SYSADMIN = "sysadmin"
    ADMIN = "admin"
    AUTHENTICATED = "authenticated"


class RoleSeed(BaseModel):
    """A single role to seed at startup.

    Operators add a role by appending a ``RoleSeed`` to either
    ``IamRolesConfig.platform_roles`` (seeded once in the global ``iam``
    schema) or ``IamRolesConfig.catalog_roles`` (seeded per-catalog in
    each tenant schema) via ``PATCH /api/catalog/v2/configs?plugin_id=…``
    — no code change.

    ``parent`` names another role whose policies this role should inherit
    via the role-hierarchy table. Parent inherits child's policies (see
    ``get_role_hierarchy`` in ``postgres_iam_storage.py``); the chain
    flows admin → editor → user → unauthenticated so an authenticated
    ``user`` automatically sees every public policy bound to
    ``unauthenticated``.
    """

    name: Mutable[str] = Field(min_length=1)
    description: str = ""
    policies: Mutable[List[str]] = Field(default_factory=list)
    level: int = 50
    parent: Optional[str] = None


_DEFAULT_PLATFORM_ROLES: List[RoleSeed] = [
    RoleSeed(
        name="sysadmin",
        description="System Administrator with full platform access.",
        policies=["sysadmin_full_access"],
        level=100,
        parent=None,
    ),
]


_DEFAULT_CATALOG_ROLES: List[RoleSeed] = [
    RoleSeed(
        name="admin",
        description="Tenant administrator — manages roles, grants, and members.",
        policies=[],
        level=100,
        parent=None,
    ),
    RoleSeed(
        name="editor",
        description="Catalog editor — creates and updates content.",
        policies=["self_service_access"],
        level=50,
        parent="admin",
    ),
    RoleSeed(
        name="user",
        description="Default role for any authenticated user.",
        policies=["self_service_access"],
        level=10,
        parent="editor",
    ),
    RoleSeed(
        name="unauthenticated",
        description="Read-only floor for anonymous (unauthenticated) requests.",
        policies=["public_access"],
        level=0,
        parent="user",
    ),
]


class IamRolesConfig(ExposableConfigMixin, PluginConfig):
    """Runtime-editable role landscape, split by tier.

    ``platform_roles`` are seeded once in the global ``iam`` schema and
    are NOT grantable per-catalog (e.g. ``sysadmin``). ``catalog_roles``
    are seeded per-catalog and are the set surfaced by
    ``GET /admin/roles?catalog_id=X`` for the catalog grant UI.

    Operators rename or add roles via the standard PluginConfig
    endpoints; a runtime PATCH propagates without redeploy. Only
    ``sysadmin_role_name`` is load-bearing for external conventions —
    the Keycloak claim mapping in
    :mod:`dynastore.modules.iam.oidc_role_sync_config` references the
    sysadmin role name to grant platform-tier admin to a Keycloak-side
    ``geoid.sysadmin`` realm role.

    ``default_user_role_name`` is the role assigned to a newly-registered
    principal when no realm roles are carried in their token.
    ``anonymous_role_name`` is the role attached to requests that arrive
    with no token at all — kept as an explicit slot so the policy engine
    can treat every request uniformly (every request has at least one
    role, no special-case branch). Per geoid#643, the default catalog-tier
    role for unauthenticated requests is ``"unauthenticated"`` (renamed
    from the historical ``"anonymous"``).
    """

    _address: ClassVar[Tuple[str, ...]] = ("platform", "iam", "roles")

    sysadmin_role_name: Mutable[str] = Field(
        default="sysadmin",
        description=(
            "Role name granting unrestricted platform access. "
            "Load-bearing: the OIDC role-sync default maps "
            "``geoid.sysadmin`` → this name. Must resolve to a "
            "``platform_roles`` entry."
        ),
    )
    admin_role_name: Mutable[str] = Field(
        default="admin",
        description=(
            "Catalog-tier admin role name (used by extension policy bindings). "
            "Must resolve to a ``catalog_roles`` entry."
        ),
    )
    editor_role_name: Mutable[str] = Field(
        default="editor",
        description=(
            "Catalog-tier content-editor role name (used by extension policy "
            "bindings). Must resolve to a ``catalog_roles`` entry."
        ),
    )
    default_user_role_name: Mutable[str] = Field(
        default="user",
        description=(
            "Role assigned to any signed-in principal with no realm roles. "
            "Must resolve to a ``catalog_roles`` entry."
        ),
    )
    anonymous_role_name: Mutable[str] = Field(
        default="unauthenticated",
        description=(
            "Role attached to requests that arrive without a token. "
            "Renamed from 'anonymous' per geoid#643. Must resolve to a "
            "``catalog_roles`` entry."
        ),
    )
    admin_tier_role_names: Mutable[List[str]] = Field(
        default_factory=lambda: ["sysadmin", "admin"],
        description=(
            "Role names that satisfy a ``Permission.ADMIN`` check. Spans "
            "tiers by design: ``sysadmin`` is platform-tier and ``admin`` "
            "is catalog-tier. Operators add a 'super-editor' role to this "
            "list to grant it admin-tier authority without renaming."
        ),
    )
    platform_roles: Mutable[List[RoleSeed]] = Field(
        default_factory=lambda: list(_DEFAULT_PLATFORM_ROLES),
        description=(
            "Roles seeded once in the platform-global ``iam`` schema. NOT "
            "surfaced as grantable in ``GET /admin/roles?catalog_id=X``. "
            "The seed is idempotent — existing rows are not removed if "
            "dropped from this list."
        ),
    )
    catalog_roles: Mutable[List[RoleSeed]] = Field(
        default_factory=lambda: list(_DEFAULT_CATALOG_ROLES),
        description=(
            "Roles seeded per-catalog in each tenant schema. Surfaced by "
            "``GET /admin/roles?catalog_id=X`` for the grant UI. The seed "
            "is idempotent — existing rows are not removed if dropped "
            "from this list."
        ),
    )

    @property
    def admin_role_set(self) -> frozenset:
        """Frozen set of role names treated as 'platform admin' for
        ``Permission.ADMIN`` checks."""
        return frozenset(self.admin_tier_role_names)

    @property
    def role_names(self) -> List[str]:
        """Names of every seeded role across both tiers — used to filter
        incoming realm_roles and to enumerate the known landscape."""
        return [r.name for r in self.platform_roles] + [
            r.name for r in self.catalog_roles
        ]

    @property
    def platform_role_names(self) -> frozenset:
        """Names of platform-tier roles (not grantable per-catalog).

        Used by ``GET /admin/roles?catalog_id=X`` to filter out
        platform-only roles from the catalog grant UI.
        """
        return frozenset(r.name for r in self.platform_roles)

    @model_validator(mode="after")
    def _validate_hierarchy_and_slots(self) -> "IamRolesConfig":
        platform_names = {r.name for r in self.platform_roles}
        catalog_names = {r.name for r in self.catalog_roles}
        if platform_names & catalog_names:
            raise ValueError(
                f"IamRolesConfig: role name(s) {sorted(platform_names & catalog_names)} "
                f"appear in BOTH platform_roles and catalog_roles. Names must be "
                f"globally unique across tiers."
            )

        # Cycle detection on parent links, per tier.
        def _check_cycles(seeds: List[RoleSeed], tier: str) -> None:
            names = {s.name for s in seeds}
            parent_of = {s.name: s.parent for s in seeds if s.parent}
            for start in names:
                seen = set()
                cur: Optional[str] = start
                while cur is not None:
                    if cur in seen:
                        raise ValueError(
                            f"IamRolesConfig.{tier}_roles contains a cycle "
                            f"through {cur!r}"
                        )
                    seen.add(cur)
                    nxt = parent_of.get(cur)
                    if nxt is not None and nxt not in names:
                        raise ValueError(
                            f"IamRolesConfig.{tier}_roles: role {cur!r} has "
                            f"parent={nxt!r} which is not in {tier}_roles. "
                            f"Cross-tier parents are not allowed."
                        )
                    cur = nxt

        _check_cycles(self.platform_roles, "platform")
        _check_cycles(self.catalog_roles, "catalog")

        # Slot integrity — slots must point at a real role IN THE EXPECTED TIER.
        for slot, value in (("sysadmin_role_name", self.sysadmin_role_name),):
            if value not in platform_names:
                raise ValueError(
                    f"IamRolesConfig.{slot}={value!r} is not present in "
                    f"`platform_roles` ({sorted(platform_names)})"
                )
        for slot, value in (
            ("admin_role_name", self.admin_role_name),
            ("editor_role_name", self.editor_role_name),
            ("default_user_role_name", self.default_user_role_name),
            ("anonymous_role_name", self.anonymous_role_name),
        ):
            if value not in catalog_names:
                raise ValueError(
                    f"IamRolesConfig.{slot}={value!r} is not present in "
                    f"`catalog_roles` ({sorted(catalog_names)})"
                )

        all_names = platform_names | catalog_names
        missing_admin = [n for n in self.admin_tier_role_names if n not in all_names]
        if missing_admin:
            raise ValueError(
                f"IamRolesConfig.admin_tier_role_names contains unknown "
                f"role(s): {missing_admin}"
            )
        return self


@runtime_checkable
class AuthorizerProtocol(Protocol):
    """Narrow façade used by guards and task-side `require_permission`.

    `check(ctx, perm)` must raise an appropriate error (HTTPException in
    FastAPI surfaces, PermissionError in task surfaces) when the
    permission is denied; returns None on success.
    """

    async def check(self, ctx: SecurityContext, permission: Permission) -> None: ...
