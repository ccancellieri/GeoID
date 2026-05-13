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
from typing import ClassVar, List, Protocol, Tuple, runtime_checkable

from pydantic import BaseModel, Field, model_validator

from dynastore.extensions.tools.exposure_mixin import ExposableConfigMixin
from dynastore.models.protocols.authorization_context import SecurityContext
from dynastore.modules.db_config.platform_config_service import PluginConfig


class Permission(str, Enum):
    SYSADMIN = "sysadmin"
    ADMIN = "admin"
    AUTHENTICATED = "authenticated"


class RoleSeed(BaseModel):
    """A single role to seed at startup.

    Operators add a 6th role by appending a ``RoleSeed`` to
    ``IamRolesConfig.roles`` via ``PATCH /api/catalog/v2/configs?plugin_id=…``
    — no code change. ``is_platform_tier=False`` flags roles that should be
    seeded per-catalog instead of in the platform-global ``iam`` schema.
    """

    name: str = Field(min_length=1)
    description: str = ""
    policies: List[str] = Field(default_factory=list)
    is_platform_tier: bool = True


_DEFAULT_ROLES: List[RoleSeed] = [
    RoleSeed(
        name="sysadmin",
        description="System Administrator with full access.",
        policies=["sysadmin_full_access"],
    ),
    RoleSeed(
        name="admin",
        description="Administrator with full access.",
        policies=["sysadmin_full_access"],
    ),
    RoleSeed(
        name="editor",
        description="Content editor with write access below admin.",
        policies=["self_service_access"],
    ),
    RoleSeed(
        name="user",
        description="Default role for any authenticated user.",
        policies=["self_service_access"],
    ),
    RoleSeed(
        name="anonymous",
        description="Anonymous user with limited access.",
        policies=["public_access"],
    ),
]

_DEFAULT_HIERARCHY: List[Tuple[str, str]] = [
    # parent → child; parent inherits child's policies.
    ("sysadmin", "admin"),
    ("admin",    "editor"),
    ("editor",   "user"),
    ("user",     "anonymous"),
]


class IamRolesConfig(ExposableConfigMixin, PluginConfig):
    """Runtime-editable platform role landscape.

    The full role set the IAM module knows about lives here. Operators
    rename or add roles via the standard PluginConfig endpoints; a runtime
    PATCH propagates without redeploy. Only ``sysadmin_role_name`` is
    load-bearing for external conventions — the Keycloak claim mapping
    in :mod:`dynastore.modules.iam.oidc_role_sync_config` references the
    sysadmin role name to grant platform-tier admin to a Keycloak-side
    ``geoid.sysadmin`` realm role.

    ``default_user_role_name`` is the role assigned to a newly-registered
    principal when no realm roles are carried in their token.
    ``anonymous_role_name`` is the role attached to requests that arrive
    with no token at all — kept as an explicit slot so the policy engine
    can treat every request uniformly (every request has at least one
    role, no special-case branch).
    """

    _address: ClassVar[Tuple[str, ...]] = ("platform", "iam", "roles")

    sysadmin_role_name: str = Field(
        default="sysadmin",
        description=(
            "Role name granting unrestricted platform access. "
            "Load-bearing: the OIDC role-sync default maps "
            "``geoid.sysadmin`` → this name."
        ),
    )
    admin_role_name: str = Field(
        default="admin",
        description="Platform-tier admin role name (used by extension policy bindings).",
    )
    editor_role_name: str = Field(
        default="editor",
        description="Content-editor role name (used by extension policy bindings).",
    )
    default_user_role_name: str = Field(
        default="user",
        description="Role assigned to any signed-in principal with no realm roles.",
    )
    anonymous_role_name: str = Field(
        default="anonymous",
        description="Role attached to requests that arrive without a token.",
    )
    admin_tier_role_names: List[str] = Field(
        default_factory=lambda: ["admin", "sysadmin"],
        description=(
            "Role names that satisfy a ``Permission.ADMIN`` check. "
            "Operators add a 'super-editor' role to this list to grant it "
            "admin-tier authority without renaming."
        ),
    )
    roles: List[RoleSeed] = Field(
        default_factory=lambda: list(_DEFAULT_ROLES),
        description=(
            "Roles seeded at startup. The seed is idempotent — existing "
            "rows are not removed if dropped from this list."
        ),
    )
    hierarchy: List[Tuple[str, str]] = Field(
        default_factory=lambda: list(_DEFAULT_HIERARCHY),
        description=(
            "Role-inheritance edges (parent, child). Parent inherits every "
            "policy bound to child. Cycles are rejected on validation."
        ),
    )

    @property
    def admin_role_set(self) -> frozenset:
        """Frozen set of role names treated as 'platform admin' for
        ``Permission.ADMIN`` checks."""
        return frozenset(self.admin_tier_role_names)

    @property
    def role_names(self) -> List[str]:
        """Names of every seeded role — used to filter incoming realm_roles
        and to enumerate the known landscape."""
        return [r.name for r in self.roles]

    @model_validator(mode="after")
    def _validate_hierarchy_and_slots(self) -> "IamRolesConfig":
        names = {r.name for r in self.roles}
        # Cycle detection on the hierarchy DAG.
        adj: dict[str, list[str]] = {}
        for parent, child in self.hierarchy:
            adj.setdefault(parent, []).append(child)
        WHITE, GRAY, BLACK = 0, 1, 2
        color: dict[str, int] = {n: WHITE for n in names}
        for parent, child in self.hierarchy:
            color.setdefault(parent, WHITE)
            color.setdefault(child, WHITE)

        def dfs(node: str) -> None:
            if color[node] == GRAY:
                raise ValueError(
                    f"IamRolesConfig.hierarchy contains a cycle through {node!r}"
                )
            if color[node] == BLACK:
                return
            color[node] = GRAY
            for nxt in adj.get(node, []):
                dfs(nxt)
            color[node] = BLACK

        for node in list(color):
            if color[node] == WHITE:
                dfs(node)

        # Slot integrity — slots must point at a real role.
        for slot, value in (
            ("sysadmin_role_name", self.sysadmin_role_name),
            ("admin_role_name", self.admin_role_name),
            ("editor_role_name", self.editor_role_name),
            ("default_user_role_name", self.default_user_role_name),
            ("anonymous_role_name", self.anonymous_role_name),
        ):
            if value not in names:
                raise ValueError(
                    f"IamRolesConfig.{slot}={value!r} is not present in "
                    f"`roles` ({sorted(names)})"
                )
        missing_admin = [
            n for n in self.admin_tier_role_names if n not in names
        ]
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
