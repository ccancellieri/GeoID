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

import os
from enum import Enum
from typing import Protocol, runtime_checkable

from pydantic import BaseModel, Field

from dynastore.models.protocols.authorization_context import SecurityContext


class Permission(str, Enum):
    SYSADMIN = "sysadmin"
    ADMIN = "admin"
    AUTHENTICATED = "authenticated"


class DefaultRole(str, Enum):
    """Default role-name *constants* used as the IamRoleConfig fallback.

    These are not "concepts" baked into code — they're just the strings
    the platform seeds when an operator hasn't supplied an alternative
    via ``IamRoleConfig``. Consumers MUST NOT reference these
    constants directly for runtime decisions: they should read role
    names from the active ``IamRoleConfig`` (typically attached to
    ``IamService`` / ``IamPolicyService`` / ``DefaultAuthorizer``).
    The enum exists so the defaults are spelled in one place.
    """

    SYSADMIN = "sysadmin"
    ADMIN = "admin"
    USER = "user"
    VIEWER = "viewer"
    ANONYMOUS = "anonymous"


class IamRoleConfig(BaseModel):
    """Operator-overridable role-name configuration consumed by IAM.

    Every field is a **role name** — a foreign key into the
    ``iam.roles`` table that the seed flow creates and runtime checks
    reference. Defaults reproduce the seeded ``DefaultRole`` names so
    existing deployments need do nothing; operators wiring a custom
    landscape construct an instance with their chosen names (or set
    ``IAM_ROLE_*`` env vars) and pass it to ``IamService`` /
    ``IamPolicyService`` / ``DefaultAuthorizer``.

    The seeding flow in
    ``IamPolicyService.provision_default_policies`` reads this config
    when creating the default ``Role`` rows; the same config flows
    through the runtime authorization decisions so seed and runtime
    can never disagree on role names. Idempotent: re-running the seed
    with the same config produces the same DB state (PostgreSQL
    upserts).
    """

    sysadmin: str = Field(
        default_factory=lambda: os.environ.get(
            "IAM_ROLE_SYSADMIN", DefaultRole.SYSADMIN.value
        ),
        description="Role name granting unrestricted platform access.",
    )
    admin: str = Field(
        default_factory=lambda: os.environ.get(
            "IAM_ROLE_ADMIN", DefaultRole.ADMIN.value
        ),
        description="Platform-tier admin role name.",
    )
    user: str = Field(
        default_factory=lambda: os.environ.get(
            "IAM_ROLE_USER", DefaultRole.USER.value
        ),
        description="Default role for any signed-in principal.",
    )
    viewer: str = Field(
        default_factory=lambda: os.environ.get(
            "IAM_ROLE_VIEWER", DefaultRole.VIEWER.value
        ),
        description="Read-only role assigned to newly auto-registered principals.",
    )
    anonymous: str = Field(
        default_factory=lambda: os.environ.get(
            "IAM_ROLE_ANONYMOUS", DefaultRole.ANONYMOUS.value
        ),
        description="Role name representing unauthenticated callers.",
    )

    @property
    def admin_role_set(self) -> frozenset:
        """Frozen set of role names that count as 'platform admin' for
        ``DefaultAuthorizer.Permission.ADMIN`` checks."""
        return frozenset({self.admin, self.sysadmin})


@runtime_checkable
class AuthorizerProtocol(Protocol):
    """Narrow façade used by guards and task-side `require_permission`.

    `check(ctx, perm)` must raise an appropriate error (HTTPException in
    FastAPI surfaces, PermissionError in task surfaces) when the
    permission is denied; returns None on success.
    """

    async def check(self, ctx: SecurityContext, permission: Permission) -> None: ...
