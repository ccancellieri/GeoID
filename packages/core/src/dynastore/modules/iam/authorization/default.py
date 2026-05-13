#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License").

"""
`DefaultAuthorizer` — fail-closed sentinel used when no `IamAuthorizer`
is registered (e.g. running with `scope_catalog`, no IAM module loaded).

Grants privileged access only if the middleware already attached the
matching role to the `SecurityContext`. Otherwise raises `PermissionError`.
Public endpoints never reach an authorizer because guards are opt-in.

`SecurityContext.policy_allowed` is intentionally NOT honoured here: it
signals only that the global path-level policy filter in
`IamMiddleware` did not explicitly deny the request. That is necessary
but not sufficient for role-gated endpoints — otherwise any principal
carrying a broad ALLOW policy (e.g. a baseline `user` role with
`resource=".*"`) would bypass the role-level checks for sysadmin and admin.
Role enforcement must be independent of path-level policy decisions.

Role *names* are read from a constructor-supplied ``IamRolesConfig`` so
deployments that rename the platform super-user role wire it through
without code changes.
"""

from typing import Optional

from dynastore.models.protocols.authorization import (
    IamRolesConfig,
    Permission,
)
from dynastore.models.protocols.authorization_context import SecurityContext


class DefaultAuthorizer:
    """Minimal role-based authorizer. Reads only `SecurityContext.roles`."""

    def __init__(self, role_config: Optional[IamRolesConfig] = None) -> None:
        self._role_config = role_config or IamRolesConfig()

    async def check(self, ctx: SecurityContext, permission: Permission) -> None:
        cfg = self._role_config
        if permission is Permission.AUTHENTICATED:
            if ctx.principal_id or ctx.roles:
                return
            raise PermissionError("Authentication required.")
        if permission is Permission.ADMIN:
            if ctx.roles & cfg.admin_role_set:
                return
            raise PermissionError("Administrative privileges required.")
        if permission is Permission.SYSADMIN:
            if cfg.sysadmin_role_name in ctx.roles:
                return
            raise PermissionError("System administrator privileges required.")
        raise PermissionError(f"Unknown permission: {permission!r}")
