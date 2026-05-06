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

"""Request → `SecurityContext` adapter for FastAPI routes.

Endpoint-level authorization is enforced dynamically by `IamMiddleware` against
the policy registry. Route handlers that still need a permission check against
a specific principal read the context through `security_context_from_request`
and call `require_permission(ctx, Permission.X)` from
`dynastore.modules.iam.authorization`.
"""

from typing import Optional

from fastapi import HTTPException, Request, status

from dynastore.models.auth import Principal
from dynastore.models.protocols.authorization import IamRoleConfig, Permission
from dynastore.models.protocols.authorization_context import SecurityContext
from dynastore.modules.iam.authorization import require_permission


def _privileged_default_roles() -> frozenset[str]:
    """Snapshot of platform-tier privileged role names at call time.

    Reads from the active ``IamRoleConfig`` so deployments that rename
    ``admin`` / ``sysadmin`` (via ``IAM_ROLE_*`` env vars) reflect the
    rename here without code changes.
    """
    cfg = IamRoleConfig()
    return frozenset({cfg.admin, cfg.sysadmin})


def security_context_from_request(request: Request) -> SecurityContext:
    """Assemble a framework-free `SecurityContext` from middleware state."""
    principal = getattr(request.state, "principal", None)
    principal_role = getattr(request.state, "principal_role", None)

    roles: set[str] = set()
    if principal is not None:
        roles.update(getattr(principal, "roles", []) or [])
    if principal_role:
        if isinstance(principal_role, str):
            roles.add(principal_role)
        elif isinstance(principal_role, (list, tuple, set, frozenset)):
            for r in principal_role:
                roles.add(r if isinstance(r, str) else getattr(r, "value", str(r)))
        else:
            roles.add(getattr(principal_role, "value", str(principal_role)))

    principal_id: Optional[str] = None
    if principal is not None:
        principal_id = (
            getattr(principal, "subject_id", None)
            or getattr(principal, "display_name", None)
            or getattr(request.state, "principal_id", None)
        )

    return SecurityContext(
        principal_id=principal_id,
        roles=frozenset(roles),
        policy_allowed=bool(getattr(request.state, "policy_allowed", False)),
    )


def principal_from_request(request: Request) -> Optional[Principal]:
    """Return the middleware-attached `Principal`, or `None` when anonymous."""
    return getattr(request.state, "principal", None)


async def ensure_privileged_role_assignment(
    request: Request,
    target_role: str,
    *,
    protected_roles: Optional[frozenset[str]] = None,
) -> None:
    """Business rule: only sysadmins may assign/manage principals with a
    privileged role. Defaults to protecting the active ``IamRoleConfig``'s
    ``admin``/``sysadmin`` role names; callers can override ``protected_roles``
    when a deployment has added further privileged roles.
    """
    if protected_roles is None:
        protected_roles = _privileged_default_roles()
    if target_role not in protected_roles:
        return
    ctx = security_context_from_request(request)
    try:
        await require_permission(ctx, Permission.SYSADMIN)
    except PermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only System Administrators can manage privileged-role principals.",
        )
