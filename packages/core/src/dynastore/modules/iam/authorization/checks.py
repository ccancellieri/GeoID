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

"""
Plain-async permission checks. Used by task handlers that have no
FastAPI `Request`; used internally by FastAPI guards too.
"""

from dynastore.models.protocols.authorization import (
    AuthorizerProtocol,
    Permission,
)
from dynastore.models.protocols.authorization_context import SecurityContext
from dynastore.tools.discovery import get_protocols

from .default import DefaultAuthorizer


async def require_permission(ctx: SecurityContext, permission: Permission) -> None:
    """Resolve the active authorizer and enforce `permission` on `ctx`.

    Returns `None` on success; raises `PermissionError` on denial.

    When `IamModule` is loaded, the first registered `AuthorizerProtocol`
    implementor is used (typically `IamAuthorizer`). Otherwise the
    fail-closed `DefaultAuthorizer` enforces role-only checks.
    """
    authorizer: AuthorizerProtocol = next(
        iter(get_protocols(AuthorizerProtocol)), DefaultAuthorizer()
    )
    await authorizer.check(ctx, permission)
