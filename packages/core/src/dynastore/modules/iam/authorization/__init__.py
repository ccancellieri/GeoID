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
Framework-free authorization façade housed inside the IAM module.

**Always importable.** This submodule has zero heavy dependencies and may
be imported and used by any module or extension, regardless of whether
`module_iam` extras are installed or `IamModule` is in scope.

- Protocol contract: `dynastore.models.protocols.authorization.AuthorizerProtocol`
- FastAPI wrappers: `dynastore.extensions.iam.guards`
- Plain-async task checks: `dynastore.modules.iam.authorization.checks`

See `CLAUDE.md` next to this file for the forbidden-imports rule.
"""

from dynastore.models.protocols.authorization import AuthorizerProtocol, Permission
from dynastore.models.protocols.authorization_context import SecurityContext

from .checks import require_permission
from .default import DefaultAuthorizer
from .iam_authorizer import IamAuthorizer

__all__ = [
    "AuthorizerProtocol",
    "Permission",
    "SecurityContext",
    "DefaultAuthorizer",
    "IamAuthorizer",
    "require_permission",
]
