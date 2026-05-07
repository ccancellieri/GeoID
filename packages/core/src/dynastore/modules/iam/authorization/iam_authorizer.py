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
`IamAuthorizer` — concrete `AuthorizerProtocol` registered by `IamModule`
during its lifespan. Reuses the role-based logic in `DefaultAuthorizer`
and is the extension point for future policy-engine delegation.

Importing this file must stay framework-free: no FastAPI, no
`pydantic[email]`, no `PyJWT`. Policy-engine delegation is resolved at
call time via `get_protocol(PermissionProtocol)` to avoid a hard import.
"""

from .default import DefaultAuthorizer


class IamAuthorizer(DefaultAuthorizer):
    """Identical role semantics to `DefaultAuthorizer` today.

    Future work: when a permission maps to a policy-engine rule, delegate
    to `PermissionProtocol.evaluate_access(...)` before falling back to
    role checks. Kept minimal for now so Phase 1 does not change any
    authorization decisions; only the registration path changes.
    """
