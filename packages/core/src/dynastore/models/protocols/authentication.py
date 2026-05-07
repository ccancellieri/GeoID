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
`AuthenticatorProtocol` — authentication surface. Framework-agnostic
(uses `Any` for request and identity/principal types to avoid layer
violations between `models/protocols/` and `modules/iam/`).
"""

from typing import Any, Dict, Optional, Protocol, runtime_checkable


@runtime_checkable
class AuthenticatorProtocol(Protocol):
    """Authenticate a caller and resolve the effective Principal/roles."""

    async def authenticate_and_get_principal(
        self,
        identity: Dict[str, Any],
        target_schema: str,
        auto_register: bool = False,
    ) -> Any:
        """Authenticates an identity and returns a principal with permissions."""
        ...

    async def authenticate_and_get_role(self, request: Any) -> Any:
        """Authenticates a request and returns (effective roles, principal)."""
        ...

    async def get_jwt_secret(self) -> str:
        """Retrieves or generates the active JWT secret."""
        ...

    async def get_jwks(self) -> Dict[str, Any]:
        """Returns the JWKS for token verification."""
        ...

    def extract_token_from_request(self, request: Any) -> Optional[str]:
        """Extracts a Bearer token from a framework-native request object."""
        ...
