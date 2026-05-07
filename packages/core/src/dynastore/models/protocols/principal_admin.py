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
`PrincipalAdminProtocol` — Principal CRUD surface. Uses `Any` for principal
type (models/protocols must not import concrete types from `modules/`).
"""

from typing import Any, List, Optional, Protocol, runtime_checkable
from uuid import UUID


@runtime_checkable
class PrincipalAdminProtocol(Protocol):
    """Manages Principals (identities) within a catalog context."""

    async def create_principal(
        self, principal: Any, catalog_id: Optional[str] = None
    ) -> Any:
        """Create a new Principal."""
        ...

    async def update_principal(
        self, principal: Any, catalog_id: Optional[str] = None
    ) -> Any:
        """Update an existing Principal."""
        ...

    async def get_principal(
        self, principal_id: UUID, catalog_id: Optional[str] = None
    ) -> Any:
        """Retrieve a Principal by id."""
        ...

    async def search_principals(
        self,
        identifier: Optional[str] = None,
        role: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        catalog_id: Optional[str] = None,
    ) -> List[Any]:
        """Search Principals with optional filters."""
        ...

    async def delete_principal(
        self, principal_id: UUID, catalog_id: Optional[str] = None
    ) -> bool:
        """Delete a Principal."""
        ...
