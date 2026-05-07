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

"""`RoleAdminProtocol` — role CRUD and hierarchy management."""

from typing import Any, List, Optional, Protocol, runtime_checkable


@runtime_checkable
class RoleAdminProtocol(Protocol):
    """Manages roles and their hierarchy within a catalog."""

    async def list_roles(self, catalog_id: Optional[str] = None) -> List[Any]:
        """List roles in a catalog context."""
        ...

    async def create_role(self, role: Any, catalog_id: Optional[str] = None) -> Any:
        """Create a new role."""
        ...

    async def update_role(self, role: Any, catalog_id: Optional[str] = None) -> Any:
        """Update an existing role."""
        ...

    async def delete_role(
        self, name: str, cascade: bool = False, catalog_id: Optional[str] = None
    ) -> bool:
        """Delete a role."""
        ...

    async def add_role_hierarchy(
        self, parent_role: str, child_role: str, catalog_id: Optional[str] = None
    ) -> None:
        """Add a parent→child role hierarchy link."""
        ...

    async def remove_role_hierarchy(
        self, parent_role: str, child_role: str, catalog_id: Optional[str] = None
    ) -> bool:
        """Remove a parent→child role hierarchy link."""
        ...

    async def get_role_hierarchy(
        self, role_name: str, catalog_id: Optional[str] = None
    ) -> List[str]:
        """Return the effective hierarchy for a role."""
        ...
