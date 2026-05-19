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
#    See the License for the specific language governing permissions and
#    limitations under the License.

from typing import Protocol, List, Literal, Optional, Tuple, Any, runtime_checkable

from pydantic import BaseModel, Field

# Re-export permission-related models so extensions only need ONE import:
#   from dynastore.models.protocols.policies import PermissionProtocol, Policy, Role, Principal
from dynastore.models.auth import Condition, Policy, Principal  # Policy, Principal, Condition canonical home
from dynastore.models.auth_models import Role               # Role canonical home

__all__ = [
    "PermissionProtocol",
    "Policy",
    "Role",
    "Principal",
    # Wire-schema DTOs for the role/principal/policy management surface.
    # Single canonical home so non-admin consumers (SDK, IAM CLI, other
    # extensions) do not have to import through the admin extension.
    "RoleCreate",
    "RoleUpdate",
    "RoleResponse",
    "AssignRoleRequest",
    "PrincipalResponse",
    "PolicyCreate",
    "PolicyUpdate",
    "PolicyResponse",
    "CatalogRoleAssignment",
]


# --- Role wire DTOs ---

class RoleCreate(BaseModel):
    name: str
    description: Optional[str] = None
    policies: List[str] = Field(default_factory=list)
    parent_roles: List[str] = Field(default_factory=list)


class RoleUpdate(BaseModel):
    description: Optional[str] = None
    policies: Optional[List[str]] = None
    parent_roles: Optional[List[str]] = None


class RoleResponse(BaseModel):
    name: str
    description: Optional[str] = None
    policies: List[str] = Field(default_factory=list)
    parent_roles: List[str] = Field(default_factory=list)


# --- Principal / assignment wire DTOs ---

class AssignRoleRequest(BaseModel):
    role: str


class PrincipalResponse(BaseModel):
    id: str
    provider: Optional[str] = None
    subject_id: Optional[str] = None
    display_name: Optional[str] = None
    roles: List[str] = Field(default_factory=list)
    is_active: bool = True


# --- Policy wire DTOs ---

class PolicyCreate(BaseModel):
    id: str
    description: Optional[str] = None
    actions: List[str]
    resources: List[str]
    effect: Literal["ALLOW", "DENY"] = "ALLOW"
    priority: int = Field(default=0, ge=-1000, le=1000)
    conditions: List[Condition] = Field(default_factory=list)


class PolicyUpdate(BaseModel):
    description: Optional[str] = None
    actions: Optional[List[str]] = None
    resources: Optional[List[str]] = None
    effect: Optional[Literal["ALLOW", "DENY"]] = None
    priority: Optional[int] = Field(default=None, ge=-1000, le=1000)
    conditions: Optional[List[Condition]] = None


class PolicyResponse(BaseModel):
    id: str
    description: Optional[str] = None
    actions: List[str]
    resources: List[str]
    effect: Literal["ALLOW", "DENY"]
    priority: int = 0
    partition_key: Optional[str] = None
    conditions: List[Condition] = Field(default_factory=list)


# --- Catalog role assignment wire DTO ---

class CatalogRoleAssignment(BaseModel):
    catalog_id: str
    role: str


@runtime_checkable
class PermissionProtocol(Protocol):
    """Unified protocol for Policy, Role, and permission management/evaluation.

    Extensions register their policies and roles via:
        get_protocol(PermissionProtocol).register_policy(Policy(...))
        get_protocol(PermissionProtocol).register_role(Role(...))
    """

    async def create_policy(
        self, policy: Policy, catalog_id: Optional[str] = None
    ) -> Any: ...

    async def get_policy(
        self, policy_id: str, catalog_id: Optional[str] = None
    ) -> Optional[Policy]: ...

    async def update_policy(
        self, policy: Policy, catalog_id: Optional[str] = None
    ) -> Optional[Policy]: ...

    async def list_policies(
        self, limit: int = 100, offset: int = 0, catalog_id: Optional[str] = None
    ) -> List[Policy]: ...

    async def delete_policy(
        self, policy_id: str, catalog_id: Optional[str] = None
    ) -> bool: ...

    async def search_policies(
        self,
        resource_pattern: str,
        action_pattern: str,
        limit: int = 10,
        offset: int = 0,
        catalog_id: Optional[str] = None,
    ) -> List[Policy]: ...

    async def evaluate_policy_statements(
        self, policy: Policy, method: str, path: str, request_context: Any = None
    ) -> bool: ...

    async def evaluate_access(
        self,
        principals: List[str],
        path: str,
        method: str,
        request_context: Any = None,
        catalog_id: Optional[str] = None,
        custom_policies: Optional[List["Policy"]] = None,
    ) -> Tuple[bool, str]: ...

    async def provision_default_policies(
        self,
        catalog_id: Optional[str] = None,
        conn: Optional[Any] = None,
        schema: Optional[str] = None,
        force: bool = False,
    ) -> None: ...

    # --- Extension injection points ---

    def register_policy(self, policy: Policy) -> Policy: ...

    def register_role(self, role: Role) -> Role: ...
