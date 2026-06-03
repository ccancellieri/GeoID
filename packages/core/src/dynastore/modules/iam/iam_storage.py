#    Copyright 2025 FAO
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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

import abc
from typing import Dict, List, Any, Optional
from uuid import UUID
from .models import Principal, Role, RefreshToken

# --- Storage Implementation ---

class AbstractIamStorage(abc.ABC):
    """
    Abstract interface for IAM storage.

    Scope conventions:
    - **Principals & identity_links** are platform-global. They live only in
      the `iam` schema; the storage layer hardcodes the schema internally
      (no `schema=` parameter on these methods).
    - **Roles** are per-scope: platform roles in `iam`, tenant-defined roles
      in each catalog schema. Methods that touch the role registry still
      take `schema=` because the same logical method writes to either scope.
    - **Refresh tokens & audit log** stay platform-only with the legacy
      `schema=` default for now (M2.5-style hard cut not in scope here).

    The 'conn' parameter is typed as Optional[Any] to allow for:
    1. SQL implementations receiving a DbResource/Connection for transactions.
    2. NoSQL/Memory implementations ignoring it or receiving a different client type.
    3. Middleware usage where the storage manages its own connection context internally.
    """

    # No initialize() or shutdown() - use lifespan() if needed.

    @abc.abstractmethod
    async def create_principal(self, principal: Principal, conn: Optional[Any] = None) -> Principal: ...

    @abc.abstractmethod
    async def get_principal(self, principal_id: UUID, conn: Optional[Any] = None) -> Optional[Principal]: ...

    @abc.abstractmethod
    async def update_principal(self, principal: Principal, conn: Optional[Any] = None) -> Optional[Principal]: ...

    @abc.abstractmethod
    async def get_principal_by_identifier(self, identifier: str, conn: Optional[Any] = None) -> Optional[Principal]: ...

    @abc.abstractmethod
    async def delete_principal(self, principal_id: UUID, conn: Optional[Any] = None) -> bool: ...

    @abc.abstractmethod
    async def get_principal_id_by_identifier(self, identifier: str, conn: Optional[Any] = None) -> Optional[UUID]: ...

    @abc.abstractmethod
    async def list_principals(self, offset: int, limit: int, conn: Optional[Any] = None) -> List[Principal]: ...

    # --- Search & Management Extensions ---

    @abc.abstractmethod
    async def search_principals(self, identifier: Optional[str] = None, role: Optional[str] = None, limit: int = 100, offset: int = 0, conn: Optional[Any] = None, schema: str = "iam") -> List[Principal]:
        """Search principals by identifier pattern or role.

        `schema` selects which `grants` table the optional role filter
        joins through (platform `iam.grants` vs. a tenant's grants).
        Principals always come from `iam.principals`.
        """
        ...

    # --- Role & Hierarchy Management ---

    @abc.abstractmethod
    async def create_role(self, role: Role, conn: Optional[Any] = None, schema: str = "iam") -> Role: ...

    @abc.abstractmethod
    async def get_role(self, name: str, conn: Optional[Any] = None, schema: str = "iam") -> Optional[Role]: ...

    @abc.abstractmethod
    async def list_roles(self, conn: Optional[Any] = None, schema: str = "iam") -> List[Role]: ...

    @abc.abstractmethod
    async def update_role(self, role: Role, conn: Optional[Any] = None, schema: str = "iam") -> Optional[Role]: ...

    @abc.abstractmethod
    async def bind_policy_to_role(
        self, role_name: str, policy_entry: Dict[str, Any], schema: str = "iam"
    ) -> None:
        """Append policy_entry[``id``] to roles.policies, deduping by that string value.

        Single atomic SQL UPDATE — no Python read-modify-write.
        ``policy_entry`` must have an ``"id"`` key; only the id string is persisted
        (roles.policies stores plain string IDs, not dicts).
        """
        ...

    @abc.abstractmethod
    async def unbind_policy_from_role(
        self, role_name: str, policy_id: str, schema: str = "iam"
    ) -> None:
        """Remove all entries where ``elem->>'id' == policy_id`` from roles.policies.

        Single atomic SQL UPDATE — no Python read-modify-write.
        """
        ...

    @abc.abstractmethod
    async def delete_role(self, name: str, cascade: bool = False, conn: Optional[Any] = None, schema: str = "iam") -> bool: ...

    @abc.abstractmethod
    async def add_role_hierarchy(self, parent_role: str, child_role: str, conn: Optional[Any] = None, schema: str = "iam"): ...

    @abc.abstractmethod
    async def get_role_hierarchy(self, role_names: List[str], conn: Optional[Any] = None, schema: str = "iam") -> List[str]: ...

    @abc.abstractmethod
    async def remove_role_hierarchy(self, parent_role: str, child_role: str, conn: Optional[Any] = None, schema: str = "iam") -> bool: ...

    @abc.abstractmethod
    async def list_role_hierarchy_edges(self, conn: Optional[Any] = None, schema: str = "iam") -> List[tuple[str, str]]: ...

    # --- Refresh Token Management ---

    @abc.abstractmethod
    async def create_refresh_token(self, token: RefreshToken, conn: Optional[Any] = None, schema: str = "iam") -> RefreshToken: ...

    @abc.abstractmethod
    async def get_refresh_token(self, token_id: str, conn: Optional[Any] = None, schema: str = "iam") -> Optional[RefreshToken]: ...

    @abc.abstractmethod
    async def invalidate_refresh_token(self, token_id: str, conn: Optional[Any] = None, schema: str = "iam") -> bool: ...

    @abc.abstractmethod
    async def run_maintenance(self, conn: Optional[Any] = None, schema: str = "iam") -> dict:
        """Runs storage-specific maintenance (e.g. pruning expired tokens)."""
        ...

    @abc.abstractmethod
    async def get_catalogs_for_identity(self, provider: str, subject_id: str) -> List[str]:
        """Get list of catalog IDs where an identity has at least one grant.

        Equivalent to ``list((await get_catalog_roles_for_identity(...)).keys())``;
        kept as a separate primitive for callers that don't need role data.
        """
        ...

    @abc.abstractmethod
    async def get_catalog_roles_for_identity(
        self, provider: str, subject_id: str,
    ) -> Dict[str, List[str]]:
        """Get a ``{catalog_id: [role_name, ...]}`` map for an identity.

        Used by catalog-scoped authorization (e.g. ``catalog_admin_required``)
        that needs to know *which* roles the principal holds in each catalog,
        not just whether they have any grant. Empty dict for unknown
        identities or identities with no catalog grants.
        """
        ...

    @abc.abstractmethod
    async def get_principal_by_identity(
        self, provider: str, subject_id: str, conn: Optional[Any] = None
    ) -> Optional[Principal]:
        """Resolves an external identity to a principal (platform-global)."""
        ...

    @abc.abstractmethod
    async def create_identity_link(
        self,
        principal_id: UUID,
        provider: str,
        subject_id: str,
        email: Optional[str] = None,
        conn: Optional[Any] = None,
    ) -> bool:
        """Links a principal to an external identity (platform-global)."""
        ...
