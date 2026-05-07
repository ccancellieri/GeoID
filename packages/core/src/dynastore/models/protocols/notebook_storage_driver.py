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

"""
NotebookStorageDriverProtocol — pluggable storage for tenant and platform notebooks.

The PostgreSQL implementation wraps ``notebooks_db.py`` (tenant-scoped,
``{catalog_schema}.notebooks``) and ``platform_db.py`` (global,
``notebooks.platform_notebooks``).

Security contract (MUST be honoured by every implementation):
  - Tenant notebooks MUST always filter by ``catalog_id`` — never return
    rows from a different tenant's schema.
  - Platform notebooks have no tenant scope, but access MUST be gated by
    the caller's authorisation policy before reaching this driver.
  - ``list_notebooks`` MUST NOT expose deleted rows (``deleted_at IS NOT NULL``).

Future drivers (e.g. Elasticsearch) MUST replicate the same tenant-isolation
guarantees and MUST NOT relax the ``deleted_at`` filter.
"""

from typing import Any, Dict, FrozenSet, List, Optional, Protocol, Tuple, runtime_checkable


@runtime_checkable
class NotebookStorageDriverProtocol(Protocol):
    """Pluggable storage abstraction for notebook CRUD.

    Drivers implement tenant-scoped notebook operations (``catalog_id`` is
    always required for tenant operations) and optionally platform-level
    operations (cross-tenant, ``notebooks.platform_notebooks``).

    ``capabilities`` declares what the driver supports using the string
    constants below.
    """

    # --- Capability constants ---
    CAP_TENANT = "tenant"          # tenant-scoped notebook CRUD
    CAP_PLATFORM = "platform"      # platform notebook CRUD (cross-tenant)
    CAP_SEARCH = "search"          # full-text search (q parameter)
    CAP_TAGS = "tags"              # tag containment filtering

    capabilities: FrozenSet[str]

    # ------------------------------------------------------------------
    # Tenant notebook operations
    # ------------------------------------------------------------------

    async def get_notebook(
        self,
        catalog_id: str,
        notebook_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Retrieve an active tenant notebook by ID.

        Raises ``ResourceNotFoundError`` if not found or soft-deleted.
        """
        ...

    async def list_notebooks(
        self,
        catalog_id: str,
        *,
        q: Optional[str] = None,
        tags: Optional[List[str]] = None,
        limit: int = 20,
        offset: int = 0,
        db_resource: Optional[Any] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """List active tenant notebooks with optional filtering and pagination.

        Args:
            catalog_id: Tenant scope — MUST be applied as an isolation filter.
            q: Case-insensitive substring match across all language values in
               ``title`` and ``description`` JSONB fields.
               Requires ``CAP_SEARCH``.
            tags: Return only notebooks whose ``tags`` array contains ALL
               provided tags. Requires ``CAP_TAGS``.
            limit: Maximum results per page.
            offset: Number of results to skip.

        Returns:
            ``(items, total_count)`` for pagination.
        """
        ...

    async def save_notebook(
        self,
        catalog_id: str,
        notebook: Dict[str, Any],
        *,
        owner_id: Optional[str] = None,
        copied_from: Optional[str] = None,
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Upsert a tenant notebook. Returns the saved record."""
        ...

    async def delete_notebook(
        self,
        catalog_id: str,
        notebook_id: str,
        *,
        soft: bool = True,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Delete a tenant notebook.

        Args:
            soft: If True, set ``deleted_at`` (default). If False, hard-delete.
        """
        ...

    # ------------------------------------------------------------------
    # Platform notebook operations (CAP_PLATFORM required)
    # ------------------------------------------------------------------

    async def get_platform_notebook(
        self,
        notebook_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Retrieve an active platform notebook by ID.

        Requires ``CAP_PLATFORM``.
        Raises ``ResourceNotFoundError`` if not found or soft-deleted.
        """
        ...

    async def list_platform_notebooks(
        self,
        *,
        q: Optional[str] = None,
        tags: Optional[List[str]] = None,
        limit: int = 20,
        offset: int = 0,
        db_resource: Optional[Any] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """List active platform notebooks with optional filtering and pagination.

        Requires ``CAP_PLATFORM``.

        Returns:
            ``(items, total_count)`` for pagination.
        """
        ...

    async def save_platform_notebook(
        self,
        notebook: Dict[str, Any],
        *,
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Upsert a platform notebook. Returns the saved record.

        Requires ``CAP_PLATFORM``.
        """
        ...

    async def delete_platform_notebook(
        self,
        notebook_id: str,
        *,
        soft: bool = True,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Delete a platform notebook.

        Requires ``CAP_PLATFORM``.
        """
        ...
