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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""
Collection-related protocol definitions.
"""

from datetime import datetime
from typing import (
    AsyncIterator,
    FrozenSet,
    Protocol,
    Optional,
    Any,
    List,
    Dict,
    Set,
    Union,
    runtime_checkable,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from dynastore.models.shared_models import Collection, CollectionUpdate
    from dynastore.modules.catalog.catalog_config import CollectionPluginConfig
    from dynastore.models.otf import SchemaEvolution, SchemaVersion, SnapshotInfo
    from dynastore.models.driver_context import DriverContext  # noqa: F401
    from dynastore.modules.storage.hints import Hint


@runtime_checkable
class CollectionsProtocol(Protocol):
    """
    Protocol for collection management operations.
    """

    async def get_collection(
        self,
        catalog_id: str,
        collection_id: str,
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
    ) -> Optional["Collection"]:
        """
        Retrieves a collection by ID.
        """
        ...

    async def create_collection(
        self,
        catalog_id: str,
        collection_data: Union[Dict[str, Any], "Collection"],
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
        **kwargs,
    ) -> "Collection":
        """
        Creates a new collection.
        """
        ...

    async def update_collection(
        self,
        catalog_id: str,
        collection_id: str,
        updates: Union[Dict[str, Any], "CollectionUpdate"],
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
    ) -> Optional["Collection"]:
        """
        Updates an existing collection.
        """
        ...

    async def delete_collection(
        self,
        catalog_id: str,
        collection_id: str,
        force: bool = False,
        ctx: Optional["DriverContext"] = None,
    ) -> bool:
        """
        Deletes a collection.
        """
        ...

    async def delete_collection_language(
        self,
        catalog_id: str,
        collection_id: str,
        lang: str,
        ctx: Optional["DriverContext"] = None,
    ) -> bool:
        """
        Deletes a specific language translation for a collection.
        """
        ...

    async def list_collections(
        self,
        catalog_id: str,
        limit: int = 10,
        offset: int = 0,
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
        q: Optional[str] = None,
    ) -> List[Any]:
        """
        Lists all collections in a catalog.
        """
        ...

    # === Metadata and Physical Resolution ===

    async def resolve_datasource(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        operation: str = "READ",
        hints: Optional[FrozenSet["Hint"]] = None,
    ) -> Any:
        """Resolve the best storage driver for a collection.

        Uses ``ItemsRoutingConfig`` to resolve the operation → driver
        mapping. For READ/SEARCH: returns the first matching driver.
        For WRITE: returns the primary write driver.

        Args:
            hints: Per-request routing preferences (e.g.
                ``frozenset({Hint.FEATURES})``, ``frozenset({Hint.ANALYTICS})``).
                An entry matches when its effective hint surface is a
                superset of ``hints`` — see ``router.resolve_drivers``.

        Returns:
            A ``CollectionItemsStore`` instance.
        """
        ...

    async def get_collection_config(
        self, catalog_id: str, collection_id: str, ctx: Optional["DriverContext"] = None
    ) -> "CollectionPluginConfig":
        """Retrieves the configuration for a collection."""
        ...

    async def get_collection_column_names(
        self, catalog_id: str, collection_id: str, ctx: Optional["DriverContext"] = None
    ) -> Set[str]:
        """Retrieves the physical column names for a collection."""
        ...

    # === Lazy Activation ===

    async def is_active(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Any = None,
    ) -> bool:
        """True once storage has been provisioned for this collection.

        A freshly-created collection is *pending* until its first
        `POST /items` (lazy activation) or an explicit
        `POST /collections/{col}/activate`. During the pending window,
        collection-scope configs (routing, write policy, schema) can be
        freely set; the Immutability guard accepts first-writes.
        """
        ...

    async def ensure_alive(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Any = None,
    ) -> None:
        """Assert the collection is ACTIVE; raise CollectionNotAliveError otherwise.

        Called at write-path boundaries to enforce the lifecycle gate.
        Fail-closed: any unexpected lookup error also raises
        CollectionNotAliveError with reason='lookup-error'.
        """
        ...

    async def activate_collection(
        self,
        catalog_id: str,
        collection_id: str,
        ctx: Optional["DriverContext"] = None,
    ) -> None:
        """Provision storage + pin routing for a pending collection.

        Idempotent: no-op when already active. Invoked transparently
        from the items write path on the first insert; no REST endpoint
        exposes it to clients.
        """
        ...

    # === OTF Extension: Snapshots ===

    async def list_snapshots(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        limit: int = 100,
        ctx: Optional["DriverContext"] = None,
    ) -> List["SnapshotInfo"]:
        """List available snapshots (versions) for a collection.

        Delegates to the collection's storage driver if it has
        ``Capability.SNAPSHOTS``. Raises ``NotImplementedError`` otherwise.
        """
        ...

    async def create_snapshot(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        label: Optional[str] = None,
        ctx: Optional["DriverContext"] = None,
    ) -> "SnapshotInfo":
        """Create an explicit snapshot/bookmark of current state."""
        ...

    async def rollback_to_snapshot(
        self,
        catalog_id: str,
        collection_id: str,
        snapshot_id: str,
        *,
        ctx: Optional["DriverContext"] = None,
    ) -> None:
        """Rollback collection to a previous snapshot."""
        ...

    # === OTF Extension: Time Travel ===

    async def read_at_snapshot(
        self,
        catalog_id: str,
        collection_id: str,
        snapshot_id: str,
        *,
        request: Optional[Any] = None,
        ctx: Optional["DriverContext"] = None,
    ) -> AsyncIterator[Any]:
        """Read entities at a specific snapshot (time-travel)."""
        ...

    async def read_at_timestamp(
        self,
        catalog_id: str,
        collection_id: str,
        as_of: datetime,
        *,
        request: Optional[Any] = None,
        ctx: Optional["DriverContext"] = None,
    ) -> AsyncIterator[Any]:
        """Read entities as they existed at a point in time."""
        ...

    # === OTF Extension: Schema Evolution ===

    async def get_schema_history(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        ctx: Optional["DriverContext"] = None,
    ) -> List["SchemaVersion"]:
        """Return schema evolution history for a collection."""
        ...

    async def evolve_schema(
        self,
        catalog_id: str,
        collection_id: str,
        changes: "SchemaEvolution",
        *,
        ctx: Optional["DriverContext"] = None,
    ) -> "SchemaVersion":
        """Apply schema changes (add/rename/drop columns, type widening).

        Does NOT rewrite data — only updates metadata.
        """
        ...
