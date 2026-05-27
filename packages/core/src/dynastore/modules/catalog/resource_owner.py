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

"""Resource Cascade Cleanup — protocol layer.

Contract
--------
Any component that owns external state (Elasticsearch indices, GCS bucket
prefixes, IAM policy rows, plugin-config rows, tile preseed tables, …) and
needs that state cleaned up when a catalog or collection is deleted implements
``ResourceOwnerProtocol``.

Discovery and orchestration are separated from the protocol: a
``CascadeCleanupRegistry`` (see ``cascade_registry.py``) collects all
registered owners at startup.  When a hard-delete is initiated, the
orchestrator calls ``describe_scope`` on every relevant owner inside the
delete transaction to snapshot ``CleanupRef`` instances into a durable task
payload.  A separate worker then calls ``cleanup_one`` for each ref —
possibly on a different instance, after instance death and restart.

Design properties
-----------------
- **Idempotent**: ``cleanup_one`` must tolerate re-execution (ref already
  gone, half-done, etc.).
- **Self-contained payload**: after ``DROP SCHEMA … CASCADE`` the routing
  config and plugin-config rows are gone; the task payload is the only
  source of truth for what must be cleaned up.
- **Scope delegation**: ``CleanupMode.SOFT`` vs ``HARD`` is passed through
  unchanged; each owner decides what soft means for its resource type.
- **Zero DDL at runtime**: owners must not issue ``ALTER TABLE`` or in-place
  ``UPDATE`` backfills during cleanup.

This module intentionally has no I/O at import time and no dependency on
FastAPI, asyncpg, or any external service.  All async work happens inside
the method implementations, not at module scope.

Naming: the existing ``dynastore.models.protocols.asset_contrib.ResourceRef``
is a geospatial resource identity used by the asset-link contribution
protocols; it is a different concept and unrelated to this module.  The
cleanup payload type here is named ``CleanupRef`` to avoid that collision.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, ClassVar, Iterable, Protocol, runtime_checkable


class CleanupMode(str, Enum):
    """Soft (tombstone) vs hard (full removal) cleanup policy."""

    SOFT = "soft"
    HARD = "hard"


class ResourceScope(str, Enum):
    """Entity tier that triggered the cleanup.

    ``ITEM`` is reserved for future item-tier owners; no item-tier owner
    ships in the initial slice.
    """

    CATALOG = "catalog"
    COLLECTION = "collection"
    ASSET = "asset"
    ITEM = "item"


class CleanupOutcome(str, Enum):
    """Result returned by a single ``cleanup_one`` call."""

    DONE = "done"
    RETRY = "retry"   # transient failure — requeue with backoff
    DEAD = "dead"     # permanent failure — give up after N retries


@dataclass(frozen=True)
class ScopeRef:
    """Identifies the entity whose deletion triggers cascade cleanup."""

    scope: ResourceScope
    catalog_id: str
    collection_id: str | None = None
    asset_id: str | None = None
    item_id: str | None = None


@dataclass(frozen=True)
class CleanupRef:
    """An individual external resource owned by an entity.

    Instances are snapshot-serialized into a durable task payload before the
    owning entity rows are dropped, so the worker can clean up resources even
    after routing configs and plugin configs have been removed.

    All fields must be JSON-serializable (plain Python scalars, dicts, lists).
    Use ``to_json`` / ``from_json`` for round-tripping through task payloads
    and asyncpg JSONB columns.

    Note: this class shares a name with
    ``dynastore.models.protocols.asset_contrib.ResourceRef`` but serves a
    different purpose.  Use a module-qualified import or alias when both are
    needed in the same scope.
    """

    kind: str
    """Owner-defined resource type, e.g. ``"es_index"``, ``"gcs_prefix"``."""

    locator: str
    """Owner-defined opaque address.  Opaque to the orchestrator."""

    owner_id: str
    """Matches ``ResourceOwnerProtocol.owner_id`` — used to route
    ``cleanup_one`` back to the correct owner instance."""

    metadata: dict[str, Any] = field(default_factory=dict, hash=False, compare=False)
    """Owner-specific auxiliary data (e.g. ``{"index_prefix": "...",
    "is_private": True}``).  Must be JSON-serializable.

    Excluded from ``__hash__`` and ``__eq__`` because ``dict`` is not
    hashable.  Identity is determined by ``kind``, ``locator``, and
    ``owner_id`` alone.
    """

    def to_json(self) -> dict[str, Any]:
        """Return a JSON-serializable dict representation."""
        return {
            "kind": self.kind,
            "locator": self.locator,
            "owner_id": self.owner_id,
            "metadata": self.metadata,
        }

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> CleanupRef:
        """Reconstruct a ``CleanupRef`` from a dict produced by ``to_json``.

        Raises ``KeyError`` if any required field is absent.
        """
        return cls(
            kind=d["kind"],
            locator=d["locator"],
            owner_id=d["owner_id"],
            metadata=d.get("metadata", {}),
        )

    def round_trip(self) -> CleanupRef:
        """Convenience: serialize to JSON bytes and back (smoke-test helper)."""
        return self.from_json(json.loads(json.dumps(self.to_json())))


@runtime_checkable
class ResourceOwnerProtocol(Protocol):
    """Contract for any component that owns external resources.

    Implementors must be registered in ``CascadeCleanupRegistry`` at app
    startup (before ``freeze()`` is called).

    Class-level attributes
    ~~~~~~~~~~~~~~~~~~~~~~
    ``owner_id`` must be a unique stable string used to route ``cleanup_one``
    calls back to the correct owner instance even after the owning entity rows
    have been removed from the database.

    Method contracts
    ~~~~~~~~~~~~~~~~
    ``supported_scopes`` — iterable of ``ResourceScope`` values this owner
    answers ``describe_scope`` for.  The registry uses this to build the
    per-scope dispatch table.

    ``describe_scope`` — called *inside* the delete transaction (live ``conn``
    available) to enumerate all resources this owner created for the given
    entity.  Results are serialized into the task payload before ``DROP
    SCHEMA`` runs.

    ``cleanup_one`` — called by the cascade worker, possibly from a different
    Cloud Run instance.  Must be idempotent: a ref that was already cleaned up
    (or never existed) must return ``DONE``, not raise.
    """

    owner_id: ClassVar[str]

    def supported_scopes(self) -> Iterable[ResourceScope]: ...

    async def describe_scope(
        self, scope_ref: ScopeRef, conn: Any
    ) -> list[CleanupRef]: ...

    async def cleanup_one(
        self,
        ref: CleanupRef,
        mode: CleanupMode,
        *,
        dry_run: bool = False,
    ) -> CleanupOutcome: ...


class BaseResourceOwner(ABC):
    """Convenience base class providing a default ``supported_scopes``.

    Subclasses must supply ``owner_id`` as a class variable and implement
    ``describe_scope`` and ``cleanup_one``.  Override ``supported_scopes``
    to restrict to a subset of scopes (e.g. ``COLLECTION`` only).
    """

    owner_id: ClassVar[str]

    def supported_scopes(self) -> Iterable[ResourceScope]:
        """Default: CATALOG and COLLECTION.  Override to restrict."""
        return (ResourceScope.CATALOG, ResourceScope.COLLECTION)

    @abstractmethod
    async def describe_scope(
        self, scope_ref: ScopeRef, conn: Any
    ) -> list[CleanupRef]: ...

    @abstractmethod
    async def cleanup_one(
        self,
        ref: CleanupRef,
        mode: CleanupMode,
        *,
        dry_run: bool = False,
    ) -> CleanupOutcome: ...
