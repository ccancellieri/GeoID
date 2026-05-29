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

"""
EntityTransformProtocol — drivers that mutate entities on the way to / from
storage.

A transformer is a driver whose role is to reshape an entity payload before
it lands in an indexer's storage, and to reverse that reshape on the way
back out for clients.

The routing config's ``transformers`` field is the **registry** of available
transformer instances (populated by ``_self_register_transformers_into`` or by
operators).  *Where* a transformer runs is decided by the per-driver
attachment fields on each :class:`OperationDriverEntry`:

- ``input_transformers``  — transformer ``driver_ref``s applied to entities
  going INTO this driver call (e.g. a secondary-index WRITE entry for an items
  indexer).  Composition is left-to-right.
- ``output_transformers`` — transformer ``driver_ref``s applied to entities
  coming OUT of this driver call (e.g. the SEARCH entry for the same driver).
  Composition is right-to-left so the inverse chain matches the original
  shape.

Routing config is the SSOT for which (operation, driver) pair owns which
chain — a transformer doesn't choose where it runs.

The protocol itself is the structural marker — discovery uses
``get_protocols(EntityTransformProtocol)``. There is no parallel capability
flag; implementing the protocol is the contract.

Empty attachment ⇒ identity (no-op).

Purity and I/O
--------------
A transformer must not mutate its input in place and must be deterministic
with respect to its inputs *for a given external state* — the chain runtime
composes transformers and relies on that.

I/O is permitted, and **enrichment transformers** (which compute fields from
an external lookup — another store, a graph DB, an embeddings service) are a
first-class, shipped use case: the in-tree ``PrivateEntityTransformer`` already
resolves ``ConfigsProtocol`` via global ``get_protocol(...)``. When a transformer
does I/O it MUST:

- resolve its dependencies via global ``get_protocol(...)`` (there is no
  execution context passed to the chain today — see the runtime caveat below);
- be **fail-safe** — degrade to a sensible default rather than raising, since a
  raise rejects the item on the write path / drops it from the read shape;
- **cache** in-process where possible — the chain runs per entity on the hot
  secondary-index fan-out, so an uncached lookup is one external round-trip per
  item;
- honor ``TransformerEntry.sla`` to bound that I/O.

Runtime caveat (tracked in geoid#1568): the chain currently runs **per item**
with **no execution context** — transformers cannot piggyback on the
dispatcher's ``pg_conn`` and there is no batch hook, so N items ⇒ N lookups.
First-class context / batch support is a proposed enhancement, not yet wired.
"""

from __future__ import annotations

from typing import Any, Literal, Optional, Protocol, runtime_checkable

EntityKind = Literal["item", "collection", "catalog", "asset"]


@runtime_checkable
class EntityTransformProtocol(Protocol):
    """A driver that mutates entities on the way to / from storage.

    Identity is the implementer's class name — same convention as
    :class:`IndexerProtocol` / :class:`SearchProtocol` and as used by
    ``_self_register_indexers_into`` / ``_self_register_searchers_into``.
    ``_self_register_transformers_into`` writes
    ``type(transformer).__name__`` into ``TransformerEntry.driver_ref``;
    routing entries reference these names from ``input_transformers`` /
    ``output_transformers`` to choose which chain runs where.

    The transform must not mutate its input in place and must be
    deterministic for a given external state — the chain runtime composes
    transformers and relies on that. I/O **is** permitted: enrichment
    transformers (e.g. the shipped ``PrivateEntityTransformer``) resolve
    dependencies via global ``get_protocol(...)``. See the module docstring's
    "Purity and I/O" section for the supported pattern (fail-safe, cache,
    honor ``TransformerEntry.sla``) and the per-item runtime caveat (#1568).
    """

    async def transform_for_index(
        self,
        entity: Any,
        *,
        catalog_id: str,
        collection_id: Optional[str],
        entity_kind: EntityKind,
    ) -> Any:
        """Mutate the entity for indexing.

        Returns the doc to be written to the indexer's store. Invoked when
        this transformer's ``driver_ref`` appears in an
        :attr:`OperationDriverEntry.input_transformers` tuple — typically
        the target driver's secondary-index ``WRITE`` entry. Composes
        left-to-right with other transformers in the same tuple.
        """
        ...

    async def restore_from_index(
        self,
        doc: Any,
        *,
        catalog_id: str,
        collection_id: Optional[str],
        entity_kind: EntityKind,
    ) -> Any:
        """Inverse of :meth:`transform_for_index`.

        Returns the entity shape clients expect. Invoked when this
        transformer's ``driver_ref`` appears in an
        :attr:`OperationDriverEntry.output_transformers` tuple — typically
        the SEARCH entry of the target driver. The chain runtime applies
        inverses right-to-left so the output shape matches the original
        entity.

        May be a no-op when the indexed shape is already what clients
        want (rare — most transformers either fully invert or accept some
        information loss documented per-driver).
        """
        ...
