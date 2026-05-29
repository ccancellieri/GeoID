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
first-class, shipped use case: the in-tree ``PrivateEntityTransformer`` resolves
``ConfigsProtocol`` and memoizes the result on the chain context. When a
transformer does I/O it MUST:

- prefer the :class:`TransformChainContext` passed on every call — reuse
  ``ctx.pg_conn`` (the dispatcher's live connection on the write path) and
  cache lookups on the shared ``ctx.cache`` rather than reaching for globals;
  fall back to ``get_protocol(...)`` only for protocols not reachable via the
  context;
- be **fail-safe** — degrade to a sensible default rather than raising, since a
  raise rejects the item on the write path / drops it from the read shape;
- **cache on ``ctx.cache``** — the context is built once per chain invocation
  (per batch on the write fan-out, per query on the read path) and shared
  across every entity in that invocation, so a cached lookup collapses
  N items ⇒ N lookups down to one round-trip per distinct key;
- honor ``TransformerEntry.sla`` to bound that I/O.

Execution context (geoid#1568): the chain runtime threads a
:class:`TransformChainContext` to every ``transform_for_index`` /
``restore_from_index`` call. It carries the caller's ``pg_conn`` (``None`` on
the read path), a ``correlation_id``, and a ``cache`` dict shared across the
whole invocation. This is what lets enrichment transformers piggyback on the
dispatcher's connection and batch their external I/O.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional, Protocol, runtime_checkable

EntityKind = Literal["item", "collection", "catalog", "asset"]


@dataclass
class TransformChainContext:
    """Execution context threaded through one entity-transform chain run.

    Built **once per chain invocation** — per batch on the write fan-out
    (``IndexDispatcher``), per query on the read path (the ES restore
    drivers) — and passed to every transformer in the chain. The same
    instance is reused for every entity in that invocation, so transformers
    can share state (most importantly the ``cache``) across the batch.

    Fields
    ------
    pg_conn:
        The caller's live PG connection / transaction handle on the write
        path (from :class:`IndexContext.pg_conn`). ``None`` on the read path
        and on operator-triggered reindex, where no PG transaction is open.
    correlation_id:
        The caller's correlation id for log/trace stitching. ``None`` when
        the caller did not supply one.
    cache:
        A plain dict shared across the whole invocation. Enrichment
        transformers memoize external lookups here (keyed however they
        like, e.g. ``(catalog_id, collection_id)``) so N items collapse to
        one round-trip per distinct key. Reset per invocation — never reused
        across batches/queries, so it cannot leak stale state.
    """

    pg_conn: Optional[Any] = None
    correlation_id: Optional[str] = None
    cache: Dict[Any, Any] = field(default_factory=dict)


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
    transformers (e.g. the shipped ``PrivateEntityTransformer``) memoize
    their dependency lookups on the :class:`TransformChainContext` passed to
    every call. See the module docstring's "Purity and I/O" section for the
    supported pattern (use ``ctx``, fail-safe, cache on ``ctx.cache``, honor
    ``TransformerEntry.sla``).
    """

    async def transform_for_index(
        self,
        entity: Any,
        *,
        catalog_id: str,
        collection_id: Optional[str],
        entity_kind: EntityKind,
        ctx: TransformChainContext,
    ) -> Any:
        """Mutate the entity for indexing.

        Returns the doc to be written to the indexer's store. Invoked when
        this transformer's ``driver_ref`` appears in an
        :attr:`OperationDriverEntry.input_transformers` tuple — typically
        the target driver's secondary-index ``WRITE`` entry. Composes
        left-to-right with other transformers in the same tuple.

        ``ctx`` is the per-invocation :class:`TransformChainContext` (shared
        across every entity in the batch) — reuse ``ctx.pg_conn`` and
        memoize external lookups on ``ctx.cache`` instead of going global.
        """
        ...

    async def restore_from_index(
        self,
        doc: Any,
        *,
        catalog_id: str,
        collection_id: Optional[str],
        entity_kind: EntityKind,
        ctx: TransformChainContext,
    ) -> Any:
        """Inverse of :meth:`transform_for_index`.

        Returns the entity shape clients expect. Invoked when this
        transformer's ``driver_ref`` appears in an
        :attr:`OperationDriverEntry.output_transformers` tuple — typically
        the SEARCH entry of the target driver. The chain runtime applies
        inverses right-to-left so the output shape matches the original
        entity.

        ``ctx`` is the per-query :class:`TransformChainContext` (shared
        across every hit). ``ctx.pg_conn`` is ``None`` on the read path;
        ``ctx.cache`` is still available to batch any external lookups.

        May be a no-op when the indexed shape is already what clients
        want (rare — most transformers either fully invert or accept some
        information loss documented per-driver).
        """
        ...
