"""EntityTransformProtocol — protocol shape + EntityKind enum.

Identity for routing is the implementer's class name (same convention as
IndexerProtocol / SearchProtocol). The protocol is runtime-checkable
structural typing — any class with the two async methods qualifies.
"""

from __future__ import annotations

import asyncio
from typing import Any, Optional, get_args

from dynastore.models.protocols.entity_transform import (
    EntityKind,
    EntityTransformProtocol,
    TransformChainContext,
)


def test_entity_kind_literal_covers_four_entities():
    """EntityKind enumerates exactly item / collection / catalog / asset."""
    assert set(get_args(EntityKind)) == {"item", "collection", "catalog", "asset"}


class _MinimalTransformer:
    async def transform_for_index(
        self,
        entity: Any,
        *,
        catalog_id: str,
        collection_id: Optional[str],
        entity_kind: EntityKind,
        ctx: TransformChainContext,
    ) -> Any:
        return entity

    async def restore_from_index(
        self,
        doc: Any,
        *,
        catalog_id: str,
        collection_id: Optional[str],
        entity_kind: EntityKind,
        ctx: TransformChainContext,
    ) -> Any:
        return doc


class _MissingMethods:
    """Stub missing both required methods — should not satisfy the protocol."""
    pass


def test_minimal_transformer_satisfies_protocol():
    """A class with both async methods passes isinstance() — class name is the id."""
    assert isinstance(_MinimalTransformer(), EntityTransformProtocol)
    assert type(_MinimalTransformer()).__name__ == "_MinimalTransformer"


def test_class_missing_methods_does_not_satisfy_protocol():
    assert not isinstance(_MissingMethods(), EntityTransformProtocol)


def test_methods_are_awaitable():
    """transform_for_index / restore_from_index return awaitables (sanity)."""
    t = _MinimalTransformer()
    coro = t.transform_for_index(
        {"x": 1}, catalog_id="c", collection_id="col", entity_kind="item",
        ctx=TransformChainContext(),
    )
    result = asyncio.run(coro)
    assert result == {"x": 1}
