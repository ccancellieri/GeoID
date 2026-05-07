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
Entity-transform chain runtime.

Walks the ordered chain of :class:`EntityTransformProtocol` implementers
resolved from ``operations[TRANSFORM]`` of the active routing config and
applies them to an entity at index time, or applies their inverses at
read time.

Empty chain ⇒ identity (no-op). The default behavior when no transformer
is registered for an entity is to leave the entity unchanged.

Composition contract:
- ``apply_transform_chain`` walks left-to-right. Transformer ``i+1``
  receives the output of transformer ``i`` as input.
- ``restore_transform_chain`` walks right-to-left so the output shape
  matches the original entity (inverses applied in reverse order).
"""

from __future__ import annotations

import logging
from typing import Any, Iterable, Optional

from dynastore.models.protocols.entity_transform import (
    EntityKind,
    EntityTransformProtocol,
)

logger = logging.getLogger(__name__)


async def apply_transform_chain(
    entity: Any,
    transformers: Iterable[EntityTransformProtocol],
    *,
    catalog_id: str,
    collection_id: Optional[str],
    entity_kind: EntityKind,
) -> Any:
    """Apply each transformer's ``transform_for_index`` in order.

    Returns the doc to be written to the indexer's store. Empty
    ``transformers`` ⇒ returns ``entity`` unchanged.
    """
    current = entity
    for transformer in transformers:
        try:
            current = await transformer.transform_for_index(
                current,
                catalog_id=catalog_id,
                collection_id=collection_id,
                entity_kind=entity_kind,
            )
        except Exception as exc:
            logger.warning(
                "transform_runtime: transformer '%s' failed at "
                "transform_for_index for catalog=%s collection=%s entity_kind=%s: %s",
                type(transformer).__name__,
                catalog_id, collection_id, entity_kind, exc,
            )
            raise
    return current


async def restore_transform_chain(
    doc: Any,
    transformers: Iterable[EntityTransformProtocol],
    *,
    catalog_id: str,
    collection_id: Optional[str],
    entity_kind: EntityKind,
) -> Any:
    """Apply each transformer's ``restore_from_index`` in reverse order.

    Inverses run right-to-left so the output shape matches what callers
    of ``apply_transform_chain`` originally passed in. Empty
    ``transformers`` ⇒ returns ``doc`` unchanged.
    """
    transformers_list = list(transformers)
    current = doc
    for transformer in reversed(transformers_list):
        try:
            current = await transformer.restore_from_index(
                current,
                catalog_id=catalog_id,
                collection_id=collection_id,
                entity_kind=entity_kind,
            )
        except Exception as exc:
            logger.warning(
                "transform_runtime: transformer '%s' failed at "
                "restore_from_index for catalog=%s collection=%s entity_kind=%s: %s",
                type(transformer).__name__,
                catalog_id, collection_id, entity_kind, exc,
            )
            raise
    return current
