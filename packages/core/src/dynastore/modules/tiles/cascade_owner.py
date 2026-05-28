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

"""Cascade cleanup owner for tile preseed data.

:class:`TilePreseedOwner` replaces the ``on_catalog_hard_deletion`` and
``on_collection_hard_deletion`` event listeners in ``tiles_module.py``.

CATALOG scope: calls ``invalidate_catalog_tiles`` (deletes all tile records
and clears caches for the catalog).

COLLECTION scope: calls ``invalidate_collection_tiles`` (deletes tile records
and clears caches for the collection).

Register via :func:`register_owners` from the tiles module lifespan BEFORE
the CascadeCleanupRegistry is frozen.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, ClassVar, Iterable

from dynastore.modules.catalog.resource_owner import (
    BaseResourceOwner,
    CleanupMode,
    CleanupOutcome,
    CleanupRef,
    ResourceScope,
    ScopeRef,
)

if TYPE_CHECKING:
    from dynastore.modules.catalog.cascade_registry import CascadeCleanupRegistry

logger = logging.getLogger(__name__)


class TilePreseedOwner(BaseResourceOwner):
    """Invalidates tile preseed data for a hard-deleted catalog or collection."""

    owner_id: ClassVar[str] = "tiles.preseed"

    def supported_scopes(self) -> Iterable[ResourceScope]:
        return (ResourceScope.CATALOG, ResourceScope.COLLECTION)

    async def describe_scope(
        self, scope_ref: ScopeRef, conn: Any
    ) -> list[CleanupRef]:
        if scope_ref.scope == ResourceScope.CATALOG:
            return [
                CleanupRef(
                    kind="tile_cache",
                    locator=scope_ref.catalog_id,
                    owner_id=self.owner_id,
                    metadata={"scope": "catalog", "catalog_id": scope_ref.catalog_id},
                )
            ]
        if scope_ref.scope == ResourceScope.COLLECTION and scope_ref.collection_id:
            return [
                CleanupRef(
                    kind="tile_cache",
                    locator=f"{scope_ref.catalog_id}:{scope_ref.collection_id}",
                    owner_id=self.owner_id,
                    metadata={
                        "scope": "collection",
                        "catalog_id": scope_ref.catalog_id,
                        "collection_id": scope_ref.collection_id,
                    },
                )
            ]
        return []

    async def cleanup_one(
        self,
        ref: CleanupRef,
        mode: CleanupMode,
        *,
        dry_run: bool = False,
    ) -> CleanupOutcome:
        if mode == CleanupMode.SOFT:
            return CleanupOutcome.DONE

        scope = ref.metadata.get("scope", "catalog")
        catalog_id = ref.metadata.get("catalog_id", "")
        collection_id = ref.metadata.get("collection_id")

        if dry_run:
            logger.info(
                "TilePreseedOwner: dry-run — would invalidate tiles for %r (scope=%s).",
                ref.locator, scope,
            )
            return CleanupOutcome.DONE

        try:
            from dynastore.modules.tiles.tiles_module import (
                invalidate_catalog_tiles,
                invalidate_collection_tiles,
            )
            if scope == "catalog":
                await invalidate_catalog_tiles(catalog_id)
            elif scope == "collection" and collection_id:
                await invalidate_collection_tiles(catalog_id, collection_id)
            return CleanupOutcome.DONE
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "TilePreseedOwner: failed to invalidate tiles for %r: %s",
                ref.locator, exc, exc_info=True,
            )
            return CleanupOutcome.RETRY


def register_owners(registry: "CascadeCleanupRegistry") -> None:
    """Register tile cascade owners into *registry*.

    Call from the tiles module lifespan BEFORE
    :meth:`~dynastore.modules.catalog.cascade_registry.CascadeCleanupRegistry.freeze`.
    """
    registry.register(TilePreseedOwner())
    logger.info("TilesModule: registered cascade owner %r.", TilePreseedOwner.owner_id)
