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

"""Cascade cleanup owners for the public Elasticsearch items and asset drivers.

Per-catalog indexes managed here:
- ``{prefix}-{catalog_id}-items``   — public items index (ItemsElasticsearchDriver)
- ``{prefix}-{catalog_id}-assets``  — per-catalog asset index (AssetElasticsearchDriver)

Platform-wide indexes (``{prefix}-catalogs``, ``{prefix}-collections``) are
shared across all tenants and are NOT owned here — deleting one catalog must
not touch them.

Register via :func:`register_owners` from the driver lifespan startup hook
BEFORE the CascadeCleanupRegistry is frozen.
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


def _get_public_items_index_name(catalog_id: str) -> str:
    from dynastore.modules.elasticsearch.client import get_index_prefix
    from dynastore.modules.elasticsearch.mappings import get_tenant_items_index
    return get_tenant_items_index(get_index_prefix(), catalog_id)


def _get_assets_index_name(catalog_id: str) -> str:
    from dynastore.modules.elasticsearch.client import get_index_prefix
    from dynastore.modules.elasticsearch.mappings import get_assets_index_name
    return get_assets_index_name(get_index_prefix(), catalog_id)


def _get_es_client() -> Any:
    from dynastore.modules.elasticsearch.client import get_client
    client = get_client()
    if client is None:
        raise RuntimeError(
            "Elasticsearch client is not initialized. "
            "Ensure ElasticsearchModule is registered and its lifespan has started."
        )
    return client


async def _delete_es_index(index_name: str) -> CleanupOutcome:
    try:
        es = _get_es_client()
        await es.indices.delete(index=index_name, params={"ignore_unavailable": "true"})
        logger.info("ES: deleted index %r.", index_name)
        return CleanupOutcome.DONE
    except Exception as exc:  # noqa: BLE001
        logger.error("ES: failed to delete index %r: %s", index_name, exc, exc_info=True)
        return CleanupOutcome.RETRY


class EsItemsPublicIndexOwner(BaseResourceOwner):
    """Resource owner for the per-catalog public items ES index.

    The public driver uses one index per catalog:
    ``{prefix}-{catalog_id}-items``.  All collections share this index
    via ``_routing=collection_id``.  Deleting a collection does NOT
    delete this shared index.
    """

    owner_id: ClassVar[str] = "es_public.items_index"

    def supported_scopes(self) -> Iterable[ResourceScope]:
        return (ResourceScope.CATALOG,)

    async def describe_scope(
        self, scope_ref: ScopeRef, conn: Any
    ) -> list[CleanupRef]:
        if scope_ref.scope != ResourceScope.CATALOG:
            return []
        index_name = _get_public_items_index_name(scope_ref.catalog_id)
        return [
            CleanupRef(
                kind="es_index",
                locator=index_name,
                owner_id=self.owner_id,
                metadata={"catalog_id": scope_ref.catalog_id},
            )
        ]

    async def cleanup_one(
        self,
        ref: CleanupRef,
        mode: CleanupMode,
        *,
        dry_run: bool = False,
    ) -> CleanupOutcome:
        if mode == CleanupMode.SOFT:
            return CleanupOutcome.DONE
        if dry_run:
            logger.info("EsItemsPublicIndexOwner: dry-run — would delete %r.", ref.locator)
            return CleanupOutcome.DONE
        return await _delete_es_index(ref.locator)


class EsAssetIndexOwner(BaseResourceOwner):
    """Resource owner for the per-catalog assets ES index.

    Pattern: ``{prefix}-{catalog_id}-assets``.  One index per catalog;
    deleting a collection does NOT touch this index.
    """

    owner_id: ClassVar[str] = "es_public.asset_index"

    def supported_scopes(self) -> Iterable[ResourceScope]:
        return (ResourceScope.CATALOG,)

    async def describe_scope(
        self, scope_ref: ScopeRef, conn: Any
    ) -> list[CleanupRef]:
        if scope_ref.scope != ResourceScope.CATALOG:
            return []
        index_name = _get_assets_index_name(scope_ref.catalog_id)
        return [
            CleanupRef(
                kind="es_index",
                locator=index_name,
                owner_id=self.owner_id,
                metadata={"catalog_id": scope_ref.catalog_id},
            )
        ]

    async def cleanup_one(
        self,
        ref: CleanupRef,
        mode: CleanupMode,
        *,
        dry_run: bool = False,
    ) -> CleanupOutcome:
        if mode == CleanupMode.SOFT:
            return CleanupOutcome.DONE
        if dry_run:
            logger.info("EsAssetIndexOwner: dry-run — would delete %r.", ref.locator)
            return CleanupOutcome.DONE
        return await _delete_es_index(ref.locator)


def register_owners(registry: "CascadeCleanupRegistry") -> None:
    """Register public ES cascade owners into *registry*.

    Call from the ES driver lifespan BEFORE
    :meth:`~dynastore.modules.catalog.cascade_registry.CascadeCleanupRegistry.freeze`.
    """
    registry.register(EsItemsPublicIndexOwner())
    registry.register(EsAssetIndexOwner())
    logger.info(
        "EsPublicDriver: registered cascade owners %r, %r.",
        EsItemsPublicIndexOwner.owner_id,
        EsAssetIndexOwner.owner_id,
    )
