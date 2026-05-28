#    Copyright 2026 FAO
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

"""Cascade cleanup owners for the Elasticsearch private items driver.

:class:`EsItemsIndexOwner` manages the per-catalog private items index named
``{prefix}-{catalog_id}-private-items`` (see
:func:`~dynastore.modules.storage.drivers.elasticsearch_private.mappings.get_private_index_name`).
Only CATALOG scope is meaningful — the private driver uses a single shared
index per tenant, so deleting a collection does not touch ES.

Do NOT import these owners at module scope from anywhere that is loaded during
startup unless you call :func:`register_owners` explicitly after construction.
Call :func:`register_owners` from the driver's lifespan once the
:class:`~dynastore.modules.catalog.cascade_registry.CascadeCleanupRegistry`
is populated but before it is frozen.
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


def _get_index_name(catalog_id: str) -> str:
    """Return the private items index name for *catalog_id*.

    Delegates to :func:`~.mappings.get_private_index_name` so the naming
    convention is defined in exactly one place.  Pattern (from mappings.py):
    ``{prefix}-{catalog_id}-private-items``.
    """
    from dynastore.modules.elasticsearch.client import get_index_prefix
    from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
        get_private_index_name,
    )
    return get_private_index_name(get_index_prefix(), catalog_id)


def _get_es_client() -> Any:
    """Return the shared singleton AsyncElasticsearch/AsyncOpenSearch client."""
    from dynastore.modules.elasticsearch.client import get_client
    client = get_client()
    if client is None:
        raise RuntimeError(
            "Elasticsearch client is not initialized. "
            "Ensure ElasticsearchModule is registered and its lifespan has started."
        )
    return client


async def _revoke_deny_policy(catalog_id: str) -> None:
    """Strip and delete the ``private_deny_{catalog_id}`` DENY policy.

    Delegates to the driver method so the atomic unbind primitive is used
    in both the cascade cleanup path and the direct revoke path.
    """
    from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
        ItemsElasticsearchPrivateDriver,
    )
    await ItemsElasticsearchPrivateDriver._revoke_deny_policy(catalog_id)


class EsItemsIndexOwner(BaseResourceOwner):
    """Resource owner for the per-catalog private items ES index.

    The private driver uses one index per *catalog* (not per collection):
    ``{prefix}-{catalog_id}-private-items``.  All collections belonging to
    the same catalog share this index via ``collection_id`` field filtering.

    CATALOG scope  → one CleanupRef for the catalog-level index.
    COLLECTION scope → returns the same catalog-level index ref, because
        deleting a *collection* does NOT delete the shared index (other
        collections still use it).  Returning [] here is the correct
        behaviour — the index is cleaned up when the *catalog* is deleted.
    """

    owner_id: ClassVar[str] = "es_private.items_index"

    def supported_scopes(self) -> Iterable[ResourceScope]:
        return (ResourceScope.CATALOG,)

    async def describe_scope(
        self, scope_ref: ScopeRef, conn: Any
    ) -> list[CleanupRef]:
        if scope_ref.scope == ResourceScope.CATALOG:
            index_name = _get_index_name(scope_ref.catalog_id)
            return [
                CleanupRef(
                    kind="es_index",
                    locator=index_name,
                    owner_id=self.owner_id,
                    metadata={"catalog_id": scope_ref.catalog_id},
                )
            ]
        # COLLECTION / ASSET / ITEM — no per-collection index in the private driver.
        return []

    async def cleanup_one(
        self,
        ref: CleanupRef,
        mode: CleanupMode,
        *,
        dry_run: bool = False,
    ) -> CleanupOutcome:
        if mode == CleanupMode.SOFT:
            logger.info(
                "EsItemsIndexOwner: soft delete not implemented for ES index %r "
                "— index retained pending TTL reaper. #1456",
                ref.locator,
            )
            return CleanupOutcome.DONE

        # HARD delete — remove the index.
        if dry_run:
            logger.info(
                "EsItemsIndexOwner: dry-run — would delete ES index %r.", ref.locator
            )
            return CleanupOutcome.DONE

        try:
            es = _get_es_client()
            # ignore_unavailable=True (404) treats an already-absent index as DONE.
            await es.indices.delete(
                index=ref.locator, params={"ignore_unavailable": "true"}
            )
            logger.info(
                "EsItemsIndexOwner: deleted ES index %r (catalog_id=%r).",
                ref.locator, ref.metadata.get("catalog_id"),
            )
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "EsItemsIndexOwner: failed to delete ES index %r: %s",
                ref.locator, exc, exc_info=True,
            )
            return CleanupOutcome.RETRY

        # Revoke the matching DENY policy after the index is gone.
        # Ordering mirrors drop_storage: delete index first, then revoke policy.
        # Best-effort: IAM hiccups must not trigger a RETRY of the entire cascade.
        catalog_id: str = ref.metadata.get("catalog_id", "")
        if catalog_id:
            try:
                await _revoke_deny_policy(catalog_id)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "EsItemsIndexOwner: failed to revoke DENY policy for catalog %r: %s",
                    catalog_id, exc,
                )
        else:
            logger.warning(
                "EsItemsIndexOwner: catalog_id missing from CleanupRef metadata %r "
                "— DENY policy not revoked.",
                ref.locator,
            )

        return CleanupOutcome.DONE


def register_owners(registry: "CascadeCleanupRegistry") -> None:
    """Register ES private cascade owners into *registry*.

    Call this from the ES driver module's lifespan startup hook, BEFORE
    :meth:`~dynastore.modules.catalog.cascade_registry.CascadeCleanupRegistry.freeze`
    is called.  Do NOT call at module import time.
    """
    registry.register(EsItemsIndexOwner())
    logger.info(
        "EsPrivateDriver: registered cascade owner %r.",
        EsItemsIndexOwner.owner_id,
    )
