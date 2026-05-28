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

"""Cascade cleanup owner for the Elasticsearch envelope items driver.

:class:`EsItemsEnvelopeIndexOwner` manages the per-catalog envelope items
index named ``{prefix}-{catalog_id}-envelope-items`` (see
:func:`~.mappings.get_envelope_index_name`).

Only CATALOG scope is meaningful — the envelope driver uses a single shared
index per catalog, so deleting a collection does not touch ES.

Register via :func:`register_owners` from the driver's lifespan BEFORE the
CascadeCleanupRegistry is frozen.
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


def _get_envelope_index_name(catalog_id: str) -> str:
    from dynastore.modules.elasticsearch.client import get_index_prefix
    from dynastore.modules.storage.drivers.elasticsearch_envelope.mappings import (
        get_envelope_index_name,
    )
    return get_envelope_index_name(get_index_prefix(), catalog_id)


def _get_es_client() -> Any:
    from dynastore.modules.elasticsearch.client import get_client
    client = get_client()
    if client is None:
        raise RuntimeError(
            "Elasticsearch client is not initialized. "
            "Ensure ElasticsearchModule is registered and its lifespan has started."
        )
    return client


class EsItemsEnvelopeIndexOwner(BaseResourceOwner):
    """Resource owner for the per-catalog envelope items ES index.

    The envelope driver uses one index per catalog:
    ``{prefix}-{catalog_id}-envelope-items``.  Deleting a collection does
    NOT delete the shared index.
    """

    owner_id: ClassVar[str] = "es_envelope.items_index"

    def supported_scopes(self) -> Iterable[ResourceScope]:
        return (ResourceScope.CATALOG,)

    async def describe_scope(
        self, scope_ref: ScopeRef, conn: Any
    ) -> list[CleanupRef]:
        if scope_ref.scope != ResourceScope.CATALOG:
            return []
        index_name = _get_envelope_index_name(scope_ref.catalog_id)
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
            logger.info(
                "EsItemsEnvelopeIndexOwner: dry-run — would delete ES index %r.",
                ref.locator,
            )
            return CleanupOutcome.DONE

        try:
            es = _get_es_client()
            await es.indices.delete(
                index=ref.locator, params={"ignore_unavailable": "true"}
            )
            logger.info(
                "EsItemsEnvelopeIndexOwner: deleted ES index %r (catalog_id=%r).",
                ref.locator, ref.metadata.get("catalog_id"),
            )
            return CleanupOutcome.DONE
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "EsItemsEnvelopeIndexOwner: failed to delete ES index %r: %s",
                ref.locator, exc, exc_info=True,
            )
            return CleanupOutcome.RETRY


def register_owners(registry: "CascadeCleanupRegistry") -> None:
    """Register ES envelope cascade owners into *registry*.

    Call from the ES envelope driver lifespan BEFORE
    :meth:`~dynastore.modules.catalog.cascade_registry.CascadeCleanupRegistry.freeze`.
    """
    registry.register(EsItemsEnvelopeIndexOwner())
    logger.info(
        "EsEnvelopeDriver: registered cascade owner %r.",
        EsItemsEnvelopeIndexOwner.owner_id,
    )
