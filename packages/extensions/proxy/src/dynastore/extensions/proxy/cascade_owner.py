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

"""Cascade cleanup owner for proxy short URLs.

:class:`ProxyUrlOwner` replaces the ``register_event_listener`` hook in
``hooks.py`` that previously cleaned up collection proxy URLs on
COLLECTION_DELETION and COLLECTION_HARD_DELETION events.

COLLECTION scope, HARD only: soft-delete retains the URL rows so they can
be restored if the collection is un-deleted.

Register via :func:`register_owners` from the proxy service lifespan BEFORE
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


class ProxyUrlOwner(BaseResourceOwner):
    """Cleans up ``collection_proxy_urls`` rows for a deleted collection.

    ``describe_scope`` reads all ``short_key`` values for the collection
    inside the caller's transaction (before the collection row is dropped).
    ``cleanup_one`` calls ``delete_short_url`` for the given short_key —
    idempotent if the key is already gone.
    """

    owner_id: ClassVar[str] = "proxy.urls"

    def supported_scopes(self) -> Iterable[ResourceScope]:
        return (ResourceScope.COLLECTION,)

    async def describe_scope(
        self, scope_ref: ScopeRef, conn: Any
    ) -> list[CleanupRef]:
        if scope_ref.scope != ResourceScope.COLLECTION or scope_ref.collection_id is None:
            return []

        from dynastore.models.protocols import CatalogsProtocol
        from dynastore.modules import get_protocol
        from dynastore.models.driver_context import DriverContext
        from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler

        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            return []

        phys_schema = await catalogs.resolve_physical_schema(
            scope_ref.catalog_id, ctx=DriverContext(db_resource=conn)
        )
        if not phys_schema:
            return []

        collection_physical_id = await catalogs.resolve_physical_id(
            scope_ref.catalog_id,
            scope_ref.collection_id,
            ctx=DriverContext(db_resource=conn),
            allow_missing=True,
        )
        lookup_id = collection_physical_id or scope_ref.collection_id

        short_keys = await DQLQuery(
            f"SELECT short_key FROM {phys_schema}.collection_proxy_urls"
            " WHERE collection_physical_id = :coll",
            result_handler=ResultHandler.ALL_SCALARS,
        ).execute(conn, coll=lookup_id)

        return [
            CleanupRef(
                kind="proxy_short_url",
                locator=str(key),
                owner_id=self.owner_id,
                metadata={
                    "catalog_id": scope_ref.catalog_id,
                    "collection_id": scope_ref.collection_id,
                },
            )
            for key in (short_keys or [])
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

        catalog_id = ref.metadata.get("catalog_id", "")
        short_key = ref.locator

        if dry_run:
            logger.info(
                "ProxyUrlOwner: dry-run — would delete short_key=%r (catalog=%r).",
                short_key, catalog_id,
            )
            return CleanupOutcome.DONE

        try:
            from dynastore.tools.protocol_helpers import get_engine
            from dynastore.modules.db_config.query_executor import managed_transaction

            engine = get_engine()
            if engine is None:
                logger.error("ProxyUrlOwner: no DB engine — cannot delete short_key=%r.", short_key)
                return CleanupOutcome.RETRY

            async with managed_transaction(engine) as conn:
                from dynastore.modules.proxy.proxy_module import delete_short_url
                await delete_short_url(conn, catalog_id=catalog_id, short_key=short_key)

            logger.info("ProxyUrlOwner: deleted short_key=%r (catalog=%r).", short_key, catalog_id)
            return CleanupOutcome.DONE
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "ProxyUrlOwner: failed to delete short_key=%r: %s",
                short_key, exc, exc_info=True,
            )
            return CleanupOutcome.RETRY


def register_owners(registry: "CascadeCleanupRegistry") -> None:
    """Register proxy cascade owners into *registry*.

    Call from the proxy service lifespan BEFORE
    :meth:`~dynastore.modules.catalog.cascade_registry.CascadeCleanupRegistry.freeze`.
    """
    registry.register(ProxyUrlOwner())
    logger.info("ProxyService: registered cascade owner %r.", ProxyUrlOwner.owner_id)
