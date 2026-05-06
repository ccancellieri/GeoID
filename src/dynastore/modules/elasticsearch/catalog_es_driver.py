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

"""
Elasticsearch catalog-tier driver — implements :class:`CatalogStore`.

Stores catalog-tier records in a SINGLE shared ES index across all
catalogs (``{prefix}-catalogs`` from ``mappings.get_index_name(prefix,
"catalog")``).  Catalog cardinality is low — typically <100 per
deployment — so a per-catalog index would waste shards; a single shared
index also enables admin-panel cross-catalog search.

The mapping is the existing ``CATALOG_MAPPING`` from
``modules/elasticsearch/mappings.py``: STAC-shaped dynamic templates for
multilingual ``title``/``description``/``keywords``, explicit typing for
``catalog_id``/``stac_version``/``created``/``updated``.

Apply-handler scope: the handler runs at PLATFORM scope (no
``catalog_id`` filter) because the index is shared.  This is the
deliberate counterpart to the per-catalog
``CollectionElasticsearchDriver`` apply handler.
"""

import logging
from typing import Any, ClassVar, Dict, FrozenSet, Optional, Tuple

from dynastore.models.driver_context import DriverContext
from dynastore.models.protocols.entity_store import EntityStoreCapability
from dynastore.models.protocols.typed_driver import (
    TypedDriver,
    _PluginDriverConfig,
)
from dynastore.modules.db_config.platform_config_service import (
    Immutable,
    PluginConfig,
)
from dynastore.modules.storage.routing_config import Operation
from dynastore.modules.storage.storage_location import StorageLocation
from pydantic import Field

logger = logging.getLogger(__name__)


class CatalogElasticsearchDriverConfig(_PluginDriverConfig):
    """Configuration for the Elasticsearch ``CatalogStore`` driver.

    ``index_name`` is ``Immutable`` because changing it would orphan all
    existing catalog documents.  The default ``"catalogs"``
    matches ``mappings.get_index_name(prefix, "catalog")`` ⇒
    ``{prefix}-catalogs``.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "drivers")
    _visibility: ClassVar[Optional[str]] = "catalog"


    index_name: Immutable[str] = Field(
        "catalogs",
        description=(
            "Logical entity-name passed to "
            "``mappings.get_index_name(prefix, name)`` — the final "
            "physical index is ``{prefix}-{index_name}``.  Immutable "
            "once set; changing it would orphan existing documents."
        ),
    )


# CatalogElasticsearchDriverConfig auto-registers via PluginConfig.__init_subclass__.


async def _on_apply_catalog_es_driver_config(
    config: PluginConfig,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """Ensure the shared catalog metadata index exists at platform scope.

    Unlike :func:`_on_apply_collection_es_driver_config` (per-catalog
    index), this handler creates ONE index for ALL catalogs — so it runs
    at platform scope and ignores any incoming ``catalog_id``.  The
    apply-handler dispatcher invokes this function for every config
    apply; we no-op for the wrong config class.
    """
    if not isinstance(config, CatalogElasticsearchDriverConfig):
        return

    from dynastore.tools.discovery import get_protocol

    driver = get_protocol(CatalogElasticsearchDriver)
    if driver is None:
        return
    try:
        await driver.ensure_storage()
    except Exception as exc:
        logger.warning(
            "ensure_storage failed for elasticsearch_catalog_metadata: %s",
            exc,
        )


CatalogElasticsearchDriverConfig.register_apply_handler(_on_apply_catalog_es_driver_config)


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


class CatalogElasticsearchDriver(TypedDriver[CatalogElasticsearchDriverConfig]):
    """Elasticsearch implementation of :class:`CatalogStore`.

    Uses opensearch-py client (wire-compatible with ES and OpenSearch).
    Indexes ONE tier — catalog-tier metadata, keyed by ``catalog_id`` —
    and opts in to :class:`CatalogIndexer` only.

    Index naming
    ------------
    Single shared index ``{prefix}-{config.index_name}`` (default
    ``{prefix}-catalogs``) across all catalogs.  Document ``_id`` =
    ``catalog_id``.  Catalog cardinality is low (typically <100); a
    shared index avoids per-tenant shard overhead and enables admin
    cross-catalog search.
    """

    is_catalog_indexer: ClassVar[bool] = True

    # Catalog ES is the canonical async indexer + primary SEARCH backend
    # for catalog metadata routing.  Auto-defaults into both Operations.
    auto_register_for_routing: ClassVar[FrozenSet[str]] = frozenset({Operation.SEARCH, Operation.INDEX})

    capabilities: FrozenSet[str] = frozenset({
        EntityStoreCapability.READ,
        EntityStoreCapability.WRITE,
        EntityStoreCapability.SOFT_DELETE,
        EntityStoreCapability.SEARCH,
        EntityStoreCapability.AGGREGATION,
        EntityStoreCapability.PHYSICAL_ADDRESSING,
    })

    # ------------------------------------------------------------------
    # ES client + index naming helpers
    # ------------------------------------------------------------------

    def _get_client(self):
        from dynastore.modules.elasticsearch.client import get_client

        return get_client()

    def _get_prefix(self) -> str:
        from dynastore.modules.elasticsearch.client import get_index_prefix

        return get_index_prefix()

    def _index_name(self) -> str:
        from dynastore.modules.elasticsearch.mappings import get_index_name

        return get_index_name(self._get_prefix(), "catalog")

    def location(
        self, catalog_id: str, collection_id: Optional[str] = None,
    ) -> StorageLocation:
        index = self._index_name()
        return StorageLocation(
            backend="elasticsearch",
            canonical_uri=f"es://{index}/{catalog_id}",
            identifiers={
                "index": index,
                "prefix": self._get_prefix(),
                "catalog_id": catalog_id,
            },
            display_label=f"ES catalog metadata: {index}/{catalog_id}",
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def ensure_storage(self, catalog_id: Optional[str] = None) -> None:
        """Ensure the shared catalog metadata index exists (idempotent).

        ``catalog_id`` is accepted for signature parity with
        :class:`CollectionElasticsearchDriver` but ignored — the index is
        shared across all catalogs.
        """
        await self._ensure_index()

    async def _ensure_index(self) -> str:
        """Create the shared index if it doesn't exist. Returns index name."""
        from dynastore.modules.elasticsearch.mappings import CATALOG_MAPPING

        client = self._get_client()
        if not client:
            raise RuntimeError("Elasticsearch client not available")

        index_name = self._index_name()
        exists = await client.indices.exists(index=index_name)
        if not exists:
            await client.indices.create(
                index=index_name,
                body={
                    "mappings": CATALOG_MAPPING,
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 0,
                    },
                },
            )
            logger.info("Created catalog metadata index: %s", index_name)
        return index_name

    # ------------------------------------------------------------------
    # CatalogStore — CRUD
    # ------------------------------------------------------------------

    async def get_catalog_metadata(
        self,
        catalog_id: str,
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        client = self._get_client()
        if not client:
            return None

        index_name = self._index_name()
        if not await client.indices.exists(index=index_name):
            return None
        try:
            resp = await client.get(index=index_name, id=catalog_id)
            return resp["_source"]
        except Exception:
            return None

    async def upsert_catalog_metadata(
        self,
        catalog_id: str,
        metadata: Dict[str, Any],
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        await self._ensure_index()
        client = self._get_client()
        if not client:
            raise RuntimeError("Elasticsearch client not available")

        doc = dict(metadata)
        doc["id"] = catalog_id
        doc["catalog_id"] = catalog_id

        await client.index(
            index=self._index_name(),
            id=catalog_id,
            body=doc,
            params={"refresh": "wait_for"},
        )

    async def delete_catalog_metadata(
        self,
        catalog_id: str,
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> None:
        client = self._get_client()
        if not client:
            return

        index_name = self._index_name()
        try:
            if soft:
                await client.update(
                    index=index_name,
                    id=catalog_id,
                    body={"doc": {"_deleted": True}},
                    params={"refresh": "wait_for"},
                )
            else:
                await client.delete(
                    index=index_name,
                    id=catalog_id,
                    params={"refresh": "wait_for"},
                )
        except Exception as e:
            logger.debug(
                "delete_catalog_metadata ES error for %s: %s", catalog_id, e,
            )

    # ------------------------------------------------------------------
    # Plumbing
    # ------------------------------------------------------------------

    async def get_driver_config(
        self,
        catalog_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Any:
        from dynastore.models.protocols import ConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        configs = get_protocol(ConfigsProtocol)
        if not configs:
            return {}
        try:
            return await configs.get_config(
                CatalogElasticsearchDriverConfig,
                catalog_id=catalog_id,
                ctx=DriverContext(db_resource=db_resource),
            )
        except Exception:
            return {}

    async def is_available(self) -> bool:
        client = self._get_client()
        if not client:
            return False
        try:
            info = await client.info()
            return bool(info)
        except Exception:
            return False
