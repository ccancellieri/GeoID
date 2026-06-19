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
from typing import Any, ClassVar, Dict, FrozenSet, List, Optional, Tuple

from dynastore.models.driver_context import DriverContext
from dynastore.models.protocols.entity_store import EntityStoreCapability
from dynastore.models.protocols.teardown_lane import TeardownLane
from dynastore.models.protocols.typed_driver import (
    TypedDriver,
    _PluginDriverConfig,
)
from dynastore.models.mutability import Immutable
from dynastore.models.plugin_config import PluginConfig
from dynastore.modules.storage.driver_config import DriverCapability
from dynastore.modules.storage.hints import Hint
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
    _freeze_at: ClassVar[Optional[str]] = "catalog"

    required_engine_class: ClassVar[str] = "elasticsearch_engine"

    capabilities: ClassVar[FrozenSet[str]] = frozenset({DriverCapability.ASYNC})

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

    teardown_lane: ClassVar[TeardownLane] = TeardownLane.ASYNC_CASCADE

    # Catalog ES is the canonical async secondary index + primary SEARCH
    # backend for catalog metadata routing.  It auto-defaults into WRITE
    # (as a secondary index, identified by ``is_catalog_indexer``), SEARCH,
    # and READ (hinted — only selected when the caller explicitly requests it
    # via ``prefer:es`` or ``geometry_simplified``; default path stays on PG).
    auto_register_for_routing: ClassVar[FrozenSet[str]] = frozenset({
        Operation.SEARCH, Operation.WRITE, Operation.READ,
    })

    # Hints this driver serves on the READ/SEARCH operations.
    # GEOMETRY_SIMPLIFIED: ES stores the index-time simplified geometry.
    # METADATA: generic "I want catalog metadata" — declares this driver
    #   participates in metadata reads at all.  There is no geometry at the
    #   metadata level so geometry hints (GEOMETRY_EXACT) do not apply here.
    supported_hints: ClassVar[FrozenSet[Hint]] = frozenset({
        Hint.GEOMETRY_SIMPLIFIED,
        Hint.METADATA,
    })

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

    async def ensure_storage(
        self, catalog_id: Optional[str] = None, **kwargs: Any
    ) -> None:
        """Ensure the shared catalog metadata index exists (idempotent).

        ``catalog_id`` is accepted for signature parity with
        :class:`CollectionElasticsearchDriver` but ignored — the index is
        shared across all catalogs.
        """
        await self._ensure_index()

    async def drop_storage(
        self, catalog_id: str, *, soft: bool = False
    ) -> None:
        """Remove this catalog's record from the shared ``{prefix}-catalogs``
        index (idempotent).

        Catalog-tier ``drop_storage``: a catalog owns no per-catalog index
        (the index is shared), so the only catalog-scoped teardown is
        removing its document — delegated to :meth:`delete_catalog_metadata`,
        which honours ``soft`` (sets ``_deleted`` instead of removing the
        doc).  Completes the ``CatalogStore`` lifecycle contract; the live
        catalog hard-delete already drives this same primitive via the
        secondary-index DELETE fan-out, so the two entry points are
        idempotent.
        """
        await self.delete_catalog_metadata(catalog_id, soft=soft)

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
        from dynastore.modules.storage.routing_config import (
            get_output_transformers_for_search,
        )
        from dynastore.modules.storage.transform_runtime import (
            restore_transform_chain,
        )
        from dynastore.tools.typed_store.base import _to_snake

        client = self._get_client()
        if not client:
            return None

        index_name = self._index_name()
        if not await client.indices.exists(index=index_name):
            return None
        try:
            resp = await client.get(index=index_name, id=catalog_id)
            from dynastore.modules.elasticsearch.catalog_canonical import (
                unproject_catalog_from_es,
            )
            doc = unproject_catalog_from_es(resp["_source"])
        except Exception as exc:  # noqa: BLE001
            # Document absent (404) or transient transport error.  The read
            # contract allows None for "not found"; a missing catalog metadata
            # document does not break the catalog itself (PG is the SoR).
            logger.debug(
                "CatalogElasticsearchDriver.get_catalog_metadata: ES get failed "
                "for catalog=%r index=%r: %s",
                catalog_id, index_name, exc,
            )
            return None

        restore_chain = await get_output_transformers_for_search(
            catalog_id,
            entity="catalog",
            collection_id=None,
            driver_ref=_to_snake(type(self).__name__),
        )
        if restore_chain:
            from dynastore.models.protocols.entity_transform import (
                TransformChainContext,
            )
            doc = await restore_transform_chain(
                doc,
                restore_chain,
                catalog_id=catalog_id,
                collection_id=None,
                entity_kind="catalog",
                ctx=TransformChainContext(),
            )
        return doc

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

        # Canonical catalog envelope (#1285/#1800): identity + system + access
        # containers, attributes under properties (unknown→extras lane).
        from dynastore.modules.elasticsearch.catalog_canonical import (
            build_canonical_catalog_doc,
        )
        from dynastore.modules.elasticsearch.items_projection import build_known_fields

        doc = build_canonical_catalog_doc(
            metadata,
            catalog_id=catalog_id,
            known_fields=build_known_fields(),
        )

        index_name = self._index_name()
        try:
            await client.index(
                index=index_name,
                id=catalog_id,
                body=doc,
                params={"refresh": "wait_for"},
            )
        except Exception:
            # #728: structured 400-body logging — the opensearchpy
            # transport logger drops response bodies, so parse-time
            # failures (e.g. document_parsing_exception on a stale
            # dynamic mapping) were invisible.
            logger.warning(
                "CatalogElasticsearchDriver.upsert_catalog_metadata failed: "
                "catalog=%r index=%r",
                catalog_id, index_name,
                exc_info=True,
            )
            raise

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
    # Indexer Protocol — dispatcher-facing surface
    # ------------------------------------------------------------------

    async def ensure_indexer(self, ctx: Any) -> None:
        """Idempotent bootstrap — delegates to :meth:`ensure_storage`."""
        await self.ensure_storage(ctx.catalog)

    async def index(self, ctx: Any, op: Any) -> None:
        """Apply a single catalog-tier op (upsert or delete).

        ``op.entity_id`` is the catalog id; ``op.payload`` carries the
        catalog metadata for upserts.

        Tier guard (#728): refuses non-catalog ops outright so a
        misconfigured ItemsRoutingConfig / CollectionRoutingConfig
        pointing at this driver surfaces immediately rather than
        poisoning the catalog index's dynamic mapping.
        """
        op_entity_type = getattr(op, "entity_type", None)
        if op_entity_type is not None and op_entity_type != "catalog":
            payload_type = (op.payload or {}).get("type") if op.op_type == "upsert" else None
            logger.error(
                "CatalogElasticsearchDriver refused non-catalog op: "
                "op_entity_type=%r op_type=%r entity_id=%r payload_type=%r — "
                "check routing config for this tier.",
                op_entity_type, op.op_type, op.entity_id, payload_type,
            )
            raise ValueError(
                f"CatalogElasticsearchDriver.index: refused op with "
                f"entity_type={op_entity_type!r}; this driver only accepts "
                f"catalog-tier ops."
            )
        if op.op_type == "upsert":
            payload = op.payload or {}
            if payload.get("type") == "Feature":
                logger.error(
                    "CatalogElasticsearchDriver refused STAC Feature payload: "
                    "entity_id=%r — items must not land on the catalog index.",
                    op.entity_id,
                )
                raise ValueError(
                    "CatalogElasticsearchDriver.index: refused payload with "
                    "type='Feature'; STAC items belong on the items-tier index."
                )
            await self.upsert_catalog_metadata(op.entity_id, payload)
        elif op.op_type == "delete":
            await self.delete_catalog_metadata(op.entity_id)
        else:
            raise ValueError(
                f"CatalogElasticsearchDriver.index: unsupported op_type "
                f"{op.op_type!r}"
            )

    async def index_bulk(self, ctx: Any, ops: Any) -> Any:
        """Apply a batch of catalog-tier ops via per-op :meth:`index`.

        Catalog cardinality is low (typically <100); a per-op loop costs
        one round-trip per op but keeps the failure-isolation simple. ES
        ``_bulk`` is reserved for high-cardinality tiers (items/assets).
        """
        from dynastore.models.protocols.indexer import BulkResult

        total = len(ops)
        failures: List[Dict[str, Any]] = []
        succeeded = 0
        for op in ops:
            try:
                await self.index(ctx, op)
                succeeded += 1
            except Exception as exc:
                # #728: log per-op failure with exc_info so the real
                # cause (e.g. document_parsing_exception body, refused
                # tier guard) reaches structured logs.
                logger.warning(
                    "CatalogElasticsearchDriver.index_bulk op failed: "
                    "entity_id=%r op_type=%r",
                    op.entity_id, op.op_type,
                    exc_info=True,
                )
                failures.append({
                    "entity_id": op.entity_id,
                    "op_type": op.op_type,
                    "error": str(exc),
                })
        return BulkResult(
            total=total,
            succeeded=succeeded,
            failed=len(failures),
            failures=failures,
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
        except Exception as exc:  # noqa: BLE001
            # Config row absent or DB unavailable at call time.  Returning an
            # empty dict lets the caller apply defaults without crashing.
            logger.debug(
                "CatalogElasticsearchDriver.get_driver_config: get_config failed "
                "for catalog=%r: %s",
                catalog_id, exc,
            )
            return {}

    async def is_available(self) -> bool:
        client = self._get_client()
        if not client:
            return False
        try:
            info = await client.info()
            return bool(info)
        except Exception as exc:  # noqa: BLE001
            # Transport error on the info probe — cluster unreachable or auth
            # rejected.  Return False so callers can degrade gracefully.
            logger.debug(
                "CatalogElasticsearchDriver.is_available: info probe failed: %s",
                exc,
            )
            return False
