import logging
import re
from typing import Any, Dict, Literal, Optional
from contextlib import asynccontextmanager

# Hard runtime dep — fail entry-point load on services without ``opensearch-py``
# installed (i.e. SCOPEs that don't include ``module_elasticsearch``).  Without
# this, the module imports cleanly via lazy ``opensearchpy`` calls inside method
# bodies and registers as an ``IndexerProtocol`` provider, which makes the
# CapabilityMap mark ``elasticsearch_index`` claimable on services that cannot
# actually run it (tools, etc.).  Failing the import here makes the framework
# fall back to ``_register_definition_only_placeholders`` and keep these tasks
# off that service's claim list.
import opensearchpy  # noqa: F401

from dynastore.modules import ModuleProtocol
from dynastore.models.protocols.event_bus import EventBusProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.catalog.event_service import CatalogEventType
from dynastore.modules.elasticsearch.es_catalog_config import ElasticsearchCatalogConfig

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _is_data_stream(es: Any, name: str) -> bool:
    """Return True when ``name`` exists as a data stream in OpenSearch.

    The OpenSearch / Elasticsearch ``indices.exists()`` API is ambiguous —
    it returns True for both regular indices and data streams sharing the
    name. Use the dedicated data-streams API to disambiguate; on a regular
    index the call returns 404 (caught) and we report False.

    Used by the lifespan startup check to fail-fast when a data stream has
    been provisioned where the platform expects a regular mutable index
    (collection / catalog metadata indices).
    """
    try:
        result = await es.indices.get_data_stream(name=name)
    except Exception:
        # 404 = no data stream by that name (regular index or no index).
        # Any other transport error is also "not provably a stream"; the
        # broader except in the caller will surface real connectivity
        # issues via the warning path.
        return False
    streams = (result or {}).get("data_streams", []) if isinstance(result, dict) else []
    return any((s or {}).get("name") == name for s in streams)


async def _get_es_catalog_config(catalog_id: str):
    """Return ElasticsearchCatalogConfig for catalog_id, or None."""
    try:
        from dynastore.models.protocols.configs import ConfigsProtocol
        configs = get_protocol(ConfigsProtocol)
        if not configs:
            return None
        return await configs.get_config(ElasticsearchCatalogConfig, catalog_id=catalog_id)
    except Exception as e:
        logger.debug("Could not resolve ES catalog config for '%s': %s", catalog_id, e)
        return None


async def _is_es_active(catalog_id: str, collection_id: str) -> bool:
    """Return True when the collection has ES as write, secondary, or read driver."""
    try:
        from dynastore.models.protocols.configs import ConfigsProtocol

        configs = get_protocol(ConfigsProtocol)
        if not configs:
            return False
        from dynastore.modules.storage.routing_config import CollectionRoutingConfig
        routing = await configs.get_config(
            CollectionRoutingConfig,
            catalog_id=catalog_id,
            collection_id=collection_id,
        )
        if not isinstance(routing, CollectionRoutingConfig):
            return False
        for entries in routing.operations.values():
            for entry in entries:
                if entry.driver_id == "items_elasticsearch_driver":
                    return True
        return False
    except Exception as e:
        logger.debug(
            "Could not resolve ES active config for %s/%s: %s",
            catalog_id, collection_id, e,
        )
        return False


async def _stac_serialize_item(catalog_id: str, collection_id: str, item_id: str) -> Optional[dict]:
    """Fetch the item and serialize it as a full STAC document."""
    try:
        from dynastore.modules.catalog.item_service import ItemService
        from dynastore.models.protocols import DbProtocol

        db = get_protocol(DbProtocol)
        item_svc = get_protocol(ItemService)
        if not item_svc:
            item_svc = ItemService(engine=db)  # type: ignore[arg-type]

        feature = await item_svc.get_item(catalog_id, collection_id, item_id)
        if feature is None:
            return None

        doc = feature.model_dump(by_alias=True, exclude_none=True)
        doc["catalog_id"] = catalog_id
        doc["collection_id"] = collection_id
        return doc
    except Exception as e:
        logger.warning(
            "Failed to STAC-serialize item %s/%s/%s: %s",
            catalog_id, collection_id, item_id, e,
        )
        return None


async def _stac_serialize_catalog(catalog_id: str) -> Optional[dict]:
    """Serialize a catalog as a STAC dict from its metadata model."""
    try:
        from dynastore.models.protocols import CatalogsProtocol
        catalogs = get_protocol(CatalogsProtocol)
        if not catalogs:
            return None
        model = await catalogs.get_catalog_model(catalog_id)
        if model is None:
            return None
        doc = model.model_dump(by_alias=True, exclude_none=True) if hasattr(model, "model_dump") else {}
        doc["catalog_id"] = catalog_id
        doc.setdefault("id", catalog_id)
        return doc
    except Exception as e:
        logger.warning("Failed to serialize catalog %s: %s", catalog_id, e)
        return None


async def _stac_serialize_collection(catalog_id: str, collection_id: str) -> Optional[dict]:
    """Serialize a collection as a STAC dict from its metadata model."""
    try:
        from dynastore.models.protocols import CatalogsProtocol
        catalogs = get_protocol(CatalogsProtocol)
        if not catalogs:
            return None
        model = await catalogs.get_collection_model(catalog_id, collection_id)  # type: ignore[attr-defined]
        if model is None:
            return None
        doc = model.model_dump(by_alias=True, exclude_none=True) if hasattr(model, "model_dump") else {}
        doc["catalog_id"] = catalog_id
        doc["collection_id"] = collection_id
        doc.setdefault("id", collection_id)
        return doc
    except Exception as e:
        logger.warning("Failed to serialize collection %s/%s: %s", catalog_id, collection_id, e)
        return None


# ---------------------------------------------------------------------------
# ElasticsearchModule
# ---------------------------------------------------------------------------

class ElasticsearchModule(ModuleProtocol):
    """
    Listens to domain events and dispatches indexing tasks to Elasticsearch.

    Implements ``IndexerProtocol`` so that other components can discover the
    indexing backend via ``get_protocol(IndexerProtocol)`` without importing
    this module directly.

    Per-catalog private indexing is configured at runtime via:
        PUT /configs/catalogs/{catalog_id}/elasticsearch  {"private": true}

    When a catalog is private:
    - Items are indexed only as {geoid, catalog_id, collection_id} in a
      dedicated geoid index — no geometry, no attributes.
    - All GET access to the catalog via any protocol is denied to all_users.
    - The standard STAC items index is never populated for this catalog.
    """

    priority: int = 50

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        events = get_protocol(EventBusProtocol)

        _registered: list = []
        if events:
            for etype, handler in [
                (CatalogEventType.CATALOG_CREATION,         self._on_catalog_upsert),
                (CatalogEventType.CATALOG_UPDATE,           self._on_catalog_upsert),
                (CatalogEventType.CATALOG_DELETION,         self._on_catalog_delete),
                (CatalogEventType.CATALOG_HARD_DELETION,    self._on_catalog_delete),
                (CatalogEventType.COLLECTION_CREATION,      self._on_collection_upsert),
                (CatalogEventType.COLLECTION_UPDATE,        self._on_collection_upsert),
                (CatalogEventType.COLLECTION_DELETION,      self._on_collection_delete),
                (CatalogEventType.COLLECTION_HARD_DELETION, self._on_collection_delete),
                (CatalogEventType.ITEM_CREATION,            self._on_item_upsert),
                (CatalogEventType.ITEM_UPDATE,              self._on_item_upsert),
                (CatalogEventType.ITEM_DELETION,            self._on_item_delete),
                (CatalogEventType.ITEM_HARD_DELETION,       self._on_item_delete),
                (CatalogEventType.BULK_ITEM_CREATION,       self._on_item_bulk_upsert),
            ]:
                decorator = events.async_event_listener(etype)
                if decorator:
                    decorator(handler)
                    _registered.append((etype, handler))
                else:
                    logger.warning(
                        "ElasticsearchModule: Failed to register listener for %s", etype
                    )
            logger.info("ElasticsearchModule: Registered async event listeners.")
        else:
            logger.warning(
                "ElasticsearchModule: EventsProtocol not found. Indexing events not captured."
            )

        # Restore in-memory DENY policies for all catalogs that have private=True.
        # on_apply is not called automatically on service restart, so we do it here.
        await self._restore_private_policies()

        from dynastore.modules.elasticsearch import client as es_client
        await es_client.init()

        # Register log backend for batch log persistence
        from dynastore.modules.elasticsearch.log_backend import ElasticsearchLogBackend
        from dynastore.modules.elasticsearch.mappings import LOG_MAPPING, get_log_index_name

        log_backend = ElasticsearchLogBackend()
        from dynastore.tools.discovery import register_plugin
        register_plugin(log_backend)

        # Ensure log index exists
        es = es_client.get_client()
        if es is not None:
            index_name = get_log_index_name(es_client.get_index_prefix())
            try:
                if not await es.indices.exists(index=index_name):
                    await es.indices.create(index=index_name, body={"mappings": LOG_MAPPING})
                    logger.info("ElasticsearchModule: Created log index '%s'.", index_name)
            except Exception as exc:
                logger.warning(
                    "ElasticsearchModule: Could not ensure log index '%s': %s",
                    index_name,
                    exc,
                )

        # Ensure platform-wide shared indexes + the regular-items alias exist.
        # Per-tenant indexes (dynastore-items-{cat}) are created on demand by
        # the regular items driver's ensure_storage; the platform creates only
        # the shared collection/catalog indexes here so reads against them
        # never hit "index_not_found_exception" before the first write.
        if es is not None:
            from dynastore.modules.elasticsearch.aliases import (
                ensure_public_alias_exists,
            )
            from dynastore.modules.elasticsearch.mappings import (
                CATALOG_MAPPING,
                COLLECTION_MAPPING,
            )

            for shared_name, mapping in (
                (f"{es_client.get_index_prefix()}-collections", COLLECTION_MAPPING),
                (f"{es_client.get_index_prefix()}-catalogs",    CATALOG_MAPPING),
            ):
                try:
                    # Data-stream fail-fast: indices.exists() returns True for
                    # both regular indices AND data streams, so a stream that
                    # snuck in (cluster-side index template, ISM policy, manual
                    # creation) would silently take precedence here. Data
                    # streams reject the upserts the collection/catalog
                    # drivers issue ("only write ops with op_type=create are
                    # allowed in data streams" — observed on review env
                    # 2026-04-30 against `{prefix}-collections`). The
                    # platform requires regular mutable indices for
                    # collection/catalog metadata; refuse to start when a
                    # stream is in the way.
                    if await _is_data_stream(es, shared_name):
                        msg = (
                            f"ElasticsearchModule: '{shared_name}' is a "
                            "data stream, but the platform requires a "
                            "regular index for mutable metadata upserts. "
                            f"Delete it before redeploy: "
                            f"DELETE /_data_stream/{shared_name}"
                        )
                        logger.error(msg)
                        raise RuntimeError(msg)
                    if not await es.indices.exists(index=shared_name):
                        await es.indices.create(
                            index=shared_name, body={"mappings": mapping},
                        )
                        logger.info(
                            "ElasticsearchModule: Created shared index '%s'.",
                            shared_name,
                        )
                except RuntimeError:
                    # Re-raise our explicit fail-fast — must surface to the
                    # operator, never get swallowed by the broad except below.
                    raise
                except Exception as exc:
                    logger.warning(
                        "ElasticsearchModule: Could not ensure shared index "
                        "'%s': %s", shared_name, exc,
                    )

            # Public items alias name (e.g. `dynastore-items`) collides with
            # the legacy singleton index name from before PR-B's topology
            # rework. If a stale physical index by that name exists, ES
            # will reject every attempt to create the alias — silently
            # leaving search broken with `index_or_alias_not_found`.
            # Fail-fast at startup with a clear remediation message so the
            # operator wipes the stale index before retrying.
            from dynastore.modules.elasticsearch.mappings import get_public_items_alias

            alias_name = get_public_items_alias(es_client.get_index_prefix())
            try:
                exists = await es.indices.exists(index=alias_name)
                is_alias = await es.indices.exists_alias(name=alias_name)
            except Exception as exc:
                logger.warning(
                    "ElasticsearchModule: alias-collision pre-check failed for "
                    "'%s': %s — proceeding without fail-fast", alias_name, exc,
                )
                exists = False
                is_alias = True
            if exists and not is_alias:
                msg = (
                    f"ElasticsearchModule: physical index '{alias_name}' "
                    f"exists where alias is required. Delete it before "
                    f"redeploy: DELETE /{alias_name}"
                )
                logger.error(msg)
                raise RuntimeError(msg)

            try:
                await ensure_public_alias_exists()
            except Exception as exc:
                logger.warning(
                    "ElasticsearchModule: ensure_public_alias_exists raised: %s",
                    exc,
                )

        # Auto-provision the logs dashboard into OpenSearch Dashboards / Kibana.
        # No-op when KIBANA_UPSTREAM_URL is unset; never raises.
        from dynastore.modules.elasticsearch.dashboards_provisioner import (
            provision_dashboards,
        )
        try:
            await provision_dashboards()
        except Exception as exc:  # defensive — provisioner already swallows internally
            logger.warning(
                "ElasticsearchModule: dashboard provisioning raised unexpectedly: %s",
                exc,
            )

        try:
            yield
        finally:
            for etype, handler in _registered:
                if events is not None:
                    events.unregister(etype, handler)  # type: ignore[attr-defined]
            await es_client.close()

    # ------------------------------------------------------------------
    # Task dispatcher
    # ------------------------------------------------------------------

    async def _dispatch_task(self, task_type: str, inputs: Any, db_resource=None):
        """Enqueue a task into the default task schema."""
        from dynastore.models.protocols import DatabaseProtocol
        db = db_resource or get_protocol(DatabaseProtocol)
        if not db:
            logger.warning(
                "ElasticsearchModule: DatabaseProtocol not found. Cannot dispatch %s.",
                task_type,
            )
            return
        engine = db.engine if isinstance(db, DatabaseProtocol) else db
        try:
            from dynastore.modules.tasks import tasks_module
            from dynastore.modules.tasks.models import TaskCreate
            await tasks_module.create_task(
                engine=engine,
                task_data=TaskCreate(
                    caller_id="system:elasticsearch",
                    task_type=task_type,
                    inputs=inputs,
                ),
                schema=tasks_module.get_task_schema(),
            )
        except Exception as e:
            logger.error("ElasticsearchModule: Failed to dispatch task %s: %s", task_type, e)

    # ------------------------------------------------------------------
    # Private mode: enable / disable
    # ------------------------------------------------------------------

    async def enable_private_mode(self, catalog_id: str, db_resource=None) -> None:
        """
        Apply the DENY access policy, ensure the geoid index exists, and
        dispatch a bulk reindex task (private mode).

        Called by on_apply when private=True is written, and by lifespan
        to restore in-memory policies on service restart.
        """
        logger.info("ElasticsearchModule: Enabling private mode for catalog '%s'.", catalog_id)
        await self._apply_private_policy(catalog_id)
        await self._ensure_private_index(catalog_id)
        # Bulk reindex of private data is no longer supported by the
        # shared task — the private driver requires a fresh-start
        # cutover instead. Toggling private indexing only flips the DENY
        # policy + ensures the private index exists; existing items
        # stay where they are.

    async def disable_private_mode(self, catalog_id: str, db_resource=None) -> None:
        """
        Remove the DENY access policy and dispatch a bulk reindex task
        targeting the per-tenant items index so historical items become
        discoverable again under the regular driver.

        Called by on_apply when private=False is written.
        """
        logger.info("ElasticsearchModule: Disabling private mode for catalog '%s'.", catalog_id)
        await self._revoke_private_policy(catalog_id)
        from dynastore.tasks.elasticsearch_indexer.tasks import BulkCatalogReindexInputs
        await self._dispatch_task(
            task_type="elasticsearch_bulk_reindex_catalog",
            inputs=BulkCatalogReindexInputs(
                catalog_id=catalog_id,
            ).model_dump(),
            db_resource=db_resource,
        )

    # ------------------------------------------------------------------
    # Access policy management
    # ------------------------------------------------------------------

    async def _apply_private_policy(self, catalog_id: str) -> None:
        """Create/update a persisted DENY policy that blocks all_users GET
        access across every protocol path under the given catalog, and
        also registers it in-memory for immediate effect in this process."""
        from dynastore.models.protocols.policies import PermissionProtocol, Policy, Role

        perm = get_protocol(PermissionProtocol)
        if not perm:
            logger.warning(
                "ElasticsearchModule: PermissionProtocol unavailable — "
                "DENY policy not applied for catalog '%s'.",
                catalog_id,
            )
            return

        policy_id = f"private_deny_{catalog_id}"
        deny_policy = Policy(
            id=policy_id,
            description=f"Blocks public access to private catalog: {catalog_id}",
            actions=["GET"],
            resources=[
                # Covers: /catalog/catalogs/X/*, /stac/catalogs/X/*,
                #         /features/catalogs/X/*, /tiles/catalogs/X/*,
                #         /wfs/catalogs/X/*, /maps/catalogs/X/*
                f"/(catalog|stac|features|tiles|wfs|maps)/catalogs/{re.escape(catalog_id)}(/.*)?",
            ],
            effect="DENY",
        )

        # In-memory: immediate effect in this process.
        perm.register_policy(deny_policy)
        perm.register_role(Role(name="all_users", policies=[policy_id]))

        # Persistent: survives restarts and propagates to other processes.
        try:
            await perm.create_policy(deny_policy)
            logger.info(
                "ElasticsearchModule: DENY policy '%s' persisted for catalog '%s'.",
                policy_id, catalog_id,
            )
        except Exception:
            try:
                await perm.update_policy(deny_policy)
                logger.debug("ElasticsearchModule: DENY policy '%s' updated.", policy_id)
            except Exception as e:
                logger.error(
                    "ElasticsearchModule: Could not persist DENY policy '%s': %s",
                    policy_id, e,
                )

    async def _revoke_private_policy(self, catalog_id: str) -> None:
        """Remove the persisted DENY policy for the catalog.
        The in-memory copy persists until the next restart — this is
        acceptable since the policy will not be recreated at startup."""
        from dynastore.models.protocols.policies import PermissionProtocol

        perm = get_protocol(PermissionProtocol)
        if not perm:
            return

        policy_id = f"private_deny_{catalog_id}"
        try:
            await perm.delete_policy(policy_id)
            logger.info(
                "ElasticsearchModule: DENY policy '%s' removed for catalog '%s'.",
                policy_id, catalog_id,
            )
        except Exception as e:
            logger.debug(
                "ElasticsearchModule: Could not remove DENY policy '%s' (may not exist): %s",
                policy_id, e,
            )

    # ------------------------------------------------------------------
    # ES index management
    # ------------------------------------------------------------------

    async def _ensure_private_index(self, catalog_id: str) -> None:
        """Ensure the per-tenant feature ES index exists with the current mapping.

        If the index exists with the legacy 3-field mapping, drop it so the
        next reindex repopulates it with full features under
        ``TENANT_FEATURE_MAPPING``. The caller (``enable_private_mode``)
        already dispatches a bulk reindex after this returns.
        """
        from dynastore.modules.elasticsearch import client as es_client
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            TENANT_FEATURE_MAPPING,
            get_private_index_name,
        )

        es = es_client.get_client()
        if es is None:
            logger.warning("ElasticsearchModule: ES client not initialized, skipping private index creation.")
            return

        index_name = get_private_index_name(es_client.get_index_prefix(), catalog_id)
        try:
            if await es.indices.exists(index=index_name):
                # Detect legacy mapping (no `geometry` field) and drop the
                # index so it can be recreated with the new shape.
                try:
                    current = await es.indices.get_mapping(index=index_name)
                    props = (
                        current.get(index_name, {})
                        .get("mappings", {})
                        .get("properties", {})
                    )
                    if "geometry" not in props:
                        logger.info(
                            "ElasticsearchModule: legacy private mapping detected on '%s', recreating.",
                            index_name,
                        )
                        await es.indices.delete(index=index_name, ignore_unavailable=True)  # type: ignore[call-arg]
                    else:
                        return
                except Exception as exc:
                    logger.warning(
                        "ElasticsearchModule: mapping inspection failed for '%s': %s",
                        index_name, exc,
                    )
                    return

            await es.indices.create(
                index=index_name,
                body={"mappings": TENANT_FEATURE_MAPPING},
            )
            logger.info(
                "ElasticsearchModule: Created tenant feature index '%s'.", index_name
            )
        except Exception as exc:
            logger.warning("ElasticsearchModule: Could not create tenant feature index '%s': %s", index_name, exc)

    # ------------------------------------------------------------------
    # Startup: restore in-memory DENY policies
    # ------------------------------------------------------------------

    async def _restore_private_policies(self) -> None:
        """
        Scan all catalogs and re-register in-memory DENY policies for those
        with private=True. Called once at lifespan startup.

        The persisted policies are already loaded from DB by PermissionProtocol,
        but register_policy() is needed for the current process's in-memory fast path.
        The bulk reindex is NOT dispatched here — items are already indexed.
        """
        from dynastore.models.protocols import CatalogsProtocol
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.models.protocols.policies import PermissionProtocol, Policy, Role

        catalogs_proto = get_protocol(CatalogsProtocol)
        configs = get_protocol(ConfigsProtocol)
        perm = get_protocol(PermissionProtocol)
        if not catalogs_proto or not configs or not perm:
            return

        try:
            offset, batch = 0, 100
            while True:
                catalog_list = await catalogs_proto.list_catalogs(limit=batch, offset=offset)
                if not catalog_list:
                    break
                for catalog in catalog_list:
                    catalog_id = getattr(catalog, "id", None)
                    if not catalog_id:
                        continue
                    try:
                        cfg = await configs.get_config(ElasticsearchCatalogConfig, catalog_id=catalog_id)
                    except Exception as exc:
                        logger.debug(
                            "ElasticsearchModule: Skipping catalog '%s' — config lookup failed: %s",
                            catalog_id, exc,
                        )
                        continue
                    if cfg and getattr(cfg, "private", False):
                        policy_id = f"private_deny_{catalog_id}"
                        deny_policy = Policy(
                            id=policy_id,
                            description=f"Blocks public access to private catalog: {catalog_id}",
                            actions=["GET"],
                            resources=[
                                f"/(catalog|stac|features|tiles|wfs|maps)/catalogs/{re.escape(catalog_id)}(/.*)?",
                            ],
                            effect="DENY",
                        )
                        perm.register_policy(deny_policy)
                        perm.register_role(
                            Role(name="all_users", policies=[policy_id])
                        )
                        logger.info(
                            "ElasticsearchModule: Restored DENY policy for private catalog '%s'.",
                            catalog_id,
                        )
                if len(catalog_list) < batch:
                    break
                offset += batch
        except Exception as e:
            logger.warning(
                "ElasticsearchModule: Could not restore private policies at startup: %s", e
            )

    # ------------------------------------------------------------------
    # Async event handlers
    # ------------------------------------------------------------------

    async def _on_catalog_upsert(self, catalog_id: Optional[str] = None, payload=None, **kwargs):
        if not catalog_id:
            return
        doc = await _stac_serialize_catalog(catalog_id)
        if doc is None:
            doc = payload if isinstance(payload, dict) else {}

        from dynastore.tasks.elasticsearch.tasks import ElasticsearchIndexInputs
        await self._dispatch_task(
            task_type="elasticsearch_index",
            inputs=ElasticsearchIndexInputs(
                entity_type="catalog",
                entity_id=catalog_id,
                catalog_id=catalog_id,
                payload=doc,
            ).model_dump(),
        )

    async def _on_catalog_delete(self, catalog_id: Optional[str] = None, **kwargs):
        if not catalog_id:
            return
        # Remove any private DENY policy when the catalog is hard-deleted.
        await self._revoke_private_policy(catalog_id)

        from dynastore.tasks.elasticsearch.tasks import ElasticsearchDeleteInputs
        await self._dispatch_task(
            task_type="elasticsearch_delete",
            inputs=ElasticsearchDeleteInputs(
                entity_type="catalog",
                entity_id=catalog_id,
            ).model_dump(),
        )

    async def _on_collection_upsert(
        self, catalog_id: Optional[str] = None, collection_id: Optional[str] = None, payload=None, **kwargs,
    ):
        if not catalog_id or not collection_id:
            return

        doc = await _stac_serialize_collection(catalog_id, collection_id)
        if doc is None:
            doc = payload if isinstance(payload, dict) else {}

        entity_id = f"{catalog_id}:{collection_id}"
        from dynastore.tasks.elasticsearch.tasks import ElasticsearchIndexInputs
        await self._dispatch_task(
            task_type="elasticsearch_index",
            inputs=ElasticsearchIndexInputs(
                entity_type="collection",
                entity_id=entity_id,
                catalog_id=catalog_id,
                collection_id=collection_id,
                payload=doc,
            ).model_dump(),
        )

    async def _on_collection_delete(
        self, catalog_id: Optional[str] = None, collection_id: Optional[str] = None, **kwargs,
    ):
        if not catalog_id or not collection_id:
            return
        entity_id = f"{catalog_id}:{collection_id}"
        from dynastore.tasks.elasticsearch.tasks import ElasticsearchDeleteInputs
        await self._dispatch_task(
            task_type="elasticsearch_delete",
            inputs=ElasticsearchDeleteInputs(
                entity_type="collection",
                entity_id=entity_id,
            ).model_dump(),
        )

    async def _on_item_upsert(
        self, catalog_id: Optional[str] = None, collection_id: Optional[str] = None, item_id: Optional[str] = None,
        payload=None, **kwargs,
    ):
        if not catalog_id or not collection_id or not item_id:
            return

        # --- Private path: index only {geoid, catalog_id, collection_id} ---
        # Per-collection resolver consults the collection-tier override first
        # (ElasticsearchCollectionConfig.private) and falls back to the
        # catalog-tier flag (ElasticsearchCatalogConfig.private).
        from dynastore.modules.elasticsearch.es_collection_config import (
            is_collection_private,
        )
        if await is_collection_private(catalog_id, collection_id):
            from dynastore.modules.storage.drivers.elasticsearch_private.tasks import (
                PrivateIndexInputs,
            )
            await self._dispatch_task(
                task_type="elasticsearch_private_index",
                inputs=PrivateIndexInputs(
                    geoid=item_id,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                ).model_dump(),
            )
            return  # never populate the STAC items index

        # --- Normal catalog path ---
        if not await _is_es_active(catalog_id, collection_id):
            return

        doc = await _stac_serialize_item(catalog_id, collection_id, item_id)
        if doc is None:
            doc = payload if isinstance(payload, dict) else {}
            doc.update({"id": item_id, "catalog_id": catalog_id, "collection_id": collection_id})

        entity_id = f"{catalog_id}:{collection_id}:{item_id}"
        from dynastore.tasks.elasticsearch.tasks import ElasticsearchIndexInputs
        await self._dispatch_task(
            task_type="elasticsearch_index",
            inputs=ElasticsearchIndexInputs(
                entity_type="item",
                entity_id=entity_id,
                catalog_id=catalog_id,
                collection_id=collection_id,
                item_id=item_id,
                payload=doc,
            ).model_dump(mode="json"),
        )

    async def _on_item_bulk_upsert(
        self, catalog_id: Optional[str] = None, collection_id: Optional[str] = None, payload=None, **kwargs,
    ):
        if not catalog_id or not collection_id:
            return

        items_subset = (payload if isinstance(payload, dict) else {}).get("items_subset", [])

        # --- Private path ---
        # Single resolver call before the loop — every item in this bulk
        # event shares the same (catalog_id, collection_id) so the
        # private indexing decision is identical for the whole batch.
        from dynastore.modules.elasticsearch.es_collection_config import (
            is_collection_private,
        )
        if await is_collection_private(catalog_id, collection_id):
            from dynastore.modules.storage.drivers.elasticsearch_private.tasks import (
                PrivateIndexInputs,
            )
            for item_doc in items_subset:
                item_id = item_doc.get("id")
                if not item_id:
                    continue
                await self._dispatch_task(
                    task_type="elasticsearch_private_index",
                    inputs=PrivateIndexInputs(
                        geoid=item_id,
                        catalog_id=catalog_id,
                        collection_id=collection_id,
                    ).model_dump(),
                )
            return

        # --- Normal catalog path ---
        if not await _is_es_active(catalog_id, collection_id):
            return

        from dynastore.tasks.elasticsearch.tasks import ElasticsearchIndexInputs
        for item_doc in items_subset:
            item_id = item_doc.get("id")
            if not item_id:
                continue
            doc = {**item_doc, "catalog_id": catalog_id, "collection_id": collection_id}
            entity_id = f"{catalog_id}:{collection_id}:{item_id}"
            await self._dispatch_task(
                task_type="elasticsearch_index",
                inputs=ElasticsearchIndexInputs(
                    entity_type="item",
                    entity_id=entity_id,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    item_id=item_id,
                    payload=doc,
                ).model_dump(),
            )

    async def _on_item_delete(
        self, catalog_id: Optional[str] = None, collection_id: Optional[str] = None, item_id: Optional[str] = None,
        payload=None, **kwargs,
    ):
        if not item_id:
            item_id = (payload if isinstance(payload, dict) else {}).get("geoid")
        if not catalog_id or not collection_id or not item_id:
            return

        entity_id = f"{catalog_id}:{collection_id}:{item_id}"

        # Delete from the STAC items index (no-op if not indexed there).
        from dynastore.tasks.elasticsearch.tasks import ElasticsearchDeleteInputs
        await self._dispatch_task(
            task_type="elasticsearch_delete",
            inputs=ElasticsearchDeleteInputs(
                entity_type="item",
                entity_id=entity_id,
            ).model_dump(),
        )

        # Delete from the private geoid index (no-op if catalog is not private).
        from dynastore.modules.storage.drivers.elasticsearch_private.tasks import (
            PrivateDeleteInputs,
        )
        await self._dispatch_task(
            task_type="elasticsearch_private_delete",
            inputs=PrivateDeleteInputs(
                geoid=item_id,
                catalog_id=catalog_id,
            ).model_dump(),
        )

    # ------------------------------------------------------------------
    # IndexerProtocol facade — exposes indexing via protocol discovery
    # ------------------------------------------------------------------

    async def index_document(
        self,
        entity_type: Literal["catalog", "collection", "item", "asset"],
        entity_id: str,
        document: Dict[str, Any],
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        db_resource: Optional[Any] = None,
    ) -> None:
        from dynastore.tasks.elasticsearch.tasks import ElasticsearchIndexInputs
        await self._dispatch_task(
            task_type="elasticsearch_index",
            inputs=ElasticsearchIndexInputs(
                entity_type=entity_type,
                entity_id=entity_id,
                catalog_id=catalog_id or "",
                collection_id=collection_id,
                payload=document,
            ).model_dump(),
            db_resource=db_resource,
        )

    async def delete_document(
        self,
        entity_type: Literal["catalog", "collection", "item", "asset"],
        entity_id: str,
        catalog_id: Optional[str] = None,
        db_resource: Optional[Any] = None,
    ) -> None:
        from dynastore.tasks.elasticsearch.tasks import ElasticsearchDeleteInputs
        await self._dispatch_task(
            task_type="elasticsearch_delete",
            inputs=ElasticsearchDeleteInputs(
                entity_type=entity_type,
                entity_id=entity_id,
            ).model_dump(),
            db_resource=db_resource,
        )

    async def index_private(
        self,
        geoid: str,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[Any] = None,
    ) -> None:
        from dynastore.modules.storage.drivers.elasticsearch_private.tasks import (
                PrivateIndexInputs,
            )
        await self._dispatch_task(
            task_type="elasticsearch_private_index",
            inputs=PrivateIndexInputs(
                geoid=geoid,
                catalog_id=catalog_id,
                collection_id=collection_id,
            ).model_dump(),
            db_resource=db_resource,
        )

    async def delete_private(
        self,
        geoid: str,
        catalog_id: str,
        db_resource: Optional[Any] = None,
    ) -> None:
        from dynastore.modules.storage.drivers.elasticsearch_private.tasks import (
            PrivateDeleteInputs,
        )
        await self._dispatch_task(
            task_type="elasticsearch_private_delete",
            inputs=PrivateDeleteInputs(
                geoid=geoid,
                catalog_id=catalog_id,
            ).model_dump(),
            db_resource=db_resource,
        )

    async def bulk_reindex(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        if collection_id:
            from dynastore.tasks.elasticsearch_indexer.tasks import BulkCollectionReindexInputs
            await self._dispatch_task(
                task_type="elasticsearch_bulk_reindex_collection",
                inputs=BulkCollectionReindexInputs(
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                ).model_dump(),
                db_resource=db_resource,
            )
        else:
            from dynastore.tasks.elasticsearch_indexer.tasks import BulkCatalogReindexInputs
            await self._dispatch_task(
                task_type="elasticsearch_bulk_reindex_catalog",
                inputs=BulkCatalogReindexInputs(
                    catalog_id=catalog_id,
                ).model_dump(),
                db_resource=db_resource,
            )
        return {"catalog_id": catalog_id, "collection_id": collection_id, "status": "dispatched"}

    async def ensure_index(
        self,
        entity_type: Literal["catalog", "collection", "item", "asset", "private"],
        catalog_id: Optional[str] = None,
    ) -> None:
        if entity_type == "private":
            if not catalog_id:
                raise ValueError("catalog_id is required for private index.")
            await self._ensure_private_index(catalog_id)
        else:
            # Standard indices are created on-demand by the index tasks.
            pass
