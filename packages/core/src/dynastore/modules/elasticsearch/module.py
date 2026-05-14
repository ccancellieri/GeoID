import fnmatch
import logging
import re
from typing import Any, Dict, List, Literal, Optional, Tuple
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

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _find_overbroad_dynastore_data_stream_templates(
    es: Any, prefix: str,
) -> List[Tuple[str, List[str]]]:
    """Return composable templates that would force dynastore metadata indices into data streams.

    PR #172 fail-fast catches the SYMPTOM (a metadata index already exists as a
    stream). This helper catches the CAUSE: an over-broad composable template
    with ``data_stream != null`` whose ``index_patterns`` would intercept the
    next ``indices.create('{prefix}-collections')`` / ``-catalogs`` call and
    auto-create it as a stream — which then breaks every subsequent upsert.

    The intended logs template scope is ``{prefix}-logs-*``. Anything broader
    (e.g. ``{prefix}-*``, ``*``) is over-broad and is reported here.

    Returns a list of ``(template_name, patterns)`` for every offending template.
    On API errors returns an empty list (the existing fail-fast in lifespan
    will fall back to the symptom check).

    Observed on review env 2026-05-01: a manually-created template named
    ``dynastore_logs`` had ``index_patterns=['dynastore-*']`` and
    ``data_stream={...}``, which converted ``dynastore-collections`` into a
    stream and produced the "only write ops with op_type=create are allowed
    in data streams" error on every catalog/collection write.
    """
    canonical_metadata_indices = (
        f"{prefix}-collections",
        f"{prefix}-catalogs",
    )
    try:
        result = await es.indices.get_index_template(name="*")
    except Exception as exc:
        logger.debug(
            "ElasticsearchModule: get_index_template probe failed (%s) — "
            "skipping over-broad-template fail-fast (symptom check still active).",
            exc,
        )
        return []

    templates = (result or {}).get("index_templates", []) if isinstance(result, dict) else []
    offending: List[Tuple[str, List[str]]] = []
    for entry in templates:
        if not isinstance(entry, dict):
            continue
        body = entry.get("index_template") or {}
        if not isinstance(body, dict):
            continue
        # Only composable templates that emit data streams are dangerous —
        # a regular template doesn't change the index mode on creation.
        # Note: ``data_stream`` may be an empty dict (`{}`) when the operator
        # accepted defaults — still a data-stream template, so we test for
        # key presence + non-None, NOT truthiness (`{}` is falsy in Python).
        if body.get("data_stream") is None:
            continue
        patterns = body.get("index_patterns") or []
        if isinstance(patterns, str):
            patterns = [patterns]
        # Pattern is over-broad iff it matches a canonical metadata index name
        # via ES glob semantics (fnmatch covers the "*" / "?" wildcards ES
        # uses for index_patterns).
        for p in patterns:
            if not isinstance(p, str):
                continue
            if any(fnmatch.fnmatchcase(name, p) for name in canonical_metadata_indices):
                offending.append((entry.get("name") or "<unnamed>", list(patterns)))
                break
    return offending


async def _warn_if_mapping_drifted(
    es: Any, index: str, expected_mapping: Dict[str, Any],
) -> None:
    """Warn when ``index`` is missing top-level properties present in
    ``expected_mapping``.

    Catches the "field added in code, index never re-rolled" class of
    bug — a new property in the mapping is silently ignored by live
    indices that predate it. Top-level only:
    deep mapping equality is fragile (ES adds metadata) and field
    additions are by far the common case. Operator sees an actionable
    log line at startup; runtime writes that touch the missing field
    surface ``IndexMappingMismatchError`` → 503.
    """
    try:
        live = await es.indices.get_mapping(index=index)
    except Exception as exc:
        logger.debug(
            "ElasticsearchModule: drift check skipped for '%s' (get_mapping failed: %s)",
            index, exc,
        )
        return
    live_props = (
        (live or {}).get(index, {}).get("mappings", {}).get("properties", {})
        if isinstance(live, dict) else {}
    )
    expected_props = (expected_mapping or {}).get("properties", {}) or {}
    missing = sorted(set(expected_props) - set(live_props))
    if missing:
        logger.warning(
            "ElasticsearchModule: index '%s' mapping is missing fields %s "
            "declared in code. Writes touching those fields will fail with "
            "503 IndexMappingMismatch until the index is re-rolled.",
            index, missing,
        )


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




async def _is_es_active(catalog_id: str, collection_id: str) -> bool:
    """Return True when the collection has ES as write, secondary, or read driver."""
    try:
        from dynastore.models.protocols.configs import ConfigsProtocol

        configs = get_protocol(ConfigsProtocol)
        if not configs:
            return False
        from dynastore.modules.storage.routing_config import ItemsRoutingConfig
        routing = await configs.get_config(
            ItemsRoutingConfig,
            catalog_id=catalog_id,
            collection_id=collection_id,
        )
        if not isinstance(routing, ItemsRoutingConfig):
            return False
        for entries in routing.operations.values():
            for entry in entries:
                if entry.driver_ref == "items_elasticsearch_driver":
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

    Privacy is per-collection (Cycle E) — see
    ``CollectionPrivacy.is_private`` and the
    ``items_elasticsearch_private_driver`` (per-tenant geoid-only index
    + DENY policy management).  This module no longer carries
    catalog-wide private-mode toggles; the catalog-tier
    ``CatalogPrivacy.collection_defaults.is_private`` only seeds the
    default for newly-created collections.
    """

    priority: int = 50

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        events = get_protocol(EventBusProtocol)

        # ITEM_* propagation moved to the IndexDispatcher (Phase 2d of
        # the indexer-protocol harmonisation).  Catalog/collection-tier
        # listeners stay event-driven for now — a follow-up phase will
        # migrate those to the dispatcher too.
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
            ]:
                decorator = events.async_event_listener(etype)
                if decorator:
                    decorator(handler)
                    _registered.append((etype, handler))
                else:
                    logger.warning(
                        "ElasticsearchModule: Failed to register listener for %s", etype
                    )
            logger.info(
                "ElasticsearchModule: Registered catalog/collection listeners "
                "(item propagation now dispatched via IndexDispatcher).",
            )
        else:
            logger.warning(
                "ElasticsearchModule: EventsProtocol not found. "
                "Catalog/collection events not captured.",
            )

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

            # Over-broad-template fail-fast: a composable template with
            # data_stream=true and index_patterns matching `{prefix}-collections`
            # or `{prefix}-catalogs` would auto-convert those indices to data
            # streams on first create, breaking every subsequent metadata
            # upsert. PR #172 catches the SYMPTOM (existing stream); this
            # catches the CAUSE so a fresh-cluster deploy doesn't cycle back
            # into the same broken state after the operator deletes the
            # stream. Observed on review env 2026-05-01 — template
            # `dynastore_logs` with patterns=['dynastore-*'].
            prefix = es_client.get_index_prefix()
            offending_templates = await _find_overbroad_dynastore_data_stream_templates(
                es, prefix,
            )
            if offending_templates:
                lines = [
                    f"  - {name}: index_patterns={patterns}"
                    for name, patterns in offending_templates
                ]
                msg = (
                    "ElasticsearchModule: cluster has data-stream-emitting "
                    "composable template(s) whose index_patterns match the "
                    "platform's metadata indices ('{prefix}-collections' / "
                    "'{prefix}-catalogs'). Catalog/collection writes will fail "
                    "with 'only write ops with op_type=create are allowed in "
                    "data streams' as soon as those indices are auto-created.\n"
                    "Offending templates:\n"
                    + "\n".join(lines)
                    + "\n"
                    "Tighten each template's index_patterns to exclude the "
                    f"metadata indices (e.g. ['{prefix}-logs-*'] for the "
                    "logs backend), then redeploy."
                ).format(prefix=prefix)
                logger.error(msg)
                raise RuntimeError(msg)

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
                    else:
                        await _warn_if_mapping_drifted(es, shared_name, mapping)
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
        entity_type: Literal["catalog", "collection", "item", "asset"],
        catalog_id: Optional[str] = None,
    ) -> None:
        # Standard indices are created on-demand by the index tasks.
        # Per-tenant private indexes are managed by their own driver's
        # ``ensure_indexer`` (CollectionItemsStore lifecycle).
        return None
