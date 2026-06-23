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

#    dynastore/modules/gcp/gcp_events.py
from dynastore.modules.concurrency import run_in_thread
import logging
from typing import (
    Optional,
    Dict,
    Any,
    List,
    Callable,
    Coroutine,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from dynastore.modules.gcp.gcp_finalize_activator import ActivationOutcome
from collections import defaultdict
import fnmatch
import asyncio
from dynastore.tools.protocol_helpers import get_engine
from dynastore.modules import get_protocol
from dynastore.modules.gcp.tools import bucket as bucket_tool
from dynastore.modules.db_config.query_executor import (
    managed_transaction,
    DbResource,
)
from dynastore.modules.gcp.gcp_config import (
    GcpEventingConfig,
    GcpCatalogBucketConfig,
    GcpCollectionBucketConfig,
    GcsNotificationEventType,
)
from dynastore.modules.catalog.event_service import CatalogEventType
from dynastore.modules.tasks.events.models import (
    API_KEY_NAME,
    AuthConfigAPIKey,
    AuthMethod,
    EventSubscriptionCreate,
)
from fastapi.security import APIKeyHeader
from dynastore.modules.processes.models import ExecuteRequest
import dynastore.modules.processes.processes_module as processes_module
try:
    from google.api_core import exceptions as google_exceptions
except ImportError:
    google_exceptions = None

logger = logging.getLogger(__name__)

api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)


# --- Helpers ---

from dynastore.models.protocols import (
    StorageProtocol,
    EventingProtocol,
    AssetsProtocol,
    CatalogsProtocol,
    ConfigsProtocol,
)
from dynastore.models.protocols.cloud_storage_client import CloudStorageClientProtocol


def _get_providers():
    """
    Helper to safely retrieve the required protocol implementations.
    """
    from dynastore.modules import get_protocol

    storage = get_protocol(StorageProtocol)
    eventing = get_protocol(EventingProtocol)
    assets = get_protocol(AssetsProtocol)
    configs = get_protocol(ConfigsProtocol)

    if not all([storage, eventing, assets, configs]):
        logger.warning(
            "One or more required protocols are not available. Skipping GCP event processing."
        )
        return None, None, None, None

    return storage, eventing, assets, configs


def _interpolate(template: Any, context: Dict[str, Any]) -> Any:
    """Recursively interpolates placeholders in strings within a nested data structure."""
    if isinstance(template, dict):
        return {k: _interpolate(v, context) for k, v in template.items()}
    if isinstance(template, list):
        return [_interpolate(i, context) for i in template]
    if isinstance(template, str):
        return template.format(**context)
    return template


# --- GCP Pub/Sub Event Dispatcher ---

_gcp_event_listeners: Dict[
    str, List[Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]]
] = defaultdict(list)


def register_gcp_event_listener(subscription_id_pattern: str, listener: Callable):
    """
    Registers a listener function to be called for Pub/Sub messages that have a
    matching 'subscription_id' attribute.

    Idempotent: re-registering the same (pattern, listener) pair is a no-op.
    Required because BucketService.lifespan can run multiple times per process
    (extension re-init, gunicorn preload + worker fork), which would otherwise
    cause each GCS notification to fan out into N duplicate task enqueues.
    """
    bucket = _gcp_event_listeners[subscription_id_pattern]
    if listener in bucket:
        logger.debug(
            f"GCP event listener already registered for pattern '{subscription_id_pattern}'; skipping."
        )
        return
    bucket.append(listener)
    logger.info(
        f"Registered GCP event listener for pattern '{subscription_id_pattern}'."
    )


class CatalogSchemaUnavailable(Exception):
    """Raised when a Pub/Sub event arrives for a catalog whose physical
    schema is not yet visible (or has been hard-deleted).

    The push handler maps this to a 5xx so Pub/Sub redelivers — the
    most common cause is a race between catalog provisioning and the
    first OBJECT_FINALIZE on a freshly bound bucket. Genuinely
    unrecoverable cases (catalog deleted for good) flush via the
    push subscription's dead-letter policy.
    """

    def __init__(self, catalog_id: str, collection_id: Optional[str] = None):
        self.catalog_id = catalog_id
        self.collection_id = collection_id
        super().__init__(
            f"No physical schema for catalog '{catalog_id}'"
            + (f" (collection '{collection_id}')" if collection_id else "")
        )


async def dispatch_gcp_event(payload: Dict[str, Any]):
    """
    Dispatches a received Pub/Sub event payload to all registered listeners
    whose subscription ID pattern matches.
    """
    subscription_id = payload.get("subscription_id")
    if not subscription_id:
        return

    for pattern, listeners in _gcp_event_listeners.items():
        if fnmatch.fnmatch(subscription_id, pattern):
            await asyncio.gather(*(listener(payload) for listener in listeners))


# --- Event Handlers ---
# These functions contain the core logic for processing events.


async def on_catalog_hard_deletion(engine: DbResource, payload: Dict[str, Any]):
    """Handler to cleanup GCP resources when a catalog is hard-deleted."""
    catalog_id = payload.get("catalog_id")
    if not catalog_id:
        raise ValueError("Missing 'catalog_id' in event payload")

    storage, eventing, _, configs = _get_providers()
    if storage is None or eventing is None or configs is None:
        return

    logger.info(
        f"Event 'catalog_hard_deletion' received for '{catalog_id}'. Starting GCP resource cleanup."
    )

    # 1. Cleanup Eventing (Topics/Subscriptions)
    # config=None triggers force cleanup of deterministic/default resources in the protocol implementation.
    try:
        eventing_config = await configs.get_config(
            GcpEventingConfig, catalog_id
        )
        await eventing.teardown_catalog_eventing(catalog_id, config=eventing_config)
    except Exception as e:
        logger.info(
            f"Failed to cleanup configured eventing (likely already gone or missing). Attempting force force cleanup. Details: {e}"
        )
        await eventing.teardown_catalog_eventing(catalog_id, config=None)

    # 2. Delete bucket
    bucket_name_to_delete = await storage.get_storage_identifier(catalog_id)
    if bucket_name_to_delete:
        logger.info(f"Proceeding with deletion of bucket '{bucket_name_to_delete}'.")
        try:
            await storage.delete_file(
                bucket_name_to_delete
            )  # Note: StorageProtocol delete_file typically deletes the object, but if it supports bucket deletion it should be used.
            # In GCS, delete_file is implemented as delete_blob. We actually need delete_bucket.
            # I'll check if StorageProtocol has delete_bucket.
            await bucket_tool.delete_bucket(
                bucket_name_to_delete, force=True, client=None
            )  # client=None will use default client if possible, but better to use protocol
        except Exception as e:
            logger.warning(f"Failed to delete bucket '{bucket_name_to_delete}': {e}")


async def on_collection_hard_deletion(engine: DbResource, payload: Dict[str, Any]):
    """Handler to cleanup GCS resources when a collection is hard-deleted."""
    catalog_id = payload.get("catalog_id")
    collection_id = payload.get("collection_id")
    if not catalog_id or not collection_id:
        raise ValueError("Missing 'catalog_id' or 'collection_id' in event payload")

    # Retrieve providers using the helper
    storage, eventing, _, configs = _get_providers()
    if storage is None or configs is None or eventing is None:
        return

    logger.info(
        f"Event 'collection_hard_deletion' received for '{catalog_id}:{collection_id}'. Checking for associated GCS bucket folder."
    )

    # Check configuration: listen_catalog_events
    bucket_config = await configs.get_config(GcpCatalogBucketConfig, catalog_id)
    if (
        isinstance(bucket_config, GcpCatalogBucketConfig)
        and not bucket_config.listen_catalog_events
    ):
        logger.info(
            f"Skipping GCP resource deletion for collection '{catalog_id}:{collection_id}' because 'listen_catalog_events' is False."
        )
        return

    # Resolve the immutable physical_id for the collection.  The collection
    # row may already be deleted (hard-deletion handler), so allow_missing=True
    # returns None and we fall back to the logical id for the path lookup.
    # After this change all new collections will have their objects stored under
    # the physical_id prefix; pre-existing ones still use the logical id prefix.
    _catalogs_evt = get_protocol(CatalogsProtocol)
    _collection_physical_id: Optional[str] = None
    if _catalogs_evt:
        try:
            _collection_physical_id = await _catalogs_evt.resolve_physical_id(
                catalog_id, collection_id, allow_missing=True
            )
        except Exception:
            pass
    collection_physical_id = _collection_physical_id or collection_id

    # 1. Delete objects
    bucket_name = await storage.get_storage_identifier(catalog_id)
    if bucket_name:
        # We'll use the storage.delete_file for each blob if we want protocol-adherence
        # but for bulk deletion, bucket_tool is fine if it uses storage_client.
        # However, StorageProtocol should ideally have a delete_prefix method.
        # For now, let's stick to bucket_tool as it's an extension-level utility that can know about GCS.
        storage_client_provider = get_protocol(CloudStorageClientProtocol)
        storage_client = (
            storage_client_provider.get_storage_client()
            if storage_client_provider
            else None
        )
        bucket = storage_client.bucket(bucket_name) if storage_client else None
        folder_prefix = bucket_tool.get_blob_path_for_collection_folder(collection_physical_id)

        logger.info(
            f"Deleting all objects in bucket '{bucket_name}' with prefix '{folder_prefix}'."
        )

        # list_blobs is partial lazy but iterator makes requests. delete_blobs makes requests.
        # Ideally we should push this entire block to thread.
        if bucket is None:
            deleted_count = 0
        else:
            bucket_ref = bucket

            def _delete_helper():
                blobs_to_delete = list(bucket_ref.list_blobs(prefix=folder_prefix))
                if blobs_to_delete:
                    bucket_ref.delete_blobs(blobs_to_delete)
                return len(blobs_to_delete)

            deleted_count = await run_in_thread(_delete_helper)

        if deleted_count:
            logger.info(
                f"Successfully deleted {deleted_count} objects for collection '{collection_id}'."
            )
        else:
            logger.info(
                f"No objects found to delete for collection '{collection_id}' in bucket '{bucket_name}'."
            )

    # 2. Check managed eventing prefix
    eventing_config = await configs.get_config(
        GcpEventingConfig, catalog_id
    )
    if isinstance(eventing_config, GcpEventingConfig):
        if (
            eventing_config.managed_eventing
            and eventing_config.managed_eventing.enabled
        ):
            folder_prefix = bucket_tool.get_blob_path_for_collection_folder(
                collection_physical_id
            )
            if eventing_config.managed_eventing.blob_name_prefix == folder_prefix:
                logger.info(
                    f"Managed eventing was tracking the deleted collection prefix '{folder_prefix}'. Tearing down the channel."
                )
                await eventing.teardown_managed_eventing_channel(
                    catalog_id, eventing_config.managed_eventing
                )


async def handle_gcs_notification(payload: Dict[str, Any]):
    """
    Universal handler for GCS notifications from Pub/Sub. It determines the
    target catalog/collection and triggers any configured actions.
    """
    logger.debug(f"Received GCS notification payload: {payload}")

    gcs_payload = payload.get("gcs_event_payload", {})
    attributes = payload.get("attributes", {})

    subscription_type = attributes.get("subscription_type")
    catalog_id = attributes.get("catalog_id")
    # Fix: Try fetching 'eventType' from 'attributes' first (standard GCS notification location)
    # then fallback to gcs_payload if not present.
    gcs_event_type_str = attributes.get("eventType") or gcs_payload.get("eventType")

    if not all([subscription_type, catalog_id, gcs_event_type_str]):
        logger.warning(
            f"Received GCS event, but missing required attributes or GCS eventType. "
            f"subscription_type={subscription_type}, catalog_id={catalog_id}, eventType={gcs_event_type_str}. Skipping."
        )
        return

    try:
        gcs_event_type = GcsNotificationEventType(gcs_event_type_str)
    except ValueError:
        logger.warning(
            f"Received unknown GCS eventType '{gcs_event_type_str}'. Skipping."
        )
        return

    collection_id = None
    if subscription_type == "managed":
        # For managed subscriptions, we determine the context (catalog vs collection) from the path.
        object_name = gcs_payload.get("name")
        if not object_name:
            logger.warning("GCS event missing object name. Skipping.")
            return

        path_parts = object_name.split("/")
        # Expected path for collection assets:
        #   collections/<collection_physical_id>/<filename>
        # The folder segment is the collection's IMMUTABLE physical id (the
        # blob path is built from it so it survives a logical rename), NOT the
        # logical collection id. Downstream finalize matching keys on it.
        if len(path_parts) >= 2 and path_parts[0] == bucket_tool.COLLECTIONS_FOLDER:
            collection_id = path_parts[1]
        # Expected path for catalog assets: catalog/<filename>
        elif len(path_parts) >= 1 and path_parts[0] == bucket_tool.CATALOG_FOLDER:
            collection_id = None  # Explicitly a catalog-level asset
        else:
            # Not in a managed prefix (e.g., tiles/) - though the notification should have filtered this.
            logger.debug(
                f"Object '{object_name}' is not in a recognized managed folder. Skipping."
            )
            return

        # Trigger asset event handling for managed buckets
        await handle_asset_events(
            catalog_id, collection_id, gcs_payload, event_type_str=gcs_event_type_str
        )
    elif subscription_type == "custom":
        custom_sub_id = attributes.get("custom_subscription_id")
        if not custom_sub_id:
            logger.warning(
                "Received event from custom subscription without 'custom_subscription_id' attribute. Cannot link to actions."
            )
            return

        _, eventing_provider, _, _ = _get_providers()
        if not eventing_provider:
            return
        eventing_config = await eventing_provider.get_eventing_config(catalog_id)
        if eventing_config:
            found_sub = next(
                (
                    s
                    for s in eventing_config.custom_subscriptions
                    if s.id == custom_sub_id
                ),
                None,
            )
            if found_sub:
                collection_id = found_sub.target_collection_id
            else:
                logger.warning(
                    f"Could not find custom subscription config with id '{custom_sub_id}' in catalog '{catalog_id}'."
                )
                return

    await _trigger_configured_actions(
        catalog_id, collection_id, gcs_event_type, gcs_payload
    )


async def _trigger_configured_actions(
    catalog_id: str,
    collection_id: Optional[str],
    event_type: GcsNotificationEventType,
    gcs_payload: Dict[str, Any],
):
    """Fetches collection config and executes actions based on the event type."""
    _, eventing_provider, _, configs = _get_providers()
    if eventing_provider is None or configs is None:
        return
    # Fetch config for the specific collection. The config manager will fall back
    # to the catalog level if no collection-specific config is set.
    # This allows actions to be defined at either the catalog or collection level.
    config = await configs.get_config(
        GcpCollectionBucketConfig, catalog_id, collection_id
    )

    if not isinstance(config, GcpCollectionBucketConfig) or not config.event_actions:
        logger.debug(
            f"No event actions configured for {catalog_id}:{collection_id}. Skipping."
        )
        return

    actions_to_run = config.event_actions.get(event_type)
    if not actions_to_run:
        return

    # Look up templates if we encounter strings
    eventing_config = None
    if any(isinstance(a, str) for a in actions_to_run):
        eventing_config = await configs.get_config(GcpEventingConfig, catalog_id)

    metadata = gcs_payload.get("metadata", None)
    if metadata:
        asset_id = metadata.get("asset_id") or metadata.get("asset_code")
        if not asset_id:
            logger.warning(f"GCS event missing asset_id in metadata. Skipping.")
            return
    else:
        logger.warning(f"GCS event missing metadata. Skipping.")
        return

    # GCS object generation: monotonic per-version id, present on every
    # OBJECT_FINALIZE. Used as the idempotency token for the ingestion task.
    generation = gcs_payload.get("generation")

    context = {
        "catalog_id": catalog_id,
        "collection_id": collection_id,
        "bucket": gcs_payload.get("bucket"),
        "bucket_id": gcs_payload.get("bucket"),  # Alias for template compatibility
        "name": gcs_payload.get("name"),
        "object_id": gcs_payload.get("name"),  # Alias for template compatibility
        "event_type": event_type.value,
        "asset_id": asset_id,
        "asset_code": asset_id,  # maintain for template compatibility
    }

    for action_item in actions_to_run:
        # Resolve action
        action = None
        if isinstance(action_item, str):
            # 1. Try collection-specific asset tasks
            action = config.asset_tasks.get(action_item)

            # 2. Fallback to global templates if not found in collection
            if (
                not action
                and eventing_config
                and isinstance(eventing_config, GcpEventingConfig)
            ):
                action = eventing_config.action_templates.get(action_item)

            if not action:
                logger.warning(
                    f"Action template/task '{action_item}' referenced in {catalog_id}:{collection_id} but not found in collection asset_tasks or catalog eventing config. Skipping."
                )
                continue
        else:
            action = action_item

        logger.info(
            f"Triggering process '{action.process_id}' for {catalog_id}:{collection_id} based on event '{event_type.value}'."
        )
        try:
            # --- Special Handling for 'ingestion' process ---
            # This creates a complete, valid TaskIngestionRequest on the fly.
            if action.process_id == "ingestion":
                if not asset_id:
                    logger.warning(
                        f"Cannot trigger ingestion for object '{context['name']}' because it's missing the 'asset_id' metadata. Skipping."
                    )
                    continue
                # The template now defines the *ingestion_request* part of the OGC process inputs.
                ingestion_request_template = action.execute_request_template
                ingestion_request_body = _interpolate(
                    ingestion_request_template, context
                )

                # Construct the full OGC ExecuteRequest payload
                ogc_inputs = {
                    "catalog_id": catalog_id,
                    "collection_id": collection_id,
                    "ingestion_request": ingestion_request_body,
                }
                execute_payload = ExecuteRequest(inputs=ogc_inputs)
            else:
                # Generic process execution
                interpolated_inputs = _interpolate(
                    action.execute_request_template, context
                )
                execute_payload = ExecuteRequest(inputs=interpolated_inputs)

            engine = get_engine()
            if engine is None:
                raise RuntimeError("No DatabaseProtocol engine available to execute process.")

            # Ingestion: derive a dedup_key keyed on the GCS object generation
            # (monotonic per-version). Pub/Sub OBJECT_FINALIZE is at-least-once;
            # without this token a redelivered finalize would spawn a second
            # job against the same asset version. The runner forwards the key
            # to TaskCreate; the DB partial unique index on
            # (schema_name, dedup_key) for non-terminal tasks collapses the
            # redelivery into a single task.
            dedup_key: Optional[str] = None
            if action.process_id == "ingestion" and generation is not None:
                dedup_key = (
                    f"ingestion:{catalog_id}:{collection_id}:"
                    f"{asset_id}:{generation}"
                )

            result = await processes_module.execute_process(
                process_id=action.process_id,
                execution_request=execute_payload,
                engine=engine,
                caller_id=f"gcp_event:{event_type.value}",
                catalog_id=catalog_id,
                collection_id=collection_id,
                dedup_key=dedup_key,
            )
            if dedup_key is not None and result is None:
                logger.info(
                    f"Dedup: process '{action.process_id}' for "
                    f"{catalog_id}:{collection_id}:{asset_id}@{generation} "
                    f"already in flight; skipping redelivery."
                )
            else:
                logger.info(
                    f"Successfully deferred process '{action.process_id}' for object '{context['name']}'."
                )
        except Exception as e:
            logger.error(
                f"Failed to construct and trigger process '{action.process_id}' for object '{context['name']}': {e}",
                exc_info=True,
            )


# --- In-Process Listener Adapters ---
# --- Reactive Hooks for storage events ASSETS ---


async def handle_asset_events(
    catalog_id: str,
    collection_id: Optional[str],
    event_payload: Dict[str, Any],
    event_type_str: Optional[str] = None,
) -> Optional["ActivationOutcome"]:
    """Process a GCS object lifecycle notification inline.

    OBJECT_FINALIZE goes through :func:`gcp_finalize_activator.activate`
    in a single transaction (no task hop): the matching PENDING ``assets``
    row is UPDATEd to ACTIVE. Pub/Sub at-least-once redelivery is the
    retry mechanism — concurrent deliveries serialise on ``FOR UPDATE``.

    The function returns:

    * an :class:`ActivationOutcome` for OBJECT_FINALIZE when the inline
      path executed (success or idempotent re-delivery). The HTTP push
      handler maps this to a 200 ack.
    * ``None`` for non-FINALIZE events, missing/unsupported events, or
      missing protocol providers (DatabaseProtocol / CatalogsProtocol).
      These paths are intentionally non-fatal — operators see them via
      logs and they will not benefit from redelivery.

    Exceptions propagate (they are NOT swallowed here): the HTTP push
    route maps :class:`OrphanFinalizeEvent` to a 204 ack,
    :class:`CatalogSchemaUnavailable` to 503 (Pub/Sub redelivers — the
    schema may appear shortly because of a provisioning race), and any
    other exception to 5xx so Pub/Sub redelivers.
    """
    event_type = event_type_str or event_payload.get("eventType")
    # OBJECT_DELETE / OBJECT_ARCHIVE handling: deferred. The legacy
    # GcsStorageEventTask covered both via AssetsProtocol.delete_assets
    # but Stage 4.2 is scoped to OBJECT_FINALIZE; Stage 6 reconciliation
    # owns the deletion-cleanup surface. Until then we only act on
    # OBJECT_FINALIZE — other events are ignored after a debug log.
    if event_type != "OBJECT_FINALIZE":
        logger.debug(
            f"handle_asset_events: ignoring event_type '{event_type}' "
            f"(only OBJECT_FINALIZE is handled inline in Stage 4.2)."
        )
        return None

    from dynastore.models.driver_context import DriverContext
    from dynastore.models.protocols import DatabaseProtocol
    from dynastore.modules.gcp.gcp_finalize_activator import (
        activate,
        finalize_event_from_pubsub,
    )

    db = get_protocol(DatabaseProtocol)
    if not db:
        logger.warning("handle_asset_events: DatabaseProtocol not available.")
        return None

    catalogs_svc = get_protocol(CatalogsProtocol)
    if catalogs_svc is None:
        logger.warning("handle_asset_events: CatalogsProtocol not available.")
        return None
    # Bypass the per-process catalog-model cache: on multi-instance Cloud Run
    # the Pub/Sub push can land on an instance whose cache predates this
    # catalog's creation. ctx forces a direct DB read.
    schema = await catalogs_svc.resolve_physical_schema(
        catalog_id, ctx=DriverContext(db_resource=db.engine), allow_missing=True
    )
    if not schema:
        # Most common cause: the catalog is provisioning and the first
        # OBJECT_FINALIZE on its bucket landed before the schema is
        # visible to this Cloud Run instance. Surface as an exception so
        # the push handler returns 5xx and Pub/Sub redelivers; genuinely
        # missing catalogs eventually fall through to the dead-letter
        # policy.
        logger.warning(
            f"handle_asset_events: no physical schema for catalog "
            f"'{catalog_id}' — requesting Pub/Sub redelivery."
        )
        raise CatalogSchemaUnavailable(catalog_id, collection_id)

    # ``collection_id`` here is the object-prefix tail
    # (``collections/<collection_physical_id>/...``), i.e. the collection's
    # IMMUTABLE physical id — never the mutable logical id. The activator
    # matches the PENDING ``assets`` row on ``collection_physical_id``.
    finalize_event = finalize_event_from_pubsub(
        event_payload,
        catalog_id=catalog_id,
        collection_physical_id=collection_id,
    )

    async with managed_transaction(db.engine) as conn:
        outcome = await activate(conn, schema, finalize_event)

    logger.info(
        f"GCS finalize processed for {catalog_id}:{collection_id or ''} "
        f"asset_id={outcome.asset_id} action={outcome.action} "
        f"uri={finalize_event.uri}"
    )
    return outcome


# --- Registration Functions ---


def register_listeners():
    """
    Subscribes the GCP module to internal catalog events.
    This enables 'in-process' synchronization without needing an external webhook.

    Hard-deletion cleanup (BEFORE_CATALOG_HARD_DELETION,
    BEFORE_COLLECTION_HARD_DELETION) is handled by GcsCatalogPrefixOwner /
    GcsCollectionPrefixOwner registered in the cascade cleanup registry, so
    those event listeners are intentionally absent here.
    """
    logger.info("Registering GCP module as a listener for Catalog events.")
    logger.info("GCP module successfully subscribed to Catalog events.")


def register_default_gcp_listeners():
    """Registers the built-in GCP event handlers."""
    logger.info("Registering default GCP event listeners...")
    # Register a single, universal listener for all our subscriptions
    register_gcp_event_listener("*", handle_gcs_notification)


# --- Webhook Dispatcher (Legacy/Alternative) ---

EVENT_HANDLERS = {
    CatalogEventType.CATALOG_HARD_DELETION.value: on_catalog_hard_deletion,
    CatalogEventType.COLLECTION_HARD_DELETION.value: on_collection_hard_deletion,
}


async def dispatch_event(engine: DbResource, event_payload: Dict[str, Any]):
    event_type = event_payload.get("event_type")
    payload = event_payload.get("payload")

    if not event_type or payload is None:
        raise ValueError("Invalid event structure")

    handler = EVENT_HANDLERS.get(event_type)
    if handler:
        await handler(engine, payload)


async def register_self_as_subscriber(self_public_url):
    """Registers webhooks with the central event bus (if used)."""
    if not self_public_url:
        return

    from pydantic import HttpUrl

    webhook_url = HttpUrl(f"{self_public_url.rstrip('/')}/gcp/events/webhook")
    auth_config = AuthConfigAPIKey(
        auth_method=AuthMethod.API_KEY, header_name=API_KEY_NAME
    )

    subscriptions = [
        EventSubscriptionCreate(
            subscriber_name="gcp_module_catalog_cleanup",
            event_type=CatalogEventType.CATALOG_HARD_DELETION.value,
            webhook_url=webhook_url,
            auth_config=auth_config,
        ),
        EventSubscriptionCreate(
            subscriber_name="gcp_module_collection_cleanup",
            event_type=CatalogEventType.COLLECTION_HARD_DELETION.value,
            webhook_url=webhook_url,
            auth_config=auth_config,
        ),
    ]
    await _register_self_as_subscriber_direct(subscriptions)


async def _register_self_as_subscriber_direct(
    subscriptions: list[EventSubscriptionCreate],
):
    from dynastore.modules.tasks.event_driver import subscribe

    for sub in subscriptions:
        try:
            await subscribe(subscription_data=sub)
        except Exception:
            pass
