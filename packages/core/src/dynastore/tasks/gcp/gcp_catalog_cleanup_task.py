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
GcpCatalogCleanupTask — durable task for tearing down GCP resources when a
catalog or collection is hard-deleted.

Replaces the synchronous inline cleanup in gcp_events.py adapters with a
retryable, heartbeat-tracked background task.  Uses only StorageProtocol,
EventingProtocol and ConfigsProtocol via protocol discovery — no direct
reference to catalog internals.
"""

import logging
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict

# Hard runtime dep — see tasks/gcp_provision/task.py for rationale.
import google.cloud.storage  # noqa: F401

from dynastore.tasks.protocols import TaskProtocol
from dynastore.models.tasks import TaskPayload
from dynastore.modules import get_protocol
from dynastore.models.protocols import StorageProtocol, EventingProtocol, ConfigsProtocol

logger = logging.getLogger(__name__)


class CleanupScope(str, Enum):
    CATALOG = "catalog"
    COLLECTION = "collection"


class GcpCatalogCleanupInputs(BaseModel):
    model_config = ConfigDict(extra="ignore")

    scope: CleanupScope
    catalog_id: str
    collection_id: Optional[str] = None   # required when scope == COLLECTION
    bucket_name: Optional[str] = None     # pre-resolved before schema drop


class GcpCatalogCleanupTask(TaskProtocol):
    """
    Idempotent GCP resource teardown triggered by catalog/collection hard-deletion.

    Scope CATALOG:
      1. Tear down Pub/Sub topics and subscriptions (eventing).
      2. Delete the GCS bucket (force=True to remove objects first).

    Scope COLLECTION:
      1. Delete all objects under the collection prefix in the catalog bucket.
      2. Tear down any managed eventing channel scoped to the collection prefix.
    """

    task_type = "gcp_catalog_cleanup"
    priority: int = 90

    async def run(self, payload: TaskPayload[GcpCatalogCleanupInputs]) -> Dict[str, Any]:
        inputs = payload.inputs
        catalog_id = inputs.catalog_id

        if inputs.scope == CleanupScope.CATALOG:
            return await self._cleanup_catalog(catalog_id, bucket_name=inputs.bucket_name)
        else:
            if not inputs.collection_id:
                raise ValueError("collection_id is required for COLLECTION scope cleanup")
            return await self._cleanup_collection(
                catalog_id, inputs.collection_id, bucket_name=inputs.bucket_name
            )

    # ------------------------------------------------------------------
    # Catalog-level cleanup
    # ------------------------------------------------------------------

    async def _cleanup_catalog(self, catalog_id: str, bucket_name: Optional[str] = None) -> Dict[str, Any]:
        from dynastore.scripts.gc_phantom_catalogs import teardown_phantom_gcp_resources

        logger.info(
            "GcpCatalogCleanupTask[CATALOG]: starting GCP cleanup for catalog '%s'.",
            catalog_id,
        )

        result = await teardown_phantom_gcp_resources(catalog_id, bucket_name)
        result["scope"] = "catalog"

        if result.get("status") == "skipped_no_protocols":
            logger.warning(
                "GcpCatalogCleanupTask[CATALOG]: no GCP protocols registered for '%s'; "
                "GCP resources were not cleaned up.",
                catalog_id,
            )
        else:
            logger.info(
                "GcpCatalogCleanupTask[CATALOG]: cleanup complete for catalog '%s'.",
                catalog_id,
            )

        return result

    # ------------------------------------------------------------------
    # Collection-level cleanup
    # ------------------------------------------------------------------

    async def _cleanup_collection(
        self, catalog_id: str, collection_id: str, bucket_name: Optional[str] = None
    ) -> Dict[str, Any]:
        from dynastore.modules.gcp.gcp_config import (
            GcpCatalogBucketConfig,
            GcpEventingConfig,
        )
        from dynastore.modules.gcp.tools import bucket as bucket_tool
        from dynastore.modules.concurrency import run_in_thread

        logger.info(
            f"GcpCatalogCleanupTask[COLLECTION]: Starting GCP cleanup for "
            f"'{catalog_id}:{collection_id}'."
        )

        storage = get_protocol(StorageProtocol)
        eventing = get_protocol(EventingProtocol)
        configs = get_protocol(ConfigsProtocol)

        # Check configuration flag
        if configs:
            bucket_config = await configs.get_config(
                GcpCatalogBucketConfig, catalog_id
            )
            if (
                isinstance(bucket_config, GcpCatalogBucketConfig)
                and not bucket_config.listen_catalog_events
            ):
                logger.info(
                    f"GcpCatalogCleanupTask[COLLECTION]: Skipping cleanup for "
                    f"'{catalog_id}:{collection_id}' — listen_catalog_events is False."
                )
                return {
                    "catalog_id": catalog_id,
                    "collection_id": collection_id,
                    "scope": "collection",
                    "status": "skipped",
                }

        # 1. Delete objects under the collection prefix.
        # Prefer the pre-resolved bucket_name from task inputs (parent
        # catalog state may have changed by the time the task runs);
        # fall back to a live lookup if it wasn't pre-resolved.
        if storage:
            if not bucket_name:
                bucket_name = await storage.get_storage_identifier(catalog_id)
            if bucket_name:
                folder_prefix = bucket_tool.get_blob_path_for_collection_folder(
                    collection_id
                )
                logger.info(
                    f"GcpCatalogCleanupTask[COLLECTION]: Deleting objects with "
                    f"prefix '{folder_prefix}' from bucket '{bucket_name}'."
                )
                try:
                    from dynastore.modules.gcp.bucket_service import BucketService
                    bucket_service = get_protocol(BucketService)
                    if bucket_service:
                        storage_client = bucket_service.storage_client
                    else:
                        import google.cloud.storage as gcs
                        storage_client = gcs.Client()

                    def _delete_helper():
                        bucket_obj = storage_client.bucket(bucket_name)
                        blobs = list(bucket_obj.list_blobs(prefix=folder_prefix))
                        if blobs:
                            bucket_obj.delete_blobs(blobs)
                        return len(blobs)

                    deleted = await run_in_thread(_delete_helper)
                    logger.info(
                        f"GcpCatalogCleanupTask[COLLECTION]: Deleted {deleted} objects "
                        f"for collection '{collection_id}'."
                    )
                except Exception as e:
                    logger.warning(
                        f"GcpCatalogCleanupTask[COLLECTION]: Failed to delete objects "
                        f"for collection '{collection_id}': {e}"
                    )
                    raise

        # 2. Teardown managed eventing channel if it targets this collection prefix
        if eventing and configs:
            try:
                eventing_config = await configs.get_config(
                    GcpEventingConfig, catalog_id
                )
                if isinstance(eventing_config, GcpEventingConfig):
                    managed = eventing_config.managed_eventing
                    if managed and managed.enabled:
                        folder_prefix = bucket_tool.get_blob_path_for_collection_folder(
                            collection_id
                        )
                        if managed.blob_name_prefix == folder_prefix:
                            logger.info(
                                f"GcpCatalogCleanupTask[COLLECTION]: Tearing down "
                                f"managed eventing channel for prefix '{folder_prefix}'."
                            )
                            from dynastore.modules.gcp import gcp_module
                            teardown = getattr(gcp_module, "teardown_managed_eventing_channel", None)
                            if teardown is not None:
                                await teardown(catalog_id, managed)
            except Exception as e:
                logger.warning(
                    f"GcpCatalogCleanupTask[COLLECTION]: Eventing teardown for "
                    f"collection '{collection_id}' failed: {e}"
                )

        logger.info(
            f"GcpCatalogCleanupTask[COLLECTION]: Cleanup complete for "
            f"'{catalog_id}:{collection_id}'."
        )
        return {
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "scope": "collection",
            "status": "cleaned",
        }
