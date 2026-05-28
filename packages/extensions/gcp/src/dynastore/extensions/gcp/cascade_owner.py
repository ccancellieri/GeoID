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

"""Cascade cleanup owners for GCP resources.

Replaces the ``BEFORE_CATALOG_HARD_DELETION`` / ``BEFORE_COLLECTION_HARD_DELETION``
event listeners in ``gcp_events.py`` with durable, idempotent owners.

:class:`GcsCatalogPrefixOwner` (CATALOG scope) handles full catalog teardown:
  - GCS bucket deletion (force=True)
  - Pub/Sub eventing teardown

:class:`GcsCollectionPrefixOwner` (COLLECTION scope) handles per-collection:
  - GCS object-prefix deletion under the collection folder
  - Managed eventing channel teardown if configured

``describe_scope`` pre-resolves the bucket_name while schema rows are still
live.  ``cleanup_one`` operates only on the snapshot data in ``ref.metadata``
so it works even after the schema is dropped.

HIGH STAKES: GCS blob deletion is irreversible.  Do not add retry on partial
deletion — the GCS SDK handles continuation internally.

Register via :func:`register_owners` from the GCP module lifespan BEFORE the
CascadeCleanupRegistry is frozen.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, ClassVar, Iterable, Optional

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


async def _resolve_bucket_name(catalog_id: str) -> Optional[str]:
    from dynastore.models.protocols import StorageProtocol
    from dynastore.modules import get_protocol

    storage = get_protocol(StorageProtocol)
    if storage is None:
        return None
    try:
        return await storage.get_storage_identifier(catalog_id)
    except Exception as exc:
        logger.debug("GcsCascadeOwner: could not resolve bucket for %r: %s", catalog_id, exc)
        return None


class GcsCatalogPrefixOwner(BaseResourceOwner):
    """Deletes the GCS bucket (and eventing resources) for a hard-deleted catalog."""

    owner_id: ClassVar[str] = "gcp.gcs.catalog_prefix"

    def supported_scopes(self) -> Iterable[ResourceScope]:
        return (ResourceScope.CATALOG,)

    async def describe_scope(
        self, scope_ref: ScopeRef, conn: Any
    ) -> list[CleanupRef]:
        if scope_ref.scope != ResourceScope.CATALOG:
            return []

        bucket_name = await _resolve_bucket_name(scope_ref.catalog_id)
        return [
            CleanupRef(
                kind="gcs_bucket",
                locator=f"gs://{bucket_name}" if bucket_name else f"catalog:{scope_ref.catalog_id}",
                owner_id=self.owner_id,
                metadata={
                    "catalog_id": scope_ref.catalog_id,
                    "bucket_name": bucket_name,
                },
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

        catalog_id = ref.metadata.get("catalog_id", "")
        bucket_name = ref.metadata.get("bucket_name")

        if dry_run:
            logger.info(
                "GcsCatalogPrefixOwner: dry-run — would delete bucket %r for catalog %r.",
                bucket_name, catalog_id,
            )
            return CleanupOutcome.DONE

        try:
            task_runner = _get_task_runner()
            await task_runner._cleanup_catalog(catalog_id, bucket_name=bucket_name)
            return CleanupOutcome.DONE
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "GcsCatalogPrefixOwner: catalog cleanup failed for %r: %s",
                catalog_id, exc, exc_info=True,
            )
            return CleanupOutcome.RETRY


class GcsCollectionPrefixOwner(BaseResourceOwner):
    """Deletes GCS objects under a collection prefix for a hard-deleted collection."""

    owner_id: ClassVar[str] = "gcp.gcs.collection_prefix"

    def supported_scopes(self) -> Iterable[ResourceScope]:
        return (ResourceScope.COLLECTION,)

    async def describe_scope(
        self, scope_ref: ScopeRef, conn: Any
    ) -> list[CleanupRef]:
        if scope_ref.scope != ResourceScope.COLLECTION or scope_ref.collection_id is None:
            return []

        bucket_name = await _resolve_bucket_name(scope_ref.catalog_id)
        from dynastore.modules.gcp.tools import bucket as bucket_tool
        folder_prefix = bucket_tool.get_blob_path_for_collection_folder(scope_ref.collection_id)

        return [
            CleanupRef(
                kind="gcs_prefix",
                locator=f"gs://{bucket_name}/{folder_prefix}" if bucket_name else f"collection:{scope_ref.catalog_id}:{scope_ref.collection_id}",
                owner_id=self.owner_id,
                metadata={
                    "catalog_id": scope_ref.catalog_id,
                    "collection_id": scope_ref.collection_id,
                    "bucket_name": bucket_name,
                    "folder_prefix": folder_prefix,
                },
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

        catalog_id = ref.metadata.get("catalog_id", "")
        collection_id = ref.metadata.get("collection_id", "")
        bucket_name = ref.metadata.get("bucket_name")

        if dry_run:
            logger.info(
                "GcsCollectionPrefixOwner: dry-run — would delete collection prefix "
                "for %r:%r in bucket %r.",
                catalog_id, collection_id, bucket_name,
            )
            return CleanupOutcome.DONE

        try:
            task_runner = _get_task_runner()
            await task_runner._cleanup_collection(
                catalog_id, collection_id, bucket_name=bucket_name
            )
            return CleanupOutcome.DONE
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "GcsCollectionPrefixOwner: collection cleanup failed for %r:%r: %s",
                catalog_id, collection_id, exc, exc_info=True,
            )
            return CleanupOutcome.RETRY


def _get_task_runner():
    """Return a GcpCatalogCleanupTask instance for calling its cleanup helpers."""
    from dynastore.tasks.gcp.gcp_catalog_cleanup_task import GcpCatalogCleanupTask
    return GcpCatalogCleanupTask()  # type: ignore[abstract]


def register_owners(registry: "CascadeCleanupRegistry") -> None:
    """Register GCP cascade owners into *registry*.

    Call from the GCP BucketService lifespan BEFORE
    :meth:`~dynastore.modules.catalog.cascade_registry.CascadeCleanupRegistry.freeze`.
    """
    registry.register(GcsCatalogPrefixOwner())
    registry.register(GcsCollectionPrefixOwner())
    logger.info(
        "GcpModule: registered cascade owners %r, %r.",
        GcsCatalogPrefixOwner.owner_id,
        GcsCollectionPrefixOwner.owner_id,
    )
