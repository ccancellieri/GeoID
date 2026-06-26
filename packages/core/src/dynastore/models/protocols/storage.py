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
Storage-related protocol definitions.
"""

from typing import Protocol, Optional, Any, runtime_checkable

@runtime_checkable
class StorageProtocol(Protocol):
    """
    Protocol for storage operations (e.g., GCS, S3), enabling decoupled
    access to buckets and files.
    """
    
    async def get_storage_identifier(self, catalog_id: str) -> Optional[str]:
        """Returns the storage identifier (e.g., bucket name) associated with a catalog."""
        ...

    async def ensure_storage_for_catalog(self, catalog_id: str, conn: Optional[Any] = None, raise_on_failure: bool = False) -> Optional[str]:
        """Ensures that storage (e.g., a bucket) exists for a catalog, creating it if it doesn't."""
        ...

    async def get_catalog_storage_path(self, catalog_id: str) -> Optional[str]:
        """Returns the storage path (e.g., gs://...) for a catalog."""
        ...

    async def drop_storage(
        self,
        catalog_id: str,
        conn: Optional[Any] = None,
        physical_schema: Optional[str] = None,
        bucket_name: Optional[str] = None,
    ) -> bool:
        """Remove all binary storage resources (e.g. bucket, prefix) associated with a catalog.

        This is the uniform teardown entry point for binary storage, parallel to the
        ``drop_storage`` contract on the four metadata-tier entity-store protocols
        (AssetStore, CollectionItemsStore, CollectionStore, CatalogStore).

        Returns True when cleanup completed (idempotent — a missing bucket/resource
        is a no-op success, not a failure).

        ``physical_schema`` / ``bucket_name`` select the recreation-safe teardown
        path (#2298): an explicit old-resource target captured at delete time so a
        catalog rapidly recreated under a new schema is never collaterally deleted.
        Implementations that have no such race may ignore both.
        """
        ...

    async def get_collection_storage_path(self, catalog_id: str, collection_id: str) -> Optional[str]:
        """Returns the storage path (e.g., gs://...) for a collection."""
        ...

    async def wait_for_storage_ready(self, storage_id: str, timeout_seconds: int = 30, interval_seconds: float = 1.0) -> bool:
        """Actively waits until the given storage reports as existing."""
        ...

    async def prepare_upload_target(self, catalog_id: str, collection_id: Optional[str] = None) -> None:
        """Ensures that the target catalog and collection exist (JIT creation) before an upload."""
        ...

    async def upload_file(self, source_path: str, target_path: str, content_type: Optional[str] = None) -> str:
        """Uploads a local file to storage."""
        ...

    async def download_file(self, source_path: str, target_path: str) -> None:
        """Downloads a file from storage to local."""
        ...

    async def file_exists(self, path: str) -> bool:
        """Checks if a file exists in storage."""
        ...

    async def delete_file(self, path: str) -> None:
        """Deletes a file from storage."""
        ...
    async def upload_file_content(self, target_path: str, content: bytes, content_type: Optional[str] = None) -> str:
        """Uploads content (bytes) directly to storage."""
        ...

    async def apply_storage_config(self, catalog_id: str, config: Any) -> None:
        """Applies storage-related configuration changes to the live resource (e.g., CORS, Lifecycle)."""
        ...

    async def download_bytes_range(self, path: str, offset: int, length: int) -> bytes:
        """Download a byte range from storage.

        Default: download full file to a temp file and slice — correct but inefficient.
        Providers should override this with an efficient range-read (GCS, S3).
        """
        import tempfile
        with tempfile.NamedTemporaryFile(delete=True) as tmp:
            await self.download_file(path, tmp.name)
            with open(tmp.name, "rb") as f:
                f.seek(offset)
                return f.read(length)
