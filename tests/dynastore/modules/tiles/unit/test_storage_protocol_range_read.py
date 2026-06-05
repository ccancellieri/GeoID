"""Unit tests for StorageProtocol.download_bytes_range default fallback."""

import pytest

from dynastore.models.protocols.storage import StorageProtocol


class _FakeStorage(StorageProtocol):
    """Minimal in-memory storage that implements only download_file."""

    def __init__(self, content: bytes):
        self._content = content

    async def get_storage_identifier(self, catalog_id):
        return None

    async def get_catalog_storage_path(self, catalog_id):
        return None

    async def upload_file(self, source_path, target_path, content_type=None):
        return target_path

    async def upload_file_content(self, target_path, content, content_type=None):
        return target_path

    async def download_file(self, source_path: str, target_path: str) -> None:
        with open(target_path, "wb") as f:
            f.write(self._content)

    async def file_exists(self, path: str) -> bool:
        return True

    async def delete_file(self, path: str) -> None:
        pass

    async def ensure_storage_for_catalog(self, catalog_id, conn=None):
        return None

    async def drop_storage(self, catalog_id, conn=None) -> bool:
        return True


@pytest.mark.asyncio
async def test_download_bytes_range_default_slices_correctly():
    """Default implementation downloads full file and returns the requested slice."""
    payload = b"0123456789abcdef"
    storage = _FakeStorage(payload)

    result = await storage.download_bytes_range("gs://bucket/object", offset=4, length=6)

    assert result == payload[4:10], f"Expected {payload[4:10]!r}, got {result!r}"


@pytest.mark.asyncio
async def test_download_bytes_range_full_file():
    """Offset=0, length=len returns the entire content."""
    payload = b"hello world"
    storage = _FakeStorage(payload)

    result = await storage.download_bytes_range("gs://bucket/object", offset=0, length=len(payload))

    assert result == payload


@pytest.mark.asyncio
async def test_download_bytes_range_end_of_file():
    """Offset near end of file works correctly."""
    payload = b"abcdefghij"
    storage = _FakeStorage(payload)

    result = await storage.download_bytes_range("gs://bucket/object", offset=8, length=2)

    assert result == b"ij"
