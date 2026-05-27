"""Unit tests for :func:`upload_stream_to_gcs` — resumable-upload path.

Bug B fix (#1416): ``upload_from_file`` required a seekable stream; replaced
with ``blob.open("wb")`` (``BlobWriter`` / resumable upload) so generators
are consumed without seek/tell.
"""

from __future__ import annotations

from typing import Iterator, List
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _iter_chunks(chunks: List[bytes]) -> Iterator[bytes]:
    yield from chunks


class _FakeWriter:
    """Minimal context-manager writer that records write() calls."""

    def __init__(self) -> None:
        self.written: List[bytes] = []
        self.exited = False

    def __enter__(self) -> "_FakeWriter":
        return self

    def __exit__(self, *args: object) -> bool:
        self.exited = True
        return False

    def write(self, data: bytes) -> None:
        self.written.append(data)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_chunks_written_to_blob_writer() -> None:
    """All non-empty chunks from the generator are forwarded to the writer."""
    chunks = [b"hello", b" ", b"world"]
    fake_writer = _FakeWriter()
    fake_blob = MagicMock()
    fake_blob.open.return_value = fake_writer
    fake_bucket = MagicMock()
    fake_bucket.blob.return_value = fake_blob
    fake_client = MagicMock()
    fake_client.bucket.return_value = fake_bucket

    from dynastore.modules.gcp.tools.bucket import upload_stream_to_gcs

    upload_stream_to_gcs(
        byte_stream=_iter_chunks(chunks),
        destination_uri="gs://my-bucket/path/to/file.geojson",
        content_type="application/geo+json",
        client=fake_client,
    )

    fake_blob.open.assert_called_once_with("wb", content_type="application/geo+json")
    assert fake_writer.written == chunks
    # Context manager must have been exited (writer closed)
    assert fake_writer.exited


def test_empty_chunks_are_skipped() -> None:
    """Empty byte strings are filtered out before write."""
    chunks = [b"data", b"", b"more"]
    fake_writer = _FakeWriter()
    fake_blob = MagicMock()
    fake_blob.open.return_value = fake_writer
    fake_bucket = MagicMock()
    fake_bucket.blob.return_value = fake_blob
    fake_client = MagicMock()
    fake_client.bucket.return_value = fake_bucket

    from dynastore.modules.gcp.tools.bucket import upload_stream_to_gcs

    upload_stream_to_gcs(
        byte_stream=_iter_chunks(chunks),
        destination_uri="gs://bucket/file",
        client=fake_client,
    )

    # Only 2 non-empty chunks should have been written
    assert fake_writer.written == [b"data", b"more"]


def test_raises_on_invalid_uri() -> None:
    """A non-gs:// URI is rejected before touching GCS."""
    from dynastore.modules.gcp.tools.bucket import upload_stream_to_gcs

    with pytest.raises(ValueError, match="Invalid GCS URI"):
        upload_stream_to_gcs(
            byte_stream=_iter_chunks([b"x"]),
            destination_uri="s3://wrong-bucket/file",
        )


def test_exception_is_logged_and_reraised() -> None:
    """Writer.write failures are logged and re-raised to the caller."""

    class _FailWriter:
        def __enter__(self) -> "_FailWriter":
            return self

        def __exit__(self, *args: object) -> bool:
            return False

        def write(self, data: bytes) -> None:
            raise RuntimeError("network error")

    fake_blob = MagicMock()
    fake_blob.open.return_value = _FailWriter()
    fake_bucket = MagicMock()
    fake_bucket.blob.return_value = fake_blob
    fake_client = MagicMock()
    fake_client.bucket.return_value = fake_bucket

    from dynastore.modules.gcp.tools.bucket import upload_stream_to_gcs

    with pytest.raises(RuntimeError, match="network error"):
        upload_stream_to_gcs(
            byte_stream=_iter_chunks([b"data"]),
            destination_uri="gs://bucket/file",
            client=fake_client,
        )
