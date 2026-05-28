"""``_resolve_source_content_type`` returns the asset row's content_type
first, falling back to a single GCS HEAD only for legacy bare-URI
assets whose metadata pre-dates the upload-time injection.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock, patch

from dynastore.modules.catalog.asset_service import Asset
from dynastore.tasks.ingestion.main_ingestion import _resolve_source_content_type


def _asset(uri: str, metadata: dict | None) -> Asset:
    """Tiny stand-in for ``Asset`` — only the two attributes the helper reads."""
    return cast(Asset, SimpleNamespace(uri=uri, metadata=metadata))


def test_metadata_content_type_is_primary():
    asset = _asset(
        "gs://bucket/key",
        {"content_type": "application/zip", "asset_id": "x"},
    )
    with patch("google.cloud.storage.Client") as gcs_client:
        assert _resolve_source_content_type(asset) == "application/zip"
        # Crucial: no GCS round-trip when asset row already carries it.
        gcs_client.assert_not_called()


def test_metadata_contentType_camelCase_also_recognised():
    asset = _asset("gs://b/k", {"contentType": "image/tiff"})
    assert _resolve_source_content_type(asset) == "image/tiff"


def test_falls_back_to_gcs_head_for_legacy_bare_uri():
    """Legacy assets uploaded before GcpStorageOpsMixin.initiate_upload persisted
    content_type into custom metadata: recover via GCS HEAD."""
    asset = _asset(
        "gs://d88971-test-catalog-19/collections/test_collection_19/aoi_oasis",
        {"asset_id": "aoi_oasis", "asset_type": "ASSET"},  # no content_type
    )
    fake_blob = MagicMock()
    fake_blob.content_type = "application/zip"
    fake_bucket = MagicMock()
    fake_bucket.get_blob.return_value = fake_blob
    fake_client = MagicMock()
    fake_client.bucket.return_value = fake_bucket
    with patch("google.cloud.storage.Client", return_value=fake_client):
        assert _resolve_source_content_type(asset) == "application/zip"
        fake_client.bucket.assert_called_once_with("d88971-test-catalog-19")
        fake_bucket.get_blob.assert_called_once_with(
            "collections/test_collection_19/aoi_oasis"
        )


def test_returns_none_when_blob_missing():
    asset = _asset("gs://nonexistent/key", {})
    fake_bucket = MagicMock()
    fake_bucket.get_blob.return_value = None
    fake_client = MagicMock()
    fake_client.bucket.return_value = fake_bucket
    with patch("google.cloud.storage.Client", return_value=fake_client):
        assert _resolve_source_content_type(asset) is None


def test_non_gcs_uri_does_not_attempt_head():
    asset = _asset("/data/local/file.shp", {})
    with patch("google.cloud.storage.Client") as gcs_client:
        assert _resolve_source_content_type(asset) is None
        gcs_client.assert_not_called()


def test_gcs_exception_swallowed_and_logged():
    """A bad GCS path / missing creds must not crash the ingestion task —
    helper returns None and the reader resolution falls back to URI suffix."""
    asset = _asset("gs://bad/key", {})
    with patch("google.cloud.storage.Client", side_effect=RuntimeError("boom")):
        assert _resolve_source_content_type(asset) is None


def test_empty_metadata_is_safe():
    asset = _asset("/local/file.csv", None)
    assert _resolve_source_content_type(asset) is None
