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

"""Unit tests for BucketAnnotationPatcher (Phase 2)."""

from unittest.mock import MagicMock, patch

import pytest

from dynastore.modules.gcp.asset_sync import (
    BucketAnnotationPatcher,
    _build_metadata,
    _parse_gs_uri,
)


class TestParseGsUri:
    def test_parses_valid_gs_uri(self):
        assert _parse_gs_uri("gs://my-bucket/path/to/file.tif") == (
            "my-bucket", "path/to/file.tif",
        )

    def test_returns_none_for_non_gs_scheme(self):
        assert _parse_gs_uri("file:///tmp/x.tif") is None
        assert _parse_gs_uri("https://example.com/x.tif") is None

    def test_returns_none_for_missing_blob(self):
        assert _parse_gs_uri("gs://only-bucket") is None
        assert _parse_gs_uri("gs://only-bucket/") is None

    def test_returns_none_for_empty_bucket(self):
        assert _parse_gs_uri("gs:///path") is None


class TestBuildMetadata:
    def test_builds_with_asset_id_and_type(self):
        result = _build_metadata(
            {"asset_type": "RASTER", "metadata": {"author": "alice"}},
            "asset-1",
        )
        assert result == {
            "author": "alice",
            "asset_id": "asset-1",
            "asset_type": "RASTER",
        }

    def test_drops_none_values(self):
        result = _build_metadata(
            {"metadata": {"a": "kept", "b": None, "c": "also-kept"}},
            "asset-1",
        )
        assert "b" not in result
        assert result["a"] == "kept"
        assert result["c"] == "also-kept"

    def test_coerces_non_string_values(self):
        result = _build_metadata(
            {"metadata": {"size": 12345, "ratio": 1.5}},
            "asset-1",
        )
        assert result["size"] == "12345"
        assert result["ratio"] == "1.5"

    def test_handles_enum_asset_type(self):
        class FakeEnum:
            value = "VECTORIAL"

        result = _build_metadata({"asset_type": FakeEnum()}, "asset-1")
        assert result["asset_type"] == "VECTORIAL"

    def test_handles_empty_metadata(self):
        result = _build_metadata({}, "asset-1")
        assert result == {"asset_id": "asset-1"}


class TestBucketAnnotationPatcherSkips:
    """Patcher must short-circuit (no GCS calls) on inapplicable payloads."""

    @pytest.mark.asyncio
    async def test_skips_when_owned_by_not_gcs(self):
        with patch(
            "dynastore.modules.gcp.asset_sync.get_protocol"
        ) as mock_get:
            await BucketAnnotationPatcher.on_asset_upsert(
                catalog_id="cat-1", asset_id="aid-1",
                payload={"owned_by": "local", "uri": "file:///tmp/x"},
            )
            mock_get.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_uri_not_gs(self):
        with patch(
            "dynastore.modules.gcp.asset_sync.get_protocol"
        ) as mock_get:
            await BucketAnnotationPatcher.on_asset_upsert(
                catalog_id="cat-1", asset_id="aid-1",
                payload={"owned_by": "gcs", "uri": "https://example.com/x"},
            )
            mock_get.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_payload_missing(self):
        with patch(
            "dynastore.modules.gcp.asset_sync.get_protocol"
        ) as mock_get:
            await BucketAnnotationPatcher.on_asset_upsert(
                catalog_id="cat-1", asset_id="aid-1", payload=None,
            )
            mock_get.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_catalog_id_missing(self):
        with patch(
            "dynastore.modules.gcp.asset_sync.get_protocol"
        ) as mock_get:
            await BucketAnnotationPatcher.on_asset_upsert(
                catalog_id=None, asset_id="aid-1",
                payload={"owned_by": "gcs", "uri": "gs://b/k"},
            )
            mock_get.assert_not_called()


class TestBucketAnnotationPatcherPatch:
    """Patcher must call blob.patch() on a real GCS-backed payload."""

    @pytest.mark.asyncio
    async def test_patches_when_metadata_drift(self):
        mock_blob = MagicMock()
        mock_blob.metadata = {"asset_id": "aid-1", "stale": "yes"}
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        mock_gcp = MagicMock()
        mock_gcp.get_storage_client.return_value = mock_client

        with patch(
            "dynastore.modules.gcp.asset_sync.get_protocol",
            return_value=mock_gcp,
        ):
            await BucketAnnotationPatcher.on_asset_upsert(
                catalog_id="cat-1", asset_id="aid-1",
                payload={
                    "owned_by": "gcs",
                    "uri": "gs://my-bucket/blob/path.tif",
                    "asset_type": "RASTER",
                    "metadata": {"author": "alice"},
                },
            )

        mock_client.bucket.assert_called_once_with("my-bucket")
        mock_bucket.blob.assert_called_once_with("blob/path.tif")
        mock_blob.reload.assert_called_once()
        mock_blob.patch.assert_called_once()
        # Metadata reflects payload
        assert mock_blob.metadata == {
            "author": "alice",
            "asset_id": "aid-1",
            "asset_type": "RASTER",
        }

    @pytest.mark.asyncio
    async def test_idempotent_when_metadata_matches(self):
        """No blob.patch() call when current matches desired."""
        target = {"author": "alice", "asset_id": "aid-1", "asset_type": "RASTER"}
        mock_blob = MagicMock()
        mock_blob.metadata = dict(target)  # already matches
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        mock_gcp = MagicMock()
        mock_gcp.get_storage_client.return_value = mock_client

        with patch(
            "dynastore.modules.gcp.asset_sync.get_protocol",
            return_value=mock_gcp,
        ):
            await BucketAnnotationPatcher.on_asset_upsert(
                catalog_id="cat-1", asset_id="aid-1",
                payload={
                    "owned_by": "gcs",
                    "uri": "gs://my-bucket/blob/path.tif",
                    "asset_type": "RASTER",
                    "metadata": {"author": "alice"},
                },
            )

        mock_blob.reload.assert_called_once()
        mock_blob.patch.assert_not_called()  # idempotent

    @pytest.mark.asyncio
    async def test_swallows_gcs_exceptions(self):
        """Any GCS API error must be logged and swallowed, never raised."""
        mock_blob = MagicMock()
        mock_blob.reload.side_effect = RuntimeError("GCS 503")
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        mock_gcp = MagicMock()
        mock_gcp.get_storage_client.return_value = mock_client

        with patch(
            "dynastore.modules.gcp.asset_sync.get_protocol",
            return_value=mock_gcp,
        ):
            # Must not raise
            await BucketAnnotationPatcher.on_asset_upsert(
                catalog_id="cat-1", asset_id="aid-1",
                payload={
                    "owned_by": "gcs",
                    "uri": "gs://my-bucket/blob/path.tif",
                },
            )

    @pytest.mark.asyncio
    async def test_skips_when_storage_client_uninitialized(self):
        mock_gcp = MagicMock()
        mock_gcp.get_storage_client.side_effect = RuntimeError("uninit")

        with patch(
            "dynastore.modules.gcp.asset_sync.get_protocol",
            return_value=mock_gcp,
        ):
            # Must not raise
            await BucketAnnotationPatcher.on_asset_upsert(
                catalog_id="cat-1", asset_id="aid-1",
                payload={
                    "owned_by": "gcs",
                    "uri": "gs://my-bucket/blob/path.tif",
                },
            )

    @pytest.mark.asyncio
    async def test_skips_when_gcp_module_unregistered(self):
        with patch(
            "dynastore.modules.gcp.asset_sync.get_protocol",
            return_value=None,
        ):
            # Must not raise
            await BucketAnnotationPatcher.on_asset_upsert(
                catalog_id="cat-1", asset_id="aid-1",
                payload={
                    "owned_by": "gcs",
                    "uri": "gs://my-bucket/blob/path.tif",
                },
            )
