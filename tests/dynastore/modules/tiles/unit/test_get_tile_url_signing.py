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

"""Unit tests for `GcsTileUrlSigner.sign` (the tile-cache 307-redirect
signing provider — moved out of the deleted `TileBucketPreseedStorage`).

Validates that:
- sign() generates a V4 signed URL via IAM signBlob (mock identity provider).
- When blob.exists() raises (Forbidden), a WARNING is logged and None is returned.
- When blob.exists() returns False, None is returned with no WARNING.
- A None/invalid service-account email from the identity provider raises ValueError
  (surfaced as WARNING by the caller, keeping the proxy fallback).
- generate_gcs_signed_url guards service_account_email for None/non-email values.
"""
from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Happy path: signed URL via IAM signBlob
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sign_returns_signed_url_via_iam_signblob(monkeypatch):
    """sign() returns a signed URL when blob exists and identity provides
    a valid SA email + fresh token (the IAM signBlob path on Cloud Run)."""
    from dynastore.modules.gcp.tiles_storage import GcsTileUrlSigner

    signed_url = "https://storage.googleapis.com/shared-bucket/tiles/cat/coll/WMQ/5/17/11.mvt?X-Goog-Signature=abc"

    blob_mock = MagicMock()
    blob_mock.exists = MagicMock(return_value=True)
    blob_mock.generate_signed_url = MagicMock(return_value=signed_url)

    bucket_mock = MagicMock()
    bucket_mock.blob = MagicMock(return_value=blob_mock)

    storage_client_mock = MagicMock()
    storage_client_mock.bucket = MagicMock(return_value=bucket_mock)

    client_provider_mock = MagicMock()
    client_provider_mock.get_storage_client = MagicMock(return_value=storage_client_mock)

    identity_mock = MagicMock()
    identity_mock.get_account_email = MagicMock(
        return_value="sa@project.iam.gserviceaccount.com"
    )
    identity_mock.get_fresh_token = AsyncMock(return_value="ya29.fresh-token")

    signer = GcsTileUrlSigner()
    signer._get_client_provider = MagicMock(return_value=client_provider_mock)
    signer._get_identity_provider = MagicMock(return_value=identity_mock)

    with patch(
        "dynastore.modules.gcp.tiles_storage.run_in_thread",
        new=AsyncMock(side_effect=lambda f, *a, **kw: f(*a, **kw)),
    ):
        result = await signer.sign("gs://shared-bucket/tiles/cat/coll/WMQ/5/17/11.mvt")

    assert result == signed_url
    call_kwargs = blob_mock.generate_signed_url.call_args[1]
    assert call_kwargs["service_account_email"] == "sa@project.iam.gserviceaccount.com"
    assert call_kwargs["access_token"] == "ya29.fresh-token"
    assert call_kwargs["version"] == "v4"
    assert call_kwargs["method"] == "GET"


# ---------------------------------------------------------------------------
# blob.exists() raises -> WARNING logged, None returned
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sign_warns_when_blob_exists_raises(caplog):
    """If blob.exists() raises (e.g. 403 Forbidden on metadata API), a WARNING is
    logged and sign() returns None so the proxy path can serve the tile."""
    from google.api_core.exceptions import Forbidden
    from dynastore.modules.gcp.tiles_storage import GcsTileUrlSigner

    blob_mock = MagicMock()
    blob_mock.exists = MagicMock(side_effect=Forbidden("403 SA lacks storage.objects.get"))

    bucket_mock = MagicMock()
    bucket_mock.blob = MagicMock(return_value=blob_mock)

    storage_client_mock = MagicMock()
    storage_client_mock.bucket = MagicMock(return_value=bucket_mock)

    client_provider_mock = MagicMock()
    client_provider_mock.get_storage_client = MagicMock(return_value=storage_client_mock)

    signer = GcsTileUrlSigner()
    signer._get_client_provider = MagicMock(return_value=client_provider_mock)
    signer._get_identity_provider = MagicMock(return_value=MagicMock())

    with patch(
        "dynastore.modules.gcp.tiles_storage.run_in_thread",
        new=AsyncMock(side_effect=lambda f, *a, **kw: f(*a, **kw)),
    ):
        with caplog.at_level(logging.WARNING, logger="dynastore.modules.gcp.tiles_storage"):
            result = await signer.sign("gs://shared-bucket/tiles/cat/coll/WMQ/5/17/11.mvt")

    assert result is None, "Should return None so proxy can handle the tile"
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert warnings, "A WARNING must be logged when blob.exists() raises"
    assert any("existence probe" in w or "Forbidden" in w or "storage.objects.get" in w for w in warnings), (
        f"WARNING should explain the failure; got: {warnings}"
    )


# ---------------------------------------------------------------------------
# blob.exists() returns False -> None, no WARNING
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sign_returns_none_silently_on_blob_miss(caplog):
    """Normal cache-miss: blob.exists() returns False -> None, no WARNING."""
    from dynastore.modules.gcp.tiles_storage import GcsTileUrlSigner

    blob_mock = MagicMock()
    blob_mock.exists = MagicMock(return_value=False)

    bucket_mock = MagicMock()
    bucket_mock.blob = MagicMock(return_value=blob_mock)

    storage_client_mock = MagicMock()
    storage_client_mock.bucket = MagicMock(return_value=bucket_mock)

    client_provider_mock = MagicMock()
    client_provider_mock.get_storage_client = MagicMock(return_value=storage_client_mock)

    signer = GcsTileUrlSigner()
    signer._get_client_provider = MagicMock(return_value=client_provider_mock)
    signer._get_identity_provider = MagicMock(return_value=MagicMock())

    with patch(
        "dynastore.modules.gcp.tiles_storage.run_in_thread",
        new=AsyncMock(side_effect=lambda f, *a, **kw: f(*a, **kw)),
    ):
        with caplog.at_level(logging.WARNING, logger="dynastore.modules.gcp.tiles_storage"):
            result = await signer.sign("gs://shared-bucket/tiles/cat/coll/WMQ/5/17/11.mvt")

    assert result is None
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert not warnings, f"No WARNING expected on cache miss; got: {warnings}"


@pytest.mark.asyncio
async def test_sign_returns_none_for_non_gs_uri():
    """sign() is scoped to gs:// — an unexpected scheme is a clean None, not a crash."""
    from dynastore.modules.gcp.tiles_storage import GcsTileUrlSigner

    signer = GcsTileUrlSigner()
    assert await signer.sign("file:///tmp/tile.mvt") is None


# ---------------------------------------------------------------------------
# Invalid SA email -> ValueError from generate_gcs_signed_url
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_gcs_signed_url_raises_on_none_email():
    """If identity_provider.get_account_email() returns None, generate_gcs_signed_url
    raises ValueError so callers can log it as WARNING and fall back to proxy."""
    from datetime import timedelta

    from dynastore.modules.gcp.tools.signed_urls import generate_gcs_signed_url

    blob_mock = MagicMock()
    bucket_mock = MagicMock()
    bucket_mock.blob = MagicMock(return_value=blob_mock)
    storage_client_mock = MagicMock()
    storage_client_mock.bucket = MagicMock(return_value=bucket_mock)

    client_provider_mock = MagicMock()
    client_provider_mock.get_storage_client = MagicMock(return_value=storage_client_mock)

    identity_mock = MagicMock()
    identity_mock.get_account_email = MagicMock(return_value=None)
    identity_mock.get_fresh_token = AsyncMock(return_value="ya29.token")

    with pytest.raises(ValueError, match="IAM signing requires a valid service-account email"):
        with patch(
            "dynastore.modules.gcp.tools.signed_urls.run_in_thread",
            new=AsyncMock(side_effect=lambda f, *a, **kw: f(*a, **kw) if callable(f) else f),
        ):
            await generate_gcs_signed_url(
                "gs://bucket/path/to/blob",
                method="GET",
                expiration=timedelta(minutes=60),
                client_provider=client_provider_mock,
                identity_provider=identity_mock,
                check_exists=False,
            )


@pytest.mark.asyncio
async def test_generate_gcs_signed_url_raises_on_default_email():
    """'default' is not a valid SA email — raises ValueError (Compute Engine
    credentials populate service_account_email as 'default' before metadata
    fetch; if the metadata server was unreachable at startup this can persist)."""
    from datetime import timedelta

    from dynastore.modules.gcp.tools.signed_urls import generate_gcs_signed_url

    blob_mock = MagicMock()
    bucket_mock = MagicMock()
    bucket_mock.blob = MagicMock(return_value=blob_mock)
    storage_client_mock = MagicMock()
    storage_client_mock.bucket = MagicMock(return_value=bucket_mock)

    client_provider_mock = MagicMock()
    client_provider_mock.get_storage_client = MagicMock(return_value=storage_client_mock)

    identity_mock = MagicMock()
    identity_mock.get_account_email = MagicMock(return_value="default")
    identity_mock.get_fresh_token = AsyncMock(return_value="ya29.token")

    with pytest.raises(ValueError, match="IAM signing requires a valid service-account email"):
        with patch(
            "dynastore.modules.gcp.tools.signed_urls.run_in_thread",
            new=AsyncMock(side_effect=lambda f, *a, **kw: f(*a, **kw) if callable(f) else f),
        ):
            await generate_gcs_signed_url(
                "gs://bucket/path/to/blob",
                method="GET",
                expiration=timedelta(minutes=60),
                client_provider=client_provider_mock,
                identity_provider=identity_mock,
                check_exists=False,
            )
