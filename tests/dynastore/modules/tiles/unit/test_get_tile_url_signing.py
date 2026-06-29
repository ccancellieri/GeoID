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

"""Unit tests for TileBucketPreseedStorage.get_tile_url signing behaviour.

Validates that:
- get_tile_url generates a V4 signed URL via IAM signBlob (mock identity provider).
- When blob.exists() raises (Forbidden), a WARNING is logged and None is returned.
- When blob.exists() returns False, None is returned with no WARNING.
- A None/invalid service-account email from the identity provider raises ValueError
  (surfaced as WARNING in _try_cached_tile, keeping the proxy fallback).
- generate_gcs_signed_url guards service_account_email for None/non-email values.
"""
from __future__ import annotations

import logging
from typing import Optional, Type
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.models.plugin_config import PluginConfig
from dynastore.modules.tiles.tiles_config import TilesCachingConfig


# ---------------------------------------------------------------------------
# Shared stub: PlatformConfigsProtocol
# ---------------------------------------------------------------------------


class _StubPlatformConfigsProtocol:
    is_platform_manager = True

    def __init__(self, cfg: Optional[TilesCachingConfig]) -> None:
        self._cfg = cfg

    async def get_config(self, config_cls: Type[PluginConfig], ctx=None) -> PluginConfig:
        if self._cfg is None or config_cls is not TilesCachingConfig:
            return TilesCachingConfig()
        return self._cfg

    async def set_config(self, *a, **kw) -> None: ...
    async def list_configs(self): return {}


def _install_config(monkeypatch, cfg: TilesCachingConfig):
    stub = _StubPlatformConfigsProtocol(cfg)
    from dynastore.models.protocols import platform_configs as pc_mod
    from dynastore.tools import discovery

    def fake_get_protocol(proto, *a, **kw):
        if proto is pc_mod.PlatformConfigsProtocol:
            return stub
        return None

    monkeypatch.setattr(discovery, "get_protocol", fake_get_protocol)


# ---------------------------------------------------------------------------
# Happy path: signed URL via IAM signBlob
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_tile_url_returns_signed_url_via_iam_signblob(monkeypatch):
    """get_tile_url returns a signed URL when blob exists and identity provides
    a valid SA email + fresh token (the IAM signBlob path on Cloud Run)."""
    from dynastore.modules.gcp.tiles_storage import TileBucketPreseedStorage

    cfg = TilesCachingConfig(cache_bucket_override="shared-bucket", cache_bucket_prefix="tiles")
    _install_config(monkeypatch, cfg)

    signed_url = "https://storage.googleapis.com/shared-bucket/tiles/cat/coll/WMQ/5/17/11.mvt?X-Goog-Signature=abc"

    # Blob mock: exists() → True, generate_signed_url → the signed URL
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

    storage_provider_mock = MagicMock()
    storage_provider_mock.get_storage_identifier = AsyncMock(
        side_effect=AssertionError("should not be called with override set")
    )

    storage = TileBucketPreseedStorage()
    storage._get_storage_provider = MagicMock(return_value=storage_provider_mock)
    storage._get_client_provider = MagicMock(return_value=client_provider_mock)
    storage._get_identity_provider = MagicMock(return_value=identity_mock)

    with patch("dynastore.modules.gcp.tiles_storage.run_in_thread", new=AsyncMock(side_effect=lambda f, *a, **kw: f(*a, **kw))):
        result = await storage.get_tile_url("cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt")

    assert result == signed_url
    # Confirm signed_url was called with IAM fields
    call_kwargs = blob_mock.generate_signed_url.call_args[1]
    assert call_kwargs["service_account_email"] == "sa@project.iam.gserviceaccount.com"
    assert call_kwargs["access_token"] == "ya29.fresh-token"
    assert call_kwargs["version"] == "v4"
    assert call_kwargs["method"] == "GET"


# ---------------------------------------------------------------------------
# blob.exists() raises → WARNING logged, None returned
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_tile_url_warns_when_blob_exists_raises(monkeypatch, caplog):
    """If blob.exists() raises (e.g. 403 Forbidden on metadata API), a WARNING is
    logged and get_tile_url returns None so the proxy path can serve the tile."""
    from google.api_core.exceptions import Forbidden
    from dynastore.modules.gcp.tiles_storage import TileBucketPreseedStorage

    cfg = TilesCachingConfig(cache_bucket_override="shared-bucket", cache_bucket_prefix="tiles")
    _install_config(monkeypatch, cfg)

    blob_mock = MagicMock()
    blob_mock.exists = MagicMock(side_effect=Forbidden("403 SA lacks storage.objects.get"))

    bucket_mock = MagicMock()
    bucket_mock.blob = MagicMock(return_value=blob_mock)

    storage_client_mock = MagicMock()
    storage_client_mock.bucket = MagicMock(return_value=bucket_mock)

    client_provider_mock = MagicMock()
    client_provider_mock.get_storage_client = MagicMock(return_value=storage_client_mock)

    identity_mock = MagicMock()
    storage_provider_mock = MagicMock()
    storage_provider_mock.get_storage_identifier = AsyncMock(return_value=None)

    storage = TileBucketPreseedStorage()
    storage._get_storage_provider = MagicMock(return_value=storage_provider_mock)
    storage._get_client_provider = MagicMock(return_value=client_provider_mock)
    storage._get_identity_provider = MagicMock(return_value=identity_mock)

    with patch(
        "dynastore.modules.gcp.tiles_storage.run_in_thread",
        new=AsyncMock(side_effect=lambda f, *a, **kw: f(*a, **kw)),
    ):
        with caplog.at_level(logging.WARNING, logger="dynastore.modules.gcp.tiles_storage"):
            result = await storage.get_tile_url("cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt")

    assert result is None, "Should return None so proxy can handle the tile"
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert warnings, "A WARNING must be logged when blob.exists() raises"
    assert any("existence probe" in w or "Forbidden" in w or "storage.objects.get" in w for w in warnings), (
        f"WARNING should explain the failure; got: {warnings}"
    )


# ---------------------------------------------------------------------------
# blob.exists() returns False → None, no WARNING
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_tile_url_returns_none_silently_on_blob_miss(monkeypatch, caplog):
    """Normal cache-miss: blob.exists() returns False → None, no WARNING."""
    from dynastore.modules.gcp.tiles_storage import TileBucketPreseedStorage

    cfg = TilesCachingConfig(cache_bucket_override="shared-bucket", cache_bucket_prefix="tiles")
    _install_config(monkeypatch, cfg)

    blob_mock = MagicMock()
    blob_mock.exists = MagicMock(return_value=False)

    bucket_mock = MagicMock()
    bucket_mock.blob = MagicMock(return_value=blob_mock)

    storage_client_mock = MagicMock()
    storage_client_mock.bucket = MagicMock(return_value=bucket_mock)

    client_provider_mock = MagicMock()
    client_provider_mock.get_storage_client = MagicMock(return_value=storage_client_mock)

    identity_mock = MagicMock()
    storage_provider_mock = MagicMock()
    storage_provider_mock.get_storage_identifier = AsyncMock(return_value=None)

    storage = TileBucketPreseedStorage()
    storage._get_storage_provider = MagicMock(return_value=storage_provider_mock)
    storage._get_client_provider = MagicMock(return_value=client_provider_mock)
    storage._get_identity_provider = MagicMock(return_value=identity_mock)

    with patch(
        "dynastore.modules.gcp.tiles_storage.run_in_thread",
        new=AsyncMock(side_effect=lambda f, *a, **kw: f(*a, **kw)),
    ):
        with caplog.at_level(logging.WARNING, logger="dynastore.modules.gcp.tiles_storage"):
            result = await storage.get_tile_url("cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt")

    assert result is None
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert not warnings, f"No WARNING expected on cache miss; got: {warnings}"


# ---------------------------------------------------------------------------
# Invalid SA email → ValueError from generate_gcs_signed_url
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_gcs_signed_url_raises_on_none_email():
    """If identity_provider.get_account_email() returns None, generate_gcs_signed_url
    raises ValueError so _try_cached_tile can log it as WARNING and fall back to proxy."""
    from datetime import timedelta
    from unittest.mock import AsyncMock, MagicMock

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
