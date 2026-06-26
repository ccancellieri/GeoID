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

"""Regression tests for BucketService._apply_bucket_settings.

Regression: an earlier version referenced ``config.cors_rules`` (non-existent
field) instead of ``config.cors``.  The AttributeError was swallowed by
ensure_storage_for_catalog (raise_on_failure=False) and surfaced only as a
contextless RuntimeError("ensure_storage_for_catalog returned None"), making
catalog provisioning fail silently at the GCS bucket step.

These tests confirm that:
1. CORS rules from config.cors are applied to the GCS bucket object.
2. bucket.patch() is called when CORS rules are present.
3. No AttributeError is raised (the regression guard).
4. When config.cors is empty, bucket.patch() is not called.
"""
from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from dynastore.modules.gcp.bucket_service import BucketService
from dynastore.modules.gcp.gcp_config import GcpCatalogBucketConfig, GcpCorsRule


def _make_service() -> BucketService:
    storage_client = MagicMock()
    return BucketService(
        engine=MagicMock(name="engine"),
        config_service=MagicMock(name="config_service"),
        storage_client=storage_client,
        project_id="test-project",
        region="europe-west1",
    )


async def _sync_run_in_thread(fn: Any) -> Any:
    """Replacement for run_in_thread that executes the sync callable directly."""
    return fn()


@pytest.mark.asyncio
async def test_apply_bucket_settings_sets_cors_from_config_cors():
    """CORS rules from config.cors must be applied to the bucket — regression guard."""
    svc = _make_service()

    mock_bucket = MagicMock()
    svc.storage_client.bucket.return_value = mock_bucket

    config = GcpCatalogBucketConfig(
        cors=[
            GcpCorsRule(
                origin=["https://example.com"],
                method=["GET", "POST"],
                response_header=["Content-Type"],
                max_age_seconds=600,
            )
        ]
    )

    with patch(
        "dynastore.modules.gcp.bucket_service.run_in_thread",
        new=_sync_run_in_thread,
    ):
        # Must not raise AttributeError ("cors_rules" regression) or any other error.
        await svc._apply_bucket_settings("test-bucket", config)

    svc.storage_client.bucket.assert_called_once_with("test-bucket")

    expected_cors = [
        {
            "origin": ["https://example.com"],
            "method": ["GET", "POST"],
            "responseHeader": ["Content-Type"],
            "maxAgeSeconds": 600,
        }
    ]
    assert mock_bucket.cors == expected_cors, (
        f"Expected bucket.cors={expected_cors!r}, got {mock_bucket.cors!r}"
    )
    mock_bucket.patch.assert_called_once()


@pytest.mark.asyncio
async def test_apply_bucket_settings_default_config_applies_cors():
    """The default GcpCatalogBucketConfig (CORS=*) must patch the bucket."""
    svc = _make_service()

    mock_bucket = MagicMock()
    svc.storage_client.bucket.return_value = mock_bucket

    config = GcpCatalogBucketConfig()  # uses the wildcard default

    with patch(
        "dynastore.modules.gcp.bucket_service.run_in_thread",
        new=_sync_run_in_thread,
    ):
        await svc._apply_bucket_settings("test-bucket", config)

    # Default has one wildcard CORS rule — bucket.cors must be set and patch called.
    assert mock_bucket.cors is not None
    assert len(mock_bucket.cors) == 1
    assert mock_bucket.cors[0]["origin"] == ["*"]
    mock_bucket.patch.assert_called_once()


@pytest.mark.asyncio
async def test_apply_bucket_settings_empty_cors_clears_and_patches():
    """An explicit empty cors=[] must clear GCS CORS (bucket.cors=[]) and call patch().

    An empty list is not None: it is an intentional "clear all CORS rules" instruction,
    so bucket.patch() IS called to push the cleared state to GCS.
    """
    svc = _make_service()

    mock_bucket = MagicMock()
    svc.storage_client.bucket.return_value = mock_bucket

    config = GcpCatalogBucketConfig(cors=[])

    with patch(
        "dynastore.modules.gcp.bucket_service.run_in_thread",
        new=_sync_run_in_thread,
    ):
        await svc._apply_bucket_settings("test-bucket", config)

    # Empty list still triggers a patch to clear CORS in GCS.
    assert mock_bucket.cors == []
    mock_bucket.patch.assert_called_once()


@pytest.mark.asyncio
async def test_apply_bucket_settings_no_storage_client_skips_gracefully():
    """When storage_client is None/falsy, the method returns without error."""
    svc = _make_service()
    svc.storage_client = None  # type: ignore[assignment]

    config = GcpCatalogBucketConfig()

    # Must not raise — storage_client=None is handled by an early guard.
    await svc._apply_bucket_settings("test-bucket", config)
