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

"""Regression tests for GcpStorageOpsMixin.ensure_storage_for_catalog raise_on_failure forwarding.

Reproduces the live TypeError:
  GcpStorageOpsMixin.ensure_storage_for_catalog() got an unexpected keyword
  argument 'raise_on_failure'

The mixin must accept raise_on_failure and forward it to the underlying
BucketService so that _provision_bucket_hard in task.py can propagate the
real GCS error instead of seeing a generic 'returned None' message.
"""
from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.gcp.gcp_storage_ops import GcpStorageOpsMixin


# ---------------------------------------------------------------------------
# Minimal concrete class — supplies only what the mixin needs to delegate
# ---------------------------------------------------------------------------

class _ConcreteOps(GcpStorageOpsMixin):
    """Minimal concrete realisation of the mixin for unit testing."""

    def __init__(self, bucket_service: Any) -> None:
        self._bucket_service = bucket_service
        self._upload_tickets: dict = {}
        self._credentials = None
        self._storage_client = None

    def get_bucket_service(self) -> Any:
        return self._bucket_service

    def get_storage_client(self) -> Any:
        return None


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_mixin_accepts_and_forwards_raise_on_failure_true() -> None:
    """raise_on_failure=True must reach the underlying bucket service.

    This is the regression guard for the live TypeError that was crashing
    GcpProvisionCatalogTask at _provision_bucket_hard.
    """
    inner = MagicMock()
    inner.ensure_storage_for_catalog = AsyncMock(return_value="my-project-cat-abc")

    ops = _ConcreteOps(bucket_service=inner)
    result = await ops.ensure_storage_for_catalog("cat-abc", raise_on_failure=True)

    assert result == "my-project-cat-abc"
    inner.ensure_storage_for_catalog.assert_awaited_once()
    _, kwargs = inner.ensure_storage_for_catalog.call_args
    assert kwargs.get("raise_on_failure") is True, (
        f"raise_on_failure must be forwarded to bucket service; got kwargs={kwargs}"
    )


@pytest.mark.asyncio
async def test_mixin_defaults_raise_on_failure_false() -> None:
    """Default call (no raise_on_failure) must still forward False to the service."""
    inner = MagicMock()
    inner.ensure_storage_for_catalog = AsyncMock(return_value="my-project-cat-xyz")

    ops = _ConcreteOps(bucket_service=inner)
    result = await ops.ensure_storage_for_catalog("cat-xyz")

    assert result == "my-project-cat-xyz"
    inner.ensure_storage_for_catalog.assert_awaited_once()
    _, kwargs = inner.ensure_storage_for_catalog.call_args
    assert kwargs.get("raise_on_failure") is False


@pytest.mark.asyncio
async def test_mixin_forwards_conn_and_raise_on_failure_together() -> None:
    """conn and raise_on_failure must both be forwarded in the same call."""
    inner = MagicMock()
    inner.ensure_storage_for_catalog = AsyncMock(return_value="bucket-name")

    fake_conn = MagicMock(name="conn")
    ops = _ConcreteOps(bucket_service=inner)
    await ops.ensure_storage_for_catalog("cat-1", conn=fake_conn, raise_on_failure=True)

    _, kwargs = inner.ensure_storage_for_catalog.call_args
    assert kwargs.get("conn") is fake_conn
    assert kwargs.get("raise_on_failure") is True
