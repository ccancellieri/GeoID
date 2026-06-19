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

"""Self-heal tests for ``GcpStorageOpsMixin.initiate_upload`` when the
backing GCS bucket is missing for a catalog marked ready.

When ``get_storage_identifier`` returns ``None`` after the catalog's
``provisioning_status == "ready"`` check has passed, two things MUST happen:

1. ``CatalogsProtocol.mark_provisioning_step(catalog_id, "gcp_bucket", "failed")``
   is called (best-effort — a failure there must not mask the original error).
2. ``GcpFailedDependencyError`` is raised so the client receives an HTTP 424
   (rather than an opaque 500) and the message explains how to repair.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.gcp.gcp_storage_ops import GcpStorageOpsMixin
from dynastore.modules.gcp.errors import GcpFailedDependencyError

# ---------------------------------------------------------------------------
# Module path constants for patching
# ---------------------------------------------------------------------------

_GET_PROTOCOL = "dynastore.modules.gcp.gcp_storage_ops.get_protocol"

# ---------------------------------------------------------------------------
# Minimal host stub
# ---------------------------------------------------------------------------


class _Host(GcpStorageOpsMixin):
    """Concrete host exposing only what ``initiate_upload`` touches."""

    def __init__(
        self,
        *,
        bucket_name: Optional[str],
        provisioning_status: str = "ready",
    ) -> None:
        self._bucket_name = bucket_name
        self._provisioning_status = provisioning_status
        self._upload_tickets: dict = {}
        self._credentials = MagicMock()
        self._storage_client = MagicMock()

    def get_bucket_service(self) -> Any:
        svc = MagicMock()
        svc.get_storage_identifier = AsyncMock(return_value=self._bucket_name)
        return svc

    def get_storage_client(self) -> Any:
        return self._storage_client

    async def get_storage_identifier(self, catalog_id: str) -> Optional[str]:
        return self._bucket_name

    async def prepare_upload_target(
        self, catalog_id: str, collection_id: Optional[str] = None
    ) -> None:
        return None


def _fake_catalog(provisioning_status: str = "ready") -> SimpleNamespace:
    return SimpleNamespace(provisioning_status=provisioning_status)


def _fake_asset_def(asset_id: str = "asset-1") -> SimpleNamespace:
    return SimpleNamespace(
        asset_id=asset_id,
        asset_type=SimpleNamespace(value="geotiff"),
        metadata={},
    )


# ---------------------------------------------------------------------------
# Test: missing bucket → mark_provisioning_step called + GcpFailedDependencyError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_bucket_marks_step_failed_and_raises_424():
    """A ready catalog with no backing bucket triggers best-effort demotion
    (mark_provisioning_step 'failed') and raises GcpFailedDependencyError."""
    host = _Host(bucket_name=None, provisioning_status="ready")
    fake_cat = _fake_catalog("ready")

    catalogs_mock = MagicMock()
    catalogs_mock.get_catalog = AsyncMock(return_value=fake_cat)
    catalogs_mock.mark_provisioning_step = AsyncMock(return_value=True)

    from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol

    def _proto(proto):
        if proto is CatalogsProtocol:
            return catalogs_mock
        if proto is ConfigsProtocol:
            return None
        return None

    with patch(_GET_PROTOCOL, side_effect=_proto):
        with pytest.raises(GcpFailedDependencyError) as exc_info:
            await host.initiate_upload(
                catalog_id="cat-broken",
                asset_def=_fake_asset_def(),
                filename="data.tif",
            )

    # Demotion must have been attempted with the right arguments.
    catalogs_mock.mark_provisioning_step.assert_awaited_once_with(
        "cat-broken", "gcp_bucket", "failed"
    )

    # The error must be actionable: mention the catalog and reprovision path.
    msg = str(exc_info.value)
    assert "cat-broken" in msg
    assert "reprovision" in msg.lower()


@pytest.mark.asyncio
async def test_missing_bucket_raises_even_when_demotion_fails():
    """A failure in ``mark_provisioning_step`` must not mask the original error;
    ``GcpFailedDependencyError`` must still be raised."""
    host = _Host(bucket_name=None, provisioning_status="ready")
    fake_cat = _fake_catalog("ready")

    catalogs_mock = MagicMock()
    catalogs_mock.get_catalog = AsyncMock(return_value=fake_cat)
    catalogs_mock.mark_provisioning_step = AsyncMock(
        side_effect=RuntimeError("DB connection lost")
    )

    from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol

    def _proto(proto):
        if proto is CatalogsProtocol:
            return catalogs_mock
        if proto is ConfigsProtocol:
            return None
        return None

    with patch(_GET_PROTOCOL, side_effect=_proto):
        with pytest.raises(GcpFailedDependencyError):
            await host.initiate_upload(
                catalog_id="cat-db-broken",
                asset_def=_fake_asset_def(),
                filename="data.tif",
            )


@pytest.mark.asyncio
async def test_missing_bucket_without_catalogs_protocol_still_raises():
    """When ``CatalogsProtocol`` is not resolvable (should not happen in production
    but must not crash silently), ``GcpFailedDependencyError`` is still raised."""
    host = _Host(bucket_name=None, provisioning_status="ready")
    fake_cat = _fake_catalog("ready")

    # catalogs_provider is used in initiate_upload before the bucket check too,
    # but the guard raises GcpServiceUnavailableError. To test the bucket-missing
    # path we need catalogs available for get_catalog, but None for the demotion
    # lookup. Use a two-step side effect.
    catalogs_mock = MagicMock()
    catalogs_mock.get_catalog = AsyncMock(return_value=fake_cat)
    catalogs_mock.mark_provisioning_step = AsyncMock(return_value=False)

    from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol

    def _proto(proto):
        if proto is CatalogsProtocol:
            return catalogs_mock
        if proto is ConfigsProtocol:
            return None
        return None

    with patch(_GET_PROTOCOL, side_effect=_proto):
        with pytest.raises(GcpFailedDependencyError):
            await host.initiate_upload(
                catalog_id="cat-no-catalogs",
                asset_def=_fake_asset_def(),
                filename="data.tif",
            )
