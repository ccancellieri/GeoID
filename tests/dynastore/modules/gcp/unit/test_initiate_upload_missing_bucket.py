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

"""Self-heal and deferred-storage tests for ``GcpStorageOpsMixin.initiate_upload``
when the backing GCS bucket is missing for a catalog marked ready.

Two separate cases when ``get_storage_identifier`` returns ``None``:

1. **Deferred catalog** (``gcp_bucket`` absent from the checklist, or present
   with the terminal ``"deferred"`` state — un-fao/GeoID#2678): the catalog
   was created bucket-free via ``?hints=defer`` and never provisioned.
   ``mark_provisioning_step`` must NOT be called (no false demotion) and
   ``GcpStorageDeferredError`` (HTTP 409) must be raised. The absent-key form
   covers a catalog created before #2678 landed (no persisted marker to read);
   the ``"deferred"`` state form covers one created after.

2. **Stale ready state** (``gcp_bucket`` IS in checklist as ``complete`` /
   any non-``"deferred"`` terminal state): the bucket went missing after
   provisioning — the stored status is stale.
   ``mark_provisioning_step(catalog_id, "gcp_bucket", "failed")`` must be
   called (best-effort) and ``GcpFailedDependencyError`` (HTTP 424) must be
   raised.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.gcp.gcp_storage_ops import GcpStorageOpsMixin
from dynastore.modules.gcp.errors import GcpFailedDependencyError, GcpStorageDeferredError

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
    # Checklist contains gcp_bucket → stale ready state, not a deferred catalog
    catalogs_mock.get_provisioning_checklist = AsyncMock(
        return_value={"gcp_bucket": "complete"}
    )

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
    # Checklist contains gcp_bucket → stale ready state, not a deferred catalog
    catalogs_mock.get_provisioning_checklist = AsyncMock(
        return_value={"gcp_bucket": "complete"}
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
    # Checklist contains gcp_bucket → stale ready state, not a deferred catalog
    catalogs_mock.get_provisioning_checklist = AsyncMock(
        return_value={"gcp_bucket": "complete"}
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
                catalog_id="cat-no-catalogs",
                asset_def=_fake_asset_def(),
                filename="data.tif",
            )


# ---------------------------------------------------------------------------
# Test: deferred catalog — raises GcpStorageDeferredError, NO demotion
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_deferred_catalog_raises_409_and_does_not_demote():
    """A catalog whose checklist has NO 'gcp_bucket' entry (deferred at
    create, before un-fao/GeoID#2678's persisted marker landed) must raise
    ``GcpStorageDeferredError`` and must NOT call ``mark_provisioning_step``
    — the catalog is intentionally bucket-free."""
    host = _Host(bucket_name=None, provisioning_status="ready")
    fake_cat = _fake_catalog("ready")

    catalogs_mock = MagicMock()
    catalogs_mock.get_catalog = AsyncMock(return_value=fake_cat)
    catalogs_mock.mark_provisioning_step = AsyncMock(return_value=True)
    # Checklist lacks 'gcp_bucket' — deferred catalog (pre-#2678 create)
    catalogs_mock.get_provisioning_checklist = AsyncMock(
        return_value={"catalog_core": "complete"}
    )

    from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol

    def _proto(proto):
        if proto is CatalogsProtocol:
            return catalogs_mock
        if proto is ConfigsProtocol:
            return None
        return None

    with patch(_GET_PROTOCOL, side_effect=_proto):
        with pytest.raises(GcpStorageDeferredError) as exc_info:
            await host.initiate_upload(
                catalog_id="cat-deferred",
                asset_def=_fake_asset_def(),
                filename="data.tif",
            )

    # Must NOT demote — the catalog is deliberately bucket-free.
    catalogs_mock.mark_provisioning_step.assert_not_awaited()

    # Error must mention the catalog, provision task, and virtual-asset path.
    msg = str(exc_info.value)
    assert "cat-deferred" in msg
    assert "catalog_provision" in msg
    assert "virtual" in msg.lower()


@pytest.mark.asyncio
async def test_deferred_state_catalog_raises_409_and_does_not_demote():
    """un-fao/GeoID#2678: a catalog whose checklist has 'gcp_bucket': 'deferred'
    (the persisted marker written by a post-fix ``?hints=defer`` create) is
    ALSO deferred, not stale — must raise ``GcpStorageDeferredError`` and must
    NOT call ``mark_provisioning_step``."""
    host = _Host(bucket_name=None, provisioning_status="ready")
    fake_cat = _fake_catalog("ready")

    catalogs_mock = MagicMock()
    catalogs_mock.get_catalog = AsyncMock(return_value=fake_cat)
    catalogs_mock.mark_provisioning_step = AsyncMock(return_value=True)
    # 'gcp_bucket' IS present, marked 'deferred' — still a deferred catalog,
    # not a stale-ready one.
    catalogs_mock.get_provisioning_checklist = AsyncMock(
        return_value={"catalog_core": "complete", "gcp_bucket": "deferred"}
    )

    from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol

    def _proto(proto):
        if proto is CatalogsProtocol:
            return catalogs_mock
        if proto is ConfigsProtocol:
            return None
        return None

    with patch(_GET_PROTOCOL, side_effect=_proto):
        with pytest.raises(GcpStorageDeferredError) as exc_info:
            await host.initiate_upload(
                catalog_id="cat-deferred-marked",
                asset_def=_fake_asset_def(),
                filename="data.tif",
            )

    # Must NOT demote — the catalog is deliberately bucket-free.
    catalogs_mock.mark_provisioning_step.assert_not_awaited()
    assert "cat-deferred-marked" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Test: stale ready state (gcp_bucket in checklist) → still demotes + 424
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stale_ready_catalog_demotes_and_raises_424():
    """A catalog whose checklist contains 'gcp_bucket' but has no bucket is a
    stale ready state — must call ``mark_provisioning_step`` and raise
    ``GcpFailedDependencyError`` (existing behaviour preserved)."""
    host = _Host(bucket_name=None, provisioning_status="ready")
    fake_cat = _fake_catalog("ready")

    catalogs_mock = MagicMock()
    catalogs_mock.get_catalog = AsyncMock(return_value=fake_cat)
    catalogs_mock.mark_provisioning_step = AsyncMock(return_value=True)
    # Checklist includes 'gcp_bucket' → stale state, not a deferred catalog
    catalogs_mock.get_provisioning_checklist = AsyncMock(
        return_value={"gcp_bucket": "complete"}
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
                catalog_id="cat-stale",
                asset_def=_fake_asset_def(),
                filename="data.tif",
            )

    # Demotion must have been called — stale ready with bucket in checklist.
    catalogs_mock.mark_provisioning_step.assert_awaited_once_with(
        "cat-stale", "gcp_bucket", "failed"
    )
