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

"""Bucket-cleanup contract for ``GcpCatalogOpsMixin.setup_catalog_gcp_resources``.

The bucket is HARD state: once ``ensure_storage_for_catalog`` returns, the
bucket exists in GCS and its name is committed to the catalog config.
Eventing (Pub/Sub topic / GCS notification) is SOFT: if setup fails, the
catalog degrades but the bucket MUST NOT be deleted — the catalog is later
repaired via the /reprovision endpoint without touching user data.

Scenario A: Eventing setup fails (e.g. PermissionDenied) while the catalog
            still exists and the bucket was already committed (reused).
            ``drop_storage`` must NOT be called; the exception must propagate.

Scenario B: The catalog row is not found during the visibility poll
            (catalog_vanished=True).
            ``drop_storage`` MUST be called to clean up the orphaned bucket.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.gcp.gcp_catalog_ops import GcpCatalogOpsMixin


# ---------------------------------------------------------------------------
# Minimal host stub
# ---------------------------------------------------------------------------


class _Host(GcpCatalogOpsMixin):
    """Concrete host providing only the collaborator methods that
    ``setup_catalog_gcp_resources`` calls."""

    def __init__(
        self,
        *,
        bucket_name: str = "test-bucket",
        eventing_raises: Optional[Exception] = None,
        catalog_exists: bool = True,
    ) -> None:
        self._bucket_name = bucket_name
        self._eventing_raises = eventing_raises
        self._catalog_exists = catalog_exists
        self._engine = MagicMock(name="engine")

        # Track calls to verify the contract.
        self.drop_storage = AsyncMock(return_value=True)
        self.teardown_managed_eventing_channel = AsyncMock(return_value=None)

    @property
    def engine(self) -> Any:
        return self._engine

    def get_bucket_service(self) -> Any:
        bucket_svc = MagicMock()
        bucket_svc.ensure_storage_for_catalog = AsyncMock(
            return_value=self._bucket_name
        )
        return bucket_svc

    async def get_eventing_config(self, *args: Any, **kw: Any) -> None:
        return None  # triggers default eventing config creation

    async def set_eventing_config(self, *args: Any, **kw: Any) -> Any:
        from dynastore.modules.gcp.gcp_config import GcpEventingConfig, ManagedBucketEventing
        cfg = GcpEventingConfig(managed_eventing=ManagedBucketEventing(enabled=True))
        return cfg

    def generate_default_subscription_id(self, catalog_id: str) -> str:
        return f"ds-{catalog_id}-events-sub"

    async def setup_managed_eventing_channel(self, *args: Any, **kw: Any) -> Any:
        if self._eventing_raises is not None:
            raise self._eventing_raises
        from dynastore.modules.gcp.gcp_config import ManagedBucketEventing
        me = ManagedBucketEventing(enabled=True)
        me.topic_path = "projects/proj/topics/ds-test-events"
        return me


# ---------------------------------------------------------------------------
# Transaction and visibility mocking helpers
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _fake_managed_transaction(_engine):
    conn = MagicMock()
    yield conn


# ---------------------------------------------------------------------------
# Scenario A — eventing failure on a live catalog preserves the bucket
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_eventing_failure_does_not_delete_committed_bucket(monkeypatch):
    """When eventing setup raises and the catalog still exists, the already-
    committed bucket must NOT be deleted.  The exception must propagate so the
    task layer can mark eventing as degraded."""
    host = _Host(
        bucket_name="cat-test-bucket",
        eventing_raises=Exception("403 pubsub.topics.attachSubscription denied"),
        catalog_exists=True,
    )

    import dynastore.modules.gcp.gcp_catalog_ops as ops_mod
    from unittest.mock import patch

    monkeypatch.setattr(ops_mod, "managed_transaction", _fake_managed_transaction)

    with (
        patch(
            "dynastore.modules.gcp.gcp_catalog_ops._CATALOG_EXISTS_QUERY.execute",
            AsyncMock(return_value=1),
        ),
        patch(
            "dynastore.modules.gcp.gcp_catalog_ops._get_catalog_visibility_tunables",
            return_value=(1, 0),
        ),
    ):
        with pytest.raises(Exception, match="attachSubscription"):
            await host.setup_catalog_gcp_resources("cat-test")

    # The bucket was committed; eventing failure must never delete it.
    host.drop_storage.assert_not_called()
    # Eventing here raised before the topic was created, so there is nothing to
    # tear down — and on a live catalog we never tear eventing down anyway.
    host.teardown_managed_eventing_channel.assert_not_called()


@pytest.mark.asyncio
async def test_eventing_failure_exception_propagates(monkeypatch):
    """The eventing failure exception must propagate unchanged so the task layer
    receives it and can mark the eventing step degraded."""
    host = _Host(
        eventing_raises=PermissionError("iam denied"),
        catalog_exists=True,
    )

    import dynastore.modules.gcp.gcp_catalog_ops as ops_mod
    from unittest.mock import patch

    monkeypatch.setattr(ops_mod, "managed_transaction", _fake_managed_transaction)

    with (
        patch(
            "dynastore.modules.gcp.gcp_catalog_ops._CATALOG_EXISTS_QUERY.execute",
            AsyncMock(return_value=1),
        ),
        patch(
            "dynastore.modules.gcp.gcp_catalog_ops._get_catalog_visibility_tunables",
            return_value=(1, 0),
        ),
    ):
        with pytest.raises(PermissionError):
            await host.setup_catalog_gcp_resources("cat-test")


# ---------------------------------------------------------------------------
# Scenario B — catalog vanished mid-provision → orphaned bucket MUST be deleted
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_catalog_vanished_triggers_orphan_bucket_delete(monkeypatch):
    """When the catalog row is not found during the visibility poll the bucket
    is a genuine orphan — drop_storage MUST be called to prevent GCS resource
    leaks."""
    host = _Host(
        bucket_name="orphan-bucket",
        eventing_raises=None,  # eventing would succeed, but catalog check fails first
        catalog_exists=False,
    )

    import dynastore.modules.gcp.gcp_catalog_ops as ops_mod
    from unittest.mock import patch

    monkeypatch.setattr(ops_mod, "managed_transaction", _fake_managed_transaction)

    with (
        patch(
            "dynastore.modules.gcp.gcp_catalog_ops._CATALOG_EXISTS_QUERY.execute",
            AsyncMock(return_value=None),  # catalog not found
        ),
        patch(
            "dynastore.modules.gcp.gcp_catalog_ops._get_catalog_visibility_tunables",
            return_value=(1, 0),
        ),
    ):
        with pytest.raises(asyncio.CancelledError):
            await host.setup_catalog_gcp_resources("cat-orphan")

    # Genuine orphan: bucket AND the (successfully created) eventing topic must
    # both be cleaned up — symmetric teardown only on a real catalog-vanish.
    host.drop_storage.assert_called_once_with("cat-orphan")
    host.teardown_managed_eventing_channel.assert_called_once()


# ---------------------------------------------------------------------------
# Scenario C — no bucket provisioned (bucket already existed in P1 check, but
#              here we simulate a failure that occurs before ensure_storage
#              returns, so provisioned_bucket is still None)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_bucket_provisioned_means_no_drop_storage_call(monkeypatch):
    """When ``ensure_storage_for_catalog`` itself raises (before provisioned_bucket
    is set), ``drop_storage`` must not be called even when the catalog exists."""
    host = _Host(catalog_exists=True)
    host.get_bucket_service = lambda: MagicMock(
        ensure_storage_for_catalog=AsyncMock(side_effect=RuntimeError("GCS 503"))
    )

    import dynastore.modules.gcp.gcp_catalog_ops as ops_mod
    from unittest.mock import patch

    monkeypatch.setattr(ops_mod, "managed_transaction", _fake_managed_transaction)

    with (
        patch(
            "dynastore.modules.gcp.gcp_catalog_ops._CATALOG_EXISTS_QUERY.execute",
            AsyncMock(return_value=1),
        ),
        patch(
            "dynastore.modules.gcp.gcp_catalog_ops._get_catalog_visibility_tunables",
            return_value=(1, 0),
        ),
    ):
        with pytest.raises(RuntimeError, match="GCS 503"):
            await host.setup_catalog_gcp_resources("cat-503")

    host.drop_storage.assert_not_called()


# ---------------------------------------------------------------------------
# Scenario D — the real dev failure mode: the topic/subscription are created
#              successfully, then the eventing-config DB persist fails with a
#              transient error while the catalog still exists. NOTHING may be
#              torn down — not the bucket, not the working topic — so reprovision
#              (idempotent) can reconcile without churn or data loss.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_db_persist_failure_after_topic_created_preserves_all(monkeypatch):
    """Eventing succeeds (topic created), then the config DB write raises a
    transient error on a live catalog. Neither the committed bucket nor the
    successfully-created topic may be torn down."""
    host = _Host(
        bucket_name="cat-keep-bucket",
        eventing_raises=None,  # setup_managed_eventing_channel succeeds → topic set
        catalog_exists=True,
    )
    # The DB persist of the eventing config is what fails (the dev trigger was an
    # asyncpg "connection is closed" on a SAVEPOINT here).
    host.set_eventing_config = AsyncMock(  # type: ignore[method-assign]
        side_effect=RuntimeError("connection is closed")
    )

    import dynastore.modules.gcp.gcp_catalog_ops as ops_mod
    from unittest.mock import patch

    monkeypatch.setattr(ops_mod, "managed_transaction", _fake_managed_transaction)

    with (
        patch(
            "dynastore.modules.gcp.gcp_catalog_ops._CATALOG_EXISTS_QUERY.execute",
            AsyncMock(return_value=1),
        ),
        patch(
            "dynastore.modules.gcp.gcp_catalog_ops._get_catalog_visibility_tunables",
            return_value=(1, 0),
        ),
    ):
        with pytest.raises(RuntimeError, match="connection is closed"):
            await host.setup_catalog_gcp_resources("cat-keep")

    # Transient failure on a live catalog: preserve everything.
    host.drop_storage.assert_not_called()
    host.teardown_managed_eventing_channel.assert_not_called()
