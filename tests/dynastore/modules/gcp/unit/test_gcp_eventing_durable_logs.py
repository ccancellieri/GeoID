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

"""Verify that Pub/Sub topic and subscription lifecycle operations emit durable
log calls (via log_info/log_warning from log_manager) in addition to the
existing stdlib logger lines.

These tests monkeypatch ``log_info`` in the ``gcp_eventing_ops`` module
namespace and assert it is awaited with the expected catalog_id and event_type
for each lifecycle path:

  - topic created (gcp_topic_created)
  - topic adopted/already-exists (gcp_topic_adopted)
  - GCS notification created (gcp_gcs_notification_created)
  - subscription created (gcp_subscription_created)
  - subscription adopted/already-exists (gcp_subscription_adopted)
  - subscription deleted (gcp_subscription_deleted)
  - topic deleted via teardown (gcp_topic_deleted)
  - topic force-deleted via teardown_catalog_eventing (gcp_topic_deleted)
  - subscription force-deleted via teardown_catalog_eventing (gcp_subscription_deleted)

Regression guard for issue #2256 gap #4.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

import dynastore.modules.gcp.gcp_eventing_ops as ops_mod
from dynastore.modules.gcp.gcp_eventing_ops import GcpEventingOpsMixin
from dynastore.modules.gcp.gcp_config import (
    GcpCatalogBucketConfig,
    GcpEventingConfig,
    ManagedBucketEventing,
)
from dynastore.modules.gcp.models import PushSubscriptionConfig


PROJECT = "proj-x"
CATALOG_ID = "test-catalog"
TOPIC_ID = f"ds-{CATALOG_ID}-events"
TOPIC_PATH = f"projects/{PROJECT}/topics/{TOPIC_ID}"
SUB_PATH = f"projects/{PROJECT}/subscriptions/ds-{CATALOG_ID}-default-sub"
BUCKET = f"bucket-{CATALOG_ID}"


class _StubMixin(GcpEventingOpsMixin):
    """Concrete fill-in so the mixin's methods can be called directly.
    All GCP clients are MagicMocks injected by fixtures."""

    def __init__(self):
        self._publisher = MagicMock()
        self._subscriber = MagicMock()
        self._storage = MagicMock()
        self._bucket_service = MagicMock()
        self._engine = MagicMock()

    @property
    def engine(self):
        return self._engine

    def get_project_id(self):
        return PROJECT

    def get_region(self):
        return "europe-west1"

    def get_account_email(self):
        return "svc@proj-x.iam"

    async def get_self_url(self):
        return "https://catalog.example/api/catalog"

    def get_publisher_client(self):
        return self._publisher

    def get_subscriber_client(self):
        return self._subscriber

    def get_storage_client(self):
        return self._storage

    def get_bucket_service(self):
        return self._bucket_service

    def get_config_service(self):
        return MagicMock()

    async def setup_catalog_gcp_resources(self, catalog_id, context=None):
        return (BUCKET, GcpEventingConfig())


@pytest.fixture
def mixin(monkeypatch):
    """Shared fixture: stub mixin wired for a happy-path eventing channel setup."""
    m = _StubMixin()

    m._publisher.topic_path.return_value = TOPIC_PATH

    mock_policy = MagicMock()
    mock_policy.bindings = MagicMock()
    m._publisher.get_iam_policy.return_value = mock_policy

    m._storage.get_service_account_email.return_value = "gcs-sa@proj-x.iam"

    m._subscriber.subscription_path.return_value = SUB_PATH

    async def _run_in_thread(fn, *args, **kw):
        return fn(*args, **kw)

    monkeypatch.setattr(ops_mod, "run_in_thread", _run_in_thread)

    async def _no_sleep(_delay):
        return None

    monkeypatch.setattr(ops_mod.asyncio, "sleep", _no_sleep)

    @asynccontextmanager
    async def _fake_managed_transaction(engine):
        yield MagicMock()

    monkeypatch.setattr(ops_mod, "managed_transaction", _fake_managed_transaction)

    bucket_cfg = GcpCatalogBucketConfig(bucket_name=BUCKET)
    mock_config_service = MagicMock()
    mock_config_service.get_config = AsyncMock(return_value=bucket_cfg)
    monkeypatch.setattr(m, "get_config_service", lambda: mock_config_service)

    mock_gcs_bucket = MagicMock()
    mock_gcs_bucket.list_notifications.return_value = []
    mock_notification = MagicMock()
    mock_notification.notification_id = "notif-1"
    mock_gcs_bucket.notification.return_value = mock_notification
    m._storage.bucket.return_value = mock_gcs_bucket

    return m


# ---------------------------------------------------------------------------
# topic create / adopt
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_topic_created_emits_durable_log(mixin, monkeypatch):
    """Successful topic create → log_info called with gcp_topic_created.

    bucket_name is passed directly to skip the DB-backed bucket-name lookup
    (that path requires a real asyncpg connection for DriverContext validation).
    """
    log_calls: list[tuple] = []

    async def _log_info(catalog_id, event_type, message, **kwargs):
        log_calls.append((catalog_id, event_type, message))

    monkeypatch.setattr(ops_mod, "log_info", _log_info)

    managed_cfg = ManagedBucketEventing(enabled=True)
    await mixin.setup_managed_eventing_channel(CATALOG_ID, managed_cfg, bucket_name=BUCKET)

    assert any(
        cid == CATALOG_ID and et == "gcp_topic_created"
        for cid, et, _ in log_calls
    ), f"Expected gcp_topic_created in {log_calls}"


@pytest.mark.asyncio
async def test_topic_adopted_emits_durable_log(mixin, monkeypatch):
    """AlreadyExists (adopt) → log_info called with gcp_topic_adopted."""
    from google.api_core import exceptions as ge

    log_calls: list[tuple] = []

    async def _log_info(catalog_id, event_type, message, **kwargs):
        log_calls.append((catalog_id, event_type, message))

    monkeypatch.setattr(ops_mod, "log_info", _log_info)
    mixin._publisher.create_topic.side_effect = ge.AlreadyExists("exists")

    managed_cfg = ManagedBucketEventing(enabled=True)
    await mixin.setup_managed_eventing_channel(CATALOG_ID, managed_cfg, bucket_name=BUCKET)

    assert any(
        cid == CATALOG_ID and et == "gcp_topic_adopted"
        for cid, et, _ in log_calls
    ), f"Expected gcp_topic_adopted in {log_calls}"


# ---------------------------------------------------------------------------
# GCS notification create
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gcs_notification_created_emits_durable_log(mixin, monkeypatch):
    """New GCS notification created → log_info called with gcp_gcs_notification_created."""
    log_calls: list[tuple] = []

    async def _log_info(catalog_id, event_type, message, **kwargs):
        log_calls.append((catalog_id, event_type, message))

    monkeypatch.setattr(ops_mod, "log_info", _log_info)

    managed_cfg = ManagedBucketEventing(enabled=True)
    await mixin.setup_managed_eventing_channel(CATALOG_ID, managed_cfg, bucket_name=BUCKET)

    assert any(
        cid == CATALOG_ID and et == "gcp_gcs_notification_created"
        for cid, et, _ in log_calls
    ), f"Expected gcp_gcs_notification_created in {log_calls}"


# ---------------------------------------------------------------------------
# subscription create / adopt
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_subscription_created_emits_durable_log(mixin, monkeypatch):
    """Successful subscription create → log_info called with gcp_subscription_created."""
    log_calls: list[tuple] = []

    async def _log_info(catalog_id, event_type, message, **kwargs):
        log_calls.append((catalog_id, event_type, message))

    monkeypatch.setattr(ops_mod, "log_info", _log_info)

    sub_cfg = PushSubscriptionConfig(subscription_id=f"ds-{CATALOG_ID}-default-sub")
    attrs = {"catalog_id": CATALOG_ID, "subscription_type": "managed"}
    await mixin.setup_push_subscription(TOPIC_PATH, sub_cfg, custom_attributes=attrs)

    assert any(
        cid == CATALOG_ID and et == "gcp_subscription_created"
        for cid, et, _ in log_calls
    ), f"Expected gcp_subscription_created in {log_calls}"


@pytest.mark.asyncio
async def test_subscription_adopted_emits_durable_log(mixin, monkeypatch):
    """Subscription AlreadyExists bound to the same topic → log_info with gcp_subscription_adopted."""
    from google.api_core import exceptions as ge

    log_calls: list[tuple] = []

    async def _log_info(catalog_id, event_type, message, **kwargs):
        log_calls.append((catalog_id, event_type, message))

    monkeypatch.setattr(ops_mod, "log_info", _log_info)
    mixin._subscriber.create_subscription.side_effect = ge.AlreadyExists("exists")
    mixin._subscriber.get_subscription.return_value = SimpleNamespace(topic=TOPIC_PATH)

    sub_cfg = PushSubscriptionConfig(subscription_id=f"ds-{CATALOG_ID}-default-sub")
    attrs = {"catalog_id": CATALOG_ID, "subscription_type": "managed"}
    await mixin.setup_push_subscription(TOPIC_PATH, sub_cfg, custom_attributes=attrs)

    assert any(
        cid == CATALOG_ID and et == "gcp_subscription_adopted"
        for cid, et, _ in log_calls
    ), f"Expected gcp_subscription_adopted in {log_calls}"


# ---------------------------------------------------------------------------
# teardown: subscription + topic deleted
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_teardown_subscription_emits_durable_log(monkeypatch):
    """Managed teardown: subscription delete → log_info with gcp_subscription_deleted."""
    m = _StubMixin()

    async def _run_in_thread(fn, *args, **kw):
        return fn(*args, **kw)

    monkeypatch.setattr(ops_mod, "run_in_thread", _run_in_thread)

    m._bucket_service.get_storage_identifier = AsyncMock(return_value=BUCKET)

    log_calls: list[tuple] = []

    async def _log_info(catalog_id, event_type, message, **kwargs):
        log_calls.append((catalog_id, event_type, message))

    monkeypatch.setattr(ops_mod, "log_info", _log_info)

    cfg = ManagedBucketEventing(
        subscription=PushSubscriptionConfig(
            subscription_id=f"ds-{CATALOG_ID}-default-sub",
            subscription_path=SUB_PATH,
        ),
        topic_path=TOPIC_PATH,
        gcs_notification_ids=[],
    )
    await m.teardown_managed_eventing_channel(CATALOG_ID, cfg)

    assert any(
        cid == CATALOG_ID and et == "gcp_subscription_deleted"
        for cid, et, _ in log_calls
    ), f"Expected gcp_subscription_deleted in {log_calls}"


@pytest.mark.asyncio
async def test_teardown_topic_emits_durable_log(monkeypatch):
    """Managed teardown: topic delete → log_info with gcp_topic_deleted."""
    m = _StubMixin()

    async def _run_in_thread(fn, *args, **kw):
        return fn(*args, **kw)

    monkeypatch.setattr(ops_mod, "run_in_thread", _run_in_thread)

    m._bucket_service.get_storage_identifier = AsyncMock(return_value=BUCKET)

    log_calls: list[tuple] = []

    async def _log_info(catalog_id, event_type, message, **kwargs):
        log_calls.append((catalog_id, event_type, message))

    monkeypatch.setattr(ops_mod, "log_info", _log_info)

    cfg = ManagedBucketEventing(
        subscription=PushSubscriptionConfig(
            subscription_id=f"ds-{CATALOG_ID}-default-sub",
            subscription_path=SUB_PATH,
        ),
        topic_path=TOPIC_PATH,
        gcs_notification_ids=[],
    )
    await m.teardown_managed_eventing_channel(CATALOG_ID, cfg)

    assert any(
        cid == CATALOG_ID and et == "gcp_topic_deleted"
        for cid, et, _ in log_calls
    ), f"Expected gcp_topic_deleted in {log_calls}"


# ---------------------------------------------------------------------------
# teardown_catalog_eventing: force-delete paths
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_force_teardown_topic_emits_durable_log(monkeypatch):
    """teardown_catalog_eventing(config=None): force topic delete → log_info with gcp_topic_deleted."""
    m = _StubMixin()

    async def _run_in_thread(fn, *args, **kw):
        return fn(*args, **kw)

    monkeypatch.setattr(ops_mod, "run_in_thread", _run_in_thread)
    m._publisher.topic_path.return_value = TOPIC_PATH

    log_calls: list[tuple] = []

    async def _log_info(catalog_id, event_type, message, **kwargs):
        log_calls.append((catalog_id, event_type, message))

    monkeypatch.setattr(ops_mod, "log_info", _log_info)

    await m.teardown_catalog_eventing(CATALOG_ID, config=None)

    assert any(
        cid == CATALOG_ID and et == "gcp_topic_deleted"
        for cid, et, _ in log_calls
    ), f"Expected gcp_topic_deleted in {log_calls}"


@pytest.mark.asyncio
async def test_force_teardown_subscription_emits_durable_log(monkeypatch):
    """teardown_catalog_eventing(config=None): force subscription delete → log_info with gcp_subscription_deleted."""
    m = _StubMixin()

    async def _run_in_thread(fn, *args, **kw):
        return fn(*args, **kw)

    monkeypatch.setattr(ops_mod, "run_in_thread", _run_in_thread)
    m._publisher.topic_path.return_value = TOPIC_PATH
    m._subscriber.subscription_path.return_value = SUB_PATH

    log_calls: list[tuple] = []

    async def _log_info(catalog_id, event_type, message, **kwargs):
        log_calls.append((catalog_id, event_type, message))

    monkeypatch.setattr(ops_mod, "log_info", _log_info)

    await m.teardown_catalog_eventing(CATALOG_ID, config=None)

    assert any(
        cid == CATALOG_ID and et == "gcp_subscription_deleted"
        for cid, et, _ in log_calls
    ), f"Expected gcp_subscription_deleted in {log_calls}"
