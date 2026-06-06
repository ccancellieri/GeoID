#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""Retry/backoff for transient Aborted/Conflict on ``create_topic``.

When two concurrent callers attempt to create or configure the same Pub/Sub
topic simultaneously (e.g. the background lifecycle setup triggered by
prepare_upload_target racing with an explicit setup_catalog_gcp_resources call
on a shared GCP project), Pub/Sub returns ``409 The request raced with another
user request``.  Mapped to ``Aborted`` via gRPC or ``Conflict`` via the REST
path, this is a transient condition: one caller wins, the other should retry
rather than propagate the error.

``setup_managed_eventing_channel`` wraps ``create_topic`` in a bounded
exponential-backoff loop that swallows ``Aborted``/``Conflict`` and retries,
while still fast-failing on genuine non-transient errors.  Regression guard
for issue #1193.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import dynastore.modules.gcp.gcp_eventing_ops as ops_mod
from dynastore.modules.gcp.gcp_eventing_ops import (
    GcpEventingOpsMixin,
    _TOPIC_CREATE_MAX_ATTEMPTS,
)
from dynastore.modules.gcp.gcp_config import GcpEventingConfig, ManagedBucketEventing
from dynastore.modules.gcp.models import PushSubscriptionConfig


PROJECT = "proj-x"
CATALOG_ID = "test-catalog"
TOPIC_ID = f"ds-{CATALOG_ID}-events"
TOPIC_PATH = f"projects/{PROJECT}/topics/{TOPIC_ID}"
BUCKET = f"bucket-{CATALOG_ID}"


class _StubMixin(GcpEventingOpsMixin):
    """Concrete fill-in so the mixin's methods can be called directly.
    GCP clients are MagicMocks injected per test."""

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
    m = _StubMixin()

    # topic_path returns the deterministic path used in assertions.
    m._publisher.topic_path.return_value = TOPIC_PATH

    # IAM policy: return an object with a .bindings.add() method.
    mock_policy = MagicMock()
    mock_policy.bindings = MagicMock()
    m._publisher.get_iam_policy.return_value = mock_policy

    # GCS service-account email (required for IAM grant step).
    m._storage.get_service_account_email.return_value = "gcs-sa@proj-x.iam"

    # subscription_path for the subsequent create_subscription call.
    m._subscriber.subscription_path.return_value = (
        f"projects/{PROJECT}/subscriptions/ds-{CATALOG_ID}-default-sub"
    )

    # run_in_thread: immediate sync caller so tests stay fast.
    async def _run_in_thread(fn, *args, **kw):
        return fn(*args, **kw)

    monkeypatch.setattr(ops_mod, "run_in_thread", _run_in_thread)

    # Suppress real sleeps.
    async def _no_sleep(_delay):
        return None

    monkeypatch.setattr(ops_mod.asyncio, "sleep", _no_sleep)

    # Stub managed_transaction so the bucket-name lookup skips the DB.
    # setup_managed_eventing_channel calls it only when conn=None.
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _fake_managed_transaction(engine):
        yield MagicMock()

    monkeypatch.setattr(ops_mod, "managed_transaction", _fake_managed_transaction)

    # gcp_db.get_bucket_for_catalog_query.execute → returns the test bucket.
    mock_query = MagicMock()
    mock_query.execute = AsyncMock(return_value=BUCKET)
    monkeypatch.setattr(ops_mod.gcp_db, "get_bucket_for_catalog_query", mock_query)

    # GCS bucket.list_notifications() → no pre-existing notifications.
    mock_gcs_bucket = MagicMock()
    mock_gcs_bucket.list_notifications.return_value = []
    mock_notification = MagicMock()
    mock_notification.notification_id = "notif-1"
    mock_gcs_bucket.notification.return_value = mock_notification
    m._storage.bucket.return_value = mock_gcs_bucket

    return m


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_topic_retries_on_aborted(mixin):
    """Transient Aborted (Pub/Sub 409 race) then success → method completes."""
    from google.api_core import exceptions as ge

    calls = {"n": 0}

    def _flaky_create(*a, **kw):
        calls["n"] += 1
        if calls["n"] < 3:
            raise ge.Aborted("raced with another user request")
        # Third call succeeds.

    mixin._publisher.create_topic.side_effect = _flaky_create

    managed_cfg = ManagedBucketEventing(enabled=True)
    result = await mixin.setup_managed_eventing_channel(CATALOG_ID, managed_cfg)

    assert calls["n"] == 3, (
        f"Expected create_topic to be called 3 times (2 retries + success), got {calls['n']}"
    )
    assert result.topic_path == TOPIC_PATH


@pytest.mark.asyncio
async def test_create_topic_retries_on_conflict(mixin):
    """Transient Conflict (HTTP 409) then success → method completes."""
    from google.api_core import exceptions as ge

    calls = {"n": 0}

    def _flaky_create(*a, **kw):
        calls["n"] += 1
        if calls["n"] < 2:
            raise ge.Conflict("raced with another user request")

    mixin._publisher.create_topic.side_effect = _flaky_create

    managed_cfg = ManagedBucketEventing(enabled=True)
    result = await mixin.setup_managed_eventing_channel(CATALOG_ID, managed_cfg)

    assert calls["n"] == 2
    assert result.topic_path == TOPIC_PATH


@pytest.mark.asyncio
async def test_create_topic_already_exists_not_retried(mixin):
    """AlreadyExists is a terminal-ok (idempotent adopt) — create is called exactly once."""
    from google.api_core import exceptions as ge

    mixin._publisher.create_topic.side_effect = ge.AlreadyExists("topic exists")

    managed_cfg = ManagedBucketEventing(enabled=True)
    result = await mixin.setup_managed_eventing_channel(CATALOG_ID, managed_cfg)

    # AlreadyExists breaks out of the retry loop immediately.
    assert mixin._publisher.create_topic.call_count == 1
    assert result.topic_path == TOPIC_PATH


@pytest.mark.asyncio
async def test_create_topic_raises_after_exhausting_retries(mixin):
    """A persistent transient error still fails after the bounded attempt budget."""
    from google.api_core import exceptions as ge

    mixin._publisher.create_topic.side_effect = ge.Aborted("persistent race")

    managed_cfg = ManagedBucketEventing(enabled=True)
    with pytest.raises(ge.Aborted):
        await mixin.setup_managed_eventing_channel(CATALOG_ID, managed_cfg)

    assert mixin._publisher.create_topic.call_count == _TOPIC_CREATE_MAX_ATTEMPTS
