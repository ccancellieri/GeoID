#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""Tests for the orphan-subscription clash guard in
``GcpEventingOpsMixin.setup_push_subscription`` and the rollback path in
``setup_managed_eventing_channel`` / ``_rollback_eventing_resources``.

Pub/Sub binds subscription→topic immutably. When a prior teardown deleted the
topic but left the subscription orphaned (pre-fix: topic delete was assumed to
cascade, it doesn't), a re-provision must either:

  - Detect the mismatch and refuse — surfacing a recovery command — instead of
    silently calling modify_push_config (which can refresh the endpoint but
    cannot rebind the immutable topic).
  - Roll back the topic + GCS notifications it just created, so the bucket isn't
    left half-wired pointing at a topic whose only subscriber is bound to a
    dead/different topic.

These tests pin those two contracts without touching real GCS / Pub/Sub.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from dynastore.modules.gcp.gcp_eventing_ops import (
    GcpEventingOpsMixin,
    OrphanSubscriptionClash,
)
from dynastore.modules.gcp.models import PushSubscriptionConfig


PROJECT = "proj-x"
TOPIC = "projects/proj-x/topics/ds-c-events"
SUB_PATH = "projects/proj-x/subscriptions/ds-c-events-sub"


class _StubMixin(GcpEventingOpsMixin):
    """Concrete fill-in for the protocol stubs on the mixin so we can call its
    methods directly. All GCP clients are MagicMocks injected per test."""

    def __init__(self):
        self._publisher = MagicMock()
        self._subscriber = MagicMock()
        self._storage = MagicMock()
        self._bucket_service = MagicMock()

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


@pytest.fixture
def mixin(monkeypatch):
    m = _StubMixin()
    m._subscriber.subscription_path.return_value = SUB_PATH

    # Force AlreadyExists branch via the publisher_v1 exception type.
    from google.api_core import exceptions as ge

    def _always_exists(*args, **kw):
        raise ge.AlreadyExists("exists")

    m._subscriber.create_subscription.side_effect = _always_exists

    # run_in_thread is the awaitable wrapper around the blocking GCP calls.
    # Replace it with an immediate sync caller so the tests stay synchronous.
    import dynastore.modules.gcp.gcp_eventing_ops as ops_mod

    async def _run_in_thread(fn, *args, **kw):
        return fn(*args, **kw)

    monkeypatch.setattr(ops_mod, "run_in_thread", _run_in_thread)
    return m


@pytest.mark.asyncio
async def test_orphan_subscription_clash_raises_when_bound_to_different_topic(mixin):
    """Existing subscription bound to a different topic → must raise, not silently
    modify_push_config."""
    mixin._subscriber.get_subscription.return_value = SimpleNamespace(topic="_deleted-topic_")

    sub_cfg = PushSubscriptionConfig(subscription_id="ds-c-events-sub", push_endpoint=None)

    with pytest.raises(OrphanSubscriptionClash) as exc:
        await mixin.setup_push_subscription(TOPIC, sub_cfg, custom_attributes={})

    msg = str(exc.value)
    assert "_deleted-topic_" in msg
    assert TOPIC in msg
    # Recovery line must include the gcloud command with the right project + sub.
    assert "gcloud pubsub subscriptions delete ds-c-events-sub" in msg
    assert f"--project={PROJECT}" in msg
    # We must NOT have called modify_push_config — that would silently mask the clash.
    mixin._subscriber.modify_push_config.assert_not_called()


@pytest.mark.asyncio
async def test_matching_topic_falls_through_to_modify_push_config(mixin):
    """Existing subscription bound to the expected topic → refresh push_config
    (pre-existing behavior). Clash guard must NOT raise."""
    mixin._subscriber.get_subscription.return_value = SimpleNamespace(topic=TOPIC)

    sub_cfg = PushSubscriptionConfig(subscription_id="ds-c-events-sub", push_endpoint=None)
    await mixin.setup_push_subscription(TOPIC, sub_cfg, custom_attributes={})

    mixin._subscriber.modify_push_config.assert_called_once()
    call_kwargs = mixin._subscriber.modify_push_config.call_args.kwargs["request"]
    assert call_kwargs["subscription"] == SUB_PATH


@pytest.mark.asyncio
async def test_rollback_deletes_notifications_and_topic_but_not_bucket(monkeypatch):
    """``_rollback_eventing_resources`` must delete every notification, then the
    topic. The bucket itself is NEVER touched (may contain user data)."""
    m = _StubMixin()
    import dynastore.modules.gcp.gcp_eventing_ops as ops_mod

    async def _run_in_thread(fn, *args, **kw):
        return fn(*args, **kw)

    monkeypatch.setattr(ops_mod, "run_in_thread", _run_in_thread)

    # Track teardown_gcs_notification call shape per notification.
    notif_calls: list[tuple[str, str]] = []

    async def _td(bucket, notif_id):
        notif_calls.append((bucket, notif_id))

    m._bucket_service.teardown_gcs_notification = _td

    await m._rollback_eventing_resources(
        catalog_id="c",
        topic_path=TOPIC,
        bucket_name="bkt",
        notification_ids=["n1", "n2"],
    )

    assert notif_calls == [("bkt", "n1"), ("bkt", "n2")]
    m._publisher.delete_topic.assert_called_once_with(request={"topic": TOPIC})
    # Bucket-level deletion is NEVER part of rollback (bucket may pre-exist with data).
    # MagicMock auto-creates attributes; .called on an auto-attr is False unless explicitly called.
    m._bucket_service.delete_bucket.assert_not_called()
    m._bucket_service.drop_storage.assert_not_called()


@pytest.mark.asyncio
async def test_rollback_continues_when_notification_delete_fails(monkeypatch):
    """One bad notification delete must not skip the next one or block topic delete."""
    m = _StubMixin()
    import dynastore.modules.gcp.gcp_eventing_ops as ops_mod

    async def _run_in_thread(fn, *args, **kw):
        return fn(*args, **kw)

    monkeypatch.setattr(ops_mod, "run_in_thread", _run_in_thread)

    notif_calls: list[str] = []

    async def _td(bucket, notif_id):
        notif_calls.append(notif_id)
        if notif_id == "n1":
            raise RuntimeError("boom")

    m._bucket_service.teardown_gcs_notification = _td

    await m._rollback_eventing_resources(
        catalog_id="c",
        topic_path=TOPIC,
        bucket_name="bkt",
        notification_ids=["n1", "n2"],
    )

    assert notif_calls == ["n1", "n2"]
    m._publisher.delete_topic.assert_called_once()


@pytest.mark.asyncio
async def test_teardown_deletes_subscription_before_topic(monkeypatch):
    """Source-of-truth fix: managed teardown must delete the subscription FIRST
    (Pub/Sub does NOT cascade topic-delete to its subscriptions). Order matters
    so the subscription is gone before the orphan window opens."""
    from dynastore.modules.gcp.gcp_config import ManagedBucketEventing
    import dynastore.modules.gcp.gcp_eventing_ops as ops_mod

    m = _StubMixin()
    m._bucket_service.get_storage_identifier = MagicMock(return_value=_make_async("bkt"))

    async def _run_in_thread(fn, *args, **kw):
        return fn(*args, **kw)

    monkeypatch.setattr(ops_mod, "run_in_thread", _run_in_thread)

    # Track call ordering via side_effects appending to a list.
    order: list[str] = []
    m._subscriber.delete_subscription.side_effect = lambda *a, **kw: order.append("sub")
    m._publisher.delete_topic.side_effect = lambda *a, **kw: order.append("topic")

    cfg = ManagedBucketEventing(
        subscription=PushSubscriptionConfig(
            subscription_id="ds-c-events-sub",
            push_endpoint=None,
            subscription_path=SUB_PATH,
        ),
        topic_path=TOPIC,
        gcs_notification_ids=[],
    )

    await m.teardown_managed_eventing_channel("c", cfg)

    assert order == ["sub", "topic"], (
        "Subscription must be deleted before the topic so it is never an orphan."
    )


def _make_async(value):
    """Wrap a sync value into an awaitable for AsyncMock-free testing."""
    async def _coro(*a, **kw):
        return value
    return _coro()
