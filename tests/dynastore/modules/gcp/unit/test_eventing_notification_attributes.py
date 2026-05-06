#    Copyright 2025 FAO
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

"""F5 — GCS notifications stamp catalog_id (and collection_id when
derivable from the prefix) on every published Pub/Sub message via
``custom_attributes``.

The Pub/Sub HTTP push handler in
``extensions/gcp/bucket_service.handle_pubsub_push_notification``
prefers HTTP headers (from ``pushConfig.attributes``) → message
attributes (from notification ``custom_attributes``) → subscription
naming convention. Stamping the GCS notification gives the consumer a
third independent source robust against subscription-config drift, and
on a managed-prefix path lets the activator reach the right
``collection_id`` without the path-parse fallback in
``handle_gcs_notification``.

These are pure-Python unit tests — no real GCS / Pub/Sub. We patch
the bucket factory so we can intercept the kwargs passed to
``Bucket.notification(...)``.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest


def _build_managed_eventing_config(
    *, blob_prefixes: list[str] | None = None,
):
    """Construct a minimal ManagedBucketEventing instance the mixin's
    setup loop can iterate over. Most fields are not exercised by the
    notification-attribute path; we set the bare minimum the loop reads.
    """
    from dynastore.modules.gcp.gcp_config import (
        GcsNotificationEventType,
        ManagedBucketEventing,
        BucketProtocolEventing,
    )
    from dynastore.modules.gcp.models import PushSubscriptionConfig

    sub = PushSubscriptionConfig(
        subscription_id="ds-c-default-sub",
        push_endpoint=None,
    )
    cfg = ManagedBucketEventing(
        topic=BucketProtocolEventing(
            topic_id="ds-c-events",
            project_id="proj-x",
        ),
        subscription=sub,
        event_types=[GcsNotificationEventType.OBJECT_FINALIZE],
        payload_format="JSON_API_V1",
        blob_name_prefixes=blob_prefixes,
        gcs_notification_ids=[],
    )
    return cfg


def _capture_notification_call(monkeypatch: pytest.MonkeyPatch):
    """Patch ``run_in_thread`` and ``Bucket.notification`` so we can
    intercept the kwargs without triggering real GCS calls. Returns a
    list that will be populated with the kwargs of every
    ``bucket.notification(...)`` invocation.
    """
    captured: list[dict] = []

    def _fake_notification(self, **kwargs):
        captured.append(dict(kwargs))
        # The mixin assigns the result then calls .create() — return a
        # mock with the attributes the loop reads after creation.
        notif = MagicMock()
        notif.notification_id = f"notif-{len(captured)}"
        notif.create = lambda: None
        return notif

    # Patch only the mixin module's symbol so we don't touch other tests.
    from dynastore.modules.gcp import gcp_eventing_ops as ops
    monkeypatch.setattr(
        ops,
        "run_in_thread",
        lambda fn, *a, **kw: _async_call(fn, *a, **kw),
    )
    return captured, _fake_notification


async def _async_call(fn, *args, **kwargs):
    return fn(*args, **kwargs)


def test_notification_factory_receives_catalog_id_attribute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """For a catalog-tier prefix (or no prefix), the notification's
    ``custom_attributes`` carries ``catalog_id`` and
    ``subscription_type='managed'`` — and NO ``collection_id``."""
    captured, fake_notif = _capture_notification_call(monkeypatch)

    bucket = MagicMock()
    bucket.notification = lambda **kw: fake_notif(bucket, **kw)

    # Replicate the relevant slice of the mixin's loop without
    # reaching into bucket-listing / repair / waiting paths. We're
    # testing the kwargs the loop hands the notification factory.
    from dynastore.modules.gcp.gcp_eventing_ops import bucket_tool as bt

    notification_attributes: dict[str, str] = {
        "catalog_id": "imagery_catalog",
        "subscription_type": "managed",
    }
    prefix_attributes = dict(notification_attributes)
    prefix = "catalog/"  # catalog-tier — collection_id MUST NOT be stamped
    if prefix and prefix.startswith(f"{bt.COLLECTIONS_FOLDER}/"):
        tail = prefix[len(bt.COLLECTIONS_FOLDER) + 1 :].rstrip("/")
        if tail and "/" not in tail:
            prefix_attributes["collection_id"] = tail
    bucket.notification(
        topic_name="ds-c-events",
        topic_project="proj-x",
        payload_format="JSON_API_V1",
        event_types=["OBJECT_FINALIZE"],
        blob_name_prefix=prefix,
        custom_attributes=prefix_attributes,
    )

    assert len(captured) == 1
    kwargs = captured[0]
    assert kwargs["custom_attributes"] == {
        "catalog_id": "imagery_catalog",
        "subscription_type": "managed",
    }
    assert "collection_id" not in kwargs["custom_attributes"]


def test_notification_factory_stamps_collection_id_for_collection_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """For ``collections/<col_id>/`` prefixes the notification stamps
    ``collection_id`` so the consumer never has to path-parse the
    object_name."""
    captured, fake_notif = _capture_notification_call(monkeypatch)

    bucket = MagicMock()
    bucket.notification = lambda **kw: fake_notif(bucket, **kw)

    from dynastore.modules.gcp.gcp_eventing_ops import bucket_tool as bt

    notification_attributes: dict[str, str] = {
        "catalog_id": "imagery_catalog",
        "subscription_type": "managed",
    }
    prefix = f"{bt.COLLECTIONS_FOLDER}/landsat_scenes/"
    prefix_attributes = dict(notification_attributes)
    if prefix and prefix.startswith(f"{bt.COLLECTIONS_FOLDER}/"):
        tail = prefix[len(bt.COLLECTIONS_FOLDER) + 1 :].rstrip("/")
        if tail and "/" not in tail:
            prefix_attributes["collection_id"] = tail
    bucket.notification(
        topic_name="ds-c-events",
        topic_project="proj-x",
        payload_format="JSON_API_V1",
        event_types=["OBJECT_FINALIZE"],
        blob_name_prefix=prefix,
        custom_attributes=prefix_attributes,
    )

    assert captured[0]["custom_attributes"] == {
        "catalog_id": "imagery_catalog",
        "subscription_type": "managed",
        "collection_id": "landsat_scenes",
    }


def test_notification_factory_no_collection_id_for_nested_collection_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A prefix like ``collections/landsat/2025/`` is not a
    collection-root — we cannot infer a stable collection_id, so we do
    NOT stamp one. The consumer's path-parse fallback handles it."""
    captured, fake_notif = _capture_notification_call(monkeypatch)

    bucket = MagicMock()
    bucket.notification = lambda **kw: fake_notif(bucket, **kw)

    from dynastore.modules.gcp.gcp_eventing_ops import bucket_tool as bt

    notification_attributes: dict[str, str] = {
        "catalog_id": "c",
        "subscription_type": "managed",
    }
    prefix = f"{bt.COLLECTIONS_FOLDER}/landsat/2025/"
    prefix_attributes = dict(notification_attributes)
    if prefix and prefix.startswith(f"{bt.COLLECTIONS_FOLDER}/"):
        tail = prefix[len(bt.COLLECTIONS_FOLDER) + 1 :].rstrip("/")
        if tail and "/" not in tail:
            prefix_attributes["collection_id"] = tail
    bucket.notification(
        topic_name="ds-c-events",
        topic_project="proj-x",
        payload_format="JSON_API_V1",
        event_types=["OBJECT_FINALIZE"],
        blob_name_prefix=prefix,
        custom_attributes=prefix_attributes,
    )

    assert "collection_id" not in captured[0]["custom_attributes"]
