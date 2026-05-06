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

"""Pub/Sub HTTP-push handler unit tests (Stage 4.2).

Drives ``POST /gcp/events/pubsub-push`` with a synthetic Pub/Sub
envelope and asserts on the returned HTTP status:

* 204 — successful activation, idempotent re-delivery, orphan
  finalize, non-FINALIZE event.
* 503 — transient infrastructure failure (DB unreachable etc).

GCP Pub/Sub push subscriptions cannot reach localhost — per the
project memory rule and Stage 4.1 user clarification, no real Pub/Sub
fixture is wired up. End-to-end verification is review-only.
"""
from __future__ import annotations

import base64
import json
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from dynastore.extensions.gcp import bucket_service as bs_module
from dynastore.extensions.gcp.bucket_service import (
    BucketService,
    verify_pubsub_jwt,
)
from dynastore.modules.gcp.gcp_finalize_activator import (
    ActivationOutcome,
    OrphanFinalizeEvent,
)


# ---------------------------------------------------------------------------
# Synthetic Pub/Sub envelope helpers
# ---------------------------------------------------------------------------


def make_pubsub_envelope(
    event_type: str = "OBJECT_FINALIZE",
    bucket: str = "bkt",
    name: str = "collections/col1/image.tif",
    generation: str = "42",
    md5: str = "MD5HASH==",
    size: int = 12345,
    custom_metadata: Optional[Dict[str, str]] = None,
    catalog_id: str = "cat_a",
) -> Dict[str, Any]:
    """Mirror the shape Pub/Sub delivers — base64 inner GCS event JSON
    + envelope ``attributes`` + ``messageId`` + ``subscription`` path."""
    inner = {
        "kind": "storage#object",
        "id": f"{bucket}/{name}/{generation}",
        "name": name,
        "bucket": bucket,
        "size": str(size),
        "md5Hash": md5,
        "metadata": custom_metadata or {},
        "generation": generation,
        "contentType": "image/tiff",
    }
    return {
        "message": {
            "data": base64.b64encode(json.dumps(inner).encode()).decode(),
            "attributes": {
                "eventType": event_type,
                "bucketId": bucket,
                "objectId": name,
                "subscription_id": f"ds-{catalog_id}-default-sub",
                "subscription_type": "managed",
                "catalog_id": catalog_id,
            },
            "messageId": "test-msg-1",
        },
        "subscription": f"projects/p/subscriptions/ds-{catalog_id}-default-sub",
    }


# ---------------------------------------------------------------------------
# App / TestClient fixture — mounts the BucketService router directly
# and bypasses JWT verification.
# ---------------------------------------------------------------------------


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(BucketService.router)
    # JWT verification fans out to Google's key-server — short-circuit it.
    app.dependency_overrides[verify_pubsub_jwt] = lambda: None
    return TestClient(app)


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_finalize_returns_204_on_activation(client: TestClient) -> None:
    """OBJECT_FINALIZE → activator returns 'activated' → 204 ack."""
    envelope = make_pubsub_envelope()
    fake_assets = AsyncMock(return_value=ActivationOutcome(
        asset_id="asset_42", action="activated"
    ))
    fake_listener = MagicMock()

    with patch(
        "dynastore.extensions.gcp.gcp_events.handle_asset_events",
        fake_assets,
    ):
        # The dispatcher fans out to handle_gcs_notification which then calls
        # handle_asset_events; we patch the latter to be the gate. The
        # default listener registered at lifespan time is handle_gcs_notification,
        # but since this test app skips lifespan, we register the dispatch
        # listener manually.
        from dynastore.extensions.gcp import gcp_events as ge
        ge._gcp_event_listeners.clear()
        ge.register_gcp_event_listener("*", ge.handle_gcs_notification)

        resp = client.post("/gcp/events/pubsub-push", json=envelope)

    assert resp.status_code == 204, resp.text
    fake_assets.assert_awaited_once()


@pytest.mark.asyncio
async def test_message_attribute_catalog_id_wins_over_subscription_naming(
    client: TestClient,
) -> None:
    """F5 hardening: catalog_id from ``message.attributes`` is the
    authoritative source — even when the subscription name encodes a
    different value, the message-attribute wins.

    This pins the contract that GCS notification ``custom_attributes``
    (set up in ``modules/gcp/gcp_eventing_ops.py``) flow through to the
    consumer regardless of subscription-naming drift.
    """
    # Subscription name encodes "ds-WRONG-default-sub" — if the handler
    # falls back to name-parsing, catalog_id would resolve to "WRONG".
    envelope = make_pubsub_envelope(catalog_id="real_catalog")
    envelope["subscription"] = "projects/p/subscriptions/ds-WRONG-default-sub"

    seen_catalog_ids: list[str] = []

    async def _spy(catalog_id: str, *args, **kwargs):
        seen_catalog_ids.append(catalog_id)
        return ActivationOutcome(asset_id="a", action="activated")

    with patch(
        "dynastore.extensions.gcp.gcp_events.handle_asset_events",
        new=_spy,
    ):
        from dynastore.extensions.gcp import gcp_events as ge
        ge._gcp_event_listeners.clear()
        ge.register_gcp_event_listener("*", ge.handle_gcs_notification)
        resp = client.post("/gcp/events/pubsub-push", json=envelope)

    assert resp.status_code == 204
    assert seen_catalog_ids == ["real_catalog"], (
        f"Expected catalog_id from message.attributes, got "
        f"{seen_catalog_ids!r}"
    )


@pytest.mark.asyncio
async def test_finalize_returns_204_on_idempotent_redelivery(
    client: TestClient,
) -> None:
    """Pub/Sub redelivers an already-activated row → 'already_active' → 204."""
    envelope = make_pubsub_envelope()
    fake_assets = AsyncMock(return_value=ActivationOutcome(
        asset_id="asset_42", action="already_active",
        reason="row status='active'"
    ))

    with patch(
        "dynastore.extensions.gcp.gcp_events.handle_asset_events",
        fake_assets,
    ):
        from dynastore.extensions.gcp import gcp_events as ge
        ge._gcp_event_listeners.clear()
        ge.register_gcp_event_listener("*", ge.handle_gcs_notification)
        # First delivery
        r1 = client.post("/gcp/events/pubsub-push", json=envelope)
        # Second delivery — same body
        r2 = client.post("/gcp/events/pubsub-push", json=envelope)

    assert r1.status_code == 204
    assert r2.status_code == 204
    assert fake_assets.await_count == 2


@pytest.mark.asyncio
async def test_finalize_returns_204_on_orphan(client: TestClient) -> None:
    """Orphan finalize (no PENDING row) → activator raised
    OrphanFinalizeEvent, handler catches it and returns 204 (acked)."""
    envelope = make_pubsub_envelope()

    from dynastore.modules.gcp.gcp_finalize_activator import FinalizeEvent

    orphan_event = FinalizeEvent(
        bucket="bkt",
        object_name="catalog/orphan.tif",
        filename="orphan.tif",
        uri="gs://bkt/catalog/orphan.tif",
        catalog_id="cat_a",
    )
    fake_assets = AsyncMock(side_effect=OrphanFinalizeEvent(orphan_event))

    with patch(
        "dynastore.extensions.gcp.gcp_events.handle_asset_events",
        fake_assets,
    ):
        from dynastore.extensions.gcp import gcp_events as ge
        ge._gcp_event_listeners.clear()
        ge.register_gcp_event_listener("*", ge.handle_gcs_notification)
        resp = client.post("/gcp/events/pubsub-push", json=envelope)

    assert resp.status_code == 204
    fake_assets.assert_awaited_once()


@pytest.mark.asyncio
async def test_finalize_returns_5xx_on_transient_db_error(
    client: TestClient,
) -> None:
    """Transient DB failure surfaces as 5xx so Pub/Sub redelivers."""
    envelope = make_pubsub_envelope()

    class _ConnectionLost(Exception):
        """Stand-in for asyncpg.ConnectionDoesNotExistError."""

    fake_assets = AsyncMock(side_effect=_ConnectionLost("pool exhausted"))

    with patch(
        "dynastore.extensions.gcp.gcp_events.handle_asset_events",
        fake_assets,
    ):
        from dynastore.extensions.gcp import gcp_events as ge
        ge._gcp_event_listeners.clear()
        ge.register_gcp_event_listener("*", ge.handle_gcs_notification)
        resp = client.post("/gcp/events/pubsub-push", json=envelope)

    assert resp.status_code >= 500
    assert resp.status_code < 600


@pytest.mark.asyncio
async def test_finalize_returns_5xx_on_unexpected_exception(
    client: TestClient,
) -> None:
    """Any non-Orphan exception → 5xx for Pub/Sub redelivery."""
    envelope = make_pubsub_envelope()
    fake_assets = AsyncMock(side_effect=RuntimeError("boom"))

    with patch(
        "dynastore.extensions.gcp.gcp_events.handle_asset_events",
        fake_assets,
    ):
        from dynastore.extensions.gcp import gcp_events as ge
        ge._gcp_event_listeners.clear()
        ge.register_gcp_event_listener("*", ge.handle_gcs_notification)
        resp = client.post("/gcp/events/pubsub-push", json=envelope)

    assert resp.status_code >= 500


@pytest.mark.asyncio
async def test_non_finalize_event_does_not_invoke_activator(
    client: TestClient,
) -> None:
    """OBJECT_DELETE → activator must not be called; route still returns 204."""
    envelope = make_pubsub_envelope(event_type="OBJECT_DELETE")
    fake_activate = AsyncMock()

    with patch(
        "dynastore.modules.gcp.gcp_finalize_activator.activate",
        fake_activate,
    ):
        from dynastore.extensions.gcp import gcp_events as ge
        ge._gcp_event_listeners.clear()
        ge.register_gcp_event_listener("*", ge.handle_gcs_notification)
        resp = client.post("/gcp/events/pubsub-push", json=envelope)

    assert resp.status_code == 204
    fake_activate.assert_not_called()
