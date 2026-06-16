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

# dynastore/tasks/webhook_delivery/task.py

"""``WebhookDeliveryTask`` — deliver one drained event to one webhook subscriber.

See the package docstring for how this fits the event-drain fan-out.  The task
is deliberately thin: it re-fetches the subscription (auth secret stays in
``tasks.event_subscriptions``), applies auth, POSTs, and lets the generic task
plane handle retry / dead-letter / replay on any HTTP failure.
"""
from __future__ import annotations

import logging
from typing import Any, ClassVar, Dict, Optional

from dynastore.models.tasks import TaskPayload
from dynastore.tasks.protocols import TaskProtocol
from dynastore.tasks.report import TaskReport

logger = logging.getLogger(__name__)


def normalize_webhook_payload(payload: Any) -> Dict[str, Any]:
    """Flatten the two event-payload shapes to a single domain dict for delivery.

    Events reach ``tasks.events`` in one of two shapes:

    * ``{"args": [...], "kwargs": {...}}`` — the generic outbox emit path
      (``EventService.emit`` packs positional + keyword args; see
      ``modules/catalog/event_service.py``).
    * a flat domain dict (e.g. ``{"catalog_id": ..., "collection_id": ...}``) —
      the ``EventDriverProtocol.publish`` path.

    External webhook consumers expect the flat domain dict — the shape the
    in-tree ``/gcp/events/webhook`` consumer reads.  This collapses the
    ``{args, kwargs}`` wrapper to its ``kwargs`` (the named domain fields),
    preserving any positional args under an ``"args"`` key so nothing is
    silently dropped.  A payload already in domain shape is returned unchanged;
    a non-dict payload degrades to ``{}``.
    """
    if not isinstance(payload, dict):
        return {}
    keys = set(payload.keys())
    if keys and keys <= {"args", "kwargs"}:
        domain: Dict[str, Any] = dict(payload.get("kwargs") or {})
        args = payload.get("args") or []
        if args:
            domain.setdefault("args", list(args))
        return domain
    return payload


def _correlation(
    subscription_id: Any, subscriber_name: Any
) -> Dict[str, str]:
    """Build the TaskReport correlation map (id + optional subscriber name)."""
    corr: Dict[str, str] = {"subscription_id": str(subscription_id)}
    if subscriber_name:
        corr["subscriber_name"] = str(subscriber_name)
    return corr


class WebhookDeliveryTask(TaskProtocol):
    """Deliver a single event to a single webhook subscriber.

    Inputs (set by the event drain fan-out):
        subscription_id:  the ``tasks.event_subscriptions`` row to deliver to.
        event_type:       the event type label (POSTed at the body top level).
        event_id:         the source ``tasks.events`` row id (correlation only).
        payload:          the normalized domain payload (POSTed under ``payload``).
        subscriber_name:  optional, for log / correlation readability.

    Routing: tier-agnostic (``affinity_tier = None``) — delivery is pure
    outbound HTTP, so the routing config places it freely (default matrix routes
    a tier-less system task to the ``catalog`` tier, co-located with the drain
    that enqueues it).  An operator can repoint it without a code change.
    """

    task_type: ClassVar[str] = "webhook_delivery"
    priority: int = 60
    affinity_tier: ClassVar[Optional[str]] = None

    def __init__(self, app_state: object | None = None) -> None:
        self.app_state = app_state

    async def run(self, payload: TaskPayload) -> TaskReport:
        inputs: Dict[str, Any] = dict(payload.inputs or {})
        subscription_id = inputs.get("subscription_id")
        event_type = inputs.get("event_type") or ""
        event_id = inputs.get("event_id")
        subscriber_name = inputs.get("subscriber_name")
        body_payload = inputs.get("payload") or {}

        if not subscription_id:
            # Malformed enqueue — nothing addressable to deliver to. Complete
            # (not fail): retrying cannot conjure a subscription_id.
            logger.warning(
                "webhook_delivery: no subscription_id in inputs for "
                "event_id=%s; nothing to deliver.",
                event_id,
            )
            return TaskReport.completed(
                message="webhook delivery skipped: no subscription_id in inputs",
                metrics={"delivered": 0},
            )

        # Re-fetch the subscription so the auth secret (and webhook URL) is read
        # from ``tasks.event_subscriptions`` at delivery time rather than copied
        # into ``tasks.tasks.inputs``.
        from dynastore.modules.tasks.event_driver import (  # noqa: PLC0415
            get_subscription_by_id,
        )

        subscription = await get_subscription_by_id(subscription_id)
        if subscription is None:
            # The operator unsubscribed between enqueue and delivery — honour
            # that intent and deliver nothing (a no-op completion, not a retry).
            logger.info(
                "webhook_delivery: subscription %s no longer exists; skipping "
                "delivery for event_id=%s.",
                subscription_id,
                event_id,
            )
            return TaskReport.completed(
                message=f"subscription {subscription_id} no longer exists; nothing delivered",
                metrics={"delivered": 0},
                correlation=_correlation(subscription_id, subscriber_name),
            )

        headers = self._build_auth_headers(subscription.auth_config)
        body = {"event_type": event_type, "payload": body_payload}

        from dynastore.modules.httpx.httpx_module import (  # noqa: PLC0415
            create_httpx_client,
        )

        # ``create_httpx_client`` already carries the standard transport-level
        # retries + timeout; any non-2xx raises below and the generic task plane
        # applies its own backoff / dead-letter on top.
        client = create_httpx_client()
        async with client:
            response = await client.post(
                str(subscription.webhook_url),
                json=body,
                headers=headers,
            )
            response.raise_for_status()

        return TaskReport.completed(
            message=(
                f"webhook delivered to {subscriber_name or subscription_id} "
                f"(HTTP {response.status_code})"
            ),
            metrics={"delivered": 1, "status_code": response.status_code},
            correlation=_correlation(subscription_id, subscriber_name),
        )

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def _build_auth_headers(self, auth_config: Any) -> Dict[str, str]:
        """Return the auth headers for *auth_config*.

        Implements the two methods wired today — ``NONE`` and ``API_KEY``.
        ``API_KEY`` sends the platform's shared outbound event key (the same
        secret the in-tree consumer validates) in the subscription's configured
        header.  ``OIDC`` / ``OAUTH2_CLIENT_CREDENTIALS`` are not yet supported
        and raise so the row is visible in the dead-letter queue rather than
        silently delivering unauthenticated.
        """
        from dynastore.modules.tasks.events.models import (  # noqa: PLC0415
            AuthConfigNone,
            AuthMethod,
        )

        if auth_config is None or isinstance(auth_config, AuthConfigNone):
            return {}

        method = getattr(auth_config, "auth_method", None)
        if method == AuthMethod.NONE:
            return {}

        if method == AuthMethod.API_KEY:
            from dynastore.modules.tasks.event_driver import (  # noqa: PLC0415
                PLATFORM_API_KEY,
            )

            header_name = getattr(auth_config, "header_name", None) or "X-API-Key"
            if not PLATFORM_API_KEY:
                logger.warning(
                    "webhook_delivery: API_KEY auth is configured but no "
                    "platform event key is set; sending the request without "
                    "the auth header.",
                )
                return {}
            return {header_name: PLATFORM_API_KEY}

        # OIDC / OAUTH2_CLIENT_CREDENTIALS: not yet implemented. Fail loudly.
        raise NotImplementedError(
            f"webhook auth method {method} is not yet supported "
            "(only NONE and API_KEY are implemented)"
        )
