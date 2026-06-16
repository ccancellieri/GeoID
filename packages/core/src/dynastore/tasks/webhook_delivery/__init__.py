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

"""Webhook-delivery task — POSTs a drained event to an external subscriber.

The delivery half of the webhook-subscription feature: the event drain
(:class:`~dynastore.tasks.workclass_drain.event_drain_task.EventDrainTask`)
fans out one ``webhook_delivery`` task per matching ``tasks.event_subscriptions``
row after a successful in-process dispatch.  Each task re-fetches its
subscription by id (so the auth secret never leaves ``tasks.event_subscriptions``
to live in ``tasks.tasks.inputs``), applies the subscription's auth, and POSTs
``{"event_type", "payload"}`` — the shape the in-tree ``/gcp/events/webhook``
consumer reads.

Running as a generic control-plane task means delivery inherits the task
plane's retry/backoff, dead-letter, and replay machinery for free: a webhook
endpoint that is slow or down dead-letters its own row without blocking the
drain or other tenants, and is replayable via the standard
``requeue_dead_letter`` process.
"""

from .task import WebhookDeliveryTask, normalize_webhook_payload

__all__ = ["WebhookDeliveryTask", "normalize_webhook_payload"]
