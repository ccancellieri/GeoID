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

"""``dynastore.modules.tasks.events`` — event primitives and subscription models.

The domain-event primitive types (``EventScope``, ``EventRegistry``,
``define_event``, ``SystemEventType``) live in ``primitives``.

Webhook subscription DTOs (``EventSubscription``, ``EventSubscriptionCreate``,
and auth config classes) live in ``models``.

The low-level emit path (``emit_event_row``) lives in ``events_emit``.
"""

from dynastore.modules.tasks.events.primitives import (
    EventScope,
    EventRegistry,
    define_event,
    SystemEventType,
)
from dynastore.modules.tasks.events.models import (
    API_KEY_NAME,
    AuthConfiguration,
    AuthConfigAPIKey,
    AuthConfigNone,
    AuthConfigOAuth2,
    AuthConfigOIDC,
    AuthMethod,
    EventSubscription,
    EventSubscriptionCreate,
)

__all__ = [
    "EventScope",
    "EventRegistry",
    "define_event",
    "SystemEventType",
    "API_KEY_NAME",
    "AuthConfiguration",
    "AuthConfigAPIKey",
    "AuthConfigNone",
    "AuthConfigOAuth2",
    "AuthConfigOIDC",
    "AuthMethod",
    "EventSubscription",
    "EventSubscriptionCreate",
]
