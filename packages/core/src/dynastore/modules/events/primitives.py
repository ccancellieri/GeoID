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

import logging
from enum import Enum
from typing import Dict, Any

from dynastore.models.shared_models import EventType

logger = logging.getLogger(__name__)

class EventScope:
    PLATFORM = "platform"
    CATALOG = "catalog"
    COLLECTION = "collection"
    ITEM = "item"
    ASSET = "asset"


class EventRegistry:
    """Central registry for dynamic event definitions."""

    _events: Dict[str, str] = {}

    @classmethod
    def register(cls, name: str, scope: str):
        if name in cls._events and cls._events[name] != scope:
            logger.warning(
                f"Event '{name}' re-registered with different scope: {scope} (was {cls._events[name]})"
            )
        cls._events[name] = scope
        logger.debug(f"Registered event '{name}' with scope '{scope}'")

    @classmethod
    def is_valid(cls, name: str) -> bool:
        return name in cls._events


def define_event(name: str, scope: str = EventScope.CATALOG):
    """
    Decorator/Function to register a new event type dynamically.
    Usage:
        MY_EVENT = define_event("my.custom.event", EventScope.CATALOG)
    """
    EventRegistry.register(name, scope)
    return name


class SystemEventType(EventType):
    APPLICATION_START = define_event("application_start", EventScope.PLATFORM)
    APPLICATION_STOP = define_event("application_stop", EventScope.PLATFORM)
