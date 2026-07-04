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

"""#2918 — TaskRetentionService.tick emits ``tasks.health_alert`` (see
``dynastore.modules.tasks.tasks_module``), which must be declared via
``define_event`` like every other event type, so the registry reflects
reality instead of relying on ``EventService.emit``'s PLATFORM fallback
for unregistered names.
"""
from __future__ import annotations


def test_tasks_health_alert_event_is_registered():
    from dynastore.modules.catalog.event_service import CatalogEventType, EventScope
    from dynastore.modules.tasks.events.primitives import EventRegistry

    assert EventRegistry.is_valid("tasks.health_alert")
    assert EventRegistry._events["tasks.health_alert"] == EventScope.PLATFORM
    assert CatalogEventType.TASKS_HEALTH_ALERT == "tasks.health_alert"
